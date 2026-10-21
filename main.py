"""Main Application Entrypoint.

Starts FastAPI server, database connection pools, background WebSocket feeds,
independent multi-index strategy scanning tasks, and Telegram alerting.
"""

from __future__ import annotations
import asyncio
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

from config import settings
from app.monitoring.logging import logger
from app.monitoring.health import health_monitor
from app.database.models import RiskState, utc_now
from app.database.repository import DatabaseManager, Repository
from app.data.instrument_master import instrument_master
from app.data.quote_validator import QuoteValidator
from app.data.candle_builder import MultiIndexCandleManager
from app.data.base_provider import BaseMarketDataProvider, UnifiedQuote, UnifiedCandle
from app.data.dhan_provider import DhanProvider
from app.data.upstox_provider import UpstoxProvider
from app.data.websocket_manager import WebSocketFeedManager
from app.signals.detector import multi_index_detector, SignalCandidate
from app.signals.state_machine import SignalStateMachine, SignalStatus
from app.analysis.price_action import PriceActionAnalyzer, SetupType
from app.signals.risk_manager import risk_manager
from app.tracking.paper_trader import paper_trading_engine
from app.tracking.outcomes import outcome_tracker
from app.notifications.telegram import telegram_notifier
from app.api.health import router as health_router
from app.api.dashboard import router as dashboard_router, set_repository, set_candle_manager, set_market_provider


# Global application state
db_manager: Optional[DatabaseManager] = None
repository: Optional[Repository] = None
market_provider: Optional[BaseMarketDataProvider] = None
ws_manager: Optional[WebSocketFeedManager] = None
quote_validator = QuoteValidator(
    max_staleness_seconds=settings.MAX_QUOTE_STALENESS_SECONDS
)
candle_manager = MultiIndexCandleManager()
_cached_risk_state: Optional[RiskState] = None
_cached_risk_date: Optional[str] = None


from datetime import datetime, timezone, timedelta


async def get_active_risk_state() -> Optional[RiskState]:
    """Retrieves or initializes the cached daily risk state."""
    global _cached_risk_state, _cached_risk_date
    if not repository:
        return None
    today_str = utc_now().strftime("%Y-%m-%d")
    if _cached_risk_state is None or _cached_risk_date != today_str:
        _cached_risk_state = await repository.get_or_create_daily_risk_state(
            today_str, settings.ACCOUNT_CAPITAL
        )
        _cached_risk_date = today_str
    return _cached_risk_state


async def on_live_quote(quote: UnifiedQuote) -> None:
    """Callback triggered whenever a valid live tick arrives from WebSocket."""
    # 1. Feed into candle builder
    candle_manager.ingest_quote(quote)

    # 1b. Update continuous outcome excursion monitoring for clean signal feed
    if quote.ltp > 0:
        outcome_tracker.update_quote(
            contract_symbol=quote.symbol,
            current_price=quote.ltp,
            instrument_id=quote.instrument_id or "",
        )

    # 2. Update paper trades on price movements
    if repository:
        risk_state = await get_active_risk_state()
        if not risk_state:
            return
        closed_trades, milestone_events = paper_trading_engine.update_quote_with_events(
            contract_symbol=quote.symbol,
            current_price=quote.ltp,
            risk_state=risk_state,
            bid_price=quote.bid,
        )
        for t in closed_trades:
            await repository.close_paper_trade(
                trade_id=t.trade_id,
                exit_price=t.exit_price or quote.ltp,
                exit_reason=t.exit_reason or "MANUAL",
                gross_pnl=t.gross_pnl,
                fees=t.fees,
                slippage=t.slippage,
                net_pnl=t.net_pnl,
            )
            await repository.update_risk_state(risk_state)
            # Record final milestone in outcome tracker while keeping continuous quote monitoring active
            outcome_tracker.record_final(t.signal_id, t.exit_reason or "CLOSED")

        # 3. Handle milestone events (TP1, TP2, TP3, SL)
        for ev in milestone_events:
            if ev.event_type == "TP1_HIT":
                open_tr = next((t for t in paper_trading_engine.get_open_trades() if t.trade_id == ev.trade_id), None)
                if open_tr:
                    await repository.update_open_paper_trade(
                        trade_id=open_tr.trade_id,
                        quantity=open_tr.quantity,
                        lots=open_tr.lots,
                        sl_price=open_tr.sl_price,
                        gross_pnl=open_tr.gross_pnl or 0.0,
                        fees=open_tr.fees or 0.0,
                        slippage=open_tr.slippage or 0.0,
                        net_pnl=open_tr.net_pnl or 0.0,
                    )
                    await repository.update_risk_state(risk_state)
            await repository.update_signal_status(
                signal_id=ev.signal_id,
                new_status=ev.event_type,
                price=ev.trigger_price,
                details={
                    "gain_pts": ev.gain_pts,
                    "gain_pct": ev.gain_pct,
                    "profit_per_lot": ev.profit_per_lot,
                    "total_profit": ev.total_profit,
                },
            )
            outcome_tracker.record_final(ev.signal_id, ev.event_type)
            # NOTE: User Requirement 4: "No (2) Shall be in telegram"
            # Execution milestones (TP hits, SL hits, reasons) are kept in dedicated Deep-Dive UI (2), NOT sent to Telegram.


async def evaluate_index_on_completed_candle(timeframe: str, candle: UnifiedCandle) -> None:
    """Triggered on every completed candle (prevents look-ahead bias)."""
    if timeframe != "5m" or not repository or not market_provider:
        return

    # Extract underlying name
    underlying = candle.symbol.split(":")[0] if ":" in candle.symbol else candle.symbol
    for und in settings.INDICES.keys():
        if und in candle.symbol.upper():
            underlying = und
            break

    cfg = instrument_master.get_index_config(underlying)
    if not cfg or not cfg.enabled:
        return

    # Check risk state
    risk_state = await get_active_risk_state()
    if not risk_state or risk_state.trading_locked:
        return

    # Fetch multi-timeframe candles
    agg = candle_manager.get_or_create(candle.instrument_id, candle.symbol)
    candles_5m = list(agg.completed_5m)
    candles_15m = list(agg.completed_15m)
    if not candles_15m:
        candles_15m = await market_provider.get_historical_candles(
            candle.instrument_id, "15m", count=20
        )
    candles_daily = await market_provider.get_historical_candles(
        candle.instrument_id, "1d", count=5
    )

    # Fetch option chain snapshot
    option_chain = await market_provider.get_option_chain_snapshot(underlying)
    if not option_chain:
        return

    # Active open trades
    open_trades = paper_trading_engine.get_open_trades()
    open_underlyings = [t.underlying for t in open_trades]

    # Run independent strategy evaluation
    candidate = multi_index_detector.analyze_index(
        underlying=underlying,
        candles_5m=candles_5m,
        candles_15m=candles_15m,
        candles_daily=candles_daily,
        option_chain=option_chain,
        risk_state=risk_state,
        is_quote_fresh=not quote_validator.is_instrument_stale(candle.instrument_id),
        open_underlyings=open_underlyings,
    )

    if not candidate:
        return

    if candidate.signal.status == "ENTRY":
        await try_fire_entry(candidate, risk_state)
    else:
        await repository.create_signal(candidate.signal, candidate.levels)


_entry_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


async def try_fire_entry(candidate: SignalCandidate, risk_state: RiskState) -> bool:
    """Atomic unified entry executor with per-underlying locking, deterministic deduplication,
    cooldown enforcement, and paper trade execution.
    """
    if not repository or candidate.signal.status != "ENTRY" or not candidate.passed_all_gates:
        return False

    und = candidate.signal.underlying
    async with _entry_locks[und]:
        # 1. Deduplication: Prevent duplicate trade execution on the same setup or signal ID
        if candidate.signal.setup_key:
            if await repository.trade_exists_for_setup_key(candidate.signal.setup_key):
                logger.debug(f"Duplicate entry blocked for {und}: trade already executed for setup_key '{candidate.signal.setup_key}'.")
                return False

        if paper_trading_engine.is_signal_executed(candidate.signal.signal_id):
            logger.debug(f"Duplicate entry blocked for {und}: signal '{candidate.signal.signal_id}' already executed.")
            return False

        # 2. Position exclusivity: no new trade if position already open on this underlying
        open_trades = paper_trading_engine.get_open_trades()
        if any(t.underlying == und for t in open_trades):
            logger.debug(f"Entry blocked for {und}: active position already open on {und}.")
            return False

        # 3. Correlated cluster limit
        is_uncorr, corr_reason = risk_manager.check_correlation_exposure(
            und, [t.underlying for t in open_trades]
        )
        if not is_uncorr:
            logger.debug(f"Entry blocked for {und}: {corr_reason}")
            return False

        # 4. Cooldown enforcement after exit or terminal signal
        cooldown_mins = getattr(settings, "RESET_COOLDOWN_MINUTES", 20)
        recent_terminals = await repository.get_recent_terminal_signals(
            underlying=und, cooldown_minutes=cooldown_mins
        )
        if recent_terminals:
            logger.debug(f"Entry blocked for {und}: in {cooldown_mins}m terminal cooldown period.")
            return False

        last_exit = await repository.get_last_exit_time(und)
        if last_exit:
            now_utc = datetime.now(timezone.utc)
            last_exit_utc = last_exit if last_exit.tzinfo else last_exit.replace(tzinfo=timezone.utc)
            if (now_utc - last_exit_utc) < timedelta(minutes=cooldown_mins):
                logger.debug(f"Entry blocked for {und}: in {cooldown_mins}m cooldown after trade exit at {last_exit_utc}.")
                return False

        # 5. Daily risk limit check
        if risk_state.trading_locked:
            logger.debug(f"Entry blocked for {und}: trading locked ({risk_state.lock_reason}).")
            return False

        # Open in memory first so a risk/size rejection cannot leave a false
        # ENTRY signal in the feed. Signal, levels and paper trade are then
        # persisted together in one database transaction.
        previous_open_risk = risk_state.open_risk
        original_signal_id = candidate.signal.signal_id
        paper_trade = paper_trading_engine.open_trade(
            signal=candidate.signal,
            levels=candidate.levels,
            lot_size=candidate.contract.lot_size,
            risk_state=risk_state,
        )
        if not paper_trade:
            logger.info(
                f"[ENTRY REJECTED] {und} passed signal qualification but paper execution "
                "was rejected by position sizing or entry-price limits."
            )
            if risk_state.trading_locked:
                await repository.update_risk_state(risk_state)
            return False

        try:
            persisted_signal_id = await repository.create_signal(
                candidate.signal,
                candidate.levels,
                paper_trade=paper_trade,
            )
        except Exception as exc:
            paper_trading_engine.remove_trade_by_signal_id(paper_trade.signal_id)
            risk_state.open_risk = previous_open_risk
            logger.error(f"Could not persist paper entry for {und}; reverted in-memory trade: {exc}")
            return False

        paper_trading_engine.rebind_signal_id(original_signal_id, persisted_signal_id)
        candidate.signal.signal_id = persisted_signal_id
        candidate.levels.signal_id = persisted_signal_id
        try:
            await repository.update_risk_state(risk_state)
        except Exception as exc:
            logger.error(f"Paper trade persisted for {und}, but risk-state update failed: {exc}")

        entry_price = paper_trade.entry_price
        logger.info(
            f"🎯 [AUTO-ENTRY] Opened qualified ENTRY for {und}: "
            f"{candidate.signal.side} @ score {candidate.signal.score}/100 [Key: {candidate.signal.setup_key}]"
        )

        outcome_tracker.register_signal(
            signal_id=candidate.signal.signal_id,
            underlying=und,
            entry_price=entry_price,
            contract_symbol=candidate.contract.symbol,
            side=candidate.signal.side,
        )

        if settings.TELEGRAM_ENABLED:
            msg_id = await telegram_notifier.send_signal_alert(
                signal=candidate.signal,
                levels=candidate.levels,
                live_price=entry_price,
                highest_gain_pct=0.0,
                first_hour_gain_pct=None,
                momentum_status="ACTIVE",
            )
            if msg_id:
                await repository.record_telegram_message_id(candidate.signal.signal_id, msg_id)

        if ws_manager and candidate.contract and candidate.contract.instrument_id:
            ws_manager.subscribe([candidate.contract.instrument_id])
            if market_provider:
                await market_provider.subscribe_quotes([candidate.contract.instrument_id])
        logger.info(
            f"⚡ [AUTO-TRADE] Executed paper trade for {candidate.contract.symbol} @ ₹{paper_trade.entry_price}"
        )
        return True


async def notify_radar_signal(candidate: SignalCandidate) -> None:
    """Send a radar alert; early/developing alerts never paper-enter."""
    if not settings.TELEGRAM_ENABLED:
        return
    status_note = (
        "EARLY WATCH — RADAR ONLY; WAIT FOR 5-MINUTE ENTRY CONFIRMATION"
        if candidate.signal.status in ("EARLY_WATCH", "WATCH")
        else "SETUP DEVELOPING — WAIT FOR ENTRY CONFIRMATION"
    )
    message_id = await telegram_notifier.send_signal_alert(
        signal=candidate.signal,
        levels=candidate.levels,
        live_price=candidate.contract.ltp,
        highest_gain_pct=0.0,
        first_hour_gain_pct=None,
        momentum_status=status_note,
    )
    if message_id and repository:
        await repository.record_telegram_message_id(candidate.signal.signal_id, message_id)


async def auto_strategy_scanner_loop() -> None:
    """Continuously scan enabled indices for radar alerts and qualified paper entries.
    """
    logger.info(f"Autonomous multi-index strategy scanner started ({settings.SCANNER_INTERVAL_SECONDS}s interval).")
    # Initial brief warmup for feeds to populate
    await asyncio.sleep(4)

    while True:
        try:
            if repository and market_provider:
                risk_state = await get_active_risk_state()
                if risk_state and not risk_state.trading_locked:
                    open_trades = paper_trading_engine.get_open_trades()
                    open_underlyings = [t.underlying for t in open_trades]

                    # 1. Open trade exits are handled asynchronously on live ticks by on_live_quote (Single-Writer architecture)

                    # 2. Scanner & Signals Evaluation Loop across Indices
                    for und in settings.INDICES.keys():
                        cfg = instrument_master.get_index_config(und)
                        if not cfg or not cfg.enabled:
                            continue

                        if settings.ENFORCE_MARKET_HOURS:
                            is_open, _ = instrument_master.is_market_session_open(und)
                            if not is_open:
                                continue

                        # Check if there's already an active signal for this index in the last 30 mins
                        active_sigs = await repository.get_active_signals(max_age_minutes=30)
                        existing_radar = next(
                            (s for s in active_sigs if s.underlying == und and s.status in ("EARLY_WATCH", "SETUP_DEVELOPING", "WATCH")),
                            None
                        )
                        has_active_entry = any(
                            s.underlying == und and s.status == "ENTRY" and und in open_underlyings
                            for s in active_sigs
                        )
                        if has_active_entry:
                            continue

                        # Fetch multi-timeframe candles & chain
                        inst_id = f"IDX_{und}"
                        agg = candle_manager.get_or_create(inst_id, und)
                        candles_1m = list(agg.completed_1m)
                        if agg._current_1m is not None:
                            # The forming minute is used only for EARLY_WATCH radar;
                            # it can never qualify an entry by itself.
                            candles_1m.append(agg._current_1m.model_copy(deep=True))
                        candles_5m = list(agg.completed_5m)
                        if len(candles_5m) < 4:
                            candles_5m = await market_provider.get_historical_candles(inst_id, "5m", count=20)
                        candles_15m = list(agg.completed_15m)
                        if not candles_15m:
                            candles_15m = await market_provider.get_historical_candles(inst_id, "15m", count=20)
                        candles_daily = await market_provider.get_historical_candles(inst_id, "1d", count=5)

                        chain = await market_provider.get_option_chain_snapshot(und)
                        if not chain:
                            continue
                        if chain.is_simulated or str(chain.source).endswith("_sim"):
                            logger.warning(f"Suppressing {und} signal scan: option-chain source is simulated ({chain.source}).")
                            continue

                        latest_quote = agg.last_quote
                        if not latest_quote or latest_quote.source.endswith("_sim"):
                            continue
                        received_at = latest_quote.receive_timestamp
                        exchange_at = latest_quote.exchange_timestamp
                        if received_at.tzinfo is None:
                            received_at = received_at.replace(tzinfo=timezone.utc)
                        if exchange_at.tzinfo is None:
                            exchange_at = exchange_at.replace(tzinfo=timezone.utc)
                        quote_age = max(
                            (datetime.now(timezone.utc) - received_at.astimezone(timezone.utc)).total_seconds(),
                            (datetime.now(timezone.utc) - exchange_at.astimezone(timezone.utc)).total_seconds(),
                        )
                        if quote_age < 0 or quote_age > settings.MAX_QUOTE_STALENESS_SECONDS:
                            continue
                        # Use the newest validated underlying tick alongside the REST
                        # chain so the radar is not anchored to an older spot snapshot.
                        chain.underlying_ltp = latest_quote.ltp

                        # --- STAGE INTEGRITY & INVALIDATION / EXPIRATION AUDIT ---
                        if existing_radar:
                            # 1. Structural Invalidation: Check if underlying spot breached invalidation level
                            radar_levels = await repository.get_signal_levels(existing_radar.signal_id)
                            if radar_levels and chain.underlying_ltp > 0:
                                spot = chain.underlying_ltp
                                invalidated = False
                                inv_reason = ""
                                if existing_radar.side == "BUY_CE" and spot <= radar_levels.underlying_invalidation:
                                    invalidated = True
                                    inv_reason = f"Spot ({spot:.2f}) dropped below invalidation level ({radar_levels.underlying_invalidation:.2f})"
                                elif existing_radar.side == "BUY_PE" and spot >= radar_levels.underlying_invalidation:
                                    invalidated = True
                                    inv_reason = f"Spot ({spot:.2f}) rallied above invalidation level ({radar_levels.underlying_invalidation:.2f})"

                                if invalidated:
                                    logger.info(f"❌ [INVALIDATION] {und} radar signal {existing_radar.signal_id} INVALIDATED: {inv_reason}")
                                    await repository.update_signal_status(
                                        signal_id=existing_radar.signal_id,
                                        new_status=SignalStatus.INVALIDATED,
                                        price=spot,
                                        details={"reason": inv_reason},
                                    )
                                    existing_radar = None

                            # 2. Expiration: Check observation window (10m for EARLY_WATCH, 15m for SETUP_DEVELOPING)
                            if existing_radar:
                                if SignalStateMachine.evaluate_expiration(
                                    status=existing_radar.status,
                                    created_at=existing_radar.created_at,
                                    max_watch_minutes=10.0,
                                    max_developing_minutes=15.0,
                                ):
                                    logger.info(
                                        f"⌛ [EXPIRED] {und} {existing_radar.status} signal {existing_radar.signal_id} EXPIRED: observation window timed out."
                                    )
                                    await repository.update_signal_status(
                                        signal_id=existing_radar.signal_id,
                                        new_status=SignalStatus.EXPIRED,
                                        details={"reason": "Observation window timed out without qualification"},
                                    )
                                    existing_radar = None

                            # 3. Directional Structural Wick Invalidation (only genuine structural failure invalidates)
                            if existing_radar and candles_5m:
                                last_candle = candles_5m[-1]
                                wick_dir = "BULLISH" if existing_radar.side == "BUY_CE" else "BEARISH"
                                boundary = radar_levels.underlying_invalidation if radar_levels else None
                                if PriceActionAnalyzer.check_structural_wick_invalidation(last_candle, wick_dir, boundary):
                                    logger.info(
                                        f"❌ [INVALIDATION] {und} {existing_radar.status} signal {existing_radar.signal_id} INVALIDATED: structural rejection wick formed."
                                    )
                                    await repository.update_signal_status(
                                        signal_id=existing_radar.signal_id,
                                        new_status=SignalStatus.INVALIDATED,
                                        details={"reason": "Structural adverse rejection wick detected on active setup"},
                                    )
                                    existing_radar = None

                        # 4. Terminal Cooldown Reset Check: Prevent automatic resurrection of dead setups
                        if not existing_radar:
                            recent_terminals = await repository.get_recent_terminal_signals(
                                underlying=und, cooldown_minutes=settings.RESET_COOLDOWN_MINUTES
                            )
                            if recent_terminals:
                                # Cooldown active; prevent immediate resurrection without structural reset
                                continue

                        candidate = multi_index_detector.analyze_index(
                            underlying=und,
                            candles_5m=candles_5m,
                            candles_15m=candles_15m,
                            candles_daily=candles_daily,
                            option_chain=chain,
                            risk_state=risk_state,
                            is_quote_fresh=(
                                quote_age <= settings.MAX_QUOTE_STALENESS_SECONDS
                                and not quote_validator.is_instrument_stale(latest_quote.instrument_id)
                            ),
                            open_underlyings=open_underlyings,
                            candles_1m=candles_1m,
                            quote_staleness_sec=quote_age,
                        )

                        if not candidate:
                            continue

                        if candidate.signal.status in ("EARLY_WATCH", "SETUP_DEVELOPING", "WATCH"):
                            contract_id = candidate.contract.instrument_id
                            if contract_id and contract_id not in getattr(market_provider, "_subscribed_ids", set()):
                                ws_manager.subscribe([contract_id])
                                await market_provider.subscribe_quotes([contract_id])

                        # Three-Stage Transition & Promotion / Downgrade Logic
                        if candidate.signal.status == "ENTRY":
                            if existing_radar:
                                if SignalStateMachine.can_transition(existing_radar.status, SignalStatus.ENTRY):
                                    logger.info(
                                        f"⚡ [PROMOTION] Upgrading {und} signal {existing_radar.signal_id} "
                                        f"from {existing_radar.status} -> ENTRY"
                                    )
                                    candidate.signal.reasons.insert(
                                        0, f"⚡ PROMOTED FROM {existing_radar.status}: Breakout qualified and verified!"
                                    )
                                    candidate.signal.signal_id = existing_radar.signal_id
                                else:
                                    continue

                            await try_fire_entry(candidate, risk_state)

                        elif candidate.signal.status == "SETUP_DEVELOPING":
                            if existing_radar:
                                if existing_radar.status in ("EARLY_WATCH", "WATCH"):
                                    if SignalStateMachine.can_transition(existing_radar.status, SignalStatus.SETUP_DEVELOPING):
                                        logger.info(
                                            f"⏳ [PROMOTION] Upgrading {und} signal {existing_radar.signal_id} "
                                            f"from {existing_radar.status} -> SETUP_DEVELOPING"
                                        )
                                        await repository.update_signal_status(
                                            signal_id=existing_radar.signal_id,
                                            new_status="SETUP_DEVELOPING",
                                            price=candidate.levels.entry_range_high,
                                            score=candidate.signal.score,
                                            reasons=candidate.signal.reasons,
                                            score_breakdown=candidate.signal.score_breakdown,
                                            details={"promoted_from": existing_radar.status},
                                        )
                                continue
                            else:
                                await repository.create_signal(candidate.signal, candidate.levels)
                                await notify_radar_signal(candidate)
                                logger.info(
                                    f"⏳ [DEVELOPING] Setup developing for {und}: "
                                    f"{candidate.signal.side} @ score {candidate.signal.score}/100"
                                )

                        elif candidate.signal.status in ("EARLY_WATCH", "WATCH"):
                            if existing_radar:
                                if existing_radar.status == "SETUP_DEVELOPING":
                                    # Momentum cooled down from developing back to early watch
                                    if SignalStateMachine.can_transition(existing_radar.status, SignalStatus.EARLY_WATCH):
                                        logger.info(
                                            f"🔻 [DOWNGRADE] {und} signal {existing_radar.signal_id} "
                                            f"cooled down from SETUP_DEVELOPING -> EARLY_WATCH"
                                        )
                                        await repository.update_signal_status(
                                            signal_id=existing_radar.signal_id,
                                            new_status="EARLY_WATCH",
                                            details={"reason": "Momentum cooled down; downgraded to early watch"},
                                        )
                                continue
                            else:
                                await repository.create_signal(candidate.signal, candidate.levels)
                                await notify_radar_signal(candidate)
                                logger.info(
                                    f"👁️ [EARLY WATCH] Setup on early radar for {und}: "
                                    f"{candidate.signal.side} @ score {candidate.signal.score}/100"
                                )

            await asyncio.sleep(settings.SCANNER_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in auto strategy scanner loop: {e}")
            await asyncio.sleep(10)


async def hourly_reporting_loop() -> None:
    """Automated hourly report: fired signals, triggered TPs, and win ratio."""
    while True:
        try:
            await asyncio.sleep(3600)  # Runs every 1 hour
            if repository and settings.TELEGRAM_ENABLED:
                one_hour_ago = utc_now() - timedelta(hours=1)
                stats = await repository.get_hourly_summary_stats(window_start=one_hour_ago)
                await telegram_notifier.send_hourly_report(stats)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in hourly reporting loop: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages application startup and graceful shutdown."""
    global db_manager, repository, market_provider, ws_manager
    logger.info("Starting Multi-Index Indian Options Signal Bot...")

    # 1. Database Initialization
    db_manager = DatabaseManager(settings.DATABASE_URL)
    await db_manager.init_db()
    repository = Repository(db_manager.session_factory)
    set_repository(repository)
    health_monitor.record_db_status(True)

    # Restore active paper trades and all historically executed signal IDs from persistent DB
    open_trades_db = await repository.get_open_paper_trades()
    for ot in open_trades_db:
        sig = await repository.get_signal_by_id(ot.signal_id)
        sig_st = sig.status if sig else None
        paper_trading_engine.restore_trade(ot, signal_status=sig_st)
    all_executed_ids = await repository.get_all_executed_signal_ids()
    paper_trading_engine.load_executed_signals(all_executed_ids)
    logger.info(
        f"Restored {len(open_trades_db)} active paper trade(s) and {len(all_executed_ids)} historically executed signal ID(s) from database."
    )

    # 2. Instrument Master
    await instrument_master.load_or_refresh(force_download=False)

    # 3. Market Data Provider Setup
    if settings.PRIMARY_BROKER == "upstox" or (settings.UPSTOX_ACCESS_TOKEN and not settings.DHAN_ACCESS_TOKEN):
        market_provider = UpstoxProvider()
    elif settings.PRIMARY_BROKER == "dhan":
        market_provider = DhanProvider()
    else:
        market_provider = DhanProvider()  # Fallback to Dhan / simulated

    set_market_provider(market_provider)

    ws_manager = WebSocketFeedManager(
        provider=market_provider,
        quote_validator=quote_validator,
    )
    ws_manager.add_quote_handler(lambda q: asyncio.create_task(on_live_quote(q)))

    set_candle_manager(candle_manager)

    # Hook candle closure to detector and pre-seed recent 5m structure
    index_ids = [f"IDX_{und}" for und, cfg in settings.INDICES.items() if cfg.enabled]
    ws_manager.subscribe(index_ids)

    for und, cfg in settings.INDICES.items():
        if not cfg.enabled:
            continue
        idx_agg = candle_manager.get_or_create(f"IDX_{und}", und)
        hist_5m = await market_provider.get_historical_candles(f"IDX_{und}", "5m", count=20)
        idx_agg.completed_5m.extend(hist_5m)
        idx_agg.on_candle_closed.append(
            lambda tf, c: asyncio.create_task(evaluate_index_on_completed_candle(tf, c))
        )

    await ws_manager.start()

    # Resubscribe restored open trade contracts for live WebSocket streaming
    restored_symbols = [ot.contract_symbol for ot in open_trades_db if ot.contract_symbol]
    if restored_symbols:
        ws_manager.subscribe(restored_symbols)
        await market_provider.subscribe_quotes(restored_symbols)
        logger.info(f"Subscribed {len(restored_symbols)} restored open trade contracts to live market feed.")

    # Start 1-Hour Automated Telegram Summary Report Task
    hourly_report_task = asyncio.create_task(hourly_reporting_loop())
    # Start Autonomous Strategy Scanner Loop (every 5 seconds)
    scanner_task = asyncio.create_task(auto_strategy_scanner_loop())

    # Telegram startup alert
    if settings.TELEGRAM_ENABLED:
        health_monitor.record_telegram_status(True)
        await telegram_notifier.send_health_notification(
            f"✅ Multi-Index Options Signal Bot initialized.\n"
            f"Mode: PAPER TRADING\n"
            f"Indices: {', '.join(settings.INDICES.keys())}\n"
            f"Broker Adapter: {market_provider.name.upper()}\n"
            f"Autonomous Scanner: Active (Auto-firing genuine signals every {settings.SCANNER_INTERVAL_SECONDS}s)\n"
            f"Hourly Report: Active (Every 60 minutes)"
        )

    yield

    # Graceful Shutdown
    logger.info("Shutting down Multi-Index Signal Bot...")
    hourly_report_task.cancel()
    scanner_task.cancel()
    try:
        await hourly_report_task
    except asyncio.CancelledError:
        pass
    try:
        await scanner_task
    except asyncio.CancelledError:
        pass

    if ws_manager:
        await ws_manager.stop()
    if db_manager:
        await db_manager.close()
    if settings.TELEGRAM_ENABLED:
        await telegram_notifier.send_health_notification("🛑 Multi-Index Options Signal Bot stopped.")
    logger.info("Graceful shutdown complete.")


# Initialize FastAPI App
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

# Include Routers
app.include_router(health_router)
app.include_router(dashboard_router)


# Web Dashboard UI Routes
@app.get("/", include_in_schema=False)
@app.get("/dashboard", include_in_schema=False)
async def serve_dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    return {"message": "Multi-Index Options Signal Bot is running. Dashboard file not found."}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
