"""Asynchronous database repository and session manager.

Handles connection pooling, table initialization, and high-performance
queries for signals, paper trades, risk state, and metrics.
"""

from __future__ import annotations
import os
from datetime import datetime, date, timezone, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, update, delete, desc, and_, func

from app.database.models import (
    Base,
    Instrument,
    MarketTick,
    Candle,
    FeatureSnapshot,
    NewsEvent,
    Signal,
    SignalLevels,
    PaperTrade,
    SignalEvent,
    RiskState,
    SystemHealthRecord,
    StrategyVersion,
    utc_now,
)
from app.monitoring.logging import logger


class DatabaseManager:
    """Manages async database engine and sessions."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        # SQLite needs check_same_thread=False
        connect_args = {"check_same_thread": False} if "sqlite" in db_url else {}
        self.engine = create_async_engine(
            self.db_url,
            echo=False,
            future=True,
            connect_args=connect_args,
        )
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def init_db(self) -> None:
        """Create all tables if they do not exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

            def _migrate_columns(sync_conn):
                try:
                    cursor = sync_conn.connection.cursor()
                    cursor.execute("PRAGMA table_info(signals)")
                    cols = [r[1] for r in cursor.fetchall()]
                    if cols and "setup_key" not in cols:
                        cursor.execute("ALTER TABLE signals ADD COLUMN setup_key VARCHAR(64)")
                        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_signals_setup_key ON signals(setup_key)")
                except Exception as e:
                    logger.debug(f"Column migration check note: {e}")

            await conn.run_sync(_migrate_columns)
        logger.info(f"Database schema initialized successfully at {self.db_url}")

    async def close(self) -> None:
        await self.engine.dispose()
        logger.info("Database engine closed.")


class Repository:
    """Data access repository for all trading system entities."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    # --- Instruments ---
    async def upsert_instruments(self, instruments: List[Instrument]) -> None:
        async with self.session_factory() as session:
            for inst in instruments:
                await session.merge(inst)
            await session.commit()

    async def get_instrument(self, instrument_id: str) -> Optional[Instrument]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(Instrument).where(Instrument.id == instrument_id)
            )
            return result.scalar_one_or_none()

    async def get_active_contracts(
        self, underlying: str, option_type: Optional[str] = None
    ) -> List[Instrument]:
        async with self.session_factory() as session:
            query = select(Instrument).where(
                and_(
                    Instrument.underlying == underlying,
                    Instrument.is_active == True,
                )
            )
            if option_type:
                query = query.where(Instrument.option_type == option_type)
            result = await session.execute(query)
            return list(result.scalars().all())

    # --- Market Ticks & Candles ---
    async def record_tick(self, tick: MarketTick) -> None:
        async with self.session_factory() as session:
            session.add(tick)
            await session.commit()

    async def record_candle(self, candle: Candle) -> None:
        async with self.session_factory() as session:
            session.add(candle)
            await session.commit()

    async def get_latest_candles(
        self, instrument_id: str, timeframe: str, limit: int = 100
    ) -> List[Candle]:
        async with self.session_factory() as session:
            query = (
                select(Candle)
                .where(
                    and_(
                        Candle.instrument_id == instrument_id,
                        Candle.timeframe == timeframe,
                    )
                )
                .order_by(desc(Candle.timestamp))
                .limit(limit)
            )
            result = await session.execute(query)
            candles = list(result.scalars().all())
            candles.reverse()  # Chronological order
            return candles

    # --- News Events ---
    async def save_news_event(self, event: NewsEvent) -> bool:
        """Saves news event. Returns True if inserted, False if duplicate."""
        async with self.session_factory() as session:
            existing = await session.execute(
                select(NewsEvent).where(NewsEvent.dedup_key == event.dedup_key)
            )
            if existing.scalar_one_or_none():
                return False
            session.add(event)
            await session.commit()
            return True

    async def get_active_event_vetoes(self, window_start: datetime, window_end: datetime) -> List[NewsEvent]:
        async with self.session_factory() as session:
            query = select(NewsEvent).where(
                and_(
                    NewsEvent.publication_time >= window_start,
                    NewsEvent.publication_time <= window_end,
                    NewsEvent.impact_level == "HIGH",
                )
            )
            result = await session.execute(query)
            return list(result.scalars().all())

    # --- Signals & Levels ---
    async def setup_key_exists(self, setup_key: str) -> bool:
        if not setup_key:
            return False
        async with self.session_factory() as session:
            result = await session.execute(
                select(func.count(Signal.signal_id)).where(Signal.setup_key == setup_key)
            )
            count = result.scalar() or 0
            return count > 0

    async def trade_exists_for_setup_key(self, setup_key: str) -> bool:
        """Checks if a paper trade has already been opened or executed for this setup_key."""
        if not setup_key:
            return False
        async with self.session_factory() as session:
            query = (
                select(func.count(PaperTrade.trade_id))
                .join(Signal, PaperTrade.signal_id == Signal.signal_id)
                .where(Signal.setup_key == setup_key)
            )
            result = await session.execute(query)
            count = result.scalar() or 0
            return count > 0

    async def trade_exists_for_signal(self, signal_id: str) -> bool:
        """Checks if a paper trade has already been opened for this signal_id."""
        if not signal_id:
            return False
        async with self.session_factory() as session:
            result = await session.execute(
                select(func.count(PaperTrade.trade_id)).where(PaperTrade.signal_id == signal_id)
            )
            count = result.scalar() or 0
            return count > 0

    async def create_signal(self, signal: Signal, levels: SignalLevels) -> str:
        async with self.session_factory() as session:
            if signal.setup_key:
                existing = await session.execute(
                    select(Signal).where(Signal.setup_key == signal.setup_key)
                )
                existing_sig = existing.scalar_one_or_none()
                if existing_sig:
                    # Upgrade/update record with latest qualified stage data
                    existing_sig.status = signal.status
                    existing_sig.score = signal.score
                    existing_sig.reasons = signal.reasons
                    existing_sig.score_breakdown = signal.score_breakdown
                    await session.commit()
                    return existing_sig.signal_id

            session.add(signal)
            levels.signal_id = signal.signal_id
            session.add(levels)
            ev = SignalEvent(
                signal_id=signal.signal_id,
                event_type="GENERATED",
                price=levels.entry_range_high,
                details={"score": signal.score, "reasons": signal.reasons},
            )
            session.add(ev)
            await session.commit()
            return signal.signal_id

    async def get_signal_by_id(self, signal_id: str) -> Optional[Signal]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(Signal).where(Signal.signal_id == signal_id)
            )
            return result.scalar_one_or_none()

    async def get_active_signals(self, max_age_minutes: Optional[int] = None) -> List[Signal]:
        async with self.session_factory() as session:
            query = select(Signal).where(
                Signal.status.in_([
                    "EARLY_WATCH", "SETUP_DEVELOPING", "ENTRY", "WATCH", "TP1_HIT", "TP2_HIT"
                ])
            )
            if max_age_minutes is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
                query = query.where(Signal.created_at >= cutoff)
            query = query.order_by(desc(Signal.created_at))
            result = await session.execute(query)
            return list(result.scalars().all())

    async def get_recent_terminal_signals(
        self, underlying: str, cooldown_minutes: int = 20
    ) -> List[Signal]:
        """Returns signals for an underlying that terminated (SL, TP, CLOSED, INVALIDATED, EXPIRED) within the cooldown period."""
        async with self.session_factory() as session:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
            query = (
                select(Signal)
                .where(
                    and_(
                        Signal.underlying == underlying,
                        Signal.status.in_([
                            "SL_HIT",
                            "BREAKEVEN_SL_HIT",
                            "TP1_HIT",
                            "TP2_HIT",
                            "TP3_HIT",
                            "CLOSED",
                            "INVALIDATED",
                            "EXPIRED",
                        ]),
                        Signal.created_at >= cutoff,
                    )
                )
                .order_by(desc(Signal.created_at))
            )
            result = await session.execute(query)
            return list(result.scalars().all())

    async def get_last_exit_time(self, underlying: str) -> Optional[datetime]:
        """Returns the most recent exit_time for an executed paper trade on this underlying."""
        async with self.session_factory() as session:
            query = (
                select(PaperTrade.exit_time)
                .where(
                    and_(
                        PaperTrade.underlying == underlying,
                        PaperTrade.exit_time != None,
                    )
                )
                .order_by(desc(PaperTrade.exit_time))
                .limit(1)
            )
            result = await session.execute(query)
            return result.scalar_one_or_none()

    async def delete_signal(self, signal_id: str) -> bool:
        """Deletes a signal and all cascading levels, events, and linked trades."""
        async with self.session_factory() as session:
            await session.execute(delete(PaperTrade).where(PaperTrade.signal_id == signal_id))
            await session.execute(delete(SignalLevels).where(SignalLevels.signal_id == signal_id))
            await session.execute(delete(SignalEvent).where(SignalEvent.signal_id == signal_id))
            res = await session.execute(delete(Signal).where(Signal.signal_id == signal_id))
            await session.commit()
            return (res.rowcount or 0) > 0

    async def clear_all_signals(self) -> int:
        """Purges all fired signals, levels, and associated executions for a clean slate."""
        async with self.session_factory() as session:
            count_res = await session.execute(select(func.count(Signal.signal_id)))
            total = count_res.scalar() or 0
            await session.execute(delete(PaperTrade))
            await session.execute(delete(SignalLevels))
            await session.execute(delete(SignalEvent))
            await session.execute(delete(Signal))
            await session.commit()
            return total

    async def update_signal_status(
        self,
        signal_id: str,
        new_status: str,
        price: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
        score: Optional[float] = None,
        reasons: Optional[List[str]] = None,
        score_breakdown: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self.session_factory() as session:
            vals: Dict[str, Any] = {"status": new_status}
            if score is not None:
                vals["score"] = score
            if reasons is not None:
                vals["reasons"] = reasons
            if score_breakdown is not None:
                vals["score_breakdown"] = score_breakdown

            await session.execute(
                update(Signal).where(Signal.signal_id == signal_id).values(**vals)
            )
            ev = SignalEvent(
                signal_id=signal_id,
                event_type=new_status,
                price=price,
                details=details or {},
            )
            session.add(ev)
            await session.commit()

    async def record_telegram_message_id(self, signal_id: str, message_id: int) -> None:
        async with self.session_factory() as session:
            await session.execute(
                update(Signal).where(Signal.signal_id == signal_id).values(telegram_message_id=message_id)
            )
            await session.commit()

    # --- Paper Trades ---
    async def create_paper_trade(self, trade: PaperTrade) -> str:
        async with self.session_factory() as session:
            session.add(trade)
            await session.commit()
            return trade.trade_id

    async def get_open_paper_trades(self) -> List[PaperTrade]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(PaperTrade).where(PaperTrade.status == "OPEN")
            )
            return list(result.scalars().all())

    async def get_all_executed_signal_ids(self) -> set[str]:
        """Returns all signal_ids ever executed in paper_trades (both OPEN and CLOSED)."""
        async with self.session_factory() as session:
            result = await session.execute(select(PaperTrade.signal_id))
            return set(result.scalars().all())

    async def update_open_paper_trade(
        self,
        trade_id: str,
        quantity: int,
        lots: int,
        sl_price: float,
        gross_pnl: float,
        fees: float,
        slippage: float,
        net_pnl: float,
    ) -> None:
        """Updates an active paper trade following partial milestone booking (e.g. TP1)."""
        async with self.session_factory() as session:
            await session.execute(
                update(PaperTrade)
                .where(PaperTrade.trade_id == trade_id)
                .values(
                    quantity=quantity,
                    lots=lots,
                    sl_price=sl_price,
                    gross_pnl=gross_pnl,
                    fees=fees,
                    slippage=slippage,
                    net_pnl=net_pnl,
                )
            )
            await session.commit()

    async def close_paper_trade(
        self,
        trade_id: str,
        exit_price: float,
        exit_reason: str,
        gross_pnl: float,
        fees: float,
        slippage: float,
        net_pnl: float,
    ) -> None:
        async with self.session_factory() as session:
            await session.execute(
                update(PaperTrade)
                .where(PaperTrade.trade_id == trade_id)
                .values(
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    exit_time=utc_now(),
                    gross_pnl=gross_pnl,
                    fees=fees,
                    slippage=slippage,
                    net_pnl=net_pnl,
                    status="CLOSED",
                )
            )
            await session.commit()

    async def get_all_paper_trades(self, limit: int = 100) -> List[PaperTrade]:
        async with self.session_factory() as session:
            query = select(PaperTrade).order_by(desc(PaperTrade.entry_time)).limit(limit)
            result = await session.execute(query)
            return list(result.scalars().all())

    async def get_or_create_daily_risk_state(
        self, session_date_str: str, default_capital: float
    ) -> RiskState:
        async with self.session_factory() as session:
            result = await session.execute(
                select(RiskState).where(RiskState.session_date == session_date_str)
            )
            state = result.scalar_one_or_none()
            if not state:
                try:
                    state = RiskState(
                        session_date=session_date_str,
                        starting_capital=default_capital,
                        realized_pnl=0.0,
                        unrealized_pnl=0.0,
                        open_risk=0.0,
                        consecutive_losses=0,
                        stop_outs_count=0,
                        trading_locked=False,
                        lock_reason=None,
                    )
                    session.add(state)
                    await session.commit()
                    await session.refresh(state)
                except Exception:
                    await session.rollback()
                    result = await session.execute(
                        select(RiskState).where(RiskState.session_date == session_date_str)
                    )
                    state = result.scalar_one_or_none()
            return state

    async def update_risk_state(self, state: RiskState) -> None:
        async with self.session_factory() as session:
            await session.merge(state)
            await session.commit()

    # --- Strategy Version ---
    async def get_or_create_strategy_version(
        self, version_tag: str, name: str, config: Dict[str, Any], weights: Dict[str, Any]
    ) -> StrategyVersion:
        async with self.session_factory() as session:
            result = await session.execute(
                select(StrategyVersion).where(StrategyVersion.version_tag == version_tag)
            )
            existing = result.scalar_one_or_none()
            if not existing:
                existing = StrategyVersion(
                    version_tag=version_tag,
                    name=name,
                    configuration=config,
                    scoring_weights=weights,
                    is_active=True,
                )
                session.add(existing)
                await session.commit()
                await session.refresh(existing)
            return existing

    async def get_signal_levels(self, signal_id: str) -> Optional[SignalLevels]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(SignalLevels).where(SignalLevels.signal_id == signal_id)
            )
            return result.scalar_one_or_none()

    async def get_hourly_summary_stats(
        self, window_start: datetime, session_start: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Calculates fired signals, triggered TPs, SLs, and success ratio for hourly & session windows."""
        async with self.session_factory() as session:
            # 1. Hourly signals
            h_sig_res = await session.execute(
                select(Signal).where(Signal.created_at >= window_start)
            )
            hourly_signals = list(h_sig_res.scalars().all())

            h_fired = len(hourly_signals)
            h_triggered = sum(1 for s in hourly_signals if s.status not in ("WATCH", "INVALIDATED", "ENTRY_MISSED"))
            h_tp1 = sum(1 for s in hourly_signals if "TP1" in s.status or "TP2" in s.status or "TP3" in s.status)
            h_tp2 = sum(1 for s in hourly_signals if "TP2" in s.status or "TP3" in s.status)
            h_tp3 = sum(1 for s in hourly_signals if "TP3" in s.status)
            h_sl = sum(1 for s in hourly_signals if "SL" in s.status)
            h_success_ratio = round((h_tp1 / max(h_triggered, 1)) * 100.0, 1) if h_triggered > 0 else 0.0

            # Hourly trades P&L
            h_trade_res = await session.execute(
                select(PaperTrade).where(
                    and_(PaperTrade.status == "CLOSED", PaperTrade.exit_time >= window_start)
                )
            )
            hourly_trades = list(h_trade_res.scalars().all())
            h_pnl = sum((t.net_pnl or 0.0) for t in hourly_trades)

            # 2. Cumulative session signals
            sess_dt = session_start or window_start.replace(hour=3, minute=45, second=0)  # 09:15 IST = 03:45 UTC
            s_sig_res = await session.execute(
                select(Signal).where(Signal.created_at >= sess_dt)
            )
            sess_signals = list(s_sig_res.scalars().all())

            s_fired = len(sess_signals)
            s_triggered = sum(1 for s in sess_signals if s.status not in ("WATCH", "INVALIDATED", "ENTRY_MISSED"))
            s_tp1 = sum(1 for s in sess_signals if "TP1" in s.status or "TP2" in s.status or "TP3" in s.status)
            s_tp2 = sum(1 for s in sess_signals if "TP2" in s.status or "TP3" in s.status)
            s_tp3 = sum(1 for s in sess_signals if "TP3" in s.status)
            s_sl = sum(1 for s in sess_signals if "SL" in s.status)
            s_success_ratio = round((s_tp1 / max(s_triggered, 1)) * 100.0, 1) if s_triggered > 0 else 0.0

            s_trade_res = await session.execute(
                select(PaperTrade).where(
                    and_(PaperTrade.status == "CLOSED", PaperTrade.exit_time >= sess_dt)
                )
            )
            sess_trades = list(s_trade_res.scalars().all())
            s_pnl = sum((t.net_pnl or 0.0) for t in sess_trades)

            return {
                "hourly": {
                    "signals_fired": h_fired,
                    "signals_triggered": h_triggered,
                    "tp1_hits": h_tp1,
                    "tp2_hits": h_tp2,
                    "tp3_hits": h_tp3,
                    "sl_hits": h_sl,
                    "success_ratio_pct": h_success_ratio,
                    "realized_pnl": round(h_pnl, 2),
                },
                "session": {
                    "signals_fired": s_fired,
                    "signals_triggered": s_triggered,
                    "tp1_hits": s_tp1,
                    "tp2_hits": s_tp2,
                    "tp3_hits": s_tp3,
                    "sl_hits": s_sl,
                    "success_ratio_pct": s_success_ratio,
                    "realized_pnl": round(s_pnl, 2),
                },
            }
