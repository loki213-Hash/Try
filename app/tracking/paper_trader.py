"""Paper Trading Engine with Realistic Fills, Fees, and Slippage.

Simulates execution strictly within paper-trading mode:
- Enforces max acceptable entry limits (no chasing)
- Dynamically calculates lot sizes from risk budget
- Deducts Indian regulatory taxes and broker fees
- Updates daily portfolio risk state and locks
"""

from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pydantic import BaseModel

from app.database.models import PaperTrade, Signal, SignalLevels, RiskState, utc_now
from app.signals.risk_manager import risk_manager
from app.monitoring.logging import logger
from config import settings


class TradeMilestoneEvent(BaseModel):
    trade_id: str
    signal_id: str
    contract_symbol: str
    underlying: str
    direction: str
    event_type: str  # TP1_HIT, TP2_HIT, TP3_HIT, SL_HIT
    target_name: str  # TP1, TP2, TP3, SL
    trigger_price: float
    entry_price: float
    gain_pts: float
    gain_pct: float
    profit_per_lot: float
    total_profit: float
    lots: int
    quantity: int
    is_closed: bool


class PaperTradingEngine:
    """Manages active paper positions and execution lifecycle."""

    def __init__(self):
        self._active_trades: Dict[str, PaperTrade] = {}
        self._reached_tp: Dict[str, set[str]] = {}
        self._executed_signals: set[str] = set()
        self._closed_trades: List[PaperTrade] = []
        # ExitGuard: require persistent tick confirmation and minimum hold period
        self._sl_breach_count: Dict[str, int] = {}
        self._tp1_breach_count: Dict[str, int] = {}
        self._tp2_breach_count: Dict[str, int] = {}
        self.min_hold_seconds: float = 30.0
        self.exit_confirm_ticks: int = 3

    def clear_trades(self) -> None:
        """Clears all in-memory active trades and milestone states."""
        self._active_trades.clear()
        self._reached_tp.clear()
        self._executed_signals.clear()
        self._closed_trades.clear()
        self._sl_breach_count.clear()
        self._tp1_breach_count.clear()
        self._tp2_breach_count.clear()

    def remove_trade_by_signal_id(self, signal_id: str) -> None:
        """Removes active trade associated with a deleted signal."""
        tids = [tid for tid, t in self._active_trades.items() if t.signal_id == signal_id]
        for tid in tids:
            self._active_trades.pop(tid, None)
            self._reached_tp.pop(tid, None)
            self._sl_breach_count.pop(tid, None)
            self._tp1_breach_count.pop(tid, None)
            self._tp2_breach_count.pop(tid, None)
        self._executed_signals.discard(signal_id)

    def get_open_trades(self) -> List[PaperTrade]:
        return list(self._active_trades.values())

    def get_closed_trades(self) -> List[PaperTrade]:
        return list(self._closed_trades)

    def restore_trade(self, trade: PaperTrade, signal_status: Optional[str] = None) -> None:
        """Restores an open paper trade from persistent storage on startup, preserving trailed SL and booked milestones."""
        self._active_trades[trade.trade_id] = trade
        self._executed_signals.add(trade.signal_id)
        reached = self._reached_tp.setdefault(trade.trade_id, set())

        # If signal or trade indicates TP1 was booked:
        if (signal_status and "TP1" in signal_status) or (trade.net_pnl and trade.net_pnl > 0):
            reached.add("TP1")
            # Enforce breakeven stop loss
            if trade.sl_price < trade.entry_price:
                trade.sl_price = trade.entry_price

    def load_executed_signals(self, signal_ids: set[str]) -> None:
        """Loads historical executed signal IDs from database history to ensure duplicate prevention persists across restarts."""
        self._executed_signals.update(signal_ids)

    def is_signal_executed(self, signal_id: str) -> bool:
        """Returns True if a paper trade has already been executed for this signal."""
        return signal_id in self._executed_signals

    def rebind_signal_id(self, old_signal_id: str, new_signal_id: str) -> None:
        """Keep in-memory trade references aligned when persistence reuses a signal."""
        if old_signal_id == new_signal_id:
            return
        self._executed_signals.discard(old_signal_id)
        self._executed_signals.add(new_signal_id)
        for trade in self._active_trades.values():
            if trade.signal_id == old_signal_id:
                trade.signal_id = new_signal_id

    def open_trade(
        self,
        signal: Signal,
        levels: SignalLevels,
        lot_size: int,
        risk_state: RiskState,
    ) -> Optional[PaperTrade]:
        """Attempts to open a paper trade within risk rules and strict duplicate prevention."""
        # 1. Duplicate Execution Safety Gate:
        # A single setup/signal_id cannot produce multiple paper entries
        if signal.signal_id in self._executed_signals:
            logger.warning(f"Duplicate paper entry blocked: {signal.signal_id} has already been executed.")
            return None

        # 2. Configurable Re-Entry Rule:
        if not settings.ALLOW_SIGNAL_REENTRY:
            for active in self._active_trades.values():
                if active.underlying == signal.underlying:
                    logger.warning(
                        f"Re-entry blocked for {signal.underlying}: active position already open and ALLOW_SIGNAL_REENTRY is False."
                    )
                    return None

        # Risk sizing approval
        sizing = risk_manager.calculate_position_size(
            premium_risk_per_unit=levels.entry_range_high - levels.sl_price,
            entry_price=levels.entry_range_high,
            lot_size=lot_size,
            risk_state=risk_state,
        )

        if not sizing.is_approved or sizing.lots <= 0:
            logger.warning(f"Paper trade rejected for {signal.signal_id}: {sizing.rejection_reason}")
            return None

        # Executable entry price
        entry_price = round(levels.entry_range_high, 2)
        if entry_price > levels.max_entry_price:
            logger.warning(
                f"Entry price (Rs {entry_price}) exceeds max limit (Rs {levels.max_entry_price})"
            )
            return None

        trade = PaperTrade(
            trade_id=f"PTRD-{signal.underlying}-{str(uuid.uuid4())[:8]}",
            signal_id=signal.signal_id,
            contract_symbol=levels.contract_symbol,
            underlying=signal.underlying,
            direction=signal.side,
            entry_time=utc_now(),
            entry_price=entry_price,
            quantity=sizing.quantity,
            lots=sizing.lots,
            current_price=entry_price,
            sl_price=levels.sl_price,
            tp1_price=levels.tp1_price,
            tp2_price=levels.tp2_price,
            tp3_price=levels.tp3_price,
            status="OPEN",
        )

        self._active_trades[trade.trade_id] = trade
        self._reached_tp[trade.trade_id] = set()
        self._executed_signals.add(signal.signal_id)
        # Update risk state
        risk_state.open_risk += sizing.risk_budget_allocated
        logger.info(
            f"Paper trade opened: {trade.trade_id} ({trade.contract_symbol}) Lots: {trade.lots} Qty: {trade.quantity} @ Rs {trade.entry_price}"
        )
        return trade

    def update_quote_with_events(
        self,
        contract_symbol: str,
        current_price: float,
        risk_state: RiskState,
        bid_price: Optional[float] = None,
        force_immediate: bool = False,
    ) -> tuple[List[PaperTrade], List[TradeMilestoneEvent]]:
        """Updates active positions with live quote, returns closed trades and milestone events."""
        closed_trades: List[PaperTrade] = []
        events: List[TradeMilestoneEvent] = []

        for trade_id, trade in list(self._active_trades.items()):
            # Match directly on contract symbol OR derive option price from underlying index tick
            opt_price: Optional[float] = None
            if trade.contract_symbol == contract_symbol:
                opt_price = current_price
                trade._last_real_ltp = current_price
                trade._last_spot_at_sync = getattr(trade, "_last_spot", None)
            elif (trade.underlying == contract_symbol or
                  f"IDX_{trade.underlying}" == contract_symbol or
                  trade.underlying in contract_symbol):
                trade._last_spot = current_price
                # If we have a verified real LTP from exchange, only apply delta to micro-movements since last sync
                if getattr(trade, "_last_real_ltp", None) is not None and getattr(trade, "_last_spot_at_sync", None) is not None:
                    micro_drift = current_price - trade._last_spot_at_sync
                    delta = 0.52 if "CE" in trade.direction else -0.52
                    opt_price = max(1.0, round(trade._last_real_ltp + (micro_drift * delta), 2))
                else:
                    # Initial fallback before first exchange sync
                    if not hasattr(trade, "_entry_spot") or trade._entry_spot is None:
                        trade._entry_spot = current_price
                    drift = current_price - trade._entry_spot
                    delta = 0.52 if "CE" in trade.direction else -0.52
                    delta_prem = drift * delta
                    opt_price = max(1.0, round(trade.entry_price + delta_prem, 2))

            if opt_price is None:
                continue

            trade.current_price = opt_price
            reached = self._reached_tp.setdefault(trade_id, set())

            # Exit pricing: judge long option exits on Bid if available
            exit_eval_price = bid_price if (bid_price is not None and bid_price > 0) else opt_price

            # ExitGuard parameters: min hold time and tick confirmation
            now = utc_now()
            entry_t = trade.entry_time or now
            if entry_t.tzinfo is None:
                entry_t = entry_t.replace(tzinfo=timezone.utc)
            hold_seconds = (now - entry_t).total_seconds()

            min_hold_threshold = self.min_hold_seconds if getattr(settings, "ENFORCE_MARKET_HOURS", False) else 0.0
            confirm_ticks_threshold = self.exit_confirm_ticks if getattr(settings, "ENFORCE_MARKET_HOURS", False) else 1
            if force_immediate:
                min_hold_threshold = 0.0
                confirm_ticks_threshold = 1

            min_hold_passed = hold_seconds >= min_hold_threshold
            # Catastrophic stop: catastrophic loss (<=40% of entry) overrides min-hold
            is_catastrophic = exit_eval_price <= (trade.entry_price * 0.40)

            # 1. Stop-Loss Hit (If TP1 was hit previously, sl_price is entry_price = breakeven exit)
            if exit_eval_price <= trade.sl_price:
                self._sl_breach_count[trade_id] = self._sl_breach_count.get(trade_id, 0) + 1
                if (min_hold_passed and self._sl_breach_count[trade_id] >= confirm_ticks_threshold) or is_catastrophic:
                    self._sl_breach_count.pop(trade_id, None)
                    exit_reason = "BREAKEVEN_SL_HIT" if "TP1" in reached else "SL_HIT"
                    exit_slip = 1.0 - (settings.SLIPPAGE_ESTIMATE_PCT / 100.0)
                    self.close_trade(
                        trade=trade,
                        exit_price=trade.sl_price * exit_slip,
                        exit_reason=exit_reason,
                        risk_state=risk_state,
                    )
                    closed_trades.append(trade)
                    events.append(
                        TradeMilestoneEvent(
                            trade_id=trade.trade_id,
                            signal_id=trade.signal_id,
                            contract_symbol=trade.contract_symbol,
                            underlying=trade.underlying,
                            direction=trade.direction,
                            event_type=exit_reason,
                            target_name="Breakeven SL" if "TP1" in reached else "SL",
                            trigger_price=trade.sl_price,
                            entry_price=trade.entry_price,
                            gain_pts=round(trade.sl_price - trade.entry_price, 2),
                            gain_pct=round(((trade.sl_price - trade.entry_price) / trade.entry_price) * 100.0, 1),
                            profit_per_lot=round((trade.sl_price - trade.entry_price) * (trade.quantity / max(trade.lots, 1)), 2),
                            total_profit=trade.net_pnl,
                            lots=trade.lots,
                            quantity=trade.quantity,
                            is_closed=True,
                        )
                    )
                    continue
            else:
                self._sl_breach_count[trade_id] = 0

            # 2. Target 1 Hit -> Book partial quantity and trail remaining runner to cost (entry price)
            if exit_eval_price >= trade.tp1_price and "TP1" not in reached:
                self._tp1_breach_count[trade_id] = self._tp1_breach_count.get(trade_id, 0) + 1
                if (min_hold_passed and self._tp1_breach_count[trade_id] >= confirm_ticks_threshold) or force_immediate:
                    self._tp1_breach_count.pop(trade_id, None)
                    reached.add("TP1")
                    orig_qty = trade.quantity
                    qty_to_close = max(1, int(orig_qty * settings.TP1_BOOKING_PCT))
                    rem_qty = orig_qty - qty_to_close

                    gain_pts = round(trade.tp1_price - trade.entry_price, 2)
                    gain_pct = round((gain_pts / trade.entry_price) * 100.0, 1)

                    costs = risk_manager.calculate_estimated_costs(
                        entry_price=trade.entry_price,
                        exit_price=trade.tp1_price,
                        quantity=qty_to_close,
                    )
                    partial_gross = round(gain_pts * qty_to_close, 2)
                    partial_net = round(partial_gross - costs.total_costs, 2)

                    # Accumulate partial realized PnL
                    trade.gross_pnl = round((trade.gross_pnl or 0.0) + partial_gross, 2)
                    trade.net_pnl = round((trade.net_pnl or 0.0) + partial_net, 2)
                    trade.fees = round((trade.fees or 0.0) + (costs.total_costs - costs.slippage), 2)
                    trade.slippage = round((trade.slippage or 0.0) + costs.slippage, 2)
                    risk_state.realized_pnl += partial_net

                    # Update remaining position & trail stop loss to entry price
                    trade._tp1_exit_price = trade.tp1_price
                    trade._tp1_qty = qty_to_close
                    trade.quantity = rem_qty if rem_qty > 0 else orig_qty
                    trade.lots = max(1, int(trade.lots * settings.TP1_RUNNER_PCT))
                    trade.sl_price = trade.entry_price

                    per_lot = round(gain_pts * (qty_to_close / max(trade.lots, 1)), 2)
                    events.append(
                        TradeMilestoneEvent(
                            trade_id=trade.trade_id,
                            signal_id=trade.signal_id,
                            contract_symbol=trade.contract_symbol,
                            underlying=trade.underlying,
                            direction=trade.direction,
                            event_type="TP1_HIT",
                            target_name=f"TP1 ({int(settings.TP1_BOOKING_PCT * 100)}% Booked)",
                            trigger_price=trade.tp1_price,
                            entry_price=trade.entry_price,
                            gain_pts=gain_pts,
                            gain_pct=gain_pct,
                            profit_per_lot=per_lot,
                            total_profit=trade.net_pnl,
                            lots=trade.lots,
                            quantity=qty_to_close,
                            is_closed=False,
                        )
                    )
            else:
                self._tp1_breach_count[trade_id] = 0

            # 3. Target 2 Hit -> Full exit of remaining position!
            if exit_eval_price >= trade.tp2_price and "TP2" not in reached:
                self._tp2_breach_count[trade_id] = self._tp2_breach_count.get(trade_id, 0) + 1
                if (min_hold_passed and self._tp2_breach_count[trade_id] >= confirm_ticks_threshold) or force_immediate:
                    self._tp2_breach_count.pop(trade_id, None)
                    reached.add("TP2")
                    exit_slip = 1.0 - (settings.SLIPPAGE_ESTIMATE_PCT / 100.0)
                    self.close_trade(
                        trade=trade,
                        exit_price=trade.tp2_price * exit_slip,
                        exit_reason="TP2_HIT",
                        risk_state=risk_state,
                    )
                    closed_trades.append(trade)
                    gain_pts = round(trade.tp2_price - trade.entry_price, 2)
                    gain_pct = round((gain_pts / trade.entry_price) * 100.0, 1)
                    per_lot = round(gain_pts * (trade.quantity / max(trade.lots, 1)), 2)
                    events.append(
                        TradeMilestoneEvent(
                            trade_id=trade.trade_id,
                            signal_id=trade.signal_id,
                            contract_symbol=trade.contract_symbol,
                            underlying=trade.underlying,
                            direction=trade.direction,
                            event_type="TP2_HIT",
                            target_name="TP2 (Full Exit)",
                            trigger_price=trade.tp2_price,
                            entry_price=trade.entry_price,
                            gain_pts=gain_pts,
                            gain_pct=gain_pct,
                            profit_per_lot=per_lot,
                            total_profit=trade.net_pnl,
                            lots=trade.lots,
                            quantity=trade.quantity,
                            is_closed=True,
                        )
                    )
                    continue
            else:
                self._tp2_breach_count[trade_id] = 0

            # 4. Time Stop: If trade held > 15 mins without reaching TP1, close to protect capital
            if hold_seconds >= 900.0 and "TP1" not in reached and getattr(settings, "ENFORCE_MARKET_HOURS", False):
                exit_slip = 1.0 - (settings.SLIPPAGE_ESTIMATE_PCT / 100.0)
                self.close_trade(
                    trade=trade,
                    exit_price=exit_eval_price * exit_slip,
                    exit_reason="TIME_STOP",
                    risk_state=risk_state,
                )
                closed_trades.append(trade)
                events.append(
                    TradeMilestoneEvent(
                        trade_id=trade.trade_id,
                        signal_id=trade.signal_id,
                        contract_symbol=trade.contract_symbol,
                        underlying=trade.underlying,
                        direction=trade.direction,
                        event_type="TIME_STOP",
                        target_name="Time Stop (15m)",
                        trigger_price=exit_eval_price,
                        entry_price=trade.entry_price,
                        gain_pts=round(exit_eval_price - trade.entry_price, 2),
                        gain_pct=round(((exit_eval_price - trade.entry_price) / trade.entry_price) * 100.0, 1),
                        profit_per_lot=round((exit_eval_price - trade.entry_price) * (trade.quantity / max(trade.lots, 1)), 2),
                        total_profit=trade.net_pnl,
                        lots=trade.lots,
                        quantity=trade.quantity,
                        is_closed=True,
                    )
                )
                continue

        return closed_trades, events

    def update_quote(
        self,
        contract_symbol: str,
        current_price: float,
        risk_state: RiskState,
    ) -> List[PaperTrade]:
        """Backward-compatible quote update returning closed trades."""
        closed, _ = self.update_quote_with_events(contract_symbol, current_price, risk_state)
        return closed

    def close_trade(
        self,
        trade: PaperTrade,
        exit_price: float,
        exit_reason: str,
        risk_state: RiskState,
    ) -> None:
        """Closes a paper trade, computes fees and net PnL, updates risk state."""
        exit_p = round(exit_price, 2)
        costs = risk_manager.calculate_estimated_costs(
            entry_price=trade.entry_price,
            exit_price=exit_p,
            quantity=trade.quantity,
        )

        gross_pnl = round((exit_p - trade.entry_price) * trade.quantity, 2)
        net_pnl = round(gross_pnl - costs.total_costs, 2)

        trade.exit_time = utc_now()
        trade.exit_price = exit_p
        trade.exit_reason = exit_reason
        trade.gross_pnl = round((trade.gross_pnl or 0.0) + gross_pnl, 2)
        trade.fees = round((trade.fees or 0.0) + (costs.total_costs - costs.slippage), 2)
        trade.slippage = round((trade.slippage or 0.0) + costs.slippage, 2)
        trade.net_pnl = round((trade.net_pnl or 0.0) + net_pnl, 2)
        trade.status = "CLOSED"

        # If trade had partial booking (e.g. TP1), compute weighted average exit price across all units
        if hasattr(trade, "_tp1_exit_price") and trade._tp1_exit_price is not None and getattr(trade, "_tp1_qty", 0) > 0:
            tp1_qty = getattr(trade, "_tp1_qty", 0)
            total_qty = tp1_qty + trade.quantity
            weighted_exit = round(((trade._tp1_exit_price * tp1_qty) + (exit_p * trade.quantity)) / max(total_qty, 1), 2)
            trade.exit_price = weighted_exit
            trade.quantity = total_qty
            if exit_reason in ("BREAKEVEN_SL_HIT", "SL_HIT"):
                exit_reason = "TP1_BOOKED_BREAKEVEN_EXIT"
            trade.exit_reason = exit_reason

        # Update Risk State
        risk_state.realized_pnl = (risk_state.realized_pnl or 0.0) + net_pnl
        trade_risk = (trade.entry_price - trade.sl_price) * trade.quantity
        risk_state.open_risk = max(0.0, (risk_state.open_risk or 0.0) - trade_risk)

        if net_pnl < 0:
            risk_state.consecutive_losses = (risk_state.consecutive_losses or 0) + 1
            if exit_reason == "SL_HIT":
                risk_state.stop_outs_count = (risk_state.stop_outs_count or 0) + 1
        else:
            risk_state.consecutive_losses = 0

        # Check daily loss limit lock (only if daily stop protection is enabled)
        if settings.ENABLE_DAILY_STOP_LOSS:
            is_loss_breached = (
                settings.MAX_DAILY_LOSS_PCT is not None
                and risk_state.realized_pnl <= -(risk_state.starting_capital * settings.MAX_DAILY_LOSS_PCT)
            )
            is_stops_breached = risk_state.stop_outs_count >= settings.MAX_DAILY_STOP_LOSSES
            if is_loss_breached or is_stops_breached:
                risk_state.trading_locked = True
                risk_state.lock_reason = "Daily loss limit reached"

        if trade.trade_id in self._active_trades:
            del self._active_trades[trade.trade_id]

        if not any(ct.trade_id == trade.trade_id for ct in self._closed_trades):
            self._closed_trades.insert(0, trade)
            if len(self._closed_trades) > 100:
                self._closed_trades.pop()

        logger.info(
            f"Paper trade closed: {trade.trade_id} Reason: {exit_reason} Net PnL: ₹{net_pnl:.2f} (Fees+Slip: ₹{costs.total_costs:.2f})"
        )


paper_trading_engine = PaperTradingEngine()
