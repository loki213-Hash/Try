"""Unit tests for position sizing, daily loss locks, correlation checks, and statutory fees."""

import pytest
from app.signals.risk_manager import risk_manager, TradeCostEstimate
from app.database.models import RiskState
from config import settings


def test_position_sizing_adheres_to_budget():
    state = RiskState(
        session_date="2026-10-12",
        starting_capital=500000.0,
        realized_pnl=0.0,
        open_risk=0.0,
        stop_outs_count=0,
        trading_locked=False,
    )
    # Trade with ₹20 premium risk per unit, lot size 75
    # Risk budget = 0.5% of 500,000 = ₹2,500
    # Risk per lot = 20 * 75 = ₹1,500
    # Expected lots = floor(2500 / 1500) = 1 lot
    sizing = risk_manager.calculate_position_size(
        premium_risk_per_unit=20.0,
        entry_price=150.0,
        lot_size=75,
        risk_state=state,
    )
    assert sizing.is_approved
    assert sizing.lots == 1
    assert sizing.quantity == 75
    assert sizing.risk_budget_allocated == 1500.0


def test_position_sizing_locks_on_two_stop_losses_when_enabled(monkeypatch):
    monkeypatch.setattr(settings, "ENABLE_DAILY_STOP_LOSS", True)
    state = RiskState(
        session_date="2026-10-12",
        starting_capital=500000.0,
        realized_pnl=-3000.0,
        open_risk=0.0,
        stop_outs_count=2,  # Max daily stop-losses reached
        trading_locked=False,
    )
    sizing = risk_manager.calculate_position_size(
        premium_risk_per_unit=15.0,
        entry_price=120.0,
        lot_size=75,
        risk_state=state,
    )
    assert not sizing.is_approved
    assert state.trading_locked
    assert "stop-outs" in state.lock_reason.lower()


def test_position_sizing_not_locked_when_daily_stop_disabled():
    # By default, ENABLE_DAILY_STOP_LOSS is False (1% daily stop removed per instruction)
    state = RiskState(
        session_date="2026-10-12",
        starting_capital=500000.0,
        realized_pnl=-10000.0,
        open_risk=0.0,
        stop_outs_count=3,
        trading_locked=False,
    )
    sizing = risk_manager.calculate_position_size(
        premium_risk_per_unit=15.0,
        entry_price=120.0,
        lot_size=75,
        risk_state=state,
    )
    assert sizing.is_approved
    assert not state.trading_locked


def test_correlation_exposure_blocks_duplicate_benchmark():
    # NIFTY and SENSEX share EQUITY_BENCHMARK cluster
    is_ok, reason = risk_manager.check_correlation_exposure("SENSEX", ["NIFTY"])
    assert not is_ok
    assert "Correlated cluster limit" in reason

    # NIFTY and BANKNIFTY are different clusters
    is_ok_bank, _ = risk_manager.check_correlation_exposure("BANKNIFTY", ["NIFTY"])
    assert is_ok_bank


def test_statutory_costs_and_slippage():
    costs = risk_manager.calculate_estimated_costs(
        entry_price=100.0,
        exit_price=130.0,
        quantity=75,
    )
    assert costs.brokerage == 40.0
    assert costs.stt > 0
    assert costs.exchange_charges > 0
    assert costs.gst > 0
    assert costs.slippage > 0
    assert costs.total_costs > 50.0


def test_paper_trader_tp1_80_percent_partial_booking_and_tp2_exit():
    from app.tracking.paper_trader import PaperTradingEngine
    from app.database.models import PaperTrade, RiskState

    engine = PaperTradingEngine()
    state = RiskState(
        session_date="2026-10-12",
        starting_capital=500000.0,
        realized_pnl=0.0,
        open_risk=1500.0,
        stop_outs_count=0,
        trading_locked=False,
    )
    trade = PaperTrade(
        trade_id="T_TEST_1",
        signal_id="SIG_1",
        contract_symbol="NIFTY 13 OCT 22400 PUT",
        underlying="NIFTY",
        direction="BUY_PE",
        entry_price=100.0,
        current_price=100.0,
        quantity=100,
        lots=1,
        sl_price=80.0,
        tp1_price=130.0,
        tp2_price=150.0,
        tp3_price=180.0,
        status="OPEN",
    )
    engine._active_trades[trade.trade_id] = trade

    # 1. Price reaches TP1 (130.0) -> Books 80% (80 shares)
    closed, events = engine.update_quote_with_events("NIFTY 13 OCT 22400 PUT", 131.0, state)
    assert len(events) == 1
    assert events[0].event_type == "TP1_HIT"
    assert events[0].quantity == 80  # 80% closed
    assert trade.quantity == 20     # 20% remaining
    assert trade.sl_price == 100.0  # Trailed to entry price (cost)
    assert trade.status == "OPEN"
    assert trade.net_pnl > 0        # Partial profit locked!
    assert state.realized_pnl > 0

    # 2. Price reaches TP2 (150.0) -> Closes remaining 20% completely!
    closed_2, events_2 = engine.update_quote_with_events("NIFTY 13 OCT 22400 PUT", 151.0, state)
    assert len(closed_2) == 1
    assert len(events_2) == 1
    assert events_2[0].event_type == "TP2_HIT"
    assert events_2[0].is_closed is True
    assert trade.status == "CLOSED"
    assert trade.exit_reason == "TP2_HIT"


def test_paper_trader_tp1_partial_booking_then_breakeven_sl_hit_remains_profitable():
    """Verifies that when TP1 books 80% and the remaining 20% pulls back to breakeven SL,
    the overall trade exits in solid NET PROFIT, never a loss.
    """
    from app.tracking.paper_trader import PaperTradingEngine
    from app.database.models import PaperTrade, RiskState, utc_now

    engine = PaperTradingEngine()
    state = RiskState(session_date="2026-10-15", starting_capital=500000.0, realized_pnl=0.0, open_risk=0.0)

    trade = PaperTrade(
        trade_id="TR-TP1-BE-01",
        signal_id="SIG-TP1-BE-01",
        contract_symbol="NIFTY 19 OCT 22950 CE",
        underlying="NIFTY",
        direction="BUY_CE",
        entry_time=utc_now(),
        entry_price=136.34,
        current_price=136.34,
        quantity=150,
        lots=2,
        sl_price=120.75,
        tp1_price=163.65,
        tp2_price=180.80,
        tp3_price=200.00,
        status="OPEN",
    )
    engine._active_trades[trade.trade_id] = trade

    # 1. Price rallies to TP1 (163.65) -> 80% (120 units) booked
    closed, events = engine.update_quote_with_events("NIFTY 19 OCT 22950 CE", 164.0, state)
    assert len(closed) == 0
    assert len(events) == 1
    assert events[0].event_type == "TP1_HIT"
    assert trade.quantity == 30  # 20% runner remains
    assert trade.sl_price == 136.34  # SL strictly moved to breakeven (entry price)
    assert trade.net_pnl > 0  # Huge profit locked in
    locked_tp1_pnl = trade.net_pnl

    # 2. Market pulls back and drops through entry price to 125.25
    closed_be, events_be = engine.update_quote_with_events("NIFTY 19 OCT 22950 CE", 125.25, state)
    assert len(closed_be) == 1
    assert len(events_be) == 1
    closed_trade = closed_be[0]

    # Must be closed at Breakeven SL
    assert closed_trade.status == "CLOSED"
    assert "BREAKEVEN" in closed_trade.exit_reason
    # Total net PnL MUST be positive (retaining the locked TP1 profit)
    assert closed_trade.net_pnl > 0
    assert closed_trade.net_pnl >= locked_tp1_pnl * 0.95
    # Weighted exit price across 120 units @ 163.65 + 30 units @ breakeven > entry price
    assert closed_trade.exit_price > closed_trade.entry_price
    # % Profit is positive
    profit_pct = ((closed_trade.exit_price - closed_trade.entry_price) / closed_trade.entry_price) * 100.0
    assert profit_pct > 10.0


