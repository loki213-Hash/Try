"""Unit tests for Telegram message formatting, idempotency, and state transitions."""

import pytest
from datetime import datetime, timezone, timedelta

from app.database.models import Signal, SignalLevels
from app.notifications.telegram import TelegramNotifier
from app.signals.state_machine import SignalStateMachine, SignalStatus


def test_telegram_message_contains_all_required_fields():
    notifier = TelegramNotifier()
    now = datetime.now(timezone.utc)
    sig = Signal(
        signal_id="SIG-NIFTY-120000-TEST01",
        underlying="NIFTY",
        side="BUY_CE",
        score=84.5,
        strategy_version="v1.0.0",
        status="ENTRY",
        reasons=[
            "Confirmed 5-min candle closed above ORB High (25100.0)",
            "Price firmly above VWAP",
            "15m structure: BULLISH_STRUCTURE",
            "RVOL: 1.5x baseline",
        ],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG-NIFTY-120000-TEST01",
        contract_symbol="NIFTY24OCT25100CE",
        strike=25100.0,
        expiry=now + timedelta(days=3),
        entry_range_low=149.0,
        entry_range_high=150.0,
        max_entry_price=152.25,
        sl_price=130.0,
        underlying_invalidation=25080.0,
        tp1_price=180.0,
        tp2_price=200.0,
        tp3_price=230.0,
        planned_rr_tp1=1.5,
        current_bid=149.5,
        current_ask=150.0,
        delta=0.52,
        quote_timestamp=now,
    )

    msg = notifier.format_signal_message(sig, levels)
    # Validate required production fields in output text
    assert "MULTI-INDEX OPTIONS SIGNAL (PAPER)" in msg
    assert "NIFTY" in msg
    assert "BUY CE" in msg
    assert "NIFTY24OCT25100CE" in msg
    assert "₹149.50 / ₹150.00" in msg  # Bid / Ask
    assert "₹149.00 – ₹150.00" in msg  # Entry Range
    assert "₹130.00" in msg            # SL
    assert "₹180.00" in msg            # TP1
    assert "₹200.00" in msg            # TP2
    assert "₹230.00" in msg            # TP3
    assert "84.5/100" in msg           # Score out of 100
    assert "SIG-NIFTY-120000-TEST01" in msg


def test_state_machine_valid_transitions():
    assert SignalStateMachine.can_transition(SignalStatus.WATCH, SignalStatus.ENTRY)
    assert SignalStateMachine.can_transition(SignalStatus.ENTRY, SignalStatus.TP1_HIT)
    assert SignalStateMachine.can_transition(SignalStatus.TP1_HIT, SignalStatus.TP2_HIT)
    assert SignalStateMachine.can_transition(SignalStatus.TP2_HIT, SignalStatus.TP3_HIT)
    assert SignalStateMachine.can_transition(SignalStatus.ENTRY, SignalStatus.SL_HIT)
    assert not SignalStateMachine.can_transition(SignalStatus.CLOSED, SignalStatus.ENTRY)


def test_target_hit_banner_highlight():
    notifier = TelegramNotifier()
    now = datetime.now(timezone.utc)
    sig = Signal(
        signal_id="SIG-TEST-TP1",
        underlying="BANKNIFTY",
        side="BUY_CE",
        score=88.0,
        strategy_version="v1.0.0",
        status="TP1_HIT",
        reasons=["5m breakout confirmed"],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG-TEST-TP1",
        contract_symbol="BANKNIFTY51000CE",
        strike=51000.0,
        expiry=now + timedelta(days=2),
        entry_range_low=240.0,
        entry_range_high=250.0,
        max_entry_price=253.0,
        sl_price=200.0,
        underlying_invalidation=50900.0,
        tp1_price=325.0,
        tp2_price=375.0,
        tp3_price=450.0,
        planned_rr_tp1=1.5,
        current_bid=324.0,
        current_ask=325.0,
        delta=0.55,
        quote_timestamp=now,
    )

    msg = notifier.format_signal_message(sig, levels)
    assert "TARGET 1 HIT! PROFIT BOOKED / TRAILING SL ACTIVE" in msg
    assert "BANKNIFTY" in msg
    assert "TP1_HIT" in msg


@pytest.mark.asyncio
async def test_hourly_report_generation():
    notifier = TelegramNotifier(enabled=False)
    stats = {
        "hourly": {
            "signals_fired": 5,
            "signals_triggered": 4,
            "tp1_hits": 3,
            "tp2_hits": 2,
            "tp3_hits": 1,
            "sl_hits": 1,
            "success_ratio_pct": 75.0,
            "realized_pnl": 4500.0,
        },
        "session": {
            "signals_fired": 12,
            "signals_triggered": 10,
            "tp1_hits": 8,
            "tp2_hits": 5,
            "tp3_hits": 2,
            "sl_hits": 2,
            "success_ratio_pct": 80.0,
            "realized_pnl": 12300.0,
        },
    }
    # When enabled=False (default test mode), it formats and logs cleanly without throwing
    res = await notifier.send_hourly_report(stats)
    assert res is None  # Mock returns None when credentials not active
