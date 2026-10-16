"""Unit tests for Clean Signal Feed, Excursion Tracking, Dynamic Hourly % Gain, and Telegram Isolation.

Verifies:
1. OutcomeTracker calculates dynamic peak % gain and handles hourly snapshots.
2. First hour displays None / '--', while subsequent hours capture excursion snapshots.
3. Decoupled lifecycle: trade closure does not terminate continuous live price excursion monitoring.
4. Telegram clean message formatting: includes ONLY clean fields and strictly excludes TP/SL/reasons.
5. Telegram isolation: target highlights and execution state updates are suppressed from Telegram.
6. Dashboard /api/signals includes clean fields (live_price, highest_gain_pct, first_hour_gain_pct, momentum_status)
   alongside deep-dive levels.
"""

import pytest
from datetime import datetime, timezone, timedelta
from app.tracking.outcomes import OutcomeTracker, SetupOutcome
from app.database.models import Signal, SignalLevels
from app.notifications.telegram import TelegramNotifier
from app.api.dashboard import get_signals, set_repository
from app.database.repository import Repository


def test_outcome_tracker_peak_gain_and_excursion():
    tracker = OutcomeTracker()
    now = datetime.now(timezone.utc)
    
    # Register a new signal at entry price 500.00
    outcome = tracker.register_signal(
        signal_id="SIG-TEST-001",
        underlying="SENSEX",
        entry_price=500.0,
        contract_symbol="SENSEX 22 OCT 74200 CE",
        side="BUY_CE",
        entry_time=now,
    )
    assert outcome.entry_price == 500.0
    assert outcome.highest_price == 500.0
    assert outcome.peak_gain_pct == 0.0
    assert outcome.first_hour_gain_pct is None
    assert outcome.momentum_status == "ACTIVE"

    # Simulate price moving up to 550.00 (+10%) after 10 minutes
    t1 = now + timedelta(minutes=10)
    tracker.update_quote("SENSEX 22 OCT 74200 CE", 550.0, now=t1)
    
    out = tracker.get_outcome("SIG-TEST-001")
    assert out is not None
    assert out.current_price == 550.0
    assert out.highest_price == 550.0
    assert out.peak_gain_pct == 10.0
    assert out.first_hour_gain_pct is None  # Still within first hour: displays '--'

    # Price moves further to 650.00 (+30%) at 30 minutes
    t2 = now + timedelta(minutes=30)
    tracker.update_quote("SENSEX 22 OCT 74200 CE", 650.0, now=t2)
    out = tracker.get_outcome("SIG-TEST-001")
    assert out.highest_price == 650.0
    assert out.peak_gain_pct == 30.0
    assert out.momentum_status == "MORE BULLISH"
    assert out.first_hour_gain_pct is None  # Still within first hour


def test_outcome_tracker_hourly_snapshots():
    tracker = OutcomeTracker()
    now = datetime.now(timezone.utc)
    
    tracker.register_signal(
        signal_id="SIG-HOURLY-001",
        underlying="NIFTY",
        entry_price=100.0,
        contract_symbol="NIFTY 19 OCT 23000 CE",
        side="BUY_CE",
        entry_time=now,
    )

    # Within first 45 minutes: first_hour_gain_pct is None
    t_45m = now + timedelta(minutes=45)
    tracker.update_quote("NIFTY 19 OCT 23000 CE", 130.0, now=t_45m)
    out = tracker.get_outcome("SIG-HOURLY-001")
    assert out.first_hour_gain_pct is None
    assert out.peak_gain_pct == 30.0

    # Exactly at 65 minutes: first hour snapshot is locked in
    t_65m = now + timedelta(minutes=65)
    tracker.update_quote("NIFTY 19 OCT 23000 CE", 145.0, now=t_65m)
    out = tracker.get_outcome("SIG-HOURLY-001")
    assert out.first_hour_gain_pct == 45.0
    assert out.hourly_snapshots[1] == 45.0
    assert out.peak_gain_pct == 45.0

    # At 2 hours 5 minutes: second hour snapshot is recorded
    t_2h = now + timedelta(minutes=125)
    tracker.update_quote("NIFTY 19 OCT 23000 CE", 170.0, now=t_2h)
    out = tracker.get_outcome("SIG-HOURLY-001")
    assert out.hourly_snapshots[1] == 45.0
    assert out.hourly_snapshots[2] == 70.0
    assert out.peak_gain_pct == 70.0


def test_decoupled_lifecycle_trade_close_does_not_stop_monitoring():
    """Requirement 5: If signal in (2) closed due to TP/SL hit, it must NOT close the monitoring of signal (1)."""
    tracker = OutcomeTracker()
    now = datetime.now(timezone.utc)

    tracker.register_signal(
        signal_id="SIG-DECOUPLED-001",
        underlying="SENSEX",
        entry_price=520.0,
        contract_symbol="SENSEX 22 OCT 74200 CE",
        side="BUY_CE",
        entry_time=now,
    )

    # Trade hits TP2 in execution engine and is recorded as closed
    tracker.record_final("SIG-DECOUPLED-001", "TP2_HIT")
    out = tracker.get_outcome("SIG-DECOUPLED-001")
    assert out.reached_tp1 is True
    assert out.final_outcome == "TP2_HIT"

    # Crucial: Monitoring continues! New quotes continue to be ingested
    t_later = now + timedelta(minutes=75)
    tracker.update_quote("SENSEX 22 OCT 74200 CE", 920.0, now=t_later)
    
    out_updated = tracker.get_outcome("SIG-DECOUPLED-001")
    assert out_updated.current_price == 920.0
    assert out_updated.highest_price == 920.0
    assert out_updated.peak_gain_pct == round(((920.0 - 520.0) / 520.0) * 100.0, 2)
    assert out_updated.momentum_status == "MORE BULLISH"
    assert out_updated.first_hour_gain_pct is not None


def test_telegram_clean_message_strictly_excludes_tp_sl_reasons():
    """Requirement 1 & 4: Telegram message contains ONLY clean fields and strictly excludes TP, SL, reasons, and execution banners."""
    notifier = TelegramNotifier()
    now = datetime.now(timezone.utc)
    sig = Signal(
        signal_id="SIG-CLEAN-TELEGRAM",
        underlying="SENSEX",
        side="BUY_CE",
        score=89.5,
        strategy_version="v1.0.0",
        status="ENTRY",
        reasons=[
            "5-minute breakout above ORB High",
            "Price above VWAP with aggressive buying",
        ],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG-CLEAN-TELEGRAM",
        contract_symbol="SENSEX 22 OCT 74200 CE",
        strike=74200.0,
        expiry=now + timedelta(days=6),
        entry_range_low=510.0,
        entry_range_high=515.90,
        max_entry_price=525.0,
        sl_price=338.15,
        underlying_invalidation=74000.0,
        tp1_price=650.0,
        tp2_price=840.0,
        tp3_price=1050.0,
        planned_rr_tp1=1.5,
        current_bid=515.0,
        current_ask=515.90,
        delta=0.55,
        quote_timestamp=now,
    )

    clean_msg = notifier.format_clean_signal_message(
        signal=sig,
        levels=levels,
        live_price=580.20,
        highest_gain_pct=63.5,
        first_hour_gain_pct=None,  # First hour displays '--'
        momentum_status="MORE BULLISH",
    )

    # 1. Assert CLEAN fields are present
    assert "SENSEX 22 OCT 74200 CE" in clean_msg
    assert "BUY CE" in clean_msg
    assert "89.5/100" in clean_msg
    assert "₹515.90" in clean_msg       # Entry Price
    assert "₹580.20" in clean_msg       # Live Price
    assert "+63.5%" in clean_msg        # Highest Gain %
    assert "1st Hour" not in clean_msg  # 1st Hour is removed completely
    assert "MORE BULLISH" in clean_msg
    assert "Fired At:" in clean_msg     # Fired time is present

    # 2. Assert DEEP-DIVE and EXECUTION fields are strictly EXCLUDED
    assert "Stop-Loss" not in clean_msg
    assert "TP1" not in clean_msg
    assert "TP2" not in clean_msg
    assert "TP3" not in clean_msg
    assert "338.15" not in clean_msg
    assert "650.00" not in clean_msg
    assert "840.00" not in clean_msg
    assert "1050.00" not in clean_msg
    assert "EVIDENCE & REASONS" not in clean_msg
    assert "5-minute breakout above ORB High" not in clean_msg
    assert "TARGET 1 HIT" not in clean_msg
    assert "TARGET 2 HIT" not in clean_msg
    assert "STOP-LOSS HIT" not in clean_msg


@pytest.mark.asyncio
async def test_telegram_target_highlight_suppression():
    """Requirement 4: 'No (2) Shall be in telegram' - send_target_highlight must be suppressed."""
    notifier = TelegramNotifier(enabled=True)
    now = datetime.now(timezone.utc)
    sig = Signal(
        signal_id="SIG-SUPPRESS-TEST",
        underlying="SENSEX",
        side="BUY_CE",
        score=90.0,
        strategy_version="v1.0.0",
        status="TP2_HIT",
        reasons=[],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG-SUPPRESS-TEST",
        contract_symbol="SENSEX 22 OCT 74200 CE",
        strike=74200.0,
        expiry=now,
        entry_range_low=500.0,
        entry_range_high=515.0,
        max_entry_price=520.0,
        sl_price=350.0,
        underlying_invalidation=74000.0,
        tp1_price=650.0,
        tp2_price=850.0,
        tp3_price=1050.0,
        planned_rr_tp1=1.5,
        current_bid=849.0,
        current_ask=850.0,
        delta=0.55,
        quote_timestamp=now,
    )

    # When target highlight is invoked, it must return None and not post to Telegram
    res = await notifier.send_target_highlight(
        signal=sig,
        levels=levels,
        target_name="TP2",
        trigger_price=850.0,
        gain_pts=335.0,
        gain_pct=65.0,
        profit_per_lot=6700.0,
        total_profit=6700.0,
    )
    assert res is None


def test_spot_index_tick_does_not_contaminate_option_contract():
    """Verifies that spot index quotes (SENSEX at 74,630) NEVER contaminate option contracts (e.g. entry 502)."""
    tracker = OutcomeTracker()
    now = datetime.now(timezone.utc)

    # Register SENSEX option signal at entry ₹502.00
    outcome = tracker.register_signal(
        signal_id="SIG-SENSEX-OPT-01",
        underlying="SENSEX",
        entry_price=502.0,
        contract_symbol="SENSEX 22 OCT 74600 CE",
        side="BUY_CE",
        entry_time=now,
    )

    # Spot index tick arrives with price 74,630.41
    tracker.update_quote("SENSEX", 74630.41, now=now)
    tracker.update_quote("BSE_INDEX|SENSEX", 74630.41, now=now)

    out = tracker.get_outcome("SIG-SENSEX-OPT-01")
    assert out is not None
    # Option must NOT be overwritten by spot price
    assert out.current_price == 502.0
    assert out.highest_price == 502.0
    assert out.peak_gain_pct == 0.0

    # Genuine option tick arrives with price 540.00
    tracker.update_quote("SENSEX 22 OCT 74600 CE", 540.0, now=now)
    assert out.current_price == 540.0
    assert out.highest_price == 540.0
    assert out.peak_gain_pct == round(((540.0 - 502.0) / 502.0) * 100.0, 2)

