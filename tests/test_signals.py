"""Unit and regression tests for signals, confidence scoring, ORB, and hard gates."""

import pytest
from datetime import datetime, timezone, timedelta

from app.signals.confidence import confidence_evaluator, ScoreBreakdown
from app.analysis.price_action import PriceActionAnalyzer, SetupType, OpeningRange, DailyContext
from app.data.base_provider import UnifiedCandle


def test_confidence_scoring_weights_max_100():
    # Test maximum possible score
    score = confidence_evaluator.calculate_score(
        structure_state="BULLISH_STRUCTURE",
        candle_body_strength=0.85,
        rvol=1.6,
        breadth_percentage=75.0,
        oi_regime_aligned=True,
        spread_pct=0.5,
        planned_rr_tp1=2.2,
        side="BUY_CE",
    )
    assert score.trend_structure == 25.0
    assert score.entry_confirmation == 20.0
    assert score.volume_participation == 15.0
    assert score.market_breadth == 10.0
    assert score.futures_options_positioning == 10.0
    assert score.contract_liquidity_spread == 10.0
    assert score.volatility_rr_suitability == 10.0
    assert score.total_score == 100.0


def test_confidence_scoring_rejection_low_score():
    score = confidence_evaluator.calculate_score(
        structure_state="BEARISH_STRUCTURE",  # Against CALL
        candle_body_strength=0.20,
        rvol=0.7,
        breadth_percentage=30.0,
        oi_regime_aligned=False,
        spread_pct=2.8,
        planned_rr_tp1=1.0,
        side="BUY_CE",
    )
    assert score.total_score < 40.0


def test_mandatory_gates_fail_on_event_veto_and_staleness():
    gate = confidence_evaluator.evaluate_mandatory_gates(
        is_quote_fresh=False,  # Fails
        is_spread_acceptable=True,
        has_event_veto=True,   # Fails
        is_risk_locked=False,
        has_three_targets=True,
    )
    assert not gate.all_passed
    assert len(gate.failed_gates) == 2
    assert any("Quote Freshness Gate" in f for f in gate.failed_gates)
    assert any("Event Risk Gate" in f for f in gate.failed_gates)


def test_orb_breakout_detection_and_wick_rejection():
    orb = OpeningRange(range_high=25100.0, range_low=25000.0, range_span=100.0, is_formed=True)
    daily = DailyContext(pdh=25080.0, pdl=24900.0, pdc=25020.0, daily_bias="BULLISH")

    now = datetime.now(timezone.utc)
    # Valid breakout candle
    valid_breakout_candles = [
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now - timedelta(minutes=15), open=25010, high=25050, low=25000, close=25040
        ),
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now - timedelta(minutes=10), open=25040, high=25080, low=25030, close=25075
        ),
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now - timedelta(minutes=5), open=25075, high=25120, low=25070, close=25115
        ),
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now, open=25080, high=25140, low=25075, close=25135  # Close > ORB High
        ),
    ]

    setup, reasons = PriceActionAnalyzer.detect_setup(
        candles_5m=valid_breakout_candles,
        orb=orb,
        daily=daily,
        vwap_value=25060.0,
    )
    assert setup == SetupType.BULLISH_BREAKOUT
    assert any("ORB High" in r for r in reasons)

    # 1. Temporary entry veto case (candle closes above ORB High with upper wick >= 40%)
    veto_candles = list(valid_breakout_candles[:-1]) + [
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now, open=25080, high=25160, low=25070, close=25105  # Upper wick = 55 / 90 > 60%, close > 25100
        )
    ]
    veto_setup, veto_reasons = PriceActionAnalyzer.detect_setup(
        candles_5m=veto_candles,
        orb=orb,
        daily=daily,
        vwap_value=25060.0,
    )
    # Candle closed above boundary, so it is a temporary entry veto (remains developing), not structural invalidation
    assert veto_setup == SetupType.EARLY_BULLISH_MOMENTUM
    assert any("temporary entry veto" in r for r in veto_reasons)

    # 2. Genuine structural breakout failure (high tested above ORB High, but closed back inside with upper wick >= 60%)
    struct_fail_candles = list(valid_breakout_candles[:-1]) + [
        UnifiedCandle(
            instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
            timestamp=now, open=25080, high=25160, low=25070, close=25095  # Spiked to 25160 but closed inside at 25095
        )
    ]
    fail_setup, fail_reasons = PriceActionAnalyzer.detect_setup(
        candles_5m=struct_fail_candles,
        orb=orb,
        daily=daily,
        vwap_value=25060.0,
    )
    assert fail_setup == SetupType.FAILED_BREAKOUT
    assert any("wick" in r for r in fail_reasons)


def test_targets_engine_confidence_scores():
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_1",
        symbol="NIFTY25100CE",
        strike=25100.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=120.0,
        max_acceptable_entry=122.0,
        delta=0.52,
        gamma=0.001,
        iv=13.5,
        current_bid=119.5,
        current_ask=120.0,
        volume=50000,
        oi=100000,
        spread_pct=0.42,
    )

    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=25110.0,
        breakout_candle_high=25120.0,
        breakout_candle_low=25080.0,
        orb_boundary=25070.0,
        direction="BUY_CE",
    )

    assert errs == []
    assert levels is not None
    assert levels.tp1_confidence >= 65.0
    assert levels.tp2_confidence >= 45.0
    assert levels.tp3_confidence >= 25.0
    assert levels.tp1_confidence > levels.tp2_confidence > levels.tp3_confidence


def test_option_activity_buying_vs_writing_distinction():
    from app.analysis.futures_oi import futures_oi_analyzer

    # Call Buying (Bullish demand)
    cb = futures_oi_analyzer.classify_option_activity("CE", premium_change_pct=25.0, oi_change_pct=10.0)
    assert cb.activity_type == "CALL_BUYING"
    assert cb.is_bullish_demand is True
    assert cb.is_resistance_buildup is False

    # Call Writing (Bearish resistance)
    cw = futures_oi_analyzer.classify_option_activity("CE", premium_change_pct=-18.0, oi_change_pct=14.0)
    assert cw.activity_type == "CALL_WRITING"
    assert cw.is_resistance_buildup is True
    assert cw.is_bullish_demand is False

    # Put Buying (Bearish downside demand)
    pb = futures_oi_analyzer.classify_option_activity("PE", premium_change_pct=30.0, oi_change_pct=8.0)
    assert pb.activity_type == "PUT_BUYING"
    assert pb.is_bearish_demand is True

    # Put Writing (Bullish support floor)
    pw = futures_oi_analyzer.classify_option_activity("PE", premium_change_pct=-20.0, oi_change_pct=12.0)
    assert pw.activity_type == "PUT_WRITING"
    assert pw.is_support_floor is True


def test_order_flow_delta_and_absorption_trap():
    from app.analysis.futures_oi import futures_oi_analyzer

    # Normal aggressive buying (bid support > ask offers, positive delta)
    flow = futures_oi_analyzer.evaluate_order_flow_delta(
        traded_volume=100000,
        bid_volume=70000,
        ask_volume=30000,
        price_change_pct=0.85,
        rvol=1.5,
    )
    assert flow.is_aggressive_buying is True
    assert flow.is_absorption_trap is False
    assert flow.volume_delta > 0

    # Absorption trap: huge volume (rvol 2.5x) but price stalled (< 0.08%)
    trap = futures_oi_analyzer.evaluate_order_flow_delta(
        traded_volume=200000,
        bid_volume=100000,
        ask_volume=100000,
        price_change_pct=0.02,
        rvol=2.5,
    )
    assert trap.is_absorption_trap is True
    assert "ABSORPTION" in trap.interpretation


@pytest.mark.asyncio
async def test_signal_repository_delete_and_clear_all(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from app.database.models import Base, Signal, SignalLevels
    from app.database.repository import Repository

    db_path = tmp_path / "test_repo.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repo = Repository(session_factory)

    now = datetime.now(timezone.utc)
    sig = Signal(
        signal_id="TEST-SIG-1",
        underlying="NIFTY",
        side="BUY_PE",
        score=88.0,
        status="ENTRY",
        reasons=["Test reason"],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="TEST-SIG-1",
        contract_symbol="NIFTY22400PE",
        strike=22400,
        expiry=now + timedelta(days=2),
        entry_range_low=110.0,
        entry_range_high=112.0,
        max_entry_price=114.0,
        sl_price=92.0,
        underlying_invalidation=22450.0,
        tp1_price=130.0,
        tp2_price=150.0,
        tp3_price=180.0,
        planned_rr_tp1=1.8,
        quote_timestamp=now,
    )

    await repo.create_signal(sig, levels)
    active = await repo.get_active_signals()
    assert len(active) == 1

    # Delete single signal
    deleted = await repo.delete_signal("TEST-SIG-1")
    assert deleted is True
    active_after = await repo.get_active_signals()
    assert len(active_after) == 0

    # Test clear_all_signals with fresh model instance
    sig2 = Signal(
        signal_id="TEST-SIG-2",
        underlying="NIFTY",
        side="BUY_CE",
        score=85.0,
        status="ENTRY",
        reasons=["Test 2"],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels2 = SignalLevels(
        signal_id="TEST-SIG-2",
        contract_symbol="NIFTY22400CE",
        strike=22400,
        expiry=now + timedelta(days=2),
        entry_range_low=100.0,
        entry_range_high=102.0,
        max_entry_price=104.0,
        sl_price=85.0,
        underlying_invalidation=22350.0,
        tp1_price=120.0,
        tp2_price=140.0,
        tp3_price=160.0,
        planned_rr_tp1=1.8,
        quote_timestamp=now,
    )
    await repo.create_signal(sig2, levels2)
    cleared_count = await repo.clear_all_signals()
    assert cleared_count == 1
    assert len(await repo.get_active_signals()) == 0


def test_state_machine_3_stage_transitions():
    from app.signals.state_machine import SignalStateMachine, SignalStatus

    # Stage 1 -> Stage 2
    assert SignalStateMachine.can_transition(SignalStatus.EARLY_WATCH, SignalStatus.SETUP_DEVELOPING) is True
    # Stage 2 -> Stage 3
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.ENTRY) is True
    # Direct Stage 1 -> Stage 3 (clean fast breakout)
    assert SignalStateMachine.can_transition(SignalStatus.EARLY_WATCH, SignalStatus.ENTRY) is True
    # Invalidation from radar stages
    assert SignalStateMachine.can_transition(SignalStatus.EARLY_WATCH, SignalStatus.INVALIDATED) is True
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.INVALIDATED) is True
    # Post-entry lifecycle
    assert SignalStateMachine.can_transition(SignalStatus.ENTRY, SignalStatus.TP1_HIT) is True
    assert SignalStateMachine.can_transition(SignalStatus.TP1_HIT, SignalStatus.TP2_HIT) is True
    assert SignalStateMachine.can_transition(SignalStatus.TP2_HIT, SignalStatus.CLOSED) is True


def test_false_move_risk_model_and_veto():
    from app.analysis.futures_oi import futures_oi_analyzer

    # 1. Clean low-risk move
    clean_risk = futures_oi_analyzer.evaluate_false_move_risk(
        price_change_pct=0.35,
        rvol=1.5,
        upper_wick_pct=0.10,
        lower_wick_pct=0.05,
        volume_delta=30.0,
        direction="BUY_CE",
    )
    assert clean_risk.risk_level == "LOW"
    assert clean_risk.is_veto_for_entry is False

    # 2. High-risk volume absorption trap + upper wick rejection
    trap_risk = futures_oi_analyzer.evaluate_false_move_risk(
        price_change_pct=0.02,  # negligible price move
        rvol=2.2,               # volume explosion
        upper_wick_pct=0.55,    # 55% upper wick
        volume_delta=-40.0,     # divergence against bullish move
        direction="BUY_CE",
    )
    assert trap_risk.risk_level == "HIGH"
    assert trap_risk.is_veto_for_entry is True
    assert len(trap_risk.factors) >= 2


def test_rolling_oi_velocity_and_graceful_fallback():
    from app.analysis.futures_oi import FuturesOIAnalyzer
    analyzer = FuturesOIAnalyzer()
    inst = "OPT_TEST_ROLLING_OI"
    now = datetime.now(timezone.utc)

    # 1. When no history exists: returns MISSING without error
    initial_vel = analyzer.calculate_rolling_oi_velocity(inst, 50000, timestamp=now)
    assert initial_vel["status"] in ("MISSING", "UNAVAILABLE")
    assert initial_vel["is_reliable"] is False

    # 2. Add history across 5 minutes
    analyzer.record_oi_snapshot(inst, 45000, timestamp=now - timedelta(minutes=5))
    analyzer.record_oi_snapshot(inst, 48000, timestamp=now - timedelta(minutes=2))
    analyzer.record_oi_snapshot(inst, 52000, timestamp=now)

    updated_vel = analyzer.calculate_rolling_oi_velocity(inst, 52000, timestamp=now)
    assert updated_vel["status"] == "AVAILABLE"
    assert updated_vel["is_reliable"] is True
    assert updated_vel["delta_3m_pct"] > 0.0


def test_candle_body_scoring_not_knife_edge():
    # Verify that a 0.40 body ratio is scored continuously rather than hard rejected
    score_40 = confidence_evaluator.calculate_score(
        structure_state="BULLISH_STRUCTURE",
        candle_body_strength=0.40,
        rvol=1.5,
        breadth_percentage=65.0,
        oi_regime_aligned=True,
        spread_pct=0.8,
        planned_rr_tp1=1.8,
        side="BUY_CE",
    )
    assert score_40.entry_confirmation == 8.0  # partial score
    assert score_40.total_score >= 80.0        # still achieves high score due to overall evidence!

    score_75 = confidence_evaluator.calculate_score(
        structure_state="BULLISH_STRUCTURE",
        candle_body_strength=0.75,
        rvol=1.5,
        breadth_percentage=65.0,
        oi_regime_aligned=True,
        spread_pct=0.8,
        planned_rr_tp1=1.8,
        side="BUY_CE",
    )
    assert score_75.entry_confirmation == 20.0


def test_net_reward_to_risk_with_slippage_and_costs():
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_RR",
        symbol="NIFTY22500CE",
        strike=22500.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=150.0,
        max_acceptable_entry=152.0,
        delta=0.55,
        gamma=0.001,
        iv=13.0,
        current_bid=149.5,
        current_ask=150.0,
        volume=50000,
        oi=100000,
        spread_pct=0.33,
    )

    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=22510.0,
        breakout_candle_high=22525.0,
        breakout_candle_low=22485.0,
        orb_boundary=22480.0,
        direction="BUY_CE",
    )
    assert errs == []
    assert levels is not None
    assert levels.net_rr_tp1 >= 1.5
    assert levels.estimated_slippage > 0.0
    assert levels.estimated_transaction_cost > 0.0

    # Validate that net reward is at least 1.5 times net loss
    slip_pct = 0.5 / 100.0
    entry_slip = contract.executable_ask * slip_pct
    sl_slip = levels.sl_price * slip_pct
    costs = levels.estimated_transaction_cost
    net_loss = levels.premium_risk_per_unit + entry_slip + sl_slip + costs

    tp1_slip = levels.tp1_price * slip_pct
    tp1_gain = levels.tp1_price - contract.executable_ask
    net_reward = tp1_gain - entry_slip - tp1_slip - costs

    assert net_reward >= 1.5 * net_loss - 0.05
    assert round(net_reward / net_loss, 2) >= 1.5


def test_signal_invalidation_and_expiration():
    from app.signals.state_machine import SignalStateMachine, SignalStatus

    now = datetime.now(timezone.utc)

    # 1. EARLY_WATCH expiration (10 min observation window)
    assert SignalStateMachine.evaluate_expiration(SignalStatus.EARLY_WATCH, now - timedelta(minutes=5)) is False
    assert SignalStateMachine.evaluate_expiration(SignalStatus.EARLY_WATCH, now - timedelta(minutes=11)) is True

    # 2. SETUP_DEVELOPING expiration (15 min window)
    assert SignalStateMachine.evaluate_expiration(SignalStatus.SETUP_DEVELOPING, now - timedelta(minutes=12)) is False
    assert SignalStateMachine.evaluate_expiration(SignalStatus.SETUP_DEVELOPING, now - timedelta(minutes=16)) is True

    # 3. Structural Invalidation via live quote evaluation
    # CE invalidation: spot drops below invalidation boundary
    res_ce = SignalStateMachine.evaluate_live_quote(
        current_status=SignalStatus.SETUP_DEVELOPING,
        current_option_price=120.0,
        underlying_spot=25040.0,
        entry_high=125.0,
        max_entry=128.0,
        sl_price=95.0,
        tp1_price=150.0,
        tp2_price=175.0,
        tp3_price=200.0,
        underlying_invalidation=25050.0,  # spot (25040) < invalidation (25050)
        direction="BUY_CE",
    )
    assert res_ce == SignalStatus.INVALIDATED

    # PE invalidation: spot rallies above invalidation boundary
    res_pe = SignalStateMachine.evaluate_live_quote(
        current_status=SignalStatus.SETUP_DEVELOPING,
        current_option_price=120.0,
        underlying_spot=25060.0,
        entry_high=125.0,
        max_entry=128.0,
        sl_price=95.0,
        tp1_price=150.0,
        tp2_price=175.0,
        tp3_price=200.0,
        underlying_invalidation=25050.0,  # spot (25060) > invalidation (25050)
        direction="BUY_PE",
    )
    assert res_pe == SignalStatus.INVALIDATED


def test_three_stages_distinct_criteria():
    from app.signals.state_machine import SignalStateMachine, SignalStatus

    # Allowed stage transitions
    assert SignalStateMachine.can_transition(SignalStatus.EARLY_WATCH, SignalStatus.SETUP_DEVELOPING) is True
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.ENTRY) is True
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.EARLY_WATCH) is True
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.INVALIDATED) is True
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.EXPIRED) is True


def test_tick_delta_estimation_and_data_source_labeling():
    from app.analysis.futures_oi import FuturesOIAnalyzer
    analyzer = FuturesOIAnalyzer()
    inst = "NIFTY_TICK_TEST"

    # Consecutive ticks
    d1 = analyzer.record_tick(inst, price=100.0, volume=10)
    d2 = analyzer.record_tick(inst, price=102.0, volume=25)  # Uptick -> +25
    d3 = analyzer.record_tick(inst, price=101.5, volume=15)  # Downtick -> -15
    d4 = analyzer.record_tick(inst, price=101.5, volume=20)  # Zero-tick -> carries -1 -> -20

    assert d2 == 25.0
    assert d3 == -15.0
    assert d4 == -20.0

    flow = analyzer.evaluate_order_flow_delta(
        traded_volume=10000,
        bid_volume=4500,
        ask_volume=5500,
        price_change_pct=0.15,
        rvol=1.2,
        estimated_tick_delta=2000.0,
        data_source="ESTIMATED_TICK_DELTA",
    )
    assert flow.data_source == "ESTIMATED_TICK_DELTA"
    assert flow.is_aggressive_buying is True


def test_oi_timestamp_window_boundaries():
    from app.analysis.futures_oi import FuturesOIAnalyzer
    analyzer = FuturesOIAnalyzer()
    inst = "NIFTY_OI_WINDOW_TEST"
    now = datetime(2026, 10, 15, 10, 30, 0, tzinfo=timezone.utc)

    # 1. Record snapshots at diverse intervals:
    # 50s ago: too young for 3m (< 90s)
    analyzer.record_oi_snapshot(inst, 105000, timestamp=now - timedelta(seconds=50))
    # 180s ago: target 3m observation in [90, 300]
    analyzer.record_oi_snapshot(inst, 100000, timestamp=now - timedelta(seconds=180))
    # 250s ago: alternate in [90, 300], but 180s is closer to 180s
    analyzer.record_oi_snapshot(inst, 98000, timestamp=now - timedelta(seconds=250))
    # 350s ago: outside both [90, 300] and [600, 1200]
    analyzer.record_oi_snapshot(inst, 95000, timestamp=now - timedelta(seconds=350))
    # 890s ago: candidate for 15m in [600, 1200] (closest to 900s)
    analyzer.record_oi_snapshot(inst, 90000, timestamp=now - timedelta(seconds=890))
    # 1400s ago: too old for 15m (> 1200s)
    analyzer.record_oi_snapshot(inst, 85000, timestamp=now - timedelta(seconds=1400))
    # Previous day: outside session boundary
    analyzer.record_oi_snapshot(inst, 70000, timestamp=now - timedelta(days=1))

    res = analyzer.calculate_rolling_oi_velocity(inst, 110000, timestamp=now)
    assert res["status"] == "AVAILABLE"
    assert res["has_valid_3m"] is True
    assert res["has_valid_15m"] is True
    assert res["is_reliable"] is True
    # 3m delta vs 180s (100,000): (110000 - 100000) / 100000 = +10.0%
    assert abs(res["delta_3m_pct"] - 10.0) < 0.01
    # 15m delta vs 890s (90,000): (110000 - 90000) / 90000 = +22.22%
    assert abs(res["delta_15m_pct"] - 22.22) < 0.1
    # Session count excludes previous day's snapshot
    assert res["obs_count"] == 6

    # 2. Rejection of observations falling outside windows
    inst_no_win = "NIFTY_OI_NO_WIN"
    analyzer.record_oi_snapshot(inst_no_win, 100000, timestamp=now - timedelta(seconds=50))  # too young
    analyzer.record_oi_snapshot(inst_no_win, 90000, timestamp=now - timedelta(seconds=350))  # in-between
    res_no_win = analyzer.calculate_rolling_oi_velocity(inst_no_win, 105000, timestamp=now)
    assert res_no_win["has_valid_3m"] is False
    assert res_no_win["has_valid_15m"] is False
    assert res_no_win["status"] == "MISSING"


def test_oi_stale_and_missing_no_bonus_points():
    from app.analysis.futures_oi import FuturesOIAnalyzer
    analyzer = FuturesOIAnalyzer()
    inst = "NIFTY_OI_STALE"
    now = datetime(2026, 10, 15, 11, 0, 0, tzinfo=timezone.utc)

    # Observation age 400s > 360s MAX_STALENESS
    analyzer.record_oi_snapshot(inst, 100000, timestamp=now - timedelta(seconds=400))
    stale_res = analyzer.calculate_rolling_oi_velocity(inst, 100000, timestamp=now)
    assert stale_res["status"] == "STALE"
    assert stale_res["is_reliable"] is False
    assert stale_res["staleness_seconds"] >= 400.0

    # Neutral missing / stale data must award ZERO bonus confidence points
    for bad_status in ("STALE", "MISSING", "UNAVAILABLE", "OUT_OF_SESSION"):
        score = confidence_evaluator.calculate_score(
            structure_state="BULLISH_STRUCTURE",
            candle_body_strength=0.8,
            rvol=1.5,
            breadth_percentage=70.0,
            oi_regime_aligned=True,
            spread_pct=0.5,
            planned_rr_tp1=2.0,
            side="BUY_CE",
            oi_status=bad_status,
        )
        assert score.futures_options_positioning == 0.0, f"Expected 0.0 for {bad_status}"


def test_net_rr_friction_matrix():
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_RR_TEST",
        symbol="NIFTY25000CE",
        strike=25000.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=100.0,
        max_acceptable_entry=102.0,
        delta=0.50,
        gamma=0.002,
        iv=14.0,
        current_bid=99.5,
        current_ask=100.0,
        volume=50000,
        oi=100000,
        bid_ask_spread=0.5,
        spread_pct=0.5,
    )

    # 1. Nonzero Slippage (0.5% per side) & Transaction Costs
    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=25010.0,
        breakout_candle_high=25025.0,
        breakout_candle_low=24985.0,
        orb_boundary=24980.0,
        direction="BUY_CE",
    )
    assert errs == []
    assert levels is not None
    # TP1 net reward / net risk must satisfy >= 1.50 after bilateral friction
    assert levels.net_rr_tp1 >= 1.50
    assert levels.tp1_price > contract.executable_ask
    assert levels.estimated_slippage > 0.0
    assert levels.estimated_transaction_cost > 0.0

    # 2. Zero Slippage calculation
    from config import settings
    old_slip = settings.SLIPPAGE_ESTIMATE_PCT
    try:
        settings.SLIPPAGE_ESTIMATE_PCT = 0.0
        levels_zero_slip, zero_errs = targets_engine.calculate_levels(
            contract=contract,
            underlying_spot=25010.0,
            breakout_candle_high=25025.0,
            breakout_candle_low=24985.0,
            orb_boundary=24980.0,
            direction="BUY_CE",
        )
        assert zero_errs == []
        assert levels_zero_slip is not None
        assert levels_zero_slip.net_rr_tp1 >= 1.50
        assert levels_zero_slip.estimated_slippage == 0.0
    finally:
        settings.SLIPPAGE_ESTIMATE_PCT = old_slip

    # 3. Invalid Stop Level rejection (calculated option SL <= 0)
    zero_ask_contract = SelectedContract(
        instrument_id="OPT_ZERO",
        symbol="NIFTY25500CE",
        strike=25500.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=0.0,  # 0 premium -> SL <= 0
        max_acceptable_entry=1.0,
        delta=0.50,
        gamma=0.001,
        iv=14.0,
        current_bid=0.0,
        current_ask=0.0,
        volume=50000,
        oi=100000,
        spread_pct=0.5,
    )
    bad_levels, bad_errs = targets_engine.calculate_levels(
        contract=zero_ask_contract,
        underlying_spot=25000.0,
        breakout_candle_high=25020.0,
        breakout_candle_low=24980.0,
        orb_boundary=24970.0,
        direction="BUY_CE",
    )
    assert bad_levels is None
    assert any("stop-loss is <= 0" in e for e in bad_errs)

    # 4. Insufficient Net Reward-to-Risk penalty in confidence scoring (< 1.2 = 0.0)
    score_low_rr = confidence_evaluator.calculate_score(
        structure_state="BULLISH_STRUCTURE",
        candle_body_strength=0.8,
        rvol=1.5,
        breadth_percentage=70.0,
        oi_regime_aligned=True,
        spread_pct=0.5,
        planned_rr_tp1=1.0,  # Below 1.2 -> 0.0 points
        side="BUY_CE",
    )
    assert score_low_rr.volatility_rr_suitability == 0.0

    score_good_rr = confidence_evaluator.calculate_score(
        structure_state="BULLISH_STRUCTURE",
        candle_body_strength=0.8,
        rvol=1.5,
        breadth_percentage=70.0,
        oi_regime_aligned=True,
        spread_pct=0.5,
        planned_rr_tp1=1.8,  # Meets >= 1.5 minimum
        side="BUY_CE",
    )
    assert score_good_rr.volatility_rr_suitability >= 8.0


def test_underlying_move_cap_delta_gamma():
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_CAP_TEST",
        symbol="NIFTY25000CE",
        strike=25000.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=100.0,
        max_acceptable_entry=102.0,
        delta=0.50,
        gamma=0.001,
        iv=14.0,
        current_bid=99.5,
        current_ask=100.0,
        volume=50000,
        oi=100000,
        bid_ask_spread=0.5,
        spread_pct=0.5,
    )

    spot = 25000.0
    # Delta-Gamma quadratic convexity conversion:
    # dS = (-delta + sqrt(delta^2 + 2 * gamma * gain)) / gamma
    req_spot = targets_engine.convert_premium_gain_to_spot_move(50.0, delta=0.50, gamma=0.001)
    assert 90.0 < req_spot < 95.0
    # Convexity yields smaller required spot move than linear approximation (50 / 0.50 = 100)
    assert req_spot < (50.0 / 0.50)

    # Rejection when spot move exceeds instrument cap (e.g. tight 0.10% cap)
    levels_breached, breach_errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=spot,
        breakout_candle_high=25020.0,
        breakout_candle_low=24950.0,
        orb_boundary=24940.0,
        direction="BUY_CE",
        max_underlying_move_pct=0.10,
    )
    assert levels_breached is None
    assert any("exceeding max configured cap" in e for e in breach_errs)

    # Acceptance when within configurable instrument cap (e.g. 1.5%)
    levels_passed, pass_errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=spot,
        breakout_candle_high=25020.0,
        breakout_candle_low=24950.0,
        orb_boundary=24940.0,
        direction="BUY_CE",
        max_underlying_move_pct=1.50,
    )
    assert pass_errs == []
    assert levels_passed is not None


def test_directional_wick_penalties_and_invalidation():
    now = datetime.now(timezone.utc)

    # 1. Bullish CE setup: Lower wick is FAVORABLE (buying the dip)
    hammer_candle = UnifiedCandle(
        instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
        timestamp=now, open=102.0, high=110.0, low=95.0, close=109.0
    )
    impact_ce = PriceActionAnalyzer.evaluate_directional_wick(hammer_candle, "BUY_CE")
    assert impact_ce.is_favorable is True
    assert impact_ce.is_scoring_penalty is False
    assert impact_ce.is_temporary_veto is False
    assert impact_ce.is_structural_invalidation is False

    # 2. Bullish CE with moderate adverse wick (30% upper wick): scoring penalty
    penalty_candle = UnifiedCandle(
        instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
        timestamp=now, open=100.0, high=110.0, low=100.0, close=107.0
    )
    impact_pen = PriceActionAnalyzer.evaluate_directional_wick(penalty_candle, "BUY_CE")
    assert impact_pen.is_scoring_penalty is True
    assert impact_pen.is_temporary_veto is False
    assert impact_pen.is_structural_invalidation is False
    assert impact_pen.penalty_points == 8.0

    # 3. Bullish CE with 45% upper wick: temporary entry veto
    veto_candle = UnifiedCandle(
        instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
        timestamp=now, open=100.0, high=200.0, low=100.0, close=155.0
    )
    impact_veto = PriceActionAnalyzer.evaluate_directional_wick(veto_candle, "BUY_CE")
    assert impact_veto.is_temporary_veto is True
    assert impact_veto.is_structural_invalidation is False

    # 4. Bullish CE with 65% upper wick AND closed back inside boundary: structural invalidation
    inv_candle = UnifiedCandle(
        instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
        timestamp=now, open=120.0, high=200.0, low=100.0, close=135.0
    )
    impact_inv = PriceActionAnalyzer.evaluate_directional_wick(inv_candle, "BUY_CE", range_boundary=150.0)
    assert impact_inv.is_structural_invalidation is True
    assert impact_inv.is_temporary_veto is True

    # 5. Bearish PE setup: Upper wick is FAVORABLE (sellers rejecting highs)
    pe_favorable = UnifiedCandle(
        instrument_id="NIFTY", symbol="NIFTY", timeframe="5m",
        timestamp=now, open=108.0, high=115.0, low=100.0, close=101.0
    )
    impact_pe = PriceActionAnalyzer.evaluate_directional_wick(pe_favorable, "BUY_PE")
    assert impact_pe.is_favorable is True
    assert impact_pe.is_temporary_veto is False


def test_entry_trigger_modes_conservative_balanced_fast():
    now = datetime.now(timezone.utc)
    orb = OpeningRange(range_high=25100.0, range_low=25000.0, range_span=100.0, is_formed=True)

    c5m_breakout = [
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="5m", timestamp=now - timedelta(minutes=10), open=25020, high=25060, low=25010, close=25050),
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="5m", timestamp=now - timedelta(minutes=5), open=25050, high=25090, low=25040, close=25080),
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="5m", timestamp=now, open=25080, high=25130, low=25070, close=25120),
    ]

    c1m_breakout = [
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="1m", timestamp=now - timedelta(minutes=2), open=25090, high=25095, low=25085, close=25092),
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="1m", timestamp=now - timedelta(minutes=1), open=25092, high=25110, low=25090, close=25108),
        UnifiedCandle(instrument_id="NIFTY", symbol="NIFTY", timeframe="1m", timestamp=now, open=25108, high=25125, low=25105, close=25122),
    ]

    # 1. CONSERVATIVE: passes on confirmed 5m close
    trig_cons, mode_cons, _ = PriceActionAnalyzer.evaluate_entry_trigger(
        mode="CONSERVATIVE",
        setup_type=SetupType.BULLISH_BREAKOUT,
        direction="BUY_CE",
        underlying_spot=25120.0,
        orb=orb,
        candles_5m=c5m_breakout,
    )
    assert trig_cons is True
    assert mode_cons == "CONSERVATIVE"

    # Conservative fails if setup is only early momentum
    trig_cons_early, _, _ = PriceActionAnalyzer.evaluate_entry_trigger(
        mode="CONSERVATIVE",
        setup_type=SetupType.EARLY_BULLISH_MOMENTUM,
        direction="BUY_CE",
        underlying_spot=25095.0,
        orb=orb,
        candles_5m=c5m_breakout,
    )
    assert trig_cons_early is False

    # 2. BALANCED: passes with 1m bar breakout even if 5m is EARLY_BULLISH_MOMENTUM
    trig_bal, mode_bal, _ = PriceActionAnalyzer.evaluate_entry_trigger(
        mode="BALANCED",
        setup_type=SetupType.EARLY_BULLISH_MOMENTUM,
        direction="BUY_CE",
        underlying_spot=25122.0,
        orb=orb,
        candles_5m=c5m_breakout,
        candles_1m=c1m_breakout,
    )
    assert trig_bal is True
    assert mode_bal == "BALANCED"

    # 3. FAST: passes with strict freshness (<= 2s), tight spread, low false move risk, tick delta
    trig_fast, mode_fast, _ = PriceActionAnalyzer.evaluate_entry_trigger(
        mode="FAST",
        setup_type=SetupType.EARLY_BULLISH_MOMENTUM,
        direction="BUY_CE",
        underlying_spot=25105.0,
        orb=orb,
        candles_5m=c5m_breakout,
        quote_staleness_sec=1.0,
        spread_pct=0.5,
        max_spread_pct=1.5,
        false_move_score=15.0,
        tick_delta=500.0,
    )
    assert trig_fast is True
    assert mode_fast == "FAST"

    # Fast mode fails if quote is stale (> 2s)
    trig_fast_stale, _, reasons_stale = PriceActionAnalyzer.evaluate_entry_trigger(
        mode="FAST",
        setup_type=SetupType.EARLY_BULLISH_MOMENTUM,
        direction="BUY_CE",
        underlying_spot=25105.0,
        orb=orb,
        candles_5m=c5m_breakout,
        quote_staleness_sec=3.5,
        spread_pct=0.5,
        max_spread_pct=1.5,
        false_move_score=15.0,
        tick_delta=500.0,
    )
    assert trig_fast_stale is False
    assert any("quote age" in r for r in reasons_stale)


def test_signal_lifecycle_downgrade_and_resurrection_prevention():
    from app.signals.state_machine import SignalStateMachine, SignalStatus

    # 1. SETUP_DEVELOPING can downgrade to EARLY_WATCH
    assert SignalStateMachine.can_transition(SignalStatus.SETUP_DEVELOPING, SignalStatus.EARLY_WATCH) is True

    # 2. Terminal states CANNOT transition to any active or pending states (zero resurrection)
    terminal_states = [SignalStatus.INVALIDATED, SignalStatus.EXPIRED, SignalStatus.CLOSED, SignalStatus.ENTRY_MISSED]
    active_targets = [SignalStatus.EARLY_WATCH, SignalStatus.SETUP_DEVELOPING, SignalStatus.ENTRY, SignalStatus.WATCH]

    for term in terminal_states:
        for target in active_targets:
            assert SignalStateMachine.can_transition(term, target) is False, f"Resurrection breach: {term} -> {target}"


def test_duplicate_paper_entry_prevention():
    from app.tracking.paper_trader import PaperTradingEngine
    from app.database.models import Signal, SignalLevels, RiskState, utc_now
    from config import settings

    engine = PaperTradingEngine()
    engine.clear_trades()

    now = utc_now()
    sig = Signal(
        signal_id="SIG_DUP_001",
        underlying="NIFTY",
        side="BUY_CE",
        score=85.0,
        status="ENTRY",
        reasons=[],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG_DUP_001",
        contract_symbol="NIFTY25000CE",
        strike=25000.0,
        expiry=now + timedelta(days=2),
        entry_range_low=100.0,
        entry_range_high=102.0,
        max_entry_price=105.0,
        sl_price=85.0,
        underlying_invalidation=24950.0,
        tp1_price=130.0,
        tp2_price=150.0,
        tp3_price=170.0,
        planned_rr_tp1=1.65,
        quote_timestamp=now,
    )
    risk_state = RiskState(
        session_date="2026-10-15",
        starting_capital=500000.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        open_risk=0.0,
        stop_outs_count=0,
        consecutive_losses=0,
        trading_locked=False,
    )

    # 1. First execution opens trade
    trade1 = engine.open_trade(sig, levels, lot_size=75, risk_state=risk_state)
    assert trade1 is not None
    assert trade1.signal_id == "SIG_DUP_001"

    # 2. Duplicate execution with same signal_id is rejected
    trade2 = engine.open_trade(sig, levels, lot_size=75, risk_state=risk_state)
    assert trade2 is None

    # 3. Concurrent trade on same underlying when ALLOW_SIGNAL_REENTRY is False
    settings.ALLOW_SIGNAL_REENTRY = False
    sig2 = Signal(
        signal_id="SIG_DUP_002",
        underlying="NIFTY",
        side="BUY_CE",
        score=82.0,
        status="ENTRY",
        reasons=[],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels2 = SignalLevels(
        signal_id="SIG_DUP_002",
        contract_symbol="NIFTY25100CE",
        strike=25100.0,
        expiry=now + timedelta(days=2),
        entry_range_low=90.0,
        entry_range_high=92.0,
        max_entry_price=95.0,
        sl_price=75.0,
        underlying_invalidation=24950.0,
        tp1_price=120.0,
        tp2_price=140.0,
        tp3_price=160.0,
        planned_rr_tp1=1.60,
        quote_timestamp=now,
    )
    trade3 = engine.open_trade(sig2, levels2, lot_size=75, risk_state=risk_state)
    assert trade3 is None


def test_independent_cash_flow_net_rr_audit():
    """Independently verifies entry and exit cash flows, round-trip brokerage/STT, and slippage."""
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_CASH_FLOW_TEST",
        symbol="NIFTY25000CE",
        strike=25000.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=120.0,
        max_acceptable_entry=122.0,
        delta=0.52,
        gamma=0.0015,
        iv=13.5,
        current_bid=119.5,
        current_ask=120.0,
        volume=50000,
        oi=100000,
        spread_pct=0.42,
    )

    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=25010.0,
        breakout_candle_high=25025.0,
        breakout_candle_low=24985.0,
        orb_boundary=24980.0,
        direction="BUY_CE",
    )
    assert errs == []
    assert levels is not None

    # Run separate independent cash-flow audit
    audit = targets_engine.audit_net_reward_risk_cash_flows(
        entry_ask=contract.executable_ask,
        sl_price=levels.sl_price,
        tp1_price=levels.tp1_price,
        lot_size=contract.lot_size,
        slippage_pct=0.005,
        transaction_cost_per_lot=40.0,
    )
    assert audit["is_compliant"] is True
    assert audit["net_rr_tp1"] >= 1.50
    # Entry fill is worse than Ask due to 0.5% entry slippage
    assert audit["entry_fill"] > contract.executable_ask
    # SL fill is worse than SL price due to 0.5% stop slippage
    assert audit["sl_fill"] < levels.sl_price
    # TP1 fill is less than TP1 price due to 0.5% target slippage
    assert audit["tp1_fill"] < levels.tp1_price


def test_exchange_tick_size_conformity():
    """Verifies that all calculated stop and target price levels strictly match exchange ₹0.05 tick size."""
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine

    contract = SelectedContract(
        instrument_id="OPT_TICK_TEST",
        symbol="NIFTY25100CE",
        strike=25100.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        lot_size=75,
        executable_ask=113.33,  # Non-tick entry ask
        max_acceptable_entry=115.0,
        delta=0.50,
        gamma=0.002,
        iv=14.0,
        current_bid=113.0,
        current_ask=113.35,
        volume=50000,
        oi=100000,
        spread_pct=0.5,
    )

    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=25100.0,
        breakout_candle_high=25115.0,
        breakout_candle_low=25085.0,
        orb_boundary=25080.0,
        direction="BUY_CE",
    )
    assert errs == []
    assert levels is not None

    for price_name, price in [
        ("sl_price", levels.sl_price),
        ("tp1_price", levels.tp1_price),
        ("tp2_price", levels.tp2_price),
        ("tp3_price", levels.tp3_price),
    ]:
        # Exchange tick size: price in paise must be a clean multiple of 5 paise (0.05)
        cents = round(price * 100)
        assert cents % 5 == 0, f"{price_name} ({price}) is not a valid 0.05 exchange tick"


def test_restart_recovery_and_persistent_duplicate_prevention():
    """Verifies that historical executed signal IDs loaded from DB block duplicate trades after restart."""
    from app.tracking.paper_trader import PaperTradingEngine
    from app.database.models import Signal, SignalLevels, RiskState, utc_now

    engine = PaperTradingEngine()
    engine.clear_trades()

    # Simulate process restart: DB has 2 previously closed trades
    historical_db_executed_ids = {"SIG_HISTORICAL_001", "SIG_HISTORICAL_002"}
    engine.load_executed_signals(historical_db_executed_ids)

    now = utc_now()
    # Attempting to execute a signal whose ID is in historical DB must be blocked immediately
    duplicate_sig = Signal(
        signal_id="SIG_HISTORICAL_001",
        underlying="NIFTY",
        side="BUY_CE",
        score=88.0,
        status="ENTRY",
        reasons=[],
        score_breakdown={},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )
    levels = SignalLevels(
        signal_id="SIG_HISTORICAL_001",
        contract_symbol="NIFTY25000CE",
        strike=25000.0,
        expiry=now + timedelta(days=2),
        entry_range_low=100.0,
        entry_range_high=102.0,
        max_entry_price=105.0,
        sl_price=85.0,
        underlying_invalidation=24950.0,
        tp1_price=130.0,
        tp2_price=150.0,
        tp3_price=170.0,
        planned_rr_tp1=1.65,
        quote_timestamp=now,
    )
    risk_state = RiskState(
        session_date="2026-10-15",
        starting_capital=500000.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        open_risk=0.0,
        stop_outs_count=0,
        consecutive_losses=0,
        trading_locked=False,
    )

    trade = engine.open_trade(duplicate_sig, levels, lot_size=75, risk_state=risk_state)
    assert trade is None, "Expected duplicate paper trade to be blocked by restored historical signal IDs"


@pytest.mark.asyncio
async def test_terminal_cooldown_expiry_and_regime_reset():
    """Verifies 15-minute terminal cooldown suppresses re-entry during cooldown and allows new setups after expiry."""
    from app.database.repository import DatabaseManager, Repository
    from app.database.models import Signal, SignalLevels, utc_now
    from config import settings

    db_mgr = DatabaseManager("sqlite+aiosqlite:///:memory:")
    await db_mgr.init_db()
    repo = Repository(db_mgr.session_factory)

    now = utc_now()
    # 1. Signal invalidated 8 minutes ago (< 15 min cooldown)
    recent_term_sig = Signal(
        signal_id="SIG_TERM_RECENT",
        underlying="BANKNIFTY",
        side="BUY_PE",
        score=75.0,
        status="INVALIDATED",
        reasons=["Structural invalidation"],
        score_breakdown={},
        created_at=now - timedelta(minutes=10),
        expires_at=now + timedelta(minutes=5),
    )
    levels1 = SignalLevels(
        signal_id="SIG_TERM_RECENT",
        contract_symbol="BANKNIFTY52000PE",
        strike=52000.0,
        expiry=now + timedelta(days=2),
        entry_range_low=200.0,
        entry_range_high=205.0,
        max_entry_price=210.0,
        sl_price=170.0,
        underlying_invalidation=52100.0,
        tp1_price=260.0,
        tp2_price=300.0,
        tp3_price=350.0,
        planned_rr_tp1=1.60,
        quote_timestamp=now - timedelta(minutes=8),
    )
    await repo.create_signal(recent_term_sig, levels1)
    await repo.update_signal_status(recent_term_sig.signal_id, "INVALIDATED")

    # Within cooldown: query returns the recent terminal signal
    term_signals = await repo.get_recent_terminal_signals("BANKNIFTY", cooldown_minutes=settings.RESET_COOLDOWN_MINUTES)
    assert len(term_signals) >= 1
    assert term_signals[0].signal_id == "SIG_TERM_RECENT"

    # After cooldown: query with smaller cooldown window (e.g. 5 minutes) finds no terminal signal
    term_signals_expired = await repo.get_recent_terminal_signals("BANKNIFTY", cooldown_minutes=5)
    assert len(term_signals_expired) == 0


def test_target_and_sl_percentages_from_entry_price():
    """Validates percentage bounds of SL and TPs relative to entry price."""
    from app.signals.contract_selector import SelectedContract
    from app.signals.targets import targets_engine
    from config import settings

    contract = SelectedContract(
        instrument_id="TEST_NIFTY_22500_CE",
        symbol="NIFTY 24 APR 22500 CALL",
        underlying="NIFTY",
        strike=22500.0,
        option_type="CE",
        expiry_str="15-OCT-2026",
        ltp=150.0,
        current_bid=149.80,
        current_ask=150.00,
        executable_ask=150.00,
        max_acceptable_entry=151.00,
        spread=0.20,
        spread_pct=0.13,
        volume=100000,
        oi=500000,
        delta=0.50,
        gamma=0.0015,
        iv=14.0,
        lot_size=75,
    )

    # 1. Standard setup: 30 pts underlying invalidation distance
    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=22500.0,
        breakout_candle_high=22515.0,
        breakout_candle_low=22470.0,
        orb_boundary=22470.0,
        direction="BUY_CE",
    )
    assert levels is not None
    assert errs == []

    # Calculate actual percentages relative to executable_ask (150.0)
    sl_pct_below_entry = ((contract.executable_ask - levels.sl_price) / contract.executable_ask) * 100.0
    tp1_pct_above_entry = ((levels.tp1_price - contract.executable_ask) / contract.executable_ask) * 100.0
    tp2_pct_above_entry = ((levels.tp2_price - contract.executable_ask) / contract.executable_ask) * 100.0
    tp3_pct_above_entry = ((levels.tp3_price - contract.executable_ask) / contract.executable_ask) * 100.0

    # Stop Loss is strictly within [settings.MIN_OPTION_RISK_PCT, settings.MAX_OPTION_RISK_PCT] (10% to 35%)
    assert settings.MIN_OPTION_RISK_PCT * 100.0 <= sl_pct_below_entry <= settings.MAX_OPTION_RISK_PCT * 100.0
    # TP1 is strictly higher than SL % by >= 1.5x (Net R:R >= 1.5)
    assert tp1_pct_above_entry > sl_pct_below_entry * 1.5
    # TP2 > TP1
    assert tp2_pct_above_entry > tp1_pct_above_entry
    # TP3 > TP2
    assert tp3_pct_above_entry > tp2_pct_above_entry
    assert levels.net_rr_tp1 >= settings.MIN_REWARD_TO_RISK_RATIO


@pytest.mark.asyncio
async def test_radar_promotion_to_entry_executes_paper_trade(tmp_path):
    """Verifies that when a radar signal (EARLY_WATCH) is promoted to ENTRY,
    the paper trade executes properly and deduplication does NOT falsely block it.
    """
    from datetime import datetime, timezone, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from app.database.models import Base, Signal, SignalLevels, RiskState
    from app.database.repository import Repository
    from app.tracking.paper_trader import PaperTradingEngine
    from app.signals.detector import SignalCandidate
    from app.signals.contract_selector import SelectedContract
    from app.signals.confidence import ScoreBreakdown
    from main import try_fire_entry
    import main

    db_path = tmp_path / "test_promo.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repo = Repository(session_factory)
    paper_engine = PaperTradingEngine()

    # Point main globals to test instances
    old_repo = main.repository
    old_paper = main.paper_trading_engine
    main.repository = repo
    main.paper_trading_engine = paper_engine

    try:
        now = datetime.now(timezone.utc)
        setup_key = "NIFTY|BUY_CE|202610161047"
        sig_id = "SIG-TEST-PROMO-01"

        # 1. First saved as EARLY_WATCH radar signal
        sig = Signal(
            signal_id=sig_id,
            setup_key=setup_key,
            underlying="NIFTY",
            side="BUY_CE",
            score=51.0,
            strategy_version="v1.0.0",
            status="EARLY_WATCH",
            reasons=["Stage: EARLY_WATCH"],
            score_breakdown={"stage": "EARLY_WATCH"},
            created_at=now,
            expires_at=now + timedelta(minutes=15),
        )
        levels = SignalLevels(
            signal_id=sig_id,
            contract_symbol="NIFTY 19 OCT 23100 CE",
            strike=23100.0,
            expiry=now + timedelta(days=3),
            entry_range_low=94.5,
            entry_range_high=95.0,
            max_entry_price=96.0,
            sl_price=80.0,
            underlying_invalidation=23050.0,
            tp1_price=120.0,
            tp2_price=140.0,
            tp3_price=165.0,
            planned_rr_tp1=1.6,
            current_bid=94.8,
            current_ask=95.0,
            delta=0.50,
            quote_timestamp=now,
        )
        await repo.create_signal(sig, levels)

        # 2. Setup qualifies as ENTRY (score 78.0)
        candidate = SignalCandidate(
            signal=Signal(
                signal_id=sig_id,
                setup_key=setup_key,
                underlying="NIFTY",
                side="BUY_CE",
                score=78.0,
                strategy_version="v1.0.0",
                status="ENTRY",
                reasons=["Stage: ENTRY", "Breakout confirmed"],
                score_breakdown={"stage": "ENTRY"},
                created_at=now,
                expires_at=now + timedelta(minutes=15),
            ),
            levels=levels,
            contract=SelectedContract(
                instrument_id="OPT-1",
                symbol="NIFTY 19 OCT 23100 CE",
                strike=23100.0,
                option_type="CE",
                expiry_str="19-OCT-2026",
                lot_size=65,
                executable_ask=95.0,
                current_bid=94.8,
                current_ask=95.0,
                max_acceptable_entry=96.0,
                spread=0.2,
                spread_pct=0.21,
                volume=50000,
                oi=200000,
                delta=0.50,
                gamma=0.001,
                iv=12.0,
            ),
            score_breakdown=ScoreBreakdown(total_score=78.0),
            passed_all_gates=True,
        )

        risk_state = RiskState(
            session_date=now.strftime("%Y-%m-%d"),
            starting_capital=500000.0,
            realized_pnl=0.0,
            open_risk=0.0,
            trading_locked=False,
        )

        # 3. Fire entry on promotion: must successfully open paper trade!
        fired = await try_fire_entry(candidate, risk_state)
        assert fired is True, "Promotion to ENTRY must open paper trade!"
        assert len(paper_engine.get_open_trades()) == 1, "Active open trade must exist!"
        open_tr = paper_engine.get_open_trades()[0]
        assert open_tr.contract_symbol == "NIFTY 19 OCT 23100 CE"
        assert open_tr.entry_price == 95.0

        # 4. Immediate second attempt on the same setup_key must be blocked by deduplication
        fired_duplicate = await try_fire_entry(candidate, risk_state)
        assert fired_duplicate is False, "Duplicate attempt on same setup_key must be blocked!"
    finally:
        main.repository = old_repo
        main.paper_trading_engine = old_paper


def test_trend_continuation_swing_breakout_vs_consolidation():
    """Verifies that once an index is established above ORB High:
    1. Flat consolidation inside swing range returns SetupType.NONE (no log spam or false rejection).
    2. A fresh breakout above recent swing high returns SetupType.BULLISH_BREAKOUT with boundary = swing_high.
    """
    from app.analysis.price_action import PriceActionAnalyzer, SetupType, OpeningRange, DailyContext
    from app.data.base_provider import UnifiedCandle
    from datetime import datetime, timezone, timedelta

    orb = OpeningRange(range_high=22980.0, range_low=22900.0, range_span=80.0, is_formed=True)
    daily = DailyContext(pdh=22950.0, pdl=22850.0, pdc=22920.0, daily_bias="BULLISH")
    now = datetime.now(timezone.utc)

    # 10 candles, all already trading high above ORB High (22980) between 23070 and 23090
    candles = []
    for i in range(10):
        candles.append(
            UnifiedCandle(
                instrument_id="NIFTY",
                symbol="NIFTY",
                timeframe="5m",
                timestamp=now - timedelta(minutes=(10 - i) * 5),
                open=23075.0,
                high=23090.0,
                low=23070.0,
                close=23080.0,
                volume=20000,
            )
        )

    # 1. Inside consolidation above ORB High: must return SetupType.NONE
    res_cons = PriceActionAnalyzer.detect_setup(candles_5m=candles, orb=orb, daily=daily, vwap_value=22950.0)
    assert res_cons[0] == SetupType.NONE
    assert "consolidating" in res_cons[1][0]
    assert getattr(res_cons, "boundary", None) is None

    # 2. Fresh breakout candle closing at 23105 above swing high (23090.0)
    breakout_candle = UnifiedCandle(
        instrument_id="NIFTY",
        symbol="NIFTY",
        timeframe="5m",
        timestamp=now,
        open=23080.0,
        high=23110.0,
        low=23078.0,
        close=23105.0,
        volume=50000,
    )
    candles.append(breakout_candle)
    res_break = PriceActionAnalyzer.detect_setup(candles_5m=candles, orb=orb, daily=daily, vwap_value=22950.0)
    assert res_break[0] == SetupType.BULLISH_BREAKOUT
    assert getattr(res_break, "boundary", None) == 23090.0  # Dynamic boundary is the swing high, NOT the morning ORB!







