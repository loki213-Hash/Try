"""Multi-Index Signal Detector and Setup Ranking Engine.

Runs independent strategy engines per index:
- SENSEX & BANKEX (BSE)
- NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY (NSE)
Coordinates multi-timeframe analysis, contract resolution, targets,
mandatory gates, and ranks candidates to prevent correlated exposure.
"""

from __future__ import annotations
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from pydantic import BaseModel, ConfigDict

from app.database.models import Signal, SignalLevels, RiskState
from app.data.base_provider import UnifiedCandle, OptionChainSnapshot
from app.data.instrument_master import instrument_master
from app.analysis.price_action import PriceActionAnalyzer, SetupType, OpeningRange, DailyContext
from app.analysis.indicators import TechnicalIndicators
from app.analysis.breadth import market_breadth_analyzer
from app.analysis.futures_oi import futures_oi_analyzer
from app.analysis.news_engine import news_engine
from app.signals.contract_selector import contract_selector, SelectedContract
from app.signals.targets import targets_engine, CalculatedLevels
from app.signals.confidence import confidence_evaluator, ScoreBreakdown
from app.signals.risk_manager import risk_manager
from app.monitoring.logging import logger
from config import settings, IndexConfig


class SignalCandidate(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    signal: Signal
    levels: SignalLevels
    contract: SelectedContract
    score_breakdown: ScoreBreakdown
    passed_all_gates: bool
    rejection_reasons: List[str] = []


class MultiIndexDetector:
    """Independent multi-index strategy detector."""

    def __init__(self):
        self.strategy_version = "v1.0.0"

    def analyze_index(
        self,
        underlying: str,
        candles_5m: List[UnifiedCandle],
        candles_15m: List[UnifiedCandle],
        candles_daily: List[UnifiedCandle],
        option_chain: OptionChainSnapshot,
        risk_state: RiskState,
        is_quote_fresh: bool = True,
        open_underlyings: Optional[List[str]] = None,
        candles_1m: Optional[List[UnifiedCandle]] = None,
        quote_staleness_sec: float = 0.0,
    ) -> Optional[SignalCandidate]:
        """Analyzes a single index and returns a candidate if valid setup exists."""
        cfg = instrument_master.get_index_config(underlying)
        if not cfg or not cfg.enabled:
            return None

        # 0. Data Provenance Gate: Reject simulated option chains if ALLOW_SIMULATED_ENTRIES is False
        if getattr(option_chain, "is_simulated", False) and not getattr(settings, "ALLOW_SIMULATED_ENTRIES", False):
            logger.warning(f"Signal suppressed for {underlying}: Simulated option feed detected and ALLOW_SIMULATED_ENTRIES is False.")
            return None

        # 0.1 Market Session Gate: Enforce active exchange trading hours if configured
        if getattr(settings, "ENFORCE_MARKET_HOURS", False):
            ref_time = candles_5m[-1].timestamp if candles_5m else None
            is_open, session_status = instrument_master.is_market_session_open(underlying, ref_dt=ref_time)
            if not is_open:
                return None

        # 1. Daily Context & Technical Indicators
        daily_ctx = PriceActionAnalyzer.calculate_daily_context(candles_daily)
        df_5m = TechnicalIndicators.candles_to_dataframe(candles_5m)
        if df_5m.empty or len(df_5m) < 4:
            return None

        vwap_series = TechnicalIndicators.calculate_vwap(df_5m)
        vwap_val = vwap_series.iloc[-1] if not vwap_series.empty and not pd.isna(vwap_series.iloc[-1]) else None
        rvol = TechnicalIndicators.calculate_rvol(df_5m, window=12)
        atr_series = TechnicalIndicators.calculate_atr(df_5m, period=cfg.atr_period)
        atr_val = atr_series.iloc[-1] if not atr_series.empty and not pd.isna(atr_series.iloc[-1]) else None

        # 2. Opening Range & Structure
        orb = PriceActionAnalyzer.calculate_opening_range(
            candles_5m, orb_minutes=cfg.orb_period_minutes
        )
        structure_15m = PriceActionAnalyzer.evaluate_15m_structure(candles_15m)

        # 3. Setup Detection on completed 5m bar
        setup_res = PriceActionAnalyzer.detect_setup(
            candles_5m=candles_5m,
            orb=orb,
            daily=daily_ctx,
            vwap_value=vwap_val,
        )
        setup_type, setup_reasons = setup_res

        is_bullish_candidate = setup_type in (
            SetupType.BULLISH_BREAKOUT,
            SetupType.EARLY_BULLISH_MOMENTUM,
            SetupType.BULLISH_RETEST,
        )
        is_bearish_candidate = setup_type in (
            SetupType.BEARISH_BREAKDOWN,
            SetupType.EARLY_BEARISH_MOMENTUM,
            SetupType.BEARISH_RETEST,
        )

        if not (is_bullish_candidate or is_bearish_candidate):
            return None  # Explicit NO TRADE

        direction = "BUY_CE" if is_bullish_candidate else "BUY_PE"

        # 3.1 Extension Filter: Reject entry if spot has run > 1.5 ATR beyond breakout level
        breakout_boundary = getattr(setup_res, "boundary", None)
        if breakout_boundary is None or breakout_boundary <= 0:
            breakout_boundary = orb.range_high if direction == "BUY_CE" else orb.range_low

        if atr_val and atr_val > 0 and breakout_boundary > 0:
            extension_dist = abs(option_chain.underlying_ltp - breakout_boundary)
            if extension_dist > 1.5 * atr_val:
                logger.debug(
                    f"Setup rejected for {underlying}: Price extended by {extension_dist:.1f} pts "
                    f"(>1.5 ATR = {1.5 * atr_val:.1f} pts) from breakout boundary ({breakout_boundary})."
                )
                return None

        # 4. Correlation check
        is_uncorrelated, corr_reason = risk_manager.check_correlation_exposure(
            underlying, open_underlyings or []
        )
        if not is_uncorrelated:
            logger.debug(f"Signal suppressed for {underlying}: {corr_reason}")
            return None

        # 5. Contract Resolution
        contract, contract_errs = contract_selector.select_contract(
            snapshot=option_chain,
            direction=direction,
            max_spread_pct=cfg.max_acceptable_spread_pct,
            delta_min=cfg.delta_target_min,
            delta_max=cfg.delta_target_max,
        )
        if not contract:
            logger.debug(f"Contract selection failed for {underlying}: {contract_errs}")
            return None

        # 6. Targets and Stop-Loss Calculation with ATR Bounds
        last_bar = candles_5m[-1]
        levels, target_errs = targets_engine.calculate_levels(
            contract=contract,
            underlying_spot=option_chain.underlying_ltp,
            breakout_candle_high=last_bar.high,
            breakout_candle_low=last_bar.low,
            orb_boundary=breakout_boundary,
            direction=direction,
            max_underlying_move_pct=getattr(cfg, "max_underlying_move_pct", settings.MAX_UNDERLYING_MOVE_PCT),
            atr=atr_val,
        )
        if not levels:
            logger.debug(f"Target calculation failed for {underlying}: {target_errs}")
            return None

        # 7. Market Breadth, Option Activity & Order Flow Delta
        breadth = market_breadth_analyzer.get_breadth(underlying)

        # Real-time underlying and contract price changes
        und_price_change = ((last_bar.close - last_bar.open) / max(last_bar.open, 1.0)) * 100.0

        # Real-time option premium change %
        if contract.close_price and contract.close_price > 0 and contract.ltp > 0:
            prem_change_pct = ((contract.ltp - contract.close_price) / contract.close_price) * 100.0
        else:
            prem_change_pct = und_price_change * 2.0  # Option leverage approximation if close unavailable

        # Real-time contract OI change %
        if contract.oi and contract.oi > 0 and contract.oi_change is not None:
            prev_oi = max(contract.oi - contract.oi_change, 1)
            oi_change_pct = round((contract.oi_change / prev_oi) * 100.0, 2)
        else:
            oi_change_pct = 0.0

        # Maintain rolling intraday OI buffer and tick rule tracking
        futures_oi_analyzer.record_oi_snapshot(contract.instrument_id, contract.oi or 0)
        oi_vel = futures_oi_analyzer.calculate_rolling_oi_velocity(contract.instrument_id, contract.oi or 0)
        tick_delta = futures_oi_analyzer.record_tick(contract.instrument_id, contract.ltp, contract.volume or 1)

        # Real-time order book Bid/Ask quantities for Order Flow Delta
        if contract.bid_qty > 0 and contract.ask_qty > 0:
            bid_vol = contract.bid_qty
            ask_vol = contract.ask_qty
        else:
            tot_v = contract.volume or 1000
            bid_vol = int(tot_v * 0.45)
            ask_vol = int(tot_v * 0.55) if direction == "BUY_CE" else int(tot_v * 0.45)
            if direction == "BUY_PE":
                bid_vol = int(tot_v * 0.55)

        oi_interp = futures_oi_analyzer.analyze_buildup(
            price_change_pct=und_price_change,
            oi_change_pct=oi_change_pct,
            pcr=option_chain.pcr_oi or 1.0,
            oi_status=oi_vel["status"],
            delta_3m=oi_vel.get("delta_3m_pct", 0.0),
            delta_15m=oi_vel.get("delta_15m_pct", 0.0),
        )
        opt_type = "CE" if direction == "BUY_CE" else "PE"
        opt_activity = futures_oi_analyzer.classify_option_activity(
            option_type=opt_type,
            premium_change_pct=prem_change_pct,
            oi_change_pct=oi_change_pct,
            underlying_trend="BULLISH" if direction == "BUY_CE" else "BEARISH",
        )
        order_flow = futures_oi_analyzer.evaluate_order_flow_delta(
            traded_volume=contract.volume or 50000,
            bid_volume=bid_vol,
            ask_volume=ask_vol,
            price_change_pct=und_price_change,
            rvol=rvol,
            estimated_tick_delta=tick_delta,
            data_source="ESTIMATED_TICK_DELTA" if abs(tick_delta) > 0 else "RESTING_DEPTH_IMBALANCE",
        )

        # 8. Candle Body Strength & Wick Rejections
        bar_range = max(last_bar.high - last_bar.low, 0.01)
        body_size = abs(last_bar.close - last_bar.open)
        body_strength = round(body_size / bar_range, 2)
        upper_wick, lower_wick = PriceActionAnalyzer.get_wick_ratios(last_bar)

        # 9. False-Move & Absorption Risk Evaluation
        false_move = futures_oi_analyzer.evaluate_false_move_risk(
            price_change_pct=und_price_change,
            rvol=rvol,
            upper_wick_pct=upper_wick,
            lower_wick_pct=lower_wick,
            volume_delta=order_flow.volume_delta,
            direction=direction,
        )

        oi_aligned = (
            (oi_interp.is_bullish_aligned or opt_activity.is_bullish_demand)
            if direction == "BUY_CE"
            else (oi_interp.is_bearish_aligned or opt_activity.is_bearish_demand)
        )

        # 10. 100-Point Continuous Confidence Score
        score_bd = confidence_evaluator.calculate_score(
            structure_state=structure_15m,
            candle_body_strength=body_strength,
            rvol=rvol,
            breadth_percentage=breadth.breadth_percentage if breadth else None,
            oi_regime_aligned=oi_aligned,
            spread_pct=contract.spread_pct,
            planned_rr_tp1=levels.planned_rr_tp1,
            side=direction,
            false_move_risk_score=false_move.risk_score,
            oi_status=oi_vel["status"],
        )

        # 11. News & Macro Event Risk
        eval_time = candles_5m[-1].timestamp if candles_5m else None
        news_res = news_engine.check_event_risk_veto(current_time=eval_time)

        # 12. Mandatory Hard Gates
        is_fresh = (quote_staleness_sec <= settings.MAX_QUOTE_STALENESS_SECONDS) if is_quote_fresh else False
        has_3_targets = bool(levels.tp1_price > 0 and levels.tp2_price > 0 and levels.tp3_price > 0)
        gate_res = confidence_evaluator.evaluate_mandatory_gates(
            is_quote_fresh=is_fresh,
            is_spread_acceptable=contract.spread_pct <= cfg.max_acceptable_spread_pct,
            has_event_veto=news_res.has_active_veto,
            is_risk_locked=risk_state.trading_locked,
            has_three_targets=has_3_targets,
            is_false_move_acceptable=not false_move.is_veto_for_entry,
            is_net_rr_acceptable=(levels.net_rr_tp1 >= settings.MIN_REWARD_TO_RISK_RATIO),
        )

        # 13. Three-Stage Lifecycle Qualification & Entry Window Gate
        ref_time = candles_5m[-1].timestamp if candles_5m else datetime.now(timezone.utc)
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=timezone.utc)
        try:
            import zoneinfo
            ist_tz = zoneinfo.ZoneInfo("Asia/Kolkata")
            ref_ist = ref_time.astimezone(ist_tz)
        except Exception:
            ref_ist = ref_time + timedelta(hours=5, minutes=30)
        current_min_val = ref_ist.hour * 60 + ref_ist.minute
        # Morning window: 09:30-11:30 (570-690 min); Afternoon window: 13:30-14:45 (810-885 min)
        is_in_entry_window = (570 <= current_min_val <= 690) or (810 <= current_min_val <= 885)
        if not getattr(settings, "ENFORCE_MARKET_HOURS", False):
            is_in_entry_window = True

        is_triggered, trigger_mode_used, trigger_reasons = PriceActionAnalyzer.evaluate_entry_trigger(
            mode=settings.ENTRY_TRIGGER_MODE,
            setup_type=setup_type,
            direction=direction,
            underlying_spot=option_chain.underlying_ltp,
            orb=orb,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            quote_staleness_sec=quote_staleness_sec,
            spread_pct=contract.spread_pct,
            max_spread_pct=cfg.max_acceptable_spread_pct,
            false_move_score=false_move.risk_score,
            tick_delta=tick_delta,
        )

        # Stage 3 - ENTRY: Strict execution qualification (trigger mode satisfied, passed all gates, in entry window, score >= 72)
        if (
            score_bd.total_score >= settings.SCORE_ENTRY_QUALIFIED
            and gate_res.all_passed
            and is_triggered
            and is_in_entry_window
        ):
            status = "ENTRY"
        # Stage 2 - SETUP_DEVELOPING: Assesses whether the move is genuine (score >= 62, rvol >= 1.0, false move risk not high)
        elif (
            score_bd.total_score >= settings.SCORE_SETUP_DEVELOPING
            and not false_move.is_veto_for_entry
            and rvol >= 1.0
            and setup_type != SetupType.FAILED_BREAKOUT
        ):
            status = "SETUP_DEVELOPING"
        # Stage 1 - EARLY_WATCH: High sensitivity radar (score >= 50, setup detected, no failed breakout)
        elif (
            score_bd.total_score >= settings.SCORE_EARLY_WATCH
            and setup_type != SetupType.FAILED_BREAKOUT
        ):
            status = "EARLY_WATCH"
        else:
            # Sub-threshold observation or failed breakout; no signal output
            return None

        # Target Probability & Confidence Scores
        score_data = score_bd.model_dump()
        score_data["tp1_probability"] = levels.tp1_confidence
        score_data["tp2_probability"] = levels.tp2_confidence
        score_data["tp3_probability"] = levels.tp3_confidence
        score_data["stage"] = status
        score_data["entry_trigger_mode"] = trigger_mode_used
        score_data["false_move_risk_level"] = false_move.risk_level
        score_data["false_move_risk_score"] = false_move.risk_score
        score_data["order_flow_source"] = order_flow.data_source
        score_data["net_rr_tp1"] = levels.net_rr_tp1

        # Compile structured, honest reasons
        all_reasons = list(setup_reasons)
        if status == "ENTRY" and trigger_reasons:
            all_reasons.extend(trigger_reasons)
        all_reasons.append(f"Stage: {status} [Trigger: {trigger_mode_used}]")
        all_reasons.append(f"Stage: {status}")
        all_reasons.append(f"15m structure: {structure_15m}")
        all_reasons.append(f"RVOL: {rvol:.1f}x baseline | Body Ratio: {body_strength:.2f}")
        all_reasons.append(f"False-Move Risk: {false_move.summary}")
        if breadth:
            all_reasons.append(f"Breadth: {breadth.breadth_percentage:.1f}% advancing")
        else:
            all_reasons.append("Breadth: Feed unavailable (0 pts awarded)")
        all_reasons.append(f"OI Flow: {opt_activity.interpretation} ({oi_vel['summary']})")
        all_reasons.append(
            f"Order Flow [{order_flow.data_source}]: {order_flow.interpretation} (Delta {order_flow.volume_delta:+.1f}%)"
        )
        all_reasons.append(
            f"Greeks: Delta {contract.delta:+.2f} | IV {contract.iv:.1f}% | Spread {contract.spread_pct:.2f}% | Net RR {levels.net_rr_tp1}:1"
        )
        all_reasons.append(
            f"Target Confidence: TP1 {levels.tp1_confidence}% | TP2 {levels.tp2_confidence}% | TP3 {levels.tp3_confidence}%"
        )

        now = datetime.now(timezone.utc)
        sig_id = f"SIG-{underlying}-{now.strftime('%H%M%S')}-{str(uuid.uuid4())[:6]}"
        bar_ts = last_bar.timestamp.strftime('%Y%m%d%H%M') if hasattr(last_bar.timestamp, 'strftime') else str(last_bar.timestamp)
        setup_key = f"{underlying}|{direction}|{bar_ts}"

        sig_model = Signal(
            signal_id=sig_id,
            setup_key=setup_key,
            underlying=underlying,
            side=direction,
            score=score_bd.total_score,
            strategy_version=self.strategy_version,
            status=status,
            reasons=all_reasons,
            score_breakdown=score_data,
            snapshot_id=f"SNAP-{underlying}-{int(now.timestamp())}",
            created_at=now,
            expires_at=now + timedelta(minutes=15),
        )

        levels_model = SignalLevels(
            signal_id=sig_id,
            contract_symbol=contract.symbol,
            strike=contract.strike,
            expiry=contract.expiry or instrument_master.get_exchange_weekly_expiry(underlying, now),
            entry_range_low=levels.entry_range_low,
            entry_range_high=levels.entry_range_high,
            max_entry_price=levels.max_entry_price,
            sl_price=levels.sl_price,
            underlying_invalidation=levels.underlying_invalidation,
            tp1_price=levels.tp1_price,
            tp2_price=levels.tp2_price,
            tp3_price=levels.tp3_price,
            planned_rr_tp1=levels.planned_rr_tp1,
            current_bid=contract.current_bid,
            current_ask=contract.current_ask,
            delta=contract.delta,
            quote_timestamp=now,
        )

        return SignalCandidate(
            signal=sig_model,
            levels=levels_model,
            contract=contract,
            score_breakdown=score_bd,
            passed_all_gates=gate_res.all_passed,
            rejection_reasons=gate_res.failed_gates,
        )

    def rank_candidates(self, candidates: List[SignalCandidate]) -> List[SignalCandidate]:
        """Ranks candidates by stage precedence (ENTRY > SETUP_DEVELOPING > EARLY_WATCH), score, and volume."""
        stage_priority = {"ENTRY": 3, "SETUP_DEVELOPING": 2, "EARLY_WATCH": 1, "WATCH": 1}
        valid_candidates = [
            c for c in candidates
            if c.passed_all_gates or c.signal.status in ("SETUP_DEVELOPING", "EARLY_WATCH", "WATCH")
        ]
        valid_candidates.sort(
            key=lambda c: (
                stage_priority.get(c.signal.status, 0),
                c.signal.score,
                c.contract.volume,
            ),
            reverse=True,
        )
        return valid_candidates


multi_index_detector = MultiIndexDetector()
