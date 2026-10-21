"""Transparent 100-Point Confidence Scoring & Hard Gate Verification Engine.

Evaluates evidence hypotheses:
1. Trend & Structure: 25 pts
2. Entry Confirmation: 20 pts
3. Volume & Participation: 15 pts
4. Market Breadth: 10 pts
5. Futures & Options Positioning: 10 pts
6. Contract Liquidity & Spread: 10 pts
7. Volatility & RR Suitability: 10 pts
Total: 100 pts

Independent Mandatory Gates:
- Feed Freshness Gate
- Bid/Ask Spread Gate
- Event Risk Veto
- Daily Risk Budget Lock
- Defensible 3-Target Gate
"""

from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel, Field
from config import settings


class ScoreBreakdown(BaseModel):
    trend_structure: float = 0.0          # Max 25
    entry_confirmation: float = 0.0       # Max 20
    volume_participation: float = 0.0     # Max 15
    market_breadth: float = 0.0           # Max 10
    futures_options_positioning: float = 0.0  # Max 10
    contract_liquidity_spread: float = 0.0    # Max 10
    volatility_rr_suitability: float = 0.0    # Max 10
    false_move_penalty: float = 0.0
    total_score: float = 0.0              # Max 100
    stage_qualified: str = "EARLY_WATCH"  # EARLY_WATCH, SETUP_DEVELOPING, ENTRY_QUALIFIED, SUB_THRESHOLD
    false_move_risk_level: str = "LOW"


class GateEvaluation(BaseModel):
    all_passed: bool
    passed_gates: List[str] = Field(default_factory=list)
    failed_gates: List[str] = Field(default_factory=list)


class ConfidenceEvaluator:
    """Calculates transparent setup scores and enforces independent hard vetoes."""

    @staticmethod
    def calculate_score(
        structure_state: str,              # BULLISH_STRUCTURE, BEARISH_STRUCTURE, CHOP
        candle_body_strength: float,       # 0.0 to 1.0 (body / range)
        rvol: float,                       # Relative volume ratio
        spread_pct: float,                 # Option spread %
        planned_rr_tp1: float,             # Expected RR to TP1
        side: str,                         # BUY_CE or BUY_PE
        breadth_percentage: Optional[float] = None,  # Advances % or None if unverified/stale
        oi_regime_aligned: bool = False,   # Long buildup / short covering aligned
        false_move_risk_score: float = 0.0,
        oi_status: str = "AVAILABLE",
    ) -> ScoreBreakdown:
        breakdown = ScoreBreakdown()

        # 1. Trend & Market Structure (Max 25)
        if side == "BUY_CE":
            if structure_state == "BULLISH_STRUCTURE":
                breakdown.trend_structure = 25.0
            elif structure_state == "RANGE":
                breakdown.trend_structure = 12.0
            else:
                breakdown.trend_structure = 0.0
        else:  # BUY_PE
            if structure_state == "BEARISH_STRUCTURE":
                breakdown.trend_structure = 25.0
            elif structure_state == "RANGE":
                breakdown.trend_structure = 12.0
            else:
                breakdown.trend_structure = 0.0

        # 2. Entry Confirmation & Candle Body Strength (Max 20)
        # Scaled continuously; 0.50 target is a scoring factor, NOT hard rejection
        if candle_body_strength >= settings.CANDLE_BODY_STRONG_RATIO:
            breakdown.entry_confirmation = 20.0
        elif candle_body_strength >= settings.CANDLE_BODY_RATIO_TARGET:
            breakdown.entry_confirmation = 14.0
        elif candle_body_strength >= settings.CANDLE_BODY_WEAK_RATIO:
            breakdown.entry_confirmation = 8.0
        else:
            breakdown.entry_confirmation = 2.0

        # 3. Volume & Participation (Max 15)
        if rvol >= settings.RVOL_STRONG_THRESHOLD:
            breakdown.volume_participation = 15.0
        elif rvol >= settings.RVOL_MODERATE_THRESHOLD:
            breakdown.volume_participation = 11.0
        elif rvol >= 1.0:
            breakdown.volume_participation = 7.0
        else:
            breakdown.volume_participation = 2.0

        # 4. Market Breadth (Max 10)
        if breadth_percentage is None:
            breakdown.market_breadth = 0.0
        else:
            b_bull_strong = getattr(settings, "BREADTH_BULLISH_STRONG_PCT", 70.0)
            b_bull_mod = getattr(settings, "BREADTH_BULLISH_MODERATE_PCT", 55.0)
            b_bear_strong = getattr(settings, "BREADTH_BEARISH_STRONG_PCT", 30.0)
            b_bear_mod = getattr(settings, "BREADTH_BEARISH_MODERATE_PCT", 45.0)

            if side == "BUY_CE":
                if breadth_percentage >= b_bull_strong:
                    breakdown.market_breadth = 10.0
                elif breadth_percentage >= b_bull_mod:
                    breakdown.market_breadth = 6.0
                else:
                    breakdown.market_breadth = 0.0
            else:  # BUY_PE
                if breadth_percentage <= b_bear_strong:
                    breakdown.market_breadth = 10.0
                elif breadth_percentage <= b_bear_mod:
                    breakdown.market_breadth = 6.0
                else:
                    breakdown.market_breadth = 0.0

        # 5. Futures & Options Positioning / OI (Max 10)
        # Neutral missing-data handling must not award bonus confidence points
        if oi_status in ("UNAVAILABLE", "STALE", "MISSING", "OUT_OF_SESSION"):
            breakdown.futures_options_positioning = 0.0  # Zero bonus points for missing/stale data
        elif oi_regime_aligned:
            breakdown.futures_options_positioning = 10.0
        else:
            breakdown.futures_options_positioning = 2.0

        # 6. Contract Liquidity & Spread (Max 10)
        if spread_pct <= 0.8:
            breakdown.contract_liquidity_spread = 10.0
        elif spread_pct <= 1.5:
            breakdown.contract_liquidity_spread = 7.0
        elif spread_pct <= 2.2:
            breakdown.contract_liquidity_spread = 4.0
        else:
            breakdown.contract_liquidity_spread = 0.0

        # 7. Volatility & Reward-to-Risk Suitability (Max 10)
        if planned_rr_tp1 >= 2.0:
            breakdown.volatility_rr_suitability = 10.0
        elif planned_rr_tp1 >= settings.MIN_REWARD_TO_RISK_RATIO:
            breakdown.volatility_rr_suitability = 8.0
        elif planned_rr_tp1 >= 1.2:
            breakdown.volatility_rr_suitability = 5.0
        else:
            breakdown.volatility_rr_suitability = 0.0

        # 8. Deduct False Move / Absorption Penalties
        penalty = 0.0
        fm_high_thresh = getattr(settings, "FALSE_MOVE_HIGH_RISK_THRESHOLD", 55.0)
        fm_mod_thresh = getattr(settings, "FALSE_MOVE_MODERATE_RISK_THRESHOLD", 25.0)
        fm_high_pen = getattr(settings, "FALSE_MOVE_HIGH_PENALTY", 20.0)
        fm_mod_pen = getattr(settings, "FALSE_MOVE_MODERATE_PENALTY", 8.0)

        if false_move_risk_score > fm_high_thresh:
            penalty = fm_high_pen
            breakdown.false_move_risk_level = "HIGH"
        elif false_move_risk_score >= fm_mod_thresh:
            penalty = fm_mod_pen
            breakdown.false_move_risk_level = "MODERATE"
        else:
            breakdown.false_move_risk_level = "LOW"
        breakdown.false_move_penalty = penalty

        raw_total = (
            breakdown.trend_structure
            + breakdown.entry_confirmation
            + breakdown.volume_participation
            + breakdown.market_breadth
            + breakdown.futures_options_positioning
            + breakdown.contract_liquidity_spread
            + breakdown.volatility_rr_suitability
        )
        total = max(0.0, min(100.0, raw_total - penalty))
        breakdown.total_score = round(total, 1)

        # Stage Qualification Mapping
        if breakdown.total_score >= settings.SCORE_ENTRY_QUALIFIED:
            breakdown.stage_qualified = "ENTRY_QUALIFIED"
        elif breakdown.total_score >= settings.SCORE_SETUP_DEVELOPING:
            breakdown.stage_qualified = "SETUP_DEVELOPING"
        elif breakdown.total_score >= settings.SCORE_EARLY_WATCH:
            breakdown.stage_qualified = "EARLY_WATCH"
        else:
            breakdown.stage_qualified = "SUB_THRESHOLD"

        return breakdown

    @staticmethod
    def evaluate_mandatory_gates(
        is_quote_fresh: bool,
        is_spread_acceptable: bool,
        has_event_veto: bool,
        is_risk_locked: bool,
        has_three_targets: bool,
        is_false_move_acceptable: bool = True,
        is_net_rr_acceptable: bool = True,
    ) -> GateEvaluation:
        """Enforces non-negotiable hard gates regardless of confidence score."""
        passed: List[str] = []
        failed: List[str] = []

        if is_quote_fresh:
            passed.append(f"Quote Freshness Gate (<={settings.MAX_QUOTE_STALENESS_SECONDS:g}s latency)")
        else:
            failed.append("Quote Freshness Gate: Stale data detected")

        if is_spread_acceptable:
            passed.append("Liquidity & Spread Gate")
        else:
            failed.append("Liquidity & Spread Gate: Excessive spread / illiquid book")

        if not has_event_veto:
            passed.append("Macroeconomic Event Risk Gate")
        else:
            failed.append("Macroeconomic Event Risk Gate: High-impact release in window")

        if not is_risk_locked:
            passed.append("Capital & Daily Risk Budget Gate")
        else:
            failed.append("Capital & Daily Risk Budget Gate: Daily loss limit reached")

        if has_three_targets:
            passed.append("Defensible 3-Target Math Gate")
        else:
            failed.append("Defensible 3-Target Math Gate: Cannot compute realistic targets")

        if is_false_move_acceptable:
            passed.append("False-Move & Absorption Gate")
        else:
            failed.append("False-Move & Absorption Gate: High trap/absorption risk veto")

        if is_net_rr_acceptable:
            passed.append("Net Reward-to-Risk Gate (>= 1:1.5)")
        else:
            failed.append("Net Reward-to-Risk Gate: Inadequate net RR after slippage & costs")

        all_ok = len(failed) == 0
        return GateEvaluation(
            all_passed=all_ok,
            passed_gates=passed,
            failed_gates=failed,
        )


confidence_evaluator = ConfidenceEvaluator()
