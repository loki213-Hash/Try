"""Price action analysis, Daily Context, and Opening Range Breakout (ORB).

Analyzes:
- Daily Context: Previous Day High (PDH), Previous Day Low (PDL), Previous Day Close (PDC)
- Intraday Structure: 15-minute higher highs / lower lows
- Confirmed 5-minute Breakouts / Breakdowns
- Wick rejection and false breakout filters
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import pandas as pd
from pydantic import BaseModel
from app.data.base_provider import UnifiedCandle
from app.analysis.indicators import TechnicalIndicators


class DailyContext(BaseModel):
    pdh: float  # Previous Day High
    pdl: float  # Previous Day Low
    pdc: float  # Previous Day Close
    daily_bias: str  # BULLISH, BEARISH, NEUTRAL


class OpeningRange(BaseModel):
    range_high: float
    range_low: float
    range_span: float
    is_formed: bool


from config import settings


class SetupType:
    BULLISH_BREAKOUT = "BULLISH_BREAKOUT"
    BEARISH_BREAKDOWN = "BEARISH_BREAKDOWN"
    BULLISH_RETEST = "BULLISH_RETEST"
    BEARISH_RETEST = "BEARISH_RETEST"
    EARLY_BULLISH_MOMENTUM = "EARLY_BULLISH_MOMENTUM"
    EARLY_BEARISH_MOMENTUM = "EARLY_BEARISH_MOMENTUM"
    FAILED_BREAKOUT = "FAILED_BREAKOUT"
    NONE = "NO_SETUP"


class SetupResult(tuple):
    """Tuple subclass (setup_type, reasons) with additional metadata attributes (boundary).
    Maintains 100% backward compatibility for unpacking (setup_type, reasons = res).
    """

    def __new__(cls, setup_type: str, reasons: List[str], boundary: Optional[float] = None):
        return super().__new__(cls, (setup_type, reasons))

    def __init__(self, setup_type: str, reasons: List[str], boundary: Optional[float] = None):
        self.setup_type = setup_type
        self.reasons = reasons
        self.boundary = boundary


class WickImpact(BaseModel):
    is_favorable: bool = False
    is_scoring_penalty: bool = False
    is_temporary_veto: bool = False
    is_structural_invalidation: bool = False
    adverse_wick_ratio: float = 0.0
    favorable_wick_ratio: float = 0.0
    penalty_points: float = 0.0
    description: str = ""


class PriceActionAnalyzer:
    """Detects structural breakouts, ranges, and false moves on index charts."""

    @staticmethod
    def calculate_daily_context(daily_candles: List[UnifiedCandle]) -> Optional[DailyContext]:
        if not daily_candles or len(daily_candles) < 2:
            return None
        # Previous day is the penultimate candle
        prev_day = daily_candles[-2]
        bias = "NEUTRAL"
        if prev_day.close > prev_day.open:
            bias = "BULLISH"
        elif prev_day.close < prev_day.open:
            bias = "BEARISH"

        return DailyContext(
            pdh=prev_day.high,
            pdl=prev_day.low,
            pdc=prev_day.close,
            daily_bias=bias,
        )

    @staticmethod
    def calculate_opening_range(
        candles_1m_or_5m: List[UnifiedCandle], orb_minutes: int = 15
    ) -> OpeningRange:
        """Determines the High and Low of the opening range (typically first 15 mins)."""
        if not candles_1m_or_5m:
            return OpeningRange(range_high=0.0, range_low=0.0, range_span=0.0, is_formed=False)

        # Assuming candles start at market open (09:15 IST)
        # Take bars within the opening duration
        bars_count = max(1, orb_minutes // 5) if candles_1m_or_5m[0].timeframe == "5m" else orb_minutes
        opening_bars = candles_1m_or_5m[:bars_count]

        if len(candles_1m_or_5m) < bars_count:
            # Range still forming
            r_high = max(c.high for c in opening_bars)
            r_low = min(c.low for c in opening_bars)
            return OpeningRange(
                range_high=r_high, range_low=r_low, range_span=r_high - r_low, is_formed=False
            )

        r_high = max(c.high for c in opening_bars)
        r_low = min(c.low for c in opening_bars)
        return OpeningRange(
            range_high=r_high, range_low=r_low, range_span=round(r_high - r_low, 2), is_formed=True
        )

    @staticmethod
    def evaluate_15m_structure(candles_15m: List[UnifiedCandle]) -> str:
        """Evaluates trend structure on 15-minute timeframe."""
        if not candles_15m or len(candles_15m) < 3:
            return "CHOP"

        c1, c2, c3 = candles_15m[-3], candles_15m[-2], candles_15m[-1]
        # Higher Highs & Higher Lows
        if c3.high > c2.high and c3.low > c2.low and c2.close > c1.close:
            return "BULLISH_STRUCTURE"
        # Lower Highs & Lower Lows
        if c3.high < c2.high and c3.low < c2.low and c2.close < c1.close:
            return "BEARISH_STRUCTURE"

        return "RANGE"

    @staticmethod
    def get_wick_ratios(candle: UnifiedCandle) -> Tuple[float, float]:
        """Returns (upper_wick_ratio, lower_wick_ratio) as fraction of total candle range."""
        total_range = candle.high - candle.low
        if total_range <= 0:
            return 0.0, 0.0
        upper_wick = candle.high - max(candle.open, candle.close)
        lower_wick = min(candle.open, candle.close) - candle.low
        return round(upper_wick / total_range, 3), round(lower_wick / total_range, 3)

    @classmethod
    def evaluate_directional_wick(
        cls,
        candle: UnifiedCandle,
        direction: str,  # BULLISH or BEARISH (or BUY_CE or BUY_PE)
        range_boundary: Optional[float] = None,
    ) -> WickImpact:
        """Evaluates wick behavior with strict directionality:
        - For BULLISH setups:
          * Upper wick is ADVERSE (sellers rejecting upside)
          * Lower wick is FAVORABLE (buyers defending lows / hammer)
        - For BEARISH setups:
          * Lower wick is ADVERSE (buyers absorbing breakdown)
          * Upper wick is FAVORABLE (sellers rejecting highs)

        Distinguishes:
        1. Scoring Penalty: 25% <= adverse wick < 40% (deducts confidence score)
        2. Temporary Veto: 40% <= adverse wick < 60% (holds entry qualification, remains developing)
        3. Structural Invalidation: adverse wick >= 60% AND price closed back inside range / past boundary
        """
        upper_ratio, lower_ratio = cls.get_wick_ratios(candle)
        is_bullish = direction in ("BULLISH", "BUY_CE")

        fav_thresh = getattr(settings, "WICK_FAVORABLE_THRESHOLD_PCT", 0.25)
        struct_thresh = getattr(settings, "WICK_STRUCTURAL_INVALIDATION_PCT", 0.60)
        penalty_thresh = getattr(settings, "WICK_SCORING_PENALTY_PCT", 0.25)

        if is_bullish:
            adverse_ratio = upper_ratio
            favorable_ratio = lower_ratio
            is_fav = favorable_ratio >= fav_thresh
            has_broken_back = (range_boundary is not None and candle.close < range_boundary)
        else:
            adverse_ratio = lower_ratio
            favorable_ratio = upper_ratio
            is_fav = favorable_ratio >= fav_thresh
            has_broken_back = (range_boundary is not None and candle.close > range_boundary)

        # 1. Genuine Structural Invalidation
        if adverse_ratio >= struct_thresh and has_broken_back:
            return WickImpact(
                is_favorable=False,
                is_scoring_penalty=True,
                is_temporary_veto=True,
                is_structural_invalidation=True,
                adverse_wick_ratio=adverse_ratio,
                favorable_wick_ratio=favorable_ratio,
                penalty_points=25.0,
                description=f"Structural breakout failure: {adverse_ratio*100:.1f}% adverse wick closed back inside range",
            )

        # 2. Temporary Entry Veto
        if adverse_ratio >= settings.WICK_REJECTION_THRESHOLD_PCT:
            return WickImpact(
                is_favorable=is_fav,
                is_scoring_penalty=True,
                is_temporary_veto=True,
                is_structural_invalidation=False,
                adverse_wick_ratio=adverse_ratio,
                favorable_wick_ratio=favorable_ratio,
                penalty_points=15.0,
                description=f"Elevated adverse rejection wick ({adverse_ratio*100:.1f}%): temporary entry veto",
            )

        # 3. Scoring Penalty
        if adverse_ratio >= penalty_thresh:
            return WickImpact(
                is_favorable=is_fav,
                is_scoring_penalty=True,
                is_temporary_veto=False,
                is_structural_invalidation=False,
                adverse_wick_ratio=adverse_ratio,
                favorable_wick_ratio=favorable_ratio,
                penalty_points=8.0,
                description=f"Moderate adverse wick ({adverse_ratio*100:.1f}%): confidence scoring penalty",
            )

        # Clean / Favorable
        return WickImpact(
            is_favorable=is_fav,
            is_scoring_penalty=False,
            is_temporary_veto=False,
            is_structural_invalidation=False,
            adverse_wick_ratio=adverse_ratio,
            favorable_wick_ratio=favorable_ratio,
            penalty_points=0.0,
            description="Clean price acceptance; no adverse wick rejection",
        )

    @classmethod
    def check_wick_rejection(cls, candle: UnifiedCandle, direction: str) -> bool:
        """Returns True if the candle has an adverse rejection wick meeting temporary veto or invalidation."""
        eval_res = cls.evaluate_directional_wick(candle, direction)
        return eval_res.is_temporary_veto or eval_res.is_structural_invalidation

    @classmethod
    def check_structural_wick_invalidation(
        cls, candle: UnifiedCandle, direction: str, range_boundary: Optional[float] = None
    ) -> bool:
        """Returns True ONLY if there is genuine structural invalidation (adverse wick >= 60% AND closed inside range)."""
        eval_res = cls.evaluate_directional_wick(candle, direction, range_boundary)
        return eval_res.is_structural_invalidation

    @classmethod
    def detect_setup(
        cls,
        candles_5m: List[UnifiedCandle],
        orb: OpeningRange,
        daily: Optional[DailyContext],
        vwap_value: Optional[float],
    ) -> Tuple[str, List[str]]:
        """Detects 5m breakout, breakdown, retest, early watch momentum, or no-trade conditions."""
        reasons: List[str] = []
        if not orb.is_formed or not candles_5m or len(candles_5m) < 4:
            return SetupResult(SetupType.NONE, ["Opening range not yet established"], None)

        current = candles_5m[-1]
        prev = candles_5m[-2]

        recent_bars = candles_5m[-6:-1] if len(candles_5m) >= 6 else candles_5m[:-1]
        swing_high = max(b.high for b in recent_bars) if recent_bars else orb.range_high
        swing_low = min(b.low for b in recent_bars) if recent_bars else orb.range_low

        # Check for Failed Breakout above ORB High
        if current.high > orb.range_high and current.close <= orb.range_high:
            wick_eval = cls.evaluate_directional_wick(current, "BULLISH", orb.range_high)
            if wick_eval.is_structural_invalidation:
                return SetupResult(
                    SetupType.FAILED_BREAKOUT,
                    [f"Breakout rejected: High ({current.high}) tested above ORB High ({orb.range_high}) but closed back inside ({current.close}) with {wick_eval.adverse_wick_ratio*100:.1f}% adverse wick"],
                    orb.range_high
                )

        # 1. Bullish Breakout Check
        if current.close > orb.range_high:
            # Check if this is a fresh ORB breakout (at least one of recent bars was inside/near range)
            is_fresh_orb_breakout = any(b.close <= orb.range_high for b in candles_5m[-4:-1])
            if is_fresh_orb_breakout:
                wick_eval = cls.evaluate_directional_wick(current, "BULLISH", orb.range_high)
                if wick_eval.is_structural_invalidation:
                    return SetupResult(SetupType.FAILED_BREAKOUT, [wick_eval.description], orb.range_high)

                if wick_eval.is_temporary_veto:
                    return SetupResult(
                        SetupType.EARLY_BULLISH_MOMENTUM,
                        [f"5m closed above ORB High ({orb.range_high}), but under temporary entry veto: {wick_eval.description}"],
                        orb.range_high
                    )

                if vwap_value and current.close < vwap_value:
                    return SetupResult(
                        SetupType.EARLY_BULLISH_MOMENTUM,
                        [f"5m candle accepted above ORB High ({orb.range_high}), but below VWAP ({vwap_value:.1f}) - Developing"],
                        orb.range_high
                    )

                reasons.append(f"Confirmed 5-min candle closed above ORB High ({orb.range_high})")
                if vwap_value and current.close > vwap_value:
                    reasons.append(f"Price ({current.close}) firmly above VWAP ({vwap_value:.1f})")
                if daily and current.close > daily.pdh:
                    reasons.append(f"Breakout above Previous Day High ({daily.pdh})")

                return SetupResult(SetupType.BULLISH_BREAKOUT, reasons, orb.range_high)

            # Not a fresh ORB breakout; price is established in trend above ORB High!
            # Evaluate fresh Swing Breakout of recent consolidation
            if current.close > swing_high and (not vwap_value or current.close > vwap_value):
                wick_eval = cls.evaluate_directional_wick(current, "BULLISH", swing_high)
                if not wick_eval.is_structural_invalidation:
                    if wick_eval.is_temporary_veto:
                        return SetupResult(
                            SetupType.EARLY_BULLISH_MOMENTUM,
                            [f"Swing high breach with temporary entry veto: {wick_eval.description}"],
                            swing_high
                        )
                    reasons.append(f"Bullish momentum: 5m candle accepted above recent swing high ({swing_high:.1f})")
                    if vwap_value and current.close > vwap_value:
                        reasons.append(f"Holding firmly above intraday VWAP ({vwap_value:.1f})")
                    if len(candles_5m) >= 3 and current.high > prev.high and current.low > prev.low:
                        reasons.append("Higher High / Higher Low sequence confirmed")
                    return SetupResult(SetupType.BULLISH_BREAKOUT, reasons, swing_high)

            # Retest of VWAP support
            if vwap_value and current.low <= vwap_value * 1.002 and current.close > vwap_value and current.close > current.open:
                return SetupResult(
                    SetupType.BULLISH_RETEST,
                    [f"Bullish pullback: Rebounded firmly off VWAP support ({vwap_value:.1f})"],
                    vwap_value
                )

            # Consolidating above ORB High within recent swing range
            return SetupResult(
                SetupType.NONE,
                [f"Price ({current.close:.1f}) consolidating above ORB High within swing range ({swing_low:.1f} - {swing_high:.1f})"],
                None
            )

        # Check for Failed Breakdown below ORB Low
        if current.low < orb.range_low and current.close >= orb.range_low:
            wick_eval = cls.evaluate_directional_wick(current, "BEARISH", orb.range_low)
            if wick_eval.is_structural_invalidation:
                return SetupResult(
                    SetupType.FAILED_BREAKOUT,
                    [f"Breakdown rejected: Low ({current.low}) tested below ORB Low ({orb.range_low}) but closed back inside ({current.close}) with {wick_eval.adverse_wick_ratio*100:.1f}% adverse wick"],
                    orb.range_low
                )

        # 2. Bearish Breakdown Check
        if current.close < orb.range_low:
            # Check if this is a fresh ORB breakdown
            is_fresh_orb_breakdown = any(b.close >= orb.range_low for b in candles_5m[-4:-1])
            if is_fresh_orb_breakdown:
                wick_eval = cls.evaluate_directional_wick(current, "BEARISH", orb.range_low)
                if wick_eval.is_structural_invalidation:
                    return SetupResult(SetupType.FAILED_BREAKOUT, [wick_eval.description], orb.range_low)

                if wick_eval.is_temporary_veto:
                    return SetupResult(
                        SetupType.EARLY_BEARISH_MOMENTUM,
                        [f"5m closed below ORB Low ({orb.range_low}), but under temporary entry veto: {wick_eval.description}"],
                        orb.range_low
                    )

                if vwap_value and current.close > vwap_value:
                    return SetupResult(
                        SetupType.EARLY_BEARISH_MOMENTUM,
                        [f"5m candle accepted below ORB Low ({orb.range_low}), but above VWAP ({vwap_value:.1f}) - Developing"],
                        orb.range_low
                    )

                reasons.append(f"Confirmed 5-min candle closed below ORB Low ({orb.range_low})")
                if vwap_value and current.close < vwap_value:
                    reasons.append(f"Price ({current.close}) firmly below VWAP ({vwap_value:.1f})")
                if daily and current.close < daily.pdl:
                    reasons.append(f"Breakdown below Previous Day Low ({daily.pdl})")

                return SetupResult(SetupType.BEARISH_BREAKDOWN, reasons, orb.range_low)

            # Not a fresh ORB breakdown; price is established in trend below ORB Low!
            # Evaluate fresh Swing Breakdown of recent consolidation
            if current.close < swing_low and (not vwap_value or current.close < vwap_value):
                wick_eval = cls.evaluate_directional_wick(current, "BEARISH", swing_low)
                if not wick_eval.is_structural_invalidation:
                    if wick_eval.is_temporary_veto:
                        return SetupResult(
                            SetupType.EARLY_BEARISH_MOMENTUM,
                            [f"Swing low breach with temporary entry veto: {wick_eval.description}"],
                            swing_low
                        )
                    reasons.append(f"Bearish momentum: 5m candle accepted below recent swing low ({swing_low:.1f})")
                    if vwap_value and current.close < vwap_value:
                        reasons.append(f"Holding firmly below intraday VWAP ({vwap_value:.1f})")
                    if len(candles_5m) >= 3 and current.high < prev.high and current.low < prev.low:
                        reasons.append("Lower High / Lower Low sequence confirmed")
                    return SetupResult(SetupType.BEARISH_BREAKDOWN, reasons, swing_low)

            # Retest of VWAP resistance
            if vwap_value and current.high >= vwap_value * 0.998 and current.close < vwap_value and current.close < current.open:
                return SetupResult(
                    SetupType.BEARISH_RETEST,
                    [f"Bearish pullback: Rejected firmly off VWAP resistance ({vwap_value:.1f})"],
                    vwap_value
                )

            # Consolidating below ORB Low within recent swing range
            return SetupResult(
                SetupType.NONE,
                [f"Price ({current.close:.1f}) consolidating below ORB Low within swing range ({swing_low:.1f} - {swing_high:.1f})"],
                None
            )

        # 3. Proximity to Range Boundary / Early Developing Watch (Inside ORB)
        if orb.range_high > 0 and (orb.range_high - current.close) / orb.range_high < 0.0020 and current.close > current.open:
            return SetupResult(
                SetupType.EARLY_BULLISH_MOMENTUM,
                [f"Early momentum: Price ({current.close}) pressing against ORB High ({orb.range_high}) boundary"],
                orb.range_high
            )
        if orb.range_low > 0 and (current.close - orb.range_low) / orb.range_low < 0.0020 and current.close < current.open:
            return SetupResult(
                SetupType.EARLY_BEARISH_MOMENTUM,
                [f"Early momentum: Price ({current.close}) pressing against ORB Low ({orb.range_low}) boundary"],
                orb.range_low
            )

        # 4. Inside Range / Consolidation
        return SetupResult(SetupType.NONE, ["Price consolidating within opening range"], None)

    @classmethod
    def evaluate_entry_trigger(
        cls,
        mode: str,  # CONSERVATIVE, BALANCED, FAST
        setup_type: str,
        direction: str,  # BUY_CE or BUY_PE
        underlying_spot: float,
        orb: OpeningRange,
        candles_5m: List[UnifiedCandle],
        candles_1m: Optional[List[UnifiedCandle]] = None,
        quote_staleness_sec: float = 0.0,
        spread_pct: float = 0.0,
        max_spread_pct: float = 2.0,
        false_move_score: float = 0.0,
        tick_delta: float = 0.0,
    ) -> Tuple[bool, str, List[str]]:
        """Evaluates entry trigger based on active trigger mode:
        1. CONSERVATIVE: Confirmed completed 5-minute breakout candle close.
        2. BALANCED: Validated 1-minute breakout with 5-minute trend context.
        3. FAST: Validated intrabar / tick-level breakout with stricter freshness, spread, and persistence.
        """
        reasons: List[str] = []
        is_bullish = direction in ("BUY_CE", "BULLISH")
        mode_upper = (mode or "CONSERVATIVE").upper()

        # Mode 1: CONSERVATIVE (5-minute confirmed close)
        if mode_upper == "CONSERVATIVE":
            is_confirmed = setup_type in (SetupType.BULLISH_BREAKOUT, SetupType.BEARISH_BREAKDOWN)
            if is_confirmed:
                reasons.append("Conservative Trigger: Confirmed 5-minute candle closed beyond range boundary")
                return True, "CONSERVATIVE", reasons
            return False, "CONSERVATIVE", ["Awaiting confirmed 5-minute candle close"]

        # Mode 2: BALANCED (1-minute breakout with 5-minute context)
        elif mode_upper == "BALANCED":
            context_ok = setup_type in (
                SetupType.BULLISH_BREAKOUT,
                SetupType.BEARISH_BREAKDOWN,
                SetupType.EARLY_BULLISH_MOMENTUM,
                SetupType.EARLY_BEARISH_MOMENTUM,
                SetupType.BULLISH_RETEST,
                SetupType.BEARISH_RETEST,
            )
            if not context_ok:
                return False, "BALANCED", ["5-minute context does not show directional momentum"]

            if candles_1m and len(candles_1m) >= 2:
                last_1m = candles_1m[-1]
                body = abs(last_1m.close - last_1m.open)
                rng = max(last_1m.high - last_1m.low, 0.01)
                body_ratio = body / rng

                if is_bullish:
                    b_high = orb.range_high
                    if last_1m.close > b_high and body_ratio >= 0.40:
                        reasons.append(f"Balanced Trigger: Validated 1-minute bar closed above boundary ({last_1m.close} > {b_high}) with 5m context")
                        return True, "BALANCED", reasons
                else:
                    b_low = orb.range_low
                    if last_1m.close < b_low and body_ratio >= 0.40:
                        reasons.append(f"Balanced Trigger: Validated 1-minute bar closed below boundary ({last_1m.close} < {b_low}) with 5m context")
                        return True, "BALANCED", reasons
                return False, "BALANCED", ["1-minute bar has not yet validated boundary breakout"]
            else:
                if setup_type in (SetupType.BULLISH_BREAKOUT, SetupType.BEARISH_BREAKDOWN):
                    reasons.append("Balanced Trigger: Confirmed 5-minute close (1m fallback)")
                    return True, "BALANCED", reasons
                return False, "BALANCED", ["1-minute bar data buffer warming up"]

        # Mode 3: FAST (Validated intrabar / tick-level trigger)
        elif mode_upper == "FAST":
            if quote_staleness_sec > settings.FAST_MODE_MAX_STALENESS_SECONDS:
                return False, "FAST", [f"Fast Mode rejected: quote age {quote_staleness_sec:.1f}s > {settings.FAST_MODE_MAX_STALENESS_SECONDS}s limit"]

            fast_spread_limit = max_spread_pct * settings.FAST_MODE_MAX_SPREAD_RATIO
            if spread_pct > fast_spread_limit:
                return False, "FAST", [f"Fast Mode rejected: spread {spread_pct:.2f}% > fast limit {fast_spread_limit:.2f}%"]

            if false_move_score > 25.0:
                return False, "FAST", [f"Fast Mode rejected: false move risk score {false_move_score:.1f} > 25.0 limit"]

            if is_bullish and tick_delta < 0:
                return False, "FAST", ["Fast Mode rejected: negative tick delta divergence on bullish trigger"]
            if (not is_bullish) and tick_delta > 0:
                return False, "FAST", ["Fast Mode rejected: positive tick delta divergence on bearish trigger"]

            if is_bullish and underlying_spot > orb.range_high:
                reasons.append(f"Fast Trigger: Live spot ({underlying_spot}) breached boundary ({orb.range_high}) with strict execution gates passed")
                return True, "FAST", reasons
            elif (not is_bullish) and underlying_spot < orb.range_low:
                reasons.append(f"Fast Trigger: Live spot ({underlying_spot}) breached boundary ({orb.range_low}) with strict execution gates passed")
                return True, "FAST", reasons

            return False, "FAST", ["Intrabar price has not breached range boundary"]

        return False, mode_upper, [f"Unknown entry trigger mode: {mode}"]
