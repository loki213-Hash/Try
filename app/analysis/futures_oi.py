"""Futures and Options Open Interest (OI) positioning analyzer.

Tracks:
- Put-Call Ratio (PCR) by Open Interest and Volume
- Price-OI relationship: Long Buildup, Short Buildup, Short Covering, Long Unwinding
- Options institutional positioning
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple, Any
from collections import deque, defaultdict
from pydantic import BaseModel, Field
from config import settings


class OIInterpretation(BaseModel):
    regime: str  # LONG_BUILDUP, SHORT_BUILDUP, SHORT_COVERING, LONG_UNWINDING, NEUTRAL
    pcr: float
    is_bullish_aligned: bool
    is_bearish_aligned: bool
    summary: str
    oi_status: str = "AVAILABLE"  # AVAILABLE, UNAVAILABLE, STABLE
    delta_3m_pct: float = 0.0
    delta_15m_pct: float = 0.0


class OptionActivityClassification(BaseModel):
    activity_type: str  # CALL_BUYING, CALL_WRITING, PUT_BUYING, PUT_WRITING, SHORT_COVERING, LONG_UNWINDING, NEUTRAL
    interpretation: str
    is_bullish_demand: bool
    is_bearish_demand: bool
    is_resistance_buildup: bool
    is_support_floor: bool
    confidence_tag: str


class ScalpOrderFlowMetrics(BaseModel):
    volume_delta: float
    cvd: float
    is_aggressive_buying: bool
    is_aggressive_selling: bool
    is_absorption_trap: bool
    interpretation: str
    data_source: str = "ESTIMATED_TICK_DELTA"  # ESTIMATED_TICK_DELTA vs RESTING_DEPTH_IMBALANCE
    tick_delta_ratio: float = 0.0
    resting_depth_ratio: float = 0.0
    absorption_confidence: float = 0.0


class FalseMoveRiskAssessment(BaseModel):
    risk_level: str = "LOW"  # LOW, MODERATE, HIGH
    risk_score: float = 0.0  # 0.0 to 100.0
    factors: List[str] = Field(default_factory=list)
    is_veto_for_entry: bool = False
    summary: str = "Low false-move risk; setup geometry consistent"


class FuturesOIAnalyzer:
    """Classifies positioning dynamics across index futures and options chains."""

    def __init__(self):
        # instrument_id -> (last_price, last_direction: +1 or -1)
        self._tick_history: Dict[str, Tuple[float, int]] = {}
        # instrument_id -> deque of (timestamp, oi) (buffer for 15-30m at 2s interval)
        self._oi_snapshots: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1500))

    def record_tick(
        self, instrument_id: str, price: float, volume: int = 1
    ) -> float:
        """Applies Tick-and-Quote rule on consecutive trades to estimate executed aggressor delta."""
        if price <= 0:
            return 0.0

        if instrument_id in self._tick_history:
            prev_price, prev_dir = self._tick_history[instrument_id]
            if price > prev_price:
                direction = 1
            elif price < prev_price:
                direction = -1
            else:
                direction = prev_dir  # Zero-tick: carry previous aggressor direction
        else:
            direction = 1  # Default initial assumption

        self._tick_history[instrument_id] = (price, direction)
        return float(volume * direction)

    def record_oi_snapshot(
        self, instrument_id: str, oi: int, timestamp: Optional[datetime] = None
    ) -> None:
        """Maintains a rolling intraday time-series buffer of Open Interest snapshots."""
        if oi <= 0:
            return
        ts = timestamp or datetime.now(timezone.utc)
        self._oi_snapshots[instrument_id].append((ts, oi))

    def calculate_rolling_oi_velocity(
        self, instrument_id: str, current_oi: int, timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Calculates rolling 3m and 15m intraday OI velocity with independent validation of:
        1. Timestamps & ordering
        2. Session boundaries (same calendar date / session)
        3. Latest-data freshness (flagged STALE if > 360s)
        4. Candidate-window boundaries ([90s, 300s] for 3m, [600s, 1200s] for 15m)
        Calculates changes ONLY from valid timestamped observations.
        """
        raw_ts = timestamp or datetime.now(timezone.utc)
        ts = raw_ts if raw_ts.tzinfo is not None else raw_ts.replace(tzinfo=timezone.utc)

        snapshots = self._oi_snapshots.get(instrument_id)
        if not snapshots or len(snapshots) < 1 or current_oi <= 0:
            return {
                "status": "MISSING",
                "delta_3m_pct": 0.0,
                "delta_15m_pct": 0.0,
                "has_valid_3m": False,
                "has_valid_15m": False,
                "is_reliable": False,
                "staleness_seconds": 0.0,
                "summary": "Intraday OI history missing: awaiting initial exchange dissemination",
            }

        # 1. Filter by Session Boundary (reject prior dates or negative/future age)
        session_snapshots: List[Tuple[datetime, int]] = []
        for s_ts, s_oi in snapshots:
            norm_s_ts = s_ts if s_ts.tzinfo is not None else s_ts.replace(tzinfo=timezone.utc)
            # Ensure same trading day and chronological validity
            if norm_s_ts.date() == ts.date() and norm_s_ts <= ts and s_oi > 0:
                session_snapshots.append((norm_s_ts, s_oi))

        if not session_snapshots:
            return {
                "status": "OUT_OF_SESSION",
                "delta_3m_pct": 0.0,
                "delta_15m_pct": 0.0,
                "has_valid_3m": False,
                "has_valid_15m": False,
                "is_reliable": False,
                "staleness_seconds": 0.0,
                "summary": "OI observations outside current trading session boundary",
            }

        # Sort chronologically to validate ordering and find true latest observation
        session_snapshots.sort(key=lambda x: x[0])

        # 2. Independent Latest-Data Freshness Check
        latest_ts = session_snapshots[-1][0]
        staleness_sec = (ts - latest_ts).total_seconds()
        is_fresh = staleness_sec <= 360.0

        if not is_fresh:
            return {
                "status": "STALE",
                "delta_3m_pct": 0.0,
                "delta_15m_pct": 0.0,
                "has_valid_3m": False,
                "has_valid_15m": False,
                "is_reliable": False,
                "staleness_seconds": round(staleness_sec, 1),
                "summary": f"OI feed stale: last observation was {int(staleness_sec)}s ago (>360s limit)",
            }

        # 3. Independent Candidate-Window Boundary Verification
        # 3-minute window: target 180s, strictly bounded within [90s, 300s]
        # 15-minute window: target 900s, strictly bounded within [600s, 1200s]
        TARGET_3M = 180.0
        MIN_3M, MAX_3M = 90.0, 300.0

        TARGET_15M = 900.0
        MIN_15M, MAX_15M = 600.0, 1200.0

        best_3m_candidate: Optional[Tuple[float, int, float]] = None  # (diff_to_target, oi, actual_age)
        best_15m_candidate: Optional[Tuple[float, int, float]] = None

        for s_ts, s_oi in session_snapshots:
            age_sec = (ts - s_ts).total_seconds()
            if age_sec <= 0:
                continue

            # Candidate boundary check for 3m
            if MIN_3M <= age_sec <= MAX_3M:
                diff_3m = abs(age_sec - TARGET_3M)
                if best_3m_candidate is None or diff_3m < best_3m_candidate[0]:
                    best_3m_candidate = (diff_3m, s_oi, age_sec)

            # Candidate boundary check for 15m
            if MIN_15M <= age_sec <= MAX_15M:
                diff_15m = abs(age_sec - TARGET_15M)
                if best_15m_candidate is None or diff_15m < best_15m_candidate[0]:
                    best_15m_candidate = (diff_15m, s_oi, age_sec)

        delta_3m = 0.0
        has_3m = False
        actual_3m_age = 0.0
        if best_3m_candidate is not None:
            _, oi_3m_val, actual_3m_age = best_3m_candidate
            delta_3m = round(((current_oi - oi_3m_val) / max(oi_3m_val, 1)) * 100.0, 2)
            has_3m = True

        delta_15m = 0.0
        has_15m = False
        actual_15m_age = 0.0
        if best_15m_candidate is not None:
            _, oi_15m_val, actual_15m_age = best_15m_candidate
            delta_15m = round(((current_oi - oi_15m_val) / max(oi_15m_val, 1)) * 100.0, 2)
            has_15m = True

        if not has_3m and not has_15m:
            return {
                "status": "MISSING",
                "delta_3m_pct": 0.0,
                "delta_15m_pct": 0.0,
                "has_valid_3m": False,
                "has_valid_15m": False,
                "is_reliable": False,
                "staleness_seconds": round(staleness_sec, 1),
                "obs_count": len(session_snapshots),
                "summary": "No historical OI observations currently within 3m [90-300s] or 15m [600-1200s] windows",
            }

        is_reliable = has_3m
        status = "AVAILABLE" if is_reliable else "PARTIAL"

        summary_parts = []
        if has_3m:
            summary_parts.append(f"3m {delta_3m:+.1f}% ({int(actual_3m_age)}s ago)")
        if has_15m:
            summary_parts.append(f"15m {delta_15m:+.1f}% ({int(actual_15m_age)}s ago)")

        return {
            "status": status,
            "delta_3m_pct": delta_3m,
            "delta_15m_pct": delta_15m,
            "has_valid_3m": has_3m,
            "has_valid_15m": has_15m,
            "is_reliable": is_reliable,
            "staleness_seconds": round(staleness_sec, 1),
            "obs_count": len(session_snapshots),
            "summary": f"Intraday OI shift: {', '.join(summary_parts)}",
        }

    def evaluate_false_move_risk(
        self,
        price_change_pct: float,
        rvol: float,
        upper_wick_pct: float = 0.0,
        lower_wick_pct: float = 0.0,
        volume_delta: float = 0.0,
        direction: str = "BUY_CE",
    ) -> FalseMoveRiskAssessment:
        """Continuous False-Move & Absorption Risk Model.
        Produces transparent score and classifies risk as LOW, MODERATE, or HIGH.
        """
        risk_score = 0.0
        factors: List[str] = []

        # 1. Volume absorption check (High RVOL without price progress)
        is_absorbed = (rvol >= settings.ABSORPTION_RVOL_THRESHOLD) and (
            abs(price_change_pct) < settings.ABSORPTION_PRICE_THRESHOLD_PCT
        )
        if is_absorbed:
            risk_score += 45.0
            factors.append(
                f"Volume Absorption Trap: RVOL {rvol:.1f}x with negligible price delta ({price_change_pct:+.2f}%)"
            )

        # 2. Wick Rejection check (Upper wick on bullish, Lower wick on bearish)
        is_bullish = direction in ("BUY_CE", "BULLISH")
        if is_bullish and upper_wick_pct >= settings.WICK_REJECTION_THRESHOLD_PCT:
            wick_pts = min(40.0, round(upper_wick_pct * 80.0, 1))
            risk_score += wick_pts
            factors.append(f"Bullish Upper Wick Rejection ({upper_wick_pct * 100:.1f}% of bar)")
        elif (not is_bullish) and lower_wick_pct >= settings.WICK_REJECTION_THRESHOLD_PCT:
            wick_pts = min(40.0, round(lower_wick_pct * 80.0, 1))
            risk_score += wick_pts
            factors.append(f"Bearish Lower Wick Rejection ({lower_wick_pct * 100:.1f}% of bar)")

        # 3. Order Flow Delta Divergence
        if is_bullish and volume_delta < -25.0:
            risk_score += 30.0
            factors.append(f"Delta Divergence: Bullish move with negative seller-initiated delta ({volume_delta:+.1f}%)")
        elif (not is_bullish) and volume_delta > 25.0:
            risk_score += 30.0
            factors.append(f"Delta Divergence: Bearish move with positive buyer-initiated delta ({volume_delta:+.1f}%)")

        # 4. Momentum Deterioration
        if rvol >= 1.4 and abs(price_change_pct) < 0.04 and not is_absorbed:
            risk_score += 20.0
            factors.append("Momentum stall: Elevated activity without directional continuation")

        risk_score = min(100.0, round(risk_score, 1))
        high_thresh = getattr(settings, "FALSE_MOVE_HIGH_RISK_THRESHOLD", 55.0)
        mod_thresh = getattr(settings, "FALSE_MOVE_MODERATE_RISK_THRESHOLD", 25.0)
        if risk_score > high_thresh:
            risk_level = "HIGH"
            is_veto = True
            summary = "🚨 HIGH FALSE-MOVE RISK: Elevated probability of bull/bear trap or institutional absorption"
        elif risk_score >= mod_thresh:
            risk_level = "MODERATE"
            is_veto = False
            summary = "⚠️ MODERATE RISK: Developing setup with minor conflicting order flow or wick shadow"
        else:
            risk_level = "LOW"
            is_veto = False
            summary = "🛡️ LOW RISK: Clean breakout structure with confirming participation"

        return FalseMoveRiskAssessment(
            risk_level=risk_level,
            risk_score=risk_score,
            factors=factors,
            is_veto_for_entry=is_veto,
            summary=summary,
        )

    @staticmethod
    def analyze_buildup(
        price_change_pct: float,
        oi_change_pct: float,
        pcr: float,
        oi_status: str = "AVAILABLE",
        delta_3m: float = 0.0,
        delta_15m: float = 0.0,
    ) -> OIInterpretation:
        """Determines price-OI buildup regime and whether it aligns with setup."""
        if oi_status == "UNAVAILABLE":
            regime = "NEUTRAL"
            summary = "OI data not yet updated by exchange; relying on price & order flow"
            is_bullish = pcr >= 0.95
            is_bearish = pcr <= 0.85
        elif price_change_pct > 0 and oi_change_pct > 0:
            regime = "LONG_BUILDUP"
            summary = "Fresh longs entering with rising Open Interest"
            is_bullish = pcr >= 0.90
            is_bearish = False
        elif price_change_pct < 0 and oi_change_pct > 0:
            regime = "SHORT_BUILDUP"
            summary = "Aggressive short positions being created"
            is_bullish = False
            is_bearish = pcr <= 0.90
        elif price_change_pct > 0 and oi_change_pct < 0:
            regime = "SHORT_COVERING"
            summary = "Shorts closing out positions, lifting prices"
            is_bullish = pcr >= 0.90
            is_bearish = False
        elif price_change_pct < 0 and oi_change_pct < 0:
            regime = "LONG_UNWINDING"
            summary = "Longs liquidating positions, pressuring prices"
            is_bullish = False
            is_bearish = pcr <= 0.90
        else:
            regime = "NEUTRAL"
            summary = "OI consolidating without clear directional pressure"
            is_bullish = pcr >= 0.95
            is_bearish = pcr <= 0.85

        return OIInterpretation(
            regime=regime,
            pcr=round(pcr, 2),
            is_bullish_aligned=is_bullish,
            is_bearish_aligned=is_bearish,
            summary=summary,
            oi_status=oi_status,
            delta_3m_pct=delta_3m,
            delta_15m_pct=delta_15m,
        )

    @staticmethod
    def classify_option_activity(
        option_type: str,  # CE or PE
        premium_change_pct: float,
        oi_change_pct: float,
        underlying_trend: str = "NEUTRAL",  # BULLISH, BEARISH, NEUTRAL
    ) -> OptionActivityClassification:
        """Distinguishes aggressive Option Buying from institutional Option Writing.
        High Call volume does not mean bullish unless accompanied by rising Call premium and positive delta.
        """
        opt = option_type.upper()
        if opt == "CE":
            if premium_change_pct > 0 and oi_change_pct > 0:
                return OptionActivityClassification(
                    activity_type="CALL_BUYING",
                    interpretation="Aggressive Call Buying (Long Build-up): Buyers paying ask, anticipating upward explosion",
                    is_bullish_demand=True,
                    is_bearish_demand=False,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="STRONG_BULLISH",
                )
            elif premium_change_pct < 0 and oi_change_pct > 0:
                return OptionActivityClassification(
                    activity_type="CALL_WRITING",
                    interpretation="Call Writing (Short Build-up): Institutional sellers capping upside; resistance wall forming",
                    is_bullish_demand=False,
                    is_bearish_demand=True,
                    is_resistance_buildup=True,
                    is_support_floor=False,
                    confidence_tag="STRONG_BEARISH_RESISTANCE",
                )
            elif premium_change_pct > 0 and oi_change_pct < 0:
                return OptionActivityClassification(
                    activity_type="CALL_SHORT_COVERING",
                    interpretation="Call Short Covering: Trapped call sellers covering positions, fueling bullish rally",
                    is_bullish_demand=True,
                    is_bearish_demand=False,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="BULLISH_SQUEEZE",
                )
            elif premium_change_pct < 0 and oi_change_pct < 0:
                return OptionActivityClassification(
                    activity_type="CALL_LONG_UNWINDING",
                    interpretation="Call Long Unwinding: Buyers liquidating calls, dampening bullish momentum",
                    is_bullish_demand=False,
                    is_bearish_demand=True,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="BEARISH_EXHAUSTION",
                )
        elif opt == "PE":
            if premium_change_pct > 0 and oi_change_pct > 0:
                return OptionActivityClassification(
                    activity_type="PUT_BUYING",
                    interpretation="Aggressive Put Buying (Long Build-up): Heavy downside demand; traders loading Puts",
                    is_bullish_demand=False,
                    is_bearish_demand=True,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="STRONG_BEARISH",
                )
            elif premium_change_pct < 0 and oi_change_pct > 0:
                return OptionActivityClassification(
                    activity_type="PUT_WRITING",
                    interpretation="Put Writing (Short Build-up): Institutional writers creating strong support floor below spot",
                    is_bullish_demand=True,
                    is_bearish_demand=False,
                    is_resistance_buildup=False,
                    is_support_floor=True,
                    confidence_tag="STRONG_BULLISH_SUPPORT",
                )
            elif premium_change_pct > 0 and oi_change_pct < 0:
                return OptionActivityClassification(
                    activity_type="PUT_SHORT_COVERING",
                    interpretation="Put Short Covering: Trapped put writers fleeing, intensifying sharp downside move",
                    is_bullish_demand=False,
                    is_bearish_demand=True,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="BEARISH_SQUEEZE",
                )
            elif premium_change_pct < 0 and oi_change_pct < 0:
                return OptionActivityClassification(
                    activity_type="PUT_LONG_UNWINDING",
                    interpretation="Put Long Unwinding: Downside panic abating as put buyers cash in; bounce likely",
                    is_bullish_demand=True,
                    is_bearish_demand=False,
                    is_resistance_buildup=False,
                    is_support_floor=False,
                    confidence_tag="BULLISH_RELIEF",
                )

        return OptionActivityClassification(
            activity_type="NEUTRAL",
            interpretation="Consolidating option positioning without decisive buying or writing pressure",
            is_bullish_demand=False,
            is_bearish_demand=False,
            is_resistance_buildup=False,
            is_support_floor=False,
            confidence_tag="NEUTRAL",
        )

    @classmethod
    def evaluate_order_flow_delta(
        cls,
        traded_volume: int,
        bid_volume: int,
        ask_volume: int,
        price_change_pct: float,
        rvol: float = 1.0,
        estimated_tick_delta: Optional[float] = None,
        data_source: str = "ESTIMATED_TICK_DELTA",
    ) -> ScalpOrderFlowMetrics:
        """Evaluates aggressor buying (at ask) vs selling (at bid) and flags absorption traps.
        Distinguishes displayed order-book depth imbalance from executed trade tick delta.
        """
        # Depth imbalance calculation: positive indicates buyer/bid support, negative indicates seller/ask pressure
        delta_depth = float(bid_volume - ask_volume)
        total_depth = max(float(ask_volume + bid_volume), 1.0)
        norm_depth = delta_depth / total_depth

        # If tick delta is provided, blend it; otherwise use tick rule approximation
        if estimated_tick_delta is not None:
            norm_delta = max(-1.0, min(1.0, estimated_tick_delta / max(float(traded_volume), 1.0)))
            delta_val = estimated_tick_delta
            effective_source = data_source
        else:
            norm_delta = norm_depth
            delta_val = delta_depth
            effective_source = "RESTING_DEPTH_IMBALANCE" if abs(norm_depth) > 0.01 else "ESTIMATED_TICK_DELTA"

        # Absorption trap: High volume (rvol >= threshold) with negligible price change (< threshold)
        is_absorption = (rvol >= settings.ABSORPTION_RVOL_THRESHOLD) and (
            abs(price_change_pct) < settings.ABSORPTION_PRICE_THRESHOLD_PCT
        )

        is_aggr_buy = (norm_delta >= 0.15) and not is_absorption
        is_aggr_sell = (norm_delta <= -0.15) and not is_absorption

        if is_absorption:
            interp = "⚠️ ABSORPTION DETECTED: Heavy volume spike without price progress; resting limit orders absorbing flow (reversal warning)"
        elif is_aggr_buy:
            interp = f"⚡ AGGRESSOR BUYING [{effective_source}]: Dominant buyer-initiated flow lifting offer"
        elif is_aggr_sell:
            interp = f"⚡ AGGRESSOR SELLING [{effective_source}]: Dominant seller-initiated flow hitting bid"
        else:
            interp = f"Balanced order flow between buyers and sellers ({effective_source})"

        return ScalpOrderFlowMetrics(
            volume_delta=round(norm_delta * 100.0, 1),
            cvd=round(delta_val, 0),
            is_aggressive_buying=is_aggr_buy,
            is_aggressive_selling=is_aggr_sell,
            is_absorption_trap=is_absorption,
            interpretation=interp,
            data_source=effective_source,
            tick_delta_ratio=round(norm_delta, 3),
            resting_depth_ratio=round(norm_depth, 3),
            absorption_confidence=85.0 if is_absorption else 10.0,
        )


futures_oi_analyzer = FuturesOIAnalyzer()
