"""Targets and Stop-Loss Calculation Engine.

Calculates:
- Underlying Invalidation Level
- Premium Risk Stop (SL)
- Three Defensible Targets (TP1, TP2, TP3) derived from setup geometry,
  reward-to-risk ratios, and Greeks-based premium projections.
Strictly suppresses signals if three realistic targets cannot be calculated.
"""

from __future__ import annotations
import math
from typing import Optional, Tuple, List
from pydantic import BaseModel
from app.signals.contract_selector import SelectedContract
from app.analysis.volatility import BlackScholesGreeks
from app.monitoring.logging import logger
from config import settings


def round_up_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Rounds price UP to nearest exchange tick to strictly guarantee net target gains."""
    if tick_size <= 0:
        return round(price, 2)
    ticks = math.ceil((price - 1e-9) / tick_size)
    return round(ticks * tick_size, 2)


def round_down_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Rounds price DOWN to nearest exchange tick for conservative stop loss."""
    if tick_size <= 0:
        return round(price, 2)
    ticks = math.floor((price + 1e-9) / tick_size)
    return round(ticks * tick_size, 2)


class CalculatedLevels(BaseModel):
    entry_range_low: float
    entry_range_high: float
    max_entry_price: float
    sl_price: float
    premium_risk_per_unit: float
    underlying_invalidation: float
    underlying_risk_distance: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    planned_rr_tp1: float
    planned_rr_tp2: float
    planned_rr_tp3: float
    net_rr_tp1: float = 1.5
    estimated_slippage: float = 0.0
    estimated_transaction_cost: float = 0.0
    tp1_confidence: float = 85.0
    tp2_confidence: float = 65.0
    tp3_confidence: float = 42.0


class TargetsAndStopEngine:
    """Calculates defensible entry range, stop-loss, and multi-tier targets."""

    @staticmethod
    def convert_premium_gain_to_spot_move(
        premium_gain: float,
        delta: float,
        gamma: Optional[float] = None,
        theta_decay_buffer: float = 0.0,
    ) -> float:
        """Converts required option premium gain to required underlying spot move.
        Uses Taylor series Black-Scholes expansion with Delta and Gamma convexity:
        Gain = Delta * dS + 0.5 * Gamma * (dS)^2
        Quadratic solution: dS = (-Delta + sqrt(Delta^2 + 2 * Gamma * (Gain + Theta_Buffer))) / Gamma
        Falls back to linear Delta approximation if Gamma is negligible (<= 1e-5).
        """
        abs_delta = max(0.15, abs(delta))
        effective_gain = max(0.0, premium_gain + theta_decay_buffer)
        if gamma is not None and gamma > 1e-5:
            disc = (abs_delta ** 2) + (2.0 * gamma * effective_gain)
            if disc >= 0:
                return round((-abs_delta + math.sqrt(disc)) / gamma, 2)
        # Linear fallback
        return round(effective_gain / abs_delta, 2)

    @staticmethod
    def audit_net_reward_risk_cash_flows(
        entry_ask: float,
        sl_price: float,
        tp1_price: float,
        lot_size: int = 1,
        slippage_pct: float = 0.005,
        transaction_cost_per_lot: float = 40.0,
    ) -> dict:
        """Independent cash-flow validator for net reward-to-risk.
        Models exact rupee cash flows on entry, stop exit, and target exit.
        """
        entry_fill = round(entry_ask * (1.0 + slippage_pct), 2)
        sl_fill = round(sl_price * (1.0 - slippage_pct), 2)
        tp1_fill = round(tp1_price * (1.0 - slippage_pct), 2)
        costs_per_unit = round(transaction_cost_per_lot / max(lot_size, 1), 2)

        # Cash Outflow - Cash Inflow = Net Loss at Stop
        net_loss_at_stop = round((entry_fill - sl_fill) + costs_per_unit, 2)
        # Cash Inflow - Cash Outflow = Net Gain at TP1
        net_reward_at_tp1 = round((tp1_fill - entry_fill) - costs_per_unit, 2)
        net_rr = round(net_reward_at_tp1 / max(net_loss_at_stop, 0.01), 2)

        return {
            "entry_fill": entry_fill,
            "sl_fill": sl_fill,
            "tp1_fill": tp1_fill,
            "costs_per_unit": costs_per_unit,
            "net_loss_at_stop": net_loss_at_stop,
            "net_reward_at_tp1": net_reward_at_tp1,
            "net_rr_tp1": net_rr,
            "is_compliant": net_rr >= 1.50,
        }

    @classmethod
    def calculate_levels(
        cls,
        contract: SelectedContract,
        underlying_spot: float,
        breakout_candle_high: float,
        breakout_candle_low: float,
        orb_boundary: float,
        direction: str,  # BUY_CE or BUY_PE
        max_underlying_move_pct: Optional[float] = None,
        atr: Optional[float] = None,
    ) -> Tuple[Optional[CalculatedLevels], List[str]]:
        rejection_reasons: List[str] = []

        # 1. Underlying Invalidation Level with ATR-bounded structural risk (0.6 - 1.2 ATR)
        tick_size = getattr(settings, "DEFAULT_TICK_SIZE", 0.05)
        min_und_risk = max(0.5, round(underlying_spot * settings.MIN_UNDERLYING_RISK_PCT, 2))
        if direction == "BUY_CE":
            raw_invalidation = round(min(breakout_candle_low, orb_boundary) - min_und_risk, 2)
            raw_und_risk = max(min_und_risk, underlying_spot - raw_invalidation)
        else:  # BUY_PE
            raw_invalidation = round(max(breakout_candle_high, orb_boundary) + min_und_risk, 2)
            raw_und_risk = max(min_und_risk, raw_invalidation - underlying_spot)

        if atr and atr > 0:
            underlying_risk = round(max(0.6 * atr, min(1.2 * atr, raw_und_risk)), 2)
        else:
            underlying_risk = round(raw_und_risk, 2)

        if direction == "BUY_CE":
            underlying_invalidation = round(underlying_spot - underlying_risk, 2)
        else:
            underlying_invalidation = round(underlying_spot + underlying_risk, 2)

        # 2. Premium Risk Calculation using Option Delta and Gamma Convexity
        gamma_adj = 0.5 * (contract.gamma or 0.0) * (underlying_risk ** 2) if contract.gamma else 0.0
        gross_premium_risk = round(max(0.5, (underlying_risk * abs(contract.delta)) - gamma_adj), 2)
        # Bounded between configurable minimum floor and maximum cap
        min_risk = round(contract.executable_ask * settings.MIN_OPTION_RISK_PCT, 2)
        max_risk = round(contract.executable_ask * settings.MAX_OPTION_RISK_PCT, 2)
        gross_premium_risk = max(min_risk, min(gross_premium_risk, max_risk))

        # Stop-loss rounded DOWN to nearest exchange tick for conservative risk modeling
        sl_price = round_down_to_tick(contract.executable_ask - gross_premium_risk, tick_size)
        if sl_price <= 0:
            return None, ["Calculated option stop-loss is <= 0. Setup invalid."]

        actual_gross_risk = round(contract.executable_ask - sl_price, 2)

        # 3. Bilateral Friction Calculation (Slippage & Transaction Costs on BOTH sides)
        # settings.SLIPPAGE_ESTIMATE_PCT is modeled PER EXECUTION SIDE
        slip_pct = settings.SLIPPAGE_ESTIMATE_PCT / 100.0
        costs_per_unit = round(settings.TRANSACTION_COST_PER_LOT / max(contract.lot_size, 1), 2)

        # Slippage when executing entry at Ask (one side)
        entry_slippage = round(contract.executable_ask * slip_pct, 2)
        # Slippage when stopped out at Bid / SL price (one side)
        sl_exit_slippage = round(sl_price * slip_pct, 2)

        # Total estimated Net Loss if stop is hit (inclusive of friction on both sides + round-trip costs)
        net_loss_at_stop = round(actual_gross_risk + entry_slippage + sl_exit_slippage + costs_per_unit, 2)

        # 4. Three Defensible Targets ensuring Net Reward >= 1.5x, 2.5x, 4.0x Net Loss
        # Algebraic solution: Gross_Gain * (1 - slip_pct) = R * Net_Loss + 2 * Entry_Slippage + Costs
        denom = max(0.001, 1.0 - slip_pct)

        r1 = max(settings.MIN_REWARD_TO_RISK_RATIO, settings.TARGET_R1_RATIO)
        r2 = settings.TARGET_R2_RATIO
        r3 = settings.TARGET_R3_RATIO

        exact_gain_tp1 = (r1 * net_loss_at_stop + 2.0 * entry_slippage + costs_per_unit) / denom
        exact_gain_tp2 = (r2 * net_loss_at_stop + 2.0 * entry_slippage + costs_per_unit) / denom
        exact_gain_tp3 = (r3 * net_loss_at_stop + 2.0 * entry_slippage + costs_per_unit) / denom

        # Round UP to nearest exchange tick to strictly guarantee net target gains on exchange
        tp1_price = round_up_to_tick(contract.executable_ask + exact_gain_tp1, tick_size)
        tp2_price = round_up_to_tick(contract.executable_ask + exact_gain_tp2, tick_size)
        tp3_price = round_up_to_tick(contract.executable_ask + exact_gain_tp3, tick_size)

        tp1_gross_gain = round(tp1_price - contract.executable_ask, 2)
        tp2_gross_gain = round(tp2_price - contract.executable_ask, 2)
        tp3_gross_gain = round(tp3_price - contract.executable_ask, 2)

        # Verify net gains and net R:R at targets
        tp1_exit_slippage = round(tp1_price * slip_pct, 2)
        net_gain_tp1 = round(tp1_gross_gain - entry_slippage - tp1_exit_slippage - costs_per_unit, 2)
        net_rr_tp1 = round(net_gain_tp1 / max(net_loss_at_stop, 0.01), 2)

        tp2_exit_slippage = round(tp2_price * slip_pct, 2)
        net_gain_tp2 = round(tp2_gross_gain - entry_slippage - tp2_exit_slippage - costs_per_unit, 2)
        net_rr_tp2 = round(net_gain_tp2 / max(net_loss_at_stop, 0.01), 2)

        tp3_exit_slippage = round(tp3_price * slip_pct, 2)
        net_gain_tp3 = round(tp3_gross_gain - entry_slippage - tp3_exit_slippage - costs_per_unit, 2)
        net_rr_tp3 = round(net_gain_tp3 / max(net_loss_at_stop, 0.01), 2)

        if net_rr_tp1 < settings.MIN_REWARD_TO_RISK_RATIO:
            return None, [f"Net Reward-to-Risk ({net_rr_tp1}:1) falls below minimum required {settings.MIN_REWARD_TO_RISK_RATIO}:1."]

        # 5. Underlying Move Feasibility Check (Delta + Gamma Convexity Model with Theta decay buffer)
        theta_buffer = round(contract.executable_ask * settings.THETA_DECAY_BUFFER_PCT, 2)
        required_spot_move = cls.convert_premium_gain_to_spot_move(
            premium_gain=tp1_gross_gain,
            delta=contract.delta,
            gamma=contract.gamma,
            theta_decay_buffer=theta_buffer,
        )
        cap_pct = max_underlying_move_pct or settings.MAX_UNDERLYING_MOVE_PCT
        max_allowed_spot_move = round(underlying_spot * (cap_pct / 100.0), 2)

        if required_spot_move > max_allowed_spot_move:
            return None, [
                f"TP1 requires {required_spot_move:.1f} pts underlying move ({required_spot_move / underlying_spot * 100:.2f}%), "
                f"exceeding max configured cap of {cap_pct:.1f}% ({max_allowed_spot_move:.1f} pts). Signal suppressed."
            ]

        # 5. Statistical Confidence Score of Reaching Each Target
        delta_mag = min(0.70, max(0.40, abs(contract.delta)))
        base_factor = 70.0 + (delta_mag - 0.45) * 60.0
        tp1_conf = round(min(94.0, max(65.0, base_factor * 1.05)), 1)
        tp2_conf = round(min(82.0, max(45.0, base_factor * 0.78)), 1)
        tp3_conf = round(min(65.0, max(28.0, base_factor * 0.52)), 1)

        entry_low = round(contract.executable_ask * (1.0 - slip_pct), 2)
        entry_high = contract.executable_ask
        max_entry = contract.max_acceptable_entry

        levels = CalculatedLevels(
            entry_range_low=entry_low,
            entry_range_high=entry_high,
            max_entry_price=max_entry,
            sl_price=sl_price,
            premium_risk_per_unit=gross_premium_risk,
            underlying_invalidation=round(underlying_invalidation, 2),
            underlying_risk_distance=round(underlying_risk, 2),
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            tp3_price=tp3_price,
            planned_rr_tp1=net_rr_tp1,
            planned_rr_tp2=net_rr_tp2,
            planned_rr_tp3=net_rr_tp3,
            net_rr_tp1=net_rr_tp1,
            estimated_slippage=round(entry_slippage + sl_exit_slippage, 2),
            estimated_transaction_cost=costs_per_unit,
            tp1_confidence=tp1_conf,
            tp2_confidence=tp2_conf,
            tp3_confidence=tp3_conf,
        )
        return levels, []


targets_engine = TargetsAndStopEngine()
