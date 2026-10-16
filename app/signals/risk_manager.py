"""Deterministic Risk Manager and Sizing Engine.

Enforces:
- 0.5% max risk budget per trade
- Sizing: floor(Risk_Budget / (Premium_Risk * Lot_Size))
- Max daily loss limit (1.0% of capital or 2 stop-losses)
- Cross-index correlated exposure limits
- Indian statutory transaction costs (STT, GST, Exchange, Stamp Duty, Brokerage)
- Slippage estimation
"""

from __future__ import annotations
import math
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Tuple
from pydantic import BaseModel

from app.database.models import RiskState
from app.monitoring.logging import logger
from config import settings


class PositionSizeResult(BaseModel):
    is_approved: bool
    lots: int
    quantity: int
    risk_budget_allocated: float
    total_trade_capital: float
    rejection_reason: Optional[str] = None


class TradeCostEstimate(BaseModel):
    brokerage: float
    stt: float
    exchange_charges: float
    gst: float
    stamp_duty: float
    sebi_charges: float
    slippage: float
    total_costs: float


class RiskManager:
    """Oversees portfolio risk, position sizing, correlation locks, and costs."""

    # Correlated clusters
    CORRELATED_CLUSTERS = {
        "NIFTY": "EQUITY_BENCHMARK",
        "SENSEX": "EQUITY_BENCHMARK",
        "BANKNIFTY": "BANKING_BENCHMARK",
        "BANKEX": "BANKING_BENCHMARK",
        "FINNIFTY": "FINANCIAL_SERVICES",
        "MIDCPNIFTY": "MIDCAP_BENCHMARK",
    }

    def __init__(self, total_capital: float = settings.ACCOUNT_CAPITAL):
        self.total_capital = total_capital
        self.risk_per_trade_pct = settings.RISK_PER_TRADE_PCT
        self.max_daily_loss_pct = settings.MAX_DAILY_LOSS_PCT
        self.max_daily_stops = settings.MAX_DAILY_STOP_LOSSES

    def calculate_position_size(
        self,
        premium_risk_per_unit: float,
        entry_price: float,
        lot_size: int,
        risk_state: RiskState,
    ) -> PositionSizeResult:
        # Check if daily loss lock is active
        if risk_state.trading_locked:
            return PositionSizeResult(
                is_approved=False,
                lots=0,
                quantity=0,
                risk_budget_allocated=0.0,
                total_trade_capital=0.0,
                rejection_reason=f"Trading locked: {risk_state.lock_reason}",
            )

        # Check daily stop-out limit (enforced only when daily stop protection is enabled)
        if settings.ENABLE_DAILY_STOP_LOSS:
            stop_outs = risk_state.stop_outs_count or 0
            if stop_outs >= self.max_daily_stops:
                risk_state.trading_locked = True
                risk_state.lock_reason = f"Max daily stop-outs ({self.max_daily_stops}) reached"
                return PositionSizeResult(
                    is_approved=False,
                    lots=0,
                    quantity=0,
                    risk_budget_allocated=0.0,
                    total_trade_capital=0.0,
                    rejection_reason=risk_state.lock_reason,
                )

        # Check daily realized loss limit (enforced only when daily stop protection is enabled)
        capital = risk_state.starting_capital or self.total_capital
        if settings.ENABLE_DAILY_STOP_LOSS and self.max_daily_loss_pct is not None:
            max_daily_loss = capital * self.max_daily_loss_pct
            realized_loss = risk_state.realized_pnl or 0.0
            if realized_loss <= -max_daily_loss:
                risk_state.trading_locked = True
                risk_state.lock_reason = f"Max daily loss (₹{max_daily_loss:.0f}) reached"
                return PositionSizeResult(
                    is_approved=False,
                    lots=0,
                    quantity=0,
                    risk_budget_allocated=0.0,
                    total_trade_capital=0.0,
                    rejection_reason=risk_state.lock_reason,
                )

        # Calculate max budget for this trade
        risk_budget = round(capital * self.risk_per_trade_pct, 2)
        risk_per_lot = premium_risk_per_unit * lot_size

        if risk_per_lot <= 0:
            return PositionSizeResult(
                is_approved=False,
                lots=0,
                quantity=0,
                risk_budget_allocated=0.0,
                total_trade_capital=0.0,
                rejection_reason="Risk per lot is <= 0",
            )

        lots = math.floor(risk_budget / risk_per_lot)
        if lots < 1:
            return PositionSizeResult(
                is_approved=False,
                lots=0,
                quantity=0,
                risk_budget_allocated=0.0,
                total_trade_capital=0.0,
                rejection_reason=f"Risk per lot (Rs {risk_per_lot:.2f}) exceeds trade risk budget (Rs {risk_budget:.2f})",
            )

        # Cap max lots to prevent oversized margin deployment
        trade_capital = lots * lot_size * entry_price
        max_permitted_capital = capital * settings.MAX_TRADE_CAPITAL_ALLOCATION_PCT  # Max capital in single options trade
        if trade_capital > max_permitted_capital:
            lots = max(1, math.floor(max_permitted_capital / (lot_size * entry_price)))
            trade_capital = lots * lot_size * entry_price

        quantity = lots * lot_size
        allocated_risk = round(lots * risk_per_lot, 2)

        return PositionSizeResult(
            is_approved=True,
            lots=lots,
            quantity=quantity,
            risk_budget_allocated=allocated_risk,
            total_trade_capital=round(trade_capital, 2),
        )

    def check_correlation_exposure(
        self, candidate_underlying: str, open_underlyings: List[str]
    ) -> Tuple[bool, Optional[str]]:
        """Ensures correlated indices (e.g. NIFTY + SENSEX) don't duplicate exposure."""
        cand_cluster = self.CORRELATED_CLUSTERS.get(candidate_underlying)
        if not cand_cluster:
            return True, None

        for open_und in open_underlyings:
            if open_und == candidate_underlying:
                return False, f"Existing open trade already active on {candidate_underlying}"
            open_cluster = self.CORRELATED_CLUSTERS.get(open_und)
            if open_cluster == cand_cluster:
                return (
                    False,
                    f"Correlated cluster limit: {candidate_underlying} shares {cand_cluster} with active {open_und}",
                )

        return True, None

    @staticmethod
    def calculate_estimated_costs(
        entry_price: float, exit_price: float, quantity: int
    ) -> TradeCostEstimate:
        """Calculates realistic Indian regulatory taxes, transaction charges, and brokerage."""
        buy_turnover = entry_price * quantity
        sell_turnover = exit_price * quantity
        total_turnover = buy_turnover + sell_turnover

        # 1. Brokerage: ₹20 per executed order (Buy + Sell = ₹40)
        brokerage = 40.0

        # 2. STT (Securities Transaction Tax): 0.15% on sell option premium turnover
        stt = round(sell_turnover * 0.0015, 2)

        # 3. Exchange Turnover Charge: ~0.05% on premium turnover
        exch_charges = round(total_turnover * 0.0005, 2)

        # 4. GST: 18% on (Brokerage + Exchange Charges)
        gst = round((brokerage + exch_charges) * 0.18, 2)

        # 5. Stamp Duty: 0.003% on buy turnover
        stamp_duty = round(buy_turnover * 0.00003, 2)

        # 6. SEBI Charges: ₹10 per crore (0.0001%)
        sebi_charges = round(total_turnover * 0.000001, 2)

        # 7. Estimated Slippage modeled per side from settings
        slip_rate = (settings.SLIPPAGE_ESTIMATE_PCT / 100.0)
        slippage = round((entry_price * slip_rate + exit_price * slip_rate) * quantity, 2)

        total_costs = round(
            brokerage + stt + exch_charges + gst + stamp_duty + sebi_charges + slippage, 2
        )

        return TradeCostEstimate(
            brokerage=brokerage,
            stt=stt,
            exchange_charges=exch_charges,
            gst=gst,
            stamp_duty=stamp_duty,
            sebi_charges=sebi_charges,
            slippage=slippage,
            total_costs=total_costs,
        )


risk_manager = RiskManager()
