"""Performance and Statistical Analytics Service.

Calculates:
- Win Rate, Expectancy, Profit Factor
- Net P&L after statutory fees & slippage
- Maximum Drawdown
- Breakdown by Index and Market Structure
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from app.database.models import PaperTrade


class PerformanceMetrics(BaseModel):
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate_pct: float = 0.0
    gross_pnl: float = 0.0
    total_fees: float = 0.0
    total_slippage: float = 0.0
    net_pnl: float = 0.0
    profit_factor: float = 0.0
    expectancy_per_trade: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown_amount: float = 0.0
    max_drawdown_pct: float = 0.0
    by_index: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class PerformanceAnalyzer:
    """Computes verified statistical performance on closed paper trades."""

    @staticmethod
    def calculate_metrics(
        trades: List[PaperTrade], initial_capital: float = 500000.0
    ) -> PerformanceMetrics:
        closed = [t for t in trades if t.status == "CLOSED"]
        if not closed:
            return PerformanceMetrics()

        total = len(closed)
        wins = [t for t in closed if t.net_pnl > 0]
        losses = [t for t in closed if t.net_pnl <= 0]

        total_wins = len(wins)
        total_losses = len(losses)
        win_rate = round((total_wins / total) * 100.0, 1)

        gross_pnl = sum((t.gross_pnl or 0.0) for t in closed)
        fees = sum((t.fees or 0.0) for t in closed)
        slippage = sum((t.slippage or 0.0) for t in closed)
        net_pnl = sum((t.net_pnl or 0.0) for t in closed)

        sum_gains = sum((t.net_pnl or 0.0) for t in wins)
        sum_losses = abs(sum((t.net_pnl or 0.0) for t in losses))

        profit_factor = round(sum_gains / max(sum_losses, 1.0), 2) if sum_losses > 0 else (99.0 if sum_gains > 0 else 0.0)

        avg_win = round(sum_gains / max(total_wins, 1), 2)
        avg_loss = round(sum_losses / max(total_losses, 1), 2)

        win_prob = total_wins / total
        loss_prob = total_losses / total
        expectancy = round((win_prob * avg_win) - (loss_prob * avg_loss), 2)

        # Max Drawdown Calculation
        peak = initial_capital
        current_equity = initial_capital
        max_dd_amount = 0.0
        max_dd_pct = 0.0

        for t in reversed(closed):  # Chronological order if list is newest-first
            current_equity += t.net_pnl
            if current_equity > peak:
                peak = current_equity
            dd = peak - current_equity
            if dd > max_dd_amount:
                max_dd_amount = dd
                max_dd_pct = round((dd / peak) * 100.0, 2)

        # Breakdown by index
        by_index: Dict[str, Dict[str, Any]] = {}
        for t in closed:
            if t.underlying not in by_index:
                by_index[t.underlying] = {"trades": 0, "wins": 0, "net_pnl": 0.0}
            by_index[t.underlying]["trades"] += 1
            if t.net_pnl > 0:
                by_index[t.underlying]["wins"] += 1
            by_index[t.underlying]["net_pnl"] += round(t.net_pnl, 2)

        for und, data in by_index.items():
            data["win_rate"] = round((data["wins"] / data["trades"]) * 100.0, 1)

        return PerformanceMetrics(
            total_trades=total,
            winning_trades=total_wins,
            losing_trades=total_losses,
            win_rate_pct=win_rate,
            gross_pnl=round(gross_pnl, 2),
            total_fees=round(fees, 2),
            total_slippage=round(slippage, 2),
            net_pnl=round(net_pnl, 2),
            profit_factor=profit_factor,
            expectancy_per_trade=expectancy,
            avg_win=avg_win,
            avg_loss=avg_loss,
            max_drawdown_amount=round(max_dd_amount, 2),
            max_drawdown_pct=max_dd_pct,
            by_index=by_index,
        )


performance_analyzer = PerformanceAnalyzer()
