"""Market breadth analysis for Indian benchmark indices.

Calculates advance/decline ratios, constituent participation, and confirms
whether the underlying index move is broad-based or an isolated constituent divergence.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Optional
from pydantic import BaseModel


class BreadthMetrics(BaseModel):
    underlying: str
    advances: int
    declines: int
    unchanged: int
    advance_decline_ratio: float
    breadth_percentage: float  # Advances / (Advances + Declines) %
    is_supportive_bullish: bool
    is_supportive_bearish: bool


class MarketBreadthAnalyzer:
    """Evaluates market breadth to validate participation behind breakouts."""

    def __init__(self):
        self._breadth_state: Dict[str, BreadthMetrics] = {}
        self._breadth_updated_at: Dict[str, datetime] = {}

    def update_breadth(
        self,
        underlying: str,
        advances: int,
        declines: int,
        unchanged: int = 0,
        timestamp: Optional[datetime] = None,
    ) -> BreadthMetrics:
        total = max(advances + declines, 1)
        ratio = round(advances / max(declines, 1), 2)
        pct = round((advances / total) * 100.0, 1)

        metrics = BreadthMetrics(
            underlying=underlying,
            advances=advances,
            declines=declines,
            unchanged=unchanged,
            advance_decline_ratio=ratio,
            breadth_percentage=pct,
            is_supportive_bullish=pct >= 65.0,
            is_supportive_bearish=pct <= 35.0,
        )
        self._breadth_state[underlying] = metrics
        self._breadth_updated_at[underlying] = timestamp or datetime.now(timezone.utc)
        return metrics

    def get_breadth(
        self, underlying: str, max_age_seconds: float = 90.0
    ) -> Optional[BreadthMetrics]:
        """Returns verified market breadth if fresh; otherwise returns None.
        Never fabricates metrics from wall-clock minutes.
        """
        if underlying not in self._breadth_state:
            return None

        last_update = self._breadth_updated_at.get(underlying)
        if last_update is None:
            return None

        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)

        if now - last_update > timedelta(seconds=max_age_seconds):
            return None

        return self._breadth_state[underlying]

    def clear(self) -> None:
        self._breadth_state.clear()
        self._breadth_updated_at.clear()


market_breadth_analyzer = MarketBreadthAnalyzer()
