"""Real-time candle builder and multi-timeframe bar aggregator.

Constructs 1-minute bars from streaming ticks, then rolls them up into
5-minute (primary trigger) and 15-minute (structure) completed candles.
Only completed candles are dispatched to the signal detector to prevent
look-ahead bias.
"""

from __future__ import annotations
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable, Any
from app.data.base_provider import UnifiedQuote, UnifiedCandle
from app.monitoring.logging import logger


class CandleAggregator:
    """Aggregates ticks into 1m, 5m, and 15m bars for an instrument."""

    def __init__(self, instrument_id: str, symbol: str):
        self.instrument_id = instrument_id
        self.symbol = symbol
        self.last_quote: Optional[UnifiedQuote] = None

        # Active forming bars
        self._current_1m: Optional[UnifiedCandle] = None
        self._current_1m_minute: Optional[int] = None

        # Buffers of completed bars
        self.completed_1m: List[UnifiedCandle] = []
        self.completed_5m: List[UnifiedCandle] = []
        self.completed_15m: List[UnifiedCandle] = []

        # Callbacks on completed bars: (timeframe, candle) -> None
        self.on_candle_closed: List[Callable[[str, UnifiedCandle], Any]] = []

    def _truncate_to_minute(self, dt: datetime) -> datetime:
        return dt.replace(second=0, microsecond=0)

    def process_tick(self, quote: UnifiedQuote) -> Optional[UnifiedCandle]:
        """Ingests a live quote. Returns closed 1m candle if a boundary just crossed."""
        self.last_quote = quote
        dt = quote.exchange_timestamp
        minute_floor = self._truncate_to_minute(dt)
        closed_1m: Optional[UnifiedCandle] = None

        if self._current_1m is None:
            # First tick creates initial bar
            self._current_1m = UnifiedCandle(
                instrument_id=self.instrument_id,
                symbol=self.symbol,
                timeframe="1m",
                timestamp=minute_floor,
                open=quote.ltp,
                high=quote.ltp,
                low=quote.ltp,
                close=quote.ltp,
                volume=quote.volume or 0,
                oi=quote.oi,
            )
        elif self._current_1m.timestamp < minute_floor:
            # Previous 1m candle has finalized!
            closed_1m = self._current_1m
            self.completed_1m.append(closed_1m)
            self._dispatch_closed_bar("1m", closed_1m)

            # Check if this closes a 5m or 15m bar
            self._check_and_roll_higher_timeframes(closed_1m)

            # Start new 1m candle
            self._current_1m = UnifiedCandle(
                instrument_id=self.instrument_id,
                symbol=self.symbol,
                timeframe="1m",
                timestamp=minute_floor,
                open=quote.ltp,
                high=quote.ltp,
                low=quote.ltp,
                close=quote.ltp,
                volume=quote.volume or 0,
                oi=quote.oi,
            )
        else:
            # Update currently forming 1m candle
            bar = self._current_1m
            bar.high = max(bar.high, quote.ltp)
            bar.low = min(bar.low, quote.ltp)
            bar.close = quote.ltp
            if quote.volume:
                bar.volume += quote.volume
            bar.oi = quote.oi

        return closed_1m

    def _check_and_roll_higher_timeframes(self, closed_1m: UnifiedCandle) -> None:
        """Rolls completed 1m bars into 5m and 15m bars."""
        ts = closed_1m.timestamp
        # A 5-minute bar closes at minute :04, :09, :14, :19, ...
        # (meaning the bar with start 09:15 closes when 09:19 finishes, i.e., at 09:20 next tick)
        m = ts.minute
        if (m + 1) % 5 == 0 and len(self.completed_1m) >= 5:
            # Roll up last five 1m candles into 5m
            recent_5 = self.completed_1m[-5:]
            bar_5m = UnifiedCandle(
                instrument_id=self.instrument_id,
                symbol=self.symbol,
                timeframe="5m",
                timestamp=recent_5[0].timestamp,
                open=recent_5[0].open,
                high=max(b.high for b in recent_5),
                low=min(b.low for b in recent_5),
                close=recent_5[-1].close,
                volume=sum(b.volume for b in recent_5),
                oi=recent_5[-1].oi,
            )
            self.completed_5m.append(bar_5m)
            self._dispatch_closed_bar("5m", bar_5m)

        # 15-minute bar roll-up
        if (m + 1) % 15 == 0 and len(self.completed_1m) >= 15:
            recent_15 = self.completed_1m[-15:]
            bar_15m = UnifiedCandle(
                instrument_id=self.instrument_id,
                symbol=self.symbol,
                timeframe="15m",
                timestamp=recent_15[0].timestamp,
                open=recent_15[0].open,
                high=max(b.high for b in recent_15),
                low=min(b.low for b in recent_15),
                close=recent_15[-1].close,
                volume=sum(b.volume for b in recent_15),
                oi=recent_15[-1].oi,
            )
            self.completed_15m.append(bar_15m)
            self._dispatch_closed_bar("15m", bar_15m)

    def _dispatch_closed_bar(self, timeframe: str, bar: UnifiedCandle) -> None:
        for cb in self.on_candle_closed:
            try:
                cb(timeframe, bar)
            except Exception as e:
                logger.error(f"Error in candle callback: {e}")


class MultiIndexCandleManager:
    """Manages candle aggregators across all monitored indices."""

    def __init__(self):
        self._aggregators: Dict[str, CandleAggregator] = {}

    def get_or_create(self, instrument_id: str, symbol: str) -> CandleAggregator:
        if instrument_id not in self._aggregators:
            self._aggregators[instrument_id] = CandleAggregator(
                instrument_id=instrument_id, symbol=symbol
            )
        return self._aggregators[instrument_id]

    def ingest_quote(self, quote: UnifiedQuote) -> None:
        agg = self.get_or_create(quote.instrument_id, quote.symbol)
        agg.process_tick(quote)
