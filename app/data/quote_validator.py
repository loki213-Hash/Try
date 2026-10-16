"""Quote validator enforcing data quality, freshness checks, and anomaly filtering.

Strictly rejects stale, zero-price, crossed-book, or corrupted quotes.
"""

from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Tuple, List, Optional
from app.data.base_provider import UnifiedQuote
from app.monitoring.logging import logger


class QuoteValidationResult:
    def __init__(self, is_valid: bool, reasons: List[str]):
        self.is_valid = is_valid
        self.reasons = reasons

    def __bool__(self) -> bool:
        return self.is_valid


class QuoteValidator:
    """Validates quotes against strict production criteria."""

    def __init__(
        self,
        max_staleness_seconds: float = 5.0,
        max_spread_pct: float = 3.5,
    ):
        self.max_staleness_seconds = max_staleness_seconds
        self.max_spread_pct = max_spread_pct
        self._last_valid_quote_time: dict[str, float] = {}

    def validate_quote(
        self, quote: UnifiedQuote, is_live_market: bool = True
    ) -> QuoteValidationResult:
        reasons: List[str] = []

        # 1. Price sanity check
        if quote.ltp <= 0.0:
            reasons.append(f"Invalid LTP {quote.ltp} <= 0")

        # 2. Bid/Ask sanity check
        if quote.bid is not None and quote.ask is not None:
            if quote.bid > quote.ask:
                reasons.append(
                    f"Crossed order book: Bid ({quote.bid}) > Ask ({quote.ask})"
                )
            if quote.ask > 0 and quote.bid >= 0:
                spread_pct = ((quote.ask - quote.bid) / quote.ask) * 100.0
                if spread_pct > self.max_spread_pct:
                    reasons.append(
                        f"Spread {spread_pct:.2f}% exceeds max threshold {self.max_spread_pct}%"
                    )

        # 3. Staleness check
        now_epoch = time.time()
        # Convert exchange_timestamp to epoch
        exch_dt = quote.exchange_timestamp
        if exch_dt.tzinfo is None:
            exch_epoch = exch_dt.replace(tzinfo=timezone.utc).timestamp()
        else:
            exch_epoch = exch_dt.timestamp()

        staleness = now_epoch - exch_epoch
        if is_live_market and staleness > self.max_staleness_seconds:
            reasons.append(
                f"Data stale: latency {staleness:.2f}s exceeds limit {self.max_staleness_seconds}s"
            )

        # 4. Volume / OI sanity
        if quote.volume is not None and quote.volume < 0:
            reasons.append(f"Negative volume: {quote.volume}")
        if quote.oi is not None and quote.oi < 0:
            reasons.append(f"Negative Open Interest: {quote.oi}")

        is_valid = len(reasons) == 0
        if is_valid:
            self._last_valid_quote_time[quote.instrument_id] = now_epoch
        else:
            logger.warning(
                f"Quote rejected for {quote.symbol} ({quote.instrument_id}): {', '.join(reasons)}"
            )

        return QuoteValidationResult(is_valid=is_valid, reasons=reasons)

    def is_instrument_stale(self, instrument_id: str) -> bool:
        last_time = self._last_valid_quote_time.get(instrument_id)
        if not last_time:
            return True
        return (time.time() - last_time) > self.max_staleness_seconds
