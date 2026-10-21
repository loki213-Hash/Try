"""Resilient WebSocket Connection Manager.

Handles streaming quotes, heartbeat ping/pong, disconnect detection,
exponential backoff reconnects, and quote dispatching.
"""

from __future__ import annotations
import asyncio
import time
from typing import List, Callable, Optional, Set
from app.data.base_provider import UnifiedQuote, BaseMarketDataProvider
from app.data.instrument_master import instrument_master
from app.data.quote_validator import QuoteValidator
from app.monitoring.health import health_monitor
from app.monitoring.logging import logger
from config import settings


class WebSocketFeedManager:
    """Oversees streaming WebSocket connection lifecycle and subscriptions."""

    def __init__(
        self,
        provider: BaseMarketDataProvider,
        quote_validator: QuoteValidator,
    ):
        self.provider = provider
        self.quote_validator = quote_validator
        self._subscribed_instruments: Set[str] = set()
        self._is_running = False
        self._reconnect_attempts = 0
        self._max_attempts = settings.WS_RECONNECT_MAX_ATTEMPTS
        self._base_delay = settings.WS_RECONNECT_BASE_DELAY_SECONDS
        self._task: Optional[asyncio.Task] = None
        self._quote_handlers: List[Callable[[UnifiedQuote], None]] = []

    def add_quote_handler(self, handler: Callable[[UnifiedQuote], None]) -> None:
        self._quote_handlers.append(handler)

    def subscribe(self, instrument_ids: List[str]) -> None:
        for i_id in instrument_ids:
            self._subscribed_instruments.add(i_id)

    def register_instruments(self, instrument_ids: List[str]) -> None:
        self.subscribe(instrument_ids)

    async def start(self) -> None:
        self._is_running = True
        # Register once for the provider lifetime; reconnects must not multiply
        # handlers and duplicate ticks/candle volume.
        self.provider.register_quote_callback(self._on_quote_received)
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"WebSocketFeedManager started with provider: {self.provider.name}")

    async def stop(self) -> None:
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.provider.disconnect()
        health_monitor.record_broker_status(False)
        logger.info("WebSocketFeedManager stopped.")

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                logger.info(f"Connecting to {self.provider.name} market feed...")
                connected = await self.provider.connect()
                if not connected:
                    raise ConnectionError(f"Failed to connect to {self.provider.name}")

                health_monitor.record_broker_status(True)
                health_monitor.set_primary_broker(self.provider.name)
                self._reconnect_attempts = 0

                # Resubscribe all registered instruments on connection/reconnect
                if self._subscribed_instruments:
                    await self.provider.subscribe_quotes(list(self._subscribed_instruments))
                    logger.info(f"Resubscribed {len(self._subscribed_instruments)} instruments.")

                # Keep connection alive and monitor health
                while self._is_running and self.provider.is_connected:
                    await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SECONDS)
                    # Check staleness only during live trading session
                    is_any_market_open = any(
                        instrument_master.is_market_session_open(und)[0]
                        for und in settings.INDICES.keys()
                    )
                    if is_any_market_open:
                        if health_monitor.is_data_stale(settings.MAX_QUOTE_STALENESS_SECONDS * 3):
                            logger.warning("No quotes received within heartbeat threshold during live market. Verifying feed...")

            except asyncio.CancelledError:
                break
            except Exception as e:
                health_monitor.record_broker_status(False)
                health_monitor.record_error(str(e))
                self._reconnect_attempts += 1
                delay = min(self._base_delay * (2 ** (self._reconnect_attempts - 1)), 60.0)
                logger.error(
                    f"WebSocket feed error: {e}. Reconnecting attempt {self._reconnect_attempts}/{self._max_attempts} in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)

    def _on_quote_received(self, quote: UnifiedQuote) -> None:
        """Validates incoming quote and dispatches to registered listeners."""
        res = self.quote_validator.validate_quote(quote, is_live_market=True)
        if not res.is_valid:
            return

        health_monitor.record_quote_received(quote.symbol, quote.exchange_timestamp)
        for handler in self._quote_handlers:
            try:
                handler(quote)
            except Exception as e:
                logger.error(f"Error handling live quote: {e}")
