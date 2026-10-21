"""Base Market Data Provider Abstract Interface and Normalized Schemas.

Enforces provider independence so the trading and signal engine does not
rely directly on broker-specific formats.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Callable
from pydantic import BaseModel, Field


class UnifiedDepthLevel(BaseModel):
    price: float
    quantity: int
    orders: Optional[int] = None


class UnifiedDepth(BaseModel):
    bids: List[UnifiedDepthLevel] = Field(default_factory=list)
    asks: List[UnifiedDepthLevel] = Field(default_factory=list)


class UnifiedQuote(BaseModel):
    """Normalized real-time quote for an index or option contract."""
    instrument_id: str  # Broker security ID / unified ID
    symbol: str
    exchange: str  # NSE, BSE
    ltp: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_qty: Optional[int] = None
    ask_qty: Optional[int] = None
    volume: Optional[int] = None
    oi: Optional[int] = None
    prev_close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    exchange_timestamp: datetime
    receive_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "dhan"
    depth: Optional[UnifiedDepth] = None
    greeks: Optional[OptionGreeks] = None
    iv: Optional[float] = None

    @property
    def spread(self) -> Optional[float]:
        if self.ask is not None and self.bid is not None:
            return round(self.ask - self.bid, 2)
        return None

    @property
    def spread_pct(self) -> Optional[float]:
        if self.ask and self.bid and self.ask > 0:
            return round(((self.ask - self.bid) / self.ask) * 100.0, 2)
        return None


class UnifiedCandle(BaseModel):
    """Normalized OHLCV candle bar."""
    instrument_id: str
    symbol: str
    timeframe: str  # 1m, 5m, 15m, 1d
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    oi: Optional[int] = None


class OptionGreeks(BaseModel):
    delta: Optional[float] = None
    theta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    iv: Optional[float] = None


class OptionContractData(BaseModel):
    """Data for a single option contract within an option chain."""
    instrument_id: str
    symbol: str
    strike: float
    option_type: str  # CE or PE
    expiry: datetime
    ltp: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[int] = None
    oi: Optional[int] = None
    oi_change: Optional[int] = None
    bid_qty: Optional[int] = None
    ask_qty: Optional[int] = None
    close_price: Optional[float] = None
    greeks: Optional[OptionGreeks] = None
    lot_size: int = 1


class OptionChainSnapshot(BaseModel):
    """Normalized snapshot of the full option chain for an index."""
    underlying: str
    underlying_ltp: float
    snapshot_timestamp: datetime
    expiry: datetime
    source: str = "unknown"
    is_simulated: bool = False
    contracts: Dict[float, Dict[str, OptionContractData]] = Field(
        default_factory=dict
    )  # strike -> {"CE": OptionContractData, "PE": OptionContractData}
    pcr_oi: Optional[float] = None
    pcr_volume: Optional[float] = None


class BaseMarketDataProvider(ABC):
    """Abstract interface that every market data provider must implement."""

    def __init__(self, name: str):
        self.name = name
        self.is_connected = False
        self._quote_callbacks: List[Callable[[UnifiedQuote], Any]] = []

    def register_quote_callback(self, callback: Callable[[UnifiedQuote], Any]) -> None:
        self._quote_callbacks.append(callback)

    async def _emit_quote(self, quote: UnifiedQuote) -> None:
        for cb in self._quote_callbacks:
            try:
                res = cb(quote)
                if hasattr(res, "__await__"):
                    await res
            except Exception as e:
                # Callback failures should not crash data feed
                pass

    @abstractmethod
    async def connect(self) -> bool:
        """Establish session and connection."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections."""
        pass

    @abstractmethod
    async def subscribe_quotes(self, instrument_ids: List[str]) -> None:
        """Subscribe to live quotes on WebSocket."""
        pass

    @abstractmethod
    async def unsubscribe_quotes(self, instrument_ids: List[str]) -> None:
        """Unsubscribe from live quotes."""
        pass

    @abstractmethod
    async def get_quote_snapshot(self, instrument_id: str) -> Optional[UnifiedQuote]:
        """Fetch REST snapshot of a single instrument quote."""
        pass

    @abstractmethod
    async def get_historical_candles(
        self, instrument_id: str, timeframe: str, count: int = 100
    ) -> List[UnifiedCandle]:
        """Fetch historical candles via REST API."""
        pass

    @abstractmethod
    async def get_option_chain_snapshot(
        self, underlying: str, expiry: Optional[datetime] = None
    ) -> Optional[OptionChainSnapshot]:
        """Fetch real-time option chain with Greeks, OI, and bid/ask."""
        pass
