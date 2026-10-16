"""System health and monitoring service.

Maintains real-time status of:
- Broker connection status (Dhan / Upstox / Simulated)
- Last valid quote timestamp and data staleness
- Database connectivity
- Telegram API connection
- Processing latencies and error counters
"""

from __future__ import annotations
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class HealthStatus(BaseModel):
    is_healthy: bool
    status: str
    uptime_seconds: float
    primary_broker: str
    broker_connected: bool
    db_connected: bool
    telegram_connected: bool
    last_quote_time: Optional[datetime] = None
    stale_data_flag: bool = False
    active_indices_monitored: list[str] = Field(default_factory=list)
    error_count: int = 0
    last_error: Optional[str] = None
    memory_summary: Dict[str, Any] = Field(default_factory=dict)


class HealthMonitor:
    """Singleton health manager tracking runtime system metrics."""

    def __init__(self):
        self._start_time = time.time()
        self._broker_connected = False
        self._db_connected = False
        self._telegram_connected = False
        self._last_quote_time: Optional[datetime] = None
        self._last_quote_epoch: float = 0.0
        self._stale_threshold_seconds: float = 5.0
        self._monitored_indices: set[str] = set()
        self._error_count: int = 0
        self._last_error: Optional[str] = None
        self._primary_broker = "dhan"

    def set_primary_broker(self, broker_name: str) -> None:
        self._primary_broker = broker_name

    def record_broker_status(self, connected: bool) -> None:
        self._broker_connected = connected

    def record_db_status(self, connected: bool) -> None:
        self._db_connected = connected

    def record_telegram_status(self, connected: bool) -> None:
        self._telegram_connected = connected

    def record_quote_received(self, index_symbol: str, timestamp: Optional[datetime] = None) -> None:
        self._monitored_indices.add(index_symbol)
        now_dt = timestamp or datetime.now(timezone.utc)
        self._last_quote_time = now_dt
        self._last_quote_epoch = time.time()

    def record_error(self, err_msg: str) -> None:
        self._error_count += 1
        self._last_error = f"{datetime.now(timezone.utc).isoformat()}: {err_msg}"

    def is_data_stale(self, max_seconds: float = 5.0) -> bool:
        if self._last_quote_epoch == 0.0:
            return True
        return (time.time() - self._last_quote_epoch) > max_seconds

    def get_status(self) -> HealthStatus:
        uptime = round(time.time() - self._start_time, 2)
        is_any_market_open = True
        try:
            from app.data.instrument_master import instrument_master
            from config import settings
            is_any_market_open = any(
                instrument_master.is_market_session_open(und)[0]
                for und in settings.INDICES.keys()
            )
        except Exception:
            pass

        stale = self.is_data_stale(self._stale_threshold_seconds) if is_any_market_open else False
        is_healthy = self._db_connected and self._broker_connected and not stale

        return HealthStatus(
            is_healthy=is_healthy,
            status="OPERATIONAL" if is_healthy else ("DEGRADED" if self._db_connected else "DOWN"),
            uptime_seconds=uptime,
            primary_broker=self._primary_broker,
            broker_connected=self._broker_connected,
            db_connected=self._db_connected,
            telegram_connected=self._telegram_connected,
            last_quote_time=self._last_quote_time,
            stale_data_flag=stale,
            active_indices_monitored=sorted(list(self._monitored_indices)),
            error_count=self._error_count,
            last_error=self._last_error
        )


health_monitor = HealthMonitor()
