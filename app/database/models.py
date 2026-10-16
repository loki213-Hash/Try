"""SQLAlchemy database models for persistence, auditability, and replay.

Includes:
- instruments
- market_ticks
- candles
- features
- news_events
- signals
- signal_levels
- paper_trades
- signal_events
- risk_state
- system_health
- strategy_versions
"""

from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Optional, Any
from sqlalchemy import (
    Column,
    String,
    Float,
    Integer,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    JSON,
    Index,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Instrument(Base):
    """Stores master contract definitions."""
    __tablename__ = "instruments"

    id = Column(String(64), primary_key=True)  # Unified key e.g. NSE:NIFTY24OCT25000CE
    instrument_id = Column(String(64), nullable=False, index=True)  # Broker security ID
    exchange = Column(String(16), nullable=False, index=True)  # NSE, BSE
    underlying = Column(String(32), nullable=False, index=True)  # NIFTY, BANKNIFTY, SENSEX
    symbol = Column(String(64), nullable=False, index=True)
    strike = Column(Float, nullable=True)
    option_type = Column(String(8), nullable=False)  # CE, PE, INDEX, FUT
    expiry = Column(DateTime, nullable=True, index=True)
    lot_size = Column(Integer, nullable=False, default=1)
    tick_size = Column(Float, nullable=False, default=0.05)
    is_active = Column(Boolean, nullable=False, default=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)

    __table_args__ = (
        Index("idx_inst_lookup", "underlying", "option_type", "expiry", "strike"),
    )


class MarketTick(Base):
    """Raw validated quotes and ticks."""
    __tablename__ = "market_ticks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(String(64), nullable=False, index=True)
    exchange_timestamp = Column(DateTime, nullable=False, index=True)
    receive_timestamp = Column(DateTime, default=utc_now, index=True)
    ltp = Column(Float, nullable=False)
    bid = Column(Float, nullable=True)
    ask = Column(Float, nullable=True)
    bid_qty = Column(Integer, nullable=True)
    ask_qty = Column(Integer, nullable=True)
    volume = Column(Integer, nullable=True)
    oi = Column(Integer, nullable=True)
    source = Column(String(32), default="dhan")

    __table_args__ = (
        Index("idx_ticks_time", "instrument_id", "exchange_timestamp"),
    )


class Candle(Base):
    """Aggregated completed and ongoing OHLCV bars."""
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(String(64), nullable=False, index=True)
    timeframe = Column(String(16), nullable=False, index=True)  # 1m, 5m, 15m, 1d
    timestamp = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False, default=0)
    oi = Column(Integer, nullable=True)

    __table_args__ = (
        Index("idx_candles_inst_time", "instrument_id", "timeframe", "timestamp"),
    )


class FeatureSnapshot(Base):
    """Calculated technical, structure, breadth, and OI features."""
    __tablename__ = "features"

    id = Column(Integer, primary_key=True, autoincrement=True)
    underlying = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(16), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    vwap = Column(Float, nullable=True)
    ema_9 = Column(Float, nullable=True)
    ema_21 = Column(Float, nullable=True)
    ema_50 = Column(Float, nullable=True)
    ema_200 = Column(Float, nullable=True)
    atr = Column(Float, nullable=True)
    rvol = Column(Float, nullable=True)
    trend_state = Column(String(32), nullable=True)  # BULLISH_STRUCTURE, BEARISH_STRUCTURE, RANGE
    breadth_ratio = Column(Float, nullable=True)  # Advances / (Advances + Declines)
    pcr = Column(Float, nullable=True)  # Put Call Ratio
    oi_change_pct = Column(Float, nullable=True)
    details = Column(JSON, nullable=True)


class NewsEvent(Base):
    """Financial news, official RBI/SEBI releases, and macro events."""
    __tablename__ = "news_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)  # Marketaux, RBI, NSE, SEBI
    url = Column(String(512), nullable=True)
    title = Column(String(512), nullable=False)
    publication_time = Column(DateTime, nullable=False, index=True)
    receive_time = Column(DateTime, default=utc_now)
    event_category = Column(String(64), default="MACRO")
    impact_level = Column(String(16), default="MEDIUM")  # LOW, MEDIUM, HIGH
    dedup_key = Column(String(128), unique=True, index=True)
    is_veto_active = Column(Boolean, default=False)


class Signal(Base):
    """Trading signals produced by the multi-index strategy engine."""
    __tablename__ = "signals"

    signal_id = Column(String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    setup_key = Column(String(64), nullable=True, unique=True, index=True)
    underlying = Column(String(32), nullable=False, index=True)
    side = Column(String(16), nullable=False)  # BUY_CE, BUY_PE
    score = Column(Float, nullable=False)  # Score out of 100
    strategy_version = Column(String(32), nullable=False, default="v1.0.0")
    status = Column(String(32), nullable=False, index=True, default="WATCH")
    # Status lifecycle: WATCH, ENTRY, ENTRY_MISSED, INVALIDATED, TP1_HIT, TP2_HIT, TP3_HIT, SL_HIT, CLOSED
    reasons = Column(JSON, nullable=False)  # Structured evidence reasons
    score_breakdown = Column(JSON, nullable=False)  # Category scores
    snapshot_id = Column(String(64), nullable=True)
    telegram_message_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=utc_now, index=True)
    expires_at = Column(DateTime, nullable=False)

    levels = relationship("SignalLevels", back_populates="signal", uselist=False, cascade="all, delete-orphan")
    events = relationship("SignalEvent", back_populates="signal", cascade="all, delete-orphan")


class SignalLevels(Base):
    """Executable prices and targets for a signal."""
    __tablename__ = "signal_levels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(String(64), ForeignKey("signals.signal_id"), nullable=False, unique=True)
    contract_symbol = Column(String(64), nullable=False)
    strike = Column(Float, nullable=False)
    expiry = Column(DateTime, nullable=False)
    entry_range_low = Column(Float, nullable=False)
    entry_range_high = Column(Float, nullable=False)
    max_entry_price = Column(Float, nullable=False)
    sl_price = Column(Float, nullable=False)
    underlying_invalidation = Column(Float, nullable=False)
    tp1_price = Column(Float, nullable=False)
    tp2_price = Column(Float, nullable=False)
    tp3_price = Column(Float, nullable=False)
    planned_rr_tp1 = Column(Float, nullable=False)
    current_bid = Column(Float, nullable=True)
    current_ask = Column(Float, nullable=True)
    delta = Column(Float, nullable=True)
    quote_timestamp = Column(DateTime, nullable=False)

    signal = relationship("Signal", back_populates="levels")


class PaperTrade(Base):
    """Paper execution tracking with fees, slippage, and net PnL."""
    __tablename__ = "paper_trades"

    trade_id = Column(String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    signal_id = Column(String(64), ForeignKey("signals.signal_id"), nullable=False, index=True)
    contract_symbol = Column(String(64), nullable=False)
    underlying = Column(String(32), nullable=False, index=True)
    direction = Column(String(16), nullable=False)  # BUY_CE, BUY_PE
    entry_time = Column(DateTime, nullable=False, index=True)
    entry_price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    lots = Column(Integer, nullable=False)
    current_price = Column(Float, nullable=False)
    sl_price = Column(Float, nullable=False)
    tp1_price = Column(Float, nullable=False)
    tp2_price = Column(Float, nullable=False)
    tp3_price = Column(Float, nullable=False)
    exit_time = Column(DateTime, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(String(32), nullable=True)  # SL, TP1, TP2, TP3, INVALIDATION, SESSION_CLOSE
    gross_pnl = Column(Float, default=0.0)
    fees = Column(Float, default=0.0)
    slippage = Column(Float, default=0.0)
    net_pnl = Column(Float, default=0.0)
    status = Column(String(16), default="OPEN", index=True)  # OPEN, CLOSED
    created_at = Column(DateTime, default=utc_now)


class SignalEvent(Base):
    """Audit log of all lifecycle events on a signal."""
    __tablename__ = "signal_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(String(64), ForeignKey("signals.signal_id"), nullable=False, index=True)
    event_type = Column(String(32), nullable=False)  # GENERATED, WATCH, TRIGGERED, TP1, TP2, TP3, SL, INVALIDATED, EXPIRED
    timestamp = Column(DateTime, default=utc_now, index=True)
    price = Column(Float, nullable=True)
    details = Column(JSON, nullable=True)

    signal = relationship("Signal", back_populates="events")


class RiskState(Base):
    """Daily session risk state and lockouts."""
    __tablename__ = "risk_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_date = Column(String(16), unique=True, nullable=False, index=True)  # YYYY-MM-DD
    starting_capital = Column(Float, nullable=False)
    realized_pnl = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    open_risk = Column(Float, default=0.0)
    consecutive_losses = Column(Integer, default=0)
    stop_outs_count = Column(Integer, default=0)
    trading_locked = Column(Boolean, default=False)
    lock_reason = Column(String(128), nullable=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)


class SystemHealthRecord(Base):
    """Historical logging of system health metrics."""
    __tablename__ = "system_health"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=utc_now, index=True)
    broker_name = Column(String(32), nullable=False)
    feed_status = Column(String(32), nullable=False)
    last_message_epoch = Column(Float, nullable=False)
    latency_ms = Column(Float, default=0.0)
    error_count = Column(Integer, default=0)
    is_healthy = Column(Boolean, default=True)


class StrategyVersion(Base):
    """Persisted strategy configurations and weights for complete auditability."""
    __tablename__ = "strategy_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_tag = Column(String(32), unique=True, nullable=False)
    name = Column(String(64), nullable=False)
    configuration = Column(JSON, nullable=False)
    scoring_weights = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utc_now)
