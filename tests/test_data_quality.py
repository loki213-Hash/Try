"""Unit and integration tests for data quality, staleness, candle aggregation, and indicators."""

import pytest
import time
from datetime import datetime, timezone, timedelta
import pandas as pd

from app.data.base_provider import UnifiedQuote, UnifiedCandle
from app.data.quote_validator import QuoteValidator
from app.data.candle_builder import CandleAggregator
from app.analysis.indicators import TechnicalIndicators


def test_quote_validator_rejects_zero_or_negative_price():
    validator = QuoteValidator(max_staleness_seconds=5.0)
    now = datetime.now(timezone.utc)
    bad_quote = UnifiedQuote(
        instrument_id="TEST_1",
        symbol="NIFTY",
        exchange="NSE",
        ltp=0.0,
        exchange_timestamp=now,
    )
    res = validator.validate_quote(bad_quote, is_live_market=False)
    assert not res.is_valid
    assert any("Invalid LTP" in r for r in res.reasons)


def test_quote_validator_rejects_crossed_book():
    validator = QuoteValidator(max_staleness_seconds=5.0)
    now = datetime.now(timezone.utc)
    crossed_quote = UnifiedQuote(
        instrument_id="TEST_2",
        symbol="NIFTY",
        exchange="NSE",
        ltp=25000.0,
        bid=25010.0,
        ask=25005.0,  # Bid > Ask
        exchange_timestamp=now,
    )
    res = validator.validate_quote(crossed_quote, is_live_market=False)
    assert not res.is_valid
    assert any("Crossed order book" in r for r in res.reasons)


def test_quote_validator_detects_staleness():
    validator = QuoteValidator(max_staleness_seconds=5.0)
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=15)
    stale_quote = UnifiedQuote(
        instrument_id="TEST_3",
        symbol="NIFTY",
        exchange="NSE",
        ltp=25000.0,
        bid=24999.0,
        ask=25001.0,
        exchange_timestamp=stale_time,
    )
    res = validator.validate_quote(stale_quote, is_live_market=True)
    assert not res.is_valid
    assert any("Data stale" in r for r in res.reasons)


def test_candle_aggregator_1m_to_5m_roll():
    agg = CandleAggregator(instrument_id="NIFTY_IDX", symbol="NIFTY")
    base_time = datetime(2026, 10, 12, 9, 15, 0, tzinfo=timezone.utc)

    # Ingest ticks across 6 consecutive minutes
    for minute in range(6):
        t = base_time + timedelta(minutes=minute, seconds=10)
        q = UnifiedQuote(
            instrument_id="NIFTY_IDX",
            symbol="NIFTY",
            exchange="NSE",
            ltp=25000.0 + (minute * 10),
            volume=1000,
            exchange_timestamp=t,
        )
        agg.process_tick(q)

    # Should have finalized 5 one-minute bars and exactly 1 five-minute bar
    assert len(agg.completed_1m) == 5
    assert len(agg.completed_5m) == 1
    bar_5m = agg.completed_5m[0]
    assert bar_5m.timeframe == "5m"
    assert bar_5m.open == 25000.0
    assert bar_5m.close == 25040.0
    assert bar_5m.volume == 5000


def test_indicator_calculations():
    base_time = datetime(2026, 10, 12, 9, 15, 0, tzinfo=timezone.utc)
    candles = [
        UnifiedCandle(
            instrument_id="TEST",
            symbol="NIFTY",
            timeframe="5m",
            timestamp=base_time + timedelta(minutes=i * 5),
            open=25000.0 + i * 5,
            high=25010.0 + i * 5,
            low=24995.0 + i * 5,
            close=25005.0 + i * 5,
            volume=10000 + i * 500,
        )
        for i in range(25)
    ]
    df = TechnicalIndicators.candles_to_dataframe(candles)
    assert len(df) == 25

    # VWAP
    vwap = TechnicalIndicators.calculate_vwap(df)
    assert not vwap.empty
    assert vwap.iloc[-1] > 25000.0

    # EMA
    ema_9 = TechnicalIndicators.calculate_ema(df, period=9)
    assert not ema_9.empty
    assert ema_9.iloc[-1] > ema_9.iloc[0]

    # ATR
    atr = TechnicalIndicators.calculate_atr(df, period=14)
    assert not atr.empty
    assert atr.iloc[-1] > 0.0

    # RVOL
    rvol = TechnicalIndicators.calculate_rvol(df, window=10)
    assert rvol > 0.0
