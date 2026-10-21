"""Technical indicators engine using pandas and numpy.

Calculates:
- VWAP (Volume Weighted Average Price)
- Exponential Moving Averages (EMA 9, 21, 50, 200)
- ATR (Average True Range)
- Relative Volume (RVOL) compared to intraday baseline
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from app.data.base_provider import UnifiedCandle


class TechnicalIndicators:
    """Calculates pure technical metrics across timeframes."""

    @staticmethod
    def candles_to_dataframe(candles: List[UnifiedCandle]) -> pd.DataFrame:
        """Converts normalized candles into a pandas DataFrame."""
        if not candles:
            return pd.DataFrame()
        records = [
            {
                "timestamp": c.timestamp,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "oi": c.oi,
            }
            for c in candles
        ]
        df = pd.DataFrame(records)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """Calculates intraday Volume Weighted Average Price (VWAP) reset daily at session open."""
        if df.empty or "volume" not in df.columns:
            return pd.Series(dtype=float)

        # If total volume is 0 (e.g. index cash spot), VWAP cannot be accurately derived
        if (df["volume"] == 0).all() or df["volume"].sum() <= 0:
            return pd.Series([np.nan] * len(df), index=df.index, dtype=float)

        df_calc = df.copy()
        typical_price = (df_calc["high"] + df_calc["low"] + df_calc["close"]) / 3.0
        pv = typical_price * df_calc["volume"]

        if "timestamp" in df_calc.columns:
            ts = pd.to_datetime(df_calc["timestamp"])
            try:
                if ts.dt.tz is not None:
                    group_key = ts.dt.tz_convert("Asia/Kolkata").dt.date
                else:
                    group_key = ts.dt.date
            except Exception:
                group_key = ts.dt.date

            cum_pv = pv.groupby(group_key).cumsum()
            cum_volume = df_calc["volume"].groupby(group_key).cumsum()
        else:
            cum_pv = pv.cumsum()
            cum_volume = df_calc["volume"].cumsum()

        vwap = cum_pv / cum_volume.replace(0, np.nan)
        return vwap.ffill()

    @staticmethod
    def calculate_ema(df: pd.DataFrame, period: int) -> pd.Series:
        """Calculates Exponential Moving Average for a given period."""
        if df.empty or len(df) < period:
            return pd.Series([np.nan] * len(df), dtype=float)
        return df["close"].ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculates Wilder's smoothed Average True Range (ATR)."""
        if df.empty or len(df) < 2:
            return pd.Series([0.0] * len(df), dtype=float)

        high_low = df["high"] - df["low"]
        high_prev_close = (df["high"] - df["close"].shift(1)).abs()
        low_prev_close = (df["low"] - df["close"].shift(1)).abs()

        true_range = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
        # Wilder's smoothing alpha = 1 / period
        atr = true_range.ewm(alpha=1.0 / max(period, 1), min_periods=min(period, len(df)), adjust=False).mean()
        return atr.bfill().fillna(true_range)

    @staticmethod
    def calculate_rvol(df: pd.DataFrame, window: int = 20) -> float:
        """Calculates Relative Volume (RVOL) of current candle vs rolling average."""
        if df.empty or len(df) < 2 or "volume" not in df.columns:
            return 0.0
        recent_vol = df["volume"].iloc[-1]
        if recent_vol <= 0:
            return 0.0
        prior = df["volume"].iloc[:-1]
        if prior.empty:
            return 0.0
        baseline_vol = prior.tail(window).mean()
        if baseline_vol <= 0 or np.isnan(baseline_vol):
            return 0.0
        return round(float(recent_vol / baseline_vol), 2)
