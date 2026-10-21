"""Configuration module for Multi-Index Indian Options Signal Bot.

Production-grade settings with validation, environment variable loading,
and per-index market configurations.
"""

from __future__ import annotations
import os
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IndexConfig(BaseModel):
    """Configuration for a specific traded Indian index."""
    name: str
    symbol: str
    exchange: str  # NSE or BSE
    segment: str = "INDEX"
    lot_size: int
    tick_size: float = 0.05
    strike_interval: float
    primary_expiry_day: str  # e.g. Thursday, Wednesday, Friday
    enabled: bool = True
    orb_period_minutes: int = 15
    atr_period: int = 14
    vwap_enabled: bool = True
    delta_target_min: float = 0.45
    delta_target_max: float = 0.65
    max_acceptable_spread_pct: float = 2.0  # Max (Ask - Bid) / Ask %
    min_volume_contracts: int = 1000
    min_oi_contracts: int = 5000
    max_underlying_move_pct: float = 2.0  # Max acceptable underlying move % required to reach TP1


class Settings(BaseSettings):
    """Global Application Settings."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )

    # General App Config
    APP_NAME: str = "Multi-Index Indian Options Signal Bot"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = "production"  # development, staging, production
    DEBUG: bool = True
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # Operating Mode - STRICTLY Signal & Paper Trading by default
    PAPER_TRADING_MODE: bool = True
    ENABLE_LIVE_ORDERS: bool = False  # Hard safety gate
    ALLOW_SIMULATED_ENTRIES: bool = False  # Disallow entries derived from simulated feeds in production
    ENFORCE_MARKET_HOURS: bool = False  # Live signals are restricted to the active exchange session

    DATABASE_URL: str = (
        "sqlite+aiosqlite:////tmp/trading.db"
        if os.getenv("VERCEL")
        else "sqlite+aiosqlite:///./trading.db"
    )
    # For PostgreSQL: "postgresql+asyncpg://user:password@localhost:5432/trading_db"

    # Primary Broker Adapter ('dhan', 'upstox', 'simulated')
    PRIMARY_BROKER: str = "dhan"
    FALLBACK_BROKER: Optional[str] = "upstox"

    # DhanHQ API Credentials
    DHAN_CLIENT_ID: str = ""
    DHAN_ACCESS_TOKEN: str = ""
    DHAN_BASE_URL: str = "https://api.dhan.co/v2"
    DHAN_WSS_URL: str = "wss://api-feed.dhan.co"

    # Upstox API Credentials
    UPSTOX_API_KEY: str = ""
    UPSTOX_API_SECRET: str = ""
    UPSTOX_ACCESS_TOKEN: str = ""
    UPSTOX_BASE_URL: str = "https://api.upstox.com/v2"
    UPSTOX_WSS_URL: str = "wss://api.upstox.com/v2/feed/market-data-feed"

    # Marketaux News API
    MARKETAUX_API_TOKEN: str = ""
    MARKETAUX_BASE_URL: str = "https://api.marketaux.com/v1"
    NEWS_CACHE_TTL_SECONDS: int = 600  # 10 minutes cache
    EVENT_RISK_BUFFER_MINUTES: int = 15  # Block signals 15m before/after high-impact macro event

    # Telegram Bot Service
    TELEGRAM_ENABLED: bool = True
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # Capital & Risk Parameters (Deterministic Safety Rules)
    ACCOUNT_CAPITAL: float = 500000.0  # ₹5,00,000 INR default paper capital
    RISK_PER_TRADE_PCT: float = 0.005   # 0.5% max risk per trade (₹2,500)
    ENABLE_DAILY_STOP_LOSS: bool = False
    MAX_DAILY_LOSS_PCT: Optional[float] = None
    MAX_DAILY_STOP_LOSSES: int = 2      # Max 2 consecutive/planned stop-outs (when daily stop enabled)
    MAX_CONCURRENT_TRADES: int = 2      # Max 2 open positions at any time
    MAX_CORRELATED_EXPOSURE: int = 1    # Max 1 trade per correlated index cluster (e.g. NIFTY + SENSEX)
    MAX_TRADE_CAPITAL_ALLOCATION_PCT: float = 0.20  # Max 20% capital in single options trade

    # Data Staleness & Latency Limits
    MAX_QUOTE_STALENESS_SECONDS: float = 15.0  # Max acceptable delay for live tick (ultra-low latency)
    HEARTBEAT_INTERVAL_SECONDS: int = 10
    WS_RECONNECT_MAX_ATTEMPTS: int = 10
    WS_RECONNECT_BASE_DELAY_SECONDS: float = 2.0

    # Staged Signal Engine Confidence Thresholds
    SCORE_EARLY_WATCH: float = 48.0
    SCORE_SETUP_DEVELOPING: float = 60.0
    SCORE_ENTRY_QUALIFIED: float = 72.0
    MIN_SCORE_FOR_ENTRY: float = 72.0   # Backwards compatibility alias
    MIN_SCORE_FOR_WATCH: float = 48.0   # Backwards compatibility alias
    SCANNER_INTERVAL_SECONDS: int = 2  # 2-second balanced autonomous scanner loop

    # Early breakout radar: more sensitive than entry qualification; alerts only.
    EARLY_WATCH_1M_PROXIMITY_PCT: float = 0.15
    EARLY_WATCH_1M_MIN_BODY_RATIO: float = 0.30
    EARLY_WATCH_5M_PROXIMITY_PCT: float = 0.25
    EARLY_WATCH_5M_MIN_BODY_RATIO: float = 0.30
    RADAR_MAX_SPREAD_PCT: float = 3.0
    RADAR_MIN_VOLUME_RATIO: float = 0.35
    RADAR_MIN_OI_RATIO: float = 0.50
    RADAR_MIN_VOLUME_CONTRACTS: int = 500
    RADAR_MIN_OI_CONTRACTS: int = 1000
    RADAR_DELTA_MIN: float = 0.40
    RADAR_DELTA_MAX: float = 0.70
    MIN_RVOL_FOR_DEVELOPING: float = 1.0
    MAX_BREAKOUT_EXTENSION_ATR: float = 2.0
    RVOL_STRONG_THRESHOLD: float = 1.5
    RVOL_MODERATE_THRESHOLD: float = 1.2
    CANDLE_BODY_STRONG_RATIO: float = 0.70
    CANDLE_BODY_WEAK_RATIO: float = 0.35

    # False Move & Absorption Risk Parameters
    ABSORPTION_RVOL_THRESHOLD: float = 1.8
    ABSORPTION_PRICE_THRESHOLD_PCT: float = 0.08
    WICK_REJECTION_THRESHOLD_PCT: float = 0.40
    CANDLE_BODY_RATIO_TARGET: float = 0.50
    MAX_FALSE_MOVE_RISK_FOR_ENTRY: str = "MODERATE"  # LOW or MODERATE allowed; HIGH rejected
    MIN_REWARD_TO_RISK_RATIO: float = 1.5
    # Slippage modeling: modeled explicitly PER SIDE (e.g. 0.5% on entry fill, 0.5% on exit fill)
    SLIPPAGE_ESTIMATE_PCT: float = 0.5
    # Total round-trip transaction costs per lot (brokerage + STT + exchange turnover fees)
    TRANSACTION_COST_PER_LOT: float = 40.0
    # Global default maximum underlying spot move % required to achieve TP1
    MAX_UNDERLYING_MOVE_PCT: float = 2.0

    # Option Targets & Stop-Loss Calculation Parameters
    DEFAULT_TICK_SIZE: float = 0.05        # NSE & BSE option tick size
    MIN_OPTION_RISK_PCT: float = 0.10     # 10% minimum premium risk floor
    MAX_OPTION_RISK_PCT: float = 0.35     # 35% maximum premium risk cap
    MIN_UNDERLYING_RISK_PCT: float = 0.0005  # 0.05% minimum underlying risk buffer
    TARGET_R1_RATIO: float = 1.5          # TP1 reward-to-risk ratio
    TARGET_R2_RATIO: float = 2.5          # TP2 reward-to-risk ratio
    TARGET_R3_RATIO: float = 4.0          # TP3 reward-to-risk ratio
    THETA_DECAY_BUFFER_PCT: float = 0.008 # 0.8% option premium theta decay buffer
    TP1_BOOKING_PCT: float = 0.80         # 80% position closed at TP1
    TP1_RUNNER_PCT: float = 0.20          # 20% position retained as runner to TP2

    # Wick & Structural Price Action Thresholds
    WICK_SCORING_PENALTY_PCT: float = 0.25      # 25% adverse wick ratio triggers confidence penalty
    WICK_STRUCTURAL_INVALIDATION_PCT: float = 0.60  # 60% adverse wick triggers structural invalidation
    WICK_FAVORABLE_THRESHOLD_PCT: float = 0.25  # 25% defense wick considered favorable

    # Market Breadth Confidence Thresholds
    BREADTH_BULLISH_STRONG_PCT: float = 70.0
    BREADTH_BULLISH_MODERATE_PCT: float = 55.0
    BREADTH_BEARISH_STRONG_PCT: float = 30.0
    BREADTH_BEARISH_MODERATE_PCT: float = 45.0

    # False Move Risk Score & Penalty Thresholds
    FALSE_MOVE_HIGH_RISK_THRESHOLD: float = 55.0
    FALSE_MOVE_MODERATE_RISK_THRESHOLD: float = 25.0
    FALSE_MOVE_HIGH_PENALTY: float = 20.0
    FALSE_MOVE_MODERATE_PENALTY: float = 8.0

    # Entry Trigger Modes: CONSERVATIVE (5m close), BALANCED (1m breakout + 5m context), FAST (validated tick/intrabar)
    ENTRY_TRIGGER_MODE: str = "BALANCED"
    FAST_MODE_MAX_STALENESS_SECONDS: float = 2.0
    FAST_MODE_MAX_SPREAD_RATIO: float = 0.8
    FAST_MODE_MIN_TICK_PERSISTENCE: int = 3

    # Lifecycle Safety & Re-Entry Protection
    ALLOW_SIGNAL_REENTRY: bool = False  # Strict default: 1 paper entry per setup
    RESET_COOLDOWN_MINUTES: int = 20    # Cooldown before reset can register a new setup after invalidation/expiry/exit

    # Default Per-Index Definitions (Verified contract defaults for NSE & BSE)
    INDICES: Dict[str, IndexConfig] = Field(
        default_factory=lambda: {
            "NIFTY": IndexConfig(
                name="NIFTY 50",
                symbol="NIFTY",
                exchange="NSE",
                lot_size=65,  # Revised NSE standard contract lot
                tick_size=0.05,
                strike_interval=50.0,
                primary_expiry_day="Thursday",
                enabled=True,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=1.8,
                min_volume_contracts=2500,
                min_oi_contracts=10000,
                max_underlying_move_pct=1.5,
            ),
            "BANKNIFTY": IndexConfig(
                name="BANK NIFTY",
                symbol="BANKNIFTY",
                exchange="NSE",
                lot_size=30,  # Verified current standard contract lot
                tick_size=0.05,
                strike_interval=100.0,
                primary_expiry_day="Wednesday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=2.0,
                min_volume_contracts=2000,
                min_oi_contracts=8000,
                max_underlying_move_pct=2.0,
            ),
            "FINNIFTY": IndexConfig(
                name="NIFTY FINANCIAL SERVICES",
                symbol="FINNIFTY",
                exchange="NSE",
                lot_size=60,
                tick_size=0.05,
                strike_interval=50.0,
                primary_expiry_day="Tuesday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=2.2,
                min_volume_contracts=1000,
                min_oi_contracts=5000,
                max_underlying_move_pct=2.0,
            ),
            "MIDCPNIFTY": IndexConfig(
                name="NIFTY MIDCAP SELECT",
                symbol="MIDCPNIFTY",
                exchange="NSE",
                lot_size=120,
                tick_size=0.05,
                strike_interval=25.0,
                primary_expiry_day="Monday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=2.5,
                min_volume_contracts=1000,
                min_oi_contracts=4000,
                max_underlying_move_pct=2.0,
            ),
            "SENSEX": IndexConfig(
                name="BSE SENSEX",
                symbol="SENSEX",
                exchange="BSE",
                lot_size=20,  # Verified BSE SENSEX contract lot
                tick_size=0.05,
                strike_interval=100.0,
                primary_expiry_day="Friday",
                enabled=True,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=2.5,
                min_volume_contracts=1500,
                min_oi_contracts=5000,
                max_underlying_move_pct=1.5,
            ),
            "BANKEX": IndexConfig(
                name="BSE BANKEX",
                symbol="BANKEX",
                exchange="BSE",
                lot_size=15,
                tick_size=0.05,
                strike_interval=100.0,
                primary_expiry_day="Monday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=3.0,
                min_volume_contracts=500,
                min_oi_contracts=2000,
                max_underlying_move_pct=2.0,
            ),
            "CRUDEOIL": IndexConfig(
                name="MCX CRUDE OIL",
                symbol="CRUDEOIL",
                exchange="MCX",
                segment="COMMODITY",
                lot_size=100,
                tick_size=1.00,
                strike_interval=50.0,
                primary_expiry_day="Friday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=2.5,
                min_volume_contracts=500,
                min_oi_contracts=2000,
                max_underlying_move_pct=3.5,
            ),
            "NATURALGAS": IndexConfig(
                name="MCX NATURAL GAS",
                symbol="NATURALGAS",
                exchange="MCX",
                segment="COMMODITY",
                lot_size=1250,
                tick_size=0.10,
                strike_interval=5.0,
                primary_expiry_day="Wednesday",
                enabled=False,
                orb_period_minutes=15,
                atr_period=14,
                delta_target_min=0.45,
                delta_target_max=0.65,
                max_acceptable_spread_pct=3.0,
                min_volume_contracts=500,
                min_oi_contracts=1500,
                max_underlying_move_pct=5.0,
            ),
        }
    )


# Singleton settings instance
settings = Settings()
