# Multi-Index Indian Options Signal Bot

A production-structured, multi-index Indian index-options signal and paper-trading bot written in Python using FastAPI, PostgreSQL/SQLite, asynchronous WebSockets, REST APIs, and the Telegram Bot API.

> **CRITICAL OPERATING BOUNDARY:**
> This system is strictly configured for **Signal-Only** and **Paper-Trading** by default (`ENABLE_LIVE_ORDERS=false`). It monitors the markets continuously, validates prices, tracks simulated executions, and measures actual edge. It **never places live exchange orders**.

---

## 1. Monitored Benchmark Indices

The engine runs independent, parallel strategy workers for each index:

| Index | Exchange | Underlying Symbol | Standard Lot Size | Strike Step | Default Expiry Day |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **NIFTY 50** | NSE | `NIFTY` | 75 | 50.0 | Thursday |
| **BANK NIFTY** | NSE | `BANKNIFTY` | 30 | 100.0 | Wednesday |
| **FINNIFTY** | NSE | `FINNIFTY` | 65 | 50.0 | Tuesday |
| **MIDCPNIFTY** | NSE | `MIDCPNIFTY` | 75 | 25.0 | Monday |
| **BSE SENSEX** | BSE | `SENSEX` | 20 | 100.0 | Friday |
| **BSE BANKEX** | BSE | `BANKEX` | 15 | 100.0 | Monday |

---

## 2. Broker Provider Assessment & API Analysis

### DhanHQ (Primary Adapter)
- **Official Documentation**: [https://docs.dhanhq.co/](https://docs.dhanhq.co/) & [https://dhanhq.co/docs/v2/option-chain/](https://dhanhq.co/docs/v2/option-chain/)
- **Capabilities Verified**:
  - **NSE & BSE Coverage**: Indexes and index option derivatives for all 6 target indices.
  - **Instrument Master**: Daily CSV scrip master downloaded from `https://images.dhan.co/api-data/api-scrip-master.csv`.
  - **Live WebSocket Feed**: Binary protocol with Request Codes 15 (Ticker), 17 (Quote), and 21 (Depth).
  - **Option Chain REST API**: Endpoint `/v2/option-chain` provides strikes, CE/PE quotes, volume, open interest (OI), implied volatility (IV), and Greeks (Delta, Theta, Gamma, Vega).
  - **Rate Limits**: 5 REST requests per second; WebSocket up to 5 concurrent connections.
  - **Fallback / Simulation**: High-fidelity simulator built-in when credentials are not supplied.

### Upstox (Secondary Adapter)
- **Official Documentation**: [https://upstox.com/developer/api-documentation/v3/get-market-data-feed/](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/)
- **Capabilities Verified**:
  - **V3 Feed**: Protobuf binary streaming market feed.
  - **Market Quotes**: Bid/Ask depth, LTP, volume, and OI via `/v2/market-quote/quotes`.
  - **Option Chain**: Expiry and strike ladder via `/v2/option/chain`.
  - **Rate Limits**: 25 requests per second for REST.

### Marketaux (News Context & Macro Calendar)
- **Published Free Tier**: 100 requests per day, up to 3 articles per request.
- **Engine Handling**: 10-minute cache TTL, SHA-256 title/URL deduplication, and scheduled macro calendar (RBI MPC, Union Budget, US Fed FOMC) that applies an independent **Event Risk Veto** within ±15 minutes of scheduled announcements.

---

## 3. Production Architecture

```
C:\Users\lokes\Desktop\Trading\
├── main.py                    # FastAPI entrypoint, lifespan startup, WebSocket & scanner loop
├── config.py                  # Pydantic Settings & per-index configurations
├── requirements.txt           # Python dependencies
├── .env.example               # Template environment configuration
├── static/
│   └── index.html             # Real-time interactive dark-theme dashboard
├── app/
│   ├── api/
│   │   ├── health.py          # /api/health endpoint
│   │   └── dashboard.py       # /api/indices, /api/signals, /api/trades, /api/risk, /api/metrics
│   ├── data/
│   │   ├── base_provider.py   # Normalized data models & abstract provider interface
│   │   ├── dhan_provider.py   # Official DhanHQ V2 adapter
│   │   ├── upstox_provider.py # Upstox V3 adapter
│   │   ├── instrument_master.py # Dynamic strike resolution & lot catalog
│   │   ├── quote_validator.py # Staleness (<5s), bid/ask, and spread verification
│   │   ├── candle_builder.py  # 1m tick aggregation into 5m and 15m completed bars
│   │   └── websocket_manager.py # Reconnects, backoff, and heartbeat manager
│   ├── analysis/
│   │   ├── price_action.py    # ORB, PDH/PDL/PDC, 15m structure, and wick rejection
│   │   ├── indicators.py      # VWAP, EMA (9, 21, 50, 200), ATR, and RVOL
│   │   ├── breadth.py         # Advance/Decline ratio & constituent momentum
│   │   ├── futures_oi.py      # PCR & OI buildup (Long/Short Buildup, Short Covering)
│   │   ├── option_chain.py    # Max Pain, Call/Put OI resistance walls
│   │   ├── volatility.py      # Black-Scholes Greeks & target premium projection
│   │   └── news_engine.py     # Marketaux cached news & macro event risk vetoes
│   ├── signals/
│   │   ├── detector.py        # Independent multi-index strategy detector & ranking
│   │   ├── confidence.py      # Transparent 100-point score & mandatory gates
│   │   ├── contract_selector.py # Liquid ATM/ITM selection (Delta 0.45 - 0.65)
│   │   ├── targets.py         # Underlying invalidation, option SL, and TP1-TP3
│   │   ├── risk_manager.py    # 0.5% risk budget, 1% daily stop, correlation locks, fees
│   │   └── state_machine.py   # WATCH -> ENTRY -> TP1/TP2/TP3 / SL / CLOSED
│   ├── notifications/
│   │   └── telegram.py        # Telegram Bot API alerts with deduplication & editing
│   ├── tracking/
│   │   ├── paper_trader.py    # Paper execution, fee/slippage deduction, PnL tracking
│   │   ├── outcomes.py        # MFE, MAE, and out-of-sample excursion tracker
│   │   └── performance.py     # Win rate, profit factor, expectancy, max drawdown
│   ├── database/
│   │   ├── models.py          # SQLAlchemy async models for 12 tables
│   │   ├── repository.py      # Async database CRUD repository
│   │   └── migrations/
│   │       └── env.py         # Database migration runner
│   └── monitoring/
│       ├── health.py          # Health monitor tracking uptime, staleness, and errors
│       └── logging.py         # Structured logging with automatic secret masking
└── tests/
    ├── test_data_quality.py   # Staleness, crossed books, indicators, candle rolling
    ├── test_signals.py        # Scoring breakdown, ORB detection, wick rejection
    ├── test_contracts.py      # ATM selection, Delta filters, defensible targets
    ├── test_risk.py           # Position sizing, 2-stop loss lock, correlation blocks, fees
    ├── test_telegram.py       # Telegram format, state transitions, deduplication
    └── test_backtest.py       # Sequential bar replay without look-ahead bias
```

---

## 4. Confidence Scoring & Mandatory Gates

Signals are evaluated against a transparent 100-point evidence score:

1. **Trend & Market Structure**: Max **25** pts (15m higher highs / lower lows)
2. **Entry Confirmation**: Max **20** pts (5m decisive body close without rejection)
3. **Volume & Participation**: Max **15** pts (RVOL ≥ 1.2x – 1.5x)
4. **Market Breadth**: Max **10** pts (Advance/Decline ratio aligned)
5. **Futures & Options Positioning**: Max **10** pts (PCR & OI buildup aligned)
6. **Contract Liquidity & Spread**: Max **10** pts (Spread < 1.5% of premium)
7. **Volatility & RR Suitability**: Max **10** pts (Expected RR ≥ 1:1.5)

**Total**: **100** points.
- **Score ≥ 80.0**: Eligible for **ENTRY** if all gates pass.
- **Score 65.0 – 79.9**: Set to **WATCH**.
- **Score < 65.0**: **REJECTED** (Explicit NO TRADE).

### Non-Negotiable Hard Gates (Independent of Score)
- **Data Freshness Gate**: Stale quote (>5s delay) blocks entry.
- **Spread Gate**: Wide bid/ask spread blocks entry.
- **Event Risk Gate**: High-impact macro events within ±15 minutes block entry.
- **Daily Capital Stop**: Realized loss ≥ 1.0% or 2 consecutive planned stop-losses locks trading for the day.
- **Defensible Targets Gate**: Inability to calculate 3 defensible targets suppresses the signal.

---

## 5. Quickstart & Installation (Windows / Linux / macOS)

### Step 1: Create Virtual Environment
```powershell
# Navigate to the folder
cd C:\Users\lokes\Desktop\Trading

# Create virtual environment
py -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1
```

### Step 2: Install Dependencies
```powershell
pip install -r requirements.txt
```

### Step 3: Configure Environment
Copy `.env.example` to `.env`:
```powershell
Copy-Item .env.example .env
```
Fill in your broker credentials (Dhan / Upstox) and Telegram details. If left blank, the bot automatically runs in **high-fidelity simulation mode**.

### Step 4: Initialize Database
```powershell
python app/database/migrations/env.py
```

### Step 5: Run the Full Test Suite
```powershell
pytest tests -v
```
All 18 unit, integration, and backtest replay tests will execute.

### Step 6: Start the Server & Live Dashboard
```powershell
python main.py
```
Open your browser at **[http://localhost:8000/](http://localhost:8000/)** to view the live dashboard.

---

## 6. Real-Time Web Dashboard

The web dashboard is served directly by FastAPI at `/`:
- **Live System Health**: Broker feed status, quote latency, database connection, and Telegram status.
- **6-Index Watch Grid**: SENSEX, BANKEX, NIFTY, BANKNIFTY, FINNIFTY, and MIDCPNIFTY status cards.
- **Active Signals Feed**: Signal badges, setup score breakdown (out of 100), calculated entry range, SL, TP1, TP2, TP3, and underlying invalidation levels.
- **Paper Executions Table**: Executed paper trades, current live prices, gross P&L, regulatory fees, slippage, and net P&L.
- **Risk & Capital Meter**: Daily stop-loss limit monitor, stop-out counter, and session lock indicator.
- **Performance Analytics**: Win rate (%), profit factor, expectancy per trade, and maximum drawdown.
- **Auto-Refresh**: Live data refreshes every 3 seconds via asynchronous REST APIs.

---

## 7. Telegram Alert Format

Telegram messages are formatted with full auditability and paper trading notices:

```
🚨 MULTI-INDEX OPTIONS SIGNAL (PAPER)
Status: ENTRY

Index: NIFTY — BUY CE
Strike / Expiry: NIFTY24OCT25100CE
Live Bid / Ask: ₹149.50 / ₹150.00
Entry Range: ₹149.00 – ₹150.00
Max Acceptable Entry: ₹152.25
Setup Score: 84.5/100

LEVELS & RISK:
• Stop-Loss: ₹130.00
• TP1 (1:1.5 RR): ₹180.00
• TP2 (1:2.5 RR): ₹200.00
• TP3 (1:4.0 RR): ₹230.00
• Underlying Invalidation: 25080.00
• Risk per Lot: ₹1,500.00

EVIDENCE & REASONS:
• Confirmed 5-min candle closed above ORB High (25100.0)
• Price firmly above VWAP
• 15m structure: BULLISH_STRUCTURE
• RVOL: 1.5x baseline

⚠️ RISK: Paper simulation only. Actual fills may differ.
Signal ID: SIG-NIFTY-120000-TEST01 | Ver: v1.0.0
```

---

## 8. License & Disclaimer

This software is for educational, research, and paper-trading purposes only. Financial derivatives involve substantial risk. Past performance or backtest results do not guarantee future profitability.
