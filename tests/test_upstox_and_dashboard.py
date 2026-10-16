"""Unit and Integration Tests for Upstox Provider and Clean Dashboard Endpoints.

Verifies:
1. Upstox quote parser handles None types gracefully.
2. Dashboard /api/indices filters strictly to enabled benchmarks (NIFTY & SENSEX).
3. Dashboard /api/options returns clean live action cards without mock TP/SL.
4. Active expiry dates match dynamic exchange schedule.
"""

import pytest
from datetime import datetime, timezone
from app.data.upstox_provider import UpstoxProvider
from app.api.dashboard import (
    get_monitored_indices,
    get_options_watchlist,
    set_market_provider,
    get_paper_trades,
    close_single_paper_trade,
    format_datetime_ist,
)
from config import settings


@pytest.mark.asyncio
async def test_upstox_quote_parser_none_handling():
    provider = UpstoxProvider(access_token="dummy_token")
    # Simulate Upstox quote response where volume and oi are None (typical for index quotes)
    mock_data = {
        "last_price": 22622.7,
        "ohlc": {"open": 22700.0, "high": 22750.0, "low": 22550.0, "close": 22622.7},
        "depth": {"buy": [], "sell": []},
        "volume": None,
        "oi": None,
        "symbol": "NSE_INDEX|Nifty 50",
    }
    quote = provider._parse_upstox_quote("NIFTY", mock_data)
    assert quote is not None
    assert quote.ltp == 22622.7
    assert quote.volume == 0
    assert quote.oi == 0
    assert quote.bid == round(22622.7 - 0.25, 2)
    assert quote.ask == round(22622.7 + 0.25, 2)


@pytest.mark.asyncio
async def test_dashboard_indices_returns_monitored_contracts():
    provider = UpstoxProvider()
    set_market_provider(provider)
    result = await get_monitored_indices()

    indices = result.get("indices", {})
    # Enabled benchmarks: NIFTY & SENSEX only
    assert "NIFTY" in indices
    assert "SENSEX" in indices
    # Disabled commodities and secondary indices
    assert "CRUDEOIL" not in indices
    assert "NATURALGAS" not in indices
    assert "BANKNIFTY" not in indices
    assert "FINNIFTY" not in indices
    assert "MIDCPNIFTY" not in indices
    assert "BANKEX" not in indices
    assert result["count"] == 2

    # Check that live spot and strikes are populated
    for sym in ("NIFTY", "SENSEX"):
        data = indices[sym]
        assert data["ltp"] > 0
        assert data["atm_strike"] > 0
        assert data["atm_ce_ltp"] > 0
        assert data["atm_pe_ltp"] > 0
        assert data["enabled"] is True


@pytest.mark.asyncio
async def test_dashboard_options_watchlist_clean_live_action():
    provider = UpstoxProvider()
    set_market_provider(provider)

    for und in ("NIFTY", "SENSEX"):
        res = await get_options_watchlist(und)
        assert res["underlying"] == und
        assert res["spot_price"] > 0
        assert len(res["contracts"]) > 0

        # Validate that EVERY contract is pure live action (NO TP/SL)
        for c in res["contracts"]:
            assert "symbol" in c
            assert und in c["symbol"]
            assert c["option_type"] in ("CE", "PE")
            assert c["side"] in ("CALL", "PUT")
            assert c["ltp"] > 0
            assert "bid" in c
            assert "ask" in c
            assert "volume" in c
            assert "oi" in c
            assert "delta" in c
            assert "iv" in c
            # Strict requirement: Watchlist cards MUST NOT have TP/SL
            assert "tp1" not in c
            assert "tp2" not in c
            assert "tp3" not in c
            assert "sl" not in c
            assert "rr_ratio" not in c
            assert "entry_range" not in c


@pytest.mark.asyncio
async def test_mcx_isolation_bypasses_upstox_equity_api():
    """Verifies MCX Commodities (CRUDEOIL, NATURALGAS) never make failing Upstox REST calls."""
    provider = UpstoxProvider(access_token="test_token")
    # Quote snapshot for commodities
    crude_quote = await provider.get_quote_snapshot("CRUDEOIL")
    assert crude_quote is not None
    assert crude_quote.exchange == "MCX"
    assert crude_quote.ltp > 0
    assert crude_quote.source == "upstox_sim"

    natgas_quote = await provider.get_quote_snapshot("NATURALGAS")
    assert natgas_quote is not None
    assert natgas_quote.exchange == "MCX"
    assert natgas_quote.ltp > 0

    # Active expiry
    crude_exp = await provider.get_active_expiry("CRUDEOIL")
    assert crude_exp is not None

    # Option chain snapshot
    chain = await provider.get_option_chain_snapshot("CRUDEOIL")
    assert chain is not None
    assert chain.underlying == "CRUDEOIL"
    assert len(chain.contracts) > 0


@pytest.mark.asyncio
async def test_dashboard_response_caching_and_ttl():
    """Verifies /api/indices and /api/options return fast cached responses within TTL."""
    provider = UpstoxProvider()
    set_market_provider(provider)

    # First call primes cache
    res1 = await get_monitored_indices()
    ts1 = res1["timestamp"]

    # Second call returns identical cached dictionary without recomputing
    res2 = await get_monitored_indices()
    assert res2["timestamp"] == ts1

    # Same for options
    opt1 = await get_options_watchlist("NIFTY")
    opt2 = await get_options_watchlist("NIFTY")
    assert opt1["spot_price"] == opt2["spot_price"]


@pytest.mark.asyncio
async def test_upstox_streaming_quote_emission():
    """Verifies that quotes emitted by UpstoxProvider reach registered listeners."""
    provider = UpstoxProvider()
    emitted_quotes = []

    def on_quote(q):
        emitted_quotes.append(q)

    provider.register_quote_callback(on_quote)

    # Verify subscribe launches subscription
    await provider.subscribe_quotes(["IDX_NIFTY", "IDX_SENSEX"])
    assert "IDX_NIFTY" in provider._subscribed_ids
    assert "IDX_SENSEX" in provider._subscribed_ids

    # Simulate quote emission
    quote = await provider.get_quote_snapshot("NIFTY")
    assert quote is not None
    await provider._emit_quote(quote)

    assert len(emitted_quotes) == 1
    assert emitted_quotes[0].symbol == "NIFTY"

    await provider.disconnect()
    assert provider.is_connected is False


@pytest.mark.asyncio
async def test_pooled_http_client_reuse():
    """Verifies _get_client reuses the same client instance without recreating."""
    provider = UpstoxProvider(access_token="test_token")
    client1 = await provider._get_client()
    client2 = await provider._get_client()
    assert client1 is client2
    assert not client1.is_closed
    await provider.disconnect()
    assert provider._client is None


@pytest.mark.asyncio
async def test_upstox_streamer_tick_message_processing():
    """Verifies MarketDataStreamerV3 message ticks update quotes and dispatch asynchronously."""
    import asyncio
    provider = UpstoxProvider(access_token="dummy_token")
    provider._loop = asyncio.get_running_loop()

    dispatched = []
    provider.register_quote_callback(lambda q: dispatched.append(q))

    # Pre-register option mapping
    opt_key = "NSE_FO|47029"
    opt_symbol = "NIFTY 15 OCT 25000 CE"
    provider._key_to_symbol[opt_key] = opt_symbol

    mock_msg = {
        "feeds": {
            "NSE_INDEX|Nifty 50": {
                "fullFeed": {
                    "indexFF": {
                        "ltpc": {"ltp": 23045.5, "cp": 22622.7},
                        "marketOHLC": {
                            "ohlc": [
                                {"interval": "1d", "open": 22650.0, "high": 23060.0, "low": 22640.0, "close": 23045.5}
                            ]
                        }
                    }
                }
            },
            opt_key: {
                "fullFeed": {
                    "marketFF": {
                        "ltpc": {"ltp": 258.5, "cp": 240.0},
                        "marketLevel": {
                            "bidAskQuote": [
                                {"bidP": 258.4, "askP": 258.6, "bidQ": "150", "askQ": "225"}
                            ]
                        },
                        "oi": 85000,
                        "marketOHLC": {
                            "ohlc": [
                                {"interval": "1d", "open": 242.0, "high": 265.0, "low": 238.0, "close": 258.5}
                            ]
                        }
                    }
                }
            }
        }
    }

    provider._on_streamer_message(mock_msg)
    # Yield control to let threadsafe dispatched tasks execute
    await asyncio.sleep(0.05)

    # Verify NIFTY index quote
    nifty_quote = provider._last_quotes.get("NIFTY")
    assert nifty_quote is not None
    assert nifty_quote.ltp == 23045.5
    assert nifty_quote.open == 22650.0
    assert nifty_quote.high == 23060.0
    assert nifty_quote.low == 22640.0

    # Verify Option contract quote
    opt_quote = provider._last_quotes.get(opt_symbol)
    assert opt_quote is not None
    assert opt_quote.ltp == 258.5
    assert opt_quote.bid == 258.4
    assert opt_quote.ask == 258.6
    assert opt_quote.oi == 85000

    # Verify quotes reached registered listener
    symbols_dispatched = [q.symbol for q in dispatched]
    assert "NIFTY" in symbols_dispatched
    assert opt_symbol in symbols_dispatched


@pytest.mark.asyncio
async def test_paper_trades_live_price_and_invested_value():
    """Verifies /api/trades calculates invested_amount and current_value with live quotes."""
    from app.api.dashboard import get_paper_trades, set_market_provider
    from app.tracking.paper_trader import paper_trading_engine
    from app.database.models import PaperTrade

    provider = UpstoxProvider(access_token="dummy_token")
    set_market_provider(provider)

    test_sym = "NIFTY 15 OCT 25000 CE"
    # Seed live quote in provider cache (as if arrived from WebSocket)
    from app.data.base_provider import UnifiedQuote
    provider._last_quotes[test_sym] = UnifiedQuote(
        instrument_id="NSE_FO|47029",
        symbol=test_sym,
        exchange="NSE",
        ltp=260.0,
        bid=259.8,
        ask=260.2,
        exchange_timestamp=datetime.now(timezone.utc),
        source="upstox_ws",
    )

    # Create dummy open trade
    trade = PaperTrade(
        trade_id="TR-TEST-001",
        signal_id="SIG-001",
        underlying="NIFTY",
        contract_symbol=test_sym,
        direction="BUY_CE",
        entry_price=250.0,
        lots=2,
        quantity=150,
        sl_price=230.0,
        tp1_price=275.0,
        tp2_price=300.0,
        tp3_price=330.0,
        status="OPEN",
    )
    paper_trading_engine._active_trades["TR-TEST-001"] = trade

    data = await get_paper_trades()
    open_trades = data.get("open_trades", [])
    found = next((t for t in open_trades if t["trade_id"] == "TR-TEST-001"), None)

    assert found is not None
    assert found["entry_price"] == 250.0
    assert found["current_price"] == 260.0
    # Invested = entry_price * quantity = 250 * 150 = 37,500.00
    assert found["invested_amount"] == 37500.0
    # Current Value = 260 * 150 = 39,000.00
    assert found["current_value"] == 39000.0
    # Unrealized = 10 * 150 = 1,500.00
    assert found["unrealized_pnl"] == 1500.0

    # Cleanup
    paper_trading_engine.remove_trade_by_signal_id("SIG-001")


def test_frontend_html_has_realtime_websocket_table_hook():
    """Verifies static/index.html includes continuous WebSocket table rendering."""
    import os
    html_path = os.path.join(settings.BASE_DIR if hasattr(settings, "BASE_DIR") else ".", "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Must contain renderTradesTable function
    assert "function renderTradesTable(openTrades, closedTrades)" in content
    # wsLive.onmessage must trigger renderTradesTable
    assert "renderTradesTable(data.open_trades, data.closed_trades)" in content
    # Polling interval is reduced/optimized to 5s
    assert "setInterval(updateDashboard, 5000)" in content


@pytest.mark.asyncio
async def test_close_paper_trade_manual_and_history():
    from app.tracking.paper_trader import paper_trading_engine
    from app.database.models import PaperTrade, utc_now

    trade = PaperTrade(
        trade_id="TR-TEST-CLOSE-99",
        signal_id="SIG-CLOSE-99",
        contract_symbol="NIFTY 24 OCT 25000 CALL",
        underlying="NIFTY",
        direction="BUY_CE",
        entry_time=utc_now(),
        entry_price=150.0,
        quantity=75,
        lots=1,
        current_price=175.0,
        sl_price=130.0,
        tp1_price=180.0,
        tp2_price=200.0,
        tp3_price=225.0,
        status="OPEN",
    )
    paper_trading_engine._active_trades["TR-TEST-CLOSE-99"] = trade

    # Initial check: open trade is present in get_paper_trades
    data_open = await get_paper_trades()
    open_trades = data_open.get("open_trades", [])
    assert any(t["trade_id"] == "TR-TEST-CLOSE-99" for t in open_trades)

    # Manually close via API
    res = await close_single_paper_trade("TR-TEST-CLOSE-99")
    assert res["status"] == "success"
    assert res["trade_id"] == "TR-TEST-CLOSE-99"
    assert res["exit_price"] == 175.0

    # Verify open_trades no longer has it, but closed_trades does
    data_after = await get_paper_trades()
    open_after = data_after.get("open_trades", [])
    closed_after = data_after.get("closed_trades", [])

    assert not any(t["trade_id"] == "TR-TEST-CLOSE-99" for t in open_after)
    closed_item = next((t for t in closed_after if t["trade_id"] == "TR-TEST-CLOSE-99"), None)
    assert closed_item is not None
    assert closed_item["exit_price"] == 175.0
    assert closed_item["exit_reason"] == "MANUAL_CLOSE"
    # % Profit from 150.0 to 175.0 = ((175 - 150) / 150) * 100 = 16.67%
    assert closed_item["pnl_pct"] == 16.67
    assert closed_item["entry_time"] is not None
    assert closed_item["exit_time"] is not None

    # Cleanup
    paper_trading_engine.clear_trades()


def test_format_datetime_ist():
    from datetime import datetime, timezone
    dt_utc = datetime(2026, 10, 15, 8, 30, 0, tzinfo=timezone.utc)
    # 8:30 UTC + 5:30 = 14:00 (02:00 PM) IST
    res = format_datetime_ist(dt_utc)
    assert "15-Oct-2026" in res
    assert "02:00:00 PM" in res


def test_frontend_html_has_close_button_and_trade_history_tab():
    """Verifies static/index.html includes the Close button and Trade History tab."""
    import os
    html_path = os.path.join(settings.BASE_DIR if hasattr(settings, "BASE_DIR") else ".", "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Must contain Trade History tab button and Active Positions tab button
    assert 'id="btnTabActiveTrades"' in content
    assert 'id="btnTabHistoryTrades"' in content
    assert "switchTradesView('active')" in content
    assert "switchTradesView('history')" in content

    # Must contain both Active Positions and Trade History tables
    assert 'id="activeTradesView"' in content
    assert 'id="historyTradesView"' in content
    assert 'id="tradesTable"' in content
    assert 'id="historyTable"' in content

    # Must contain Close button styling and trigger
    assert ".btn-close-position" in content
    assert "closeActiveTrade" in content

    # Must contain % Profit and formatted Date columns in Trade History table
    assert "Avg Closed Price" in content
    assert "% Profit" in content
    assert "Entry Date & Time" in content
    assert "Close Date & Time" in content
    assert "Exit Reason" in content


@pytest.mark.asyncio
async def test_realtime_websocket_message_decoding_and_quote_cache():
    """Verifies MarketDataStreamerV3 message handler extracts volume, greeks, IV, and caches under all aliases."""
    provider = UpstoxProvider(access_token="dummy_token")
    inst_key = "NSE_FO|54321"
    sym = "NIFTY 24 OCT 22850 CE"

    # Pre-map symbol
    provider._key_to_symbol[inst_key] = sym
    provider._symbol_to_key[sym] = inst_key
    provider._symbol_to_key["NIFTY 24 OCT 22850 CALL"] = inst_key

    # Mock full protobuf feed decoded dict
    mock_msg = {
        "feeds": {
            inst_key: {
                "fullFeed": {
                    "marketFF": {
                        "ltpc": {"ltp": 178.50, "cp": 165.00},
                        "marketLevel": {
                            "bidAskQuote": [
                                {"bidP": 178.25, "askP": 178.75, "bidQ": 1500, "askQ": 2250}
                            ]
                        },
                        "vtt": 85000,
                        "oi": 125000,
                        "iv": 15.2,
                        "optionGreeks": {
                            "delta": 0.54,
                            "theta": -10.8,
                            "gamma": 0.0014,
                            "vega": 0.08,
                        },
                        "marketOHLC": {
                            "ohlc": [
                                {"interval": "1d", "open": 160.0, "high": 185.0, "low": 155.0}
                            ]
                        }
                    }
                }
            }
        }
    }

    provider._on_streamer_message(mock_msg)

    # Check that quote is stored under inst_key, sym, and CALL alias
    quote_by_key = provider._last_quotes.get(inst_key)
    assert quote_by_key is not None
    assert quote_by_key.ltp == 178.50
    assert quote_by_key.prev_close == 165.00
    assert quote_by_key.bid == 178.25
    assert quote_by_key.ask == 178.75
    assert quote_by_key.volume == 85000
    assert quote_by_key.oi == 125000
    assert quote_by_key.iv == 15.2
    assert quote_by_key.greeks is not None
    assert quote_by_key.greeks.delta == 0.54

    # Check alias lookups
    quote_by_sym = provider._last_quotes.get("NIFTY 24 OCT 22850 CE")
    assert quote_by_sym is not None
    assert quote_by_sym.ltp == 178.50

    quote_by_call = provider._last_quotes.get("NIFTY 24 OCT 22850 CALL")
    assert quote_by_call is not None
    assert quote_by_call.ltp == 178.50


@pytest.mark.asyncio
async def test_dashboard_options_watchlist_realtime_websocket_quotes_sync():
    """Verifies get_options_watchlist dynamically prioritizes real-time quotes from _last_quotes."""
    provider = UpstoxProvider(access_token="dummy_token")
    set_market_provider(provider)

    # Inject a simulated chain snapshot
    now = datetime.now(timezone.utc)
    res_initial = await get_options_watchlist("NIFTY")
    assert len(res_initial["contracts"]) > 0

    first_contract = res_initial["contracts"][0]
    sym = first_contract["symbol"]
    inst_key = first_contract.get("instrument_key")

    # Now inject a live WebSocket tick for this contract into provider._last_quotes
    from app.data.base_provider import UnifiedQuote, OptionGreeks
    mock_live_quote = UnifiedQuote(
        instrument_id=inst_key or sym,
        symbol=sym,
        exchange="NSE",
        ltp=250.75,
        bid=250.50,
        ask=251.00,
        prev_close=200.00,
        volume=99999,
        oi=77777,
        exchange_timestamp=now,
        source="upstox_ws",
        greeks=OptionGreeks(delta=0.62, theta=-9.5, gamma=0.0018, iv=16.5),
        iv=16.5,
    )
    provider._last_quotes[sym] = mock_live_quote
    if inst_key:
        provider._last_quotes[inst_key] = mock_live_quote

    # Fetch options watchlist again; it MUST reflect the new live tick immediately
    res_updated = await get_options_watchlist("NIFTY")
    updated_contract = next(c for c in res_updated["contracts"] if c["symbol"] == sym)

    assert updated_contract["ltp"] == 250.75
    assert updated_contract["bid"] == 250.50
    assert updated_contract["ask"] == 251.00
    assert updated_contract["volume"] == 99999
    assert updated_contract["oi"] == 77777
    assert updated_contract["delta"] == 0.62
    assert updated_contract["iv"] == 16.5
    # Change = 250.75 - 200.00 = +50.75 pts
    assert updated_contract["change"] == 50.75
    assert updated_contract["change_pct"] == 25.4


def test_frontend_html_realtime_in_place_dom_updates():
    """Verifies static/index.html includes in-place DOM updating and tick flash animations."""
    import os
    html_path = os.path.join(settings.BASE_DIR if hasattr(settings, "BASE_DIR") else ".", "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Flash tick animation CSS
    assert "flash-tick-up" in content
    assert "flash-tick-down" in content
    assert "@keyframes tickFlashUp" in content
    assert "@keyframes tickFlashDown" in content

    # In-place DOM update mechanism
    assert "card.dataset.ltp" in content
    assert "flash-tick-up" in content
    assert "flash-tick-down" in content
    assert "grid.dataset.currentUnderlying" in content




