"""Upstox V3 Market Data Provider Adapter.

Implements Upstox V3 Market Feed protocols and REST APIs with normalized schemas.
Reference: https://upstox.com/developer/api-documentation/v3/get-market-data-feed/
"""

from __future__ import annotations
import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
import httpx

from app.data.base_provider import (
    BaseMarketDataProvider,
    UnifiedQuote,
    UnifiedCandle,
    OptionChainSnapshot,
    OptionContractData,
    OptionGreeks,
    UnifiedDepth,
    UnifiedDepthLevel,
)
from app.data.instrument_master import instrument_master
from app.monitoring.logging import logger
from config import settings


class UpstoxProvider(BaseMarketDataProvider):
    """Adapter for Upstox V3 Market Data and Option Chain APIs."""

    UPSTOX_INDEX_KEYS: Dict[str, str] = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
        "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
        "SENSEX": "BSE_INDEX|SENSEX",
        "BANKEX": "BSE_INDEX|BANKEX",
        "CRUDEOIL": "MCX_FO|CRUDEOIL",
        "NATURALGAS": "MCX_FO|NATURALGAS",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        super().__init__(name="upstox")
        self.api_key = api_key or settings.UPSTOX_API_KEY
        self.access_token = access_token or settings.UPSTOX_ACCESS_TOKEN
        self.base_url = base_url or settings.UPSTOX_BASE_URL
        self._is_simulated = not bool(self.access_token)
        self._rate_limiter_lock = asyncio.Lock()
        self._last_req_time = 0.0
        self._min_interval = 0.045  # Max ~22.2 req/sec (strictly keeps 10% safety buffer under Upstox 25 req/sec limit)
        self._chain_cache_ttl = 3.0  # 3.0s cache protects Upstox 500 req/min limit while preserving real-time speed
        self._rate_limit_backoff_until = 0.0  # Active backoff timestamp if HTTP 429 is received
        self._expiry_cache: Dict[str, Any] = {}
        self._chain_cache: Dict[str, Any] = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._stream_task: Optional[asyncio.Task] = None
        self._subscribed_ids: Set[str] = set()
        self._last_quotes: Dict[str, UnifiedQuote] = {}
        self._sim_commodity_prices: Dict[str, float] = {
            "CRUDEOIL": 6250.0,
            "NATURALGAS": 245.0,
        }
        self._streamer: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed_keys: Set[str] = set()
        self._key_to_symbol: Dict[str, str] = {
            "NSE_INDEX|Nifty 50": "NIFTY",
            "NSE_INDEX:Nifty 50": "NIFTY",
            "NSE_INDEX|Nifty Bank": "BANKNIFTY",
            "NSE_INDEX:Nifty Bank": "BANKNIFTY",
            "NSE_INDEX|Nifty Fin Service": "FINNIFTY",
            "NSE_INDEX:Nifty Fin Service": "FINNIFTY",
            "NSE_INDEX|NIFTY MID SELECT": "MIDCPNIFTY",
            "NSE_INDEX:NIFTY MID SELECT": "MIDCPNIFTY",
            "BSE_INDEX|SENSEX": "SENSEX",
            "BSE_INDEX:SENSEX": "SENSEX",
            "BSE_INDEX|BANKEX": "BANKEX",
            "BSE_INDEX:BANKEX": "BANKEX",
        }
        self._symbol_to_key: Dict[str, str] = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
            "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
            "SENSEX": "BSE_INDEX|SENSEX",
            "BANKEX": "BSE_INDEX|BANKEX",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(5.0, connect=2.5),
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )
        return self._client

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    async def _throttle_request(self) -> None:
        async with self._rate_limiter_lock:
            now = time.time()
            if now < self._rate_limit_backoff_until:
                backoff_wait = self._rate_limit_backoff_until - now
                if backoff_wait > 0:
                    await asyncio.sleep(min(backoff_wait, 1.0))
            elapsed = time.time() - self._last_req_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_req_time = time.time()

    def resolve_upstox_key(self, inst_id: Optional[str] = None) -> str:
        # Support both instance and static call signatures
        if inst_id is None and isinstance(self, str):
            inst_id = self
            provider_inst = None
        else:
            provider_inst = self

        cleaned = str(inst_id).replace("IDX_", "").strip().upper()
        if cleaned in ("NIFTY", "NIFTY 50", "NSE_INDEX|NIFTY 50"):
            return "NSE_INDEX|Nifty 50"
        if cleaned in ("BANKNIFTY", "NIFTY BANK", "NSE_INDEX|NIFTY BANK"):
            return "NSE_INDEX|Nifty Bank"
        if cleaned in ("FINNIFTY", "NIFTY FIN SERVICE", "NSE_INDEX|NIFTY FIN SERVICE"):
            return "NSE_INDEX|Nifty Fin Service"
        if cleaned in ("MIDCPNIFTY", "NIFTY MID SELECT", "NSE_INDEX|NIFTY MID SELECT"):
            return "NSE_INDEX|NIFTY MID SELECT"
        if cleaned in ("SENSEX", "BSE_INDEX|SENSEX"):
            return "BSE_INDEX|SENSEX"
        if cleaned in ("BANKEX", "BSE_INDEX|BANKEX"):
            return "BSE_INDEX|BANKEX"
        if cleaned in ("CRUDEOIL", "MCX_FO|CRUDEOIL"):
            return "MCX_FO|CRUDEOIL"
        if cleaned in ("NATURALGAS", "MCX_FO|NATURALGAS"):
            return "MCX_FO|NATURALGAS"

        if provider_inst and hasattr(provider_inst, "_symbol_to_key"):
            s_map = provider_inst._symbol_to_key
            if str(inst_id) in s_map:
                return s_map[str(inst_id)]
            if cleaned in s_map:
                return s_map[cleaned]
            alt = cleaned
            if " CALL" in alt:
                alt = alt.replace(" CALL", " CE")
            elif " CE" in alt:
                alt = alt.replace(" CE", " CALL")
            elif " PUT" in alt:
                alt = alt.replace(" PUT", " PE")
            elif " PE" in alt:
                alt = alt.replace(" PE", " PUT")
            if alt in s_map:
                return s_map[alt]

        return str(inst_id)

    async def connect(self) -> bool:
        if self._is_simulated:
            logger.info("UpstoxProvider operating in simulated mode (no live access token).")
            self.is_connected = True
            if self._stream_task is None or self._stream_task.done():
                self._stream_task = asyncio.create_task(self._run_live_quote_stream())
            return True

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

        try:
            client = await self._get_client()
            # Analytics Token grants direct access to Market Data APIs
            resp = await client.get(
                f"{self.base_url}/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50",
                headers=self._get_headers(),
            )
            if resp.status_code == 200:
                self.is_connected = True
                logger.info("Connected to Upstox Market Data API successfully (Analytics Token verified).")
            else:
                # Fallback to user profile check (for full trading tokens)
                resp_prof = await client.get(
                    f"{self.base_url}/user/profile",
                    headers=self._get_headers(),
                )
                if resp_prof.status_code == 200:
                    self.is_connected = True
                    logger.info("Connected to Upstox API successfully via user profile.")
                else:
                    logger.warning(f"Upstox connection check failed: HTTP {resp.status_code}")
                    self.is_connected = False
                    return False

            # Initialize high-frequency Upstox continuous WebSocket market feed
            self._init_websocket_streamer()

            # Launch commodity background runner if commodities enabled
            if self._stream_task is None or self._stream_task.done():
                self._stream_task = asyncio.create_task(self._run_live_quote_stream())

            return True
        except Exception as e:
            logger.error(f"Failed to connect to Upstox: {e}")
            self.is_connected = False
            return False

    def _init_websocket_streamer(self) -> None:
        """Initializes continuous Upstox V3 Market Data WebSocket streamer using MarketDataStreamerV3."""
        if not self.access_token:
            return

        try:
            import upstox_client
            from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

            configuration = upstox_client.Configuration()
            configuration.access_token = self.access_token
            api_client = upstox_client.ApiClient(configuration)

            initial_keys = []
            for und, key in self.UPSTOX_INDEX_KEYS.items():
                if und in ("CRUDEOIL", "NATURALGAS"):
                    continue
                cfg = settings.INDICES.get(und)
                if cfg and cfg.enabled:
                    initial_keys.append(key)
                    self._subscribed_keys.add(key)

            if not initial_keys:
                initial_keys = ["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"]
                for k in initial_keys:
                    self._subscribed_keys.add(k)

            self._streamer = MarketDataStreamerV3(
                api_client=api_client,
                instrumentKeys=initial_keys,
                mode="full",
            )
            self._streamer.on(MarketDataStreamerV3.Event["OPEN"], self._on_streamer_open)
            self._streamer.on(MarketDataStreamerV3.Event["MESSAGE"], self._on_streamer_message)
            self._streamer.on(MarketDataStreamerV3.Event["ERROR"], self._on_streamer_error)
            self._streamer.on(MarketDataStreamerV3.Event["CLOSE"], self._on_streamer_close)

            self._streamer.connect()
            logger.info(f"Upstox MarketDataStreamerV3 continuous WebSocket initialized with {len(initial_keys)} keys.")
        except Exception as e:
            logger.error(f"Error initializing Upstox MarketDataStreamerV3: {e}")

    def _on_streamer_open(self) -> None:
        logger.info("Upstox WebSocket continuous market feed OPENED.")

    def _on_streamer_close(self, code=None, reason=None) -> None:
        logger.info(f"Upstox WebSocket market feed CLOSED: code={code}, reason={reason}")

    def _on_streamer_error(self, error) -> None:
        logger.warning(f"Upstox WebSocket market feed ERROR: {error}")

    def _on_streamer_message(self, message: Any) -> None:
        """Handles incoming real-time protobuf ticks decoded as dict by MarketDataStreamerV3."""
        if not isinstance(message, dict):
            return
        feeds = message.get("feeds")
        if not feeds or not isinstance(feeds, dict):
            return

        now = datetime.now(timezone.utc)
        for inst_key, feed_item in feeds.items():
            try:
                full_feed = feed_item.get("fullFeed", {})
                index_ff = full_feed.get("indexFF")
                market_ff = full_feed.get("marketFF")

                ltpc = None
                if index_ff:
                    ltpc = index_ff.get("ltpc", {})
                elif market_ff:
                    ltpc = market_ff.get("ltpc", {})
                elif "ltpc" in feed_item:
                    ltpc = feed_item.get("ltpc", {})

                if not ltpc or "ltp" not in ltpc:
                    continue

                ltp = float(ltpc["ltp"])
                cp = float(ltpc.get("cp", ltp))

                # Identify symbol
                sym = self._key_to_symbol.get(inst_key)
                if not sym:
                    colon_key = inst_key.replace("|", ":")
                    sym = self._key_to_symbol.get(colon_key)
                if not sym:
                    sym = inst_key.split("|")[-1].strip().upper()

                inst_id = f"IDX_{sym}" if sym in self.UPSTOX_INDEX_KEYS else inst_key

                bid = None
                ask = None
                bid_qty = None
                ask_qty = None
                oi = None
                day_open = None
                day_high = None
                day_low = None

                vol = None
                greeks = None
                iv = None

                if market_ff:
                    mkt_lvl = market_ff.get("marketLevel", {})
                    bids_asks = mkt_lvl.get("bidAskQuote", [])
                    if bids_asks:
                        best = bids_asks[0]
                        bid = float(best.get("bidP", ltp))
                        ask = float(best.get("askP", ltp))
                        bid_qty = int(best.get("bidQ", 0))
                        ask_qty = int(best.get("askQ", 0))
                    oi = int(market_ff.get("oi", 0))
                    vol = int(market_ff.get("vtt", 0) or market_ff.get("volume", 0))
                    if market_ff.get("iv") is not None:
                        try:
                            iv = float(market_ff["iv"])
                        except (ValueError, TypeError):
                            pass
                    opt_greeks_raw = market_ff.get("optionGreeks")
                    if opt_greeks_raw and isinstance(opt_greeks_raw, dict):
                        try:
                            greeks = OptionGreeks(
                                delta=float(opt_greeks_raw.get("delta", 0.0)) if opt_greeks_raw.get("delta") is not None else None,
                                theta=float(opt_greeks_raw.get("theta", 0.0)) if opt_greeks_raw.get("theta") is not None else None,
                                gamma=float(opt_greeks_raw.get("gamma", 0.0)) if opt_greeks_raw.get("gamma") is not None else None,
                                vega=float(opt_greeks_raw.get("vega", 0.0)) if opt_greeks_raw.get("vega") is not None else None,
                                iv=iv,
                            )
                        except Exception:
                            pass

                    ohlc_list = market_ff.get("marketOHLC", {}).get("ohlc", [])
                    for o in ohlc_list:
                        if o.get("interval") == "1d":
                            day_open = float(o.get("open", ltp))
                            day_high = float(o.get("high", ltp))
                            day_low = float(o.get("low", ltp))

                elif index_ff:
                    ohlc_list = index_ff.get("marketOHLC", {}).get("ohlc", [])
                    for o in ohlc_list:
                        if o.get("interval") == "1d":
                            day_open = float(o.get("open", ltp))
                            day_high = float(o.get("high", ltp))
                            day_low = float(o.get("low", ltp))
                    bid = round(ltp - 0.25, 2)
                    ask = round(ltp + 0.25, 2)

                quote = UnifiedQuote(
                    instrument_id=inst_id,
                    symbol=sym,
                    exchange="BSE" if "BSE" in inst_key or sym in ("SENSEX", "BANKEX") else "NSE",
                    ltp=ltp,
                    bid=bid or ltp,
                    ask=ask or ltp,
                    bid_qty=bid_qty,
                    ask_qty=ask_qty,
                    volume=vol,
                    prev_close=cp,
                    open=day_open,
                    high=day_high,
                    low=day_low,
                    oi=oi,
                    exchange_timestamp=now,
                    source="upstox_ws",
                    greeks=greeks,
                    iv=iv,
                )

                # Store in fast lookup cache
                self._last_quotes[sym] = quote
                self._last_quotes[inst_id] = quote
                self._last_quotes[inst_key] = quote
                colon_k = inst_key.replace("|", ":")
                self._last_quotes[colon_k] = quote
                if " CE" in sym:
                    self._last_quotes[sym.replace(" CE", " CALL")] = quote
                elif " CALL" in sym:
                    self._last_quotes[sym.replace(" CALL", " CE")] = quote
                elif " PE" in sym:
                    self._last_quotes[sym.replace(" PE", " PUT")] = quote
                elif " PUT" in sym:
                    self._last_quotes[sym.replace(" PUT", " PE")] = quote

                def _dispatch(q: UnifiedQuote) -> None:
                    try:
                        asyncio.create_task(self._emit_quote(q))
                    except Exception:
                        pass

                if self._loop and self._loop.is_running():
                    self._loop.call_soon_threadsafe(_dispatch, quote)
            except Exception as e:
                logger.debug(f"Error parsing Upstox WS tick for {inst_key}: {e}")

    async def disconnect(self) -> None:
        self.is_connected = False
        if self._streamer:
            try:
                self._streamer.disconnect()
            except Exception as e:
                logger.debug(f"Error disconnecting Upstox streamer: {e}")
            self._streamer = None
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
        logger.info("UpstoxProvider disconnected.")

    async def subscribe_quotes(self, instrument_ids: List[str]) -> None:
        new_keys: List[str] = []
        for inst_id in instrument_ids:
            if not inst_id:
                continue
            self._subscribed_ids.add(inst_id)
            upstox_key = self.resolve_upstox_key(inst_id)
            cleaned = inst_id.replace("IDX_", "").strip().upper()
            self._key_to_symbol[upstox_key] = cleaned
            self._key_to_symbol[upstox_key.replace("|", ":")] = cleaned
            self._symbol_to_key[cleaned] = upstox_key
            if "|" in upstox_key and upstox_key not in self._subscribed_keys:
                self._subscribed_keys.add(upstox_key)
                new_keys.append(upstox_key)

        valid_ws_keys = [k for k in new_keys if "|" in k]
        if valid_ws_keys:
            logger.info(f"Upstox subscribed to {len(self._subscribed_ids)} instruments ({len(valid_ws_keys)} new WS keys: {valid_ws_keys}).")

            # If continuous streamer is active, subscribe dynamically
            if self._streamer:
                try:
                    feeder = getattr(self._streamer, "feeder", None)
                    if feeder and getattr(feeder, "ws", None) and getattr(feeder.ws, "sock", None):
                        self._streamer.subscribe(valid_ws_keys, mode="full")
                        logger.info(f"Dynamically subscribed {len(valid_ws_keys)} keys to live Upstox WebSocket feed.")
                    else:
                        if hasattr(self._streamer, "subscriptions") and "full" in self._streamer.subscriptions:
                            self._streamer.subscriptions["full"].update(valid_ws_keys)
                except Exception as e:
                    logger.warning(f"Error dynamically subscribing keys to Upstox streamer: {e}")

        if self.is_connected and (self._stream_task is None or self._stream_task.done()):
            self._stream_task = asyncio.create_task(self._run_live_quote_stream())

    async def unsubscribe_quotes(self, instrument_ids: List[str]) -> None:
        for inst_id in instrument_ids:
            self._subscribed_ids.discard(inst_id)

    async def _run_live_quote_stream(self) -> None:
        """Background streaming worker that handles MCX synthetic commodities or REST fallback."""
        logger.info("Upstox live quote streaming loop started.")
        # Allow initial subscriber handlers to attach
        await asyncio.sleep(0.2)

        equity_keys_map = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
            "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
            "SENSEX": "BSE_INDEX|SENSEX",
            "BANKEX": "BSE_INDEX|BANKEX",
        }

        while self.is_connected:
            try:
                now = datetime.now(timezone.utc)

                # Only run REST polling fallback if WebSocket streamer is NOT active
                streamer_active = bool(
                    self._streamer
                    and hasattr(self._streamer, "feeder")
                    and self._streamer.feeder
                    and self._streamer.feeder.ws
                    and self._streamer.feeder.ws.sock
                )

                if not self._is_simulated and not streamer_active:
                    # Gather active equity index keys that are enabled or subscribed
                    keys_to_fetch = []
                    for und, key in equity_keys_map.items():
                        inst_id = f"IDX_{und}"
                        is_sub = inst_id in self._subscribed_ids or und in self._subscribed_ids
                        is_cfg_enabled = bool(settings.INDICES.get(und) and settings.INDICES[und].enabled)
                        if is_sub or is_cfg_enabled:
                            keys_to_fetch.append((und, key))

                    if keys_to_fetch:
                        if time.time() < self._rate_limit_backoff_until:
                            # Backoff cooldown active; skip live REST call to allow rate limit quota to reset
                            pass
                        else:
                            joined_keys = ",".join(k[1] for k in keys_to_fetch)
                            await self._throttle_request()
                            client = await self._get_client()
                            resp = await client.get(
                                f"{self.base_url}/market-quote/quotes",
                                headers=self._get_headers(),
                                params={"instrument_key": joined_keys},
                            )
                            if resp.status_code == 200:
                                data_dict = resp.json().get("data", {})
                                for und, key in keys_to_fetch:
                                    colon_key = key.replace("|", ":")
                                    q_data = data_dict.get(key) or data_dict.get(colon_key)
                                    if q_data:
                                        quote = self._parse_upstox_quote(und, q_data)
                                        quote.instrument_id = f"IDX_{und}"
                                        quote.symbol = und
                                        quote.exchange_timestamp = now
                                        self._last_quotes[und] = quote
                                        self._last_quotes[f"IDX_{und}"] = quote
                                        await self._emit_quote(quote)
                            elif resp.status_code == 429:
                                self._rate_limit_backoff_until = time.time() + 10.0
                                logger.warning(
                                    "Upstox live quote stream hit HTTP 429 (Too Many Requests). Activating 10s backoff cooldown..."
                                )

                # Emit synthetic/micro-jitter quotes for MCX commodities (CRUDEOIL, NATURALGAS)
                # Upstox Analytics Token is authorized for Equity/Derivatives
                for comm, base_price in [("CRUDEOIL", 6250.0), ("NATURALGAS", 245.0)]:
                    inst_id = f"IDX_{comm}"
                    is_sub = inst_id in self._subscribed_ids or comm in self._subscribed_ids
                    is_cfg_enabled = bool(settings.INDICES.get(comm) and settings.INDICES[comm].enabled)
                    if is_sub or is_cfg_enabled:
                        cur = self._sim_commodity_prices.get(comm, base_price)
                        jitter = round(random.uniform(-0.5, 0.5) if comm == "CRUDEOIL" else random.uniform(-0.1, 0.1), 2)
                        new_price = round(max(base_price * 0.9, min(base_price * 1.1, cur + jitter)), 2)
                        self._sim_commodity_prices[comm] = new_price
                        c_quote = UnifiedQuote(
                            instrument_id=inst_id,
                            symbol=comm,
                            exchange="MCX",
                            ltp=new_price,
                            bid=round(new_price - (0.5 if comm == "CRUDEOIL" else 0.1), 2),
                            ask=round(new_price + (0.5 if comm == "CRUDEOIL" else 0.1), 2),
                            volume=125000,
                            oi=45000,
                            exchange_timestamp=now,
                            source="upstox_sim",
                        )
                        self._last_quotes[comm] = c_quote
                        self._last_quotes[inst_id] = c_quote
                        await self._emit_quote(c_quote)

                # If in simulated mode, also emit simulated index quotes
                if self._is_simulated:
                    for und, base_price in [("NIFTY", 22622.70), ("SENSEX", 72781.89)]:
                        inst_id = f"IDX_{und}"
                        is_sub = inst_id in self._subscribed_ids or und in self._subscribed_ids
                        is_cfg_enabled = bool(settings.INDICES.get(und) and settings.INDICES[und].enabled)
                        if is_sub or is_cfg_enabled:
                            s_quote = UnifiedQuote(
                                instrument_id=inst_id,
                                symbol=und,
                                exchange="BSE" if und == "SENSEX" else "NSE",
                                ltp=base_price,
                                bid=round(base_price - 0.25, 2),
                                ask=round(base_price + 0.25, 2),
                                volume=140000,
                                oi=820000,
                                exchange_timestamp=now,
                                source="upstox_sim",
                            )
                            self._last_quotes[und] = s_quote
                            self._last_quotes[inst_id] = s_quote
                            await self._emit_quote(s_quote)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in Upstox quote stream loop: {e}")

            await asyncio.sleep(1.0)

    async def get_quote_snapshot(self, instrument_id: str) -> Optional[UnifiedQuote]:
        cleaned = instrument_id.replace("IDX_", "").upper()
        now = datetime.now(timezone.utc)

        # 1. Fast in-memory cache lookup (<0.05ms)
        upstox_key = self.resolve_upstox_key(instrument_id)
        cached = (
            self._last_quotes.get(instrument_id)
            or self._last_quotes.get(cleaned)
            or self._last_quotes.get(upstox_key)
            or self._last_quotes.get(upstox_key.replace("|", ":"))
        )
        if cached:
            if (now - cached.exchange_timestamp).total_seconds() < 2.5:
                return cached

        # 2. Commodity isolation (Crude Oil & Natural Gas)
        if cleaned in ("CRUDEOIL", "NATURALGAS"):
            base = self._sim_commodity_prices.get(cleaned, 6250.0 if cleaned == "CRUDEOIL" else 245.0)
            quote = UnifiedQuote(
                instrument_id=f"IDX_{cleaned}",
                symbol=cleaned,
                exchange="MCX",
                ltp=base,
                bid=round(base - (0.5 if cleaned == "CRUDEOIL" else 0.1), 2),
                ask=round(base + (0.5 if cleaned == "CRUDEOIL" else 0.1), 2),
                volume=125000,
                oi=45000,
                exchange_timestamp=now,
                source="upstox_sim",
            )
            self._last_quotes[instrument_id] = quote
            self._last_quotes[cleaned] = quote
            return quote

        # 3. Simulated mode fallback
        if self._is_simulated:
            base = 22622.7 if "NIFTY" in cleaned else 72781.89
            quote = UnifiedQuote(
                instrument_id=instrument_id,
                symbol=cleaned,
                exchange="BSE" if "SENSEX" in cleaned else "NSE",
                ltp=base,
                bid=round(base - 0.5, 2),
                ask=round(base + 0.5, 2),
                volume=140000,
                oi=820000,
                exchange_timestamp=now,
                source="upstox_sim",
            )
            self._last_quotes[instrument_id] = quote
            self._last_quotes[cleaned] = quote
            return quote

        # 4. Live fetch via pooled client
        upstox_key = self.resolve_upstox_key(instrument_id)
        await self._throttle_request()
        try:
            client = await self._get_client()
            resp = await client.get(
                f"{self.base_url}/market-quote/quotes",
                headers=self._get_headers(),
                params={"instrument_key": upstox_key},
            )
            if resp.status_code == 200:
                data_dict = resp.json().get("data", {})
                colon_key = upstox_key.replace("|", ":")
                data = data_dict.get(upstox_key) or data_dict.get(colon_key) or (next(iter(data_dict.values())) if data_dict else {})
                if data:
                    parsed = self._parse_upstox_quote(instrument_id, data)
                    self._last_quotes[instrument_id] = parsed
                    self._last_quotes[cleaned] = parsed
                    return parsed
        except Exception as e:
            logger.error(f"Error fetching Upstox quote snapshot for {instrument_id}: {e}")
        return None

    def _parse_upstox_quote(self, instrument_id: str, data: Dict[str, Any]) -> UnifiedQuote:
        now = datetime.now(timezone.utc)
        depth_raw = data.get("depth", {})
        depth = UnifiedDepth()
        for b in depth_raw.get("buy", []):
            depth.bids.append(UnifiedDepthLevel(price=b.get("price", 0.0), quantity=b.get("quantity", 0)))
        for a in depth_raw.get("sell", []):
            depth.asks.append(UnifiedDepthLevel(price=a.get("price", 0.0), quantity=a.get("quantity", 0)))

        exchange = "BSE" if "SENSEX" in instrument_id else ("MCX" if "CRUDE" in instrument_id or "NATURAL" in instrument_id else "NSE")
        ltp = float(data.get("last_price") or 0.0)
        ohlc = data.get("ohlc") or {}
        if ltp == 0.0 and ohlc.get("close"):
            ltp = float(ohlc["close"] or 0.0)

        cleaned = instrument_id.replace("IDX_", "").upper()
        sym = data.get("symbol")
        if not sym or sym == "NA":
            sym = cleaned

        return UnifiedQuote(
            instrument_id=instrument_id if instrument_id.startswith("IDX_") else f"IDX_{cleaned}",
            symbol=sym,
            exchange=exchange,
            ltp=ltp,
            bid=depth.bids[0].price if depth.bids else round(ltp - 0.25, 2),
            ask=depth.asks[0].price if depth.asks else round(ltp + 0.25, 2),
            volume=int(data.get("volume") or 0),
            oi=int(data.get("oi") or 0),
            exchange_timestamp=now,
            source="upstox",
            depth=depth,
        )

    async def get_historical_candles(
        self, instrument_id: str, timeframe: str, count: int = 100
    ) -> List[UnifiedCandle]:
        if self._is_simulated:
            return self._generate_simulated_candles(instrument_id, timeframe, count)

        upstox_key = self.resolve_upstox_key(instrument_id)
        encoded_key = upstox_key.replace("|", "%7C").replace(" ", "%20")
        interval = "1minute"

        await self._throttle_request()
        try:
            url = f"{self.base_url}/historical-candle/intraday/{encoded_key}/{interval}"
            client = await self._get_client()
            resp = await client.get(url, headers=self._get_headers())
            if resp.status_code == 200:
                raw_candles = resp.json().get("data", {}).get("candles", [])
                if raw_candles:
                    parsed: List[UnifiedCandle] = []
                    # Upstox returns newest first; reverse for chronological order
                    for c in reversed(raw_candles[:count * (5 if timeframe == "5m" else 1)]):
                        try:
                            dt = datetime.fromisoformat(c[0])
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            parsed.append(
                                UnifiedCandle(
                                    instrument_id=instrument_id,
                                    symbol=instrument_id,
                                    timeframe="1m",
                                    timestamp=dt,
                                    open=float(c[1]),
                                    high=float(c[2]),
                                    low=float(c[3]),
                                    close=float(c[4]),
                                    volume=int(c[5]) if len(c) > 5 else 0,
                                    oi=int(c[6]) if len(c) > 6 else 0,
                                )
                            )
                        except Exception:
                            pass

                    if timeframe == "5m" and parsed:
                        return self._aggregate_1m_to_5m(parsed, instrument_id, count)
                    return parsed[-count:]
        except Exception as e:
            logger.warning(f"Error fetching live Upstox candles for {instrument_id}: {e}")

        return self._generate_simulated_candles(instrument_id, timeframe, count)

    def _aggregate_1m_to_5m(
        self, candles_1m: List[UnifiedCandle], instrument_id: str, count: int
    ) -> List[UnifiedCandle]:
        candles_5m: List[UnifiedCandle] = []
        # Group by 5-minute chunks
        bucket: List[UnifiedCandle] = []
        for c in candles_1m:
            bucket.append(c)
            if len(bucket) == 5:
                o = bucket[0].open
                h = max(b.high for b in bucket)
                l = min(b.low for b in bucket)
                close_p = bucket[-1].close
                vol = sum(b.volume for b in bucket)
                candles_5m.append(
                    UnifiedCandle(
                        instrument_id=instrument_id,
                        symbol=instrument_id,
                        timeframe="5m",
                        timestamp=bucket[-1].timestamp,
                        open=round(o, 2),
                        high=round(h, 2),
                        low=round(l, 2),
                        close=round(close_p, 2),
                        volume=vol,
                        oi=bucket[-1].oi,
                    )
                )
                bucket = []
        if bucket:
            candles_5m.append(
                UnifiedCandle(
                    instrument_id=instrument_id,
                    symbol=instrument_id,
                    timeframe="5m",
                    timestamp=bucket[-1].timestamp,
                    open=round(bucket[0].open, 2),
                    high=round(max(b.high for b in bucket), 2),
                    low=round(min(b.low for b in bucket), 2),
                    close=round(bucket[-1].close, 2),
                    volume=sum(b.volume for b in bucket),
                    oi=bucket[-1].oi,
                )
            )
        return candles_5m[-count:]

    async def get_active_expiry(self, underlying: str) -> Optional[str]:
        now_ts = time.time()
        und = underlying.upper()
        if und in self._expiry_cache:
            exp_date, ts = self._expiry_cache[und]
            if now_ts - ts < 600:
                return exp_date

        # Commodities do not use Upstox Equity Option Contract API
        if und in ("CRUDEOIL", "NATURALGAS"):
            calc_dt = instrument_master.get_exchange_weekly_expiry(und, datetime.now(timezone.utc))
            fallback_str = calc_dt.strftime("%Y-%m-%d")
            self._expiry_cache[und] = (fallback_str, now_ts)
            return fallback_str

        upstox_key = self.UPSTOX_INDEX_KEYS.get(und)
        if not upstox_key:
            calc_dt = instrument_master.get_exchange_weekly_expiry(und, datetime.now(timezone.utc))
            fallback_str = calc_dt.strftime("%Y-%m-%d")
            self._expiry_cache[und] = (fallback_str, now_ts)
            return fallback_str

        await self._throttle_request()
        try:
            client = await self._get_client()
            resp = await client.get(
                f"{self.base_url}/option/contract",
                headers=self._get_headers(),
                params={"instrument_key": upstox_key},
            )
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                expiries = sorted(list(set(x.get("expiry") for x in data if x.get("expiry"))))
                if expiries:
                    first_exp = expiries[0]
                    self._expiry_cache[und] = (first_exp, now_ts)
                    return first_exp
        except Exception as e:
            logger.warning(f"Error resolving Upstox active expiry for {und}: {e}")

        calc_dt = instrument_master.get_exchange_weekly_expiry(und, datetime.now(timezone.utc))
        fallback_str = calc_dt.strftime("%Y-%m-%d")
        self._expiry_cache[und] = (fallback_str, now_ts)
        return fallback_str

    async def get_option_chain_snapshot(
        self, underlying: str, expiry: Optional[datetime] = None
    ) -> Optional[OptionChainSnapshot]:
        now = datetime.now(timezone.utc)
        resolved_expiry = expiry or instrument_master.get_exchange_weekly_expiry(underlying, now)
        exp_date_str = await self.get_active_expiry(underlying) or resolved_expiry.strftime("%Y-%m-%d")
        cache_key = f"{underlying.upper()}_{exp_date_str}"

        # Fast cache check (self._chain_cache_ttl protects rate limits and delivers <0.1ms response)
        if cache_key in self._chain_cache:
            snap, c_time = self._chain_cache[cache_key]
            if time.time() - c_time < self._chain_cache_ttl:
                return snap

        # Commodities do not use Upstox Option Chain API
        if underlying.upper() in ("CRUDEOIL", "NATURALGAS"):
            fallback_snap = self._generate_simulated_option_chain(underlying, resolved_expiry)
            self._chain_cache[cache_key] = (fallback_snap, time.time())
            return fallback_snap

        if not self._is_simulated:
            # Active 429 backoff protection: serve cache or fallback without spamming Upstox API
            if time.time() < self._rate_limit_backoff_until:
                if cache_key in self._chain_cache:
                    return self._chain_cache[cache_key][0]
                fallback_snap = self._generate_simulated_option_chain(underlying, resolved_expiry)
                self._chain_cache[cache_key] = (fallback_snap, time.time())
                return fallback_snap

            inst_key = self.UPSTOX_INDEX_KEYS.get(underlying.upper())
            if inst_key:
                try:
                    await self._throttle_request()
                    client = await self._get_client()
                    resp = await client.get(
                        f"{self.base_url}/option/chain",
                        headers=self._get_headers(),
                        params={"instrument_key": inst_key, "expiry_date": exp_date_str},
                    )
                    if resp.status_code == 200:
                        data = resp.json().get("data", [])
                        if data:
                            exp_dt = datetime.strptime(exp_date_str, "%Y-%m-%d").replace(
                                hour=10, minute=0, tzinfo=timezone.utc
                            )
                            snapshot = self._parse_upstox_option_chain(underlying, exp_dt, data)
                            self._chain_cache[cache_key] = (snapshot, time.time())
                            return snapshot
                    elif resp.status_code == 429:
                        self._rate_limit_backoff_until = time.time() + 10.0
                        logger.warning(
                            f"Upstox option chain hit HTTP 429 (Too Many Requests). "
                            f"Activating 10s backoff cooldown across all endpoints."
                        )
                        if cache_key in self._chain_cache:
                            return self._chain_cache[cache_key][0]
                    else:
                        logger.warning(f"Upstox option chain returned HTTP {resp.status_code}: {resp.text}")
                except Exception as e:
                    logger.warning(f"Failed to fetch live Upstox option chain for {underlying}: {e}")

        # Simulated fallback - cached for self._chain_cache_ttl to guarantee immediate responsiveness
        fallback_snap = self._generate_simulated_option_chain(underlying, resolved_expiry)
        self._chain_cache[cache_key] = (fallback_snap, time.time())
        return fallback_snap

    def _parse_upstox_option_chain(
        self, underlying: str, expiry: datetime, data: List[Dict[str, Any]]
    ) -> OptionChainSnapshot:
        now = datetime.now(timezone.utc)
        cfg = instrument_master.get_index_config(underlying)
        lot_size = cfg.lot_size if cfg else 75
        contracts_dict: Dict[float, Dict[str, OptionContractData]] = {}
        spot_price = 0.0
        total_ce_oi = 0
        total_pe_oi = 0

        for item in data:
            strike = float(item.get("strike_price", 0.0))
            if strike <= 0:
                continue
            if not spot_price and item.get("underlying_spot_price"):
                spot_price = float(item["underlying_spot_price"])

            contracts_dict[strike] = {}

            # CE
            ce_data = item.get("call_options")
            if ce_data:
                mkt = ce_data.get("market_data", {})
                greeks_raw = ce_data.get("option_greeks") or {}
                ce_oi = int(mkt.get("oi", 0))
                ce_prev_oi = int(mkt.get("prev_oi", ce_oi))
                total_ce_oi += ce_oi
                ce_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "CE")
                ce_call_sym = f"{underlying} {expiry.strftime('%d %b').upper()} {int(strike)} CALL"
                ce_key = str(ce_data.get("instrument_key", f"U_{underlying}_{int(strike)}_CE"))
                self._key_to_symbol[ce_key] = ce_sym
                self._key_to_symbol[ce_key.replace("|", ":")] = ce_sym
                self._symbol_to_key[ce_sym] = ce_key
                self._symbol_to_key[ce_call_sym] = ce_key
                contracts_dict[strike]["CE"] = OptionContractData(
                    instrument_id=ce_key,
                    symbol=ce_sym,
                    strike=strike,
                    option_type="CE",
                    expiry=expiry,
                    ltp=float(mkt.get("ltp", 0.0)),
                    bid=float(mkt.get("bid_price") or mkt.get("ltp", 0.0)),
                    ask=float(mkt.get("ask_price") or mkt.get("ltp", 0.0)),
                    bid_qty=int(mkt.get("bid_qty", 0)),
                    ask_qty=int(mkt.get("ask_qty", 0)),
                    close_price=float(mkt.get("close_price", mkt.get("ltp", 0.0))),
                    volume=int(mkt.get("volume", 0)),
                    oi=ce_oi,
                    oi_change=ce_oi - ce_prev_oi,
                    greeks=OptionGreeks(
                        delta=float(greeks_raw.get("delta", 0.5)),
                        theta=float(greeks_raw.get("theta", 0.0)),
                        gamma=float(greeks_raw.get("gamma", 0.0)),
                        vega=float(greeks_raw.get("vega", 0.0)),
                        iv=float(greeks_raw.get("iv", 0.0)),
                    ),
                    lot_size=lot_size,
                )

            # PE
            pe_data = item.get("put_options")
            if pe_data:
                mkt = pe_data.get("market_data", {})
                greeks_raw = pe_data.get("option_greeks") or {}
                pe_oi = int(mkt.get("oi", 0))
                pe_prev_oi = int(mkt.get("prev_oi", pe_oi))
                total_pe_oi += pe_oi
                pe_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "PE")
                pe_put_sym = f"{underlying} {expiry.strftime('%d %b').upper()} {int(strike)} PUT"
                pe_key = str(pe_data.get("instrument_key", f"U_{underlying}_{int(strike)}_PE"))
                self._key_to_symbol[pe_key] = pe_sym
                self._key_to_symbol[pe_key.replace("|", ":")] = pe_sym
                self._symbol_to_key[pe_sym] = pe_key
                self._symbol_to_key[pe_put_sym] = pe_key
                contracts_dict[strike]["PE"] = OptionContractData(
                    instrument_id=pe_key,
                    symbol=pe_sym,
                    strike=strike,
                    option_type="PE",
                    expiry=expiry,
                    ltp=float(mkt.get("ltp", 0.0)),
                    bid=float(mkt.get("bid_price") or mkt.get("ltp", 0.0)),
                    ask=float(mkt.get("ask_price") or mkt.get("ltp", 0.0)),
                    bid_qty=int(mkt.get("bid_qty", 0)),
                    ask_qty=int(mkt.get("ask_qty", 0)),
                    close_price=float(mkt.get("close_price", mkt.get("ltp", 0.0))),
                    volume=int(mkt.get("volume", 0)),
                    oi=pe_oi,
                    oi_change=pe_oi - pe_prev_oi,
                    greeks=OptionGreeks(
                        delta=float(greeks_raw.get("delta", -0.5)),
                        theta=float(greeks_raw.get("theta", 0.0)),
                        gamma=float(greeks_raw.get("gamma", 0.0)),
                        vega=float(greeks_raw.get("vega", 0.0)),
                        iv=float(greeks_raw.get("iv", 0.0)),
                    ),
                    lot_size=lot_size,
                )

        pcr = round(total_pe_oi / max(total_ce_oi, 1), 3) if total_ce_oi > 0 else 1.0

        # Auto-subscribe ATM option keys to live continuous WebSocket feed
        step = cfg.strike_interval if cfg else 50.0
        atm = round((spot_price or 25000.0) / step) * step
        target_strikes = [atm - 2 * step, atm - step, atm, atm + step, atm + 2 * step]
        auto_keys = []
        for strk in target_strikes:
            if strk in contracts_dict:
                if "CE" in contracts_dict[strk]:
                    k = contracts_dict[strk]["CE"].instrument_id
                    if k and ("NSE_FO|" in k or "BSE_FO|" in k):
                        auto_keys.append(k)
                if "PE" in contracts_dict[strk]:
                    k = contracts_dict[strk]["PE"].instrument_id
                    if k and ("NSE_FO|" in k or "BSE_FO|" in k):
                        auto_keys.append(k)
        if auto_keys:
            try:
                if self._loop and self._loop.is_running():
                    asyncio.run_coroutine_threadsafe(self.subscribe_quotes(auto_keys), self._loop)
                else:
                    asyncio.create_task(self.subscribe_quotes(auto_keys))
            except Exception:
                pass
        return OptionChainSnapshot(
            underlying=underlying,
            underlying_ltp=spot_price or 25050.0,
            snapshot_timestamp=now,
            expiry=expiry,
            contracts=contracts_dict,
            pcr_oi=pcr,
        )

    def _generate_simulated_option_chain(
        self, underlying: str, expiry: datetime
    ) -> OptionChainSnapshot:
        now = datetime.now(timezone.utc)
        cfg = instrument_master.get_index_config(underlying)
        step = cfg.strike_interval if cfg else 50.0
        und_u = underlying.upper()
        if "CRUDE" in und_u:
            spot = 6250.0
            base_prem = 115.0
        elif "NATURAL" in und_u:
            spot = 245.0
            base_prem = 8.5
        elif "SENSEX" in und_u:
            spot = 72781.89
            base_prem = 280.0
        else:
            spot = 22622.7
            base_prem = 145.0

        atm = round(spot / step) * step
        contracts_dict: Dict[float, Dict[str, OptionContractData]] = {}

        for off in range(-3, 4):
            strike = atm + (off * step)
            ce_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "CE")
            pe_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "PE")
            ce_p = max(1.0, round(base_prem - (off * step * 0.45), 2))
            pe_p = max(1.0, round(base_prem + (off * step * 0.45), 2))
            contracts_dict[strike] = {
                "CE": OptionContractData(
                    instrument_id=f"U_{underlying}_{int(strike)}_CE",
                    symbol=ce_sym,
                    strike=strike,
                    option_type="CE",
                    expiry=expiry,
                    ltp=ce_p,
                    bid=round(ce_p - 0.25, 2),
                    ask=round(ce_p + 0.25, 2),
                    volume=40000,
                    oi=95000,
                    greeks=OptionGreeks(delta=0.51, iv=14.0),
                ),
                "PE": OptionContractData(
                    instrument_id=f"U_{underlying}_{int(strike)}_PE",
                    symbol=pe_sym,
                    strike=strike,
                    option_type="PE",
                    expiry=expiry,
                    ltp=pe_p,
                    bid=round(pe_p - 0.25, 2),
                    ask=round(pe_p + 0.25, 2),
                    volume=38000,
                    oi=92000,
                    greeks=OptionGreeks(delta=-0.49, iv=14.2),
                ),
            }

        return OptionChainSnapshot(
            underlying=underlying,
            underlying_ltp=spot,
            snapshot_timestamp=now,
            expiry=expiry,
            contracts=contracts_dict,
            pcr_oi=0.98,
        )

    def _generate_simulated_candles(
        self, instrument_id: str, timeframe: str, count: int = 100
    ) -> List[UnifiedCandle]:
        now = datetime.now(timezone.utc)
        minutes_step = 5 if timeframe == "5m" else (15 if timeframe == "15m" else 1)
        candles: List[UnifiedCandle] = []

        und = instrument_id.replace("IDX_", "").upper()
        if "CRUDE" in und:
            base = 6250.0
            drift_scale = 10.0
        elif "NATURAL" in und:
            base = 245.0
            drift_scale = 1.0
        elif "SENSEX" in und:
            base = 72781.89
            drift_scale = 40.0
        else:
            base = 22622.7
            drift_scale = 12.0

        for i in range(count, 0, -1):
            ts = now - timedelta(minutes=i * minutes_step)
            drift = ((i % 5) - 2) * (drift_scale / 10.0)
            o = round(base + drift, 2)
            c = round(o + 1.25, 2)
            h = round(max(o, c) + (drift_scale / 8.0), 2)
            l = round(min(o, c) - (drift_scale / 8.0), 2)
            candles.append(
                UnifiedCandle(
                    instrument_id=instrument_id,
                    symbol=instrument_id,
                    timeframe=timeframe,
                    timestamp=ts,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=35000,
                    oi=950000,
                )
            )
        return candles
