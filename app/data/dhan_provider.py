"""DhanHQ V2 Market Data Provider Adapter.

Implements official DhanHQ V2 REST APIs and WebSocket Binary Feed protocols
with rate limiting, backoff, heartbeats, and normalized data output.
Reference: https://docs.dhanhq.co/ and https://dhanhq.co/docs/v2/option-chain/
"""

from __future__ import annotations
import asyncio
import struct
import json
import time
import random
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import httpx
import websockets

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


class DhanProvider(BaseMarketDataProvider):
    """Adapter for DhanHQ V2 Trading & Market Feed API."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        access_token: Optional[str] = None,
        base_url: Optional[str] = None,
        wss_url: Optional[str] = None,
    ):
        super().__init__(name="dhan")
        self.client_id = client_id or settings.DHAN_CLIENT_ID
        self.access_token = access_token or settings.DHAN_ACCESS_TOKEN
        self.base_url = base_url or settings.DHAN_BASE_URL
        self.wss_url = wss_url or settings.DHAN_WSS_URL

        self._ws_client: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_reader_task: Optional[asyncio.Task] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._subscribed_ids: set[str] = set()
        self._security_metadata: Dict[str, tuple[str, str, str]] = {}
        self._last_quotes: Dict[str, UnifiedQuote] = {}
        self._chain_cache: Dict[str, tuple[OptionChainSnapshot, float]] = {}
        self._expiry_cache: Dict[str, tuple[str, float]] = {}
        self._running = False
        self._rate_limiter_lock = asyncio.Lock()
        self._last_req_time = 0.0
        self._min_request_interval = 0.2  # Max 5 req/sec
        self._sim_task: Optional[asyncio.Task] = None

        # Simulated fallback state when credentials are not configured
        self._is_simulated = not bool(self.client_id and self.access_token)
        self._current_sim_prices: Dict[str, float] = {
            "NIFTY": 25050.0,
            "BANKNIFTY": 51600.0,
            "FINNIFTY": 23850.0,
            "MIDCPNIFTY": 13150.0,
            "SENSEX": 81650.0,
            "BANKEX": 58600.0,
        }

    async def _throttle_request(self) -> None:
        async with self._rate_limiter_lock:
            now = time.time()
            elapsed = now - self._last_req_time
            if elapsed < self._min_request_interval:
                await asyncio.sleep(self._min_request_interval - elapsed)
            self._last_req_time = time.time()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "client-id": self.client_id,
            "access-token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(8.0, connect=3.0),
                limits=httpx.Limits(max_keepalive_connections=8, max_connections=16),
            )
        return self._client

    async def connect(self) -> bool:
        if self._is_simulated:
            logger.info("DhanProvider operating in high-fidelity simulated test mode (no live keys supplied).")
            self.is_connected = True
            self._running = True
            self._sim_task = asyncio.create_task(self._run_simulated_stream())
            return True

        self._running = True
        try:
            # Validate credentials by querying API
            await self._throttle_request()
            client = await self._get_client()
            resp = await client.get(f"{self.base_url}/marketfeed/quote", headers=self._get_headers())
            if resp.status_code in (200, 400):  # 400 may occur if no query params passed
                ws_params = urlencode({
                    "version": "2",
                    "token": self.access_token,
                    "clientId": self.client_id,
                    "authType": "2",
                })
                ws_url = f"{self.wss_url}?{ws_params}"
                self._ws_client = await websockets.connect(ws_url, ping_interval=20, ping_timeout=40)
                self.is_connected = True
                self._ws_reader_task = asyncio.create_task(self._read_market_feed())
                logger.info("Connected to DhanHQ REST API and live market feed.")
                return True
            logger.warning(f"Dhan REST check failed: {resp.status_code} {resp.text}")
            self.is_connected = False
            return False
        except Exception as e:
            logger.error(f"Failed to connect to DhanHQ: {e}")
            self.is_connected = False
            return False

    async def disconnect(self) -> None:
        self._running = False
        self.is_connected = False
        if self._sim_task:
            self._sim_task.cancel()
            try:
                await self._sim_task
            except asyncio.CancelledError:
                pass
            self._sim_task = None

        if self._ws_client:
            try:
                await self._ws_client.send(json.dumps({"RequestCode": 12}))
                await self._ws_client.close()
            except Exception:
                pass
            self._ws_client = None
        if self._ws_reader_task:
            self._ws_reader_task.cancel()
            try:
                await self._ws_reader_task
            except asyncio.CancelledError:
                pass
            self._ws_reader_task = None
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._client = None
        self._subscribed_ids.clear()
        logger.info("DhanProvider disconnected.")

    def _resolve_subscription(self, instrument_id: str) -> Optional[tuple[str, str, str]]:
        """Resolve an internal/provider identifier to (segment, security ID, symbol)."""
        key = str(instrument_id)
        if key.startswith("IDX_"):
            underlying = key[4:].upper()
            inst = instrument_master.get_index_instrument(underlying)
            if not inst or not inst.instrument_id or inst.instrument_id.startswith("IDX_"):
                return None
            segment = "IDX_B" if inst.exchange.upper().startswith("BSE") else "IDX_I"
            return segment, str(inst.instrument_id), underlying

        meta = self._security_metadata.get(key)
        if meta:
            return meta[2], key, meta[1]
        inst = instrument_master.get_instrument_by_id(key)
        if not inst:
            return None
        segment = "IDX_B" if inst.option_type == "INDEX" and inst.exchange.upper().startswith("BSE") else (
            "IDX_I" if inst.option_type == "INDEX" else
            "BSE_FNO" if inst.exchange.upper().startswith("BSE") else "NSE_FNO"
        )
        return segment, str(inst.instrument_id), inst.symbol

    async def _read_market_feed(self) -> None:
        """Decode Dhan's little-endian quote/full packets and publish live ticks."""
        while self._running and self._ws_client:
            try:
                packet = await self._ws_client.recv()
                if isinstance(packet, bytes):
                    await self._process_market_packet(packet)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._running:
                    logger.error(f"Dhan market feed reader stopped: {exc}")
                self.is_connected = False
                if self._ws_client:
                    try:
                        await self._ws_client.close()
                    except Exception:
                        pass
                    self._ws_client = None
                self._subscribed_ids.clear()
                break

    async def _process_market_packet(self, packet: bytes) -> None:
        if len(packet) < 17:
            return
        try:
            packet_type = packet[0]
            security_id = str(struct.unpack_from("<I", packet, 4)[0])
            meta = self._security_metadata.get(security_id)
            if not meta:
                # Index keys can be registered before option-chain parsing.
                for und in settings.INDICES:
                    inst = instrument_master.get_index_instrument(und)
                    if inst and str(inst.instrument_id) == security_id:
                        segment = "IDX_B" if inst.exchange.upper().startswith("BSE") else "IDX_I"
                        meta = (f"IDX_{und}", und, segment)
                        self._security_metadata[security_id] = meta
                        break
            if not meta:
                return

            internal_id, symbol, segment = meta
            previous = self._last_quotes.get(internal_id)
            now = datetime.now(timezone.utc)
            if packet_type == 8 and len(packet) >= 163:
                ltp = float(struct.unpack_from("<f", packet, 9)[0])
                ltt = int(struct.unpack_from("<i", packet, 15)[0])
                volume = int(struct.unpack_from("<i", packet, 23)[0])
                oi = int(struct.unpack_from("<i", packet, 35)[0])
                day_open = float(struct.unpack_from("<f", packet, 47)[0])
                day_high = float(struct.unpack_from("<f", packet, 55)[0])
                day_low = float(struct.unpack_from("<f", packet, 59)[0])
                bid_qty = int(struct.unpack_from("<i", packet, 63)[0])
                ask_qty = int(struct.unpack_from("<i", packet, 67)[0])
                bid = float(struct.unpack_from("<f", packet, 75)[0])
                ask = float(struct.unpack_from("<f", packet, 79)[0])
                depth = UnifiedDepth(
                    bids=[UnifiedDepthLevel(price=float(struct.unpack_from("<f", packet, 75 + i * 20)[0]), quantity=int(struct.unpack_from("<i", packet, 63 + i * 20)[0])) for i in range(5)],
                    asks=[UnifiedDepthLevel(price=float(struct.unpack_from("<f", packet, 79 + i * 20)[0]), quantity=int(struct.unpack_from("<i", packet, 67 + i * 20)[0])) for i in range(5)],
                )
            elif packet_type == 4 and len(packet) >= 51:
                ltp = float(struct.unpack_from("<f", packet, 9)[0])
                ltt = int(struct.unpack_from("<i", packet, 15)[0])
                volume = int(struct.unpack_from("<i", packet, 23)[0])
                oi = previous.oi if previous else 0
                day_open = float(struct.unpack_from("<f", packet, 35)[0])
                day_high = float(struct.unpack_from("<f", packet, 43)[0])
                day_low = float(struct.unpack_from("<f", packet, 47)[0])
                bid = previous.bid if previous else None
                ask = previous.ask if previous else None
                bid_qty = previous.bid_qty if previous else None
                ask_qty = previous.ask_qty if previous else None
                depth = previous.depth if previous else None
            elif packet_type == 2 and len(packet) >= 17:
                ltp = float(struct.unpack_from("<f", packet, 9)[0])
                ltt = int(struct.unpack_from("<i", packet, 13)[0])
                volume = previous.volume if previous else 0
                oi = previous.oi if previous else 0
                day_open = previous.open if previous else None
                day_high = previous.high if previous else None
                day_low = previous.low if previous else None
                bid = previous.bid if previous else None
                ask = previous.ask if previous else None
                bid_qty = previous.bid_qty if previous else None
                ask_qty = previous.ask_qty if previous else None
                depth = previous.depth if previous else None
            elif packet_type == 5 and len(packet) >= 13:
                if previous:
                    previous.oi = int(struct.unpack_from("<i", packet, 9)[0])
                return
            else:
                return

            quote = UnifiedQuote(
                instrument_id=internal_id,
                symbol=symbol,
                exchange="BSE" if segment.startswith("BSE") or segment == "IDX_B" else "NSE",
                ltp=ltp,
                bid=bid,
                ask=ask,
                bid_qty=bid_qty,
                ask_qty=ask_qty,
                volume=volume,
                oi=oi,
                open=day_open,
                high=day_high,
                low=day_low,
                exchange_timestamp=datetime.fromtimestamp(ltt, timezone.utc) if ltt > 0 else now,
                receive_timestamp=now,
                source="dhan_ws",
                depth=depth,
            )
            self._last_quotes[internal_id] = quote
            self._last_quotes[security_id] = quote
            await self._emit_quote(quote)
        except (struct.error, ValueError, OverflowError) as exc:
            logger.warning(f"Rejected malformed Dhan feed packet: {exc}")

    async def _fetch_public_index_prices(self) -> Dict[str, float]:
        """Fetches real-time actual index prices from public financial endpoints."""
        symbols = {
            "NIFTY": "^NSEI",
            "BANKNIFTY": "^NSEBANK",
            "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
            "MIDCPNIFTY": "NIFTY_MID_SELECT.NS",
            "SENSEX": "^BSESN",
            "BANKEX": "BSE-BANK.BO",
        }
        fetched = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                for name, sym in symbols.items():
                    try:
                        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d"
                        r = await client.get(url, headers=headers)
                        if r.status_code == 200:
                            meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
                            val = meta.get("regularMarketPrice")
                            if val:
                                fetched[name] = float(val)
                    except Exception:
                        pass
        except Exception:
            pass
        return fetched

    async def _run_simulated_stream(self) -> None:
        """Emits continuous, high-fidelity simulated ticks synced to real-time market prices."""
        indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
        prices = dict(self._current_sim_prices)
        steps = {
            "NIFTY": 1.5,
            "BANKNIFTY": 6.0,
            "FINNIFTY": 2.5,
            "MIDCPNIFTY": 1.2,
            "SENSEX": 9.0,
            "BANKEX": 6.5,
        }

        # Initial synchronization with real-world prices
        try:
            real_prices = await self._fetch_public_index_prices()
            for k, v in real_prices.items():
                prices[k] = v
            logger.info(f"Synchronized real-world index baseline prices: {prices}")
        except Exception as e:
            logger.warning(f"Using default index baselines: {e}")

        self._current_sim_prices = prices
        tick_counter = 0

        while self._running and self.is_connected:
            try:
                now = datetime.now(timezone.utc)
                tick_counter += 1

                # Periodically re-sync with real market prices every 60 seconds
                if tick_counter % 60 == 0:
                    real = await self._fetch_public_index_prices()
                    for k, v in real.items():
                        prices[k] = v

                for und in indices:
                    drift = (random.random() - 0.48) * steps.get(und, 2.0)
                    prices[und] = round(prices[und] + drift, 2)
                    inst_id = f"IDX_{und}"
                    spread = 0.5
                    quote = UnifiedQuote(
                        instrument_id=inst_id,
                        symbol=und,
                        exchange="BSE" if und in ("SENSEX", "BANKEX") else "NSE",
                        ltp=prices[und],
                        bid=round(prices[und] - spread / 2, 2),
                        ask=round(prices[und] + spread / 2, 2),
                        bid_qty=5000,
                        ask_qty=4800,
                        volume=150000,
                        oi=850000,
                        exchange_timestamp=now,
                        source="dhan_sim",
                        depth=UnifiedDepth(
                            bids=[UnifiedDepthLevel(price=round(prices[und] - spread / 2, 2), quantity=2500)],
                            asks=[UnifiedDepthLevel(price=round(prices[und] + spread / 2, 2), quantity=2400)],
                        ),
                    )
                    await self._emit_quote(quote)

                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in simulated feed loop: {e}")
                await asyncio.sleep(1.0)

    async def subscribe_quotes(self, instrument_ids: List[str]) -> None:
        subscriptions = []
        for internal_id in instrument_ids:
            already_subscribed = internal_id in self._subscribed_ids
            self._subscribed_ids.add(internal_id)
            if already_subscribed:
                continue
            resolved = self._resolve_subscription(internal_id)
            if not resolved:
                logger.warning(f"Cannot subscribe to Dhan instrument without a live security ID: {internal_id}")
                continue
            segment, security_id, symbol = resolved
            self._security_metadata[security_id] = (internal_id if internal_id.startswith("IDX_") else security_id, symbol, segment)
            subscriptions.append({"ExchangeSegment": segment, "SecurityId": security_id})

        if self._ws_client and self.is_connected and subscriptions:
            # Dhan accepts at most 100 instruments per JSON subscription.
            for start in range(0, len(subscriptions), 100):
                batch = subscriptions[start:start + 100]
                await self._ws_client.send(json.dumps({
                    "RequestCode": 21,  # Full packet: quote, OI and five-level depth
                    "InstrumentCount": len(batch),
                    "InstrumentList": batch,
                }))

    def _overlay_live_quotes(self, snapshot: OptionChainSnapshot) -> OptionChainSnapshot:
        now = datetime.now(timezone.utc)
        spot = self._last_quotes.get(f"IDX_{snapshot.underlying}")
        if spot and (now - spot.receive_timestamp).total_seconds() <= settings.MAX_QUOTE_STALENESS_SECONDS:
            snapshot.underlying_ltp = spot.ltp
        for options in snapshot.contracts.values():
            for contract in options.values():
                quote = self._last_quotes.get(contract.instrument_id)
                if not quote or (now - quote.receive_timestamp).total_seconds() > settings.MAX_QUOTE_STALENESS_SECONDS:
                    continue
                contract.ltp = quote.ltp
                if quote.bid and quote.bid > 0:
                    contract.bid = quote.bid
                if quote.ask and quote.ask > 0:
                    contract.ask = quote.ask
                contract.bid_qty = quote.bid_qty
                contract.ask_qty = quote.ask_qty
                contract.volume = quote.volume if quote.volume is not None else contract.volume
                contract.oi = quote.oi if quote.oi is not None else contract.oi
        return snapshot

    async def unsubscribe_quotes(self, instrument_ids: List[str]) -> None:
        for i_id in instrument_ids:
            self._subscribed_ids.discard(i_id)

    async def get_quote_snapshot(self, instrument_id: str) -> Optional[UnifiedQuote]:
        """Fetches normalized quote snapshot via REST."""
        if self._is_simulated:
            return self._generate_simulated_quote(instrument_id)

        cached = self._last_quotes.get(instrument_id)
        if cached and (datetime.now(timezone.utc) - cached.receive_timestamp).total_seconds() <= settings.MAX_QUOTE_STALENESS_SECONDS:
            return cached

        await self._throttle_request()
        try:
            resolved = self._resolve_subscription(instrument_id)
            if not resolved:
                return None
            segment, security_id, _ = resolved
            client = await self._get_client()
            resp = await client.post(
                f"{self.base_url}/marketfeed/quote",
                headers=self._get_headers(),
                json={segment: [security_id]},
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get(segment, {}).get(security_id, {})
                quote = self._parse_dhan_rest_quote(instrument_id, data)
                self._last_quotes[instrument_id] = quote
                return quote
        except Exception as e:
            logger.error(f"Error fetching Dhan quote snapshot for {instrument_id}: {e}")
        return None

    def _parse_dhan_rest_quote(self, instrument_id: str, data: Dict[str, Any]) -> UnifiedQuote:
        now = datetime.now(timezone.utc)
        depth_data = data.get("depth", {})
        depth = UnifiedDepth()
        for b in depth_data.get("buy", []):
            depth.bids.append(UnifiedDepthLevel(price=b.get("price", 0.0), quantity=b.get("quantity", 0)))
        for a in depth_data.get("sell", []):
            depth.asks.append(UnifiedDepthLevel(price=a.get("price", 0.0), quantity=a.get("quantity", 0)))

        return UnifiedQuote(
            instrument_id=instrument_id,
            symbol=data.get("trading_symbol", instrument_id),
            exchange=data.get("exchange", "NSE"),
            ltp=float(data.get("last_price", 0.0)),
            bid=depth.bids[0].price if depth.bids else float(data.get("buy_price", 0.0)),
            ask=depth.asks[0].price if depth.asks else float(data.get("sell_price", 0.0)),
            bid_qty=depth.bids[0].quantity if depth.bids else int(data.get("buy_quantity", 0)),
            ask_qty=depth.asks[0].quantity if depth.asks else int(data.get("sell_quantity", 0)),
            volume=int(data.get("volume", 0)),
            oi=int(data.get("oi", 0)),
            high=float(data.get("high", 0.0)),
            low=float(data.get("low", 0.0)),
            open=float(data.get("open", 0.0)),
            prev_close=float(data.get("prev_close", 0.0)),
            exchange_timestamp=now,
            source="dhan",
            depth=depth,
        )

    async def get_historical_candles(
        self, instrument_id: str, timeframe: str, count: int = 100
    ) -> List[UnifiedCandle]:
        """Fetches historical candles from Dhan."""
        if self._is_simulated:
            return self._generate_simulated_candles(instrument_id, timeframe, count)

        await self._throttle_request()
        try:
            # Dhan interval: 1, 5, 15, 25, 60
            interval_map = {"1m": "1", "5m": "5", "15m": "15", "1d": "D"}
            iv = interval_map.get(timeframe, "5")
            to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            from_date = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")

            index_inst = instrument_master.get_index_instrument(instrument_id.replace("IDX_", "")) if instrument_id.startswith("IDX_") else None
            payload = {
                "securityId": str(index_inst.instrument_id) if index_inst else instrument_id,
                "exchangeSegment": ("IDX_B" if index_inst.exchange.upper().startswith("BSE") else "IDX_I") if index_inst else "NSE_FNO",
                "instrument": "INDEX" if index_inst else "OPTIDX",
                "interval": iv,
                "fromDate": from_date,
                "toDate": to_date,
            }
            client = await self._get_client()
            resp = await client.post(f"{self.base_url}/charts/intraday", headers=self._get_headers(), json=payload)
            if resp.status_code == 200:
                raw = resp.json().get("data", {})
                return self._parse_dhan_candles(instrument_id, timeframe, raw)
        except Exception as e:
            logger.error(f"Error fetching Dhan candles: {e}")
        return []

    def _parse_dhan_candles(
        self, instrument_id: str, timeframe: str, raw: Dict[str, Any]
    ) -> List[UnifiedCandle]:
        candles: List[UnifiedCandle] = []
        timestamps = raw.get("timestamp", [])
        opens = raw.get("open", [])
        highs = raw.get("high", [])
        lows = raw.get("low", [])
        closes = raw.get("close", [])
        volumes = raw.get("volume", [])

        for i in range(len(timestamps)):
            dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            candles.append(
                UnifiedCandle(
                    instrument_id=instrument_id,
                    symbol=instrument_id,
                    timeframe=timeframe,
                    timestamp=dt,
                    open=float(opens[i]),
                    high=float(highs[i]),
                    low=float(lows[i]),
                    close=float(closes[i]),
                    volume=int(volumes[i]) if i < len(volumes) else 0,
                )
            )
        return candles

    async def get_option_chain_snapshot(
        self, underlying: str, expiry: Optional[datetime] = None
    ) -> Optional[OptionChainSnapshot]:
        """Fetches and briefly caches Dhan's official optionchain snapshot."""
        now = datetime.now(timezone.utc)
        if self._is_simulated:
            return self._generate_simulated_option_chain(underlying, expiry or instrument_master.get_exchange_weekly_expiry(underlying, now))

        index_inst = instrument_master.get_index_instrument(underlying)
        if not index_inst or not str(index_inst.instrument_id).isdigit():
            logger.error(f"No numeric Dhan underlying security ID loaded for {underlying}; refusing chain fallback.")
            return None
        segment = "IDX_B" if index_inst.exchange.upper().startswith("BSE") else "IDX_I"
        resolved_expiry = expiry or await self._get_active_expiry(underlying, index_inst.instrument_id, segment)
        cache_key = f"{underlying.upper()}:{resolved_expiry:%Y-%m-%d}"
        cached = self._chain_cache.get(cache_key)
        if cached and time.time() - cached[1] < 3.0:
            return self._overlay_live_quotes(cached[0])

        await self._throttle_request()
        try:
            client = await self._get_client()
            resp = await client.post(
                f"{self.base_url}/optionchain",
                headers=self._get_headers(),
                json={"UnderlyingScrip": int(index_inst.instrument_id), "UnderlyingSeg": segment, "Expiry": resolved_expiry.strftime("%Y-%m-%d")},
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                if data:
                    snapshot = self._parse_dhan_option_chain(underlying, data)
                    self._chain_cache[cache_key] = (snapshot, time.time())
                    return self._overlay_live_quotes(snapshot)
            logger.warning(f"Dhan option chain returned HTTP {resp.status_code} for {underlying}: {resp.text[:300]}")
        except Exception as e:
            logger.error(f"Error fetching Dhan option chain: {e}")
        return None

    async def _get_active_expiry(self, underlying: str, security_id: str, segment: str) -> datetime:
        cached = self._expiry_cache.get(underlying.upper())
        today_ist = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()
        if cached and time.time() - cached[1] < 600 and cached[0] >= today_ist.isoformat():
            return datetime.strptime(cached[0], "%Y-%m-%d").replace(hour=10, tzinfo=timezone.utc)
        try:
            await self._throttle_request()
            client = await self._get_client()
            resp = await client.post(
                f"{self.base_url}/optionchain/expirylist",
                headers=self._get_headers(),
                json={"UnderlyingScrip": int(security_id), "UnderlyingSeg": segment},
            )
            if resp.status_code == 200:
                expiries = sorted(x for x in resp.json().get("data", []) if x >= today_ist.isoformat())
                if expiries:
                    self._expiry_cache[underlying.upper()] = (expiries[0], time.time())
                    return datetime.strptime(expiries[0], "%Y-%m-%d").replace(hour=10, tzinfo=timezone.utc)
        except Exception as exc:
            logger.warning(f"Could not resolve active Dhan expiry for {underlying}: {exc}")
        return instrument_master.get_exchange_weekly_expiry(underlying, datetime.now(timezone.utc))

    def _parse_dhan_option_chain(
        self, underlying: str, data: Dict[str, Any]
    ) -> OptionChainSnapshot:
        now = datetime.now(timezone.utc)
        spot_price = float(data.get("last_price", 0.0))
        contracts_dict: Dict[float, Dict[str, OptionContractData]] = {}
        total_ce_oi = 0
        total_pe_oi = 0

        exp_date_str = data.get("expiry") or ""
        chain_expiry = None
        if exp_date_str:
            for fmt in ("%Y-%m-%d", "%d-%b-%Y"):
                try:
                    chain_expiry = datetime.strptime(exp_date_str, fmt).replace(
                        hour=10, minute=0, tzinfo=timezone.utc
                    )
                    break
                except ValueError:
                    pass
        if not chain_expiry:
            chain_expiry = instrument_master.get_exchange_weekly_expiry(underlying, now)

        for strike_str, strike_data in data.get("oc", {}).items():
            try:
                strike_val = float(strike_str)
            except ValueError:
                continue

            contracts_dict[strike_val] = {}
            for opt_type in ["ce", "pe"]:
                item = strike_data.get(opt_type)
                if not item:
                    continue
                type_upper = opt_type.upper()
                oi = int(item.get("oi", 0))
                if type_upper == "CE":
                    total_ce_oi += oi
                else:
                    total_pe_oi += oi

                g = item.get("greeks", {})
                greeks = OptionGreeks(
                    delta=float(g["delta"]) if g.get("delta") is not None else None,
                    theta=float(g.get("theta", 0.0)),
                    gamma=float(g.get("gamma", 0.0)),
                    vega=float(g.get("vega", 0.0)),
                    iv=float(item.get("implied_volatility", 0.0)),
                )

                contracts_dict[strike_val][type_upper] = OptionContractData(
                    instrument_id=str(item.get("security_id", "")),
                    symbol=str(item.get("trading_symbol", "")),
                    strike=strike_val,
                    option_type=type_upper,
                    expiry=chain_expiry,
                    ltp=float(item.get("last_price", 0.0)),
                    bid=float(item["top_bid_price"]) if item.get("top_bid_price") else None,
                    ask=float(item["top_ask_price"]) if item.get("top_ask_price") else None,
                    volume=int(item.get("volume", 0)),
                    oi=oi,
                    oi_change=int(item.get("oi_change", 0)),
                    greeks=greeks,
                    lot_size=instrument_master.get_index_config(underlying).lot_size
                    if instrument_master.get_index_config(underlying)
                    else 50,
                )
                security_id = str(item.get("security_id", ""))
                cfg = instrument_master.get_index_config(underlying)
                segment = "BSE_FNO" if cfg and cfg.exchange.upper().startswith("BSE") else "NSE_FNO"
                if security_id:
                    self._security_metadata[security_id] = (security_id, str(item.get("trading_symbol", "")), segment)

        pcr = round(total_pe_oi / max(total_ce_oi, 1), 3)
        return OptionChainSnapshot(
            underlying=underlying,
            underlying_ltp=spot_price,
            snapshot_timestamp=now,
            expiry=chain_expiry,
            contracts=contracts_dict,
            pcr_oi=pcr,
            source="dhan_rest",
            is_simulated=False,
        )

    # --- Simulated Generator Functions for Offline / Replay / Unit Tests ---
    def _generate_simulated_quote(self, instrument_id: str) -> UnifiedQuote:
        now = datetime.now(timezone.utc)
        base_spot = 22350.0 if "NIFTY" in instrument_id else 54775.0
        bid = round(base_spot - 0.25, 2)
        ask = round(base_spot + 0.25, 2)
        return UnifiedQuote(
            instrument_id=instrument_id,
            symbol=instrument_id,
            exchange="NSE",
            ltp=base_spot,
            bid=bid,
            ask=ask,
            bid_qty=5000,
            ask_qty=4800,
            volume=150000,
            oi=850000,
            exchange_timestamp=now,
            source="dhan_sim",
            depth=UnifiedDepth(
                bids=[UnifiedDepthLevel(price=bid, quantity=2500)],
                asks=[UnifiedDepthLevel(price=ask, quantity=2400)],
            ),
        )

    def _generate_simulated_candles(
        self, instrument_id: str, timeframe: str, count: int
    ) -> List[UnifiedCandle]:
        """Generates realistic multi-directional candles supporting both bullish and bearish setups.

        Strategy: Simulates an intraday ORB session where:
        - First N bars (ORB formation) are choppy/ranging
        - Last bar is a decisive breakout (bullish) or breakdown (bearish)
        - Direction is derived from recent price drift in simulation (deterministic per session)
        """
        now = datetime.now(timezone.utc)
        minutes_step = 5 if timeframe == "5m" else (15 if timeframe == "15m" else 1)
        candles: List[UnifiedCandle] = []

        # Use live sim prices for realistic base level
        und = instrument_id.replace("IDX_", "")
        base = self._current_sim_prices.get(und, 22300.0)
        step = 50.0  # Nifty step; good enough for simulation

        # Determine session direction: use minute of the hour as a pseudo-random seed
        # This makes the direction change every 5-minute window in a realistic way
        session_minute = now.minute
        is_bearish = (session_minute % 10) >= 5  # Alternates every 5 minutes

        # ORB Range: first 3 bars form the range
        orb_bars = max(3, count // 4)
        orb_high = base + step * 0.4
        orb_low = base - step * 0.3
        candle_vol_base = 20000

        for i in range(count, 0, -1):
            ts = now - timedelta(minutes=i * minutes_step)
            bar_idx = count - i  # 0-indexed from oldest

            if bar_idx < orb_bars:
                # ORB formation: choppy within range
                drift = (bar_idx % 2) * 2.0 - 1.0  # alternates ±1
                o = round((orb_high + orb_low) / 2 + drift, 2)
                h = round(orb_high - step * 0.05, 2)
                l = round(orb_low + step * 0.05, 2)
                c = round(o + drift * 0.5, 2)
                vol = candle_vol_base + bar_idx * 500
            elif bar_idx == count - 1:
                # Last / most recent bar: decisive breakout or breakdown
                if is_bearish:
                    # Bearish: close firmly below ORB low with body strength
                    o = round(orb_low + step * 0.02, 2)
                    c = round(orb_low - step * 0.25, 2)  # closes well below
                    h = round(o + step * 0.05, 2)
                    l = round(c - step * 0.05, 2)
                else:
                    # Bullish: close firmly above ORB high with body strength
                    o = round(orb_high - step * 0.02, 2)
                    c = round(orb_high + step * 0.25, 2)  # closes well above
                    h = round(c + step * 0.05, 2)
                    l = round(o - step * 0.05, 2)
                vol = candle_vol_base * 3  # High volume on breakout
            else:
                # Middle bars: trending into the breakout
                progress = (bar_idx - orb_bars) / max(count - orb_bars - 1, 1)
                if is_bearish:
                    o = round(orb_low + step * 0.1 * (1 - progress), 2)
                    c = round(o - step * 0.05, 2)
                    h = round(o + step * 0.05, 2)
                    l = round(c - step * 0.05, 2)
                else:
                    o = round(orb_high - step * 0.1 * (1 - progress), 2)
                    c = round(o + step * 0.05, 2)
                    h = round(c + step * 0.05, 2)
                    l = round(o - step * 0.05, 2)
                vol = candle_vol_base + bar_idx * 300

            candles.append(
                UnifiedCandle(
                    instrument_id=instrument_id,
                    symbol=instrument_id,
                    timeframe=timeframe,
                    timestamp=ts,
                    open=round(o, 2),
                    high=round(h, 2),
                    low=round(l, 2),
                    close=round(c, 2),
                    volume=vol,
                    oi=900000,
                )
            )
        return candles

    def _generate_simulated_option_chain(
        self, underlying: str, expiry: datetime
    ) -> OptionChainSnapshot:
        """Generates a realistic option chain using live sim prices.

        Includes directionally-biased PCR and OI to match expected breadth.
        """
        now = datetime.now(timezone.utc)
        if not expiry:
            expiry = instrument_master.get_exchange_weekly_expiry(underlying, now)
        cfg = instrument_master.get_index_config(underlying)
        step = cfg.strike_interval if cfg else 50.0
        lot_size = cfg.lot_size if cfg else 75

        # Use live simulated prices (synced from real market every 60s)
        spot = self._current_sim_prices.get(underlying, 25000.0)
        atm = round(spot / step) * step

        # Session direction matches candle generator: bearish when minute%10 >= 5
        is_bearish = (now.minute % 10) >= 5
        # Bearish: more Put OI, lower PCR denominator; Bullish: more Call OI
        pcr = 1.35 if is_bearish else 0.78  # bearish → high PCR, bullish → low PCR

        contracts_dict: Dict[float, Dict[str, OptionContractData]] = {}
        for off in range(-6, 7):
            strike = atm + (off * step)
            distance = abs(off)

            # Realistic premium scaling: ATM ≈ 120, drops with distance
            # CE is ITM (dist < 0 means strike < spot) → higher premium
            atm_prem = 120.0
            dist_factor = distance * 0.45 * step
            ce_raw = atm_prem - (spot - strike) * 0.50
            pe_raw = atm_prem + (spot - strike) * 0.50
            ce_prem = max(3.0, round(ce_raw, 2))
            pe_prem = max(3.0, round(pe_raw, 2))

            # Delta: decreases as we go OTM
            # CE: positive delta, decreasing as strike moves above spot
            # PE: negative delta, increasing magnitude as strike moves below spot
            ce_delta_base = 0.52 - off * 0.05
            pe_delta_base = -0.48 + off * 0.05
            ce_delta = max(0.05, min(0.98, round(ce_delta_base, 3)))
            pe_delta = min(-0.05, max(-0.98, round(pe_delta_base, 3)))

            # OI: ATM has highest OI, drops exponentially with distance
            oi_scale = max(0.1, 1.0 - distance * 0.12)
            # Directional OI bias: bearish → more PE OI at/near ATM
            pe_oi_bias = 1.4 if is_bearish else 0.85
            ce_oi_bias = 0.80 if is_bearish else 1.35

            ce_oi = int(120000 * oi_scale * ce_oi_bias)
            pe_oi = int(120000 * oi_scale * pe_oi_bias)
            ce_vol = int(50000 * oi_scale * ce_oi_bias)
            pe_vol = int(45000 * oi_scale * pe_oi_bias)

            ce_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "CE")
            pe_sym = instrument_master.format_contract_symbol(underlying, expiry, strike, "PE")

            contracts_dict[strike] = {
                "CE": OptionContractData(
                    instrument_id=f"D_{underlying}_{int(strike)}_CE",
                    symbol=ce_sym,
                    strike=strike,
                    option_type="CE",
                    expiry=expiry,
                    ltp=ce_prem,
                    bid=round(ce_prem - 0.5, 2),
                    ask=round(ce_prem + 0.5, 2),
                    volume=ce_vol,
                    oi=ce_oi,
                    oi_change=int(ce_oi * (0.02 if not is_bearish else -0.015)),
                    greeks=OptionGreeks(delta=ce_delta, iv=14.5, theta=-12.0),
                    lot_size=lot_size,
                ),
                "PE": OptionContractData(
                    instrument_id=f"D_{underlying}_{int(strike)}_PE",
                    symbol=pe_sym,
                    strike=strike,
                    option_type="PE",
                    expiry=expiry,
                    ltp=pe_prem,
                    bid=round(pe_prem - 0.5, 2),
                    ask=round(pe_prem + 0.5, 2),
                    volume=pe_vol,
                    oi=pe_oi,
                    oi_change=int(pe_oi * (0.025 if is_bearish else -0.012)),
                    greeks=OptionGreeks(delta=pe_delta, iv=14.8, theta=-11.5),
                    lot_size=lot_size,
                ),
            }

        return OptionChainSnapshot(
            underlying=underlying,
            underlying_ltp=spot,
            snapshot_timestamp=now,
            expiry=expiry,
            contracts=contracts_dict,
            pcr_oi=pcr,
            pcr_volume=round(pcr * 0.9, 3),
            source="dhan_sim",
            is_simulated=True,
        )
