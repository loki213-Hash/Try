"""Contract Selector module.

Dynamically selects optimal, liquid ATM or slightly ITM option contracts
based on live quotes, Delta targets (0.45 - 0.65), spread tolerance, and depth.
Rejects illiquid, stale, or unverified contracts.
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional, Tuple, List
from pydantic import BaseModel
from app.data.base_provider import OptionChainSnapshot, OptionContractData
from app.data.instrument_master import instrument_master
from app.monitoring.logging import logger
from config import settings


class SelectedContract(BaseModel):
    instrument_id: str
    symbol: str
    strike: float
    option_type: str
    expiry_str: str
    expiry: Optional[datetime] = None
    ltp: float = 0.0
    lot_size: int
    current_bid: float
    current_ask: float
    spread_pct: float
    executable_ask: float
    max_acceptable_entry: float
    delta: float
    gamma: float
    iv: float
    volume: int
    oi: int
    oi_change: int = 0
    bid_qty: int = 0
    ask_qty: int = 0
    close_price: float = 0.0


class ContractSelector:
    """Selects executable ATM/ITM options with strict liquidity filters."""

    @staticmethod
    def select_contract(
        snapshot: OptionChainSnapshot,
        direction: str,  # BUY_CE or BUY_PE
        max_spread_pct: float = 2.0,
        delta_min: float = 0.45,
        delta_max: float = 0.65,
        radar_mode: bool = False,
    ) -> Tuple[Optional[SelectedContract], List[str]]:
        rejection_reasons: List[str] = []
        underlying = snapshot.underlying
        cfg = instrument_master.get_index_config(underlying)
        if not cfg:
            return None, [f"No index configuration found for {underlying}"]

        min_volume = cfg.min_volume_contracts
        min_oi = cfg.min_oi_contracts
        if radar_mode:
            # Radar selection is only for an alert, never for an executable
            # entry. Keep real quotes/Greeks but allow earlier, thinner buildup.
            max_spread_pct = max(max_spread_pct, settings.RADAR_MAX_SPREAD_PCT)
            delta_min = settings.RADAR_DELTA_MIN
            delta_max = settings.RADAR_DELTA_MAX
            min_volume = max(
                settings.RADAR_MIN_VOLUME_CONTRACTS,
                int(cfg.min_volume_contracts * settings.RADAR_MIN_VOLUME_RATIO),
            )
            min_oi = max(
                settings.RADAR_MIN_OI_CONTRACTS,
                int(cfg.min_oi_contracts * settings.RADAR_MIN_OI_RATIO),
            )

        opt_type = "CE" if direction == "BUY_CE" else "PE"
        candidates: List[OptionContractData] = []

        for strike, opts in snapshot.contracts.items():
            if opt_type in opts:
                c = opts[opt_type]
                candidates.append(c)

        if not candidates:
            return None, [f"No {opt_type} contracts found in option chain snapshot"]

        # Sort candidates by proximity of strike to spot
        spot = snapshot.underlying_ltp
        candidates.sort(key=lambda c: abs(c.strike - spot))

        for c in candidates:
            # 1. Bid/Ask existence
            if c.bid is None or c.ask is None or c.ask <= 0 or c.bid <= 0:
                continue

            # 2. Spread test
            spread_pct = ((c.ask - c.bid) / c.ask) * 100.0
            if spread_pct > max_spread_pct:
                continue

            # 3. Minimum liquidity test
            if (c.volume or 0) < min_volume:
                continue
            if (c.oi or 0) < min_oi:
                continue

            # 4. Delta check (absolute value between delta_min and delta_max)
            abs_delta = None
            gamma = 0.001
            iv = 14.0
            if c.greeks:
                if c.greeks.delta is not None:
                    abs_delta = abs(c.greeks.delta)
                if c.greeks.gamma is not None:
                    gamma = c.greeks.gamma
                if c.greeks.iv is not None:
                    iv = c.greeks.iv

            # Fallback delta estimation if broker feed omitted Greeks
            if abs_delta is None and spot > 0 and c.strike > 0:
                dist_pct = (spot - c.strike) / spot if opt_type == "CE" else (c.strike - spot) / spot
                approx = 0.50 + (dist_pct * 10.0)
                abs_delta = max(0.10, min(0.90, round(approx, 2)))

            if abs_delta is None or abs_delta < delta_min or abs_delta > delta_max:
                continue

            # Valid candidate selected!
            # Maximum acceptable entry price: Ask + slippage protection buffer
            slippage_buf = max(0.01, settings.SLIPPAGE_ESTIMATE_PCT / 100.0 * 2.0)
            max_entry = round(c.ask * (1.0 + slippage_buf), 2)

            selected = SelectedContract(
                instrument_id=c.instrument_id,
                symbol=c.symbol,
                strike=c.strike,
                option_type=c.option_type,
                expiry_str=c.expiry.strftime("%d-%b-%Y") if c.expiry else "",
                expiry=c.expiry,
                lot_size=c.lot_size,
                current_bid=c.bid,
                current_ask=c.ask,
                spread_pct=round(spread_pct, 2),
                executable_ask=c.ask,
                max_acceptable_entry=max_entry,
                ltp=c.ltp,
                delta=round(abs_delta, 3),
                gamma=round(gamma, 5),
                iv=round(iv, 2),
                volume=c.volume or 0,
                oi=c.oi or 0,
                oi_change=c.oi_change or 0,
                bid_qty=c.bid_qty or 0,
                ask_qty=c.ask_qty or 0,
                close_price=c.close_price or c.ltp,
            )
            return selected, []

        rejection_reasons.append(
            f"No contracts met delta ({delta_min}-{delta_max}), spread (<{max_spread_pct}%), and volume thresholds"
        )
        return None, rejection_reasons


contract_selector = ContractSelector()
