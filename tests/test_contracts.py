"""Unit tests for contract resolution, delta/spread filters, and targets."""

import pytest
from datetime import datetime, timezone, timedelta

from app.signals.contract_selector import contract_selector, SelectedContract
from app.signals.targets import targets_engine
from app.data.base_provider import (
    OptionChainSnapshot,
    OptionContractData,
    OptionGreeks,
)


@pytest.fixture
def sample_option_chain():
    now = datetime.now(timezone.utc)
    expiry = now + timedelta(days=3)
    spot = 25120.0
    contracts = {
        25100.0: {
            "CE": OptionContractData(
                instrument_id="NIFTY25100CE",
                symbol="NIFTY25100CE",
                strike=25100.0,
                option_type="CE",
                expiry=expiry,
                ltp=150.0,
                bid=149.5,
                ask=150.5,
                volume=12000,
                oi=50000,
                greeks=OptionGreeks(delta=0.52, iv=13.5, gamma=0.001),
                lot_size=75,
            ),
            "PE": OptionContractData(
                instrument_id="NIFTY25100PE",
                symbol="NIFTY25100PE",
                strike=25100.0,
                option_type="PE",
                expiry=expiry,
                ltp=110.0,
                bid=109.5,
                ask=110.5,
                volume=11000,
                oi=48000,
                greeks=OptionGreeks(delta=-0.48, iv=13.8, gamma=0.001),
                lot_size=75,
            ),
        },
        25150.0: {
            "CE": OptionContractData(
                instrument_id="NIFTY25150CE",
                symbol="NIFTY25150CE",
                strike=25150.0,
                option_type="CE",
                expiry=expiry,
                ltp=120.0,
                bid=119.5,
                ask=120.5,
                volume=15000,
                oi=60000,
                greeks=OptionGreeks(delta=0.48, iv=13.6, gamma=0.001),
                lot_size=75,
            ),
            "PE": OptionContractData(
                instrument_id="NIFTY25150PE",
                symbol="NIFTY25150PE",
                strike=25150.0,
                option_type="PE",
                expiry=expiry,
                ltp=140.0,
                bid=139.5,
                ask=140.5,
                volume=14000,
                oi=55000,
                greeks=OptionGreeks(delta=-0.52, iv=13.7, gamma=0.001),
                lot_size=75,
            ),
        },
    }
    return OptionChainSnapshot(
        underlying="NIFTY",
        underlying_ltp=spot,
        snapshot_timestamp=now,
        expiry=expiry,
        contracts=contracts,
        pcr_oi=1.05,
    )


def test_contract_selector_resolves_liquid_atm_ce(sample_option_chain):
    contract, errs = contract_selector.select_contract(
        snapshot=sample_option_chain,
        direction="BUY_CE",
        max_spread_pct=2.0,
        delta_min=0.45,
        delta_max=0.65,
    )
    assert contract is not None
    assert contract.symbol in ("NIFTY25100CE", "NIFTY25150CE")
    assert 0.45 <= contract.delta <= 0.65
    assert contract.executable_ask > 0
    assert contract.max_acceptable_entry > contract.executable_ask


def test_targets_engine_computes_three_realistic_targets(sample_option_chain):
    contract, _ = contract_selector.select_contract(
        snapshot=sample_option_chain,
        direction="BUY_CE",
    )
    assert contract is not None

    levels, errs = targets_engine.calculate_levels(
        contract=contract,
        underlying_spot=25120.0,
        breakout_candle_high=25130.0,
        breakout_candle_low=25090.0,
        orb_boundary=25100.0,
        direction="BUY_CE",
    )
    assert levels is not None
    assert levels.sl_price < contract.executable_ask
    assert levels.tp1_price > contract.executable_ask
    assert levels.tp2_price > levels.tp1_price
    assert levels.tp3_price > levels.tp2_price
    assert levels.planned_rr_tp1 == 1.5
    assert levels.planned_rr_tp2 == 2.5
    assert levels.planned_rr_tp3 == 4.0


def test_exchange_weekly_expiry_calendar_accuracy():
    """Verify that weekly expiries strictly align with Indian exchange rules and never land on weekends."""
    from app.data.instrument_master import get_exchange_weekly_expiry, format_contract_symbol

    # Reference Wednesday: Oct 14, 2026 09:30 UTC (15:00 IST)
    ref_wed = datetime(2026, 10, 14, 9, 30, tzinfo=timezone.utc)

    # 1. SENSEX must expire on Friday (Oct 16, 2026)
    sensex_exp = get_exchange_weekly_expiry("SENSEX", ref_wed)
    assert sensex_exp.weekday() == 4, "SENSEX must expire on Friday"
    assert sensex_exp.date().day == 16
    assert sensex_exp.strftime("%d-%b-%Y") == "16-Oct-2026"

    # 2. NIFTY must expire on Thursday (Oct 15, 2026)
    nifty_exp = get_exchange_weekly_expiry("NIFTY", ref_wed)
    assert nifty_exp.weekday() == 3, "NIFTY must expire on Thursday"
    assert nifty_exp.date().day == 15
    assert nifty_exp.strftime("%d-%b-%Y") == "15-Oct-2026"

    # 3. BANKNIFTY must expire on Wednesday (Today: Oct 14, 2026 before market close)
    bn_exp = get_exchange_weekly_expiry("BANKNIFTY", ref_wed)
    assert bn_exp.weekday() == 2, "BANKNIFTY must expire on Wednesday"
    assert bn_exp.date().day == 14
    assert bn_exp.strftime("%d-%b-%Y") == "14-Oct-2026"

    # 4. BANKNIFTY after market close (10:15 UTC = 15:45 IST) rolls to next Wednesday (Oct 21, 2026)
    ref_wed_after_close = datetime(2026, 10, 14, 10, 15, tzinfo=timezone.utc)
    bn_exp_next = get_exchange_weekly_expiry("BANKNIFTY", ref_wed_after_close)
    assert bn_exp_next.weekday() == 2
    assert bn_exp_next.date().day == 21
    assert bn_exp_next.strftime("%d-%b-%Y") == "21-Oct-2026"

    # 5. Contracts NEVER expire on Saturday or Sunday
    for day_offset in range(14):
        test_dt = ref_wed + timedelta(days=day_offset)
        for und in ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            exp = get_exchange_weekly_expiry(und, test_dt)
            assert exp.weekday() not in (5, 6), f"{und} expiry {exp} must never be on Saturday/Sunday"

    # 6. Standard Contract Symbol Format
    sym = format_contract_symbol("SENSEX", sensex_exp, 81500, "CE")
    assert sym == "SENSEX 16 OCT 81500 CE"

