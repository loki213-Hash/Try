"""Option chain structure analysis.

Calculates:
- Call Resistance Wall (Highest Call OI)
- Put Support Wall (Highest Put OI)
- Max Pain strike level
- In-the-money / At-the-money liquidity verification
"""

from __future__ import annotations
from typing import Dict, Optional, Tuple
from pydantic import BaseModel
from app.data.base_provider import OptionChainSnapshot


class OptionChainAnalysis(BaseModel):
    underlying: str
    spot_price: float
    call_oi_wall: float  # Key overhead resistance
    put_oi_wall: float   # Key support floor
    max_pain: float
    total_call_oi: int
    total_put_oi: int
    pcr: float
    is_liquid_atm: bool


class OptionChainAnalyzer:
    """Analyzes option chain distributions and institutional open interest walls."""

    @staticmethod
    def analyze(snapshot: OptionChainSnapshot) -> OptionChainAnalysis:
        max_call_oi = -1
        call_wall = snapshot.underlying_ltp
        max_put_oi = -1
        put_wall = snapshot.underlying_ltp
        total_call_oi = 0
        total_put_oi = 0
        strikes = sorted(list(snapshot.contracts.keys()))

        for strike, opts in snapshot.contracts.items():
            if "CE" in opts:
                c = opts["CE"]
                oi = c.oi or 0
                total_call_oi += oi
                if oi > max_call_oi:
                    max_call_oi = oi
                    call_wall = strike
            if "PE" in opts:
                p = opts["PE"]
                oi = p.oi or 0
                total_put_oi += oi
                if oi > max_put_oi:
                    max_put_oi = oi
                    put_wall = strike

        # Max Pain Calculation
        max_pain_strike = snapshot.underlying_ltp
        min_total_loss = float("inf")
        if strikes:
            for eval_strike in strikes:
                total_loss = 0.0
                for s, opts in snapshot.contracts.items():
                    if "CE" in opts and eval_strike > s:
                        total_loss += (eval_strike - s) * (opts["CE"].oi or 0)
                    if "PE" in opts and eval_strike < s:
                        total_loss += (s - eval_strike) * (opts["PE"].oi or 0)
                if total_loss < min_total_loss:
                    min_total_loss = total_loss
                    max_pain_strike = eval_strike

        pcr = round(total_put_oi / max(total_call_oi, 1), 3)

        return OptionChainAnalysis(
            underlying=snapshot.underlying,
            spot_price=snapshot.underlying_ltp,
            call_oi_wall=call_wall,
            put_oi_wall=put_wall,
            max_pain=max_pain_strike,
            total_call_oi=total_call_oi,
            total_put_oi=total_put_oi,
            pcr=pcr,
            is_liquid_atm=(total_call_oi > 10000 and total_put_oi > 10000),
        )


option_chain_analyzer = OptionChainAnalyzer()
