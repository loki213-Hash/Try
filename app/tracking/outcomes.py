"""Setup Outcome Tracker and Excursion Metrics.

Monitors:
- Maximum Favorable Excursion (MFE)
- Maximum Adverse Excursion (MAE)
- Time duration to reach TP1 vs time to stop-out
- Hit rates for confidence calibration
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class SetupOutcome(BaseModel):
    signal_id: str
    underlying: str
    contract_symbol: str = ""
    side: str = "BUY_CE"
    entry_price: float
    current_price: float = 0.0
    highest_price: float
    lowest_price: float
    mfe_pct: float = 0.0  # (highest - entry) / entry %
    mae_pct: float = 0.0  # (entry - lowest) / entry %
    peak_gain_pct: float = 0.0  # Highest % change from entry
    first_hour_gain_pct: Optional[float] = None  # None ("--") during first hour
    hourly_snapshots: Dict[int, float] = Field(default_factory=dict)
    entry_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    momentum_status: str = "ACTIVE"  # "ACTIVE", "MORE BULLISH", "MORE BEARISH", "EXPIRED"
    reached_tp1: bool = False
    reached_sl: bool = False
    final_outcome: str = "PENDING"  # TP1, TP2, TP3, SL, EXPIRED, PENDING


def is_option_symbol(sym: str) -> bool:
    """Verifies that a symbol represents an options contract (has strike digits and CE/PE/CALL/PUT) and is not a spot index."""
    if not sym:
        return False
    s = sym.strip().upper().replace(" ", "").replace("_", "")
    if s in ("NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "BANKEX"):
        return False
    if "INDEX" in s:
        return False
    has_type = ("CE" in s or "PE" in s or "CALL" in s or "PUT" in s)
    has_digit = any(c.isdigit() for c in s)
    return has_type and has_digit


class OutcomeTracker:
    """Tracks out-of-sample excursion statistics and dynamic hourly momentum for every signal."""

    @staticmethod
    def normalize_symbol(sym: str) -> str:
        """Normalizes symbol by removing whitespace, separators, and standardizing CE/CALL and PE/PUT."""
        if not sym:
            return ""
        s = sym.strip().upper().replace(" ", "").replace("_", "").replace("-", "")
        s = s.replace("CALL", "CE").replace("PUT", "PE")
        if ":" in s:
            s = s.split(":")[-1]
        if "|" in s:
            s = s.split("|")[-1]
        return s

    def __init__(self):
        self._outcomes: Dict[str, SetupOutcome] = {}
        self._symbol_to_signals: Dict[str, List[str]] = {}

    def _map_symbol_key(self, key: str, signal_id: str) -> None:
        if not key:
            return
        clean = key.strip()
        if clean not in self._symbol_to_signals:
            self._symbol_to_signals[clean] = []
        if signal_id not in self._symbol_to_signals[clean]:
            self._symbol_to_signals[clean].append(signal_id)

    def register_signal(
        self,
        signal_id: str,
        underlying: str,
        entry_price: float,
        contract_symbol: str = "",
        side: str = "BUY_CE",
        entry_time: Optional[datetime] = None,
    ) -> SetupOutcome:
        now = entry_time or datetime.now(timezone.utc)
        clean_sym = contract_symbol.strip()
        outcome = SetupOutcome(
            signal_id=signal_id,
            underlying=underlying,
            contract_symbol=clean_sym,
            side=side,
            entry_price=entry_price,
            current_price=entry_price,
            highest_price=entry_price,
            lowest_price=entry_price,
            mfe_pct=0.0,
            mae_pct=0.0,
            peak_gain_pct=0.0,
            first_hour_gain_pct=None,
            hourly_snapshots={},
            entry_time=now,
            momentum_status="ACTIVE",
            reached_tp1=False,
            reached_sl=False,
            final_outcome="PENDING",
        )
        self._outcomes[signal_id] = outcome
        if clean_sym:
            self._map_symbol_key(clean_sym, signal_id)
            self._map_symbol_key(self.normalize_symbol(clean_sym), signal_id)
            if " CE" in clean_sym:
                self._map_symbol_key(clean_sym.replace(" CE", " CALL"), signal_id)
            elif " CALL" in clean_sym:
                self._map_symbol_key(clean_sym.replace(" CALL", " CE"), signal_id)
            elif " PE" in clean_sym:
                self._map_symbol_key(clean_sym.replace(" PE", " PUT"), signal_id)
            elif " PUT" in clean_sym:
                self._map_symbol_key(clean_sym.replace(" PUT", " PE"), signal_id)
        return outcome

    def update_tick(
        self, signal_id: str, current_price: float, now: Optional[datetime] = None
    ) -> None:
        outcome = self._outcomes.get(signal_id)
        if not outcome or current_price <= 0:
            return
        self._apply_tick_to_outcome(outcome, current_price, now)

    def update_quote(
        self, contract_symbol: str, current_price: float, now: Optional[datetime] = None, instrument_id: str = ""
    ) -> None:
        """Updates all monitored signals matching this contract symbol or instrument ID."""
        if current_price <= 0 or (not contract_symbol and not instrument_id):
            return

        # Spot index prices (e.g. SENSEX 74,000 or NIFTY 23,000) must NEVER update options contracts
        is_opt = is_option_symbol(contract_symbol) or is_option_symbol(instrument_id)
        if not is_opt and current_price > 5000:
            return

        matched = False
        candidates = [contract_symbol, self.normalize_symbol(contract_symbol)]
        if instrument_id:
            candidates.extend([instrument_id, self.normalize_symbol(instrument_id)])

        seen_sig_ids = set()
        for cand in candidates:
            if not cand:
                continue
            for sig_id in self._symbol_to_signals.get(cand, []):
                if sig_id not in seen_sig_ids:
                    seen_sig_ids.add(sig_id)
                    outcome = self._outcomes.get(sig_id)
                    if outcome:
                        self._apply_tick_to_outcome(outcome, current_price, now)
                        matched = True

        if not matched and is_opt:
            norm_target = self.normalize_symbol(contract_symbol)
            for outcome in self._outcomes.values():
                if outcome.underlying and outcome.underlying.upper() not in norm_target:
                    continue
                is_ce = "CE" in outcome.side.upper()
                target_is_ce = "CE" in norm_target
                if is_ce != target_is_ce:
                    continue
                out_norm = self.normalize_symbol(outcome.contract_symbol)
                if norm_target and (norm_target in out_norm or out_norm in norm_target):
                    self._apply_tick_to_outcome(outcome, current_price, now)

    def _apply_tick_to_outcome(
        self, outcome: SetupOutcome, current_price: float, now: Optional[datetime] = None
    ) -> None:
        # Sanity check: options contracts for Indian indices never trade > 5000 (spot index is 23k-75k)
        if outcome.entry_price < 2000 and current_price > 5000:
            return

        # Self-heal previously contaminated outcome if spot index leaked
        if outcome.entry_price < 2000 and outcome.highest_price > 5000:
            outcome.highest_price = outcome.entry_price
            outcome.current_price = outcome.entry_price
            outcome.peak_gain_pct = 0.0

        curr_time = now or datetime.now(timezone.utc)
        outcome.current_price = round(current_price, 2)
        outcome.highest_price = round(max(outcome.highest_price, current_price), 2)
        outcome.lowest_price = round(min(outcome.lowest_price, current_price), 2)

        if outcome.entry_price > 0:
            gain_from_entry = ((current_price - outcome.entry_price) / outcome.entry_price) * 100.0
            peak_gain = ((outcome.highest_price - outcome.entry_price) / outcome.entry_price) * 100.0
            mae = ((outcome.entry_price - outcome.lowest_price) / outcome.entry_price) * 100.0

            outcome.peak_gain_pct = round(max(outcome.peak_gain_pct, peak_gain), 2)
            outcome.mfe_pct = outcome.peak_gain_pct
            outcome.mae_pct = round(mae, 2)

            entry_t = outcome.entry_time
            if entry_t.tzinfo is None:
                entry_t = entry_t.replace(tzinfo=timezone.utc)
            if curr_time.tzinfo is None:
                curr_time = curr_time.replace(tzinfo=timezone.utc)

            elapsed_sec = max(0.0, (curr_time - entry_t).total_seconds())

            # First hour: displays "--" until 3600 seconds have elapsed
            if elapsed_sec >= 3600 and outcome.first_hour_gain_pct is None:
                outcome.first_hour_gain_pct = round(gain_from_entry, 2)
                outcome.hourly_snapshots[1] = outcome.first_hour_gain_pct

            # Subsequent hours: snapshot dynamically at hour boundaries
            hours_passed = int(elapsed_sec // 3600)
            if hours_passed >= 2:
                for h in range(2, hours_passed + 1):
                    if h not in outcome.hourly_snapshots:
                        outcome.hourly_snapshots[h] = round(gain_from_entry, 2)

            # Dynamic Momentum Status:
            # Active -> More Bullish (for CE) / More Bearish (for PE) -> Expired
            is_ce = "CE" in outcome.side.upper() or "CALL" in outcome.side.upper()
            if elapsed_sec > 21600 or gain_from_entry <= -40.0:  # 6 hours or down > 40%
                outcome.momentum_status = "EXPIRED"
            elif is_ce and (gain_from_entry >= 15.0 or outcome.peak_gain_pct >= 25.0):
                outcome.momentum_status = "MORE BULLISH"
            elif (not is_ce) and (gain_from_entry >= 15.0 or outcome.peak_gain_pct >= 25.0):
                outcome.momentum_status = "MORE BEARISH"
            else:
                outcome.momentum_status = "ACTIVE"

    def record_final(self, signal_id: str, final_outcome: str) -> None:
        """Records milestone or closure without interrupting continuous clean price excursion monitoring."""
        outcome = self._outcomes.get(signal_id)
        if outcome:
            outcome.final_outcome = final_outcome
            if "TP" in final_outcome:
                outcome.reached_tp1 = True
            elif "SL" in final_outcome:
                outcome.reached_sl = True

    def get_outcome(self, signal_id: str) -> Optional[SetupOutcome]:
        return self._outcomes.get(signal_id)

    def get_outcome_by_symbol(self, contract_symbol: str) -> Optional[SetupOutcome]:
        clean_sym = contract_symbol.strip()
        sig_ids = self._symbol_to_signals.get(clean_sym, [])
        if sig_ids:
            return self._outcomes.get(sig_ids[-1])
        for outcome in reversed(list(self._outcomes.values())):
            if outcome.contract_symbol == clean_sym or (clean_sym and clean_sym in outcome.contract_symbol):
                return outcome
        return None

    def get_all_outcomes(self) -> List[SetupOutcome]:
        return list(self._outcomes.values())


outcome_tracker = OutcomeTracker()
