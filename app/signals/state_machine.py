r"""Signal State Machine and Lifecycle Transition Engine.

Validates and manages atomic lifecycle transitions:
WATCH -> ENTRY -> TP1_HIT -> TP2_HIT -> TP3_HIT -> CLOSED
              \-> SL_HIT
              \-> ENTRY_MISSED
              \-> INVALIDATED
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from pydantic import BaseModel
from app.monitoring.logging import logger


class SignalStatus:
    EARLY_WATCH = "EARLY_WATCH"
    SETUP_DEVELOPING = "SETUP_DEVELOPING"
    ENTRY = "ENTRY"
    ENTRY_QUALIFIED = "ENTRY"  # Synonym
    WATCH = "WATCH"  # General alias
    ENTRY_MISSED = "ENTRY_MISSED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    TP1_HIT = "TP1_HIT"
    TP2_HIT = "TP2_HIT"
    TP3_HIT = "TP3_HIT"
    SL_HIT = "SL_HIT"
    CLOSED = "CLOSED"


class SignalStateMachine:
    """Enforces deterministic lifecycle transitions on signals."""

    # Allowed directional transitions across 3-stage lifecycle and post-entry execution
    VALID_TRANSITIONS: Dict[str, Set[str]] = {
        SignalStatus.EARLY_WATCH: {
            SignalStatus.SETUP_DEVELOPING,
            SignalStatus.ENTRY,
            SignalStatus.INVALIDATED,
            SignalStatus.EXPIRED,
        },
        SignalStatus.WATCH: {
            SignalStatus.SETUP_DEVELOPING,
            SignalStatus.ENTRY,
            SignalStatus.INVALIDATED,
            SignalStatus.EXPIRED,
        },
        SignalStatus.SETUP_DEVELOPING: {
            SignalStatus.ENTRY,
            SignalStatus.EARLY_WATCH,
            SignalStatus.INVALIDATED,
            SignalStatus.EXPIRED,
        },
        SignalStatus.ENTRY: {
            SignalStatus.TP1_HIT,
            SignalStatus.SL_HIT,
            SignalStatus.ENTRY_MISSED,
            SignalStatus.INVALIDATED,
            SignalStatus.EXPIRED,
        },
        SignalStatus.TP1_HIT: {
            SignalStatus.TP2_HIT,
            SignalStatus.SL_HIT,
            SignalStatus.CLOSED,
        },
        SignalStatus.TP2_HIT: {
            SignalStatus.TP3_HIT,
            SignalStatus.SL_HIT,
            SignalStatus.CLOSED,
        },
        SignalStatus.TP3_HIT: {
            SignalStatus.CLOSED,
        },
        SignalStatus.SL_HIT: {
            SignalStatus.CLOSED,
        },
        SignalStatus.ENTRY_MISSED: set(),
        SignalStatus.INVALIDATED: set(),
        SignalStatus.EXPIRED: set(),
        SignalStatus.CLOSED: set(),
    }

    @classmethod
    def can_transition(cls, current_status: str, next_status: str) -> bool:
        allowed = cls.VALID_TRANSITIONS.get(current_status, set())
        return next_status in allowed

    @classmethod
    def evaluate_live_quote(
        cls,
        current_status: str,
        current_option_price: float,
        underlying_spot: float,
        entry_high: float,
        max_entry: float,
        sl_price: float,
        tp1_price: float,
        tp2_price: float,
        tp3_price: float,
        underlying_invalidation: float,
        direction: str,
    ) -> Optional[str]:
        """Determines if a live price update triggers a lifecycle event."""
        # Check underlying invalidation
        if direction == "BUY_CE" and underlying_spot <= underlying_invalidation:
            return SignalStatus.INVALIDATED
        if direction == "BUY_PE" and underlying_spot >= underlying_invalidation:
            return SignalStatus.INVALIDATED

        # If in ENTRY phase, check fill or missed
        if current_status == SignalStatus.ENTRY:
            if current_option_price > max_entry:
                return SignalStatus.ENTRY_MISSED
            if current_option_price <= sl_price:
                return SignalStatus.SL_HIT
            if current_option_price >= tp1_price:
                return SignalStatus.TP1_HIT

        # If TP1 was already hit
        elif current_status == SignalStatus.TP1_HIT:
            if current_option_price <= sl_price:
                return SignalStatus.SL_HIT
            if current_option_price >= tp2_price:
                return SignalStatus.TP2_HIT

        # If TP2 was already hit
        elif current_status == SignalStatus.TP2_HIT:
            if current_option_price <= sl_price:
                return SignalStatus.SL_HIT
            if current_option_price >= tp3_price:
                return SignalStatus.TP3_HIT

        return None

    @classmethod
    def evaluate_expiration(
        cls,
        status: str,
        created_at: datetime,
        max_watch_minutes: float = 10.0,
        max_developing_minutes: float = 15.0,
    ) -> bool:
        """Checks if a radar signal has exceeded its maximum observation window."""
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - created_at).total_seconds() / 60.0
        if status in (SignalStatus.EARLY_WATCH, SignalStatus.WATCH) and age_min >= max_watch_minutes:
            return True
        if status == SignalStatus.SETUP_DEVELOPING and age_min >= max_developing_minutes:
            return True
        return False
