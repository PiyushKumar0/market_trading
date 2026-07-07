"""Foundational, dependency-graph-root package (§3.2.1).

Event bus, IST clock + NTP skew, NSE trading calendar, config loading, protected store (R4), DPAPI
secrets (R10), structured logging, SQLite helpers, and the foundational enums/value types shared
across tiers. Depends on NO other engine package — this is the root of the import graph (the R1 tier
separation is built on top of it).
"""

from __future__ import annotations

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock, ClockSkewUnavailable
from engine.core.config import Settings, get_settings, load_settings, load_yaml
from engine.core.enums import Actor, DegradeTier, Mode, RiskState, Routing
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.protected_store import IntegrityError, ProtectedStore, UnauthorizedUpdate
from engine.core.secrets import MissingSecretError, Secrets
from engine.core.types import CorpAction, OwnerConfirmation, Session, TradeWindow

__all__ = [
    "IST",
    "Actor",
    "Clock",
    "ClockSkewUnavailable",
    "CorpAction",
    "DegradeTier",
    "EventBus",
    "IntegrityError",
    "Mode",
    "MissingSecretError",
    "NSECalendar",
    "OwnerConfirmation",
    "ProtectedStore",
    "RiskState",
    "Routing",
    "Secrets",
    "Session",
    "Settings",
    "TradeWindow",
    "UnauthorizedUpdate",
    "configure_logging",
    "get_logger",
    "get_settings",
    "load_settings",
    "load_yaml",
]
