"""Risk-domain event payloads published on the bus (§3.2.1 canonical topics).

Consumed by ``notify``/``api`` for owner alerts + the dashboard (R8). Risk owns the *origination* of
these; the I/O surfaces never originate orders/state, they only react (§3.2.11).
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel

from engine.core.enums import Actor, Mode, RiskState, Routing

TOPIC_MODE_CHANGED = "mode.changed"
TOPIC_RISK_STATE = "risk.state"
TOPIC_KILL_STATE = "kill.state"
TOPIC_TRADE_WINDOW = "trade_window.changed"


class ModeChanged(BaseModel):
    old_mode: Mode
    new_mode: Mode
    routing: Routing | None
    actor: Actor
    reason: str
    at: AwareDatetime


class RiskStateChanged(BaseModel):
    old_state: RiskState
    new_state: RiskState
    actor: Actor
    reason: str
    at: AwareDatetime


class KillStateChanged(BaseModel):
    killed: bool
    reason: str
    actor: Actor
    at: AwareDatetime


class TradeWindowChanged(BaseModel):
    start_ist: str
    end_ist: str
    squareoff_buffer_min: int
    actor: Actor
    at: AwareDatetime
