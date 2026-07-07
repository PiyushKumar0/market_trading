"""Mode + risk-state machines (R5, §3.5.3). Tier-2-owned, sticky in SQLite.

Two orthogonal sticky machines plus a routing flag:
- **Mode** (``OFF ↔ RECOMMEND ↔ AUTO``): owner-controlled; the gate may only DOWNGRADE
  (:meth:`force_downgrade`). Upgrading to AUTO requires an owner two-step confirmation (R10).
- **RiskState** (``NORMAL | FROZEN | CLOSE_ONLY | KILLED``): reached by per-cause edges; risk-forced
  downgrades to CLOSE_ONLY/KILLED latch until explicit owner re-arm (never a timer; R3/R5).

The order-surface predicate (a) lives here as :meth:`opening_orders_allowed` — a position-OPENING call
is permitted iff ``mode == AUTO ∧ risk_state == NORMAL ∧ in_window`` (§3.5.3). Risk-reducing actions
(predicate b) are NEVER gated by this — they run in every mode/state (R3) and are not this class's
concern. The full latch/recovery logic + the runtime square-off-on-shrink wiring land with the gate
(Phase 2) and OMS (Phase 3); Phase 0 ships the sticky persistence, the transition guards, and the
window setter's validate→persist→audit→publish path.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import time

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor, Mode, RiskState, Routing
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import OwnerConfirmation, TradeWindow
from engine.risk.events import (
    TOPIC_MODE_CHANGED,
    TOPIC_RISK_STATE,
    TOPIC_TRADE_WINDOW,
    ModeChanged,
    RiskStateChanged,
    TradeWindowChanged,
)

_log = get_logger("engine.risk.mode")

_MODE_RANK = {Mode.OFF: 0, Mode.RECOMMEND: 1, Mode.AUTO: 2}

SquareOffCallback = Callable[[], Awaitable[None]]


class ModeManager:
    """Sticky mode / routing / risk-state, plus the owner-set trade window (§3.2.7)."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        bus: EventBus | None = None,
        calendar: NSECalendar | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._bus = bus
        self._calendar = calendar

    # ----------------------------------------------------------------- reads (sticky)
    def _row(self) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT mode, routing, risk_state FROM mode_state WHERE id=1"
        ).fetchone()
        if row is None:
            raise RuntimeError("mode_state singleton missing — migrations not applied?")
        return row

    def mode(self) -> Mode:
        return Mode(self._row()["mode"])

    def routing(self) -> Routing | None:
        raw = self._row()["routing"]
        return Routing(raw) if raw else None

    def risk_state(self) -> RiskState:
        return RiskState(self._row()["risk_state"])

    # ----------------------------------------------------------------- order-surface predicate (a)
    def entries_allowed(self, in_window: bool) -> bool:
        """Entries/entry-recommendations require NORMAL risk state + inside the trade window, in AUTO
        or RECOMMEND (§3.5.3). Does NOT consider the kill switch (checked separately) or per-§7.1
        limits (the gate's job)."""
        return self.mode() in (Mode.AUTO, Mode.RECOMMEND) and self.risk_state() == RiskState.NORMAL and in_window

    def opening_orders_allowed(self, in_window: bool) -> bool:
        """Position-OPENING broker order calls require AUTO + NORMAL + in-window (predicate a, §3.5.3).
        Routing (paper/live) then selects the broker. Risk-reducing calls bypass this entirely (R3)."""
        return self.mode() == Mode.AUTO and self.risk_state() == RiskState.NORMAL and in_window

    # ----------------------------------------------------------------- transitions
    async def request_transition(
        self,
        to: Mode,
        who: Actor,
        confirmation: OwnerConfirmation | None = None,
        routing: Routing | None = None,
    ) -> bool:
        """Owner-initiated mode change. →AUTO requires a two-step owner confirmation (R10)."""
        if who != Actor.OWNER:
            raise PermissionError("mode transitions are owner-only (R10)")
        if to == Mode.AUTO:
            if confirmation is None or not confirmation.confirmed or confirmation.actor != Actor.OWNER:
                raise PermissionError("→AUTO requires an authenticated OWNER two-step confirmation (R10)")
            routing = routing or Routing.PAPER  # default safe routing; live gated elsewhere (§8.5)
        else:
            routing = None  # routing is valid ONLY with AUTO (§3.5.3)
        return await self._apply_mode(to, who, reason="owner_request", routing=routing)

    async def force_downgrade(self, to: Mode, reason: str) -> None:
        """Risk-triggered downgrade (actor RISK_GATE). Only ever lowers the mode; re-arm is owner-only."""
        current = self.mode()
        if _MODE_RANK[to] >= _MODE_RANK[current]:
            _log.info("force_downgrade_noop", current=current.value, requested=to.value, reason=reason)
            return
        await self._apply_mode(to, Actor.RISK_GATE, reason=reason, routing=None)

    async def _apply_mode(self, to: Mode, actor: Actor, *, reason: str, routing: Routing | None) -> bool:
        old = self.mode()
        now = self._clock.now()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE mode_state SET mode=?, routing=?, reason=?, changed_by=?, changed_at=? WHERE id=1",
                (to.value, routing.value if routing else None, reason, actor.value, now.isoformat()),
            )
            self._audit("mode_state", {"old": old.value, "new": to.value, "routing": routing.value if routing else None, "reason": reason}, actor, now.isoformat())
        _log.warning("mode_changed", old=old.value, new=to.value, routing=routing.value if routing else None, actor=actor.value, reason=reason)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_MODE_CHANGED,
                ModeChanged(old_mode=old, new_mode=to, routing=routing, actor=actor, reason=reason, at=now),
            )
        return True

    async def set_risk_state(self, to: RiskState, reason: str, who: Actor) -> None:
        """Persist a risk-state transition (sticky). Most-restrictive-wins / latch logic is the gate's
        (Phase 2); this is the durable setter + publisher."""
        old = self.risk_state()
        if old == to:
            return
        now = self._clock.now()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE mode_state SET risk_state=? WHERE id=1", (to.value,)
            )
            self._audit("risk_state", {"old": old.value, "new": to.value, "reason": reason}, who, now.isoformat())
        _log.warning("risk_state_changed", old=old.value, new=to.value, actor=who.value, reason=reason)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_RISK_STATE,
                RiskStateChanged(old_state=old, new_state=to, actor=who, reason=reason, at=now),
            )

    # ----------------------------------------------------------------- trade window (§3.2.7/§7.1)
    def get_trade_window(self) -> TradeWindow | None:
        row = self._conn.execute(
            "SELECT start_ist, end_ist, squareoff_buffer_min FROM trade_window_state WHERE id=1"
        ).fetchone()
        if row is None or not row["start_ist"]:
            return None
        return TradeWindow(
            start=_parse_time(row["start_ist"]),
            end=_parse_time(row["end_ist"]),
            squareoff_buffer_min=int(row["squareoff_buffer_min"] or 0),
        )

    def seed_trade_window_if_absent(self, seed: TradeWindow) -> bool:
        """Seed ``trade_window_state`` from the settings default on first run (lifecycle, §2.6).
        Returns True if a row was inserted."""
        if self.get_trade_window() is not None:
            return False
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO trade_window_state (id, start_ist, end_ist, squareoff_buffer_min, set_by, changed_at)
                VALUES (1, ?, ?, ?, 'settings_seed', ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (seed.start.strftime("%H:%M"), seed.end.strftime("%H:%M"), seed.squareoff_buffer_min, now),
            )
        _log.info("trade_window_seeded", start=str(seed.start), end=str(seed.end))
        return True

    def validate_window(self, window: TradeWindow) -> str | None:
        """Return an error string if the window is invalid, else None (§7.1/§3.2.7).

        Checks: start<end; non-empty MIS sub-window after the buffer; within the day's session (if a
        calendar is available and today is a trading day)."""
        if window.start >= window.end:
            return "start must be before end"
        if window.mis_entry_cutoff() <= window.start:
            return "empty MIS entry sub-window after the square-off buffer"
        if self._calendar is not None:
            today = self._clock.today()
            session = self._calendar.session(today)
            if session is not None:
                if window.start < session.open.time():
                    return "window start is before the session open"
                if window.end > session.close.time():
                    return "window end is after the session close"
        return None

    async def set_trade_window(
        self,
        start: time,
        end: time,
        who: Actor,
        *,
        squareoff_buffer_min: int | None = None,
        on_shrink_squareoff: SquareOffCallback | None = None,
    ) -> bool:
        """Owner-only single-step window setter (§3.2.7). Validate → persist sticky → audit → publish →
        apply. If the new ``end`` is at-or-before now, fire ``on_shrink_squareoff`` (risk-reducing,
        Phase 3). Not learnable, never Tier-1-callable (R4)."""
        if who != Actor.OWNER:
            raise PermissionError("trade-window changes are owner-only (R10)")
        existing = self.get_trade_window()
        buffer = squareoff_buffer_min if squareoff_buffer_min is not None else (
            existing.squareoff_buffer_min if existing else 5
        )
        window = TradeWindow(start=start, end=end, squareoff_buffer_min=buffer)
        err = self.validate_window(window)
        if err is not None:
            _log.warning("trade_window_rejected", reason=err, start=str(start), end=str(end))
            return False  # value unchanged

        now = self._clock.now()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO trade_window_state (id, start_ist, end_ist, squareoff_buffer_min, set_by, changed_at)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    start_ist=excluded.start_ist, end_ist=excluded.end_ist,
                    squareoff_buffer_min=excluded.squareoff_buffer_min,
                    set_by=excluded.set_by, changed_at=excluded.changed_at
                """,
                (start.strftime("%H:%M"), end.strftime("%H:%M"), buffer, who.value, now.isoformat()),
            )
            self._audit(
                "trade_window_state",
                {"start": start.strftime("%H:%M"), "end": end.strftime("%H:%M"), "buffer": buffer},
                who,
                now.isoformat(),
            )
        _log.warning("trade_window_changed", start=str(start), end=str(end), buffer=buffer, actor=who.value)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_TRADE_WINDOW,
                TradeWindowChanged(
                    start_ist=start.strftime("%H:%M"), end_ist=end.strftime("%H:%M"),
                    squareoff_buffer_min=buffer, actor=who, at=now,
                ),
            )
        # If end <= now and an MIS is open, square it off at the new end (risk-reducing, §3.2.7).
        end_dt = self._clock.combine(self._clock.today(), end)
        if on_shrink_squareoff is not None and end_dt <= now:
            await on_shrink_squareoff()
        return True

    # ----------------------------------------------------------------- audit helper
    def _audit(self, name: str, diff: dict, actor: Actor, at: str) -> None:
        self._conn.execute(
            "INSERT INTO config_audit (name, diff, actor, at) VALUES (?, ?, ?, ?)",
            (name, json.dumps(diff), actor.value, at),
        )


def _parse_time(value: str) -> time:
    hh, mm = value.split(":")[:2]
    return time(int(hh), int(mm))
