"""Kill switch (R10, §7.2). Tier-2-owned, sticky across crash/reboot/NSSM restart.

The defining property: ``kill_state=KILLED`` is persisted to SQLite (fsync via WAL) BEFORE any order
action, and :meth:`is_killed` is checked before ANY order on EVERY startup — so no startup path
(manual, scheduled-task, or NSSM crash-restart) can resurrect trading after a kill (R10, §2.6).

The flatten sequence (cancel opens → flatten MIS → verify CNC GTTs) is OMS-dependent and arrives in
Phase 3; here it is invoked via an injected ``flatten_callback`` (None until then). What ships in
Phase 0 is the load-bearing part: sticky persistence, the before-act ordering, single-step trigger /
two-step owner reset, and the startup order-path block (the G0 gate item).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import OwnerConfirmation
from engine.risk.events import TOPIC_KILL_STATE, KillStateChanged

_log = get_logger("engine.risk.kill")

FlattenCallback = Callable[[str], Awaitable[None]]
AlertCallback = Callable[[str], Awaitable[None]]


class KillSwitchEngaged(RuntimeError):
    """Raised by :meth:`KillSwitch.assert_orders_allowed` when the kill switch is engaged (R10)."""


class KillSwitch:
    """Sticky kill switch (R10)."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        bus: EventBus | None = None,
        *,
        flatten_callback: FlattenCallback | None = None,
        alert_callback: AlertCallback | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._bus = bus
        self._flatten = flatten_callback
        self._alert = alert_callback

    # ----------------------------------------------------------------- read (checked pre-order)
    def is_killed(self) -> bool:
        row = self._conn.execute("SELECT killed FROM kill_state WHERE id=1").fetchone()
        return bool(row and row["killed"])

    def reason(self) -> str | None:
        row = self._conn.execute("SELECT reason FROM kill_state WHERE id=1").fetchone()
        return row["reason"] if row else None

    def assert_orders_allowed(self) -> None:
        """Block ANY order while killed. The kill sequence's own flatten/cancel bypasses this by not
        going through it (no self-deadlock, §3.2.7)."""
        if self.is_killed():
            raise KillSwitchEngaged(f"kill switch engaged: {self.reason()!r} (R10)")

    # ----------------------------------------------------------------- trigger (single-step)
    async def trigger(self, reason: str, *, actor: Actor = Actor.OWNER, flatten: bool = True) -> None:
        """Engage the kill switch. Persists KILLED BEFORE acting (R10).

        Idempotent: re-triggering an already-killed switch re-runs the flatten/verify (defensive) but
        does not stack state. Killing is single-step (fast); only *reset* is two-step (§7.2).
        """
        already = self.is_killed()
        now = self._clock.now().isoformat()
        # 1) Persist KILLED FIRST — before any order action — so a crash mid-sequence stays killed.
        with transaction(self._conn):
            self._conn.execute(
                """
                UPDATE kill_state SET killed=1, reason=?, at=?, reset_by=NULL, reset_at=NULL WHERE id=1
                """,
                (reason, now),
            )
        _log.critical("kill_triggered", reason=reason, actor=actor.value, was_already_killed=already)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_KILL_STATE,
                KillStateChanged(killed=True, reason=reason, actor=actor, at=self._clock.now()),
            )
        # 2) Flatten sequence (OMS-owned, Phase 3) — risk-reducing, exempt from the order block.
        if flatten and self._flatten is not None:
            await self._flatten(reason)
        # 3) Alert the owner.
        if self._alert is not None:
            await self._alert(f"KILL SWITCH ENGAGED: {reason}")

    # ----------------------------------------------------------------- reset (owner two-step)
    async def owner_reset(self, confirmation: OwnerConfirmation) -> None:
        """Clear the kill switch — owner-only, two-step authenticated flow (R10, §7.2)."""
        if not confirmation.confirmed or confirmation.actor != Actor.OWNER:
            raise PermissionError("kill reset requires an authenticated OWNER two-step confirmation (R10)")
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE kill_state SET killed=0, reset_by=?, reset_at=? WHERE id=1",
                (confirmation.actor.value, now),
            )
        _log.warning("kill_reset", actor=confirmation.actor.value, note=confirmation.note)
        if self._bus is not None:
            await self._bus.apublish(
                TOPIC_KILL_STATE,
                KillStateChanged(killed=False, reason="owner_reset", actor=confirmation.actor, at=self._clock.now()),
            )
