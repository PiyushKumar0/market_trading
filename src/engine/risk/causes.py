"""Per-cause risk-state latch ledger (§3.5.3). Tier-2-owned, sticky in SQLite.

``RiskState`` is NOT a linear chain: ``FROZEN``/``CLOSE_ONLY``/``KILLED`` are reached by **direct
per-cause entry edges**, composed **most-restrictive-wins**, and re-arm means *every* latching cause
cleared (§3.5.3). :class:`RiskStateLatch` is the ledger that makes those three sentences executable:
one row per cause in ``risk_state_causes`` (active ⇔ ``cleared_at IS NULL``), and after every
set/clear the resolved state — the most restrictive ACTIVE cause, or ``NORMAL`` when none remain — is
pushed to :class:`~engine.risk.mode.ModeManager.set_risk_state`, which owns persistence, audit and the
``risk.state`` publish.

Why a ledger rather than a counter: two independent causes (say a stale feed and a rejection storm)
must each hold the freeze on their own, and clearing either one must NOT re-arm entries while the
other is live. A bare risk-state column cannot express that; a set of causes can.

**Load-bearing constraint:** once wired, this ledger is the single writer of ``risk_state``. A cause
that escalates the state by calling ``set_risk_state`` directly is invisible here, and the next
:meth:`clear_cause` will relax the state right out from under it. Register the cause instead.

Recovery policy (§3.5.3) lives with the *callers*, not here: data/auth-quality causes (stale feed,
clock skew, warm-up/regime, token invalid) call :meth:`clear_cause` the moment the condition clears;
behavioural causes (consecutive_losses, daily soft-loss) clear at the next session; a rejection-storm
freeze and an owner pause clear only on the owner's ``/resume_entries`` (§3.2.11).
"""

from __future__ import annotations

import sqlite3

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import Actor, RiskState
from engine.core.log import get_logger
from engine.risk.mode import ModeManager

_log = get_logger("engine.risk.causes")

#: Most-restrictive-wins ordering (§3.5.3). Explicit ranks, not enum order: mirrors ``_RISK_RANK`` in
#: ``engine.risk.exposure`` so the two composition points can never disagree.
_RISK_RANK: dict[RiskState, int] = {
    RiskState.NORMAL: 0,
    RiskState.FROZEN: 1,
    RiskState.CLOSE_ONLY: 2,
    RiskState.KILLED: 3,
}

#: The states a cause may latch. ``NORMAL`` is the ABSENCE of causes, never a cause itself — the
#: ``risk_state_causes`` CHECK constraint (0003_phase2.sql) rejects it at the storage layer too.
LATCHING_STATES: tuple[RiskState, ...] = (RiskState.FROZEN, RiskState.CLOSE_ONLY, RiskState.KILLED)

#: Owner pause via ``/pause_entries`` (§3.2.11). Clears only on the owner's ``/resume_entries``.
CAUSE_OWNER_PAUSE = "owner_pause"

#: ≥3 broker rejects in 60 s (§3.5.1). The ONE FROZEN cause §3.5.3 says never auto-recovers: it clears
#: only on the owner's ``/resume_entries``, which is why that command clears this cause too.
CAUSE_REJECTION_STORM = "rejection_storm"


class RiskStateLatch:
    """The ``risk_state_causes`` ledger + the most-restrictive-wins projection onto ``risk_state``."""

    def __init__(self, conn: sqlite3.Connection, clock: Clock, mode_manager: ModeManager) -> None:
        self._conn = conn
        self._clock = clock
        self._mode = mode_manager

    # ----------------------------------------------------------------- reads
    def active_causes(self) -> list[tuple[str, RiskState, str]]:
        """Every currently-latching ``(cause, state, detail)``, most restrictive first.

        Rebuilt from the table on every call (never an in-process set), so a restart mid-freeze still
        sees the freeze and cannot silently re-arm entries (§2.6).
        """
        rows = self._conn.execute(
            "SELECT cause, state, detail FROM risk_state_causes WHERE cleared_at IS NULL"
        ).fetchall()
        causes = [(row["cause"], RiskState(row["state"]), row["detail"] or "") for row in rows]
        causes.sort(key=lambda c: (-_RISK_RANK[c[1]], c[0]))
        return causes

    def resolved_state(self) -> RiskState:
        """The state the ledger currently implies: most-restrictive active cause, else ``NORMAL``."""
        causes = self.active_causes()
        return causes[0][1] if causes else RiskState.NORMAL

    # ----------------------------------------------------------------- writes
    async def set_cause(self, cause: str, state: RiskState, detail: str, who: Actor) -> RiskState:
        """Latch ``cause`` at ``state`` and apply the most-restrictive ACTIVE state (§3.5.3).

        Idempotent: re-setting a cause that is already active keeps its original ``set_at`` (one
        episode, not a churn of timestamps) and re-applying the same resolved state is a no-op inside
        ``set_risk_state``. Re-raising a cause that was cleared starts a fresh episode.

        Returns the resolved state actually in force after the call.
        """
        if state not in LATCHING_STATES:
            raise ValueError(
                f"{state.value} is not a latching state — clear the cause instead (§3.5.3)"
            )
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO risk_state_causes (cause, state, detail, set_at, cleared_at)
                VALUES (?, ?, ?, ?, NULL)
                ON CONFLICT(cause) DO UPDATE SET
                    state=excluded.state,
                    detail=excluded.detail,
                    set_at=CASE WHEN risk_state_causes.cleared_at IS NULL
                                THEN risk_state_causes.set_at ELSE excluded.set_at END,
                    cleared_at=NULL
                """,
                (cause, state.value, detail, now),
            )
        resolved = self.resolved_state()
        _log.warning(
            "risk_cause_set",
            cause=cause,
            state=state.value,
            detail=detail,
            resolved=resolved.value,
            actor=who.value,
        )
        await self._mode.set_risk_state(resolved, f"{cause}: {detail}" if detail else cause, who)
        return resolved

    async def clear_cause(self, cause: str, who: Actor) -> RiskState:
        """Clear ``cause`` and recompute: no active causes ⇒ ``NORMAL``, else the most restrictive left.

        Idempotent and safe on an unknown cause (nothing to stamp; the recompute still runs). Clearing
        one cause NEVER re-arms entries while another is active — that is the whole point of the
        ledger (§3.5.3 "re-arm = every latching cause cleared").

        Returns the resolved state actually in force after the call.
        """
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            cur = self._conn.execute(
                "UPDATE risk_state_causes SET cleared_at=? WHERE cause=? AND cleared_at IS NULL",
                (now, cause),
            )
            was_active = cur.rowcount > 0
        remaining = [name for name, _state, _detail in self.active_causes()]
        resolved = self.resolved_state()
        reason = f"{cause} cleared" if was_active else f"{cause} not active"
        reason += (
            f"; still latched by {', '.join(remaining)}" if remaining else "; re-armed (no active causes)"
        )
        _log.warning(
            "risk_cause_cleared",
            cause=cause,
            was_active=was_active,
            remaining=remaining,
            resolved=resolved.value,
            actor=who.value,
        )
        # Defense-in-depth (2026-07-28 review): clearing a cause that was NOT active must never RELAX
        # a state some out-of-ledger writer set (e.g. a freeze from a latch-less code path) — the
        # historical failure was /resume_entries re-arming NORMAL over a token-rejected freeze. When
        # the cause WAS active, the ledger owned the state and the resolve-write is authoritative.
        current = self._mode.risk_state()
        if not was_active and _RISK_RANK[current] > _RISK_RANK[resolved]:
            _log.warning("risk_cause_clear_preserved_state", cause=cause, state=current.value,
                         resolved=resolved.value)
            return current
        await self._mode.set_risk_state(resolved, reason, who)
        return resolved

    async def clear_stale_daily(self, today: str, who: Actor) -> list[str]:
        """§3.5.3 behavioural auto-clear: day-scoped causes (``daily_loss_*``, ``consecutive_losses``)
        latch only for THEIR session — clear any still active from a prior day (called at startup /
        first tick of a new session). Returns the causes cleared."""
        rows = self._conn.execute(
            "SELECT cause FROM risk_state_causes WHERE cleared_at IS NULL "
            "AND (cause LIKE 'daily_loss_%' OR cause = 'consecutive_losses') "
            "AND substr(set_at, 1, 10) < ?",
            (today,),
        ).fetchall()
        cleared = [str(r["cause"]) for r in rows]
        for cause in cleared:
            await self.clear_cause(cause, who)
        return cleared
