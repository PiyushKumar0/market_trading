"""Alert-cadence throttle: one owner alert per EPISODE, not one per occurrence.

Repeats of one persistent condition would flood the bounded Telegram outbox and starve fresh owner
acks. This is a per-key first-fire-then-throttle clock, used by
:class:`~engine.intelligence.harness.AgentHarness` and the recommendation pipeline for their
per-``(agent, reason)`` failure alerts. The health monitor's own dedup is *set*-shaped (alert when the
PROBLEM SET changes, announce recovery once) and lives with the monitor.

* **The first occurrence always alerts.** Throttling is about the 2nd..Nth, never the 1st.
* **A repeat is still LOGGED.** Only the owner-facing cadence changes; the log stays complete.
* **Recovery closes the episode.** :meth:`AlertEpisodes.reset` is called on success, so the next
  failure after a good call is a NEW incident and alerts immediately.

State is per-process and in-memory on purpose: a restart is itself a change of situation the owner
should hear about, and nothing here is a safety input (alerting is best-effort by construction).
"""

from __future__ import annotations

from datetime import datetime, timedelta

#: Default quiet period for a still-unchanged condition: a real outage keeps nagging, a whole session
#: of one broken thing is ~13 messages.
REPEAT_AFTER = timedelta(minutes=30)


class AlertEpisodes:
    """Per-key episode clock: alert on the first occurrence, then at most once per ``repeat_after``.

    A *key* is whatever identifies "the same thing happening again" for the caller — a
    ``(agent_id, reason)`` tuple, typically. Keys are compared by equality, so anything hashable
    works; tuples are used so :meth:`reset` can retire every key belonging to one subject.

    All times come from the caller's :class:`~engine.core.clock.Clock` — this class
    never reads a wall clock of its own, which is what makes the windows testable.
    """

    def __init__(self, repeat_after: timedelta = REPEAT_AFTER) -> None:
        self._repeat_after = repeat_after
        self._last_alert: dict[tuple[str, ...], datetime] = {}

    def should_alert(self, key: tuple[str, ...], now: datetime) -> bool:
        """True iff this occurrence should reach the owner; records the decision when it says yes.

        Yes on the first occurrence of ``key`` (no open episode) and on the first occurrence after
        ``repeat_after`` has elapsed; no in between. Calling it is what advances the window, so a
        caller must call it exactly once per occurrence and honour the answer.
        """
        last = self._last_alert.get(key)
        if last is not None and (now - last) < self._repeat_after:
            return False
        self._last_alert[key] = now
        return True

    def reset(self, *prefix: str) -> None:
        """Close the episode(s) whose key starts with ``prefix`` — call this on SUCCESS.

        With no arguments every episode is closed. With ``reset("intraday_analyst")`` every
        ``("intraday_analyst", <reason>)`` episode is closed, because one good call means the agent is
        working and the NEXT failure — whatever its reason — is fresh news, not a repeat.
        """
        if not prefix:
            self._last_alert.clear()
            return
        for key in [k for k in self._last_alert if k[: len(prefix)] == prefix]:
            del self._last_alert[key]

    def open_episodes(self) -> int:
        """How many keys currently hold an open window (diagnostics / tests only)."""
        return len(self._last_alert)


__all__ = ["REPEAT_AFTER", "AlertEpisodes"]
