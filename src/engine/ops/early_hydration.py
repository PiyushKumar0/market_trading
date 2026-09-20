"""Early hydration — an early Kite login runs the pre-open chain ahead of its clock (§2.6 addendum).

Owner-directed 2026-09-09: *"on most days I am travelling at 08:15 and only able to start the system
around 09:30–10:00 … early hydration whenever I login early, like 06:30"*. The pre-open chain is
scheduled at surveillance 08:20, news chain 08:25, universe 08:30, catalyst digest 08:35, pre-open
planner 08:50; on a 09:30+ boot every one of them fires AFTER the trade window opens, which is why
the G2 digest-before-open criterion reads 61%. Nothing in the catch-up machinery could help: its
due-gates treat today's job as not-missed until its fire-time has passed, so a 06:30 pass replays
nothing (see :meth:`engine.ops.jobs.CatchUpRunner.hydrate_ahead`, which is the deliberate exception
this module drives).

:class:`EarlyHydration` is a :meth:`~engine.broker.session.SessionManager.add_login_hook` callback,
registered beside :class:`~engine.ops.post_login.PostLoginRecovery`. Registration order buys NOTHING:
:meth:`SessionManager._fire_login_hooks` creates one task per hook, so the two hooks run CONCURRENTLY
(2026-09-09 review). The recovery's ``completed`` event is what actually sequences them. Its decision
table:

- not a trading day (R6) ⇒ nothing to hydrate;
- login at/after session open ⇒ nothing to do HERE: from the open on, every one of those jobs is
  simply DUE, and the boot pass / 30-min sweep own due jobs. Two paths for one job would only race.
  The open is resolved PER LOGIN from the calendar (a zero-arg callable), never frozen at boot — the
  hook outlives the day it was built on, and a special/muhurat session moves the boundary;
- otherwise wait for ``scheduler.start()`` (the ``armed`` event) before running anything. This is the
  WO-15 firing-point rule verbatim: the news chain and digest must never run ahead of arming — the
  2026-08-10 wedge starved every scheduled job for 8 h, *including the sweep that self-heals*. The
  wait is bounded (:data:`EARLY_HYDRATION_ARM_TIMEOUT_S`) so a boot that never arms leaves no
  parked task;
- then wait for the sibling :class:`PostLoginRecovery` task (``recovery_done``): the chain wants the
  token map, backfill and ticker it restores. That is a PREFERENCE, not a requirement — the chain
  reads the store and the news feeds, never a Kite session — so this wait times out into a log line
  and the hydration PROCEEDS. Aborting would cost the day the very digest this exists to buy;
- then ``hydrate_ahead`` (single-flighted against the boot pass by the runner's own lock) and one
  owner line.

A second login the same day needs no latch: every watermark is already set, so every job reports
``already_run`` and the pass is free. Nothing here ever raises — a login hook that throws would break
the login path itself.

Boot trigger (2026-09-21): a login is not the only gap. On 2026-09-16 the engine booted 06:58 with no
Kite login until 10:04, the PC slept 08:00-10:00 through the scheduled 08:20-08:50 fires, and the
catalyst digest landed 10:18, after the trade window opened. :meth:`EarlyHydration.on_boot` now runs
the identical chain once at process boot, under the same gates as an early login — the watermarks it
leaves behind make a later login free.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import time

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.jobs import CatchUpRunner

_log = get_logger("engine.ops.early_hydration")

Notify = Callable[[CatalogMessage], Awaitable[None]]

#: How long the hook waits for ``scheduler.start()`` before giving up (§2.6/WO-15: the chain never
#: runs ahead of arming, so a boot that never arms must end as a logged skip, not a parked task).
#: 15 min is far longer than any observed boot (the worst recorded is minutes, WO-25c) and still
#: leaves the whole 06:30 → 08:20 head-start intact on the mornings this exists for.
EARLY_HYDRATION_ARM_TIMEOUT_S = 900


class EarlyHydration:
    """The §2.6 early-login pre-open hydration hook (see the module docstring).

    Parameters
    ----------
    armed:
        Set by the composition root inside ``start_scheduler_and_fire_post_arm`` — after
        ``scheduler.start()`` (the WO-15 firing point) AND after the post-arm one-shot is dispatched,
        so a login landing mid-boot cannot win the single-flight pass lock and turn that one-shot
        into a ``skipped_in_flight`` no-op.
    job_ids:
        The chain to hydrate, in registry order (``engine.ops.main.EARLY_HYDRATION_JOB_IDS``).
        Deliberately a caller decision: this module owns WHEN, never WHAT.
    session_open:
        Zero-arg callable returning the login day's continuous-session open (IST) — the boundary past
        which the boot/sweep catch-up owns the jobs. A CALLABLE, not a value (2026-09-09 review): the
        hook is built once at boot and lives for the whole process, so a frozen open would be some
        earlier day's. The composition root resolves it from the calendar on each call.
    recovery_done:
        :attr:`PostLoginRecovery.completed`. Login hooks are fire-and-forget tasks, so this event —
        not registration order — is what makes the recovery run first. ``None`` when no recovery is
        wired (then there is nothing to wait for).
    """

    def __init__(
        self,
        catch_up: CatchUpRunner,
        clock: Clock,
        calendar: NSECalendar,
        armed: asyncio.Event,
        job_ids: Sequence[str],
        session_open: Callable[[], time],
        *,
        recovery_done: asyncio.Event | None = None,
        notify: Notify | None = None,
    ) -> None:
        self._catch_up = catch_up
        self._clock = clock
        self._calendar = calendar
        self._armed = armed
        self._job_ids = tuple(job_ids)
        self._session_open = session_open
        self._recovery_done = recovery_done
        self._notify = notify

    async def on_login(self) -> None:
        """The login hook. Guarded exactly like :class:`PostLoginRecovery`'s steps: a failure here is
        logged and dropped — it must never propagate into ``complete_login`` or the event loop."""
        try:
            await self._hydrate("early_login")
        except Exception:  # noqa: BLE001 - a login hook degrades + logs, never breaks the login path
            _log.exception("early_hydration_failed", reason="early_login")

    async def on_boot(self) -> None:
        """The boot hook (2026-09-21): a boot on a trading day before the open runs the same chain a
        login would, under the same gates — a login that never comes must not cost the digest."""
        try:
            await self._hydrate("early_boot")
        except Exception:  # noqa: BLE001 - a boot task degrades + logs, never breaks the boot path
            _log.exception("early_hydration_failed", reason="early_boot")

    async def _hydrate(self, reason: str) -> None:
        now = self._clock.now()
        today = now.date()
        if not self._calendar.is_trading_day(today):
            _log.info("early_hydration_skipped_non_trading_day", d=today.isoformat(), reason=reason)
            return
        session_open = self._session_open()          # resolved per login, never frozen at boot
        if now.time() >= session_open:
            # From the open on, the chain is simply DUE — the boot pass and the 30-min sweep own due
            # jobs, and a second path over the same watermarks buys nothing but a race.
            _log.info(
                "early_hydration_skipped_session_open",
                now=now.isoformat(), session_open=session_open.isoformat(), reason=reason,
            )
            return
        try:
            await asyncio.wait_for(self._armed.wait(), timeout=EARLY_HYDRATION_ARM_TIMEOUT_S)
        except TimeoutError:  # asyncio.wait_for raises the builtin on 3.11+
            _log.warning(
                "early_hydration_arm_timeout", timeout_s=EARLY_HYDRATION_ARM_TIMEOUT_S,
                note="scheduler never armed — the chain must not run ahead of it (WO-15)",
            )
            return
        if self._recovery_done is not None:
            # The sibling PostLoginRecovery task (2026-09-09): concurrent, not ordered by registration.
            # Unlike the arming wait above, a timeout here does NOT abort — the chain needs no Kite
            # session, so a wedged recovery must cost freshness, never the day's digest.
            try:
                await asyncio.wait_for(
                    self._recovery_done.wait(), timeout=EARLY_HYDRATION_ARM_TIMEOUT_S
                )
            except TimeoutError:
                _log.warning(
                    "early_hydration_recovery_wait_timeout",
                    timeout_s=EARLY_HYDRATION_ARM_TIMEOUT_S,
                    note="post-login recovery still running — hydrating anyway (no Kite session needed)",
                )
        # Re-read the clock and re-apply the open gate AFTER both waits (2026-09-09 review): each is
        # bounded at up to 900 s, so a login parked on either can sit past ``session_open`` by the time
        # it wakes — the ``now`` captured before them is then stale for BOTH the gate and the reporting
        # stamp. A login that waited its way past the open must still defer to the boot pass / 30-min
        # sweep rather than race them, and the "at" the owner sees must be when the chain actually ran.
        run_now = self._clock.now()
        if run_now.time() >= session_open:
            _log.info(
                "early_hydration_skipped_session_open_after_wait",
                login_at=now.isoformat(), run_at=run_now.isoformat(),
                session_open=session_open.isoformat(), reason=reason,
            )
            return
        # The runner re-checks the open once it actually HOLDS the pass lock (2026-09-10: a 09:06
        # login queued 36 min behind the post-arm one-shot and ran in-session) — the gates above
        # cannot see that wait.
        outcomes = await self._catch_up.hydrate_ahead(
            self._job_ids, reason=reason, not_after=session_open,
        )
        _log.info("early_hydration_done", outcomes=outcomes, at=run_now.strftime("%H:%M"), reason=reason)
        await self._emit_summary(run_now.strftime("%H:%M"), outcomes)

    async def _emit_summary(self, at: str, outcomes: dict[str, str]) -> None:
        """One owner line per early login — the visible proof the day was hydrated ahead of the
        window (and which job, if any, did not make it). Ordered by the chain, not by dict luck."""
        if self._notify is None or not outcomes:
            return
        ordered = [(j, outcomes[j]) for j in self._job_ids if j in outcomes]
        try:
            await self._notify(catalog.early_hydration(at=at, outcomes=ordered))
        except Exception:  # noqa: BLE001 - the summary notify must never fail the hydration
            _log.exception("early_hydration_notify_failed")
