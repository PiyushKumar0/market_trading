"""Early hydration — an early Kite login runs the pre-open chain ahead of its clock.

The owner often starts the system only around 09:30–10:00, after the pre-open chain (surveillance,
news, universe, catalyst digest, pre-open planner) was scheduled to fire. The catch-up machinery's
due-gates treat today's job as not-missed until its fire-time has passed, so an early pass replays
nothing; :meth:`engine.ops.jobs.CatchUpRunner.hydrate_ahead` is the deliberate exception this module
drives.

:class:`EarlyHydration` is a :meth:`~engine.broker.session.SessionManager.add_login_hook` callback,
registered beside :class:`~engine.ops.post_login.PostLoginRecovery`. Registration order buys NOTHING:
:meth:`SessionManager._fire_login_hooks` creates one task per hook, so the hooks run CONCURRENTLY; the
recovery's ``completed`` event is what sequences them. Decision table:

- not a trading day ⇒ nothing to hydrate;
- login at/after session open ⇒ nothing to do HERE: from the open on, every job is simply DUE and the
  boot pass / 30-min sweep own it. Two paths for one job would only race. The open is resolved PER
  LOGIN from the calendar (a zero-arg callable), never frozen at boot (special/muhurat sessions);
- otherwise wait for ``scheduler.start()`` (the ``armed`` event): the news chain and digest must never
  run ahead of arming. The wait is bounded (:data:`EARLY_HYDRATION_ARM_TIMEOUT_S`) so a boot that never
  arms leaves no parked task;
- then wait for the sibling :class:`PostLoginRecovery` task (``recovery_done``). That is a PREFERENCE,
  not a requirement — the chain reads the store and the news feeds, never a Kite session — so the wait
  times out into a log line and the hydration PROCEEDS;
- then ``hydrate_ahead`` (single-flighted against the boot pass by the runner's own lock) and one
  owner line.

A second login the same day needs no latch: every watermark is set, so every job reports
``already_run``. Nothing here ever raises — a login hook that throws would break the login path.

Boot trigger: a login is not the only gap (the PC may sleep through the scheduled fires).
:meth:`EarlyHydration.on_boot` runs the identical chain once at process boot, under the same gates.
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

#: How long each wait (arming, recovery) lasts: far longer than any boot, and it leaves the early
#: head-start intact.
EARLY_HYDRATION_ARM_TIMEOUT_S = 900


class EarlyHydration:
    """The early-login pre-open hydration hook (see the module docstring).

    Parameters
    ----------
    armed:
        Set by the composition root inside ``start_scheduler_and_fire_post_arm`` — after
        ``scheduler.start()`` AND after the post-arm one-shot is dispatched, so a login landing
        mid-boot cannot win the single-flight pass lock and turn that one-shot into a no-op.
    job_ids:
        The chain to hydrate, in registry order (``engine.ops.main.EARLY_HYDRATION_JOB_IDS``).
        This module owns WHEN, never WHAT.
    session_open:
        Zero-arg callable returning the login day's continuous-session open (IST) — a CALLABLE
        because the hook lives for the whole process, so a frozen open would be an earlier day's.
    recovery_done:
        :attr:`PostLoginRecovery.completed`; ``None`` when no recovery is wired.
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
        """The login hook. A failure is logged and dropped, never propagated into ``complete_login``."""
        try:
            await self._hydrate("early_login")
        except Exception:  # noqa: BLE001 - a login hook degrades + logs, never breaks the login path
            _log.exception("early_hydration_failed", reason="early_login")

    async def on_boot(self) -> None:
        """The boot hook: a pre-open boot on a trading day runs the same chain a login would."""
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
                note="scheduler never armed — the chain must not run ahead of it",
            )
            return
        if self._recovery_done is not None:
            # Unlike the arming wait, a timeout here does NOT abort: the chain needs no Kite session.
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
        # Re-apply the open gate on a fresh clock AFTER both waits (each up to 900 s): a login that
        # waited past the open must defer to the boot pass / sweep, and the reported "at" is the real run time.
        run_now = self._clock.now()
        if run_now.time() >= session_open:
            _log.info(
                "early_hydration_skipped_session_open_after_wait",
                login_at=now.isoformat(), run_at=run_now.isoformat(),
                session_open=session_open.isoformat(), reason=reason,
            )
            return
        # The runner re-checks the open once it HOLDS the pass lock; the gates above cannot see that wait.
        outcomes = await self._catch_up.hydrate_ahead(
            self._job_ids, reason=reason, not_after=session_open,
        )
        _log.info("early_hydration_done", outcomes=outcomes, at=run_now.strftime("%H:%M"), reason=reason)
        await self._emit_summary(run_now.strftime("%H:%M"), outcomes)

    async def _emit_summary(self, at: str, outcomes: dict[str, str]) -> None:
        """One owner line per early run, ordered by the chain."""
        if self._notify is None or not outcomes:
            return
        ordered = [(j, outcomes[j]) for j in self._job_ids if j in outcomes]
        try:
            await self._notify(catalog.early_hydration(at=at, outcomes=ordered))
        except Exception:  # noqa: BLE001 - the summary notify must never fail the hydration
            _log.exception("early_hydration_notify_failed")
