"""Early hydration login hook (§2.6 "Early-hydration addendum", owner-directed 2026-09-09).

The owner leaves at 08:15 and boots the PC 09:30–10:00 — the pre-open chain then fires after the
trade window opens and the G2 digest-before-open criterion reads 61%. On the days they log in early
(~06:30), :class:`EarlyHydration` turns that login into the pre-open chain: trading day + before
session open ⇒ wait for ``scheduler.start()`` (WO-15: never run the chain ahead of arming) ⇒ wait for
the sibling ``PostLoginRecovery`` task ⇒ ``CatchUpRunner.hydrate_ahead`` ⇒ one owner line.

These tests pin the DECISION table (the hydration itself is pinned in ``test_catchup_runner.py``):
before-open hydrates once armed, at/after open is the boot/sweep catch-up's business, a non-trading
day and an un-armed scheduler do nothing, and no failure ever escapes into the login path. Plus the
two 2026-09-09 review corrections: the recovery dependency is a real EVENT (login hooks are
concurrent tasks) that times out into a log line rather than an abort, and the session open is
resolved per login, not frozen at boot.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime, time

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops import early_hydration as eh
from engine.ops.early_hydration import EarlyHydration

EARLY = datetime(2026, 6, 17, 6, 32, tzinfo=IST)       # Wed — the owner's early-login shape
LATE = datetime(2026, 6, 17, 9, 42, tzinfo=IST)        # Wed, after the 09:15 open
AT_OPEN = datetime(2026, 6, 17, 9, 15, tzinfo=IST)     # the boundary itself belongs to catch-up
SATURDAY = datetime(2026, 6, 20, 6, 32, tzinfo=IST)
SESSION_OPEN = time(9, 15)
JOB_IDS = ("surveillance", "universe_build", "news_chain", "catalyst_digest", "preopen_planner")


class _Ticker:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


def _clock_at(when: datetime) -> Clock:
    return Clock(time_source=_Ticker(when))


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


class _FakeCatchUp:
    """Records the ``hydrate_ahead`` calls the hook makes (the runner itself is tested elsewhere)."""

    def __init__(self, outcomes: dict[str, str] | None = None, *, boom: bool = False) -> None:
        self.calls: list[tuple[tuple[str, ...], str]] = []
        self.not_afters: list[time | None] = []
        self._outcomes = outcomes or {}
        self._boom = boom

    async def hydrate_ahead(
        self, job_ids: Sequence[str], *, reason: str, not_after: time | None = None,
    ) -> dict[str, str]:
        self.calls.append((tuple(job_ids), reason))
        self.not_afters.append(not_after)
        if self._boom:
            raise RuntimeError("hydrate blew up")
        return dict(self._outcomes)


def _hook(
    catch_up, calendar, *, at: datetime, armed: asyncio.Event, notify=None,
    recovery_done: asyncio.Event | None = None, session_open: time = SESSION_OPEN,
) -> EarlyHydration:
    return EarlyHydration(
        catch_up, _clock_at(at), calendar, armed, JOB_IDS, lambda: session_open,
        recovery_done=recovery_done, notify=notify,
    )


@pytest.mark.asyncio
async def test_early_login_hydrates_once_the_scheduler_is_armed(calendar) -> None:
    """The whole point: a 06:32 login runs the pre-open chain hours ahead of its clock — but only
    behind an armed scheduler (WO-15), so it WAITS while the boot is still coming up."""
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    task = asyncio.create_task(_hook(catch_up, calendar, at=EARLY, armed=armed).on_login())

    await asyncio.sleep(0)
    assert catch_up.calls == []                      # blocked on arming, never ahead of it

    armed.set()
    await asyncio.wait_for(task, timeout=5)
    assert catch_up.calls == [(JOB_IDS, "early_login")]


@pytest.mark.asyncio
async def test_the_session_open_is_handed_to_the_runner_as_not_after(calendar) -> None:
    """2026-09-10 09:06 login: the hook's own gates passed, then ``hydrate_ahead`` queued 36 minutes
    on the pass lock and ran in-session. The runner re-checks the boundary once it holds the lock,
    so the hook must hand it the day's open."""
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    armed.set()

    await _hook(catch_up, calendar, at=EARLY, armed=armed, session_open=time(9, 15)).on_login()

    assert catch_up.not_afters == [time(9, 15)]


@pytest.mark.asyncio
@pytest.mark.parametrize("when", [LATE, AT_OPEN])
async def test_login_at_or_after_session_open_is_a_no_op(calendar, caplog, when) -> None:
    """From session open on, every named job is DUE — the boot/sweep catch-up owns them, and running
    a second path over the same watermarks buys nothing."""
    catch_up = _FakeCatchUp()
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await _hook(catch_up, calendar, at=when, armed=armed).on_login()

    assert catch_up.calls == []
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_skipped_session_open"]


@pytest.mark.asyncio
async def test_non_trading_day_login_is_a_no_op(calendar, caplog) -> None:
    catch_up = _FakeCatchUp()
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await _hook(catch_up, calendar, at=SATURDAY, armed=armed).on_login()

    assert catch_up.calls == []
    assert [r for r in caplog.records
            if r.getMessage() == "early_hydration_skipped_non_trading_day"]


@pytest.mark.asyncio
async def test_arm_timeout_gives_up_without_hydrating(calendar, caplog, monkeypatch) -> None:
    """A boot that never arms (wedged startup) must not leave the hook parked forever: the wait is
    bounded, and the give-up is logged, not raised."""
    monkeypatch.setattr(eh, "EARLY_HYDRATION_ARM_TIMEOUT_S", 0.01)
    catch_up = _FakeCatchUp()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await asyncio.wait_for(
            _hook(catch_up, calendar, at=EARLY, armed=asyncio.Event()).on_login(), timeout=5,
        )

    assert catch_up.calls == []
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_arm_timeout"]


# ------------------------------------------- the PostLoginRecovery dependency (2026-09-09 review)
# ``SessionManager._fire_login_hooks`` creates ONE TASK PER HOOK, so registering this hook after the
# recovery orders nothing — the two run concurrently. The recovery's ``completed`` event is what
# actually sequences them, and it is a preference, not a hard requirement: the chain needs no Kite
# session, so a recovery that never finishes must be logged and stepped over, never abort the day's
# hydration.


@pytest.mark.asyncio
async def test_hydration_waits_for_the_post_login_recovery(calendar) -> None:
    """The chain wants the token map, backfill and ticker the recovery restores, so it waits while
    ``PostLoginRecovery.run`` is in flight (its ``completed`` event cleared) and goes on the moment
    the ladder ends."""
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    armed.set()
    recovery_done = asyncio.Event()                 # cleared: the recovery is mid-ladder

    task = asyncio.create_task(
        _hook(catch_up, calendar, at=EARLY, armed=armed, recovery_done=recovery_done).on_login()
    )
    await asyncio.sleep(0.05)
    assert catch_up.calls == []                     # parked behind the recovery, not racing it

    recovery_done.set()
    await asyncio.wait_for(task, timeout=5)
    assert catch_up.calls == [(JOB_IDS, "early_login")]


@pytest.mark.asyncio
async def test_a_recovery_that_never_finishes_is_logged_and_stepped_over(
    calendar, caplog, monkeypatch
) -> None:
    """The recovery is a freshness preference, not a dependency — the pre-open chain runs off the
    store and the news feeds, never off a Kite session. A wedged recovery therefore LOGS and the
    hydration PROCEEDS; aborting would cost the day the digest the whole feature exists to buy."""
    monkeypatch.setattr(eh, "EARLY_HYDRATION_ARM_TIMEOUT_S", 0.01)
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await asyncio.wait_for(
            _hook(catch_up, calendar, at=EARLY, armed=armed,
                  recovery_done=asyncio.Event()).on_login(),
            timeout=5,
        )

    assert catch_up.calls == [(JOB_IDS, "early_login")]
    assert [r for r in caplog.records
            if r.getMessage() == "early_hydration_recovery_wait_timeout"]


@pytest.mark.asyncio
async def test_session_open_is_resolved_per_login_not_frozen_at_boot(calendar, caplog) -> None:
    """The hook is built once and lives for the whole process; a session open frozen at boot would be
    some earlier day's boundary (and would miss a special/muhurat session entirely). It is a
    zero-arg callable, evaluated inside the hook on every login."""
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    armed.set()
    opens = [time(9, 15)]
    hook = EarlyHydration(
        catch_up, _clock_at(EARLY), calendar, armed, JOB_IDS, lambda: opens[0],
    )

    await hook.on_login()
    assert catch_up.calls == [(JOB_IDS, "early_login")]

    opens[0] = time(6, 0)                           # a day whose session opens before this login
    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await hook.on_login()

    assert len(catch_up.calls) == 1                 # now at/after the open ⇒ the catch-up's business
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_skipped_session_open"]


# --------------------------------- gate re-applied after the waits (2026-09-09 review)
# Both waits are bounded at up to 900s each — a login parked on either can cross the session open
# before ``hydrate_ahead`` ever runs. The pre-wait ``now`` used for the initial gate is then stale for
# BOTH the gate and the reporting stamp; the fix re-reads the clock after both waits and re-applies the
# identical open check.


@pytest.mark.asyncio
async def test_session_open_crossed_while_parked_on_arming_skips_after_wait(calendar, caplog) -> None:
    """A login before the open whose arming wait ends AFTER the open must defer to the boot pass /
    30-min sweep — hydrating on the stale pre-wait clock read would race them."""
    ticker = _Ticker(EARLY)
    clock = Clock(time_source=ticker)
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    hook = EarlyHydration(catch_up, clock, calendar, armed, JOB_IDS, lambda: SESSION_OPEN)

    task = asyncio.create_task(hook.on_login())
    await asyncio.sleep(0)
    assert catch_up.calls == []                      # parked on arming, nothing decided yet

    after_open = datetime(2026, 6, 17, 9, 20, tzinfo=IST)
    ticker.at = after_open                            # the clock moves on while the hook is parked

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        armed.set()
        await asyncio.wait_for(task, timeout=5)

    assert catch_up.calls == []                       # never hydrated — the open was crossed mid-wait
    skipped = [
        r for r in caplog.records
        if r.getMessage() == "early_hydration_skipped_session_open_after_wait"
    ]
    assert skipped
    assert skipped[0].login_at == EARLY.isoformat()
    assert skipped[0].run_at == after_open.isoformat()


@pytest.mark.asyncio
async def test_summary_stamp_is_the_run_time_not_the_login_time(calendar) -> None:
    """The owner line (and the ``early_hydration_done`` log) must carry WHEN the chain actually ran,
    not when the login landed — a login parked for minutes on the recovery wait must not misreport its
    own start time as the hydration time."""
    ticker = _Ticker(EARLY)
    clock = Clock(time_source=ticker)
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    armed.set()
    recovery_done = asyncio.Event()                   # cleared: parks the hook on the recovery wait
    sent = []

    async def notify(msg):
        sent.append(msg)

    hook = EarlyHydration(
        catch_up, clock, calendar, armed, JOB_IDS, lambda: SESSION_OPEN,
        recovery_done=recovery_done, notify=notify,
    )
    task = asyncio.create_task(hook.on_login())
    await asyncio.sleep(0)
    assert catch_up.calls == []                       # parked on the recovery, clock about to move

    run_at = datetime(2026, 6, 17, 8, 10, tzinfo=IST)  # later than EARLY, still before the 09:15 open
    ticker.at = run_at
    recovery_done.set()
    await asyncio.wait_for(task, timeout=5)

    assert catch_up.calls == [(JOB_IDS, "early_login")]
    assert len(sent) == 1
    assert "08:10" in sent[0].body and "06:32" not in sent[0].body


@pytest.mark.asyncio
async def test_owner_gets_exactly_one_summary_line(calendar) -> None:
    """One line, carrying the outcomes — the owner's proof the day was hydrated early (and of what
    the login did NOT need to re-run)."""
    sent: list[CatalogMessage] = []

    async def notify(msg: CatalogMessage) -> None:
        sent.append(msg)

    catch_up = _FakeCatchUp({
        "surveillance": "already_run", "universe_build": "ran", "news_chain": "ran",
        "catalyst_digest": "ran", "preopen_planner": "ran",
    })
    armed = asyncio.Event()
    armed.set()

    await _hook(catch_up, calendar, at=EARLY, armed=armed, notify=notify).on_login()

    assert len(sent) == 1
    msg = sent[0]
    assert msg.kind == MessageKind.EARLY_HYDRATION and msg.severity == "info"
    assert "06:32" in msg.body
    assert "universe_build ran" in msg.body and "preopen_planner ran" in msg.body
    assert "surveillance" in msg.body and "already run" in msg.body


@pytest.mark.asyncio
async def test_a_failing_hydration_never_escapes_the_login_hook(calendar, caplog) -> None:
    """This runs inside ``SessionManager.complete_login``'s hook fan-out (PostLoginRecovery's guard
    style): a raise here must never break the login path or the loop."""
    catch_up = _FakeCatchUp(boom=True)
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await _hook(catch_up, calendar, at=EARLY, armed=armed).on_login()   # must not raise

    assert catch_up.calls == [(JOB_IDS, "early_login")]
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_failed"]


# ------------------------------------------------- the boot trigger (2026-09-21)
# 2026-09-16: the engine booted 06:58 with no Kite login until 10:04, the PC slept 08:00-10:00 through
# the scheduled 08:20-08:50 fires, and the catalyst digest landed 10:18, after the open. ``on_boot``
# runs the identical chain once at process boot, under the same gates as an early login.


@pytest.mark.asyncio
async def test_on_boot_hydrates_once_the_scheduler_is_armed(calendar) -> None:
    """A boot before the open runs the pre-open chain — but only behind an armed scheduler (WO-15),
    so it WAITS while the boot is still coming up, same as an early login."""
    catch_up = _FakeCatchUp({"universe_build": "ran"})
    armed = asyncio.Event()
    hook = _hook(catch_up, calendar, at=EARLY, armed=armed)
    task = asyncio.create_task(hook.on_boot())

    await asyncio.sleep(0)
    assert catch_up.calls == []                      # blocked on arming, never ahead of it

    armed.set()
    await asyncio.wait_for(task, timeout=5)
    assert catch_up.calls == [(JOB_IDS, "early_boot")]
    assert catch_up.not_afters == [time(9, 15)]


@pytest.mark.asyncio
@pytest.mark.parametrize("when", [LATE, AT_OPEN])
async def test_on_boot_at_or_after_session_open_is_a_no_op(calendar, caplog, when) -> None:
    """From session open on, every named job is DUE — a boot after the open must defer to the boot
    pass / 30-min sweep exactly like a late login."""
    catch_up = _FakeCatchUp()
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await _hook(catch_up, calendar, at=when, armed=armed).on_boot()

    assert catch_up.calls == []
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_skipped_session_open"]


@pytest.mark.asyncio
async def test_a_failing_hydration_never_escapes_the_boot_hook(calendar, caplog) -> None:
    """The boot task is a fire-and-forget ``asyncio.create_task`` too: a raise here must never
    propagate into the boot path, same guard shape as ``on_login``."""
    catch_up = _FakeCatchUp(boom=True)
    armed = asyncio.Event()
    armed.set()

    with caplog.at_level(logging.INFO, logger="engine.ops.early_hydration"):
        await _hook(catch_up, calendar, at=EARLY, armed=armed).on_boot()   # must not raise

    assert catch_up.calls == [(JOB_IDS, "early_boot")]
    assert [r for r in caplog.records if r.getMessage() == "early_hydration_failed"]
