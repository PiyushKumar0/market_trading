"""Session lifecycle: the §2.6 every-startup recovery + catch-up sequence, and the shutdown guard.

The engine is up only during active periods and may be deliberately stopped in between (§2.6). The
defining invariant: **every startup is a full recovery** — scheduled or manual, after a clean stop or a
crash, after a gap of minutes or days — and the platform never breaks because it was off. Capital
protection is broker-resident throughout (R3), so being off is safe.

Phase-1 shape of the §2.6 sequence (steps that are OMS/broker-side land in Phase 3 as injected hooks):

0. **Lifecycle boot (§2.2):** read the prior ``engine_lifecycle`` row. Prior ``state ∈ {RUNNING,
   STOPPING}`` with the prior pid ALIVE ⇒ **refuse to start** (:class:`SingleInstanceError` — another
   instance owns the single-writer stores, O7/E4); prior pid dead ⇒ crash/interrupted-shutdown, the
   startup report leads with a crash-recovered notice. Then atomically commit ``state='RUNNING'`` +
   fresh ``last_alive_at``/``pid``/``started_at`` (the fresh ``last_alive_at`` IS the watchdog re-arm
   clear — its debounce predicate is ``last_down_alert_at < last_alive_at``), start the
   dedicated-thread :class:`~engine.ops.heartbeat.HeartbeatWriter`, and emit ``ENGINE_STARTED`` —
   all before step 1. Fast unclean boots inside ``lifecycle.crashloop_window_s`` coalesce into a
   single ``ENGINE_CRASHLOOP`` alert (§2.2/§10.7) instead of a page per respawn.
1. Read sticky kill / mode / trade-window state (R10) + run the startup self-test (D11).
2. Reconcile vs broker = truth (R5) — injected hook, TODO(Phase 3).
3. Overdue-MIS startup square-off (§2.6 step 3) — injected hook, TODO(Phase 3).
4. Data-gap backfill (bars + NIFTY50/India-VIX history, §7.1 ``regime_data_ready``) — injected hook;
   the integrator wires ``BackfillJob.warmup_gap``.
5. Missed-job catch-up — :class:`engine.ops.jobs.CatchUpRunner` (per-job ``job_runs`` watermarks,
   §2.6 step 5 classes); safety-critical failures ⇒ FROZEN-for-entries.
6. Cold-start warm-up gate (§7.1 ``warmup_ready``/``regime_data_ready``) — the injected
   :class:`~engine.ops.warmup.WarmupGate` answers; this class applies the consequence PER COVERAGE
   CLASS (2026-09-13) and PER SYMBOL (2026-09-17): only a REGIME shortfall (NIFTY 50 / India VIX —
   every candidate depends on them) or an UNATTRIBUTABLE blocker is FROZEN via the risk-state setter
   + ``WARMUP_FROZEN`` alert; an INTRADAY or DAILY shortfall is logged and left to the per-candidate
   rules (risk gate + pre-screen), which refuse only the SYMBOLS that are short. Never trade on thin
   data — and never take the book down for one symbol's coverage hole.
7. Re-arm schedules + resume the ticker (WARMING — feed-stale alarms suppressed, §3.2.12) and send
   the startup/recovery report.

Clean stop (§2.2/§10.8): ``STOPPING`` FIRST (draws the planned-vs-crash line) → shutdown-guard hooks
(cancel entries / flatten MIS / verify CNC PROTECTED / backup — Phase-3 seams) → ``STOPPED`` commit
(**the point of no return** — nothing capital-critical runs after it) → heartbeat join → best-effort
``ENGINE_STOPPED``. A crash during STOPPING leaves ``state='STOPPING'`` ⇒ the watchdog fires
``ENGINE_DOWN`` and the next startup re-verifies — never silently mislabelled clean.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import Settings
from engine.core.db import transaction
from engine.core.enums import Actor, RiskState
from engine.core.log import get_logger
from engine.core.types import TradeWindow
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.heartbeat import HeartbeatWriter, pid_alive
from engine.ops.jobs import (  # noqa: F401  (CatchUpRunner re-exported: §3.2.12 name)
    CatchUpResult,
    CatchUpRunner,
)
from engine.ops.selftest import SelfTest, SelfTestReport
from engine.ops.warmup import (
    CLASS_DAILY,
    CLASS_INTRADAY,
    CLASS_REGIME,
    CLASS_UNKNOWN,
    blocker_class,
    classify_blockers,
)

_log = get_logger("engine.ops.lifecycle")

Hook = Callable[[], Awaitable[None]]
AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)
LifecycleNotify = Callable[[CatalogMessage], Awaitable[None]]   # typed process-lifecycle send (§2.2)

#: Unclean boots within ``lifecycle.crashloop_window_s`` before ENGINE_STARTED coalesces into one
#: ENGINE_CRASHLOOP (§2.2 — spec-silent threshold, resolved here: 3rd fast respawn trips the alarm).
CRASHLOOP_MIN_BOOTS = 3


#: A not-ready warm-up answer that rendered NO blocker at all. The shipped gate cannot produce it
#: (``status()`` sets ``ready = not blockers``), but the gate seam is duck-typed, and a shortfall
#: nobody attributed must not read as "intraday-only" and skip the freeze — it is normalised into a
#: rendered blocker so it classifies UNKNOWN and travels the fail-closed path like any other.
_UNATTRIBUTED_BLOCKER = "warmup not ready — no blocker rendered"


#: The only two GLOBAL warm-up conditions (§2.6 step-6 addendum, owner-directed 2026-09-17). REGIME
#: is NIFTY 50 / India VIX daily history — every candidate of every class depends on the market
#: context built from it, so there is no per-symbol answer to give. UNKNOWN is a blocker nobody could
#: attribute (R6): what cannot be verified is treated as missing, for the whole book.
_FREEZING_CLASSES = (CLASS_REGIME, CLASS_UNKNOWN)


def _freezes_entries(blockers: Sequence[str]) -> bool:
    """Does this shortfall drive the GLOBAL FROZEN-for-entries (§2.6 step 6, narrowed 2026-09-13,
    narrowed again 2026-09-17)? True ONLY for the REGIME class and for a blocker whose class cannot
    be told from its rendering (unattributable coverage freezes, R6).

    Rationale for the 2026-09-17 narrowing: since 2026-09-13 an INTRADAY shortfall was already
    refused PER CANDIDATE rather than freezing, and DAILY now gets the identical treatment
    (per-symbol refusal at the gate), so only the two genuinely GLOBAL conditions set the global
    FROZEN cause. One symbol's daily hole must not take the book down — on 2026-09-15 a single
    ``rsi2/trend/mom:OLAELEC daily bars 193/200`` held the global ``warmup_ready`` FROZEN cause for
    ~12 hours. A market-wide daily hole (the nightly ``daily_bars`` job failed) still refuses every
    swing/position candidate per symbol at the gate AND pages the owner through the per-class
    transition notice (``engine.ops.main._log_class_transition``).

    False ⇒ the shortfall is intraday and/or daily, per-symbol, and the risk state is not this
    gate's business."""
    if not blockers:
        return True                    # not ready for a reason nobody named ⇒ never the open side
    return any(blocker_class(b) in _FREEZING_CLASSES for b in blockers)


def _rendered_blockers(status) -> list[str]:
    """The not-ready blocker lines to act on. ``None`` status = the gate RAISED (coverage could not
    be verified); an empty list = it answered not-ready without saying why. Both are unattributable
    and both freeze — the class scoping may only ever narrow a shortfall it can NAME."""
    blockers = list(status.blockers) if status is not None else ["warmup check failed"]
    return blockers or [_UNATTRIBUTED_BLOCKER]


class SingleInstanceError(RuntimeError):
    """§2.6 step 0: prior state RUNNING/STOPPING and the prior pid is ALIVE — another instance owns
    the single-writer stores (O7/E4). The new process must refuse to start; nothing was written."""


class ShutdownBlockedError(RuntimeError):
    """A shutdown-guard step failed and no owner override was given (§10.8) — the stop is blocked,
    the engine stays RUNNING (never exit with an unprotected position / working entry order, R3)."""


class StartupReport(BaseModel):
    started_at: str
    sticky_mode: str
    sticky_risk_state: str
    killed: bool
    needs_login: bool
    integrity_ok: bool
    crash_recovered: bool = False        # prior run exited uncleanly (state RUNNING/STOPPING) — §2.6 step 0
    prior_state: str | None = None       # engine_lifecycle.state read before this run marked RUNNING
    crashloop: bool = False              # this boot is part of a coalesced crash-loop episode (§2.2)
    off_duration_s: float | None = None  # since last_alive_at (crash) / last_clean_stop_at (clean)
    frozen_reasons: list[str] = Field(default_factory=list)
    jobs_caught_up: list[str] = Field(default_factory=list)
    jobs_failed: list[str] = Field(default_factory=list)
    warmup_blockers: list[str] = Field(default_factory=list)
    warmup_young_excluded: list[str] = Field(default_factory=list)  # young listings off the lookback gate
    #: Warm-up COVERAGE CLASSES short at boot, sorted (``intraday``/``daily``/``regime``/``unknown``).
    #: Only regime/unknown freeze entries (2026-09-17); the field says which class was short whether
    #: or not it froze, so "entries are open with intraday or daily coverage short for some symbols"
    #: is never silent.
    warmup_classes_short: list[str] = Field(default_factory=list)
    deferred_steps: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class WarmupReapply(BaseModel):
    """Outcome of a post-login :meth:`SessionLifecycle.reapply_warmup_gate` (§2.6 cold-start RE-TRIGGER).

    ``outcome`` is one of ``deferred`` (no gate wired), ``frozen`` (a freezing class — REGIME or an
    unattributable blocker — still short, re-frozen), ``ready_<why>`` (every class covered), or
    ``short_<why>`` (2026-09-17, renamed from ``intraday_short_<why>``: the freezing classes are
    covered and the freeze is resolved, with a NON-FREEZING class — intraday and/or daily — still
    short, whose shortfall keeps being refused per symbol at the gate). ``why`` ∈ {``lifted`` (freeze
    cleared), ``already_normal``, ``kill_held``, ``latched`` (CLOSE_ONLY/KILLED — owner re-arm only),
    ``other_freeze`` (a non-warm-up precondition still stands)}.

    ``ready`` keeps the whole-gate meaning (no blockers in ANY class), so a short lift reports
    ``ready=False, lifted=True`` — never "ready" with coverage missing."""

    ready: bool
    blockers: list[str] = Field(default_factory=list)
    young_excluded: list[str] = Field(default_factory=list)
    classes_short: list[str] = Field(default_factory=list)   # sorted coverage classes still short
    froze: bool = False
    lifted: bool = False
    outcome: str = ""


class SessionLifecycle:
    """Drives the §2.6 every-startup recovery & catch-up sequence and the shutdown guard.

    Parameters (beyond the Phase-0 set)
    -----------------------------------
    heartbeat:
        The dedicated-thread liveness writer (§2.2). Started right after the RUNNING commit at
        step 0; joined after the STOPPED commit at shutdown (so a long STOPPING guard keeps beating).
    warmup_gate:
        Duck-typed warm-up gate (``async status() -> WarmupStatus`` with ``.ready``/``.blockers``) —
        :class:`engine.ops.warmup.WarmupGate`. Consequence (FROZEN + alert) applied HERE (§2.6 step 6).
    boot_history_path:
        Private JSON file for crash-loop coalescing (§2.2). ``None`` disables tracking (tests /
        Phase-0 wiring); the integrator passes ``<data_dir>/lifecycle_boots.json``.
    reconcile_hook / overdue_squareoff_hook:
        §2.6 steps 2–3 — OMS/broker-side. TODO(Phase 3): the integrator wires ReconcileJob /
        SquareOffScheduler.run_window_squareoff; ``None`` logs the step as deferred.
    backfill_hook:
        §2.6 step 4 — the integrator wires ``BackfillJob.warmup_gap`` + NIFTY50/VIX history fill.
    rearm_schedules_hook / ticker_resume_hook:
        §2.6 step 7 — re-arm the §10.1 schedule and resume the ticker into WARMING.
    cancel_entries_hook / flatten_mis_hook / verify_cnc_protected_hook / backup_hook:
        The §10.8 shutdown-guard seams, run in that order while STOPPING. TODO(Phase 3) for the
        order-touching three; backup is the §10.5 watermark snapshot.
    """

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        clock: Clock,
        calendar: NSECalendar,
        settings: Settings,
        mode_manager,
        kill_switch,
        self_test: SelfTest,
        catch_up: CatchUpRunner,
        alert: AlertCallback | None = None,
        notify: LifecycleNotify | None = None,
        build_version: str = "0.0.0",
        heartbeat: HeartbeatWriter | None = None,
        warmup_gate=None,
        latch=None,
        boot_history_path: Path | None = None,
        reconcile_hook: Hook | None = None,
        overdue_squareoff_hook: Hook | None = None,
        backfill_hook: Hook | None = None,
        rearm_schedules_hook: Hook | None = None,
        ticker_resume_hook: Hook | None = None,
        cancel_entries_hook: Hook | None = None,
        flatten_mis_hook: Hook | None = None,
        verify_cnc_protected_hook: Hook | None = None,
        backup_hook: Hook | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar
        self._settings = settings
        self._mode = mode_manager
        self._kill = kill_switch
        self._selftest = self_test
        self._catch_up = catch_up
        self._alert = alert
        self._notify = notify
        self._build_version = build_version
        self._heartbeat = heartbeat
        self._warmup_gate = warmup_gate
        self._latch = latch
        self._boot_history_path = boot_history_path
        self._reconcile = reconcile_hook
        self._overdue_squareoff = overdue_squareoff_hook
        self._backfill = backfill_hook
        self._rearm_schedules = rearm_schedules_hook
        self._ticker_resume = ticker_resume_hook
        self._cancel_entries = cancel_entries_hook
        self._flatten_mis = flatten_mis_hook
        self._verify_cnc = verify_cnc_protected_hook
        self._backup = backup_hook
        self._suppress_report_notify = False   # crash-loop coalescing (§2.2): silent boots stay silent

    async def _freeze(self, cause: str, detail: str) -> None:
        """FROZEN-for-entries through the §3.5.3 cause ledger when wired (single-writer discipline —
        2026-07-28 review: a direct write is invisible to ``clear_cause`` re-arms and to the lift
        path); the direct setter remains only for latch-less construction (older tests)."""
        if self._latch is not None:
            await self._latch.set_cause(cause, RiskState.FROZEN, detail, Actor.RISK_GATE)
        else:
            await self._mode.set_risk_state(RiskState.FROZEN, detail, Actor.RISK_GATE)

    # ------------------------------------------------------------------ startup (§2.6)
    async def startup(self, *, check_skew: bool = True) -> StartupReport:
        # 0) Process-lifecycle boot (§2.6 step 0 / §2.2): crash detection → atomic RUNNING commit →
        #    heartbeat thread → ENGINE_STARTED, all BEFORE the recovery body.
        #
        #    PRIMARY vs SECONDARY (2026-07-21 double-run): the real mutual exclusion is now the
        #    OS-level ``engine.ops.single_instance.InstanceLock``, acquired in the composition root
        #    (``engine.ops.main.run``) BEFORE any shared resource is touched (sqlite / DuckDB / :8400 /
        #    Telegram) — a kernel file lock, released on ANY process death. This ``engine_lifecycle``
        #    check STAYS as the SECONDARY, belt-and-suspenders guard: it drives crash-recovered
        #    detection (``crash_recovered`` below) and carries the pid-alive semantics the file lock has
        #    no need for. On its own it is only a TOCTOU read (two boots can both pass it, and an
        #    instance that wedges before this commit is invisible to it) — which is precisely why the
        #    file lock is now primary and this remains defence in depth. No behaviour change here.
        prior = self._read_prior()
        prior_state = prior["state"] if prior else None
        prior_pid = int(prior["pid"]) if prior and prior["pid"] else None
        if prior_state in ("RUNNING", "STOPPING") and prior_pid and prior_pid != os.getpid() and pid_alive(prior_pid):
            # Documented caveat (engine.ops.heartbeat.pid_alive): pid reuse can rarely alias a dead
            # engine to an unrelated live process — the guard then refuses a start that manual
            # inspection must clear (safer than double-running the single-writer stores).
            _log.error("single_instance_refused", prior_state=prior_state, prior_pid=prior_pid)
            raise SingleInstanceError(
                f"engine_lifecycle state={prior_state!r} with pid {prior_pid} still ALIVE — another "
                "instance owns the single-writer stores; refusing to start (§2.6 step 0, O7/E4)"
            )
        crash_recovered = prior_state in ("RUNNING", "STOPPING")
        off_since = self._off_since(prior, crash_recovered)

        self._commit_running()
        if self._heartbeat is not None:
            self._heartbeat.start()

        now = self._clock.now()
        report = StartupReport(
            started_at=now.isoformat(),
            sticky_mode=self._mode.mode().value,
            sticky_risk_state=self._mode.risk_state().value,
            killed=self._kill.is_killed(),
            needs_login=False,
            integrity_ok=True,
            crash_recovered=crash_recovered,
            prior_state=prior_state,
            off_duration_s=max(0.0, (now - off_since).total_seconds()) if off_since else None,
        )
        if crash_recovered:
            report.notes.append(f"crash_recovered_prior_state={prior_state}")

        await self._emit_boot_notification(report)

        # 1) Sticky kill/mode/trade-window state is read above (R10) — log it.
        _log.info("startup_sticky_state", mode=report.sticky_mode, risk_state=report.sticky_risk_state,
                  killed=report.killed, crash_recovered=crash_recovered,
                  off_duration_s=report.off_duration_s)

        # 1b) Seed the trade window from settings on first run (§2.6); sticky thereafter.
        seed = TradeWindow(
            start=self._settings.trade_window.start_ist,
            end=self._settings.trade_window.end_ist,
            squareoff_buffer_min=self._settings.trade_window.squareoff_buffer_min,
        )
        if self._mode.seed_trade_window_if_absent(seed):
            report.notes.append("trade_window_seeded")

        # 1c) Self-test (D11) — freshness/warm-up excluded here: catch-up (step 5) has not run yet,
        #     so the lifecycle verifies data-freshness after step 5 and warm-up at step 6 instead.
        st: SelfTestReport = await self._selftest.run(check_skew=check_skew, include_freshness=False)
        report.needs_login = st.needs_login
        report.frozen_reasons = list(st.frozen_reasons)

        # Apply the §2.4 single integrity rule: a protected-store failure with a FLAT book ⇒ FROZEN;
        # with a LIVE book (or at runtime) ⇒ kill.
        integrity_failed = any(c.name.startswith("protected_store:") and c.status.value == "FAIL" for c in st.checks)
        report.integrity_ok = not integrity_failed
        if integrity_failed:
            if self._open_positions_count() == 0:
                await self._freeze("protected_store_integrity", "protected_store_integrity")
                report.notes.append("integrity_failed_flat_book_frozen")
            else:
                await self._kill.trigger("protected_store_integrity_live_book", actor=Actor.RISK_GATE, flatten=True)
                report.killed = True

        # Other FROZEN-implying causes (secrets/clock/window/token) ⇒ FROZEN entries until cleared.
        if report.frozen_reasons and not self._kill.is_killed():
            await self._freeze("startup_selftest", ",".join(report.frozen_reasons))

        # 2c) Day-scoped risk-counter rebuild + continuous equity halt-ladder re-eval (§2.6) —
        #     ledger/reconcile-dependent. TODO(Phase 2/3): wired with ExposureTracker + the ledger.
        report.deferred_steps.append("equity_halt_ladder_reeval (§2.6 — TODO(Phase 2/3))")

        # 2) Reconcile vs broker = truth (R5) — TODO(Phase 3): OMS reconcile + REC_FILL_SUSPECTED.
        await self._run_hook("reconcile", self._reconcile, report)
        # 3) Overdue-MIS startup square-off (§2.6 step 3) — TODO(Phase 3): SquareOffScheduler.
        await self._run_hook("overdue_mis_squareoff", self._overdue_squareoff, report)
        # 4) Data-gap backfill incl. NIFTY50 + India VIX history (§2.6 step 4 / §7.1 regime_data_ready).
        await self._run_hook("data_gap_backfill", self._backfill, report)

        # 5) Missed-job catch-up (§2.6 step 5) — watermark-driven, by class in dependency order.
        result: CatchUpResult = await self._catch_up.catch_up(off_since=off_since)
        report.jobs_caught_up = list(result.jobs_caught_up)
        report.jobs_failed = list(result.jobs_failed)
        if result.frozen_reasons:
            # Belt-and-suspenders: the runner's own freeze seam may not be wired — the lifecycle
            # guarantees a safety-critical catch-up failure never leaves entries open (§2.6).
            report.frozen_reasons.extend(r for r in result.frozen_reasons if r not in report.frozen_reasons)
            if not self._kill.is_killed():
                await self._freeze("catchup_safety_jobs", ",".join(result.frozen_reasons))
        elif self._latch is not None and not self._kill.is_killed():
            # Inverse of the freeze above (2026-09-01: the cause had a set site but no clear site,
            # so one transient boot failure kept entries FROZEN for two full sessions after the
            # catch-up had recovered). A clean pass IS the re-verification of exactly this cause's
            # predicate — same clear-only-what-was-re-verified rule as _maybe_lift_warmup_freeze;
            # clear_cause is an idempotent no-op when the cause is not latched, and cause-scoped,
            # so any other standing freeze (owner_pause, floor rung, warm-up) keeps the state.
            await self._latch.clear_cause("catchup_safety_jobs", Actor.RISK_GATE)

        # 6) Cold-start warm-up gate (§2.6 step 6 / §7.1 warmup_ready + regime_data_ready).
        await self._apply_warmup_gate(report)

        # 7) Re-arm schedules + resume the ticker into WARMING (feed-stale alarms suppressed, §3.2.12).
        await self._run_hook("rearm_schedules", self._rearm_schedules, report)
        await self._run_hook("ticker_resume", self._ticker_resume, report)

        # 7b) Alert the owner with the startup/recovery report (§2.6).
        await self._emit_report(report)
        return report

    # ------------------------------------------------------------------ shutdown (§2.2/§10.8)
    async def shutdown(self, *, owner_override: bool = False, reason: str = "owner") -> None:
        """Clean/planned stop (§2.2/§2.6/§3.5.3): draw the planned-vs-crash line, run the shutdown
        guard, commit the clean-stop state, and emit ENGINE_STOPPED as the last act.

        Ordering is load-bearing (§2.2): set ``state='STOPPING'`` FIRST (a crash while STOPPING stays
        STOPPING ⇒ watchdog ``ENGINE_DOWN``, never mislabelled clean), run the guard, THEN commit
        ``state='STOPPED'`` + ``last_clean_stop_at`` (the point of no return), join the heartbeat,
        THEN best-effort send ENGINE_STOPPED — a failed send never blocks exit.

        Guard semantics (§10.8): cancel + verify every working/unfilled entry order; flatten an open
        MIS before its window-end completes (or ``owner_override`` accepts the broker 15:25 backstop);
        verify every open CNC is PROTECTED (live resting GTT); snapshot a backup if due. The
        order-touching hooks are TODO(Phase 3) — ``None`` logs the step as deferred. A guard step
        that RAISES without ``owner_override`` blocks the stop (:class:`ShutdownBlockedError`) and
        the engine returns to RUNNING — never exit with an unprotected position (R3).
        """
        self._mark_stopping()   # FIRST — draws the planned-vs-crash line (§2.2)
        open_count = self._open_positions_count()
        if open_count and not owner_override:
            _log.warning("shutdown_guard_open_positions", count=open_count,
                         note="Phase 3 flattens MIS / verifies CNC GTTs before exit (§10.8)")

        guard_steps: tuple[tuple[str, Hook | None], ...] = (
            ("cancel_entry_orders", self._cancel_entries),      # TODO(Phase 3) §10.8 (i)
            ("flatten_open_mis", self._flatten_mis),            # TODO(Phase 3) §3.2.8 shutdown guard
            ("verify_cnc_protected", self._verify_cnc),         # TODO(Phase 3) §10.8 (ii)
            ("shutdown_backup", self._backup),                  # §10.5 watermark snapshot
        )
        for name, hook in guard_steps:
            if hook is None:
                _log.info("shutdown_guard_deferred", step=name)
                continue
            try:
                await hook()
            except Exception:  # noqa: BLE001 - a guard failure must block the stop, not crash past it
                _log.exception("shutdown_guard_failed", step=name, owner_override=owner_override)
                if self._alert is not None:
                    await self._alert("critical", f"shutdown guard step failed: {name}")
                if not owner_override:
                    self._mark_running_again()   # stop blocked — still the live instance (R3)
                    raise ShutdownBlockedError(
                        f"shutdown guard step {name!r} failed and no owner override was given (§10.8)"
                    ) from None
        _log.info("shutdown", owner_override=owner_override, open_positions=open_count, reason=reason)

        self._mark_stopped()    # point of no return — clean stop committed (§2.2)
        if self._heartbeat is not None:
            self._heartbeat.stop()
        await self._emit_lifecycle_stopped(reason=reason, open_positions=open_count)

    # ----------------------------------------------------------------- lifecycle state (§2.2/§4.2)
    def _read_prior(self) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT state, pid, last_alive_at, started_at, last_clean_stop_at FROM engine_lifecycle WHERE id=1"
        ).fetchone()

    def _off_since(self, prior: sqlite3.Row | None, crash_recovered: bool) -> datetime | None:
        """The last clean checkpoint the off-window started at (§2.6 step 4): the crash's last
        heartbeat, or the clean stop's commit time."""
        if prior is None:
            return None
        raw = prior["last_alive_at"] if crash_recovered else prior["last_clean_stop_at"]
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    def _commit_running(self) -> None:
        """§2.6 step 0: atomically mark this run RUNNING with fresh pid/started_at/last_alive_at.
        The fresh ``last_alive_at`` also clears the watchdog re-arm (its debounce predicate is
        ``last_down_alert_at < last_alive_at`` — a just-booted engine can't be mistaken for the
        outage it is recovering from, §2.2)."""
        now = self._clock.now().isoformat()
        pid = os.getpid()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO engine_lifecycle (id, state, last_alive_at, pid, started_at, version)
                VALUES (1, 'RUNNING', ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    state='RUNNING', last_alive_at=excluded.last_alive_at, pid=excluded.pid,
                    started_at=excluded.started_at, version=excluded.version
                """,
                (now, pid, now, self._build_version),
            )

    def _mark_stopping(self) -> None:
        with transaction(self._conn):
            self._conn.execute("UPDATE engine_lifecycle SET state='STOPPING' WHERE id=1")

    def _mark_running_again(self) -> None:
        """A blocked stop returns to RUNNING (the process IS still the live instance, §10.8)."""
        with transaction(self._conn):
            self._conn.execute("UPDATE engine_lifecycle SET state='RUNNING' WHERE id=1")

    def _mark_stopped(self) -> None:
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE engine_lifecycle SET state='STOPPED', last_clean_stop_at=? WHERE id=1", (now,)
            )

    # ----------------------------------------------------------------- boot notification + crash-loop
    async def _emit_boot_notification(self, report: StartupReport) -> None:
        """ENGINE_STARTED — or, on fast unclean respawns, the coalesced ENGINE_CRASHLOOP (§2.2)."""
        kind, restarts, window_s = self._crashloop_track(report.crash_recovered)
        report.crashloop = kind in ("crashloop", "crashloop_silent")
        if kind == "crashloop":
            report.notes.append(f"crashloop_coalesced_restarts={restarts}")
            await self._notify_safe(catalog.engine_crashloop(restarts=restarts, window_s=window_s),
                                    "engine_crashloop")
            return
        if kind == "crashloop_silent":
            # Already alerted for this loop episode — one page per outage, not per respawn (§2.2).
            self._suppress_report_notify = True
            _log.warning("crashloop_boot_silenced", restarts=restarts, window_s=window_s)
            return
        if not self._settings.lifecycle.notify_started:
            return
        await self._notify_safe(
            catalog.engine_started(mode=self._mode.mode().value, version=self._build_version,
                                   crash_recovered=report.crash_recovered),
            "engine_started",
        )

    def _crashloop_track(self, crash_recovered: bool) -> tuple[str, int, int]:
        """Track unclean boots in the private ``boot_history_path`` JSON and classify this boot:
        ``("started", ...)`` normal; ``("crashloop", n, window)`` — the coalescing alert fires now;
        ``("crashloop_silent", ...)`` — inside an already-alerted loop episode. Tracking disabled
        (path None) or a clean prior exit ⇒ always "started"."""
        if self._boot_history_path is None or not crash_recovered:
            return ("started", 0, 0)
        window_s = int(self._settings.lifecycle.crashloop_window_s)
        now = self._clock.now()
        data: dict = {}
        try:
            data = json.loads(self._boot_history_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        boots: list[datetime] = []
        for raw in data.get("unclean_boots", []):
            try:
                ts = datetime.fromisoformat(raw)
            except (TypeError, ValueError):
                continue
            if 0 <= (now - ts).total_seconds() <= window_s:
                boots.append(ts)
        boots.append(now)

        alerted_recently = False
        alert_raw = data.get("last_crashloop_alert_at")
        if alert_raw:
            try:
                alerted_recently = 0 <= (now - datetime.fromisoformat(alert_raw)).total_seconds() <= window_s
            except (TypeError, ValueError):
                alerted_recently = False

        n = len(boots)
        if n >= CRASHLOOP_MIN_BOOTS:
            kind = "crashloop_silent" if alerted_recently else "crashloop"
        else:
            kind = "started"
        out = {
            "unclean_boots": [b.isoformat() for b in boots],
            "last_crashloop_alert_at": now.isoformat() if kind == "crashloop" else alert_raw,
        }
        try:
            self._boot_history_path.parent.mkdir(parents=True, exist_ok=True)
            self._boot_history_path.write_text(json.dumps(out), encoding="utf-8")
        except OSError:  # pragma: no cover - tracking is best-effort; never blocks the boot
            _log.exception("crashloop_history_write_failed")
        return (kind, n, window_s)

    # ----------------------------------------------------------------- warm-up gate (§2.6 step 6)
    async def _apply_warmup_gate(self, report: StartupReport) -> None:
        """§2.6 step 6 (startup): FREEZE-when-not-ready, per coverage class. Startup never LIFTS here
        — the other startup steps own their own freezes, so the composite FROZEN must stand while any
        reason holds; the reopen once coverage is met is the post-login re-trigger's job
        (:meth:`reapply_warmup_gate`).

        2026-09-13 plan change, narrowed 2026-09-17 (§2.6 step-6 addenda): only the REGIME class and
        an UNATTRIBUTABLE blocker freeze entries. An INTRADAY shortfall — one symbol's minute hole
        (2026-09-16 13:36 PTCIL), a market-wide one-bar gap after a reconnect — and a DAILY one
        (2026-09-15 OLAELEC) are now logged and left to the per-candidate rules, which refuse only
        the SYMBOLS that are short."""
        if self._warmup_gate is None:
            report.deferred_steps.append("warmup_gate")
            _log.info("startup_step_deferred", step="warmup_gate")
            return
        status = await self._warmup_gate_status()
        # Young listings excluded from the lookback gate are surfaced whether or not the gate is ready
        # (an exclusion is never silent), so capture them before the ready-path early return.
        report.warmup_young_excluded = list(getattr(status, "young_excluded", []) or [])
        if status is not None and status.ready:
            report.notes.append("warmup_ready")
            return
        blockers = _rendered_blockers(status)
        report.warmup_blockers = blockers
        report.warmup_classes_short = sorted(classify_blockers(blockers))
        if _freezes_entries(blockers):
            report.frozen_reasons.append("warmup_ready")
            await self._freeze_for_warmup(blockers)
            return
        # INTRADAY and/or DAILY only: no risk-state consequence and no WARMUP_FROZEN page (nothing
        # froze), but never silent — ``warmup_classes_short`` rides the OWNER's STARTUP_REPORT body
        # (see :meth:`_emit_report`), not only this log, because a session trading on incomplete
        # coverage with `frozen: none` is exactly the silence the step-5 addendum was written
        # against. The gate and the pre-screen refuse the SHORT SYMBOLS' candidates one by one.
        for cls in (CLASS_INTRADAY, CLASS_DAILY):
            if cls in report.warmup_classes_short:
                report.notes.append(f"warmup_{cls}_not_ready")
        _log.warning("warmup_short_not_frozen", classes=report.warmup_classes_short,
                     blockers=blockers[:8], frozen=False,
                     note="regime coverage met — entries stay open; per-symbol shortfalls are "
                          "refused at the gate")

    async def _warmup_gate_status(self):
        """Query the injected warm-up gate; a raise means coverage cannot be VERIFIED ⇒ treated as
        missing (R6-style: never trade on data you could not confirm). ``None`` ⇒ unverifiable."""
        try:
            return await self._warmup_gate.status()
        except Exception:  # noqa: BLE001 - coverage that cannot be VERIFIED is treated as missing
            _log.exception("warmup_gate_check_failed")
            return None

    async def _freeze_for_warmup(self, blockers: list[str]) -> bool:
        """Never trade on thin data: FROZEN-for-entries via the risk-state setter + WARMUP_FROZEN alert
        (§2.6 step 6). Shared by startup step 6 and the post-login reapply, and called ONLY when a
        freezing class (regime/unattributable) is short — the caller owns that decision. Entries
        reopen only once coverage is met (the gate is re-checked before entries open). Returns True if
        this call transitioned the state to FROZEN (it was not already)."""
        froze = False
        if not self._kill.is_killed():
            before = self._mode.risk_state()
            await self._freeze("warmup_ready", "warmup_ready")
            froze = before != RiskState.FROZEN and self._mode.risk_state() == RiskState.FROZEN
        # The class summary leads the alert (its own field, never smuggled into ``blockers``): which
        # coverage class froze entries is the first thing the owner needs, and an intraday line riding
        # along in the same freeze must not read as the cause.
        await self._notify_safe(
            catalog.warmup_frozen(blockers=blockers, classes=sorted(classify_blockers(blockers))),
            "warmup_frozen",
        )
        return froze

    async def reapply_warmup_gate(self) -> WarmupReapply:
        """Post-login re-evaluation of the §2.6 step-6 warm-up gate — the RE-TRIGGER half of the
        cold-start-family fix. Uses the SAME injected :class:`~engine.ops.warmup.WarmupGate` and the
        SAME risk-state seam as startup (never a direct bypass): FREEZE while a FREEZING class
        (regime / unattributable) is still short, and LIFT the warm-up FROZEN-for-entries once those
        classes are covered — conservatively (see :meth:`_maybe_lift_warmup_freeze`), and since
        2026-09-13/2026-09-17 even with the INTRADAY or DAILY class still short. Called by
        :class:`~engine.ops.post_login.PostLoginRecovery`."""
        if self._warmup_gate is None:
            _log.info("warmup_reapply_deferred", reason="no warmup gate wired")
            return WarmupReapply(ready=False, outcome="deferred")
        status = await self._warmup_gate_status()
        young = list(getattr(status, "young_excluded", []) or [])
        ready = bool(status is not None and status.ready)
        blockers = [] if ready else _rendered_blockers(status)
        classes = sorted(classify_blockers(blockers))
        # 2026-09-13/2026-09-17: the LIFT is owed to the freezing classes alone — one symbol's
        # intraday hole (09-04 11:09, 09-09 14:47, 09-16 13:36) or daily hole (09-15 OLAELEC) must
        # not hold the whole book frozen for the rest of the session. The short blockers still ride
        # the result, so post-login detail keeps naming them.
        if ready or not _freezes_entries(blockers):
            lifted, why = await self._maybe_lift_warmup_freeze()
            outcome = f"ready_{why}" if ready else f"short_{why}"
            return WarmupReapply(ready=ready, blockers=blockers, young_excluded=young,
                                 classes_short=classes, lifted=lifted, outcome=outcome)
        froze = await self._freeze_for_warmup(blockers)
        return WarmupReapply(
            ready=False, blockers=blockers, young_excluded=young, classes_short=classes,
            froze=froze, outcome="frozen",
        )

    async def _maybe_lift_warmup_freeze(self) -> tuple[bool, str]:
        """Lift the warm-up FROZEN-for-entries once coverage is met — SAFELY (§2.6 step-6 reopen).

        Conservative by construction: never overrides the kill switch, never clears a CLOSE_ONLY /
        KILLED latch (those re-arm only on owner action, R3/R5), and re-runs the CHEAP self-test
        preconditions (no NTP, no catch-up) so a still-standing secrets / clock / trade-window /
        integrity / token freeze is respected — the warm-up reopen must never clear a warranted freeze.
        Multi-reason data-freshness arbitration is the Phase-2 gate's job (risk/mode.py: "most-
        restrictive-wins / latch logic is the gate's"). Returns ``(lifted, why)``."""
        if self._kill.is_killed():
            return False, "kill_held"
        state = self._mode.risk_state()
        if state == RiskState.NORMAL:
            return False, "already_normal"
        if state != RiskState.FROZEN:
            return False, "latched"   # CLOSE_ONLY / KILLED — owner re-arm only (R3/R5)
        st = await self._selftest.run(check_skew=False, include_freshness=False)
        if st.needs_login or st.frozen_reasons:
            _log.info("warmup_lift_held", needs_login=st.needs_login, other_frozen=st.frozen_reasons)
            return False, "other_freeze"
        if self._latch is not None:
            # Clear ONLY the causes this lifecycle owns and has just re-verified; the ledger resolves
            # the rest — an owner_pause, rejection-storm, floor rung or daily-loss cause that is still
            # active keeps the state (2026-07-28 review: the previous direct NORMAL write erased any
            # standing cause, defeating /pause_entries and a floor rung the selftest itself applied).
            await self._latch.clear_cause("warmup_ready", Actor.RISK_GATE)
            await self._latch.clear_cause("startup_selftest", Actor.RISK_GATE)
            after = self._mode.risk_state()
            lifted = after == RiskState.NORMAL
            _log.warning("warmup_freeze_lifted" if lifted else "warmup_lift_partial",
                         state=after.value)
            return lifted, "lifted" if lifted else "other_causes_hold"
        await self._mode.set_risk_state(RiskState.NORMAL, "warmup_ready_lifted", Actor.RISK_GATE)
        _log.warning("warmup_freeze_lifted")
        return True, "lifted"

    # ----------------------------------------------------------------- notifications
    async def _notify_safe(self, msg: CatalogMessage, what: str) -> None:
        if self._notify is None:
            return
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - a lifecycle notification must never crash the boot/stop
            _log.exception("lifecycle_notify_failed", what=what)

    async def _emit_lifecycle_stopped(self, *, reason: str, open_positions: int) -> None:
        if not self._settings.lifecycle.notify_planned_stop:
            return
        # Best-effort: a failed send never blocks exit (§2.2 — the owner reconciles it against the
        # next STARTUP_REPORT; the watchdog is deliberately silent for a clean stop).
        await self._notify_safe(
            catalog.engine_stopped(reason=reason, open_positions=open_positions), "engine_stopped"
        )

    async def _emit_report(self, report: StartupReport) -> None:
        _log.warning(
            "startup_report",
            mode=report.sticky_mode, risk_state=report.sticky_risk_state, killed=report.killed,
            needs_login=report.needs_login, integrity_ok=report.integrity_ok,
            crash_recovered=report.crash_recovered, off_duration_s=report.off_duration_s,
            jobs_caught_up=report.jobs_caught_up, jobs_failed=report.jobs_failed,
            frozen=report.frozen_reasons, warmup_blockers=report.warmup_blockers,
            warmup_classes_short=report.warmup_classes_short,
            warmup_young_excluded=report.warmup_young_excluded,
            deferred=report.deferred_steps,
        )
        if self._suppress_report_notify:
            return   # coalesced crash-loop boot (§2.2): the single ENGINE_CRASHLOOP page stands
        msg = catalog.startup_report(
            mode=report.sticky_mode, risk_state=report.sticky_risk_state, killed=report.killed,
            needs_login=report.needs_login, integrity_ok=report.integrity_ok,
            crash_recovered=report.crash_recovered, prior_state=report.prior_state,
            frozen_reasons=report.frozen_reasons, deferred_steps=report.deferred_steps,
            # 2026-09-13: an INTRADAY-only shortfall adds NO ``frozen:`` reason and sends no
            # WARMUP_FROZEN page, so without this line the owner's only boot-time notice that the
            # session runs on incomplete minute coverage would be a structured log they never see.
            warmup_classes_short=report.warmup_classes_short,
            warmup_blockers=report.warmup_blockers,
        )
        # Prefer the typed notification sink so the owner gets a clean STARTUP_REPORT; fall back to
        # the raw alert string only if no notify sink is wired (message carries its own severity).
        # off-duration + per-job catch-up detail already reached the owner via CATCHUP_REPORT (§2.6).
        if self._notify is not None:
            await self._notify_safe(msg, "startup_report")
        elif self._alert is not None:
            await self._alert(msg.severity, msg.body)

    # ----------------------------------------------------------------- helpers
    def _open_positions_count(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM positions WHERE state='OPEN' AND origin IN ('platform','recommended')"
        ).fetchone()
        return int(row["n"]) if row else 0

    async def _run_hook(self, name: str, hook: Hook | None, report: StartupReport) -> None:
        if hook is None:
            report.deferred_steps.append(name)
            _log.info("startup_step_deferred", step=name)
            return
        try:
            await hook()
            report.notes.append(f"{name}_ok")
        except Exception:  # noqa: BLE001 - a recovery step must not crash startup; log + alert
            _log.exception("startup_step_failed", step=name)
            report.notes.append(f"{name}_failed")
            if self._alert is not None:
                await self._alert("critical", f"startup step failed: {name}")
