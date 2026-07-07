"""Session lifecycle: every-startup recovery + catch-up, and the shutdown guard (§2.6).

The engine is up only during active periods and may be deliberately stopped in between (§2.6). The
defining invariant: **every startup is a full recovery** — scheduled or manual, after a clean stop or a
crash, after a gap of minutes or days — and the platform never breaks because it was off. Capital
protection is broker-resident throughout (R3), so being off is safe.

This module orchestrates the §2.6 sequence. The steps that exist in Phase 0 are real (read sticky
state → seed trade window → self-test → apply the §2.4 integrity rule → honor kill state → alert a
startup report). The broker-dependent steps (reconcile, overdue-MIS square-off, data-gap backfill,
missed-job catch-up, warm-up gate, ticker resume) are injected as optional hooks and wired by Phase 1/3;
until then they are logged as deferred so the recovery skeleton is visible and testable from day one.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import date

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
from engine.ops.selftest import SelfTest, SelfTestReport

_log = get_logger("engine.ops.lifecycle")

Hook = Callable[[], Awaitable[None]]
AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)
LifecycleNotify = Callable[[CatalogMessage], Awaitable[None]]   # typed process-lifecycle send (§2.2)


class StartupReport(BaseModel):
    started_at: str
    sticky_mode: str
    sticky_risk_state: str
    killed: bool
    needs_login: bool
    integrity_ok: bool
    crash_recovered: bool = False        # prior run exited uncleanly (state RUNNING/STOPPING) — §2.6 step 0
    prior_state: str | None = None       # engine_lifecycle.state read before this run marked RUNNING
    frozen_reasons: list[str] = Field(default_factory=list)
    jobs_caught_up: list[str] = Field(default_factory=list)
    deferred_steps: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class CatchUpRunner:
    """Watermark-driven missed-job catch-up (§2.6). Phase-0: watermark helpers + a classification skeleton.

    On every startup, any scheduled job whose fire-time fell in the off-window and is not recorded done
    in ``job_runs`` is caught up, in dependency order, by class (safety/deadline-critical → idempotent
    run-latest → date-keyed backfill). Phase 1 wires the actual jobs; the watermark machinery is real now.
    """

    def __init__(self, conn: sqlite3.Connection, clock: Clock, calendar: NSECalendar) -> None:
        self._conn = conn
        self._clock = clock
        self._calendar = calendar

    def record_run(self, job_id: str, run_for: date, status: str = "success") -> None:
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                """
                INSERT INTO job_runs (job_id, run_for_date, last_success_at, last_attempt_at, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id, run_for_date) DO UPDATE SET
                    last_success_at=CASE WHEN excluded.status='success' THEN excluded.last_success_at ELSE job_runs.last_success_at END,
                    last_attempt_at=excluded.last_attempt_at,
                    status=excluded.status
                """,
                (job_id, run_for.isoformat(), now if status == "success" else None, now, status),
            )

    def was_run(self, job_id: str, run_for: date) -> bool:
        row = self._conn.execute(
            "SELECT status FROM job_runs WHERE job_id=? AND run_for_date=?",
            (job_id, run_for.isoformat()),
        ).fetchone()
        return bool(row and row["status"] == "success")

    async def catch_up(self) -> list[str]:
        """Run missed jobs in dependency order (§2.6). Phase-0 skeleton: returns the (currently empty)
        list of caught-up job ids; Phase 1 registers the real jobs against this watermark machinery."""
        _log.info("catch_up_skeleton", note="job registry wired in Phase 1 (§2.6)")
        return []


class SessionLifecycle:
    """Drives the §2.6 every-startup recovery & catch-up sequence and the shutdown guard."""

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
        reconcile_hook: Hook | None = None,
        overdue_squareoff_hook: Hook | None = None,
        backfill_hook: Hook | None = None,
        warmup_gate_hook: Hook | None = None,
        ticker_resume_hook: Hook | None = None,
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
        self._reconcile = reconcile_hook
        self._overdue_squareoff = overdue_squareoff_hook
        self._backfill = backfill_hook
        self._warmup_gate = warmup_gate_hook
        self._ticker_resume = ticker_resume_hook

    async def startup(self, *, check_skew: bool = True) -> StartupReport:
        # 0) Process-lifecycle boot (§2.6 step 0 / §2.2): detect an unclean prior exit, atomically mark
        #    the run RUNNING (+ pid/started_at/liveness), and emit ENGINE_STARTED BEFORE the recovery
        #    body so the owner knows the engine is alive even if catch-up takes a while.
        prior_state = self._begin_run()
        crash_recovered = prior_state in ("RUNNING", "STOPPING")
        await self._emit_lifecycle_started(mode=self._mode.mode().value, crash_recovered=crash_recovered)

        report = StartupReport(
            started_at=self._clock.now().isoformat(),
            sticky_mode=self._mode.mode().value,
            sticky_risk_state=self._mode.risk_state().value,
            killed=self._kill.is_killed(),
            needs_login=False,
            integrity_ok=True,
            crash_recovered=crash_recovered,
            prior_state=prior_state,
        )
        if crash_recovered:
            report.notes.append(f"crash_recovered_prior_state={prior_state}")

        # 1) Sticky kill/mode/trade-window state is read above (R10) — log it.
        _log.info("startup_sticky_state", mode=report.sticky_mode, risk_state=report.sticky_risk_state,
                  killed=report.killed, crash_recovered=crash_recovered)

        # 1b) Seed the trade window from settings on first run (§2.6); sticky thereafter.
        seed = TradeWindow(
            start=self._settings.trade_window.start_ist,
            end=self._settings.trade_window.end_ist,
            squareoff_buffer_min=self._settings.trade_window.squareoff_buffer_min,
        )
        if self._mode.seed_trade_window_if_absent(seed):
            report.notes.append("trade_window_seeded")

        # 2) Self-test (D11) — the gate on whether entries may open.
        st: SelfTestReport = await self._selftest.run(check_skew=check_skew)
        report.needs_login = st.needs_login
        report.frozen_reasons = list(st.frozen_reasons)

        # Apply the §2.4 single integrity rule: a protected-store failure with a FLAT book ⇒ FROZEN;
        # with a LIVE book (or at runtime) ⇒ kill. Phase 0 has no live book yet, so this is FROZEN.
        integrity_failed = any(c.name.startswith("protected_store:") and c.status.value == "FAIL" for c in st.checks)
        report.integrity_ok = not integrity_failed
        if integrity_failed:
            if self._open_positions_count() == 0:
                await self._mode.set_risk_state(RiskState.FROZEN, "protected_store_integrity", Actor.RISK_GATE)
                report.notes.append("integrity_failed_flat_book_frozen")
            else:
                await self._kill.trigger("protected_store_integrity_live_book", actor=Actor.RISK_GATE, flatten=True)
                report.killed = True

        # Other FROZEN-implying causes (secrets/clock/window/token) ⇒ FROZEN entries until cleared.
        if report.frozen_reasons and not self._kill.is_killed():
            await self._mode.set_risk_state(RiskState.FROZEN, ",".join(report.frozen_reasons), Actor.RISK_GATE)

        # 2c) The continuous equity halt-ladder re-eval + day-scoped counter rebuild (§2.6) — Phase 2.
        report.deferred_steps.append("equity_halt_ladder_reeval (§2.6 — Phase 2)")

        # 3) Reconcile vs broker = truth (R5) — Phase 3.
        await self._run_hook("reconcile", self._reconcile, report)
        # 3b) Overdue-MIS startup square-off (§2.6 step 3) — Phase 3.
        await self._run_hook("overdue_mis_squareoff", self._overdue_squareoff, report)
        # 4) Data-gap backfill (§2.6 step 4) — Phase 1.
        await self._run_hook("data_gap_backfill", self._backfill, report)
        # 5) Missed-job catch-up (§2.6 step 5).
        report.jobs_caught_up = await self._catch_up.catch_up()
        # 6) Cold-start warm-up gate (§2.6 step 6) — Phase 1.
        await self._run_hook("warmup_gate", self._warmup_gate, report)
        # 7) Resume ticker (WARMING) + re-arm schedules (§2.6 step 7) — Phase 1.
        await self._run_hook("ticker_resume", self._ticker_resume, report)

        # 7b) Alert the owner with the startup/recovery report (§2.6).
        await self._emit_report(report)
        return report

    async def shutdown(self, *, owner_override: bool = False, reason: str = "owner") -> None:
        """Clean/planned stop (§2.2/§2.6/§3.5.3): draw the planned-vs-crash line, run the shutdown guard,
        commit the clean-stop state, and emit ENGINE_STOPPED as the last act.

        Ordering is load-bearing (§2.2): set ``state='STOPPING'`` FIRST (a crash while STOPPING stays
        STOPPING ⇒ watchdog ``ENGINE_DOWN``, never mislabelled clean), run the guard, THEN commit
        ``state='STOPPED'`` + ``last_clean_stop_at`` (the point of no return), THEN best-effort send
        ENGINE_STOPPED — a failed send never blocks exit.

        Phase-3 guard behaviour (documented): if a platform MIS is open before window-end completion,
        flatten it first (run_window_squareoff, verify flat) OR require ``owner_override`` accepting the
        broker 15:25 backstop; cancel + verify every working/unfilled entry order; verify every open CNC
        has a live resting GTT (place/repair PROTECTION_PENDING/FAILED) — or block the stop.
        """
        self._mark_stopping()   # FIRST — draws the planned-vs-crash line (§2.2)
        open_count = self._open_positions_count()
        if open_count and not owner_override:
            _log.warning("shutdown_guard_open_positions", count=open_count,
                         note="Phase 3 flattens MIS / verifies CNC GTTs before exit (§2.6)")
        _log.info("shutdown", owner_override=owner_override, open_positions=open_count, reason=reason)
        self._mark_stopped()    # point of no return — clean stop committed
        await self._emit_lifecycle_stopped(reason=reason, open_positions=open_count)

    # ----------------------------------------------------------------- lifecycle state (§2.2/§4.2)
    def _begin_run(self) -> str | None:
        """§2.6 step 0: read the prior ``engine_lifecycle.state`` (for crash detection), then atomically
        mark this run RUNNING with fresh pid/started_at/last_alive_at. Returns the prior state string."""
        now = self._clock.now().isoformat()
        pid = os.getpid()
        with transaction(self._conn):
            row = self._conn.execute("SELECT state FROM engine_lifecycle WHERE id=1").fetchone()
            prior = row["state"] if row else None
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
        return prior

    def _mark_stopping(self) -> None:
        with transaction(self._conn):
            self._conn.execute("UPDATE engine_lifecycle SET state='STOPPING' WHERE id=1")

    def _mark_stopped(self) -> None:
        now = self._clock.now().isoformat()
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE engine_lifecycle SET state='STOPPED', last_clean_stop_at=? WHERE id=1", (now,)
            )

    async def _emit_lifecycle_started(self, *, mode: str, crash_recovered: bool) -> None:
        if self._notify is None or not self._settings.lifecycle.notify_started:
            return
        try:
            await self._notify(catalog.engine_started(
                mode=mode, version=self._build_version, crash_recovered=crash_recovered))
        except Exception:  # noqa: BLE001 - a lifecycle notification must never crash the boot
            _log.exception("engine_started_notify_failed")

    async def _emit_lifecycle_stopped(self, *, reason: str, open_positions: int) -> None:
        if self._notify is None or not self._settings.lifecycle.notify_planned_stop:
            return
        try:
            await self._notify(catalog.engine_stopped(reason=reason, open_positions=open_positions))
        except Exception:  # noqa: BLE001 - best-effort; a failed send never blocks exit (watchdog backstop)
            _log.exception("engine_stopped_notify_failed")

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

    async def _emit_report(self, report: StartupReport) -> None:
        _log.warning(
            "startup_report",
            mode=report.sticky_mode, risk_state=report.sticky_risk_state, killed=report.killed,
            needs_login=report.needs_login, integrity_ok=report.integrity_ok,
            crash_recovered=report.crash_recovered,
            frozen=report.frozen_reasons, deferred=report.deferred_steps,
        )
        msg = catalog.startup_report(
            mode=report.sticky_mode, risk_state=report.sticky_risk_state, killed=report.killed,
            needs_login=report.needs_login, integrity_ok=report.integrity_ok,
            crash_recovered=report.crash_recovered, prior_state=report.prior_state,
            frozen_reasons=report.frozen_reasons, deferred_steps=report.deferred_steps,
        )
        # Prefer the typed notification sink so the owner gets a clean STARTUP_REPORT; fall back to the
        # raw alert string only if no notify sink is wired (message carries its own severity).
        if self._notify is not None:
            await self._notify(msg)
        elif self._alert is not None:
            await self._alert(msg.severity, msg.body)
