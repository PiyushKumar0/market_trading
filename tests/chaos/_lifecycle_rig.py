"""Shared scenario rig for the §9.4 lifecycle / stop-start chaos cases (15, 16, 17, 19, 22, 23).

Not a test module (no ``test_`` prefix, so pytest does not collect it). It composes the REAL engine
components the way ``engine.ops.main.run`` wires them — ``SessionLifecycle`` + ``SelfTest`` + the
``RiskStateLatch`` cause ledger, a ``CatchUpRunner`` over the real ``build_job_registry`` inventory
(``deferred=POST_ARM_JOB_IDS``) with ``tick_compact`` in its own ``CompactionLane``, the
dedicated-thread ``HeartbeatWriter``, the ``Scheduler`` armed by
``_arm_registry_jobs`` and started by ``start_scheduler_and_fire_post_arm``, the real
``_snapshot_backup`` shutdown hook, optionally a ``MarketStore`` + ``BarBuilder`` on the ``EventBus``
— and drives the REAL ``scripts/watchdog.py`` IO shell (``read_snapshot`` → ``run_tick`` →
``save_debounce``) against the same on-disk ``state.db``.

Each :class:`EngineProcess` is one engine process: its own sqlite connection and component graph over
the SHARED temp data dir, exactly as consecutive boots of ``python -m engine.ops.main`` share
``data/``. A clean stop replays main.py's teardown order; :meth:`EngineProcess.kill_9` is a process
death (nothing after it runs).

Only external boundaries are faked:

* job BODIES (Kite / NSE / LLM work) — recorded per ``(job_id, run_for)`` instead of performed;
* Telegram — the notify/alert sinks and the watchdog's direct Bot-API sender record messages;
* the DPAPI secrets store — :class:`FakeSecrets`;
* the OS process table the watchdog probes (:class:`ProcessTable`) — every rig "process" runs inside
  the pytest process, so liveness is tracked per boot/exit instead of by pid.

Everything lives under ``tmp_path`` (``MT_DATA_DIR`` points there; the protected-store config dir is
a temp copy) — the live ``data/state.db`` / ``data/market.duckdb`` / ``config/`` are never written.
Time is a :class:`MovableTime` shared by every Clock in the rig, so a multi-day off-gap costs nothing.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import time as time_mod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.db import connect
from engine.core.enums import Actor, RiskState
from engine.core.eventbus import EventBus
from engine.core.migrations import apply_migrations
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.core.types import OwnerConfirmation, TradeWindow
from engine.notify.catalog import CatalogMessage, MessageKind
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.jobs import JOB_TICK_COMPACT, CatchUpResult, CatchUpRunner
from engine.ops.lifecycle import SessionLifecycle, StartupReport
from engine.ops.main import (
    _SHUTDOWN_BUS_DRAIN_S,
    DEFER_POST_ARM_JOBS,
    PHASE1_JOB_IDS,
    PHASE2_JOB_IDS,
    POST_ARM_JOB_IDS,
    CompactionLane,
    _arm_registry_jobs,
    _catchup_sweep_once,
    _snapshot_backup,
    build_job_registry,
    cancel_post_arm,
    start_scheduler_and_fire_post_arm,
)
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.risk.causes import RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True)

#: Heartbeat write cadence inside the rig (the live 20 s would make every clock jump look stale; the
#: rig instead re-syncs the beat to the movable clock before each watchdog poll, see heartbeat_synced).
HEARTBEAT_INTERVAL_S = 0.02

#: Bound on every "wait for a background thing" loop in the rig — a regression fails, never hangs.
WAIT_BOUND_S = 5.0

#: Kinds that would be FALSE alarms on a clean stop/restart (§9.4 case 15 "no spurious FEED_STALE/
#: incident alerts", case 22 (a) "no false alarm across the idle gap").
INCIDENT_KINDS = frozenset({
    MessageKind.FEED_STALE, MessageKind.FEED_DEGRADED, MessageKind.FEED_WEDGED,
    MessageKind.ENGINE_CRASHLOOP, MessageKind.DATA_FRESHNESS_FROZEN, MessageKind.KILL,
    MessageKind.LIMIT_BREACH,
})


# --------------------------------------------------------------------------- scripts/watchdog.py
_WATCHDOG_PATH = Path(__file__).resolve().parents[2] / "scripts" / "watchdog.py"


def load_watchdog() -> ModuleType:
    """Import ``scripts/watchdog.py`` (a bare script, not a package module) once per session."""
    name = "mt_watchdog_chaos"
    mod = sys.modules.get(name)
    if mod is None:
        spec = importlib.util.spec_from_file_location(name, _WATCHDOG_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- boundaries
class MovableTime:
    """The single time source behind every Clock in the rig (``Clock(time_source=...)``)."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at

    def set(self, at: datetime) -> None:
        assert at >= self.at, f"time never runs backwards in a scenario ({at} < {self.at})"
        self.at = at


class FakeMonotonic:
    """Injectable ``time.monotonic`` for the counted stop handler (``_make_stop_handler``)."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class FakeSecrets:
    """DPAPI/Credential-Manager boundary: every startup-required secret present."""

    def has(self, k: str) -> bool:
        return k in REQUIRED_AT_STARTUP

    def get_optional(self, k: str) -> str | None:
        return "x" if k in REQUIRED_AT_STARTUP else None

    def missing_required(self) -> list[str]:
        return []


class ProcessTable:
    """OS process-liveness boundary for the out-of-band watchdog (its ``is_pid_alive`` seam).

    Every rig process shares the pytest pid, so liveness is tracked per boot: ``spawn`` when a
    process starts, ``reap`` when it exits (clean or killed)."""

    def __init__(self) -> None:
        self._alive: set[int] = set()

    def spawn(self, pid: int) -> None:
        self._alive.add(pid)

    def reap(self, pid: int) -> None:
        self._alive.discard(pid)

    def __call__(self, pid: int | None) -> bool:
        return pid is not None and pid in self._alive


class FakeTicker:
    """TickerSupervisor boundary (the Kite websocket child): only the state the engine reads."""

    def __init__(self) -> None:
        self.state = "STOPPED"

    def health(self) -> SimpleNamespace:
        return SimpleNamespace(state=self.state, last_tick_age_s=None)

    async def resume(self) -> None:   # ticker_resume_hook: a fresh spawn enters WARMING (§3.2.12)
        self.state = "WARMING"

    async def stop(self) -> None:
        self.state = "STOPPED"


@dataclass(frozen=True)
class Sent:
    at: datetime
    msg: CatalogMessage


@dataclass(frozen=True)
class JobCall:
    job_id: str
    run_for: date | None
    at: datetime


@dataclass(frozen=True)
class Page:
    """One message the out-of-band watchdog sent through its direct Telegram Bot-API sender."""

    at: datetime
    text: str


# --------------------------------------------------------------------------- the environment
class RigEnv:
    """What survives across engine processes: the temp data dir (state.db, backups, the watchdog's
    private debounce file), the movable clock, the OS process table, and the owner's Telegram inbox."""

    def __init__(self, tmp_path: Path, monkeypatch: Any, start: datetime) -> None:
        self.data_dir = tmp_path / "data"
        monkeypatch.setenv("MT_DATA_DIR", str(self.data_dir))
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        self.settings = load_settings()
        # Hermetic guard: every path the rig (and the real hooks it wires) writes must be under tmp.
        for p in (self.settings.sqlite_path(), self.settings.duckdb_path(), self.settings.backups_dir()):
            assert self.data_dir in p.parents, f"rig path escaped tmp: {p}"
        self.time = MovableTime(start)
        self.clock = Clock(time_source=self.time)
        # Protected-store config: a temp copy, never the repo's config/ (registration writes hashes).
        self.protected_cfg = tmp_path / "protected_cfg"
        self.protected_cfg.mkdir()
        (self.protected_cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
        (self.protected_cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
        self.protected_registered = False
        self.processes = ProcessTable()
        self.sent: list[Sent] = []
        self.alerts: list[tuple[datetime, str, str]] = []
        self.job_calls: list[JobCall] = []
        self.pages: list[Page] = []
        self.force_kills: list[int] = []
        self.watchdog_state_path = self.data_dir / "watchdog_state.json"
        self._trading_day = load_watchdog().make_trading_day_fn(config_dir() / "calendar")

    # ---- clock
    def at(self, when: datetime) -> None:
        self.time.set(when)

    # ---- Telegram boundary (the engine's notify/alert sinks)
    async def notify(self, msg: CatalogMessage) -> None:
        self.sent.append(Sent(self.clock.now(), msg))

    async def alert(self, severity: str, message: str) -> None:
        self.alerts.append((self.clock.now(), severity, message))

    def messages(self, *, since: int = 0) -> list[CatalogMessage]:
        return [s.msg for s in self.sent[since:]]

    def kinds(self, *, since: int = 0) -> list[str]:
        return [str(s.msg.kind) for s in self.sent[since:]]

    # ---- job bodies (Kite / NSE / LLM boundary): recorded, not performed
    def job_fns(self) -> dict[str, Callable[..., Awaitable[None]]]:
        """A runner for every Phase-1 + Phase-2 registry id. DATE_KEYED runs receive ``run_for``;
        the others take no argument — ``*args`` serves both call shapes."""

        def recorder(job_id: str) -> Callable[..., Awaitable[None]]:
            async def run(*args: Any) -> None:
                self.job_calls.append(JobCall(job_id, args[0] if args else None, self.clock.now()))

            return run

        return {job_id: recorder(job_id) for job_id in (*PHASE1_JOB_IDS, *PHASE2_JOB_IDS)}

    def calls(self, *, since: int = 0) -> list[tuple[str, date | None]]:
        return [(c.job_id, c.run_for) for c in self.job_calls[since:]]

    # ---- the out-of-band watchdog (scripts/watchdog.py), one Scheduled-Task tick
    def watchdog_poll(self) -> dict:
        """One ``scripts/watchdog.py`` tick exactly as its ``main()`` runs it — settings-derived config,
        strictly read-only ``read_snapshot`` of state.db, the private debounce file, the real calendar
        YAML predicate — with the Telegram sender and the OS probes/killer as the faked boundary."""
        wd = load_watchdog()
        lc = self.settings.lifecycle
        cfg = wd.WatchdogConfig(
            down_stale_s=float(lc.down_stale_s),
            catchup_grace_s=float(lc.catchup_grace_s),
            start_grace_s=float(lc.start_grace_s),
            active_period_starts=tuple(lc.active_period_starts),
        )
        now = self.clock.now()
        snap = wd.read_snapshot(self.settings.sqlite_path())
        debounce = wd.load_debounce(self.watchdog_state_path)
        new_debounce, summary = wd.run_tick(
            snap, cfg, debounce, now,
            is_pid_alive=self.processes, is_trading_day=self._trading_day,
            send=self._page, kill=self._force_kill,
        )
        if new_debounce != debounce:
            wd.save_debounce(self.watchdog_state_path, new_debounce)
        return summary

    def _page(self, text: str) -> bool:
        self.pages.append(Page(self.clock.now(), text))
        return True

    def _force_kill(self, pid: int) -> bool:
        self.force_kills.append(pid)
        self.processes.reap(pid)
        return True

    def lifecycle_row(self) -> dict:
        """The engine_lifecycle row as a fresh reader sees it (own read-only connection)."""
        import sqlite3

        c = sqlite3.connect(f"file:{self.settings.sqlite_path().as_posix()}?mode=ro", uri=True)
        try:
            row = c.execute(
                "SELECT state, pid, last_alive_at, started_at, last_clean_stop_at FROM engine_lifecycle WHERE id=1"
            ).fetchone()
        finally:
            c.close()
        keys = ("state", "pid", "last_alive_at", "started_at", "last_clean_stop_at")
        return dict(zip(keys, row, strict=True))


# --------------------------------------------------------------------------- one engine process
class EngineProcess:
    """One boot of the engine, composed like ``engine.ops.main.run`` (see the module docstring)."""

    def __init__(
        self,
        env: RigEnv,
        *,
        ticker: FakeTicker | None = None,
        market_store: bool = False,
        bar_builder: bool = False,
        warmup_gate_factory: Callable[[EngineProcess], Any] | None = None,
        backfill_hook_factory: Callable[[EngineProcess], Callable[[], Awaitable[None]]] | None = None,
    ) -> None:
        self.env = env
        s, clock = env.settings, env.clock
        self.pid = os.getpid()
        self.conn = connect(s.sqlite_path())
        apply_migrations(self.conn)
        window_seed = TradeWindow(
            start=s.trade_window.start_ist, end=s.trade_window.end_ist,
            squareoff_buffer_min=s.trade_window.squareoff_buffer_min,
        )
        self.calendar = NSECalendar(
            config_dir() / "calendar", clock,
            strict=(s.env == "prod"), sqlite_conn=self.conn, window_seed=window_seed,
        )
        self.bus = EventBus()
        self.protected_store = ProtectedStore(env.protected_cfg, self.conn, clock)
        if not env.protected_registered:
            self.protected_store.register_initial("limits.yaml", OWNER_OK)
            self.protected_store.register_initial("envelope.yaml", OWNER_OK)
            env.protected_registered = True
        self.mode = ModeManager(self.conn, clock, self.bus, self.calendar)
        self.latch = RiskStateLatch(self.conn, clock, self.mode)
        self.kill = KillSwitch(self.conn, clock, self.bus)

        # --- market data (optional): main.py builds MarketStore.from_settings + BarBuilder on "tick"
        self.store = None
        self.bar_builder = None
        if market_store or bar_builder:
            from engine.marketdata.store import MarketStore

            self.store = MarketStore.from_settings(s, clock)
            self.store.open()
        if bar_builder:
            from engine.marketdata.bar_builder import BarBuilder

            self.bar_builder = BarBuilder(self.store, clock, self.bus, notify=env.notify)
            self.bus.subscribe("tick", self.bar_builder.on_tick_event)

        async def freeze_entries(reason: str) -> None:        # mirrors main.py freeze_entries
            if not self.kill.is_killed():
                await self.latch.set_cause(reason, RiskState.FROZEN, reason, Actor.RISK_GATE)

        async def clear_entries_cause(reason: str) -> None:   # mirrors main.py clear_entries_cause
            await self.latch.clear_cause(reason, Actor.RISK_GATE)

        self.registry = build_job_registry(s, env.job_fns(), calendar=self.calendar)
        self.catch_up = CatchUpRunner(
            self.conn, clock, self.calendar, self.registry.select(lambda sp: sp.job_id != JOB_TICK_COMPACT),
            freeze=freeze_entries, notify=env.notify, clear=clear_entries_cause,
            deferred=POST_ARM_JOB_IDS if DEFER_POST_ARM_JOBS else (),
        )
        self.compaction_lane = CompactionLane(
            CatchUpRunner(self.conn, clock, self.calendar,
                          self.registry.select(lambda sp: sp.job_id == JOB_TICK_COMPACT), notify=env.notify),
            clock, self.calendar,
        )
        self.warmup_gate = warmup_gate_factory(self) if warmup_gate_factory is not None else None
        self.self_test = SelfTest(
            conn=self.conn, clock=clock, settings=s, secrets=FakeSecrets(),
            protected_store=self.protected_store, kill_switch=self.kill, mode_manager=self.mode,
            session_manager=None, catch_up=self.catch_up, warmup_gate=self.warmup_gate,
            latch=self.latch, calendar=self.calendar,
        )
        self.heartbeat = HeartbeatWriter(s.sqlite_path(), clock, interval_s=HEARTBEAT_INTERVAL_S)
        self.scheduler = Scheduler(clock, self.calendar)
        self.ticker = ticker
        #: Test seam: runs at the top of the shutdown backup hook — where a repeat stop signal landed
        #: in every 2026-09-02..23 ``nssm stop`` (``stop_forced`` mid shutdown-backup).
        self.during_backup: Callable[[], None] | None = None
        self.backups_written: list[Path] = []

        async def backup_hook() -> None:                      # mirrors main.py backup_hook
            if self.during_backup is not None:
                self.during_backup()
            try:
                await _snapshot_backup(self.conn, s, clock)
                self.backups_written = sorted(s.backups_dir().glob("state-*.db"))
            except Exception:  # noqa: BLE001 - best-effort backup never blocks a clean stop (main.py)
                pass

        async def ticker_resume_hook() -> None:
            if self.ticker is not None:
                await self.ticker.resume()

        #: §2.6 step-4 hook; kept on the process because the post-login re-trigger re-runs the SAME one.
        self.backfill_hook = backfill_hook_factory(self) if backfill_hook_factory is not None else None
        self.lifecycle = SessionLifecycle(
            conn=self.conn, clock=clock, calendar=self.calendar, settings=s,
            mode_manager=self.mode, kill_switch=self.kill, self_test=self.self_test,
            catch_up=self.catch_up, alert=env.alert, notify=env.notify, build_version="chaos-rig",
            heartbeat=self.heartbeat, warmup_gate=self.warmup_gate, latch=self.latch,
            boot_history_path=env.data_dir / "lifecycle_boots.json",
            backfill_hook=self.backfill_hook,
            ticker_resume_hook=ticker_resume_hook if ticker is not None else None,
            backup_hook=backup_hook,
        )
        self.post_arm: asyncio.Task | None = None
        self.report: StartupReport | None = None

    # ------------------------------------------------------------------ boot (main.py order)
    async def boot(self) -> StartupReport:
        """``_arm_registry_jobs`` → ``lifecycle.startup`` → ``start_scheduler_and_fire_post_arm`` →
        the compaction lane. Both post-arm tasks run in the background in main.py; the rig awaits them
        so a scenario's job ledger is complete when the boot returns."""
        self.env.processes.spawn(self.pid)
        _arm_registry_jobs(self.scheduler, self.registry, self.catch_up, self.env.clock)
        self.report = await self.lifecycle.startup(check_skew=False)
        self.post_arm = start_scheduler_and_fire_post_arm(self.scheduler, self.catch_up, armed=asyncio.Event())
        self.compaction_lane.spawn("post_arm")
        # APScheduler triggers run on the REAL wall clock; the scenario runs on the movable one.
        # Paused (still armed, still ``is_running()``) so a real-world cron minute can never fire a
        # job mid-test — scenarios fire jobs explicitly through fire_scheduled().
        self.scheduler._sched.pause()
        if self.post_arm is not None:
            await asyncio.wait_for(asyncio.shield(self.post_arm), timeout=WAIT_BOUND_S)
        await self._compaction_settled()
        return self.report

    async def sweep(self) -> CatchUpResult:
        """One 30-min catch-up sweep exactly as scheduled, with the compaction lane pass it may start
        awaited too (a background task in main.py)."""
        result = await _catchup_sweep_once(self.catch_up, self.latch, self.kill, self.compaction_lane)
        await self._compaction_settled()
        return result

    async def _compaction_settled(self) -> None:
        task = self.compaction_lane._task
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout=WAIT_BOUND_S)

    async def fire_scheduled(self, job_id: str) -> None:
        """Invoke the job exactly as APScheduler would at its fire-time: the armed job's own
        callable (calendar guard + ``_scheduled_runner`` watermark wrapper)."""
        job = self.scheduler._sched.get_job(job_id)
        assert job is not None, f"{job_id} is not armed"
        await job.func()

    async def heartbeat_synced(self) -> None:
        """Wait (bounded) until the heartbeat THREAD has written the current movable-clock time —
        the rig's stand-in for 'the engine has been beating continuously up to now'."""
        want = self.env.clock.now().isoformat()
        deadline = time_mod.monotonic() + WAIT_BOUND_S
        while time_mod.monotonic() < deadline:
            if self.env.lifecycle_row()["last_alive_at"] == want:
                return
            await asyncio.sleep(0.005)
        raise AssertionError(f"heartbeat never caught up to {want}")

    # ------------------------------------------------------------------ clean stop (main.py order)
    async def stop(self) -> None:
        """main.py's graceful teardown: scheduler down → post-arm cancelled → bars flushed → ticker
        stopped → ``lifecycle.shutdown()`` (backup hook, STOPPED commit, heartbeat join,
        ENGINE_STOPPED) → bus drained → store.close → conn.close → process exit."""
        self.scheduler.shutdown()
        await cancel_post_arm(self.post_arm)
        await self.compaction_lane.cancel()
        if self.bar_builder is not None:
            self.bar_builder.flush_all()
        if self.ticker is not None:
            await self.ticker.stop()
        await self.lifecycle.shutdown()
        await self.bus.drain(_SHUTDOWN_BUS_DRAIN_S)
        if self.store is not None:
            self.store.close()
        await asyncio.sleep(0)                  # let APScheduler's loop-deferred shutdown land
        self.conn.close()
        self._exit()

    # ------------------------------------------------------------------ process death
    async def kill_9(self) -> None:
        """``kill -9`` / power loss: the process and all its threads vanish; NOTHING of the teardown
        runs (no STOPPING, no STOPPED commit, no ENGINE_STOPPED). The resource releases below are test
        hygiene only — the OS does the equivalent for a real process."""
        self.env.processes.reap(self.pid)
        self.heartbeat.stop()                   # its thread died with the process
        if self.scheduler.is_running():
            self.scheduler.shutdown()
        for task in (self.post_arm, self.compaction_lane._task):
            if task is not None and not task.done():
                task.cancel()
        if self.store is not None:
            self.store.close()
        await asyncio.sleep(0)
        self.conn.close()

    def _exit(self) -> None:
        self.env.processes.reap(self.pid)

    # ------------------------------------------------------------------ helpers
    async def arm_recommend(self) -> None:
        """Owner arms RECOMMEND (sticky mode, R10) — a restart must resume it."""
        from engine.core.enums import Mode

        await self.mode.request_transition(Mode.RECOMMEND, Actor.OWNER)

    def armed_job_ids(self) -> set[str]:
        return {j.id for j in self.scheduler._sched.get_jobs()}


async def run_session(env: RigEnv, boot_at: datetime, stop_at: datetime, **kw: Any) -> EngineProcess:
    """Boot one engine process at ``boot_at`` and stop it cleanly at ``stop_at`` (a prior session)."""
    env.at(boot_at)
    proc = EngineProcess(env, **kw)
    await proc.boot()
    env.at(stop_at)
    await proc.stop()
    return proc
