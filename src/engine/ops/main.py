"""Composition root + service entrypoint (§3.2.12).

The ONLY module allowed to import everything — it wires the dependency graph and runs the engine.
``python -m engine.ops.main`` (or the ``mt-engine`` console script). Start is the SAME code path whether
manual/demand or scheduled (§2.6); every startup runs the full §2.6 recovery sequence via
``SessionLifecycle.startup``.

Phase-1 shape: the Phase-0 deterministic core (clock, calendar, secrets, protected store, sticky
mode/kill, dashboard, lifecycle) PLUS the full **data plane** — the single-writer
:class:`~engine.marketdata.store.MarketStore`, the tick→bar :class:`~engine.marketdata.bar_builder.BarBuilder`
subscribed to the ``tick`` bus topic, the §4.4 daily jobs (instruments/surveillance/universe/news/
reconcile/bhavcopy/corp-actions/earnings/deals/sector-map/daily-bars/features) registered on the
:class:`~engine.ops.jobs.JobRegistry` and driven by BOTH the live :class:`~engine.ops.scheduler.Scheduler`
and the startup :class:`~engine.ops.jobs.CatchUpRunner` (same registry), the cold-start
:class:`~engine.ops.warmup.WarmupGate`, and the dedicated-thread
:class:`~engine.ops.heartbeat.HeartbeatWriter`.

Everything broker-touching (instruments dump, backfill, reconcile, ticker) is built only when Kite
credentials exist — a fresh install with no secrets stays runnable and safe (entries FROZEN until
login), which is exactly the §2.6 posture. The Tier-1 harness / OMS / live routing land in later phases.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sqlite3
import threading
import time as time_module  # `time` itself is datetime.time here (below) — WO-25c needs monotonic()
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, NoReturn

import httpx
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from engine.broker.instruments import InstrumentStore, UnknownInstrument
from engine.broker.kite_client import KiteClient, OrderSurfaceViolation
from engine.broker.rate_limiter import RateLimiter
from engine.broker.session import SessionManager
from engine.broker.ticker_supervisor import TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings, load_yaml
from engine.core.db import connect
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.migrations import apply_migrations
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.secrets import DASHBOARD_TOKEN, KITE_API_KEY, TELEGRAM_BOT_TOKEN, Secrets
from engine.core.types import TradeWindow
from engine.datafeeds.bhavcopy import BhavcopyJob, BhavcopyResult
from engine.datafeeds.corp_actions import CorpActionsJob, CorpActionsResult
from engine.datafeeds.deals import DealsJob, DealsResult
from engine.datafeeds.earnings_calendar import EarningsCalendarJob, EarningsCalendarResult
from engine.datafeeds.filings_pit import FilingsPitJob, FilingsPitResult
from engine.datafeeds.filings_pit_fresh import FilingsPitFreshJob, FilingsPitFreshResult
from engine.datafeeds.filings_results import FilingsResultsJob, FilingsResultsResult
from engine.datafeeds.filings_shp import FilingsShpJob, FilingsShpResult
from engine.datafeeds.ins_crossings import InsCrossingsJob, InsCrossingsResult
from engine.datafeeds.news import Headline, NewsIngest
from engine.datafeeds.news_pipeline import CatalystDigestJob, EntityResolver, HeadlineClusterer
from engine.datafeeds.sector_map import SectorMapJob, SectorMapResult
from engine.features.engine import FeatureEngine
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentHarness, load_agent_roster, run_sdk_smoke
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.reconcile import ReconcileJob
from engine.marketdata.store import MarketStore
from engine.marketdata.tick_compact import TickCompactionResult, compact_ticks
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind, login_prompt
from engine.ops.health import HealthMonitor
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.jobs import (
    JOB_BACKUP,
    JOB_BHAVCOPY,
    JOB_CATALYST_DIGEST,
    JOB_CORP_ACTIONS,
    JOB_DAILY_BARS,
    JOB_DEALS,
    JOB_EARNINGS,
    JOB_FEATURES,
    JOB_FILINGS_PIT,
    JOB_FILINGS_PIT_FRESH,
    JOB_FILINGS_RESULTS,
    JOB_FILINGS_SHP,
    JOB_INS_CROSSINGS,
    JOB_INSTRUMENTS,
    JOB_NEWS_CHAIN,
    JOB_NIGHTLY_REVIEW,
    JOB_PREOPEN_PLANNER,
    JOB_RECO_EXPIRE,
    JOB_RECONCILE,
    JOB_SECTOR_MAP,
    JOB_SURVEILLANCE,
    JOB_TICK_COMPACT,
    JOB_UNIVERSE,
    AdvisoryRun,
    CatchUpResult,
    CatchUpRunner,
    CatchUpScope,
    JobClass,
    JobRegistry,
    JobSpec,
    _job_result_ok,
)
from engine.ops.keep_awake import KeepAwake
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.news_scoring import NewsScoringJob
from engine.ops.nightly_review import NightlyReviewJob, read_funnel_raw_counts
from engine.ops.pipeline import RecommendationBook, RecommendationPipeline
from engine.ops.post_login import (
    PostLoginRecovery,
    hydrate_instruments_at_startup,
    regime_and_warmup_backfill,
    resume_ticker,
)
from engine.ops.preopen_planner import PreopenPlannerJob
from engine.ops.scan_context import LiveScanContextProvider
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.ops.single_instance import InstanceLock
from engine.ops.token_check import TOKEN_CHECK_IST, TokenCheckJob
from engine.ops.warmup import WarmupGate, WarmupStatus
from engine.risk.causes import RiskStateLatch
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContextBuilder, RiskGate
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine, floor_limits_from
from engine.risk.mode import ModeManager
from engine.strategy.cost_model import CostModel
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import brk20, build_enabled_scanners, cat, cat_reversal, hi52, ins
from engine.universe.builder import UniverseBuilder
from engine.universe.leverage import MisLeverageIngest
from engine.universe.surveillance import SurveillanceIngest

_log = get_logger("engine.ops.main")

#: Canonical bars_1d symbols the backfill job persists NIFTY 50 / India VIX under; the WarmupGate and
#: FeatureEngine defaults MUST match these (§7.1 ``regime_data_ready``).
INDEX_SYMBOL = "NIFTY 50"
VIX_SYMBOL = "INDIA VIX"

#: Fallback tick size when an instrument is not in today's dump (NSE minimum, A10) — mirrors
#: engine.marketdata.reconcile.DEFAULT_TICK_SIZE; only ever loosens the reconcile close-drift check.
_DEFAULT_TICK = Decimal("0.05")

#: How far back to scan universe_daily for the current active watchlist (bounded, trading-day-agnostic).
_WATCHLIST_LOOKBACK_DAYS = 15

#: Fire-times for the three §10.1 jobs the plan lists without a dedicated ``settings.jobs`` key: the news
#: chain runs just before the 08:30 universe build (backfill→cluster→resolve is never entry-blocking,
#: §2.7); the nightly incremental daily-bar backfill and the feature snapshot follow the EOD data jobs.
#: Features moved 18:50→20:45 (2026-08-18): it reads day-``d`` deals flags and corp actions, whose
#: jobs moved to 20:30/20:15 on 2026-07-24 (NSE evening-maintenance 503s) — at 18:50 it had been
#: reading both BEFORE their daily writes ever since, so every live-fired features row since 07-24
#: carried ``flagged``/ex-date context from stale data (catch-up-fired rows, ordered deals→features
#: by ``order``, did not — an inconsistency in the training data). 20:45 restores the write→read
#: ordering the ``order=40→50`` catch-up sequence always encoded, and stays before the 21:00 nightly
#: review/backup slots.
_NEWS_CHAIN_IST = time(8, 25)
_DAILY_BARS_IST = time(18, 5)
_FEATURES_IST = time(20, 45)

#: §4.3 tick-partition compaction (WO-7) — late evening, after every EOD data job and the nightly
#: review, so a multi-minute filesystem pass never competes with them. Date-keyed: the run for day D
#: compacts the closed date partitions up to D (never D itself — the writer still owns it).
_TICK_COMPACT_IST = time(22, 30)

#: Async job-runner type: DATE_KEYED runners take the run-for ``date``; all others take no args.
JobRunFn = Callable[..., Awaitable[None]]

#: Every §10.1/§4.4 Phase-1 job id the composition root registers (watermark identities — never rename).
#: The single source of truth ``build_job_registry`` and the CatchUpRunner/Scheduler arming iterate.
PHASE1_JOB_IDS: tuple[str, ...] = (
    JOB_INSTRUMENTS, JOB_SURVEILLANCE, JOB_EARNINGS,          # safety/deadline-critical
    JOB_UNIVERSE, JOB_NEWS_CHAIN, JOB_CORP_ACTIONS, JOB_SECTOR_MAP, JOB_BACKUP,  # run-latest
    JOB_FILINGS_SHP,                                                             # run-latest (§2.8)
    JOB_RECONCILE, JOB_BHAVCOPY, JOB_DAILY_BARS, JOB_DEALS, JOB_FEATURES,        # date-keyed
    JOB_FILINGS_PIT, JOB_FILINGS_PIT_FRESH, JOB_FILINGS_RESULTS,                 # date-keyed (§2.8)
    JOB_INS_CROSSINGS,                                                           # date-keyed (§6.1 `ins`)
    JOB_TICK_COMPACT,                                                            # date-keyed (§4.3/WO-7)
)

#: Phase-2 additions (§8.3): digest → planner run pre-open in dependency order after the news chain;
#: rec-expiry labels stale unconfirmed recommendations post-window; the nightly reviewer is date-keyed
#: (one review per missed trading day, §2.6). Registered only when the owning object was built.
PHASE2_JOB_IDS: tuple[str, ...] = (
    JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER, JOB_RECO_EXPIRE,   # run-latest
    JOB_NIGHTLY_REVIEW,                                          # date-keyed
)

#: Fire-time for the §3.6 expiry labeling sweep — after the 15:30 close, before EOD reconcile.
_RECO_EXPIRE_IST = time(15, 45)

#: WO-15 (i): the §2.6 catch-up jobs that are NEVER load-bearing for entries and therefore must not
#: run INSIDE boot. The 2026-08-10 wedge was an unbounded news chain in ``lifecycle.startup``, ahead
#: of ``scheduler.start()``: it starved every scheduled job for 8 h — including the 30-min catchup
#: sweep that exists to self-heal — while the scheduler guard (calendar-only, no recovery awareness)
#: made naive early arming unsafe. These now fire as one-shots through the SAME catch-up machinery
#: (same watermarks, same dependency order) immediately AFTER the scheduler is armed, so an identical
#: wedge costs the digest alone. Digest staleness already degrades ``cat`` safely (digest_stale_max_h).
#: Members, in catch-up dependency order: news chain (20) → digest (25) → planner (28), plus the WO-7
#: tick compaction — pure EOD housekeeping (readers see fragments and compacted files identically),
#: and the single heaviest catch-up step by wall-clock, so boot is precisely where it must not be.
POST_ARM_JOB_IDS: tuple[str, ...] = (
    JOB_NEWS_CHAIN, JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER, JOB_TICK_COMPACT,
)

#: ROLLBACK FLAG (WO-15 risk note). ``False`` restores the pre-WO-15 firing point exactly: the boot
#: catch-up pass runs the whole registry (``deferred`` empty ⇒ every scope is the full registry) and
#: the post-arm one-shot becomes a no-op. Flip + restart; no other code path changes.
DEFER_POST_ARM_JOBS = True

#: WO-21 (ii): the IST window in which a boot must NOT fire the post-arm ``tick_compact`` one-shot.
#: 2026-08-20, 11:26 IST — a mid-session crash-recovery boot fired the compaction backlog catch-up
#: while the market was open: ~16 GB memory peak, tick processing fell more than an hour behind wall
#: clock, ``/db/query`` went unresponsive and Telegram sends timed out. Compaction is idle-hours
#: housekeeping (§4.3/WO-7): the 22:30 scheduled slot still covers the day, and the 30-min ``ALL``
#: sweep re-runs whatever the skipped one-shot left unwatermarked once the session is over — so the
#: skip costs a few hours of fragment retention, never a compaction. Bounds bracket the session with
#: margin either side (08:45 is ahead of the 09:00 pre-open, 15:45 behind the 15:30 close).
_IN_SESSION_START_IST = time(8, 45)
_IN_SESSION_END_IST = time(15, 45)


def _is_sunday(d: date) -> bool:
    return d.weekday() == 6   # §4.4 job 13 weekly cadence — fires Sunday, not a trading day


def build_job_registry(settings, fns: Mapping[str, JobRunFn]) -> JobRegistry:
    """Populate the §10.1/§4.4 Phase-1 ``JobRegistry`` — the single inventory driving BOTH the live
    :class:`~engine.ops.scheduler.Scheduler` and the startup :class:`~engine.ops.jobs.CatchUpRunner`.

    ``fns`` maps each :data:`PHASE1_JOB_IDS` entry to its async runner. Job classes/fire-times/dependency
    ``order`` encode §2.6 step-5 semantics: safety-critical (run/verify-before-entries), idempotent
    run-latest (single catch-up over the gap), date-keyed (one run per missed trading day). Extracted to
    module scope so the wiring is unit-testable independent of the broker/store side of the graph.
    """
    registry = JobRegistry()
    for spec in (
        # safety/deadline-critical — run/verify before entries open, else FROZEN-for-entries (§2.6)
        JobSpec(JOB_INSTRUMENTS, JobClass.SAFETY_CRITICAL, settings.jobs.instruments_ist, fns[JOB_INSTRUMENTS], order=10),
        JobSpec(JOB_SURVEILLANCE, JobClass.SAFETY_CRITICAL, settings.jobs.surveillance_ist, fns[JOB_SURVEILLANCE], order=20),
        JobSpec(JOB_EARNINGS, JobClass.SAFETY_CRITICAL, settings.jobs.earnings_ist, fns[JOB_EARNINGS], order=30),
        # idempotent run-latest (single catch-up covering the gap; never per-day)
        JobSpec(JOB_UNIVERSE, JobClass.RUN_LATEST, settings.jobs.universe_build_ist, fns[JOB_UNIVERSE], order=10),
        JobSpec(JOB_NEWS_CHAIN, JobClass.RUN_LATEST, _NEWS_CHAIN_IST, fns[JOB_NEWS_CHAIN], order=20),
        JobSpec(JOB_CORP_ACTIONS, JobClass.RUN_LATEST, settings.jobs.corp_actions_ist, fns[JOB_CORP_ACTIONS], order=30),
        JobSpec(JOB_SECTOR_MAP, JobClass.RUN_LATEST, settings.jobs.universe_build_ist, fns[JOB_SECTOR_MAP],
                order=40, fire_day=_is_sunday),
        # §2.8 SHP + pledge: run-latest (per-symbol BSE detail only for new submissions), after the
        # EOD data jobs; never entry-blocking (filings are features/risk-context only in stage 1).
        JobSpec(JOB_FILINGS_SHP, JobClass.RUN_LATEST, settings.jobs.filings_shp_ist, fns[JOB_FILINGS_SHP], order=80),
        JobSpec(JOB_BACKUP, JobClass.RUN_LATEST, settings.jobs.backup_ist, fns[JOB_BACKUP], order=90),
        # date-keyed backfill — one run per missed trading day, ascending
        JobSpec(JOB_RECONCILE, JobClass.DATE_KEYED, settings.jobs.reconcile_ist, fns[JOB_RECONCILE], order=10),
        JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, settings.jobs.bhavcopy_ist, fns[JOB_BHAVCOPY], order=20),
        JobSpec(JOB_DAILY_BARS, JobClass.DATE_KEYED, _DAILY_BARS_IST, fns[JOB_DAILY_BARS], order=30),
        JobSpec(JOB_DEALS, JobClass.DATE_KEYED, settings.jobs.deals_ist, fns[JOB_DEALS], order=40),
        JobSpec(JOB_FEATURES, JobClass.DATE_KEYED, _FEATURES_IST, fns[JOB_FEATURES], order=50),
        # §2.8 filings: insider trades (PIT) + results/board-meeting dates — one run per missed day.
        JobSpec(JOB_FILINGS_PIT, JobClass.DATE_KEYED, settings.jobs.filings_pit_ist, fns[JOB_FILINGS_PIT], order=60),
        JobSpec(JOB_FILINGS_PIT_FRESH, JobClass.DATE_KEYED, settings.jobs.filings_pit_fresh_ist, fns[JOB_FILINGS_PIT_FRESH], order=65),
        JobSpec(JOB_FILINGS_RESULTS, JobClass.DATE_KEYED, settings.jobs.filings_results_ist, fns[JOB_FILINGS_RESULTS], order=70),
        # §6.1 `ins`: EOD insider net-buy crossing detection -> ins_pending. order=66 puts it directly
        # after filings_pit_fresh (65) and before filings_results (70), so a §2.6 catch-up replay of a
        # missed day ingests the day's BSE fresh rows BEFORE computing crossings over them — the same
        # dependency the 19:00/19:15 fire-times encode for the live path.
        JobSpec(JOB_INS_CROSSINGS, JobClass.DATE_KEYED, settings.jobs.ins_crossings_ist, fns[JOB_INS_CROSSINGS], order=66),
        # §4.3 storage housekeeping (WO-7): collapse each closed symbol-day's ~7.5 K tick fragments
        # into one file. LAST in the date-keyed order — it reads no engine state and blocks nothing.
        JobSpec(JOB_TICK_COMPACT, JobClass.DATE_KEYED, _TICK_COMPACT_IST, fns[JOB_TICK_COMPACT], order=90),
    ):
        registry.register(spec)
    # Phase-2 jobs (§8.3) register only when composition built their owning object (LLM tier may be
    # disabled, D7). Catch-up dependency order within RUN_LATEST: universe(10) → news_chain(20) →
    # digest(25) → planner(28) — the digest needs scored clusters + today's universe; the planner
    # needs the digest (§2.7 steps 4-6).
    for spec in (
        # NOTE (2026-08-18): the digest reads the PRIOR session's deals flags. In one catch-up pass
        # RUN_LATEST runs before DATE_KEYED, so a same-pass deals catch-up lands AFTER the digest —
        # the ordering that saves this is WO-15: the digest is a DEFERRED job, so the boot pass
        # replays deals first and the digest fires in the post-arm one-shot. Un-deferring the digest
        # would reopen a stale-flags read on multi-day-gap boots.
        JobSpec(JOB_CATALYST_DIGEST, JobClass.RUN_LATEST, settings.jobs.catalyst_digest_ist,
                fns.get(JOB_CATALYST_DIGEST), order=25),
        JobSpec(JOB_PREOPEN_PLANNER, JobClass.RUN_LATEST, settings.jobs.preopen_planner_ist,
                fns.get(JOB_PREOPEN_PLANNER), order=28),
        JobSpec(JOB_RECO_EXPIRE, JobClass.RUN_LATEST, _RECO_EXPIRE_IST,
                fns.get(JOB_RECO_EXPIRE), order=60),
        JobSpec(JOB_NIGHTLY_REVIEW, JobClass.DATE_KEYED, settings.jobs.nightly_review_ist,
                fns.get(JOB_NIGHTLY_REVIEW), order=80),
    ):
        if spec.run is not None:
            registry.register(spec)
    return registry


async def run() -> int:
    settings = load_settings()
    configure_logging(level="INFO", logs_dir=settings.logs_dir())
    _log.info("engine_boot", env=settings.env, tz=settings.timezone)

    # --- §2.6 step-0 PRIMARY single-instance guard (2026-07-21 double-run). An exclusive OS file lock,
    #     acquired BEFORE any shared resource is touched (sqlite connect / MarketStore.open / Telegram
    #     poll / :8400 bind). That day TWO instances ran: one wedged BEFORE lifecycle.startup ever
    #     committed RUNNING, so the DB-row guard (§2.6 step 0) was blind to it, and two boots can both
    #     pass its check-then-act read (TOCTOU). This kernel lock IS real mutual exclusion — released on
    #     ANY process death (crash / taskkill /F), so no stale lock to reap. The DB check stays as the
    #     secondary crash-detection / pid-alive guard. Exit code 3 is deliberate: non-zero (a refusal is
    #     never mistaken for a clean run) and distinct from the watchdog's 2. ---
    lock_path = settings.resolved_data_dir() / "engine.lock"
    instance_lock = InstanceLock(lock_path)
    if not instance_lock.acquire():
        _log.critical(
            "single_instance_refused_lock", lock=str(lock_path), holder_pid=instance_lock.holder_pid(),
            hint="another engine instance owns the single-writer stores (§2.6 step 0); this one exits",
        )
        return 3

    # --- graceful-stop wiring, installed EARLY (2026-07-21 13:34 IST zombie). Ctrl-C during a wedged
    #     lifecycle.startup used to hit Python's default SIGINT handler (raw KeyboardInterrupt, no
    #     shutdown, process lingered on data/engine.lock). Installing here — right after the lock, before
    #     ANY shared resource is opened — covers the ENTIRE boot; a first Ctrl-C requests a graceful stop
    #     (honoured at await stop_event.wait()), a second forces a hard exit even if the boot is wedged. ---
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    # --- WO-25c BOOT CONTRACT WATCHDOG, armed HERE — the first point at which this process is
    #     committed to running (the instance lock is held, signals are wired) and still ahead of every
    #     resource that can wedge: sqlite, MarketStore.open, Telegram, the :8400 bind, the token probe,
    #     lifecycle.startup, and the warm-up seeding. On 2026-08-24 the 12:44:58 boot logged
    #     startup_complete at 12:46:56 and then NOTHING — it parked inside the pre-arm warm-up refresh
    #     and scheduler.start() was never reached, so not one trigger fired for the rest of the day and
    #     no alarm existed to say so (the health pulse that would have noticed is itself an APScheduler
    #     job). This watchdog is a PLAIN asyncio task on purpose: it must stay alive precisely when the
    #     scheduler is the broken thing. `boot_state` is late-bound — the watchdog reads it at check
    #     time, so it can be armed before the alert sink / scheduler / engine_ready even exist. ---
    boot_state: dict[str, Any] = {"engine_ready": False, "scheduler": None, "alert": None}
    boot_watchdog = asyncio.create_task(boot_contract_watchdog(boot_state), name="boot_contract")

    # --- persistence + migrations ---
    conn = connect(settings.sqlite_path())
    applied = apply_migrations(conn)
    if applied:
        _log.info("migrations_applied_on_boot", files=applied)

    # --- deterministic core ---
    clock = Clock(ntp_servers=settings.clock.ntp_servers)
    secrets = Secrets()
    window_seed = TradeWindow(
        start=settings.trade_window.start_ist,
        end=settings.trade_window.end_ist,
        squareoff_buffer_min=settings.trade_window.squareoff_buffer_min,
    )
    calendar = NSECalendar(
        config_dir() / "calendar", clock,
        strict=(settings.env == "prod"), sqlite_conn=conn, window_seed=window_seed,
    )
    bus = EventBus()
    protected_store = ProtectedStore(config_dir(), conn, clock)
    mode = ModeManager(conn, clock, bus, calendar)
    latch = RiskStateLatch(conn, clock, mode)
    limits_engine = LimitsEngine(protected_store)
    governor = BudgetGovernor.from_config(conn, clock, calendar, bus)

    # --- live tick cache: last (ltp, exchange_ts) per symbol. Feeds the ExposureTracker mark
    #     source and the gate's ltp/tick-age seams (§7.1 stale_data_guard) — a symbol never seen
    #     this session reads None ⇒ every price-derived rule fails CLOSED. ---
    last_ticks: dict[str, tuple[Decimal, Any]] = {}

    async def _cache_tick(evt: Any) -> None:
        last_ticks[evt.tradingsymbol] = (evt.ltp, evt.exchange_ts)

    bus.subscribe("tick", _cache_tick)

    def mark_price(symbol: str) -> Decimal | None:
        cached = last_ticks.get(symbol)
        return cached[0] if cached else None

    def tick_age_s(symbol: str) -> float | None:
        cached = last_ticks.get(symbol)
        if cached is None:
            return None
        return max(0.0, (clock.now() - cached[1]).total_seconds())

    # Capital base from the protected limit table; an unregistered/tampered store must not crash the
    # boot (the self-test FAILs it to FROZEN separately) — fall back to the §7.1 starting value.
    try:
        _capital_base = limits_engine.load().capital_base_inr
    except IntegrityError:
        _log.warning("limits_unverified_at_boot", hint="scripts/seed_protected_config.py; using S7.1 default base")
        _capital_base = Decimal("20000")
    exposure = ExposureTracker(conn, clock, _capital_base, mark_price=mark_price)

    # --- owner I/O alert sink (Telegram if configured, else log-only) ---
    telegram_holder: dict[str, object] = {"bot": None}

    async def alert(severity: str, message: str) -> None:
        bot = telegram_holder["bot"]
        if bot is not None:
            try:
                await bot.send(CatalogMessage(
                    kind=MessageKind.LIMIT_BREACH, title="alert", body=message,
                    severity=severity if severity in ("info", "warning", "critical") else "warning",
                ))
            except Exception:  # noqa: BLE001 - alerting must never crash the engine
                _log.exception("telegram_alert_failed", message=message)
        getattr(_log, severity if severity in ("info", "warning", "critical") else "warning")(
            "alert", severity=severity, message=message
        )

    async def notify(msg: CatalogMessage) -> None:
        """Typed owner-notification sink (§3.2.11): send a catalog message via Telegram if configured,
        always mirrored to the structured log. Best-effort — a failed send never propagates. This is
        the single ``CatalogMessage`` sink handed to every data job (reconcile drift, feed-degraded,
        universe/sector failures) and to the lifecycle process-lifecycle signals + catch-up report."""
        bot = telegram_holder["bot"]
        if bot is not None:
            try:
                await bot.send(msg)
            except Exception:  # noqa: BLE001 - notification must never crash the engine
                _log.exception("telegram_notify_failed", kind=str(msg.kind))
        _log.info("notify", kind=str(msg.kind), severity=msg.severity, title=msg.title)

    async def kill_alert(message: str) -> None:
        await alert("critical", message)

    # WO-25c: hand the boot-contract watchdog its owner channel now that one exists. Telegram itself
    # comes up later (`telegram.start()`); `alert` reads `telegram_holder` at CALL time, so a violation
    # detected at T+180 s always uses whatever channel is live by then — and falls back to the
    # structured CRITICAL log when none is.
    boot_state["alert"] = alert

    kill = KillSwitch(conn, clock, bus, alert_callback=kill_alert)
    session = SessionManager(secrets, clock, redirect_path=settings.broker.kite_login_redirect_path)

    # --- kill ⇄ cause-latch bridge (§3.5.3): the sticky kill_state is the KillSwitch's own store;
    #     mirroring it into the risk_state_causes ledger keeps mode_state.risk_state showing KILLED
    #     (and re-arming to the next-most-restrictive cause on owner reset, never straight to NORMAL).
    from engine.risk.events import TOPIC_KILL_STATE

    async def _kill_to_latch(evt: Any) -> None:
        if evt.killed:
            await latch.set_cause("kill", RiskState.KILLED, evt.reason or "kill", evt.actor)
        else:
            await latch.clear_cause("kill", evt.actor)

    bus.subscribe(TOPIC_KILL_STATE, _kill_to_latch)

    # --- Telegram (optional in Phase 0/1). reco_book is wired after the pipeline exists (Phase-2
    #     second pass); every other owner-command dependency is live from boot. ---
    telegram = _build_telegram(
        settings, secrets, clock, mode, kill,
        latch=latch, governor=governor, limits_engine=limits_engine, exposure=exposure,
        session=session, conn=conn, bus=bus,
    )
    telegram_holder["bot"] = telegram

    # --- Tier-1 harness (§3.2.6): the ONLY SDK call site. Consumed by the D11 sdk-smoke check and
    #     (second wiring pass) the recommendation pipeline / planner / news-scoring jobs. Defs load
    #     PER-AGENT (2026-08-03: one unmapped model name used to raise and dark the WHOLE tier for
    #     the run); an invalid def is quarantined + surfaced by the self-test, the rest stay live.
    agent_defs: dict[str, Any] = {}
    roster_quarantined: dict[str, str] = {}
    harness: AgentHarness | None = None
    try:
        roster = load_agent_roster(load_yaml(config_dir() / "agents.yaml"))
        agent_defs = roster.defs
        roster_quarantined = roster.quarantined
        if agent_defs:
            harness = AgentHarness(agent_defs, governor, clock, conn, alert=alert)
        else:
            _log.error("agent_roster_empty", hint="config/agents.yaml — LLM tier disabled this run")
    except Exception:  # noqa: BLE001 - a bad roster must not stop the deterministic engine (D7)
        _log.exception("agent_roster_unloadable", hint="config/agents.yaml — LLM tier disabled this run")

    # =========================================================================================
    # DATA PLANE (§2.5 / §3.2.3 / §4.4). Single-writer store + tick→bar builder + daily jobs.
    # =========================================================================================
    store = MarketStore.from_settings(settings, clock).open()
    instruments = InstrumentStore(clock)

    def tick_size_for(symbol: str) -> Decimal:
        """Per-symbol banded tick (A10) for the reconcile close-drift check; NSE minimum if unknown."""
        try:
            return instruments.by_symbol(symbol).tick_size
        except UnknownInstrument:
            return _DEFAULT_TICK

    # BarBuilder: ticks (published by TickerSupervisor on "tick") → finalized 1m bars → bar.1m +
    # single-writer batch persist. It also buffers raw ticks into the §4.3 Parquet dataset (do NOT
    # buffer a second time elsewhere). advance() runs on a coarse timer; flush_all() at EOD/shutdown.
    # notify: the WO-25a processing-lag watchdog pages the owner ONCE per lag episode (2026-08-24
    # late-tick death spiral — the engine ran 20 min behind for two hours with nothing but INFO lines).
    bar_builder = BarBuilder(store, clock, bus, notify=notify)
    bus.subscribe("tick", bar_builder.on_tick_event)

    # Shared injected httpx client for every best-effort feed (convention 11 / E5). Owned here.
    # Default browser headers + split timeout (A3/A4): the bare python-httpx UA is tarpitted/blocked by
    # NSE hosts; the per-feed fetches funnel through engine.core.nse_http.nse_get for cookie priming +
    # bounded retry on top of this client's shared (primed) cookie jar.
    http = httpx.AsyncClient(
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=httpx.Timeout(30.0, connect=10.0),
    )

    # Broker REST facade — only when credentials exist (fresh install stays runnable + FROZEN, §2.6).
    def order_guard(intent: str) -> None:
        """Order-surface predicate (a) (§3.5.3, A7/B7): a position-OPENING broker call requires
        AUTO ∧ NORMAL ∧ inside the owner trade window; risk-reducing intent is NEVER gated (R3)."""
        if intent == "risk_reducing":
            return
        kill.assert_orders_allowed()
        try:
            start, end = calendar.trade_window(clock.today())
            in_window = start <= clock.now() <= end
        except ValueError:                          # not a trading day ⇒ never in-window
            in_window = False
        if not mode.opening_orders_allowed(in_window):
            raise OrderSurfaceViolation(
                f"opening order blocked: mode={mode.mode().value} "
                f"risk_state={mode.risk_state().value} in_window={in_window} (B7/§3.5.3)"
            )

    kite: KiteClient | None = None
    if secrets.has(KITE_API_KEY):
        kc = session.kite_connect()
        if kc is not None:
            # on_token_rejected wires the §2.6/R6 circuit breaker: the FIRST TokenException on any
            # broker call fires SessionManager.on_token_rejected → the invalidation hook (freeze +
            # alert), so a mid-day token death stops entries instead of failing silently (2026-07-21).
            kite = KiteClient(kc, RateLimiter(clock), clock,
                              on_token_rejected=session.on_token_rejected, order_guard=order_guard)
    else:
        _log.warning("kite_client_absent", hint="seed kite_api_key/secret; entries stay FROZEN until login")

    backfill = (
        BackfillJob(store, kite, clock, settings, conn, instruments.token_for_symbol)
        if kite is not None else None
    )
    reconcile = (
        ReconcileJob(
            store, kite, clock, settings,
            token_for_symbol=instruments.token_for_symbol,
            tick_size_for=tick_size_for,
            notify=notify,
        )
        if kite is not None else None
    )

    # --- universe (A8/C7/E5): ingests refreshed 08:15/08:20, build 08:30 ---
    data_dir = settings.resolved_data_dir()
    leverage = MisLeverageIngest(clock, http, data_dir / "universe" / "mis_margins.json", notify=notify)
    surveillance = SurveillanceIngest(clock, http, data_dir / "universe" / "surveillance.json", notify=notify)
    universe_builder = UniverseBuilder(
        settings, store, instruments, leverage, surveillance, clock, http, notify=notify
    )

    # --- EOD datafeeds (§4.4 jobs 6-9/13) ---
    bhavcopy = BhavcopyJob(store, clock, http, notify=notify)
    corp_actions = CorpActionsJob(store, clock, http, notify=notify)
    earnings = EarningsCalendarJob(store, clock, http, notify=notify)
    deals = DealsJob(store, clock, http, notify=notify)
    # §2.8 corporate-filings feeds (data-only in stage 1; never entry-blocking, E5). filings_results
    # reuses the earnings provider's historical leg (run_range) for board-meeting dates.
    filings_pit = FilingsPitJob(store, clock, http, notify=notify)
    filings_pit_fresh = FilingsPitFreshJob(store, clock, http, notify=notify)
    filings_results = FilingsResultsJob(store, clock, http, earnings=earnings, notify=notify)
    filings_shp = FilingsShpJob(store, clock, http, notify=notify)
    # §6.1 `ins` (2026-08-17): EOD insider net-buy crossing detection over the SAME validated crossing
    # function the WO-16 study runs (engine.datafeeds.insider_crossings). No HTTP — it reads the rows
    # filings_pit_fresh ingested at 19:00 and journals crossings into ins_pending for the next
    # session's sweep. threshold_inr is owner-fixed (it defines the validated event population).
    ins_crossings = InsCrossingsJob(
        store, conn, clock, calendar, threshold_inr=settings.ins.threshold_inr
    )
    sector_map = SectorMapJob(store, clock, http, data_dir / "datafeeds" / "sector_lists.json", notify=notify)

    # --- news pipeline data side (§2.7 steps 1-3) ---
    news_ingest = NewsIngest(settings.news, store, clock, http)
    clusterer = HeadlineClusterer(
        store, sim_threshold=settings.news.cluster_sim_threshold,
        max_event_age_days=settings.cat.max_event_age_days,
    )
    resolver = EntityResolver(store, clock)

    # --- feature engine v2 (§3.2.5/§6.2) ---
    features = FeatureEngine(store, clock, calendar, index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)

    # =========================================================================================
    # PHASE-2 DECISION PLANE (§3.2.6/§3.2.7/§3.6): cost model → gate → pipeline → agent jobs.
    # Everything below is inert without the Tier-1 harness (LLM tier disabled ⇒ scanners still
    # run, the gate still exists, but no proposals are minted — fails to no-recommendation, D7).
    # =========================================================================================
    # `edge_multiple_min` is the ONE learner-movable gate knob (§6.3/§7.1): operating value from
    # envelope_state when a promotion has written one, else the owner's limits.yaml default —
    # never the CostModel's Python default (2026-07-28 review: an owner limit change was invisible).
    # Re-read at boot; intra-run envelope promotions land Phase 5 (§6.4).
    def _edge_multiple_min() -> Decimal:
        row = conn.execute(
            "SELECT value FROM envelope_state WHERE parameter='edge_multiple_min'"
        ).fetchone()
        if row is not None and row["value"]:
            return Decimal(str(row["value"]))
        try:
            return Decimal(str(limits_engine.load().limits.min_viable_size.edge_multiple_min_default))
        except IntegrityError:
            return Decimal("2.0")   # §6.3 default; the store failure is already FROZEN elsewhere

    cost_model = CostModel.from_config(instruments=instruments, edge_multiple_min=_edge_multiple_min())
    gate = RiskGate(
        limits_engine, cost_model, clock,
        # §6.1 `ins` (2026-08-17): the C3 edge check derives the expected move from the proposal's
        # TARGET, and `ins` has none by design (its exit is the §7.1 20-td time cap, and the drift it
        # captures was MEASURED, not predicted). Rather than invent a target to feed the arithmetic,
        # the strategy registers its validated T+20 NET drift (WO-16: +1.5797%, owner-set in
        # settings.yaml, never learner-movable). Used ONLY when target_price is None; every other
        # strategy and every targetless proposal without a registered edge behaves exactly as before.
        strategy_expected_edge_pct={
            ins.STRATEGY_ID: Decimal(str(settings.ins.expected_edge_pct)),
        },
        # §2.7 news SHADOWS: C3 rejects these unconditionally, before target_price is even read, so
        # "signals are a measurement, never a recommendation" is ENFORCED at Tier 2 rather than
        # inferred from a missing expected_edge_pct (see risk/gate.py _SHADOW_NO_EDGE for why, and
        # NO_EDGE_SHADOW_STRATEGIES below for the membership and its history).
        no_edge_shadow_strategies=NO_EDGE_SHADOW_STRATEGIES,
    )

    # Warm-up status cache for the gate context: WarmupGate.status() is async + store-heavy, so the
    # gate reads a snapshot refreshed by the equity/health cadence. Unset ⇒ a NOT-READY status with a
    # regime blocker — both §7.1 readiness rules fail CLOSED until the first refresh lands.
    warmup_holder: dict[str, Any] = {"status": None}
    _WARMUP_UNREFRESHED = WarmupStatus(
        ready=False, blockers=["warmup:unrefreshed 0/0", "regime:unrefreshed 0/0"]
    )

    def warmup_status_snapshot() -> WarmupStatus:
        return warmup_holder["status"] or _WARMUP_UNREFRESHED

    # Clock-skew verdict is boot-scoped (§3.2.12 self-test measures it; the health loop deliberately
    # skips per-minute NTP). A mid-day drift is caught at the next startup — accepted for Phase 2.
    skew_holder: dict[str, bool] = {"ok": False}

    ctx_builder = GateContextBuilder(
        limits_engine, exposure, instruments, store, calendar, clock, mode, kill,
        ltp_fn=mark_price, tick_age_fn=tick_age_s,
        warmup_status_fn=warmup_status_snapshot,
        clock_skew_ok_fn=lambda: skew_holder["ok"],
        degrade_tier_fn=lambda: governor.degrade_tier().value,
        conn=conn, index_symbol=INDEX_SYMBOL,
        # nifty50_fn/expiry_day_fn unwired in Phase 2: the expiry-day NIFTY50-MIS leg of
        # `no_trade_windows` is inert until Phase 3 wires index membership (WORKLOG'd).
    )

    assembler = ContextAssembler(store, conn, clock, calendar)
    book = RecommendationBook(conn, clock, cost_model)
    pipeline = (
        RecommendationPipeline(
            assembler, harness, agent_defs, gate, ctx_builder, book, mode, kill,
            governor, exposure, limits_engine, notify, clock, calendar, conn, store,
            # Late-bound like momentum_universe below: `prescreen` is constructed a few lines further
            # down; the lambda resolves it at call time (an analyst failure long after wiring).
            rearm=lambda sym, sid: prescreen.rearm(sym, sid),
            # 2026-08-21: the drain tick also flushes the WO-9 RAW counters to `funnel_raw_counts`
            # (late-bound for the same reason as `rearm`). Paired with `raw_counts_loader` below —
            # the flush writes ABSOLUTE totals, so it must only ever run against a hydrated counter.
            funnel_raw=lambda d: prescreen.raw_counts(d),
            # 2026-08-27 cap displacement, the two halves of the §3.2.5 admission seam (same
            # late-binding reason as `rearm`). `claim_slot` is the compare-and-set the pipeline must
            # pass before spending an analyst call — it is what makes an EVALUATED candidate's slot
            # permanent; `take_displaced` pulls the pre-screen's evictions onto the event loop, the
            # only place the forward queue may be touched. Both unwired ⇒ pre-2026-08-27 behaviour.
            claim_slot=lambda sym, sid: prescreen.claim_slot(sym, sid),
            take_displaced=lambda: prescreen.take_displaced(),
            admission_mode=settings.strategy.prescreen.admission_mode,   # WO-1 rollback flag
            # 2026-08-14 rollback flag: `immediate` restores the inline drain (see forward_drain_tick).
            forward_drain_mode=settings.strategy.prescreen.forward_drain_mode,
        )
        if harness is not None else None
    )
    if pipeline is not None:
        bus.subscribe("signal.candidate", pipeline.on_signal_candidate)
        bus.subscribe("bar.1m", pipeline.on_bar)
    if telegram is not None:
        telegram.set_reco_book(book)

    # --- live pre-screen (§3.2.5): scanners → signal.candidate. Runs whenever the engine is up.
    #     Since 2026-08-18 origination is ALSO window-gated here, on bar time, so a candidate the
    #     pipeline is guaranteed to drop never charges an unrefundable day slot; the pipeline's own
    #     Clock-based check remains the authoritative gate on tradability. ---
    scan_provider = LiveScanContextProvider(
        store, clock, calendar, features, index_symbol=INDEX_SYMBOL,
        # Late-bound: watchlist_symbols is defined further down this function; the lambda resolves it
        # at day-cache build time (first bar of the day), long after the whole graph is wired.
        momentum_universe=lambda: watchlist_symbols(),
        # WO-13 (2026-08-13, F11): persists the mom rebalance-day marker (mom_rebalance_state,
        # migration 0006) so live mom fires only every rebalance_days sessions instead of every day.
        conn=conn,
    )
    prescreen = SignalPreScreen(
        # The registry holds only the per-bar price baselines; brk20/ins/cat are batch rules swept
        # below and admitted through prescreen.admit (same dedupe/caps, no bypass).
        scanners=build_enabled_scanners(("orb", "rsi2", "trend", "mom")),
        context_provider=scan_provider,
        bus=bus,
        max_candidates_per_day=settings.strategy.prescreen.max_candidates_per_day,
        max_per_strategy_day=settings.strategy.prescreen.max_per_strategy_day,
        admission_mode=settings.strategy.prescreen.admission_mode,       # WO-1 rollback flag
        # 2026-09-02: cumulative cap tranches by bar time — the window-open burst can no longer
        # spend a whole sub-cap in its first minute (see the prescreen module docstring).
        cap_release_schedule=settings.strategy.prescreen.cap_release_schedule,
        # §2.7 news carve-out (WO-18): the per-day catalyst-entry cap, read at the ENFORCEMENT site
        # from the hash-verified limits.yaml (§2.4 item 1) — never a constructor number, never in the
        # gate. A raise here (unverifiable store) refuses cat candidates; the pre-screen handles it.
        catalyst_cap_fn=lambda: limits_engine.catalyst_guard().max_catalyst_entries_day,
        # 2026-08-21: the WO-9 RAW counters were the last piece of funnel state with no DB home, so
        # every restart zeroed them and the 22:35 review reported `raw=None` for the whole day (4 of
        # the last 6 trade days). Loaded on the first day roll of this process, flushed back by the
        # drain tick above — the two halves ship together or the flush would overwrite the day's
        # persisted total with this process's smaller one.
        raw_counts_loader=lambda d: read_funnel_raw_counts(conn, d),
        # 2026-08-27: how much better a later candidate must score to take a full cap's slot from an
        # unevaluated incumbent. Owner knob (§6.3) — the reasoning, and the orb-saturation caveat,
        # live with the value in settings.yaml. `null` there disables displacement outright.
        displacement_margin=settings.strategy.prescreen.displacement_margin,
    )
    # 2026-08-04: dedupe/caps day-state is process memory — rehydrate it from the day-slot journal
    # so a restart no longer resets the 20/day bound (observed: ~54 publications across two
    # mid-session restarts) or re-sends already-evaluated candidates.
    _hydrate_prescreen(conn, prescreen, clock.today())
    bus.subscribe("bar.1m", prescreen.handle_bar)

    # 2026-08-18: the scan context day-caches `trade_window` (per PROCESS per date), so an owner
    # change mid-session was invisible to every scanner until the next restart. ModeManager has
    # always published `trade_window.changed` — only telegram and the API relay listened. Wiring the
    # scan provider to it is what stops `orb` intersecting a window that no longer exists (live
    # 2026-08-18: cache built 09:52:06 holding 10:00–10:30, owner moved it to 10:10–15:30 at
    # 09:57:52, orb fired 10:00:05–10:01:07 against the stale copy and burned its whole 6-slot
    # sub-cap nine minutes before the real window opened).
    from engine.risk.events import TOPIC_TRADE_WINDOW

    async def _refresh_scan_window(_event) -> None:
        scan_provider.invalidate_trade_window()

    bus.subscribe(TOPIC_TRADE_WINDOW, _refresh_scan_window)

    # --- Tier-1 jobs (all fail to no-output, never blocking — D7/E5) ---
    # §6.5 envelope_state read at boot, same policy as `edge_multiple_min` above (re-read at boot;
    # intra-run promotions land Phase 5). load_cat_params ignores every non-`cat` key, so the whole
    # mapping goes in as-is; empty ⇒ None ⇒ the envelope.yaml/settings.yaml defaults.
    _envelope_state = {
        r["parameter"]: r["value"] for r in conn.execute("SELECT parameter, value FROM envelope_state").fetchall()
    }
    digest_job = CatalystDigestJob(store, clock, calendar, protected_store, envelope=_envelope_state or None)
    scoring_job = (
        NewsScoringJob(store, resolver, assembler, harness, agent_defs, governor, clock, calendar)
        if harness is not None and "news_analyst" in agent_defs else None
    )
    planner_job = (
        PreopenPlannerJob(store, conn, assembler, harness, agent_defs, governor, clock, calendar,
                          notify=alert)
        if harness is not None and "preopen_planner" in agent_defs else None
    )
    nightly_job = (
        NightlyReviewJob(protected_store, conn, assembler, harness, agent_defs, governor, clock,
                         calendar, notify=notify,
                         # WO-9: raw scanner output is the one funnel number that is not journaled
                         # (it is not a decision) — read it from the live pre-screen counters.
                         funnel_raw=lambda d: prescreen.raw_counts(d))
        if harness is not None and "nightly_reviewer" in agent_defs else None
    )

    # --- ticker subprocess supervisor (started into WARMING at step 7 when a token exists) ---
    # calendar+notify power the in-session tick-silence guard (§7.1): a tickless-but-heartbeating child
    # used to read HEALTHY all session (2026-07-22) — now it degrades visibly + alerts during market hours.
    ticker = TickerSupervisor(
        settings, clock, bus,
        symbol_for_token=instruments.symbol_for_token, calendar=calendar, notify=notify,
        # The WS URL needs the api_key alongside the access token; omitting it left the default ""
        # and every websocket upgrade 400-rejected forever (2026-07-23 root cause, zero ticks ever).
        api_key=session.api_key() or "",
    )

    # ------------------------------------------------------------------ watchlist helpers
    def watchlist_symbols() -> list[str]:
        """Today's active intraday watchlist (latest ``universe_daily`` snapshot ≤ today, included)."""
        d = clock.today()
        for _ in range(_WATCHLIST_LOOKBACK_DAYS):
            rows = store.get_universe_daily(d, included_only=True)
            if rows:
                return [r["symbol"] for r in rows]
            d = d - timedelta(days=1)
        return []

    def held_symbols() -> list[str]:
        """Open platform/recommended position symbols — MUST stay in the feed even after the universe
        drops them (2026-07-28 review: an unsubscribed holding marks at avg_entry, so its loss is
        invisible to the §7.1 floor ladder and day-MTM rungs)."""
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM positions WHERE state='OPEN' "
            "AND origin IN ('platform','recommended')"
        ).fetchall()
        return [str(r["symbol"]) for r in rows]

    def ticker_tokens() -> list[int]:
        """Ticker subscription set: watchlist + HELD symbols + NIFTY 50 + India VIX → tokens (A3)."""
        out: list[int] = []
        seen: set[str] = set()
        for sym in [*watchlist_symbols(), *held_symbols(), INDEX_SYMBOL, VIX_SYMBOL]:
            if sym in seen:
                continue
            seen.add(sym)
            tok = instruments.token_for_symbol(sym)
            if tok is not None:
                out.append(tok)
        return out

    # =========================================================================================
    # §10.1/§4.4 JOB REGISTRY — the single inventory driving BOTH the live scheduler and the
    # startup CatchUpRunner (watermark identities are the JOB_* constants; never rename).
    # =========================================================================================
    def _require_kite() -> KiteClient:
        if kite is None:
            raise RuntimeError("Kite client unavailable (login required) — safety job cannot run (§2.6)")
        return kite

    async def job_instruments() -> None:
        await instruments.refresh(_require_kite())
        # Persist today's dump so a restart after this job (whose watermark makes catch-up skip it) can
        # hydrate the token map pre-login instead of an unknown_token storm (§4.3, F1). Surveillance/MIS
        # columns are left NULL here — the 08:20 surveillance job owns that join.
        today = clock.today()
        rows = instruments.snapshot_rows(today)
        persisted = await store.arun(store.upsert_instruments_daily, rows)
        # §3.2.4 alias SEED from this dump's company names (idempotent upsert; curated rows live under
        # their own keys). Production ran with an EMPTY entity_aliases table until 2026-08-03 — every
        # headline no-matched — because nothing called this; the daily job now owns it, which also
        # tracks renames/new listings.
        seeded = await store.arun(resolver.seed_aliases, rows)
        curated = await store.arun(
            resolver.seed_curated_aliases, load_yaml(config_dir() / "aliases.yaml")
        )
        _log.info("instruments_persisted", d=today.isoformat(), rows=persisted,
                  aliases_seeded=seeded, aliases_curated=curated)

    async def job_surveillance() -> None:
        await surveillance.refresh()

    async def job_earnings() -> EarningsCalendarResult:
        # Forwarded (2026-08-13): earnings_calendar degrades-without-raising (E5) — the watermark
        # verdict needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await earnings.run(clock.today())

    async def job_universe() -> None:
        await leverage.refresh()          # 08:15/08:20 inputs re-read on catch-up (self-refresh, §3.2.4)
        await surveillance.current()
        before = set(watchlist_symbols())
        await universe_builder.build(clock.today())
        # Re-point the feed AND the warm-up coverage set at today's universe (2026-07-28 review: both
        # were frozen at boot, so a pre-08:30 start ran the whole day on YESTERDAY's watchlist).
        warmup_gate.set_symbols(watchlist_symbols())
        try:
            if ticker.health().state != "STOPPED":
                await ticker.update_subscriptions(ticker_tokens())
        except Exception:  # noqa: BLE001 - a resubscribe failure degrades to the old set, never fails the job
            _log.exception("ticker_resubscribe_failed")
        # Symbols ENTERING the watchlist mid-session have no session bars yet, and the boot/post-login
        # gap fill ran against the PREVIOUS set (2026-07-29: SWIGGY/TITAN entered 15 s after the fill
        # and their 09:15→09:53 hole kept warm-up FROZEN all day). Fill the newcomers' minutes now —
        # the gate is already watching them. Pre-open builds skip this (nothing missed yet).
        added = [s for s in watchlist_symbols() if s not in before]
        session = calendar.session(clock.today())
        if added and backfill is not None and session is not None and clock.now() > session.open:
            try:
                gap = await backfill.warmup_gap(added, session.open, clock.now())
                _log.info("universe_added_gap_filled", symbols=added, bars=gap.bars_written)
            except Exception:  # noqa: BLE001 - a failed fill leaves the gate blocking (fail closed), never fails the job
                _log.exception("universe_added_gap_fill_failed", symbols=added)

    # ONE writer through the news chain at a time (2026-07-28 review): the three per-feed polls fire
    # on independent intervals and the scorer writes whole cluster rows back — un-serialized, a poll
    # updating a cluster between the scorer's read and its write-back gets clobbered by the stale
    # snapshot (cluster assignment is also read-modify-write). Volumes are tiny; a lock is free.
    news_chain_lock = asyncio.Lock()

    async def resolve_news(headlines: list, *, alert_on_timeout: bool = False) -> None:
        if not headlines:
            return

        async def _chain() -> None:
            touched = await clusterer.run(headlines)
            await resolver.aload(clock.today())
            await resolver.run(touched)

        # Bounded (2026-08-10): the 12:38 boot's resolve hung 8+ h and wedged the whole startup.
        # The owner alert fires only from the chain/catch-up path (review round: every per-feed
        # poll shares this closure — a persistent hang would page once per poll per 600 s forever;
        # the poll path degrades to the helper's own error log).
        await resolve_news_bounded(
            news_chain_lock, _chain,
            on_timeout=(lambda: alert(
                "warning",
                f"news resolve timed out after {_NEWS_RESOLVE_TIMEOUT_S:.0f}s — chain skipped; "
                "unclustered headlines re-sweep on the next run (E5, never entry-blocking)",
            )) if alert_on_timeout else None,
        )

    async def score_news(*, force: bool = False) -> None:
        if scoring_job is None:
            return
        async with news_chain_lock:
            await scoring_job.run_batch(force=force)

    async def job_news_chain() -> None:
        # §4.4 job 10 startup/catch-up: backfill → cluster → resolve (never entry-blocking, §2.7),
        # then the §5.4 pre-open scoring batch (force=True: score ALL unscored regardless of the ≥8
        # minimum) so the ~08:35 digest sees today's scores. Scoring failure never blocks the chain.
        # Unclustered re-sweep (2026-08-10): a bounded/cancelled resolve leaves inserted-but-unlinked
        # rows — fold them into this run's batch so an abandoned backlog is retried, never orphaned.
        inserted = await news_ingest.backfill()
        seen = {h.headline_id for h in inserted}
        # Capped slice (review round): clustering is O(headlines × window clusters), so an uncapped
        # re-sweep after repeated timeouts grows the next attempt past its own deadline forever.
        # get_news orders by published_at ⇒ oldest-first: every run makes bounded forward progress.
        orphans = [
            Headline(**{k: r[k] for k in ("headline_id", "title", "source_domain", "url", "published_at")})
            for r in await store.arun(
                store.get_news,
                published_after=clock.now() - timedelta(days=4), unclustered_only=True,
            )
            if r["headline_id"] not in seen
        ][:500]
        if orphans:
            _log.info("news_orphans_reswept", orphans=len(orphans))
        await resolve_news(inserted + orphans, alert_on_timeout=True)
        try:
            await score_news(force=True)
        except Exception:  # noqa: BLE001 - unscored clusters just stay off the watchlist (§2.7)
            _log.exception("preopen_scoring_batch_failed")

    async def job_catalyst_digest() -> None:
        # §4.4 job 14 (~08:35): emits CATALYST_WATCHLIST on every SUCCESSFUL run — including an
        # empty-but-fresh (0, 0); a failed run alerts CATALYST_DISABLED and re-raises so the
        # watermark records the failure (§2.7 fail-safe ladder, chaos case 20 convention).
        from engine.notify.catalog import catalyst_disabled, catalyst_watchlist

        try:
            result = await digest_job.run(clock.today())
        except Exception as exc:
            await notify(catalyst_disabled(f"digest failed: {exc}"))
            raise
        await notify(catalyst_watchlist(result.n_originating, result.n_context))

    async def job_preopen_planner() -> AdvisoryRun:
        if planner_job is None:
            # A FAILED watermark, not a silent success: catch-up retries once the roster is fixed
            # (2026-08-03: the sonnet-5 roster failure made this a 29 ms "success" no-op all day).
            raise RuntimeError("preopen planner unavailable — LLM roster quarantined/unloaded")
        # WO-14 (c): translate the advisory tri-state into the ok-bearing watermark verdict here, in
        # the wrapper — governor-blocked is a CORRECT outcome (success watermark, no retry), only a
        # harness failure is retryable. ``_job_result_ok`` stays unwidened for bare bools.
        return AdvisoryRun(await planner_job.run(clock.today()))

    async def job_reco_expire() -> None:
        # §3.6: expired-unconfirmed recommendations become labelled no_action rows (unbiased non-fill
        # signal); aged tracked positions get their §7.1 max_holding exit recommendations.
        expired = book.expire_stale(clock.now())
        if expired:
            _log.info("recommendations_expired", count=expired)
        if pipeline is not None:
            await pipeline.check_aged_positions(clock.today())

    async def job_nightly_review(d) -> AdvisoryRun:
        if nightly_job is None:
            raise RuntimeError("nightly reviewer unavailable — LLM roster quarantined/unloaded")
        # WO-14 (c), as job_preopen_planner: blocked ⇒ success watermark (the governor said no on
        # purpose), harness-failed ⇒ failed watermark ⇒ the next sweep retries — and that retry is
        # itself governor-gated inside NightlyReviewJob.run, so the governor bounds the spend.
        return AdvisoryRun(await nightly_job.run(d))

    async def job_sector_map() -> SectorMapResult:
        # Forwarded (2026-08-13): sector_map degrades-without-raising (E5) — the watermark verdict
        # needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await sector_map.run(clock.today(), universe_symbols=watchlist_symbols())

    async def job_corp_actions() -> CorpActionsResult:
        # Forwarded (2026-08-13): corp_actions degrades-without-raising (E5) — the watermark verdict
        # needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await corp_actions.run(clock.today())

    async def job_backup() -> None:
        await _snapshot_backup(conn, settings, clock)

    async def job_bhavcopy(d) -> BhavcopyResult:
        # Forwarded (2026-08-13): bhavcopy degrades-without-raising (E5) — the watermark verdict
        # needs the real ok/degraded outcome, not a swallowed None (§ composition-root closure gap).
        return await bhavcopy.run(d)

    async def job_deals(d) -> DealsResult:
        # Forwarded (2026-08-13): deals degrades-without-raising (E5) — the watermark verdict needs
        # the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await deals.run(d)

    async def job_filings_pit(d) -> FilingsPitResult:
        # Forwarded (2026-08-13): filings_pit degrades-without-raising (E5) — the watermark verdict
        # needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await filings_pit.run(d)

    async def job_filings_pit_fresh(d) -> FilingsPitFreshResult:
        # Forwarded (2026-08-13): filings_pit_fresh degrades-without-raising (E5) — the watermark
        # verdict needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await filings_pit_fresh.run(d)

    async def job_filings_results(d) -> FilingsResultsResult:
        # Forwarded (2026-08-13): filings_results degrades-without-raising (E5) — the watermark
        # verdict needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await filings_results.run(d)

    async def job_filings_shp() -> FilingsShpResult:
        # Forwarded (2026-08-13): filings_shp degrades-without-raising (E5) — the watermark verdict
        # needs the real ok/degraded outcome, not a swallowed None (composition-root gap).
        return await filings_shp.run()

    async def job_ins_crossings(d) -> InsCrossingsResult:
        # §6.1 `ins`: ok-bearing (E5 — never raises). ok=False means the day could not be EVALUATED
        # (no universe row / no daily bar), so the watermark sinks and the §2.6 sweep retries it;
        # zero crossings on an evaluated day is ok=True — a real answer, not a failure.
        return await ins_crossings.run(d)

    async def job_reconcile(d) -> None:
        if reconcile is not None:
            await reconcile.run(d)

    async def job_daily_bars(d) -> None:
        # §4.4 job 3 nightly incremental backfill: today's daily bar for the watchlist + regime symbols.
        if backfill is not None:
            symbols = [*watchlist_symbols(), INDEX_SYMBOL, VIX_SYMBOL]
            await backfill.run(symbols, "day", d, d)

    async def job_features(d) -> None:
        await asyncio.to_thread(features.daily_snapshot, d)

    async def job_tick_compact(d) -> TickCompactionResult:
        # §4.3/WO-7: collapse the closed tick partitions up to ``d`` into one file per symbol-day.
        # Off the event loop (DuckDB + a large filesystem walk, §2.2) and on its OWN connection —
        # never the live MarketStore's, which is the bar/tick write path. Today's partition is
        # skipped inside compact_ticks (the writer still owns it). Ok-bearing: a failed symbol-day
        # sinks the watermark and the next sweep retries only what did not compact.
        result = await asyncio.to_thread(
            compact_ticks, settings.parquet_dir(), upto=d, today=clock.today()
        )
        # WO-23: the §4.5 retention sweep runs HERE, right after the nightly compaction — see
        # ``apply_tick_retention``. It never changes this job's ok-bearing result.
        await apply_tick_retention(store, result)
        return result

    registry = build_job_registry(settings, {
        JOB_INSTRUMENTS: job_instruments,
        JOB_SURVEILLANCE: job_surveillance,
        JOB_EARNINGS: job_earnings,
        JOB_UNIVERSE: job_universe,
        JOB_NEWS_CHAIN: job_news_chain,
        JOB_CORP_ACTIONS: job_corp_actions,
        JOB_SECTOR_MAP: job_sector_map,
        JOB_BACKUP: job_backup,
        JOB_RECONCILE: job_reconcile,
        JOB_BHAVCOPY: job_bhavcopy,
        JOB_DAILY_BARS: job_daily_bars,
        JOB_DEALS: job_deals,
        JOB_FEATURES: job_features,
        JOB_FILINGS_PIT: job_filings_pit,
        JOB_FILINGS_PIT_FRESH: job_filings_pit_fresh,
        JOB_FILINGS_RESULTS: job_filings_results,
        JOB_FILINGS_SHP: job_filings_shp,
        JOB_INS_CROSSINGS: job_ins_crossings,
        JOB_TICK_COMPACT: job_tick_compact,
        # Phase-2 (§8.3): digest always (deterministic, $0); planner/nightly/expire register even
        # when the LLM tier is down — their fns no-op internally so the watermark records the skip.
        JOB_CATALYST_DIGEST: job_catalyst_digest,
        JOB_PREOPEN_PLANNER: job_preopen_planner,
        JOB_RECO_EXPIRE: job_reco_expire,
        JOB_NIGHTLY_REVIEW: job_nightly_review,
    })

    # =========================================================================================
    # OPS: freeze seam, warm-up gate, heartbeat, catch-up (registry), self-test, lifecycle.
    # =========================================================================================
    async def freeze_entries(reason: str) -> None:
        # Through the cause ledger (§3.5.3 single-writer discipline, 2026-07-28 review): a direct
        # risk_state write is invisible to clear_cause re-arms and the warm-up lift.
        if not kill.is_killed():
            await latch.set_cause(reason, RiskState.FROZEN, reason, Actor.RISK_GATE)

    async def clear_entries_cause(reason: str) -> None:
        # Mirror of freeze_entries (2026-08-06): a safety-critical job verified fresh clears its
        # own latched data_freshness cause through the same ledger (clear_cause recomputes and is
        # idempotent on inactive causes) — without this a pre-login catch-up failure held FROZEN
        # for the whole session after the post-login re-run had already succeeded.
        await latch.clear_cause(reason, Actor.RISK_GATE)

    # §2.6/R6 mid-day token-death circuit breaker: the KiteClient (built above with
    # on_token_rejected=session.on_token_rejected) fires this hook on the FIRST TokenException of a
    # burst (SessionManager.on_token_rejected is idempotent) so entries FREEZE and the owner is
    # alerted to re-login — the invalidation seam that was never wired before 2026-07-21.
    async def _on_session_invalidated() -> None:
        await freeze_entries("kite_token_rejected")
        await alert(
            "critical",
            "Kite token rejected by broker — entries FROZEN; re-login via the login link or /token (R6)",
        )
        # A tappable link, not just a notice: this path also covers the probe-'inconclusive'-then-dead
        # boot (network flake at probe time), where NO login prompt was sent at startup. The hook only
        # fires via the KiteClient (which exists ⇒ api_key exists), so login_url() cannot raise here.
        await notify(login_prompt(session.login_url()))
    session.set_invalidation_hook(_on_session_invalidated)

    async def _clear_token_freeze() -> None:
        # §3.5.3 auto-recovery class: a token-invalid freeze clears on successful re-login. The ledger
        # resolves — any OTHER active cause (owner_pause, floor rung, warm-up) keeps the state.
        await latch.clear_cause("kite_token_rejected", Actor.RISK_GATE)

    session.add_login_hook(_clear_token_freeze)

    warmup_gate = WarmupGate(
        store, clock, calendar,
        symbols=watchlist_symbols(), index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL,
    )
    heartbeat = HeartbeatWriter(settings.sqlite_path(), clock, interval_s=settings.lifecycle.heartbeat_write_s)
    catch_up = CatchUpRunner(conn, clock, calendar, registry, freeze=freeze_entries, notify=notify,
                             clear=clear_entries_cause,
                             # WO-15 (i): boot replays load-bearing data steps only; POST_ARM_JOB_IDS
                             # fire after scheduler.start() (rollback: DEFER_POST_ARM_JOBS = False).
                             deferred=POST_ARM_JOB_IDS if DEFER_POST_ARM_JOBS else ())

    self_test = SelfTest(
        conn=conn, clock=clock, settings=settings, secrets=secrets,
        protected_store=protected_store, kill_switch=kill, mode_manager=mode, session_manager=session,
        catch_up=catch_up, warmup_gate=warmup_gate,
        exposure=exposure, limits_engine=limits_engine, latch=latch,
        sdk_smoke=(None if harness is None else (lambda: run_sdk_smoke(harness))),
        calendar=calendar,
        roster_quarantined=roster_quarantined, roster_loaded=len(agent_defs),
    )
    # In-session OS keep-awake (2026-07-23 sleep/resume wedge): keeps Windows from auto-sleeping while
    # the NSE session is open (the display may still sleep). Driven off the always-on health loop below.
    keep_awake = KeepAwake(enabled=settings.ticker.keep_awake_in_session)
    def _prescreen_funnel_today() -> tuple[int, int, int | None]:
        # Origination-liveness probe (2026-09-01): today's prescreen slot count, forward events that
        # reached the analyst, and the governor's live daily forward cap. `forwarded` is the
        # LLM-reach marker (`evaluated` is re-armed to 0 on age-out, so a drought day reads
        # all-zeros there — see prescreen_day_slots semantics in pipeline.py); it is day-cumulative,
        # which is why the monitor stalls on "no PROGRESS", not on "zero". A cap read failure
        # degrades to None (the monitor then alarms only on the zero-forwarded shape).
        row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(forwarded), 0) FROM prescreen_day_slots WHERE d = ?",
            (clock.today().isoformat(),),
        ).fetchone()
        try:
            cap: int | None = int(governor.prescreen_forward_cap())
        except Exception:  # noqa: BLE001 - the cap refines the alarm; its absence must not kill it
            cap = None
        return (int(row[0]), int(row[1]), cap)

    health = HealthMonitor(
        clock, settings, ticker_supervisor=ticker, alert=alert,
        calendar=calendar, keep_awake=keep_awake,
        # WO-24b-prime: the store stall watchdog rides the always-on health pulse — the one timer
        # that kept beating through the 2026-08-21 freeze while every store path was wedged.
        store=store,
        # Origination liveness (2026-09-01, catchup_safety_jobs latch incident: FROZEN entries for
        # two whole sessions, zero pages): entries_frozen_in_session + funnel_zero_in_session ride
        # the same always-on pulse and the WO-25b episode cadence.
        mode_manager=mode, latch=latch, funnel_probe=_prescreen_funnel_today,
    )
    scheduler = Scheduler(clock, calendar)
    boot_state["scheduler"] = scheduler   # WO-25c: the watchdog reads its REAL running state

    # --- §2.6 injected recovery hooks (steps 4 & 7) ---
    async def backfill_hook() -> None:
        # Step 4: warm-up the regime history (NIFTY50/VIX daily — checkpointed, cheap on re-runs) and
        # gap-fill today's intraday minute bars from official candles so warm-up never needs live ticks.
        # Shared with the post-login re-trigger so both issue the IDENTICAL calls (engine.ops.post_login).
        # 2026-07-21: a token-less (or expired-token) boot must NOT issue doomed historical calls — the
        # live probe makes token_valid() truthful, and PostLoginRecovery re-runs this SAME hook the
        # moment a valid token arrives.
        if backfill is None or not session.token_valid():
            _log.info("backfill_hook_skipped", reason="no valid Kite token")
            return
        await regime_and_warmup_backfill(
            backfill, clock, calendar, settings, watchlist_symbols, INDEX_SYMBOL, VIX_SYMBOL
        )

    async def ticker_resume_hook() -> None:
        # Step 7: resume the ticker into WARMING (feed-stale alarms suppressed) once a token exists.
        # Extracted to engine.ops.post_login so startup and the post-login re-trigger share the exact
        # same resume logic rather than duplicating it.
        await resume_ticker(session, kite, ticker, ticker_tokens)

    async def backup_hook() -> None:
        # §10.5 backup is best-effort — a failure must never block the §10.8 shutdown guard (whose job
        # is capital protection, not housekeeping); swallow here so the STOPPED commit still proceeds.
        try:
            await _snapshot_backup(conn, settings, clock)
        except Exception:  # noqa: BLE001 - best-effort backup never blocks a clean stop
            _log.exception("shutdown_backup_failed")

    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=settings,
        mode_manager=mode, kill_switch=kill, self_test=self_test, catch_up=catch_up,
        alert=alert, notify=notify, build_version=_build_version(),
        heartbeat=heartbeat, warmup_gate=warmup_gate, latch=latch,
        boot_history_path=data_dir / "lifecycle_boots.json",
        backfill_hook=backfill_hook, ticker_resume_hook=ticker_resume_hook, backup_hook=backup_hook,
    )

    # --- §2.6 post-login RE-TRIGGER: a boot BEFORE the daily login is token-less, so the step-4/6/7
    #     broker-touching recovery no-ops and (until this fix) NOTHING re-ran it when the token arrived —
    #     the ticker never started (zero live 1m bars ever captured), warm-up stayed frozen. The
    #     PostLoginRecovery re-runs those steps (each guarded + logged) the moment EITHER login path
    #     mints a valid token; it registers as a SessionManager login hook fired fire-and-forget so the
    #     login HTTP/Telegram path returns at once and the recovery reports its outcome on Telegram. ---
    post_login_recovery = PostLoginRecovery(
        instruments=instruments, store=store, kite=kite, session=session, clock=clock,
        calendar=calendar, settings=settings, backfill=backfill, ticker=ticker, lifecycle=lifecycle,
        ticker_tokens=ticker_tokens, watchlist_symbols=watchlist_symbols,
        index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL, notify=notify, alert=alert,
    )
    session.add_login_hook(post_login_recovery.run)

    # --- dashboard API ---
    app = _create_app(session, mode, kill, secrets, clock, bus, conn=conn,
                      protected_store=protected_store, exposure=exposure, governor=governor,
                      limits_engine=limits_engine, market_store=store)
    if not secrets.has(DASHBOARD_TOKEN):
        _log.warning("dashboard_token_missing", hint="run scripts/dpapi_set.py --generate-dashboard-token")

    # --- §7.1 platform-equity minute persist + continuous floor-ladder evaluation ("evaluated on
    #     every minute-persist"): in-session only; a breached rung applies its FULL action through
    #     the cause latch (state + downgrade + kill where specified). ---
    async def equity_tick() -> None:
        now = clock.now()
        day_session = calendar.session(now.date())
        if day_session is None or not (day_session.open <= now <= day_session.close):
            return
        exposure.persist_snapshot()
        try:
            table = limits_engine.load()
        except IntegrityError:
            return   # unverifiable limits are the self-test/kill path's problem, not this loop's
        breaches = exposure.evaluate_floors(floor_limits_from(table))
        if breaches:
            await exposure.apply_floor_breaches(
                breaches, mode, kill, latch=latch,
                alert=lambda m: alert("critical", m),
            )
        # §7.1 daily-loss rungs (2026-07-28 review: −7% hard halt had no enforcement locus).
        rungs = exposure.evaluate_day_loss(
            Decimal(str(table.limits.daily_loss_soft.day_mtm_pct)),
            Decimal(str(table.limits.daily_loss_hard.day_mtm_pct)),
        )
        if rungs:
            await exposure.apply_day_loss(rungs, mode, latch,
                                          alert=lambda m: alert("critical", m))

    # --- §5.4 in-session/evening scoring cadence: run_batch() self-gates on the scoring windows,
    #     the ≥8/30-min batch trigger and the governor — this loop only provides the pulse. ---
    async def scoring_tick() -> None:
        await score_news()

    # --- §5.2(c) heartbeat pulse: cadence owned by the governor (20 min at DG0, 45 at DG1, off at
    #     DG2+); pipeline.heartbeat() itself enforces in-window + admission, this loop the spacing. ---
    hb_holder: dict[str, Any] = {"last": None}

    async def heartbeat_tick() -> None:
        if pipeline is None:
            return
        interval_min = governor.heartbeat_interval_min()
        if interval_min is None:
            return
        now = clock.now()
        last = hb_holder["last"]
        if last is not None and (now - last).total_seconds() < interval_min * 60:
            return
        hb_holder["last"] = now
        await pipeline.heartbeat()

    # --- §5.2(a) paced forward drain (2026-08-14): one-minute pulse; pipeline.drain_forward_queue()
    #     owns BOTH the FORWARD_PACING_MIN cadence and the in-window/mode/kill/governor/cap
    #     admission, exactly as heartbeat() owns its own. This loop only provides the pulse, so the
    #     `immediate` rollback needs no scheduler change — the drain self-disables in that mode. ---
    async def forward_drain_tick() -> None:
        if pipeline is None:
            return
        try:
            await pipeline.drain_forward_queue()
        except Exception:  # noqa: BLE001 - a drain failure must never take down the scheduler loop
            _log.exception("forward_drain_tick_failed")

    # --- refresh the gate's warm-up snapshot alongside the equity cadence (fail-closed until set),
    #     LIFT a standing warm-up freeze once coverage completes (see refresh_and_lift_warmup), and
    #     SELF-HEAL persistent intraday coverage holes (2026-08-06 seam hole: a login-lagged boot's
    #     gap fill ended before live ticks began — one bar missing in all 100 symbols, warm-up
    #     unliftable without a manual restart). ---
    _gap_repair_state: dict = {}

    async def warmup_refresh() -> None:
        await refresh_and_lift_warmup(warmup_gate, warmup_holder, mode, lifecycle)
        status = warmup_holder.get("status")
        if status is not None and backfill is not None:
            await maybe_repair_warmup_gaps(
                status, _gap_repair_state, clock=clock, calendar=calendar,
                repair=lambda frm, to: backfill.warmup_gap(watchlist_symbols(), frm, to),
                token_valid=session.token_valid,
            )

    # --- periodic missed-job sweep (2026-07-28): boot-time catch-up cannot help a machine that SLEEPS
    #     through a fire slot and resumes without a restart — 2026-07-27 slept 17:56→evening, missed
    #     18:05 daily_bars, and warm-up then blocked the whole next session on the absent bar. The
    #     runner's watermarks make a repeat pass a no-op, so sweeping on a cadence is safe. ---
    async def catchup_sweep() -> None:
        try:
            # ALL scope (WO-15): the sweep is the retry path for the post-arm one-shots too — a
            # news chain / digest / planner / compaction run that failed after arming is swept here
            # exactly like any other missed job. Single-flight makes a sweep landing on top of a
            # still-running pass a logged no-op rather than a double replay.
            result = await catch_up.catch_up(scope=CatchUpScope.ALL)
            await _reconcile_catchup_freeze(result, latch, kill)
        except Exception:  # noqa: BLE001 - the sweep must never take down the scheduler loop
            _log.exception("catchup_sweep_failed")

    # --- on-demand scanner sweep (§3.2.5 addendum, owner-directed 2026-07-29): the answer to "what
    #     could I trade right now, and at what price would today's setups arm?" Runs when the trade
    #     window becomes ACTIVE and on /scan_now. Live candidates re-enter the NORMAL pipeline path
    #     (dedupe/caps intact); the verdict is never silence. ---
    async def run_scan_sweep(trigger: str) -> str:
        now = clock.now()
        session_day = calendar.session(now.date())
        if session_day is None or not (session_day.open <= now <= session_day.close):
            return "no session in progress — the sweep reads live bars; try during market hours"

        def _collect_and_scan():
            latest = []
            for sym in watchlist_symbols():
                tail = store.get_bars_1m(sym, session_day.open, now)
                if tail:
                    latest.append(tail[-1])
            accepted, pendings = prescreen.sweep(latest)

            # --- brk20 daily leg (2026-08-04, owner-directed after the BPCL miss): completed-
            #     daily-bar breakouts over the FULL ELIGIBLE universe — watchlist_cap symbols have
            #     no 1m bars, so the bar-driven scanners can never see them. Admission goes through
            #     prescreen.admit so the §3.2.5 dedupe/caps bind identically; a second sweep the
            #     same day re-admits nothing.
            today = now.date()
            # --- Trade-window gate for the BATCH legs (2026-08-18). The bar-driven leg above gates
            #     itself off BAR time inside the pre-screen (which stays Clock-free for §9.6); these
            #     candidates carry no bar, so the sweep — which HAS the Clock — decides, applying the
            #     same test as `pipeline.on_signal_candidate`. Without it a /scan_now outside the
            #     window spends unrefundable day slots on candidates the pipeline is guaranteed to
            #     drop as `signal_candidate_out_of_window`. The window-open sweep is unaffected: it
            #     fires ON the INACTIVE→ACTIVE edge, so the window is open by construction.
            try:
                _w = calendar.trade_window(today)
                batch_in_window = _w[0] <= now <= _w[1]
            except ValueError:
                batch_in_window = False      # not a trading day ⇒ no window ⇒ nothing originates (R6)
            # 2026-09-01 refactor: the eligibility predicate lives in ONE place now
            # (store.get_universe_eligible_symbols) — this was one of three inline duplicates of the
            # strict reasons==['watchlist_cap'] equality that the batch-universe addendum would have
            # silently missed. Semantics unchanged: brk20/ins stay on the ELIGIBLE (gate-approvable)
            # set; only the shadow hi52 leg below scans the wider batch universe.
            eligible = store.get_universe_eligible_symbols(today)
            histories: dict[str, list[brk20.DailyRow]] = {}
            hist_start = today - timedelta(days=70)   # comfortably ≥ lookback+2 sessions
            yesterday = today - timedelta(days=1)     # completed sessions only, never today's forming bar
            for sym in eligible:
                frame = store.get_bars_1d_frame(sym, hist_start, yesterday)
                if len(frame):
                    histories[sym] = [
                        brk20.DailyRow(high=float(h), close=float(c), volume=float(v), open=float(o))
                        for h, c, v, o in zip(
                            frame["high"], frame["close"], frame["volume"], frame["open"],
                            strict=True,  # columns of ONE frame — a length mismatch is corrupt data
                        )
                    ]
            ex_map: dict[str, list[date]] = {}
            # Horizon = the WIDEST ex_skip_days of every rule that reuses this map (2026-09-01
            # review: a hardcoded brk20 horizon would silently under-fetch for hi52 if either
            # default is ever retuned — the veto then sees an empty list and fires anyway).
            _ex_horizon = max(
                int(brk20.DEFAULT_PARAMS["ex_skip_days"]),
                int(hi52.DEFAULT_PARAMS["ex_skip_days"]),
            )
            for row in store.get_corp_actions(
                ex_from=today,
                ex_to=today + timedelta(days=_ex_horizon),
            ):
                if row.get("ex_date") is not None:
                    ex_map.setdefault(row["symbol"], []).append(row["ex_date"])
            brk20_vetoes: dict[str, int] = {}
            brk20_raw = brk20.sweep_daily(
                histories, today=today, ex_dates_by_symbol=ex_map, veto_counts=brk20_vetoes
            )
            # WO-19 veto visibility (the `cat` line's discipline, §6.1): the stop-geometry floor
            # refuses candidates that used to ship, so a run where it eats everything must be
            # readable as a floor decision rather than as an empty tape. Counts are ORIGINATION-
            # stage — pre-admission, so candidates + vetoes reconcile against symbols_scanned on one
            # line; what the §3.2.5 caps then do with the survivors is the prescreen's own logging.
            _log.info(
                "brk20_sweep", d=today.isoformat(), trigger=trigger,
                symbols_scanned=len(histories),
                candidates=len(brk20_raw),
                gap_floor_vetoes=brk20_vetoes.get(brk20.VETO_GAP_FLOOR, 0),
                floor_unavailable=brk20_vetoes.get(brk20.VETO_FLOOR_UNAVAILABLE, 0),
            )

            # --- `ins` daily leg (§6.1 addendum, owner-directed 2026-08-17): the crossings last
            #     night's ins_crossings job journalled into ins_pending. Same batch shape as brk20 —
            #     admitted through prescreen.admit so the §3.2.5 dedupe/caps bind identically — but
            #     the EVENT was decided EOD by the validated crossing function; nothing is re-derived
            #     here. Rows are marked consumed as part of admission, so a second sweep the same day
            #     (or a sweep after a mid-session restart) re-admits nothing: the flag is persisted
            #     state, not process memory.
            ins_pending = _read_ins_pending(conn, today)
            ins_raw: list = []
            if ins_pending:
                ins_raw = ins.sweep_crossings(
                    ins_pending,
                    params={
                        "stop_pct": settings.ins.stop_pct,
                        "hold_sessions": settings.ins.hold_sessions,
                        "threshold_inr": settings.ins.threshold_inr,
                    },
                )

            # --- `cat` v2 SHADOW leg (§2.7 amendment, owner-directed 2026-08-18 — WO-18): today's
            #     `originating` catalyst_watchlist rows, graded by the ~08:35 digest, on the SAME
            #     ins-shaped batch path. No consumed flag is needed for once-only: the age<=1 filter
            #     inside sweep_watchlist IS the single-shot rule (an age-2 re-grade of the same story
            #     never re-originates), and a second sweep the same day hits the §3.2.5 dedupe.
            #     Downstream, the §7.1 C3 check rejects every one of these by construction — no
            #     cat.expected_edge_pct exists — so ADMISSION here is the shadow's validation
            #     population, not RECOMMEND. An absent/empty digest simply yields no rows (§2.7
            #     fail-safe ladder: cat originates nothing, every other strategy unaffected).
            cat_rows, cat_rev_rows = _read_cat_watchlist(store, today)
            # 2026-09-01 review (§3.2.4 widening): the news layer now grades the BATCH universe, but
            # BOTH cat legs' origination stays pinned to the ELIGIBLE (index) set — their WO-18/§2.7
            # verdict populations and clocks are FROZEN (restarted 2026-08-28, verdict ~mid-Oct), and
            # an extended-symbol candidate would both skew that population (different cost/liquidity
            # class) and burn the shared 2/day catalyst budget an index story may need. Extended
            # symbols' originating-grade rows still journal in catalyst_watchlist (analyzable later);
            # they just never become cat/cat_reversal candidates inside the frozen window.
            _eligible_set = set(eligible)
            cat_rows = _watchlist_rows_for_symbols(cat_rows, _eligible_set)
            cat_rev_rows = _watchlist_rows_for_symbols(cat_rev_rows, _eligible_set)
            cat_raw: list = []
            if cat_rows:
                cat_raw = cat.sweep_watchlist(
                    cat_rows,
                    params={
                        "stop_pct": settings.cat.stop_pct,
                        "hold_sessions": settings.cat.hold_sessions,
                    },
                )

            # --- `cat_reversal` SHADOW leg (§2.7, 2026-08-27 — the HINDZINC denial): the SUBSET of
            #     today's originating rows whose winning cluster REVERSES an earlier, floor-clearing,
            #     opposite-direction cluster of the same (symbol, event_type) story. A separate
            #     experiment from `cat` v2 with its own thresholds, its own T+5/T+10 clock and its own
            #     journal series — `cat`'s in-flight shadow window is untouched, which is also why a
            #     reversal row deliberately still originates for `cat` as well (narrowing `cat` would
            #     restart its clock). Both legs' entries share the ONE
            #     catalyst_guard.max_catalyst_entries_day budget — the pre-screen keys that cap off
            #     catalyst_ref, not strategy_id — so adding this strategy widens no exposure surface.
            #     Downstream the §7.1 C3 check rejects every one of these UNCONDITIONALLY (registered
            #     in the gate's no_edge_shadow_strategies, so an analyst-supplied target cannot buy it
            #     an edge basis either): ADMISSION here is the shadow's validation population.
            cat_rev_raw: list = []
            if cat_rev_rows:
                cat_rev_raw = cat_reversal.sweep_watchlist(
                    cat_rev_rows,
                    params={
                        "stop_pct": settings.cat_reversal.stop_pct,
                        "hold_sessions": settings.cat_reversal.hold_sessions,
                    },
                )

            # --- ONE ranked admission across all three batch legs (2026-08-18). Until now brk20,
            #     ins and cat each made their OWN prescreen.admit call, and WO-1's ranking orders
            #     only WITHIN one batch — so the legs were effectively first-come, in source order,
            #     and cat ran last. Live 2026-08-18: brk20 OBEROIRLTY (0.552) and BHEL (0.523) took
            #     the day's final slots and cat LT (0.82) was suppressed on the day cap 94 ms later.
            #     That is unrecoverable, not merely unlucky: `cat` is single-shot by construction
            #     (cat.MAX_EVENT_AGE_SESSIONS = 1), so a story that loses the race can never
            #     re-originate at age 2. Concatenating first lets _rank see all three legs as one
            #     batch and a binding cap keeps the best of them, whatever leg produced it.
            #     Snapshot mint stays AFTER admit (dedupe/caps first — a suppressed candidate never
            #     spends a snapshot write); without it the analyst's Rule 6 refuses every batch
            #     candidate unseen.
            batch = _attach_feature_snapshots(
                features,
                prescreen.admit(
                    brk20_raw + ins_raw + cat_raw + cat_rev_raw, today,
                    in_window=batch_in_window,
                ),
            )
            if ins_pending and batch_in_window:
                # Consume EVERY row read, not just the admitted ones: a row suppressed by the daily
                # cap or the (symbol, strategy) dedupe has HAD its evaluation — leaving it unconsumed
                # would re-offer it on the next sweep tick forever. Suppression is a decision.
                # A WINDOW refusal is NOT (2026-08-18 review finding): the whole premise of the
                # window gate is that a shut-window candidate was never evaluated, so an
                # out-of-window /scan_now must leave the day's crossings pending for the next
                # in-window sweep rather than silently destroying them. (Before the combined-admit
                # rewrite this same path consumed the rows anyway — the loss was pre-existing;
                # the gate is what makes not-consuming correct.)
                _consume_ins_pending(conn, today, [c.symbol for c in ins_pending], now=now)
            # THE starvation-visibility line (the `ins` convention, §6.1): sustained zeros must be
            # readable as "the news layer went quiet" vs "rows arrived and the rule/caps declined
            # them" — WO-18 pre-registers <0.2 signals/session for 3 weeks as a STARVATION finding,
            # which is only detectable if the three counts sit on one line.
            # `cat_reversal` shares this line rather than adding a second one: its own starvation
            # criterion (pre-registered in scanners/cat_reversal.py's docstring) is only checkable
            # against the originating flow it is a subset OF, and splitting the counts across two log
            # events is how that comparison stops being greppable.
            _log.info(
                "cat_watchlist_sweep", d=today.isoformat(), trigger=trigger,
                originating_rows=len(cat_rows),
                age_eligible=sum(1 for r in cat_rows if cat.is_eligible(r)),
                candidates=sum(1 for c in batch if c.strategy_id == cat.STRATEGY_ID),
                reversal_eligible=sum(1 for r in cat_rev_rows if cat_reversal.is_eligible(r)),
                reversal_candidates=sum(
                    1 for c in batch if c.strategy_id == cat_reversal.STRATEGY_ID
                ),
                in_window=batch_in_window,
            )

            # --- `hi52` SHADOW leg (§3.2.4 extended-leg + §6.1 addendum, owner-directed 2026-09-01
            #     after the JINDALSAW/movers review): 52-week-high-proximity FRESH-CROSSES over the
            #     BATCH universe — criteria-passing non-index names included, the WELCORP/DYCL class
            #     the index-scoped scanners structurally never see. Swing thesis (T+5..T+20, George
            #     & Hwang drift; intraday capture is cost-refuted). C3 rejects every candidate
            #     UNCONDITIONALLY (no_edge_shadow_strategies): ADMISSION is the shadow's validation
            #     population, pending the backtest + §8.6 owner gate. Placement is load-bearing
            #     (2026-09-01 review, two findings): the leg runs AFTER the actionable admit — its
            #     ~800-symbol × 400-day history fetch never delays the admission race (the 08-18
            #     94 ms lesson), and its own SECOND admit call means shadow candidates whose scores
            #     cluster in [0.95, 1] take LEFTOVER day-cap capacity only, never an actionable
            #     leg's slot. window_open-only: the signal derives from COMPLETED sessions, so one
            #     scan per session is its natural cadence and /scan_now stays cheap (a mid-day
            #     restart re-fires window_open on the next INACTIVE→ACTIVE edge, so coverage holds).
            hi52_admitted: list = []
            if trigger == "window_open":
                hi52_histories: dict[str, list[brk20.DailyRow]] = {}
                hi52_start = today - timedelta(days=400)   # ≥252 sessions + weekend/holiday margin
                for sym in store.get_batch_universe_symbols(today):
                    frame = store.get_bars_1d_frame(sym, hi52_start, yesterday)
                    if len(frame):
                        hi52_histories[sym] = [
                            brk20.DailyRow(
                                high=float(h), close=float(c), volume=float(v), open=float(o)
                            )
                            for h, c, v, o in zip(
                                frame["high"], frame["close"], frame["volume"], frame["open"],
                                strict=True,
                            )
                        ]
                hi52_vetoes: dict[str, int] = {}
                hi52_raw = hi52.sweep_daily(
                    hi52_histories, today=today, ex_dates_by_symbol=ex_map,
                    veto_counts=hi52_vetoes,
                )
                hi52_admitted = _attach_feature_snapshots(
                    features,
                    prescreen.admit(hi52_raw, today, in_window=batch_in_window),
                )
                _log.info(
                    "hi52_sweep", d=today.isoformat(), trigger=trigger,
                    symbols_scanned=len(hi52_histories),
                    candidates=len(hi52_raw),
                    admitted=len(hi52_admitted),
                    ex_date_vetoes=hi52_vetoes.get(hi52.VETO_EX_DATE_SKIP, 0),
                )

            return accepted + batch + hi52_admitted, pendings

        accepted, pendings = await asyncio.to_thread(_collect_and_scan)
        for cand in accepted:
            await bus.apublish("signal.candidate", cand)

        def _distance(p) -> float:
            if p.trigger_price is None or p.last_price is None or p.last_price == 0:
                return float("inf")
            return abs(float(p.trigger_price) - float(p.last_price)) / float(p.last_price)

        held = set(held_symbols())
        live_rows = [
            {
                "symbol": c.symbol, "side": c.side, "strategy_id": c.strategy_id, "style": c.style,
                "entry": str(c.raw_levels.entry),
                "stop": None if c.raw_levels.stop is None else str(c.raw_levels.stop),
                "target": None if c.raw_levels.target is None else str(c.raw_levels.target),
                "held": c.symbol in held,
            }
            for c in accepted
        ]
        pending_rows = [
            {
                "symbol": p.symbol, "side": p.side, "strategy_id": p.strategy_id, "style": p.style,
                "trigger": str(p.trigger_price), "arms_when": p.arms_when,
                "last": None if p.last_price is None else str(p.last_price),
                "stop": None if p.stop_price is None else str(p.stop_price),
                "target": None if p.target_price is None else str(p.target_price),
                "exit_rule": p.exit_rule, "held": p.symbol in held,
            }
            for p in sorted((p for p in pendings if p.trigger_price is not None), key=_distance)[:10]
        ]
        msg = catalog.scan_sweep(
            trigger=trigger, live=live_rows, pending=pending_rows,
            suppressed_today=prescreen.seen_today(),
        )
        _log.info("scan_sweep_done", trigger=trigger, published=len(live_rows),
                  pending=len(pending_rows), suppressed=prescreen.seen_today())
        if trigger != "scan_now":       # /scan_now gets the body as its direct reply — no double send
            await notify(msg)
        return msg.body

    # Fire the sweep on the window-INACTIVE→ACTIVE edge (covers both the daily window-open moment
    # and an owner moving/extending the window onto "now").
    _window_active = {"was": False}

    async def window_sweep_tick() -> None:
        now = clock.now()
        session_day = calendar.session(now.date())
        window = mode.get_trade_window()
        active = bool(
            session_day is not None and window is not None
            and session_day.open <= now <= session_day.close
            and window.start <= now.time() <= window.end
            and mode.mode() in (Mode.RECOMMEND, Mode.AUTO)
        )
        was, _window_active["was"] = _window_active["was"], active
        if active and not was:
            try:
                await run_scan_sweep("window_open")
            except Exception:  # noqa: BLE001 - a sweep failure must never take down the scheduler
                _log.exception("window_open_sweep_failed")

    if telegram is not None:
        telegram.set_scan_sweep_fn(run_scan_sweep)

    # --- arm the schedule (calendar-guarded, R6) BEFORE recovery so a late startup still fires today ---
    _arm_registry_jobs(scheduler, registry, catch_up, clock)
    # WO-21 (iii): pre-open token probe at 08:40. Wired only when a broker facade exists (no api_key
    # ⇒ nothing to authenticate); NOT a registry job — a pre-open check has no meaningful catch-up.
    if kite is not None:
        _arm_token_check(
            scheduler,
            TokenCheckJob(kite=kite, clock=clock, calendar=calendar, notify=notify,
                          login_url=session.login_url),
        )
    _arm_live_jobs(scheduler, settings, bar_builder, health, news_ingest, resolve_news,
                   ticker=ticker, calendar=calendar, clock=clock, equity_tick=equity_tick,
                   scoring_tick=scoring_tick, heartbeat_tick=heartbeat_tick,
                   warmup_refresh=warmup_refresh, catchup_sweep=catchup_sweep,
                   window_sweep_tick=window_sweep_tick, forward_drain_tick=forward_drain_tick)

    # --- bring the owner alert channel up BEFORE recovery so startup notifications + any alert raised
    #     during recovery actually reach the owner instead of being dropped 'not_started' (§3.2.11). ---
    if telegram is not None:
        await telegram.start()

    # --- BIND THE LOGIN CALLBACK API *BEFORE* startup recovery (2026-07-21 lockout). The owner's only
    #     browser login route (GET /kite/callback, :8400) is hosted by THIS uvicorn server, and it used
    #     to bind only AFTER lifecycle.startup(). A boot on an expired token then wedged inside the
    #     warm-up backfill (every historical call TokenException) and the port never bound — login was
    #     locked out both ways. The app is already fully wired (session_manager in app.state), so
    #     /kite/callback works the instant the socket binds; a login mid-startup just fires the
    #     idempotent PostLoginRecovery hook. _serve_api binds the socket ITSELF (uvicorn's own bind
    #     paths sys.exit(1), which would kill the loop) and returns None on failure — the engine
    #     CONTINUES either way (Telegram /token still permits the daily login). ---
    server_task = await _serve_api(app, settings)
    _bound = getattr(server_task, "_mt_server", None)
    if _bound is None or not _bound.started:
        await alert(
            "critical",
            f"dashboard/login API failed to bind {settings.api.host}:{settings.api.port} — "
            "browser login unreachable; use Telegram /token",
        )

    # --- LIVE token probe (Fix-1, R6/A5): behavioural token_valid() answers True on a stale-but-not-yet-
    #     rejected token, so a boot on yesterday's expired token passed the self-test and ground the
    #     warm-up backfill on a dead token (2026-07-21). Probe it live ONCE here so token_valid() is
    #     truthful for the hydrate + startup below, and send the login link the instant we KNOW the token
    #     is rejected/absent (guarded on kite so login_url() never raises on a fresh, api-key-less install). ---
    probe = await session.verify_token()
    _log.info("token_probe", outcome=probe)
    login_prompt_sent = False
    if probe in ("rejected", "absent") and telegram is not None and kite is not None:
        await notify(login_prompt(session.login_url()))
        login_prompt_sent = True

    # --- F2 cold-start token-map recovery: rebuild the in-memory instruments index (from the persisted
    #     snapshot pre-login, or a live refresh) BEFORE the §2.6 step-4 backfill + step-6 warm-up run
    #     inside lifecycle.startup — otherwise a restart after 08:15 finds an empty map (unknown_token).
    #     session_valid here is now the LIVE-probed truth (above), not a stale behavioural guess. ---
    try:
        instruments_source = await hydrate_instruments_at_startup(
            instruments, store, kite, session_valid=session.token_valid(), clock=clock,
        )
    except Exception:  # noqa: BLE001 - a recovery step must never crash the boot (§2.6): degrade to
        # an empty token map (entries stay FROZEN via the warm-up gate) with a loud, named cause.
        _log.exception("instruments_hydrate_failed")
        instruments_source = "failed"

    # --- every-startup recovery (§2.6). check_skew honours NTP; degrades to FROZEN if unreachable (R6).
    #     Boot-phase safety ticks run alongside it (2026-08-07): OBSERVATION-ONLY — the warm-up
    #     SNAPSHOT (never the lift/repair; see boot_phase_ticks) + the health/keep-awake pulse must
    #     not wait out a news-backlog-sized catch-up; cancelled AND awaited before startup handling
    #     continues, so no tick body overlaps the post-boot warmup_refresh. ---
    _boot_ticks = asyncio.create_task(
        boot_phase_ticks(
            lambda: refresh_warmup_snapshot(warmup_gate, warmup_holder),
            lambda: health.check(check_skew=False),
        )
    )
    try:
        report = await lifecycle.startup(check_skew=True)
    finally:
        _boot_ticks.cancel()
        try:
            await _boot_ticks
        except asyncio.CancelledError:
            pass
    _log.info("startup_complete", mode=report.sticky_mode, killed=report.killed,
              needs_login=report.needs_login, integrity_ok=report.integrity_ok,
              jobs_caught_up=len(report.jobs_caught_up), frozen=report.frozen_reasons,
              instruments=instruments_source)
    # Boot-scoped skew verdict for the gate's §7.1 clock_skew rule (self-test measured it above);
    # seed the warm-up snapshot immediately so the gate isn't blind until the first 60s refresh.
    skew_holder["ok"] = "clock_skew" not in report.frozen_reasons
    # WO-25c ROOT CAUSE. These two seeding refreshes used to be bare awaits sitting between
    # startup_complete and scheduler.start(), and on 2026-08-24 the 12:44:58 mid-session boot parked
    # in the first one FOREVER: the ticker came up 1.5 s earlier, the tick flush loop began taking
    # MarketStore._lock a few hundred times per flush (one COPY per (date, symbol) partition —
    # store.flush_ticks), and WarmupGate._evaluate's ~400 sequential to_thread hops for the same lock
    # never got through. The scheduler was never armed; no forward drain, no health pulse, no EOD job
    # fired for 11 h. WO-15 established the invariant — nothing unbounded ahead of scheduler.start() —
    # but only moved the CATCH-UP behind it; these two awaits were left in front and are the same bug.
    #
    # The seeding is a convenience, never a safety property: an unseeded warm-up holder reads
    # _WARMUP_UNREFRESHED (fail-CLOSED, both §7.1 readiness rules blocking) and the 60 s
    # warmup_status_refresh / health_check jobs re-seed both the moment the scheduler is up. So the
    # correct trade is a hard deadline and carry on — losing a snapshot costs one fail-closed minute,
    # losing the scheduler costs the trading day.
    await seed_boot_snapshots(
        warmup_refresh, lambda: health.check(check_skew=False), alert=alert,
    )

    # --- start remaining services + idle until a stop signal (§2.6: being up is an active period). The
    #     login API is already bound (above); only prompt again here if startup still needs a login AND we
    #     did not already send the link off the token probe (no double link). ---
    # WO-15 (i)+(iii): arm the scheduler FIRST, then fire the never-load-bearing one-shots behind it
    # as a background task. engine_ready (below) must not wait on the news chain — a wedged chain now
    # costs the digest, not the whole scheduled day (2026-08-10). Its own 600 s resolve cap bounds it.
    post_arm_task = start_scheduler_and_fire_post_arm(scheduler, catch_up, clock, calendar)
    if telegram is not None and report.needs_login and not login_prompt_sent:
        await notify(login_prompt(session.login_url()))

    # stop_event + signal handlers were installed EARLY (right after the instance lock) so a wedged boot
    # is still interruptible; here we simply idle on it until the first signal requests a graceful stop.
    _log.info("engine_ready", host=settings.api.host, port=settings.api.port, mode=mode.mode().value)
    boot_state["engine_ready"] = True   # WO-25c: the other half of the boot contract
    await stop_event.wait()

    # --- graceful shutdown (§2.6/§10.8 shutdown guard): flatten in-flight bars, stop the ticker, run
    #     the lifecycle guard (backup + STOPPED commit + heartbeat join), then tear down the rest. ---
    _log.info("engine_stopping")
    await cancel_post_arm(boot_watchdog)      # WO-25c: retire the contract watchdog BEFORE the
    # scheduler stops — otherwise a slow teardown re-reads is_running()==False and pages a violation
    # for an engine that is deliberately shutting down.
    scheduler.shutdown()                      # no new job fires can race the teardown
    await cancel_post_arm(post_arm_task)      # a still-running post-arm one-shot never blocks a stop
    bar_builder.flush_all()                   # finalize any open minute bars (EOD/shutdown, §4.4 job 1)
    await ticker.stop()
    await lifecycle.shutdown()                # runs backup hook, commits STOPPED, joins the heartbeat
    if telegram is not None:
        await telegram.stop()
    await _stop_api(server_task)
    await http.aclose()
    store.close()
    conn.close()
    instance_lock.release()   # cosmetic tidiness — every non-clean exit is covered by the kernel
    # releasing the OS lock on process death; this just frees it promptly on a graceful stop.
    _log.info("engine_stopped")
    return 0


# NOTE: ``hydrate_instruments_at_startup`` (the F2 cold-start ladder) now lives in
# ``engine.ops.post_login`` so the composition root AND the post-login re-trigger share the exact same
# ladder; it is imported above and re-exported here (existing callers/tests keep importing it from
# ``engine.ops.main``).


# --------------------------------------------------------------------------- boot tail (WO-15)
def post_arm_exclusions(clock: Clock, calendar: NSECalendar) -> tuple[str, ...]:
    """Post-arm one-shots this boot must NOT fire, given WHEN the boot happened (WO-21 (ii)).

    Only ``tick_compact`` is ever vetoed, and only for a boot landing inside a live trading session
    (:data:`_IN_SESSION_START_IST`..:data:`_IN_SESSION_END_IST` on an NSE trading day). Every other
    post-arm job is unchanged: the news chain / digest / planner are pre-open work that a
    mid-session recovery boot still wants done, whereas compaction competes with the tick writer for
    exactly the resources the session needs (2026-08-20 11:26 IST — see the constants above).

    Non-trading day (weekend / holiday) inside the same clock window ⇒ no veto: there is no session
    to protect, and a Saturday recovery boot is precisely when the backlog SHOULD be collapsed.
    """
    now = clock.now()
    if not calendar.is_trading_day(now.date()):
        return ()
    if not (_IN_SESSION_START_IST <= now.time() <= _IN_SESSION_END_IST):
        return ()
    _log.info("post_arm_skipped_in_session", job_id=JOB_TICK_COMPACT, now=now.isoformat())
    return (JOB_TICK_COMPACT,)


def start_scheduler_and_fire_post_arm(
    scheduler: Scheduler, catch_up: CatchUpRunner, clock: Clock, calendar: NSECalendar
) -> asyncio.Task | None:
    """Arm the scheduler, THEN fire the deferred one-shots behind it — never the other way round.

    The §2.6 boot order before WO-15 was: catch-up (news chain inside it) → scheduler.start(). The
    2026-08-10 wedge proved the cost: a chain that never returns starves every scheduled job for 8 h,
    including the 30-min catchup sweep that exists to self-heal. Reversing the two makes the chain's
    worst case local to itself.

    The one-shot IS a catch-up pass (``DEFERRED`` scope): identical code, identical ``job_runs``
    watermarks, identical dependency order — only the firing point moved. A run that fails records a
    FAILED watermark and the next sweep (``ALL``) retries it. It is deliberately NOT awaited: the
    caller logs ``engine_ready`` immediately after, and that is the invariant WO-15 (iii) demands.
    Returns the task (``None`` when the rollback flag is off / nothing is deferred) so shutdown can
    cancel it; a crash inside is logged, never raised into the boot path.

    WO-21 (ii): :func:`post_arm_exclusions` decides, from the boot's own wall clock, which one-shots
    this boot must skip — today only the in-session ``tick_compact``.
    """
    scheduler.start()
    if not (DEFER_POST_ARM_JOBS and POST_ARM_JOB_IDS):
        return None
    exclude = post_arm_exclusions(clock, calendar)
    fired = [j for j in POST_ARM_JOB_IDS if j not in exclude]

    async def _fire() -> None:
        try:
            result: CatchUpResult = await catch_up.catch_up(
                scope=CatchUpScope.DEFERRED, exclude=exclude
            )
            _log.info("post_arm_jobs_complete", jobs=fired,
                      caught_up=result.jobs_caught_up, failed=result.jobs_failed,
                      skipped_in_flight=result.skipped_in_flight)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - these jobs are never entry-blocking (§2.6/§2.7)
            _log.exception("post_arm_jobs_failed")

    _log.info("post_arm_jobs_fired", jobs=fired, skipped=list(exclude))
    return asyncio.create_task(_fire(), name="post_arm_catchup")


async def cancel_post_arm(task: asyncio.Task | None) -> None:
    """Cancel + join the post-arm one-shot on shutdown (nothing it does is worth waiting out; its
    watermarks make the next boot resume exactly where it stopped)."""
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):  # noqa: BLE001 - teardown never raises
        pass


# --------------------------------------------------------------------------- scheduler arming
def _arm_registry_jobs(
    scheduler: Scheduler, registry: JobRegistry, catch_up: CatchUpRunner, clock: Clock
) -> None:
    """Arm every registry job on the live scheduler (§10.1). Each fire records the ``job_runs``
    watermark so the CatchUpRunner never re-runs a job the live scheduler already ran today."""
    for spec in registry.specs():
        fire = _scheduled_runner(spec, catch_up, clock)
        if spec.job_id == JOB_SECTOR_MAP:
            # Weekly Sunday cadence (not a trading day) — a plain day-of-week cron, calendar-unguarded.
            scheduler.add_job(
                fire,
                trigger=CronTrigger(day_of_week="sun", hour=spec.at.hour, minute=spec.at.minute, timezone=IST),
                job_id=spec.job_id, guard=False,
            )
        else:
            scheduler.add_trading_day_job(fire, hour=spec.at.hour, minute=spec.at.minute, job_id=spec.job_id)


def _arm_token_check(scheduler: Scheduler, job: TokenCheckJob) -> None:
    """Arm the WO-21 (iii) pre-open token probe — one calendar-guarded daily fire at 08:40 IST.

    Deliberately NOT routed through ``_scheduled_runner``: that wrapper records a ``job_runs``
    watermark, and a watermark is what makes a job catch-up-eligible. A pre-open check replayed
    hours later is meaningless, so this job stays outside the registry entirely (see
    :mod:`engine.ops.token_check`). ``TokenCheckJob.run`` never raises, so no failure wrapper is
    needed here either.
    """
    scheduler.add_trading_day_job(
        job.run, hour=TOKEN_CHECK_IST.hour, minute=TOKEN_CHECK_IST.minute, job_id="token_check",
    )


def _scheduled_runner(spec: JobSpec, catch_up: CatchUpRunner, clock: Clock):
    async def _fire() -> None:
        today = clock.today()
        try:
            if spec.job_class == JobClass.DATE_KEYED:
                outcome = await spec.run(today)  # type: ignore[call-arg]
            else:
                outcome = await spec.run()       # type: ignore[call-arg]
            if not _job_result_ok(outcome):
                # degraded return = failure for the watermark; the job already alerted its own
                # failure (E5) — no new alert from the runner.
                _log.warning("scheduled_job_degraded", job_id=spec.job_id)
                catch_up.record_run(spec.job_id, today, status="failed")
                return
            catch_up.record_run(spec.job_id, today)
        except Exception:  # noqa: BLE001 - a scheduled job failure records + alerts, never crashes the loop
            _log.exception("scheduled_job_failed", job_id=spec.job_id)
            catch_up.record_run(spec.job_id, today, status="failed")

    return _fire


def _arm_live_jobs(
    scheduler: Scheduler, settings, bar_builder: BarBuilder, health: HealthMonitor,
    news_ingest: NewsIngest, resolve_news,
    *, ticker: TickerSupervisor, calendar: NSECalendar, clock: Clock, equity_tick=None,
    scoring_tick=None, heartbeat_tick=None, warmup_refresh=None, catchup_sweep=None,
    window_sweep_tick=None, forward_drain_tick=None,
) -> None:
    """Interval jobs that run continuously while the engine is up (not calendar-gated): the coarse
    bar-finalization timer, the state-aware health check, the per-feed news poll cadences (§4.4 job 10),
    and the periodic in-session ``feed_stats`` observability line (R8)."""

    async def _advance_bars() -> None:
        bar_builder.advance()

    async def _health() -> None:
        await health.check(check_skew=False)

    async def _feed_stats() -> None:
        # In-session only (no all-night noise; the 22:57 STALE in the 2026-07-22 log is expected). One
        # INFO line so an operator can see AT A GLANCE whether ticks/bars are flowing: the tickless
        # session would have shown ticks_received=0, bars_written=0 every 5 minutes.
        now = clock.now()
        session = calendar.session(now.date())
        if session is None or not (session.open <= now <= session.close):
            return
        ts = ticker.stats_snapshot()
        bs = bar_builder.stats_snapshot()
        _log.info(
            "feed_stats",
            ticks_received=ts["ticks_received"], frames_dropped=ts["frames_dropped"],
            bars_finalized=bs["bars_finalized"], bars_written=bs["bars_written"],
            # WO-25a: late-tick pressure, and how much of it reached DuckDB. late_ticks >>
            # late_store_calls is the in-memory range check absorbing them (healthy); the two
            # climbing together means real amendments — or the recent-bars window being missed.
            late_ticks=bs["late_ticks"], late_store_calls=bs["late_store_calls"],
            feed_state=ticker.health().state,
        )

    def _news_poll(feed: str):
        async def _poll() -> None:
            await resolve_news(await news_ingest.poll(feeds=(feed,)))
        return _poll

    scheduler.add_job(_advance_bars, trigger=IntervalTrigger(seconds=5), job_id="bar_advance", guard=False)
    scheduler.add_job(_feed_stats, trigger=IntervalTrigger(seconds=settings.ticker.feed_stats_interval_s),
                      job_id="feed_stats", guard=False)
    scheduler.add_job(_health, trigger=IntervalTrigger(seconds=settings.lifecycle.watchdog_poll_s),
                      job_id="health_check", guard=False)
    # One job per configured RSS feed (§3.2.4): the feed set is settings, not code, so adding a
    # source is a config_audit'd settings.yaml edit and the scheduler picks it up at the next boot.
    for name, feed in settings.news.feeds.rss.items():
        scheduler.add_job(_news_poll(name), trigger=IntervalTrigger(seconds=feed.poll_s),
                          job_id=f"news_poll_{name}", guard=False)
    scheduler.add_job(_news_poll("gdelt"), trigger=IntervalTrigger(seconds=settings.news.gdelt_poll_s),
                      job_id="news_poll_gdelt", guard=False)
    if equity_tick is not None:
        # §7.1: platform equity persisted each minute + the halt ladder evaluated on every persist.
        scheduler.add_job(equity_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="equity_tick", guard=False)
    if scoring_tick is not None:
        # §5.4 cadence pulse — run_batch() self-gates (windows, batch trigger, governor).
        scheduler.add_job(scoring_tick, trigger=IntervalTrigger(seconds=300),
                          job_id="news_scoring_tick", guard=False)
    if heartbeat_tick is not None:
        # §5.2(c) — one-minute pulse; the governor-owned interval gates the actual call.
        scheduler.add_job(heartbeat_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="analyst_heartbeat_tick", guard=False)
    if warmup_refresh is not None:
        # Gate readiness snapshot (§7.1 warmup_ready/regime_data_ready) — fail-closed until first run.
        scheduler.add_job(warmup_refresh, trigger=IntervalTrigger(seconds=60),
                          job_id="warmup_status_refresh", guard=False)
    if catchup_sweep is not None:
        # Missed-job sweep for sleep/resume gaps (watermark-deduped ⇒ idempotent; see wiring note).
        scheduler.add_job(catchup_sweep, trigger=IntervalTrigger(seconds=1800),
                          job_id="catchup_sweep", guard=False)
    if window_sweep_tick is not None:
        # §3.2.5 sweep addendum (2026-07-29): fire the scanner sweep on the trade-window
        # INACTIVE→ACTIVE edge — the "what could I trade right now?" verdict is never silent.
        scheduler.add_job(window_sweep_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="window_sweep_tick", guard=False)
    if forward_drain_tick is not None:
        # §5.2(a) — one-minute pulse; the pipeline's FORWARD_PACING_MIN cadence gates the drain.
        scheduler.add_job(forward_drain_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="forward_drain_tick", guard=False)


# --------------------------------------------------------------------------- warm-up lift (§2.6/§7.1)
async def _reconcile_catchup_freeze(result: CatchUpResult, latch, kill) -> None:
    """``catchup_safety_jobs`` symmetry on the SWEEP path (2026-09-02 review: the 09-01 fix cleared
    only at boot, so a boot-latched freeze survived a mid-session recovery until the next reboot).

    Same rule as lifecycle step 5: a pass with safety-critical failures latches, a CLEAN pass — the
    re-verification of exactly this cause's predicate — clears. A ``skipped_in_flight`` pass
    verified nothing and leaves the latch alone; so does a killed engine. The clear fires only when
    the cause is actually ACTIVE — ``clear_cause`` is idempotent but logs every call, and an
    unconditional clear would add ~48 no-op WARNING lines a day (review, minor)."""
    if result.skipped_in_flight or kill.is_killed():
        return
    if result.frozen_reasons:
        await latch.set_cause(
            "catchup_safety_jobs", RiskState.FROZEN,
            ",".join(result.frozen_reasons), Actor.RISK_GATE,
        )
    elif any(c == "catchup_safety_jobs" for c, _s, _d in latch.active_causes()):
        await latch.clear_cause("catchup_safety_jobs", Actor.RISK_GATE)


async def refresh_warmup_snapshot(warmup_gate, warmup_holder: dict):
    """Refresh the gate's warm-up snapshot ONLY — no latch mutation, no lift, no repair.

    This is the piece safe to run at ANY time, including mid-``lifecycle.startup()`` (the boot
    ticks, 2026-08-07 review round: a mid-boot LIFT races the catch-up's safety-critical freeze —
    ``_maybe_lift_warmup_freeze`` clears causes the still-running recovery believes are set — and
    a mid-boot REPAIR chases a tail hole the not-yet-started ticker regrows forever, burning the
    day's budget; both belong on the post-boot scheduler cadence). A refresh failure keeps the
    PREVIOUS snapshot (holder untouched ⇒ stays fail-closed). Returns the status, or None."""
    try:
        status = await warmup_gate.status()
        warmup_holder["status"] = status
        return status
    except Exception:  # noqa: BLE001 - an unevaluable warm-up stays NOT-READY (fail closed)
        _log.exception("warmup_status_refresh_failed")
        return None


async def refresh_and_lift_warmup(warmup_gate, warmup_holder: dict, mode, lifecycle) -> None:
    """Refresh the gate's warm-up snapshot AND lift a standing warm-up freeze once coverage completes.

    The lift used to hang off the post-login hook only — a VALID-TOKEN mid-session restart (first
    seen 2026-08-03: boot 12:22 IST, ORB lookbacks ~17 min short) froze entries and nothing ever
    lifted them, because no login event fires on such a boot. Runs on the 60 s
    ``warmup_status_refresh`` cadence; ``reapply_warmup_gate`` resolves through the cause latch
    (never a blanket NORMAL write) and is a no-op unless the state is FROZEN with coverage ready.
    """
    status = await refresh_warmup_snapshot(warmup_gate, warmup_holder)
    if status is None:
        return
    if status.ready and mode.risk_state() == RiskState.FROZEN:
        try:
            await lifecycle.reapply_warmup_gate()
        except Exception:  # noqa: BLE001 - a failed lift retries on the next 60s tick
            _log.exception("warmup_freeze_lift_failed")


# --------------------------------------------------------------------------- bounded news resolve (§2.7/E5)
#: 2026-08-10 boot wedge: a post-clustering await inside the resolve chain hung 8+ hours during
#: catch-up — the boot never completed and NOTHING flagged it. The chain is never-load-bearing
#: (E5), so a bounded skip + alert strictly beats a wedged boot. Budget = ~6× the measured
#: worst case (429 weekend headlines × 1,500 window clusters = 90.6 s clustering) plus store hops.
_NEWS_RESOLVE_TIMEOUT_S = 600.0


async def resolve_news_bounded(lock: asyncio.Lock, chain, *, timeout_s: float = _NEWS_RESOLVE_TIMEOUT_S,
                               on_timeout=None) -> bool:
    """Run one news resolve pass under the chain lock with a hard deadline (2026-08-10).

    The deadline covers LOCK ACQUISITION too (a wedged holder must not wedge every later caller).
    On expiry: cancellation releases the lock at the ``async with`` exit, partially-persisted
    clusters stand (idempotent upserts), and inserted-but-unlinked headlines are re-swept by the
    next ``job_news_chain`` run — nothing is lost, the boot/chain just moves on. Returns True on
    completion, False on timeout."""
    try:
        async def _locked() -> None:
            async with lock:
                await chain()
        await asyncio.wait_for(_locked(), timeout=timeout_s)
        return True
    except TimeoutError:
        _log.error("news_resolve_timeout", timeout_s=timeout_s)
        if on_timeout is not None:
            try:
                await on_timeout()
            except Exception:  # noqa: BLE001 - the alert is best-effort; the skip already happened
                _log.exception("news_resolve_timeout_alert_failed")
        return False


# --------------------------------------------------------------------------- boot-phase safety ticks
async def boot_phase_ticks(refresh, health_check, *, interval_s: float = 60.0) -> None:
    """Run the two OBSERVATION-ONLY refreshes on a cadence DURING boot recovery (2026-08-07).

    The scheduler — owner of ``warmup_status_refresh`` and ``health_check`` — starts only after
    ``lifecycle.startup()`` returns, and the catch-up inside it scales with the news backlog (a
    122-cluster scoring batch held the 10:52 boot ~17 min): until this existed the box had no
    health/keep-awake pulse and a stale warm-up snapshot for that whole window. DELIBERATELY
    observation-only (2026-08-07 review round): the callables must be :func:`refresh_warmup_snapshot`
    (never the lifting/repairing ``warmup_refresh`` — a mid-boot lift races the still-running
    recovery's cause state, and a mid-boot repair chases the tail hole the not-yet-started ticker
    regrows, burning the day's repair budget) and the health check (whose ``keep_awake.update()``
    is the real prize on a sleep-prone box). The lift itself costs nothing by waiting: the
    composition root calls the full ``warmup_refresh()`` immediately after startup returns. The
    task is spawned right before recovery and cancelled (and awaited) when the scheduler takes
    over. Sleep-first: a normal boot finishes in well under a tick and never fires. One tick's
    failure never ends the loop (fail-open on observation, fail-closed on state)."""
    while True:
        await asyncio.sleep(interval_s)
        try:
            await refresh()
            await health_check()
        except Exception:  # noqa: BLE001 - observation must keep ticking; CancelledError still propagates
            _log.exception("boot_tick_failed")


# --------------------------------------------------------------------------- boot tail hardening (WO-25c)
#: Budget for the two pre-arm seeding refreshes (warm-up snapshot + first health pulse). Generous
#: against their measured cost — the healthy 2026-08-24 10:41:20 boot did both in 1.66 s — because the
#: point is not to trim a slow boot but to put a CEILING on a wedged one.
_BOOT_SEED_TIMEOUT_S = 45.0


async def seed_boot_snapshots(
    warmup_refresh, health_check, *, timeout_s: float = _BOOT_SEED_TIMEOUT_S, alert=None
) -> bool:
    """Seed the warm-up + health snapshots before arming — under a deadline that CANNOT be missed.

    WO-25c. The 2026-08-24 12:44:58 boot died here: ``await warmup_refresh()`` never returned, so
    ``scheduler.start()`` was never called and the engine ran the whole day with no timers at all.
    The invariant this restores is WO-15's, applied to the last two awaits it did not cover: **the
    boot tail reaches ``scheduler.start()`` no matter what the seeding does.**

    Bulletproofing detail (the reason this is not just ``asyncio.wait_for``): ``wait_for`` cancels the
    inner task and then AWAITS the cancellation, so a coroutine parked on a thread-offload that will
    not come back can wedge the timeout too. :func:`asyncio.wait` returns after ``timeout_s``
    unconditionally — it neither cancels nor joins — so the deadline is real. The straggler is
    cancelled afterwards and deliberately never awaited; the 60 s ``warmup_status_refresh`` /
    ``health_check`` interval jobs re-do this work the moment the scheduler is up.

    Returns True when the seeding completed inside the budget, False on the loud timeout path.
    """
    started = time_module.monotonic()

    async def _seed() -> None:
        await warmup_refresh()
        await health_check()

    task = asyncio.create_task(_seed(), name="boot_seed_snapshots")
    done, _pending = await asyncio.wait({task}, timeout=timeout_s)
    if done:
        exc = task.exception()
        if exc is not None:
            # A RAISING seed is not the incident this guards, but it must not reach the boot path
            # either: the snapshots stay fail-closed and arming proceeds.
            _log.exception("boot_seed_failed", exc_info=exc)
            return False
        return True

    elapsed = time_module.monotonic() - started
    _log.critical(
        "boot_seed_timeout", timeout_s=timeout_s, elapsed_s=round(elapsed, 1),
        hint="warm-up/health seeding did not return (2026-08-24 WO-25c); arming the scheduler anyway "
             "— the gate stays fail-closed until the 60s refresh jobs re-seed it",
    )
    task.cancel()   # fire-and-forget: awaiting it is exactly what we refuse to do
    if alert is not None:
        try:
            await alert(
                "critical",
                f"boot seeding wedged >{timeout_s:.0f}s (warm-up/health snapshot) — scheduler armed "
                "anyway; entries stay FROZEN until the next refresh lands",
            )
        except Exception:  # noqa: BLE001 - the alert is best-effort; arming must not depend on it
            _log.exception("boot_seed_timeout_alert_failed")
    return False


# --------------------------------------------------------------------------- boot contract (WO-25c)
#: How long a boot may take before the contract is checked. The four healthy 2026-08-24 boots reached
#: engine_ready in 22 s / 13 s / 15 s / 15 s — but the watchdog's FIRST live run (2026-08-25 01:26,
#: a crash-recovery boot catching up a full missed EOD day: 14 jobs) took a legitimate 305 s and
#: paged a false positive at 180 s. 420 s clears an honest heavy catch-up boot while still paging a
#: real zombie (the 2026-08-24 one ran ELEVEN HOURS) within the same trading window it broke in.
_BOOT_CONTRACT_DEADLINE_S = 420.0

#: Re-check/re-log cadence while the contract stands violated.
_BOOT_CONTRACT_RECHECK_S = 300.0


def _boot_contract_missing(boot_state: Mapping[str, Any]) -> list[str]:
    """Which half (or both) of the boot contract is unmet: ``engine_ready`` and a RUNNING scheduler."""
    missing: list[str] = []
    if not boot_state.get("engine_ready"):
        missing.append("engine_ready")
    scheduler = boot_state.get("scheduler")
    if scheduler is None or not scheduler.is_running():
        missing.append("scheduler_running")
    return missing


async def boot_contract_watchdog(
    boot_state: Mapping[str, Any],
    *,
    deadline_s: float = _BOOT_CONTRACT_DEADLINE_S,
    recheck_s: float = _BOOT_CONTRACT_RECHECK_S,
) -> None:
    """Verify the boot actually COMPLETED, and page the owner when it did not (WO-25c).

    The 2026-08-24 12:44:58 boot is the case this exists for: ``startup_complete`` logged, ticks
    processed, bars built, features written — every outward sign of a live engine — while APScheduler
    had never been started and no scheduled job fired again until the process was killed at midnight.
    Nothing noticed for 11 hours, because everything that could have noticed was itself a scheduled
    job.

    So this is a PLAIN asyncio task, armed at the top of :func:`run` and owned by nobody else. It must
    keep working when the scheduler is the broken component, which rules out an APScheduler job; the
    owner-notify path is likewise an asyncio send, independent of any trigger.

    Contract: by ``deadline_s`` the boot must have reached ``engine_ready`` AND
    ``Scheduler.is_running()`` must be True — read from APScheduler's own state, never from a flag the
    boot path sets (see :meth:`engine.ops.scheduler.Scheduler.is_running`). Satisfied ⇒ one INFO
    ``boot_contract_ok`` and the task retires. Violated ⇒ CRITICAL ``boot_incomplete`` naming what is
    missing, ONE owner page, then a re-check + re-log every ``recheck_s`` until it resolves (a boot
    that arms late still gets its ``boot_contract_ok``).

    Never crashes the boot: it is detached, and every failure inside it — including a raising alert
    sink — is swallowed. Cancelled at shutdown via :func:`cancel_post_arm`.
    """
    started = time_module.monotonic()
    try:
        await asyncio.sleep(deadline_s)
        paged = False
        while True:
            missing = _boot_contract_missing(boot_state)
            elapsed = round(time_module.monotonic() - started, 1)
            if not missing:
                _log.info("boot_contract_ok", elapsed_s=elapsed)
                return
            _log.critical(
                "boot_incomplete", missing=missing, elapsed_s=elapsed,
                hint="the boot never finished arming (2026-08-24 WO-25c): no scheduled job can fire "
                     "in this state — restart the engine",
            )
            if not paged:
                paged = True   # ONE page per episode; the CRITICAL log carries the repeats
                alert = boot_state.get("alert")
                if alert is not None:
                    try:
                        await alert(
                            "critical",
                            f"BOOT INCOMPLETE after {elapsed:.0f}s — missing: {', '.join(missing)}. "
                            "No scheduled job (forward drains, health pulses, EOD jobs) can fire. "
                            "Restart the engine.",
                        )
                    except Exception:  # noqa: BLE001 - a failed page must not end the watchdog
                        _log.exception("boot_incomplete_alert_failed")
            await asyncio.sleep(recheck_s)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - the watchdog must never take the boot down with it
        _log.exception("boot_contract_watchdog_failed")


# --------------------------------------------------------------------------- warm-up gap self-repair
#: One repair attempt per this window, at most _GAP_REPAIR_MAX_PER_DAY broker-touching attempts per
#: session day: a hole the fill cannot close (no candle upstream — zero-trade minute, halt) will
#: never close by retrying harder, so broker spend must be capped; local-only scans are free.
_GAP_REPAIR_COOLDOWN_S = 300
_GAP_REPAIR_MAX_PER_DAY = 3


def _has_intraday_gap_blockers(blockers: list[str]) -> bool:
    """The intraday minute-bars blocker shape (``orb:<sym> bars have/need``) — the only class the
    warmup_gap re-backfill can heal; daily-bars blockers belong to the daily_bars job/catchup_sweep."""
    return any(b.startswith("orb:") and " bars " in b for b in blockers)


async def maybe_repair_warmup_gaps(status, state: dict, *, clock, calendar, repair,
                                   token_valid=None) -> bool:
    """Re-trigger the §2.6 warm-up gap backfill when intraday coverage holes persist (2026-08-06:
    a login-lagged boot left the 11:24 bar missing in ALL 100 symbols — ``bars 146/147`` with one
    permanent gap, warm-up could never lift, and only a manual restart re-ran the fill).

    Bounds (2026-08-06 review round): in-session only; NEVER on an invalid token (a doomed
    historical call must not fire, and — the review's key finding — must not spend the repair
    budget pre-login, or the budget is gone before the login-lag seam hole even exists);
    ``to`` is trimmed 2 minutes back from ``now`` so the repair never touches minutes the live
    bar builder still owns (the 2026-07-23 provenance-clobber class) and a transient
    just-closed-minute deficit sees ZERO gaps — the fill's per-symbol gap check then skips every
    symbol without a broker call. The :data:`_GAP_REPAIR_MAX_PER_DAY` budget is charged only on
    attempts with actual broker activity (bars written or failed spans); no-op scans are free and
    only paced by :data:`_GAP_REPAIR_COOLDOWN_S`. Returns True iff a repair fired (productive or
    not); the NEXT 60 s ``warmup_status_refresh`` tick observes healed coverage and lifts through
    the normal path — this function never lifts anything itself."""
    if status.ready or repair is None or not _has_intraday_gap_blockers(status.blockers):
        return False
    now = clock.now()
    today = now.date()
    session = calendar.session(today)
    if session is None or not (session.open <= now <= session.close):
        return False
    if state.get("day") != today:
        state.update(day=today, count=0, last=None, exhausted_logged=False, no_token_logged=False)
    if token_valid is not None and not token_valid():
        if not state.get("no_token_logged"):
            _log.warning("warmup_gap_repair_skipped_no_token", blockers=len(status.blockers))
            state["no_token_logged"] = True
        return False
    state["no_token_logged"] = False
    to = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    if to <= session.open:
        return False                       # session too young to have a repairable window yet
    if state["count"] >= _GAP_REPAIR_MAX_PER_DAY:
        if not state.get("exhausted_logged"):
            _log.warning("warmup_gap_repair_exhausted", attempts=state["count"],
                         blockers=len(status.blockers))
            state["exhausted_logged"] = True
        return False
    if state.get("last") is not None and (now - state["last"]).total_seconds() < _GAP_REPAIR_COOLDOWN_S:
        return False
    state["last"] = now
    try:
        report = await repair(session.open, to)
    except Exception:  # noqa: BLE001 - a failed repair leaves the gate blocking (fail closed); the cooldown paces retries
        state["count"] += 1                # spend-safe: an erroring attempt still consumes budget
        _log.exception("warmup_gap_repair_failed", attempt=state["count"])
        return True
    # Budget charges on BROKER SPEND, not on bars landed (review round 2): ``fetched`` is non-empty
    # iff ≥1 real historical call completed — an UNFILLABLE hole (no candle upstream) then still
    # consumes budget instead of resweeping the broker every cooldown all session. Real broker
    # failures also charge (spend-safe); ``unknown_instrument_token`` spans are recorded pre-network
    # (a broken instruments map — post-login's repair, not ours) and are deliberately free.
    fetched = getattr(report, "fetched", ()) or ()
    broker_failures = [
        f for f in (getattr(report, "failed", ()) or ())
        if getattr(f, "error", "") != "unknown_instrument_token"
    ]
    if fetched or broker_failures:
        state["count"] += 1
        _log.warning("warmup_gap_repair", attempt=state["count"],
                     bars_written=getattr(report, "bars_written", 0), fetched=len(fetched),
                     failed=len(broker_failures), blockers=len(status.blockers))
    else:
        # No broker call happened: zero gaps in the trimmed window (the just-closed-minute
        # transient) or instruments-map misses only. Free — paced by the cooldown alone.
        _log.info("warmup_gap_repair_scan_only", blockers=len(status.blockers))
    return True


# --------------------------------------------------------------------------- prescreen day-state (§3.2.5)
def _hydrate_prescreen(conn: sqlite3.Connection, prescreen, today: date) -> None:
    """Rebuild the prescreen's in-memory day state from ``prescreen_day_slots`` at boot (2026-08-04).

    ``charged`` = every pair published today (the caps bound — counts attempts, never refunded);
    ``seen`` = pairs whose slot is spent (``evaluated=1``). A charged-but-unseen pair was lost
    in flight (or re-armed) and may re-publish within its already-paid quota — the 2026-07-29
    rearm semantics, now restart-proof.

    ``catalyst_strategies`` is what lets the pre-screen rebuild the §2.7
    ``catalyst_guard.max_catalyst_entries_day`` budget from the same rows (2026-08-28); the journal
    has no ref column, so the reconstruction and its one fail-open direction are documented at
    :meth:`~engine.strategy.prescreen.SignalPreScreen.hydrate`."""
    rows = conn.execute(
        "SELECT symbol, strategy_id, evaluated FROM prescreen_day_slots WHERE d = ?",
        (today.isoformat(),),
    ).fetchall()
    charged = {(r["symbol"], r["strategy_id"]) for r in rows}
    seen = {(r["symbol"], r["strategy_id"]) for r in rows if r["evaluated"]}
    prescreen.hydrate(today, seen=sorted(seen), charged=sorted(charged),
                      catalyst_strategies=CATALYST_STRATEGY_IDS)
    _log.info("prescreen_hydrated", d=today.isoformat(), charged=len(charged), seen=len(seen))


# --------------------------------------------------------------------------- §6.1 `ins` pending queue
def _read_ins_pending(conn: sqlite3.Connection, today: date) -> list[ins.Crossing]:
    """Today's UNCONSUMED ``ins_pending`` rows (migration 0008) as :class:`ins.Crossing` tuples.

    The EOD ``ins_crossings`` job wrote these last night keyed on ``for_session`` = today. Money
    columns are TEXT and are re-hydrated to ``Decimal`` here (§8.1: money never round-trips through a
    float). A malformed row costs itself and nothing else — the sweep must not die on one bad row."""
    rows = conn.execute(
        "SELECT symbol, crossing_session, trailing_value, contributing_filings_n, reference_close "
        "FROM ins_pending WHERE for_session = ? AND consumed = 0 ORDER BY symbol",
        (today.isoformat(),),
    ).fetchall()
    out: list[ins.Crossing] = []
    for r in rows:
        try:
            out.append(
                ins.Crossing(
                    symbol=str(r["symbol"]),
                    crossing_session=date.fromisoformat(str(r["crossing_session"])),
                    trailing_value=Decimal(str(r["trailing_value"])),
                    contributing_filings_n=int(r["contributing_filings_n"] or 0),
                    reference_close=Decimal(str(r["reference_close"])),
                )
            )
        except (ValueError, ArithmeticError, TypeError) as exc:
            _log.warning("ins_pending_unparseable", symbol=str(r["symbol"]), error=str(exc))
    return out


def _consume_ins_pending(
    conn: sqlite3.Connection, today: date, symbols: list[str], *, now: datetime
) -> None:
    """Mark today's admitted/evaluated ``ins_pending`` rows consumed — the restart-safe once-only bound.

    ``now`` comes from the engine ``Clock`` (§3.2: never ``datetime.now()``). Rows are never deleted:
    ``ins_pending`` is the audit trail of what the EOD job found, and a consumed-but-suppressed
    candidate must stay distinguishable from one that never existed."""
    if not symbols:
        return
    try:
        # Bare execute on the autocommit connection (``isolation_level=None``), matching the other
        # journal writers (``scan_context._write_last_rebalance_d``, ``pipeline._journal_slot``).
        # Deliberately NOT ``transaction()``: this runs on a worker thread against the connection the
        # loop thread also uses, and an explicit BEGIN could land inside one the OMS already opened.
        conn.executemany(
            "UPDATE ins_pending SET consumed = 1, consumed_at = ? "
            "WHERE for_session = ? AND symbol = ? AND consumed = 0",
            [(now.isoformat(), today.isoformat(), sym) for sym in symbols],
        )
    except Exception as exc:  # noqa: BLE001 - journalling is bookkeeping; it never kills the sweep
        _log.warning("ins_pending_consume_failed", d=today.isoformat(), error=str(exc))


# --------------------------------------------------------------------------- §2.7 `cat` watchlist read
#: Calendar days to look back for the prior session's daily bar. Comfortably clears the longest NSE
#: holiday stretch (a long weekend plus a mid-week holiday); no bar in that span means the symbol has
#: no usable committed price, which the scanner turns into no candidate.
_CAT_REF_CLOSE_LOOKBACK_DAYS = 10


#: Strategies in SHADOW mode: the §7.1 C3 cost check rejects them UNCONDITIONALLY, so their signals
#: accumulate a validation population and can never become a recommendation (see ``risk/gate.py``
#: :data:`~engine.risk.gate._SHADOW_NO_EDGE` for why this is declared rather than inferred from a
#: missing ``expected_edge_pct``). A module-level constant so the property is assertable without
#: booting the engine — a wiring that only exists inside ``build_engine`` is a wiring that silently
#: disappears. Removing an id from here is the §8.6 owner promotion gate, never a refactor.
#:
#: ``cat`` joined 2026-08-28: it rested on the SAME indirect argument ``cat_reversal``'s docstring
#: identifies the hole in (ship no ``expected_edge_pct``, rely on C3's targetless branch) — an
#: analyst-volunteered ``target_price`` defeats that argument for ``cat`` exactly as it would have
#: for ``cat_reversal``. Zero ``cat`` recommendations have ever been delivered (queried live), so
#: this closes a real but not-yet-exploited hole rather than fixing an incident.
#: 2026-09-01 (§3.2.4/§6.1 addendum): ``hi52`` joins at birth — a 52wk-high-proximity swing shadow
#: over the BATCH universe with no measured edge until its backtest + §8.6 owner gate. Its shadow
#: status is doubly load-bearing: beyond the no-edge rule, most of its candidates are non-included
#: (extended-leg) symbols the gate could never approve anyway.
NO_EDGE_SHADOW_STRATEGIES: frozenset[str] = frozenset(
    {cat.STRATEGY_ID, cat_reversal.STRATEGY_ID, hi52.STRATEGY_ID}
)

#: Strategies whose candidates can carry a ``catalyst_ref`` — the §2.7 news-originated legs. Read at
#: ONE place: :func:`_hydrate_prescreen`, which uses it to rebuild the
#: ``catalyst_guard.max_catalyst_entries_day`` budget across a restart from a journal that records no
#: refs (see ``SignalPreScreen.hydrate``). Enforcement itself is keyed on the FIELD, never on this
#: set — a strategy id must not be what decides whether the news guard applies — so a leg missing
#: from here still faces the cap while it runs; it only loses budget continuity over a boot.
CATALYST_STRATEGY_IDS: frozenset[str] = frozenset({cat.STRATEGY_ID, cat_reversal.STRATEGY_ID})


def _watchlist_rows_for_symbols(rows: list, symbols: set[str]) -> list:
    """Watchlist rows (cat/cat_reversal ``WatchlistRow`` NamedTuples) whose ``symbol`` is in
    ``symbols`` — the §3.2.4 eligible-pin filter (2026-09-01 review). ATTRIBUTE access, never
    ``.get``: the 2026-09-02 10:51 window_open sweep died on exactly that (``'WatchlistRow' object
    has no attribute 'get'`` — the whole batch admission incl. hi52's first sweep aborted), because
    the filter shipped inline without a test on the real row type. Now a helper, tested with the
    real NamedTuples."""
    return [r for r in rows if r.symbol in symbols]


def _read_cat_watchlist(
    store: MarketStore, today: date
) -> tuple[list[cat.WatchlistRow], list[cat_reversal.WatchlistRow]]:
    """Today's ``originating`` ``catalyst_watchlist`` rows, as BOTH scanners' row tuples.

    The ~08:35 ``CatalystDigestJob`` wrote these; nothing here re-grades one. The ONE thing this adds
    is ``reference_close`` — the PRIOR SESSION's bhavcopy-final close from ``bars_1d`` (the last bar
    strictly before today), which is the freshest committed price at sweep time and the anchor for
    the whole level set (§2.7 2026-08-18 amendment). The grade filter is pushed into the store query
    because it is cheap there; each RULE's own filter (``cat``: grade ∧ direction ∧ age<=1;
    ``cat_reversal``: the same PLUS ``reversal_of``) is re-applied inside its ``sweep_watchlist``,
    which is the authority. A malformed row costs itself and nothing else — the sweep must not die on
    one bad row (§3.2.5 fail-to-zero).

    ONE fetch, two projections (2026-08-27): the ``cat_reversal`` shadow reads exactly the same rows
    and the same per-symbol ``bars_1d`` lookup ``cat`` already does, so building both here keeps the
    added strategy free at the store — a second pass would double the sweep's query count for a leg
    that fires on a strict subset of the same rows. The projections are independent tuples so the two
    pre-registered event definitions can diverge later without a lockstep edit.
    """
    out: list[cat.WatchlistRow] = []
    rev: list[cat_reversal.WatchlistRow] = []
    for r in store.get_catalyst_watchlist(today, grade="originating"):
        symbol = str(r.get("symbol") or "")
        if not symbol:
            continue
        try:
            bars = store.get_bars_1d(
                symbol,
                today - timedelta(days=_CAT_REF_CLOSE_LOOKBACK_DAYS),
                today - timedelta(days=1),
            )
            age = r.get("event_age_sessions")
            materiality = r.get("materiality")
            entry_id = str(r.get("entry_id") or "")
            grade = str(r.get("grade") or "")
            direction = None if r.get("direction") is None else str(r["direction"])
            age_sessions = None if age is None else int(age)
            score = None if materiality is None else float(materiality)
            # No bar in the lookback ⇒ None ⇒ the scanner emits nothing for this symbol.
            reference_close = Decimal(str(bars[-1].close)) if bars else None
            shared_fields = dict(
                entry_id=entry_id,
                symbol=symbol,
                grade=grade,
                direction=direction,
                event_age_sessions=age_sessions,
                materiality=score,
                reference_close=reference_close,
            )
            out.append(cat.WatchlistRow(**shared_fields))
            # `reversal_of` is NULL on every ordinary row and on every row a pre-2026-08-27 DB wrote
            # (the column is nullable and back-filled by no one) — both read as "not a reversal". Such
            # a row can never satisfy ``cat_reversal.is_eligible`` (it requires a NON-EMPTY
            # ``reversal_of``), so it is skipped here rather than built and discarded downstream —
            # harmless either way: the starvation-visibility ``reversal_eligible`` count sums
            # ``is_eligible`` over this list, and a row that can never pass it contributes 0 whether
            # or not it is in the population being summed.
            reversal_of = r.get("reversal_of")
            if reversal_of:
                rev.append(cat_reversal.WatchlistRow(**shared_fields, reversal_of=str(reversal_of)))
        except (ValueError, ArithmeticError, TypeError) as exc:
            _log.warning("cat_watchlist_row_unparseable", symbol=symbol, error=str(exc))
    return out, rev


# --------------------------------------------------------------------------- brk20 feature link (§4.3)
def _attach_feature_snapshots(features: FeatureEngine, candidates: list) -> list:
    """Mint the §4.3 ``features_snapshot_id`` for batch-rule candidates (brk20, ins, cat) post-admit.

    The per-bar path gets its id from ScanContext at signal time; batch rules bypass ScanContext, and
    a candidate with a null id is structurally un-recommendable (intraday.py Rule 6 mandates
    no_action on a missing identifier — the 2026-08-04 all-15-refused sweep). Runs AFTER
    ``prescreen.admit`` so suppressed/capped candidates never spend a snapshot write. A minting
    failure costs that one candidate its feature link, never the sweep (scan-path posture, §3.2.5)."""
    out = []
    for c in candidates:
        try:
            sid = features.intraday_snapshot(c.symbol).features_snapshot_id
        except Exception as exc:  # noqa: BLE001 - fail to None, never propagate into the sweep
            _log.warning("batch_snapshot_failed", symbol=c.symbol, error=str(exc))
            sid = None
        out.append(c.model_copy(update={"features_snapshot_id": sid}))
    return out


# --------------------------------------------------------------------------- retention (§4.5)
async def apply_tick_retention(store: MarketStore, result: TickCompactionResult) -> None:
    """Run the §4.5 retention purge after a successful nightly compaction pass (WO-23).

    ``MarketStore.apply_retention`` (ticks 30 d by partition dir, news/clusters/sentiment 1 y,
    corrections 90 d) was designed, implemented and unit-tested — and never CALLED from any
    scheduled job. This is the missing wiring, hung off ``tick_compact`` because that job already
    owns the closed tick partitions and runs nightly after the writer is done with them.

    Gated on a clean pass: ``ok=False`` means a symbol-day is still un-compacted (retention would be
    purging under a job that is going to be retried), and ``skipped_in_flight=True`` means ANOTHER
    compaction run holds the lock right now — ``rmtree`` of an old partition while that run is
    walking it is the one race worth avoiding. Either way the next nightly pass sweeps.

    Failure is contained: a retention error WARNs and returns, never touching the compaction job's
    ok-bearing result (a purge that could not run is not a data-freshness failure).
    """
    if not result.ok or result.skipped_in_flight:
        return
    try:
        report = await store.aapply_retention()
    except Exception as exc:  # noqa: BLE001 - retention must never fail the compaction job
        _log.warning("tick_retention_failed", error=str(exc), error_type=type(exc).__name__)
    else:
        _log.info("tick_retention_applied", **report)


# --------------------------------------------------------------------------- backup (§10.5)
async def _snapshot_backup(conn: sqlite3.Connection, settings, clock: Clock) -> None:
    """Watermark-safe SQLite state snapshot (§10.5). Uses a fresh source connection inside the worker
    thread (the online-backup API over a second read handle is WAL-safe) so it never contends with the
    engine's live connection. DuckDB market data has its own Parquet monthly archive (§4.3)."""
    backups = settings.backups_dir()
    backups.mkdir(parents=True, exist_ok=True)
    dst = backups / f"state-{clock.now():%Y%m%dT%H%M%S}.db"
    src_path = str(settings.sqlite_path())

    def _do() -> None:
        src = sqlite3.connect(src_path)
        try:
            with sqlite3.connect(str(dst)) as bck:
                src.backup(bck)
        finally:
            src.close()

    await asyncio.to_thread(_do)
    _log.info("backup_written", path=str(dst))


# --------------------------------------------------------------------------- builders (guarded)
def _build_version() -> str:
    """Best-effort build/version string for the ENGINE_STARTED signal + STARTUP_REPORT (§2.2)."""
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version("market-trading")
        except PackageNotFoundError:
            return "0.0.0"
    except Exception:  # noqa: BLE001 - versioning must never block boot
        return "0.0.0"


def _build_telegram(settings, secrets, clock, mode, kill, *, latch=None, governor=None,
                    limits_engine=None, exposure=None, session=None, conn=None, bus=None,
                    reco_book=None):
    token = secrets.get_optional(TELEGRAM_BOT_TOKEN)
    owner_chat_id = settings.telegram.owner_chat_id
    if not token or not owner_chat_id:
        _log.warning("telegram_disabled", reason="missing bot token or owner_chat_id (settings.telegram.owner_chat_id)")
        return None
    from engine.notify.telegram import TelegramBot

    bot = TelegramBot(
        token, owner_chat_id, clock, mode_manager=mode, kill_switch=kill,
        reco_book=reco_book, latch=latch, governor=governor, limits_engine=limits_engine,
        exposure=exposure, session=session, conn=conn,
    )
    if bus is not None:
        bot.attach_bus(bus)   # §10.3 alert catalog: mode/risk/kill/window/budget events → owner
    return bot


def _create_app(session, mode, kill, secrets, clock, bus, *, conn=None, protected_store=None,
                exposure=None, governor=None, limits_engine=None, market_store=None):
    from engine.api.app import create_app

    return create_app(session_manager=session, mode_manager=mode, kill_switch=kill,
                      secrets=secrets, clock=clock, bus=bus, conn=conn, store=protected_store,
                      exposure=exposure, governor=governor, limits_engine=limits_engine,
                      market_store=market_store)


async def _serve_api(app, settings):
    """Bind + serve the dashboard/login API; return the serve task, or ``None`` if the bind failed.

    2026-07-21 (both halves of the lockout): the login callback (:8400 ``GET /kite/callback``) must
    come up EARLY and must fail LOUDLY. Two uvicorn traps make the naive version deadly:

    * uvicorn's own bind paths call ``sys.exit(1)`` on a bind ``OSError`` (``Config.bind_socket`` and
      the host/port branch of ``Server.startup``). Inside a task that ``SystemExit`` escapes the
      coroutine and KILLS the whole engine loop — no alert, no Telegram ``/token`` fallback, and the
      §2.6 single-instance guard never runs — precisely when a stale process already holds the port.
      So we bind the socket OURSELVES (plain ``OSError`` on failure) and hand it to
      ``server.serve(sockets=[...])``, whose pre-bound path has NO ``sys.exit`` (uvicorn closes the
      socket again on its own shutdown).
    * On Windows the default ``SO_REUSEADDR`` lets a second bind silently STEAL a live port;
      ``SO_EXCLUSIVEADDRUSE`` makes the double-bind fail loudly here instead (O7/E4 posture).
    """
    import socket

    import uvicorn

    host, port = settings.api.host, settings.api.port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):  # Windows: refuse to share a port a live engine holds
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:  # POSIX: match uvicorn's default rebind-after-restart behaviour
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except OSError as exc:
        sock.close()
        _log.critical(
            "api_bind_failed", host=host, port=port, error=str(exc),
            hint="port already in use? browser login unreachable — use the Telegram /token fallback",
        )
        return None

    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)

    async def _guarded_serve() -> None:
        try:
            await server.serve(sockets=[sock])
        except SystemExit as exc:  # any residual uvicorn sys.exit must never kill the engine loop
            _log.critical("api_serve_exited", code=getattr(exc, "code", None))

    task = asyncio.create_task(_guarded_serve())
    # Stash the server on the task so _stop_api can signal it.
    task._mt_server = server  # type: ignore[attr-defined]
    # CONFIRM startup completed: ``started`` flips once the listeners are up (the socket is already
    # bound above, so this is near-instant); the task dying first means startup failed some other way.
    for _ in range(200):  # 200 × 0.05s ≈ 10s
        if server.started or task.done():
            break
        await asyncio.sleep(0.05)
    if task.done() or not server.started:
        _log.critical(
            "api_start_unconfirmed", host=host, port=port, task_done=task.done(),
            hint="socket bound but uvicorn never reported started — browser login may be unreachable",
        )
    return task


async def _stop_api(task) -> None:
    if task is None:  # bind failed at boot (_serve_api returned None) — nothing to stop
        return
    server = getattr(task, "_mt_server", None)
    if server is not None:
        server.should_exit = True
    try:
        await asyncio.wait_for(task, timeout=10)
    except TimeoutError:
        task.cancel()
    except Exception:  # noqa: BLE001 - a server task that died earlier must never abort the
        # shutdown teardown that follows (http.aclose / store.close / conn.close).
        _log.exception("api_server_task_failed")


def _make_stop_handler(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event, force_exit: Callable[[int], object],
) -> Callable[..., None]:
    """Build the COUNTED stop handler shared across every registered signal (extracted so the two-press
    semantics are unit-testable WITHOUT raising real signals — the returned handler is called directly).

    The same handler object is registered for SIGINT/SIGTERM/SIGBREAK, so its counter is shared across
    them (a SIGINT then a SIGTERM is still "second signal"). It accepts ``*_args`` so it serves BOTH the
    zero-arg :meth:`loop.add_signal_handler` callback and the ``(signum, frame)`` :func:`signal.signal`
    fallback. It never raises — a signal handler that raised would surface at an arbitrary ``await``.

    * FIRST signal: graceful — wake the idle ``await stop_event.wait()`` via
      :meth:`loop.call_soon_threadsafe` (the ``signal.signal`` fallback runs in the main thread OUTSIDE
      the loop callback context on Windows, so the threadsafe hand-off is the correct wake-up; the loop
      is captured at install time). Honoured at the next safe point; a second Ctrl-C forces exit.
    * SECOND (and later) signal: hard exit. State stays RUNNING (never committed STOPPED) so the next
      boot runs crash recovery — that is BY DESIGN; R3 protection is broker-resident, not process-local.
    """
    state = {"count": 0}

    def _handler(*_args: object) -> None:
        state["count"] += 1
        if state["count"] == 1:
            _log.warning(
                "stop_requested",
                hint="graceful stop — honoured at the next safe point; a second Ctrl-C forces exit",
            )
            try:
                loop.call_soon_threadsafe(stop_event.set)
            except RuntimeError:
                # Loop already closed (signal.signal fallback handlers are process-global and outlive
                # asyncio.run on Windows): a stray signal in the post-run teardown window has nothing to
                # wake — swallow so the "never raises" contract holds; _hard_exit is already imminent.
                pass
            return
        _log.critical(
            "stop_forced",
            hint="second signal — engine exits hard; state stays RUNNING so the next boot runs crash "
            "recovery (R3 protection is broker-resident)",
        )
        try:
            logging.shutdown()
        except Exception:  # noqa: BLE001 - a hard-exit path must never raise out of a signal handler
            pass
        force_exit(130)

    return _handler


def _install_signal_handlers(stop_event: asyncio.Event, *, force_exit: Callable[[int], object] = os._exit) -> None:
    """Install the graceful-stop signal handlers EARLY — before ``connect()``, so the WHOLE boot is
    covered (2026-07-21 13:34 IST: Ctrl-C during a wedged ``lifecycle.startup`` hit Python's default
    SIGINT handler → a raw ``KeyboardInterrupt`` at an arbitrary ``await`` with no ``lifecycle.shutdown``;
    the process then lingered holding ``data/engine.lock`` — a zombie that blocks every future start).

    ``force_exit`` is injectable so the second-press hard exit is unit-testable without killing the
    interpreter (default :func:`os._exit`). The counted handler is SHARED across all registered signals.
    """
    loop = asyncio.get_running_loop()
    handler = _make_stop_handler(loop, stop_event, force_exit)

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):   # Windows console ctrl-break / NSSM stop → CTRL_BREAK_EVENT
        signals.append(signal.SIGBREAK)
    for sig in signals:
        try:
            loop.add_signal_handler(sig, handler)   # POSIX: runs inside the loop
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop has no add_signal_handler; fall back to signal.signal (the
            # handler accepts the (signum, frame) args and wakes the loop threadsafe from the main thread).
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass


def _hard_exit(code: int) -> NoReturn:
    """Force process exit that the interpreter's own shutdown CANNOT (2026-07-21 13:34 IST zombie).

    After ``main()`` returns, interpreter shutdown joins non-daemon threads and ``concurrent.futures``'
    atexit hook joins executor workers WITHOUT a timeout. A single wedged worker (blocked DuckDB op, a
    long feature snapshot, a hung HTTP call) then hangs the process forever — still holding the §2.6
    instance lock and heartbeat, a zombie that blocks every future start until it is killed by hand
    (exactly what happened at 13:34). ``os._exit`` skips those joins entirely, so a wedged worker can
    never zombify the process. Runs ONLY from ``main()`` (never at import), so tests import this module
    freely without risk of exiting the test runner.
    """
    main_thread = threading.main_thread()
    stragglers = [t.name for t in threading.enumerate() if not t.daemon and t is not main_thread]
    if stragglers:
        _log.warning(
            "nondaemon_threads_at_exit", threads=stragglers,
            hint="abandoned non-daemon threads would block interpreter shutdown; forcing os._exit",
        )
    else:
        _log.info("exit_clean")
    try:
        logging.shutdown()
    except Exception:  # noqa: BLE001 - flushing the log handlers must never block the forced exit
        pass
    os._exit(code)


def main() -> NoReturn:
    # The interpreter MUST NOT be trusted to exit on its own after run() returns (abandoned executor
    # workers, above), so BOTH the normal-return and the KeyboardInterrupt path funnel through the
    # os._exit backstop via `finally` — no path may fall through to a bare return. A raw KeyboardInterrupt
    # escaping asyncio.run (2026-07-21: run() wedged before its own handlers could honour the stop) is
    # still caught here and mapped to 130.
    code = 1
    try:
        code = asyncio.run(run())
    except KeyboardInterrupt:
        _log.info("engine_interrupted")
        code = 130
    finally:
        _hard_exit(code)


if __name__ == "__main__":
    main()
