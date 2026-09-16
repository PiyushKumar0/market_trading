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
import json
import logging
import os
import signal
import sqlite3
import threading
import time as time_module  # `time` itself is datetime.time here (below) — WO-25c needs monotonic()
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
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
from engine.core.config import Settings, config_dir, load_settings, load_yaml
from engine.core.db import connect
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.migrations import apply_migrations
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.recommendations import parse_valid_until
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
from engine.datafeeds.news import NSE_ANN_KEY, Headline, NewsIngest
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
from engine.ops.early_hydration import EarlyHydration
from engine.ops.health import HealthMonitor
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.holdings_reconcile import (
    HoldingsReconcileJob,
    in_reconcile_window,
    positions_missing_from_holdings,
)
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
from engine.ops.warmup import (
    CLASS_DAILY,
    CLASS_INTRADAY,
    CLASS_REGIME,
    WarmupGate,
    WarmupStatus,
)
from engine.risk.causes import RiskStateLatch
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import GateContextBuilder, RiskGate
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine, floor_limits_from
from engine.risk.mode import ModeManager
from engine.strategy.cost_model import CostModel
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.retest import RestingLevelBook
from engine.strategy.scanners import brk20, build_enabled_scanners, cat, cat_reversal, hi52, ins
from engine.strategy.types import SignalCandidate
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

#: §2.6 early-hydration addendum (owner-directed 2026-09-09): the pre-open chain an EARLY Kite login
#: (~06:30, before session open) pulls forward from its 08:20–08:50 clock, in registry order. The
#: owner travels at 08:15 and boots the PC 09:30–10:00 on most trading days, so on those days the
#: whole chain fired after the trade window opened (G2 digest-before-open 61%); on the days they log
#: in early this turns the login into the pre-open chain. Fired by ``EarlyHydration`` through
#: ``CatchUpRunner.hydrate_ahead`` — the same ``job_runs`` watermarks a scheduled fire writes.
#: Those watermarks do NOT silence the 08:20–08:50 fires (``_scheduled_runner`` stays unconditional —
#: review reversed a draft that skipped them): results and exchange filings published 07:00–09:00 IST
#: are the largest catalyst class, so on a day the PC stays awake the chain must run AGAIN — a fresher
#: digest and plan, and a universe rebuilt behind the 08:15 instruments refresh instead of one pinned
#: to a pre-08:00 dump. The watermark guards the SLEEP case: a PC that sleeps through the fire times
#: and wakes at 09:30 finds the catch-up sweep satisfied and keeps the 06:30 run. The cost — a second
#: pre-open plan message on an awake-PC early-login day — is accepted (§2.6 addendum).
#: SLEEP-case residual: the wake-up sweep re-runs ``instruments`` (job id ``instruments``) because it
#: is SAFETY_CRITICAL and due by 09:30, so the token map itself is fresh again — but ``universe_build``
#: is RUN_LATEST and was already watermarked by the 06:30 ``hydrate_ahead`` run, so the sweep leaves it
#: alone: the universe stays built on the 06:30 map for the rest of the day. Accepted: intraday token
#: changes and F&O membership changes inside one trading day are rare, and the ticker's own
#: subscription set is re-derived from the FRESH instruments map by the post-login recovery (``ticker``
#: step) regardless of when the universe itself was last built.
#: Deliberately EXCLUDED:
#:   * ``instruments`` — Kite regenerates its instruments dump around 08:00 IST, so a 06:30 refresh
#:     could pin a STALE map for the whole day. It is SAFETY_CRITICAL: the 08:15 fire or the boot
#:     catch-up owns it, and both run/verify it before entries open.
#:   * ``sector_map`` — Sunday-only cadence (never a trading day, so never an early-login morning).
#:   * ``token_check`` — not a registry job at all (no watermark by design; see ``_arm_token_check``).
EARLY_HYDRATION_JOB_IDS: tuple[str, ...] = (
    JOB_SURVEILLANCE, JOB_UNIVERSE, JOB_NEWS_CHAIN, JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER,
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

#: Last-resort session open for :func:`_session_open_ist` — NOT a second source of truth: it is the
#: value ``NSECalendar.session`` itself defaults to when a year file declares no ``continuous`` span,
#: used only when the calendar can answer nothing at all (and logged when it happens).
_SESSION_OPEN_FALLBACK = time(9, 15)


def _is_sunday(d: date) -> bool:
    return d.weekday() == 6   # §4.4 job 13 weekly cadence — fires Sunday, not a trading day


def _session_open_ist(calendar: NSECalendar, clock: Clock) -> time:
    """Today's continuous-session open (IST), from the calendar the rest of the engine reads (R6) —
    the SAME source ``HealthMonitor._session_open`` uses, never a second hardcoded 09:15 (2026-09-09).

    Boot may land on a weekend/holiday, so fall back to the next trading day's open; a calendar that
    can answer neither (unverified horizon) leaves the engine with no session concept at all, and the
    early-hydration hook it feeds is a pre-open optimisation — so log and take the calendar's own
    documented default (``NSECalendar.session``) rather than failing the boot over it.
    """
    today = clock.today()
    try:
        day = today if calendar.is_trading_day(today) else calendar.next_trading_day(today)
        session = calendar.session(day)
        if session is not None:
            return session.open.time()
    except ValueError:  # no trading day within the calendar horizon (R6)
        pass
    _log.warning("session_open_unresolved", d=today.isoformat(), fallback=_SESSION_OPEN_FALLBACK.isoformat())
    return _SESSION_OPEN_FALLBACK


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
    # feed.health drives the 2026-09-03 reconnect grace: a WARMING/HEALTHY transition suspends the
    # lag watchdog for 30 s so Kite's connect-time snapshot (one last-trade-stamped tick per
    # subscribed instrument) cannot flap the single lag episode — 14 ERROR/recovered pairs in 83 ms
    # and 6 owner pages on the 15:40:02 boot. Subscribed HERE, well before the ticker is spawned
    # (step 7, ticker_resume_hook), so the boot's own transitions are never missed.
    bus.subscribe("feed.health", bar_builder.on_feed_health)

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
    # settings: filings_pit_fresh rebuilds its scrip->symbol map every run from the cached
    # index-constituents CSV (symbol->ISIN) + the BSE bulk master (ISIN->scrip). Without it the map
    # is whatever the last manual isin_map backfill left, which is how the §6.1 `ins` feed ended up
    # reaching 199 of 480 eligible issuers (plan §2.8, 2026-09-12).
    filings_pit_fresh = FilingsPitFreshJob(store, clock, http, settings=settings, notify=notify)
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
        # The per-strategy C3 edge registrations — see :func:`_strategy_expected_edge_pct`, which is
        # this map and is module-level so BOTH halves of the hi52 promotion are assertable without
        # booting an engine (a wiring that only exists inside build_engine silently disappears).
        strategy_expected_edge_pct=_strategy_expected_edge_pct(settings),
        # §2.7 news SHADOWS: C3 rejects these unconditionally, before target_price is even read, so
        # "signals are a measurement, never a recommendation" is ENFORCED at Tier 2 rather than
        # inferred from a missing expected_edge_pct (see risk/gate.py _SHADOW_NO_EDGE for why, and
        # NO_EDGE_SHADOW_STRATEGIES below for the membership and its history).
        no_edge_shadow_strategies=NO_EDGE_SHADOW_STRATEGIES,
    )

    # Warm-up status cache for the gate context: WarmupGate.status() is async + store-heavy, so the
    # gate reads a snapshot refreshed by the equity/health cadence. Unset ⇒ a NOT-READY status with a
    # regime blocker — both §7.1 readiness rules fail CLOSED until the first refresh lands. The
    # "warmup:" line classifies as CLASS_UNKNOWN by design (2026-09-13): an unattributable blocker
    # holds EVERY coverage class down, so the per-class scoping cannot open a leg pre-refresh either.
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
        # WO-D2 (2026-09-12): a position the broker has shown EMPTY on two consecutive sessions no
        # longer produces exit recommendations, so the gate excludes it from the position/sector
        # counts on the same §3.6 journal the position-event screen reads (require_zero: a partial
        # exit still holds its slot — it still has exposure).
        missing_holdings_fn=lambda d: positions_missing_from_holdings(conn, d, require_zero=True),
        # nifty50_fn/expiry_day_fn unwired in Phase 2: the expiry-day NIFTY50-MIS leg of
        # `no_trade_windows` is inert until Phase 3 wires index membership (WORKLOG'd).
    )

    assembler = ContextAssembler(store, conn, clock, calendar)
    # WO-V re-review (2026-09-13): the /taken drift check judges a swing/position fill against the
    # SAME overnight_gap_mult the gate sized it on, read from the hash-verified limits at call time.
    book = RecommendationBook(
        conn, clock, cost_model,
        overnight_gap_mult_fn=lambda: limits_engine.load().limits.per_trade_risk.overnight_gap_mult,
    )
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
            # 2026-09-04: a no_action verdict makes its pair displaceable again (same late binding).
            decline=lambda sym, sid: prescreen.decline(sym, sid),
            ltp_fn=mark_price,          # D1 (e): the brk20 entry-band screen at the forward slot
            # 2026-09-13 per-class warm-up: the SAME snapshot the gate context reads, so an intraday
            # candidate the gate would refuse for intraday coverage never spends an analyst call.
            warmup_status_fn=warmup_status_snapshot,
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

    # --- `brk20` RETEST re-arm (WO-R, 2026-09-12). The 09-12 pre-registered entry-mechanism backtest
    #     selected `V2_limit_at_H20_N5` — a limit resting at the broken level for up to five sessions
    #     — while the live rule publishes that level exactly ONCE (the unregistered, unmeasured N=1).
    #     This book is the durable half (migration 0014, so a mid-session reboot cannot silently turn
    #     the multi-session mechanism back into a single-session one); `forward_drain_tick` below is
    #     the minute pulse that re-offers a rested level the moment the price is back inside the band.
    #     It widens nothing: same levels, same band, same caps, same gate (see retest.py).
    retest_book = RestingLevelBook(conn, calendar, clock,
                                   sessions=settings.brk20.retest_sessions)
    #: What the DAY's sweep knows that the retest tick must respect: the eligible population brk20
    #: originates over and the A12 ex-date horizon, both already read by `_collect_and_scan` (so the
    #: minute pulse pays nothing for them). `day` doubles as the retest's readiness flag — see
    #: `sweep_ready` in `forward_drain_tick`.
    _retest_state: dict[str, Any] = {
        "day": None, "eligible": frozenset(), "ex_skip": frozenset(), "unadjusted": frozenset(),
    }

    def _retest_limits() -> tuple[float, float]:
        """The two §7.1 numbers the retest enforces — the CNC ``entry_sanity_band`` and
        ``stale_data_guard.max_tick_age_s`` — read TOGETHER at the ENFORCEMENT site from ONE
        hash-verified load (the ``catalyst_cap_fn`` convention above). Never constructor numbers, so
        an owner tightening either tightens the retest with it, and never two loads that could
        straddle an owner edit. A raise here (unverifiable store) propagates as the exception
        `_retest_republish` turns into "re-offer nothing this tick"."""
        lim = limits_engine.load().limits
        return (float(lim.entry_sanity_band.cnc_pct), float(lim.stale_data_guard.max_tick_age_s))

    def _retest_skip(symbols: list[str]) -> dict[str, str]:
        """Which of today's due resting symbols must NOT be re-offered (see `_retest_skip_reasons`).

        Called at most once per tick and only when a row is actually in the window, so the two small
        SQLite reads never touch the ordinary minute. A raise offers nothing this tick."""
        return _retest_skip_reasons(
            symbols,
            eligible=_retest_state["eligible"], ex_skip=_retest_state["ex_skip"],
            unadjusted=_retest_state["unadjusted"],
            held=set(held_symbols()),
            pending=_pending_entry_rec_symbols(conn, clock.now()),
        )

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

    def batch_universe_symbols() -> list[str]:
        """Today's BATCH universe (latest ``universe_daily`` snapshot ≤ today): eligible + capped +
        extended names — the widest rule-passing set (``store.get_batch_universe_symbols``).

        Same bounded look-back as :func:`watchlist_symbols`, and for a stronger reason: the only
        consumer is the SUNDAY sector-map job, and Sunday has no universe row of its own. Falls back
        to the active watchlist when nothing is found in the window (pre-first-build / a cold boot),
        which is the pre-O15 behaviour."""
        d = clock.today()
        for _ in range(_WATCHLIST_LOOKBACK_DAYS):
            symbols = store.get_batch_universe_symbols(d)
            if symbols:
                return symbols
            d = d - timedelta(days=1)
        return watchlist_symbols()

    def held_symbols() -> list[str]:
        """Open platform/recommended position symbols — MUST stay in the feed even after the universe
        drops them (2026-07-28 review: an unsubscribed holding marks at avg_entry, so its loss is
        invisible to the §7.1 floor ladder and day-MTM rungs)."""
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM positions WHERE state='OPEN' "
            "AND origin IN ('platform','recommended')"
        ).fetchall()
        return [str(r["symbol"]) for r in rows]

    # --- batch-leg tick subscriptions (2026-09-11 forensics). The §7.1 gate's ONLY price source is
    #     the live tick cache (`mark_price` above): brk20/hi52/ins/cat originate over the whole
    #     ELIGIBLE set (~480 symbols) while the feed carries the included watchlist (~300), so an
    #     eligible-but-unticked candidate reached the gate with ltp=None and failed CLOSED
    #     (stale_data_guard "symbol no feed", entry_sanity_band "LIMIT with no usable LTP") — 27 of 84
    #     brk20 slots and 1 of 16 hi52 slots. The sweep records what the pre-screen ADMITTED and the
    #     feed follows. Per-day: tomorrow's sweep re-admits off tomorrow's universe.
    #     These symbols are deliberately NOT in the warm-up coverage set (`warmup_gate.set_symbols`
    #     stays on `watchlist_symbols()` — job_universe below): a missing bar on one of them would
    #     FREEZE entries, which is the opposite of the point.
    _batch_ticks: dict[str, Any] = {"day": None, "symbols": set()}

    def ticker_tokens() -> list[int]:
        """Ticker subscription set: watchlist + HELD symbols + today's batch-admitted symbols +
        NIFTY 50 + India VIX → tokens (A3). The composition — order, dedupe AND the midnight roll of
        the batch set — lives in :func:`_ticker_tokens` so a test exercises the production function
        instead of re-assembling the same list beside it (2026-09-11 review: a hand-rolled copy still
        passes after the real one drops the day roll and the subscription grows every session)."""
        return _ticker_tokens(
            watchlist=watchlist_symbols(), held=held_symbols(), batch_state=_batch_ticks,
            today=clock.today(), token_for_symbol=instruments.token_for_symbol,
        )

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
        # the gate is already watching them. Pre-open builds skip this (nothing missed yet). This is
        # ONE of two newcomer repairs — the daily-history fill below is the other, and runs regardless
        # of time of day.
        added = [s for s in watchlist_symbols() if s not in before]
        session = calendar.session(clock.today())
        # 2026-09-15: a newcomer's DAILY history was never repaired by anything — OLAELEC joined at
        # 10:09 with a 57-session hole in bars_1d and the DAILY class froze every entry for 12 h. Fill
        # the gate's exact window for the newcomers first; the minute fill below stays mid-session-only.
        if added and backfill is not None:
            sessions = warmup_gate.daily_window()
            if sessions is None:
                _log.warning("universe_added_daily_fill_skipped", symbols=added, reason="calendar_horizon")
            else:
                try:
                    daily = await backfill.daily_gap(added, sessions)
                    _log.info("universe_added_daily_filled", symbols=added, bars=daily.bars_written,
                              skipped_covered=daily.skipped_covered, failed=len(daily.failed))
                except Exception:  # noqa: BLE001 - a failed fill leaves the gate blocking (fail closed), never fails the job
                    _log.exception("universe_added_daily_fill_failed", symbols=added)
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
    # The invariant is that STORE read-modify-write, never the model call (2026-09-09, §2.6): held
    # around the whole scoring batch the lock also spanned one LLM await per chunk, and the polls
    # (whose resolve deadline covers lock ACQUISITION) queued behind it — 14 news_resolve_timeout on
    # 2026-09-07 after a post-sleep backlog, nine of them inside one second at 10:12:40. The scorer
    # now takes this same lock itself, for its read and each conditional write-back only: under the
    # write-back hold it re-reads the CURRENT rows and applies the score columns to those, so a poll
    # merge that landed during the model call survives (the upsert overwrites every non-key column,
    # so re-emitting the pre-LLM snapshot would revert it) and an id that left the queue meanwhile is
    # skipped, never resurrected.
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
        # The batch takes the lock itself (see above) — nothing here may hold it: asyncio.Lock is
        # not reentrant, so a wrapping `async with` would now deadlock on the first store hop.
        # Both callers of this closure — job_news_chain's forced pre-open batch and the 300 s
        # news_scoring_tick — are separate scheduler jobs and can overlap; run_batch single-flights
        # itself on a lock it owns (the waiter re-reads an emptied queue), so no guard is needed here.
        await scoring_job.run_batch(force=force, lock=news_chain_lock)

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
        # O15 (2026-09-04): classify the BATCH universe, not the tick watchlist. The eligible set is
        # now NIFTY 500 while universe_max_watchlist stays 200, so a watchlist-scoped run would
        # leave every capped and extended name without a sector_map row — and §7.1 caps the
        # UNCLASSIFIED bucket at 1 open position, which would gate-block the widened swing legs.
        # Only the UNCLASSIFIED fill-in list grows (~200 → ~800 set-membership checks against the
        # already-fetched sectoral lists, then that many more upserted rows); the ten NSE fetches
        # are per-index and unchanged.
        return await sector_map.run(clock.today(), universe_symbols=batch_universe_symbols())

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

    # --- §3.6 holdings reconcile (owner-directed 2026-09-07): the only answer to "is this tracked
    #     position still IN the account?" until the §3.2.8 fill-side reconciler lands. Wired only when
    #     a broker facade exists (no api_key ⇒ no holdings to read). Alert-only: it never writes state,
    #     never touches the gate/risk/prescreen, and its failures are warnings. ---
    holdings_reconcile = (
        HoldingsReconcileJob(conn, kite, clock, calendar, notify) if kite is not None else None
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
        holdings_reconcile=holdings_reconcile,
    )
    session.add_login_hook(post_login_recovery.run)

    # --- §2.6 EARLY HYDRATION (owner-directed 2026-09-09): the owner travels at 08:15 and boots the
    #     PC 09:30–10:00, so the pre-open chain fired after the trade window opened and G2's
    #     digest-before-open criterion fell to 61%. On the mornings they log in early (~06:30) that
    #     login now IS the pre-open chain: surveillance → universe → news → digest → planner, run
    #     ahead of their clock through the same catch-up watermarks.
    #     Registration order buys NOTHING here: `SessionManager._fire_login_hooks` creates one task
    #     per hook, so this hook and the recovery run CONCURRENTLY. What sequences them is the
    #     recovery's own `completed` event, passed below — the chain wants the token map, backfill and
    #     ticker it restores (bounded: a wedged recovery is logged and stepped over). The hook also
    #     holds itself behind `scheduler.start()` (WO-15) via the event armed in the boot tail.
    #     The session open is passed as a CALLABLE: this hook outlives the day it was built on. ---
    scheduler_armed = asyncio.Event()
    early_hydration = EarlyHydration(
        catch_up, clock, calendar, scheduler_armed, EARLY_HYDRATION_JOB_IDS,
        lambda: _session_open_ist(calendar, clock),
        recovery_done=post_login_recovery.completed, notify=notify,
    )
    session.add_login_hook(early_hydration.on_login)

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
        # --- WO-R `brk20` RETEST re-arm, on this same 60 s pulse. Placed BEFORE the drain so a level
        #     that came back inside the band during this minute can be chosen by this very tick's
        #     forward slot instead of waiting for the next one. Its own try/except and its own
        #     module-level body: a retest failure must cost neither the drain nor the scheduler (D7),
        #     and `_retest_republish` is testable on its candidates rather than on a source string.
        #     The window predicate is `_retest_active`: the sweep's own `_sweep_window_active` plus
        #     the states in which the pipeline is certain to drop the publication (FROZEN, killed)
        #     and "a sweep is running" — the retest has no freeze-lift path to redo an offer that
        #     was dropped, and its once-a-day offer bound is spent at offer time (2026-09-12 review).
        #     OFF-LOOP (`asyncio.to_thread`), like every other caller of these two seams: the body
        #     takes `SignalPreScreen._lock` and then mints a feature snapshot under
        #     `MarketStore._lock` — `prescreen.handle_bar` and `_collect_and_scan` are both threaded
        #     for exactly that reason, and a store lock held 59 s by a partition COPY (store.py) or
        #     ~14 minutes by the 2026-08-21 stall would otherwise freeze the WHOLE loop: the tick
        #     cache, the §2.2 heartbeat, the kill path, Telegram and every APScheduler job.
        #     `sweep_ready` is the day's admission having happened: until today's IN-WINDOW sweep has
        #     published, the retest holds off so a fresh crossing of a resting symbol always wins the
        #     pre-screen's `(symbol, strategy)` dedupe over a level broken days ago — and so
        #     `_retest_skip` is reading TODAY's eligible set and ex-date horizon rather than an empty
        #     (or yesterday's) one. Fails to zero: a day whose sweep never published re-offers
        #     nothing, which is the pre-WO-R behaviour.
        try:
            for cand in await asyncio.to_thread(
                _retest_republish,
                retest_book, prescreen, features,
                active=_retest_active(clock.now(), calendar, mode, kill, _sweep_lock),
                sweep_ready=_retest_state["day"] == clock.today(),
                ltp_fn=mark_price, tick_age_fn=tick_age_s, limits_fn=_retest_limits,
                skip_fn=_retest_skip, today=clock.today(),
            ):
                await bus.apublish("signal.candidate", cand)
        except Exception:  # noqa: BLE001 - a retest failure must never cost the drain below it
            _log.exception("brk20_retest_tick_failed")
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
        await refresh_and_lift_warmup(warmup_gate, warmup_holder, mode, lifecycle, alert=alert,
                                      clock=clock)
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
            # still-running pass a logged no-op rather than a double replay. The in-session
            # tick_compact veto applies here too (2026-09-04) — see _catchup_sweep_once.
            await _catchup_sweep_once(catch_up, latch, kill, clock, calendar)
        except Exception:  # noqa: BLE001 - the sweep must never take down the scheduler loop
            _log.exception("catchup_sweep_failed")

    # --- §3.6 holdings reconcile pulse (2026-09-07): hourly, in-session only. The job itself is
    #     diagnostic and swallows its own broker failures; this wrapper only supplies the cadence and
    #     the window, and swallows anything else so a reconcile can never take down the scheduler. ---
    async def holdings_reconcile_tick() -> None:
        if holdings_reconcile is None or not in_reconcile_window(clock.now(), calendar):
            return
        try:
            await holdings_reconcile.run()
        except Exception:  # noqa: BLE001 - a diagnostic must never take down the scheduler loop
            _log.exception("holdings_reconcile_tick_failed")

    # --- on-demand scanner sweep (§3.2.5 addendum, owner-directed 2026-07-29): the answer to "what
    #     could I trade right now, and at what price would today's setups arm?" Runs when the trade
    #     window becomes ACTIVE, when a freeze lifts inside the window, and on /scan_now. Live
    #     candidates re-enter the NORMAL pipeline path (dedupe/caps intact); the verdict is never
    #     silence. ---
    #: The last sweep's three PHASE marks (start / publication / completion), its counts and the risk
    #: state it published under — the freeze-lift debounce reads all of them (see
    #: :func:`_freeze_lift_skip_reason`). Written only by a sweep that got past the session check.
    _last_sweep: dict[str, Any] = {"started_at": None, "published_at": None, "done_at": None,
                                   "published": 0, "pending": 0, "frozen": False}

    #: ONE sweep at a time (2026-09-11 review). The window-edge tick and the warm-up refresh ride 60 s
    #: timers armed by the same `scheduler.start()`, so the window_open sweep and the lift that clears
    #: the freeze fire in the SAME tick — and a sweep is two passes over the eligible universe's daily
    #: history, each taking MarketStore._lock. Two of them at once is the WO-25c contention profile
    #: that wedged the 2026-08-24 boot for 11 hours. Caps and the ins journal survive concurrency
    #: (prescreen.admit is lock-guarded); the store load is the reason.
    _sweep_lock = asyncio.Lock()

    async def run_scan_sweep(trigger: str, *, queue_behind_in_flight: bool = False) -> str:
        """THE sweep entry point for every trigger (window edge, freeze lift, /scan_now), single-flight.

        ``queue_behind_in_flight`` is the freeze-lift path's exception: a dropped re-sweep costs the
        day's batch origination, so the lift WAITS for the running sweep instead of being skipped by
        it (see `_lift_scan_sweep`). Everything else — the window edge, the owner's /scan_now — gets
        the skip, because their next cadence tick is free."""
        return await _single_flight_sweep(_sweep_lock, _scan_sweep, trigger,
                                          queue=queue_behind_in_flight)

    async def _scan_sweep(trigger: str) -> str:
        now = clock.now()
        session_day = calendar.session(now.date())
        if session_day is None or not (session_day.open <= now <= session_day.close):
            return "no session in progress — the sweep reads live bars; try during market hours"
        # PHASE mark 1 of 3 (2026-09-11 review). `_sweep_lock` above is what stops two sweeps running
        # at once; this mark is what lets the freeze lift tell the two in-flight cases apart — a sweep
        # that has NOT published yet will publish under the state this lift just established (skip),
        # one that already published while FROZEN is the one to redo (queue behind it).
        _last_sweep["started_at"] = now

        today = now.date()
        # --- Trade-window gate for the BATCH legs (2026-08-18). The bar-driven leg below gates
        #     itself off BAR time inside the pre-screen (which stays Clock-free for §9.6); these
        #     candidates carry no bar, so the sweep — which HAS the Clock — decides, applying the
        #     same test as `pipeline.on_signal_candidate`. Without it a /scan_now outside the
        #     window spends unrefundable day slots on candidates the pipeline is guaranteed to
        #     drop as `signal_candidate_out_of_window`. The window-open sweep is unaffected: it
        #     fires ON the INACTIVE→ACTIVE edge, so the window is open by construction.
        #     Computed HERE rather than inside the worker (2026-09-11): the `ins` consume decision is
        #     taken at PUBLICATION, on the loop thread, and both sides must read ONE window verdict.
        try:
            _w = calendar.trade_window(today)
            batch_in_window = _w[0] <= now <= _w[1]
        except ValueError:
            batch_in_window = False      # not a trading day ⇒ no window ⇒ nothing originates (R6)

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
            # 2026-09-01 refactor: the eligibility predicate lives in ONE place now
            # (store.get_universe_eligible_symbols) — this was one of three inline duplicates of the
            # strict reasons==['watchlist_cap'] equality that the batch-universe addendum would have
            # silently missed. Every batch leg reads this ONE set: hi52 was the last on the wider
            # batch universe and came back to it on 2026-09-09, before the 09-12 promotion made the
            # eligible population part of its registered rule.
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
            # Trailing structural corp actions (2026-09-09, the hi52 veto's premise): stored bars_1d
            # is never re-adjusted across an ex-date, so a bonus/split/rights/demerger inside the
            # 20-session window leaves a phantom pre-ex high for the breakout test to clear.
            # 35 calendar days ≥ lookback+1 sessions with weekend/holiday margin.
            brk20_unadjusted = hi52.unadjusted_history(
                store.get_corp_actions(ex_from=today - timedelta(days=35), ex_to=yesterday)
            )
            brk20_vetoes: dict[str, int] = {}
            brk20_raw = brk20.sweep_daily(
                histories, today=today, ex_dates_by_symbol=ex_map,
                unadjusted_symbols=brk20_unadjusted, veto_counts=brk20_vetoes,
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
                unadjusted_vetoes=brk20_vetoes.get(brk20.VETO_UNADJUSTED_HISTORY, 0),
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

            # The once-per-session `hi52` leg — the only trigger-keyed branch in the sweep. A
            # module-level function (2026-09-11 review) so "freeze_lift takes the same branch as
            # window_open" is assertable on its candidates rather than on a source string. Called
            # HERE, before the admission below, because since the 2026-09-12 promotion its
            # candidates are part of that ONE ranked batch (see the leg's own docstring).
            hi52_raw = _hi52_daily_leg(
                store, trigger=trigger, eligible=eligible, today=today,
                yesterday=yesterday, ex_map=ex_map,
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
            #     `hi52` joined this batch on 2026-09-12 (§8.6 promotion): a rule that can reach
            #     RECOMMEND must contest the same slots as every other actionable leg, and its own
            #     per-strategy cap (3) is what bounds it.
            #     ADMISSION is that one ranked call; PUBLICATION order is a second, separable
            #     consequence of its result and `_publication_order` owns it (see that function for
            #     why a promoted hi52 must not inherit the front of the queue).
            batch = _attach_feature_snapshots(
                features,
                _publication_order(prescreen.admit(
                    brk20_raw + ins_raw + cat_raw + cat_rev_raw + hi52_raw, today,
                    in_window=batch_in_window,
                )),
            )
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

            # Third element = the BATCH legs' admitted symbols (brk20/ins/cat/cat_reversal/hi52), the
            # ones with no 1m bar and therefore no guaranteed tick. The bar-driven `accepted` are
            # watchlist symbols the feed already carries. Fourth = every `ins_pending` row this sweep
            # READ; whether it may be consumed is decided at publication (_ins_rows_to_consume).
            #
            # WO-R: the resting brk20 levels ride the SAME list. `_batch_ticks` rolls at midnight, so
            # without this a level admitted on day 1 has no tick on days 2-5 and `RestingLevelBook
            # .due` — which bands against the LIVE price — could never offer it again. That would
            # kill the retest silently for the sub-cap symbols brk20 exists to catch, the same hole
            # the 2026-09-11 forensics found at the gate (27 of 84 brk20 slots, ltp=None).
            #
            # Fifth = the two NARROWINGS the retest tick re-applies to a level broken days ago, both
            # already read here so the 60 s pulse pays nothing for them: today's ELIGIBLE population
            # (`brk20` originates over it, and a name that churned out mid-window would otherwise
            # keep originating outside the registered scope) and the A12 ex-date veto over brk20's
            # OWN `ex_skip_days` horizon (`bars_1d` is never re-adjusted, so a stored level is a
            # pre-ex price the tape stops quoting — `brk20.scan_daily` refuses such a symbol on the
            # crossing day and the re-offer must refuse it on the retest day for the same reason).
            _brk20_ex_horizon = today + timedelta(days=int(brk20.DEFAULT_PARAMS["ex_skip_days"]))
            retest_ctx = {
                "eligible": frozenset(eligible),
                "ex_skip": frozenset(
                    sym for sym, dates in ex_map.items()
                    if any(today <= xd <= _brk20_ex_horizon for xd in dates)
                ),
                # The BACKWARD-looking half of the corporate-action veto (2026-09-12 review): the
                # sweep skips a symbol whose bars_1d holds two units after a structural ex-date
                # inside the 35-day lookback; a stored level is a pre-ex price on the same series,
                # so the re-offer must refuse it too — `ex_skip` above looks forward only.
                "unadjusted": frozenset(brk20_unadjusted),
            }
            return (
                accepted + batch,
                pendings,
                [c.symbol for c in batch] + retest_book.resting_symbols(today),
                [c.symbol for c in ins_pending],
                retest_ctx,
            )

        accepted, pendings, batch_symbols, ins_read, retest_ctx = await asyncio.to_thread(
            _collect_and_scan
        )
        # Point the feed at today's batch admissions BEFORE publishing them (2026-09-11): the §7.1
        # gate reads the tick cache and nothing else, and the pipeline's analyst hop is the only
        # window in which a first tick can arrive. Guarded exactly like the job_universe resubscribe
        # — a failed control frame degrades to the previous set and never fails the sweep.
        _todays_batch = _roll_batch_ticks(_batch_ticks, clock.today())
        _new_batch = sorted(set(batch_symbols) - _todays_batch)
        _todays_batch.update(_new_batch)
        try:
            if ticker.health().state != "STOPPED":
                await ticker.update_subscriptions(ticker_tokens())
        except Exception:  # noqa: BLE001 - a resubscribe failure degrades to the old set, never fails the sweep
            _log.exception("ticker_resubscribe_failed")
        if _new_batch:
            _log.info("batch_ticks_subscribed", trigger=trigger, added=_new_batch,
                      total=len(_todays_batch))
        # Read at PUBLICATION, which is the moment that decides the pairs' fate: a candidate handed to
        # a frozen pipeline is re-armed and never re-published, so this sweep is the one a freeze lift
        # must redo rather than debounce against (see _freeze_lift_skip_reason). ONE read serves both
        # that debounce and the `ins` consume decision below — they must never disagree.
        _published_frozen = mode.risk_state() != RiskState.NORMAL
        _last_sweep.update(published_at=clock.now(), frozen=_published_frozen)
        # The §6.1 once-only bound, applied at the moment the rows' fate is decided rather than back
        # in the worker (2026-09-11 review): the whole worker — since 2026-09-12 the hi52 leg's
        # ~480×400-session read included — sits between the rows' READ and this decision.
        _consume_ins_pending(
            conn, now.date(),
            _ins_rows_to_consume(ins_read, in_window=batch_in_window,
                                 published_frozen=_published_frozen),
            now=now,
        )
        # WO-R (2026-09-12): every ADMITTED brk20 level starts RESTING here, so the next
        # `retest_sessions` sessions can re-offer it when the price comes back to it — the
        # `V2_limit_at_H20_N5` mechanism the pre-registered entry backtest selected. ADMITTED, never
        # raw: the pre-screen has already decided which levels are worth judgement, and resting a
        # capped-out fire would re-offer tomorrow what the caps refused today (`accepted` is the
        # bar-driven candidates plus the post-admit `batch`, and no bar-driven rule is brk20).
        # On the LOOP THREAD, beside `_consume_ins_pending` and for the same reason: this connection
        # is `isolation_level=None` with `check_same_thread=False` because the engine serialises its
        # writes on this thread (§4.1). A bare INSERT issued from the sweep WORKER while the loop is
        # between BEGIN and COMMIT in `core.db.transaction` joins that transaction and dies with its
        # ROLLBACK — silently, since `record` has already returned True and logged. The level would
        # then never be re-offered while the log said it was resting for five sessions.
        for _cand in accepted:
            if _cand.strategy_id == brk20.STRATEGY_ID:
                retest_book.record(_cand)
        # …and the retest tick is armed for today only by an IN-WINDOW sweep — the one that can
        # actually admit a fresh crossing. Pre-open `/scan_now` admits nothing (in_window False), so
        # arming on it would let the minute pulse publish a days-old level into the pre-screen's
        # `(symbol, strategy)` dedupe moments before the window-open sweep's fresh cross of the same
        # symbol, which would then be dropped as a duplicate and never journalled.
        if batch_in_window:
            _retest_state.update(day=today, eligible=retest_ctx["eligible"],
                                 ex_skip=retest_ctx["ex_skip"],
                                 unadjusted=retest_ctx["unadjusted"])
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
        # Completion stamp (`frozen` was set at publication, which is the moment it describes): the
        # freeze-lift debounce asks both "is a sweep still RUNNING?" and "did one just FINISH?", and
        # the eligible-universe history read is the slow part between the two.
        _last_sweep.update(done_at=clock.now(), published=len(live_rows),
                           pending=len(pending_rows))
        if trigger != "scan_now":       # /scan_now gets the body as its direct reply — no double send
            await notify(msg)
        return msg.body

    # Fire the sweep on the window-INACTIVE→ACTIVE edge (covers both the daily window-open moment
    # and an owner moving/extending the window onto "now").
    _window_active = {"was": False}

    async def window_sweep_tick() -> None:
        active = _sweep_window_active(clock.now(), calendar, mode)
        was, _window_active["was"] = _window_active["was"], active
        if active and not was:
            try:
                if await run_scan_sweep("window_open") == _SWEEP_IN_FLIGHT_REPLY:
                    # The edge is a ONCE-PER-DAY event and the single-flight guard just declined it:
                    # give it back rather than spend it on a sweep that never ran, and the next 60 s
                    # tick re-fires it. The in-flight sweep is not a substitute — an owner /scan_now
                    # skips the once-per-session hi52 leg and sends the owner no window-open digest.
                    _window_active["was"] = False
            except Exception:  # noqa: BLE001 - a sweep failure must never take down the scheduler
                # A FAILED sweep keeps the edge spent (unchanged): a duckdb stall would otherwise
                # re-fire the whole eligible-universe read every 60 s for the rest of the session.
                _log.exception("window_open_sweep_failed")

    # --- freeze-lift re-sweep (2026-09-11 forensics). The batch legs (brk20/hi52/cat/ins) originate
    #     ONLY from this sweep, and a candidate published INTO a standing freeze is re-armed by
    #     `pipeline.on_signal_candidate` with nothing left to re-publish it — batch rules have no next
    #     bar. Every in-session boot re-applies a warm-up freeze (lifecycle step 6) AND re-fires the
    #     window edge (`_window_active` is per-process), so a boot inside the window lost the whole
    #     day's batch origination: 08-28 (freeze 09:55–10:52, 3 pairs re-armed), 08-31 (5 published,
    #     0 forwarded), 09-04 (3 boots, sweeps 09:50/10:32/11:11 all inside freezes, 5 pairs). The
    #     LIFT is the missing re-publication trigger. Idempotent for everything else: the pre-screen's
    #     same-day (symbol, strategy) dedupe drops every pair that was not re-armed.
    #     Armed HERE, ahead of `lifecycle.startup()`, because the subscription must exist before the
    #     first transition can be published — and gated on `engine_ready` for the same reason (the
    #     boot itself publishes lifts; see `_freeze_lift_sweep`'s `ready`). ---
    from engine.risk.events import TOPIC_RISK_STATE

    _freeze_lift = {"fired": None}
    _freeze_lift_tasks: set[asyncio.Task] = set()

    async def _lift_scan_sweep(trigger: str) -> str:
        """The lift's sweep call: it QUEUES behind an in-flight sweep rather than taking the
        single-flight skip. The skip is right for a cadence trigger (the next tick is free) and wrong
        here — the lift is the day's ONLY re-publication of the batch legs, and a sweep that is still
        running can already have published its whole batch INTO the freeze this lift just cleared
        (`_last_sweep["frozen"]`). `_freeze_lift_skip_reason` has already dropped the case where the
        running sweep has NOT published yet, which is the common one; what reaches here waits seconds
        for the store, then re-sweeps."""
        return await run_scan_sweep(trigger, queue_behind_in_flight=True)

    async def _on_risk_state_sweep(evt: Any) -> None:
        # DETACHED: `apublish` awaits its subscribers in turn, and a sweep is a full eligible-universe
        # daily-history read — inline, it would stall the 60 s warm-up-refresh job that publishes most
        # lifts (the job that lifts freezes at all) and any owner /resume reply behind it. The debounce
        # claim inside `_freeze_lift_sweep` happens before that coroutine's first await, so two lifts
        # in one minute still sweep exactly once. The set holds a strong reference until the task
        # finishes — the loop keeps only a weak one.
        task = asyncio.create_task(
            _freeze_lift_sweep(evt, clock, calendar, mode, _lift_scan_sweep, _freeze_lift,
                               _last_sweep, ready=lambda: bool(boot_state["engine_ready"]),
                               in_flight=_sweep_lock.locked),
            name="freeze_lift_sweep",
        )
        _freeze_lift_tasks.add(task)
        task.add_done_callback(_freeze_lift_tasks.discard)

    bus.subscribe(TOPIC_RISK_STATE, _on_risk_state_sweep)

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
                   window_sweep_tick=window_sweep_tick, forward_drain_tick=forward_drain_tick,
                   holdings_reconcile_tick=(
                       holdings_reconcile_tick if holdings_reconcile is not None else None
                   ))

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
    post_arm_task = start_scheduler_and_fire_post_arm(
        scheduler, catch_up, clock, calendar, armed=scheduler_armed,
    )
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
    if _freeze_lift_tasks:
        # …nor a detached freeze-lift re-sweep. WAIT it out first (a sweep is seconds): the task parks
        # in `asyncio.to_thread`, and cancelling the TASK does not stop the worker THREAD — it would
        # keep reading the store that `store.close()` below is about to take away. The cancel after
        # the wait is the bound: a stop can never hang on a wedged sweep.
        await asyncio.wait(list(_freeze_lift_tasks), timeout=_SHUTDOWN_LIFT_WAIT_S)
    for _lift_task in list(_freeze_lift_tasks):
        await cancel_post_arm(_lift_task)
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
def post_arm_exclusions(
    clock: Clock, calendar: NSECalendar, *, path: str = "post_arm"
) -> tuple[str, ...]:
    """Catch-up jobs a pass must NOT fire, given WHEN it runs (WO-21 (ii)).

    Only ``tick_compact`` is ever vetoed, and only for a pass landing inside a live trading session
    (:data:`_IN_SESSION_START_IST`..:data:`_IN_SESSION_END_IST` on an NSE trading day). Every other
    job is unchanged: the news chain / digest / planner are pre-open work that a mid-session recovery
    still wants done, whereas compaction competes with the tick writer for exactly the resources the
    session needs (2026-08-20 11:26 IST — see the constants above).

    Two callers share the veto (``path`` names which, in the log): the post-arm one-shot at boot,
    and — since 2026-09-04 — every 30-min catch-up sweep (:func:`_catchup_sweep_once`): on 09-04 the
    sweep replayed a missed ``tick_compact`` at 11:39 IST inside the session and the engine spent
    the afternoon in store stalls and late ticks, the very class the one-shot veto exists for.

    Non-trading day (weekend / holiday) inside the same clock window ⇒ no veto: there is no session
    to protect, and a Saturday pass is precisely when the backlog SHOULD be collapsed.
    """
    now = clock.now()
    if not calendar.is_trading_day(now.date()):
        return ()
    if not (_IN_SESSION_START_IST <= now.time() <= _IN_SESSION_END_IST):
        return ()
    _log.info("post_arm_skipped_in_session", job_id=JOB_TICK_COMPACT, now=now.isoformat(), path=path)
    return (JOB_TICK_COMPACT,)


async def _catchup_sweep_once(catch_up: CatchUpRunner, latch, kill, clock: Clock, calendar: NSECalendar):
    """One 30-min catch-up sweep: ALL scope, the in-session ``tick_compact`` veto, then the
    catchup_safety_jobs freeze reconciliation (2026-09-02). Extracted from the scheduler closure so
    the veto on THIS path is testable (2026-09-04)."""
    exclude = post_arm_exclusions(clock, calendar, path="sweep")
    result = await catch_up.catch_up(scope=CatchUpScope.ALL, exclude=exclude)
    await _reconcile_catchup_freeze(result, latch, kill)
    return result


def start_scheduler_and_fire_post_arm(
    scheduler: Scheduler, catch_up: CatchUpRunner, clock: Clock, calendar: NSECalendar,
    *, armed: asyncio.Event,
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

    ``armed`` (§2.6 early hydration, 2026-09-09) is the composition root's "the scheduler is up"
    event that the early-login hook waits on, so its pre-open chain is held to the SAME firing-point
    rule this function exists to enforce. REQUIRED, not optional: a caller that forgot it would park
    every early login until the hook's own 15-minute timeout.
    """
    scheduler.start()
    if not (DEFER_POST_ARM_JOBS and POST_ARM_JOB_IDS):
        armed.set()                     # nothing to dispatch — arming itself is the release point
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
    task = asyncio.create_task(_fire(), name="post_arm_catchup")
    # §2.6 early hydration (2026-09-09): release the login hook only AFTER the one-shot is dispatched.
    # Both are the same single-flighted CatchUpRunner: released first, the woken hook takes the pass
    # lock and this one-shot degrades to a `skipped_in_flight` no-op — the deferred chain would then
    # wait for its own 08:25 clock. Dispatched first, it holds the lock and the hook queues behind it.
    armed.set()
    return task


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
    """The live-scheduler wrapper: run the job, then record its ``job_runs`` watermark (§2.6).

    The fire is UNCONDITIONAL even when today's watermark already exists — a 2026-09-09 draft skipped
    it, and review reversed that: the skip pinned the 06:30 early-hydration digest/planner, and a
    universe built on a pre-08:00 instruments dump, for the whole day. Results and exchange filings
    published 07:00–09:00 IST are the largest catalyst class, so on a day the PC stays awake the
    08:25/08:35/08:50 fires MUST still run (a fresher digest and plan; the universe rebuilt behind the
    08:15 instruments refresh). The watermark ``CatchUpRunner.hydrate_ahead`` records guards only the
    SLEEP case: a PC that sleeps through the fire times and wakes at 09:30 finds the catch-up sweep
    satisfied and keeps the 06:30 run. A second pre-open plan message on an awake-PC early-login day
    is the accepted, documented cost (§2.6 "Early-hydration addendum").
    """
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
    window_sweep_tick=None, forward_drain_tick=None, holdings_reconcile_tick=None,
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
    # The exchange-announcements feed (§2.7 amendment 2026-09-04) keeps its own cadence and its own
    # off switch: disabled ⇒ the job is never armed (the poll key stays valid either way).
    nse_ann = settings.news.feeds.nse_announcements
    if nse_ann.enabled:
        scheduler.add_job(_news_poll(NSE_ANN_KEY), trigger=IntervalTrigger(seconds=nse_ann.poll_s),
                          job_id=f"news_poll_{NSE_ANN_KEY}", guard=False)
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
    if holdings_reconcile_tick is not None:
        # §3.6 holdings reconcile (2026-09-07) — hourly; the tick self-gates on the trading day and
        # the 09:20–15:30 window (holdings_reconcile.in_reconcile_window).
        scheduler.add_job(holdings_reconcile_tick, trigger=IntervalTrigger(seconds=3600),
                          job_id="holdings_reconcile", guard=False)


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


async def refresh_and_lift_warmup(warmup_gate, warmup_holder: dict, mode, lifecycle,
                                  *, alert=None, clock=None) -> None:
    """Refresh the gate's warm-up snapshot AND lift a standing warm-up freeze once coverage completes.

    The lift used to hang off the post-login hook only — a VALID-TOKEN mid-session restart (first
    seen 2026-08-03: boot 12:22 IST, ORB lookbacks ~17 min short) froze entries and nothing ever
    lifted them, because no login event fires on such a boot. Runs on the 60 s
    ``warmup_status_refresh`` cadence; ``reapply_warmup_gate`` resolves through the cause latch
    (never a blanket NORMAL write) and is a no-op unless the state is FROZEN with the FREEZING
    classes (daily ∪ regime ∪ unattributable) covered — since 2026-09-13 the lift is owed to those
    alone, so a still-short INTRADAY class no longer blocks it.

    ``alert`` is the owner channel for the INTRADAY class's transitions (best-effort, once per
    transition): that class reaches neither the risk state nor a WARMUP_FROZEN page any more, so a
    hole that opens MID-SESSION — the 2026-09-09 14:47 reconnect — would otherwise be visible only
    in the structured log. Unwired ⇒ log-only. ``clock`` scopes the transition memo to the IST day
    (unwired ⇒ per-process, the pre-2026-09-13 memo).
    """
    status = await refresh_warmup_snapshot(warmup_gate, warmup_holder)
    if status is None:
        return
    # 2026-09-13 (per-CLASS scoping): the lift is owed to the DAILY ∪ REGIME classes, not to a flat
    # ``ready``. An intraday hole — one symbol at 11:09, or the market-wide one-bar hole a reconnect
    # leaves — no longer holds the daily-bar legs frozen for the session; the gate and the pre-screen
    # refuse intraday candidates on their own, per candidate.
    if _daily_regime_ready(status) and mode.risk_state() == RiskState.FROZEN:
        try:
            await lifecycle.reapply_warmup_gate()
        except Exception:  # noqa: BLE001 - a failed lift retries on the next 60s tick
            _log.exception("warmup_freeze_lift_failed")
    # The intraday notice comes AFTER the lift so that "entries are NOT frozen by warm-up" is said of
    # the state the owner is now in, not of the one a few lines above.
    today = clock.today() if clock is not None else None
    notice = _log_intraday_class_transition(warmup_holder, status, today=today)
    if notice is not None and alert is not None:
        try:
            await alert(notice[0], notice[1])
        except Exception:  # noqa: BLE001 - the owner notice is best-effort
            _log.exception("warmup_intraday_alert_failed")


def _daily_regime_ready(status) -> bool:
    """Are both FREEZING coverage classes satisfied? Duck-typed like every other gate seam here: a
    status without ``ready_for`` (a fake, or an older snapshot) falls back to the flat ``ready``,
    which is the pre-2026-09-13 behaviour and never looser than it."""
    ready_for = getattr(status, "ready_for", None)
    if ready_for is None:
        return bool(getattr(status, "ready", False))
    return bool(ready_for(CLASS_DAILY)) and bool(ready_for(CLASS_REGIME))


def _log_intraday_class_transition(warmup_holder: dict, status, *, today=None) -> tuple[str, str] | None:
    """Log the INTRADAY class's readiness ONCE PER TRANSITION and return the owner notice to send,
    as ``(severity, message)``, or None when there is nothing new to say.

    This runs on the 60 s cadence, and an intraday hole that survives the session would otherwise
    print the same line ~375 times. The memo lives in the snapshot holder, so it is per-process (a
    restart logs the standing state once more, which is the harmless side); with ``today`` it is
    also per IST DAY — the intraday class reads trivially "ready" whenever there is no session yet
    (``WarmupGate._missing_intraday`` returns nothing before the open and on a non-trading day), so
    a memo that survived the midnight rollover would manufacture a "coverage restored" that no repair
    produced.

    **Only the intraday class is judged here.** While a FREEZING class (daily ∪ regime ∪ unattributable)
    is short, entries ARE frozen and ``WARMUP_FROZEN`` owns that state — a notice saying "entries are
    NOT frozen" in that state would be the opposite of the truth, so nothing is said and the memo is
    left alone; the intraday shortfall is reported on the first tick after the freeze lifts, when the
    wording is true. (``ready_for(intraday)`` is False in that compound state too, because an
    unknown-class blocker holds every class down.)

    The FIRST observation of a ready intraday class is a transition for the log but NOT a notice:
    there is nothing to recover from, and every clean boot would page "coverage restored". A first
    observation of a SHORT one is a notice — the owner needs that on the tick it is learned, not only
    from the boot report."""
    ready_for = getattr(status, "ready_for", None)
    if ready_for is None:
        return None
    if not _daily_regime_ready(status):
        return None
    if today is not None and warmup_holder.get("intraday_short_day") != today:
        warmup_holder["intraday_short_day"] = today
        warmup_holder.pop("intraday_short", None)
    short = not ready_for(CLASS_INTRADAY)
    seen = warmup_holder.get("intraday_short")
    if seen is short:
        return None
    warmup_holder["intraday_short"] = short
    # With the freezing classes covered, whatever holds intraday down is the intraday bucket itself.
    by_class = getattr(status, "blockers_by_class", {}) or {}
    blockers = list(by_class.get(CLASS_INTRADAY, []))
    if short:
        _log.warning("warmup_intraday_not_ready", blockers=blockers[:8], count=len(blockers),
                     note="intraday candidates refused per-candidate; warm-up freezes nothing for this")
        # A status that says short without rendering a blocker still gets a notice — the owner needs
        # the state, and "(…)" with nothing in it reads as a formatting bug rather than as coverage.
        more = f", +{len(blockers) - 3} more" if len(blockers) > 3 else ""
        detail = f" ({', '.join(blockers[:3])}{more})" if blockers else ""
        return ("warning",
                f"warm-up: INTRADAY coverage short{detail} — intraday candidates are "
                "refused one by one; entries are NOT frozen by warm-up and the daily-bar legs are "
                "unaffected (§2.6 step-6 addendum)")
    _log.info("warmup_intraday_ready")
    if seen is None:
        return None
    return ("info", "warm-up: INTRADAY coverage restored — intraday candidates are accepted again")


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
    candidate must stay distinguishable from one that never existed. An empty ``symbols`` is the
    normal no-op (see :func:`_ins_rows_to_consume`), not an error."""
    if not symbols:
        return
    try:
        # Bare execute on the autocommit connection (``isolation_level=None``), matching the other
        # journal writers (``scan_context._write_last_rebalance_d``, ``pipeline._journal_slot``).
        # Deliberately NOT ``transaction()``: an explicit BEGIN could land inside one the OMS already
        # opened on this same connection.
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
#: ``hi52`` joined at birth 2026-09-01 (§3.2.4/§6.1 addendum) and LEFT 2026-09-12 — the §8.6 record
#: is the plan's "hi52 v2 promotion" addendum. Promoted as a FORWARD TEST, not as a validated edge:
#: the v2 backtest is CPCV-promotable (T+20 median net +1.71%, fold pass 86.7% under N=2) but its
#: edge lives in an index cell labelled by CURRENT membership applied backwards, so RECOMMEND mode
#: IS the soak and it ships with a mechanical kill rule (``scripts/hi52_forward_verdict.py``:
#: n >= 20 signals AND (median net at T+20 <= 0 OR T+20 hit rate < 50%) ⇒ DEMOTE). DEMOTION IS THIS
#: LINE: re-add ``hi52.STRATEGY_ID`` here and delete ``hi52.expected_edge_pct`` from settings.yaml —
#: both, since either alone leaves a half-promoted rule (the shadow set wins a contradiction, but a
#: dangling edge key reads as a live registration). BOTH edits are real edits: ``Hi52Cfg
#: .expected_edge_pct`` defaults to ``None``, so deleting the settings key un-registers the edge in
#: ``build_engine`` above rather than falling back to a value hidden in code.
NO_EDGE_SHADOW_STRATEGIES: frozenset[str] = frozenset(
    {cat.STRATEGY_ID, cat_reversal.STRATEGY_ID}
)


def _strategy_expected_edge_pct(settings: Settings) -> dict[str, Decimal]:
    """The §7.1 C3 per-strategy edge registrations the gate is built with — the OTHER half of the
    promotion state above, extracted so both halves are assertable without booting an engine.

    §6.1 ``ins`` (2026-08-17): the C3 edge check derives the expected move from the proposal's
    TARGET, and ``ins`` has none by design (its exit is the §7.1 20-td time cap, and the drift it
    captures was MEASURED, not predicted). Rather than invent a target to feed the arithmetic, the
    strategy registers its validated T+20 NET drift (WO-16, owner-set in ``settings.yaml``, never
    learner-movable). Used ONLY when ``target_price`` is None; every other strategy and every
    targetless proposal without a registered edge behaves exactly as before.

    §8.6 ``hi52`` (2026-09-12): registered on the SAME seam for the same reason — its exit is the
    §7.1 20-session time cap and its drift was MEASURED, not predicted to a level, so it ships
    ``target=None`` and the gate has no levels to derive an edge from. Its number is a FORWARD-TEST
    edge, not a validated one (``settings.yaml`` carries the derivation and the kill rule).
    CONDITIONAL on the key being PRESENT, unlike ``ins``'s: ``Hi52Cfg.expected_edge_pct`` defaults to
    ``None`` precisely so that deleting the ``settings.yaml`` key — step 2 of the kill criterion —
    actually demotes. Registering it unconditionally off a model default would make that edit a
    no-op and leave the gate consuming an edge from a number ``settings.yaml`` does not show. A
    targetless proposal with no registered edge is the gate's ordinary ``_NO_TARGET`` reject, so the
    absence fails closed.
    """
    edges = {ins.STRATEGY_ID: Decimal(str(settings.ins.expected_edge_pct))}
    if settings.hi52.expected_edge_pct is not None:
        edges[hi52.STRATEGY_ID] = Decimal(str(settings.hi52.expected_edge_pct))
    return edges

#: Strategies whose candidates can carry a ``catalyst_ref`` — the §2.7 news-originated legs. Read at
#: ONE place: :func:`_hydrate_prescreen`, which uses it to rebuild the
#: ``catalyst_guard.max_catalyst_entries_day`` budget across a restart from a journal that records no
#: refs (see ``SignalPreScreen.hydrate``). Enforcement itself is keyed on the FIELD, never on this
#: set — a strategy id must not be what decides whether the news guard applies — so a leg missing
#: from here still faces the cap while it runs; it only loses budget continuity over a boot.
CATALYST_STRATEGY_IDS: frozenset[str] = frozenset({cat.STRATEGY_ID, cat_reversal.STRATEGY_ID})

#: Sweep triggers that ALSO run the once-per-session legs (today: ``hi52``'s ~480-symbol × 400-session
#: daily read). ``freeze_lift`` is a ``window_open`` sweep re-run because a freeze swallowed the first one,
#: so it must take every ``window_open`` branch — keying those branches on this set rather than on the
#: literal is what stops the two triggers drifting apart. ``scan_now`` stays out: its cadence is the
#: owner's, and the legs read completed sessions only. Module-level so the membership is assertable
#: without booting the engine.
_FULL_SWEEP_TRIGGERS: frozenset[str] = frozenset({"window_open", "freeze_lift"})

#: A lift this close behind a FINISHED sweep re-publishes nothing that sweep did not (the pre-screen
#: dedupe holds every pair it already admitted), and every sweep costs a full eligible-universe daily
#: history read. Two minutes covers a lift racing the window-open sweep that it interrupted.
_FREEZE_LIFT_MIN_GAP = timedelta(minutes=2)

#: How long a graceful stop waits for an in-flight freeze-lift sweep before cancelling it. The wait
#: is what keeps a worker thread from outliving `store.close()`; the bound is what keeps a wedged
#: sweep from holding the service stop open.
_SHUTDOWN_LIFT_WAIT_S = 15.0



def _subscription_tokens(symbols: Iterable[str], token_for_symbol: Callable[[str], int | None]) -> list[int]:
    """Ticker subscription symbols → tokens (A3): FIRST-occurrence dedupe (the caller's order is the
    priority order), and a symbol with no token in today's dump drops out — there is nothing to
    subscribe to, and the ticker child would reject the whole frame."""
    out: list[int] = []
    seen: set[str] = set()
    for sym in symbols:
        if sym in seen:
            continue
        seen.add(sym)
        tok = token_for_symbol(sym)
        if tok is not None:
            out.append(tok)
    return out


def _ticker_tokens(*, watchlist: Sequence[str], held: Sequence[str], batch_state: dict,
                   today: date, token_for_symbol: Callable[[str], int | None]) -> list[int]:
    """THE ticker subscription set (A3): watchlist → HELD → TODAY's batch admissions → index → VIX.

    Order is priority order — :func:`_subscription_tokens` keeps the FIRST occurrence, so a batch
    symbol that is already on the watchlist does not duplicate. The day roll happens HERE rather than
    in the caller: a composition handed an already-rolled list would keep passing its test after the
    reset was dropped, and an ever-growing subscription walks into the per-connection cap."""
    return _subscription_tokens(
        [*watchlist, *held, *sorted(_roll_batch_ticks(batch_state, today)),
         INDEX_SYMBOL, VIX_SYMBOL],
        token_for_symbol,
    )


def _roll_batch_ticks(state: dict, today: date) -> set[str]:
    """The batch legs' admitted-symbol set for ``today``, RESET on the day change (R6 midnight
    rollover): yesterday's brk20/hi52/cat/ins admissions are not today's universe, and an
    ever-growing subscription set would walk into the A3 per-connection cap. Mutable — callers
    ``update()`` it in place."""
    if state["day"] != today:
        state["day"], state["symbols"] = today, set()
    return state["symbols"]


#: What a caller hears when its sweep was refused because one is already running. A verdict, never
#: silence (§3.2.5): the owner's /scan_now reply must say why no candidate list came back.
_SWEEP_IN_FLIGHT_REPLY = "a scanner sweep is already running — its verdict lands in a moment"


async def _single_flight_sweep(lock: asyncio.Lock, sweep: Callable[[str], Awaitable[str]],
                               trigger: str, *, queue: bool = False) -> str:
    """Run ``sweep(trigger)`` unless one is already running, in which case log and decline.

    ``lock.locked()`` is read and, when free, taken with no await in between (``asyncio.Lock.acquire``
    returns synchronously on an uncontended lock), so the check and the claim cannot interleave on the
    single event loop. ``queue=True`` waits for the running sweep instead — for the freeze lift, whose
    whole purpose is that the day's batch origination is not lost; it is bounded by the lift's own
    once-per-minute claim."""
    if lock.locked():
        if not queue:
            _log.info("scan_sweep_skipped_in_flight", trigger=trigger)
            return _SWEEP_IN_FLIGHT_REPLY
        # Named too: a lift that silently waited out a slow sweep is the "nothing happened" log 09-04
        # took a day to read.
        _log.info("scan_sweep_queued_behind_in_flight", trigger=trigger)
    async with lock:
        return await sweep(trigger)


def _sweep_window_active(now: datetime, calendar: NSECalendar, mode: ModeManager) -> bool:
    """May the on-demand sweep fire at ``now``? A live session, inside the owner trade window, in a
    mode that originates (§3.2.5). ONE definition for both call sites — the INACTIVE→ACTIVE window
    tick and the freeze-lift subscriber — so the edge and the re-sweep cannot drift apart."""
    session_day = calendar.session(now.date())
    window = mode.get_trade_window()
    return bool(
        session_day is not None and window is not None
        and session_day.open <= now <= session_day.close
        and window.start <= now.time() <= window.end
        and mode.mode() in (Mode.RECOMMEND, Mode.AUTO)
    )


def _retest_active(now: datetime, calendar: NSECalendar, mode: ModeManager, kill: KillSwitch,
                   sweep_lock: asyncio.Lock) -> bool:
    """May the brk20 RETEST re-offer a level at ``now``? (2026-09-12 review of WO-R.)

    The sweep's window predicate, AND the states in which the pipeline is certain to DROP the
    publication: ``risk_state != NORMAL`` and the kill switch (``pipeline.on_signal_candidate``
    re-arms the pair on either). The sweep may publish into a freeze because the freeze-lift sweep
    re-publishes it afterwards; the retest has no lift path, and its once-a-day offer bound is spent
    at OFFER time — so an offer into a freeze would silence that level for the rest of the day. It
    also waits while a sweep is RUNNING (``sweep_lock.locked()``, the same signal the freeze lift
    reads): a pair that sweep re-armed has an open ``(symbol, strategy)`` dedupe slot, and a
    days-old level must not take it ahead of today's fresh crossing of the same symbol."""
    return (
        _sweep_window_active(now, calendar, mode)
        and mode.risk_state() == RiskState.NORMAL
        and not kill.is_killed()
        and not sweep_lock.locked()
    )


def _freeze_lift_skip_reason(now: datetime, last_fired, last_sweep: Mapping[str, Any], *,
                             in_flight: bool = False) -> str | None:
    """Why this freeze lift must NOT re-sweep, or ``None`` to fire.

    Three debounces, all about re-entry rather than correctness (a re-sweep is idempotent — the
    pre-screen's same-day dedupe drops everything already admitted): a cause latch resolving several
    causes at once publishes a NORMAL transition per cause, so at most one sweep per (day, minute); a
    sweep still RUNNING will publish under the state this lift just established, so it needs no
    second one beside it; and a lift landing on the heels of a finished sweep has nothing new to
    publish.

    ``in_flight`` is the single-flight lock's own ``locked()`` (2026-09-12 review), never an
    inference from the phase marks: a sweep that RAISES never stamps ``done_at``, and a "running"
    derived from ``started_at`` would latch the lift shut for as long as any grace period — the
    set-without-clear shape that froze entries for two sessions on 2026-09-01. The lock is released
    by ``async with`` on every exit path, so it is the one signal that cannot stick.

    ``last_sweep["frozen"]`` is what keeps the third rule from eating the case it was written around.
    The window-edge tick and the warm-up refresh both ride 60 s timers armed at the same boot, so on
    an in-session boot the window_open sweep and the lift that follows it land within the SAME two
    minutes — and that sweep published its whole batch into the freeze. A sweep that published while
    FROZEN is the one to redo, never the one to debounce against."""
    if last_fired == (now.date(), now.hour, now.minute):
        return "already_swept_this_minute"
    started = last_sweep.get("started_at")
    done, published = last_sweep.get("done_at"), last_sweep.get("published_at")
    # A held lock with no start mark yet is a sweep between `acquire` and its first phase mark — it
    # has published nothing, so it too will publish under this lift's state.
    if in_flight and (published is None or started is None or published < started):
        return "sweep_in_flight_pre_publication"
    if (done is not None and not last_sweep.get("frozen")
            and now - done < _FREEZE_LIFT_MIN_GAP):
        return "sweep_finished_under_2min_ago"
    return None


def _ins_rows_to_consume(symbols: Sequence[str], *, in_window: bool,
                         published_frozen: bool) -> list[str]:
    """Which ``ins_pending`` rows this sweep may mark consumed — the §6.1 once-only bound's gate.

    Consume EVERY row READ, not just the admitted ones: a row suppressed by the daily cap or the
    (symbol, strategy) dedupe has HAD its evaluation, and leaving it unconsumed would re-offer it on
    every later sweep tick forever. Suppression is a decision.

    Two refusals are NOT decisions, and unlike brk20/hi52/cat — re-derived from stored bars on any
    later sweep — a consumed crossing is gone for the day (the EOD job decided the event; nothing
    here re-computes it):

      * a WINDOW refusal (2026-08-18 review): the whole premise of the batch window gate is that a
        shut-window candidate was never evaluated, so an out-of-window /scan_now must leave the day's
        crossings pending for the next in-window sweep;
      * a publication into a FREEZE (2026-09-11 forensics): ``pipeline.on_signal_candidate`` re-arms
        the pair and no next bar re-publishes a batch rule — the freeze-lift re-sweep is that
        re-publication, and it can only re-derive `ins` from rows still marked pending. This is the
        one leg 08-28/08-31/09-04 could not have recovered even with the lift wired.

    Never a widening of the bound: an unconsumed row that is never re-published simply dies with its
    ``for_session`` date, and the pre-screen's same-day dedupe keeps a re-offer from spending
    anything."""
    if not in_window or published_frozen:
        return []
    return list(symbols)


async def _freeze_lift_sweep(evt, clock: Clock, calendar: NSECalendar, mode: ModeManager,
                             sweep, state: dict, last_sweep: dict, *,
                             ready: Callable[[], bool],
                             in_flight: Callable[[], bool] = lambda: False) -> None:
    """Re-run the scanner sweep when a freeze LIFTS inside the trade window (2026-09-11 forensics).

    The batch legs originate only from the sweep, and a candidate published into a standing freeze is
    re-armed by the pipeline with nothing left to re-publish it — batch rules have no next bar. Every
    in-session boot re-applies a warm-up freeze at lifecycle step 6 and re-fires the window edge, so
    the sweep landed inside the freeze and the day's brk20/hi52/cat/ins origination was lost (08-28,
    08-31, 09-04). ONLY the FROZEN-side→NORMAL edge fires: a NORMAL→FROZEN transition has nothing to
    re-publish, and a FROZEN→KILLED one must not trade. ``ready`` is the boot contract's
    ``engine_ready`` — the recovery itself clears causes, and a sweep must never run ahead of the
    backfill/catch-up/ticker steps it reads from. ``in_flight`` is the sweep lock's ``locked``
    (the composition root passes ``_sweep_lock.locked``; the default exists for the unit harness
    only and is asserted against in the wiring test). Extracted from the composition root so the
    predicate, the debounce and the edge are testable without booting the engine."""
    if evt.new_state != RiskState.NORMAL or evt.old_state == RiskState.NORMAL:
        return
    now = clock.now()
    if not ready():
        # BOOT GUARD (2026-09-11 review). Two lift paths fire from INSIDE the recovery — the
        # self-test's `clear_stale_daily` at step 1c and step 5's `catchup_safety_jobs` clear — i.e.
        # ahead of step 4's data-gap backfill, step 5's daily_bars catch-up, step 6's warm-up gate and
        # step 7's ticker resume. A sweep there reads a daily history still missing the last
        # session(s) (wrong brk20/hi52 breakout references in both directions), CHARGES those pairs
        # through prescreen.admit so the real window-open sweep is deduped out of exactly them, and
        # resubscribes nothing (the ticker is still STOPPED). Nothing is lost by waiting: a lift this
        # early precedes every sweep, so there is nothing to redo, and the INACTIVE→ACTIVE window
        # edge sweeps in NORMAL once the scheduler is armed.
        _log.info("freeze_lift_sweep_skipped", reason="boot_in_progress",
                  old_state=evt.old_state.value, at=now.isoformat())
        return
    if not _sweep_window_active(now, calendar, mode):
        return
    skip = _freeze_lift_skip_reason(now, state.get("fired"), last_sweep, in_flight=in_flight())
    if skip is not None:
        _log.info("freeze_lift_sweep_skipped", reason=skip, old_state=evt.old_state.value,
                  at=now.isoformat())
        return
    # Claimed BEFORE the await: a sweep is seconds long and a second lift inside it must not start a
    # concurrent one.
    state["fired"] = (now.date(), now.hour, now.minute)
    done_before = last_sweep.get("done_at")
    try:
        await sweep("freeze_lift")
    except Exception:  # noqa: BLE001 - a sweep failure must never break the risk-state fan-out
        _log.exception("freeze_lift_sweep_failed")
        return
    swept = last_sweep.get("done_at") != done_before   # False ⇒ the session closed under the sweep
    _log.info("freeze_lift_sweep", old_state=evt.old_state.value, reason=evt.reason, swept=swept,
              published=last_sweep.get("published", 0) if swept else 0,
              pending=last_sweep.get("pending", 0) if swept else 0)


#: PUBLICATION order of the batch legs (2026-09-12 review decision), by strategy id. Module-level
#: so the property is assertable without booting the engine. This is the order the legs are
#: CONCATENATED in for the ranked admit — publication simply stops depending on the sort that
#: allocates the caps. A strategy not listed here publishes after the listed ones, in admit order.
_PUBLICATION_LEG_ORDER: tuple[str, ...] = (
    brk20.STRATEGY_ID, ins.STRATEGY_ID, cat.STRATEGY_ID, cat_reversal.STRATEGY_ID, hi52.STRATEGY_ID,
)


def _publication_order(admitted: list) -> list:
    """PUBLICATION order for one batch admission: LEG order (:data:`_PUBLICATION_LEG_ORDER`), and
    inside one leg the order ``SignalPreScreen.admit`` returned (its own score-descending rank).

    ADMISSION and PUBLICATION are two separable consequences of one ranked list, and only the first
    one ``prescreen._rank`` reasons about ("a batch competes for the shared daily cap, and the
    per-strategy sub-caps — not this sort — are what bound one strategy's share"). The second is
    ``fired_at``: ``bus.apublish`` order stamps it, and ``pipeline._forward_key`` breaks ties INSIDE
    a per-strategy quantile band by ``fired_at`` ASCENDING — so whoever publishes first wins the
    scarce analyst slot among candidates of comparable standing, which under
    ``MIN_RANK_POPULATION`` = 3 is every strategy's first two candidates of the day (all in the
    middle band).

    The raw scores that sort that ranked list are NOT comparable across strategies: ``hi52`` scores
    ``prox`` in [0.95, 1.0], ``ins`` ~0.5 at a bare ₹1cr crossing, ``brk20`` ~0.5-0.6 — which is
    exactly why the forward slot ranks by per-strategy QUANTILE and not by raw score. Letting that
    same cross-strategy sort also stamp ``fired_at`` would smuggle the incomparable scale back in
    through the tie-break: folding hi52 into the ranked batch (the §8.6 promotion) put an
    UNVALIDATED forward test at the head of the publication queue every morning and handed it those
    ties against the platform's only validated leg, under a DG1-degraded forward cap (4/day, run at
    cap all of 2026-09-10). Publishing in LEG order removes the whole class: no strategy's score
    SCALE can buy it queue priority, and the fixed order is a stated, auditable precedence — the
    actionable legs by seniority, the 2026-09-12 forward test last — rather than an emergent
    property of three unrelated scoring formulas.

    ``sorted`` is stable, so within each leg the admit order (score descending) is untouched, and
    §9.6 replay determinism is unaffected — the key reads only ``strategy_id``, never a clock.
    """
    rank = {sid: i for i, sid in enumerate(_PUBLICATION_LEG_ORDER)}
    return sorted(admitted, key=lambda c: rank.get(c.strategy_id, len(rank)))


def _hi52_daily_leg(store: MarketStore, *, trigger: str, eligible: Sequence[str], today: date,
                    yesterday: date, ex_map: Mapping[str, list[date]]) -> list:
    """The `hi52` v2 daily leg (§6.1/§8.6): 52-week-high-proximity FRESH-CROSSES over COMPLETED daily
    sessions. Returns RAW candidates for the sweep's ONE ranked batch admission.

    Scoped to the ELIGIBLE universe since 2026-09-09: the 09-03 full-market backtest measured the edge
    as index-class only (extended names −0.26% net / 49% hit at T+20, n=13,922), so the extended-name
    population is no longer originated. Swing thesis (T+5..T+20, George & Hwang drift; intraday
    capture is cost-refuted).

    PROMOTED 2026-09-12 (plan §8.6 addendum): until then this was a shadow whose own SECOND
    ``prescreen.admit`` ran AFTER the actionable one, so candidates scoring in [0.95, 1] could only
    take LEFTOVER day-cap capacity. A promoted rule cannot keep that seat — its candidates now go
    into the SAME concatenated batch as brk20/ins/cat/cat_reversal and are ranked against them under
    the ordinary per-strategy cap (``max_per_strategy_day.hi52`` = 3, unchanged). The 08-18 fairness
    lesson that motivated the second admit is served by that single ranked admit, which is what it
    was built for; what the merge costs is LATENCY — this leg's ~480-symbol × 400-session history
    read now sits BEFORE the batch admit instead of after it. The bar-driven ``prescreen.sweep`` at
    the top of the worker is untouched and still admits first. What the merge deliberately does NOT
    buy hi52 is the front of the PUBLICATION queue: :func:`_publication_order` keeps it last, so the
    forward-slot ``fired_at`` tie-break stays where it was before the promotion.

    Session-cadence triggers only (``_FULL_SWEEP_TRIGGERS``): the signal derives from COMPLETED
    sessions, so one scan per session is its natural cadence and /scan_now stays cheap. ``freeze_lift``
    is IN that set — it is the window_open sweep re-run because a freeze swallowed the first one
    (2026-09-11), so excluding it would lose the leg for the day exactly as the swallowed sweep did.
    Extracted from the sweep body so THAT is testable on the candidates rather than on a source
    string."""
    if trigger not in _FULL_SWEEP_TRIGGERS:
        return []
    hi52_histories: dict[str, list[brk20.DailyRow]] = {}
    hi52_start = today - timedelta(days=400)   # ≥252 sessions + weekend/holiday margin
    # No bars_1d source re-adjusts STORED history across an ex-date (Kite candles are adjusted at
    # fetch time, but the series is seeded once and extended one session at a time; bhavcopy rows are
    # raw), so a bonus/split/rights/demerger inside the window leaves phantom pre-ex highs: those
    # symbols sit out until it rolls past (2026-09-03).
    hi52_unadjusted = hi52.unadjusted_history(
        store.get_corp_actions(ex_from=hi52_start, ex_to=yesterday)
    )
    for sym in eligible:                       # the actionable legs' set (2026-09-09)
        frame = store.get_bars_1d_frame(sym, hi52_start, yesterday)
        if len(frame):
            hi52_histories[sym] = [
                brk20.DailyRow(high=float(h), close=float(c), volume=float(v), open=float(o))
                for h, c, v, o in zip(
                    frame["high"], frame["close"], frame["volume"], frame["open"], strict=True,
                )
            ]
    hi52_vetoes: dict[str, int] = {}
    hi52_raw = hi52.sweep_daily(
        hi52_histories, today=today, ex_dates_by_symbol=ex_map,
        unadjusted_symbols=hi52_unadjusted, veto_counts=hi52_vetoes,
    )
    # The v2 GATE's own inputs, per SURVIVING candidate. The aggregate veto counts say how much flow
    # the filters ate; they say nothing about the shape of the population that PASSED, and the
    # pre-registered 20- and 40-signal reviews ask exactly that — was the live smooth distribution
    # the 09-09 study's? Nothing else records it: these two numbers have no place on SignalCandidate
    # (§3.2.5 field set) and no row of their own. Computed off the SAME rows and the SAME helper the
    # gate used, and only for candidates (a handful), so the per-symbol cost discipline holds.
    candidate_diag = {}
    for cand in hi52_raw:
        diag = hi52.diagnostics_for(hi52_histories.get(cand.symbol, ()))
        if diag is not None:
            candidate_diag[cand.symbol] = {
                "up_day_frac": diag.up_day_frac, "max_day_move": diag.max_day_move,
            }
    # ORIGINATION-stage counts (the brk20/cat convention): candidates + vetoes reconcile against
    # symbols_scanned on one line, and what the §3.2.5 caps then do with the survivors is the
    # prescreen's own logging — no `admitted` here any more, because this leg no longer admits.
    # The v2 filters are the two counts that can eat a whole day's flow (the 09-09 v2 run dropped
    # 12,218 of its 16,663 discrete signals on them — smooth 11,405 + gap 813), so a quiet tape must
    # be readable as their decision.
    _log.info(
        "hi52_sweep", d=today.isoformat(), trigger=trigger,
        symbols_scanned=len(hi52_histories),
        candidates=len(hi52_raw),
        ex_date_vetoes=hi52_vetoes.get(hi52.VETO_EX_DATE_SKIP, 0),
        unadjusted_vetoes=hi52_vetoes.get(hi52.VETO_UNADJUSTED_HISTORY, 0),
        smooth_vetoes=hi52_vetoes.get(hi52.VETO_SMOOTH, 0),
        gap_vetoes=hi52_vetoes.get(hi52.VETO_GAP, 0),
        candidate_diag=candidate_diag,
    )
    return hi52_raw


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


# --------------------------------------------------------------------- brk20 RETEST re-arm (WO-R)
def _pending_entry_rec_symbols(conn: sqlite3.Connection, now: datetime) -> set[str]:
    """Symbols carrying an UNEXPIRED, UNACTIONED entry recommendation — the gate's own
    ``GateContext.pending_rec_symbols`` set, read here because the retest must refuse what the gate
    would refuse (``per_stock_exposure: already held or pending`` is a HARD reason, uncurable by
    shrinking) BEFORE the slot and the analyst call are spent rather than after.

    Shares ``core.recommendations.parse_valid_until`` with the gate rather than re-deriving expiry:
    an absent/naive/unparseable ``valid_until`` stays EXCLUDED, exactly as it does there. An
    unreadable row is skipped, not fatal — this is a narrowing, and the caller's screen is what fails
    to zero if the whole read raises."""
    out: set[str] = set()
    # The actioned rows are dropped in SQL because this runs on a 60 s pulse and `recommendations`
    # only grows; the Python guard below stays, and the predicates MATCH — the gate's test is
    # truthiness, so an empty-string action is "not actioned" on both sides.
    rows = conn.execute(
        "SELECT payload, human_action FROM recommendations "
        "WHERE human_action IS NULL OR human_action = ''"
    ).fetchall()
    for row in rows:
        if row["human_action"]:
            continue                    # taken / expired / dismissed / closed ⇒ not pending
        try:
            data = json.loads(row["payload"] or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(data, dict) or data.get("kind") != "entry":
            continue
        valid_until = parse_valid_until(data.get("valid_until"))
        if valid_until is not None and valid_until > now and data.get("instrument"):
            out.add(str(data["instrument"]))
    return out


def _retest_skip_reasons(
    symbols: Sequence[str], *,
    eligible: Collection[str], ex_skip: Collection[str],
    held: Collection[str], pending: Collection[str],
    unadjusted: Collection[str] = (),
) -> dict[str, str]:
    """``{symbol: reason}`` for every due resting level that must NOT be re-offered today.

    Five narrowings, none of which the book can know on its own, in the order their reasons are
    reported (a symbol can qualify under several; the first is the one logged):

    * **held** — a position is open on it. The mechanism the backtest measured books ONE trade per
      signal; re-offering here spends a §3.2.5 day slot and one of the day's analyst calls on a
      proposal ``gate._rule_per_stock_exposure`` is certain to hard-reject.
    * **pending** — an unexpired, unactioned entry recommendation already sits on it: the same gate
      reason, and the owner is already looking at this trade.
    * **ex_skip** — a known ex-date inside brk20's own A12 horizon. ``bars_1d`` is never re-adjusted,
      so the stored level is a pre-ex price; ``brk20.scan_daily`` refuses such a symbol on the
      crossing day and a re-offer must refuse it on the retest day for the same reason.
    * **unadjusted** — the BACKWARD half of the same veto (2026-09-12 review): a structural ex-date
      (bonus/split/rights/demerger) already inside the 35-day lookback, the set the sweep feeds to
      ``brk20.sweep_daily(unadjusted_symbols=...)``. ``ex_skip`` looks forward from today only, so an
      ex-date that passed BETWEEN the crossing and the retest would otherwise re-offer a pre-ex
      level as a limit ABOVE the adjusted tape.
    * **not eligible** — the symbol has left the eligible universe since the crossing (liquidity or
      index churn). ``brk20``'s registered origination scope IS that population; a level may not
      outlive it just because it was journalled while the symbol was still in.

    An EMPTY ``eligible`` would skip everything, which is why the caller only arms the retest once
    today's in-window sweep has published the set (`sweep_ready`)."""
    out: dict[str, str] = {}
    for sym in symbols:
        if sym in held:
            out[sym] = "already held — §7.1 per_stock_exposure is a HARD reject"
        elif sym in pending:
            out[sym] = "an unexpired, unactioned entry recommendation is already pending"
        elif sym in ex_skip:
            out[sym] = "a known ex-date inside brk20's A12 horizon (the stored level is pre-ex)"
        elif sym in unadjusted:
            out[sym] = "a structural ex-date inside the 35-day lookback (stored history is in two units)"
        elif sym not in eligible:
            out[sym] = "no longer in today's eligible universe (brk20's origination scope)"
    return out


def _retest_republish(
    retest_book: RestingLevelBook,
    prescreen: SignalPreScreen,
    features: FeatureEngine,
    *,
    active: bool,
    sweep_ready: bool,
    ltp_fn: Callable[[str], Decimal | None],
    tick_age_fn: Callable[[str], float | None],
    limits_fn: Callable[[], tuple[float, float]],
    skip_fn: Callable[[list[str]], Mapping[str, str]],
    today: date,
) -> list[SignalCandidate]:
    """One 60 s retest tick: expire, find the levels back inside the band, admit them.

    SYNCHRONOUS AND OFF-LOOP. ``prescreen.admit`` takes ``SignalPreScreen._lock`` and the post-admit
    mint takes ``MarketStore._lock``; the caller runs this whole body through ``asyncio.to_thread``,
    as ``prescreen.handle_bar`` and ``_collect_and_scan`` do, because either lock can be held for
    tens of seconds (a partition COPY) and the event loop owns the tick cache, the §2.2 heartbeat and
    the kill path.

    The book's three operations are TOTAL (a journal failure logs and yields nothing), and an
    unreadable band returns early; what can still raise is ``prescreen.admit`` itself, which
    propagates to the drain tick's own guard — logged there, with the drain still running after it.
    Nothing in this function may take the scheduler loop down.

    The `brk20` levels admitted over the last ``retest_sessions`` sessions rest in
    ``brk20_resting_levels`` (migration 0014); this is the pulse that re-offers one the moment its
    price returns to it — the ``V2_limit_at_H20_N5`` mechanism the 2026-09-12 pre-registered
    entry-mechanism backtest selected over both market-on-confirmation and the shipped single-session
    publication (see ``engine.strategy.retest``).

    ``active`` is the caller's ``_sweep_window_active`` verdict — a live session, inside the owner
    trade window, in a mode that originates. INACTIVE does nothing at all, not even the expiry sweep:
    outside the window there is no live LTP worth banding against and nothing may spend a §3.2.5 day
    slot, and expiry is idempotent housekeeping that the next in-window tick performs anyway (``due``
    re-tests the window itself, so a day with no in-window tick re-offers nothing it should not).

    ADMISSION is ``prescreen.admit`` — the SAME call, the same day cap, the same per-strategy cap and
    the same same-day ``(symbol, strategy)`` dedupe the sweep faces. ``in_window=True`` is a
    statement of ``active``, not a bypass of anything: the pre-screen is Clock-free by design and the
    caller owns the window verdict (see ``SignalPreScreen.admit``). The ``features_snapshot_id`` is
    minted AFTER that admit by the sweep's own ``_attach_feature_snapshots``, so a candidate the caps
    suppress never spends a snapshot write — and a candidate with a null id is structurally
    un-recommendable (intraday.py Rule 6), which is why it cannot simply be left off.

    ``sweep_ready`` is "today's IN-WINDOW sweep has published". Until it has, this tick only expires:
    the day's ranked admission must reach ``prescreen.admit`` FIRST, or a level broken days ago can
    take the ``(symbol, strategy)`` dedupe slot moments before the same symbol's fresh crossing —
    which would then be dropped as a duplicate and, being un-admitted, never journalled either, so
    the book would carry the stale geometry for the rest of the window. It is also what guarantees
    ``skip_fn`` reads TODAY's eligible set and ex-date horizon rather than an empty one.

    A band that cannot be read (an unverifiable limits store) re-offers NOTHING: the band is the only
    thing standing between this mechanism and re-publishing a level the gate is certain to reject, so
    an unreadable one fails to zero rather than to a constant (§2.4 item 1 — limits are read at the
    enforcement site or not used). ``max_tick_age_s`` rides the SAME load: the trigger price must be
    as fresh as the gate will demand of it.

    ``brk20_retest_republished`` is emitted here, per candidate ``prescreen.admit`` ACCEPTED, off the
    ``offers`` detail the book fills in place. The book's own line is ``brk20_retest_offered``, so
    the two stages of the funnel are separately greppable (WO-9) instead of a re-publication count
    that includes everything the caps suppressed.
    """
    if not active:
        return []
    retest_book.expire(today)
    if not sweep_ready:
        return []
    try:
        band_pct, max_tick_age_s = limits_fn()
    except Exception as exc:  # noqa: BLE001 - an unreadable band re-offers nothing (D7, §2.4)
        _log.warning("brk20_retest_band_unreadable", d=today.isoformat(), error=str(exc))
        return []
    offers: dict[str, dict[str, str]] = {}
    due = retest_book.due(ltp_fn, band_pct, today, tick_age_fn=tick_age_fn,
                          max_tick_age_s=max_tick_age_s, skip_fn=skip_fn, offers=offers)
    if not due:
        return []
    published = _attach_feature_snapshots(features, prescreen.admit(due, today, in_window=True))
    for cand in published:
        _log.info("brk20_retest_republished", signal_id=cand.signal_id,
                  **offers.get(cand.signal_id, {"symbol": cand.symbol}))
    _log.info("brk20_retest_tick", d=today.isoformat(), band_pct=band_pct,
              max_tick_age_s=max_tick_age_s,
              due=len(due), published=len(published),
              symbols=[c.symbol for c in published])
    return published


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
