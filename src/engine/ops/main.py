"""Composition root + service entrypoint.

The ONLY module allowed to import everything — it wires the dependency graph and runs the engine.
``python -m engine.ops.main`` (or the ``mt-engine`` console script). Start is the SAME code path whether
manual/demand or scheduled; every startup runs the full recovery sequence via
``SessionLifecycle.startup``.

``run`` drives three phases: :func:`compose` builds the graph, :func:`_boot` recovers and arms it,
:func:`_shutdown` tears it down. Daily jobs sit on one :class:`~engine.ops.jobs.JobRegistry` driven by BOTH
the live :class:`~engine.ops.scheduler.Scheduler` and the startup :class:`~engine.ops.jobs.CatchUpRunner`.

Everything broker-touching (instruments dump, backfill, reconcile, ticker) is built only when Kite
credentials exist — a fresh install with no secrets stays runnable and safe (entries FROZEN until
login). Live AUTO routing and the real-order shutdown guard are not wired.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import time as time_module  # `time` itself is datetime.time here (below) — the boot-contract watchdog needs monotonic()
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple, NoReturn

import httpx
import pandas as pd
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from engine.broker.instruments import InstrumentStore, UnknownInstrument
from engine.broker.kite_client import KiteClient, OrderSurfaceViolation
from engine.broker.rate_limiter import RateLimiter
from engine.broker.session import SessionManager
from engine.broker.ticker_supervisor import FEED_HEALTH_TOPIC, TickerSupervisor
from engine.core.browser_ua import BROWSER_USER_AGENT
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings, config_dir, load_settings, load_yaml, repo_root
from engine.core.contracts import EnterAction, GateVerdict
from engine.core.db import connect
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.migrations import apply_migrations
from engine.core.protected_store import IntegrityError, ProtectedStore
from engine.core.recommendations import pending_entry_symbols
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.secrets import DASHBOARD_TOKEN, KITE_API_KEY, TELEGRAM_BOT_TOKEN, Secrets
from engine.core.types import TradeWindow
from engine.datafeeds.bhavcopy import BhavcopyJob, BhavcopyResult
from engine.datafeeds.corp_actions import CorpActionsJob, CorpActionsResult
from engine.datafeeds.deals import DealsJob, DealsResult
from engine.datafeeds.earnings_calendar import EarningsCalendarJob, EarningsCalendarResult
from engine.datafeeds.filings_pit import FilingsPitJob, FilingsPitResult
from engine.datafeeds.filings_pit_fresh import FilingsPitFreshJob, FilingsPitFreshResult
from engine.datafeeds.filings_results import (
    FilingsResultsJob,
    FilingsResultsResult,
    ResultsLineItemsJob,
    ResultsLineItemsResult,
)
from engine.datafeeds.filings_shp import FilingsShpJob, FilingsShpResult
from engine.datafeeds.ins_crossings import InsCrossingsJob, InsCrossingsResult
from engine.datafeeds.isin_map import IsinMapJob, IsinMapResult
from engine.datafeeds.news import NSE_ANN_KEY, Headline, NewsIngest
from engine.datafeeds.news_pipeline import (
    CatalystDigestJob,
    EntityResolver,
    HeadlineClusterer,
    load_cat_params,
)
from engine.datafeeds.sector_map import SectorMapJob, SectorMapResult
from engine.features.engine import FeatureEngine
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentHarness, load_agent_roster, resolve_cli_path, run_sdk_smoke
from engine.marketdata.backfill import BackfillJob, BackfillReport
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.reconcile import ReconcileJob, ReconcileReport
from engine.marketdata.store import MarketStore
from engine.marketdata.tick_compact import TickCompactionResult, compact_ticks
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage, MessageKind, catalyst_disabled, login_prompt
from engine.oms.exits import ExitManager
from engine.oms.manager import OrderManager, PaperOrderBlocked, paper_order_guard
from engine.oms.positions import PositionBook
from engine.oms.prep import SessionPrep
from engine.oms.protection import ProtectionManager
from engine.oms.reconcile import Reconciler
from engine.oms.state import PlatformOrder
from engine.ops.calendar_horizon import check_calendar_horizon
from engine.ops.capital import CapitalChanger
from engine.ops.early_hydration import EarlyHydration
from engine.ops.feed_freshness import FeedFreshnessJob, FeedFreshnessResult
from engine.ops.health import HealthMonitor
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.holdings_reconcile import (
    HoldingsReconcileJob,
    in_reconcile_window,
    positions_missing_from_holdings,
)
from engine.ops.holds import HoldFn, build_hold_fn, session_of
from engine.ops.jobs import (
    JOB_BACKUP,
    JOB_BHAVCOPY,
    JOB_CATALYST_DIGEST,
    JOB_CLAUDE_LOGIN,
    JOB_CORP_ACTIONS,
    JOB_DAILY_BARS,
    JOB_DEALS,
    JOB_EARNINGS,
    JOB_FEATURES,
    JOB_FEED_FRESHNESS,
    JOB_FILINGS_PIT,
    JOB_FILINGS_PIT_FRESH,
    JOB_FILINGS_RESULTS,
    JOB_FILINGS_SHP,
    JOB_INS_CROSSINGS,
    JOB_INSTRUMENTS,
    JOB_ISIN_MAP,
    JOB_MARKET_BACKUP,
    JOB_NEWS_CHAIN,
    JOB_NIGHTLY_REVIEW,
    JOB_PREOPEN_PLANNER,
    JOB_REC_OUTCOMES,
    JOB_RECO_EXPIRE,
    JOB_RECONCILE,
    JOB_RESULTS_LINE_ITEMS,
    JOB_SECTOR_MAP,
    JOB_SIGNAL_OUTCOMES,
    JOB_SURVEILLANCE,
    JOB_TICK_COMPACT,
    JOB_TIME_EXIT_CHECK,
    JOB_UNIVERSE,
    JOB_WEEKLY_SUMMARY,
    AdvisoryRun,
    CatchUpResult,
    CatchUpRunner,
    CatchUpScope,
    JobClass,
    JobRegistry,
    JobSpec,
    _job_result_ok,
    _job_unfinished,
)
from engine.ops.keep_awake import KeepAwake
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.news_scoring import NewsScoringJob
from engine.ops.nightly_review import NightlyReviewJob, read_funnel_raw_counts
from engine.ops.paper_control import PaperControl
from engine.ops.paper_risk import PaperRisk, paper_effective_state
from engine.ops.paper_runtime import (
    PAPER_TICK_S,
    PaperNotifier,
    PaperRuntime,
    backfill_bars_fn,
    exit_sim_replay,
    no_notify,
    store_corp_actions_fn,
    store_last_mark_fn,
    store_pre_ex_mark_fn,
)
from engine.ops.paper_summary import PaperView
from engine.ops.pipeline import RecommendationBook, RecommendationPipeline
from engine.ops.post_login import (
    PostLoginRecovery,
    hydrate_instruments_at_startup,
    regime_and_warmup_backfill,
    resume_ticker,
)
from engine.ops.preopen_planner import PreopenPlannerJob
from engine.ops.protection_reminders import ProtectionReminders
from engine.ops.rec_outcomes import RecOutcomesJob
from engine.ops.scan_context import LiveScanContextProvider
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import CLAUDE_RELOGIN_HINT, SelfTest, alert_claude_login, claude_credentials_path
from engine.ops.signal_outcomes import SignalOutcomesJob
from engine.ops.single_instance import InstanceLock
from engine.ops.stop_path import hard_exit, install_signal_handlers
from engine.ops.token_check import TOKEN_CHECK_IST, TokenCheckJob
from engine.ops.warmup import (
    CLASS_DAILY,
    CLASS_INTRADAY,
    CLASS_REGIME,
    WarmupGate,
    WarmupStatus,
)
from engine.ops.weekly_summary import WeeklySummaryJob
from engine.ops.why import make_why_fn
from engine.risk.causes import RiskStateLatch, feed_health_to_latch
from engine.risk.exposure import ExposureTracker
from engine.risk.gate import TERMINAL_ORDER_STATES_SQL, GateContextBuilder, RiskGate
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine, floor_limits_from
from engine.risk.mode import ModeManager
from engine.strategy import regime
from engine.strategy.cost_model import CostModel
from engine.strategy.hi52_edge import StrategyEdges
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.retest import RestingLevelBook
from engine.strategy.scanners import (
    SCANNER_REGISTRY,
    brk20,
    build_enabled_scanners,
    cat,
    cat_reversal,
    hi52,
    ins,
)
from engine.strategy.types import SignalCandidate, round_to_tick
from engine.universe.builder import UniverseBuilder
from engine.universe.leverage import MisLeverageIngest
from engine.universe.surveillance import SurveillanceIngest

_log = get_logger("engine.ops.main")

#: Canonical bars_1d symbols the backfill job persists NIFTY 50 / India VIX under; the WarmupGate and
#: FeatureEngine defaults MUST match these (``regime_data_ready``).
INDEX_SYMBOL = regime.INDEX_SYMBOL
VIX_SYMBOL = "INDIA VIX"

#: Fallback tick size when an instrument is not in today's dump (NSE minimum) — mirrors
#: engine.marketdata.reconcile.DEFAULT_TICK_SIZE; only ever loosens the reconcile close-drift check.
_DEFAULT_TICK = Decimal("0.05")

#: Boot fallback ONLY: the tracker's base while the protected limit table fails verification (which also
#: FREEZES the boot, so it prices no entry). Keep at the shipped limits.yaml capital_base_inr; a dashboard
#: capital change moves the verified table, not this, so it may lag.
_FALLBACK_CAPITAL_BASE_INR = Decimal("40000")

#: How far back to scan universe_daily for the current active watchlist (bounded, trading-day-agnostic).
_WATCHLIST_LOOKBACK_DAYS = 15

#: Fire-times for the three jobs without a dedicated ``settings.jobs`` key: the news
#: chain runs just before the 08:30 universe build (backfill→cluster→resolve is never entry-blocking);
#: the nightly incremental daily-bar backfill and the feature snapshot follow the EOD data jobs.
#: Features must fire after the deals (20:30) and corp-actions (20:15) writes it reads (the ``order=40→50``
#: catch-up sequence encodes the same ordering) and before the 21:00 nightly review/backup slots.
_NEWS_CHAIN_IST = time(8, 25)
_DAILY_BARS_IST = time(18, 5)
_FEATURES_IST = time(20, 45)

#: Tick-partition compaction — late evening, after every EOD data job and the nightly
#: review, so a multi-minute filesystem pass never competes with them. Date-keyed: the run for day D
#: compacts the closed date partitions up to D (never D itself — the writer still owns it).
_TICK_COMPACT_IST = time(22, 30)

#: Daily read of the CLI's stored claude.ai login: ahead of the 08:25 pre-open chain, so a WARN reaches
#: the owner before the chain needs the login. Not the day's first LLM call (news scoring and early
#: hydration can hit a dead login first; their auth_failed alerts carry the re-login hint).
_CLAUDE_LOGIN_CHECK_IST = time(8, 10)

#: DuckDB backup (``EXPORT DATABASE``, :func:`_market_backup`) — after every EOD data job and the
#: 21:00 SQLite backup, ahead of the 22:30 compaction it shares :class:`CompactionLane` with.
_MARKET_BACKUP_IST = time(22, 0)

#: Async job-runner type: DATE_KEYED runners take the run-for ``date``; all others take no args.
JobRunFn = Callable[..., Awaitable[None]]

#: Every job id the composition root registers (watermark identities — never rename).
#: The single source of truth ``build_job_registry`` and the CatchUpRunner/Scheduler arming iterate.
CORE_JOB_IDS: tuple[str, ...] = (
    JOB_INSTRUMENTS, JOB_SURVEILLANCE, JOB_EARNINGS,          # safety/deadline-critical
    JOB_UNIVERSE, JOB_NEWS_CHAIN, JOB_CORP_ACTIONS, JOB_SECTOR_MAP, JOB_BACKUP,  # run-latest
    JOB_ISIN_MAP, JOB_FILINGS_SHP, JOB_RESULTS_LINE_ITEMS,                       # run-latest
    JOB_FEED_FRESHNESS, JOB_CLAUDE_LOGIN,                                        # run-latest
    JOB_RECONCILE, JOB_BHAVCOPY, JOB_DAILY_BARS, JOB_DEALS, JOB_FEATURES,        # date-keyed
    JOB_FILINGS_PIT, JOB_FILINGS_PIT_FRESH, JOB_FILINGS_RESULTS,                 # date-keyed
    JOB_INS_CROSSINGS,                                                           # date-keyed (`ins`)
    JOB_TICK_COMPACT,                                                            # date-keyed
    JOB_MARKET_BACKUP,                                                           # run-latest
)

#: Decision-plane additions: digest → planner run pre-open in dependency order after the news chain;
#: rec-expiry labels stale unconfirmed recommendations post-window; the nightly reviewer is date-keyed
#: (one review per missed trading day). build_job_registry skips a None fn (tests); the
#: composition root always passes a wrapper.
DECISION_JOB_IDS: tuple[str, ...] = (
    JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER, JOB_RECO_EXPIRE,   # run-latest
    JOB_TIME_EXIT_CHECK,                                         # run-latest (regular sessions)
    JOB_REC_OUTCOMES, JOB_SIGNAL_OUTCOMES, JOB_WEEKLY_SUMMARY,   # date-keyed
    JOB_NIGHTLY_REVIEW,                                          # date-keyed
)

#: Fire-time for the expiry labeling sweep — after the 15:30 close, before EOD reconcile.
_RECO_EXPIRE_IST = time(15, 45)

#: Catch-up jobs never load-bearing for entries, so they must not run INSIDE boot (an unbounded one would
#: starve every scheduled job, including the self-healing catchup sweep, because the scheduler guard has
#: no recovery awareness and cannot be armed early). They fire as one-shots through the SAME catch-up
#: machinery (same watermarks, same dependency order) immediately AFTER the scheduler is armed.
#: Digest staleness already degrades ``cat`` safely (digest_stale_max_h).
#: Members, in catch-up dependency order: time exit (5) → news chain (20) → digest (25) → planner
#: (28), plus ``results_line_items`` (up to 300 paced XBRL fetches), ``feed_freshness`` (must judge the
#: feeds AFTER catch-up refills them), and ``rec_outcomes`` + ``signal_outcomes`` + ``weekly_summary``.
#: The :data:`LANE_JOB_IDS` are not members: they run in their own runner (:class:`CompactionLane`).
POST_ARM_JOB_IDS: tuple[str, ...] = (
    JOB_TIME_EXIT_CHECK, JOB_NEWS_CHAIN, JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER,
    JOB_RESULTS_LINE_ITEMS, JOB_FEED_FRESHNESS, JOB_REC_OUTCOMES, JOB_SIGNAL_OUTCOMES,
    JOB_WEEKLY_SUMMARY,
)

#: The jobs :class:`CompactionLane` runs, and nothing else does: idle-hours housekeeping that never
#: starts inside the tick_compact no-run window, never runs in boot catch-up and never overlaps another lane job
#: (one single-flight pass runs them in order; their scheduled fires start a pass,
#: :func:`_arm_registry_jobs`). The main runner never holds them.
LANE_JOB_IDS: frozenset[str] = frozenset({JOB_TICK_COMPACT, JOB_MARKET_BACKUP})

#: Early hydration: the pre-open chain an EARLY Kite login (~06:30, before session open) pulls forward
#: from its 08:20–08:50 clock, in registry order. Fired by ``EarlyHydration`` through
#: ``CatchUpRunner.hydrate_ahead`` — the same ``job_runs`` watermarks a scheduled fire writes.
#: Those watermarks do NOT silence the 08:20–08:50 fires (``_scheduled_runner`` stays unconditional):
#: results and filings published 07:00–09:00 IST are the largest catalyst class, so on an awake PC the
#: chain runs AGAIN (fresher digest and plan, universe rebuilt behind the 08:15 instruments refresh). The
#: watermark guards the SLEEP case: a PC waking at 09:30 finds the catch-up sweep satisfied and keeps the
#: 06:30 run. A second pre-open plan message on an awake early-login day is accepted.
#: SLEEP-case residual, accepted: the wake-up sweep re-runs ``instruments`` (SAFETY_CRITICAL), but
#: ``universe_build`` (RUN_LATEST) is already watermarked, so the universe stays on the 06:30 map; the
#: ticker's subscription set is re-derived from the fresh instruments map by the post-login recovery.
#: Deliberately EXCLUDED:
#:   * ``instruments`` — Kite regenerates its dump around 08:00 IST, so a 06:30 refresh could pin a STALE
#:     map for the day. The 08:15 fire or the boot catch-up owns it.
#:   * ``sector_map`` — Sunday-only cadence (never a trading day, so never an early-login morning).
#:   * ``token_check`` — not a registry job at all (no watermark by design; see ``_arm_token_check``).
EARLY_HYDRATION_JOB_IDS: tuple[str, ...] = (
    JOB_SURVEILLANCE, JOB_UNIVERSE, JOB_NEWS_CHAIN, JOB_CATALYST_DIGEST, JOB_PREOPEN_PLANNER,
)

#: ``False`` makes the boot catch-up pass run the whole registry (``deferred`` empty) and the post-arm
#: one-shot a no-op.
DEFER_POST_ARM_JOBS = True

#: The IST window in which ``tick_compact`` must NOT run: no compaction lane pass starts inside it, and a
#: running one stops before its next symbol-day. Compaction mid-session starves tick processing (~16 GB
#: peak). The first lane pass after the window replays whatever was skipped, so the veto only delays
#: compaction. Bounds bracket the session with margin (08:45 ahead of pre-open, 15:45 behind the close).
_IN_SESSION_START_IST = time(8, 45)
_IN_SESSION_END_IST = time(15, 45)

#: Last-resort session open for :func:`_session_open_ist` — NOT a second source of truth: it is the
#: value ``NSECalendar.session`` itself defaults to when a year file declares no ``continuous`` span,
#: used only when the calendar can answer nothing at all (and logged when it happens).
_SESSION_OPEN_FALLBACK = time(9, 15)


def _is_sunday(d: date) -> bool:
    return d.weekday() == 6   # weekly sector-map cadence — fires Sunday, not a trading day


def _regular_session(calendar: NSECalendar) -> Callable[[date], bool]:
    """A ``fire_day``: a trading day that is not a muhurat/special session. It replaces the
    trading-day test in the catch-up, so it must include it."""
    def fires(d: date) -> bool:
        session = calendar.session(d)
        return session is not None and not session.is_muhurat
    return fires


def _last_regular_session_of_week(calendar: NSECalendar) -> Callable[[date], bool]:
    """A ``fire_day``: a regular session whose next regular session is in a later ISO week. A calendar
    horizon with no next session counts as the last."""
    regular = _regular_session(calendar)

    def fires(d: date) -> bool:
        if not regular(d):
            return False
        try:
            return calendar.add_sessions(d, 1).isocalendar()[:2] > d.isocalendar()[:2]
        except ValueError:
            return True
    return fires


def _session_open_ist(calendar: NSECalendar, clock: Clock) -> time:
    """Today's continuous-session open (IST), from the same calendar source ``HealthMonitor._session_open``
    uses, never a second hardcoded 09:15.

    Boot may land on a weekend/holiday, so fall back to the next trading day's open; a calendar that
    answers neither logs and takes the calendar's own default rather than failing the boot (the
    early-hydration hook it feeds is only a pre-open optimisation).
    """
    today = clock.today()
    try:
        day = today if calendar.is_trading_day(today) else calendar.next_trading_day(today)
        session = calendar.session(day)
        if session is not None:
            return session.open.time()
    except ValueError:  # no trading day within the calendar horizon
        pass
    _log.warning("session_open_unresolved", d=today.isoformat(), fallback=_SESSION_OPEN_FALLBACK.isoformat())
    return _SESSION_OPEN_FALLBACK


def build_job_registry(
    settings, fns: Mapping[str, JobRunFn], *, calendar: NSECalendar | None = None
) -> JobRegistry:
    """Populate the ``JobRegistry`` — the single inventory driving BOTH the live
    :class:`~engine.ops.scheduler.Scheduler` and the startup :class:`~engine.ops.jobs.CatchUpRunner`.

    ``fns`` maps each :data:`CORE_JOB_IDS` entry to its async runner; ``calendar`` is required when
    ``fns`` carries ``time_exit_check`` (its ``fire_day``). Job classes/fire-times/dependency
    ``order`` encode the catch-up semantics: safety-critical (run/verify-before-entries), idempotent
    run-latest (single catch-up over the gap), date-keyed (one run per missed trading day). Extracted to
    module scope so the wiring is unit-testable independent of the broker/store side of the graph.
    """
    registry = JobRegistry()
    for spec in (
        # safety/deadline-critical — run/verify before entries open, else FROZEN-for-entries
        JobSpec(JOB_INSTRUMENTS, JobClass.SAFETY_CRITICAL, settings.jobs.instruments_ist, fns[JOB_INSTRUMENTS], order=10),
        JobSpec(JOB_SURVEILLANCE, JobClass.SAFETY_CRITICAL, settings.jobs.surveillance_ist, fns[JOB_SURVEILLANCE], order=20),
        JobSpec(JOB_EARNINGS, JobClass.SAFETY_CRITICAL, settings.jobs.earnings_ist, fns[JOB_EARNINGS], order=30),
        # idempotent run-latest (single catch-up covering the gap; never per-day)
        JobSpec(JOB_UNIVERSE, JobClass.RUN_LATEST, settings.jobs.universe_build_ist, fns[JOB_UNIVERSE], order=10),
        JobSpec(JOB_NEWS_CHAIN, JobClass.RUN_LATEST, _NEWS_CHAIN_IST, fns[JOB_NEWS_CHAIN], order=20),
        JobSpec(JOB_CORP_ACTIONS, JobClass.RUN_LATEST, settings.jobs.corp_actions_ist, fns[JOB_CORP_ACTIONS], order=30),
        JobSpec(JOB_SECTOR_MAP, JobClass.RUN_LATEST, settings.jobs.universe_build_ist, fns[JOB_SECTOR_MAP],
                order=40, fire_day=_is_sunday),
        # Filings SHP + pledge: run-latest (per-symbol BSE detail only for new submissions), after the
        # EOD data jobs; never entry-blocking (filings are features/risk-context only in stage 1).
        JobSpec(JOB_FILINGS_SHP, JobClass.RUN_LATEST, settings.jobs.filings_shp_ist, fns[JOB_FILINGS_SHP], order=80),
        JobSpec(JOB_ISIN_MAP, JobClass.RUN_LATEST, settings.jobs.isin_map_ist, fns[JOB_ISIN_MAP], order=78),
        JobSpec(JOB_RESULTS_LINE_ITEMS, JobClass.RUN_LATEST, settings.jobs.results_line_items_ist,
                fns[JOB_RESULTS_LINE_ITEMS], order=85),
        JobSpec(JOB_FEED_FRESHNESS, JobClass.RUN_LATEST, settings.jobs.feed_freshness_ist,
                fns[JOB_FEED_FRESHNESS], order=95),
        JobSpec(JOB_BACKUP, JobClass.RUN_LATEST, settings.jobs.backup_ist, fns[JOB_BACKUP], order=90),
        JobSpec(JOB_CLAUDE_LOGIN, JobClass.RUN_LATEST, _CLAUDE_LOGIN_CHECK_IST, fns[JOB_CLAUDE_LOGIN],
                order=97),
        # date-keyed backfill — one run per missed trading day, ascending
        JobSpec(JOB_RECONCILE, JobClass.DATE_KEYED, settings.jobs.reconcile_ist, fns[JOB_RECONCILE], order=10),
        JobSpec(JOB_BHAVCOPY, JobClass.DATE_KEYED, settings.jobs.bhavcopy_ist, fns[JOB_BHAVCOPY], order=20),
        JobSpec(JOB_DAILY_BARS, JobClass.DATE_KEYED, _DAILY_BARS_IST, fns[JOB_DAILY_BARS], order=30),
        JobSpec(JOB_DEALS, JobClass.DATE_KEYED, settings.jobs.deals_ist, fns[JOB_DEALS], order=40),
        JobSpec(JOB_FEATURES, JobClass.DATE_KEYED, _FEATURES_IST, fns[JOB_FEATURES], order=50),
        # Filings: insider trades (PIT) + results/board-meeting dates — one run per missed day.
        JobSpec(JOB_FILINGS_PIT, JobClass.DATE_KEYED, settings.jobs.filings_pit_ist, fns[JOB_FILINGS_PIT], order=60),
        JobSpec(JOB_FILINGS_PIT_FRESH, JobClass.DATE_KEYED, settings.jobs.filings_pit_fresh_ist, fns[JOB_FILINGS_PIT_FRESH], order=65),
        JobSpec(JOB_FILINGS_RESULTS, JobClass.DATE_KEYED, settings.jobs.filings_results_ist, fns[JOB_FILINGS_RESULTS], order=70),
        # `ins`: EOD insider net-buy crossing detection -> ins_pending. order=66 puts it directly
        # after filings_pit_fresh (65) and before filings_results (70), so a catch-up replay of a
        # missed day ingests the day's BSE fresh rows BEFORE computing crossings over them — the same
        # dependency the 19:00/19:15 fire-times encode for the live path.
        JobSpec(JOB_INS_CROSSINGS, JobClass.DATE_KEYED, settings.jobs.ins_crossings_ist, fns[JOB_INS_CROSSINGS], order=66),
        # Storage housekeeping: collapse each closed symbol-day's ~7.5 K tick fragments
        # into one file. LAST in the date-keyed order — it reads no engine state and blocks nothing.
        JobSpec(JOB_TICK_COMPACT, JobClass.DATE_KEYED, _TICK_COMPACT_IST, fns[JOB_TICK_COMPACT], order=90),
        # DuckDB leg: one export covers the whole gap. A lane job (LANE_JOB_IDS), so RUN_LATEST
        # ahead of DATE_KEYED puts it before compaction in a lane pass.
        JobSpec(JOB_MARKET_BACKUP, JobClass.RUN_LATEST, _MARKET_BACKUP_IST, fns[JOB_MARKET_BACKUP], order=95),
    ):
        registry.register(spec)
    # Decision-plane jobs: a None fn is skipped here (tests); the composition root always passes a
    # wrapper. Catch-up order within RUN_LATEST: universe(10) → news_chain(20) → digest(25) → planner(28)
    # — the digest needs scored clusters + today's universe; the planner needs the digest.
    if calendar is None and any(fns.get(j) is not None for j in (JOB_TIME_EXIT_CHECK, JOB_WEEKLY_SUMMARY)):
        raise ValueError("time_exit_check and weekly_summary need the calendar for their fire_day")
    for spec in (
        JobSpec(JOB_TIME_EXIT_CHECK, JobClass.RUN_LATEST, settings.jobs.time_exit_check_ist,
                fns.get(JOB_TIME_EXIT_CHECK), order=5,
                fire_day=_regular_session(calendar) if calendar is not None else None),
        # The digest reads the PRIOR session's deals flags. RUN_LATEST runs before DATE_KEYED in one pass;
        # the digest is a DEFERRED job, so the boot pass replays deals first. Un-deferring it would
        # reopen a stale-flags read on multi-day-gap boots.
        JobSpec(JOB_CATALYST_DIGEST, JobClass.RUN_LATEST, settings.jobs.catalyst_digest_ist,
                fns.get(JOB_CATALYST_DIGEST), order=25),
        JobSpec(JOB_PREOPEN_PLANNER, JobClass.RUN_LATEST, settings.jobs.preopen_planner_ist,
                fns.get(JOB_PREOPEN_PLANNER), order=28),
        JobSpec(JOB_RECO_EXPIRE, JobClass.RUN_LATEST, _RECO_EXPIRE_IST,
                fns.get(JOB_RECO_EXPIRE), order=60),
        # After bhavcopy (20) and daily_bars (30): it scores on their bars.
        JobSpec(JOB_REC_OUTCOMES, JobClass.DATE_KEYED, settings.jobs.rec_outcomes_ist,
                fns.get(JOB_REC_OUTCOMES), order=35),
        # After rec_outcomes: it reads the EW returns that job writes.
        JobSpec(JOB_SIGNAL_OUTCOMES, JobClass.DATE_KEYED, settings.jobs.signal_outcomes_ist,
                fns.get(JOB_SIGNAL_OUTCOMES), order=36),
        JobSpec(JOB_WEEKLY_SUMMARY, JobClass.DATE_KEYED, settings.jobs.weekly_summary_ist,
                fns.get(JOB_WEEKLY_SUMMARY), order=37,
                fire_day=_last_regular_session_of_week(calendar) if calendar is not None else None),
        JobSpec(JOB_NIGHTLY_REVIEW, JobClass.DATE_KEYED, settings.jobs.nightly_review_ist,
                fns.get(JOB_NIGHTLY_REVIEW), order=80),
    ):
        if spec.run is not None:
            registry.register(spec)
    return registry


def make_order_guard(
    mode: ModeManager, kill: KillSwitch, calendar: NSECalendar, clock: Clock
) -> Callable[[str], None]:
    """Order-surface predicate: a position-OPENING broker call requires
    AUTO ∧ NORMAL ∧ inside the owner trade window; risk-reducing intent is NEVER gated."""

    def order_guard(intent: str) -> None:
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
                f"risk_state={mode.risk_state().value} in_window={in_window}"
            )

    return order_guard


async def run() -> int:
    settings = load_settings()
    configure_logging(level="INFO", logs_dir=settings.logs_dir())
    _log.info("engine_boot", env=settings.env, tz=settings.timezone)

    # --- PRIMARY single-instance guard: an exclusive OS file lock acquired BEFORE any shared resource is
    #     touched (sqlite connect / MarketStore.open / Telegram poll / :8400 bind). The DB-row guard is
    #     blind to a boot wedged before lifecycle.startup commits RUNNING and is TOCTOU-racy; the kernel
    #     lock is real mutual exclusion, released on ANY process death. The DB check stays as the
    #     secondary crash-detection / pid-alive guard. Exit code 3 is deliberate: non-zero and distinct
    #     from the watchdog's 2. ---
    lock_path = settings.resolved_data_dir() / "engine.lock"
    instance_lock = InstanceLock(lock_path)
    if not instance_lock.acquire():
        _log.critical(
            "single_instance_refused_lock", lock=str(lock_path), holder_pid=instance_lock.holder_pid(),
            hint="another engine instance owns the single-writer stores; this one exits",
        )
        return 3

    # --- graceful-stop wiring, installed EARLY (right after the lock, before ANY shared resource opens) so
    #     the ENTIRE boot is interruptible: a first Ctrl-C requests a graceful stop (honoured at
    #     await stop_event.wait()), a second forces a hard exit even if the boot is wedged. ---
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    # --- BOOT CONTRACT WATCHDOG, armed HERE: the process is committed (lock held, signals wired) and still
    #     ahead of every resource that can wedge (sqlite, MarketStore.open, Telegram, :8400 bind, token
    #     probe, lifecycle.startup, warm-up seeding). A boot that never reaches scheduler.start() fires no
    #     trigger all day, and the health pulse is itself an APScheduler job. A PLAIN asyncio task on
    #     purpose: it must stay alive precisely when the scheduler is broken. `boot_state` is late-bound —
    #     read at check time, so it can be armed before the alert sink / scheduler / engine_ready exist. ---
    boot_state: dict[str, Any] = {
        "engine_ready": False, "scheduler": None, "alert": None, "startup_done_at": None,
    }
    boot_watchdog = asyncio.create_task(boot_contract_watchdog(boot_state), name="boot_contract")

    engine = await compose(
        settings, secrets=Secrets(), clock=Clock(ntp_servers=settings.clock.ntp_servers),
        stop_event=stop_event, boot_state=boot_state,
    )
    tasks = await _boot(engine, boot_state)
    await stop_event.wait()
    await _shutdown(engine, tasks, boot_watchdog=boot_watchdog)
    instance_lock.release()   # cosmetic tidiness — every non-clean exit is covered by the kernel
    # releasing the OS lock on process death; this just frees it promptly on a graceful stop.
    _log.info("engine_stopped")
    return 0


@dataclass
class Engine:
    """The composed object graph: what :func:`_boot` and :func:`_shutdown` drive, plus test seams."""

    settings: Settings
    conn: sqlite3.Connection
    clock: Clock
    calendar: NSECalendar
    bus: EventBus
    mode: ModeManager
    session: SessionManager
    kite: KiteClient | None
    telegram: Any
    app: Any
    alert: Callable[[str, str], Awaitable[None]]
    notify: Callable[[CatalogMessage], Awaitable[None]]
    instruments: InstrumentStore
    store: MarketStore
    http: httpx.AsyncClient
    bar_builder: BarBuilder
    ticker: TickerSupervisor
    paper: PaperStack | None
    warmup_gate: WarmupGate
    warmup_holder: dict[str, Any]
    skew_holder: dict[str, bool]
    warmup_refresh: Callable[[], Awaitable[None]]
    health: HealthMonitor
    lifecycle: SessionLifecycle
    scheduler: Scheduler
    scheduler_armed: asyncio.Event
    catch_up: CatchUpRunner
    compaction_lane: CompactionLane
    early_hydration: EarlyHydration
    freeze_lift_tasks: set[asyncio.Task]
    registry: JobRegistry
    scan_sweep: Callable[..., Awaitable[str]]


async def compose(
    settings: Settings, *, secrets: Secrets, clock: Clock, stop_event: asyncio.Event,
    boot_state: dict[str, Any],
) -> Engine:
    # --- persistence + migrations ---
    conn = connect(settings.sqlite_path())
    applied = apply_migrations(conn)
    if applied:
        _log.info("migrations_applied_on_boot", files=applied)

    # --- deterministic core ---
    window_seed = TradeWindow(
        start=settings.trade_window.start_ist,
        end=settings.trade_window.end_ist,
        squareoff_buffer_min=settings.trade_window.squareoff_buffer_min,
    )
    # include_special=False: special sessions (muhurat) are closed days. BarBuilder/ReconcileJob take no
    # per-day session bounds, so a settings opt-in would be half-wired; there is deliberately no such key.
    calendar = NSECalendar(
        config_dir() / "calendar", clock,
        strict=(settings.env == "prod"), sqlite_conn=conn, window_seed=window_seed,
        include_special=False,
    )
    bus = EventBus()
    protected_store = ProtectedStore(config_dir(), conn, clock)
    mode = ModeManager(conn, clock, bus, calendar, paper_only=settings.feature_flags.paper_only)
    latch = RiskStateLatch(conn, clock, mode)
    limits_engine = LimitsEngine(protected_store)
    governor = BudgetGovernor.from_config(conn, clock, calendar, bus)

    # --- live tick cache: last (ltp, exchange_ts) per symbol. Feeds the ExposureTracker mark
    #     source and the gate's ltp/tick-age seams (stale_data_guard) — a symbol never seen
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

    # Capital base is read live from the protected limit table (a `LimitsEngine.reload()` reaches the
    # real brakes with no setter); a tampered store must not crash the boot (the self-test FREEZEs it
    # separately) — `_limits_capital_base` falls back to the shipped base.
    try:
        limits_engine.load()
    except IntegrityError:
        _log.warning("limits_unverified_at_boot",
                     hint="scripts/protected_config_status.py (a reverted dashboard change: --restore "
                          "limits.yaml); reseed only after an owner-reviewed edit; using the shipped "
                          "fallback capital base")
    exposure = ExposureTracker(conn, clock, _limits_capital_base(limits_engine), mark_price=mark_price)

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
        """The single ``CatalogMessage`` sink handed to every data job and the lifecycle: Telegram if
        configured, always mirrored to the structured log. Best-effort — a failed send never propagates."""
        bot = telegram_holder["bot"]
        if bot is not None:
            try:
                await bot.send(msg)
            except Exception:  # noqa: BLE001 - notification must never crash the engine
                _log.exception("telegram_notify_failed", kind=str(msg.kind))
        _log.info("notify", kind=str(msg.kind), severity=msg.severity, title=msg.title)

    async def kill_alert(message: str) -> None:
        await alert("critical", message)

    # Watchdog owner channel: `alert` reads `telegram_holder` at CALL time, so it uses whatever channel is
    # live then and falls back to the structured CRITICAL log.
    boot_state["alert"] = alert

    kill = KillSwitch(conn, clock, bus, alert_callback=kill_alert)
    session = SessionManager(secrets, clock, redirect_path=settings.broker.kite_login_redirect_path)

    # --- kill ⇄ cause-latch bridge: the sticky kill_state is the KillSwitch's own store;
    #     mirroring it into the risk_state_causes ledger keeps mode_state.risk_state showing KILLED
    #     (and re-arming to the next-most-restrictive cause on owner reset, never straight to NORMAL).
    from engine.risk.events import TOPIC_KILL_STATE

    async def _kill_to_latch(evt: Any) -> None:
        if evt.killed:
            await latch.set_cause("kill", RiskState.KILLED, evt.reason or "kill", evt.actor)
        else:
            await latch.clear_cause("kill", evt.actor)

    bus.subscribe(TOPIC_KILL_STATE, _kill_to_latch)

    # --- Telegram (optional). reco_book is wired after the pipeline exists
    #     (second pass); every other owner-command dependency is live from boot. ---
    telegram = _build_telegram(
        settings, secrets, clock, mode, kill,
        latch=latch, governor=governor, limits_engine=limits_engine, exposure=exposure,
        session=session, conn=conn, bus=bus,
    )
    telegram_holder["bot"] = telegram
    paper_control = PaperControl(conn, clock)
    if telegram is not None:
        telegram.set_paper(paper_control, autopilot_built=lambda: settings.paper.subsystem_enabled)
    # After _build_telegram subscribed to the bus, so the MODE_CHANGE alert is journalled.
    await mode.enforce_paper_only()

    # --- Tier-1 harness: the ONLY SDK call site. Defs load PER-AGENT: an invalid def is quarantined +
    #     surfaced by the self-test, the rest stay live.
    agent_defs: dict[str, Any] = {}
    roster_quarantined: dict[str, str] = {}
    harness: AgentHarness | None = None
    try:
        agents_cfg = load_yaml(config_dir() / "agents.yaml")
        roster = load_agent_roster(agents_cfg)
        agent_defs = roster.defs
        roster_quarantined = roster.quarantined
        if agent_defs:
            cli_path = resolve_cli_path(agents_cfg, repo_root())
            if cli_path is not None and not os.path.isfile(cli_path):
                _log.error("agent_cli_missing", cli_path=cli_path, hint="every SDK call will fail closed")
            else:
                _log.info("agent_cli", cli_path=cli_path or "sdk-bundled")
            harness = AgentHarness(agent_defs, governor, clock, conn, alert=alert, cli_path=cli_path)
        else:
            _log.error("agent_roster_empty", hint="config/agents.yaml — LLM tier disabled this run")
    except Exception:  # noqa: BLE001 - a bad roster must not stop the deterministic engine
        _log.exception("agent_roster_unloadable", hint="config/agents.yaml — LLM tier disabled this run")
    # The login warnings print the same concrete command as the harness's auth_failed alert.
    relogin_hint = harness.relogin_hint() if harness is not None else CLAUDE_RELOGIN_HINT

    # =========================================================================================
    # DATA PLANE. Single-writer store + tick→bar builder + daily jobs.
    # =========================================================================================
    store = MarketStore.from_settings(settings, clock).open()
    instruments = InstrumentStore(clock)

    def tick_size_for(symbol: str) -> Decimal:
        """Per-symbol banded tick for the reconcile close-drift check; NSE minimum if unknown."""
        try:
            return instruments.by_symbol(symbol).tick_size
        except UnknownInstrument:
            return _DEFAULT_TICK

    # BarBuilder: ticks (published by TickerSupervisor on "tick") → finalized 1m bars → bar.1m +
    # single-writer batch persist. It also buffers raw ticks into the Parquet dataset (do NOT
    # buffer a second time elsewhere). advance() runs on a coarse timer; flush_all() at EOD/shutdown.
    # notify: the processing-lag watchdog pages the owner ONCE per lag episode.
    bar_builder = BarBuilder(store, clock, bus, notify=notify)
    bus.subscribe("tick", bar_builder.on_tick_event)
    # feed.health: a WARMING/HEALTHY transition suspends the lag watchdog for 30 s so Kite's connect-time
    # snapshot (one tick per subscribed instrument) cannot flap the lag episode. Subscribed HERE, before
    # the ticker is spawned, so the boot's own transitions are never missed.
    bus.subscribe(FEED_HEALTH_TOPIC, bar_builder.on_feed_health)

    # Shared injected httpx client for every best-effort feed. Owned here.
    # Default browser headers + split timeout: the bare python-httpx UA is tarpitted/blocked by
    # NSE hosts; the per-feed fetches funnel through engine.core.nse_http.nse_get for cookie priming +
    # bounded retry on top of this client's shared (primed) cookie jar.
    http = httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": BROWSER_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
        timeout=httpx.Timeout(30.0, connect=10.0),
    )

    # Broker REST facade — only when credentials exist (fresh install stays runnable + FROZEN).
    kite: KiteClient | None = None
    if secrets.has(KITE_API_KEY):
        kc = session.kite_connect()
        if kc is not None:
            # Token-rejection circuit breaker: the FIRST TokenException on any broker call fires
            # SessionManager.on_token_rejected → the invalidation hook (freeze + alert).
            kite = KiteClient(kc, RateLimiter(clock), clock,
                              on_token_rejected=session.on_token_rejected,
                              order_guard=make_order_guard(mode, kill, calendar, clock))
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

    # --- universe: ingests refreshed 08:15/08:20, build 08:30 ---
    data_dir = settings.resolved_data_dir()
    leverage = MisLeverageIngest(clock, http, data_dir / "universe" / "mis_margins.json", notify=notify)
    surveillance = SurveillanceIngest(
        clock, http, data_dir / "universe" / "surveillance.json", notify=notify,
        held_positions=lambda: _held_positions(conn),
    )
    universe_builder = UniverseBuilder(
        settings, store, instruments, leverage, surveillance, clock, http, notify=notify
    )

    # --- EOD datafeeds ---
    bhavcopy = BhavcopyJob(store, clock, http, notify=notify)
    corp_actions = CorpActionsJob(store, clock, http, notify=notify)
    earnings = EarningsCalendarJob(store, clock, http, notify=notify)
    deals = DealsJob(store, clock, http, notify=notify)
    # Corporate-filings feeds (data-only in stage 1; never entry-blocking). filings_results
    # reuses the earnings provider's historical leg (run_range) for board-meeting dates.
    filings_pit = FilingsPitJob(store, clock, http, settings=settings, notify=notify)
    # settings: filings_pit_fresh rebuilds its scrip->symbol map every run from the cached
    # index-constituents CSV (symbol->ISIN) + the BSE bulk master (ISIN->scrip); without it the map is
    # only what symbol_isin holds, which misses most eligible issuers.
    filings_pit_fresh = FilingsPitFreshJob(store, clock, http, settings=settings, notify=notify)
    filings_results = FilingsResultsJob(store, clock, http, earnings=earnings, notify=notify)
    results_line_items = ResultsLineItemsJob(store, clock, http)
    isin_map = IsinMapJob(settings, store, clock, http, notify=notify)
    feed_freshness = FeedFreshnessJob(store, clock, calendar, notify=notify)
    filings_shp = FilingsShpJob(store, clock, http, notify=notify)
    # `ins`: EOD insider net-buy crossing detection over the SAME validated crossing function the study
    # runs (engine.datafeeds.insider_crossings). No HTTP — it reads the rows filings_pit_fresh ingested
    # and journals crossings into ins_pending for the next session's sweep. threshold_inr is owner-fixed
    # (it defines the validated event population).
    ins_crossings = InsCrossingsJob(
        store, conn, clock, calendar, threshold_inr=settings.ins.threshold_inr
    )
    # industry_paths: the third classification rung — NSE's Industry column for NIFTY 500 names the
    # sectoral indices and config/sector_overrides.yaml leave unclaimed. Runtime cache first, committed
    # seed as fallback (index_seed_path is repo-root-relative; `/` on an absolute value keeps it).
    sector_map = SectorMapJob(
        store, clock, http, data_dir / "datafeeds" / "sector_lists.json", notify=notify,
        industry_paths=(
            data_dir / "universe" / "index_cached.csv",
            repo_root() / settings.universe.index_seed_path,
        ),
    )

    # --- news pipeline data side ---
    # envelope_state is read at boot only (intra-run promotions are not built). load_cat_params ignores
    # every non-`cat` key; empty ⇒ None ⇒ the envelope.yaml/settings.yaml defaults. The clusterer and
    # CatalystDigestJob read the ONE row cat.max_event_age_days off this dict — the clusterer counts
    # CALENDAR days from the headline, the digest trading sessions.
    _envelope_state = {
        r["parameter"]: r["value"]
        for r in conn.execute("SELECT parameter, value FROM envelope_state").fetchall()
    }
    _cat_params = load_cat_params(_envelope_state or None)
    news_ingest = NewsIngest(settings.news, store, clock, http)
    clusterer = HeadlineClusterer(
        store, sim_threshold=settings.news.cluster_sim_threshold,
        max_event_age_days=int(_cat_params["max_event_age_days"]),
    )
    resolver = EntityResolver(store, clock)

    # --- feature engine v2 ---
    features = FeatureEngine(store, clock, calendar, index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)

    # =========================================================================================
    # DECISION PLANE: cost model → gate → pipeline → agent jobs.
    # Inert without the Tier-1 harness (LLM tier disabled ⇒ scanners and gate still run, but no proposals
    # are minted — fails to no-recommendation).
    # =========================================================================================
    # `edge_multiple_min` is the ONE learner-movable gate knob: envelope_state when a promotion has
    # written one, else the owner's limits.yaml default — never the CostModel's Python default.
    # Read at boot only.
    def _edge_multiple_min() -> Decimal:
        row = conn.execute(
            "SELECT value FROM envelope_state WHERE parameter='edge_multiple_min'"
        ).fetchone()
        if row is not None and row["value"]:
            return Decimal(str(row["value"]))
        try:
            return Decimal(str(limits_engine.load().limits.min_viable_size.edge_multiple_min_default))
        except IntegrityError:
            return Decimal("2.0")   # envelope default; the store failure is already FROZEN elsewhere

    cost_model = CostModel.from_config(instruments=instruments, edge_multiple_min=_edge_multiple_min())
    # Per-strategy cost-gate edge registrations — see :func:`_strategy_expected_edge_pct` (module-level so
    # it is assertable without booting an engine). ONE live object for the gate and the pipeline: hi52
    # reads min(registered, derived at the CURRENT verified limits) on the gate's own cost model, so the
    # two never disagree after a capital change.
    strategy_edges = StrategyEdges(
        _strategy_expected_edge_pct(settings), table_fn=limits_engine.load, cost_model=cost_model,
    )
    gate = RiskGate(
        limits_engine, cost_model, clock,
        strategy_expected_edge_pct=strategy_edges,
        # News SHADOWS: the cost gate rejects these unconditionally, before target_price is read, so
        # "signals are a measurement, never a recommendation" is ENFORCED at Tier 2 (see risk/gate.py
        # _SHADOW_NO_EDGE, and NO_EDGE_SHADOW_STRATEGIES below for the membership).
        no_edge_shadow_strategies=NO_EDGE_SHADOW_STRATEGIES,
    )

    # Warm-up status cache for the gate context: WarmupGate.status() is async + store-heavy, so the
    # gate reads a snapshot refreshed by the equity/health cadence. Unset ⇒ a NOT-READY status with a
    # regime blocker — both readiness rules fail CLOSED until the first refresh. The "warmup:" line
    # classifies as CLASS_UNKNOWN by design: an unattributable blocker holds EVERY coverage class down.
    warmup_holder: dict[str, Any] = {"status": None}
    _WARMUP_UNREFRESHED = WarmupStatus(
        ready=False, blockers=["warmup:unrefreshed 0/0", "regime:unrefreshed 0/0"]
    )

    def warmup_status_snapshot() -> WarmupStatus:
        return warmup_holder["status"] or _WARMUP_UNREFRESHED

    # Clock-skew verdict is boot-scoped (the self-test measures it; the health loop deliberately
    # skips per-minute NTP). A mid-day drift is caught at the next startup — accepted in RECOMMEND mode.
    skew_holder: dict[str, bool] = {"ok": False}

    ctx_builder = GateContextBuilder(
        limits_engine, exposure, instruments, store, calendar, clock, mode, kill,
        ltp_fn=mark_price, tick_age_fn=tick_age_s,
        warmup_status_fn=warmup_status_snapshot,
        clock_skew_ok_fn=lambda: skew_holder["ok"],
        degrade_tier_fn=lambda: governor.degrade_tier().value,
        conn=conn, index_symbol=INDEX_SYMBOL,
        # A position the broker has shown EMPTY on two consecutive sessions is excluded from the
        # position/sector counts (same journal as the position-event screen; require_zero: a partial
        # exit still holds its slot).
        missing_holdings_fn=lambda d: positions_missing_from_holdings(conn, d, require_zero=True),
        # nifty50_fn/expiry_day_fn unwired in RECOMMEND mode: the expiry-day NIFTY50-MIS leg of
        # `no_trade_windows` is inert until index membership is wired.
    )

    # Late-bound: ``catch_up`` is bound further down, before any job or candidate can run. Success
    # only, never ``was_run``: a terminal give-up (``skipped``) is no digest.
    assembler = ContextAssembler(
        store, conn, clock, calendar,
        digest_ran_fn=lambda d: (catch_up.last_success_date(JOB_CATALYST_DIGEST) or date.min) >= d,
    )
    # The /taken drift check judges a swing/position fill against the SAME overnight_gap_mult the gate
    # sized it on, read from the hash-verified limits at call time.
    book = RecommendationBook(
        conn, clock, cost_model,
        overnight_gap_mult_fn=lambda: limits_engine.load().limits.per_trade_risk.overnight_gap_mult,
        calendar=calendar, veto_window_sessions=settings.recommend.veto_window_sessions,
    )
    hold_fn = build_hold_fn(settings, limits_engine)

    # --- paper autopilot: nothing paper exists unless the subsystem is enabled. Built
    #     before the pipeline that takes its seams, the scheduler and the ticker, so the bridge sees the
    #     first tick. A failed build leaves paper off and the real engine untouched. ---
    paper: PaperStack | None = None
    if settings.paper.subsystem_enabled:
        paper = await _compose_paper(
            conn, clock, calendar, bus, settings, paper_control,
            real_risk_state=mode.risk_state, limits=limits_engine, tick_size_fn=tick_size_for,
            round_tick_fn=_round_tick_fn(instruments), cost_model=cost_model, hold_fn=hold_fn,
            store=store, backfill=backfill,
            ctx_builder_fn=lambda tracker: GateContextBuilder(
                limits_engine, tracker, instruments, store, calendar, clock, mode, kill,
                ltp_fn=mark_price, tick_age_fn=tick_age_s,
                warmup_status_fn=warmup_status_snapshot,
                clock_skew_ok_fn=lambda: skew_holder["ok"],
                degrade_tier_fn=lambda: governor.degrade_tier().value,
                conn=conn, index_symbol=INDEX_SYMBOL, scope="paper",
            ),
            notify=notify,
        )
        if telegram is not None:
            built = paper is not None
            telegram.set_paper(
                paper_control, autopilot_built=lambda: built,
                status_fn=paper.status if paper is not None else None,
                counters_fn=paper.counters if paper is not None else None,
            )

    pipeline = (
        RecommendationPipeline(
            assembler, harness, agent_defs, gate, ctx_builder, book, mode, kill,
            governor, exposure, limits_engine, notify, clock, calendar, conn, store,
            # `prescreen` is constructed further down; these lambdas late-bind it at call time.
            rearm=lambda sym, sid: prescreen.rearm(sym, sid),
            # The drain tick also flushes the RAW counters to `funnel_raw_counts`. Paired with
            # `raw_counts_loader` below — the flush writes ABSOLUTE totals, so it must only run against
            # a hydrated counter.
            funnel_raw=lambda d: prescreen.raw_counts(d),
            # Cap displacement, the two halves of the prescreen admission seam. `claim_slot` is the
            # compare-and-set the pipeline must pass before spending an analyst call — it makes an
            # EVALUATED candidate's slot permanent; `take_displaced` pulls the pre-screen's evictions
            # onto the event loop, the only place the forward queue may be touched.
            claim_slot=lambda sym, sid: prescreen.claim_slot(sym, sid),
            take_displaced=lambda: prescreen.take_displaced(),
            # A no_action verdict makes its pair displaceable again.
            decline=lambda sym, sid: prescreen.decline(sym, sid),
            # brk20 entry-band screen; the MARKET anchor of the stop re-anchor.
            ltp_fn=mark_price,
            # The SAME warm-up snapshot the gate context reads, so a candidate the gate would refuse for
            # coverage never spends an analyst call.
            warmup_status_fn=warmup_status_snapshot,
            admission_mode=settings.strategy.prescreen.admission_mode,   # rollback flag
            # rollback flag: `immediate` restores the inline drain (see forward_drain_tick).
            forward_drain_mode=settings.strategy.prescreen.forward_drain_mode,
            hold_fn=hold_fn,
            round_tick_fn=_round_tick_fn(instruments),
            recommend=settings.recommend,
            strategy_edge_pct=strategy_edges,   # the gate's own object, never a copy
            reports_dir=data_dir / "reports",
            # None (subsystem disabled or failed to build) ⇒ no paper verdict, no paper order.
            paper_ctx_builder=paper.ctx_builder if paper is not None else None,
            paper_submit=paper.submit if paper is not None else None,
            paper_enabled_fn=paper.entries_open if paper is not None else None,
        )
        if harness is not None else None
    )
    if pipeline is not None:
        bus.subscribe("signal.candidate", pipeline.on_signal_candidate)
        bus.subscribe("bar.1m", pipeline.on_bar)
    if telegram is not None:
        telegram.set_reco_book(book)

    # --- live pre-screen: scanners → signal.candidate. Runs whenever the engine is up. Origination is
    #     window-gated here on bar time, so a candidate the pipeline would drop never charges an
    #     unrefundable day slot; the pipeline's own Clock-based check stays the authoritative gate. ---
    scan_provider = LiveScanContextProvider(
        store, clock, calendar, features, index_symbol=INDEX_SYMBOL,
        # Late-bound: watchlist_symbols is defined further down; resolved at day-cache build time.
        momentum_universe=lambda: watchlist_symbols(),
        # Persists the mom rebalance-day marker (mom_rebalance_state) so mom fires every rebalance_days
        # sessions, not every day.
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
        admission_mode=settings.strategy.prescreen.admission_mode,       # rollback flag
        # Cumulative cap tranches by bar time, so the window-open burst cannot spend a whole sub-cap in
        # its first minute (see the prescreen module docstring).
        cap_release_schedule=settings.strategy.prescreen.cap_release_schedule,
        # News carve-out: the per-day catalyst-entry cap, read at the ENFORCEMENT site from the
        # hash-verified limits.yaml — never a constructor number, never in the gate. A raise here
        # (unverifiable store) refuses cat candidates; the pre-screen handles it.
        catalyst_cap_fn=lambda: limits_engine.catalyst_guard().max_catalyst_entries_day,
        # Loaded on the first day roll of this process, flushed back by the drain tick above — the two
        # halves ship together or the flush would overwrite the day's persisted total with a smaller one.
        raw_counts_loader=lambda d: read_funnel_raw_counts(conn, d),
        # How much better a later candidate must score to take a full cap's slot from an unevaluated
        # incumbent. Owner knob (reasoning lives with the value in settings.yaml); `null` disables
        # displacement.
        displacement_margin=settings.strategy.prescreen.displacement_margin,
    )
    # Dedupe/caps day-state is process memory — rehydrate it from the day-slot journal so a restart
    # does not reset the daily bound or re-send already-evaluated candidates.
    _hydrate_prescreen(conn, prescreen, clock.today())
    bus.subscribe("bar.1m", prescreen.handle_bar)

    # --- `brk20` RETEST re-arm: a limit resting at the broken level for up to five sessions, where the
    #     live rule would publish that level once. This book is the durable half (migration 0014, so a
    #     reboot cannot turn it back into a single-session mechanism); `forward_drain_tick` below is the
    #     minute pulse that re-offers a rested level when the price is back inside the band. It widens
    #     nothing: same levels, band, caps, gate (see retest.py). ---
    retest_book = RestingLevelBook(conn, calendar, clock,
                                   sessions=settings.brk20.retest_sessions)
    #: What the DAY's sweep knows that the retest tick must respect: the eligible population brk20
    #: originates over and the ex-date horizon, both already read by `_collect_and_scan`. `day` doubles
    #: as the retest's readiness flag — see `sweep_ready` in `forward_drain_tick`.
    _retest_state: dict[str, Any] = {
        "day": None, "eligible": frozenset(), "ex_skip": frozenset(), "unadjusted": frozenset(),
    }

    def _retest_limits() -> tuple[float, float]:
        """The CNC ``entry_sanity_band`` and ``stale_data_guard.max_tick_age_s``, read TOGETHER at the
        ENFORCEMENT site from ONE hash-verified load (the ``catalyst_cap_fn`` convention): never
        constructor numbers, never two loads that could straddle an owner edit. A raise (unverifiable
        store) propagates; `_retest_republish` turns it into "re-offer nothing this tick"."""
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

    # The scan context day-caches `trade_window`; subscribing to `trade_window.changed` keeps `orb` from
    # intersecting a window the owner has since changed.
    from engine.risk.events import TOPIC_TRADE_WINDOW

    async def _refresh_scan_window(_event) -> None:
        scan_provider.invalidate_trade_window()

    bus.subscribe(TOPIC_TRADE_WINDOW, _refresh_scan_window)

    # --- Tier-1 jobs (all fail to no-output, never blocking) ---
    # `_envelope_state` is the boot read shared with the clusterer (news pipeline block above).
    digest_job =CatalystDigestJob(store, clock, calendar, protected_store, envelope=_envelope_state or None)
    catalyst_disabled_alert = _CatalystDisabledAlert(notify, clock)
    scoring_job = (
        NewsScoringJob(store, resolver, assembler, harness, agent_defs, governor, clock, calendar)
        if harness is not None and "news_analyst" in agent_defs else None
    )
    planner_job = (
        PreopenPlannerJob(store, conn, assembler, harness, agent_defs, governor, clock, calendar,
                          notify=alert,
                          # Late-bound like ``digest_ran_fn`` above.
                          last_success=lambda job_id: catch_up.last_success_date(job_id))
        if harness is not None and "preopen_planner" in agent_defs else None
    )
    nightly_job = (
        NightlyReviewJob(protected_store, conn, assembler, harness, agent_defs, governor, clock,
                         calendar, notify=notify,
                         # Raw scanner output is the one funnel number that is not journaled
                         # (it is not a decision) — read it from the live pre-screen counters.
                         funnel_raw=lambda d: prescreen.raw_counts(d))
        if harness is not None and "nightly_reviewer" in agent_defs else None
    )

    # --- ticker subprocess supervisor (started into WARMING at the startup ticker-resume step when a token exists) ---
    # calendar+notify power the in-session tick-silence guard (a tickless-but-heartbeating child degrades
    # visibly and alerts during market hours).
    ticker = TickerSupervisor(
        settings, clock, bus,
        symbol_for_token=instruments.symbol_for_token, calendar=calendar, notify=notify,
        # The WS URL needs the api_key alongside the access token; without it every upgrade is 400-rejected.
        api_key=session.api_key() or "",
    )
    # stale_data_guard: a heartbeat-silent feed latches FROZEN until the feed is HEALTHY.
    bus.subscribe(FEED_HEALTH_TOPIC, feed_health_to_latch(latch, ticker.in_market_hours))

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

        Same bounded look-back as :func:`watchlist_symbols`; the only consumer is the SUNDAY sector-map
        job, and Sunday has no universe row of its own. Falls back to the active watchlist when nothing
        is found in the window (pre-first-build / a cold boot)."""
        d = clock.today()
        for _ in range(_WATCHLIST_LOOKBACK_DAYS):
            symbols = store.get_batch_universe_symbols(d)
            if symbols:
                return symbols
            d = d - timedelta(days=1)
        return watchlist_symbols()

    def held_symbols() -> list[str]:
        return _held_symbols(conn)

    # --- batch-leg tick subscriptions. The risk gate's ONLY price source is the live tick cache
    #     (`mark_price` above): brk20 originates over the whole ELIGIBLE set and hi52/ins/cat over its
    #     NIFTY 500 core, while the feed carries only the included watchlist, so an eligible-but-unticked
    #     candidate reached the gate with ltp=None and failed CLOSED. The sweep records what the
    #     pre-screen ADMITTED and the feed follows. Per-day: tomorrow's sweep re-admits off tomorrow's
    #     universe. These symbols are deliberately NOT in the warm-up coverage set
    #     (`warmup_gate.set_symbols` stays on `watchlist_symbols()`): a missing bar on one of them would
    #     FREEZE entries. ---
    _batch_ticks: dict[str, Any] = {"day": None, "symbols": set()}

    def ticker_tokens() -> list[int]:
        """Ticker subscription set: watchlist + FEED symbols + today's batch-admitted symbols +
        NIFTY 50 + India VIX → tokens. The composition — order, dedupe AND the midnight roll of
        the batch set — lives in :func:`_ticker_tokens` so a test exercises the production function."""
        return _ticker_tokens(
            watchlist=watchlist_symbols(), held=_feed_symbols(conn), batch_state=_batch_ticks,
            today=clock.today(), token_for_symbol=instruments.token_for_symbol,
        )

    # =========================================================================================
    # JOB REGISTRY — the single inventory driving BOTH the live scheduler and the
    # startup CatchUpRunner (watermark identities are the JOB_* constants; never rename).
    # =========================================================================================
    def _require_kite() -> KiteClient:
        if kite is None:
            raise RuntimeError("Kite client unavailable (login required) — safety job cannot run")
        return kite

    async def job_instruments() -> None:
        await instruments.refresh(_require_kite())
        # Persist today's dump so a restart after this job (whose watermark makes catch-up skip it) can
        # hydrate the token map pre-login. Surveillance/MIS columns are left NULL here: the surveillance
        # rule is enforced by the 08:30 universe exclusion, and the gate and /why read the surveillance_*
        # codes from universe_daily.
        today = clock.today()
        rows = instruments.snapshot_rows(today)
        persisted = await store.arun(store.upsert_instruments_daily, rows)
        # Alias SEED from this dump's company names (idempotent upsert; curated rows live under their
        # own keys); also tracks renames/new listings.
        seeded = await store.arun(resolver.seed_aliases, rows)
        curated = await store.arun(
            resolver.seed_curated_aliases, load_yaml(config_dir() / "aliases.yaml")
        )
        _log.info("instruments_persisted", d=today.isoformat(), rows=persisted,
                  aliases_seeded=seeded, aliases_curated=curated)

    async def job_surveillance() -> None:
        await surveillance.refresh()

    async def job_earnings() -> EarningsCalendarResult:
        # Returned, not swallowed: earnings_calendar degrades without raising, so the watermark verdict
        # needs the real ok/degraded outcome.
        return await earnings.run(clock.today())

    async def job_universe() -> None:
        await leverage.refresh()          # 08:15/08:20 inputs re-read on catch-up (self-refresh)
        await surveillance.current()
        before = set(watchlist_symbols())
        await universe_builder.build(clock.today())
        # Re-point the feed AND the warm-up coverage set at today's universe (both are set at boot, so a
        # pre-08:30 start would otherwise run the day on YESTERDAY's watchlist).
        warmup_gate.set_symbols(watchlist_symbols())
        try:
            if ticker.health().state != "STOPPED":
                await ticker.update_subscriptions(ticker_tokens())
        except Exception:  # noqa: BLE001 - a resubscribe failure degrades to the old set, never fails the job
            _log.exception("ticker_resubscribe_failed")
        # Symbols ENTERING the watchlist mid-session have no session bars yet (the boot/post-login gap
        # fill ran against the PREVIOUS set), and a hole keeps warm-up FROZEN. Two newcomer repairs: the
        # daily-history fill (the gate's exact window; runs at any time of day) and the minute fill
        # (mid-session only; pre-open builds have nothing missed yet).
        added = [s for s in watchlist_symbols() if s not in before]
        session = calendar.session(clock.today())
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
                gap = await backfill.warmup_gap(
                    added, session.open, clock.now(),
                    confirm_until=min(session.close, clock.now() - timedelta(minutes=2)),
                )
                _log.info("universe_added_gap_filled", symbols=added, bars=gap.bars_written)
            except Exception:  # noqa: BLE001 - a failed fill leaves the gate blocking (fail closed), never fails the job
                _log.exception("universe_added_gap_fill_failed", symbols=added)

    # ONE writer through the news chain at a time: the per-feed polls fire on independent intervals and
    # the scorer writes whole cluster rows back, so un-serialized a poll's cluster update is clobbered
    # by the scorer's stale snapshot (cluster assignment is also read-modify-write).
    # The lock guards that STORE read-modify-write, never the model call: the polls' resolve deadline
    # covers lock ACQUISITION, so holding it across an LLM await times them out. The scorer takes this
    # lock itself for its read and each conditional write-back only; under the write-back hold it
    # re-reads the CURRENT rows and applies the score columns to those (the upsert overwrites every
    # non-key column, so re-emitting the pre-LLM snapshot would revert a poll merge), and an id that
    # left the queue meanwhile is skipped, never resurrected.
    news_chain_lock = asyncio.Lock()

    async def resolve_news(headlines: list, *, alert_on_timeout: bool = False) -> None:
        if not headlines:
            return

        async def _chain() -> None:
            touched = await clusterer.run(headlines)
            await resolver.aload(clock.today())
            await resolver.run(touched)

        # Bounded: an unbounded resolve can wedge the whole startup. The owner alert fires only from the
        # chain/catch-up path (every per-feed poll shares this closure; a persistent hang would page once
        # per poll forever — the poll path degrades to the helper's own error log).
        await resolve_news_bounded(
            news_chain_lock, _chain,
            on_timeout=(lambda: alert(
                "warning",
                f"news resolve timed out after {_NEWS_RESOLVE_TIMEOUT_S:.0f}s — chain skipped; "
                "unclustered headlines re-sweep on the next run (never entry-blocking)",
            )) if alert_on_timeout else None,
        )

    async def score_news(*, force: bool = False) -> None:
        if scoring_job is None:
            return
        # The batch takes the lock itself (see above) — nothing here may hold it: asyncio.Lock is not
        # reentrant, so a wrapping `async with` would deadlock. job_news_chain's forced batch and the 300 s
        # news_scoring_tick can overlap; run_batch single-flights itself on a lock it owns.
        await scoring_job.run_batch(force=force, lock=news_chain_lock)

    async def job_news_chain() -> None:
        # backfill → cluster → resolve (never entry-blocking), then the pre-open scoring batch (force=True:
        # score ALL unscored regardless of the ≥8 minimum) so the digest sees today's scores. Scoring
        # failure never blocks the chain. Unclustered re-sweep: a bounded/cancelled resolve leaves
        # inserted-but-unlinked rows — fold them into this run so an abandoned backlog is retried.
        inserted = await news_ingest.backfill()
        seen = {h.headline_id for h in inserted}
        # Capped slice: clustering is O(headlines × window clusters), so an uncapped re-sweep after
        # repeated timeouts outgrows its own deadline. get_news orders oldest-first: bounded forward
        # progress every run.
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
        except Exception:  # noqa: BLE001 - unscored clusters just stay off the watchlist
            _log.exception("preopen_scoring_batch_failed")

    async def job_catalyst_digest() -> None:
        # Emits CATALYST_WATCHLIST on every SUCCESSFUL run — including an empty-but-fresh (0, 0); a failed
        # run alerts CATALYST_DISABLED and re-raises so the watermark records the failure.
        from engine.notify.catalog import catalyst_watchlist

        try:
            result = await digest_job.run(clock.today())
        except Exception as exc:
            await catalyst_disabled_alert(f"digest failed: {exc}")
            raise
        await notify(catalyst_watchlist(result.n_originating, result.n_context))

    async def job_preopen_planner() -> AdvisoryRun:
        if planner_job is None:
            # A FAILED watermark, not a silent success: catch-up retries once the roster is fixed.
            raise RuntimeError("preopen planner unavailable — LLM roster quarantined/unloaded")
        # Translate the advisory tri-state into the ok-bearing watermark verdict here: governor-blocked
        # is a CORRECT outcome (success watermark, no retry), only a harness failure is retryable.
        # ``_job_result_ok`` stays unwidened for bare bools.
        return AdvisoryRun(await planner_job.run(clock.today()))

    async def job_reco_expire() -> None:
        # Expired-unconfirmed recommendations become labelled no_action rows (unbiased non-fill
        # signal).
        now = clock.now()
        expired = book.expire_stale(now)
        if expired:
            _log.info("recommendations_expired", count=len(expired))
        cfg = settings.recommend
        if cfg.reminder_window_start <= now.time() <= cfg.reminder_window_end:
            rows = book.reminder_rows(now)
            if rows:
                await notify(catalog.rec_decision_reminder(rows))
                book.mark_reminded([r["rec_id"] for r in rows], now)
        if pipeline is not None:
            pipeline.log_hot_path_stats("eod")

    async def job_time_exit_check() -> None:
        if pipeline is None:
            # A FAILED watermark, not a silent success: open positions still need their time exit.
            raise RuntimeError("time exit unavailable — recommendation pipeline not built")
        await pipeline.time_exit_check(clock.today())

    async def job_nightly_review(d) -> AdvisoryRun:
        if nightly_job is None:
            raise RuntimeError("nightly reviewer unavailable — LLM roster quarantined/unloaded")
        # As job_preopen_planner: blocked ⇒ success watermark, harness-failed ⇒ failed watermark ⇒ the
        # next sweep retries (itself governor-gated inside NightlyReviewJob.run, bounding the spend).
        return AdvisoryRun(await nightly_job.run(d))

    async def job_sector_map() -> SectorMapResult:
        # Returned, not swallowed (degrades without raising; the watermark needs the real outcome).
        # Classifies the BATCH universe, not the tick watchlist: a watchlist-scoped run would leave every
        # capped and extended name without a sector_map row, and the risk limits cap the UNCLASSIFIED
        # bucket at 1 open position, which would gate-block the widened swing legs.
        return await sector_map.run(clock.today(), universe_symbols=batch_universe_symbols())

    async def job_corp_actions() -> CorpActionsResult:
        # Returned, not swallowed (degrades without raising; the watermark needs the real outcome).
        return await corp_actions.run(clock.today())

    async def job_backup() -> None:
        await _snapshot_backup(conn, settings, clock)
        # Nightly only — never on the shutdown backup, which runs inside NSSM's stop grace.
        try:
            pruned = await asyncio.to_thread(
                _prune_retained_files, settings.backups_dir(), settings.logs_dir(), clock.now()
            )
        except Exception as exc:  # noqa: BLE001 - housekeeping must not fail the backup job
            _log.warning("retention_prune_failed", error=str(exc), error_type=type(exc).__name__)
        else:
            _log.info("retention_pruned", **pruned)

    async def job_bhavcopy(d) -> BhavcopyResult:
        # Returned, not swallowed (degrades without raising; the watermark needs the real outcome).
        return await bhavcopy.run(d)

    async def job_deals(d) -> DealsResult:
        # Returned, not swallowed (same reason as job_bhavcopy).
        return await deals.run(d)

    async def job_filings_pit(d) -> FilingsPitResult:
        # Returned, not swallowed (same reason as job_bhavcopy).
        return await filings_pit.run(d)

    async def job_filings_pit_fresh(d) -> FilingsPitFreshResult:
        # Returned, not swallowed (same reason as job_bhavcopy).
        return await filings_pit_fresh.run(d)

    async def job_filings_results(d) -> FilingsResultsResult:
        # Returned, not swallowed (same reason as job_bhavcopy).
        return await filings_results.run(d)

    async def job_results_line_items() -> ResultsLineItemsResult:
        return await results_line_items.run()

    async def job_isin_map() -> IsinMapResult:
        return await isin_map.run()

    async def job_feed_freshness() -> FeedFreshnessResult:
        result = await feed_freshness.run()
        await check_calendar_horizon(
            calendar, clock, settings.clock.calendar_horizon_alert_sessions, notify)
        return result

    async def job_claude_login_check() -> None:
        await alert_claude_login(clock.now(), claude_credentials_path(), alert, relogin_hint)

    async def job_filings_shp() -> FilingsShpResult:
        # Returned, not swallowed (same reason as job_bhavcopy).
        return await filings_shp.run()

    async def job_ins_crossings(d) -> InsCrossingsResult:
        # `ins`: ok-bearing (never raises). ok=False means the day could not be EVALUATED
        # (no universe row / no daily bar), so the watermark sinks and the catch-up sweep retries it;
        # zero crossings on an evaluated day is ok=True — a real answer, not a failure.
        return await ins_crossings.run(d)

    async def job_reconcile(d) -> ReconcileReport | None:
        # Ok-bearing: a token abort is ok=False, so the watermark records the day failed and the
        # catch-up sweep retries it after re-login (run() resumes at the unlogged symbols). A known-dead
        # token makes no doomed broker call — the breaker already alerted once (backfill_hook gate).
        if reconcile is None:
            return None
        if not session.token_valid():
            _log.info("reconcile_skipped_no_token", d=d.isoformat())
            return ReconcileReport(d=d, ok=False, aborted_reason="no_valid_token")
        return await reconcile.run(d)

    async def job_daily_bars(d) -> BackfillReport | None:
        # Nightly incremental backfill: today's daily bar for the watchlist + regime symbols.
        # Ok-bearing (BackfillReport.ok): a token abort or a broker/network failure records the day
        # failed, so the catch-up sweep retries it and rec_outcomes waits for the bars.
        if backfill is None:
            return None
        symbols = [*watchlist_symbols(), INDEX_SYMBOL, VIX_SYMBOL]
        return await backfill.run(symbols, "day", d, d)

    async def job_features(d) -> None:
        await asyncio.to_thread(features.daily_snapshot, d)

    async def job_tick_compact(d) -> TickCompactionResult:
        # Collapse the closed tick partitions up to ``d`` into one file per symbol-day. Off the event loop
        # and on its OWN connection — never the live MarketStore's write path. Today's partition is
        # skipped inside compact_ticks (the writer still owns it). Ok-bearing: a failed symbol-day sinks
        # the watermark and the next sweep retries only what did not compact. ``stop`` ends the run at
        # the no-run window or on a stop signal (compact_ticks docstring). Buffered ticks are flushed
        # first (waiting out an in-flight flush, as close() does), else the session tail lands beside an
        # already-compacted date and costs a later merge rewrite (compact_ticks module docstring).
        await asyncio.to_thread(store.flush_ticks, wait_s=15.0)
        result = await asyncio.to_thread(
            compact_ticks, settings.parquet_dir(), upto=d, today=clock.today(),
            stop=lambda: stop_event.is_set() or _in_session_window(clock, calendar),
        )
        # Retention sweep runs here (see ``apply_tick_retention``); never changes the ok-bearing result.
        await apply_tick_retention(store, result)
        return result

    market_backup_retry = MarketBackupRetry()

    async def job_market_backup() -> MarketBackupResult:
        # DuckDB leg — a lane job only: never in boot catch-up, never on the stop path. It never starts
        # 06:00-15:45 on a trading day and is held for 6 h after a failed export (``_market_backup``).
        return await _market_backup(store, settings, clock, calendar, retry=market_backup_retry)

    # ``catch_up`` is bound below, before any job can run.
    rec_outcomes = RecOutcomesJob(conn, store, calendar, cost_model, hold_fn, clock,
                                  was_run=lambda job_id, d: catch_up.was_run(job_id, d))
    signal_outcomes = SignalOutcomesJob(conn, store, calendar, cost_model, limits_engine, settings, clock,
                                        was_run=lambda job_id, d: catch_up.was_run(job_id, d))
    weekly_summary = WeeklySummaryJob(conn, notify, lambda job_id, d: catch_up.was_run(job_id, d))

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
        JOB_RESULTS_LINE_ITEMS: job_results_line_items,
        JOB_ISIN_MAP: job_isin_map,
        JOB_FEED_FRESHNESS: job_feed_freshness,
        JOB_CLAUDE_LOGIN: job_claude_login_check,
        JOB_INS_CROSSINGS: job_ins_crossings,
        JOB_TICK_COMPACT: job_tick_compact,
        JOB_MARKET_BACKUP: job_market_backup,
        # Decision-plane: always registered; the wrappers whose object may be unbuilt (planner, nightly,
        # time-exit) RAISE, so the watermark records a FAILURE and the catch-up sweep retries.
        JOB_CATALYST_DIGEST: job_catalyst_digest,
        JOB_PREOPEN_PLANNER: job_preopen_planner,
        JOB_RECO_EXPIRE: job_reco_expire,
        JOB_TIME_EXIT_CHECK: job_time_exit_check,
        JOB_REC_OUTCOMES: rec_outcomes.run,
        JOB_SIGNAL_OUTCOMES: signal_outcomes.run,
        JOB_WEEKLY_SUMMARY: weekly_summary.run,
        JOB_NIGHTLY_REVIEW: job_nightly_review,
    }, calendar=calendar)

    # =========================================================================================
    # OPS: freeze seam, warm-up gate, heartbeat, catch-up (registry), self-test, lifecycle.
    # =========================================================================================
    async def freeze_entries(reason: str) -> None:
        # Through the cause ledger (single-writer discipline): a direct risk_state write is invisible to
        # clear_cause re-arms and the warm-up lift.
        if not kill.is_killed():
            await latch.set_cause(reason, RiskState.FROZEN, reason, Actor.RISK_GATE)

    async def clear_entries_cause(reason: str) -> None:
        # Mirror of freeze_entries: a safety-critical job verified fresh clears its own latched
        # data_freshness cause through the same ledger (clear_cause is idempotent on inactive causes).
        await latch.clear_cause(reason, Actor.RISK_GATE)

    # Mid-day token-death circuit breaker: the KiteClient fires this hook on the FIRST TokenException of
    # a burst (SessionManager.on_token_rejected is idempotent) so entries FREEZE and the owner is alerted.
    async def _on_session_invalidated() -> None:
        await freeze_entries("kite_token_rejected")
        await alert(
            "critical",
            "Kite token rejected by broker — entries FROZEN; re-login via the login link or /token",
        )
        # A tappable link: this path also covers the probe-'inconclusive'-then-dead boot, where NO login
        # prompt was sent at startup. The hook fires only via the KiteClient (⇒ api_key exists), so
        # login_url() cannot raise here.
        await notify(login_prompt(session.login_url()))
    session.set_invalidation_hook(_on_session_invalidated)

    async def _clear_token_freeze() -> None:
        # Auto-recovery class: a token-invalid freeze clears on successful re-login. The ledger
        # resolves — any OTHER active cause (owner_pause, floor rung, warm-up) keeps the state.
        await latch.clear_cause("kite_token_rejected", Actor.RISK_GATE)

    session.add_login_hook(_clear_token_freeze)

    warmup_gate = WarmupGate(
        store, clock, calendar,
        symbols=watchlist_symbols(), index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL,
    )
    heartbeat = HeartbeatWriter(settings.sqlite_path(), clock, interval_s=settings.lifecycle.heartbeat_write_s)
    catch_up = CatchUpRunner(conn, clock, calendar,
                             registry.select(lambda s: s.job_id not in LANE_JOB_IDS),
                             freeze=freeze_entries, notify=notify, clear=clear_entries_cause,
                             # Boot replays load-bearing data steps only; POST_ARM_JOB_IDS
                             # fire after scheduler.start().
                             deferred=POST_ARM_JOB_IDS if DEFER_POST_ARM_JOBS else ())
    compaction_lane = CompactionLane(
        CatchUpRunner(conn, clock, calendar, registry.select(lambda s: s.job_id in LANE_JOB_IDS),
                      notify=notify),
        clock, calendar, interrupt=store.interrupt_export,
    )

    self_test = SelfTest(
        conn=conn, clock=clock, settings=settings, secrets=secrets,
        protected_store=protected_store, kill_switch=kill, mode_manager=mode, session_manager=session,
        catch_up=catch_up, warmup_gate=warmup_gate,
        exposure=exposure, limits_engine=limits_engine, latch=latch,
        sdk_smoke=(None if harness is None else (lambda: run_sdk_smoke(harness))),
        calendar=calendar,
        roster_quarantined=roster_quarantined, roster_loaded=len(agent_defs),
        claude_credentials=claude_credentials_path(), claude_relogin_hint=relogin_hint,
    )
    # In-session OS keep-awake: keeps Windows from auto-sleeping while the NSE session is open (the
    # display may still sleep). Driven off the always-on health loop below.
    keep_awake = KeepAwake(enabled=settings.ticker.keep_awake_in_session)
    def _prescreen_funnel_today() -> tuple[int, int, int | None]:
        # Origination-liveness probe: today's prescreen slot count, forward events that reached the
        # analyst, and the governor's live daily forward cap. `forwarded` is the LLM-reach marker
        # (`evaluated` is re-armed to 0 on age-out — see prescreen_day_slots in pipeline.py); it is
        # day-cumulative, so the monitor stalls on "no PROGRESS", not on "zero". A cap read failure
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
        # The store stall watchdog rides the always-on health pulse — the one timer that keeps beating
        # while every store path is wedged.
        store=store,
        # Origination liveness: entries_frozen_in_session + funnel_zero_in_session ride the same
        # always-on pulse and episode cadence.
        mode_manager=mode, latch=latch, funnel_probe=_prescreen_funnel_today,
        # feed_stopped_in_session: token_valid only qualifies the page (a dead token vs a ticker that
        # never launched).
        token_valid=session.token_valid,
    )
    scheduler = Scheduler(clock, calendar)
    boot_state["scheduler"] = scheduler   # the boot-contract watchdog reads its REAL running state

    # --- injected recovery hooks (backfill; schedule re-arm and ticker resume) ---
    async def backfill_hook() -> None:
        # Step 4: warm up the regime history (NIFTY50/VIX daily — checkpointed) and gap-fill today's
        # intraday minute bars from official candles so warm-up never needs live ticks. Shared with the
        # post-login re-trigger (engine.ops.post_login). A token-less (or expired-token) boot must NOT
        # issue doomed historical calls; PostLoginRecovery re-runs this SAME hook once a token arrives.
        if backfill is None or not session.token_valid():
            _log.info("backfill_hook_skipped", reason="no valid Kite token")
            return
        await regime_and_warmup_backfill(
            backfill, clock, calendar, settings, watchlist_symbols, INDEX_SYMBOL, VIX_SYMBOL
        )

    async def ticker_resume_hook() -> None:
        # Step 7: resume the ticker into WARMING (feed-stale alarms suppressed) once a token exists;
        # shared with the post-login re-trigger via engine.ops.post_login.
        await resume_ticker(session, kite, ticker, ticker_tokens)

    async def backup_hook() -> None:
        # Best-effort: a failure must never block the shutdown guard (capital protection, not
        # housekeeping); swallowed so the STOPPED commit still proceeds.
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

    # --- holdings reconcile: answers "is this tracked position still IN the account?". Wired only when
    #     a broker facade exists. Alert-only: never writes state, never touches the gate/risk/prescreen;
    #     its failures are warnings. ---
    holdings_reconcile = (
        HoldingsReconcileJob(conn, kite, clock, calendar, notify) if kite is not None else None
    )

    # --- Post-login RE-TRIGGER: a boot BEFORE the daily login is token-less, so the broker-touching
    #     backfill/warm-up/ticker recovery no-ops. PostLoginRecovery re-runs those steps (each guarded +
    #     logged) the moment EITHER login path mints a valid token; it is a SessionManager login hook
    #     fired fire-and-forget so the login HTTP/Telegram path returns at once. ---
    post_login_recovery = PostLoginRecovery(
        instruments=instruments, store=store, kite=kite, session=session, clock=clock,
        calendar=calendar, settings=settings, backfill=backfill, ticker=ticker, lifecycle=lifecycle,
        ticker_tokens=ticker_tokens, watchlist_symbols=watchlist_symbols,
        index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL, notify=notify, alert=alert,
        holdings_reconcile=holdings_reconcile,
    )
    session.add_login_hook(post_login_recovery.run)

    # --- EARLY HYDRATION: an early login (~06:30) runs the pre-open chain (surveillance → universe → news
    #     → digest → planner) ahead of its clock through the same catch-up watermarks.
    #     Registration order buys NOTHING: `SessionManager._fire_login_hooks` creates one task per hook,
    #     so this hook and the recovery run CONCURRENTLY. What sequences them is the recovery's own
    #     `completed` event, passed below — the chain wants the token map, backfill and ticker it
    #     restores (bounded: a wedged recovery is logged and stepped over). The hook also holds itself
    #     behind `scheduler.start()` via the event armed in the boot tail.
    #     The session open is passed as a CALLABLE: this hook outlives the day it was built on. ---
    scheduler_armed = asyncio.Event()
    early_hydration = EarlyHydration(
        catch_up, clock, calendar, scheduler_armed, EARLY_HYDRATION_JOB_IDS,
        lambda: _session_open_ist(calendar, clock),
        recovery_done=post_login_recovery.completed, notify=notify,
    )
    session.add_login_hook(early_hydration.on_login)

    # --- dashboard API ---
    # The owner's capital change: ONE changer on the live tracker, the gate's own StrategyEdges, the paper
    # book's mark, and the last stored 1m close for a held symbol neither live cache has priced (both
    # are empty after an out-of-session restart).
    capital = CapitalChanger(
        conn=conn, clock=clock, calendar=calendar, store=protected_store, limits=limits_engine,
        mode=mode, real=exposure,
        paper_mark=paper.view().mark if paper is not None else None,
        last_mark=store_last_mark_fn(store),
        edges=strategy_edges, notify=notify,
    )
    app = _create_app(session, mode, kill, secrets, clock, bus, conn=conn,
                      protected_store=protected_store, exposure=exposure, governor=governor,
                      limits_engine=limits_engine, market_store=store, paper_control=paper_control,
                      paper_view=paper.view() if paper is not None else None, latch=latch,
                      capital=capital)
    if not secrets.has(DASHBOARD_TOKEN):
        _log.warning("dashboard_token_missing", hint="run scripts/dpapi_set.py --generate-dashboard-token")

    # --- platform-equity minute persist + continuous floor-ladder evaluation ("evaluated on
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
        # Daily-loss rungs.
        rungs = exposure.evaluate_day_loss(
            Decimal(str(table.limits.daily_loss_soft.day_mtm_pct)),
            Decimal(str(table.limits.daily_loss_hard.day_mtm_pct)),
        )
        if rungs:
            await exposure.apply_day_loss(rungs, mode, latch,
                                          alert=lambda m: alert("critical", m))

    # --- In-session/evening scoring cadence: run_batch() self-gates on the scoring windows,
    #     the ≥8/30-min batch trigger and the governor — this loop only provides the pulse. ---
    async def scoring_tick() -> None:
        await score_news()

    # --- Paced forward drain: one-minute pulse; pipeline.drain_forward_queue() owns BOTH the
    #     FORWARD_PACING_MIN cadence and the in-window/mode/kill/governor/cap admission. This loop only
    #     provides the pulse; the drain self-disables in `immediate` mode. ---
    async def forward_drain_tick() -> None:
        if pipeline is None:
            return
        # --- `brk20` RETEST re-arm, on this same 60 s pulse. BEFORE the drain so a level back inside
        #     the band this minute can take this tick's forward slot. Own try/except and module-level
        #     body: a retest failure must cost neither the drain nor the scheduler.
        #     The window predicate is `_retest_active`: the sweep's `_sweep_window_active` plus the
        #     states in which the pipeline is certain to drop the publication (FROZEN, killed) and "a
        #     sweep is running" — the retest has no freeze-lift path to redo a dropped offer, and its
        #     once-a-day offer bound is spent at offer time.
        #     OFF-LOOP (`asyncio.to_thread`): the body takes `SignalPreScreen._lock` and then mints a
        #     feature snapshot under `MarketStore._lock`; a store lock held for minutes would otherwise
        #     freeze the WHOLE loop (tick cache, heartbeat, kill path, Telegram, every APScheduler job).
        #     `sweep_ready` is the day's admission having happened: until today's IN-WINDOW sweep has
        #     published, the retest holds off so a fresh crossing of a resting symbol wins the
        #     pre-screen's `(symbol, strategy)` dedupe over a level broken days ago, and `_retest_skip`
        #     reads TODAY's eligible set and ex-date horizon. Fails to zero: a day whose sweep never
        #     published re-offers nothing.
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
    #     SELF-HEAL persistent intraday coverage holes (a gap fill that ended before live ticks began
    #     leaves a seam hole warm-up cannot lift past). ---
    _gap_repair_state: dict = {}

    async def warmup_refresh() -> None:
        await refresh_and_lift_warmup(warmup_gate, warmup_holder, mode, lifecycle, alert=alert,
                                      clock=clock)
        status = warmup_holder.get("status")
        if status is not None and backfill is not None:
            await maybe_repair_warmup_gaps(
                status, _gap_repair_state, clock=clock, calendar=calendar,
                repair=lambda frm, to: backfill.warmup_gap(watchlist_symbols(), frm, to,
                                                           confirm_until=to),
                token_valid=session.token_valid,
            )

    # --- periodic missed-job sweep: boot-time catch-up cannot help a machine that SLEEPS through a fire
    #     slot and resumes without a restart. The runner's watermarks make a repeat pass a no-op, so
    #     sweeping on a cadence is safe. ---
    async def catchup_sweep() -> None:
        try:
            # ALL scope: also the retry path for the post-arm one-shots. Single-flight makes a sweep
            # landing on a still-running pass a logged no-op. Compaction is swept in its own lane.
            await _catchup_sweep_once(catch_up, latch, kill, compaction_lane)
        except Exception:  # noqa: BLE001 - the sweep must never take down the scheduler loop
            _log.exception("catchup_sweep_failed")

    # --- holdings reconcile pulse: hourly, in-session only. The job swallows its own broker failures;
    #     this wrapper supplies the cadence and window and swallows anything else. ---
    async def holdings_reconcile_tick() -> None:
        if holdings_reconcile is None or not in_reconcile_window(clock.now(), calendar):
            return
        try:
            await holdings_reconcile.run()
        except Exception:  # noqa: BLE001 - a diagnostic must never take down the scheduler loop
            _log.exception("holdings_reconcile_tick_failed")

    protection_reminders = ProtectionReminders(conn, clock, calendar, notify, settings.recommend)

    async def protection_reminder_tick() -> None:
        try:
            await protection_reminders.tick()
        except Exception:  # noqa: BLE001 - a reminder must never take down the scheduler loop
            _log.exception("protection_reminder_tick_failed")

    # --- on-demand scanner sweep: "what could I trade right now, and at what price would today's setups
    #     arm?" Runs when the trade window becomes ACTIVE, when a freeze lifts inside the window, and on
    #     /scan_now. Live candidates re-enter the NORMAL pipeline path (dedupe/caps intact); the verdict
    #     is never silence. ---
    #: The last sweep's three PHASE marks (start / publication / completion), its counts and the risk
    #: state it published under — the freeze-lift debounce reads all of them (see
    #: :func:`_freeze_lift_skip_reason`). Written only by a sweep that got past the session check.
    _last_sweep: dict[str, Any] = {"started_at": None, "published_at": None, "done_at": None,
                                   "published": 0, "pending": 0, "frozen": False}

    #: ONE sweep at a time. The window_open sweep and the freeze lift can fire in the SAME tick, and a
    #: sweep is two passes over the eligible universe's daily history, each taking MarketStore._lock;
    #: two at once is a store-lock contention profile. Caps and the ins journal survive concurrency
    #: (prescreen.admit is lock-guarded); the store load is the reason.
    _sweep_lock = asyncio.Lock()

    async def run_scan_sweep(trigger: str, *, queue_behind_in_flight: bool = False) -> str:
        """THE sweep entry point for every trigger (window edge, freeze lift, /scan_now), single-flight.

        ``queue_behind_in_flight`` is the freeze-lift path's exception: a dropped re-sweep costs the
        day's batch origination, so the lift WAITS for the running sweep (see `_lift_scan_sweep`).
        The window edge and /scan_now get the skip, because their next cadence tick is free."""
        return await _single_flight_sweep(_sweep_lock, _scan_sweep, trigger,
                                          queue=queue_behind_in_flight)

    async def _scan_sweep(trigger: str) -> str:
        now = clock.now()
        session_day = calendar.session(now.date())
        if session_day is None or not (session_day.open <= now <= session_day.close):
            return "no session in progress — the sweep reads live bars; try during market hours"
        # PHASE mark 1 of 3. `_sweep_lock` stops two sweeps running at once; this mark lets the freeze
        # lift tell the in-flight cases apart — a sweep that has NOT published yet will publish under
        # the state this lift just established (skip), one that already published while FROZEN is the
        # one to redo (queue behind it).
        _last_sweep["started_at"] = now

        today = now.date()
        # --- Trade-window gate for the BATCH legs. The bar-driven leg gates itself off BAR time inside
        #     the pre-screen (which stays Clock-free); these candidates carry no bar, so the sweep —
        #     which HAS the Clock — applies the same test as `pipeline.on_signal_candidate`, else a
        #     /scan_now outside the window spends unrefundable day slots on candidates the pipeline
        #     drops. The window-open sweep fires ON the INACTIVE→ACTIVE edge, so it is open by
        #     construction. Computed HERE, not in the worker: the `ins` consume decision is taken at
        #     PUBLICATION on the loop thread, and both sides must read ONE window verdict. ---
        try:
            _w = calendar.trade_window(today)
            batch_in_window = _w[0] <= now <= _w[1]
        except ValueError:
            batch_in_window = False      # not a trading day ⇒ no window ⇒ nothing originates

        def _collect_and_scan():
            latest = []
            for sym in watchlist_symbols():
                tail = store.get_bars_1m(sym, session_day.open, now)
                if tail:
                    latest.append(tail[-1])
            accepted, pendings = prescreen.sweep(latest)

            # --- brk20 daily leg: completed-daily-bar breakouts over the FULL ELIGIBLE universe —
            #     watchlist_cap symbols have no 1m bars, so the bar-driven scanners never see them.
            #     Admission goes through prescreen.admit so dedupe/caps bind identically; a second
            #     sweep the same day re-admits nothing.
            # The eligibility predicate lives in ONE place (store.get_universe_eligible_symbols); every
            # batch leg reads this ONE set.
            eligible = store.get_universe_eligible_symbols(today)
            # brk20 (no registered edge — the cost gate prices each candidate from its own levels) scans
            # the full eligible set. hi52, ins and both cat legs originate from the CORE (NIFTY 500)
            # subset only: their registered edge / frozen shadow verdict was measured there, and hi52's
            # full-market cell was negative.
            core_eligible = store.get_core_eligible_symbols(today)
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
            # Horizon = the WIDEST ex_skip_days of every rule that reuses this map (a hardcoded brk20
            # horizon would under-fetch for hi52 if either default is retuned).
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
            # Trailing structural corp actions (the hi52 veto's premise): stored bars_1d is never
            # re-adjusted across an ex-date, so a bonus/split/rights/demerger inside the 20-session window
            # leaves a phantom pre-ex high. 35 calendar days ≥ lookback+1 sessions with holiday margin.
            brk20_unadjusted = hi52.unadjusted_history(
                store.get_corp_actions(ex_from=today - timedelta(days=35), ex_to=yesterday)
            )
            brk20_vetoes: dict[str, int] = {}
            brk20_raw = brk20.sweep_daily(
                histories, today=today, ex_dates_by_symbol=ex_map,
                unadjusted_symbols=brk20_unadjusted, veto_counts=brk20_vetoes,
            )
            # Regime gate: judged once, at origination. Admitted levels rest their window and the
            # retest tick is never re-gated, because the gate measured the regime at the crossing session.
            regime_keep, idx_regime, regime_reason = True, None, None
            if settings.brk20.regime_filter:
                try:
                    regime_keep, idx_regime, regime_reason = _brk20_regime_gate(
                        store.get_bars_1d_frame(
                            regime.INDEX_SYMBOL, today - timedelta(days=regime.LOOKBACK_DAYS), yesterday
                        ),
                        calendar, today,
                    )
                except Exception:  # noqa: BLE001 - an unreadable regime costs brk20 alone, never the sweep
                    _log.exception("brk20_regime_unreadable", d=today.isoformat())
                    regime_keep, idx_regime, regime_reason = False, None, "undefined"
            # Veto visibility: a run where the stop-geometry floor eats everything must read as a floor
            # decision, not an empty tape. Counts are ORIGINATION-stage (pre-admission), so candidates +
            # vetoes reconcile against symbols_scanned; the prescreen logs what its caps then do.
            _log.info(
                "brk20_sweep", d=today.isoformat(), trigger=trigger,
                symbols_scanned=len(histories),
                candidates=len(brk20_raw),
                gap_floor_vetoes=brk20_vetoes.get(brk20.VETO_GAP_FLOOR, 0),
                floor_unavailable=brk20_vetoes.get(brk20.VETO_FLOOR_UNAVAILABLE, 0),
                unadjusted_vetoes=brk20_vetoes.get(brk20.VETO_UNADJUSTED_HISTORY, 0),
                regime_up=None if idx_regime is None else idx_regime.up,
            )
            brk20_blocked: list[SignalCandidate] = []
            if not regime_keep:
                _log.info(
                    "brk20_regime_blocked", d=today.isoformat(), reason=regime_reason,
                    close=None if idx_regime is None else round(idx_regime.close, 2),
                    sma50=None if idx_regime is None else round(idx_regime.sma, 2),
                    rising=None if idx_regime is None else idx_regime.rising,
                    blocked_leg=None if idx_regime is None else idx_regime.blocked_leg,
                    blocked=len(brk20_raw),
                )
                brk20_blocked, brk20_raw = brk20_raw, []

            # --- `ins` daily leg: the crossings last night's ins_crossings job journalled into
            #     ins_pending. Same batch shape as brk20 (admitted through prescreen.admit), but the
            #     EVENT was decided EOD by the validated crossing function; nothing is re-derived here.
            #     Rows are marked consumed as part of admission, so a second sweep the same day (or after
            #     a restart) re-admits nothing: the flag is persisted state, not process memory.
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

            # --- `cat` v2 SHADOW leg: today's `originating` catalyst_watchlist rows, graded by the digest,
            #     on the SAME ins-shaped batch path. Once-only needs no consumed flag: the age<=1 filter
            #     inside sweep_watchlist IS the single-shot rule, and a second sweep the same day hits
            #     the prescreen dedupe. The gate's cost check rejects every one of these by construction
            #     (no cat.expected_edge_pct exists), so ADMISSION is the shadow's validation population,
            #     not RECOMMEND. An absent/empty digest yields no rows (cat originates nothing, every
            #     other strategy unaffected).
            cat_rows, cat_rev_rows = _read_cat_watchlist(store, today)
            # BOTH cat legs' origination stays pinned to the CORE (NIFTY 500) set although the news layer
            # grades the wider batch universe: the shadow verdict populations and clocks are FROZEN, and
            # an extended-symbol candidate would skew that population and burn the shared 2/day catalyst
            # budget. Extended symbols' rows still journal in catalyst_watchlist (analyzable later).
            _core_set = set(core_eligible)
            cat_rows = _watchlist_rows_for_symbols(cat_rows, _core_set)
            cat_rev_rows = _watchlist_rows_for_symbols(cat_rev_rows, _core_set)
            cat_raw: list = []
            if cat_rows:
                cat_raw = cat.sweep_watchlist(
                    cat_rows,
                    params={
                        "stop_pct": settings.cat.stop_pct,
                        "hold_sessions": settings.cat.hold_sessions,
                    },
                )

            # --- `cat_reversal` SHADOW leg: the SUBSET of today's originating rows whose winning cluster
            #     REVERSES an earlier, floor-clearing, opposite-direction cluster of the same
            #     (symbol, event_type) story. A separate experiment from `cat` v2 (own thresholds, own
            #     T+5/T+10 clock, own journal series); a reversal row deliberately still originates for
            #     `cat` too (narrowing `cat` would restart its clock). Both legs share the ONE
            #     catalyst_guard.max_catalyst_entries_day budget (the pre-screen keys that cap off
            #     catalyst_ref, not strategy_id), so this widens no exposure surface. The gate's cost
            #     check rejects every one UNCONDITIONALLY (no_edge_shadow_strategies): ADMISSION is the
            #     shadow's validation population.
            cat_rev_raw: list = []
            if cat_rev_rows:
                cat_rev_raw = cat_reversal.sweep_watchlist(
                    cat_rev_rows,
                    params={
                        "stop_pct": settings.cat_reversal.stop_pct,
                        "hold_sessions": settings.cat_reversal.hold_sessions,
                    },
                )

            # The once-per-session `hi52` leg — the only trigger-keyed branch in the sweep; module-level
            # so "freeze_lift takes the same branch as window_open" is assertable. Called HERE, before
            # the admission below, because its candidates join that ONE ranked batch.
            hi52_raw = _hi52_daily_leg(
                store, trigger=trigger, eligible=core_eligible, today=today,
                yesterday=yesterday, ex_map=ex_map,
            )

            # --- ONE ranked admission across all batch legs. Ranking orders only WITHIN one batch, so
            #     separate admit calls would make the legs first-come in source order — and `cat` is
            #     single-shot by construction (cat.MAX_EVENT_AGE_SESSIONS = 1), so a story that loses
            #     the race on the day cap can never re-originate. Concatenating first lets _rank see all
            #     legs as one batch and a binding cap keeps the best of them, whatever leg produced it.
            #     `hi52` is in it because a rule that can reach RECOMMEND must contest the same slots;
            #     its per-strategy cap (3) bounds it.
            #     Snapshot mint stays AFTER admit (a suppressed candidate never spends a snapshot
            #     write); without it the analyst's Rule 6 refuses every batch candidate unseen.
            #     ADMISSION is that one ranked call; PUBLICATION order is a separate consequence owned
            #     by `_publication_order` (see it for why a promoted hi52 must not take the queue front).
            batch = _attach_feature_snapshots(
                features,
                _publication_order(prescreen.admit(
                    brk20_raw + ins_raw + cat_raw + cat_rev_raw + hi52_raw, today,
                    in_window=batch_in_window,
                )),
            )
            # THE starvation-visibility line: sustained zeros must read as "the news layer went quiet" vs
            # "rows arrived and the rule/caps declined them" — only detectable if the counts sit on one
            # line. `cat_reversal` shares it: its starvation criterion (scanners/cat_reversal.py
            # docstring) is checked against the originating flow it is a subset OF.
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
            # The resting brk20 levels ride the SAME list: `_batch_ticks` rolls at midnight, so without
            # this a level admitted on day 1 has no tick on days 2-5 and `RestingLevelBook.due` (which
            # bands against the LIVE price) could never offer it again.
            #
            # Fifth = the two NARROWINGS the retest tick re-applies to a level broken days ago: today's
            # ELIGIBLE population (a name that churned out must not keep originating) and the ex-date
            # veto over brk20's OWN `ex_skip_days` horizon (`bars_1d` is never re-adjusted, so a stored
            # level is a pre-ex price the tape stops quoting — `brk20.scan_daily` refuses such a symbol
            # on the crossing day and the re-offer must refuse it for the same reason).
            _brk20_ex_horizon = today + timedelta(days=int(brk20.DEFAULT_PARAMS["ex_skip_days"]))
            retest_ctx = {
                "eligible": frozenset(eligible),
                "ex_skip": frozenset(
                    sym for sym, dates in ex_map.items()
                    if any(today <= xd <= _brk20_ex_horizon for xd in dates)
                ),
                # The BACKWARD-looking half of the corporate-action veto: a stored level is a pre-ex price
                # on the same unadjusted series, so the re-offer must refuse it — `ex_skip` looks forward only.
                "unadjusted": frozenset(brk20_unadjusted),
            }
            return (
                accepted + batch,
                pendings,
                [c.symbol for c in batch] + retest_book.resting_symbols(today),
                [c.symbol for c in ins_pending],
                retest_ctx,
                (brk20_blocked, idx_regime, regime_reason),
            )

        accepted, pendings, batch_symbols, ins_read, retest_ctx, regime_blocked = await asyncio.to_thread(
            _collect_and_scan
        )
        # Point the feed at today's batch admissions BEFORE publishing them: the risk gate reads the tick
        # cache and nothing else, and the pipeline's analyst hop is the only window in which a first tick
        # can arrive. Guarded like the job_universe resubscribe — a failure degrades to the previous set.
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
        # The once-only bound is applied at the moment the rows' fate is decided, not in the worker (the
        # whole worker, hi52's large read included, sits between the rows' READ and this decision).
        _consume_ins_pending(
            conn, now.date(),
            _ins_rows_to_consume(ins_read, in_window=batch_in_window,
                                 published_frozen=_published_frozen),
            now=now,
        )
        # Every ADMITTED brk20 level starts RESTING here, so the next `retest_sessions` − 1 sessions can
        # re-offer it when the price comes back. ADMITTED, never raw: resting a capped-out fire would
        # re-offer tomorrow what the caps refused today (`accepted` is the bar-driven candidates plus
        # the post-admit `batch`, and no bar-driven rule is brk20).
        # On the LOOP THREAD, beside `_consume_ins_pending`: this connection is `isolation_level=None`
        # with `check_same_thread=False` because the engine serialises its writes on this thread. A bare
        # INSERT from the sweep WORKER while the loop is between BEGIN and COMMIT in
        # `core.db.transaction` joins that transaction and dies with its ROLLBACK — silently, since
        # `record` has already returned True and logged.
        for _cand in accepted:
            if _cand.strategy_id == brk20.STRATEGY_ID:
                retest_book.record(_cand)
        # The regime gate's blocked arm, on the same thread; in-window only, as only an in-window sweep admits.
        if batch_in_window and regime_blocked[0]:
            _journal_regime_blocked(conn, *regime_blocked, today, now=clock.now())
        # The retest tick is armed for today only by an IN-WINDOW sweep — the one that can admit a fresh
        # crossing. Pre-open `/scan_now` admits nothing, so arming on it would let the minute pulse
        # publish a days-old level into the `(symbol, strategy)` dedupe just before the window-open
        # sweep's fresh cross of the same symbol, which would then be dropped as a duplicate.
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
        # Completion stamp (`frozen` was set at publication, the moment it describes): the freeze-lift
        # debounce asks both "is a sweep still RUNNING?" and "did one just FINISH?".
        _last_sweep.update(done_at=clock.now(), published=len(live_rows),
                           pending=len(pending_rows))
        if trigger != "scan_now":       # /scan_now gets the body as its direct reply — no double send
            await notify(msg)
        if batch_in_window:
            # Without today's digest `cat` originated nothing in this sweep. Said even when no digest run
            # raised (a late boot sweeps before the catch-up digest), and only after the sweep.
            try:
                await catalyst_disabled_alert.if_no_digest_today(store, today)
            except Exception:  # noqa: BLE001 - an alert must never fail the sweep
                _log.exception("catalyst_digest_check_failed")
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
                    # The edge is a ONCE-PER-DAY event and the single-flight guard just declined it: give
                    # it back so the next 60 s tick re-fires it. An in-flight owner /scan_now is no
                    # substitute (it skips the once-per-session hi52 leg and the window-open digest).
                    _window_active["was"] = False
            except Exception:  # noqa: BLE001 - a sweep failure must never take down the scheduler
                # A FAILED sweep keeps the edge spent: a stall would otherwise re-fire the whole
                # eligible-universe read every 60 s.
                _log.exception("window_open_sweep_failed")

    # --- freeze-lift re-sweep. The batch legs (brk20/hi52/cat/ins) originate ONLY from the sweep, and a
    #     candidate published INTO a standing freeze is re-armed by `pipeline.on_signal_candidate` with
    #     nothing left to re-publish it (batch rules have no next bar). Every in-session boot re-applies
    #     a warm-up freeze AND re-fires the window edge (`_window_active` is per-process), so the LIFT is
    #     the re-publication trigger. Idempotent otherwise: the pre-screen's same-day (symbol, strategy)
    #     dedupe drops every pair that was not re-armed.
    #     Armed HERE, ahead of `lifecycle.startup()`, because the subscription must exist before the
    #     first transition is published — and gated on `engine_ready` for the same reason (the boot
    #     itself publishes lifts; see `_freeze_lift_sweep`'s `ready`). ---
    from engine.risk.events import TOPIC_RISK_STATE

    _freeze_lift = {"fired": None}
    _freeze_lift_tasks: set[asyncio.Task] = set()

    async def _lift_scan_sweep(trigger: str) -> str:
        """The lift's sweep call: it QUEUES behind an in-flight sweep rather than taking the
        single-flight skip — the lift is the day's ONLY re-publication of the batch legs, and a running
        sweep can already have published its batch INTO the freeze this lift just cleared
        (`_last_sweep["frozen"]`). `_freeze_lift_skip_reason` has already dropped the case where the
        running sweep has NOT published yet."""
        return await run_scan_sweep(trigger, queue_behind_in_flight=True)

    async def _on_risk_state_sweep(evt: Any) -> None:
        # DETACHED: `apublish` awaits its subscribers in turn, and a sweep is a full eligible-universe
        # history read — inline, it would stall the 60 s warm-up-refresh job that publishes most lifts
        # and any owner /resume reply. The debounce claim inside `_freeze_lift_sweep` happens before
        # that coroutine's first await, so two lifts in one minute still sweep once. The set holds a
        # strong reference (the loop keeps only a weak one).
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
        telegram.set_why_fn(make_why_fn(store=store, conn=conn, clock=clock, calendar=calendar))

    # --- arm the schedule (calendar-guarded) BEFORE recovery so a late startup still fires today ---
    _arm_registry_jobs(scheduler, registry, catch_up, clock, lane=compaction_lane)
    # Pre-open token probe at 08:40. Only with a broker facade; NOT a registry job (a pre-open check
    # has no meaningful catch-up).
    if kite is not None:
        _arm_token_check(
            scheduler,
            TokenCheckJob(kite=kite, clock=clock, calendar=calendar, notify=notify,
                          login_url=session.login_url),
        )
    _arm_live_jobs(scheduler, settings, bar_builder, health, news_ingest, resolve_news,
                   ticker=ticker, calendar=calendar, clock=clock, equity_tick=equity_tick,
                   scoring_tick=scoring_tick,
                   warmup_refresh=warmup_refresh, catchup_sweep=catchup_sweep,
                   window_sweep_tick=window_sweep_tick, forward_drain_tick=forward_drain_tick,
                   holdings_reconcile_tick=(
                       holdings_reconcile_tick if holdings_reconcile is not None else None
                   ),
                   protection_reminder_tick=protection_reminder_tick,
                   paper_tick=paper.runtime.paper_tick if paper is not None else None)

    return Engine(
        settings=settings, conn=conn, clock=clock, calendar=calendar, bus=bus, mode=mode,
        session=session, kite=kite, telegram=telegram, app=app, alert=alert, notify=notify,
        instruments=instruments, store=store, http=http, bar_builder=bar_builder, ticker=ticker,
        paper=paper, warmup_gate=warmup_gate, warmup_holder=warmup_holder, skew_holder=skew_holder,
        warmup_refresh=warmup_refresh, health=health, lifecycle=lifecycle, scheduler=scheduler,
        scheduler_armed=scheduler_armed, catch_up=catch_up, compaction_lane=compaction_lane,
        early_hydration=early_hydration, freeze_lift_tasks=_freeze_lift_tasks,
        registry=registry, scan_sweep=run_scan_sweep,
    )


class _BootTasks(NamedTuple):
    server_task: asyncio.Task | None
    post_arm_task: asyncio.Task | None
    early_hydration_task: asyncio.Task


async def _boot(engine: Engine, boot_state: dict[str, Any]) -> _BootTasks:
    # --- bring the owner alert channel up BEFORE recovery so startup notifications + any alert raised
    #     during recovery actually reach the owner instead of being dropped 'not_started'. ---
    if engine.telegram is not None:
        await engine.telegram.start()
    await check_calendar_horizon(engine.calendar, engine.clock, engine.settings.clock.calendar_horizon_alert_sessions, engine.notify)

    # --- BIND THE LOGIN CALLBACK API *BEFORE* startup recovery. The owner's only browser login route
    #     (GET /kite/callback, :8400) is hosted by THIS uvicorn server; a boot on an expired token can
    #     wedge inside the warm-up backfill, so binding after lifecycle.startup() locks login out. The
    #     app is fully wired, so /kite/callback works the instant the socket binds; a login mid-startup
    #     just fires the idempotent PostLoginRecovery hook. _serve_api binds the socket ITSELF (uvicorn's
    #     own bind paths sys.exit(1), which would kill the loop) and returns None on failure — the
    #     engine CONTINUES either way (Telegram /token still permits the daily login). ---
    server_task = await _serve_api(engine.app, engine.settings)
    _bound = getattr(server_task, "_mt_server", None)
    if _bound is None or not _bound.started:
        await engine.alert(
            "critical",
            f"dashboard/login API failed to bind {engine.settings.api.host}:{engine.settings.api.port} — "
            "browser login unreachable; use Telegram /token",
        )

    # --- LIVE token probe: behavioural token_valid() answers True on a stale-but-not-yet-rejected token.
    #     Probe ONCE here so token_valid() is truthful for the hydrate + startup below, and send the
    #     login link the instant the token is known rejected/absent (guarded on kite so login_url()
    #     never raises on an api-key-less install). ---
    probe = await engine.session.verify_token()
    _log.info("token_probe", outcome=probe)
    login_prompt_sent = False
    if probe in ("rejected", "absent") and engine.telegram is not None and engine.kite is not None:
        await engine.notify(login_prompt(engine.session.login_url()))
        login_prompt_sent = True

    # --- cold-start token-map recovery: rebuild the in-memory instruments index (persisted snapshot
    #     pre-login, or a live refresh) BEFORE the backfill + warm-up inside lifecycle.startup, else a
    #     restart after 08:15 finds an empty map (unknown_token). session_valid is the LIVE-probed truth. ---
    try:
        instruments_source = await hydrate_instruments_at_startup(
            engine.instruments, engine.store, engine.kite, session_valid=engine.session.token_valid(), clock=engine.clock,
        )
    except Exception:  # noqa: BLE001 - a recovery step must never crash the boot: degrade to
        # an empty token map (entries stay FROZEN via the warm-up gate) with a loud, named cause.
        _log.exception("instruments_hydrate_failed")
        instruments_source = "failed"

    # --- every-startup recovery. check_skew honours NTP; degrades to FROZEN if unreachable.
    #     Boot-phase safety ticks run alongside it: OBSERVATION-ONLY — the warm-up SNAPSHOT (never the
    #     lift/repair; see boot_phase_ticks) + the health/keep-awake pulse must not wait out a long
    #     catch-up; cancelled AND awaited before startup handling continues, so no tick body overlaps
    #     the post-boot warmup_refresh. ---
    _boot_ticks = asyncio.create_task(
        boot_phase_ticks(
            lambda: refresh_warmup_snapshot(engine.warmup_gate, engine.warmup_holder),
            lambda: engine.health.check(check_skew=False, booting=True),
        )
    )
    try:
        report = await engine.lifecycle.startup(check_skew=True)
    finally:
        _boot_ticks.cancel()
        try:
            await _boot_ticks
        except asyncio.CancelledError:
            pass
    boot_state["startup_done_at"] = time_module.monotonic()   # arms the watchdog's deadline clock
    _log.info("startup_complete", mode=report.sticky_mode, killed=report.killed,
              needs_login=report.needs_login, integrity_ok=report.integrity_ok,
              jobs_caught_up=len(report.jobs_caught_up), frozen=report.frozen_reasons,
              instruments=instruments_source)
    # Boot-scoped skew verdict for the gate's clock_skew rule (self-test measured it above);
    # seed the warm-up snapshot immediately so the gate isn't blind until the first 60s refresh.
    engine.skew_holder["ok"] = "clock_skew" not in report.frozen_reasons
    # Invariant: nothing unbounded ahead of scheduler.start(). A bare await here can park forever (the
    # tick flush loop takes MarketStore._lock per partition COPY and starves WarmupGate._evaluate's
    # sequential to_thread hops), leaving the scheduler unarmed all day.
    # The seeding is a convenience, never a safety property: an unseeded warm-up holder reads
    # _WARMUP_UNREFRESHED (fail-CLOSED, both gate readiness rules blocking) and the 60 s
    # warmup_status_refresh / health_check jobs re-seed both once the scheduler is up. So: a hard
    # deadline and carry on — losing a snapshot costs one fail-closed minute, losing the scheduler
    # costs the trading day.
    await seed_boot_snapshots(
        engine.warmup_refresh, lambda: engine.health.check(check_skew=False), alert=engine.alert,
    )

    # --- start remaining services; run() then idles until a stop signal (being up is an active
    #     period). The login API is already bound (above); only prompt again here if startup still needs
    #     a login AND we did not already send the link off the token probe (no double link). ---
    # Arm the scheduler FIRST, then fire the never-load-bearing one-shots behind it as a background
    # task. engine_ready (below) must not wait on the news chain — a wedged chain costs the digest, not
    # the scheduled day. Its own 600 s resolve cap bounds it.
    post_arm_task = start_scheduler_and_fire_post_arm(engine.scheduler, engine.catch_up, armed=engine.scheduler_armed)
    engine.compaction_lane.spawn("post_arm")
    # EARLY HYDRATION boot trigger: a boot on a trading day before the open hydrates under the same
    # gates as an early login — the watermarks it leaves make a later login free.
    early_hydration_task = asyncio.create_task(engine.early_hydration.on_boot(), name="early_hydration_boot")
    if engine.telegram is not None and report.needs_login and not login_prompt_sent:
        await engine.notify(login_prompt(engine.session.login_url()))

    _log.info("engine_ready", host=engine.settings.api.host, port=engine.settings.api.port, mode=engine.mode.mode().value)
    boot_state["engine_ready"] = True   # the other half of the boot contract
    return _BootTasks(server_task, post_arm_task, early_hydration_task)


async def _shutdown(engine: Engine, tasks: _BootTasks, *, boot_watchdog: asyncio.Task) -> None:
    # --- graceful shutdown (shutdown guard): flatten in-flight bars, stop the ticker, run
    #     the lifecycle guard (backup + STOPPED commit + heartbeat join), then tear down the rest. ---
    _log.info("engine_stopping")
    await cancel_post_arm(boot_watchdog)      # retire the contract watchdog BEFORE the
    # scheduler stops, else a slow teardown reads is_running()==False and pages a violation.
    engine.scheduler.shutdown()                      # no new job fires can race the teardown
    await cancel_post_arm(tasks.post_arm_task)      # a still-running post-arm one-shot never blocks a stop
    await engine.compaction_lane.cancel()            # …nor a compaction lane pass
    await cancel_post_arm(tasks.early_hydration_task)  # a still-running boot hydration never blocks a stop
    if engine.freeze_lift_tasks:
        # …nor a detached freeze-lift re-sweep. WAIT it out first: cancelling the TASK does not stop its
        # `asyncio.to_thread` worker, which would keep reading the store `store.close()` is about to take
        # away. The cancel after the wait is the bound: a stop never hangs on a wedged sweep.
        await asyncio.wait(list(engine.freeze_lift_tasks), timeout=_SHUTDOWN_LIFT_WAIT_S)
    for _lift_task in list(engine.freeze_lift_tasks):
        await cancel_post_arm(_lift_task)
    engine.bar_builder.flush_all()                   # finalize any open minute bars (EOD/shutdown)
    await engine.ticker.stop()
    if engine.paper is not None:
        await engine.paper.stop()                    # no paper write may race the shutdown backup
    await engine.lifecycle.shutdown()                # runs backup hook, commits STOPPED, joins the heartbeat
    if engine.telegram is not None:
        await engine.telegram.stop()
    await _stop_api(tasks.server_task)
    await engine.http.aclose()
    undrained = await engine.bus.drain(_SHUTDOWN_BUS_DRAIN_S)   # no handler may reach a closed store
    if undrained:
        _log.warning("shutdown_bus_undrained", deliveries=undrained, waited_s=_SHUTDOWN_BUS_DRAIN_S)
    engine.store.close()
    engine.conn.close()


# NOTE: ``hydrate_instruments_at_startup`` lives in ``engine.ops.post_login`` (shared with the
# post-login re-trigger) and is re-exported here; callers/tests import it from ``engine.ops.main``.


# --------------------------------------------------------------------------- boot tail
def _in_session_window(clock: Clock, calendar: NSECalendar) -> bool:
    """Inside :data:`_IN_SESSION_START_IST`..:data:`_IN_SESSION_END_IST` on an NSE trading
    day, where tick compaction must not run — it competes with the tick writer for the resources the
    session needs. A non-trading day inside the same clock window is outside it: a Saturday is
    precisely when the backlog SHOULD be collapsed."""
    now = clock.now()
    return calendar.is_trading_day(now.date()) and _IN_SESSION_START_IST <= now.time() <= _IN_SESSION_END_IST


class CompactionLane:
    """Every :data:`LANE_JOB_IDS` job (``tick_compact``, the ``market_backup`` export) in its OWN
    single-flight catch-up runner.

    In the shared runner a backlog compaction would hold the pass lock for hours and starve the main
    sweep. This runner shares only the ``job_runs`` table with the main one, so a long compaction delays
    nothing but compaction. At most one lane pass runs at a time, and none starts inside the
    tick_compact no-run window; a running one stops there by itself (``job_tick_compact``'s ``stop``).
    ``interrupt`` (``MarketStore.interrupt_export``) ends an export's worker thread on :meth:`cancel`.
    """

    def __init__(
        self, runner: CatchUpRunner, clock: Clock, calendar: NSECalendar,
        *, interrupt: Callable[[], object] | None = None,
    ) -> None:
        self._runner = runner
        self._clock = clock
        self._calendar = calendar
        self._interrupt = interrupt
        self._task: asyncio.Task | None = None

    def spawn(self, path: str, *, quiet: frozenset[str] = frozenset()) -> asyncio.Task | None:
        """Start a lane pass unless one is running or the tick_compact no-run window vetoes it (``path`` names the
        caller in the log). Returns the new task, or ``None`` when nothing was started.
        ``quiet`` is the on-time scheduled fire's own entry (:func:`_lane_fire`): that entry's success
        alone is no CATCHUP_REPORT (``CatchUpRunner.catch_up``)."""
        if self._task is not None and not self._task.done():
            return None
        if _in_session_window(self._clock, self._calendar):
            _log.info("post_arm_skipped_in_session", job_ids=sorted(LANE_JOB_IDS),
                      now=self._clock.now().isoformat(), path=path)
            return None
        self._task = asyncio.create_task(self._pass(quiet), name="compaction_lane")
        return self._task

    async def _pass(self, quiet: frozenset[str]) -> None:
        try:
            await self._runner.catch_up(scope=CatchUpScope.ALL, quiet=quiet)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - housekeeping must never surface into the scheduler loop
            _log.exception("compaction_lane_failed")

    async def cancel(self) -> None:
        """Cancel + join a running lane pass on shutdown. Its worker thread cannot be cancelled: a
        compaction ends before its next symbol-day once the stop signal is set (``compact_ticks``'s
        ``stop``), an export when ``interrupt`` reaches it (``MarketStore.close`` repeats it)."""
        if self._interrupt is not None and self._task is not None and not self._task.done():
            try:
                self._interrupt()
            except Exception:  # noqa: BLE001 - teardown never raises; close() interrupts again
                _log.exception("compaction_lane_interrupt_failed")
        await cancel_post_arm(self._task)


async def _catchup_sweep_once(catch_up: CatchUpRunner, latch, kill, lane: CompactionLane) -> CatchUpResult:
    """One 30-min catch-up sweep: ALL scope, the catchup_safety_jobs freeze reconciliation, then a
    compaction lane pass (:meth:`CompactionLane.spawn`). Extracted from the scheduler closure to be testable."""
    result = await catch_up.catch_up(scope=CatchUpScope.ALL)
    await _reconcile_catchup_freeze(result, latch, kill)
    lane.spawn("sweep")
    return result


def start_scheduler_and_fire_post_arm(
    scheduler: Scheduler, catch_up: CatchUpRunner, *, armed: asyncio.Event,
) -> asyncio.Task | None:
    """Arm the scheduler, THEN fire the deferred one-shots behind it — never the other way round.

    A chain that never returns would starve every scheduled job, including the self-healing 30-min
    catchup sweep; arming first makes its worst case local to itself.

    The one-shot IS a catch-up pass (``DEFERRED`` scope): identical code, ``job_runs`` watermarks and
    dependency order — only the firing point moved. A run that fails records a FAILED watermark and the
    next sweep (``ALL``) retries it. Deliberately NOT awaited: the caller logs ``engine_ready``
    immediately after. Returns the task (``None`` when the flag is off / nothing is deferred) so
    shutdown can cancel it; a crash inside is logged, never raised into the boot path.

    ``armed`` is the "the scheduler is up" event the early-login hook waits on, so its pre-open chain
    obeys the SAME firing-point rule. REQUIRED: a caller that forgot it would park every early login
    until the hook's own 15-minute timeout.
    """
    scheduler.start()
    if not (DEFER_POST_ARM_JOBS and POST_ARM_JOB_IDS):
        armed.set()                     # nothing to dispatch — arming itself is the release point
        return None

    async def _fire() -> None:
        try:
            result: CatchUpResult = await catch_up.catch_up(scope=CatchUpScope.DEFERRED)
            _log.info("post_arm_jobs_complete", jobs=list(POST_ARM_JOB_IDS),
                      caught_up=result.jobs_caught_up, failed=result.jobs_failed,
                      skipped_in_flight=result.skipped_in_flight)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - these jobs are never entry-blocking
            _log.exception("post_arm_jobs_failed")

    _log.info("post_arm_jobs_fired", jobs=list(POST_ARM_JOB_IDS))
    task = asyncio.create_task(_fire(), name="post_arm_catchup")
    # Release the early-hydration hook only AFTER the one-shot is dispatched. Both use the same
    # single-flighted CatchUpRunner: released first, the hook takes the pass lock and this one-shot
    # degrades to a `skipped_in_flight` no-op (the deferred chain would wait for its 08:25 clock).
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
    scheduler: Scheduler, registry: JobRegistry, catch_up: CatchUpRunner, clock: Clock,
    *, lane: CompactionLane | None = None,
) -> None:
    """Arm every registry job on the live scheduler. Each fire records the ``job_runs``
    watermark so the CatchUpRunner never re-runs a job the live scheduler already ran today.

    With ``lane``, a :data:`LANE_JOB_IDS` fire starts a lane pass instead of running its job: the
    lane's single-flight pass is then the only runner of those jobs, so the 22:00 export can never
    overlap a sweep's lane pass or the 22:30 compaction."""
    for spec in registry.specs():
        if lane is not None and spec.job_id in LANE_JOB_IDS:
            fire = _lane_fire(lane, spec.job_id, clock)
        else:
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


def _lane_fire(lane: CompactionLane, job_id: str, clock: Clock):
    """The scheduled fire of a lane job: start a lane pass, whose runner finds the job due and records
    its watermark. A pass already in flight (or the no-run window veto) starts nothing; the next sweep's
    pass picks the job up. Only this fire's own run is quiet: the runner's ``job_id:<today>`` entry.
    Missed days the pass also replays still report."""
    async def _fire() -> None:
        quiet = frozenset({f"{job_id}:{clock.today().isoformat()}"})
        started = lane.spawn(f"scheduled:{job_id}", quiet=quiet) is not None
        _log.info("scheduled_lane_fire", job_id=job_id, started=started)

    return _fire


def _arm_token_check(scheduler: Scheduler, job: TokenCheckJob) -> None:
    """Arm the pre-open token probe — one calendar-guarded daily fire at 08:40 IST.

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
    """The live-scheduler wrapper: run the job, then record its ``job_runs`` watermark.

    The fire is UNCONDITIONAL even when today's watermark already exists: skipping would pin the 06:30
    early-hydration digest/planner, and a universe built on a pre-08:00 instruments dump, for the whole
    day (see :data:`EARLY_HYDRATION_JOB_IDS`). The watermark ``CatchUpRunner.hydrate_ahead`` records
    guards only the SLEEP case.

    An unfinished run (:func:`~engine.ops.jobs._job_unfinished`) records nothing: the day stays missed
    and the next catch-up pass replays it. A day ``spec.fire_day`` excludes runs nothing and records
    nothing.
    """
    async def _fire() -> None:
        today = clock.today()
        if spec.fire_day is not None and not spec.fire_day(today):
            _log.info("scheduled_job_not_fire_day", job_id=spec.job_id, d=today.isoformat())
            return
        try:
            if spec.job_class == JobClass.DATE_KEYED:
                outcome = await spec.run(today)  # type: ignore[call-arg]
            else:
                outcome = await spec.run()       # type: ignore[call-arg]
            if _job_unfinished(outcome):
                _log.info("scheduled_job_unfinished", job_id=spec.job_id)
                return
            if not _job_result_ok(outcome):
                # degraded return = failure for the watermark; the job already alerted its own
                # failure — no new alert from the runner.
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
    scoring_tick=None, warmup_refresh=None, catchup_sweep=None,
    window_sweep_tick=None, forward_drain_tick=None, holdings_reconcile_tick=None,
    protection_reminder_tick=None, paper_tick=None,
) -> None:
    """Interval jobs that run continuously while the engine is up (not calendar-gated): the coarse
    bar-finalization timer, the state-aware health check, the per-feed news poll cadences,
    and the periodic in-session ``feed_stats`` observability line."""

    async def _advance_bars() -> None:
        bar_builder.advance()

    async def _health() -> None:
        await health.check(check_skew=False)

    async def _feed_stats() -> None:
        # In-session only (no all-night noise). One INFO line to see AT A GLANCE whether ticks/bars flow.
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
            # late_ticks >> late_store_calls is the in-memory range check absorbing them (healthy); both
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
    # One job per configured RSS feed: the feed set is settings, not code, so adding a
    # source is a config_audit'd settings.yaml edit and the scheduler picks it up at the next boot.
    for name, feed in settings.news.feeds.rss.items():
        scheduler.add_job(_news_poll(name), trigger=IntervalTrigger(seconds=feed.poll_s),
                          job_id=f"news_poll_{name}", guard=False)
    scheduler.add_job(_news_poll("gdelt"), trigger=IntervalTrigger(seconds=settings.news.gdelt_poll_s),
                      job_id="news_poll_gdelt", guard=False)
    # The exchange-announcements feed keeps its own cadence and off switch: disabled ⇒ never armed.
    nse_ann = settings.news.feeds.nse_announcements
    if nse_ann.enabled:
        scheduler.add_job(_news_poll(NSE_ANN_KEY), trigger=IntervalTrigger(seconds=nse_ann.poll_s),
                          job_id=f"news_poll_{NSE_ANN_KEY}", guard=False)
    if equity_tick is not None:
        # Platform equity persisted each minute + the halt ladder evaluated on every persist.
        scheduler.add_job(equity_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="equity_tick", guard=False)
    if scoring_tick is not None:
        # Cadence pulse — run_batch() self-gates (windows, batch trigger, governor).
        scheduler.add_job(scoring_tick, trigger=IntervalTrigger(seconds=300),
                          job_id="news_scoring_tick", guard=False)
    if warmup_refresh is not None:
        # Gate readiness snapshot (warmup_ready/regime_data_ready) — fail-closed until first run.
        scheduler.add_job(warmup_refresh, trigger=IntervalTrigger(seconds=60),
                          job_id="warmup_status_refresh", guard=False)
    if catchup_sweep is not None:
        # Missed-job sweep for sleep/resume gaps (watermark-deduped ⇒ idempotent; see wiring note).
        scheduler.add_job(catchup_sweep, trigger=IntervalTrigger(seconds=1800),
                          job_id="catchup_sweep", guard=False)
    if window_sweep_tick is not None:
        # Fire the scanner sweep on the trade-window INACTIVE→ACTIVE edge.
        scheduler.add_job(window_sweep_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="window_sweep_tick", guard=False)
    if forward_drain_tick is not None:
        # One-minute pulse; the pipeline's FORWARD_PACING_MIN cadence gates the drain.
        scheduler.add_job(forward_drain_tick, trigger=IntervalTrigger(seconds=60),
                          job_id="forward_drain_tick", guard=False)
    if holdings_reconcile_tick is not None:
        # Hourly; the tick self-gates on the trading day and the 09:20–15:30 window
        # (holdings_reconcile.in_reconcile_window).
        scheduler.add_job(holdings_reconcile_tick, trigger=IntervalTrigger(seconds=3600),
                          job_id="holdings_reconcile", guard=False)
    if protection_reminder_tick is not None:
        # Self-gates on the reminder window; not calendar-gated (a late-night /taken).
        scheduler.add_job(protection_reminder_tick, trigger=IntervalTrigger(seconds=300),
                          job_id="protection_reminder_tick", guard=False)
    if paper_tick is not None:
        # Armed only when the paper runtime exists; gated on its session prep.
        scheduler.add_job(paper_tick, trigger=IntervalTrigger(seconds=PAPER_TICK_S),
                          job_id="paper_tick", guard=False)


# --------------------------------------------------------------------------- warm-up lift
async def _reconcile_catchup_freeze(result: CatchUpResult, latch, kill) -> None:
    """``catchup_safety_jobs`` symmetry on the SWEEP path, so a boot-latched freeze does not survive a
    mid-session recovery.

    Same rule as the lifecycle's catch-up step: a pass with safety-critical failures latches, a CLEAN
    pass (the re-verification of exactly this cause's predicate) clears. A ``skipped_in_flight`` pass
    verified nothing and leaves the latch alone; so does a killed engine. The clear fires only when
    the cause is ACTIVE — ``clear_cause`` logs every call, so an unconditional clear adds no-op
    WARNING lines."""
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

    Safe at ANY time, including mid-``lifecycle.startup()`` (the boot ticks): a mid-boot LIFT races the
    catch-up's safety-critical freeze (``_maybe_lift_warmup_freeze`` clears causes the running recovery
    believes are set) and a mid-boot REPAIR chases a tail hole the not-yet-started ticker regrows; both
    belong on the post-boot scheduler cadence. A refresh failure keeps the PREVIOUS snapshot (holder
    untouched ⇒ stays fail-closed). Returns the status, or None."""
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

    The lift hangs off this cadence, not the post-login hook, because a VALID-TOKEN mid-session restart
    fires no login event. Runs on the 60 s ``warmup_status_refresh`` cadence; ``reapply_warmup_gate``
    resolves through the cause latch (never a blanket NORMAL write) and is a no-op unless the state is
    FROZEN with the FREEZING classes (regime ∪ unattributable) covered; the INTRADAY and DAILY classes
    never block it.

    ``alert`` is the owner channel for the NON-FREEZING classes' transitions (best-effort, once per
    transition, one notice per class): neither reaches the risk state nor a WARMUP_FROZEN page, so a
    hole opening MID-SESSION would otherwise be visible only in the structured log. Unwired ⇒ log-only.
    ``clock`` scopes the transition memos to the IST day (unwired ⇒ per-process).
    """
    status = await refresh_warmup_snapshot(warmup_gate, warmup_holder)
    if status is None:
        return
    # The lift is owed to the REGIME class (plus unattributable blockers), not to a flat ``ready``. One
    # symbol's intraday or daily hole no longer holds the book frozen for the session; the gate and the
    # pre-screen refuse the short symbols on their own, per candidate.
    if _freezing_classes_ready(status) and mode.risk_state() == RiskState.FROZEN:
        try:
            await lifecycle.reapply_warmup_gate()
        except Exception:  # noqa: BLE001 - a failed lift retries on the next 60s tick
            _log.exception("warmup_freeze_lift_failed")
    # The class notices come AFTER the lift so that "entries are NOT frozen by warm-up" is said of
    # the state the owner is now in, not of the one a few lines above.
    today = clock.today() if clock is not None else None
    for cls in (CLASS_INTRADAY, CLASS_DAILY):
        notice = _log_class_transition(warmup_holder, status, cls, today=today)
        if notice is None or alert is None:
            continue
        try:
            await alert(notice[0], notice[1])
        except Exception:  # noqa: BLE001 - the owner notice is best-effort
            _log.exception("warmup_class_alert_failed", cls=cls)


def _freezing_classes_ready(status) -> bool:
    """Are the FREEZING coverage classes satisfied? That is REGIME alone — an unattributable blocker is
    handled INSIDE ``ready_for`` (it holds every class down). Duck-typed: a status without
    ``ready_for`` (a fake) falls back to the flat ``ready``, never looser."""
    ready_for = getattr(status, "ready_for", None)
    if ready_for is None:
        return bool(getattr(status, "ready", False))
    return bool(ready_for(CLASS_REGIME))


#: Per-class wording for :func:`_log_class_transition`. ``{detail}`` is the parenthesised sample; ``{n}``
#: the symbol count.
_CLASS_NOTICE_SHORT = {
    CLASS_INTRADAY: ("warm-up: INTRADAY coverage short{detail} — intraday candidates are "
                     "refused one by one; entries are NOT frozen by warm-up and the daily-bar legs "
                     "are unaffected"),
    CLASS_DAILY: ("warm-up: DAILY coverage short for {n} symbol(s){detail} — swing/position "
                  "candidates for those symbols are refused one by one; entries are NOT frozen by "
                  "warm-up (per-symbol since 2026-09-17)"),
}
_CLASS_NOTICE_RESTORED = {
    CLASS_INTRADAY: "warm-up: INTRADAY coverage restored — intraday candidates are accepted again",
    CLASS_DAILY: "warm-up: DAILY coverage restored — swing/position candidates are accepted again",
}


def _sample_detail(items: list[str]) -> str:
    """``" (A, B, C, +k more)"`` from the first three items; ``""`` for an empty list — "(…)" with
    nothing in it reads as a formatting bug rather than as coverage."""
    if not items:
        return ""
    more = f", +{len(items) - 3} more" if len(items) > 3 else ""
    return f" ({', '.join(items[:3])}{more})"


def _log_class_transition(warmup_holder: dict, status, cls: str, *, today=None) -> tuple[str, str] | None:
    """Log ONE non-freezing class's readiness ONCE PER TRANSITION and return the owner notice to
    send, as ``(severity, message)``, or None when there is nothing new to say.

    The DAILY class does not freeze entries, so a market-wide daily hole (the nightly ``daily_bars``
    job failed) reaches a human only through this notice and the STARTUP_REPORT ``warm-up short:`` line.

    This runs on the 60 s cadence, so the memos (``{cls}_short`` / ``{cls}_short_day``) in the snapshot
    holder suppress repeats; they are per-process (a restart logs the standing state once more) and,
    with ``today``, per IST DAY — the intraday class reads trivially "ready" whenever there is no
    session yet (``WarmupGate._missing_intraday`` returns nothing before the open and on a non-trading
    day), so a memo surviving midnight would manufacture a "coverage restored" no repair produced.

    **Only non-freezing classes are judged here.** While a FREEZING class (regime ∪ unattributable)
    is short, entries ARE frozen and ``WARMUP_FROZEN`` owns that state; "entries are NOT frozen" would
    be false, so nothing is said and the memo is left alone — the shortfall is reported on the first
    tick after the freeze lifts. (``ready_for(cls)`` is False in that compound state too, because an
    unknown-class blocker holds every class down.)

    The FIRST observation of a ready class is a transition for the log but NOT a notice (every clean
    boot would page "coverage restored"). A first observation of a SHORT one IS a notice."""
    ready_for = getattr(status, "ready_for", None)
    if ready_for is None:
        return None
    if not _freezing_classes_ready(status):
        return None
    memo, day_memo = f"{cls}_short", f"{cls}_short_day"
    if today is not None and warmup_holder.get(day_memo) != today:
        warmup_holder[day_memo] = today
        warmup_holder.pop(memo, None)
    short = not ready_for(cls)
    seen = warmup_holder.get(memo)
    if seen is short:
        return None
    warmup_holder[memo] = short
    # With the freezing classes covered, whatever holds this class down is its own bucket.
    by_class = getattr(status, "blockers_by_class", {}) or {}
    blockers = list(by_class.get(cls, []))
    short_symbols_fn = getattr(status, "short_symbols", None)
    symbols = list(short_symbols_fn(cls)) if short_symbols_fn is not None else []
    if short:
        # A status that says short without rendering a blocker still gets a notice — the owner needs
        # the state.
        if cls == CLASS_DAILY:
            _log.warning("warmup_daily_not_ready", symbols=symbols[:8], count=len(symbols),
                         note="swing/position candidates refused per symbol; warm-up freezes "
                              "nothing for this")
            detail = _sample_detail(symbols)
        else:
            _log.warning(f"warmup_{cls}_not_ready", blockers=blockers[:8], count=len(blockers),
                         note="intraday candidates refused per-candidate; warm-up freezes nothing "
                              "for this")
            detail = _sample_detail(blockers)
        return ("warning", _CLASS_NOTICE_SHORT[cls].format(detail=detail, n=len(symbols)))
    _log.info(f"warmup_{cls}_ready")
    if seen is None:
        return None
    return ("info", _CLASS_NOTICE_RESTORED[cls])


def _log_intraday_class_transition(warmup_holder: dict, status, *, today=None) -> tuple[str, str] | None:
    """The INTRADAY class's transition notice; see :func:`_log_class_transition`."""
    return _log_class_transition(warmup_holder, status, CLASS_INTRADAY, today=today)


# --------------------------------------------------------------------------- bounded news resolve
#: The chain is never-load-bearing, so a bounded skip + alert strictly beats a wedged boot. Budget =
#: ~6× the measured worst case (a weekend backlog clusters in ~90 s) plus store hops.
_NEWS_RESOLVE_TIMEOUT_S = 600.0


async def resolve_news_bounded(lock: asyncio.Lock, chain, *, timeout_s: float = _NEWS_RESOLVE_TIMEOUT_S,
                               on_timeout=None) -> bool:
    """Run one news resolve pass under the chain lock with a hard deadline.

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
    """Run the two OBSERVATION-ONLY refreshes on a cadence DURING boot recovery.

    The scheduler (owner of ``warmup_status_refresh`` and ``health_check``) starts only after
    ``lifecycle.startup()`` returns, and the catch-up inside it can run long: without this the box has
    no health/keep-awake pulse and a stale warm-up snapshot for that window. DELIBERATELY
    observation-only: the callables must be :func:`refresh_warmup_snapshot` (never the
    lifting/repairing ``warmup_refresh`` — a mid-boot lift races the running recovery's cause state, a
    mid-boot repair burns the day's repair budget) and the health check (whose ``keep_awake.update()``
    matters on a sleep-prone box). The lift loses nothing by waiting: the composition root calls the
    full ``warmup_refresh()`` right after startup returns. Spawned right before recovery, cancelled
    (and awaited) when the scheduler takes over. Sleep-first: a normal boot never fires a tick. One
    tick's failure never ends the loop (fail-open on observation, fail-closed on state)."""
    while True:
        await asyncio.sleep(interval_s)
        try:
            await refresh()
            await health_check()
        except Exception:  # noqa: BLE001 - observation must keep ticking; CancelledError still propagates
            _log.exception("boot_tick_failed")


# --------------------------------------------------------------------------- boot tail hardening
#: Budget for the two pre-arm seeding refreshes (warm-up snapshot + first health pulse). Generous
#: (they take seconds): the point is a CEILING on a wedged boot, not trimming a slow one.
_BOOT_SEED_TIMEOUT_S = 45.0


async def seed_boot_snapshots(
    warmup_refresh, health_check, *, timeout_s: float = _BOOT_SEED_TIMEOUT_S, alert=None
) -> bool:
    """Seed the warm-up + health snapshots before arming — under a deadline that CANNOT be missed.

    Invariant: **the boot tail reaches ``scheduler.start()`` no matter what the seeding does.** A
    ``warmup_refresh()`` that never returns would leave the engine with no timers at all.

    Not ``asyncio.wait_for``: it cancels the inner task and then AWAITS the cancellation, so a
    coroutine parked on a thread-offload that will not come back can wedge the timeout too.
    :func:`asyncio.wait` returns after ``timeout_s`` unconditionally — it neither cancels nor joins.
    The straggler is cancelled afterwards and deliberately never awaited; the 60 s
    ``warmup_status_refresh`` / ``health_check`` interval jobs re-do the work once the scheduler is up.

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
        hint="warm-up/health seeding did not return (2026-08-24); arming the scheduler anyway "
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


# --------------------------------------------------------------------------- boot contract
#: Arming deadline, measured from the END of startup recovery: seeding (capped at 45 s) +
#: scheduler.start() + engine_ready take well under this.
_BOOT_CONTRACT_DEADLINE_S = 180.0

#: Startup-recovery ceiling, measured from watchdog start: catch-up + backfills legitimately take
#: over 20 minutes (a slow BSE scrape); a 420 s ceiling paged falsely.
_BOOT_STARTUP_CEILING_S = 3600.0

#: Re-check/re-log cadence while the contract stands violated.
_BOOT_CONTRACT_RECHECK_S = 300.0

#: How often the contract is read.
_BOOT_CONTRACT_POLL_S = 5.0


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
    startup_ceiling_s: float = _BOOT_STARTUP_CEILING_S,
    recheck_s: float = _BOOT_CONTRACT_RECHECK_S,
    poll_s: float = _BOOT_CONTRACT_POLL_S,
) -> None:
    """Verify the boot actually COMPLETED, and page the owner when it did not.

    Guards a boot that logs ``startup_complete`` and processes ticks while APScheduler was never
    started, so no scheduled job (including anything that could notice) ever fires again.

    So this is a PLAIN asyncio task, armed at the top of :func:`run` and owned by nobody else. It must
    keep working when the scheduler is the broken component, which rules out an APScheduler job; the
    owner-notify path is likewise an asyncio send, independent of any trigger.

    Contract: ``engine_ready`` AND ``Scheduler.is_running()`` True — read from APScheduler's own
    state, never from a flag the boot path sets (see :meth:`engine.ops.scheduler.Scheduler.is_running`).
    Polled every ``poll_s``; satisfied at any poll ⇒ one INFO ``boot_contract_ok`` and the task
    retires. Two phases decide when an unmet contract is DUE: ``arming`` — ``deadline_s`` after
    startup recovery returned (``boot_state["startup_done_at"]``); ``startup`` — startup recovery
    itself has not returned ``startup_ceiling_s`` after the watchdog began. Due and unmet ⇒ CRITICAL
    ``boot_incomplete`` (with ``phase``), ONE owner page per phase, then a re-log every ``recheck_s``
    until it resolves; a paged owner gets an all-clear when it does.

    Never crashes the boot: it is detached, and every failure inside it — including a raising alert
    sink — is swallowed. Cancelled at shutdown via :func:`cancel_post_arm`.
    """
    started = time_module.monotonic()
    paged_phases: set[str] = set()
    last_logged: float | None = None
    try:
        while True:
            now = time_module.monotonic()
            missing = _boot_contract_missing(boot_state)
            elapsed = round(now - started, 1)
            if not missing:
                _log.info("boot_contract_ok", elapsed_s=elapsed)
                alert = boot_state.get("alert")
                if paged_phases and alert is not None:   # a paged owner hears that it resolved
                    try:
                        await alert("info", f"BOOT COMPLETED after {elapsed / 60:.0f} min — the "
                                            "engine is ready and scheduled jobs run again.")
                    except Exception:  # noqa: BLE001 - the all-clear is best-effort
                        _log.exception("boot_contract_ok_alert_failed")
                return
            startup_done_at = boot_state.get("startup_done_at")
            phase: str | None = None
            if startup_done_at is not None:
                if now - startup_done_at >= deadline_s:
                    phase = "arming"
            elif now - started >= startup_ceiling_s:
                phase = "startup"
            if phase is not None and (
                phase not in paged_phases
                or last_logged is None
                or now - last_logged >= recheck_s
            ):
                last_logged = now
                if phase == "arming":
                    hint = ("the boot never finished arming (2026-08-24): no scheduled job can "
                            "fire in this state — restart the engine")
                    text = (f"BOOT INCOMPLETE after {elapsed:.0f}s — missing: {', '.join(missing)}. "
                            "No scheduled job (forward drains, health pulses, EOD jobs) can fire. "
                            "Restart the engine.")
                else:
                    hint = ("startup recovery has not returned: wait while engine.log shows catch-up "
                            "or backfill progress (a restart repeats it); restart if it has gone quiet")
                    text = (f"BOOT STILL IN STARTUP RECOVERY after {elapsed / 60:.0f} min — startup "
                            "has not returned, so no scheduled job (forward drains, EOD jobs) can "
                            "fire yet. If engine.log still shows catch-up or backfill progress, wait: "
                            "a restart repeats it. If the log has gone quiet, restart the engine.")
                _log.critical("boot_incomplete", missing=missing, elapsed_s=elapsed,
                              phase=phase, hint=hint)
                if phase not in paged_phases:
                    paged_phases.add(phase)   # ONE page per phase; the CRITICAL log carries the repeats
                    alert = boot_state.get("alert")
                    if alert is not None:
                        try:
                            await alert("critical", text)
                        except Exception:  # noqa: BLE001 - a failed page must not end the watchdog
                            _log.exception("boot_incomplete_alert_failed")
            await asyncio.sleep(poll_s)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - the watchdog must never take the boot down with it
        _log.exception("boot_contract_watchdog_failed")


# --------------------------------------------------------------------------- warm-up gap self-repair
#: One repair attempt per this window, and at most _GAP_REPAIR_MAX_REQUESTS_PER_DAY Kite REQUESTS
#: per session day: a hole the fill cannot close will never close by retrying harder, so broker
#: spend must be capped; local-only scans are free. 900 = 3 sweeps x a 300-symbol watchlist (see
#: maybe_repair_warmup_gaps).
_GAP_REPAIR_COOLDOWN_S = 300
_GAP_REPAIR_MAX_REQUESTS_PER_DAY = 900


def _is_intraday_gap_blocker(blocker: str) -> bool:
    """One blocker line of the intraday minute-bars shape (``orb:<sym> bars have/need``)."""
    return blocker.startswith("orb:") and " bars " in blocker


def _has_intraday_gap_blockers(blockers: list[str]) -> bool:
    """The intraday minute-bars blocker shape (``orb:<sym> bars have/need``) — the only class the
    warmup_gap re-backfill can heal; daily-bars blockers belong to the daily_bars job/catchup_sweep."""
    return any(_is_intraday_gap_blocker(b) for b in blockers)


async def maybe_repair_warmup_gaps(status, state: dict, *, clock, calendar, repair,
                                   token_valid=None,
                                   max_requests_per_day: int = _GAP_REPAIR_MAX_REQUESTS_PER_DAY) -> bool:
    """Re-trigger the warm-up gap backfill when intraday coverage holes persist (a login-lagged boot can
    leave a permanent seam hole warm-up could never lift past).

    Bounds: in-session only; NEVER on an invalid token (a doomed historical call must not fire, nor
    spend the repair budget pre-login); ``to`` is trimmed 2 minutes back from ``now`` so the repair
    never touches minutes the live bar builder still owns and a transient just-closed-minute deficit
    sees ZERO gaps — the fill's per-symbol gap check then skips every symbol without a broker call.

    The budget is denominated in KITE REQUESTS, not attempts: it bounds BROKER SPEND, and an attempt
    is not a unit of spend (a market-wide sweep and a one-symbol confirmation differ ~300×).
    Charging: each completed per-symbol span is one Kite request for its minute interval (spans are
    ≤60 days, so one span is one call) and a broker-failure span also cost a request; a RAISING
    repair charges ``max(1, intraday blockers)`` because we cannot know how far it got (spend-safe);
    ``unknown_instrument_token`` spans (recorded pre-network) and pure local scans are free and only
    paced by :data:`_GAP_REPAIR_COOLDOWN_S`. ``state["attempts"]`` counts every fired attempt for the
    logs and bounds nothing. Returns True iff a repair fired (productive or not); the NEXT 60 s
    ``warmup_status_refresh`` tick observes healed coverage and lifts through the normal path — this
    function never lifts anything itself."""
    if status.ready or repair is None or not _has_intraday_gap_blockers(status.blockers):
        return False
    now = clock.now()
    today = now.date()
    session = calendar.session(today)
    if session is None or not (session.open <= now <= session.close):
        return False
    if state.get("day") != today:
        state.update(day=today, requests=0, attempts=0, last=None, exhausted_logged=False,
                     no_token_logged=False)
    if token_valid is not None and not token_valid():
        if not state.get("no_token_logged"):
            _log.warning("warmup_gap_repair_skipped_no_token", blockers=len(status.blockers))
            state["no_token_logged"] = True
        return False
    state["no_token_logged"] = False
    to = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    if to <= session.open:
        return False                       # session too young to have a repairable window yet
    if state["requests"] >= max_requests_per_day:
        if not state.get("exhausted_logged"):
            _log.warning("warmup_gap_repair_exhausted", requests=state["requests"],
                         attempts=state["attempts"], blockers=len(status.blockers))
            state["exhausted_logged"] = True
        return False
    if state.get("last") is not None and (now - state["last"]).total_seconds() < _GAP_REPAIR_COOLDOWN_S:
        return False
    state["last"] = now
    try:
        report = await repair(session.open, to)
    except Exception:  # noqa: BLE001 - a failed repair leaves the gate blocking (fail closed); the cooldown paces retries
        # Spend-safe: an erroring attempt got an unknown distance into its sweep, so it is charged
        # one request per symbol it could have touched (never less than one).
        charged = max(1, sum(1 for b in status.blockers if _is_intraday_gap_blocker(b)))
        state["attempts"] += 1
        state["requests"] += charged
        _log.exception("warmup_gap_repair_failed", attempt=state["attempts"], requests=charged,
                       requests_today=state["requests"])
        return True
    # Budget charges on BROKER SPEND, not on bars landed: each ``fetched`` span is one completed
    # historical call — an UNFILLABLE hole (no candle upstream) is charged like a productive one rather
    # than resweeping the broker every cooldown. Real broker failures also charge (spend-safe);
    # ``unknown_instrument_token`` spans are recorded pre-network (a broken instruments map —
    # post-login's repair, not ours) and are deliberately free.
    fetched = getattr(report, "fetched", ()) or ()
    broker_failures = [
        f for f in (getattr(report, "failed", ()) or ())
        if getattr(f, "error", "") != "unknown_instrument_token"
    ]
    requests = len(fetched) + len(broker_failures)
    state["attempts"] += 1
    if requests:
        state["requests"] += requests
        _log.warning("warmup_gap_repair", attempt=state["attempts"],
                     bars_written=getattr(report, "bars_written", 0), fetched=len(fetched),
                     failed=len(broker_failures), requests=requests,
                     requests_today=state["requests"], blockers=len(status.blockers))
    else:
        # No broker call happened: zero gaps in the trimmed window (the just-closed-minute
        # transient) or instruments-map misses only. Free — paced by the cooldown alone.
        _log.info("warmup_gap_repair_scan_only", blockers=len(status.blockers))
    return True


# --------------------------------------------------------------------------- prescreen day-state
def _hydrate_prescreen(conn: sqlite3.Connection, prescreen, today: date) -> None:
    """Rebuild the prescreen's in-memory day state from ``prescreen_day_slots`` at boot.

    ``charged`` = every pair published today (the caps bound — counts attempts, never refunded);
    ``seen`` = pairs whose slot is spent (``evaluated=1``). A charged-but-unseen pair was lost
    in flight (or re-armed) and may re-publish within its already-paid quota.

    ``catalyst_strategies`` lets the pre-screen rebuild the ``catalyst_guard.max_catalyst_entries_day``
    budget from the same rows; the journal has no ref column, so the reconstruction and its one
    fail-open direction are documented at :meth:`~engine.strategy.prescreen.SignalPreScreen.hydrate`."""
    rows = conn.execute(
        "SELECT symbol, strategy_id, evaluated FROM prescreen_day_slots WHERE d = ?",
        (today.isoformat(),),
    ).fetchall()
    charged = {(r["symbol"], r["strategy_id"]) for r in rows}
    seen = {(r["symbol"], r["strategy_id"]) for r in rows if r["evaluated"]}
    prescreen.hydrate(today, seen=sorted(seen), charged=sorted(charged),
                      catalyst_strategies=CATALYST_STRATEGY_IDS)
    _log.info("prescreen_hydrated", d=today.isoformat(), charged=len(charged), seen=len(seen))


# --------------------------------------------------------------------------- `ins` pending queue
def _read_ins_pending(conn: sqlite3.Connection, today: date) -> list[ins.Crossing]:
    """Today's UNCONSUMED ``ins_pending`` rows (migration 0008) as :class:`ins.Crossing` tuples.

    The EOD ``ins_crossings`` job wrote these last night keyed on ``for_session`` = today. Money
    columns are TEXT and are re-hydrated to ``Decimal`` here (money never round-trips through a
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

    ``now`` comes from the engine ``Clock`` (never ``datetime.now()``). Rows are never deleted:
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


# --------------------------------------------------------------------------- brk20 regime gate
def _brk20_regime_gate(
    frame: pd.DataFrame, calendar: NSECalendar, today: date
) -> tuple[bool, regime.IndexRegime | None, str | None]:
    """(keep, regime, reason) for today's brk20 crossings, judged at the crossing session
    (:func:`regime.session_regime`); reason ``"down"`` when the regime is defined and not up."""
    idx, reason = regime.session_regime(frame, calendar, today)
    if idx is None:
        return False, None, reason
    return (True, idx, None) if idx.up else (False, idx, "down")


def _journal_regime_blocked(
    conn: sqlite3.Connection, blocked: Sequence[SignalCandidate], idx: regime.IndexRegime | None,
    reason: str, signal_d: date, *, now: datetime,
) -> None:
    """Upsert the crossings the regime gate blocked into ``brk20_regime_blocked`` (the ``rule_blocked`` arm); a later
    sweep refreshes the reason and index figures unless the stored reason is already ``down``.

    Loop thread only, for the reason ``RestingLevelBook.record`` is; money as TEXT in its format."""
    try:
        conn.executemany(
            "INSERT INTO brk20_regime_blocked (symbol, signal_d, entry, stop, target, score, "
            "index_close, sma50, sma50_then, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (symbol, signal_d) DO UPDATE SET reason = excluded.reason, "
            "index_close = excluded.index_close, sma50 = excluded.sma50, sma50_then = excluded.sma50_then "
            "WHERE brk20_regime_blocked.reason != 'down'",
            [(c.symbol, signal_d.isoformat(), str(c.raw_levels.entry),
              None if c.raw_levels.stop is None else str(c.raw_levels.stop),
              None if c.raw_levels.target is None else str(c.raw_levels.target), float(c.score),
              None if idx is None else idx.close, None if idx is None else idx.sma,
              None if idx is None else idx.sma_then, reason, now.isoformat())
             for c in blocked],
        )
    except Exception as exc:  # noqa: BLE001 - journalling is bookkeeping; it never kills the sweep
        _log.warning("brk20_regime_blocked_journal_failed", d=signal_d.isoformat(), error=str(exc))


# --------------------------------------------------------------------------- `cat` watchlist read
#: Calendar days to look back for the prior session's daily bar. Comfortably clears the longest NSE
#: holiday stretch (a long weekend plus a mid-week holiday); no bar in that span means the symbol has
#: no usable committed price, which the scanner turns into no candidate.
_CAT_REF_CLOSE_LOOKBACK_DAYS = 10


#: Strategies in SHADOW mode: the gate's cost check rejects them UNCONDITIONALLY, so their signals
#: accumulate a validation population and can never become a recommendation (see ``risk/gate.py``
#: :data:`~engine.risk.gate._SHADOW_NO_EDGE` for why this is declared rather than inferred from a
#: missing ``expected_edge_pct``). A module-level constant so the property is assertable without
#: booting the engine — a wiring that only exists inside ``compose`` is a wiring that silently
#: disappears. Removing an id from here is the owner promotion gate, never a refactor.
#:
#: ``cat`` is here because an analyst-volunteered ``target_price`` would defeat the indirect argument
#: "ship no ``expected_edge_pct`` and rely on the cost check's targetless branch" (see
#: ``cat_reversal``'s docstring).
#: ``hi52`` is NOT here: it is a FORWARD TEST, not a validated edge (its backtest edge lives in an
#: index cell labelled by CURRENT membership applied backwards), with a mechanical kill rule
#: (``scripts/hi52_forward_verdict.py``: n >= 20 signals AND (median net at T+20 <= 0 OR T+20 hit
#: rate < 50%) ⇒ DEMOTE). DEMOTION IS THIS LINE: re-add ``hi52.STRATEGY_ID`` here AND delete
#: ``hi52.expected_edge_pct`` from settings.yaml — either alone leaves a half-promoted rule (the shadow
#: set wins a contradiction, but a dangling edge key reads as a live registration). ``Hi52Cfg
#: .expected_edge_pct`` defaults to ``None``, so deleting the key un-registers the edge in ``compose``.
NO_EDGE_SHADOW_STRATEGIES: frozenset[str] = frozenset(
    {cat.STRATEGY_ID, cat_reversal.STRATEGY_ID}
)


def _strategy_expected_edge_pct(settings: Settings) -> dict[str, Decimal]:
    """The per-strategy edge registrations the gate is built with — the OTHER half of the
    promotion state above, extracted so both halves are assertable without booting an engine.

    ``ins``: the cost-gate edge check derives the expected move from the proposal's TARGET, and ``ins``
    has none by design (its exit is the 20-td time cap, and the drift it captures was MEASURED, not
    predicted). Rather than invent a target, the strategy registers its validated T+20 NET drift
    (owner-set in ``settings.yaml``, never learner-movable). Used ONLY when ``target_price`` is None.

    ``hi52``: registered on the SAME seam for the same reason (20-session time cap, MEASURED drift,
    ``target=None``). Its number is a FORWARD-TEST edge, not a validated one (``settings.yaml`` carries
    the derivation and the kill rule). CONDITIONAL on the key being PRESENT, unlike ``ins``'s:
    ``Hi52Cfg.expected_edge_pct`` defaults to ``None`` so that deleting the ``settings.yaml`` key — the
    second demotion edit — actually demotes; registering off a model default would make that edit a
    no-op. A targetless proposal with no registered edge is the gate's ordinary ``_NO_TARGET`` reject,
    so the absence fails closed.
    """
    edges = {ins.STRATEGY_ID: Decimal(str(settings.ins.expected_edge_pct))}
    if settings.hi52.expected_edge_pct is not None:
        edges[hi52.STRATEGY_ID] = Decimal(str(settings.hi52.expected_edge_pct))
    return edges


def _limits_capital_base(limits_engine: LimitsEngine) -> Callable[[], Decimal]:
    """The real tracker's base, read through the verified limit table on every call (``load()`` is cached). ``IntegrityError`` ONLY returns
    :data:`_FALLBACK_CAPITAL_BASE_INR` — the boot is FROZEN then — logged once per process: an
    unverified table is never cached, so the next call after a reseed reads the verified base. Any
    other error propagates; a malformed table must never be silently priced at the fallback."""
    warned = False

    def capital_base() -> Decimal:
        nonlocal warned
        try:
            return limits_engine.load().capital_base_inr
        except IntegrityError:
            if not warned:
                warned = True
                _log.warning("limits_unverified_capital_fallback",
                             capital_base=str(_FALLBACK_CAPITAL_BASE_INR))
            return _FALLBACK_CAPITAL_BASE_INR

    return capital_base


def _round_tick_fn(instruments: InstrumentStore) -> Callable[[str, Decimal], Decimal]:
    """``instruments.round_to_tick``, falling back to the scanners' NSE tick (logged) for a symbol
    missing from today's dump."""

    def round_tick(symbol: str, price: Decimal) -> Decimal:
        try:
            return instruments.round_to_tick(symbol, price)
        except UnknownInstrument:
            _log.warning("round_tick_fallback", symbol=symbol)
            return round_to_tick(price)

    return round_tick

#: Strategies whose candidates can carry a ``catalyst_ref`` — the news-originated legs. Read at
#: ONE place: :func:`_hydrate_prescreen`, which uses it to rebuild the
#: ``catalyst_guard.max_catalyst_entries_day`` budget across a restart from a journal that records no
#: refs (see ``SignalPreScreen.hydrate``). Enforcement itself is keyed on the FIELD, never on this
#: set — a strategy id must not be what decides whether the news guard applies — so a leg missing
#: from here still faces the cap while it runs; it only loses budget continuity over a boot.
CATALYST_STRATEGY_IDS: frozenset[str] = frozenset({cat.STRATEGY_ID, cat_reversal.STRATEGY_ID})

#: Sweep triggers that ALSO run the once-per-session legs (today: ``hi52``'s large daily read).
#: ``freeze_lift`` is a ``window_open`` sweep re-run because a freeze swallowed the first one, so it
#: must take every ``window_open`` branch — keying on this set, not the literal, stops the two drifting
#: apart. ``scan_now`` stays out: its cadence is the owner's. Module-level so it is assertable.
_FULL_SWEEP_TRIGGERS: frozenset[str] = frozenset({"window_open", "freeze_lift"})

#: A lift this close behind a FINISHED sweep re-publishes nothing that sweep did not (the pre-screen
#: dedupe holds every pair it already admitted), and every sweep costs a full eligible-universe daily
#: history read. Two minutes covers a lift racing the window-open sweep that it interrupted.
_FREEZE_LIFT_MIN_GAP = timedelta(minutes=2)

#: How long a graceful stop waits for an in-flight freeze-lift sweep before cancelling it. The wait
#: is what keeps a worker thread from outliving `store.close()`; the bound is what keeps a wedged
#: sweep from holding the service stop open.
_SHUTDOWN_LIFT_WAIT_S = 15.0

#: How long a graceful stop waits for event-bus deliveries (mostly tick handlers awaiting a flush)
#: before `store.close()`, which then waits up to 15 s on the same flush. A stop takes 13-20 s, the
#: backup alone 3-11 s; a wedged flush on top can pass the 30 s the service manager allows and end in a
#: kill (the next boot then replays the store's WAL).
_SHUTDOWN_BUS_DRAIN_S = 5.0


@dataclass(frozen=True)
class PaperStack:
    """The composed paper autopilot, built only when ``paper.subsystem_enabled``."""

    clock: Clock
    control: PaperControl
    runtime: PaperRuntime
    orders: OrderManager
    exits: ExitManager
    risk: PaperRisk
    prep: SessionPrep
    reconciler: Reconciler
    ctx_builder: GateContextBuilder
    notifier: PaperNotifier

    def entries_open(self) -> bool:
        """The pipeline's ``paper_enabled_fn``: ``/paper on`` and this session's prep completed."""
        return self.control.enabled() and self.runtime.prep_ready()

    async def submit(self, proposal: EnterAction, verdict: GateVerdict) -> PlatformOrder | None:
        """The pipeline's ``paper_submit``: the corporate-action entry refusal, then the paper
        entry. Unreadable corporate actions refuse too."""
        try:
            refusal = await self.exits.entry_refusal(
                proposal.tradingsymbol, proposal.strategy_id, proposal.style, self.clock.now()
            )
        except Exception:
            _log.exception("paper_entry_refusal_unreadable", proposal_id=proposal.proposal_id)
            refusal = "corporate actions unreadable"
        if refusal is not None:
            _log.warning("paper_entry_refused", proposal_id=proposal.proposal_id,
                         symbol=proposal.tradingsymbol, reason=refusal)
            return None
        return await self.orders.submit(proposal, verdict)

    def status(self) -> dict[str, Any]:
        tracker = self.risk.tracker
        return {"open_positions": tracker.open_position_counts().total, "day_pnl": f"{tracker.day_mtm():.2f}"}

    def counters(self) -> dict[str, int]:
        exits = self.exits.counters
        return {
            "reconcile_mismatches": self.reconciler.counters.mismatches,
            "voids": exits.voids + self.prep.counters.voids,
            "late_corp_actions": exits.late_corp_actions,
        }

    def view(self) -> PaperView:
        """The read-only slice the dashboard API sees (no write-capable manager crosses over)."""
        def entry_guard() -> str | None:
            try:
                self.runtime.guard("entry")
            except PaperOrderBlocked as exc:
                return str(exc)
            return None

        return PaperView(self.runtime.prep_ready, entry_guard, self.runtime.mark, self.counters)

    async def stop(self) -> None:
        parts = (("runtime", self.runtime.stop), ("notifier", self.notifier.tick), ("orders", self.orders.stop))
        for part, stop in parts:
            try:
                await stop()
            except Exception:
                _log.exception("paper_stop_failed", part=part)


async def _compose_paper(
    conn: sqlite3.Connection,
    clock: Clock,
    calendar: NSECalendar,
    bus: EventBus,
    settings: Settings,
    control: PaperControl,
    *,
    real_risk_state: Callable[[], RiskState],
    limits: LimitsEngine,
    tick_size_fn: Callable[[str], Decimal],
    round_tick_fn: Callable[[str, Decimal], Decimal],
    cost_model: CostModel,
    hold_fn: HoldFn,
    store: MarketStore,
    backfill: BackfillJob | None,
    ctx_builder_fn: Callable[[ExposureTracker], GateContextBuilder],
    notify: Callable[[CatalogMessage], Awaitable[None]] = no_notify,
) -> PaperStack | None:
    """Build every paper manager on one PaperBroker and wire them. Never raises: a
    failure leaves nothing subscribed or armed, sends PAPER_ALERT and returns None."""
    corp_actions = store_corp_actions_fn(store)
    pre_ex_mark = store_pre_ex_mark_fn(store)
    notifier = PaperNotifier(conn, clock, notify)
    runtime: PaperRuntime | None = None
    try:
        runtime = PaperRuntime(
            conn, clock, calendar, bus, settings.paper,
            capital_base_fn=lambda: limits.load().capital_base_inr,
            tick_size_fn=tick_size_fn,
            order_guard=paper_order_guard(control.enabled, paper_effective_state(conn, real_risk_state)),
            alert=notifier.alert,
            bars_fn=backfill_bars_fn(backfill, store, calendar),
            replay_fn=exit_sim_replay,
            corp_actions_fn=corp_actions,
            last_mark_fn=store_last_mark_fn(store),
        )
        await runtime.start()
        broker = runtime.broker
        if runtime.degraded or broker is None:
            return None                       # start() alerted and subscribed nothing
        # OrderManager, PositionBook, ProtectionManager and PaperRisk need each other: the callbacks bind late.
        orders = OrderManager(
            conn, clock, broker, order_guard=runtime.guard,
            sell_check=lambda position_id, qty: book.check_sell(position_id, qty),
            on_fill=lambda *fill: protection.on_fill(*fill),
            on_transition=lambda order, event: protection.on_transition(order, event),
            on_reject=lambda _order, at: risk.note_reject(at),
        )
        sessions: dict[str, Any] = {
            "session_of": lambda ts: session_of(calendar, ts), "add_sessions": calendar.add_sessions,
        }
        book = PositionBook(
            conn, clock, broker, orders=orders, hold_fn=hold_fn,
            round_trip_fn=lambda notional, product: cost_model.round_trip(notional, product).total_cost,
            fees_fn=lambda notional, product: (
                (bd := cost_model.round_trip(notional, product)).total_cost - bd.components["spread"]),
            no_stop_strategies=frozenset(settings.paper.no_stop_strategies),
            **sessions,
        )
        _unknown_no_stop_strategies(settings.paper.no_stop_strategies)
        exits = ExitManager(
            conn, clock, broker, orders=orders, book=book, session_fn=calendar.session, hold_fn=hold_fn,
            corp_actions_fn=corp_actions, pre_ex_mark_fn=pre_ex_mark, settings=settings.paper, **sessions,
        )
        protection = ProtectionManager(
            conn, clock, broker, orders=orders, book=book, exit_fn=exits.exit_position,
            notify=notifier.alert, session_fn=calendar.session, ltp_fn=runtime.mark,
            round_tick_fn=round_tick_fn,
            gtt_limit_offset_pct=Decimal(str(settings.recommend.gtt_limit_offset_pct)),
            settings=settings.paper,
        )
        risk = PaperRisk(
            conn, clock, calendar.session, broker, capital_base=lambda: limits.load().capital_base_inr,
            mark_price=runtime.mark, limits_fn=limits.load, flatten=exits.flatten_all,
        )
        prep, reconciler = runtime.attach(
            orders=orders, book=book, protection=protection, exits=exits, pre_ex_mark_fn=pre_ex_mark,
        )
        async def equity_and_fills() -> None:
            try:
                await risk.tick()
            finally:
                await notifier.tick()

        runtime.set_tick_steps(protection=protection.tick, exits=exits.tick, equity=equity_and_fills)
        stack = PaperStack(
            clock, control, runtime, orders, exits, risk, prep, reconciler, ctx_builder_fn(risk.tracker), notifier,
        )
        orders.start(bus)
    except Exception:
        _log.exception("paper_compose_failed")
        if runtime is not None:
            await runtime.stop()              # a restored GTT must not fire with no OMS consumer
        await notifier.alert("construct", "paper construction failed; paper is off until a restart")
        return None
    _log.info("paper_composed")
    return stack


def _held_symbols(conn: sqlite3.Connection) -> list[str]:
    """Open REAL platform/recommended position symbols: the retest skip and the sweep's held flag.
    Paper never counts here; it reaches the ticker through :func:`_feed_symbols`."""
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE state='OPEN' "
        f"AND {scope_sql('real', has_origin=True)}"
    ).fetchall()
    return [str(r["symbol"]) for r in rows]


def _held_positions(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Open REAL platform/recommended positions as ``(position_id, symbol, qty)``: the
    surveillance-migration alert's input. Paper never alerts the owner here."""
    rows = conn.execute(
        "SELECT position_id, symbol, qty FROM positions "
        f"WHERE {HELD_STATES_SQL['real']} AND {scope_sql('real', has_origin=True)} ORDER BY opened_at"
    ).fetchall()
    return [(str(r["position_id"]), str(r["symbol"]), int(r["qty"] or 0)) for r in rows]


def _feed_symbols(conn: sqlite3.Connection) -> list[str]:
    """The ticker's held leg, and nothing else's: real held symbols, then every paper position and
    every working order or ACTIVE GTT. They MUST stay in the feed after the universe drops them —
    an unsubscribed holding marks at avg_entry, so its loss is invisible to the floor ladder and
    day-MTM rungs, and a paper order or GTT only fills on ticks."""
    rows = conn.execute(
        "SELECT symbol FROM positions "
        f"WHERE {HELD_STATES_SQL['paper']} AND {scope_sql('paper', has_origin=True)} "
        "UNION SELECT COALESCE(p.symbol, json_extract(pr.payload, '$.tradingsymbol')) FROM orders o "
        "LEFT JOIN positions p ON p.position_id = o.position_id "
        "LEFT JOIN proposals pr ON pr.proposal_id = o.proposal_id "
        f"WHERE o.state NOT IN {TERMINAL_ORDER_STATES_SQL} "
        "UNION SELECT symbol FROM gtts WHERE UPPER(state) = 'ACTIVE' "
        "ORDER BY 1"
    ).fetchall()
    return [*_held_symbols(conn), *(str(r[0]) for r in rows if r[0])]


def _subscription_tokens(symbols: Iterable[str], token_for_symbol: Callable[[str], int | None]) -> list[int]:
    """Ticker subscription symbols → tokens: FIRST-occurrence dedupe (the caller's order is the
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
    """THE ticker subscription set: watchlist → HELD → TODAY's batch admissions → index → VIX.

    Order is priority order — :func:`_subscription_tokens` keeps the FIRST occurrence, so a batch
    symbol already on the watchlist does not duplicate. The day roll happens HERE, not in the caller,
    so a test exercises it and an ever-growing subscription cannot reach the per-connection cap."""
    return _subscription_tokens(
        [*watchlist, *held, *sorted(_roll_batch_ticks(batch_state, today)),
         INDEX_SYMBOL, VIX_SYMBOL],
        token_for_symbol,
    )


def _roll_batch_ticks(state: dict, today: date) -> set[str]:
    """The batch legs' admitted-symbol set for ``today``, RESET on the day change (midnight
    rollover): yesterday's brk20/hi52/cat/ins admissions are not today's universe, and an
    ever-growing subscription set would walk into the per-connection cap. Mutable — callers
    ``update()`` it in place."""
    if state["day"] != today:
        state["day"], state["symbols"] = today, set()
    return state["symbols"]


#: What a caller hears when its sweep was refused because one is already running. A verdict, never
#: silence: the owner's /scan_now reply must say why no candidate list came back.
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
        _log.info("scan_sweep_queued_behind_in_flight", trigger=trigger)
    async with lock:
        return await sweep(trigger)


def _sweep_window_active(now: datetime, calendar: NSECalendar, mode: ModeManager) -> bool:
    """May the on-demand sweep fire at ``now``? A live session, inside the owner trade window, in a
    mode that originates. ONE definition for both call sites — the INACTIVE→ACTIVE window
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
    """May the brk20 RETEST re-offer a level at ``now``?

    The sweep's window predicate, AND the states in which the pipeline is certain to DROP the
    publication: ``risk_state != NORMAL`` and the kill switch (``pipeline.on_signal_candidate``
    re-arms the pair on either). The sweep may publish into a freeze because the freeze-lift sweep
    re-publishes it; the retest has no lift path and its once-a-day offer bound is spent at OFFER
    time, so an offer into a freeze would silence that level for the day. It also waits while a sweep
    is RUNNING (``sweep_lock.locked()``): a pair that sweep re-armed has an open ``(symbol, strategy)``
    dedupe slot, which a days-old level must not take ahead of today's fresh crossing."""
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

    ``in_flight`` is the single-flight lock's own ``locked()``, never an inference from the phase
    marks: a sweep that RAISES never stamps ``done_at``, so a "running" derived from ``started_at``
    would latch the lift shut (a set-without-clear shape). The lock is released by ``async with`` on
    every exit path, so it is the one signal that cannot stick.

    ``last_sweep["frozen"]`` keeps the third rule from eating its own case: on an in-session boot the
    window_open sweep and the lift that follows it land within the SAME two minutes, and that sweep
    published its whole batch into the freeze. A sweep that published while FROZEN is the one to
    redo, never the one to debounce against."""
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
    """Which ``ins_pending`` rows this sweep may mark consumed — the once-only bound's gate.

    Consume EVERY row READ, not just the admitted ones: a row suppressed by the daily cap or the
    (symbol, strategy) dedupe has HAD its evaluation, and leaving it unconsumed would re-offer it on
    every later sweep tick forever. Suppression is a decision.

    Two refusals are NOT decisions, and unlike brk20/hi52/cat — re-derived from stored bars on any
    later sweep — a consumed crossing is gone for the day (the EOD job decided the event; nothing
    here re-computes it):

      * a WINDOW refusal: a shut-window candidate was never evaluated, so an out-of-window /scan_now
        must leave the day's crossings pending for the next in-window sweep;
      * a publication into a FREEZE: ``pipeline.on_signal_candidate`` re-arms the pair and no next bar
        re-publishes a batch rule — the freeze-lift re-sweep is that re-publication, and it can only
        re-derive `ins` from rows still marked pending.

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
    """Re-run the scanner sweep when a freeze LIFTS inside the trade window.

    The batch legs originate only from the sweep, and a candidate published into a standing freeze is
    re-armed by the pipeline with nothing left to re-publish it — batch rules have no next bar. Every
    in-session boot re-applies a warm-up freeze and re-fires the window edge, so without this the
    day's brk20/hi52/cat/ins origination is lost. ONLY the FROZEN-side→NORMAL edge fires: a
    NORMAL→FROZEN transition has nothing to re-publish, and a FROZEN→KILLED one must not trade.
    ``ready`` is the boot contract's ``engine_ready`` — the recovery itself clears causes, and a sweep
    must never run ahead of the backfill/catch-up/ticker steps it reads from. ``in_flight`` is the
    sweep lock's ``locked`` (the composition root passes ``_sweep_lock.locked``; the default exists
    for the unit harness only). Extracted so the predicate, the debounce and the edge are testable."""
    if evt.new_state != RiskState.NORMAL or evt.old_state == RiskState.NORMAL:
        return
    now = clock.now()
    if not ready():
        # BOOT GUARD. Two lift paths fire from INSIDE the recovery (the self-test's `clear_stale_daily`
        # and the catch-up's `catchup_safety_jobs` clear), ahead of the data-gap backfill, the daily_bars
        # catch-up, the warm-up gate and the ticker resume. A sweep there reads a daily history still
        # missing the last session(s) (wrong breakout references), CHARGES those pairs through
        # prescreen.admit so the real window-open sweep is deduped out of them, and resubscribes nothing
        # (the ticker is STOPPED). Nothing is lost by waiting: a lift this early precedes every sweep,
        # and the INACTIVE→ACTIVE window edge sweeps in NORMAL once the scheduler is armed.
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


#: PUBLICATION order of the batch legs, by strategy id; publication does not depend on the sort that
#: allocates the caps. Module-level so it is assertable. A strategy not listed here publishes after the
#: listed ones, in admit order.
_PUBLICATION_LEG_ORDER: tuple[str, ...] = (
    brk20.STRATEGY_ID, ins.STRATEGY_ID, cat.STRATEGY_ID, cat_reversal.STRATEGY_ID, hi52.STRATEGY_ID,
)


def _unknown_no_stop_strategies(names: Iterable[str]) -> list[str]:
    """``paper.no_stop_strategies`` entries naming no strategy (a registered scanner or a batch leg),
    logged at paper composition. The PositionBook matches ids exactly, so a typo would silently keep
    that strategy's stop in the no-stop test."""
    known = {*SCANNER_REGISTRY, *_PUBLICATION_LEG_ORDER}
    unknown = sorted(set(names) - known)
    if unknown:
        _log.warning("paper_no_stop_unknown_strategy", unknown=unknown, known=sorted(known))
    return unknown


def _publication_order(admitted: list) -> list:
    """PUBLICATION order for one batch admission: LEG order (:data:`_PUBLICATION_LEG_ORDER`), and
    inside one leg the order ``SignalPreScreen.admit`` returned (its own score-descending rank).

    ADMISSION and PUBLICATION are two separable consequences of one ranked list. Publication order
    stamps ``fired_at`` (``bus.apublish`` order), and ``pipeline._forward_key`` breaks ties INSIDE a
    per-strategy quantile band by ``fired_at`` ASCENDING — so whoever publishes first wins the scarce
    analyst slot among candidates of comparable standing.

    Raw scores are NOT comparable across strategies (``hi52`` ``prox`` in [0.95, 1.0], ``ins`` ~0.5,
    ``brk20`` ~0.5-0.6), which is why the forward slot ranks by per-strategy QUANTILE. Letting the
    cross-strategy sort also stamp ``fired_at`` would smuggle that incomparable scale back in through
    the tie-break (the unvalidated hi52 forward test would head the queue). Publishing in LEG order
    means no strategy's score SCALE can buy queue priority: a fixed, auditable precedence — the
    actionable legs by seniority, the forward test last.

    ``sorted`` is stable, so within each leg the admit order (score descending) is untouched, and
    replay determinism holds — the key reads only ``strategy_id``, never a clock.
    """
    rank = {sid: i for i, sid in enumerate(_PUBLICATION_LEG_ORDER)}
    return sorted(admitted, key=lambda c: rank.get(c.strategy_id, len(rank)))


def _hi52_daily_leg(store: MarketStore, *, trigger: str, eligible: Sequence[str], today: date,
                    yesterday: date, ex_map: Mapping[str, list[date]]) -> list:
    """The `hi52` v2 daily leg: 52-week-high-proximity FRESH-CROSSES over COMPLETED daily
    sessions. Returns RAW candidates for the sweep's ONE ranked batch admission.

    Scoped to the CORE (NIFTY 500) subset: the caller passes ``store.get_core_eligible_symbols``, as
    the full-market backtest measured the edge as index-class only (extended names −0.26% net / 49%
    hit at T+20). Swing thesis (T+5..T+20, George & Hwang drift; intraday capture is cost-refuted).

    Its candidates go into the SAME concatenated batch as brk20/ins/cat/cat_reversal and are ranked
    against them under the ordinary per-strategy cap (``max_per_strategy_day.hi52`` = 3); the cost is
    LATENCY — this leg's large history read sits BEFORE the batch admit. The bar-driven
    ``prescreen.sweep`` at the top of the worker still admits first. hi52 does NOT get the front of
    the PUBLICATION queue: :func:`_publication_order` keeps it last.

    Session-cadence triggers only (``_FULL_SWEEP_TRIGGERS``): the signal derives from COMPLETED
    sessions, so one scan per session is its natural cadence and /scan_now stays cheap. ``freeze_lift``
    is IN that set (the window_open sweep re-run because a freeze swallowed the first one), else the
    leg is lost for the day. Extracted so that is testable on the candidates."""
    if trigger not in _FULL_SWEEP_TRIGGERS:
        return []
    hi52_histories: dict[str, list[brk20.DailyRow]] = {}
    hi52_start = today - timedelta(days=400)   # ≥252 sessions + weekend/holiday margin
    # No bars_1d source re-adjusts STORED history across an ex-date (Kite candles are adjusted at
    # fetch time, but the series is seeded once and extended one session at a time; bhavcopy rows are
    # raw), so a bonus/split/rights/demerger inside the window leaves phantom pre-ex highs: those
    # symbols sit out until it rolls past.
    hi52_unadjusted = hi52.unadjusted_history(
        store.get_corp_actions(ex_from=hi52_start, ex_to=yesterday)
    )
    for sym in eligible:                       # the CORE subset (owner decision)
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
    # The v2 GATE's own inputs, per SURVIVING candidate: the aggregate veto counts say nothing about the
    # shape of the population that PASSED, which the pre-registered signal-count reviews ask. Nothing
    # else records these two numbers (no place on SignalCandidate). Computed off the SAME rows and
    # helper the gate used, and only for candidates (a handful).
    candidate_diag = {}
    for cand in hi52_raw:
        diag = hi52.diagnostics_for(hi52_histories.get(cand.symbol, ()))
        if diag is not None:
            candidate_diag[cand.symbol] = {
                "up_day_frac": diag.up_day_frac, "max_day_move": diag.max_day_move,
            }
    # ORIGINATION-stage counts (the brk20/cat convention): candidates + vetoes reconcile against
    # symbols_scanned on one line; this leg no longer admits, so there is no `admitted`. The smooth/gap
    # filters can eat most of a day's flow, so a quiet tape must be readable as their decision.
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
    ``symbols`` — the eligible-pin filter. ATTRIBUTE access, never ``.get`` (a NamedTuple has none,
    and the failure aborts the whole batch admission); tested with the real NamedTuples."""
    return [r for r in rows if r.symbol in symbols]


class _CatalystDisabledAlert:
    """``CATALYST_DISABLED`` at most once a day per process: a failed digest run and
    the window-open sweep that then finds no digest for today are one outage, not two pages."""

    def __init__(self, notify: Callable[[CatalogMessage], Awaitable[None]], clock: Clock) -> None:
        self._notify = notify
        self._clock = clock
        self._sent_on: date | None = None

    async def __call__(self, reason: str) -> None:
        today = self._clock.today()
        if self._sent_on != today:
            self._sent_on = today
            await self._notify(catalyst_disabled(reason))

    async def if_no_digest_today(self, store: MarketStore, d: date) -> None:
        """`cat` sweeps only day ``d``'s watchlist, so it is off unless a digest ran on ``d``; every
        run stamps ``sentiment_agg``. A late run yesterday still reads "fresh" by age at ``d``'s
        window open, which is why this checks the date, not ``digest_status``."""
        last = await store.arun(store.latest_sentiment_as_of)
        if last is None or last.astimezone(IST).date() < d:
            when = "none on record" if last is None else f"last {last.astimezone(IST):%d-%b %H:%M}"
            await self(f"no catalyst digest has run today ({when})")


def _read_cat_watchlist(
    store: MarketStore, today: date
) -> tuple[list[cat.WatchlistRow], list[cat_reversal.WatchlistRow]]:
    """Today's ``originating`` ``catalyst_watchlist`` rows, as BOTH scanners' row tuples.

    The ``CatalystDigestJob`` wrote these; nothing here re-grades one. The ONE thing this adds is
    ``reference_close`` — the PRIOR SESSION's bhavcopy-final close from ``bars_1d`` (the last bar
    strictly before today), the freshest committed price at sweep time and the anchor for the whole
    level set. The grade filter is pushed into the store query; each RULE's own filter (``cat``: grade ∧
    direction ∧ age<=1; ``cat_reversal``: the same PLUS ``reversal_of``) is re-applied inside its
    ``sweep_watchlist``, which is the authority. A malformed row costs itself and nothing else
    (fail-to-zero).

    ONE fetch, two projections: ``cat_reversal`` reads the same rows and the same per-symbol
    ``bars_1d`` lookup as ``cat``, so a second pass would double the query count. The projections are
    independent tuples so the two pre-registered event definitions can diverge without a lockstep edit.
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
            # `reversal_of` is NULL on every ordinary row and on rows written before the column existed
            # — both read as "not a reversal". Such a row can never satisfy
            # ``cat_reversal.is_eligible`` (needs a NON-EMPTY ``reversal_of``), so it is skipped here;
            # the ``reversal_eligible`` count is unaffected either way.
            reversal_of = r.get("reversal_of")
            if reversal_of:
                rev.append(cat_reversal.WatchlistRow(**shared_fields, reversal_of=str(reversal_of)))
        except (ValueError, ArithmeticError, TypeError) as exc:
            _log.warning("cat_watchlist_row_unparseable", symbol=symbol, error=str(exc))
    return out, rev


# --------------------------------------------------------------------------- brk20 feature link
def _attach_feature_snapshots(features: FeatureEngine, candidates: list) -> list:
    """Mint the ``features_snapshot_id`` for batch-rule candidates (brk20, ins, cat) post-admit.

    The per-bar path gets its id from ScanContext at signal time; batch rules bypass ScanContext, and
    a candidate with a null id is structurally un-recommendable (intraday.py Rule 6 mandates
    no_action on a missing identifier). Runs AFTER
    ``prescreen.admit`` so suppressed/capped candidates never spend a snapshot write. A minting
    failure costs that one candidate its feature link, never the sweep (scan-path posture)."""
    out = []
    for c in candidates:
        try:
            sid = features.intraday_snapshot(c.symbol).features_snapshot_id
        except Exception as exc:  # noqa: BLE001 - fail to None, never propagate into the sweep
            _log.warning("batch_snapshot_failed", symbol=c.symbol, error=str(exc))
            sid = None
        out.append(c.model_copy(update={"features_snapshot_id": sid}))
    return out


# --------------------------------------------------------------------- brk20 RETEST re-arm
def _pending_entry_rec_symbols(conn: sqlite3.Connection, now: datetime) -> frozenset[str]:
    """Symbols carrying an UNEXPIRED, UNACTIONED entry recommendation — the gate's own
    ``GateContext.pending_rec_symbols`` set, read here because the retest must refuse what the gate
    would refuse (``per_stock_exposure: already held or pending`` is a HARD reason, uncurable by
    shrinking) BEFORE the slot and the analyst call are spent rather than after.

    The actioned rows are dropped in SQL because this runs on a 60 s pulse and `recommendations`
    only grows; :func:`engine.core.recommendations.pending_entry_symbols` re-applies the same guard
    in Python (its truthiness test matches this SQL, so an empty-string action is "not actioned" on
    both sides) so the two callers can never disagree when one of them does not pre-filter.
    """
    rows = conn.execute(
        "SELECT payload, human_action FROM recommendations "
        "WHERE human_action IS NULL OR human_action = ''"
    ).fetchall()
    return pending_entry_symbols(rows, now)


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
      signal; re-offering here spends a day slot and one of the day's analyst calls on a
      proposal ``gate._rule_per_stock_exposure`` is certain to hard-reject.
    * **pending** — an unexpired, unactioned entry recommendation already sits on it: the same gate
      reason, and the owner is already looking at this trade.
    * **ex_skip** — a known ex-date inside brk20's own ex-date horizon. ``bars_1d`` is never re-adjusted,
      so the stored level is a pre-ex price; ``brk20.scan_daily`` refuses such a symbol on the
      crossing day and a re-offer must refuse it on the retest day for the same reason.
    * **unadjusted** — the BACKWARD half of the same veto: a structural ex-date
      (bonus/split/rights/demerger) already inside the 35-day lookback, the set the sweep feeds to
      ``brk20.sweep_daily(unadjusted_symbols=...)``. ``ex_skip`` looks forward only, so an ex-date
      that passed BETWEEN the crossing and the retest would otherwise re-offer a pre-ex level as a
      limit ABOVE the adjusted tape.
    * **not eligible** — the symbol has left the eligible universe since the crossing (liquidity or
      index churn). ``brk20``'s registered origination scope IS that population; a level may not
      outlive it just because it was journalled while the symbol was still in.

    An EMPTY ``eligible`` would skip everything, which is why the caller only arms the retest once
    today's in-window sweep has published the set (`sweep_ready`)."""
    out: dict[str, str] = {}
    for sym in symbols:
        if sym in held:
            out[sym] = "already held — per_stock_exposure is a HARD reject"
        elif sym in pending:
            out[sym] = "an unexpired, unactioned entry recommendation is already pending"
        elif sym in ex_skip:
            out[sym] = "a known ex-date inside brk20's horizon (the stored level is pre-ex)"
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
    mint takes ``MarketStore._lock``; the caller runs this whole body through ``asyncio.to_thread``
    (as ``prescreen.handle_bar`` and ``_collect_and_scan`` do) because either lock can be held for
    tens of seconds and the event loop owns the tick cache, the heartbeat and the kill path.

    The book's three operations are TOTAL (a journal failure logs and yields nothing), and an
    unreadable band returns early; what can still raise is ``prescreen.admit``, which propagates to the
    drain tick's own guard. Nothing in this function may take the scheduler loop down.

    The `brk20` levels admitted over the last ``retest_sessions`` sessions rest in
    ``brk20_resting_levels``; this is the pulse that re-offers one the moment its price returns to it
    (see ``engine.strategy.retest``).

    ``active`` is the caller's ``_sweep_window_active`` verdict. INACTIVE does nothing at all, not even
    the expiry sweep: outside the window there is no live LTP worth banding against and nothing may
    spend a day slot; expiry is idempotent housekeeping the next in-window tick performs anyway
    (``due`` re-tests the window itself).

    ADMISSION is ``prescreen.admit`` — the SAME call, day cap, per-strategy cap and same-day
    ``(symbol, strategy)`` dedupe the sweep faces. ``in_window=True`` states ``active``, it bypasses
    nothing: the pre-screen is Clock-free by design and the caller owns the window verdict (see
    ``SignalPreScreen.admit``). The ``features_snapshot_id`` is minted AFTER that admit by the sweep's
    own ``_attach_feature_snapshots``, so a suppressed candidate never spends a snapshot write — and a
    null id is structurally un-recommendable (intraday.py Rule 6).

    ``sweep_ready`` is "today's IN-WINDOW sweep has published". Until it has, this tick only expires:
    the day's ranked admission must reach ``prescreen.admit`` FIRST, or a level broken days ago can
    take the ``(symbol, strategy)`` dedupe slot just before the same symbol's fresh crossing, which
    would then be dropped as a duplicate and never journalled. It also guarantees ``skip_fn`` reads
    TODAY's eligible set and ex-date horizon rather than an empty one.

    A band that cannot be read (an unverifiable limits store) re-offers NOTHING: the band is the only
    thing between this mechanism and re-publishing a level the gate is certain to reject, so it fails
    to zero rather than to a constant. ``max_tick_age_s`` rides the SAME load: the trigger price must
    be as fresh as the gate will demand.

    ``brk20_retest_republished`` is emitted here per candidate ``prescreen.admit`` ACCEPTED, off the
    ``offers`` detail the book fills in place; the book's own line is ``brk20_retest_offered``, so the
    two funnel stages are separately greppable.
    """
    if not active:
        return []
    retest_book.expire(today)
    if not sweep_ready:
        return []
    try:
        band_pct, max_tick_age_s = limits_fn()
    except Exception as exc:  # noqa: BLE001 - an unreadable band re-offers nothing
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


# --------------------------------------------------------------------------- retention
async def apply_tick_retention(store: MarketStore, result: TickCompactionResult) -> None:
    """Run the retention purge after a successful nightly compaction pass.

    ``MarketStore.apply_retention`` (ticks 30 d by partition dir, news/clusters/sentiment 1 y,
    corrections 90 d) is hung off ``tick_compact`` because that job already owns the closed tick
    partitions and runs nightly after the writer is done with them.

    Gated on a clean pass: ``ok=False`` means a symbol-day is still un-compacted (retention would be
    purging under a job that is going to be retried), ``skipped_in_flight=True`` means ANOTHER
    compaction run holds the lock (``rmtree`` of a partition that run is walking is the race to avoid),
    and ``stopped=True`` means the run was cut off at the no-run window or by a stop signal, where no
    store work belongs. The next completed pass sweeps.

    Failure is contained: a retention error WARNs and returns, never touching the compaction job's
    ok-bearing result (a purge that could not run is not a data-freshness failure).
    """
    if not result.ok or result.unfinished:
        return
    try:
        report = await store.aapply_retention()
    except Exception as exc:  # noqa: BLE001 - retention must never fail the compaction job
        _log.warning("tick_retention_failed", error=str(exc), error_type=type(exc).__name__)
    else:
        _log.info("tick_retention_applied", **report)


# --------------------------------------------------------------------------- backup
async def _snapshot_backup(conn: sqlite3.Connection, settings, clock: Clock) -> None:
    """Watermark-safe SQLite state snapshot. Uses a fresh source connection inside the worker
    thread (the online-backup API over a second read handle is WAL-safe) so it never contends with the
    engine's live connection. DuckDB market data is not covered here: the nightly ``market_backup``
    lane job covers it (:func:`_market_backup`, an ``EXPORT DATABASE`` — Windows cannot copy the open
    file). ``MarketStore.export_bars_1m_month`` (the monthly Parquet archive) exists but nothing
    schedules it."""
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


#: Retention for state snapshots and NSSM-rotated service logs. engine.log keeps its own 90 days (core.log).
BACKUP_KEEP_ALL_DAYS = 14
RETENTION_DAYS = 90


def _prune_retained_files(backups: Path, logs: Path, now: datetime) -> dict[str, int]:
    """Delete state snapshots past retention — every one for 14 days, then the newest per ISO week to
    90 days — and rotated ``service.*-*.log`` files older than 90 days. The newest snapshot always
    survives, however old (an engine offline for months still has its last state)."""
    snapshots: list[tuple[datetime, Path]] = []
    for p in backups.glob("state-*.db"):
        try:
            stamp = datetime.strptime(p.name[len("state-"):-len(".db")], "%Y%m%dT%H%M%S")
        except ValueError:
            continue
        snapshots.append((stamp.replace(tzinfo=now.tzinfo), p))
    doomed: list[Path] = []
    weeks_kept: set[tuple[int, int]] = set()
    for i, (stamp, p) in enumerate(sorted(snapshots, reverse=True)):
        age = now - stamp
        if i == 0 or age <= timedelta(days=BACKUP_KEEP_ALL_DAYS):
            continue
        week = stamp.isocalendar()[:2]
        if age <= timedelta(days=RETENTION_DAYS) and week not in weeks_kept:
            weeks_kept.add(week)
            continue
        doomed.append(p)
    cutoff = (now - timedelta(days=RETENTION_DAYS)).timestamp()
    logs_doomed = [p for p in logs.glob("service.*-*.log") if p.stat().st_mtime < cutoff]
    for p in (*doomed, *logs_doomed):
        p.unlink(missing_ok=True)
    return {"snapshots_deleted": len(doomed), "service_logs_deleted": len(logs_doomed)}


#: Retention for ``market-<stamp>`` DuckDB exports (~2 GB each): the newest 3, plus the
#: newest of each ISO week for 4 weeks — about 7 generations. The newest always survives.
MARKET_SNAPSHOT_KEEP_NEWEST = 3
MARKET_SNAPSHOT_WEEKLY_DAYS = 28

#: Free-space floor for an export, as a multiple of the newest export's size (the first export, with
#: none to measure, needs market.duckdb's size instead).
MARKET_BACKUP_FREE_FACTOR = 3

_MARKET_SNAPSHOT_PREFIX = "market-"
_MARKET_SNAPSHOT_STAMP = "%Y%m%dT%H%M%S"

#: On a trading day no export STARTS from here to :data:`_IN_SESSION_END_IST`: early hydration
#: (~06:30), the boot catch-up and the pre-open chain run in that span, and an export (~45 s, ~2.1 GB
#: written) started just before 08:45 would run into the session window.
_MARKET_BACKUP_DAY_START_IST = time(6, 0)

#: After a failed export, later attempts in the same process wait this long; otherwise every 30-min
#: sweep's lane pass would re-run the whole export.
MARKET_BACKUP_RETRY_AFTER = timedelta(hours=6)


@dataclass(frozen=True)
class MarketBackupResult:
    """One ``market_backup`` run, ok-bearing for the watermark. ``unfinished`` (not started: the
    trading-day window or the retry hold) records nothing; ``ok=False`` (too little disk) records a
    failure the sweep retries."""

    ok: bool = True
    unfinished: bool = False
    path: str | None = None
    bytes: int = 0


@dataclass
class MarketBackupRetry:
    """When this process's last export failed (``None``: never, or a later export succeeded). In
    memory on purpose: a restart allows one fresh attempt."""

    failed_at: datetime | None = None


def _market_snapshot_stamp(p: Path) -> tuple[datetime, bool] | None:
    """``(stamp, is_tmp)`` for a ``market-<stamp>`` or ``market-<stamp>.tmp`` directory; None for
    anything else, which the backup job never touches."""
    if not p.name.startswith(_MARKET_SNAPSHOT_PREFIX) or not p.is_dir():
        return None
    body = p.name[len(_MARKET_SNAPSHOT_PREFIX):]
    is_tmp = body.endswith(".tmp")
    try:
        stamp = datetime.strptime(body[:-len(".tmp")] if is_tmp else body, _MARKET_SNAPSHOT_STAMP)
    except ValueError:
        return None
    return stamp, is_tmp


def _market_backup_space(backups: Path, db_path: Path) -> tuple[int, int]:
    """``(free, needed)`` bytes on the backups volume for the next export."""
    exports = []
    for p in backups.glob(f"{_MARKET_SNAPSHOT_PREFIX}*"):
        parsed = _market_snapshot_stamp(p)
        if parsed is not None and not parsed[1]:
            exports.append((parsed[0], p))
    if exports:
        newest = max(exports)[1]
        size = sum(f.stat().st_size for f in newest.iterdir() if f.is_file())
        needed = MARKET_BACKUP_FREE_FACTOR * size
    else:
        needed = db_path.stat().st_size if db_path.exists() else 0
    return shutil.disk_usage(backups).free, needed


def _prune_market_snapshots(backups: Path, now: datetime) -> dict[str, int]:
    """Delete ``market-<stamp>`` exports past retention (:data:`MARKET_SNAPSHOT_KEEP_NEWEST`, then the
    newest per ISO week for :data:`MARKET_SNAPSHOT_WEEKLY_DAYS`) and every ``market-<stamp>.tmp`` left
    by a killed export. Runs only after an export finished, so no tmp belongs to a live one."""
    snapshots: list[tuple[datetime, Path]] = []
    stale: list[Path] = []
    for p in backups.glob(f"{_MARKET_SNAPSHOT_PREFIX}*"):
        parsed = _market_snapshot_stamp(p)
        if parsed is None:
            continue
        stamp, is_tmp = parsed
        if is_tmp:
            stale.append(p)
        else:
            snapshots.append((stamp.replace(tzinfo=now.tzinfo), p))
    doomed: list[Path] = []
    weeks_kept: set[tuple[int, int]] = set()
    for i, (stamp, p) in enumerate(sorted(snapshots, reverse=True)):
        week = stamp.isocalendar()[:2]
        if i < MARKET_SNAPSHOT_KEEP_NEWEST:
            weeks_kept.add(week)
            continue
        if now - stamp <= timedelta(days=MARKET_SNAPSHOT_WEEKLY_DAYS) and week not in weeks_kept:
            weeks_kept.add(week)
            continue
        doomed.append(p)
    for p in (*doomed, *stale):
        shutil.rmtree(p, ignore_errors=True)
    return {"market_snapshots_deleted": len(doomed), "market_tmp_deleted": len(stale)}


async def _market_backup(
    store: MarketStore, settings, clock: Clock, calendar: NSECalendar,
    *, retry: MarketBackupRetry | None = None,
) -> MarketBackupResult:
    """DuckDB leg: ``EXPORT DATABASE`` of the live store to ``<backups>/market-<stamp>``, then
    retention. Never STARTS within :data:`_MARKET_BACKUP_DAY_START_IST`..:data:`_IN_SESSION_END_IST`
    on a trading day, nor within :data:`MARKET_BACKUP_RETRY_AFTER` of a failed export recorded in
    ``retry`` (both unfinished: a later lane pass runs it); never with free disk under the floor
    (:func:`_market_backup_space`, failed + WARNING). Non-trading days are unrestricted. The export
    holds no store lock (``MarketStore.export_snapshot``) and runs on the store's pool."""
    now = clock.now()
    if calendar.is_trading_day(now.date()) and (
        _MARKET_BACKUP_DAY_START_IST <= now.time() <= _IN_SESSION_END_IST
    ):
        _log.info("market_backup_deferred_trading_day", now=now.isoformat(),
                  not_before=_IN_SESSION_END_IST.isoformat())
        return MarketBackupResult(unfinished=True)
    if retry is not None and retry.failed_at is not None and (
        now - retry.failed_at < MARKET_BACKUP_RETRY_AFTER
    ):
        _log.info("market_backup_retry_deferred", failed_at=retry.failed_at.isoformat(),
                  retry_from=(retry.failed_at + MARKET_BACKUP_RETRY_AFTER).isoformat())
        return MarketBackupResult(unfinished=True)
    backups = settings.backups_dir()
    backups.mkdir(parents=True, exist_ok=True)
    free, needed = await asyncio.to_thread(_market_backup_space, backups, settings.duckdb_path())
    if free < needed:
        _log.warning("market_backup_skipped_low_disk", free_bytes=free, needed_bytes=needed,
                     path=str(backups))
        return MarketBackupResult(ok=False)
    dest = backups / f"{_MARKET_SNAPSHOT_PREFIX}{clock.now():{_MARKET_SNAPSHOT_STAMP}}"
    started = time_module.monotonic()
    try:
        report = await store.arun(store.export_snapshot, dest)
    except Exception:
        if retry is not None:
            retry.failed_at = clock.now()
        raise
    if retry is not None:
        retry.failed_at = None
    _log.info("market_backup_written", path=str(dest),
              duration_s=round(time_module.monotonic() - started, 1),
              bytes=report["bytes"], tables=report["tables"])
    try:
        pruned = await asyncio.to_thread(_prune_market_snapshots, backups, clock.now())
    except Exception as exc:  # noqa: BLE001 - housekeeping must not fail the backup job
        _log.warning("market_backup_prune_failed", error=str(exc), error_type=type(exc).__name__)
    else:
        _log.info("market_backup_pruned", **pruned)
    return MarketBackupResult(path=str(dest), bytes=report["bytes"])


# --------------------------------------------------------------------------- builders (guarded)
def _build_version() -> str:
    """Best-effort build/version string for the ENGINE_STARTED signal + STARTUP_REPORT."""
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
        bot.attach_bus(bus)   # alert catalog: mode/risk/kill/window/budget events → owner
    return bot


def _create_app(session, mode, kill, secrets, clock, bus, *, conn=None, protected_store=None,
                exposure=None, governor=None, limits_engine=None, market_store=None,
                paper_control=None, paper_view=None, latch=None, capital=None):
    from engine.api.app import create_app

    return create_app(session_manager=session, mode_manager=mode, kill_switch=kill,
                      secrets=secrets, clock=clock, bus=bus, conn=conn, store=protected_store,
                      exposure=exposure, governor=governor, limits_engine=limits_engine,
                      market_store=market_store, paper_control=paper_control, paper_view=paper_view,
                      latch=latch, capital=capital)


async def _serve_api(app, settings):
    """Bind + serve the dashboard/login API; return the serve task, or ``None`` if the bind failed.

    The login callback (:8400 ``GET /kite/callback``) must come up EARLY and fail LOUDLY. Two traps:

    * uvicorn's own bind paths call ``sys.exit(1)`` on a bind ``OSError`` (``Config.bind_socket`` and
      the host/port branch of ``Server.startup``). Inside a task that ``SystemExit`` escapes the
      coroutine and KILLS the whole engine loop — no alert, no Telegram ``/token`` fallback, and the
      single-instance guard never runs — precisely when a stale process already holds the port.
      So we bind the socket OURSELVES (plain ``OSError`` on failure) and hand it to
      ``server.serve(sockets=[...])``, whose pre-bound path has NO ``sys.exit`` (uvicorn closes the
      socket again on its own shutdown).
    * On Windows the default ``SO_REUSEADDR`` lets a second bind silently STEAL a live port;
      ``SO_EXCLUSIVEADDRUSE`` makes the double-bind fail loudly here instead.
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


def main() -> NoReturn:
    # The interpreter MUST NOT be trusted to exit on its own after run() returns (abandoned executor
    # workers, see hard_exit), so BOTH the normal-return and the KeyboardInterrupt path funnel through the
    # os._exit backstop via `finally` — no path may fall through to a bare return. A raw KeyboardInterrupt
    # escaping asyncio.run is still caught here and mapped to 130. A crash is logged here: os._exit skips
    # the interpreter's own traceback print.
    code = 1
    try:
        code = asyncio.run(run())
    except KeyboardInterrupt:
        _log.info("engine_interrupted")
        code = 130
    except Exception:
        _log.exception("engine_crashed")
    finally:
        hard_exit(code)


if __name__ == "__main__":
    main()
