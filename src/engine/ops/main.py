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
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, time, timedelta
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
from engine.datafeeds.bhavcopy import BhavcopyJob
from engine.datafeeds.corp_actions import CorpActionsJob
from engine.datafeeds.deals import DealsJob
from engine.datafeeds.earnings_calendar import EarningsCalendarJob
from engine.datafeeds.filings_pit import FilingsPitJob
from engine.datafeeds.filings_pit_fresh import FilingsPitFreshJob
from engine.datafeeds.filings_results import FilingsResultsJob
from engine.datafeeds.filings_shp import FilingsShpJob
from engine.datafeeds.news import NewsIngest
from engine.datafeeds.news_pipeline import CatalystDigestJob, EntityResolver, HeadlineClusterer
from engine.datafeeds.sector_map import SectorMapJob
from engine.features.engine import FeatureEngine
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.reconcile import ReconcileJob
from engine.marketdata.store import MarketStore
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
    JOB_NIGHTLY_REVIEW,
    JOB_PREOPEN_PLANNER,
    JOB_RECO_EXPIRE,
    JOB_FILINGS_PIT,
    JOB_FILINGS_PIT_FRESH,
    JOB_FILINGS_RESULTS,
    JOB_FILINGS_SHP,
    JOB_INSTRUMENTS,
    JOB_NEWS_CHAIN,
    JOB_RECONCILE,
    JOB_SECTOR_MAP,
    JOB_SURVEILLANCE,
    JOB_UNIVERSE,
    CatchUpRunner,
    JobClass,
    JobRegistry,
    JobSpec,
)
from engine.ops.keep_awake import KeepAwake
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.post_login import (
    PostLoginRecovery,
    hydrate_instruments_at_startup,
    regime_and_warmup_backfill,
    resume_ticker,
)
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.ops.single_instance import InstanceLock
from engine.intelligence.context import ContextAssembler
from engine.intelligence.governor import BudgetGovernor
from engine.intelligence.harness import AgentHarness, load_agent_roster, run_sdk_smoke
from engine.ops.news_scoring import NewsScoringJob
from engine.ops.nightly_review import NightlyReviewJob
from engine.ops.pipeline import RecommendationBook, RecommendationPipeline
from engine.ops.preopen_planner import PreopenPlannerJob
from engine.ops.scan_context import LiveScanContextProvider
from engine.ops.warmup import WarmupGate, WarmupStatus
from engine.risk.gate import GateContextBuilder, RiskGate
from engine.strategy.cost_model import CostModel
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import build_enabled_scanners
from engine.risk.causes import RiskStateLatch
from engine.risk.exposure import ExposureTracker
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine, floor_limits_from
from engine.risk.mode import ModeManager
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
_NEWS_CHAIN_IST = time(8, 25)
_DAILY_BARS_IST = time(18, 5)
_FEATURES_IST = time(18, 50)

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
    ):
        registry.register(spec)
    # Phase-2 jobs (§8.3) register only when composition built their owning object (LLM tier may be
    # disabled, D7). Catch-up dependency order within RUN_LATEST: universe(10) → news_chain(20) →
    # digest(25) → planner(28) — the digest needs scored clusters + today's universe; the planner
    # needs the digest (§2.7 steps 4-6).
    for spec in (
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
    bar_builder = BarBuilder(store, clock, bus)
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
    gate = RiskGate(limits_engine, cost_model, clock)

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
        )
        if harness is not None else None
    )
    if pipeline is not None:
        bus.subscribe("signal.candidate", pipeline.on_signal_candidate)
        bus.subscribe("bar.1m", pipeline.on_bar)
    if telegram is not None:
        telegram.set_reco_book(book)

    # --- live pre-screen (§3.2.5): scanners → signal.candidate. Runs whenever the engine is up;
    #     candidate-forwarding to the analyst is window-gated inside the pipeline, not here. ---
    scan_provider = LiveScanContextProvider(
        store, clock, calendar, features, index_symbol=INDEX_SYMBOL,
        # Late-bound: watchlist_symbols is defined further down this function; the lambda resolves it
        # at day-cache build time (first bar of the day), long after the whole graph is wired.
        momentum_universe=lambda: watchlist_symbols(),
    )
    prescreen = SignalPreScreen(
        scanners=build_enabled_scanners(("orb", "rsi2", "trend", "mom")),   # `cat` lands Phase 3
        context_provider=scan_provider,
        bus=bus,
        max_candidates_per_day=settings.strategy.prescreen.max_candidates_per_day,
        max_per_strategy_day=settings.strategy.prescreen.max_per_strategy_day,
    )
    bus.subscribe("bar.1m", prescreen.handle_bar)

    # --- Tier-1 jobs (all fail to no-output, never blocking — D7/E5) ---
    digest_job = CatalystDigestJob(store, clock, calendar, protected_store)
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
                         calendar, notify=notify)
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

    async def job_earnings() -> None:
        await earnings.run(clock.today())

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

    async def resolve_news(headlines: list) -> None:
        if not headlines:
            return
        async with news_chain_lock:
            touched = await clusterer.run(headlines)
            await resolver.aload(clock.today())
            await resolver.run(touched)

    async def score_news(*, force: bool = False) -> None:
        if scoring_job is None:
            return
        async with news_chain_lock:
            await scoring_job.run_batch(force=force)

    async def job_news_chain() -> None:
        # §4.4 job 10 startup/catch-up: backfill → cluster → resolve (never entry-blocking, §2.7),
        # then the §5.4 pre-open scoring batch (force=True: score ALL unscored regardless of the ≥8
        # minimum) so the ~08:35 digest sees today's scores. Scoring failure never blocks the chain.
        await resolve_news(await news_ingest.backfill())
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

    async def job_preopen_planner() -> None:
        if planner_job is None:
            # A FAILED watermark, not a silent success: catch-up retries once the roster is fixed
            # (2026-08-03: the sonnet-5 roster failure made this a 29 ms "success" no-op all day).
            raise RuntimeError("preopen planner unavailable — LLM roster quarantined/unloaded")
        await planner_job.run(clock.today())

    async def job_reco_expire() -> None:
        # §3.6: expired-unconfirmed recommendations become labelled no_action rows (unbiased non-fill
        # signal); aged tracked positions get their §7.1 max_holding exit recommendations.
        expired = book.expire_stale(clock.now())
        if expired:
            _log.info("recommendations_expired", count=expired)
        if pipeline is not None:
            await pipeline.check_aged_positions(clock.today())

    async def job_nightly_review(d) -> None:
        if nightly_job is None:
            raise RuntimeError("nightly reviewer unavailable — LLM roster quarantined/unloaded")
        await nightly_job.run(d)

    async def job_sector_map() -> None:
        await sector_map.run(clock.today(), universe_symbols=watchlist_symbols())

    async def job_corp_actions() -> None:
        await corp_actions.run(clock.today())

    async def job_backup() -> None:
        await _snapshot_backup(conn, settings, clock)

    async def job_bhavcopy(d) -> None:
        await bhavcopy.run(d)

    async def job_deals(d) -> None:
        await deals.run(d)

    async def job_filings_pit(d) -> None:
        await filings_pit.run(d)

    async def job_filings_pit_fresh(d) -> None:
        await filings_pit_fresh.run(d)

    async def job_filings_results(d) -> None:
        await filings_results.run(d)

    async def job_filings_shp() -> None:
        await filings_shp.run()

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
    catch_up = CatchUpRunner(conn, clock, calendar, registry, freeze=freeze_entries, notify=notify)

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
    health = HealthMonitor(
        clock, settings, ticker_supervisor=ticker, alert=alert,
        calendar=calendar, keep_awake=keep_awake,
    )
    scheduler = Scheduler(clock, calendar)

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

    # --- refresh the gate's warm-up snapshot alongside the equity cadence (fail-closed until set),
    #     and LIFT a standing warm-up freeze once coverage completes (see refresh_and_lift_warmup). ---
    async def warmup_refresh() -> None:
        await refresh_and_lift_warmup(warmup_gate, warmup_holder, mode, lifecycle)

    # --- periodic missed-job sweep (2026-07-28): boot-time catch-up cannot help a machine that SLEEPS
    #     through a fire slot and resumes without a restart — 2026-07-27 slept 17:56→evening, missed
    #     18:05 daily_bars, and warm-up then blocked the whole next session on the absent bar. The
    #     runner's watermarks make a repeat pass a no-op, so sweeping on a cadence is safe. ---
    async def catchup_sweep() -> None:
        try:
            await catch_up.catch_up()
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
            return prescreen.sweep(latest)

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
    _arm_live_jobs(scheduler, settings, bar_builder, health, news_ingest, resolve_news,
                   ticker=ticker, calendar=calendar, clock=clock, equity_tick=equity_tick,
                   scoring_tick=scoring_tick, heartbeat_tick=heartbeat_tick,
                   warmup_refresh=warmup_refresh, catchup_sweep=catchup_sweep,
                   window_sweep_tick=window_sweep_tick)

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

    # --- every-startup recovery (§2.6). check_skew honours NTP; degrades to FROZEN if unreachable (R6). ---
    report = await lifecycle.startup(check_skew=True)
    _log.info("startup_complete", mode=report.sticky_mode, killed=report.killed,
              needs_login=report.needs_login, integrity_ok=report.integrity_ok,
              jobs_caught_up=len(report.jobs_caught_up), frozen=report.frozen_reasons,
              instruments=instruments_source)
    # Boot-scoped skew verdict for the gate's §7.1 clock_skew rule (self-test measured it above);
    # seed the warm-up snapshot immediately so the gate isn't blind until the first 60s refresh.
    skew_holder["ok"] = "clock_skew" not in report.frozen_reasons
    await warmup_refresh()
    await health.check(check_skew=False)

    # --- start remaining services + idle until a stop signal (§2.6: being up is an active period). The
    #     login API is already bound (above); only prompt again here if startup still needs a login AND we
    #     did not already send the link off the token probe (no double link). ---
    scheduler.start()
    if telegram is not None and report.needs_login and not login_prompt_sent:
        await notify(login_prompt(session.login_url()))

    # stop_event + signal handlers were installed EARLY (right after the instance lock) so a wedged boot
    # is still interruptible; here we simply idle on it until the first signal requests a graceful stop.
    _log.info("engine_ready", host=settings.api.host, port=settings.api.port, mode=mode.mode().value)
    await stop_event.wait()

    # --- graceful shutdown (§2.6/§10.8 shutdown guard): flatten in-flight bars, stop the ticker, run
    #     the lifecycle guard (backup + STOPPED commit + heartbeat join), then tear down the rest. ---
    _log.info("engine_stopping")
    scheduler.shutdown()                      # no new job fires can race the teardown
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


def _scheduled_runner(spec: JobSpec, catch_up: CatchUpRunner, clock: Clock):
    async def _fire() -> None:
        today = clock.today()
        try:
            if spec.job_class == JobClass.DATE_KEYED:
                await spec.run(today)  # type: ignore[call-arg]
            else:
                await spec.run()       # type: ignore[call-arg]
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
    window_sweep_tick=None,
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
    scheduler.add_job(_news_poll("et"), trigger=IntervalTrigger(seconds=settings.news.et_poll_s),
                      job_id="news_poll_et", guard=False)
    scheduler.add_job(_news_poll("mc"), trigger=IntervalTrigger(seconds=settings.news.mc_poll_s),
                      job_id="news_poll_mc", guard=False)
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


# --------------------------------------------------------------------------- warm-up lift (§2.6/§7.1)
async def refresh_and_lift_warmup(warmup_gate, warmup_holder: dict, mode, lifecycle) -> None:
    """Refresh the gate's warm-up snapshot AND lift a standing warm-up freeze once coverage completes.

    The lift used to hang off the post-login hook only — a VALID-TOKEN mid-session restart (first
    seen 2026-08-03: boot 12:22 IST, ORB lookbacks ~17 min short) froze entries and nothing ever
    lifted them, because no login event fires on such a boot. Runs on the 60 s
    ``warmup_status_refresh`` cadence; ``reapply_warmup_gate`` resolves through the cause latch
    (never a blanket NORMAL write) and is a no-op unless the state is FROZEN with coverage ready.
    A refresh failure keeps the PREVIOUS snapshot semantics (holder untouched ⇒ stays fail-closed).
    """
    try:
        status = await warmup_gate.status()
        warmup_holder["status"] = status
    except Exception:  # noqa: BLE001 - an unevaluable warm-up stays NOT-READY (fail closed)
        _log.exception("warmup_status_refresh_failed")
        return
    if status.ready and mode.risk_state() == RiskState.FROZEN:
        try:
            await lifecycle.reapply_warmup_gate()
        except Exception:  # noqa: BLE001 - a failed lift retries on the next 60s tick
            _log.exception("warmup_freeze_lift_failed")


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
