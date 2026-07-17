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
import signal
import sqlite3
import sys
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, time, timedelta
from decimal import Decimal

import httpx
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from engine.broker.instruments import InstrumentStore, UnknownInstrument
from engine.broker.kite_client import KiteClient
from engine.broker.rate_limiter import RateLimiter
from engine.broker.session import SessionManager
from engine.broker.ticker_supervisor import TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.core.db import connect
from engine.core.enums import Actor, RiskState
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.migrations import apply_migrations
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import DASHBOARD_TOKEN, KITE_API_KEY, TELEGRAM_BOT_TOKEN, Secrets
from engine.core.types import TradeWindow
from engine.datafeeds.bhavcopy import BhavcopyJob
from engine.datafeeds.corp_actions import CorpActionsJob
from engine.datafeeds.deals import DealsJob
from engine.datafeeds.earnings_calendar import EarningsCalendarJob
from engine.datafeeds.filings_pit import FilingsPitJob
from engine.datafeeds.filings_results import FilingsResultsJob
from engine.datafeeds.filings_shp import FilingsShpJob
from engine.datafeeds.news import NewsIngest
from engine.datafeeds.news_pipeline import EntityResolver, HeadlineClusterer
from engine.datafeeds.sector_map import SectorMapJob
from engine.features.engine import FeatureEngine
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.reconcile import ReconcileJob
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind, login_prompt
from engine.ops.health import HealthMonitor
from engine.ops.heartbeat import HeartbeatWriter
from engine.ops.jobs import (
    JOB_BACKUP,
    JOB_BHAVCOPY,
    JOB_CORP_ACTIONS,
    JOB_DAILY_BARS,
    JOB_DEALS,
    JOB_EARNINGS,
    JOB_FEATURES,
    JOB_FILINGS_PIT,
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
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.ops.warmup import WarmupGate
from engine.risk.kill import KillSwitch
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
    JOB_FILINGS_PIT, JOB_FILINGS_RESULTS,                                        # date-keyed (§2.8)
)


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
        JobSpec(JOB_FILINGS_RESULTS, JobClass.DATE_KEYED, settings.jobs.filings_results_ist, fns[JOB_FILINGS_RESULTS], order=70),
    ):
        registry.register(spec)
    return registry


async def run() -> int:
    settings = load_settings()
    configure_logging(level="INFO", logs_dir=settings.logs_dir())
    _log.info("engine_boot", env=settings.env, tz=settings.timezone)

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

    # --- Telegram (optional in Phase 0/1) ---
    telegram = _build_telegram(settings, secrets, clock, mode, kill)
    telegram_holder["bot"] = telegram

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
    kite: KiteClient | None = None
    if secrets.has(KITE_API_KEY):
        kc = session.kite_connect()
        if kc is not None:
            kite = KiteClient(kc, RateLimiter(clock), clock)
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

    # --- feature engine v1 (§3.2.5/§6.2) ---
    features = FeatureEngine(store, clock, calendar, index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)

    # --- ticker subprocess supervisor (started into WARMING at step 7 when a token exists) ---
    ticker = TickerSupervisor(settings, clock, bus, symbol_for_token=instruments.symbol_for_token)

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

    def ticker_tokens() -> list[int]:
        """Ticker subscription set: watchlist + NIFTY 50 + India VIX → instrument tokens (A3)."""
        out: list[int] = []
        for sym in [*watchlist_symbols(), INDEX_SYMBOL, VIX_SYMBOL]:
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

    async def job_surveillance() -> None:
        await surveillance.refresh()

    async def job_earnings() -> None:
        await earnings.run(clock.today())

    async def job_universe() -> None:
        await leverage.refresh()          # 08:15/08:20 inputs re-read on catch-up (self-refresh, §3.2.4)
        await surveillance.current()
        await universe_builder.build(clock.today())

    async def resolve_news(headlines: list) -> None:
        if not headlines:
            return
        touched = await clusterer.run(headlines)
        await resolver.aload(clock.today())
        await resolver.run(touched)

    async def job_news_chain() -> None:
        # §4.4 job 10 startup/catch-up: backfill → cluster → resolve (never entry-blocking, §2.7).
        await resolve_news(await news_ingest.backfill())

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
        JOB_FILINGS_RESULTS: job_filings_results,
        JOB_FILINGS_SHP: job_filings_shp,
    })

    # =========================================================================================
    # OPS: freeze seam, warm-up gate, heartbeat, catch-up (registry), self-test, lifecycle.
    # =========================================================================================
    async def freeze_entries(reason: str) -> None:
        if not kill.is_killed():
            await mode.set_risk_state(RiskState.FROZEN, reason, Actor.RISK_GATE)

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
    )
    health = HealthMonitor(clock, settings, ticker_supervisor=ticker, alert=alert)
    scheduler = Scheduler(clock, calendar)

    # --- §2.6 injected recovery hooks (steps 4 & 7) ---
    async def backfill_hook() -> None:
        # Step 4: warm-up the regime history (NIFTY50/VIX daily — checkpointed, cheap on re-runs) and
        # gap-fill today's intraday minute bars from official candles so warm-up never needs live ticks.
        if backfill is None:
            return
        today = clock.today()
        await backfill.run([INDEX_SYMBOL, VIX_SYMBOL], "day",
                           today - timedelta(days=365 * settings.data.backfill_daily_years), today)
        session_ = calendar.session(today)
        watch = watchlist_symbols()
        if session_ is not None and watch:
            await backfill.warmup_gap(watch, session_.open, clock.now())

    async def ticker_resume_hook() -> None:
        # Step 7: resume the ticker into WARMING (feed-stale alarms suppressed) once a token exists.
        token = session.access_token()
        if kite is None or not session.token_valid() or token is None:
            _log.info("ticker_resume_skipped", reason="no valid Kite token")
            return
        tokens = ticker_tokens()
        if not tokens:
            _log.info("ticker_resume_skipped", reason="no subscription tokens (universe not built yet)")
            return
        await ticker.start(tokens, token)

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
        heartbeat=heartbeat, warmup_gate=warmup_gate,
        boot_history_path=data_dir / "lifecycle_boots.json",
        backfill_hook=backfill_hook, ticker_resume_hook=ticker_resume_hook, backup_hook=backup_hook,
    )

    # --- dashboard API ---
    app = _create_app(session, mode, kill, secrets, clock, bus)
    if not secrets.has(DASHBOARD_TOKEN):
        _log.warning("dashboard_token_missing", hint="run scripts/dpapi_set.py --generate-dashboard-token")

    # --- arm the schedule (calendar-guarded, R6) BEFORE recovery so a late startup still fires today ---
    _arm_registry_jobs(scheduler, registry, catch_up, clock)
    _arm_live_jobs(scheduler, settings, bar_builder, health, news_ingest, resolve_news)

    # --- bring the owner alert channel up BEFORE recovery so startup notifications + any alert raised
    #     during recovery actually reach the owner instead of being dropped 'not_started' (§3.2.11). ---
    if telegram is not None:
        await telegram.start()

    # --- every-startup recovery (§2.6). check_skew honours NTP; degrades to FROZEN if unreachable (R6). ---
    report = await lifecycle.startup(check_skew=True)
    _log.info("startup_complete", mode=report.sticky_mode, killed=report.killed,
              needs_login=report.needs_login, integrity_ok=report.integrity_ok,
              jobs_caught_up=len(report.jobs_caught_up), frozen=report.frozen_reasons)
    await health.check(check_skew=False)

    # --- start remaining services + idle until a stop signal (§2.6: being up is an active period) ---
    scheduler.start()
    if telegram is not None and report.needs_login:
        await notify(login_prompt(session.login_url()))

    server_task = await _serve_api(app, settings)

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
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
    _log.info("engine_stopped")
    return 0


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
) -> None:
    """Interval jobs that run continuously while the engine is up (not calendar-gated): the coarse
    bar-finalization timer, the state-aware health check, and the per-feed news poll cadences (§4.4
    job 10 — ET 5 min / MC 15 min / GDELT 15 min from settings.news)."""

    async def _advance_bars() -> None:
        bar_builder.advance()

    async def _health() -> None:
        await health.check(check_skew=False)

    def _news_poll(feed: str):
        async def _poll() -> None:
            await resolve_news(await news_ingest.poll(feeds=(feed,)))
        return _poll

    scheduler.add_job(_advance_bars, trigger=IntervalTrigger(seconds=5), job_id="bar_advance", guard=False)
    scheduler.add_job(_health, trigger=IntervalTrigger(seconds=settings.lifecycle.watchdog_poll_s),
                      job_id="health_check", guard=False)
    scheduler.add_job(_news_poll("et"), trigger=IntervalTrigger(seconds=settings.news.et_poll_s),
                      job_id="news_poll_et", guard=False)
    scheduler.add_job(_news_poll("mc"), trigger=IntervalTrigger(seconds=settings.news.mc_poll_s),
                      job_id="news_poll_mc", guard=False)
    scheduler.add_job(_news_poll("gdelt"), trigger=IntervalTrigger(seconds=settings.news.gdelt_poll_s),
                      job_id="news_poll_gdelt", guard=False)


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


def _build_telegram(settings, secrets, clock, mode, kill):
    token = secrets.get_optional(TELEGRAM_BOT_TOKEN)
    owner_chat_id = settings.telegram.owner_chat_id
    if not token or not owner_chat_id:
        _log.warning("telegram_disabled", reason="missing bot token or owner_chat_id (settings.telegram.owner_chat_id)")
        return None
    from engine.notify.telegram import TelegramBot

    return TelegramBot(token, owner_chat_id, clock, mode_manager=mode, kill_switch=kill)


def _create_app(session, mode, kill, secrets, clock, bus):
    from engine.api.app import create_app

    return create_app(session_manager=session, mode_manager=mode, kill_switch=kill,
                      secrets=secrets, clock=clock, bus=bus)


async def _serve_api(app, settings):
    import uvicorn

    config = uvicorn.Config(app, host=settings.api.host, port=settings.api.port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    # Stash the server on the task so _stop_api can signal it.
    task._mt_server = server  # type: ignore[attr-defined]
    return task


async def _stop_api(task) -> None:
    server = getattr(task, "_mt_server", None)
    if server is not None:
        server.should_exit = True
    try:
        await asyncio.wait_for(task, timeout=10)
    except TimeoutError:
        task.cancel()


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop does not support add_signal_handler for SIGTERM; fall back to
            # the default SIGINT (KeyboardInterrupt) handling in main().
            try:
                signal.signal(sig, lambda *_: stop_event.set())
            except (ValueError, OSError):
                pass


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        _log.info("engine_interrupted")
        return 0


if __name__ == "__main__":
    sys.exit(main())
