"""Composition root + service entrypoint (§3.2.12).

The ONLY module allowed to import everything — it wires the dependency graph and runs the engine.
``python -m engine.ops.main`` (or the ``mt-engine`` console script). Start is the SAME code path whether
manual/demand or scheduled (§2.6); every startup runs the full §2.6 recovery sequence via
``SessionLifecycle.startup``.

Phase-0 behaviour: builds the deterministic core (clock, calendar, secrets, protected store, sticky
mode/kill), the daily-session + dashboard surfaces, and the scheduler/health/lifecycle; runs startup
recovery; then idles in the sticky mode (OFF on a fresh install — the safe no-trading default). It
degrades gracefully when secrets/token are not yet seeded (warns, stays safe) so a fresh install is
runnable. The Tier-1 harness, OMS, ticker start, and live routing are wired in later phases.
"""

from __future__ import annotations

import asyncio
import signal
import sys

from engine.broker.instruments import InstrumentStore
from engine.broker.session import SessionManager
from engine.broker.ticker_supervisor import TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import config_dir, load_settings
from engine.core.db import connect
from engine.core.eventbus import EventBus
from engine.core.log import configure_logging, get_logger
from engine.core.migrations import apply_migrations
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import DASHBOARD_TOKEN, TELEGRAM_BOT_TOKEN, Secrets
from engine.core.types import TradeWindow
from engine.notify.catalog import CatalogMessage, MessageKind, login_prompt
from engine.ops.health import HealthMonitor
from engine.ops.lifecycle import CatchUpRunner, SessionLifecycle
from engine.ops.scheduler import Scheduler
from engine.ops.selftest import SelfTest
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

_log = get_logger("engine.ops.main")


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
        always mirrored to the structured log. Best-effort — a failed send never propagates (used for
        the ENGINE_STARTED/ENGINE_STOPPED process-lifecycle signals + the daily login prompt)."""
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

    # --- Telegram (optional in Phase 0) ---
    telegram = _build_telegram(settings, secrets, clock, mode, kill)
    telegram_holder["bot"] = telegram

    # --- broker-side skeletons (started in later phases) ---
    _instruments = InstrumentStore(clock)
    ticker = TickerSupervisor(settings, clock, bus)

    # --- ops: selftest, health, catch-up, lifecycle, scheduler ---
    self_test = SelfTest(
        conn=conn, clock=clock, settings=settings, secrets=secrets,
        protected_store=protected_store, kill_switch=kill, mode_manager=mode, session_manager=session,
    )
    health = HealthMonitor(clock, settings, ticker_supervisor=ticker, alert=alert)
    catch_up = CatchUpRunner(conn, clock, calendar)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=settings,
        mode_manager=mode, kill_switch=kill, self_test=self_test, catch_up=catch_up, alert=alert,
        notify=notify, build_version=_build_version(),
    )
    scheduler = Scheduler(clock, calendar)

    # --- dashboard API ---
    app = _create_app(session, mode, kill, secrets, clock, bus)
    if not secrets.has(DASHBOARD_TOKEN):
        _log.warning("dashboard_token_missing", hint="run scripts/dpapi_set.py --generate-dashboard-token")

    # --- bring the owner alert channel up BEFORE recovery so startup notifications
    #     (engine_started / startup_report) and any kill/health alert raised during recovery
    #     actually reach the owner, instead of being dropped 'not_started' (§3.2.11). ---
    if telegram is not None:
        await telegram.start()

    # --- every-startup recovery (§2.6). check_skew honours NTP; degrades to FROZEN if unreachable (R6). ---
    report = await lifecycle.startup(check_skew=True)
    _log.info("startup_complete", mode=report.sticky_mode, killed=report.killed,
              needs_login=report.needs_login, integrity_ok=report.integrity_ok)
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

    # --- graceful shutdown (§2.6 shutdown guard) ---
    _log.info("engine_stopping")
    await lifecycle.shutdown()
    if telegram is not None:
        await telegram.stop()
    scheduler.shutdown()
    await _stop_api(server_task)
    conn.close()
    _log.info("engine_stopped")
    return 0


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
    except (TimeoutError, asyncio.TimeoutError):
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
