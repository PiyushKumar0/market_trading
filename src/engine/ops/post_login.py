"""Post-login recovery — the §2.6 cold-start RE-TRIGGER (third cold-start-family defect, 2026-07-21).

The every-startup recovery sequence (:meth:`engine.ops.lifecycle.SessionLifecycle.startup`) runs ONCE
at boot: instruments load, regime + warm-up-gap backfill, warm-up evaluation, and ticker start. The
owner's normal morning pattern is to bring the engine up (~08:10) and log into Kite LATER. A boot
BEFORE login is token-less, so those broker-touching steps no-op (the ticker never starts — ZERO live
1m bars are ever captured — and warm-up stays frozen) and, crucially, NOTHING re-runs them when the
token finally arrives.

The 2026-07-20 fix (commit 5d34e77) closed the token-MAP half (hydrate / persist / young-listing).
This module closes the RE-TRIGGER half: :class:`PostLoginRecovery` is registered as a
:meth:`~engine.broker.session.SessionManager.add_login_hook` callback and fires the moment a token
becomes valid — from EITHER login path (the LAN ``/kite/callback`` route or the Telegram ``/token``
fallback both funnel through :meth:`SessionManager.complete_login`). It re-runs, each step GUARDED and
logged individually (a failure degrades + alerts, never crashes the loop), the four broker-touching
startup steps:

    (a) instruments  — ensure the token map is loaded + persisted (the 5d34e77 ladder: hydrate if
                       empty, else a live refresh + persist when the pre-login boot only had a stale
                       hydrated snapshot);
    (b) backfill     — regime daily history (NIFTY 50 / India VIX) + intraday warm-up-gap fill for the
                       watchlist — the SAME calls startup makes, shared via
                       :func:`regime_and_warmup_backfill`;
    (c) warm-up      — re-evaluate the §2.6 step-6 gate and, once the FREEZING classes (REGIME /
                       unattributable — since 2026-09-17 an INTRADAY or DAILY shortfall is refused
                       per SYMBOL at the gate instead) are covered, LIFT the warm-up
                       FROZEN-for-entries through the lifecycle's own gate application
                       (:meth:`SessionLifecycle.reapply_warmup_gate` — never a direct risk-state bypass);
    (d) ticker       — start the feed with the subscription tokens — the SAME step-7 resume logic,
                       shared via :func:`resume_ticker`;
    (e) holdings     — §3.6 holdings reconcile (2026-09-07). NOT a recovery step: it restores nothing
                       and gates nothing. It runs here because a fresh token is the first moment the
                       account can be read at all, and the owner's overnight sells are exactly what
                       the morning needs to know. Last in the ladder, and its own failures are already
                       swallowed by the job.

Idempotent by construction: safe to fire on every login / daily token refresh — each step skips when
its state is already good and says so in the summary. One ``post_login_recovery`` event is emitted with
the per-step outcome plus an owner notify so a BACKGROUND recovery is never silent.

This module (and :func:`hydrate_instruments_at_startup`, which lives here so the composition root and
the recovery share the exact same ladder) talks only to ``core`` + ``broker`` + ``marketdata`` + the
``ops`` lifecycle/notify seams; the composition root re-exports the helpers it also uses.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from engine.broker.instruments import InstrumentStore
from engine.broker.kite_client import KiteClient
from engine.broker.session import SessionManager
from engine.broker.ticker_supervisor import TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.store import MarketStore
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.holdings_reconcile import HoldingsReconcileJob
from engine.ops.warmup import DAILY_LOOKBACK_SESSIONS, recent_sessions

if TYPE_CHECKING:  # only for typing — lifecycle imports nothing from here, so no runtime cycle
    from engine.ops.lifecycle import SessionLifecycle

_log = get_logger("engine.ops.post_login")

Notify = Callable[[CatalogMessage], Awaitable[None]]
AlertCallback = Callable[[str, str], Awaitable[None]]   # (severity, message)


# --------------------------------------------------------------------------- cold-start hydration (F2)
async def hydrate_instruments_at_startup(
    instruments: InstrumentStore,
    store: MarketStore,
    kite: KiteClient | None,
    *,
    session_valid: bool,
    clock: Clock,
) -> str:
    """Cold-start token-map recovery ladder (§2.6 / §4.3, F2). Returns the branch tag taken.

    A fresh process holds an EMPTY :class:`InstrumentStore`. If this boot is a restart after the 08:15
    ``instruments`` job, that job's ``job_runs`` watermark makes the catch-up runner skip it, so nothing
    would repopulate the token map — every backfill/warm-up/ticker lookup then fails (``unknown_token``
    storm, warm-up frozen). Run this BEFORE the §2.6 step-4 backfill and step-6 warm-up (both inside
    ``lifecycle.startup``) to rebuild the map, cheapest source first:

    - ``already_loaded`` — the store is non-empty (a refresh already ran this process); nothing to do.
    - ``hydrated`` — rebuild from the latest persisted ``instruments_daily`` snapshot (works pre-login).
    - ``startup_refresh`` — table empty but a valid Kite session exists: live refresh + persist (as the
      08:15 job would), so the next restart can hydrate.
    - ``unavailable`` — neither possible (no snapshot, no session): log a WARNING with the explicit
      cause. Behaviour then matches today (entries stay FROZEN), but with a named reason in the startup
      report instead of a silent ``unknown_token`` storm.

    Extracted from the composition root so the ladder is unit-testable without booting the engine (and,
    from 2026-07-21, reused by :class:`PostLoginRecovery` for the post-login re-trigger).
    """
    if not instruments.is_empty:
        _log.info("instruments_startup_skip", reason="store already populated this process")
        return "already_loaded"
    latest = await store.arun(store.get_latest_instruments_daily)
    if latest is not None:
        d, rows = latest
        count = instruments.hydrate(rows)
        _log.info("instruments_hydrated", d=d.isoformat(), count=count, indices=instruments.index_count)
        if instruments.index_count == 0:
            # DEGRADED-snapshot escalation (2026-07-21): a snapshot with ZERO index rows cannot resolve
            # the regime symbols (NIFTY 50 / INDIA VIX) — regime backfill then fails unknown_token and
            # warm-up never clears. Observed cause: a taskkill mid-persist TORE the 07-21 day (the index
            # rows are appended last, so exactly they were lost) and every boot then hydrated the broken
            # MAX(d) day over the complete prior one. A real Kite dump ALWAYS carries indices, so
            # index_count==0 is a reliable degradation signal. With a live session, escalate to a full
            # refresh + persist — healing the stored day in place; without one, warn loudly (the
            # PostLoginRecovery instruments step re-refreshes the moment the owner logs in).
            if kite is not None and session_valid:
                count = await instruments.refresh(kite)
                today = clock.today()
                persisted = await store.arun(
                    store.upsert_instruments_daily, instruments.snapshot_rows(today)
                )
                _log.warning(
                    "instruments_hydrate_degraded_refreshed",
                    hydrated_d=d.isoformat(), count=count, persisted=persisted,
                    indices=instruments.index_count,
                )
                return "hydrated_degraded_refreshed"
            _log.warning(
                "instruments_hydrate_degraded",
                d=d.isoformat(),
                reason="snapshot has no index rows — regime tokens unresolvable until the post-login refresh",
            )
        return "hydrated"
    if kite is not None and session_valid:
        count = await instruments.refresh(kite)
        today = clock.today()
        persisted = await store.arun(store.upsert_instruments_daily, instruments.snapshot_rows(today))
        _log.info("instruments_startup_refresh", d=today.isoformat(), count=count, persisted=persisted)
        return "startup_refresh"
    _log.warning(
        "instruments_unavailable",
        reason="no persisted instruments_daily snapshot and no valid Kite session",
        effect="token map empty — backfill/warm-up/ticker lookups fail; entries stay FROZEN until login",
    )
    return "unavailable"


# --------------------------------------------------------------------------- reusable startup steps
async def resume_ticker(
    session: SessionManager,
    kite: KiteClient | None,
    ticker: TickerSupervisor,
    ticker_tokens: Callable[[], list[int]],
) -> str:
    """Resume the ticker into WARMING once a valid token exists (§2.6 step 7). Returns an outcome tag.

    Extracted from the composition root so the SAME logic serves both startup step 7 and the post-login
    re-trigger (rather than duplicating the guard). Idempotent: a ticker already running is left alone.
    Tags: ``skipped_no_token`` | ``already_running`` | ``skipped_no_tokens`` | ``started``.
    """
    token = session.access_token()
    if kite is None or not session.token_valid() or token is None:
        _log.info("ticker_resume_skipped", reason="no valid Kite token")
        return "skipped_no_token"
    if ticker.health().state != "STOPPED":
        _log.info("ticker_resume_skipped", reason="ticker already running")
        return "already_running"
    tokens = ticker_tokens()
    if not tokens:
        _log.info("ticker_resume_skipped", reason="no subscription tokens (universe not built yet)")
        return "skipped_no_tokens"
    await ticker.start(tokens, token)
    _log.info("ticker_resumed", tokens=len(tokens))
    return "started"


async def regime_and_warmup_backfill(
    backfill: BackfillJob,
    clock: Clock,
    calendar: NSECalendar,
    settings: Settings,
    watchlist_symbols: Callable[[], list[str]],
    index_symbol: str,
    vix_symbol: str,
    *,
    daily_lookback_sessions: int = DAILY_LOOKBACK_SESSIONS,
) -> dict[str, int]:
    """§2.6 step 4: warm the regime daily history (NIFTY 50 / India VIX — checkpointed, cheap on
    re-runs), repair the watchlist's daily lookback (the warm-up gate's own window — coverage-checked
    per symbol, so a healthy watchlist costs zero Kite requests), and gap-fill today's intraday minute
    bars for the watchlist from official candles so warm-up never needs live ticks. Extracted so
    startup and the post-login re-trigger issue the IDENTICAL calls. Returns bars written per leg
    (``{"regime_daily_bars", "watchlist_daily_gap_bars", "warmup_gap_bars"}``)."""
    today = clock.today()
    session = calendar.session(today)
    # The day-interval end is clamped to YESTERDAY until today's session has closed: an intraday
    # fetch returns today's RUNNING candle, and writing it advances the observed-through checkpoint
    # so the 18:05 daily_bars job skips the day — freezing a partial snapshot as today's daily bar
    # forever (2026-07-29: NIFTY 50/VIX closes stuck at the 09:53 boot's LTP until repaired).
    # Today's FINAL bar is the evening job's business, never an intraday fetch's.
    day_end = today
    if session is not None and clock.now() <= session.close:
        day_end = today - timedelta(days=1)
    day_report = await backfill.run(
        [index_symbol, vix_symbol], "day",
        today - timedelta(days=365 * settings.data.backfill_daily_years), day_end,
    )
    written = {"regime_daily_bars": day_report.bars_written, "watchlist_daily_gap_bars": 0, "warmup_gap_bars": 0}
    watch = watchlist_symbols()
    # 2026-09-15: repair the WATCHLIST's daily lookback too (the gate's exact window) — a boot onto a
    # universe whose members have bars_1d holes froze the DAILY class with nothing to lift it. Coverage
    # is checked per symbol from the store first, so a healthy watchlist costs zero Kite requests.
    if watch:
        sessions = recent_sessions(calendar, today, daily_lookback_sessions)
        if sessions is None:
            _log.warning("watchlist_daily_gap_skipped", reason="calendar_horizon")
        else:
            daily_report = await backfill.daily_gap(watch, sessions)
            written["watchlist_daily_gap_bars"] = daily_report.bars_written
    if session is not None and watch:
        # ``confirm_until`` clamped to session.close (2026-09-18): a post-close boot must not mark
        # every after-hours minute no-trade — the §2.6 gate clamps to the close, so those were never
        # holes. The −2 min keeps a minute Kite has not published yet out of the confirmation.
        gap_report = await backfill.warmup_gap(
            watch, session.open, clock.now(),
            confirm_until=min(session.close, clock.now() - timedelta(minutes=2)),
        )
        written["warmup_gap_bars"] = gap_report.bars_written
    return written


# --------------------------------------------------------------------------- recovery report
class StepOutcome(BaseModel):
    name: str
    status: str          # "ok" | "skipped" | "failed"
    detail: str = ""


class PostLoginRecoveryReport(BaseModel):
    steps: list[StepOutcome] = Field(default_factory=list)
    ok: bool = True
    any_failed: bool = False


# --------------------------------------------------------------------------- the re-trigger
class PostLoginRecovery:
    """The §2.6 post-login re-trigger (see module docstring).

    Registered via :meth:`SessionManager.add_login_hook`; :meth:`run` is the guarded four-step recovery
    fired fire-and-forget when a token becomes valid. Every step is guarded and logged individually —
    a step that raises marks itself failed and alerts, the others still run, and the loop never crashes.

    :attr:`completed` is the §2.6 early-hydration handshake (2026-09-09).
    :meth:`SessionManager._fire_login_hooks` creates ONE TASK PER HOOK, so registration order gives
    :class:`~engine.ops.early_hydration.EarlyHydration` no ordering against this recovery at all — the
    two run concurrently. This event is what sequences them: SET in ``__init__`` (an engine that never
    runs a recovery must not park the chain), CLEARED at the top of :meth:`run` before its first
    await, and SET again in a ``finally`` — a FAILED recovery releases the chain exactly like a clean
    one, because the pre-open chain needs no Kite session and a stuck event would cost the digest.
    DEPTH-COUNTED via ``_in_flight`` (2026-09-09 review): two overlapping logins fire two concurrent
    ``run()`` tasks, and a bare Event would be SET by whichever finishes FIRST — releasing the chain
    while the other is still mid-ladder. The event re-sets only once every in-flight ``run()`` has
    reached its own ``finally``.
    """

    def __init__(
        self,
        *,
        instruments: InstrumentStore,
        store: MarketStore,
        kite: KiteClient | None,
        session: SessionManager,
        clock: Clock,
        calendar: NSECalendar,
        settings: Settings,
        backfill: BackfillJob | None,
        ticker: TickerSupervisor,
        lifecycle: SessionLifecycle,
        ticker_tokens: Callable[[], list[int]],
        watchlist_symbols: Callable[[], list[str]],
        index_symbol: str,
        vix_symbol: str,
        notify: Notify | None = None,
        alert: AlertCallback | None = None,
        holdings_reconcile: HoldingsReconcileJob | None = None,
    ) -> None:
        self._instruments = instruments
        self._store = store
        self._kite = kite
        self._session = session
        self._clock = clock
        self._calendar = calendar
        self._settings = settings
        self._backfill = backfill
        self._ticker = ticker
        self._lifecycle = lifecycle
        self._ticker_tokens = ticker_tokens
        self._watchlist_symbols = watchlist_symbols
        self._index_symbol = index_symbol
        self._vix_symbol = vix_symbol
        self._notify = notify
        self._alert = alert
        self._holdings_reconcile = holdings_reconcile
        #: "This recovery is not running" (§2.6 early hydration, 2026-09-09 — see the class docstring).
        #: Starts SET so an engine that never fires a login hook blocks nothing. DEPTH-COUNTED
        #: (2026-09-09 review): a bare Event is set by whichever ``run()`` finishes FIRST, which would
        #: release the early-hydration chain while a SECOND overlapping login's recovery is still
        #: mid-ladder — ``SessionManager._fire_login_hooks`` fires one task per hook per login, so two
        #: logins landing close together really do overlap. ``_in_flight`` counts concurrent runs; the
        #: event only sets again once the count returns to zero.
        self.completed = asyncio.Event()
        self.completed.set()
        self._in_flight = 0

    async def run(self) -> PostLoginRecoveryReport:
        """Fire the guarded four-step recovery and emit the summary. Never raises: this is a login
        hook — a failure degrades + alerts, it must never propagate into the login path or the loop."""
        # Cleared BEFORE the first await (2026-09-09): the early-hydration hook is a sibling task
        # dispatched in the same fan-out, so any await here is a chance for it to run and miss the
        # ladder it depends on. Depth-counted (2026-09-09 review) so a SECOND overlapping ``run()``
        # does not let the FIRST run's ``finally`` release the chain out from under it — the event is
        # only re-set once every in-flight run (this one included) has reached its own ``finally``.
        self._in_flight += 1
        self.completed.clear()
        try:
            _log.info("post_login_recovery_start", token_valid=self._session.token_valid())
            steps = [
                await self._guard("instruments", self._step_instruments),
                await self._guard("backfill", self._step_backfill),
                await self._guard("warmup", self._step_warmup),
                await self._guard("ticker", self._step_ticker),
                await self._guard("holdings", self._step_holdings),
            ]
            any_failed = any(s.status == "failed" for s in steps)
            report = PostLoginRecoveryReport(steps=steps, ok=not any_failed, any_failed=any_failed)
            _log.info(
                "post_login_recovery",
                ok=report.ok,
                steps={s.name: s.status for s in steps},
                detail={s.name: s.detail for s in steps},
            )
            await self._emit_summary(report)
            return report
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self.completed.set()

    async def _guard(
        self, name: str, fn: Callable[[], Awaitable[tuple[str, str]]]
    ) -> StepOutcome:
        """Run one recovery step; a raise becomes a ``failed`` outcome + alert, never a crash — so a
        later step (e.g. ticker) always runs even if an earlier one (e.g. backfill) blew up."""
        try:
            status, detail = await fn()
            _log.info("post_login_step", step=name, status=status, detail=detail)
            return StepOutcome(name=name, status=status, detail=detail)
        except Exception as exc:  # noqa: BLE001 - a recovery step degrades + alerts, never crashes
            _log.exception("post_login_step_failed", step=name)
            if self._alert is not None:
                await self._alert(
                    "warning", f"post-login recovery step failed: {name} ({type(exc).__name__})"
                )
            return StepOutcome(name=name, status="failed", detail=f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------ (a) instruments
    async def _step_instruments(self) -> tuple[str, str]:
        """Ensure the token map is loaded + persisted (reuse the 5d34e77 ladder): hydrate/refresh when
        empty; live-refresh + persist when a pre-login boot only had a stale hydrated snapshot and a
        session now exists; skip when a live dump is already loaded this process."""
        if self._instruments.is_empty:
            source = await hydrate_instruments_at_startup(
                self._instruments, self._store, self._kite,
                session_valid=self._session.token_valid(), clock=self._clock,
            )
            return ("ok", f"hydrate_ladder={source}")
        if self._instruments.hydrated and self._kite is not None and self._session.token_valid():
            # Booted pre-login on a stored (possibly stale) snapshot; a live session now exists ⇒
            # upgrade to today's live dump + persist so the next restart hydrates today's map.
            count = await self._instruments.refresh(self._kite)
            persisted = await self._store.arun(
                self._store.upsert_instruments_daily,
                self._instruments.snapshot_rows(self._clock.today()),
            )
            _log.info("instruments_refreshed_after_login", count=count, persisted=persisted)
            return ("ok", f"refreshed_after_login count={count} persisted={persisted}")
        return ("skipped", "already_live" if not self._instruments.hydrated else "hydrated_no_session")

    # ------------------------------------------------------------------ (b) backfill
    async def _step_backfill(self) -> tuple[str, str]:
        if self._backfill is None:
            return ("skipped", "no_backfill_no_kite_client")
        written = await regime_and_warmup_backfill(
            self._backfill, self._clock, self._calendar, self._settings,
            self._watchlist_symbols, self._index_symbol, self._vix_symbol,
        )
        return (
            "ok",
            f"regime_daily={written['regime_daily_bars']} warmup_gap={written['warmup_gap_bars']}",
        )

    # ------------------------------------------------------------------ (c) warm-up
    async def _step_warmup(self) -> tuple[str, str]:
        res = await self._lifecycle.reapply_warmup_gate()
        detail = f"outcome={res.outcome} ready={res.ready} lifted={res.lifted}"
        if res.blockers:
            detail += f" blockers={res.blockers[:4]}"
        return ("ok", detail)

    # ------------------------------------------------------------------ (d) ticker
    async def _step_ticker(self) -> tuple[str, str]:
        status = await resume_ticker(self._session, self._kite, self._ticker, self._ticker_tokens)
        return (("ok" if status == "started" else "skipped"), status)

    # ------------------------------------------------------------------ (e) holdings reconcile
    async def _step_holdings(self) -> tuple[str, str]:
        """§3.6 holdings reconcile — NON-load-bearing (see the module docstring): it never fails the
        ladder. The job swallows its own broker errors and reports them as ``error``, which is
        recorded here as a ``skipped`` step: "could not read the account" is not a recovery failure,
        and marking it one would put a red step in every pre-login-token report.

        ``observed`` (WO-D2) is the §3.6 journal rows this run actually wrote: ``checked`` on a
        trading day, 0 on any other (this ladder calls ``run`` unconditionally — only the hourly tick
        is window-gated) and 0 when the journal write itself failed and was swallowed. Reporting it
        says whether the "sold outside the ledger" evidence grew, which ``checked`` alone cannot."""
        if self._holdings_reconcile is None:
            return ("skipped", "not_wired")
        result = await self._holdings_reconcile.run()
        if result.error is not None:
            return ("skipped", f"error={result.error[:120]}")
        return (
            "ok",
            f"checked={result.checked} flagged={len(result.flagged)} "
            f"skipped_young={result.skipped_young} observed={result.observed}",
        )

    # ------------------------------------------------------------------ summary notify
    async def _emit_summary(self, report: PostLoginRecoveryReport) -> None:
        if self._notify is None:
            return
        msg = catalog.post_login_recovery(steps=[(s.name, s.status, s.detail) for s in report.steps])
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - the summary notify must never crash the recovery
            _log.exception("post_login_recovery_notify_failed")
