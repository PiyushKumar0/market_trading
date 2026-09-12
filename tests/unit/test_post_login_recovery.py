"""Post-login recovery — the §2.6 cold-start RE-TRIGGER (third cold-start-family defect, 2026-07-21).

Covers the three pieces the fix adds:

  * the SEAM — both login paths (the LAN ``/kite/callback`` route and the Telegram ``/token`` fallback)
    funnel through ``SessionManager.complete_login``, which fires the registered recovery hook once a
    token becomes valid;
  * the RECOVERY — :class:`PostLoginRecovery` re-runs the four broker-touching startup steps, each
    guarded (a raise marks that step failed + alerts, the others still run), idempotent on re-fire, and
    starts the ticker with the resolved subscription tokens;
  * the WARM-UP LIFT — ``SessionLifecycle.reapply_warmup_gate`` re-evaluates the SAME gate and lifts the
    warm-up FROZEN-for-entries only when coverage is met AND it is safe (kill clear, no other freeze).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from engine.broker.instruments import InstrumentStore
from engine.broker.session import SessionManager
from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_settings
from engine.core.enums import Actor, RiskState
from engine.marketdata.store import MarketStore
from engine.ops.holdings_reconcile import HoldingsReconcileResult
from engine.ops.lifecycle import WarmupReapply
from engine.ops.post_login import PostLoginRecovery, resume_ticker
from engine.ops.warmup import WarmupStatus
from tests.unit.test_instruments import INDIA_VIX_ROW, NIFTY50_ROW, RELIANCE_ROW, FakeKite
from tests.unit.test_lifecycle_selftest import OWNER_OK, _build


# --------------------------------------------------------------------------- fixtures / fakes
@pytest.fixture
def market_store(tmp_path, clock):
    """A hermetic tmp DuckDB store (never data/market.duckdb)."""
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def temp_config(tmp_path):
    """Hermetic protected-store config dir for the lifecycle-consequence tests (a fixture cannot cross
    module boundaries, so it is redefined locally — mirrors test_lifecycle_selftest / test_warmup_gate)."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    return cfg


class FakeSession:
    """Duck-typed SessionManager surface the recovery + resume consume."""

    def __init__(self, *, token: str = "tok-abc", valid: bool = True) -> None:
        self._token = token
        self._valid = valid

    def token_valid(self) -> bool:
        return self._valid

    def access_token(self) -> str | None:
        return self._token if self._valid else None


class FakeTicker:
    def __init__(self, state: str = "STOPPED") -> None:
        self._state = state
        self.started: tuple[list[int], str] | None = None

    def health(self):
        return SimpleNamespace(state=self._state)

    async def start(self, tokens, access_token) -> None:
        self.started = (list(tokens), access_token)
        self._state = "WARMING"


class FakeBackfill:
    def __init__(self, *, boom: bool = False) -> None:
        self.boom = boom
        self.run_calls: list[tuple[list[str], str]] = []
        self.gap_calls: list[list[str]] = []

    async def run(self, symbols, interval, start, end):
        if self.boom:
            raise RuntimeError("backfill exploded")
        self.run_calls.append((list(symbols), interval, start, end))
        return SimpleNamespace(bars_written=len(list(symbols)) * 10)

    async def warmup_gap(self, symbols, frm, to):
        self.gap_calls.append(list(symbols))
        return SimpleNamespace(bars_written=len(list(symbols)) * 5)


class FakeLifecycle:
    def __init__(self, reapply: WarmupReapply) -> None:
        self._reapply = reapply
        self.calls = 0

    async def reapply_warmup_gate(self) -> WarmupReapply:
        self.calls += 1
        return self._reapply


class _FakeGate:
    def __init__(self, ready: bool, blockers: list[str] | None = None) -> None:
        self._status = WarmupStatus(ready=ready, blockers=blockers or [])

    async def status(self) -> WarmupStatus:
        return self._status


def _mk_recovery(
    *, instruments, market_store, kite, session, backfill, ticker, lifecycle, clock, calendar,
    watch=("RELIANCE",), notify=None, alert=None, holdings_reconcile=None,
) -> PostLoginRecovery:
    def ticker_tokens() -> list[int]:
        out = []
        for sym in [*watch, "NIFTY 50", "INDIA VIX"]:
            tok = instruments.token_for_symbol(sym)
            if tok is not None:
                out.append(tok)
        return out

    return PostLoginRecovery(
        instruments=instruments, store=market_store, kite=kite, session=session, clock=clock,
        calendar=calendar, settings=load_settings(), backfill=backfill, ticker=ticker,
        lifecycle=lifecycle, ticker_tokens=ticker_tokens, watchlist_symbols=lambda: list(watch),
        index_symbol="NIFTY 50", vix_symbol="INDIA VIX", notify=notify, alert=alert,
        holdings_reconcile=holdings_reconcile,
    )


# --------------------------------------------------------------------------- the seam (both login paths)
class _FakeSecrets:
    def __init__(self) -> None:
        self._d: dict[str, str] = {}

    def get(self, k):
        return self._d.get(k, "x")

    def get_optional(self, k):
        return self._d.get(k)

    def set(self, k, v):
        self._d[k] = v

    def has(self, k):
        return k in self._d


class _FakeKC:
    def generate_session(self, request_token, api_secret):
        return {"access_token": "live-tok"}

    def set_access_token(self, token):
        pass


@pytest.mark.asyncio
async def test_login_hook_fires_recovery_on_complete_login(clock):
    """The RE-TRIGGER seam: ``complete_login`` (where BOTH login paths converge) makes the token valid
    AND fires every registered recovery hook, fire-and-forget."""
    session = SessionManager(_FakeSecrets(), clock)
    session._connect = lambda: _FakeKC()          # avoid the real network checksum exchange
    fired = asyncio.Event()

    async def hook() -> None:
        fired.set()

    session.add_login_hook(hook)
    assert session.token_valid() is False
    await session.complete_login("req-token")
    await asyncio.wait_for(fired.wait(), timeout=1.0)
    assert session.token_valid() is True          # token became valid at the same seam


def test_kite_callback_route_reaches_the_login_seam(clock):
    """Path A (the LAN redirect): GET /kite/callback drives ``complete_login`` — the seam that fires the
    recovery. (Path B, the Telegram /token fallback, lands in the same method by design.)"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from engine.api.kite_callback import build_kite_callback_router

    completed: dict[str, str] = {}

    class RouteSession:
        async def complete_login(self, request_token: str) -> None:
            completed["token"] = request_token

    app = FastAPI()
    app.include_router(build_kite_callback_router(RouteSession(), clock))
    client = TestClient(app)
    r = client.get("/kite/callback", params={"request_token": "abc123", "status": "success"})
    assert r.status_code == 200
    assert completed["token"] == "abc123"


# --------------------------------------------------------------------------- the recovery (four steps)
@pytest.mark.asyncio
async def test_recovery_refreshes_instruments_backfills_and_starts_ticker(market_store, clock, calendar):
    """Happy path from a pre-login boot: empty token map + a now-valid session ⇒ live refresh + persist,
    backfill runs, warm-up lifts, and the ticker starts with the resolved subscription tokens."""
    instruments = InstrumentStore(clock)                      # empty (pre-login boot, no snapshot)
    kite = FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW])
    ticker = FakeTicker()
    lifecycle = FakeLifecycle(WarmupReapply(ready=True, lifted=True, outcome="ready_lifted"))
    sent = []

    async def notify(m):
        sent.append(m)

    rec = _mk_recovery(instruments=instruments, market_store=market_store, kite=kite,
                       session=FakeSession(valid=True), backfill=FakeBackfill(), ticker=ticker,
                       lifecycle=lifecycle, clock=clock, calendar=calendar, notify=notify)
    report = await rec.run()

    assert report.ok is True and report.any_failed is False
    by = {s.name: s for s in report.steps}
    assert by["instruments"].status == "ok" and "startup_refresh" in by["instruments"].detail
    assert by["backfill"].status == "ok" and "regime_daily=20" in by["backfill"].detail
    assert by["warmup"].status == "ok" and "ready_lifted" in by["warmup"].detail
    assert by["ticker"].status == "ok" and by["ticker"].detail == "started"
    # Ticker started with the resolved subscription set (watchlist + NIFTY 50 + India VIX).
    assert ticker.started is not None
    tokens, access = ticker.started
    assert set(tokens) == {408065, 256265, 264969} and access == "tok-abc"
    # Exactly one info-severity summary reached the owner.
    assert len(sent) == 1
    assert str(sent[0].kind) == "post_login_recovery" and sent[0].severity == "info"


@pytest.mark.asyncio
async def test_step_failure_isolated_others_still_run(market_store, clock, calendar):
    """A step that RAISES marks itself failed + alerts, and every other step (incl. the later ticker
    start) still runs — a recovery step never crashes the loop."""
    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))   # already live
    ticker = FakeTicker()
    lifecycle = FakeLifecycle(WarmupReapply(ready=True, lifted=False, outcome="ready_already_normal"))
    alerts = []

    async def alert(sev, msg):
        alerts.append((sev, msg))

    rec = _mk_recovery(instruments=instruments, market_store=market_store,
                       kite=FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]),
                       session=FakeSession(valid=True), backfill=FakeBackfill(boom=True), ticker=ticker,
                       lifecycle=lifecycle, clock=clock, calendar=calendar, alert=alert)
    report = await rec.run()

    by = {s.name: s for s in report.steps}
    assert by["backfill"].status == "failed" and "RuntimeError" in by["backfill"].detail
    assert by["instruments"].status == "skipped" and by["instruments"].detail == "already_live"
    assert by["warmup"].status == "ok"
    assert by["ticker"].status == "ok" and ticker.started is not None   # ran AFTER the failed backfill
    assert report.any_failed is True and report.ok is False
    assert any("backfill" in msg for _sev, msg in alerts)


class _FakeHoldingsReconcile:
    """Duck-typed ``HoldingsReconcileJob.run`` surface — the step only reads the result."""

    def __init__(self, result: HoldingsReconcileResult) -> None:
        self._result = result
        self.calls = 0

    async def run(self) -> HoldingsReconcileResult:
        self.calls += 1
        return self._result


@pytest.mark.asyncio
async def test_holdings_step_reports_how_many_observations_were_journalled(
    market_store, clock, calendar
):
    """WO-D2 residue: ``observed`` is how many §3.6 journal rows the run actually WROTE, and this
    ladder calls the job unconditionally — only the hourly tick is window-gated. So checked=3 with
    observed=0 is a non-trading-day boot that grew no "sold outside the ledger" evidence, which
    ``checked`` alone cannot say."""
    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))

    async def _holdings_step(result: HoldingsReconcileResult):
        job = _FakeHoldingsReconcile(result)
        rec = _mk_recovery(
            instruments=instruments, market_store=market_store, kite=None,
            session=FakeSession(valid=True), backfill=FakeBackfill(), ticker=FakeTicker(),
            lifecycle=FakeLifecycle(WarmupReapply(ready=True, lifted=True, outcome="ready_lifted")),
            clock=clock, calendar=calendar, holdings_reconcile=job,
        )
        report = await rec.run()
        assert job.calls == 1                      # last in the ladder, and it ran
        return {s.name: s for s in report.steps}["holdings"]

    journalled = await _holdings_step(
        HoldingsReconcileResult(checked=3, flagged=["pos-1"], skipped_young=1, observed=3)
    )
    assert journalled.status == "ok"
    assert journalled.detail == "checked=3 flagged=1 skipped_young=1 observed=3"

    weekend = await _holdings_step(
        HoldingsReconcileResult(checked=3, flagged=[], skipped_young=0, observed=0)
    )
    assert weekend.status == "ok" and weekend.detail == "checked=3 flagged=0 skipped_young=0 observed=0"


# ------------------------------------ the ``completed`` gate (§2.6 early hydration, 2026-09-09)
# Login hooks are fire-and-forget TASKS (``SessionManager._fire_login_hooks`` creates one task per
# hook), so registration order gives the early-hydration chain NO ordering against this recovery.
# This event is the real dependency: SET at construction (an engine that never runs a recovery must
# not block the chain), CLEARED at the top of ``run`` before its first await, SET again in a
# ``finally`` when the ladder ends — success or not.


@pytest.mark.asyncio
async def test_completed_is_cleared_during_the_run_and_set_afterwards(market_store, clock, calendar):
    seen: list[bool] = []
    holder: dict = {}

    class _WatchingBackfill(FakeBackfill):
        async def run(self, symbols, interval, start, end):
            seen.append(holder["rec"].completed.is_set())
            return await super().run(symbols, interval, start, end)

    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    rec = _mk_recovery(
        instruments=instruments, market_store=market_store, kite=None,
        session=FakeSession(valid=True), backfill=_WatchingBackfill(), ticker=FakeTicker(),
        lifecycle=FakeLifecycle(WarmupReapply(ready=True, lifted=True, outcome="ready_lifted")),
        clock=clock, calendar=calendar,
    )
    holder["rec"] = rec

    assert rec.completed.is_set() is True        # a never-run recovery must not park the chain
    await rec.run()

    assert seen == [False]                       # cleared for the whole ladder, not just the top
    assert rec.completed.is_set() is True


@pytest.mark.asyncio
async def test_completed_is_set_even_when_a_step_raises(market_store, clock, calendar):
    """A ``finally``, not a happy-path line: a recovery that FAILS still releases the early-hydration
    chain — that chain needs no Kite session, and a permanently-cleared event would park it until its
    own 15-min timeout on every bad morning."""
    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    rec = _mk_recovery(
        instruments=instruments, market_store=market_store,
        kite=FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]),
        session=FakeSession(valid=True), backfill=FakeBackfill(boom=True), ticker=FakeTicker(),
        lifecycle=FakeLifecycle(
            WarmupReapply(ready=True, lifted=False, outcome="ready_already_normal")
        ),
        clock=clock, calendar=calendar,
    )

    report = await rec.run()

    assert report.any_failed is True
    assert rec.completed.is_set() is True


class _BlockingBackfill(FakeBackfill):
    """Blocks the FIRST ``run`` call on an externally controlled gate; every later call passes
    straight through — lets a test park one ``run()`` mid-ladder while a second overlaps it."""

    def __init__(self, gate: asyncio.Event) -> None:
        super().__init__()
        self._gate = gate
        self._blocked_once = False

    async def run(self, symbols, interval, start, end):
        if not self._blocked_once:
            self._blocked_once = True
            await self._gate.wait()
        return await super().run(symbols, interval, start, end)


@pytest.mark.asyncio
async def test_completed_stays_clear_until_both_overlapping_runs_finish(market_store, clock, calendar):
    """Depth-counted (2026-09-09 review): a bare Event set by whichever ``run()`` finishes FIRST would
    release the early-hydration chain while a second overlapping login's recovery is still mid-ladder.
    ``SessionManager._fire_login_hooks`` creates one task per hook per login, so two logins landing
    close together really do fire two concurrent ``run()`` calls — this is not a hypothetical race."""
    instruments = InstrumentStore(clock)
    await instruments.refresh(FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]))
    gate = asyncio.Event()
    rec = _mk_recovery(
        instruments=instruments, market_store=market_store,
        kite=FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]),
        session=FakeSession(valid=True), backfill=_BlockingBackfill(gate), ticker=FakeTicker(),
        lifecycle=FakeLifecycle(WarmupReapply(ready=True, lifted=True, outcome="ready_lifted")),
        clock=clock, calendar=calendar,
    )

    first = asyncio.create_task(rec.run())
    await asyncio.sleep(0.05)                        # first run() is parked in its backfill step
    assert rec.completed.is_set() is False

    second = asyncio.create_task(rec.run())
    second_report = await asyncio.wait_for(second, timeout=5)   # not blocked — finishes on its own
    assert second_report.ok is True
    # The SECOND run() finished, but the FIRST is still in flight (parked on the gate). A bare Event
    # would have been SET by the second run's own ``finally`` already; depth-counted, it must stay
    # clear until BOTH are done.
    assert rec.completed.is_set() is False

    gate.set()                                        # release the first run()'s backfill wait
    first_report = await asyncio.wait_for(first, timeout=5)
    assert first_report.ok is True
    assert rec.completed.is_set() is True


@pytest.mark.asyncio
async def test_idempotent_second_fire_skips_already_good(market_store, clock, calendar):
    """Safe to fire on every login: the second fire skips instruments (a live dump exists) and the ticker
    (already running), each with a log-worthy reason — no clobber, no double-start."""
    instruments = InstrumentStore(clock)
    ticker = FakeTicker()
    lifecycle = FakeLifecycle(WarmupReapply(ready=True, lifted=True, outcome="ready_lifted"))
    rec = _mk_recovery(instruments=instruments, market_store=market_store,
                       kite=FakeKite([RELIANCE_ROW, NIFTY50_ROW, INDIA_VIX_ROW]),
                       session=FakeSession(valid=True), backfill=FakeBackfill(), ticker=ticker,
                       lifecycle=lifecycle, clock=clock, calendar=calendar)

    first = await rec.run()
    assert first.ok is True
    assert {s.name: s.detail for s in first.steps}["ticker"] == "started"

    lifecycle._reapply = WarmupReapply(ready=True, lifted=False, outcome="ready_already_normal")
    second = await rec.run()
    by = {s.name: s for s in second.steps}
    assert by["instruments"].status == "skipped" and by["instruments"].detail == "already_live"
    assert by["ticker"].status == "skipped" and by["ticker"].detail == "already_running"
    assert second.ok is True


@pytest.mark.asyncio
async def test_ticker_skips_when_no_subscription_tokens(market_store, clock, calendar):
    """Degraded no-op: a valid session but an empty instrument dump ⇒ the ticker step SKIPS (no tokens),
    never starting a token-less feed; the backfill step skips when no Kite client is wired."""
    instruments = InstrumentStore(clock)
    ticker = FakeTicker()
    lifecycle = FakeLifecycle(WarmupReapply(ready=False, blockers=["orb:X 1/50"], outcome="frozen"))
    rec = _mk_recovery(instruments=instruments, market_store=market_store, kite=FakeKite([]),
                       session=FakeSession(valid=True), backfill=None, ticker=ticker,
                       lifecycle=lifecycle, clock=clock, calendar=calendar)
    report = await rec.run()

    by = {s.name: s for s in report.steps}
    assert by["instruments"].detail == "hydrate_ladder=startup_refresh"   # refresh ran (0 rows)
    assert by["backfill"].status == "skipped" and "no_backfill" in by["backfill"].detail
    assert by["ticker"].status == "skipped" and by["ticker"].detail == "skipped_no_tokens"
    assert ticker.started is None


@pytest.mark.asyncio
async def test_resume_ticker_skips_without_valid_token(clock):
    ticker = FakeTicker()
    status = await resume_ticker(FakeSession(valid=False), FakeKite([]), ticker, lambda: [1, 2, 3])
    assert status == "skipped_no_token"
    assert ticker.started is None


# --------------------------------------------------------------------------- day-leg end clamp (2026-07-29)
@pytest.mark.asyncio
async def test_day_backfill_end_clamps_to_yesterday_until_session_close(clock, calendar_fixture=None):
    """2026-07-29: a day-interval fetch through 'today' DURING the session returns today's RUNNING
    candle; writing it advances the observed-through checkpoint, so the evening daily_bars job then
    skips the day and a partial snapshot freezes as today's bar. The day leg must stop at yesterday
    until the session has closed — today's final bar is the evening job's business."""
    import datetime as _dt

    from engine.core.clock import IST, Clock
    from engine.ops.post_login import regime_and_warmup_backfill

    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    bf = FakeBackfill()
    # conftest clock is 2026-06-17 10:05 IST — mid-session on a trading day → clamp to yesterday.
    await regime_and_warmup_backfill(bf, clock, calendar, load_settings(),
                                     lambda: [], "NIFTY 50", "INDIA VIX")
    _syms, interval, _start, end = bf.run_calls[0]
    assert interval == "day"
    assert end == _dt.date(2026, 6, 16)

    # Post-close the same day: today's candle is final and fetchable → end is today.
    evening = Clock(time_source=lambda: _dt.datetime(2026, 6, 17, 19, 0, tzinfo=IST))
    cal2 = NSECalendar(config_dir() / "calendar", evening, strict=False)
    bf2 = FakeBackfill()
    await regime_and_warmup_backfill(bf2, evening, cal2, load_settings(),
                                     lambda: [], "NIFTY 50", "INDIA VIX")
    *_rest, end2 = bf2.run_calls[0]
    assert end2 == _dt.date(2026, 6, 17)


# --------------------------------------------------------------------------- warm-up gate reapply / lift
@pytest.mark.asyncio
async def test_reapply_lifts_warmup_freeze_when_ready(conn, clock, temp_config, monkeypatch):
    """§2.6 step-6 reopen: after a warm-up freeze, once coverage is met the reapply lifts FROZEN→NORMAL
    through the SAME risk-state seam startup uses (never a bypass)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, store, lifecycle = _build(conn, clock, temp_config)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:RELIANCE bars 3/50"])
    await lifecycle.startup(check_skew=False)
    assert mode.risk_state() == RiskState.FROZEN

    lifecycle._warmup_gate = _FakeGate(ready=True)
    res = await lifecycle.reapply_warmup_gate()
    assert res.ready is True and res.lifted is True and res.outcome == "ready_lifted"
    assert mode.risk_state() == RiskState.NORMAL


@pytest.mark.asyncio
async def test_reapply_holds_lift_when_another_freeze_stands(conn, clock, temp_config, monkeypatch):
    """Safety: a still-standing NON-warm-up freeze (here a protected-store integrity failure) is
    respected — the warm-up reopen must never clear a warranted freeze."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mode, _kill, _store, lifecycle = _build(conn, clock, temp_config)   # protected store NOT registered
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:X 1/50"])
    await lifecycle.startup(check_skew=False)
    assert mode.risk_state() == RiskState.FROZEN

    lifecycle._warmup_gate = _FakeGate(ready=True)
    res = await lifecycle.reapply_warmup_gate()
    assert res.ready is True and res.lifted is False and res.outcome == "ready_other_freeze"
    assert mode.risk_state() == RiskState.FROZEN


@pytest.mark.asyncio
async def test_reapply_does_not_lift_when_killed(conn, clock, temp_config, monkeypatch):
    """A latched kill is never overridden by a warm-up reopen (R3/R5)."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _mode, kill, store, lifecycle = _build(conn, clock, temp_config)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    await kill.trigger("owner kill", actor=Actor.OWNER, flatten=False)

    lifecycle._warmup_gate = _FakeGate(ready=True)
    res = await lifecycle.reapply_warmup_gate()
    assert res.lifted is False and res.outcome == "ready_kill_held"


@pytest.mark.asyncio
async def test_reapply_freezes_entries_when_not_ready(conn, clock, temp_config, monkeypatch):
    """Coverage still short on re-evaluation ⇒ FROZEN-for-entries + a WARMUP_FROZEN alert (same as
    startup step 6): never trade on thin data."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    sent = []

    async def notify(m):
        sent.append(m)

    mode, _kill, store, lifecycle = _build(conn, clock, temp_config, notify=notify)
    store.register_initial("limits.yaml", OWNER_OK)
    store.register_initial("envelope.yaml", OWNER_OK)
    lifecycle._warmup_gate = _FakeGate(ready=False, blockers=["orb:Y 2/50"])
    assert mode.risk_state() == RiskState.NORMAL

    res = await lifecycle.reapply_warmup_gate()
    assert res.ready is False and res.outcome == "frozen" and res.froze is True
    assert mode.risk_state() == RiskState.FROZEN
    assert any(str(m.kind) == "warmup_frozen" for m in sent)
