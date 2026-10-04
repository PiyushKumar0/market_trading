"""Plan §9.4 chaos case 18 — Cold start TOO CLOSE to the window (warm-up insufficient).

Must hold (§9.4 row 18, verbatim): "`warmup_ready` FROZEN-for-entries + alert; backfill attempted from
official candles; entries open only once lookbacks are covered — never trades on thin data (§2.6 risk;
§14 intraday-candle-latency). **Per COVERAGE CLASS since 2026-09-13 (§2.6 step-6 addendum): a cold start
whose only shortfall is today's 1-minute coverage refuses INTRADAY candidates per-candidate instead of
freezing entries; a daily/regime shortfall (the offline-overnight case) still freezes**".

Scenario: the engine was offline overnight (the 18:05 ``daily_bars`` job never ran for Tue 06-16) and
cold-starts at 09:55 IST on Wed 2026-06-17, five minutes before the 10:00 trade window, with no live
minute bars yet — and Kite's official historical candles are unavailable at boot (§14 candle latency /
a 503). Composed as ``engine.ops.main`` wires it: the real ``SessionLifecycle.startup`` whose step-4
``backfill_hook`` is the real ``regime_and_warmup_backfill`` over a real ``BackfillJob`` → real
``KiteClient``; the real ``WarmupGate`` over a real temp ``MarketStore`` holding real bars; the §3.5.3
cause ledger; the 60 s ``warmup_status_refresh`` body (``refresh_and_lift_warmup`` +
``maybe_repair_warmup_gaps``, both from ``engine.ops.main``); and the §7.1 entry gate reading the same
warm-up snapshot. Faked: only pykiteconnect's ``historical_data`` (the Kite candle edge) and the
Telegram sink.

Clauses (see "resolved ambiguity" for DAILY):
* REGIME shortfall ⇒ FROZEN + alert; backfill attempted; entries open only once covered —
  ``test_regime_shortfall_freezes_until_official_candles_cover_it``.
* INTRADAY-only shortfall ⇒ per-candidate (per-symbol) refusal, no freeze; opens once covered —
  ``test_intraday_only_shortfall_refuses_intraday_candidates_without_freezing``.
* DAILY-only shortfall ⇒ per-symbol refusal of that symbol's swing candidates, no freeze —
  ``test_daily_only_shortfall_refuses_that_symbols_swing_candidates_without_freezing``.

Phase-3-gated: none.

Resolved ambiguity: the row text (written 2026-09-13) says "a daily/regime shortfall ... still
freezes"; the later §2.6 per-SYMBOL addendum (owner-directed 2026-09-17, plan §2.6 step 6 and the
§7.1 ``warmup_ready`` row) narrowed the freezing set to REGIME ∪ unattributable and moved DAILY to
per-symbol refusal. The later owner directive wins: REGIME freezes, DAILY does not.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from kiteconnect.exceptions import NetworkException

import engine.broker.rate_limiter as rate_limiter_mod
from engine.broker.kite_client import KiteClient
from engine.broker.rate_limiter import RateLimiter
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.core.enums import Actor, Mode, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import REQUIRED_AT_STARTUP
from engine.core.types import Bar
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.store import DailyBar, MarketStore
from engine.notify.catalog import MessageKind
from engine.ops.jobs import CatchUpRunner
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.main import INDEX_SYMBOL, VIX_SYMBOL, maybe_repair_warmup_gaps, refresh_and_lift_warmup
from engine.ops.post_login import regime_and_warmup_backfill
from engine.ops.selftest import SelfTest
from engine.ops.warmup import DAILY_LOOKBACK_SESSIONS, WarmupGate, WarmupStatus, recent_sessions
from engine.risk.causes import RiskStateLatch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager
from tests.chaos._entry_gate_rig import build_entry_gate, entry_checks
from tests.unit.test_lifecycle_selftest import OWNER_OK, FakeSecrets

TODAY = date(2026, 6, 17)                                    # Wed, a trading day
BOOT_AT = datetime(2026, 6, 17, 9, 55, tzinfo=IST)           # 5 min before the 10:00 window
WINDOW_OPEN = datetime(2026, 6, 17, 10, 1, tzinfo=IST)
MISSED_SESSION = date(2026, 6, 16)                           # the day whose EOD jobs never ran
WATCH = ["RELIANCE", "TCS"]
TOKENS = {"RELIANCE": 738561, "TCS": 2953217, INDEX_SYMBOL: 256265, VIX_SYMBOL: 264969}
#: main.py `_WARMUP_UNREFRESHED`: the fail-closed snapshot the gate reads until the first refresh.
UNREFRESHED = WarmupStatus(ready=False, blockers=["warmup:unrefreshed 0/0", "regime:unrefreshed 0/0"])


class _Now:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class _FakeKiteHistorical:
    """pykiteconnect ``KiteConnect.historical_data`` — the official-candle edge. ``down`` = Kite's
    historical service 503s (or has not published yet, §14). Up, it serves one day candle per NSE
    session strictly before today, and one minute candle per CLOSED session minute (never the
    still-forming one — the §14 latency)."""

    def __init__(self, clock: Clock, calendar: NSECalendar) -> None:
        self.clock, self.calendar = clock, calendar
        self.down = True
        self.calls: list[tuple[str, str]] = []
        self._symbol = {tok: sym for sym, tok in TOKENS.items()}

    def historical_data(self, instrument_token, from_date, to_date, interval):
        self.calls.append((self._symbol[instrument_token], interval))
        if self.down:
            raise NetworkException("historical candles unavailable (503)")
        now = self.clock.now()
        if interval == "day":
            out, d = [], from_date.date()
            while d <= min(to_date.date(), now.date() - timedelta(days=1)):
                if self.calendar.is_trading_day(d):
                    out.append(_candle(datetime.combine(d, time(0, 0), tzinfo=IST)))
                d += timedelta(days=1)
            return out
        out, m = [], from_date.astimezone(IST).replace(second=0, microsecond=0)
        last_closed = now.replace(second=0, microsecond=0)
        session = self.calendar.session(m.date())
        while m < to_date.astimezone(IST) and m < last_closed:
            if session is not None and session.open <= m < session.close:
                out.append(_candle(m))
            m += timedelta(minutes=1)
        return out


def _candle(ts: datetime) -> dict:
    return {"date": ts, "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5, "volume": 1000}


def _daily(symbol: str, days) -> list[DailyBar]:
    return [DailyBar(symbol=symbol, d=d, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
                     close=Decimal("100"), volume=1000) for d in days]


def _live_minutes(symbol: str, start: datetime, end: datetime) -> list[Bar]:
    """What the live tick → BarBuilder path writes (``src='self'``) for ``[start, end)``."""
    out, m = [], start
    while m < end:
        out.append(Bar(symbol=symbol, ts_minute=m, open=Decimal("100"), high=Decimal("100.5"),
                       low=Decimal("99.5"), close=Decimal("100.2"), volume=500, src="self"))
        m += timedelta(minutes=1)
    return out


@pytest.fixture
def warm_rig(conn, tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("MT_DATA_DIR", str(tmp_path / "data"))
    settings = load_settings()
    now = _Now(BOOT_AT)
    clock = Clock(time_source=now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    mode = ModeManager(conn, clock, None, calendar)
    kill = KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_text("schema_version: 1\nlimits: {}\n", encoding="utf-8")
    (cfg / "envelope.yaml").write_text("schema_version: 1\nparameters: {}\n", encoding="utf-8")
    protected = ProtectedStore(cfg, conn, clock)
    protected.register_initial("limits.yaml", OWNER_OK)
    protected.register_initial("envelope.yaml", OWNER_OK)

    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    kc = _FakeKiteHistorical(clock, calendar)
    # A2 pacing is not this case's subject, and the scenario clock only moves when the test moves it,
    # so the 3-token historical bucket would never refill: widen the burst (pacing is unit-tested).
    monkeypatch.setattr(rate_limiter_mod, "_HISTORICAL_BURST", 10_000)
    kite = KiteClient(kc, RateLimiter(clock, burst=10_000), clock)
    backfill = BackfillJob(store, kite, clock, settings, conn, TOKENS.get)
    warmup_gate = WarmupGate(store, clock, calendar, symbols=WATCH,
                             index_symbol=INDEX_SYMBOL, vix_symbol=VIX_SYMBOL)
    notified: list = []
    alerts: list[tuple[str, str]] = []

    async def notify(msg) -> None:
        notified.append(msg)

    async def alert(severity: str, message: str) -> None:
        alerts.append((severity, message))

    async def backfill_hook() -> None:           # main.py `backfill_hook` (token valid ⇒ not skipped)
        await regime_and_warmup_backfill(backfill, clock, calendar, settings, lambda: list(WATCH),
                                         INDEX_SYMBOL, VIX_SYMBOL)

    self_test = SelfTest(conn=conn, clock=clock, settings=settings, secrets=FakeSecrets(REQUIRED_AT_STARTUP),
                         protected_store=protected, kill_switch=kill, mode_manager=mode, latch=latch,
                         calendar=calendar)
    lifecycle = SessionLifecycle(
        conn=conn, clock=clock, calendar=calendar, settings=settings, mode_manager=mode, kill_switch=kill,
        self_test=self_test, catch_up=CatchUpRunner(conn, clock, calendar), alert=alert, notify=notify,
        build_version="chaos-18", warmup_gate=warmup_gate, latch=latch, backfill_hook=backfill_hook,
    )
    holder: dict = {"status": None}
    builder, gate = build_entry_gate(
        conn=conn, clock=clock, calendar=calendar, mode=mode, kill=kill, store=store,
        warmup_status_fn=lambda: holder["status"] or UNREFRESHED, clock_skew_ok_fn=lambda: True,
    )
    repair_state: dict = {}

    async def warmup_refresh() -> None:          # main.py `warmup_refresh` (the 60 s job body)
        await refresh_and_lift_warmup(warmup_gate, holder, mode, lifecycle, alert=alert, clock=clock)
        status = holder.get("status")
        if status is not None:
            await maybe_repair_warmup_gaps(
                status, repair_state, clock=clock, calendar=calendar,
                repair=lambda frm, to: backfill.warmup_gap(list(WATCH), frm, to, confirm_until=to),
                token_valid=lambda: True,
            )

    async def checks(symbol: str, style: str = "intraday"):
        return await entry_checks(builder, gate, clock, symbol, style=style)

    sessions = recent_sessions(calendar, TODAY, DAILY_LOOKBACK_SESSIONS)
    assert sessions is not None and sessions[0] == MISSED_SESSION
    rig = SimpleNamespace(now=now, clock=clock, mode=mode, latch=latch, store=store, kc=kc,
                          lifecycle=lifecycle, notified=notified, alerts=alerts, holder=holder,
                          warmup_refresh=warmup_refresh, checks=checks, sessions=sessions,
                          backfill_hook=backfill_hook)

    async def boot():
        await mode.request_transition(Mode.RECOMMEND, Actor.OWNER)
        report = await lifecycle.startup(check_skew=False)
        await warmup_refresh()                   # main.py seeds the gate's snapshot right after boot
        return report

    rig.boot = boot
    yield rig
    store.close()


def _seed_history(rig, *, regime_missing_last: bool, tcs_daily_hole: int = 0) -> None:
    """Watchlist + regime daily history as an engine that was off overnight would find it."""
    for sym in WATCH:
        days = list(rig.sessions)
        if sym == "TCS" and tcs_daily_hole:
            del days[50:50 + tcs_daily_hole]            # an OLD stock with an interior gap
        rig.store.upsert_bars_1d(_daily(sym, days))
    regime = rig.sessions[1:] if regime_missing_last else rig.sessions
    rig.store.upsert_bars_1d(_daily(INDEX_SYMBOL, regime))
    rig.store.upsert_bars_1d(_daily(VIX_SYMBOL, regime))


def _kinds(rig) -> list[str]:
    return [str(m.kind) for m in rig.notified]


# ------------------------------------------------------------- REGIME shortfall: freeze + alert
async def test_regime_shortfall_freezes_until_official_candles_cover_it(warm_rig):
    rig = warm_rig
    _seed_history(rig, regime_missing_last=True)          # NIFTY 50 / INDIA VIX lack Tue 06-16

    report = await rig.boot()

    # Backfill ATTEMPTED from official candles at boot (step 4) — regime daily + today's minutes.
    assert {(INDEX_SYMBOL, "day"), (VIX_SYMBOL, "day"), ("RELIANCE", "minute"), ("TCS", "minute")} <= set(rig.kc.calls)
    # warmup_ready FROZEN-for-entries through its own §3.5.3 cause ...
    assert "warmup_ready" in report.frozen_reasons
    assert report.warmup_classes_short == ["intraday", "regime"]
    assert rig.mode.risk_state() == RiskState.FROZEN
    assert [c for c, _s, _d in rig.latch.active_causes()] == ["warmup_ready"]
    # ... + alert: WARMUP_FROZEN leads with the class that froze entries and names the blockers.
    frozen_msg = next(m for m in rig.notified if m.kind is MessageKind.WARMUP_FROZEN)
    assert frozen_msg.data["classes"] == ["intraday", "regime"]
    assert "regime:NIFTY 50 daily bars 199/200" in frozen_msg.data["blockers"]
    assert "regime:INDIA VIX daily bars 19/20" in frozen_msg.data["blockers"]

    # The window opens while still short: every entry is refused — never on thin data.
    rig.now.at = WINDOW_OPEN
    await rig.warmup_refresh()                            # Kite still down: the lift must NOT happen
    assert rig.mode.risk_state() == RiskState.FROZEN
    for sym in WATCH:
        verdict, checks = await rig.checks(sym)
        assert verdict == "reject"
        assert not checks["mode_risk_state"].passed
        assert not checks["regime_data_ready"].passed
        assert not checks["warmup_ready"].passed

    # Kite's candles become available; the shared step-4 / post-login official-candle backfill is
    # re-attempted, and the next 60 s refresh lifts — entries open only once coverage is complete.
    rig.kc.down = False
    rig.now.at = WINDOW_OPEN + timedelta(minutes=1)
    await rig.backfill_hook()
    await rig.warmup_refresh()
    assert rig.holder["status"].ready is True
    assert rig.mode.risk_state() == RiskState.NORMAL
    assert rig.latch.active_causes() == []                # no latch left behind
    for sym in WATCH:
        _verdict, checks = await rig.checks(sym)
        assert checks["mode_risk_state"].passed
        assert checks["regime_data_ready"].passed
        assert checks["warmup_ready"].passed


# ------------------------------------------------------- INTRADAY-only shortfall: per candidate
async def test_intraday_only_shortfall_refuses_intraday_candidates_without_freezing(warm_rig):
    rig = warm_rig
    _seed_history(rig, regime_missing_last=False)         # every daily/regime lookback covered
    session_open = datetime(2026, 6, 17, 9, 15, tzinfo=IST)
    # TCS's live bars have been flowing since the open; RELIANCE has none (its ticks never arrived).
    rig.store.insert_bars_1m(_live_minutes("TCS", session_open, BOOT_AT))

    report = await rig.boot()

    assert report.frozen_reasons == [] and rig.mode.risk_state() == RiskState.NORMAL   # NOT frozen
    assert report.warmup_classes_short == ["intraday"]
    assert MessageKind.WARMUP_FROZEN.value not in _kinds(rig)
    startup = next(m for m in rig.notified if m.kind is MessageKind.STARTUP_REPORT)
    assert "warm-up short: intraday (orb:RELIANCE bars 0/40)" in startup.body     # never silent
    assert ("RELIANCE", "minute") in rig.kc.calls         # the gap fill WAS attempted (Kite down)

    rig.now.at = WINDOW_OPEN
    rig.store.insert_bars_1m(_live_minutes("TCS", BOOT_AT, WINDOW_OPEN))
    await rig.warmup_refresh()
    # Per candidate AND per symbol: RELIANCE's intraday entry is refused; its swing entry (daily
    # class covered) and TCS's intraday entry are not refused on warm-up grounds.
    _v, rel_intraday = await rig.checks("RELIANCE", "intraday")
    _v, rel_swing = await rig.checks("RELIANCE", "swing")
    _v, tcs_intraday = await rig.checks("TCS", "intraday")
    assert not rel_intraday["warmup_ready"].passed
    assert rel_swing["warmup_ready"].passed and rel_swing["regime_data_ready"].passed
    assert tcs_intraday["warmup_ready"].passed
    for c in (rel_intraday, rel_swing, tcs_intraday):
        assert c["mode_risk_state"].passed                # the book was never frozen

    # Kite recovers; the in-session gap self-repair (warmup_refresh → maybe_repair_warmup_gaps)
    # fills RELIANCE's minutes from official candles up to now−2 min, the live feed owns the tail,
    # and the NEXT refresh sees full coverage: RELIANCE's intraday entries open.
    rig.kc.down = False
    rig.now.at = WINDOW_OPEN + timedelta(minutes=6)       # past the repair cooldown
    rig.store.insert_bars_1m(_live_minutes("TCS", WINDOW_OPEN, rig.now.at))     # TCS kept ticking
    await rig.warmup_refresh()
    repaired_to = rig.now.at.replace(second=0) - timedelta(minutes=2)
    rel = rig.store.get_bars_1m("RELIANCE", session_open, repaired_to)
    assert len(rel) == 50 and {b.src for b in rel} == {"gap_backfilled"}      # official candles
    # RELIANCE's own ticks resume: the live bar builder owns the 2-minute tail the repair leaves.
    rig.store.insert_bars_1m(_live_minutes("RELIANCE", repaired_to, rig.now.at))
    await rig.warmup_refresh()
    assert rig.holder["status"].ready is True
    _v, rel_intraday = await rig.checks("RELIANCE", "intraday")
    assert rel_intraday["warmup_ready"].passed


# ---------------------------------------------------------- DAILY-only shortfall: per candidate
async def test_daily_only_shortfall_refuses_that_symbols_swing_candidates_without_freezing(warm_rig):
    rig = warm_rig
    _seed_history(rig, regime_missing_last=False, tcs_daily_hole=7)   # TCS 193/200, an OLD stock
    session_open = datetime(2026, 6, 17, 9, 15, tzinfo=IST)
    for sym in WATCH:
        rig.store.insert_bars_1m(_live_minutes(sym, session_open, BOOT_AT))

    report = await rig.boot()

    assert report.frozen_reasons == [] and rig.mode.risk_state() == RiskState.NORMAL
    assert report.warmup_classes_short == ["daily"]
    assert report.warmup_blockers == ["rsi2/trend/mom:TCS daily bars 193/200"]
    assert MessageKind.WARMUP_FROZEN.value not in _kinds(rig)
    assert ("TCS", "day") in rig.kc.calls                 # daily-gap repair attempted (Kite down)

    rig.now.at = WINDOW_OPEN
    for sym in WATCH:
        rig.store.insert_bars_1m(_live_minutes(sym, BOOT_AT, WINDOW_OPEN))
    await rig.warmup_refresh()
    _v, tcs_swing = await rig.checks("TCS", "swing")
    _v, rel_swing = await rig.checks("RELIANCE", "swing")
    _v, tcs_intraday = await rig.checks("TCS", "intraday")
    assert not tcs_swing["warmup_ready"].passed           # TCS's daily hole refuses TCS swing only
    assert rel_swing["warmup_ready"].passed
    assert tcs_intraday["warmup_ready"].passed
    assert all(c["mode_risk_state"].passed for c in (tcs_swing, rel_swing, tcs_intraday))

    # Kite recovers; the shared official-candle backfill's watchlist daily leg repairs the hole.
    rig.kc.down = False
    await rig.backfill_hook()
    await rig.warmup_refresh()
    _v, tcs_swing = await rig.checks("TCS", "swing")
    assert tcs_swing["warmup_ready"].passed
