"""``LiveScanContextProvider`` (§3.2.5) — the live ``context_provider`` seam.

Covers the :class:`ScanContext` contract it must fill (intraday series with the scanned bar last,
daily/index/flagged/ex-date/momentum context, calendar window), the §3.2 hot-path read budget (ONE
``get_bars_1d`` per symbol per day, one flagged/corp-action read per day, rebuilt on date change,
nothing read at construction), and its degradation rules (out-of-order + redelivered bars, snapshot
minting failure, non-trading date).
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.features.engine import FeatureEngine
from engine.features.snapshots import load_snapshot
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops.scan_context import LiveScanContextProvider
from engine.strategy.prescreen import SignalPreScreen
from engine.strategy.scanners import MomentumScanner
from engine.strategy.scanners.base import Scanner
from tests.conftest import FIXED_NOW

D = FIXED_NOW.date()                      # 2026-06-17 (Wed), a real trading day (conftest)
PREV_D = date(2026, 6, 16)                # Tue — D's prior trading session (flagged reads anchor here)
NEXT_D = date(2026, 6, 18)                # Thu, also a trading day
SUNDAY = date(2026, 6, 21)                # not a trading day (weekend, no muhurat) — R6
SESSION_OPEN = datetime(2026, 6, 17, 9, 15, tzinfo=IST)


# --------------------------------------------------------------------------- spies / stubs
class SpyStore(MarketStore):
    """MarketStore that records every read the provider makes (read-budget assertions)."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.daily_reads: list[str] = []
        self.minute_reads: list[str] = []
        self.flagged_reads: list[date] = []
        self.corp_action_reads: int = 0

    def get_bars_1d(self, symbol, start, end):        # noqa: ANN001, ANN201 - spy passthrough
        self.daily_reads.append(symbol)
        return super().get_bars_1d(symbol, start, end)

    def get_bars_1m(self, symbol, start, end):        # noqa: ANN001, ANN201
        self.minute_reads.append(symbol)
        return super().get_bars_1m(symbol, start, end)

    def get_flagged_instrument_days(self, d):         # noqa: ANN001, ANN201
        self.flagged_reads.append(d)
        return super().get_flagged_instrument_days(d)

    def get_corp_actions(self, **kwargs):             # noqa: ANN201
        self.corp_action_reads += 1
        return super().get_corp_actions(**kwargs)


class StubFeatures:
    """Minimal ``FeatureEngine`` stand-in: the provider only reads ``features_snapshot_id``."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    def intraday_snapshot(self, symbol: str):         # noqa: ANN201 - duck-typed FeatureVector
        self.calls.append(symbol)
        if self.fail:
            raise RuntimeError("duckdb read failed mid-session")

        class _V:
            features_snapshot_id = f"snap-{symbol}-{len(self.calls)}"

        return _V()


# --------------------------------------------------------------------------- fixtures
@pytest.fixture
def store(tmp_path, clock) -> SpyStore:
    s = SpyStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock)


@pytest.fixture
def features() -> StubFeatures:
    return StubFeatures()


@pytest.fixture
def provider(store, clock, calendar, features) -> LiveScanContextProvider:
    return LiveScanContextProvider(store, clock, calendar, features)


# --------------------------------------------------------------------------- synthetic data
def _weekdays_back(end: date, n: int) -> list[date]:
    """The last ``n`` weekdays ending at ``end`` inclusive, ascending (bars_1d needs no calendar)."""
    days: list[date] = []
    probe = end
    while len(days) < n:
        if probe.weekday() < 5:
            days.append(probe)
        probe -= timedelta(days=1)
    return list(reversed(days))


def _seed_daily(store: MarketStore, symbol: str, days: list[date], base: float, drift: float):
    bars = [
        DailyBar(
            symbol=symbol, d=dd,
            open=Decimal(f"{base + drift * i:.2f}"),
            high=Decimal(f"{base + drift * i + 1:.2f}"),
            low=Decimal(f"{base + drift * i - 1:.2f}"),
            close=Decimal(f"{base + drift * i:.2f}"),
            volume=10_000 + i,
        )
        for i, dd in enumerate(days)
    ]
    store.upsert_bars_1d(bars)
    store.daily_reads.clear()          # seeding is not a provider read
    return bars


def _bar(symbol: str = "AAA", *, d: date = D, hh: int = 9, mm: int = 20, close: str = "100.00") -> Bar:
    return Bar(
        symbol=symbol, ts_minute=datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST),
        open=Decimal("99.00"), high=Decimal("101.00"), low=Decimal("98.00"),
        close=Decimal(close), volume=1_000,
    )


def _seed_intraday(store: MarketStore, symbol: str, minutes: range) -> list[Bar]:
    bars = [_bar(symbol, hh=9, mm=m, close=f"{100 + m}.00") for m in minutes]
    store.insert_bars_1m(bars)
    store.minute_reads.clear()
    return bars


# --------------------------------------------------------------------------- construction
def test_no_store_read_at_construction(store, clock, calendar, features):
    LiveScanContextProvider(store, clock, calendar, features)
    assert store.daily_reads == [] and store.minute_reads == [] and store.flagged_reads == []
    assert store.corp_action_reads == 0


def test_constructor_validation(store, clock, calendar, features):
    with pytest.raises(ValueError):
        LiveScanContextProvider(store, clock, calendar, features, momentum_weeks=0)
    with pytest.raises(ValueError):
        LiveScanContextProvider(store, clock, calendar, features, daily_lookback_days=0)
    with pytest.raises(ValueError):
        LiveScanContextProvider(store, clock, calendar, features, ex_horizon_days=-1)


# --------------------------------------------------------------------------- intraday series
def test_intraday_seeded_from_store_with_current_bar_last(store, provider):
    _seed_intraday(store, "AAA", range(15, 20))          # 09:15..09:19 already persisted
    incoming = _bar(mm=20, close="120.00")

    ctx = provider(incoming)

    ts = [b.ts_minute for b in ctx.intraday_bars]
    assert ts == sorted(ts)                              # ascending (§3.2.5 contract)
    assert len(ctx.intraday_bars) == 6                   # 5 seeded + the scanned bar
    assert ctx.intraday_bars[-1].ts_minute == incoming.ts_minute
    assert ctx.intraday_bars[-1].close == incoming.close
    assert store.minute_reads == ["AAA"]                 # exactly one seed read for the symbol

    # Subsequent bars append in memory — never another store read.
    ctx2 = provider(_bar(mm=21, close="121.00"))
    assert len(ctx2.intraday_bars) == 7
    assert ctx2.intraday_bars[-1].ts_minute == datetime(2026, 6, 17, 9, 21, tzinfo=IST)
    assert store.minute_reads == ["AAA"]


def test_intraday_seed_skipped_at_session_open(store, provider):
    ctx = provider(_bar(hh=9, mm=15))
    assert [b.ts_minute for b in ctx.intraday_bars] == [SESSION_OPEN]
    assert store.minute_reads == []                      # nothing to seed at the open


def test_duplicate_bar_redelivery_replaces_last(store, provider):
    provider(_bar(mm=20, close="100.00"))
    ctx = provider(_bar(mm=20, close="103.50"))          # same minute re-delivered (late-tick amend)
    assert len(ctx.intraday_bars) == 1
    assert ctx.intraday_bars[-1].close == Decimal("103.50")


def test_out_of_order_bar_ignored(store, provider):
    provider(_bar(mm=20, close="100.00"))
    provider(_bar(mm=21, close="101.00"))
    ctx = provider(_bar(mm=20, close="999.00"))          # stale delivery
    assert [b.close for b in ctx.intraday_bars] == [Decimal("100.00"), Decimal("101.00")]
    # The scanned bar is NOT last ⇒ intraday scanners fail to zero, which is the intended degradation.
    assert ctx.intraday_bars[-1].ts_minute == datetime(2026, 6, 17, 9, 21, tzinfo=IST)


def test_intraday_series_is_per_symbol(store, provider):
    provider(_bar("AAA", mm=20))
    provider(_bar("AAA", mm=21))
    ctx = provider(_bar("BBB", mm=21))
    assert len(ctx.intraday_bars) == 1 and ctx.intraday_bars[0].symbol == "BBB"


# --------------------------------------------------------------------------- daily cache / read budget
def test_one_daily_read_per_symbol_per_day(store, provider):
    _seed_daily(store, "AAA", _weekdays_back(D - timedelta(days=1), 30), 100.0, 0.5)
    _seed_daily(store, "BBB", _weekdays_back(D - timedelta(days=1), 30), 200.0, -0.5)

    for mm in range(20, 25):
        provider(_bar("AAA", mm=mm))
        provider(_bar("BBB", mm=mm))

    # 10 bars, 2 symbols: one bars_1d read each + one for the index, ALL on the first bar of the day.
    assert store.daily_reads.count("AAA") == 1
    assert store.daily_reads.count("BBB") == 1
    assert store.daily_reads.count("NIFTY 50") == 1
    assert store.flagged_reads == [PREV_D]               # prior session (2026-08-18), once per day
    assert store.corp_action_reads == 1


def test_daily_bars_stop_at_the_prior_session(store, provider):
    days = _weekdays_back(D - timedelta(days=1), 30)
    _seed_daily(store, "AAA", days, 100.0, 0.5)
    _seed_daily(store, "AAA", [D], 500.0, 0.0)           # today's row must never leak in

    ctx = provider(_bar("AAA"))
    assert [b.d for b in ctx.daily_bars] == days
    assert ctx.daily_bars[-1].d == days[-1] < D


def test_symbol_first_seen_mid_day_loads_lazily_once(store, provider):
    _seed_daily(store, "AAA", _weekdays_back(D - timedelta(days=1), 30), 100.0, 0.5)
    _seed_daily(store, "CCC", _weekdays_back(D - timedelta(days=1), 30), 300.0, 1.0)

    provider(_bar("AAA", mm=20))
    assert "CCC" not in store.daily_reads
    provider(_bar("CCC", mm=21))
    provider(_bar("CCC", mm=22))
    assert store.daily_reads.count("CCC") == 1


def test_day_cache_rebuilt_on_date_change(store, provider):
    _seed_daily(store, "AAA", _weekdays_back(D - timedelta(days=1), 30), 100.0, 0.5)

    provider(_bar("AAA", d=D, mm=20))
    provider(_bar("AAA", d=D, mm=21))
    assert store.daily_reads.count("AAA") == 1

    ctx = provider(_bar("AAA", d=NEXT_D, mm=20))
    assert store.daily_reads.count("AAA") == 2           # rebuilt for the new day
    assert store.daily_reads.count("NIFTY 50") == 2
    assert store.flagged_reads == [PREV_D, D]            # each day reads ITS prior session's flags
    assert store.corp_action_reads == 2
    assert ctx.session_open == datetime(2026, 6, 18, 9, 15, tzinfo=IST)
    assert len(ctx.intraday_bars) == 1                   # yesterday's series does not survive rollover


# --------------------------------------------------------------------------- momentum
def test_momentum_present_for_cached_symbols(store, provider):
    days = _weekdays_back(D - timedelta(days=1), 30)
    aaa = _seed_daily(store, "AAA", days, 100.0, 0.5)
    _seed_daily(store, "BBB", days, 200.0, -0.5)

    ctx = provider(_bar("AAA"))
    assert set(ctx.momentum_by_symbol) == {"AAA"}        # cross-section fills in as symbols tick
    expected = float(aaa[-1].close) / float(aaa[-21].close) - 1.0
    assert ctx.momentum_by_symbol["AAA"] == pytest.approx(expected)

    ctx2 = provider(_bar("BBB"))
    assert set(ctx2.momentum_by_symbol) == {"AAA", "BBB"}
    assert ctx2.momentum_by_symbol["BBB"] < 0.0          # declining series


def test_momentum_nan_when_history_is_short(store, provider):
    _seed_daily(store, "AAA", _weekdays_back(D - timedelta(days=1), 5), 100.0, 0.5)
    ctx = provider(_bar("AAA"))
    assert math.isnan(ctx.momentum_by_symbol["AAA"])     # unrankable, never an error (§6.1 mom)


def test_momentum_nan_when_symbol_has_no_daily_history(store, provider):
    ctx = provider(_bar("ZZZ"))
    assert ctx.daily_bars == []
    assert math.isnan(ctx.momentum_by_symbol["ZZZ"])


def test_momentum_universe_preloads_full_cross_section(store, clock, calendar, features):
    days = _weekdays_back(D - timedelta(days=1), 30)
    _seed_daily(store, "AAA", days, 100.0, 0.5)
    _seed_daily(store, "BBB", days, 200.0, -0.5)

    provider = LiveScanContextProvider(
        store, clock, calendar, features, momentum_universe=["AAA", "BBB"]
    )
    ctx = provider(_bar("AAA"))                          # first bar of the day already sees both
    assert set(ctx.momentum_by_symbol) == {"AAA", "BBB"}
    provider(_bar("BBB"))
    assert store.daily_reads.count("BBB") == 1           # preload counts as the one read


# --------------------------------------------------------------------------- index / flagged / ex-dates
def test_index_daily_closes_populated(store, provider):
    days = _weekdays_back(D - timedelta(days=1), 30)
    nifty = _seed_daily(store, "NIFTY 50", days, 20_000.0, 5.0)

    ctx = provider(_bar("AAA"))
    assert ctx.index_daily_closes == [b.close for b in nifty]
    assert ctx.index_daily_closes[0] < ctx.index_daily_closes[-1]


def test_flagged_reads_the_prior_session(store, provider):
    """2026-08-18 fix: the deals job writes day-``d``'s flags at 20:30 of ``d`` itself, so a
    same-day read was structurally empty and orb's §6.1 bulk/block-deal suppression had never
    fired live (log-verified flagged=0 on every day 08-11→08-18, including days the job
    succeeded). The knowable — and now pinned — semantics: a deal on the PRIOR trading session
    flags the symbol today."""
    store.upsert_flagged_instrument_days([{"symbol": "AAA", "d": PREV_D, "reason": "block_deal"}])
    store.upsert_flagged_instrument_days([{"symbol": "BBB", "d": D, "reason": "block_deal"}])
    assert provider(_bar("AAA")).flagged is True          # prior-session deal — orb suppresses
    assert provider(_bar("BBB")).flagged is False         # same-day rows are unknowable intraday
    assert store.flagged_reads == [PREV_D]                # one read per day, prior session only


def test_flagged_prior_session_walks_back_over_the_weekend(store, clock, calendar, features):
    """Monday's prior session is Friday — the walk uses the trading calendar, never d-1."""
    monday, friday = date(2026, 6, 15), date(2026, 6, 12)
    store.upsert_flagged_instrument_days([{"symbol": "AAA", "d": friday, "reason": "bulk_deal"}])
    provider = LiveScanContextProvider(store, clock, calendar, features)
    assert provider(_bar("AAA", d=monday)).flagged is True
    assert store.flagged_reads == [friday]


def test_upcoming_ex_dates_bucketed_per_symbol(store, provider):
    store.upsert_corp_actions([
        {"symbol": "AAA", "ex_date": D + timedelta(days=3), "kind": "dividend"},
        {"symbol": "AAA", "ex_date": D + timedelta(days=90), "kind": "dividend"},   # beyond horizon
        {"symbol": "AAA", "ex_date": D - timedelta(days=2), "kind": "dividend"},    # past
        {"symbol": "BBB", "ex_date": D + timedelta(days=5), "kind": "bonus"},
    ])
    assert provider(_bar("AAA")).upcoming_ex_dates == [D + timedelta(days=3)]
    assert provider(_bar("BBB")).upcoming_ex_dates == [D + timedelta(days=5)]
    assert provider(_bar("CCC")).upcoming_ex_dates == []
    assert store.corp_action_reads == 1                  # ONE range read for every symbol


def test_ex_date_horizon_covers_the_mom_skip_window(store, provider):
    # §6.1 mom skips on ex-dates within ceil(rebalance_days x 7/5) calendar days — 28 at the §6.3
    # upper bound of 20. A shorter provider horizon would silently defeat the A12 skip.
    store.upsert_corp_actions([{"symbol": "AAA", "ex_date": D + timedelta(days=27), "kind": "dividend"}])
    assert provider(_bar("AAA")).upcoming_ex_dates == [D + timedelta(days=27)]


# --------------------------------------------------------------------------- calendar context
def test_trade_window_and_session_open_on_a_trading_day(provider):
    ctx = provider(_bar("AAA"))
    assert ctx.session_open == SESSION_OPEN
    assert ctx.trade_window == (
        datetime(2026, 6, 17, 10, 0, tzinfo=IST), datetime(2026, 6, 17, 10, 30, tzinfo=IST),
    )


def test_non_trading_date_yields_no_window_and_no_session_open(store, provider):
    ctx = provider(_bar("AAA", d=SUNDAY))
    assert ctx.trade_window is None                      # NSECalendar raises; provider degrades (R6)
    assert ctx.session_open is None
    assert store.minute_reads == []                      # no session ⇒ no intraday seed
    assert len(ctx.intraday_bars) == 1                   # still the scanned bar, ascending contract


def test_mom_sessions_since_rebalance_no_conn_degrades_to_legacy_none(provider):
    # WO-13: the `provider` fixture wires no `conn` — this is the backward-compat degradation path
    # (pre-WO-13 behaviour), not the resolved feature. Every day still reads "due" with no state
    # persisted; see the ``mom rebalance state`` block below for the real (conn-wired) behaviour.
    assert provider(_bar("AAA", d=D)).mom_sessions_since_rebalance is None
    assert provider(_bar("AAA", d=NEXT_D)).mom_sessions_since_rebalance is None


# --------------------------------------------------------------------------- mom rebalance state (WO-13)
#
# Trading-day sequence from D=2026-06-17 (conftest FIXED_NOW, a Wednesday): 06-17(D), 06-18(NEXT_D,
# Thu), 06-19(Fri) — 06-20/21 are a weekend, 06-26 (Fri) is a holiday (Muharram, config/calendar/
# 2026.yaml) — then 06-22(Mon), the next trading day after 06-19. Tests use a small
# ``mom_rebalance_days=2`` override so the cadence exercises in three real trading days instead of
# the §6.3 default 15.
THIRD_D = date(2026, 6, 19)     # Fri — 2 trading sessions after D
FOURTH_D = date(2026, 6, 22)    # Mon — 1 trading session after THIRD_D


def test_mom_rebalance_bootstrap_is_none_and_persists_today(conn, store, clock, calendar, features):
    provider = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    ctx = provider(_bar("AAA", d=D))
    assert ctx.mom_sessions_since_rebalance is None          # never rebalanced ⇒ due now (unchanged)
    row = conn.execute("SELECT last_rebalance_d FROM mom_rebalance_state WHERE id=1").fetchone()
    assert row["last_rebalance_d"] == D.isoformat()           # today stamped as the new reference point


def test_mom_rebalance_not_due_reports_sessions_elapsed_and_does_not_advance(conn, store, clock, calendar, features):
    provider = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    provider(_bar("AAA", d=D))                                # bootstrap: marker := D
    ctx = provider(_bar("AAA", d=NEXT_D))                     # 1 session later; cadence=2 ⇒ not due
    assert ctx.mom_sessions_since_rebalance == 1
    row = conn.execute("SELECT last_rebalance_d FROM mom_rebalance_state WHERE id=1").fetchone()
    assert row["last_rebalance_d"] == D.isoformat()           # unchanged — not due yet


def test_mom_rebalance_due_day_fires_and_advances_marker(conn, store, clock, calendar, features):
    provider = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    provider(_bar("AAA", d=D))
    provider(_bar("AAA", d=NEXT_D))
    ctx = provider(_bar("AAA", d=THIRD_D))                    # 2 sessions later; cadence=2 ⇒ due
    assert ctx.mom_sessions_since_rebalance == 2
    row = conn.execute("SELECT last_rebalance_d FROM mom_rebalance_state WHERE id=1").fetchone()
    assert row["last_rebalance_d"] == THIRD_D.isoformat()      # today becomes the new reference point


def test_mom_rebalance_restart_preserves_state(conn, store, clock, calendar, features):
    p1 = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    p1(_bar("AAA", d=D))
    p1(_bar("AAA", d=NEXT_D))
    p1(_bar("AAA", d=THIRD_D))                                # due here — marker advances to THIRD_D

    # "Restart": a brand-new provider instance over the SAME conn/db — no in-memory cache survives.
    p2 = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    ctx = p2(_bar("AAA", d=FOURTH_D))                          # 1 session after the PERSISTED marker
    # Had the marker not survived the restart, this would read back as the None bootstrap (due now)
    # instead of the correct "1 session since THIRD_D, not yet due at cadence 2".
    assert ctx.mom_sessions_since_rebalance == 1


def test_mom_rebalance_non_trading_day_never_advances_marker(conn, store, clock, calendar, features):
    provider = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    provider(_bar("AAA", d=D))                                 # bootstrap: marker := D
    ctx = provider(_bar("AAA", d=SUNDAY))                      # not a trading day — no session to count
    assert ctx.mom_sessions_since_rebalance is None
    row = conn.execute("SELECT last_rebalance_d FROM mom_rebalance_state WHERE id=1").fetchone()
    assert row["last_rebalance_d"] == D.isoformat()            # untouched


def test_mom_scanner_fires_only_when_due_end_to_end(conn, store, clock, calendar, features):
    """The full WO-13 path: MomentumScanner + LiveScanContextProvider wired together, no hand-built
    ``ScanContext``. Not-due emits nothing; due emits; the marker persists across the sequence."""
    days = _weekdays_back(D - timedelta(days=1), 30)
    _seed_daily(store, "AAA", days, 100.0, 0.5)                # rising momentum ⇒ ranks top_n=1
    provider = LiveScanContextProvider(store, clock, calendar, features, conn=conn, mom_rebalance_days=2)
    scanner = MomentumScanner({"top_n": 1, "rebalance_days": 2})

    bar_d = _bar("AAA", d=D)
    assert scanner.scan(bar_d, provider(bar_d)) != []          # bootstrap ⇒ due

    bar_next = _bar("AAA", d=NEXT_D)
    assert scanner.scan(bar_next, provider(bar_next)) == []    # 1 session in, cadence 2 ⇒ not due

    bar_third = _bar("AAA", d=THIRD_D)
    assert scanner.scan(bar_third, provider(bar_third)) != []  # 2 sessions in ⇒ due again


# --------------------------------------------------------------------------- feature snapshot
def test_features_snapshot_id_minted_per_bar(provider, features):
    assert provider(_bar("AAA", mm=20)).features_snapshot_id == "snap-AAA-1"
    assert provider(_bar("AAA", mm=21)).features_snapshot_id == "snap-AAA-2"
    assert features.calls == ["AAA", "AAA"]


def test_snapshot_minting_failure_yields_none(store, clock, calendar):
    provider = LiveScanContextProvider(store, clock, calendar, StubFeatures(fail=True))
    ctx = provider(_bar("AAA"))                          # must not raise into the scan path
    assert ctx.features_snapshot_id is None
    assert ctx.session_open == SESSION_OPEN              # the rest of the context is still built


def test_real_feature_engine_snapshot_is_persisted(store, clock, calendar):
    engine = FeatureEngine(store, clock, calendar)
    provider = LiveScanContextProvider(store, clock, calendar, engine)
    _seed_intraday(store, "AAA", range(15, 20))

    snapshot_id = provider(_bar("AAA", mm=20)).features_snapshot_id

    assert snapshot_id is not None
    vector = load_snapshot(store, snapshot_id)
    assert vector is not None and vector.symbol == "AAA"


# --------------------------------------------------------------------------- pre-screen wiring
def test_provider_satisfies_the_prescreen_context_provider_seam(store, provider):
    """The provider is the injected ``context_provider``: SignalPreScreen calls it per bar."""
    seen = []

    class _Recorder(Scanner):
        strategy_id = "recorder"
        style = "intraday"

        def scan(self, bar, ctx):  # noqa: ANN001 - test stub
            seen.append(ctx)
            return []

    ps = SignalPreScreen([_Recorder()], provider)
    ps.on_bar(_bar("AAA", mm=20))
    ps.on_bar(_bar("AAA", mm=21))

    assert len(seen) == 2
    assert seen[-1].session_open == SESSION_OPEN
    assert [b.ts_minute.minute for b in seen[-1].intraday_bars] == [20, 21]


# ================================ owner window changes mid-session (§3.2.7, 2026-08-18 origination fix)
#
# THE BUG this section pins. `trade_window` is day-cached like the rest, but unlike the rest it is a
# CONTROL-PLANE value the owner can change mid-session. The cache is per PROCESS per date, so a
# change was invisible to every scanner until the next restart or day rollover. Live 2026-08-18: the
# cache was built 09:52:06 holding 10:00-10:30, the owner moved the window to 10:10-15:30 at
# 09:57:52, and `orb` — which intersects ctx.trade_window — went on firing from 10:00:05 against the
# window that no longer existed, exhausting its whole 6-slot day sub-cap nine minutes before the real
# window opened. ModeManager had always published `trade_window.changed`; nothing in the scan path
# listened. engine.ops.main now wires it to invalidate_trade_window().

class _SwappableWindowCalendar:
    """Real calendar with an owner-swappable ``trade_window`` — the §3.2.7 seam, without SQLite."""

    def __init__(self, inner: NSECalendar, window) -> None:
        self._inner = inner
        self.window = window
        self.reads = 0

    def session(self, d):          # noqa: ANN001 - delegate
        return self._inner.session(d)

    def is_trading_day(self, d):   # noqa: ANN001 - delegate
        return self._inner.is_trading_day(d)

    def previous_trading_day(self, d):  # noqa: ANN001 - delegate
        return self._inner.previous_trading_day(d)

    def trade_window(self, d):     # noqa: ANN001 - the swappable bit
        self.reads += 1
        if self.window is None:
            raise ValueError(f"{d} is not a trading day; no trade window")
        return self.window


OLD_W = (datetime(2026, 6, 17, 10, 0, tzinfo=IST), datetime(2026, 6, 17, 10, 30, tzinfo=IST))
NEW_W = (datetime(2026, 6, 17, 10, 10, tzinfo=IST), datetime(2026, 6, 17, 15, 30, tzinfo=IST))


@pytest.fixture
def swappable(store, clock, calendar, features):
    cal = _SwappableWindowCalendar(calendar, OLD_W)
    return cal, LiveScanContextProvider(store, clock, cal, features)


def test_window_change_is_invisible_until_invalidated(swappable):
    """The staleness itself — asserted so the fix cannot be silently reverted into a per-bar read."""
    cal, provider = swappable
    assert provider(_bar("AAA", mm=20)).trade_window == OLD_W
    cal.window = NEW_W                                   # owner moves it mid-session
    assert provider(_bar("AAA", mm=21)).trade_window == OLD_W    # still cached — this WAS the bug
    assert cal.reads == 1                                # and the read budget is why


def test_invalidate_trade_window_refreshes_on_the_next_bar(swappable):
    cal, provider = swappable
    assert provider(_bar("AAA", mm=20)).trade_window == OLD_W
    cal.window = NEW_W
    provider.invalidate_trade_window()                   # what `trade_window.changed` now triggers
    assert provider(_bar("AAA", mm=21)).trade_window == NEW_W
    assert cal.reads == 2                                # exactly one extra read, not one per bar
    # ...and it stays refreshed without re-reading again.
    assert provider(_bar("AAA", mm=22)).trade_window == NEW_W
    assert cal.reads == 2


def test_invalidate_refreshes_only_the_window_not_the_whole_day_cache(store, swappable):
    """Why one field and not a cache drop: a rebuild re-runs the index/flagged/ex-date reads and the
    ~100-symbol momentum preload on the pre-screen's worker — exactly the per-bar read budget this
    cache exists to protect (§3.2 hot-path invariant)."""
    cal, provider = swappable
    provider(_bar("AAA", mm=20))
    daily, flagged, minute = list(store.daily_reads), list(store.flagged_reads), list(store.minute_reads)

    cal.window = NEW_W
    provider.invalidate_trade_window()
    assert provider(_bar("AAA", mm=21)).trade_window == NEW_W

    assert store.daily_reads == daily                    # no re-read of ANY day-scoped input
    assert store.flagged_reads == flagged
    assert store.minute_reads == minute


def test_invalidate_before_the_first_bar_is_a_no_op(swappable):
    """Wired at composition time, long before the worker thread exists — must not build or crash."""
    cal, provider = swappable
    provider.invalidate_trade_window()
    assert cal.reads == 0                                # nothing read at construction (§3.2)
    assert provider(_bar("AAA", mm=20)).trade_window == OLD_W
    assert cal.reads == 1


def test_invalidate_is_idempotent(swappable):
    cal, provider = swappable
    provider(_bar("AAA", mm=20))
    cal.window = NEW_W
    for _ in range(5):                                   # five owner changes before the next bar
        provider.invalidate_trade_window()
    assert provider(_bar("AAA", mm=21)).trade_window == NEW_W
    assert cal.reads == 2                                # coalesced into ONE refresh


def test_window_refresh_degrades_to_none_when_unreadable(swappable):
    """A window that stops resolving degrades to ``None`` — which the pre-screen treats as
    fail-closed (no origination), never as unrestricted."""
    cal, provider = swappable
    provider(_bar("AAA", mm=20))
    cal.window = None                                    # NSECalendar would raise ValueError here
    provider.invalidate_trade_window()
    assert provider(_bar("AAA", mm=21)).trade_window is None


def test_day_rollover_still_rebuilds_the_window_without_invalidation(swappable):
    """The dirty flag is an ADDITION to date-change rebuilds, never a replacement."""
    cal, provider = swappable
    assert provider(_bar("AAA", mm=20)).trade_window == OLD_W
    cal.window = NEW_W
    assert provider(_bar("AAA", d=NEXT_D, mm=20)).trade_window == NEW_W   # new date ⇒ full rebuild


def test_flagged_degrades_to_none_when_no_prior_session_resolves(store, clock, calendar, features):
    """The E5 degrade: an unresolvable prior session (calendar hole) yields NO flags and NO error —
    orb runs unsuppressed rather than the whole day cache failing to build."""

    class _HolyCalendar(_SwappableWindowCalendar):
        def previous_trading_day(self, d):  # noqa: ANN001 - the hole under test
            raise ValueError("no trading day within ~1y")

    cal = _HolyCalendar(calendar, OLD_W)
    provider = LiveScanContextProvider(store, clock, cal, features)
    ctx = provider(_bar("AAA"))
    assert ctx.flagged is False
    assert store.flagged_reads == []                     # degraded BEFORE the store read
