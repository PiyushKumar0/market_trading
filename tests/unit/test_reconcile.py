"""ReconcileJob (§3.2.3 / §4.4 job 2, A13): drift math, threshold boundaries, offline exclusion.

Hand-computed self-vs-official bar sets: drift only when STRICTLY beyond |Δvol|>2% or |Δclose|>1
tick (settings.reconcile [tunable]); an official minute with no self-built bar is an OFFLINE span —
backfilled from official and EXCLUDED from the drift denominator (§2.6), never a failure. Official
candles become the canonical bars_1m rows; the A14 auction_open survives the canonical upsert.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from engine.core.clock import IST
from engine.core.config import ReconcileCfg, Settings
from engine.core.types import Bar
from engine.marketdata.reconcile import ReconcileJob
from engine.marketdata.store import MarketStore
from engine.notify.catalog import MessageKind

D = dt.date(2026, 6, 17)
TOKENS = {"A": 1, "B": 2, "C": 3}


def at(h: int, m: int) -> dt.datetime:
    return dt.datetime(D.year, D.month, D.day, h, m, tzinfo=IST)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    s.open()
    yield s
    s.close()


def self_bar(symbol: str, minute: dt.datetime, close: str, volume: int, auction=None) -> Bar:
    return Bar(
        symbol=symbol, ts_minute=minute, open=Decimal(close), high=Decimal(close),
        low=Decimal(close), close=Decimal(close), volume=volume, src="self",
        auction_open=None if auction is None else Decimal(auction),
    )


def official_candle(minute: dt.datetime, close: float, volume: int) -> dict:
    return {"date": minute, "open": close, "high": close, "low": close,
            "close": close, "volume": volume}


class FakeKite:
    def __init__(self, candles_by_token: dict[int, list[dict]]) -> None:
        self.candles_by_token = candles_by_token
        self.calls: list[tuple[int, dt.datetime, dt.datetime, str]] = []

    async def historical(self, token, frm, to, interval):
        self.calls.append((token, frm, to, interval))
        return self.candles_by_token.get(token, [])


def _job(store, kite, clock, settings=None, notify=None) -> ReconcileJob:
    return ReconcileJob(
        store, kite, clock, settings or Settings(),
        token_for_symbol=lambda s: TOKENS.get(s), notify=notify,
    )


# --------------------------------------------------------------------------- drift + boundaries
async def test_drift_math_boundaries_offline_exclusion_and_alert(store, clock):
    # Self-built bars for A: 09:15..09:19. Official: 09:15..09:21 (last two = offline span).
    store.insert_bars_1m([
        self_bar("A", at(9, 15), "100.00", 1020, auction="99.50"),  # |Δvol| = 2.0% EXACTLY → clean
        self_bar("A", at(9, 16), "100.00", 1021),                   # |Δvol| = 2.1% → vol drift
        self_bar("A", at(9, 17), "100.05", 1000),                   # |Δclose| = 1 tick EXACTLY → clean
        self_bar("A", at(9, 18), "100.10", 1000),                   # |Δclose| = 2 ticks → close drift
        self_bar("A", at(9, 19), "100.00", 1000),                   # identical → clean
    ])
    official = [official_candle(at(9, 15 + i), 100.00, 1000) for i in range(5)]
    official += [official_candle(at(9, 20), 101.00, 500),
                 official_candle(at(9, 21), 102.00, 600)]
    kite = FakeKite({TOKENS["A"]: official})
    sent = []

    async def notify(msg):
        sent.append(msg)

    report = await _job(store, kite, clock, notify=notify).run(D, ["A"])

    (sym,) = report.symbols
    assert sym.bars_self == 5
    assert sym.bars_official == 7
    assert sym.bars_compared == 5                 # offline minutes NOT in the denominator (§2.6)
    assert sym.vol_drift_bars == 1
    assert sym.close_drift_bars == 1
    assert sym.offline_bars == 2
    assert sym.bad_bar_fraction == pytest.approx(0.4)
    assert sym.flagged is True
    assert report.symbols_flagged == ["A"]
    assert report.alerted is True

    # The alert went to the injected async sink with the catalog shape (A13 "alert on drift").
    assert len(sent) == 1
    assert sent[0].kind == MessageKind.RECONCILE_DRIFT
    assert sent[0].data["symbols_flagged"] == ["A"]
    assert sent[0].data["bars_compared"] == 5

    # Official candles are now the CANONICAL rows (§4.4 job 2)…
    bars = store.get_bars_1m("A", at(9, 15), at(9, 22))
    assert len(bars) == 7
    assert {b.src for b in bars} == {"kite_official"}
    assert bars[1].volume == 1000                 # official replaced the drifted self volume
    # …the offline span was backfilled from official…
    assert bars[5].ts_minute == at(9, 20) and bars[5].close == Decimal("101.00")
    # …and the A14 auction_open survived the canonical upsert on the 09:15 row.
    assert bars[0].auction_open == Decimal("99.50")

    # reconcile_log row = the §2.6 per-day catch-up checkpoint.
    assert store.has_reconcile_entry(D) is True
    (row,) = store.get_reconcile_log(D)
    assert row["bars_self"] == 5 and row["bars_official"] == 7
    assert row["bars_compared"] == 5 and row["offline_bars"] == 2
    assert row["vol_drift_bars"] == 1 and row["close_drift_bars"] == 1
    assert row["alerted"] is True

    # The official fetch went out for the session span at minute interval.
    (call,) = kite.calls
    assert call[0] == TOKENS["A"] and call[3] == "minute"
    assert call[1] == clock.combine(D, dt.time(9, 15))
    assert call[2] == clock.combine(D, dt.time(15, 30))


async def test_within_threshold_day_does_not_alert(store, clock):
    store.insert_bars_1m([
        self_bar("B", at(9, 15), "100.00", 1020),   # exactly 2% vol diff → clean (strict >)
        self_bar("B", at(9, 16), "100.05", 1000),   # exactly 1 tick close diff → clean (strict >)
        self_bar("B", at(9, 17), "100.00", 1000),
    ])
    official = [
        official_candle(at(9, 15), 100.00, 1000),
        official_candle(at(9, 16), 100.00, 1000),
        official_candle(at(9, 17), 100.00, 1000),
    ]
    sent = []

    async def notify(msg):
        sent.append(msg)

    report = await _job(store, clock=clock, kite=FakeKite({TOKENS["B"]: official}),
                        notify=notify).run(D, ["B"])
    (sym,) = report.symbols
    assert sym.bars_compared == 3
    assert sym.vol_drift_bars == 0 and sym.close_drift_bars == 0
    assert sym.flagged is False
    assert report.alerted is False
    assert sent == []
    (row,) = store.get_reconcile_log(D)
    assert row["alerted"] is False


async def test_bad_fraction_boundary_is_strict(store, clock):
    """'>1% of bars' is strict: a fraction exactly AT max_bad_bar_fraction does not flag."""
    seed = [
        self_bar("A", at(9, 15), "100.00", 1500),   # 50% vol diff → drifted bar
        self_bar("A", at(9, 16), "100.00", 1000),
        self_bar("A", at(9, 17), "100.00", 1000),
        self_bar("A", at(9, 18), "100.00", 1000),
    ]
    store.insert_bars_1m(seed)
    official = [official_candle(at(9, 15 + i), 100.00, 1000) for i in range(4)]
    kite = FakeKite({TOKENS["A"]: official})
    settings = Settings(reconcile=ReconcileCfg(max_bad_bar_fraction=0.25))   # fraction == 1/4
    report = await _job(store, kite, clock, settings=settings).run(D, ["A"])
    assert report.symbols[0].bad_bar_fraction == pytest.approx(0.25)
    assert report.symbols[0].flagged is False                                # 0.25 > 0.25 is False
    assert report.alerted is False

    # Re-seed the self rows (the first run made official canonical) and tighten the threshold.
    store.insert_bars_1m(seed)
    tighter = Settings(reconcile=ReconcileCfg(max_bad_bar_fraction=0.24))
    report2 = await _job(store, kite, clock, settings=tighter).run(D, ["A"])
    assert report2.symbols[0].flagged is True


async def test_fully_offline_symbol_is_backfilled_not_flagged(store, clock):
    """A day the engine never built bars for: everything is offline span — filled, zero drift."""
    official = [official_candle(at(9, 15 + i), 55.00, 10 * (i + 1)) for i in range(4)]
    kite = FakeKite({TOKENS["C"]: official})
    report = await _job(store, kite, clock).run(D, ["C"])
    (sym,) = report.symbols
    assert sym.bars_self == 0
    assert sym.bars_compared == 0
    assert sym.offline_bars == 4
    assert sym.flagged is False and sym.bad_bar_fraction == 0.0
    bars = store.get_bars_1m("C", at(9, 15), at(9, 30))
    assert len(bars) == 4 and {b.src for b in bars} == {"kite_official"}


async def test_symbols_default_to_the_days_active_set(store, clock):
    """run(d) without symbols reconciles every symbol holding bars_1m rows on d (catch-up path)."""
    store.insert_bars_1m([self_bar("A", at(9, 15), "10.00", 100)])
    kite = FakeKite({TOKENS["A"]: [official_candle(at(9, 15), 10.00, 100)]})
    report = await _job(store, kite, clock).run(D)
    assert [s.symbol for s in report.symbols] == ["A"]
    assert kite.calls[0][0] == TOKENS["A"]
    assert store.has_reconcile_entry(D)


async def test_unknown_token_yields_empty_official_row(store, clock):
    store.insert_bars_1m([self_bar("ZZ", at(9, 15), "10.00", 100)])
    kite = FakeKite({})
    report = await _job(store, kite, clock).run(D, ["ZZ"])
    (sym,) = report.symbols
    assert sym.bars_official == 0 and sym.bars_compared == 0
    assert sym.flagged is False
    assert kite.calls == []                     # no token → no request, never a guess
