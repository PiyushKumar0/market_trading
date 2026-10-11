"""ReconcileJob (§3.2.3 / §4.4 job 2, A13): drift math, threshold boundaries, offline exclusion.

Hand-computed self-vs-official bar sets: drift only when STRICTLY beyond |Δvol|>2% or |Δclose|>1
tick (settings.reconcile [tunable]); an official minute with no self-built bar is an OFFLINE span —
backfilled from official and EXCLUDED from the drift denominator (§2.6), never a failure. Official
candles become the canonical bars_1m rows; the A14 auction_open survives the canonical upsert.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from decimal import Decimal

import pytest
from kiteconnect.exceptions import DataException, TokenException

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
    def seed(symbol: str) -> list[Bar]:
        return [
            self_bar(symbol, at(9, 15), "100.00", 1500),   # 50% vol diff → drifted bar
            self_bar(symbol, at(9, 16), "100.00", 1000),
            self_bar(symbol, at(9, 17), "100.00", 1000),
            self_bar(symbol, at(9, 18), "100.00", 1000),
        ]

    store.insert_bars_1m(seed("A") + seed("B"))
    official = [official_candle(at(9, 15 + i), 100.00, 1000) for i in range(4)]
    kite = FakeKite({TOKENS["A"]: official, TOKENS["B"]: official})
    settings = Settings(reconcile=ReconcileCfg(max_bad_bar_fraction=0.25))   # fraction == 1/4
    report = await _job(store, kite, clock, settings=settings).run(D, ["A"])
    assert report.symbols[0].bad_bar_fraction == pytest.approx(0.25)
    assert report.symbols[0].flagged is False                                # 0.25 > 0.25 is False
    assert report.alerted is False

    # A second symbol with the identical shape (A is logged for D now, so a re-run skips it).
    tighter = Settings(reconcile=ReconcileCfg(max_bad_bar_fraction=0.24))
    report2 = await _job(store, kite, clock, settings=tighter).run(D, ["B"])
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


# --------------------------------------------------------------------------- TokenException abort (2026-07-21)
class _KiteTokenAfter:
    """historical() returns ``ok`` candles for the first ``die_after`` calls, then a TokenException —
    lets us assert the processed portion survives while the remaining symbols abort."""

    def __init__(self, ok: list[dict], die_after: int = 1) -> None:
        self.calls = 0
        self._ok = ok
        self._die_after = die_after

    async def historical(self, token, frm, to, interval):
        self.calls += 1
        if self.calls > self._die_after:
            raise TokenException("Incorrect api_key or access_token")
        return self._ok


async def test_token_rejection_aborts_remaining_symbols_processed_portion_intact(store, clock):
    """Symbols A/B/C on D: A reconciles cleanly, then the token dies on B's official fetch — B and C
    are skipped (one warning, not three), A's result persists, and the day carries A's log row."""
    store.insert_bars_1m([
        self_bar("A", at(9, 15), "10.00", 100),
        self_bar("B", at(9, 15), "10.00", 100),
        self_bar("C", at(9, 15), "10.00", 100),
    ])
    kite = _KiteTokenAfter([official_candle(at(9, 15), 10.00, 100)], die_after=1)
    report = await _job(store, kite, clock).run(D, ["A", "B", "C"])

    assert kite.calls == 2                            # A fetched, B raised, C never attempted
    assert [s.symbol for s in report.symbols] == ["A"]   # only the processed portion is reported
    assert store.has_reconcile_entry(D) is True      # A's row landed (day partially checkpointed)
    (row,) = store.get_reconcile_log(D)
    assert row["symbol"] == "A"


async def test_token_rejection_on_first_symbol_leaves_day_uncheckpointed(store, clock):
    """When the token is already dead, the very first fetch aborts: nothing processed, no log row,
    and ok=False — the job_runs watermark records the day failed, so the §2.6 sweep re-runs it
    after re-login."""
    store.insert_bars_1m([
        self_bar("A", at(9, 15), "10.00", 100),
        self_bar("B", at(9, 15), "10.00", 100),
    ])
    kite = _KiteTokenAfter([], die_after=0)          # dies on the first call
    report = await _job(store, kite, clock).run(D, ["A", "B"])

    assert kite.calls == 1
    assert report.symbols == []
    assert report.ok is False and report.aborted_reason == "token_rejected"
    assert report.symbols_remaining == 2
    assert store.has_reconcile_entry(D) is False


async def test_token_abort_reports_not_ok_with_remaining_count(store, clock):
    store.insert_bars_1m([self_bar(s, at(9, 15), "10.00", 100) for s in ("A", "B", "C")])
    kite = _KiteTokenAfter([official_candle(at(9, 15), 10.00, 100)], die_after=1)
    report = await _job(store, kite, clock).run(D, ["A", "B", "C"])

    assert report.ok is False
    assert report.aborted_reason == "token_rejected"
    assert report.symbols_remaining == 2              # B (raised) and C (never attempted)
    assert report.symbols_already_done == 0
    assert [s.symbol for s in report.symbols] == ["A"]
    assert [r["symbol"] for r in store.get_reconcile_log(D)] == ["A"]


async def test_clean_run_reports_ok(store, clock):
    store.insert_bars_1m([self_bar("A", at(9, 15), "10.00", 100)])
    kite = FakeKite({TOKENS["A"]: [official_candle(at(9, 15), 10.00, 100)]})
    report = await _job(store, kite, clock).run(D, ["A"])
    assert report.ok is True and report.aborted_reason is None
    assert report.symbols_remaining == 0 and report.symbols_already_done == 0


# --------------------------------------------------------------------------- per-(day, symbol) resume
def _drift_seed() -> list[Bar]:
    """A: 09:15 close 2 ticks off official (flags at any max_bad_bar_fraction < 1); B/C: clean."""
    return [
        self_bar("A", at(9, 15), "10.10", 100),
        self_bar("B", at(9, 15), "10.00", 100),
        self_bar("C", at(9, 15), "10.00", 100),
    ]


def _official_all() -> dict[int, list[dict]]:
    return {tok: [official_candle(at(9, 15), 10.00, 100)] for tok in TOKENS.values()}


async def test_rerun_resumes_only_unlogged_symbols_and_preserves_drift_rows(store, clock):
    """2026-09-15 live bug: the retry of a partly reconciled day re-compared the processed prefix
    against already-canonical bars (no self bars left) and overwrote its drift rows with
    bars_compared=0. The retry must fetch only the unlogged symbols and leave A's row untouched."""
    sent = []

    async def notify(msg):
        sent.append(msg)

    store.insert_bars_1m(_drift_seed())
    dying = _KiteTokenAfter([official_candle(at(9, 15), 10.00, 100)], die_after=1)
    first = await _job(store, dying, clock, notify=notify).run(D)
    assert first.ok is False and first.symbols_flagged == ["A"]
    assert _drift_alerts(sent) == [["A"]]                # a token abort still alerts its prefix
    (a_row,) = store.get_reconcile_log(D)
    assert a_row["bars_compared"] == 1 and a_row["close_drift_bars"] == 1 and a_row["alerted"] is True

    healthy = FakeKite(_official_all())
    second = await _job(store, healthy, clock, notify=notify).run(D)
    assert second.alerted is False and len(sent) == 1

    assert [c[0] for c in healthy.calls] == [TOKENS["B"], TOKENS["C"]]   # A never re-fetched
    assert second.ok is True and second.symbols_already_done == 1
    assert [s.symbol for s in second.symbols] == ["B", "C"]
    rows = {r["symbol"]: r for r in store.get_reconcile_log(D)}
    assert set(rows) == {"A", "B", "C"}
    assert rows["A"] == a_row                                            # drift row untouched


def _drift_alerts(sent: list) -> list[list[str]]:
    return [m.data["symbols_flagged"] for m in sent if m.kind == MessageKind.RECONCILE_DRIFT]


async def test_non_token_exception_persists_processed_prefix_then_raises(store, clock):
    """A 502 mid-run (2026-09-15) propagates to the runner as a failure, but the symbols processed
    before it are already canonical — their rows must land, or the retry re-compares them. A's drift
    alert never went out on the failed run, so its row says alerted=False and the retry sends it:
    exactly one RECONCILE_DRIFT naming A across both runs."""
    class _Kite502After(FakeKite):
        async def historical(self, token, frm, to, interval):
            if len(self.calls) >= 1:
                self.calls.append((token, frm, to, interval))
                raise DataException("502 Bad Gateway")
            return await super().historical(token, frm, to, interval)

    sent = []

    async def notify(msg):
        sent.append(msg)

    store.insert_bars_1m(_drift_seed())
    with pytest.raises(DataException):
        await _job(store, _Kite502After(_official_all()), clock, notify=notify).run(D)
    (a_row,) = store.get_reconcile_log(D)
    assert a_row["symbol"] == "A" and a_row["bars_compared"] == 1 and a_row["alerted"] is False
    assert sent == []

    healthy = FakeKite(_official_all())
    report = await _job(store, healthy, clock, notify=notify).run(D)
    assert [c[0] for c in healthy.calls] == [TOKENS["B"], TOKENS["C"]]
    assert report.ok is True and report.symbols_already_done == 1
    assert report.symbols_flagged == [] and report.symbols_alert_carried == ["A"]
    assert report.alerted is True
    assert _drift_alerts(sent) == [["A"]]
    assert sent[0].data["bars_compared"] == 3        # A's carried bar + B + C
    rows = {r["symbol"]: r for r in store.get_reconcile_log(D)}
    assert rows["A"]["alerted"] is True
    assert rows["A"]["bars_compared"] == 1 and rows["A"]["close_drift_bars"] == 1
    assert rows["B"]["alerted"] is False and rows["C"]["alerted"] is False

    again = await _job(store, FakeKite(_official_all()), clock, notify=notify).run(D)
    assert again.alerted is False and _drift_alerts(sent) == [["A"]]   # never a second alert


async def test_store_closed_mid_run_keeps_every_processed_symbol_logged(store, clock):
    """A graceful stop closes the store under a running reconcile (scheduler.shutdown(wait=False)
    does not cancel it). Every symbol made official before the close must already carry its log
    row, so the retry never re-compares it against canonical bars (bars_compared=0)."""
    class _KiteClosesStore(FakeKite):
        async def historical(self, token, frm, to, interval):
            if len(self.calls) == 1:
                store.close()
            return await super().historical(token, frm, to, interval)

    store.insert_bars_1m(_drift_seed())
    with pytest.raises(RuntimeError, match="not open"):
        await _job(store, _KiteClosesStore(_official_all()), clock).run(D)

    store.open()
    (a_row,) = store.get_reconcile_log(D)
    assert a_row["symbol"] == "A" and a_row["bars_compared"] == 1 and a_row["close_drift_bars"] == 1
    assert {b.src for b in store.get_bars_1m("A", at(9, 15), at(9, 16))} == {"kite_official"}
    assert {b.src for b in store.get_bars_1m("B", at(9, 15), at(9, 16))} == {"self"}

    healthy = FakeKite(_official_all())
    report = await _job(store, healthy, clock).run(D)
    assert [c[0] for c in healthy.calls] == [TOKENS["B"], TOKENS["C"]]
    assert report.ok is True and report.symbols_already_done == 1
    assert store.get_reconcile_log(D)[0]["bars_compared"] == 1


async def test_alert_send_failure_leaves_rows_unalerted_and_next_run_resends(store, clock):
    sent = []
    fail = {"on": True}

    async def notify(msg):
        if fail["on"]:
            raise RuntimeError("telegram down")
        sent.append(msg)

    store.insert_bars_1m(_drift_seed())
    first = await _job(store, FakeKite(_official_all()), clock, notify=notify).run(D)
    assert first.symbols_flagged == ["A"] and first.alerted is False
    assert all(r["alerted"] is False for r in store.get_reconcile_log(D))

    fail["on"] = False
    kite = FakeKite(_official_all())
    second = await _job(store, kite, clock, notify=notify).run(D)
    assert kite.calls == [] and second.symbols_already_done == 3
    assert second.symbols_alert_carried == ["A"] and second.alerted is True
    assert _drift_alerts(sent) == [["A"]]
    assert sent[0].data["bars_compared"] == 1
    rows = {r["symbol"]: r for r in store.get_reconcile_log(D)}
    assert rows["A"]["alerted"] is True and rows["B"]["alerted"] is False

    third = await _job(store, FakeKite(_official_all()), clock, notify=notify).run(D)
    assert third.alerted is False and len(sent) == 1


async def test_flagged_rows_stay_unalerted_without_a_notify_sink(store, clock):
    """alerted records a send that happened: with no sink wired nothing is sent, nothing is marked."""
    store.insert_bars_1m(_drift_seed())
    report = await _job(store, FakeKite(_official_all()), clock).run(D)
    assert report.symbols_flagged == ["A"] and report.alerted is False
    assert all(r["alerted"] is False for r in store.get_reconcile_log(D))


async def test_concurrent_runs_are_serialized_and_second_is_noop(store, clock):
    """08-28 / 10-05 / 10-08 live race: the 15:50 fire and the catch-up sweep ran run(d) at the same
    time, each fetched every symbol, and the later one zeroed every drift row. The second caller
    must wait for the first, then find the whole day logged."""
    class _YieldingKite(FakeKite):
        async def historical(self, token, frm, to, interval):
            await asyncio.sleep(0)
            return await super().historical(token, frm, to, interval)

    store.insert_bars_1m(_drift_seed())
    kite = _YieldingKite(_official_all())
    job = _job(store, kite, clock)
    first, second = await asyncio.gather(job.run(D), job.run(D))

    assert len(kite.calls) == 3                                           # not 6
    assert first.ok is True and [s.symbol for s in first.symbols] == ["A", "B", "C"]
    assert second.ok is True and second.symbols == [] and second.symbols_already_done == 3
    assert all(r["bars_compared"] == 1 for r in store.get_reconcile_log(D))


async def test_fully_reconciled_day_is_ok_with_zero_broker_calls(store, clock):
    sent = []

    async def notify(msg):
        sent.append(msg)

    store.insert_bars_1m(_drift_seed())
    await _job(store, FakeKite(_official_all()), clock, notify=notify).run(D)
    assert len(sent) == 1
    before = store.get_reconcile_log(D)

    kite = FakeKite(_official_all())
    report = await _job(store, kite, clock, notify=notify).run(D)
    assert kite.calls == []
    assert report.ok is True and report.symbols == [] and report.symbols_already_done == 3
    assert report.alerted is False and len(sent) == 1                    # no repeat drift alert
    assert store.get_reconcile_log(D) == before
