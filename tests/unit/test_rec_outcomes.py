"""Plan Q2.3: the hindsight ``rec_outcomes`` job."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, time
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.types import Bar
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops.jobs import (
    JOB_BHAVCOPY,
    JOB_DAILY_BARS,
    JOB_REC_OUTCOMES,
    CatchUpRunner,
    JobClass,
    JobRegistry,
    JobSpec,
)
from engine.ops.pipeline import RecommendationBook
from engine.ops.rec_outcomes import RecOutcomesJob
from engine.strategy.cost_model import CostModel
from tests.unit.test_reco_pipeline import log_events, make_rec

S = date(2026, 9, 1)
UNTOUCHED = ("positions", "learning_ledger", "shadow_trades")


class _Now:
    def __init__(self) -> None:
        self.at = datetime(2026, 9, 1, 10, 0, tzinfo=IST)

    def __call__(self) -> datetime:
        return self.at


@pytest.fixture
def now() -> _Now:
    return _Now()


@pytest.fixture
def clk(now) -> Clock:
    return Clock(time_source=now)


@pytest.fixture
def cal(clk) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clk, strict=False)


@pytest.fixture
def store(tmp_path, clk):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clk).open()
    yield s
    s.close()


@pytest.fixture(scope="module")
def cost_model() -> CostModel:
    return CostModel.from_config()


@pytest.fixture
def book(conn, clk, cost_model) -> RecommendationBook:
    return RecommendationBook(conn, clk, cost_model)


@pytest.fixture
def sess(cal) -> list[date]:
    return [cal.add_sessions(S, i) for i in range(25)]


def _job(conn, store, cal, cost_model, clk, *, hold=3, was_run=lambda job_id, d: job_id == JOB_DAILY_BARS):
    return RecOutcomesJob(conn, store, cal, cost_model, lambda sid, style: hold, clk, was_run)


def _deliver(book, now, cost_model, *, symbol="RELIANCE", at=None, **kw):
    now.at = at or datetime(S.year, S.month, S.day, 10, 0, tzinfo=IST)
    fields = dict(created_at=now.at, instrument=symbol, style="swing", product="CNC", targets=[],
                  stop=Decimal("94"), strategy_id="hi52", entry_type="LIMIT")
    rec = make_rec(cost_model, **{**fields, **kw})
    book.deliver(rec, ledger_fields={"strategy_id": rec.strategy_id})
    return rec


def _bars(store, symbol, days, *, skip=()):
    """1m on the first day (one bar before the 10:00 delivery that would hit the stop, then a
    trade-through of 100 opening at 101); daily closes 100 + i on every day, ``skip`` excepted."""
    d0 = days[0]

    def m(hh, mm, o, h, low, c):
        return Bar(symbol=symbol, ts_minute=datetime(d0.year, d0.month, d0.day, hh, mm, tzinfo=IST),
                   open=Decimal(o), high=Decimal(h), low=Decimal(low), close=Decimal(c), volume=1)

    store.insert_bars_1m([m(9, 30, "95", "95", "90", "95"), m(10, 1, "101", "101", "99.5", "100"),
                          m(10, 2, "100", "100.5", "99.8", "100.2")])
    store.upsert_bars_1d([
        DailyBar(symbol=symbol, d=x, open=Decimal(100 + i) - Decimal("0.5"), high=Decimal(100 + i) + 1,
                 low=Decimal(100 + i) - 1, close=Decimal(100 + i), volume=1)
        for i, x in enumerate(days) if i not in skip
    ])


def _seed_ew(conn, days, ret=0.001):
    conn.executemany("INSERT INTO universe_ew_returns (d, ret, n) VALUES (?, ?, 1)",
                     [(x.isoformat(), ret) for x in days])


def _rows(conn) -> dict[str, dict]:
    cur = conn.execute("SELECT * FROM rec_outcomes")
    cols = [c[0] for c in cur.description]
    return {r[0]: {k: v for k, v in zip(cols, r, strict=True) if k != "updated_at"} for r in cur}


async def test_scores_entry_recs_idempotently_and_writes_nothing_else(
    conn, store, cal, cost_model, clk, now, book, sess
):
    full = _deliver(book, now, cost_model)
    gap = _deliver(book, now, cost_model, symbol="TCS")
    _deliver(book, now, cost_model, symbol="INFY", kind="exit")
    _bars(store, "RELIANCE", sess)
    _bars(store, "TCS", sess, skip={1})
    _seed_ew(conn, sess[1:])
    conn.execute("INSERT INTO positions (position_id, symbol, origin, state) "
                 "VALUES ('P1', 'X', 'recommended', 'OPEN')")
    conn.execute("INSERT INTO shadow_trades (symbol) VALUES ('X')")
    before = {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in UNTOUCHED}

    job = _job(conn, store, cal, cost_model, clk)
    await job.run(sess[24])
    rows = _rows(conn)
    assert set(rows) == {full.rec_id, gap.rec_id}            # the exit rec is ignored

    cost = float(cost_model.breakeven_pct(Decimal("1000"), "CNC"))
    bench, bench20 = (1.001 ** 2 - 1) * 100, (1.001 ** 19 - 1) * 100
    numbers = {
        "gross_pct": 2.0, "cost_pct": cost, "net_pct": 2 - cost, "net_t5": 4 - cost,
        "net_t10": 9 - cost, "net_t20": 19 - cost, "bench_pct": bench,
        "excess_pct": 2 - cost - bench, "excess_t20": 19 - cost - bench20,
    }
    assert {k: rows[full.rec_id][k] for k in numbers} == pytest.approx(numbers)
    assert {k: v for k, v in rows[full.rec_id].items() if k not in numbers} == {
        "rec_id": full.rec_id, "strategy_id": "hi52", "entry_type": "LIMIT",
        "fill_basis": "1m_post_delivery", "status": "closed", "fill_d": S.isoformat(),
        "fill_px": "100", "exit_d": sess[2].isoformat(), "exit_px": "102.00", "exit_reason": "time",
    }
    # A missing middle bar stops the walk: filled, still open, nothing measured.
    assert {k: v for k, v in rows[gap.rec_id].items() if v is not None} == {
        "rec_id": gap.rec_id, "strategy_id": "hi52", "entry_type": "LIMIT",
        "fill_basis": "1m_post_delivery", "status": "open", "fill_d": S.isoformat(),
        "fill_px": "100", "cost_pct": pytest.approx(cost),
    }

    await job.run(sess[24])
    assert _rows(conn) == rows
    assert {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in UNTOUCHED} == before


async def test_unfinished_until_daily_bars_resolves_and_never_alerts(
    conn, store, cal, cost_model, clk, now, book, sess
):
    _deliver(book, now, cost_model)
    _bars(store, "RELIANCE", sess)
    d = sess[3]
    sent: list = []

    async def notify(msg) -> None:
        sent.append(msg)

    registry = JobRegistry()
    runner = CatchUpRunner(conn, clk, cal, registry, notify=notify)
    job = _job(conn, store, cal, cost_model, clk, was_run=runner.was_run)
    registry.register(JobSpec(JOB_REC_OUTCOMES, JobClass.DATE_KEYED, time(18, 30), job.run, order=35))
    now.at = datetime(d.year, d.month, d.day, 19, 0, tzinfo=IST)
    off = datetime(d.year, d.month, d.day, 9, 0, tzinfo=IST)

    for late in (None, "failed"):
        if late:
            runner.record_run(JOB_DAILY_BARS, d, status=late)
        result = await runner.catch_up(off_since=off)
        assert (result.jobs_caught_up, result.jobs_failed) == ([], [])
        assert not runner.was_run(JOB_REC_OUTCOMES, d)
        assert _rows(conn) == {}
    assert sent == []

    runner.record_run(JOB_DAILY_BARS, d, status="skipped")
    result = await runner.catch_up(off_since=off)
    assert result.jobs_caught_up == [f"{JOB_REC_OUTCOMES}:{d.isoformat()}"]
    assert [r["status"] for r in _rows(conn).values()] == ["closed"]


async def test_a_horizon_past_the_calendar_stays_pending(
    conn, store, cal, cost_model, clk, now, book, caplog
):
    start, end = date(2026, 12, 15), date(2026, 12, 31)
    days = [cal.add_sessions(start, i) for i in range(12)]
    assert days[-1] == end
    rec = _deliver(book, now, cost_model, at=datetime(2026, 12, 15, 10, 0, tzinfo=IST))
    _bars(store, "RELIANCE", days)
    with caplog.at_level(logging.INFO, logger="engine.ops.rec_outcomes"):
        await _job(conn, store, cal, cost_model, clk, hold=20).run(end)
    row = _rows(conn)[rec.rec_id]
    assert (row["status"], row["exit_d"], row["net_t20"], row["excess_t20"]) == ("open", None, None, None)
    assert row["net_t5"] is not None and row["net_t10"] is not None
    assert [r.sessions for r in log_events(caplog, "rec_outcome_horizon_pending")] == [[20]]


@pytest.mark.parametrize(
    ("fields", "exit_idx"),
    [({"hold_sessions": 6, "exit_session": date(2026, 9, 8)}, 5), ({"hold_sessions": 2}, 1), ({}, 2)],
    ids=["payload_exit_session", "payload_hold_sessions", "legacy_hold_fn"],
)
async def test_horizon_comes_from_the_payload_before_the_live_hold(
    conn, store, cal, cost_model, clk, now, book, sess, fields, exit_idx
):
    assert sess[5] == date(2026, 9, 8)
    rec = _deliver(book, now, cost_model, **fields)
    _bars(store, "RELIANCE", sess)
    _seed_ew(conn, sess[1:])
    await _job(conn, store, cal, cost_model, clk).run(sess[24])
    assert _rows(conn)[rec.rec_id]["exit_d"] == sess[exit_idx].isoformat()


async def test_a_failing_benchmark_does_not_fail_the_run(
    conn, store, cal, cost_model, clk, now, book, sess, monkeypatch
):
    def boom(*_a):
        raise ValueError("calendar horizon")

    monkeypatch.setattr("engine.ops.rec_outcomes.bench_pct", boom)
    rec = _deliver(book, now, cost_model)
    _bars(store, "RELIANCE", sess)
    _seed_ew(conn, sess[1:])
    await _job(conn, store, cal, cost_model, clk).run(sess[24])
    row = _rows(conn)[rec.rec_id]
    assert (row["status"], row["bench_pct"], row["net_pct"] is None) == ("closed", None, False)


@pytest.mark.parametrize(("later_success", "scored"), [(False, 1), (True, 0)])
async def test_replaying_an_earlier_day_after_a_later_one_does_not_rescore(
    conn, store, cal, cost_model, clk, now, book, sess, later_success, scored
):
    _deliver(book, now, cost_model)
    _bars(store, "RELIANCE", sess)
    if later_success:
        CatchUpRunner(conn, clk, cal, JobRegistry()).record_run(JOB_REC_OUTCOMES, sess[10])
    await _job(conn, store, cal, cost_model, clk, was_run=lambda job_id, d: True).run(sess[3])
    assert len(_rows(conn)) == scored
    assert conn.execute("SELECT count(*) FROM universe_ew_returns WHERE d=?",
                        (sess[3].isoformat(),)).fetchone()[0] == 1


@pytest.mark.parametrize(
    ("ledger", "proposal_type", "zone", "expected"),
    [
        (True, None, ("100", "100"), ("closed", "hi52", "LIMIT", "100")),
        (True, "MARKET", ("100", "100"), ("closed", "hi52", "MARKET", "101.00")),
        (True, None, ("100", "102"), ("closed", "hi52", "MARKET", "101.00")),
        (False, None, ("100", "100"), ("unscorable", None, "LIMIT", None)),
    ],
    ids=["degenerate_zone_limit", "proposal_payload", "zone_market", "no_ledger_row"],
)
async def test_old_recs_follow_the_old_rec_contract(
    conn, store, cal, cost_model, clk, now, book, sess, ledger, proposal_type, zone, expected
):
    rec = _deliver(book, now, cost_model, entry_zone=tuple(Decimal(z) for z in zone))
    payload = json.loads(conn.execute("SELECT payload FROM recommendations").fetchone()[0])
    for key in ("strategy_id", "entry_type", "proposal_id", "hold_sessions", "exit_session", "exit_kind"):
        payload.pop(key, None)
    conn.execute("UPDATE recommendations SET payload=?", (json.dumps(payload),))
    if proposal_type:
        conn.execute("INSERT INTO proposals VALUES ('P-OLD', 'a', 'enter', ?, '', '')",
                     (json.dumps({"entry_type": proposal_type}),))
        conn.execute("UPDATE learning_ledger SET proposal_id='P-OLD'")
    if not ledger:
        conn.execute("DELETE FROM learning_ledger")
    _bars(store, "RELIANCE", sess)
    _seed_ew(conn, sess[1:])
    await _job(conn, store, cal, cost_model, clk).run(sess[24])
    row = _rows(conn)[rec.rec_id]
    assert (row["status"], row["strategy_id"], row["entry_type"], row["fill_px"]) == expected


async def test_ew_returns_backfill_from_aug_26_once_bhavcopy_lands(conn, store, cal, cost_model, clk):
    d = S
    store.upsert_universe_daily([{"d": d, "symbol": s, "included": True, "exclusion_reasons": None,
                                  "median_traded_value": Decimal("1")} for s in ("A", "B")])
    prev = cal.previous_trading_day(d)
    store.upsert_bars_1d([DailyBar(symbol=s, d=x, open=Decimal(c), high=Decimal(c), low=Decimal(c),
                                   close=Decimal(c), volume=1)
                          for s in ("A", "B") for x, c in ((prev, 100), (d, 101))])
    expected_days = {x.isoformat() for x in (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28),
                                             date(2026, 8, 31), d)}

    def ew() -> dict:
        return dict(conn.execute("SELECT d, ret FROM universe_ew_returns").fetchall())

    await _job(conn, store, cal, cost_model, clk,
               was_run=lambda job_id, x: job_id != JOB_BHAVCOPY or x != d).run(d)
    assert ew() == dict.fromkeys(expected_days - {d.isoformat()})
    await _job(conn, store, cal, cost_model, clk, was_run=lambda job_id, x: True).run(d)
    assert ew() == pytest.approx({**dict.fromkeys(expected_days - {d.isoformat()}), d.isoformat(): 0.01})
