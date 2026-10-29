"""Review §9 #5/#6: the hindsight ``signal_outcomes`` job."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.marketdata.store import DailyBar, MarketStore
from engine.ops.jobs import JOB_DAILY_BARS, JOB_SIGNAL_OUTCOMES, CatchUpRunner, JobRegistry
from engine.ops.signal_outcomes import SignalOutcomesJob
from engine.risk.limits import LimitTable
from engine.strategy.cost_model import CostModel
from engine.strategy.hi52_edge import sizing_notional
from tests.unit.test_reco_pipeline import log_events

S = date(2026, 9, 1)
SRC = Path(__file__).resolve().parents[2] / "src" / "engine"


class _Now:
    def __init__(self) -> None:
        self.at = datetime(2026, 9, 1, 19, 0, tzinfo=IST)

    def __call__(self) -> datetime:
        return self.at


class _Limits:
    def __init__(self) -> None:
        raw = yaml.safe_load((config_dir() / "limits.yaml").read_text(encoding="utf-8"))
        self.table = LimitTable.model_validate(raw)

    def load(self) -> LimitTable:
        return self.table

    def set(self, block: str | None, field: str, value: Decimal) -> None:
        if block is None:
            self.table = self.table.model_copy(update={field: value})
            return
        lim = self.table.limits
        new = getattr(lim, block).model_copy(update={field: value})
        self.table = self.table.model_copy(update={"limits": lim.model_copy(update={block: new})})


@pytest.fixture
def clk() -> Clock:
    return Clock(time_source=_Now())


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
def limits() -> _Limits:
    return _Limits()


@pytest.fixture
def sess(cal) -> list[date]:
    return [cal.add_sessions(S, i) for i in range(30)]


@pytest.fixture
def job(conn, store, cal, cost_model, limits, clk):
    return _job(conn, store, cal, cost_model, limits, clk)


def _job(conn, store, cal, cost_model, limits, clk, *, was_run=lambda job_id, d: job_id == JOB_DAILY_BARS):
    return SignalOutcomesJob(conn, store, cal, cost_model, limits, load_settings(), clk, was_run)


def _cost(cost_model, notional) -> float:
    return float(cost_model.breakeven_pct(Decimal(notional), "CNC"))


def _bars(store, symbol, days, *, skip=(), low=None, extra=()):
    """Bar ``i``: open 100 + i, close open + 0.5, high close + 1, low open - 1 (``low`` overrides)."""
    low = low or {}
    store.upsert_bars_1d([
        DailyBar(symbol=symbol, d=x, open=Decimal(100 + i), high=Decimal(100 + i) + Decimal("1.5"),
                 low=Decimal(low.get(i, 99 + i)), close=Decimal(100 + i) + Decimal("0.5"), volume=1)
        for i, x in enumerate(days) if i not in skip
    ] + list(extra))


def _slot(conn, d, symbol="RELIANCE", strategy_id="hi52"):
    conn.execute("INSERT INTO prescreen_day_slots (d, symbol, strategy_id, published_at) VALUES (?, ?, ?, ?)",
                 (d.isoformat(), symbol, strategy_id, f"{d.isoformat()}T10:00:00+05:30"))


def _resting(conn, signal_d, expires_d, *, symbol="RELIANCE", entry="90", stop="84.6"):
    conn.execute("INSERT INTO brk20_resting_levels (symbol, signal_d, entry, stop, target, score, expires_d, "
                 "created_at, updated_at) VALUES (?, ?, ?, ?, NULL, 1.0, ?, 't', 't')",
                 (symbol, signal_d.isoformat(), entry, stop, expires_d.isoformat()))


def _blocked(conn, signal_d, *, symbol="RELIANCE", entry="90", stop="84.6"):
    conn.execute("INSERT INTO brk20_regime_blocked (symbol, signal_d, entry, stop, score, reason, created_at) "
                 "VALUES (?, ?, ?, ?, 1.0, 'down', 't')", (symbol, signal_d.isoformat(), entry, stop))


def _split(store, symbol, ex_date, kind="split"):
    store.upsert_corp_actions([{"symbol": symbol, "ex_date": ex_date, "kind": kind}])


def _seed_ew(conn, days, ret=0.001):
    conn.executemany("INSERT INTO universe_ew_returns (d, ret, n) VALUES (?, ?, 1)",
                     [(x.isoformat(), ret) for x in days])


def _rows(conn) -> dict[tuple, dict]:
    cur = conn.execute("SELECT * FROM signal_outcomes")
    cols = [c[0] for c in cur.description]
    out = {}
    for r in cur:
        row = dict(zip(cols, r, strict=True))
        key = (row.pop("d"), row.pop("symbol"), row.pop("strategy_id"), row.pop("construct"))
        row.pop("updated_at")
        out[key] = row
    return out


def _row(conn, d, construct, symbol="RELIANCE", strategy_id="hi52") -> dict:
    return _rows(conn)[(d.isoformat(), symbol, strategy_id, construct)]


async def test_hi52_rule_fills_at_the_open_of_d_and_next_open_after_d(conn, store, cost_model, job, sess):
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess)
    _seed_ew(conn, sess[1:])
    result = await job.run(sess[24])
    cost = _cost(cost_model, "6000")
    bench = (1.001 ** 19 - 1) * 100
    assert _row(conn, sess[0], "rule") == pytest.approx({
        "status": "closed", "fill_d": sess[0].isoformat(), "fill_px": "100.00", "exit_d": sess[19].isoformat(),
        "cost_pct": cost, "net_t5": 4.5 - cost, "net_t10": 9.5 - cost, "net_t20": 19.5 - cost,
        "bench_t20": bench, "excess_t20": 19.5 - cost - bench,
    })
    nxt = _row(conn, sess[0], "next_open")
    assert (nxt["fill_d"], nxt["fill_px"], nxt["exit_d"]) == (sess[1].isoformat(), "101.00", sess[20].isoformat())
    assert nxt["net_t20"] == pytest.approx((120.5 / 101 - 1) * 100 - cost, abs=1e-5)
    assert result.scored == 2

    rows = _rows(conn)
    again = await job.run(sess[24])
    assert (_rows(conn), again.scored) == (rows, 0)


@pytest.mark.parametrize(
    ("touch", "open_", "expected"),
    [(0, "100", ("closed", 0, "90")), (0, "85", ("closed", 0, "85.00")), (4, "104", ("closed", 4, "90")),
     (5, "105", ("unfilled", None, None))],
    ids=["on_signal_d", "gap_down_on_signal_d", "signal_d_plus_4", "signal_d_plus_5"],
)
async def test_brk20_rule_touch_fill_inside_v2_5_window(conn, store, job, sess, touch, open_, expected):
    _resting(conn, sess[0], sess[5])
    _slot(conn, sess[0], strategy_id="brk20")
    _bars(store, "RELIANCE", sess)
    store.upsert_bars_1d([DailyBar(symbol="RELIANCE", d=sess[touch], open=Decimal(open_), high=Decimal(open_) + 2,
                                   low=Decimal(open_) - 16, close=Decimal(open_) + 1, volume=1)])
    await job.run(sess[24])
    row = _row(conn, sess[0], "rule", strategy_id="brk20")
    status, fill_i, px = expected
    assert (row["status"], row["fill_d"], row["fill_px"]) == (
        status, None if fill_i is None else sess[fill_i].isoformat(), px)
    assert row["exit_d"] == (None if fill_i is None else sess[fill_i + 19].isoformat())


async def test_brk20_rule_blocked_scored_like_rule(conn, store, job, sess):
    for symbol in ("A", "B"):
        _bars(store, symbol, sess, low={2: "89"})
    _resting(conn, sess[0], sess[5], symbol="A")
    _slot(conn, sess[0], "A", "brk20")
    _blocked(conn, sess[0], symbol="B")
    await job.run(sess[24])
    rows = _rows(conn)
    d = sess[0].isoformat()
    assert {k for k in rows if k[1] == "B"} == {(d, "B", "brk20", "rule_blocked")}
    assert rows[(d, "B", "brk20", "rule_blocked")] == rows[(d, "A", "brk20", "rule")]
    assert rows[(d, "B", "brk20", "rule_blocked")]["status"] == "closed"


async def test_brk20_reoffer_row_uses_latest_resting_row(conn, store, cost_model, job, sess):
    _resting(conn, sess[0], sess[5])
    _resting(conn, sess[1], sess[6], stop="85.5")             # 5% stop: notional 7,200
    _slot(conn, sess[3], strategy_id="brk20")
    _slot(conn, sess[3], "TCS", "brk20")                      # no resting row (pre-2026-09-12 shape)
    for symbol in ("RELIANCE", "TCS"):
        _bars(store, symbol, sess)
    await job.run(sess[24])
    rows = _rows(conn)
    d = sess[3].isoformat()
    assert {k[3] for k in rows if k[1] == "RELIANCE"} == {"next_open"}
    reoffer = rows[(d, "RELIANCE", "brk20", "next_open")]
    assert (reoffer["status"], reoffer["fill_d"]) == ("closed", sess[4].isoformat())
    assert reoffer["cost_pct"] == pytest.approx(_cost(cost_model, "7200"))
    assert {k[3]: v["status"] for k, v in rows.items() if k[1] == "TCS"} == {
        "rule": "unscorable", "next_open": "unscorable"}


async def test_muhurat_bar_is_ignored(conn, store, job, cal):
    days = [cal.add_sessions(date(2026, 11, 2), i) for i in range(25)]
    assert date(2026, 11, 8) not in days and days[4] == date(2026, 11, 6)
    muhurat = DailyBar(symbol="B", d=date(2026, 11, 8), open=Decimal("1"), high=Decimal("500"),
                       low=Decimal("1"), close=Decimal("500"), volume=1)
    _bars(store, "A", days)
    _bars(store, "B", days, extra=[muhurat])
    for symbol in ("A", "B"):
        _slot(conn, days[0], symbol)
        _slot(conn, days[4], symbol)
    await job.run(days[24])
    rows = _rows(conn)
    assert len(rows) == 8
    assert {k[0::3]: v for k, v in rows.items() if k[1] == "B"} == {k[0::3]: v for k, v in rows.items() if k[1] == "A"}
    assert rows[(days[4].isoformat(), "B", "hi52", "next_open")]["fill_d"] == days[5].isoformat()
    assert rows[(days[0].isoformat(), "B", "hi52", "rule")]["net_t5"] == rows[
        (days[0].isoformat(), "A", "hi52", "rule")]["net_t5"]


async def test_filled_row_never_moves(conn, store, cost_model, job, sess):
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess, skip={7})
    await job.run(sess[24])
    first = _row(conn, sess[0], "rule")
    cost = _cost(cost_model, "6000")
    assert (first["status"], first["fill_px"], first["net_t5"], first["net_t10"]) == (
        "open", "100.00", pytest.approx(4.5 - cost), None)

    store.upsert_bars_1d([
        DailyBar(symbol="RELIANCE", d=sess[0], open=Decimal("50"), high=Decimal("51"), low=Decimal("49"),
                 close=Decimal("50"), volume=1),
        DailyBar(symbol="RELIANCE", d=sess[4], open=Decimal("104"), high=Decimal("201"), low=Decimal("103"),
                 close=Decimal("200"), volume=1),
        DailyBar(symbol="RELIANCE", d=sess[7], open=Decimal("107"), high=Decimal("108.5"), low=Decimal("106"),
                 close=Decimal("107.5"), volume=1),
    ])
    await job.run(sess[24])
    row = _row(conn, sess[0], "rule")
    assert {k: row[k] for k in ("fill_d", "fill_px", "cost_pct", "net_t5")} == {
        k: first[k] for k in ("fill_d", "fill_px", "cost_pct", "net_t5")}
    assert (row["status"], row["net_t20"]) == ("closed", pytest.approx(19.5 - cost))


async def test_cost_fixed_at_first_scoring(conn, store, cost_model, limits, job, sess):
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess)
    await job.run(sess[3])
    assert _row(conn, sess[0], "rule")["status"] == "open"

    limits.set(None, "capital_base_inr", Decimal("70000"))       # floor rung 63,000: notional 10,500
    _slot(conn, sess[3], "TCS")
    _bars(store, "TCS", sess)
    await job.run(sess[24])
    assert _row(conn, sess[0], "rule")["cost_pct"] == pytest.approx(_cost(cost_model, "6000"))
    assert _row(conn, sess[3], "rule", "TCS")["cost_pct"] == pytest.approx(_cost(cost_model, "10500"))


async def test_closed_row_only_gains_bench(conn, store, job, sess):
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess)
    await job.run(sess[24])
    closed = _row(conn, sess[0], "rule")
    assert (closed["status"], closed["bench_t20"]) == ("closed", None)

    store.upsert_bars_1d([DailyBar(symbol="RELIANCE", d=sess[19], open=Decimal("1"), high=Decimal("1"),
                                   low=Decimal("1"), close=Decimal("1"), volume=1)])
    _seed_ew(conn, sess[1:])
    await job.run(sess[24])
    row = _row(conn, sess[0], "rule")
    bench = (1.001 ** 19 - 1) * 100
    assert row == {**closed, "bench_t20": pytest.approx(bench),
                   "excess_t20": pytest.approx(closed["net_t20"] - bench)}
    assert (await job.run(sess[24])).scored == 0


@pytest.mark.parametrize(
    ("strategy_id", "low", "ex_i", "kind", "expected"),
    [
        ("hi52", None, 10, "split", "void_ca"),
        ("hi52", None, 10, "dividend", "closed"),
        ("brk20", {3: "89"}, 2, "bonus", "void_ca"),          # (signal_d, fill]
        ("brk20", None, 2, "rights", "void_ca"),              # inside an unfilled window
        ("brk20", {3: "89"}, 0, "split", "closed"),           # on signal_d itself: outside (signal_d, fill]
    ],
    ids=["hold", "dividend_ignored", "before_fill", "unfilled_window", "on_signal_d"],
)
async def test_corporate_actions_void(conn, store, job, sess, strategy_id, low, ex_i, kind, expected):
    if strategy_id == "brk20":
        _resting(conn, sess[0], sess[5])
    _slot(conn, sess[0], strategy_id=strategy_id)
    _bars(store, "RELIANCE", sess, low=low)
    _split(store, "RELIANCE", sess[ex_i], kind)
    await job.run(sess[24])
    row = _row(conn, sess[0], "rule", strategy_id=strategy_id)
    assert row["status"] == expected
    if expected == "void_ca":
        assert (row["exit_d"], row["net_t20"]) == (sess[ex_i].isoformat(), None)
    if strategy_id == "hi52" and expected == "void_ca":
        assert row["net_t5"] is not None                       # T+5 exited before the ex-date


async def test_missing_anchor_bar_is_unscorable_after_a_two_session_grace(conn, store, job, sess):
    _slot(conn, sess[0])
    _slot(conn, sess[0], "TCS")
    _bars(store, "RELIANCE", sess, skip={0})
    _bars(store, "TCS", sess, skip={1})

    def statuses() -> dict:
        return {(k[1], k[3]): v["status"] for k, v in _rows(conn).items()}

    await job.run(sess[2])
    assert statuses()[("RELIANCE", "rule")] == statuses()[("TCS", "next_open")] == "open"
    await job.run(sess[24])
    assert statuses() == {("RELIANCE", "rule"): "unscorable", ("RELIANCE", "next_open"): "closed",
                          ("TCS", "rule"): "open", ("TCS", "next_open"): "unscorable"}


async def test_bar_missing_alarm_fires_every_night_only_while_t20_is_open(conn, store, job, sess, caplog):
    _slot(conn, sess[0])
    _slot(conn, sess[0], "TCS")
    _bars(store, "RELIANCE", sess, skip={1})                  # the hole right after the fill
    _bars(store, "TCS", sess, skip={22})                      # a hole after both T+20 exits
    with caplog.at_level(logging.WARNING, logger="engine.ops.signal_outcomes"):
        await job.run(sess[24])
        await job.run(sess[24])
    alarms = [(r.key[1], r.key[3], r.session) for r in log_events(caplog, "signal_outcome_bar_missing")]
    assert alarms == [("RELIANCE", "rule", sess[1].isoformat())] * 2


async def test_calendar_horizon_leaves_row_open(conn, store, cost_model, job, cal, sess, monkeypatch):
    add_sessions = cal.add_sessions

    def bounded(d, n, **kw):
        out = add_sessions(d, n, **kw)
        if out > sess[20]:
            raise ValueError("calendar horizon")
        return out

    monkeypatch.setattr(cal, "add_sessions", bounded)
    _slot(conn, sess[20], "A")
    _slot(conn, sess[0], "B")
    _bars(store, "A", sess)
    _bars(store, "B", sess)
    await job.run(sess[24])
    rows = _rows(conn)
    a = rows[(sess[20].isoformat(), "A", "hi52", "next_open")]
    assert (a["status"], a["fill_d"], a["cost_pct"]) == ("open", None, pytest.approx(_cost(cost_model, "6000")))
    assert rows[(sess[20].isoformat(), "A", "hi52", "rule")]["status"] == "open"
    assert rows[(sess[0].isoformat(), "B", "hi52", "rule")]["status"] == "closed"


async def test_a_table_that_cannot_size_fails_the_job_and_marks_nothing(conn, store, limits, job, sess):
    limits.set("per_trade_risk", "swing_position_pct", 0.0)
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess)
    with pytest.raises(ValueError, match="cannot size"):
        await job.run(sess[24])
    assert _rows(conn) == {}


async def test_unfinished_until_daily_bars_ran_and_a_replay_after_a_later_day_skips(
    conn, store, cal, cost_model, limits, clk, sess
):
    _slot(conn, sess[0])
    _bars(store, "RELIANCE", sess)
    waiting = await _job(conn, store, cal, cost_model, limits, clk, was_run=lambda job_id, d: False).run(sess[24])
    assert (waiting.unfinished, _rows(conn)) == (True, {})

    CatchUpRunner(conn, clk, cal, JobRegistry()).record_run(JOB_SIGNAL_OUTCOMES, sess[25])
    replay = await _job(conn, store, cal, cost_model, limits, clk).run(sess[24])
    assert (replay.unfinished, replay.scored, _rows(conn)) == (False, 0, {})


@pytest.mark.parametrize(
    ("strategy_id", "cap", "notional"),
    [
        ("hi52", None, "6000"),
        ("ins", None, "7200"),
        ("hi52", ("per_stock_exposure", "cnc_notional_inr"), "5000"),
        ("hi52", ("capital_cap", "max_deployed_capital_inr"), "5000"),
    ],
    ids=["hi52_floor_rung", "ins_stop", "cnc_cap", "deployed_cap"],
)
async def test_cost_pct_floor_rung(conn, store, cost_model, limits, job, sess, strategy_id, cap, notional):
    assert limits.table.capital_base_inr == Decimal("40000")
    assert sizing_notional(Decimal("36000"), position_pct=Decimal("2.0"), gap_mult=Decimal("2.0"),
                           stop_pct=Decimal("6")) == Decimal("6000")
    if cap is not None:
        limits.set(*cap, Decimal("5000"))
    _slot(conn, sess[0], strategy_id=strategy_id)
    _bars(store, "RELIANCE", sess)
    await job.run(sess[24])
    assert _row(conn, sess[0], "rule", strategy_id=strategy_id)["cost_pct"] == pytest.approx(
        _cost(cost_model, notional))


async def test_only_signal_strategies_are_scored(conn, store, job, sess):
    _slot(conn, sess[0], strategy_id="cat")
    _bars(store, "RELIANCE", sess)
    await job.run(sess[24])
    assert _rows(conn) == {}


def test_no_decision_path_reads_signal_outcomes():
    paths = [*(SRC / p for p in ("strategy", "risk", "intelligence")), SRC / "ops" / "pipeline.py"]
    files = [f for p in paths for f in (p.rglob("*.py") if p.is_dir() else [p])]
    assert len(files) > 20
    assert [f for f in files if re.search("signal_outcomes|forward_verdicts", f.read_text(encoding="utf-8"))] == []
