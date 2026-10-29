"""scripts/llm_scorecard.py: the news and planner scorecard (review A1). Synthetic only."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_llm_scorecard", _ROOT / "scripts" / "llm_scorecard.py")
sc = importlib.util.module_from_spec(_spec)
sys.modules["mt_llm_scorecard"] = sc
_spec.loader.exec_module(sc)
rc = sc.rc

from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402


def _weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


D = _weekdays(date(2026, 10, 5), 10)


def _recorder(calls: list) -> object:
    return lambda symbol, entry, k: calls.append((symbol, entry, k)) or 1.0


def test_news_buckets_are_materiality_tercile_by_direction():
    rows = [{"d": D[0], "symbol": s, "direction": dr, "materiality": m}
            for s, dr, m in (("A", "long", 0.1), ("B", "long", 0.4), ("C", "short", 0.7), ("E", None, None))]
    calls: list = []
    news = sc.news_section(rows, _recorder(calls))
    assert {k: v["T+5"]["n"] for k, v in news["buckets"].items()} == {
        "low x long": 1, "mid x long": 1, "high x short": 1, "none x none": 1}
    assert {(e, k) for _, e, k in calls} == {(D[0] + timedelta(days=1), 5), (D[0] + timedelta(days=1), 10)}


def _series(close_at_exit: float) -> rc.bb.Series:
    c = np.full(len(D), 100.0)
    c[7] = close_at_exit
    return rc.bb.Series("SYM", D, np.full(len(D), 100.0), c, c, c, np.full(len(D), 1000.0), [])


def _daily(**holes) -> dict:
    daily = {d: 0.01 for d in D}
    daily[D[2]] = daily[D[8]] = 0.5                       # outside the window
    return daily | {D[int(k[1:])]: v for k, v in holes.items()}


def test_excess_is_against_the_ew_over_the_same_window_never_raw():
    daily = _daily()
    assert sc.excess_pct(_series(110.0), D[3], 5, daily, list(daily), []) == pytest.approx(10 - (1.01 ** 5 - 1) * 100)
    assert sc.excess_pct(_series(110.0), D[3], 1, daily, list(daily), []) == pytest.approx(-1.0)


@pytest.mark.parametrize("daily, ex_dates, k", [
    (_daily(d5=None), [], 5),                             # an EW hole in the window
    (_daily(), [D[5]], 5),                                # a structural ex-date inside the hold
    (_daily(), [], 8),                                    # the exit is past the last bar
    ({}, [], 5),                                          # no EW sessions at all: never raw
])
def test_unmeasurable_excess_is_none(daily, ex_dates, k):
    assert sc.excess_pct(_series(110.0), D[3], k, daily, list(daily), ex_dates) is None


def test_planner_bias_buckets_enter_at_the_first_open_after_the_plan():
    plans = [(D[2], json.dumps({"focus": [{"symbol": "A", "bias": "long"}, {"symbol": "B", "bias": "avoid"}]}),
              f"{D[2]}T08:50:00+05:30"),
             (D[3], json.dumps({"focus": [{"symbol": "C", "bias": "long"}]}), f"{D[3]}T10:05:00+05:30")]
    calls: list = []
    planner = sc.planner_section(plans, _recorder(calls))
    assert {k: (v["T+1"]["n"], v["T+5"]["n"]) for k, v in planner["buckets"].items()} == {"avoid": (1, 1), "long": (2, 2)}
    assert {(s, e) for s, e, _ in calls} == {("A", D[2]), ("B", D[2]), ("C", D[3] + timedelta(days=1))}


def test_reliability_is_by_agent_fail_reason_and_ist_week():
    calls = [("news", 1, None, "2026-10-26T10:00:00+05:30"),
             ("news", 0, "timeout", "2026-10-25T20:00:00+00:00"),     # Sunday UTC, Monday 01:30 IST
             ("planner", 0, "parse", "2026-10-30T08:50:00+05:30")]    # after as_of
    rel = sc.reliability(calls, date(2026, 10, 29))
    assert rel["by_agent"] == {"news": {"calls": 2, "failed": 1, "fail_rate": 0.5}}
    assert rel["fail_reasons"] == {"news": {"timeout": 1}}
    assert list(rel["by_week"]["news"]) == ["2026-W44"]


def _snapshot(tmp_path: Path, clock, *, seeded: bool = False) -> Path:
    db = tmp_path / "research" / "market_2026-10-29.duckdb"
    store = MarketStore(db, tmp_path / "parquet", clock).open()
    if seeded:
        store.upsert_bars_1d([DailyBar(symbol="AAA", d=d, open=Decimal(100), high=Decimal(100), low=Decimal(100),
                                       close=Decimal(100), volume=1000, src="bhavcopy") for d in D])
        store.replace_catalyst_watchlist(D[0], [{"symbol": "AAA", "grade": "originating", "direction": "long",
                                                 "materiality": 0.8}])
    store.close()
    (tmp_path / "universe").mkdir()
    (tmp_path / "universe" / "core_index_cached.csv").write_text("Symbol\nAAA\n", encoding="utf-8")
    return db


def test_empty_inputs_render_none(tmp_path, clock, conn, db_path, capsys):
    out = tmp_path / "reports" / "s.json"
    argv = ["--state-db", db_path, "--market-db", str(_snapshot(tmp_path, clock)), "--as-of", "2026-10-29",
            "--out", str(out)]
    assert sc.main(argv) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["news"]["buckets"] == doc["planner"]["buckets"] == doc["reliability"]["by_agent"] == {}
    text = capsys.readouterr().out
    assert text.isascii() and text.count("\n  none") == 3 and text.count(sc.LABEL) == 5
    assert doc["analyst"]["verdict"] in text and out.with_suffix(".md").exists()


def test_snapshot_and_state_rows_reach_their_sections(tmp_path, clock, conn, db_path):
    conn.execute("INSERT INTO day_plans (d, payload, created_at) VALUES (?, ?, ?)",
                 (D[1].isoformat(), json.dumps({"focus": [{"symbol": "AAA", "bias": "long"}]}), f"{D[1]}T08:50:00+05:30"))
    conn.commit()
    out = tmp_path / "reports" / "s.json"
    argv = ["--state-db", db_path, "--market-db", str(_snapshot(tmp_path, clock, seeded=True)),
            "--as-of", str(D[-1]), "--out", str(out)]
    assert sc.main(argv) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["news"]["buckets"]["low x long"]["T+10"] == {"n": 0, "median": None, "mean": None, "hit_rate": None,
                                                             "unmeasured": 1}      # D[1] + 9 sessions: past D[-1]
    assert doc["news"]["buckets"]["low x long"]["T+5"]["median"] == 0.0
    assert doc["planner"]["buckets"]["long"]["T+5"]["median"] == 0.0


def test_refuses_the_live_store(tmp_path, db_path, capsys):
    assert sc.main(["--state-db", db_path, "--market-db", str(tmp_path / "market.duckdb")]) == 2
    assert "live store" in capsys.readouterr().err
