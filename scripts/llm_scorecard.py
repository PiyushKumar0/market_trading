#!/usr/bin/env python
"""News and planner scorecard (review A1). Read-only. Every section is interim and decides nothing.

* News: ``catalyst_watchlist`` rows by materiality tercile (order-statistic cuts over the rows read) x direction.
  Entry at the next session's open after d (a row carries no build time, and a post-open digest can grade a
  story first seen after the open); excess at T+5 and T+10. The excess is the stock's, unsigned by direction.
* Planner: ``day_plans`` focus symbols by bias. Entry at open(d) when the plan was written before 09:15 IST on d,
  else at the next session's open; excess at T+1 and T+5.
* Excess: close(k-th session, the entry session counting 1) / open(entry) - 1, minus the core EW
  (``research_common.core_ew``, survivorship-tainted) compounded over the entry through exit sessions, in percent;
  never raw. The EW window includes the entry session's overnight gap, which the trade does not hold:
  ``bench_pct``'s (entry, exit] window would leave T+1 unbenchmarked. A structural ex-date in (entry, exit], a
  missing bar or an EW hole leaves the observation unmeasured (counted).
* Reliability: ``agent_calls`` by agent, fail reason and IST ISO week.
* Analyst: ``engine.learning.forward_verdicts.analyst_reading`` (verdict, reason, looks).

state.db opens through a ``file:...?mode=ro`` URI; market data comes from a research snapshot, never the live
store. Exit codes: 0 ran, 2 refused.

Usage:
    python scripts/llm_scorecard.py [--state-db PATH] [--market-db PATH] [--as-of YYYY-MM-DD] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import duckdb  # noqa: E402

import research_common as rc  # noqa: E402
from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning import forward_verdicts as fv  # noqa: E402

LABEL = "interim, decides nothing"
NEWS_HORIZONS = (5, 10)
PLAN_HORIZONS = (1, 5)
SESSION_OPEN = time(9, 15)
EXCESS = ("close(T+k, entry session = 1)/open(entry) - 1 minus the core EW compounded over the entry..exit "
          "sessions, %")

Measure = Callable[[str, date, int], float | None]


# ----------------------------------------------------------------------------- measurement
def excess_pct(s, entry: date, k: int, daily: dict[date, float | None], grid: list[date], ex_dates) -> float | None:
    """Excess % of the trade entered at the first bar on or after ``entry`` and exited at its k-th session's close."""
    i = bisect_left(s.dates, entry)
    j = i + k - 1
    if j >= len(s.dates) or not s.open[i] > 0:
        return None
    e, x = s.dates[i], s.dates[j]
    if any(e < d <= x for d in ex_dates):
        return None
    sessions = grid[bisect_left(grid, e):bisect_right(grid, x)]
    ew = rc.compound(daily, sessions) if sessions else None
    return None if ew is None else (float(s.close[j] / s.open[i]) - 1 - ew) * 100


def regular_sessions(s, cal: NSECalendar):
    """``s`` without special-session (muhurat) bars, which ``research_common.core_ew`` does not count either."""
    keep = [(sess := cal.session(d)) is None or not sess.is_muhurat for d in s.dates]
    if all(keep):
        return s
    return replace(s, dates=[x for x, k in zip(s.dates, keep, strict=True) if k], open=s.open[keep],
                   high=s.high[keep], low=s.low[keep], close=s.close[keep], volume=s.volume[keep],
                   rows=[x for x, k in zip(s.rows, keep, strict=False) if k])


def measurer(conn, as_of: date, daily: dict[date, float | None], structural: dict[str, list[date]]) -> Measure:
    grid, cache = list(daily), {}
    cal = NSECalendar(config_dir() / "calendar", Clock())

    def m(symbol: str, entry: date, k: int) -> float | None:
        if not symbol:
            return None
        if symbol not in cache:
            found = rc.bb.load_series(conn, rc.FULL_HISTORY_START, as_of, symbols=[symbol])
            s = next(iter(found.values()), None)
            cache[symbol] = None if s is None else regular_sessions(s, cal)
        s = cache[symbol]
        return None if s is None else excess_pct(s, entry, k, daily, grid, structural.get(s.symbol, ()))
    return m


def buckets(obs: list[tuple[str, list[float | None]]], horizons: tuple[int, ...]) -> dict[str, Any]:
    """Per label and horizon: stats of the measured excess and the unmeasured count."""
    grouped: dict[str, list[list[float | None]]] = defaultdict(list)
    for label, xs in obs:
        grouped[label].append(xs)
    return {label: {f"T+{k}": {**asdict(fv.stats([x[h] for x in rows if x[h] is not None])),
                                "unmeasured": sum(x[h] is None for x in rows)}
                    for h, k in enumerate(horizons)}
            for label, rows in sorted(grouped.items())}


# ----------------------------------------------------------------------------- sections
def tercile(m: float | None, cuts: tuple[float, float] | None) -> str:
    if m is None or cuts is None:
        return "none"
    return "low" if m <= cuts[0] else "mid" if m <= cuts[1] else "high"


def news_section(rows: list[dict[str, Any]], m: Measure) -> dict[str, Any]:
    v = sorted(r["materiality"] for r in rows if r["materiality"] is not None)
    cuts = (v[(len(v) - 1) // 3], v[2 * (len(v) - 1) // 3]) if v else None
    obs = [(f"{tercile(r['materiality'], cuts)} x {r['direction'] or 'none'}",
            [m(r["symbol"], r["d"] + timedelta(days=1), k) for k in NEWS_HORIZONS]) for r in rows]
    return {"tercile_cuts": cuts, "buckets": buckets(obs, NEWS_HORIZONS)}


def plan_entry(d: date, created_at: str | None) -> date:
    """open(d) when the plan existed before the 09:15 open on d, else the next session."""
    try:
        at = datetime.fromisoformat(created_at)
    except (TypeError, ValueError):
        return d + timedelta(days=1)
    at = at if at.tzinfo else at.replace(tzinfo=IST)
    return d if at < datetime.combine(d, SESSION_OPEN, tzinfo=IST) else d + timedelta(days=1)


def planner_section(plans: list[tuple[date, str, str]], m: Measure) -> dict[str, Any]:
    obs, unparsable = [], 0
    for d, payload, created_at in plans:
        try:
            focus = json.loads(payload).get("focus") or []
        except (TypeError, ValueError, AttributeError):
            unparsable += 1
            continue
        entry = plan_entry(d, created_at)
        obs += [(str(f.get("bias")), [m(f.get("symbol"), entry, k) for k in PLAN_HORIZONS]) for f in focus]
    return {"unparsable_plans": unparsable, "buckets": buckets(obs, PLAN_HORIZONS)}


def _ist(at: Any) -> datetime | None:
    try:
        t = datetime.fromisoformat(str(at))
    except ValueError:
        return None
    return t.astimezone(IST) if t.tzinfo else t.replace(tzinfo=IST)


def _rate(c: Counter) -> dict[str, Any]:
    n = c["ok"] + c["failed"]
    return {"calls": n, "failed": c["failed"], "fail_rate": round(c["failed"] / n, 4)}


def reliability(calls: list[tuple], as_of: date) -> dict[str, Any]:
    by_agent: dict[str, Counter] = defaultdict(Counter)
    reasons: dict[str, Counter] = defaultdict(Counter)
    weeks: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    unparsable = 0
    for agent, ok, reason, at in calls:
        t = _ist(at)
        if t is None:
            unparsable += 1
            continue
        if t.date() > as_of:
            continue
        y, w, _ = t.isocalendar()
        outcome = "ok" if ok else "failed"
        by_agent[agent][outcome] += 1
        weeks[agent][f"{y}-W{w:02d}"][outcome] += 1
        if not ok:
            reasons[agent][reason or "none"] += 1
    return {"by_agent": {a: _rate(c) for a, c in sorted(by_agent.items())},
            "fail_reasons": {a: dict(c.most_common()) for a, c in sorted(reasons.items())},
            "by_week": {a: {wk: _rate(c) for wk, c in sorted(ws.items())} for a, ws in sorted(weeks.items())},
            "unparsable_at": unparsable}


# ----------------------------------------------------------------------------- run + render
def run(sconn: sqlite3.Connection, mconn, db: Path, as_of: date) -> dict[str, Any]:
    daily, source = rc.core_ew(mconn, db, as_of)
    m = measurer(mconn, as_of, daily, rc.bb.load_structural_ex_dates(mconn))
    watch = [dict(zip(("d", "symbol", "direction", "materiality"), r, strict=True)) for r in mconn.execute(
        "SELECT d, symbol, direction, materiality FROM catalyst_watchlist WHERE d <= ? ORDER BY d, symbol",
        [as_of]).fetchall()]
    plans = [(date.fromisoformat(d), p, c) for d, p, c in sconn.execute(
        "SELECT d, payload, created_at FROM day_plans WHERE d <= ? ORDER BY d", (as_of.isoformat(),))]
    calls = sconn.execute("SELECT agent_id, ok, fail_reason, at FROM agent_calls").fetchall()
    reading = fv.analyst_reading(sconn, as_of=as_of, registered_from=fv.REGISTERED_FROM)
    return {
        "meta": {"script": "scripts/llm_scorecard.py", "label": LABEL, "as_of": str(as_of), "market_db": str(db),
                 "excess": EXCESS, "ew": {"source": source, "label": rc.SURVIVORSHIP_LABEL, "sessions": len(daily)}},
        "news": news_section(watch, m),
        "planner": planner_section(plans, m),
        "reliability": reliability(calls, as_of),
        "analyst": {"verdict": reading.verdict, "reason": reading.reason, "looks": [asdict(lk) for lk in reading.looks]},
    }


def _num(v: float | None, fmt: str = "+.2f", unit: str = "") -> str:
    return "n/a" if v is None else format(v, fmt) + unit


def _bucket_lines(b: dict[str, Any]) -> list[str]:
    return [f"  {label:<16} " + "  ".join(
        f"{h} n={s['n']} med {_num(s['median'])} mean {_num(s['mean'])} hit {_num(s['hit_rate'], '.0%')} "
        f"unmeasured {s['unmeasured']}" for h, s in hs.items()) for label, hs in b.items()] or ["  none"]


def _reliability_lines(r: dict[str, Any]) -> list[str]:
    out = []
    for a, c in r["by_agent"].items():
        out.append(f"  {a}: calls {c['calls']} failed {c['failed']} ({c['fail_rate']:.1%})")
        if a in r["fail_reasons"]:
            out.append("    fail reasons: " + ", ".join(f"{k} {n}" for k, n in r["fail_reasons"][a].items()))
        out += [f"    {wk} calls {w['calls']} failed {w['failed']}" for wk, w in r["by_week"][a].items()]
    return out or ["  none"]


def render(doc: dict[str, Any]) -> str:
    m, a = doc["meta"], doc["analyst"]
    out = [f"LLM SCORECARD as of {m['as_of']} ({LABEL}); market data {m['market_db']}",
           f"excess: {m['excess']}", f"EW: {m['ew']['source']}, {m['ew']['sessions']} sessions [{m['ew']['label']}]",
           "", f"NEWS - catalyst_watchlist materiality tercile x direction, next-open excess ({LABEL})",
           f"  tercile cuts {doc['news']['tercile_cuts'] or 'none'}", *_bucket_lines(doc["news"]["buckets"]),
           "", f"PLANNER - day_plans focus bias, excess ({LABEL})", *_bucket_lines(doc["planner"]["buckets"])]
    if doc["planner"]["unparsable_plans"]:
        out.append(f"  unparsable plans: {doc['planner']['unparsable_plans']}")
    out += ["", f"RELIABILITY - agent_calls by agent, fail reason, IST week ({LABEL})",
            *_reliability_lines(doc["reliability"]),
            "", f"ANALYST - forward_verdicts.analyst_reading ({LABEL})", f"  verdict {a['verdict']} ({a['reason']})"]
    out += [f"  look {i} (first {lk['size']} cells): {'formed' if lk['formed'] else 'not formed'}; {lk['n_cells']} "
            f"cells, final {lk['n_final']}, valued {lk['n_valued']}; mean enter - no_action {_num(lk['mean_diff'], unit='pp')}"
            for i, lk in enumerate(a["looks"], 1)]
    return rc.ascii_text("\n".join(out))


def newest_snapshot() -> Path | None:
    return max((repo_root() / "data" / "research").glob("market_*.duckdb"), key=lambda p: p.name, default=None)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=f"News and planner scorecard (read-only; {LABEL}).")
    ap.add_argument("--state-db", type=Path, default=repo_root() / "data" / "state.db")
    ap.add_argument("--market-db", type=Path, default=None,
                    help="research snapshot (default: the newest data/research/market_*.duckdb)")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="default: today IST")
    ap.add_argument("--out", type=Path, default=None, help="JSON path; the .md is written beside it")
    args = ap.parse_args(argv)
    as_of = args.as_of or datetime.now(IST).date()
    db = args.market_db or newest_snapshot()
    try:
        if db is None:
            raise rc.Refusal("no research snapshot (data/research/market_*.duckdb); pass --market-db")
        mconn = rc.open_snapshot(db)
        try:
            sconn = sqlite3.connect(f"file:{args.state_db.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0)
            try:
                doc = run(sconn, mconn, db, as_of)
            finally:
                sconn.close()
        finally:
            mconn.close()
    except (rc.Refusal, sqlite3.Error, duckdb.Error) as exc:
        why = str(exc) if isinstance(exc, rc.Refusal) else f"{type(exc).__name__}: {exc}"
        print(f"llm_scorecard: REFUSED: {rc.ascii_text(why)}", file=sys.stderr)
        return 2
    out = args.out or repo_root() / "data" / "reports" / f"llm_scorecard_{as_of}.json"
    text = render(doc)
    md = rc.write_reports(doc, f"# LLM SCORECARD ({LABEL})\n\n```\n{text}\n```\n", out)
    print(text)
    print(f"JSON -> {out}\nMD   -> {md}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
