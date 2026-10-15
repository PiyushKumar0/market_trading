#!/usr/bin/env python
"""C5 NO-STOP FORWARD-TEST VERDICT (IMPLEMENTATION_PLAN.md §6.1 "C5 NO-STOP FORWARD TEST", owner D14).

Compares the shipped stop/target exit with NO stop plus a fixed 20-session time exit on live
signals. Read-only: state.db through a ``file:...?mode=ro`` URI, the optional market DuckDB through
``read_only=True``. Nothing is ever written to either database; only the report JSON is written.

THE RULE (pre-registered; do not re-derive it after seeing the numbers)
* Population: ``rec_outcomes`` joined to ``recommendations`` whose payload ``kind`` is ``entry`` (a
  missing kind counts as entry, as in ``rec_outcomes.py``), ``strategy_id`` in {hi52, ins, brk20}.
* Eligible: ``status = 'closed'`` AND ``net_pct``, ``net_t20``, ``excess_t20`` all non-NULL.
* E = median ``excess_t20`` (no-stop T+20 net minus the benchmark). D = median paired
  ``net_t20 - net_pct`` (no-stop minus the shipped rule's outcome).
* n < 20 -> INSUFFICIENT; else NO-STOP SUPPORTED iff D > 0 AND E > 0; STOP NOT THE PROBLEM iff
  D <= 0; NO EDGE EITHER WAY iff D > 0 AND E <= 0.
* Registered LOOKS (no optional stopping): eligible rows are ordered by (fill_d, rec_id); look 1 is
  the first 20, look 2 the first 40 (supersedes look 1). The running all-n figures carry no label.
Everything else printed (T+5/T+10, bootstrap, sign test, confirmation slice, the paper-portfolio
section and its counterfactual) is a diagnostic and decides nothing.

The DECISION reads ``rec_outcomes`` as stored (its latest ``updated_at`` is printed); ``--as-of``
bounds only the (P) counterfactual walk. Exit code is always 0.

Usage:
    python scripts/no_stop_verdict.py
    python scripts/no_stop_verdict.py --state-db data/state.db --market-db <research snapshot>
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sqlite3
import statistics
import sys
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning.benchmark import bench_pct  # noqa: E402
from engine.learning.exit_sim import Held, Outcome, SimBar, simulate  # noqa: E402

STRATEGIES = ("hi52", "ins", "brk20")
MIN_N = 20
REVIEW_N = 40
CONFIRM_FROM = "2026-10-16"          # fill_d >= this = the confirmation slice (decides nothing)
POLICY_FROM = "2026-10-16"           # first session under the time-only deploy: window for (P) refusals/halts
FLOOR_CAUSES = ("floor_weekly_drawdown", "floor_equity_floor_rung", "floor_cumulative_floor")
LOOKS = (("look1", 20), ("look2", 40))
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20261015
DIAG_HORIZONS = (5, 10)
EXIT_POLICY = "time_only"

LABEL_INSUFFICIENT = "INSUFFICIENT"
LABEL_SUPPORTED = "NO-STOP SUPPORTED"
LABEL_STOP_OK = "STOP NOT THE PROBLEM"
LABEL_NO_EDGE = "NO EDGE EITHER WAY"

_FINAL = ("void_ca", "unfilled", "unscorable")
LOW_POWER_CAVEAT = "at n = 20 the verdict is an owner input, not proof"


# =============================================================================== statistics
def cell(values: Sequence[float]) -> dict[str, Any]:
    """n, median, mean and hit rate (share strictly > 0)."""
    if not values:
        return {"n": 0, "median": None, "mean": None, "hit_rate": None}
    return {
        "n": len(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "hit_rate": sum(1 for v in values if v > 0) / len(values),
    }


def decide(n: int, d: float | None, e: float | None) -> str:
    """The registered decision rule."""
    if n < MIN_N or d is None or e is None:
        return LABEL_INSUFFICIENT
    if d <= 0:
        return LABEL_STOP_OK
    return LABEL_SUPPORTED if e > 0 else LABEL_NO_EDGE


def sign_test_p(diffs: Sequence[float]) -> dict[str, Any]:
    """Exact two-sided sign test on the NONZERO paired differences."""
    nz = [x for x in diffs if x != 0]
    n = len(nz)
    if n == 0:
        return {"n_nonzero": 0, "n_positive": 0, "p": None}
    k = sum(1 for x in nz if x > 0)
    tail = sum(math.comb(n, i) for i in range(min(k, n - k) + 1))
    return {"n_nonzero": n, "n_positive": k, "p": min(1.0, 2 * tail / 2**n)}


def bootstrap_ci(
    e: Sequence[float], d: Sequence[float], *, resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """90% percentile intervals for median(e) and median(d). One resample draws row indices once and
    applies them to both series, so the pairing is preserved. Interval = the 5th and 95th percentile
    ranks of the sorted resampled statistics."""
    n = len(e)
    if n == 0 or len(d) != n:
        return {"E": None, "D": None, "resamples": resamples, "seed": seed}
    rng = random.Random(seed)
    es: list[float] = []
    ds: list[float] = []
    for _ in range(resamples):
        idx = [rng.randrange(n) for _ in range(n)]
        es.append(statistics.median([e[i] for i in idx]))
        ds.append(statistics.median([d[i] for i in idx]))
    es.sort()
    ds.sort()
    lo, hi = math.ceil(0.05 * resamples) - 1, math.ceil(0.95 * resamples) - 1
    return {"E": [es[lo], es[hi]], "D": [ds[lo], ds[hi]], "resamples": resamples, "seed": seed}


# =============================================================================== population
def _is_entry(payload: str | None) -> bool:
    try:
        data = json.loads(payload or "{}")
    except (TypeError, ValueError):
        return False
    return isinstance(data, dict) and (data.get("kind") or "entry") == "entry"


def load_population(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Every scored entry rec of the three strategies, whatever its status."""
    cur = conn.execute(
        "SELECT o.rec_id, o.strategy_id, o.status, o.fill_d, o.exit_reason, o.net_pct, o.net_t5, "
        "o.net_t10, o.net_t20, o.excess_t20, o.updated_at, r.payload "
        "FROM rec_outcomes o JOIN recommendations r ON r.rec_id = o.rec_id "
        f"WHERE o.strategy_id IN ({', '.join('?' for _ in STRATEGIES)}) ORDER BY o.rec_id",
        STRATEGIES,
    )
    cols = [c[0] for c in cur.description]
    rows = []
    for rec in cur.fetchall():
        row = dict(zip(cols, rec, strict=True))
        if _is_entry(row.pop("payload")):
            rows.append(row)
    return rows


def is_eligible(row: dict[str, Any]) -> bool:
    return row["status"] == "closed" and None not in (
        row["net_pct"], row["net_t20"], row["excess_t20"]
    )


def _t20_past(fill_d: str, calendar: NSECalendar, today: date) -> bool:
    """True once the T+20 session (fill session + 19) is on or before ``today``, the last day
    ``rec_outcomes`` has scored (a T+20 that falls on an unscored day is still awaited). A date beyond
    the calendar's horizon is not past."""
    try:
        return calendar.add_sessions(date.fromisoformat(fill_d), 19) <= today
    except ValueError:
        return False


def split_population(
    rows: Sequence[dict[str, Any]], calendar: NSECalendar | None = None, today: date | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Eligible rows (ordered by (fill_d, rec_id)) and the counts. Pending is split: ``awaiting_t20``
    (T+20 not yet reached, or open with no fill), ``t20_past_due`` (closed, T+20 past, still NULL:
    can be permanent) and ``awaiting_shipped_exit`` (open although T+20 is past)."""
    today = today or datetime.now(IST).date()
    eligible = sorted((r for r in rows if is_eligible(r)), key=lambda r: (r["fill_d"] or "", r["rec_id"]))
    counts = {
        "population": len(rows),
        "eligible": len(eligible),
        **{s: sum(1 for r in rows if r["status"] == s) for s in _FINAL},
        "awaiting_t20": 0, "t20_past_due": 0, "awaiting_shipped_exit": 0,
    }
    for r in rows:
        if is_eligible(r) or r["status"] in _FINAL:
            continue
        if r["fill_d"] is None or calendar is None or not _t20_past(r["fill_d"], calendar, today):
            counts["awaiting_t20"] += 1
        elif r["status"] == "closed":
            counts["t20_past_due"] += 1
        else:
            counts["awaiting_shipped_exit"] += 1
    counts["pending"] = counts["awaiting_t20"] + counts["t20_past_due"] + counts["awaiting_shipped_exit"]
    return eligible, counts


def paired(rows: Sequence[dict[str, Any]]) -> list[float]:
    """``net_t20 - net_pct`` of the SAME rec."""
    return [r["net_t20"] - r["net_pct"] for r in rows]


def decision_block(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    e_vals = [r["excess_t20"] for r in rows]
    d_vals = paired(rows)
    e = statistics.median(e_vals) if rows else None
    d = statistics.median(d_vals) if rows else None
    return {"n": len(rows), "E": e, "D": d, "label": decide(len(rows), d, e)}


def registered_looks(eligible: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Look 1 = the first 20 eligible rows, look 2 = the first 40 (``eligible`` is already ordered by
    (fill_d, rec_id)); a look exists only once its row count is reached. Later rows never move it."""
    return {
        name: decision_block(eligible[:size]) if len(eligible) >= size else None
        for name, size in LOOKS
    }


def build_signal_level(
    rows: Sequence[dict[str, Any]], ew: dict[date, float | None], calendar: NSECalendar,
    today: date | None = None,
) -> dict[str, Any]:
    eligible, counts = split_population(rows, calendar, today)
    looks = registered_looks(eligible)
    binding = looks["look2"] or looks["look1"]
    running = {k: v for k, v in decision_block(eligible).items() if k != "label"}
    e_vals = [r["excess_t20"] for r in eligible]
    d_vals = paired(eligible)
    out: dict[str, Any] = {
        "counts": counts,
        "looks": looks,
        "running": running,
        "decision": binding["label"] if binding else LABEL_INSUFFICIENT,
        "review_due_at_40": len(eligible) >= REVIEW_N,
        "diagnostics": {
            "net_t20": cell([r["net_t20"] for r in eligible]),
            "excess_t20": cell(e_vals),
            "paired_t20": cell(d_vals),
            "stop_exit_share": (
                sum(1 for r in eligible if r["exit_reason"] == "stop") / len(eligible)
                if eligible else None
            ),
            "per_strategy": {
                s: {k: v for k, v in decision_block(
                    [r for r in eligible if r["strategy_id"] == s]).items() if k != "label"}
                for s in STRATEGIES
            },
            "bootstrap_90": bootstrap_ci(e_vals, d_vals),
            "sign_test": sign_test_p(d_vals),
            "horizons": {str(k): horizon_diagnostics(rows, k, ew, calendar) for k in DIAG_HORIZONS},
        },
        "confirmation_slice": {
            "from_fill_d": CONFIRM_FROM,
            **{k: v for k, v in decision_block(
                [r for r in eligible if (r["fill_d"] or "") >= CONFIRM_FROM]).items()
               if k != "label"},
        },
    }
    return out


def horizon_diagnostics(
    rows: Sequence[dict[str, Any]], k: int, ew: dict[date, float | None], calendar: NSECalendar,
) -> dict[str, Any]:
    """T+k net, no-stop excess and the pairing against the shipped ``net_pct`` (closed rows)."""
    key = f"net_t{k}"
    have = [r for r in rows if r[key] is not None and r["status"] not in _FINAL]
    excess: list[float] = []
    skipped = 0
    for r in have:
        try:
            fill = date.fromisoformat(r["fill_d"])
            b = bench_pct(ew, fill, calendar.add_sessions(fill, k - 1), "time", calendar)
        except (TypeError, ValueError, ArithmeticError):
            b = None
        if b is None:
            skipped += 1
        else:
            excess.append(r[key] - float(b))
    pair = [r[key] - r["net_pct"] for r in have if r["status"] == "closed" and r["net_pct"] is not None]
    return {
        "net": cell([r[key] for r in have]),
        "excess": cell(excess),
        "excess_skipped": skipped,
        "paired": cell(pair),
    }


# =============================================================================== portfolio
def _ist(ts: str | None) -> datetime | None:
    if not ts:
        return None
    dt = datetime.fromisoformat(ts)
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt.astimezone(IST)


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(r[1] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def counterfactual_walk(
    bars: Sequence[SimBar], avg_entry: Decimal, opened_at: datetime, *, stop: Decimal,
    target: Decimal | None, horizon: date | None, end: date, cost_pct: Decimal,
) -> Outcome:
    """The shipped stop/target rule walked over a time-only position's own bars. Pure."""
    return simulate(
        bars, Held(avg_entry, opened_at), stop=stop, target=target, horizon=horizon, end=end,
        cost_pct=cost_pct,
    )


def load_walk_bars(
    market: Any, symbol: str, opened_at: datetime, as_of: date,
) -> tuple[list[SimBar], str]:
    """``(bars, kind)``: 1m bars after ``opened_at`` on the fill day, then daily bars of later days
    (the fill day's daily bar only when it has no 1m bars), as rec_outcomes ``_load`` composes them.
    ``kind`` is ``1m+daily`` or ``daily_only`` (research snapshots carry no ``bars_1m``)."""
    fill_d = opened_at.date()
    day_end = datetime.combine(fill_d + timedelta(days=1), datetime.min.time(), tzinfo=IST)
    has_1m = market.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'bars_1m'"
    ).fetchone() is not None
    one_m = market.execute(
        'SELECT ts_minute, "open", high, low, "close" FROM bars_1m '
        "WHERE symbol = ? AND ts_minute >= ? AND ts_minute < ? ORDER BY ts_minute",
        [symbol, opened_at, day_end],
    ).fetchall() if has_1m else []
    daily = market.execute(
        'SELECT d, "open", high, low, "close" FROM bars_1d WHERE symbol = ? AND d >= ? AND d <= ? '
        "ORDER BY d",
        [symbol, fill_d + timedelta(days=1) if one_m else fill_d, as_of],
    ).fetchall()
    bars = [SimBar(r[0].astimezone(IST), *map(Decimal, r[1:])) for r in one_m]
    bars += [SimBar(r[0], *map(Decimal, r[1:])) for r in daily]
    return bars, ("1m+daily" if one_m else "daily_only")


def _net_pct(pos: dict[str, Any]) -> float | None:
    if pos["realized_pnl"] is None or pos["costs"] is None:
        return None
    basis = Decimal(pos["qty"]) * Decimal(pos["avg_entry"])
    return float((Decimal(pos["realized_pnl"]) - Decimal(pos["costs"])) / basis * 100)


def build_portfolio(
    conn: sqlite3.Connection, ew: dict[date, float | None], calendar: NSECalendar, as_of: date,
    market: Any | None = None,
) -> dict[str, Any]:
    if not _has_column(conn, "positions", "exit_policy"):
        positions: list[dict[str, Any]] = []
        note = "positions.exit_policy column absent (engine not yet on the C5 schema): no time-only positions"
    else:
        cur = conn.execute(
            "SELECT position_id, symbol, qty, avg_entry, state, close_reason, opened_at, closed_at, "
            "realized_pnl, costs, exit_session, close_basis FROM positions "
            "WHERE is_paper = 1 AND exit_policy = ? "
            "ORDER BY opened_at", (EXIT_POLICY,),
        )
        cols = [c[0] for c in cur.description]
        positions = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        note = None
    closed = [p for p in positions if p["state"] == "CLOSED"]
    open_ = [p for p in positions if p["state"] in ("OPEN", "PENDING_EXIT")]
    reasons: dict[str, int] = {}
    for p in closed:
        reasons[str(p["close_reason"])] = reasons.get(str(p["close_reason"]), 0) + 1

    # A void or a paper reset is not a trade outcome: counted, kept out of the net/excess cells.
    voids = [p for p in closed if p["close_reason"] == "void"]
    resets = [p for p in closed if p["close_basis"] == "paper_reset" and p["close_reason"] != "void"]
    scored = [p for p in closed if p not in voids and p not in resets]
    trades = []
    for p in scored:
        net = _net_pct(p)
        bench = None
        try:
            bench = bench_pct(
                ew, _ist(p["opened_at"]).date(), _ist(p["closed_at"]).date(),
                "time" if p["close_reason"] == "time_stop" else "stop", calendar,
            )
        except (AttributeError, ValueError, ArithmeticError):
            pass
        trades.append({
            "position_id": p["position_id"], "symbol": p["symbol"], "close_reason": p["close_reason"],
            "net_pct": net, "bench_pct": None if bench is None else float(bench),
            "excess_pct": None if net is None or bench is None else net - float(bench),
        })
    nets = [t["net_pct"] for t in trades if t["net_pct"] is not None]
    excs = [t["excess_pct"] for t in trades if t["excess_pct"] is not None]

    since = _ist(POLICY_FROM)
    refused = 0
    for payload, evaluated_at in conn.execute(
        "SELECT payload, evaluated_at FROM verdicts WHERE is_paper = 1"
    ):
        when = _ist(evaluated_at)
        try:
            reasons_list = json.loads(payload or "{}").get("reasons") or []
        except (TypeError, ValueError, AttributeError):
            continue
        if when is not None and when >= since and any(
            isinstance(x, str) and x.startswith("max_open_positions") for x in reasons_list
        ):
            refused += 1
    # ``cause`` is the table's primary key and a re-latch overwrites set_at: distinct causes, not events.
    placeholders = ", ".join("?" for _ in FLOOR_CAUSES)
    floor_halts = sum(
        1 for (set_at,) in conn.execute(
            f"SELECT set_at FROM paper_halts WHERE cause IN ({placeholders})", FLOOR_CAUSES)
        if _ist(set_at) is not None and _ist(set_at) >= since
    )
    flatten = [p for p in closed if p["close_reason"] == "risk_flatten"]
    out: dict[str, Any] = {
        "note": note,
        "counts": {"time_only": len(positions), "open": len(open_), "closed": len(closed),
                   "excluded_void": len(voids), "excluded_paper_reset": len(resets),
                   "closed_without_costs": len(scored) - len(nets)},
        "close_reasons": reasons,
        "net_pct": {k: v for k, v in cell(nets).items() if k != "hit_rate"},
        "excess_pct": {k: v for k, v in cell(excs).items() if k != "hit_rate"},
        "trades": trades,
        "refused_full_book": refused,
        "floor_events": {
            "floor_halts_latched_distinct_causes": floor_halts,
            "risk_flatten_equity_floor": sum(1 for p in flatten if p["close_basis"] == "equity_floor"),
            "risk_flatten_paper_reset": sum(1 for p in flatten if p["close_basis"] == "paper_reset"),
        },
        "since": since.isoformat(),
        "counterfactual": None,
    }
    if market is not None:
        out["counterfactual"] = [
            _counterfactual_for(conn, market, p, as_of) for p in positions
        ]
    return out


def _counterfactual_for(
    conn: sqlite3.Connection, market: Any, p: dict[str, Any], as_of: date,
) -> dict[str, Any]:
    res: dict[str, Any] = {"position_id": p["position_id"], "symbol": p["symbol"],
                           "time_only_state": p["state"]}
    row = conn.execute(
        "SELECT pr.payload FROM orders o JOIN proposals pr ON pr.proposal_id = o.proposal_id "
        "WHERE o.role = 'entry' AND o.position_id = ? ORDER BY o.created_at LIMIT 1",
        (p["position_id"],),
    ).fetchone()
    try:
        payload = json.loads(row[0]) if row else {}
        stop = Decimal(str(payload["stop_price"]))
        tgt = payload.get("target_price")
        target = None if tgt is None else Decimal(str(tgt))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return {**res, "error": "no shipped stop_price/target_price on the entry proposal"}
    opened = _ist(p["opened_at"])
    net = _net_pct(p)
    cost_pct = Decimal(0)
    if p["costs"] is not None:
        cost_pct = Decimal(p["costs"]) / (Decimal(p["qty"]) * Decimal(p["avg_entry"])) * 100
    try:
        horizon = date.fromisoformat(p["exit_session"]) if p["exit_session"] else None
        bars, bars_kind = load_walk_bars(market, p["symbol"], opened, as_of)
        o = counterfactual_walk(
            bars, Decimal(p["avg_entry"]), opened, stop=stop, target=target, horizon=horizon,
            end=as_of, cost_pct=cost_pct,
        )
    except Exception as exc:  # noqa: BLE001 - a report row, never a crash
        return {**res, "error": f"{type(exc).__name__}: {exc}"}
    shipped_net = None if o.net_pct is None else float(o.net_pct)
    res.update(
        stop_price=str(stop), target_price=None if target is None else str(target),
        shipped_status=o.status, shipped_reason=o.reason, shipped_net_pct=shipped_net,
        shipped_pct_basis="net" if p["state"] == "CLOSED" else "gross_open",
        bars=bars_kind, time_only_net_pct=net,
        paired_diff=(net - shipped_net) if p["state"] == "CLOSED" and net is not None
        and shipped_net is not None else None,
    )
    return res


# =============================================================================== report
def read_ew(conn: sqlite3.Connection) -> dict[date, float | None]:
    return {date.fromisoformat(d): r for d, r in conn.execute("SELECT d, ret FROM universe_ew_returns")}


def open_state_db(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0)


def scored_through(conn: sqlite3.Connection) -> date | None:
    """The last day the ``rec_outcomes`` job scored (its success watermark); None if it never ran."""
    row = conn.execute(
        "SELECT MAX(run_for_date) FROM job_runs WHERE job_id = 'rec_outcomes' AND status = 'success'"
    ).fetchone()
    return date.fromisoformat(row[0]) if row and row[0] else None


def build_report(
    conn: sqlite3.Connection, calendar: NSECalendar, as_of: date, market: Any | None = None,
) -> dict[str, Any]:
    ew = read_ew(conn)
    rows = load_population(conn)
    stamps = [r["updated_at"] for r in rows if r["updated_at"]]
    through = scored_through(conn)
    return {
        "meta": {
            "script": "scripts/no_stop_verdict.py", "as_of": as_of.isoformat(),
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "strategies": list(STRATEGIES), "min_n": MIN_N, "review_n": REVIEW_N,
            "latest_rec_outcomes_update": max(stamps) if stamps else None,
            "rec_outcomes_scored_through": None if through is None else through.isoformat(),
            "caveat": LOW_POWER_CAVEAT,
        },
        "signal_level": build_signal_level(rows, ew, calendar, through),
        "portfolio": build_portfolio(conn, ew, calendar, as_of, market),
    }


def _f(v: Any, w: int = 8, nd: int = 4) -> str:
    return f"{'n/a':>{w}}" if v is None else f"{float(v):+{w}.{nd}f}"


def _p(v: Any) -> str:
    return "  n/a" if v is None else f"{float(v):5.1%}"


def _cell_line(label: str, c: dict[str, Any]) -> str:
    return f"  {label:<26} n={c['n']:<4} median {_f(c['median'])}  mean {_f(c['mean'])}  hit {_p(c['hit_rate'])}"


def render(doc: dict[str, Any]) -> str:
    s, pf = doc["signal_level"], doc["portfolio"]
    c = s["counts"]
    dg = s["diagnostics"]
    bar = "=" * 92
    look1, look2, run = s["looks"]["look1"], s["looks"]["look2"], s["running"]
    if look2:
        decision = (f"DECISION        : {look2['label']}   "
                    "(look 2: first 40 eligible by fill date; supersedes look 1)")
    elif look1:
        decision = (f"DECISION        : {look1['label']}   (look 1: first 20 eligible by fill date; "
                    f"E={_f(look1['E'])} pp, D={_f(look1['D'])} pp)")
    else:
        decision = (f"DECISION        : {LABEL_INSUFFICIENT}   "
                    f"({run['n']} of 20 eligible for look 1)")
    L = [bar, "C5 NO-STOP FORWARD-TEST VERDICT (owner D14: no stop + 20-session time exit)", bar,
         decision]
    if look1 and look2:
        L.append(f"look 1          : {look1['label']}   (first 20: E={_f(look1['E'])} pp, "
                 f"D={_f(look1['D'])} pp)")
    L += [f"running (all n eligible, interim, decides nothing): n={run['n']} "
          f"E={_f(run['E'])} pp D={_f(run['D'])} pp",
          f"basis           : DECISION reads rec_outcomes AS STORED (latest updated_at "
          f"{doc['meta']['latest_rec_outcomes_update']}); --as-of {doc['meta']['as_of']} bounds only the "
          "(P) counterfactual walk",
          f"policy start    : {POLICY_FROM} (window for (P) refusals and floor halts)",
          f"caveat          : {LOW_POWER_CAVEAT}",
          "-" * 92, "(S) SIGNAL LEVEL (decision source)",
          f"  population {c['population']}: eligible {c['eligible']}; pending {c['pending']} = awaiting "
          f"T+20 {c['awaiting_t20']} / T+20 past due, not scored {c['t20_past_due']} / awaiting shipped "
          f"exit {c['awaiting_shipped_exit']}",
          f"  excluded void_ca {c['void_ca']} / unfilled {c['unfilled']} / unscorable {c['unscorable']}",
          "  E = median excess_t20 and D = median paired (net_t20 - net_pct), in pp (percentage points)",
          "  diagnostics (decide nothing):",
         _cell_line("net_t20", dg["net_t20"]), _cell_line("excess_t20", dg["excess_t20"]),
         _cell_line("paired t20 - shipped", dg["paired_t20"]),
         f"  shipped-rule stop-exit share of eligible: {_p(dg['stop_exit_share'])}"]
    bs = dg["bootstrap_90"]
    if bs["E"]:
        L.append(f"  bootstrap 90% ({bs['resamples']} resamples, seed {bs['seed']}): "
                 f"E [{bs['E'][0]:+.4f}, {bs['E'][1]:+.4f}]  D [{bs['D'][0]:+.4f}, {bs['D'][1]:+.4f}]")
    st = dg["sign_test"]
    p_txt = "n/a" if st["p"] is None else format(st["p"], ".4f")
    L.append(f"  sign test on D: {st['n_positive']}/{st['n_nonzero']} nonzero diffs positive, "
             f"two-sided p = {p_txt}")
    for name, v in dg["per_strategy"].items():
        L.append(f"  {name:<6} n={v['n']:<4} E {_f(v['E'])}  D {_f(v['D'])}")
    for k, h in dg["horizons"].items():
        L += [f"  T+{k} (diagnostic only; own row set, closed rows for the pairing):",
              _cell_line(f"net_t{k}", h["net"]),
              _cell_line(f"no-stop excess (skip {h['excess_skipped']})", h["excess"]),
              _cell_line(f"paired t{k} - shipped", h["paired"])]
    cf = s["confirmation_slice"]
    L += ["-" * 92,
          f"CONFIRMATION SLICE (fill_d >= {cf['from_fill_d']}): n={cf['n']} "
          f"E {_f(cf['E'])} pp D {_f(cf['D'])} pp",
          "-" * 92, "(P) PORTFOLIO LEVEL (diagnostic only)"]
    if pf["note"]:
        L.append(f"  note: {pf['note']}")
    pc = pf["counts"]
    L += [f"  time-only paper positions: {pc['time_only']} (open {pc['open']}, closed {pc['closed']}, "
          f"closed without costs {pc['closed_without_costs']}; excluded from the cells: void "
          f"{pc['excluded_void']}, paper_reset {pc['excluded_paper_reset']})",
          f"  close reasons (all closed): {pf['close_reasons'] or '-'}",
          f"  closed net %: n={pf['net_pct']['n']} median {_f(pf['net_pct']['median'])} "
          f"mean {_f(pf['net_pct']['mean'])}",
          f"  closed excess vs EW bench %: n={pf['excess_pct']['n']} median "
          f"{_f(pf['excess_pct']['median'])} mean {_f(pf['excess_pct']['mean'])}",
          f"  entries refused for a full book (paper, since {pf['since']}): {pf['refused_full_book']}",
          f"  floor halts latched (distinct causes) since {pf['since']}: "
          f"{pf['floor_events']['floor_halts_latched_distinct_causes']}; risk_flatten closes: "
          f"equity_floor {pf['floor_events']['risk_flatten_equity_floor']}, "
          f"paper_reset {pf['floor_events']['risk_flatten_paper_reset']}"]
    if pf["counterfactual"] is None:
        L.append("  counterfactual: not run (pass --market-db PATH)")
    else:
        L.append("  counterfactual (shipped stop/target walked over the same bars):")
        for r in pf["counterfactual"]:
            if "error" in r:
                L.append(f"    {r['symbol']:<12} {r['time_only_state']:<8} ERROR {r['error']}")
            else:
                basis = "net" if r["shipped_pct_basis"] == "net" else "(gross, open)"
                L.append(
                    f"    {r['symbol']:<12} {r['time_only_state']:<8} shipped {r['shipped_status']}/"
                    f"{r['shipped_reason']} {basis} {_f(r['shipped_net_pct'])}%  time-only net "
                    f"{_f(r['time_only_net_pct'])}%  diff {_f(r['paired_diff'])}  bars: {r['bars']}"
                )
    L += ["-" * 92, f"NOTE: {LOW_POWER_CAVEAT}", bar]
    return "\n".join(L)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="C5 no-stop forward-test verdict (read-only).")
    ap.add_argument("--state-db", type=Path, default=repo_root() / "data" / "state.db")
    ap.add_argument("--market-db", type=Path, default=None,
                    help="DuckDB snapshot for the counterfactual (never defaults to the live file)")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="bounds ONLY the (P) counterfactual walk; the DECISION reads rec_outcomes as "
                         "stored. Default: today IST (also names the default --out file)")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    as_of = args.as_of or datetime.now(IST).date()
    out = args.out or repo_root() / "data" / "reports" / f"no_stop_verdict_{as_of.isoformat()}.json"
    calendar = NSECalendar(config_dir() / "calendar", Clock(), include_special=False)

    conn = open_state_db(args.state_db)
    market = None
    try:
        if args.market_db is not None:
            import duckdb
            market = duckdb.connect(str(args.market_db), read_only=True)
        doc = build_report(conn, calendar, as_of, market)
    finally:
        conn.close()
        if market is not None:
            market.close()
    print(render(doc))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    print(f"json            : {out}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
