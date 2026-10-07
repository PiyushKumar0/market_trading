"""Per-strategy hindsight scorecard over ``rec_outcomes`` joined to ``recommendations`` (plan Q2.4).

HINDSIGHT: read by the owner surfaces only, never by scanner, threshold or promotion code.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from decimal import Decimal
from statistics import mean, median
from typing import Any

from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.learning.benchmark import BENCH_INTRASESSION, BENCH_TIME

LABEL = "hindsight on official bars — not platform equity"
UNATTRIBUTED = "unattributed"

# Entry recs only (rec_outcomes never scores exit recs); void_ca is excluded from every stat.
_SQL = (
    "SELECT COALESCE(o.strategy_id, json_extract(r.payload, '$.strategy_id'), l.strategy_id), "
    "o.status, o.fill_basis, o.net_pct, o.net_t20, o.excess_pct, r.human_action, r.skip_reason "
    "FROM recommendations r LEFT JOIN rec_outcomes o ON o.rec_id = r.rec_id "
    "LEFT JOIN learning_ledger l ON l.rec_id = r.rec_id "
    "WHERE r.delivered_at IS NOT NULL "
    "AND COALESCE(json_extract(r.payload, '$.kind'), 'entry') = 'entry' "
    "AND COALESCE(o.status, '') != 'void_ca'"
)


# Paper trades of every epoch (a /paper reset keeps the record); a void is excluded.
_PAPER_CLOSED_SQL = (
    "SELECT strategy_id, net_pnl FROM learning_ledger "
    f"WHERE {scope_sql('paper')} AND closed_at IS NOT NULL AND COALESCE(outcome_label, '') != 'void'"
)
_PAPER_OPEN_SQL = (
    f"SELECT strategy_id FROM positions WHERE {HELD_STATES_SQL['paper']} "
    f"AND {scope_sql('paper', has_origin=True)}"
)


def _mean(xs: list[float]) -> float | None:
    return mean(xs) if xs else None


def scorecard(conn: sqlite3.Connection, *, with_paper: bool = True) -> dict[str, Any]:
    by_strategy: dict[str, list[tuple]] = defaultdict(list)
    for row in conn.execute(_SQL):
        by_strategy[row[0] or UNATTRIBUTED].append(tuple(row))
    paper = _paper(conn) if with_paper else {}
    return {
        "label": LABEL,
        "bench": {"time": BENCH_TIME, "intrasession": BENCH_INTRASESSION},
        "strategies": {
            sid: _strategy(by_strategy.get(sid, []), paper.get(sid, _paper_stats([], 0)))
            for sid in sorted(by_strategy.keys() | paper.keys())
        },
    }


def _paper_stats(nets: list[Decimal], open_n: int) -> dict[str, Any]:
    return {
        "closed": len(nets),
        "hit_rate": sum(n > 0 for n in nets) / len(nets) if nets else None,
        "net": float(sum(nets)) if nets else None,
        "open": open_n,
    }


def _paper(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Per strategy: closed trades, hit rate, net rupees (a float) and open positions."""
    nets: dict[str, list[Decimal]] = defaultdict(list)
    for sid, net_pnl in conn.execute(_PAPER_CLOSED_SQL):
        nets[sid or UNATTRIBUTED].append(Decimal(net_pnl))
    open_counts = Counter(sid or UNATTRIBUTED for (sid,) in conn.execute(_PAPER_OPEN_SQL))
    return {sid: _paper_stats(nets[sid], open_counts[sid]) for sid in nets.keys() | open_counts.keys()}


def _strategy(rows: list[tuple], paper: dict[str, Any]) -> dict[str, Any]:
    closed = [r for r in rows if r[1] == "closed"]
    nets = [r[3] for r in closed if r[3] is not None]
    actions = Counter("taken" if r[6] == "closed" else (r[6] or "open") for r in rows)
    return {
        "recs": {
            "n": len(rows),
            "filled": sum(r[1] in ("open", "closed") for r in rows),
            "closed": len(closed),
            "hit_rate": sum(x > 0 for x in nets) / len(nets) if nets else None,
            "median_net": median(nets) if nets else None,
            "mean_net": _mean(nets),
            "net_t20": _mean([r[4] for r in rows if r[4] is not None]),
            "mean_excess": _mean([r[5] for r in closed if r[5] is not None]),
            "actions": dict(actions),
            "skip_reasons": dict(Counter(r[7] for r in rows if r[7])),
            "daily_basis": sum(r[2] == "daily" for r in rows),
            "unscorable": sum(r[1] == "unscorable" for r in rows),
        },
        "paper": paper,
    }
