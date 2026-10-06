"""Per-strategy hindsight scorecard over ``rec_outcomes`` joined to ``recommendations`` (plan Q2.4).

HINDSIGHT: read by the owner surfaces only, never by scanner, threshold or promotion code.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from statistics import mean, median
from typing import Any

from engine.learning.benchmark import BENCH_INTRASESSION, BENCH_TIME

LABEL = "hindsight on official bars — not platform equity"
UNATTRIBUTED = "unattributed"

# Entry recs only (rec_outcomes never scores exit recs); void_ca is excluded from every stat.
_SQL = (
    "SELECT o.strategy_id, o.status, o.fill_basis, o.net_pct, o.net_t20, o.excess_pct, "
    "r.human_action, r.skip_reason "
    "FROM recommendations r LEFT JOIN rec_outcomes o ON o.rec_id = r.rec_id "
    "WHERE r.delivered_at IS NOT NULL "
    "AND COALESCE(json_extract(r.payload, '$.kind'), 'entry') = 'entry' "
    "AND COALESCE(o.status, '') != 'void_ca'"
)


def _mean(xs: list[float]) -> float | None:
    return mean(xs) if xs else None


def scorecard(conn: sqlite3.Connection) -> dict[str, Any]:
    by_strategy: dict[str, list[tuple]] = defaultdict(list)
    for row in conn.execute(_SQL):
        by_strategy[row[0] or UNATTRIBUTED].append(tuple(row))
    return {
        "label": LABEL,
        "bench": {"time": BENCH_TIME, "intrasession": BENCH_INTRASESSION},
        "strategies": {sid: _strategy(rows) for sid, rows in sorted(by_strategy.items())},
    }


def _strategy(rows: list[tuple]) -> dict[str, Any]:
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
        "paper": {"closed": 0, "hit_rate": None, "net": None, "open": 0},
    }
