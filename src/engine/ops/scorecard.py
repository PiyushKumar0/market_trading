"""Per-strategy hindsight scorecard over ``rec_outcomes`` joined to ``recommendations`` (plan Q2.4), and
the published signals scored in ``signal_outcomes`` with their pre-registered verdicts (review A1).

HINDSIGHT: read by the owner surfaces only, never by scanner, threshold or promotion code.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from statistics import mean, median
from typing import Any

from engine.core.clock import IST
from engine.core.log import get_logger
from engine.core.money import pct_of
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.learning import forward_verdicts as fv
from engine.learning.benchmark import BENCH_INTRASESSION, BENCH_TIME

_log = get_logger("engine.ops.scorecard")

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
    "SELECT strategy_id, net_pnl, qty, entry_px FROM learning_ledger "
    f"WHERE {scope_sql('paper')} AND closed_at IS NOT NULL AND COALESCE(outcome_label, '') != 'void'"
)
_PAPER_OPEN_SQL = (
    f"SELECT strategy_id FROM positions WHERE {HELD_STATES_SQL['paper']} "
    f"AND {scope_sql('paper', has_origin=True)}"
)


_HISTORY_SQL = (
    "SELECT r.delivered_at, COALESCE(o.strategy_id, json_extract(r.payload, '$.strategy_id'), "
    "l.strategy_id), o.status, o.net_pct, o.exit_reason "
    "FROM recommendations r LEFT JOIN learning_ledger l ON l.rec_id = r.rec_id "
    "LEFT JOIN rec_outcomes o ON o.rec_id = r.rec_id "
    "WHERE r.delivered_at IS NOT NULL AND json_extract(r.payload, '$.instrument') = ? "
    "AND COALESCE(json_extract(r.payload, '$.kind'), 'entry') = 'entry' "
    "ORDER BY r.delivered_at DESC LIMIT ?"
)
_STATUS_WORDS = {"open": "open", "unfilled": "did not fill", "void_ca": "voided (corporate action)"}


def symbol_history_line(conn: sqlite3.Connection, symbol: str, *, limit: int = 3) -> str:
    """The card's same-ticker line: up to ``limit`` delivered entry recs on ``symbol``, newest first.
    Called before the new rec is delivered, so every row is an earlier one."""
    rows = conn.execute(_HISTORY_SQL, (symbol, limit)).fetchall()
    if not rows:
        return f"same-ticker history: no earlier recommendation on {symbol}"
    return "same-ticker history (hindsight): " + "; ".join(
        f"{datetime.fromisoformat(at).astimezone(IST).date()} {sid or UNATTRIBUTED} "
        f"{_status_text(status, net, reason)}"
        for at, sid, status, net, reason in rows
    )


def _status_text(status: str | None, net: float | None, reason: str | None) -> str:
    if status != "closed":
        return _STATUS_WORDS.get(status, "not scored")
    text = "closed" if net is None else f"closed {net:+.2f}%"
    return f"{text} ({reason})" if reason else text


def _mean(xs: list[float]) -> float | None:
    return mean(xs) if xs else None


def scorecard(conn: sqlite3.Connection, *, as_of: date, with_paper: bool = True) -> dict[str, Any]:
    """``as_of`` is the caller's IST day; it bounds only the ``signals`` block."""
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
        "signals": signals_block(conn, as_of=as_of),
    }


def signals_block(conn: sqlite3.Connection, *, as_of: date) -> dict[str, Any]:
    """Every published brk20/hi52/ins signal scored in hindsight with the kill and analyst readings, over
    the population from ``forward_verdicts.REGISTERED_FROM`` (read per call), as known on ``as_of``.
    hi52's rule row is a diagnostic: its costing differs from ``scripts/hi52_forward_verdict.py``, whose
    verdict stays authoritative. Never raises: an error reads ``{"error": ...}``."""
    try:
        return _signals(conn, as_of, fv.REGISTERED_FROM)
    except Exception as exc:  # noqa: BLE001 - an owner surface degrades, never fails its caller
        _log.exception("signals_block_failed")
        return {"error": f"{type(exc).__name__}: {exc}"}


def _signals(conn: sqlite3.Connection, as_of: date, registered_from: date | None) -> dict[str, Any]:
    sids, start = fv.ANALYST_STRATEGIES, registered_from or date.min
    published = fv.load_rows(conn, strategies=sids, construct="next_open", start=start, as_of=as_of)
    rule = fv.load_rows(conn, strategies=sids, construct="rule", start=start, as_of=as_of)
    kept, _ = fv.split_by_key(published, fv.registered_keys(published))
    cells = Counter(c.strategy_id for c in fv.analyst_cells(kept))
    analyst = fv.analyst_reading(conn, as_of=as_of, registered_from=registered_from)
    kills = {r: fv.kill_reading(conn, r, as_of=as_of, registered_from=registered_from) for r in fv.KILL_RULES}

    def strategy(sid: str) -> dict[str, Any]:
        rows = [r for r in rule if r.strategy_id == sid]
        net = fv.stats([r.net_t20 for r in rows if r.net_t20 is not None])
        decided = Counter(r.analyst_decision for r in kept if r.strategy_id == sid)
        kill = kills.get(sid)
        c2 = kill.c2 if kill else None
        return {
            "published": sum(r.strategy_id == sid for r in published),
            "rule": {
                "diagnostic": sid not in fv.KILL_RULES, "closed": sum(r.status == "closed" for r in rows),
                "median_net_t20": net.median, "hit_rate": net.hit_rate,
                "median_excess_t20": fv.stats([r.excess_t20 for r in rows if r.excess_t20 is not None]).median,
            },
            "blocked": {**asdict(c2.blocked), "neither": c2.neither} if c2 else None,
            "kill_rule": _verdict(kill) if kill else None,
            "analyst": {"enter": decided["enter"], "decline": decided["no_action"], "cells": cells[sid],
                        "mean_diff": analyst.per_strategy[sid].mean},
        }

    return {
        "window": f"from {registered_from.isoformat()}" if registered_from else "pre-registration history",
        "analyst": _verdict(analyst),
        "strategies": {sid: strategy(sid) for sid in sids},
    }


def _verdict(reading: fv.KillReading | fv.AnalystReading) -> dict[str, Any]:
    return {"verdict": reading.verdict, "reason": reading.reason, "looks": [asdict(lk) for lk in reading.looks]}


def _paper_stats(nets: list[Decimal], open_n: int, basis: Decimal | None = None) -> dict[str, Any]:
    """``basis`` is Σ(qty × entry_px) of the closed trades, None when any row lacks one (never partial)."""
    net_pct = pct_of(sum(nets), basis) if nets else None
    return {
        "closed": len(nets),
        "hit_rate": sum(n > 0 for n in nets) / len(nets) if nets else None,
        "net": float(sum(nets)) if nets else None,
        "net_pct": None if net_pct is None else float(net_pct),
        "open": open_n,
    }


def _paper(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Per strategy: closed trades, hit rate, net rupees (a float), net % of cost and open positions."""
    nets: dict[str, list[Decimal]] = defaultdict(list)
    bases: dict[str, Decimal | None] = defaultdict(Decimal)
    for sid, net_pnl, qty, entry_px in conn.execute(_PAPER_CLOSED_SQL):
        key = sid or UNATTRIBUTED
        nets[key].append(Decimal(net_pnl))
        if qty is None or entry_px is None or bases[key] is None:
            bases[key] = None
        else:
            bases[key] += Decimal(str(qty)) * Decimal(str(entry_px))
    open_counts = Counter(sid or UNATTRIBUTED for (sid,) in conn.execute(_PAPER_OPEN_SQL))
    return {
        sid: _paper_stats(nets[sid], open_counts[sid], bases[sid] if sid in nets else None)
        for sid in nets.keys() | open_counts.keys()
    }


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
