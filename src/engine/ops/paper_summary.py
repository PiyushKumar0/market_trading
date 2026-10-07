"""Read-only paper-book summary behind ``GET /paper`` (dashboard Paper panel).

Every money value crosses as a string. Reads never construct a ``PaperControl`` (its ctor writes) and
never touch the write-capable managers: the built stack is seen only through :class:`PaperView`.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from engine.core.scope import HELD_STATES_SQL, PAPER_EPOCH_SQL, scope_sql
from engine.ops.paper_control import PaperControl
from engine.risk.gate import TERMINAL_ORDER_STATES_SQL

_CURVE_DAYS = 250
_CLOSED_ROWS = 20
_CENT = Decimal("0.01")

_POSITIONS_SQL = (
    "SELECT position_id, symbol, product, qty, avg_entry, stop, target, state, protection_state, "
    "strategy_id, exit_session, opened_at, side FROM positions "
    f"WHERE {scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']} "
    f"AND opened_at >= {PAPER_EPOCH_SQL} ORDER BY opened_at"
)
_ORDERS_SQL = (
    "SELECT o.order_id, COALESCE(p.symbol, json_extract(pr.payload, '$.tradingsymbol')) AS symbol, "
    "COALESCE(p.strategy_id, json_extract(pr.payload, '$.strategy_id')) AS strategy_id, "
    "o.role, o.side, o.qty, o.filled_qty, o.price, o.trigger_price, o.state, o.created_at "
    "FROM orders o LEFT JOIN positions p ON p.position_id = o.position_id "
    "LEFT JOIN proposals pr ON pr.proposal_id = o.proposal_id "
    f"WHERE {scope_sql('paper', 'o')} AND o.state NOT IN {TERMINAL_ORDER_STATES_SQL} "
    "ORDER BY o.created_at"
)
_CLOSED_SQL = (
    "SELECT l.position_id, p.symbol, l.strategy_id, l.qty, l.entry_px, l.exit_px, l.net_pnl, "
    "l.close_reason, p.close_basis, l.outcome_label, l.closed_at "
    "FROM learning_ledger l LEFT JOIN positions p ON p.position_id = l.position_id "
    f"WHERE {scope_sql('paper', 'l')} AND l.closed_at >= {PAPER_EPOCH_SQL} "
    f"ORDER BY l.closed_at DESC LIMIT {_CLOSED_ROWS}"
)
_TOTALS_SQL = (
    "SELECT outcome_label, net_pnl FROM learning_ledger "
    f"WHERE {scope_sql('paper')} AND closed_at >= {PAPER_EPOCH_SQL}"
)
_SNAPSHOT_BEFORE_SQL = (
    "SELECT at, equity, realized_pnl, open_mtm, day_mtm, positions_open FROM paper_equity_snapshots "
    f"WHERE at < ? AND at >= {PAPER_EPOCH_SQL} ORDER BY at DESC LIMIT 1"
)


@dataclass(frozen=True)
class PaperView:
    """The read-only slice of a built PaperStack."""

    prep_ready: Callable[[], bool]
    entry_guard: Callable[[], str | None]      # None = an entry would pass the paper order guard
    mark: Callable[[str], Decimal | None]
    counters: Callable[[], dict[str, int]]


def _dec(value: Any) -> Decimal:
    return Decimal(0) if value is None or value == "" else Decimal(str(value))


def _money(x: Decimal) -> str:
    return str(x.quantize(_CENT, ROUND_HALF_UP))


def _snapshots(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Last snapshot of each IST day since the epoch, newest first: one index lookup per day, each
    bounded by the previous row's day start (a GROUP BY over minute snapshots is too slow here)."""
    rows: list[sqlite3.Row] = []
    bound = "9999"
    for _ in range(_CURVE_DAYS):
        row = conn.execute(_SNAPSHOT_BEFORE_SQL, (bound,)).fetchone()
        if row is None:
            break
        rows.append(row)
        bound = row["at"][:10]
    return rows


def _equity(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    pnl = _dec(row["realized_pnl"]) + _dec(row["open_mtm"])
    return {
        "at": row["at"], "equity": row["equity"], "realized_pnl": row["realized_pnl"],
        "open_mtm": row["open_mtm"], "day_mtm": row["day_mtm"], "positions_open": row["positions_open"],
        "pnl": _money(pnl), "capital_base": _money(_dec(row["equity"]) - pnl),
    }


def _positions(conn: sqlite3.Connection, view: PaperView | None) -> list[dict[str, Any]]:
    out = []
    for row in conn.execute(_POSITIONS_SQL):
        mark = view.mark(row["symbol"]) if view is not None else None
        sign = -1 if (row["side"] or "").upper() in ("SELL", "SHORT") else 1
        unrealized = _money(_dec(row["qty"]) * (mark - _dec(row["avg_entry"])) * sign) if mark is not None else None
        out.append({**{k: row[k] for k in row.keys() if k != "side"},
                    "mark": None if mark is None else str(mark), "unrealized": unrealized})
    return out


def _totals(conn: sqlite3.Connection) -> dict[str, Any]:
    closed = wins = voids = 0
    void_net = Decimal(0)
    for label, net_pnl in conn.execute(_TOTALS_SQL):
        net = _dec(net_pnl)
        if label == "void":
            voids += 1
            void_net += net
        else:
            closed += 1
            wins += net > 0
    return {"closed": closed, "wins": wins, "voids": voids, "void_net": _money(void_net) if voids else None}


def paper_summary(
    conn: sqlite3.Connection, control: PaperControl, *, view: PaperView | None, subsystem_enabled: bool
) -> dict[str, Any]:
    snapshots = _snapshots(conn)
    positions = _positions(conn, view)
    unmarked = list(dict.fromkeys(p["symbol"] for p in positions if p["mark"] is None)) if view else []
    return {
        **control.state(),
        "subsystem_enabled": subsystem_enabled,
        "built": view is not None,
        "prep_ready": view.prep_ready() if view is not None else None,
        "entry_guard": view.entry_guard() if view is not None else "not built",
        "unmarked": unmarked,
        "halts": [{**h, "latched": bool(h["latched"])} for h in control.active_halts()],
        "equity": _equity(snapshots[0] if snapshots else None),
        "curve": [{"d": r["at"][:10], "at": r["at"], "equity": r["equity"]} for r in reversed(snapshots)],
        "positions": positions,
        "orders": [dict(r) for r in conn.execute(_ORDERS_SQL)],
        "closed": [dict(r) for r in conn.execute(_CLOSED_SQL)],
        "totals": _totals(conn),
        "counters": view.counters() if view is not None else None,
    }
