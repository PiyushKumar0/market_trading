"""Order persistence over the state.db SQLite connection (WO-P3-1, 2026-09-10; §4.2, R8).

Follows the repo's state-store idiom (``engine.risk.mode`` / ``engine.risk.kill``): the caller owns
the connection (opened by :func:`engine.core.db.connect`, WAL, single writer), the SQL is
hand-written next to its owner, and a state change that must precede a side effect is wrapped in
:func:`engine.core.db.transaction`.

**The defining property: persist BEFORE any side effect.** :meth:`OrderStore.record` writes the new
``orders`` row and its ``order_events`` row in ONE transaction and returns only after the COMMIT, so
a caller that acts (places/modifies/cancels a broker order) strictly after ``record`` returns can
never act on a state that isn't durable. A crash in the gap leaves a consistent DB and the reconciler
a complete audit chain (R5/R8) — the same before-act ordering the kill switch uses (R10).

Two write guards, both raising rather than absorbing:

* a persisted **terminal** order rejects every further record — a FILLED/CANCELLED/REJECTED/LAPSED
  row is never re-opened (§3.5.1);
* a record whose ``order_before`` does not match the persisted state is refused
  (:class:`StaleOrderWrite`) — a compare-and-swap, so a caller holding a stale snapshot cannot roll
  the row backwards over a concurrent write.

A no-op postback (a replay, an intermediate, a held cancel intent) is audited through
:meth:`OrderStore.record_noop`, which writes the ``order_events`` row ALONE and is therefore allowed
on a terminal row — §3.5.1 promises every broker frame is recorded, and "terminal is final" is a
statement about the ``orders`` row, not about the audit trail (2026-09-10 review).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any

from engine.core.db import transaction
from engine.core.log import get_logger
from engine.oms.state import (
    TERMINAL_ORDER_STATES,
    OrderEvent,
    OrderRole,
    OrderState,
    PlatformOrder,
)
from engine.oms.updates import NOOP_FLAGS

_log = get_logger("engine.oms.store")

_ORDER_COLUMNS = (
    "order_id",
    "broker_order_id",
    "position_id",
    "proposal_id",
    "verdict_id",
    "role",
    "is_paper",
    "state",
    "product",
    "side",
    "qty",
    "filled_qty",
    "price",
    "trigger_price",
    "modifications",
    "reject_reason",
    "raw_broker_payload",
    "created_at",
    "updated_at",
)

_SELECT = f"SELECT {', '.join(_ORDER_COLUMNS)} FROM orders"


class TerminalOrderWrite(RuntimeError):
    """A record against an order already persisted in a terminal state (§3.5.1: never re-opened)."""


class StaleOrderWrite(RuntimeError):
    """``order_before`` disagrees with the persisted row — a stale snapshot, refused (never backwards)."""


def _text(value: Decimal | None) -> str | None:
    """Money as TEXT (§4.2): SQLite has no decimal type and floats corrupt ticks."""
    return None if value is None else str(value)


def _ts(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _json(value: dict[str, Any] | None) -> str | None:
    # default=str so a Decimal/datetime that reached a payload serialises instead of exploding at the
    # point of persistence (losing the audit row would be worse than losing the exact type, R8).
    return None if value is None else json.dumps(value, default=str, sort_keys=True)


def _parse_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        raise ValueError(f"naive timestamp {value!r} in state.db (§3.2 requires tz-aware IST)")
    return ts


def _parse_decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _row_to_order(row: sqlite3.Row) -> PlatformOrder:
    return PlatformOrder(
        order_id=row["order_id"],
        broker_order_id=row["broker_order_id"],
        position_id=row["position_id"],
        proposal_id=row["proposal_id"],
        verdict_id=row["verdict_id"],
        role=OrderRole(row["role"]),
        is_paper=bool(row["is_paper"]),
        state=OrderState(row["state"]),
        product=row["product"],
        side=row["side"],
        qty=row["qty"],
        filled_qty=row["filled_qty"] or 0,
        price=_parse_decimal(row["price"]),
        trigger_price=_parse_decimal(row["trigger_price"]),
        modifications=row["modifications"] or 0,
        reject_reason=row["reject_reason"],
        raw_broker_payload=json.loads(row["raw_broker_payload"]) if row["raw_broker_payload"] else None,
        created_at=_parse_ts(row["created_at"]),
        updated_at=_parse_ts(row["updated_at"]),
    )


class OrderStore:
    """``orders`` + ``order_events`` persistence. The caller owns the connection (§4.1 single writer)."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ----------------------------------------------------------------- writes
    def insert(self, order: PlatformOrder) -> None:
        """Insert a new order row. NOT an upsert — a duplicate ``order_id`` raises ``IntegrityError``
        (a re-inserted order is a duplicate-order hazard, the §4 review priority)."""
        placeholders = ", ".join("?" for _ in _ORDER_COLUMNS)
        self._conn.execute(
            f"INSERT INTO orders ({', '.join(_ORDER_COLUMNS)}) VALUES ({placeholders})",
            self._values(order),
        )

    def record(
        self, order_before: PlatformOrder, order_after: PlatformOrder, event: OrderEvent
    ) -> None:
        """Persist a transition: the updated order row AND its event, in ONE transaction (R8).

        Returns only after the COMMIT — the caller acts (broker call, alert, protection placement)
        strictly afterwards, so no side effect can precede its durable record.

        Raises
        ------
        LookupError
            The order was never :meth:`insert`ed.
        TerminalOrderWrite
            The persisted row is already terminal. A no-op duplicate event for a terminal order (a
            replayed postback, §3.5.1) goes to :meth:`record_noop` instead — it is still audited.
        StaleOrderWrite
            The persisted state is not ``order_before.state`` — a stale snapshot; refused.
        """
        if order_after.order_id != order_before.order_id:
            raise ValueError("record() got two different orders")
        with transaction(self._conn):
            row = self._conn.execute(
                "SELECT state FROM orders WHERE order_id=?", (order_before.order_id,)
            ).fetchone()
            if row is None:
                raise LookupError(f"order {order_before.order_id} not persisted; insert() first")
            persisted = OrderState(row["state"])
            if persisted in TERMINAL_ORDER_STATES:
                raise TerminalOrderWrite(
                    f"order {order_before.order_id} is persisted {persisted}; refusing further records "
                    f"(§3.5.1 terminal is final)"
                )
            if persisted is not order_before.state:
                raise StaleOrderWrite(
                    f"order {order_before.order_id}: persisted {persisted}, caller had "
                    f"{order_before.state} — stale write refused"
                )
            assignments = ", ".join(f"{c}=?" for c in _ORDER_COLUMNS if c != "order_id")
            values = [v for c, v in zip(_ORDER_COLUMNS, self._values(order_after), strict=True) if c != "order_id"]
            self._conn.execute(
                f"UPDATE orders SET {assignments} WHERE order_id=?",
                (*values, order_after.order_id),
            )
            self._conn.execute(
                "INSERT INTO order_events (order_id, from_state, to_state, payload, at) VALUES (?, ?, ?, ?, ?)",
                (
                    order_after.order_id,
                    event.from_state.value,
                    event.to_state.value,
                    _json(event.payload),
                    event.at.isoformat(),
                ),
            )
        _log.info(
            "order_transition_persisted",
            order_id=order_after.order_id,
            broker_order_id=order_after.broker_order_id,
            from_state=event.from_state.value,
            to_state=event.to_state.value,
            filled_qty=order_after.filled_qty,
        )

    def record_noop(self, order_id: str, event: OrderEvent) -> None:
        """Audit a postback that changed nothing: writes ONLY the ``order_events`` row.

        §3.5.1 promises every broker frame is recorded, but :meth:`record` refuses terminal rows —
        so a replayed postback against a FILLED/CANCELLED order had nowhere to land and the audit
        chain silently lost it (2026-09-10 review). An unrecorded broker action is an unexplained
        one, which is exactly what the Reconciler (R5) later has to explain. This door is therefore
        allowed on terminal rows; it never touches the ``orders`` row, so "terminal is final" holds.

        Raises
        ------
        LookupError
            The order was never :meth:`insert`ed.
        ValueError
            The event is not an annotated no-op: it claims a transition (``from_state != to_state``)
            OR it is an unannotated self-edge — the PARTIALLY_FILLED accretion is a REAL transition
            that moves ``filled_qty`` (2026-09-10 round-3 review: routing it here audited a clean
            event while the fill never reached the row, an under-protected position, R3). Both
            belong in :meth:`record`, which applies the order-row guards.
        """
        annotated = any(event.payload.get(flag) for flag in NOOP_FLAGS)
        if event.from_state is not event.to_state or not annotated:
            raise ValueError(
                f"record_noop got a {event.from_state} -> {event.to_state} event without a "
                f"{sorted(NOOP_FLAGS)} annotation; use record()"
            )
        with transaction(self._conn):
            row = self._conn.execute(
                "SELECT state FROM orders WHERE order_id=?", (order_id,)
            ).fetchone()
            if row is None:
                raise LookupError(f"order {order_id} not persisted; insert() first")
            self._conn.execute(
                "INSERT INTO order_events (order_id, from_state, to_state, payload, at) VALUES (?, ?, ?, ?, ?)",
                (
                    order_id,
                    event.from_state.value,
                    event.to_state.value,
                    _json(event.payload),
                    event.at.isoformat(),
                ),
            )
        _log.info(
            "order_noop_recorded",
            order_id=order_id,
            state=event.from_state.value,
            persisted_state=row["state"],
            reason=event.payload.get("duplicate_reason") or event.payload.get("broker_status"),
        )

    @staticmethod
    def _values(order: PlatformOrder) -> tuple[Any, ...]:
        return (
            order.order_id,
            order.broker_order_id,
            order.position_id,
            order.proposal_id,
            order.verdict_id,
            order.role.value,
            1 if order.is_paper else 0,
            order.state.value,
            order.product,
            order.side,
            order.qty,
            order.filled_qty,
            _text(order.price),
            _text(order.trigger_price),
            order.modifications,
            order.reject_reason,
            _json(order.raw_broker_payload),
            _ts(order.created_at),
            _ts(order.updated_at),
        )

    # ----------------------------------------------------------------- reads
    def get(self, order_id: str) -> PlatformOrder | None:
        row = self._conn.execute(f"{_SELECT} WHERE order_id=?", (order_id,)).fetchone()
        return None if row is None else _row_to_order(row)

    def by_broker_id(self, broker_order_id: str | None) -> PlatformOrder | None:
        """Correlate a postback to a platform order (A3). A NULL broker id never matches — an
        un-correlated order must go through the pending-correlation buffer, not a NULL join."""
        if broker_order_id is None:
            return None
        row = self._conn.execute(
            f"{_SELECT} WHERE broker_order_id=?", (str(broker_order_id),)
        ).fetchone()
        return None if row is None else _row_to_order(row)

    def open_orders(self) -> list[PlatformOrder]:
        """Every non-terminal order, oldest first (the reconciler's and square-off's working set)."""
        marks = ", ".join("?" for _ in TERMINAL_ORDER_STATES)
        rows = self._conn.execute(
            f"{_SELECT} WHERE state NOT IN ({marks}) ORDER BY created_at, order_id",
            tuple(s.value for s in sorted(TERMINAL_ORDER_STATES)),
        ).fetchall()
        return [_row_to_order(r) for r in rows]

    def events(self, order_id: str) -> list[OrderEvent]:
        """The order's transition audit trail in arrival order (R8)."""
        rows = self._conn.execute(
            "SELECT from_state, to_state, payload, at FROM order_events WHERE order_id=? ORDER BY id",
            (order_id,),
        ).fetchall()
        return [
            OrderEvent(
                from_state=OrderState(r["from_state"]),
                to_state=OrderState(r["to_state"]),
                payload=json.loads(r["payload"]) if r["payload"] else {},
                at=_parse_ts(r["at"]),
            )
            for r in rows
        ]
