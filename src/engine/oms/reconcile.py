"""Paper Reconciler (plan Q4.7; R5): the OMS and ``gtts`` rows against the PaperBroker's book.

* A non-terminal paper order the broker reports differently is re-delivered through the OrderManager's
  postback consumer, so ``apply_update`` and the fill callbacks run as for a live postback.
* A fired leg the OMS has never seen is re-delivered too; the consumer adopts it.
* A working broker order nothing accounts for is cancelled; an active GTT no held position owns is deleted.
* ProtectionManager then verifies every OPEN position; PENDING_EXIT ones are left to their exits.

Mismatches are logged and counted, never raised. A pass is its own job, never a SessionLifecycle hook.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.oms.manager import OrderManager
from engine.oms.positions import leg_of
from engine.oms.protection import ProtectionManager
from engine.oms.state import TERMINAL_ORDER_STATES, PlatformOrder
from engine.oms.store import OrderStore
from engine.oms.updates import NOOP_FLAGS, apply_update, parse_postback
from engine.paper.broker import TERMINAL_STATUSES, PaperBroker

_log = get_logger("engine.oms.reconcile")

_PAPER = scope_sql("paper")
_WORKING = "state NOT IN ({})".format(", ".join(f"'{s.value}'" for s in sorted(TERMINAL_ORDER_STATES)))
#: How long a pass waits for the postback consumer to drain before it compares.
SETTLE_S = 5.0


@dataclass
class ReconcileCounters:
    """Since boot, for ``/paper status``; ``mismatches`` sums the three kinds below it."""

    passes: int = 0
    failures: int = 0
    mismatches: int = 0
    redelivered: int = 0
    orphans_cancelled: int = 0
    gtts: int = 0


class Reconciler:
    def __init__(
        self, conn: sqlite3.Connection, clock: Clock, broker: PaperBroker, *, orders: OrderManager,
        protection: ProtectionManager,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"Reconciler takes a PaperBroker only, got {type(broker).__name__} (D8)")
        self._conn = conn
        self._clock = clock
        self._broker = broker
        self._orders = orders
        self._protection = protection
        self._store = OrderStore(conn)
        self._lock = asyncio.Lock()
        self.counters = ReconcileCounters()

    async def run(self) -> None:
        """One pass; passes requested while one runs queue behind it."""
        async with self._lock:
            self.counters.passes += 1
            try:
                await self._pass()
            except Exception:
                self.counters.failures += 1
                _log.exception("paper_reconcile_failed")

    async def _pass(self) -> None:
        # Postbacks already queued land first, so one in flight is not taken for a lost one.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._orders.join(), SETTLE_S)
        book = {o["order_id"]: o for o in await self._broker.orders()}
        await self._orders_pass(book)
        await self._gtts_pass()
        await self._protection.verify_all()

    async def _orders_pass(self, book: dict[str, dict[str, Any]]) -> None:
        now = self._clock.now()
        known = {r["broker_order_id"] for r in self._conn.execute(
            f"SELECT broker_order_id FROM orders WHERE broker_order_id IS NOT NULL AND {_PAPER}")}
        for r in self._conn.execute(
            f"SELECT order_id FROM orders WHERE broker_order_id IS NOT NULL AND {_PAPER} AND {_WORKING}"
        ).fetchall():
            order = self._store.get(r["order_id"])
            row = book.get(order.broker_order_id)
            if row is None:
                self._mismatch("paper_reconcile_order_unknown_at_broker", order_id=order.order_id,
                               broker_order_id=order.broker_order_id, state=order.state.value)
            elif self._differs(order, row, now):
                self._redeliver(row, "paper_reconcile_order_differs", order_id=order.order_id, state=order.state.value)
        gtt_ids = {r["gtt_id"] for r in self._conn.execute(f"SELECT gtt_id FROM gtts WHERE {_PAPER}")}
        for broker_id, row in book.items():
            if broker_id in known:
                continue
            leg = leg_of(row.get("tag"))
            if leg is not None and leg[0] in gtt_ids:
                self._redeliver(row, "paper_reconcile_leg_unadopted", gtt_id=leg[0])
            elif row["status"] not in TERMINAL_STATUSES:
                self.counters.orphans_cancelled += 1
                self._mismatch("paper_reconcile_orphan_order", broker_order_id=broker_id, status=row["status"])
                await self._orders.cancel(broker_id, reason="reconcile_orphan")

    async def _gtts_pass(self) -> None:
        rows = {r["gtt_id"]: r for r in self._conn.execute(
            f"SELECT g.gtt_id, g.state, p.position_id IS NOT NULL AS held FROM gtts g LEFT JOIN positions p "
            f"ON p.position_id = g.position_id AND {scope_sql('paper', 'p', has_origin=True)} "
            f"AND p.{HELD_STATES_SQL['paper']} WHERE {scope_sql('paper', 'g')}")}
        live = {g["id"]: g["status"] for g in await self._broker.gtts()}
        for gtt_id, row in rows.items():
            if row["held"] and row["state"] in ("active", "triggered") and live.get(gtt_id) != row["state"]:
                self.counters.gtts += 1
                self._mismatch("paper_reconcile_gtt_differs", gtt_id=gtt_id, stored=row["state"],
                               broker=live.get(gtt_id))
        for gtt_id, status in live.items():
            row = rows.get(gtt_id)
            if status == "active" and (row is None or not row["held"]):
                self.counters.gtts += 1
                self._mismatch("paper_reconcile_orphan_gtt", gtt_id=gtt_id)
                await self._broker.delete_gtt(gtt_id)
                self._conn.execute(
                    f"UPDATE gtts SET state = 'deleted', last_verified_at = ? WHERE gtt_id = ? AND {_PAPER}",
                    (self._clock.now().isoformat(), gtt_id),
                )

    def _differs(self, order: PlatformOrder, row: dict[str, Any], now: datetime) -> bool:
        try:
            _, event = apply_update(order, parse_postback(row), at=now)
        except Exception as exc:
            self._mismatch("paper_reconcile_row_unusable", broker_order_id=row.get("order_id"), error=repr(exc))
            return False
        return event is not None and not any(event.payload.get(flag) for flag in NOOP_FLAGS)

    def _redeliver(self, row: dict[str, Any], event: str, **fields: Any) -> None:
        self.counters.redelivered += 1
        self._mismatch(event, broker_order_id=row["order_id"], status=row["status"], **fields)
        self._orders.redeliver(row)

    def _mismatch(self, event: str, **fields: Any) -> None:
        self.counters.mismatches += 1
        _log.warning(event, **fields)
