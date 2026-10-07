"""Paper equity and halts (plan Q4.8).

:meth:`PaperRisk.tick` is ``paper_tick``'s equity step: it snapshots paper equity, evaluates the §7.1
floor ladder and daily-loss rungs on the paper tracker, and latches ``paper_halts`` only. Paper never
writes ``mode_state``, ``risk_state_causes`` or ``kill_state`` (§1.5) and never runs in ``equity_tick``.

Daily causes clear on the next session; every other cause latches until a ``/paper reset``, which
exits the book and opens a new epoch on the first flat book after the request.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from decimal import Decimal

from engine.core.clock import Clock
from engine.core.db import transaction
from engine.core.enums import RiskState
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Session
from engine.paper.broker import PaperBroker
from engine.risk.exposure import ExposureTracker, MarkPrice
from engine.risk.gate import TERMINAL_ORDER_STATES_SQL
from engine.risk.limits import LimitTable, floor_limits_from

_log = get_logger("engine.ops.paper_risk")

_ORDER = (RiskState.NORMAL, RiskState.FROZEN, RiskState.CLOSE_ONLY, RiskState.KILLED)

RESET_CAUSE = "reset_pending"
#: cause -> (rung, latched until a reset). Unlatched causes are daily.
HALTS: dict[str, tuple[RiskState, bool]] = {
    "daily_loss_soft": (RiskState.FROZEN, False),
    "daily_loss_hard": (RiskState.CLOSE_ONLY, False),
    "floor_weekly_drawdown": (RiskState.CLOSE_ONLY, True),
    "floor_equity_floor_rung": (RiskState.CLOSE_ONLY, True),
    "floor_cumulative_floor": (RiskState.KILLED, True),
    RESET_CAUSE: (RiskState.CLOSE_ONLY, True),
}
FLATTEN_CAUSES = frozenset({"floor_equity_floor_rung", "floor_cumulative_floor", RESET_CAUSE})

#: ``flatten(basis)``: the Q4.6 exit routine for every held paper position (``ExitManager.flatten_all``).
FlattenFn = Callable[[str], Awaitable[int]]
SessionFn = Callable[[date], Session | None]

_HELD = (f"SELECT position_id FROM positions "
         f"WHERE {scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}")
_WORKING = f"SELECT 1 FROM orders WHERE {scope_sql('paper')} AND state NOT IN {TERMINAL_ORDER_STATES_SQL} LIMIT 1"


async def no_exit_routine(basis: str) -> int:
    """``flatten`` until the ExitManager is composed; nothing can hold a paper position before then."""
    raise RuntimeError(f"no paper exit routine is wired (flatten {basis})")


def active_halts(conn: sqlite3.Connection) -> dict[str, RiskState]:
    """Uncleared paper halts; a rung that is not a ``RiskState`` value fails closed to KILLED."""
    halts: dict[str, RiskState] = {}
    for row in conn.execute("SELECT cause, rung FROM paper_halts WHERE cleared_at IS NULL"):
        try:
            halts[row["cause"]] = RiskState(row["rung"])
        except ValueError:
            halts[row["cause"]] = RiskState.KILLED
    return halts


def paper_effective_state(conn: sqlite3.Connection, real_state: Callable[[], RiskState]) -> Callable[[], RiskState]:
    """The paper entry state: the worse of the real risk state and the active paper halts (§1.5)."""

    def state() -> RiskState:
        return max([real_state(), *active_halts(conn).values()], key=_ORDER.index)

    return state


class PaperRisk:
    """The paper tracker and halt ladder. :attr:`tracker` also feeds the paper GateContextBuilder,
    whose consecutive-losses rule is the paper rung for losing streaks."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        session_fn: SessionFn,
        broker: PaperBroker,
        *,
        capital_base: Decimal,
        mark_price: MarkPrice,
        limits_fn: Callable[[], LimitTable],
        flatten: FlattenFn,
    ) -> None:
        if not isinstance(broker, PaperBroker):
            raise TypeError(f"PaperRisk takes a PaperBroker only, got {type(broker).__name__} (D8)")
        self.tracker = ExposureTracker(conn, clock, capital_base, mark_price, scope="paper")
        self._conn = conn
        self._clock = clock
        self._session_fn = session_fn
        self._broker = broker
        self._capital_base = capital_base
        self._limits_fn = limits_fn
        self._flatten = flatten
        self._flattened: set[str] = set()

    async def tick(self) -> None:
        now = self._clock.now()
        self._clear_stale_daily(now)
        # Before the snapshot: this tick's snapshot then carries the new epoch's equity.
        self._start_epoch_if_flat(now)
        session = self._session_fn(now.date())
        if session is not None and session.open <= now <= session.close:
            try:
                self._evaluate(now)
            except Exception:
                _log.exception("paper_equity_evaluation_failed")
        if self._reset_requested():
            self._latch({RESET_CAUSE: "/paper reset requested"}, now)
        await self._flatten_if_due()

    def _evaluate(self, now: datetime) -> None:
        self.tracker.persist_snapshot()
        table = self._limits_fn()
        reasons = {
            f"floor_{b.rung}": f"equity {b.equity} vs {b.threshold}"
            for b in self.tracker.evaluate_floors(floor_limits_from(table))
        }
        day = table.limits
        for rung in self.tracker.evaluate_day_loss(
            Decimal(str(day.daily_loss_soft.day_mtm_pct)), Decimal(str(day.daily_loss_hard.day_mtm_pct))
        ):
            reasons[rung] = f"day MTM {self.tracker.day_mtm()}"
        self._latch(reasons, now)

    def _latch(self, reasons: Mapping[str, str], now: datetime) -> None:
        new: list[str] = []
        with transaction(self._conn):
            for cause in reasons:
                rung, latched = HALTS[cause]
                cur = self._conn.execute(
                    "INSERT INTO paper_halts (cause, rung, set_at, latched) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(cause) DO UPDATE SET rung = excluded.rung, set_at = excluded.set_at, "
                    "latched = excluded.latched, cleared_at = NULL WHERE paper_halts.cleared_at IS NOT NULL",
                    (cause, rung.value, now.isoformat(), int(latched)),
                )
                if cur.rowcount:
                    new.append(cause)
        for cause in new:
            _log.warning("paper_halt_set", cause=cause, rung=HALTS[cause][0].value, reason=reasons[cause])

    def _clear_stale_daily(self, now: datetime) -> None:
        cur = self._conn.execute(
            "UPDATE paper_halts SET cleared_at = ? WHERE cleared_at IS NULL AND latched = 0 "
            "AND substr(set_at, 1, 10) < ?",
            (now.isoformat(), now.date().isoformat()),
        )
        if cur.rowcount:
            _log.info("paper_daily_halts_cleared", count=cur.rowcount)

    async def _flatten_if_due(self) -> None:
        """Exit the book once per cause; a position that turns up later (a resting entry filling)
        is exited too."""
        due = FLATTEN_CAUSES & active_halts(self._conn).keys()
        if not due:
            self._flattened.clear()
            return
        held = {r["position_id"] for r in self._conn.execute(_HELD)}
        if held <= self._flattened:
            return
        await self._flatten("paper_reset" if due == {RESET_CAUSE} else "equity_floor")
        self._flattened = held

    def _reset_requested(self) -> bool:
        row = self._conn.execute("SELECT reset_requested_at FROM paper_state WHERE id = 1").fetchone()
        return row is not None and row["reset_requested_at"] is not None

    def _start_epoch_if_flat(self, now: datetime) -> None:
        if (
            not self._reset_requested()
            or self._conn.execute(_HELD).fetchone() is not None
            or self._conn.execute(_WORKING).fetchone() is not None
        ):
            return
        with transaction(self._conn):
            self._conn.execute(
                "UPDATE paper_state SET epoch_started_at = ?, reset_requested_at = NULL WHERE id = 1",
                (now.isoformat(),),
            )
            self._conn.execute("UPDATE paper_halts SET cleared_at = ? WHERE cleared_at IS NULL", (now.isoformat(),))
        self._broker.reseed_margin(self._capital_base)
        self.tracker.set_day_baseline(self.tracker.equity())
        self._flattened.clear()
        _log.warning("paper_epoch_started", epoch_started_at=now.isoformat(), capital_base=str(self._capital_base))
