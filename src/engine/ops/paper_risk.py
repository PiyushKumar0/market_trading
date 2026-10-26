"""Paper equity and halts (plan Q4.8).

:meth:`PaperRisk.tick` is ``paper_tick``'s equity step: it snapshots paper equity, evaluates the §7.1
floor ladder and daily-loss rungs on the paper tracker, and latches ``paper_halts`` only. Paper never
writes ``mode_state``, ``risk_state_causes`` or ``kill_state`` (§1.5) and never runs in ``equity_tick``.

:meth:`PaperRisk.note_reject` is the paper broker-rejection storm detector (§3.5.1), fed by the paper
OrderManager's ``on_reject`` seam.

Daily causes clear on the next session; every other cause latches until a ``/paper reset``, which
exits the book and opens a new epoch on the first flat book after the request.
"""

from __future__ import annotations

import bisect
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal

from engine.core.clock import IST, Clock
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
STORM_CAUSE = "rejection_storm"
#: cause -> (rung, latched until a reset). Unlatched causes are daily.
HALTS: dict[str, tuple[RiskState, bool]] = {
    "daily_loss_soft": (RiskState.FROZEN, False),
    "daily_loss_hard": (RiskState.CLOSE_ONLY, False),
    "floor_weekly_drawdown": (RiskState.CLOSE_ONLY, True),
    "floor_equity_floor_rung": (RiskState.CLOSE_ONLY, True),
    "floor_cumulative_floor": (RiskState.KILLED, True),
    RESET_CAUSE: (RiskState.CLOSE_ONLY, True),
    STORM_CAUSE: (RiskState.FROZEN, False),
}
FLATTEN_CAUSES = frozenset({"floor_equity_floor_rung", "floor_cumulative_floor", RESET_CAUSE})

#: §3.5.1: this many broker rejects inside one window (span inclusive) is a rejection storm.
STORM_REJECTS = 3
STORM_WINDOW = timedelta(seconds=60)
#: Reject times kept per session day: a memory bound only, far above any real day's count.
_REJECTS_KEPT = 256

#: ``flatten(basis)``: the Q4.6 exit routine for every held paper position (``ExitManager.flatten_all``).
FlattenFn = Callable[[str], Awaitable[int]]
SessionFn = Callable[[date], Session | None]

_HELD = (f"SELECT position_id, symbol FROM positions "
         f"WHERE {scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}")
_WORKING = f"SELECT 1 FROM orders WHERE {scope_sql('paper')} AND state NOT IN {TERMINAL_ORDER_STATES_SQL} LIMIT 1"


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
        capital_base: Decimal | Callable[[], Decimal],
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
        self._mark_price = mark_price
        self._limits_fn = limits_fn
        self._flatten = flatten
        self._flattened: set[str] = set()
        self._reject_day: date | None = None
        self._rejects: list[datetime] = []        # today's broker reject times, sorted

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

    def note_reject(self, at: datetime) -> None:
        """Count one paper broker reject at its broker time ``at`` (tz-aware). ``STORM_REJECTS`` of
        today's rejects inside any ``STORM_WINDOW`` latch the daily ``rejection_storm`` halt (§3.5.1).

        Decided on the sorted broker times, so arrival order neither creates nor hides a storm. A
        reject dated on another day than the clock's is not counted: a stale redelivery must not
        freeze today, and the halt would be cleared as stale on the next tick anyway."""
        if at.tzinfo is None:
            raise ValueError(f"reject time {at!r} is naive; a tz-aware broker time is required")
        now = self._clock.now()
        when = at.astimezone(IST)
        if when.date() != now.date():
            _log.info("paper_reject_other_day_not_counted", at=when.isoformat())
            return
        if self._reject_day != now.date():
            self._reject_day, self._rejects = now.date(), []
        k = bisect.bisect_right(self._rejects, when)
        self._rejects.insert(k, when)
        storm = self._storm(k)
        del self._rejects[:-_REJECTS_KEPT]
        if storm is None:
            return
        # Yesterday's daily halt, if no tick has cleared it yet, would swallow today's latch.
        self._clear_stale_daily(now)
        self._latch({STORM_CAUSE: storm}, now)

    def _storm(self, k: int) -> str | None:
        """The storm the reject at sorted index ``k`` completes, if any. Only windows holding it
        count: a storm already latched and since cleared (``/paper reset``) is not re-found."""
        times = self._rejects
        for i in range(max(0, k - STORM_REJECTS + 1), min(k, len(times) - STORM_REJECTS) + 1):
            if times[i + STORM_REJECTS - 1] - times[i] <= STORM_WINDOW:
                inside = [t for t in times[i:] if t - times[i] <= STORM_WINDOW]
                span = (inside[-1] - inside[0]).total_seconds()
                return (f"{len(inside)} paper broker rejects in {span:g} s "
                        f"({inside[0]:%H:%M:%S}-{inside[-1]:%H:%M:%S} IST); "
                        f"limit {STORM_REJECTS} in {STORM_WINDOW.total_seconds():g} s")
        return None

    def _evaluate(self, now: datetime) -> None:
        # An unmarked holding would count at avg_entry against a baseline that carried its mark.
        unmarked = sorted({r["symbol"] for r in self._conn.execute(_HELD) if self._mark_price(r["symbol"]) is None})
        if unmarked:
            _log.warning("paper_equity_skipped_unmarked", symbols=unmarked)
            return
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
        base = self.tracker.capital_base           # the one base source (O19 §2); read once
        self._broker.reseed_margin(base)
        self.tracker.set_day_baseline(self.tracker.equity())
        self._flattened.clear()
        _log.warning("paper_epoch_started", epoch_started_at=now.isoformat(), capital_base=str(base))
