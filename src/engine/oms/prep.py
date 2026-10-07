"""Paper session prep (plan Q4.7): what the unobserved session minutes did to the open paper positions,
applied before the bridge forwards a tick.

1. Catch-up, PENDING_EXIT included. An unadjusted ex-date after the window start voids without a walk:
   Kite candles come corporate-action adjusted (A11) and the stops are not. Otherwise the backfilled 1m
   bars are walked through ``exit_sim``; an exit closes by bookkeeping, and a symbol whose fetch failed
   voids. A minute without a candle inside a successful fetch had no trades.
2. Corporate-action check: an ex-date today voids a position held across it.
3. Survivors still PENDING_EXIT re-enter the exit routine.

One deadline covers the three steps; whatever a step did not finish, by failure or timeout, is voided.
SQLite runs on the event-loop thread and no write spans an ``await``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from engine.core.clock import IST
from engine.core.log import get_logger
from engine.core.scope import HELD_STATES_SQL, scope_sql
from engine.core.types import Bar
from engine.oms.exits import CorpActionsFn, PreExMarkFn
from engine.oms.positions import PositionBook
from engine.oms.protection import ExitFn
from engine.oms.state import CloseReason, PositionState

_log = get_logger("engine.oms.prep")

_HELD = f"{scope_sql('paper', has_origin=True)} AND {HELD_STATES_SQL['paper']}"

PREP_DEADLINE_S = 120.0
#: The walk stops this long before prep start: the forming minute has no candle yet (``confirm_until``).
WALK_LAG = timedelta(minutes=2)


@dataclass(frozen=True)
class Walk:
    """One position's catch-up walk: from ``observed``, the last observed instant (the bar of the minute
    holding it is walked), to ``end``, exclusive."""

    avg_entry: Decimal
    observed: datetime
    stop: Decimal | None
    target: Decimal | None
    exit_session: date | None
    end: datetime


@dataclass(frozen=True)
class Replayed:
    price: Decimal
    reason: CloseReason
    at: datetime


#: ``bars_fn(symbols, frm, to)``: each symbol's 1m bars in ``[frm, to)`` once the window's session minutes
#: are backfilled; None for a symbol whose fetch failed.
BarsFn = Callable[[Sequence[str], datetime, datetime], Awaitable[Mapping[str, Sequence[Bar] | None]]]
#: ``replay_fn(bars, walk)``: the walk's exit, or None while the position stays open.
ReplayFn = Callable[[Sequence[Bar], Walk], Replayed | None]
#: ``mark_fn(symbol)``: the mark the paper tracker last carried; None prices a void at ``avg_entry``.
MarkFn = Callable[[str], Decimal | None]
_Todo = dict[str, sqlite3.Row]


class PrepIncomplete(RuntimeError):
    """A step failed or ran out of time; what it did not finish was voided."""


@dataclass
class PrepCounters:
    """Since boot, for ``/paper status``."""

    runs: int = 0
    replay_exits: int = 0
    voids: int = 0
    failed_steps: int = 0


def _dec(value: Any) -> Decimal | None:
    return None if value is None or value == "" else Decimal(str(value))


class SessionPrep:
    """:meth:`run` is the PaperRuntime's prep coroutine."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        book: PositionBook,
        exit_fn: ExitFn,
        bars_fn: BarsFn,
        replay_fn: ReplayFn,
        corp_actions_fn: CorpActionsFn,
        pre_ex_mark_fn: PreExMarkFn,
        mark_fn: MarkFn,
    ) -> None:
        self._conn = conn
        self._book = book
        self._exit_fn = exit_fn
        self._bars_fn = bars_fn
        self._replay_fn = replay_fn
        self._corp_actions = corp_actions_fn
        self._pre_ex_mark = pre_ex_mark_fn
        self._mark_fn = mark_fn
        self.counters = PrepCounters()

    async def run(self, since: datetime | None, until: datetime) -> None:
        """Prep for the window ``(since, until]``, ``since`` None when nothing was ever observed. Raises
        :class:`PrepIncomplete` only after voiding what a failed step left."""
        self.counters.runs += 1
        deadline = asyncio.get_running_loop().time() + PREP_DEADLINE_S
        today = until.astimezone(IST).date()
        steps: tuple[tuple[str, Callable[[_Todo], Awaitable[None]]], ...] = (
            ("catch_up", lambda todo: self._catch_up(todo, since, until, today)),
            ("corp_actions", lambda todo: self._corp_check(todo, until, today)),
            ("pending_exits", lambda todo: self._pending_exits(todo, until)),
        )
        failed: list[str] = []
        for name, step in steps:
            todo: _Todo = {r["position_id"]: r for r in self._held()}
            try:
                async with asyncio.timeout_at(deadline):
                    await step(todo)
            except Exception:
                failed.append(name)
                self.counters.failed_steps += 1
                _log.exception("paper_prep_step_failed", step=name, unfinished=sorted(todo))
                for row in todo.values():
                    await self._void(row, until, "prep_incomplete")
        _log.info("paper_prep_complete", since=since, until=until, failed=failed)
        if failed:
            raise PrepIncomplete(f"steps {failed} failed or timed out; their unfinished positions were voided")

    # ---------------------------------------------------------------- steps
    async def _catch_up(self, todo: _Todo, since: datetime | None, until: datetime, today: date) -> None:
        if not todo:
            return
        observed = {pid: self._observed(since, row) for pid, row in todo.items()}
        first = min(observed.values()).astimezone(IST).date()
        ex_dates: dict[str, list[date]] = {}
        for r in await self._corp_actions(first + timedelta(days=1), today):
            ex_dates.setdefault(r["symbol"], []).append(r["ex_date"])
        for pid, row in list(todo.items()):
            start = observed[pid].astimezone(IST).date()
            hits = sorted(d for d in ex_dates.get(row["symbol"], ()) if start < d <= today)
            if hits:
                await self._void(row, until, "corp_action", hits[0])
                del todo[pid]

        end = until - WALK_LAG
        frm = min((observed[pid].replace(second=0, microsecond=0) for pid in todo), default=end)
        symbols = sorted({row["symbol"] for row in todo.values()})
        bars = await self._bars_fn(symbols, frm, end) if frm < end else {s: () for s in symbols}

        async def walk(row: sqlite3.Row) -> None:
            series = bars.get(row["symbol"])
            if series is None:
                await self._void(row, until, "downtime_uncovered")
                return
            exit_session = row["exit_session"]
            replayed = self._replay_fn(series, Walk(
                avg_entry=Decimal(str(row["avg_entry"])), observed=observed[row["position_id"]],
                stop=_dec(row["stop"]), target=_dec(row["target"]),
                exit_session=None if exit_session is None else date.fromisoformat(exit_session), end=end,
            ))
            if replayed is not None:
                await self._book.close_bookkeeping(row["position_id"], replayed.price, replayed.at, replayed.reason,
                                                   "downtime_replay")
                self.counters.replay_exits += 1

        await self._each(todo, until, walk)

    async def _corp_check(self, todo: _Todo, until: datetime, today: date) -> None:
        symbols = {r["symbol"] for r in await self._corp_actions(today, today) if r["ex_date"] == today}

        async def check(row: sqlite3.Row) -> None:
            opened = datetime.fromisoformat(row["opened_at"]).astimezone(IST).date()
            if row["symbol"] in symbols and opened < today:
                await self._void(row, until, "corp_action", today)

        await self._each(todo, until, check)

    async def _pending_exits(self, todo: _Todo, until: datetime) -> None:
        async def resend(row: sqlite3.Row) -> None:
            if row["state"] == PositionState.PENDING_EXIT.value:
                reason = CloseReason(row["close_reason"] or CloseReason.GTT_FAILURE_EXIT)
                await self._exit_fn(row["position_id"], reason, "restart_late")

        await self._each(todo, until, resend)

    # ---------------------------------------------------------------- helpers
    async def _each(self, todo: _Todo, until: datetime, work: Callable[[sqlite3.Row], Awaitable[None]]) -> None:
        """``work`` per position, each removed from ``todo`` once done; one that raises is voided."""
        for pid in list(todo):
            try:
                await work(todo[pid])
            except Exception:
                _log.exception("paper_prep_position_failed", position_id=pid)
                await self._void(todo[pid], until, "prep_incomplete")
            del todo[pid]

    async def _void(self, row: sqlite3.Row, at: datetime, basis: str, ex_date: date | None = None) -> None:
        """Void at the pre-ex mark for an ex-date, else the tracker's mark; logged and counted, never raises."""
        pid, symbol = row["position_id"], row["symbol"]
        try:
            mark = await self._pre_ex_mark(symbol, ex_date) if ex_date is not None else self._mark_fn(symbol)
            await self._book.void(pid, mark, at, basis)
        except Exception:
            _log.exception("paper_prep_void_failed", position_id=pid, basis=basis)
            return
        self.counters.voids += 1
        _log.warning("paper_prep_void", position_id=pid, symbol=symbol, basis=basis, mark=None if mark is None else str(mark))

    def _observed(self, since: datetime | None, row: sqlite3.Row) -> datetime:
        opened = datetime.fromisoformat(row["opened_at"])
        return opened if since is None else max(since, opened)

    def _held(self) -> list[sqlite3.Row]:
        return self._conn.execute(f"SELECT * FROM positions WHERE {_HELD} ORDER BY opened_at").fetchall()
