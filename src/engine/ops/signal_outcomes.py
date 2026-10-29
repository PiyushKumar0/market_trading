"""Hindsight scoring of every published brk20/hi52/ins signal and every C2-blocked brk20 crossing
(review §9 #5/#6). The constructs and what freezes when are migration 0020's.

HINDSIGHT: no scanner, threshold, gate, prompt or promotion code may read ``signal_outcomes``. Writes
only ``signal_outcomes``; reads ``universe_ew_returns``, whose only writer is ``RecOutcomesJob``.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.learning.benchmark import bench_pct
from engine.learning.exit_sim import Held, Pending, SimBar, simulate
from engine.marketdata.store import MarketStore
from engine.ops.jobs import JOB_DAILY_BARS, JOB_SIGNAL_OUTCOMES
from engine.ops.rec_outcomes import EW_BACKFILL_FROM, FIXED_HORIZONS, WasRunFn
from engine.risk.limits import LimitsEngine, LimitTable
from engine.strategy.cost_model import CostModel
from engine.strategy.hi52_edge import PRODUCT, STOP_PCT, sizing_notional
from engine.strategy.scanners import brk20, hi52, ins
from engine.strategy.scanners.hi52 import UNADJUSTED_KINDS

_log = get_logger("engine.ops.signal_outcomes")

_STRATEGIES = (brk20.STRATEGY_ID, hi52.STRATEGY_ID, ins.STRATEGY_ID)
_FINAL = frozenset({"unfilled", "void_ca", "unscorable"})
_KEY = ("d", "symbol", "strategy_id", "construct")
_COLUMNS = (
    *_KEY, "status", "fill_d", "fill_px", "exit_d", "cost_pct", "net_t5", "net_t10", "net_t20",
    "bench_t20", "excess_t20", "updated_at",
)
_UPSERT = (
    f"INSERT OR REPLACE INTO signal_outcomes ({', '.join(_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)
_FILL_CLOSE = time(15, 30)
_HUNDRED = Decimal("100")


@dataclass(frozen=True)
class SignalOutcomesResult:
    d: date
    unfinished: bool = False
    scored: int = 0


@dataclass(frozen=True)
class _Signal:
    d: date
    symbol: str
    strategy_id: str
    construct: str
    levels: tuple[str, str | None] | None = None   # brk20 (entry, stop) text; None for hi52/ins

    @property
    def key(self) -> tuple[str, str, str, str]:
        return self.d.isoformat(), self.symbol, self.strategy_id, self.construct


class _Unscorable(Exception):
    pass


class SignalOutcomesJob:
    def __init__(
        self, conn: sqlite3.Connection, store: MarketStore, calendar: NSECalendar, cost_model: CostModel,
        limits_engine: LimitsEngine, settings: Settings, clock: Clock, was_run: WasRunFn,
    ) -> None:
        self._conn = conn
        self._store = store
        self._calendar = calendar
        self._costs = cost_model
        self._limits = limits_engine
        self._settings = settings
        self._clock = clock
        self._was_run = was_run

    async def run(self, d: date) -> SignalOutcomesResult:
        """Score every non-final signal published by ``d``, walking daily bars through ``d`` only.

        ``unfinished`` (no watermark, replayed later) while ``daily_bars`` for ``d`` is unrecorded or
        failed."""
        if not self._was_run(JOB_DAILY_BARS, d):
            _log.info("signal_outcomes_unfinished", d=d.isoformat(), waiting_for=JOB_DAILY_BARS)
            return SignalOutcomesResult(d, unfinished=True)
        if self._conn.execute(
            "SELECT 1 FROM job_runs WHERE job_id=? AND status='success' AND run_for_date>? LIMIT 1",
            (JOB_SIGNAL_OUTCOMES, d.isoformat()),
        ).fetchone():
            _log.info("signal_outcomes_replay_skipped", d=d.isoformat(), reason="a later day already scored")
            return SignalOutcomesResult(d)
        cur = self._conn.execute("SELECT * FROM signal_outcomes")
        cols = [c[0] for c in cur.description]
        stored = {r[:4]: dict(zip(cols, r, strict=True)) for r in map(tuple, cur)}
        todo: dict[str, list[_Signal]] = defaultdict(list)
        for s in self._signals(d):
            if not _final(stored.get(s.key)):
                todo[s.symbol].append(s)
        ew = {date.fromisoformat(r[0]): r[1] for r in self._conn.execute("SELECT d, ret FROM universe_ew_returns")}
        table = self._limits.load()
        now = self._clock.now().isoformat()
        rows = []
        for symbol, signals in todo.items():
            bars, ex = await self._store.arun(self._load, symbol, min(s.d for s in signals), d)
            for s in signals:
                prev = stored.get(s.key)
                try:
                    row = self._score(s, prev, bars, ex, d, ew, table)
                except Exception as exc:  # noqa: BLE001 - one bad row never costs the others
                    _log.warning("signal_outcome_failed", key=list(s.key), error=repr(exc))
                    continue
                if prev is None or {**row, "updated_at": None} != {**prev, "updated_at": None}:
                    rows.append({**row, "updated_at": now})
        if rows:
            with transaction(self._conn):
                self._conn.executemany(_UPSERT, [tuple(r[c] for c in _COLUMNS) for r in rows])
        _log.info("signal_outcomes_scored", d=d.isoformat(), label="hindsight", scored=len(rows),
                  statuses=dict(Counter(r["status"] for r in rows)))
        return SignalOutcomesResult(d, scored=len(rows))

    # ------------------------------------------------------------------ population
    def _signals(self, d: date) -> list[_Signal]:
        """hi52/ins: ``rule`` + ``next_open``. brk20 on its crossing session: ``rule`` + ``next_open``;
        a re-offer: ``next_open`` costed from the latest resting row covering it. Blocked crossings:
        ``rule_blocked``."""
        resting: dict[str, list[tuple[date, date, str, str | None]]] = defaultdict(list)
        for symbol, signal_d, expires_d, entry, stop in self._conn.execute(
            "SELECT symbol, signal_d, expires_d, entry, stop FROM brk20_resting_levels ORDER BY signal_d"
        ):
            resting[symbol].append((date.fromisoformat(signal_d), date.fromisoformat(expires_d), entry, stop))
        out = []
        for day_s, symbol, sid in self._conn.execute(
            f"SELECT d, symbol, strategy_id FROM prescreen_day_slots WHERE strategy_id IN "
            f"({', '.join('?' for _ in _STRATEGIES)}) AND d <= ? ORDER BY d, symbol, strategy_id",
            (*_STRATEGIES, d.isoformat()),
        ):
            day = date.fromisoformat(day_s)
            constructs: tuple[str, ...] = ("rule", "next_open")
            levels = None
            if sid == brk20.STRATEGY_ID:
                rows = resting.get(symbol, [])
                first = [r for r in rows if r[0] == day]
                covering = [r for r in rows if r[0] < day <= r[1]]
                if first:
                    levels = first[0][2:]
                elif covering:
                    constructs, levels = ("next_open",), covering[-1][2:]
            out += [_Signal(day, symbol, sid, c, levels) for c in constructs]
        out += [
            _Signal(date.fromisoformat(signal_d), symbol, brk20.STRATEGY_ID, "rule_blocked", (entry, stop))
            for symbol, signal_d, entry, stop in self._conn.execute(
                "SELECT symbol, signal_d, entry, stop FROM brk20_regime_blocked WHERE signal_d <= ?",
                (d.isoformat(),),
            )
        ]
        return out

    def _load(self, symbol: str, start: date, d: date) -> tuple[list[SimBar], list[date]]:
        daily = self._store.get_bars_1d(symbol, start, d)
        corp = self._store.get_corp_actions(symbol=symbol, ex_from=start)
        return ([SimBar(b.d, b.open, b.high, b.low, b.close) for b in daily],
                [r["ex_date"] for r in corp if r["kind"] in UNADJUSTED_KINDS])

    # ------------------------------------------------------------------ one row
    def _score(
        self, s: _Signal, prev: dict[str, Any] | None, bars: Sequence[SimBar], ex: Sequence[date], d: date,
        ew: Mapping[date, float | None], table: LimitTable,
    ) -> dict[str, Any]:
        row = dict(prev) if prev is not None else {
            **dict.fromkeys(_COLUMNS), **dict(zip(_KEY, s.key, strict=True)), "status": "open"}
        if row["status"] == "closed":
            return self._with_bench(row, ew)
        try:
            level, stop_pct = self._geometry(s)
            if row["cost_pct"] is None:
                row["cost_pct"] = float(self._cost(stop_pct, table))
        except (_Unscorable, ArithmeticError, ValueError) as exc:
            _log.warning("signal_outcome_unscorable", key=list(s.key), reason=str(exc))
            return {**row, "status": "unscorable"}
        try:
            if row["fill_d"] is not None:
                filled = date.fromisoformat(row["fill_d"])
                start: Pending | Held = Held(Decimal(str(row["fill_px"])),
                                             datetime.combine(filled, _FILL_CLOSE, tzinfo=IST))
                first = self._calendar.add_sessions(filled, 1)
            elif s.construct == "next_open":
                first = self._calendar.add_sessions(s.d, 1)
                start = Pending("market", None, s.d, first)
            elif level is None:
                first = self._calendar.add_sessions(s.d, 0)
                start = Pending("market", None, s.d - timedelta(days=1), s.d)
            else:
                first = self._calendar.add_sessions(s.d, 0)
                start = Pending("touch", level, s.d, self._calendar.add_sessions(s.d, 4))
        except ValueError:                      # the calendar horizon: re-scored once it is extended
            return {**row, "status": "open"}

        walk = self._walk(bars, first, s)
        if isinstance(start, Pending) and not walk and any(b.d > first for b in bars):
            _log.warning("signal_outcome_unscorable", key=list(s.key), reason=f"no bar on {first}")
            return {**row, "status": "unscorable"}
        cost = Decimal(str(row["cost_pct"]))
        outs = {k: simulate(walk, start, stop=None, target=None, horizon=k - 1, end=d, cost_pct=cost,
                            ex_dates=ex) for k in FIXED_HORIZONS}
        t20 = outs[20]
        fill_d = t20.fill_d
        if row["fill_d"] is None and fill_d is not None:
            row.update(fill_d=fill_d.isoformat(), fill_px=str(t20.fill_px))
        status = {"exit": "closed", "void_ca": "void_ca", "open": "open",
                  "unfilled": "unscorable" if level is None else "unfilled"}[t20.status]
        exit_d = t20.exit_d if status == "closed" else None
        if status == "void_ca" and fill_d is not None:
            exit_d = min(x for x in ex if x > fill_d)
        # brk20's level is a pre-ex price: an ex-date after the signal and before the fill (or inside
        # a window still unfilled) voids it too.
        pre_end = fill_d or (min(start.until, d) if isinstance(start, Pending) else d)
        pre_fill = [x for x in ex if level is not None and s.d < x <= pre_end]
        if pre_fill:
            status, exit_d = "void_ca", min(pre_fill)
        for k, o in outs.items():
            if row[f"net_t{k}"] is None and not pre_fill and o.net_pct is not None:
                row[f"net_t{k}"] = float(o.net_pct)
        row.update(status=status, exit_d=None if exit_d is None else exit_d.isoformat())
        return self._with_bench(row, ew) if status == "closed" else row

    def _geometry(self, s: _Signal) -> tuple[Decimal | None, Decimal]:
        """``(touch level, stop %)``: the level for brk20's rule constructs only."""
        if s.strategy_id == hi52.STRATEGY_ID:
            return None, STOP_PCT
        if s.strategy_id == ins.STRATEGY_ID:
            return None, Decimal(str(self._settings.ins.stop_pct))
        if s.levels is None or s.levels[1] is None:
            raise _Unscorable("no resting or blocked row with a stop")
        entry = Decimal(s.levels[0])
        stop_pct = (entry - Decimal(s.levels[1])) / entry * _HUNDRED
        return (None if s.construct == "next_open" else entry), stop_pct

    def _cost(self, stop_pct: Decimal, table: LimitTable) -> Decimal:
        """One CNC round trip at the §7.1-sized notional at the ``equity_floor_rung``, capped by the
        per-stock and deployed-capital caps."""
        lim = table.limits
        floor = table.capital_base_inr * (1 + Decimal(str(lim.equity_floor_rung.equity_pct_of_base)) / _HUNDRED)
        notional = min(
            sizing_notional(floor, position_pct=Decimal(str(lim.per_trade_risk.swing_position_pct)),
                            gap_mult=Decimal(str(lim.per_trade_risk.overnight_gap_mult)), stop_pct=stop_pct),
            lim.per_stock_exposure.cnc_notional_inr, lim.capital_cap.max_deployed_capital_inr,
        )
        return self._costs.breakeven_pct(notional, PRODUCT)

    def _walk(self, bars: Sequence[SimBar], first: date, s: _Signal) -> list[SimBar]:
        """``bars`` on consecutive regular sessions from ``first``. ``add_sessions`` never counts a
        muhurat session, so its bar is never walked; a missing session or the calendar horizon ends
        the walk (the row stays ``open``) instead of the next bar standing in for that session."""
        out: list[SimBar] = []
        want = first
        for b in bars:
            if b.d < want:
                continue
            if b.d > want:
                if out:
                    _log.warning("signal_outcome_bar_missing", key=list(s.key), session=want.isoformat())
                break
            out.append(b)
            try:
                want = self._calendar.add_sessions(want, 1)
            except ValueError:
                break
        return out

    def _with_bench(self, row: dict[str, Any], ew: Mapping[date, float | None]) -> dict[str, Any]:
        if row["bench_t20"] is not None:
            return row
        bench = bench_pct(ew, date.fromisoformat(row["fill_d"]), date.fromisoformat(row["exit_d"]), "time",
                          self._calendar)
        if bench is None:
            return row
        return {**row, "bench_t20": float(bench), "excess_t20": row["net_t20"] - float(bench)}


def _final(row: Mapping[str, Any] | None) -> bool:
    if row is None:
        return False
    return row["status"] in _FINAL or row["status"] == "closed" and (
        row["bench_t20"] is not None or row["fill_d"] < EW_BACKFILL_FROM.isoformat())
