"""Hindsight scoring of every published brk20/hi52/ins signal and every C2-blocked brk20 crossing
(review §9 #5/#6). The constructs and what freezes when are migration 0020's.

HINDSIGHT: no scanner, threshold, gate, prompt or promotion code may read ``signal_outcomes``. Writes
only ``signal_outcomes``; reads ``universe_ew_returns``, whose only writer is ``RecOutcomesJob``.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.learning.benchmark import bench_pct
from engine.learning.exit_sim import Held, Pending, SimBar, simulate
from engine.marketdata.store import MarketStore, SymbolRename
from engine.ops.jobs import JOB_DAILY_BARS, JOB_SIGNAL_OUTCOMES
from engine.ops.rec_outcomes import EW_BACKFILL_FROM, FIXED_HORIZONS, WasRunFn
from engine.risk.limits import LimitsEngine, LimitTable
from engine.strategy import regime
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
_PCT_Q = Decimal("0.000001")                # exit_sim's gross-percent quantum
_NO_TOUCH = Decimal("Infinity")             # a halt session's low: no touch fills on it (no stop is simulated)
#: Regular sessions in a row on which the store holds a NIFTY 50 bar but none for the row's symbol (after its
#: fill, or its anchor if unfilled) before the row is final: delisted, merged or suspended. Pre-registered.
STALE_SESSIONS = 5
#: get_symbol_renames window: every stored snapshot (rekey_symbol's pair check uses the same), so a rename
#: stays followed for as long as a row can stay open.
_RENAME_LOOKBACK_DAYS = 3650


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


@dataclass(frozen=True)
class _Gaps:
    """What a missing bar may be judged against: the sessions with a NIFTY 50 bar, and the oldest failed,
    unrepaired ``daily_bars`` day (no missing bar on or after it is judged)."""

    index_days: frozenset[date]
    failed_from: date | None

    def judged(self, s: date) -> bool:
        return self.failed_from is None or s < self.failed_from

    def symbol_missing(self, s: date) -> bool:
        """``s`` may count against a symbol without a bar: the market traded and the day is not failed."""
        return s in self.index_days and self.judged(s)


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
        # Where each company's rows live NOW (rekey_symbol moves them), not where they lived on d. A failed
        # read fails the night: without it a renamed row would read as stale.
        names = _aliases(await self._store.arun(
            self._store.get_symbol_renames, max(d, self._clock.today()), lookback_days=_RENAME_LOOKBACK_DAYS))
        todo: dict[str, list[_Signal]] = defaultdict(list)
        for s in self._signals(d, names):
            if not _final(stored.get(s.key)):
                todo[s.symbol].append(s)
        ew = {date.fromisoformat(r[0]): r[1] for r in self._conn.execute("SELECT d, ret FROM universe_ew_returns")}
        sizing = _sizing(self._limits.load())
        failed = self._conn.execute(          # CatchUpRunner.first_failed_date: the oldest unrepaired day
            "SELECT min(run_for_date) FROM job_runs WHERE job_id=? AND status='failed'", (JOB_DAILY_BARS,),
        ).fetchone()[0]
        gaps = _Gaps(await self._index_days(min((s.d for v in todo.values() for s in v), default=d), d),
                     None if failed is None else date.fromisoformat(failed))
        now = self._clock.now().isoformat()
        rows = []
        for symbol, signals in todo.items():
            try:
                bars, ex = await self._store.arun(self._load, names(symbol), min(s.d for s in signals), d)
            except Exception as exc:  # noqa: BLE001 - one symbol's read never costs the others; retried next night
                _log.warning("signal_outcome_read_failed", symbol=symbol, error=repr(exc))
                continue
            for s in signals:
                prev = stored.get(s.key)
                try:
                    row = self._score(s, prev, bars, ex, d, ew, sizing, gaps)
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
    def _signals(self, d: date, names: Callable[[str], tuple[str, ...]]) -> list[_Signal]:
        """hi52/ins: ``rule`` + ``next_open``. brk20 on its publication session ``signal_d``: ``rule`` + ``next_open``;
        a re-offer: ``next_open`` costed from the latest resting row covering it. Blocked crossings:
        ``rule_blocked``. A journal symbol's resting rows are read through ``names`` (rekey_symbol moves them to
        the new symbol; the journal keeps the old one)."""
        stored: dict[str, list[tuple[date, date, str, str | None]]] = defaultdict(list)
        for symbol, signal_d, expires_d, entry, stop in self._conn.execute(
            "SELECT symbol, signal_d, expires_d, entry, stop FROM brk20_resting_levels ORDER BY signal_d"
        ):
            stored[symbol].append((date.fromisoformat(signal_d), date.fromisoformat(expires_d), entry, stop))

        def resting_of(symbol: str) -> list[tuple[date, date, str, str | None]]:
            by_day: dict[date, tuple[date, date, str, str | None]] = {}
            for name in names(symbol):                 # current symbol first: its row wins, as in rekey
                for r in stored.get(name, ()):
                    by_day.setdefault(r[0], r)
            return [by_day[k] for k in sorted(by_day)]

        resting: dict[str, list[tuple[date, date, str, str | None]]] = {}
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
                if symbol not in resting:
                    resting[symbol] = resting_of(symbol)
                rows = resting[symbol]
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

    def _load(self, names: Sequence[str], start: date, d: date) -> tuple[list[SimBar], list[date]]:
        """One company's bars and unadjusted-kind ex-dates under every name in ``names`` (current first: its
        same-day bar wins, as in rekey_symbol)."""
        by_day: dict[date, SimBar] = {}
        ex: set[date] = set()
        for name in names:
            for b in self._store.get_bars_1d(name, start, d):
                by_day.setdefault(b.d, SimBar(b.d, b.open, b.high, b.low, b.close))
            ex.update(r["ex_date"] for r in self._store.get_corp_actions(symbol=name, ex_from=start)
                      if r["kind"] in UNADJUSTED_KINDS)
        return [by_day[k] for k in sorted(by_day)], sorted(ex)

    async def _index_days(self, start: date, d: date) -> frozenset[date]:
        """Sessions the store holds a NIFTY 50 bar for. A failed read yields none: no row reads as stale."""
        try:
            bars = await self._store.arun(self._store.get_bars_1d, regime.INDEX_SYMBOL, start, d)
        except Exception as exc:  # noqa: BLE001 - the stale rule waits a night; scoring goes on
            _log.warning("signal_outcome_index_read_failed", error=repr(exc))
            return frozenset()
        return frozenset(b.d for b in bars)

    # ------------------------------------------------------------------ one row
    def _score(
        self, s: _Signal, prev: dict[str, Any] | None, bars: Sequence[SimBar], ex: Sequence[date], d: date,
        ew: Mapping[date, float | None], sizing: Callable[[Decimal], Decimal], gaps: _Gaps,
    ) -> dict[str, Any]:
        row = dict(prev) if prev is not None else {
            **dict.fromkeys(_COLUMNS), **dict(zip(_KEY, s.key, strict=True)), "status": "open"}
        if row["status"] == "closed":
            return self._with_bench(row, ew)
        try:
            level, stop_pct = self._geometry(s)
            if row["cost_pct"] is None:
                row["cost_pct"] = float(self._costs.breakeven_pct(sizing(stop_pct), PRODUCT))
        except (_Unscorable, ArithmeticError, ValueError) as exc:
            if row["cost_pct"] is not None:     # scored before: its levels went missing (a rename not followed)
                _log.warning("signal_outcome_levels_lost", key=list(s.key), reason=str(exc))
                return row
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

        seed = next((b for b in bars if b.d == start.at.date()), None) if isinstance(start, Held) else None
        walk, missing = self._walk(bars, first, seed, d, gaps)
        if isinstance(start, Pending) and missing == first and gaps.judged(first) and self._anchor_lost(bars, first):
            _log.warning("signal_outcome_unscorable", key=list(s.key), reason=f"no bar on {first}")
            return {**row, "status": "unscorable"}
        cost = Decimal(str(row["cost_pct"]))
        outs = {k: simulate(walk, start, stop=None, target=None, horizon=k - 1, end=d, cost_pct=cost,
                            ex_dates=ex) for k in FIXED_HORIZONS}
        t20 = outs[20]
        if t20.status == "open" and missing is not None:
            _log.warning("signal_outcome_bar_missing", key=list(s.key), session=missing.isoformat())
        fill_d = t20.fill_d
        if row["fill_d"] is None and fill_d is not None:
            row.update(fill_d=fill_d.isoformat(), fill_px=str(t20.fill_px))
        status = {"exit": "closed", "void_ca": "void_ca", "open": "open",
                  "unfilled": "unscorable" if level is None else "unfilled"}[t20.status]
        exit_d = t20.exit_d if status == "closed" else None
        stale_net = None
        base = walk[-1].d if walk else (fill_d or first)
        if status == "open" and self._stale(bars, base, d, gaps):
            last = bars[-1] if bars else None
            fill_px = t20.fill_px
            if fill_d is None or fill_px is None:
                status = "unscorable"
            elif last is not None and last.d >= fill_d:
                status, exit_d = "closed", last.d
                gross = ((last.close - fill_px) / fill_px * _HUNDRED).quantize(_PCT_Q, ROUND_HALF_UP)
                stale_net = gross - cost
            if status != "open":
                _log.warning("signal_outcome_stale_closed", symbol=s.symbol, d=s.d.isoformat(),
                             strategy_id=s.strategy_id, construct=s.construct, status=status,
                             last_bar=None if last is None else last.d.isoformat())
        if status == "void_ca" and fill_d is not None:
            exit_d = min(x for x in ex if x > fill_d)
        # brk20's level is a pre-ex price: an ex-date after the signal and before the fill (or inside
        # a window still unfilled) voids it too.
        pre_end = fill_d or (min(start.until, d) if isinstance(start, Pending) else d)
        pre_fill = [x for x in ex if level is not None and s.d < x <= pre_end]
        if pre_fill:
            status, exit_d = "void_ca", min(pre_fill)
        for k, o in outs.items():
            net = o.net_pct if o.net_pct is not None else stale_net    # an unreached horizon: the last close
            if row[f"net_t{k}"] is None and not pre_fill and net is not None:
                row[f"net_t{k}"] = float(net)
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

    def _walk(
        self, bars: Sequence[SimBar], first: date, seed: SimBar | None, d: date, gaps: _Gaps,
    ) -> tuple[list[SimBar], date | None]:
        """``(bars on consecutive regular sessions from first, the first missing session that a later
        bar proves missing)``. ``add_sessions`` never counts a muhurat session, so its bar is never
        walked; the calendar horizon ends the walk. A missing session that a later bar proves missing is a
        halt once :meth:`_halt` judges it: it walks as a session at the last earlier close (the previous
        walked bar, else ``seed``, the fill session's bar), which no touch can fill. Until then it ends the
        walk (the row stays ``open``) instead of the next bar standing in for that session. The anchor
        (``first`` with no ``seed``) is never a halt: the anchor rule judges it."""
        out: list[SimBar] = []
        want = first
        try:
            for b in bars:
                while b.d > want:
                    prior = out[-1] if out else seed
                    if prior is None or not self._halt(want, d, gaps):
                        return out, want
                    out.append(SimBar(want, prior.close, prior.close, _NO_TOUCH, prior.close))
                    want = self._calendar.add_sessions(want, 1)
                if b.d < want:
                    continue
                out.append(b)
                want = self._calendar.add_sessions(want, 1)
        except ValueError:
            pass
        return out, None

    def _halt(self, s: date, d: date, gaps: _Gaps) -> bool:
        """``s`` (no bar, a later bar for the symbol) is a halt: NIFTY 50 has a bar on it, its day is not failed,
        and it is :data:`STALE_SESSIONS` sessions old (that many NIFTY 50 sessions in ``s..d``)."""
        if not gaps.symbol_missing(s):
            return False
        n, x = 0, s
        while True:
            n += x in gaps.index_days
            if n >= STALE_SESSIONS:
                return True
            try:
                x = self._calendar.add_sessions(x, 1)
            except ValueError:
                return False
            if x > d:
                return False

    def _stale(self, bars: Sequence[SimBar], base: date, d: date, gaps: _Gaps) -> bool:
        """No bar after ``base`` (the last walked bar, else the fill or the anchor) and :data:`STALE_SESSIONS`
        consecutive regular sessions after it, through ``d``, each with a NIFTY 50 bar and none on or after a
        failed ``daily_bars`` day. A later bar is a hole, left to :meth:`_walk`'s halt rule; a session without
        an index bar (a market-wide gap) restarts the count."""
        if any(b.d > base for b in bars):
            return False
        run, s = 0, base
        while run < STALE_SESSIONS:
            try:
                s = self._calendar.add_sessions(s, 1)
            except ValueError:
                return False
            if s > d:
                return False
            run = run + 1 if gaps.symbol_missing(s) else 0
        return True

    def _anchor_lost(self, bars: Sequence[SimBar], first: date) -> bool:
        """A bar beyond ``first`` + 2 sessions: a hole the next boot's daily-gap backfill did not repair."""
        try:
            grace = self._calendar.add_sessions(first, 2)
        except ValueError:
            return False
        return any(b.d > grace for b in bars)

    def _with_bench(self, row: dict[str, Any], ew: Mapping[date, float | None]) -> dict[str, Any]:
        if row["bench_t20"] is not None:
            return row
        bench = bench_pct(ew, date.fromisoformat(row["fill_d"]), date.fromisoformat(row["exit_d"]), "time",
                          self._calendar)
        if bench is None:
            return row
        return {**row, "bench_t20": float(bench), "excess_t20": row["net_t20"] - float(bench)}


def _sizing(table: LimitTable) -> Callable[[Decimal], Decimal]:
    """``stop % -> notional``: the §7.1 swing size at the ``equity_floor_rung``, capped by the per-stock
    and deployed-capital caps. Raises ``ValueError`` on a table that can size no row, so the job fails
    rather than marking the night's rows unscorable."""
    lim = table.limits
    floor = table.capital_base_inr * (1 + Decimal(str(lim.equity_floor_rung.equity_pct_of_base)) / _HUNDRED)
    position_pct = Decimal(str(lim.per_trade_risk.swing_position_pct))
    gap_mult = Decimal(str(lim.per_trade_risk.overnight_gap_mult))
    caps = (lim.per_stock_exposure.cnc_notional_inr, lim.capital_cap.max_deployed_capital_inr)
    if min(floor, position_pct, gap_mult, *caps) <= 0:
        raise ValueError(f"limits cannot size a signal: floor={floor} position_pct={position_pct} "
                         f"gap_mult={gap_mult} caps={caps}")
    return lambda stop_pct: min(
        sizing_notional(floor, position_pct=position_pct, gap_mult=gap_mult, stop_pct=stop_pct), *caps)


def _aliases(renames: Sequence[SymbolRename]) -> Callable[[str], tuple[str, ...]]:
    """``journal symbol -> every name its company's rows may sit under``: the current symbol first, then the
    journal's, then the token's other retired symbols. rekey_symbol moves bars, corporate actions and resting
    levels to the current symbol but may not have run (or been refused), so every name is read."""
    current = {r.old_symbol: r.new_symbol for r in renames}
    retired: dict[str, list[str]] = defaultdict(list)
    for r in renames:
        retired[r.new_symbol].append(r.old_symbol)

    def names(symbol: str) -> tuple[str, ...]:
        now = current.get(symbol, symbol)
        return tuple(dict.fromkeys((now, symbol, *retired.get(now, ()))))

    return names


def _final(row: Mapping[str, Any] | None) -> bool:
    if row is None:
        return False
    return row["status"] in _FINAL or row["status"] == "closed" and (
        row["bench_t20"] is not None or row["fill_d"] < EW_BACKFILL_FROM.isoformat())
