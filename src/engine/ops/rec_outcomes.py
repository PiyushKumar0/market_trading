"""Hindsight scoring of delivered entry recs on official bars (plan Q2.3).

HINDSIGHT: no scanner, threshold or promotion code may read ``rec_outcomes``. The owner's action
and skip reason are joined at read time, never copied here. Writes only ``rec_outcomes`` and
``universe_ew_returns``.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.db import transaction
from engine.core.log import get_logger
from engine.learning.benchmark import BENCH_INTRASESSION, BENCH_TIME, bench_pct, compute_ew_return
from engine.learning.exit_sim import Outcome, Pending, SimBar, simulate
from engine.marketdata.store import MarketStore
from engine.ops.holds import HoldFn, session_of
from engine.ops.jobs import JOB_BHAVCOPY, JOB_DAILY_BARS
from engine.strategy.cost_model import CostModel
from engine.strategy.scanners.hi52 import UNADJUSTED_KINDS

_log = get_logger("engine.ops.rec_outcomes")

EW_BACKFILL_FROM = date(2026, 8, 26)
FIXED_HORIZONS = (5, 10, 20)

_FINAL = frozenset({"unfilled", "void_ca", "unscorable"})
_STATUS = {"exit": "closed", "void_ca": "void_ca", "open": "open", "unfilled": "unfilled"}
_BASIS = {"1m": "1m_post_delivery", "daily": "daily"}
_COLUMNS = (
    "rec_id", "strategy_id", "entry_type", "fill_basis", "status", "fill_d", "fill_px", "exit_d",
    "exit_px", "exit_reason", "gross_pct", "cost_pct", "net_pct", "net_t5", "net_t10", "net_t20",
    "bench_pct", "excess_pct", "excess_t20", "updated_at",
)
_UPSERT = (
    f"INSERT OR REPLACE INTO rec_outcomes ({', '.join(_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in _COLUMNS)})"
)

#: ``(job_id, d) -> resolved`` — ``CatchUpRunner.was_run``.
WasRunFn = Callable[[str, date], bool]


@dataclass(frozen=True)
class RecOutcomesResult:
    d: date
    unfinished: bool = False
    scored: int = 0


class _Unscorable(Exception):
    pass


class RecOutcomesJob:
    def __init__(
        self, conn: sqlite3.Connection, store: MarketStore, calendar: NSECalendar,
        cost_model: CostModel, hold_fn: HoldFn, clock: Clock, was_run: WasRunFn,
    ) -> None:
        self._conn = conn
        self._store = store
        self._calendar = calendar
        self._costs = cost_model
        self._hold_fn = hold_fn
        self._clock = clock
        self._was_run = was_run

    async def run(self, d: date) -> RecOutcomesResult:
        """Score every non-final entry rec delivered by ``d``, walking bars through ``d`` only.

        ``unfinished`` (no watermark, replayed later) while ``daily_bars`` for ``d`` is unrecorded
        or failed; that job's own give-up bounds the wait."""
        if not self._was_run(JOB_DAILY_BARS, d):
            _log.info("rec_outcomes_unfinished", d=d.isoformat(), waiting_for=JOB_DAILY_BARS)
            return RecOutcomesResult(d, unfinished=True)
        ew = await self._ew_returns(d)
        rows = []
        for rec_id, data, delivered_at in self._open_entry_recs():
            row = await self._score(rec_id, data, delivered_at, d, ew)
            if row is not None:
                rows.append(row)
        if rows:
            with transaction(self._conn):
                self._conn.executemany(_UPSERT, [tuple(r[c] for c in _COLUMNS) for r in rows])
        _log.info("rec_outcomes_scored", d=d.isoformat(), label="hindsight", scored=len(rows),
                  statuses=dict(Counter(r["status"] for r in rows)))
        return RecOutcomesResult(d, scored=len(rows))

    # ------------------------------------------------------------------ benchmark
    async def _ew_returns(self, d: date) -> dict[date, float | None]:
        """``universe_ew_returns`` from :data:`EW_BACKFILL_FROM` through ``d``; a missing or NULL day
        is (re)computed once its ``bhavcopy`` is resolved."""
        have: dict[date, float | None] = {
            date.fromisoformat(r[0]): r[1]
            for r in self._conn.execute("SELECT d, ret FROM universe_ew_returns")
        }
        todo = [
            x for x in _regular_sessions(self._calendar, EW_BACKFILL_FROM, d)
            if have.get(x) is None and self._was_run(JOB_BHAVCOPY, x)
        ]
        if todo:
            computed = await self._store.arun(self._compute_ew, todo)
            with transaction(self._conn):
                self._conn.executemany(
                    "INSERT OR REPLACE INTO universe_ew_returns (d, ret, n) VALUES (?, ?, ?)",
                    [(x.isoformat(), ret, n) for x, (ret, n) in computed.items()],
                )
            have.update({x: ret for x, (ret, _) in computed.items()})
        return have

    def _compute_ew(self, days: list[date]) -> dict[date, tuple[float | None, int]]:
        out: dict[date, tuple[float | None, int]] = {}
        for x in days:
            try:
                res = compute_ew_return(self._store, self._calendar, x)
            except ValueError as exc:
                _log.warning("ew_return_unavailable", d=x.isoformat(), error=str(exc))
                res = None
            out[x] = res if res is not None else (None, 0)
        return out

    # ------------------------------------------------------------------ recs
    def _open_entry_recs(self) -> list[tuple[str, dict[str, Any], str]]:
        rows = self._conn.execute(
            "SELECT r.rec_id, r.payload, r.delivered_at, o.status, o.net_t20, o.bench_pct, "
            "o.excess_t20 FROM recommendations r LEFT JOIN rec_outcomes o ON o.rec_id = r.rec_id "
            "WHERE r.delivered_at IS NOT NULL ORDER BY r.delivered_at"
        ).fetchall()
        out = []
        for rec_id, payload, delivered_at, status, net_t20, bench, excess_t20 in rows:
            if status in _FINAL or (
                status == "closed" and None not in (net_t20, bench, excess_t20)
            ):
                continue
            try:
                data = json.loads(payload or "{}")
            except (TypeError, ValueError):
                _log.warning("rec_outcome_payload_unreadable", rec_id=rec_id)
                continue
            if (data.get("kind") or "entry") == "entry":
                out.append((str(rec_id), data, str(delivered_at)))
        return out

    async def _score(
        self, rec_id: str, data: dict[str, Any], delivered_at: str, d: date,
        ew: Mapping[date, float | None],
    ) -> dict[str, Any] | None:
        try:
            delivered = datetime.fromisoformat(delivered_at).astimezone(IST)
            s = session_of(self._calendar, delivered)
        except ValueError as exc:
            _log.warning("rec_outcome_undated", rec_id=rec_id, error=str(exc))
            return None
        if s > d:
            return None
        ledger = self._conn.execute(
            "SELECT strategy_id, proposal_id FROM learning_ledger WHERE rec_id=? "
            "ORDER BY created_at LIMIT 1", (rec_id,),
        ).fetchone()
        strategy_id = ledger[0] if ledger is not None else None
        row: dict[str, Any] = dict.fromkeys(_COLUMNS)
        row.update(rec_id=rec_id, strategy_id=strategy_id, status="unscorable",
                   updated_at=self._clock.now().isoformat())
        try:
            row["entry_type"] = self._entry_type(data, ledger)
            if not strategy_id:
                raise _Unscorable("no learning_ledger strategy_id")
            pending, stop, target, cost = self._levels(data, row["entry_type"], s)
            symbol, style = str(data["instrument"]), str(data["style"])
        except (_Unscorable, ArithmeticError, LookupError, TypeError, ValueError) as exc:
            _log.warning("rec_outcome_unscorable", rec_id=rec_id, reason=str(exc))
            return row
        hold = self._hold_fn(strategy_id, style)
        n = hold if hold is not None else 1   # intraday: squared off on the fill session

        session = self._calendar.session(s)
        one_m, daily, corp = await self._store.arun(
            self._load, symbol, max(delivered, session.open), session.close, s, d,  # type: ignore[union-attr]
        )
        bars = [SimBar(b.ts_minute, b.open, b.high, b.low, b.close) for b in one_m]
        bars += [SimBar(b.d, b.open, b.high, b.low, b.close) for b in daily]
        common = dict(end=d, cost_pct=cost, add_sessions=self._calendar.add_sessions,
                      ex_dates=[r["ex_date"] for r in corp if r["kind"] in UNADJUSTED_KINDS])
        main = simulate(bars, pending, stop=stop, target=target, horizon=n - 1, **common)
        fixed = {k: simulate(bars, pending, stop=None, target=None, horizon=k - 1, **common)
                 for k in FIXED_HORIZONS}

        row.update(
            status=_STATUS[main.status], fill_basis=_BASIS.get(main.fill_basis or ""),
            fill_d=_iso(main.fill_d), fill_px=_str(main.fill_px), exit_d=_iso(main.exit_d),
            exit_px=_str(main.exit_px), exit_reason=main.reason, gross_pct=_f(main.gross_pct),
            cost_pct=float(cost), net_pct=_f(main.net_pct),
            **{f"net_t{k}": _f(o.net_pct) for k, o in fixed.items()},
        )
        bench = self._bench(ew, main)
        t20 = fixed[20]
        bench20 = self._bench(ew, t20)
        row.update(
            bench_pct=_f(bench), excess_pct=_f(_minus(main.net_pct, bench)),
            excess_t20=_f(_minus(t20.net_pct, bench20)),
        )
        pending_k = self._pending_horizons(main.fill_d, (n, *FIXED_HORIZONS)) \
            if main.status == "open" else []
        if pending_k:
            _log.info("rec_outcome_horizon_pending", rec_id=rec_id, sessions=pending_k)
        _log.info("rec_outcome", rec_id=rec_id, label="hindsight", status=row["status"],
                  net_pct=row["net_pct"], excess_pct=row["excess_pct"],
                  bench=None if main.reason is None else (
                      BENCH_TIME if main.reason == "time" else BENCH_INTRASESSION))
        return row

    def _entry_type(self, data: Mapping[str, Any], ledger: sqlite3.Row | None) -> str:
        """The payload's, else the proposal's, else a degenerate zone means LIMIT."""
        if data.get("entry_type"):
            return str(data["entry_type"])
        proposal_id = (ledger[1] if ledger is not None else None) or data.get("proposal_id")
        if proposal_id:
            prop = self._conn.execute(
                "SELECT payload FROM proposals WHERE proposal_id=?", (proposal_id,)
            ).fetchone()
            if prop is not None:
                entry_type = json.loads(prop[0] or "{}").get("entry_type")
                if entry_type:
                    return str(entry_type)
        lo, hi = data["entry_zone"]
        return "LIMIT" if Decimal(str(lo)) == Decimal(str(hi)) else "MARKET"

    def _levels(
        self, data: Mapping[str, Any], entry_type: str, s: date,
    ) -> tuple[Pending, Decimal, Decimal | None, Decimal]:
        if data.get("side") != "BUY":
            raise _Unscorable(f"side {data.get('side')!r}: the simulator is long-only")
        if entry_type == "LIMIT":
            pending = Pending("trade_through", Decimal(str(data["entry_zone"][0])), s, s)
        else:
            pending = Pending("market", None, s, s)
        targets = data.get("targets") or []
        cost = self._costs.breakeven_pct(Decimal(str(data["notional"])), str(data["product"]))
        return (pending, Decimal(str(data["stop"])),
                Decimal(str(targets[0])) if targets else None, cost)

    def _load(self, symbol: str, start: datetime, close: datetime, s: date, d: date):
        """1m bars of the delivery session from ``start``, then daily bars; the delivery session's
        daily bar only when it has no 1m bars."""
        one_m = self._store.get_bars_1m(symbol, start, close)
        daily = self._store.get_bars_1d(symbol, s + timedelta(days=1) if one_m else s, d)
        return one_m, daily, self._store.get_corp_actions(symbol=symbol, ex_from=s)

    def _bench(self, ew: Mapping[date, float | None], o: Outcome) -> Decimal | None:
        if o.status != "exit":
            return None
        return bench_pct(ew, o.fill_d, o.exit_d, o.reason, self._calendar)  # type: ignore[arg-type]

    def _pending_horizons(self, fill_d: date | None, ks: tuple[int, ...]) -> list[int]:
        if fill_d is None:
            return []
        pending = set()
        for k in ks:
            try:
                self._calendar.add_sessions(fill_d, k - 1)
            except ValueError:
                pending.add(k)
        return sorted(pending)


def _regular_sessions(calendar: NSECalendar, start: date, end: date) -> list[date]:
    out = []
    x = start
    while x <= end:
        session = calendar.session(x)
        if session is not None and not session.is_muhurat:
            out.append(x)
        x += timedelta(days=1)
    return out


def _minus(a: Decimal | None, b: Decimal | None) -> Decimal | None:
    return None if a is None or b is None else a - b


def _f(x: Decimal | None) -> float | None:
    return None if x is None else float(x)


def _str(x: Decimal | None) -> str | None:
    return None if x is None else str(x)


def _iso(x: date | None) -> str | None:
    return None if x is None else x.isoformat()
