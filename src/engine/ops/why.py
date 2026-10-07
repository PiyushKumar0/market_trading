"""``/why SYMBOL`` (plan Q1.10): a deterministic, read-only account of where one symbol stands.

No LLM, no sweep, nothing enqueued. A section whose read fails or times out says so and the rest of
the reply still goes out. Telegram caps a message at 4096 chars and ``_reply`` does not split, so every
section is truncated and nine capped sections plus the header fit under the cap.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from engine.core.calendar import NSECalendar
from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.scope import scope_sql
from engine.marketdata.store import MarketStore
from engine.strategy.scanners import brk20, hi52

_log = get_logger("engine.ops.why")

READ_TIMEOUT_S = 5.0
SECTION_CAP = 400
_RESULTS_HORIZON_DAYS = 10
_HISTORY_DAYS = 400          # >= hi52's 252 sessions plus weekend/holiday margin
_SYMBOL = re.compile(r"[A-Z0-9&-]{1,20}")


@dataclass(frozen=True)
class _Ctx:
    sym: str
    store: MarketStore
    conn: sqlite3.Connection
    clock: Clock
    calendar: NSECalendar

    async def read(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return await asyncio.wait_for(self.store.arun(fn, *args, **kwargs), READ_TIMEOUT_S)


async def _universe(c: _Ctx) -> str:
    today = c.clock.today()
    tried = []
    for d in (today, c.calendar.previous_trading_day(today)):
        rows = await c.read(c.store.get_universe_daily, d)
        if not rows:
            tried.append(d.isoformat())
            continue
        row = next((r for r in rows if r["symbol"] == c.sym), None)
        if row is None:
            return f"universe: not listed on {d}"
        if row["included"]:
            return f"universe: included on {d}"
        return f"universe: excluded on {d}: {', '.join(row['exclusion_reasons'] or ['no reason recorded'])}"
    return f"universe: no snapshot for {' or '.join(tried)}"


async def _surveillance(c: _Ctx) -> str:
    latest = await c.read(c.store.get_latest_instruments_daily)
    if latest is None:
        return "surveillance: no instrument snapshot"
    d, rows = latest
    row = next((r for r in rows if r["tradingsymbol"] == c.sym), None)
    if row is None:
        return f"surveillance: not in the {d} instrument dump"
    return f"surveillance: {row['surveillance'] or 'none'} ({d})"


async def _results(c: _Ctx) -> str:
    today = c.clock.today()
    rows = await c.read(
        c.store.get_earnings_calendar, today, today + timedelta(days=_RESULTS_HORIZON_DAYS),
        symbol=c.sym,
    )
    if not rows:
        return f"results: none dated in the next {_RESULTS_HORIZON_DAYS} days"
    d = rows[0]["event_date"]
    return "results: TODAY" if d == today else f"results: next on {d}"


async def _levels(c: _Ctx) -> str:
    today = c.clock.today()
    frame = await c.read(
        c.store.get_bars_1d_frame, c.sym, today - timedelta(days=_HISTORY_DAYS), today
    )
    if not len(frame):
        return "levels: no daily bars"
    rows = [
        brk20.DailyRow(high=h, close=cl, volume=v, open=o)
        for h, cl, v, o in zip(
            frame["high"], frame["close"], frame["volume"], frame["open"], strict=True
        )
    ]
    last = rows[-1].close
    lines = [f"levels (daily close {last:.2f} on {frame.index[-1].date()}):"]

    min_sessions = int(hi52.DEFAULT_PARAMS["min_sessions"])
    diag = hi52.diagnostics_for(rows)
    if len(rows) < min_sessions:
        lines.append(f"  hi52: {len(rows)} sessions of history, needs {min_sessions}")
    elif diag is None:
        lines.append("  hi52: not computable")
    else:
        band = float(hi52.DEFAULT_PARAMS["proximity_min"])
        arm = band * diag.high_52wk
        lines.append(
            f"  hi52: already inside the band ({diag.prox:.0%} of the 52w high), no fresh cross"
            if diag.prox >= band else
            f"  hi52: arms on a close >= {arm:.2f} ({arm / last - 1:+.1%}); 52w high {diag.high_52wk:.2f}"
        )

    n = int(brk20.DEFAULT_PARAMS["lookback_days"])
    if len(rows) < n + 2:
        lines.append(f"  brk20: {len(rows)} sessions of history, needs {n + 2}")
    else:
        h = max(r.high for r in rows[-n:])
        lines.append(f"  brk20: needs a close above {h:.2f} ({h / last - 1:+.1%})")
    return "\n".join(lines)


async def _insider(c: _Ctx) -> str:
    now = c.clock.now()
    rows = [
        r for r in await c.read(c.store.get_insider_trades, symbol=c.sym)
        if r["broadcast_dt"] is not None and r["broadcast_dt"] <= now
    ]
    if not rows:
        return "insider: no filings"
    parts = [
        f"{r['txn_to'] or r['txn_from']} {r['txn_type']} {r['person_category'] or '?'} "
        f"{r['qty']} sh Rs{r['value']} (filed {r['broadcast_dt'].date()})"
        for r in rows[-3:][::-1]
    ]
    return "insider (latest first): " + "; ".join(parts)


async def _pledge(c: _Ctx) -> str:
    now = c.clock.now()
    rows = [
        r for r in await c.read(c.store.get_shp_quarterly, symbol=c.sym)
        if r["broadcast_dt"] is not None and r["broadcast_dt"] <= now
    ]
    if not rows:
        return "pledge: no shareholding filings"
    latest = max(r["qtr_end"] for r in rows)
    cur = [r for r in rows if r["qtr_end"] == latest]
    pledged = [f"{r['category']} {r['pledged_pct']:.1f}%" for r in cur if r["pledged_pct"]]
    return (
        f"pledge: quarter {latest} (filed {max(r['broadcast_dt'] for r in cur).date()}): "
        + (", ".join(pledged[:3]) if pledged else "none pledged")
    )


async def _last_rec(c: _Ctx) -> str:
    row = c.conn.execute(
        "SELECT r.payload, r.delivered_at, r.human_action, r.skip_reason, o.status, o.net_pct "
        "FROM recommendations r LEFT JOIN rec_outcomes o ON o.rec_id = r.rec_id "
        "WHERE json_extract(r.payload, '$.instrument') = ? AND r.delivered_at IS NOT NULL "
        "ORDER BY r.delivered_at DESC LIMIT 1",
        (c.sym,),
    ).fetchone()
    if row is None:
        return "last rec: none"
    out = [f"last rec: {row['delivered_at'][:10]} {json.loads(row['payload']).get('kind') or 'entry'}",
           row["human_action"] or "no decision recorded"]
    if row["skip_reason"]:
        out.append(f"skip reason {row['skip_reason']}")
    if row["status"]:
        out.append(f"outcome {row['status']}" + (f" net {row['net_pct']:+.2f}%" if row["net_pct"] is not None else ""))
    return " - ".join(out)


async def _positions(c: _Ctx) -> str:
    rows = c.conn.execute(
        "SELECT side, qty, avg_entry, stop, target, origin FROM positions "
        "WHERE symbol = ? AND is_paper = 0 AND state IN ('OPEN', 'PENDING_EXIT') ORDER BY opened_at",
        (c.sym,),
    ).fetchall()
    if not rows:
        return "positions: none open"
    return "positions: " + "; ".join(
        f"{r['side']} {r['qty']} @ {r['avg_entry']} stop {r['stop']} target {r['target']} ({r['origin']})"
        for r in rows
    )


async def _verdict(c: _Ctx) -> str:
    row = c.conn.execute(
        "SELECT v.verdict, v.payload, v.evaluated_at FROM verdicts v "
        "JOIN proposals p ON p.proposal_id = v.proposal_id "
        f"WHERE json_extract(p.payload, '$.tradingsymbol') = ? AND {scope_sql('real', 'v')} "
        "ORDER BY v.evaluated_at DESC LIMIT 1",
        (c.sym,),
    ).fetchone()
    if row is None:
        return "last verdict: none"
    reasons = json.loads(row["payload"]).get("reasons") or []
    return (f"last verdict: {row['verdict']} on {row['evaluated_at'][:10]}"
            + (f" - reasons: {'; '.join(map(str, reasons))}" if reasons else ""))


_SECTIONS: tuple[tuple[str, Callable[[_Ctx], Coroutine[Any, Any, str]]], ...] = (
    ("universe", _universe), ("surveillance", _surveillance), ("results", _results),
    ("levels", _levels), ("insider", _insider), ("pledge", _pledge),
    ("last rec", _last_rec), ("positions", _positions), ("last verdict", _verdict),
)


async def _section(name: str, fn: Callable[[_Ctx], Coroutine[Any, Any, str]], c: _Ctx) -> str:
    try:
        text = await fn(c)
    except Exception as exc:  # noqa: BLE001 - one dead read must not take the reply down
        _log.warning("why_section_failed", section=name, symbol=c.sym, error=repr(exc))
        return f"{name}: unavailable"
    return text if len(text) <= SECTION_CAP else text[:SECTION_CAP - 3] + "..."


def make_why_fn(
    *, store: MarketStore, conn: sqlite3.Connection, clock: Clock, calendar: NSECalendar
) -> Callable[[str], Awaitable[str]]:
    async def why(raw_symbol: str) -> str:
        sym = raw_symbol.strip().upper()
        if not _SYMBOL.fullmatch(sym):
            return "usage: /why <symbol>"
        c = _Ctx(sym, store, conn, clock, calendar)
        body = await asyncio.gather(*(_section(name, fn, c) for name, fn in _SECTIONS))
        return "\n".join([f"why {sym} - {clock.now():%Y-%m-%d %H:%M} IST", *body])

    return why
