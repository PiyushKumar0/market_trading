"""Weekly owner summary of recommendation outcomes (plan Q2.6).

HINDSIGHT: every figure is read from ``rec_outcomes``; the owner sees it, no decision code reads it.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from engine.core.clock import IST
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.jobs import JOB_REC_OUTCOMES
from engine.ops.scorecard import scorecard

_ENTRY = "COALESCE(json_extract(r.payload, '$.kind'), 'entry') = 'entry'"
_WEEK_RECS = (
    "SELECT r.delivered_at, r.human_action, r.skip_reason FROM recommendations r "
    f"WHERE r.delivered_at IS NOT NULL AND r.delivered_at >= ? AND {_ENTRY}"
)
# Rupees at each rec's own notional.
_SKIPPED_CLOSED = (
    "SELECT COUNT(*), COALESCE(SUM(o.net_pct / 100.0 * CAST(json_extract(r.payload, '$.notional') AS REAL)), 0) "
    "FROM recommendations r JOIN rec_outcomes o ON o.rec_id = r.rec_id "
    f"WHERE r.skip_reason IS NOT NULL AND o.status = 'closed' AND o.net_pct IS NOT NULL AND {_ENTRY} "
    "AND json_extract(r.payload, '$.notional') IS NOT NULL"
)


@dataclass(frozen=True)
class WeeklySummaryResult:
    d: date
    unfinished: bool = False


class WeeklySummaryJob:
    def __init__(
        self, conn: sqlite3.Connection, notify: Callable[[CatalogMessage], Awaitable[None]],
        was_run: Callable[[str, date], bool],
    ) -> None:
        self._conn = conn
        self._notify = notify
        self._was_run = was_run

    async def run(self, d: date) -> WeeklySummaryResult:
        """``unfinished`` (replayed later) only while ``rec_outcomes`` for ``d`` is unrecorded or failed."""
        if not self._was_run(JOB_REC_OUTCOMES, d):
            return WeeklySummaryResult(d, unfinished=True)
        start = d - timedelta(days=d.weekday())
        # The SQL bound is a day looser than the IST week: delivered_at carries any offset.
        week = [
            r for r in self._conn.execute(_WEEK_RECS, ((start - timedelta(days=1)).isoformat(),))
            if start <= datetime.fromisoformat(r[0]).astimezone(IST).date() <= d
        ]
        skipped = Counter(r[2] for r in week if r[2])
        n_closed, rupees = self._conn.execute(_SKIPPED_CLOSED).fetchone()
        card = scorecard(self._conn)["strategies"]
        strategies = [
            (sid, s["recs"]["closed"], s["recs"]["mean_net"], s["recs"]["mean_excess"])
            for sid, s in card.items() if s["recs"]["closed"]
        ]
        message = catalog.weekly_summary(
            d=d, delivered=len(week), taken=sum(r[1] in ("taken", "closed") for r in week),
            skipped=dict(skipped), expired=sum(r[1] == "expired" and not r[2] for r in week),
            skipped_closed=n_closed, skipped_rupees=rupees, strategies=strategies,
        )
        paper = [s["paper"] for s in card.values()]
        closed, open_n = sum(p["closed"] for p in paper), sum(p["open"] for p in paper)
        if closed or open_n:
            wins = sum(round(p["hit_rate"] * p["closed"]) for p in paper if p["closed"])
            net = sum(p["net"] or 0 for p in paper)
            line = (f"Paper, current epoch, simulated fills: {closed} closed"
                    + (f", hit {wins / closed:.0%}, net {net:+,.0f} rupees" if closed else "")
                    + f", {open_n} open.")
            message = message.model_copy(update={
                "body": f"{message.body}\n{line}",
                "data": {**message.data, "paper": {"closed": closed, "net": net, "open": open_n}},
            })
        await self._notify(message)
        return WeeklySummaryResult(d)
