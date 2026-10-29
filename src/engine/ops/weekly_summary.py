"""Weekly owner summary of recommendation outcomes (plan Q2.6).

HINDSIGHT: every figure is read from ``rec_outcomes`` and ``signal_outcomes``; the owner sees it, no
decision code reads it.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from engine.core.clock import IST
from engine.core.log import get_logger
from engine.learning import forward_verdicts as fv
from engine.notify import catalog
from engine.notify.catalog import CatalogMessage
from engine.ops.jobs import JOB_REC_OUTCOMES, JOB_SIGNAL_OUTCOMES
from engine.ops.scorecard import scorecard_ro

_log = get_logger("engine.ops.weekly_summary")

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
_LATEST_SCORING = (
    "SELECT run_for_date, status FROM job_runs WHERE job_id = ? AND run_for_date <= ? "
    "ORDER BY run_for_date DESC LIMIT 1"
)


def analyst_line(signals: dict[str, Any]) -> str | None:
    """The analyst-value verdict of the binding (largest formed) look, else look 1's interim progress;
    None before registration."""
    if "error" in signals:
        return f"Analyst reading unavailable: {signals['error']}."
    analyst = signals["analyst"]
    if analyst["verdict"] == fv.UNREGISTERED:
        return None
    formed = [(i, lk) for i, lk in enumerate(analyst["looks"], 1) if lk["formed"]]
    if not formed:
        lk = analyst["looks"][0]
        return (f"Analyst (pre-registered {signals['window']}): look 1 formed {lk['n_final']} of {lk['size']} "
                "cells — interim, decides nothing.")
    i, lk = formed[-1]
    diff = "n/a" if lk["mean_diff"] is None else f"{lk['mean_diff']:+.1f} pp"
    return (f"Analyst verdict (look {i}, {lk['size']} cells): {analyst['verdict']} — mean approved − declined "
            f"at T+20 {diff}.")


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
        """``unfinished`` (replayed later) while ``rec_outcomes`` for ``d`` is unrecorded or failed, or,
        once registered, while ``signal_outcomes`` for ``d`` is still due. The analyst reading is as known
        on ``d``, also on a replay."""
        if not self._was_run(JOB_REC_OUTCOMES, d):
            return WeeklySummaryResult(d, unfinished=True)
        scored = True if fv.REGISTERED_FROM is None else self._signals_scored(d)
        if scored is None:
            return WeeklySummaryResult(d, unfinished=True)
        start = d - timedelta(days=d.weekday())
        # The SQL bound is a day looser than the IST week: delivered_at carries any offset.
        week = [
            r for r in self._conn.execute(_WEEK_RECS, ((start - timedelta(days=1)).isoformat(),))
            if start <= datetime.fromisoformat(r[0]).astimezone(IST).date() <= d
        ]
        skipped = Counter(r[2] for r in week if r[2])
        n_closed, rupees = self._conn.execute(_SKIPPED_CLOSED).fetchone()
        try:
            sc = await scorecard_ro(self._conn, as_of=d)
            paper = [s["paper"] for s in sc["strategies"].values()]
        except Exception:
            _log.exception("weekly_summary_paper_failed")
            sc = await scorecard_ro(self._conn, as_of=d, with_paper=False)
            paper = []
        strategies = [
            (sid, s["recs"]["closed"], s["recs"]["mean_net"], s["recs"]["mean_excess"])
            for sid, s in sc["strategies"].items() if s["recs"]["closed"]
        ]
        message = catalog.weekly_summary(
            d=d, delivered=len(week), taken=sum(r[1] in ("taken", "closed") for r in week),
            skipped=dict(skipped), expired=sum(r[1] == "expired" and not r[2] for r in week),
            skipped_closed=n_closed, skipped_rupees=rupees, strategies=strategies,
            analyst_line=analyst_line(sc["signals"]) if scored
            else f"Analyst reading unavailable: signal scoring did not complete for {d.isoformat()}.",
        )
        closed, open_n = sum(p["closed"] for p in paper), sum(p["open"] for p in paper)
        if closed or open_n:
            wins = sum(round(p["hit_rate"] * p["closed"]) for p in paper if p["closed"])
            net = sum(p["net"] or 0 for p in paper)
            line = (f"Paper, all epochs, simulated fills: {closed} closed"
                    + (f", hit {wins / closed:.0%}, net {net:+,.0f} rupees" if closed else "")
                    + f", {open_n} open.")
            message = message.model_copy(update={
                "body": f"{message.body}\n{line}",
                "data": {**message.data, "paper": {"closed": closed, "net": net, "open": open_n}},
            })
        await self._notify(message)
        return WeeklySummaryResult(d)

    def _signals_scored(self, d: date) -> bool | None:
        """True once ``signal_outcomes`` succeeded for ``d``; None (wait) while its run for ``d`` is
        unrecorded and it has run for an earlier day; False when it failed or gave up on ``d``, or never
        scanned ``d`` (deployed after it), so the digest never waits on a run that may not come."""
        row = self._conn.execute(_LATEST_SCORING, (JOB_SIGNAL_OUTCOMES, d.isoformat())).fetchone()
        if row is None:
            return False
        if row[0] != d.isoformat():
            return None
        return row[1] == "success"
