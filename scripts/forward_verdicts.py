#!/usr/bin/env python
"""PRE-REGISTERED FORWARD VERDICTS: the brk20 and ins kill rules and the analyst-value verdict (review §9
#2/#5). Read-only: state.db through a ``file:...?mode=ro`` URI. Exit code is ALWAYS 0; a store that cannot
be read prints ``UNAVAILABLE`` as the verdict.

THE RULE (pre-registered; do not re-derive it after seeing the numbers)

**Analyst-value verdict (registered <T19 date>, population from R).**
- **Population:** `prescreen_day_slots` rows of brk20, hi52 and ins with `d >= R` and a recorded first
  `analyst_decision`, whose call carries the strategy's registered key `(prompt_version, model)`. The registered
  key is the one on that strategy's first decision on or after R. Rows with another key are excluded and counted.
  A change to the analyst prompt, a population contract or the model after R therefore stops the population from
  growing. Accrual resumes only through an owner-approved re-registration with a new R.
- **Outcome:** `signal_outcomes` `next_open`:
  - entry at the open of the first regular session after d;
  - exit at the close of the 20th regular session, counting the entry session;
  - net of one CNC round trip at the §7.1 notional at the floor-rung equity, fixed at the row's first scoring.
- **Cells:** a (d, strategy) cell exists once at least one enter and at least one no_action were decided. Cells are
  ordered by (d, strategy_id). A cell is final when all its decided rows are final. Its value is the mean net of
  its closed enters minus the mean net of its closed no_actions. A cell with an arm and no closed outcome is
  reported and not averaged.
- **Looks:** look 1 is the first 30 cells, formed when all 30 are final. Look 2 is the first 60 and supersedes it.
- **Decision:** a formed look's mean ≤ 0 ⇒ **ADVISORY**; > 0 ⇒ **KEEP**. Below that, INSUFFICIENT.
- **Tool:** `scripts/forward_verdicts.py --rule analyst`. Interim readings decide nothing.
- **Diagnostics:** T+5/T+10, per-strategy cells, and confidence buckets.
- **Duration:** at least three months, since brk20 originates only in regime-ON sessions.

**brk20 kill rule (registered <date>, population from R).**
- **Population:** the brk20 crossings admitted on or after R (`brk20_resting_levels` rows), under C2.
- **Construct** (`signal_outcomes` `rule`):
  - entry: a limit resting at the broken level on `signal_d` (y+1, the session after the crossing session y)
    through `signal_d` + 4 sessions (V2-5);
  - fill: on the first regular daily bar whose low touches the level, at min(open, level). Touch fills are
    optimistic, as in C2;
  - exit: at the close of the 20th regular session, counting the fill session (C2's arm (ii); V2-5 itself
    measured close(fill+20)). No stop, no target;
  - voids: a structural ex-date between `signal_d` and the exit voids the crossing;
  - cost: one CNC round trip at the §7.1 notional implied by the signal's own stop at the floor-rung equity,
    fixed at the row's first scoring.
- **Looks:** look 1 is the first 20 filled crossings by (fill date, symbol), formed when all 20 are final. Look 2
  is the first 40 and supersedes it.
- **Verdict:** DEMOTE iff the look's median net over its closed members ≤ 0.
- **Scope:** this rule judges the signal's drift. The card's stop and target are D14/C5's question.
- **Demotion:** add brk20 to `ops.main.NO_EDGE_SHADOW_STRATEGIES` in an owner-visible commit.
- **Diagnostic only:** the C2 blocked arm (`rule_blocked`) against the admitted arm.
- **Tool:** `scripts/forward_verdicts.py --rule brk20`.

**ins kill rule (registered <date>, population from R).**
- **Looks and verdict:** as brk20.
- **Construct:** entry at the open of the publication session d, which is the validated next-session open after
  the disclosure. Exit at the close of the 20th regular session. No stop. Cost at the §7.1 notional for the 5%
  stop, fixed at first scoring.
- **Demotion:** add ins to `NO_EDGE_SHADOW_STRATEGIES`, which wins over its registered edge (`gate.py:1046-1049`).
- **Tool:** `scripts/forward_verdicts.py --rule ins`.

R is ``forward_verdicts.REGISTERED_FROM``; while it is None every row is read and the verdict is
UNREGISTERED. Rows holding an unformed look are named.

Usage:
    python scripts/forward_verdicts.py --rule brk20|ins|analyst [--db PATH] [--as-of YYYY-MM-DD] [--json PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning import forward_verdicts as fv  # noqa: E402

UNAVAILABLE = "UNAVAILABLE"
_INTERIM = "interim, decides nothing"


def _f(v: float | None, unit: str = "") -> str:
    return "n/a" if v is None else f"{v:+.2f}{unit}"


def _pct(v: float | None) -> str:
    return "n/a" if v is None else f"{v:.0%}"


def _stats(s: fv.Stats, unit: str = "") -> str:
    return f"n={s.n} median {_f(s.median, unit)} mean {_f(s.mean, unit)} hit {_pct(s.hit_rate)}"


def _t20(cal: NSECalendar, fill_d: date, as_of: date) -> str:
    """The T+20 session (the fill session counts as 1) and the regular sessions since it."""
    try:
        t20 = cal.add_sessions(fill_d, 19)
    except ValueError:
        return "T+20 beyond the calendar"
    if t20 > as_of:
        return f"T+20 {t20}, not reached"
    n = 0
    try:
        while cal.add_sessions(t20, n + 1) <= as_of:
            n += 1
    except ValueError:
        pass
    return f"T+20 {t20}, {n} sessions past"


def _head(rule: str, rd: fv.KillReading | fv.AnalystReading) -> list[str]:
    return [f"VERDICT {rule}: {rd.verdict} ({rd.reason})",
            f"registered from {rd.registered_from or 'none'}; as of {rd.as_of}"]


def _holders(rd: fv.KillReading | fv.AnalystReading, cal: NSECalendar) -> list[str]:
    if rd.holding_look is None:
        return []
    return [f"holding look {rd.holding_look}: {len(rd.holders)} rows", *(
        f"    {r.strategy_id:<6} {r.symbol:<12} d={r.d} {r.status:<8} fill={r.fill_d or '-'}"
        + (f"  {_t20(cal, r.fill_d, rd.as_of)}" if r.fill_d else "") for r in rd.holders)]


def _statuses(counts: dict[str, int]) -> str:
    return "".join(f", {k} {v}" for k, v in sorted(counts.items()))


def render_kill(rd: fv.KillReading, cal: NSECalendar) -> list[str]:
    out = [*_head(rd.rule, rd), f"population: {rd.n_rows} rows{_statuses(rd.statuses)}"]
    for i, lk in enumerate(rd.looks, 1):
        out.append(f"look {i}: {lk.n_closed + lk.n_void} of {lk.size} final, {'FORMED' if lk.formed else _INTERIM}; "
                   f"closed {lk.n_closed}, void {lk.n_void}; median net T+20 {_f(lk.median_net_t20, '%')} "
                   f"mean {_f(lk.mean_net_t20, '%')} hit {_pct(lk.hit_rate)} "
                   f"median excess {_f(lk.median_excess_t20, '%')}")
    fill_rate = rd.n_filled / rd.n_rows if rd.n_rows else None
    out += [*_holders(rd, cal), "diagnostics (every row, decide nothing):",
            f"    fill rate: {rd.n_filled} of {rd.n_rows} = {_pct(fill_rate)}",
            f"    net T+5 {_stats(rd.net_t5, '%')}", f"    net T+10 {_stats(rd.net_t10, '%')}",
            f"    net T+20 {_stats(rd.net_t20, '%')}", f"    excess T+20 {_stats(rd.excess_t20, '%')}"]
    if rd.c2 is not None:
        out += [f"    C2 admitted net T+20 {_stats(rd.c2.admitted, '%')}",
                f"    C2 blocked (down) net T+20 {_stats(rd.c2.blocked, '%')}",
                f"    C2 blocked for another reason, in neither arm: {rd.c2.neither}"]
    return out


def render_analyst(rd: fv.AnalystReading, cal: NSECalendar) -> list[str]:
    keys = ", ".join(f"{s} {k[0]}/{k[1]}" for s, k in sorted(rd.keys.items())) or "none"
    out = [*_head("analyst", rd), f"registered keys: {keys}",
           f"population: {rd.n_kept} decided rows{_statuses(rd.statuses)}"]
    for i, lk in enumerate(rd.looks, 1):
        out.append(f"look {i}: {lk.n_final} of {lk.size} final, {'FORMED' if lk.formed else _INTERIM}; "
                   f"{lk.n_cells} cells, valued {lk.n_valued}; mean enter - no_action {_f(lk.mean_diff, 'pp')}")
    out += [*_holders(rd, cal),
            f"unvalued final cells (an arm with no closed outcome): {len(rd.unvalued)}",
            *(f"    {c.d} {c.strategy_id}" for c in rd.unvalued),
            f"excluded (another key, or after the freeze): {sum(e.n for e in rd.excluded)}",
            *(f"    {e.strategy_id} {e.prompt_version}/{e.model} {e.n}" for e in rd.excluded),
            "diagnostics (decide nothing):",
            f"    T+5 cell difference {_stats(rd.diff_t5, 'pp')}",
            f"    T+10 cell difference {_stats(rd.diff_t10, 'pp')}",
            *(f"    {s} T+20 cell difference {_stats(st, 'pp')}" for s, st in rd.per_strategy.items()),
            *(f"    confidence {b.label} {b.decision}: net T+20 {_stats(b.stats, '%')}" for b in rd.confidence)]
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pre-registered forward verdicts (read-only; always exits 0).")
    ap.add_argument("--rule", required=True, choices=(*fv.KILL_RULES, "analyst"))
    ap.add_argument("--db", type=Path, default=repo_root() / "data" / "state.db")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="read outcomes as known on this day (default: today IST). The verdict figures are "
                         "reconstructed exactly; diagnostics (T+5/T+10, excess) may include scoring done after it")
    ap.add_argument("--json", type=Path, default=None, metavar="PATH", help="also write the reading as JSON")
    args = ap.parse_args(argv)
    as_of = args.as_of or datetime.now(IST).date()
    r_from = fv.REGISTERED_FROM
    reading: fv.KillReading | fv.AnalystReading
    try:
        conn = sqlite3.connect(f"file:{args.db.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0)
        try:
            if args.rule == "analyst":
                reading = fv.analyst_reading(conn, as_of=as_of, registered_from=r_from)
            else:
                reading = fv.kill_reading(conn, args.rule, as_of=as_of, registered_from=r_from)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        print(f"VERDICT {args.rule}: {UNAVAILABLE} ({type(exc).__name__}: {exc})")
        return 0
    cal = NSECalendar(config_dir() / "calendar", Clock(), include_special=False)
    lines = render_analyst(reading, cal) if isinstance(reading, fv.AnalystReading) else render_kill(reading, cal)
    print("\n".join(lines))
    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(asdict(reading), indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
