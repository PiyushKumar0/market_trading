#!/usr/bin/env python
"""PRE-REGISTERED FORWARD VERDICTS: the brk20 and ins kill rules and the analyst-value verdict (review §9
#2/#5). Read-only: state.db through a ``file:...?mode=ro`` URI. Exit code is always 0.

THE RULE (pre-registered; do not re-derive it after seeing the numbers)

brk20 kill rule (population from R).
* Population: the brk20 crossings admitted on or after R (`brk20_resting_levels` rows), under C2.
* Looks: look 1 is the first 20 filled crossings by (fill date, symbol), formed when all 20 are final. Look 2
  is the first 40 and supersedes it.
* Verdict: DEMOTE iff the look's median net over its closed members ≤ 0.
* Diagnostic only: the C2 blocked arm (`rule_blocked`) against the admitted arm.

ins kill rule (population from R).
* Looks and verdict: as brk20.
* Construct: entry at the open of the publication session d, which is the validated next-session open after
  the disclosure. Exit at the close of the 20th regular session. No stop. Cost at the §7.1 notional for the 5%
  stop, fixed at first scoring.

Analyst-value verdict (population from R).
* Population: `prescreen_day_slots` rows of brk20, hi52 and ins with `d >= R` and a recorded first
  `analyst_decision`, whose call carries the strategy's registered key `(prompt_version, model)`. The registered
  key is the one on that strategy's first decision on or after R. Rows with another key are excluded and counted.
* Outcome: `signal_outcomes` `next_open`.
* Cells: a (d, strategy) cell exists once at least one enter and at least one no_action were decided. Cells are
  ordered by (d, strategy_id). A cell is final when all its decided rows are final. Its value is the mean net of
  its closed enters minus the mean net of its closed no_actions. A cell with an arm and no closed outcome is
  reported and not averaged.
* Looks: look 1 is the first 30 cells, formed when all 30 are final. Look 2 is the first 60 and supersedes it.
* Decision: a formed look's mean ≤ 0 ⇒ ADVISORY; > 0 ⇒ KEEP. Below that, INSUFFICIENT.

Interim readings decide nothing. Without a registered R (``forward_verdicts.REGISTERED_FROM``) every row is
read and the verdict is UNREGISTERED. Rows holding an unformed look are named.

Usage:
    python scripts/forward_verdicts.py --rule brk20|ins|analyst [--state-db PATH] [--as-of YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, repo_root  # noqa: E402
from engine.learning import forward_verdicts as fv  # noqa: E402

ANALYST_STRATEGIES = ("brk20", "hi52", "ins")


def _f(v: float | None, unit: str = "") -> str:
    return "n/a" if v is None else f"{v:+.2f}{unit}"


def _stats(s: fv.Stats) -> str:
    hit = "n/a" if s.hit_rate is None else f"{s.hit_rate:.0%}"
    return f"n={s.n} median {_f(s.median)} mean {_f(s.mean)} hit {hit}"


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


def _holders(rows: Sequence[fv.SignalRow], cal: NSECalendar, as_of: date) -> list[str]:
    return [f"    {r.strategy_id:<6} {r.symbol:<12} d={r.d} {r.status:<8} fill={r.fill_d or '-'}"
            + (f"  {_t20(cal, r.fill_d, as_of)}" if r.fill_d else "") for r in rows]


def _population(rows: Sequence[fv.SignalRow]) -> str:
    counts = sorted(Counter(r.status for r in rows).items())
    filled_open = sum(1 for r in rows if r.status == "open" and r.fill_d)
    return (f"population: {len(rows)} rows" + "".join(f", {k} {v}" for k, v in counts)
            + f"; open and filled {filled_open}")


def kill_report(conn: sqlite3.Connection, rule: str, start: date, as_of: date, r_from: date | None,
                cal: NSECalendar) -> list[str]:
    rows = fv.load_rows(conn, strategies=(rule,), construct="rule", start=start, as_of=as_of)
    looks = fv.kill_looks(rows)
    label, reason = fv.kill_verdict(looks, registered_from=r_from)
    out = [f"VERDICT {rule}: {label} ({reason})", f"registered from {r_from or 'none'}; as of {as_of}",
           _population(rows)]
    for i, lk in enumerate(looks, 1):
        out.append(f"look {i} (first {lk.size} fills): {'FORMED' if lk.formed else 'interim, decides nothing'}; "
                   f"final {lk.n_closed + lk.n_void}/{lk.size} (closed {lk.n_closed}, void {lk.n_void}); "
                   f"median net T+20 {_f(lk.median_net_t20, '%')} mean {_f(lk.mean_net_t20, '%')} hit "
                   f"{'n/a' if lk.hit_rate is None else f'{lk.hit_rate:.0%}'} "
                   f"median excess {_f(lk.median_excess_t20, '%')}")
    unformed = next((lk for lk in looks if not lk.formed), None)
    if unformed is not None:
        held = fv.look_holders(rows, unformed.size)
        out += [f"holding look {looks.index(unformed) + 1}: {len(held)} rows", *_holders(held, cal, as_of)]
    if rule == "brk20":
        blocked = fv.load_rows(conn, strategies=(rule,), construct="rule_blocked", start=start, as_of=as_of)
        arms = fv.c2_arms(rows, blocked)
        out += ["diagnostic, C2 arms (closed net T+20 %, decides nothing):",
                f"    admitted {_stats(arms.admitted)}", f"    blocked (down) {_stats(arms.blocked)}",
                f"    blocked for another reason, in neither arm: {arms.neither}"]
    return out


def analyst_report(conn: sqlite3.Connection, start: date, as_of: date, r_from: date | None,
                   cal: NSECalendar) -> list[str]:
    rows = fv.load_rows(conn, strategies=ANALYST_STRATEGIES, construct="next_open", start=start, as_of=as_of)
    keys = fv.registered_keys(rows)
    kept, excluded = fv.split_by_key(rows, keys)
    cells = fv.analyst_cells(kept)
    looks = fv.analyst_looks(cells)
    label, reason = fv.analyst_verdict(looks, registered_from=r_from)
    out = [f"VERDICT analyst: {label} ({reason})", f"registered from {r_from or 'none'}; as of {as_of}",
           "registered keys: " + (", ".join(f"{s}={k[0]}/{k[1]}" for s, k in sorted(keys.items())) or "none"),
           f"decided rows: kept {len(kept)}; excluded for another key "
           + (", ".join(f"{s} {n}" for s, n in sorted(Counter(r.strategy_id for r in excluded).items())) or "0"),
           _population(kept)]
    for i, lk in enumerate(looks, 1):
        out.append(f"look {i} (first {lk.size} cells): {'FORMED' if lk.formed else 'interim, decides nothing'}; "
                   f"{lk.n_cells} cells, final {lk.n_final}, valued {lk.n_valued}; "
                   f"mean enter - no_action {_f(lk.mean_diff, 'pp')}")
    unformed = next((lk for lk in looks if not lk.formed), None)
    if unformed is not None:
        held = fv.cell_holders(kept, cells, unformed.size)
        out += [f"holding look {looks.index(unformed) + 1}: {len(held)} rows", *_holders(held, cal, as_of)]
    unvalued = [c for c in cells if c.final and c.diff is None]
    out.append(f"unvalued final cells (an arm with no closed outcome): {len(unvalued)}"
               + "".join(f"\n    {c.d} {c.strategy_id}" for c in unvalued))
    out.append("diagnostics (decide nothing):")
    for h in (5, 10):
        diffs = [c.diff for c in fv.analyst_cells(kept, horizon=h) if c.diff is not None]
        out.append(f"    T+{h} cell difference: {_stats(fv.stats(diffs))}")
    for s in ANALYST_STRATEGIES:
        diffs = [c.diff for c in cells if c.strategy_id == s and c.diff is not None]
        out.append(f"    {s} T+20 cell difference: {_stats(fv.stats(diffs))}")
    out += [f"    confidence {b} {d}: {_stats(st)}" for (b, d), st in fv.confidence_buckets(kept).items()]
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pre-registered forward verdicts (read-only).")
    ap.add_argument("--rule", required=True, choices=("brk20", "ins", "analyst"))
    ap.add_argument("--state-db", type=Path, default=repo_root() / "data" / "state.db")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="read outcomes as known on this day (default: today IST)")
    args = ap.parse_args(argv)
    as_of = args.as_of or datetime.now(IST).date()
    r_from = fv.REGISTERED_FROM
    start = r_from or date.min
    cal = NSECalendar(config_dir() / "calendar", Clock(), include_special=False)
    conn = sqlite3.connect(f"file:{Path(args.state_db).resolve().as_posix()}?mode=ro", uri=True, timeout=5.0)
    try:
        lines = (analyst_report(conn, start, as_of, r_from, cal) if args.rule == "analyst"
                 else kill_report(conn, args.rule, start, as_of, r_from, cal))
    finally:
        conn.close()
    print("\n".join(lines))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
