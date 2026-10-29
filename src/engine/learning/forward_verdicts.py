"""Pre-registered forward verdicts: the brk20 and ins kill rules and the analyst-value verdict (review
§9 #2/#5). The rules' text is quoted in ``scripts/forward_verdicts.py``.

HINDSIGHT: reads ``signal_outcomes``; only owner surfaces, verdict code and research may import this
module. ``load_rows`` is the only function that touches SQLite; the rest is pure and never reads the
clock. Looks are cut from a fixed cohort and form only when every member is final, so a formed look
never moves.
"""

from __future__ import annotations

import bisect
import sqlite3
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date

#: First session of the registered populations; None until registered. Only a caller's default.
REGISTERED_FROM: date | None = None

KILL_SIZES = (20, 40)
ANALYST_SIZES = (30, 60)

DEMOTE, HOLD, ADVISORY, KEEP = "DEMOTE", "HOLD", "ADVISORY", "KEEP"
INSUFFICIENT, UNREGISTERED = "INSUFFICIENT", "UNREGISTERED"

_CONFIDENCE_EDGES = (0.55, 0.65, 0.75)
_BUCKETS = ("<0.55", "0.55-0.65", "0.65-0.75", ">=0.75", "none")
_DECISIONS = ("enter", "no_action")

_OUTCOME_COLS = "o.status, o.fill_d, o.exit_d, o.net_t5, o.net_t10, o.net_t20, o.excess_t20"
_JOURNAL_SQL = (
    f"SELECT p.d, p.symbol, p.strategy_id, {_OUTCOME_COLS}, p.analyst_decision, p.analyst_confidence, "
    "a.prompt_version, a.model, NULL FROM prescreen_day_slots p "
    "LEFT JOIN signal_outcomes o ON o.d = p.d AND o.symbol = p.symbol AND o.strategy_id = p.strategy_id "
    "AND o.construct = ? LEFT JOIN agent_calls a ON a.call_id = p.analyst_call_id "
    "WHERE p.strategy_id IN ({ph}) AND p.d >= ? AND p.d <= ?{first} ORDER BY p.d, p.symbol, p.strategy_id"
)
# brk20's `rule` is scored for first publications only (signal_outcomes._signals).
_FIRST_PUBLICATION = (
    " AND (p.strategy_id <> 'brk20' OR EXISTS (SELECT 1 FROM brk20_resting_levels r "
    "WHERE r.symbol = p.symbol AND r.signal_d = p.d))"
)
_BLOCKED_SQL = (
    f"SELECT b.signal_d, b.symbol, 'brk20', {_OUTCOME_COLS}, NULL, NULL, NULL, NULL, b.reason "
    "FROM brk20_regime_blocked b LEFT JOIN signal_outcomes o ON o.d = b.signal_d AND o.symbol = b.symbol "
    "AND o.strategy_id = 'brk20' AND o.construct = 'rule_blocked' "
    "WHERE b.signal_d >= ? AND b.signal_d <= ? ORDER BY b.signal_d, b.symbol"
)


@dataclass(frozen=True)
class SignalRow:
    d: date
    symbol: str
    strategy_id: str
    construct: str
    status: str
    fill_d: date | None
    exit_d: date | None
    net_t5: float | None
    net_t10: float | None
    net_t20: float | None
    excess_t20: float | None
    analyst_decision: str | None = None
    analyst_confidence: float | None = None
    prompt_version: str | None = None
    model: str | None = None
    reason: str | None = None          # rule_blocked only: brk20_regime_blocked.reason

    @property
    def final(self) -> bool:
        return self.status != "open"


@dataclass(frozen=True)
class Look:
    size: int
    formed: bool
    n_closed: int
    n_void: int            # final members without a return (void_ca, unscorable)
    median_net_t20: float | None
    hit_rate: float | None
    mean_net_t20: float | None
    median_excess_t20: float | None


@dataclass(frozen=True)
class Cell:
    d: date
    strategy_id: str
    n_enter: int
    n_decline: int
    final: bool
    diff: float | None


@dataclass(frozen=True)
class CellLook:
    size: int
    formed: bool
    n_cells: int
    n_final: int
    n_valued: int
    mean_diff: float | None


@dataclass(frozen=True)
class Stats:
    n: int
    median: float | None
    mean: float | None
    hit_rate: float | None


@dataclass(frozen=True)
class C2Arms:
    admitted: Stats
    blocked: Stats
    neither: int           # blocked for an undefined regime or a missing index bar: in no C2 cohort


# ------------------------------------------------------------------ loader
def load_rows(
    conn: sqlite3.Connection, *, strategies: Sequence[str], construct: str, start: date, as_of: date,
) -> list[SignalRow]:
    """Journal rows (``rule_blocked``: ``brk20_regime_blocked`` rows) with ``start <= d <= as_of`` and
    their ``construct`` outcome, as known on ``as_of``: a fill after ``as_of`` reads unfilled and an exit
    after it reads ``open``. A row the nightly job has not scored reads ``open``."""
    if construct == "rule_blocked":
        if "brk20" not in strategies:
            return []
        cur = conn.execute(_BLOCKED_SQL, (start.isoformat(), as_of.isoformat()))
    elif construct in ("rule", "next_open"):
        sql = _JOURNAL_SQL.format(ph=", ".join("?" for _ in strategies),
                                  first=_FIRST_PUBLICATION if construct == "rule" else "")
        cur = conn.execute(sql, (construct, *strategies, start.isoformat(), as_of.isoformat()))
    else:
        raise ValueError(f"unknown construct {construct!r}")
    return [_known_on(_row(r, construct), as_of) for r in cur]


def _row(r: Sequence, construct: str) -> SignalRow:
    d, symbol, sid, status, fill_d, exit_d, *rest = r
    return SignalRow(date.fromisoformat(d), symbol, sid, construct, status or "open", _date(fill_d),
                     _date(exit_d), *rest)


def _date(x: str | None) -> date | None:
    return None if x is None else date.fromisoformat(x)


def _known_on(row: SignalRow, as_of: date) -> SignalRow:
    late_fill = row.fill_d is not None and row.fill_d > as_of
    if late_fill or row.exit_d is not None and row.exit_d > as_of:
        return replace(row, status="open", fill_d=None if late_fill else row.fill_d, exit_d=None,
                       net_t5=None, net_t10=None, net_t20=None, excess_t20=None)
    return row


# ------------------------------------------------------------------ kill rules
def _cohort(rows: Iterable[SignalRow]) -> list[SignalRow]:
    return sorted((r for r in rows if r.fill_d is not None), key=lambda r: (r.fill_d, r.symbol, r.d))


def look_holders(rows: Sequence[SignalRow], size: int) -> list[SignalRow]:
    """What keeps the ``size`` look from forming: its non-final members and, once ``size`` rows filled,
    every unfilled ``open`` row dated on or before the last member's fill (it could still fill ahead of
    it — a missing bar holds a fill back)."""
    members = _cohort(rows)[:size]
    held = [r for r in members if not r.final]
    if len(members) == size and (cutoff := members[-1].fill_d) is not None:
        held += [r for r in rows if r.fill_d is None and not r.final and r.d <= cutoff]
    return held


def kill_looks(rows: Sequence[SignalRow], sizes: Sequence[int] = KILL_SIZES) -> list[Look]:
    """Look k is the first ``size`` FILLED rows by (fill_d, symbol, d): a fixed cohort whose membership
    never depends on finishing order. Figures over an unformed look are interim and decide nothing."""
    cohort = _cohort(rows)
    out = []
    for size in sizes:
        members = cohort[:size]
        nets = _nets((r for r in members if r.status == "closed"), 20)
        s = stats(nets)
        out.append(Look(
            size, len(members) == size and not look_holders(rows, size), len(nets),
            sum(1 for r in members if r.final and r.status != "closed"), s.median, s.hit_rate, s.mean,
            stats([r.excess_t20 for r in members if r.status == "closed" and r.excess_t20 is not None]).median,
        ))
    return out


def kill_verdict(looks: Sequence[Look], *, registered_from: date | None) -> tuple[str, str]:
    """DEMOTE iff the binding look's median net T+20 over its closed members <= 0, else HOLD. The binding
    look is the largest formed one (look 2 supersedes look 1)."""
    if registered_from is None:
        return UNREGISTERED, "no registered population start; every reading is interim"
    formed = [(i, lk) for i, lk in enumerate(looks, 1) if lk.formed]
    if not formed:
        lk = looks[0]
        return INSUFFICIENT, f"look 1 not formed: {lk.n_closed + lk.n_void} of its {lk.size} members final"
    i, lk = formed[-1]
    if lk.median_net_t20 is None:
        return INSUFFICIENT, f"look {i} formed without a closed member"
    label = DEMOTE if lk.median_net_t20 <= 0 else HOLD
    return label, (f"look {i} (first {lk.size} fills from {registered_from}): median net T+20 "
                   f"{lk.median_net_t20:+.2f}% over {lk.n_closed} closed")


# ------------------------------------------------------------------ analyst value
def registered_keys(rows: Iterable[SignalRow]) -> dict[str, tuple[str, str]]:
    """Per strategy, the (prompt_version, model) of its first decided row by (d, symbol) carrying both.
    ``rows`` are the population from R, so this is the key of the first decision on or after R."""
    keys: dict[str, tuple[str, str]] = {}
    for r in sorted(rows, key=lambda r: (r.d, r.symbol)):
        if r.analyst_decision and r.prompt_version and r.model:
            keys.setdefault(r.strategy_id, (r.prompt_version, r.model))
    return keys


def split_by_key(
    rows: Iterable[SignalRow], keys: dict[str, tuple[str, str]],
) -> tuple[list[SignalRow], list[SignalRow]]:
    """(decided rows carrying their strategy's registered key, decided rows excluded for another key)."""
    kept: list[SignalRow] = []
    excluded: list[SignalRow] = []
    for r in rows:
        if r.analyst_decision:
            (kept if keys.get(r.strategy_id) == (r.prompt_version, r.model) else excluded).append(r)
    return kept, excluded


def _nets(rows: Iterable[SignalRow], horizon: int) -> list[float]:
    return [v for r in rows if (v := getattr(r, f"net_t{horizon}")) is not None]


def analyst_cells(rows: Iterable[SignalRow], horizon: int = 20) -> list[Cell]:
    """One cell per (d, strategy_id) with at least one ``enter`` and one ``no_action``, ordered by
    (d, strategy_id). ``diff`` = mean enter net minus mean no_action net at T+``horizon`` (closed rows
    at T+20); None when an arm has none. ``final`` once every decided row is final."""
    groups: dict[tuple[date, str], list[SignalRow]] = defaultdict(list)
    for r in rows:
        if r.analyst_decision in _DECISIONS:
            groups[(r.d, r.strategy_id)].append(r)
    out = []
    for (d, sid), grp in sorted(groups.items()):
        enter = [r for r in grp if r.analyst_decision == "enter"]
        decline = [r for r in grp if r.analyst_decision == "no_action"]
        if not enter or not decline:
            continue
        a, b = _nets(enter, horizon), _nets(decline, horizon)
        out.append(Cell(d, sid, len(enter), len(decline), all(r.final for r in grp),
                        statistics.fmean(a) - statistics.fmean(b) if a and b else None))
    return out


def cell_holders(rows: Iterable[SignalRow], cells: Sequence[Cell], size: int) -> list[SignalRow]:
    """The decided rows keeping the first ``size`` cells from being final."""
    keys = {(c.d, c.strategy_id) for c in cells[:size]}
    return [r for r in rows if r.analyst_decision in _DECISIONS and (r.d, r.strategy_id) in keys and not r.final]


def analyst_looks(cells: Sequence[Cell], sizes: Sequence[int] = ANALYST_SIZES) -> list[CellLook]:
    """Look k is the first ``size`` cells, formed when all are final. Its mean is over the valued cells;
    a cell with an arm and no closed outcome is counted, not averaged."""
    out = []
    for size in sizes:
        first = cells[:size]
        valued = [c.diff for c in first if c.diff is not None]
        out.append(CellLook(size, len(first) == size and all(c.final for c in first), len(first),
                            sum(c.final for c in first), len(valued), stats(valued).mean))
    return out


def analyst_verdict(looks: Sequence[CellLook], *, registered_from: date | None) -> tuple[str, str]:
    """ADVISORY iff the binding look's mean cell difference <= 0, else KEEP. The binding look is the
    largest formed one (look 2 supersedes look 1)."""
    if registered_from is None:
        return UNREGISTERED, "no registered population start; every reading is interim"
    formed = [(i, lk) for i, lk in enumerate(looks, 1) if lk.formed]
    if not formed:
        lk = looks[0]
        return INSUFFICIENT, f"look 1 not formed: {lk.n_final} of {lk.size} cells final ({lk.n_cells} exist)"
    i, lk = formed[-1]
    if lk.mean_diff is None:
        return INSUFFICIENT, f"look {i} formed without a valued cell"
    label = ADVISORY if lk.mean_diff <= 0 else KEEP
    return label, (f"look {i} (first {lk.size} cells from {registered_from}): mean enter - no_action net "
                   f"T+20 {lk.mean_diff:+.2f}pp over {lk.n_valued} valued cells")


# ------------------------------------------------------------------ diagnostics
def stats(values: Sequence[float]) -> Stats:
    """n, median, mean and hit rate (share > 0)."""
    if not values:
        return Stats(0, None, None, None)
    return Stats(len(values), statistics.median(values), statistics.fmean(values),
                 sum(1 for v in values if v > 0) / len(values))


def confidence_buckets(rows: Iterable[SignalRow]) -> dict[tuple[str, str], Stats]:
    """Closed net T+20 per (confidence bucket, decision) over the decided rows."""
    groups: dict[tuple[str, str], list[SignalRow]] = defaultdict(list)
    for r in rows:
        if r.analyst_decision in _DECISIONS:
            c = r.analyst_confidence
            bucket = _BUCKETS[-1] if c is None else _BUCKETS[bisect.bisect_right(_CONFIDENCE_EDGES, c)]
            groups[(bucket, r.analyst_decision)].append(r)
    order = sorted(groups, key=lambda k: (_BUCKETS.index(k[0]), k[1]))
    return {k: stats(_nets(groups[k], 20)) for k in order}


def c2_arms(rule_rows: Iterable[SignalRow], blocked_rows: Sequence[SignalRow]) -> C2Arms:
    """Closed net T+20 of C2's admitted crossings against those it blocked for a down regime; as in the
    C2 study, a crossing blocked for any other reason is in neither arm, only counted."""
    down = [r for r in blocked_rows if r.reason == "down"]
    return C2Arms(stats(_nets(rule_rows, 20)), stats(_nets(down, 20)), len(blocked_rows) - len(down))
