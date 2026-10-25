#!/usr/bin/env python
"""``cat`` v2 SHADOW VERDICT — the pre-registered retire-or-review rule (§2.7 2026-08-18 amendment).

``cat`` v2 runs as a SHADOW strategy: its candidates are journalled at prescreen admission and the
§7.1 C3 check rejects every one of them (no registered edge), so nothing reaches RECOMMEND. The
WO-18 acceptance pre-registered how the shadow ends; this script applies that rule mechanically. It
changes nothing: it reads two stores read-only and prints a verdict.

===============================================================================================
THE RULE (pre-registered 2026-08-18, clock restarted 2026-08-28 — do not re-derive it after
seeing the numbers)
===============================================================================================
* Quoted: "shadow runs >= 30 sessions AND >= 20 admitted signals; then measure T+10/T+20 net drift
  (next-session-open fills, spread-inclusive CNC costs, WO-3 margin floor) of the signal
  population. Net <= the cost floor at BOTH horizons => ``cat`` origination is retired and the
  watchlist stays context-only permanently. Clearing it => owner review at the §8.6 gate (R4)
  before any live wiring." Clock restart (2026-08-28): "signals admitted 2026-08-18 through
  2026-08-28 are EXCLUDED from the verdict computation."
* **Population:** every ``prescreen_day_slots`` row with ``strategy_id='cat'`` and ``d`` in
  [:data:`CLOCK_RESTART_FROM`, ``as_of``], i.e. every signal the rule PUBLISHED. Never filtered on
  ``evaluated``/``forwarded``: the claim under test is the rule's drift, not the analyst's or the
  gate's later choices. Rows dated :data:`EXCLUDED_FROM` .. :data:`EXCLUDED_THROUGH` are dropped
  whatever ``--from`` says, and the report counts them.
* **Sessions:** regular NSE trading sessions in [``--from``, ``as_of``], counted with the repo's
  ``NSECalendar`` over ``config/calendar/`` with special sessions (muhurat, DR drills) EXCLUDED.
  ``as_of`` counts when it is a session, even if the run is before 15:30 — the session-count gate
  never binds after the cohort gate below anyway (a measured T+20 needs 20 sessions of bars).
* **Fixed verdict cohort (pinned-looks rule):** the FIRST :data:`COHORT_SIZE` non-VOID signals in
  the window ordered by (``d``, ``symbol``) (VOID: see CONVENTIONS). The cohort is cut by publication order, never by maturity order:
  an early signal that matures late HOLDS the verdict instead of being replaced by a later signal,
  and no signal after the 20th ever enters it.
* **Verdict** (:func:`verdict`, the one expression of the rule):
  ``INSUFFICIENT`` unless sessions >= :data:`MIN_SESSIONS` AND admitted signals >=
  :data:`MIN_SIGNALS` AND the cohort has :data:`COHORT_SIZE` non-void members AND every one of them
  has a measured T+10 and T+20. A member that cannot be
  measured because of a store hole HOLDS the verdict too ("fix the data, never skip the row").
  Then ``RETIRE`` iff cohort median net T+10 <= threshold_10 AND cohort median net T+20 <=
  threshold_20 (equality retires); otherwise ``OWNER REVIEW`` — clearing at either horizon sends
  ``cat`` to the §8.6 owner review, and nothing is wired by this result.
* **WO-3 margin floor:** WO-3 requires net expectancy >= cost_floor / 20 per day
  (``engine.learning.validate.margin_floor_pct_per_day``, ``MARGIN_FLOOR_DAYS`` = 20), so the bar at
  horizon H is ``cost_round_trip_pct x H / 20``: T+20 must net more than one full round trip and
  T+10 more than half of one. The report prints both thresholds and this derivation.

===============================================================================================
CONVENTIONS (pinned; each one is a choice, so each one is stated)
===============================================================================================
* **Entry and exit are ``hi52``'s, through the same code** (``engine.learning.hi52_forward``).
  Entry = the OPEN of the journal day ``d`` — the sweep that journals ``d`` runs on ``d``, so
  ``open(d)`` is the next-session-open fill after the prior-session close the rule's entry level is
  anchored on. Exit for horizon k = the CLOSE of the k-th session of the hold, entry session first:
  ``close(d + k-1 sessions) / open(d) - 1``. Sessions are positions in the symbol's own ``bars_1d``
  series. Horizons T+10 and T+20 only — nothing else is pre-registered, so nothing else is printed.
* **The 5% disaster stop and the 20-session time exit are not simulated.** The pre-registration
  measures net DRIFT at fixed horizons; ``cat.stop_pct`` enters only through the §7.1 sizing
  below (a wider stop means a smaller notional and a higher cost floor).
* **Costs: one full CNC round trip from the repo's ``CostModel``** (``breakeven_pct``, spread
  included) at the notional §7.1 sizing produces for this rule: ``per_trade_risk`` caps a swing
  position at ``equity x swing_position_pct% / (overnight_gap_mult x cat.stop_pct%)``
  (``scripts/hi52_forward_verdict.resolve_cost_floor`` called with ``cat``'s ``stop_pct``: the same
  code the ``hi52`` verdict charges). ``cat.stop_pct`` is read from ``config/settings.yaml`` through the
  repo loader (fallback: ``cat.DEFAULT_PARAMS``, stated). The equity is the SMALLEST
  ``equity_snapshots`` reading in the window (the tightest base any signal would have been sized
  under); with no snapshot it is the §7.1 ``equity_floor_rung`` (the smallest book that can still
  open a position), stated. Integer-qty rounding lands a real notional at or below the cap, so the
  floor is a lower bound by a few hundredths of a pp. ``--equity`` / ``--notional`` are what-ifs;
  ``--notional`` prints ``OVERRIDE``.
* **A cohort member is MEASURED, PENDING or a HOLE.** PENDING: ``d`` is after the last stored
  session, or a horizon's calendar session is after it (the ordinary state of a forward test).
  HOLE: no bars for the symbol, ``d`` missing from its series although the store has that session,
  a malformed entry or exit bar, or a horizon whose calendar session the store holds for other
  symbols while this symbol's series ends before it (``overdue`` — a halt or a missing bar; it
  clears when the symbol's bar lands or its own session count catches up). Both hold the verdict;
  they are listed separately because one waits and the other needs the data fixed.
* **VOID — an unadjusted corporate action inside the hold** (``engine.learning.exit_sim.simulate``'s
  rule, as ``rec_outcomes`` applies it). Stored ``bars_1d`` are never re-adjusted, so a bonus /
  split / rights / demerger ex-date inside the window reads as a crash. A signal is VOID
  (``void_ca``) when an ``engine.strategy.scanners.hi52.UNADJUSTED_KINDS`` ex-date ``x`` from
  ``corp_actions`` (bounded by ``as_of``) satisfies ``d < x <= last``, where ``last`` is its T+20
  exit session once measured, else the symbol's last stored session. An ex-date ON ``d`` does not
  void: ``open(d)`` is already in the new unit. VOID is FINAL (the ``_FINAL`` rows of
  ``scripts/no_stop_verdict.py look_cohort``): the cohort is the first 20 NON-void signals, voids
  enter no running, segment or excess cell, and they are counted and listed with ex-date and kind.
  It still counts as an ADMITTED signal (it was published). A corp-action row that lands after the
  cohort formed can still void a member and pull the 21st non-void signal in — a data correction,
  never a re-cut by outcome.
* **The medians compare unrounded.** Rounding (4 dp) is display-only; the verdict compares the raw
  median against the raw threshold.

===============================================================================================
DIAGNOSTICS (each one decides nothing)
===============================================================================================
* **Running cells** over ALL measured signals in the window (n, median/mean net, hit rate on net
  ``> 0``), each horizon on its own event set.
* **Segments** (owner directive: split at the first NIFTY 500 universe build, O15, and at the news
  scorer's switch to sonnet-5.5): pre-O15 [``--from``, :data:`O15_FIRST_SESSION` - 1], O15 +
  Sonnet-5 scorer [:data:`O15_FIRST_SESSION`, S - 1], Sonnet-5.5 scorer [S, ``as_of``]. S = the
  first trading session strictly after the IST date of the earliest ``news_clusters.scored_at``
  with ``scorer_model = 'sonnet-5.5'`` (bounded by ``as_of``); when that cannot be read, or no such
  row exists, S = :data:`SCORER_SWITCH_FALLBACK`, stated. Both boundaries are +-1-session
  approximations: neither store records the deploy, and a signal on a boundary day may have been
  graded by either side.
* **Excess over the equal-weight benchmark**: net minus
  ``engine.learning.benchmark.bench_pct(daily, fill_d=d, exit_d=<exit session>, "time", calendar)``
  with ``daily`` from ``state.db universe_ew_returns`` (bounded by ``as_of``). That window is
  close(d) .. close(exit) while the trade's is open(d) .. close(exit), so the entry session's
  intraday move is in the trade and not in the benchmark — the engine's own rec-outcome
  convention, stated rather than re-derived. A signal whose window has a missing EW day is omitted
  from that cell and counted.

Read-only by construction: SQLite through a ``file:...?mode=ro`` URI, DuckDB through
``read_only=True`` (which a running engine refuses — that refusal prints ``UNAVAILABLE``).
``--as-of`` bounds both stores: journal rows, bars, EW returns, equity snapshots and the scorer
switch lookup. Exit code is ALWAYS 0.

Usage:
    python scripts/cat_shadow_verdict.py
    python scripts/cat_shadow_verdict.py --as-of 2026-11-30 --json out.json
    python scripts/cat_shadow_verdict.py --notional 8000     # what-if; reports OVERRIDE
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import statistics
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any, NamedTuple

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, load_settings, repo_root  # noqa: E402
from engine.learning.benchmark import bench_pct  # noqa: E402
from engine.learning.hi52_forward import (  # noqa: E402
    Measured,
    Series,
    Signal,
    StoreUnreadable,
    load_series,
    load_signal_rows,
    measure_signal,
)
from engine.learning.validate import MARGIN_FLOOR_DAYS, margin_floor_pct_per_day  # noqa: E402
from engine.strategy.hi52_edge import PRODUCT  # noqa: E402
from engine.strategy.scanners import cat as cat_rule  # noqa: E402
from engine.strategy.scanners.hi52 import UNADJUSTED_KINDS  # noqa: E402


def _load_script(name: str) -> ModuleType:
    """A sibling script as a module, under the ``mt_<name>`` key the tests use too (one object)."""
    mod_name = f"mt_{name}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, Path(__file__).with_name(f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


#: The hi52 verdict's store openers, §7.1 limit readers, min-equity read and cell statistics.
h52v = _load_script("hi52_forward_verdict")

# =============================================================================== pinned constants
STRATEGY_ID = cat_rule.STRATEGY_ID

#: First day of the restarted clock (2026-08-28 amendment): the default ``--from``.
CLOCK_RESTART_FROM = date(2026, 8, 29)
#: Signals admitted in this span are EXCLUDED from the verdict, whatever ``--from`` says.
EXCLUDED_FROM = date(2026, 8, 18)
EXCLUDED_THROUGH = date(2026, 8, 28)

MIN_SESSIONS = 30
MIN_SIGNALS = 20
#: The verdict cohort is the first MIN_SIGNALS signals — the pre-registered n, cut once.
COHORT_SIZE = MIN_SIGNALS

#: Pre-registered horizons, in sessions of the hold. Both vote.
HORIZONS: tuple[int, ...] = (10, 20)

#: Segment boundary 1 (owner directive): the first session on the NIFTY 500 universe (O15).
O15_FIRST_SESSION = date(2026, 9, 7)
O15_PROVENANCE = (
    "WORKLOG 2026-09-04: O15 implemented after the session; first sweep on the NIFTY 500 universe "
    "= the next session"
)

#: Segment boundary 2: the news scorer's switch to this model (``news_clusters.scorer_model``).
SCORER_SWITCH_MODEL = "sonnet-5.5"
#: Used, and stated, when ``news_clusters.scored_at`` cannot be read or holds no such row.
SCORER_SWITCH_FALLBACK = date(2026, 10, 5)

SEGMENTS: tuple[str, ...] = ("pre_o15", "o15_sonnet5", "sonnet55")
SEGMENT_LABELS = {
    "pre_o15": "pre-O15",
    "o15_sonnet5": "O15 + Sonnet-5 scorer",
    "sonnet55": "Sonnet-5.5 scorer",
}

VERDICT_RETIRE = "RETIRE"
VERDICT_OWNER_REVIEW = "OWNER REVIEW"
VERDICT_INSUFFICIENT = "INSUFFICIENT"
VERDICT_UNAVAILABLE = "UNAVAILABLE"

MEASURED = "measured"
PENDING = "pending"
HOLE = "hole"
#: Final data-validity status (rec_outcomes / exit_sim name): never in the cohort or any cell.
VOID = "void_ca"

_HUNDRED = Decimal("100")


# =============================================================================== the rule
def margin_threshold_pct(cost_pct: float, horizon: int) -> float:
    """WO-3 bar at ``horizon``: the per-day floor (``cost / MARGIN_FLOOR_DAYS``) times the hold."""
    per_day = margin_floor_pct_per_day(cost_pct)
    if per_day is None:          # only for a None cost, which no caller passes
        raise ValueError("no cost floor")
    return per_day * horizon


def is_excluded(d: date) -> bool:
    return EXCLUDED_FROM <= d <= EXCLUDED_THROUGH


def apply_exclusion(signals: Sequence[Signal]) -> tuple[list[Signal], int]:
    """``(kept, n_excluded)``: the clock-restart exclusion, applied whatever the window is."""
    kept = [s for s in signals if not is_excluded(s.d)]
    return kept, len(signals) - len(kept)


def count_sessions(calendar: NSECalendar, start: date, as_of: date) -> int:
    """Regular sessions in ``[start, as_of]`` (the calendar must exclude special sessions)."""
    n, d = 0, start
    while d <= as_of:
        if calendar.is_trading_day(d):
            n += 1
        d += timedelta(days=1)
    return n


def fixed_cohort(signals: Sequence[Signal]) -> list[Signal]:
    """The first :data:`COHORT_SIZE` signals by (``d``, ``symbol``) — publication order, so a
    signal that matures late keeps its place and no later signal can take it."""
    return sorted(signals, key=lambda s: (s.d, s.symbol))[:COHORT_SIZE]


class Assessed(NamedTuple):
    """One signal's measurement state. ``exits`` maps each measured horizon to its exit session."""

    signal: Signal
    status: str
    detail: str
    trade: Measured | None
    exits: dict[int, date]


def assess(
    sig: Signal,
    series: Series | None,
    *,
    cost_pct: float,
    calendar: NSECalendar,
    stored_through: date | None,
    ex_dates: Sequence[tuple[date, str]] = (),
    horizons: Sequence[int] = HORIZONS,
) -> Assessed:
    """Measure ``sig``; VOID it on an unadjusted corp action inside its measured window, else say
    why any pre-registered horizon is missing (module docstring)."""
    out = _measure_state(sig, series, cost_pct=cost_pct, calendar=calendar,
                         stored_through=stored_through, horizons=horizons)
    # exit_sim.simulate's rule: an ex-date x voids when fill session < x <= bar session. The last
    # bar session is the T+20 exit once measured, else the symbol's last stored session (<= as_of).
    last = out.exits.get(max(horizons))
    if last is None and series is not None and len(series):
        last = series.dates[-1]
    if last is not None:
        inside = sorted((x, kind) for x, kind in ex_dates
                        if kind in UNADJUSTED_KINDS and sig.d < x <= last)
        if inside:
            x, kind = inside[0]
            return out._replace(status=VOID, detail=f"void_ca: {kind} ex-date {x}")
    return out


def _measure_state(
    sig: Signal,
    series: Series | None,
    *,
    cost_pct: float,
    calendar: NSECalendar,
    stored_through: date | None,
    horizons: Sequence[int],
) -> Assessed:
    if stored_through is None or sig.d > stored_through:
        return Assessed(sig, PENDING, "no_entry_yet: d is after the last stored session", None, {})
    if series is None or not len(series):
        return Assessed(sig, HOLE, "no_bars: no bars_1d rows for this symbol in the window "
                        "(renamed or delisted?)", None, {})
    if sig.d not in series.dates:
        return Assessed(sig, HOLE, f"unanchored: {sig.d} is not in this symbol's bars_1d "
                        f"(store holds sessions through {stored_through})", None, {})
    trade = measure_signal(series, sig.d, cost_pct=cost_pct, horizons=horizons)
    if trade is None:
        return Assessed(sig, HOLE, "bad_entry_bar: the open of d is unusable", None, {})
    i = series.dates.index(sig.d)
    exits = {k: series.dates[i + k - 1] for k in trade.net}
    status, detail = MEASURED, ""
    for k in sorted(horizons):
        if k in trade.net:
            continue
        if i + k - 1 < len(series):
            return Assessed(sig, HOLE, f"bad_exit_bar: the T+{k} close is unusable", trade, exits)
        try:
            due = calendar.add_sessions(sig.d, k - 1)
        except ValueError:
            due = None
        if due is not None and due <= stored_through:
            return Assessed(
                sig, HOLE,
                f"overdue_t{k}: calendar T+{k} session {due} is stored, this symbol's series ends "
                f"{series.dates[-1]} (halt or missing bar)", trade, exits,
            )
        if status == MEASURED:
            status, detail = PENDING, f"awaiting_t{k}: due {due if due is not None else 'n/a'}"
    return Assessed(sig, status, detail, trade, exits)


def verdict(
    *,
    sessions: int,
    admitted: int,
    cohort: Sequence[Assessed],
    threshold_10: float,
    threshold_20: float,
) -> tuple[str, str]:
    """``(verdict, reason)`` — the module docstring's THE RULE, and nothing else.

    No severity ladder, no "close to the line" wording, no running cell casting a vote.
    """
    holds = []
    if sessions < MIN_SESSIONS:
        holds.append(f"{sessions} sessions, rule needs {MIN_SESSIONS}")
    if admitted < MIN_SIGNALS:
        holds.append(f"{admitted} admitted signals, rule needs {MIN_SIGNALS}")
    if len(cohort) < COHORT_SIZE:
        holds.append(f"{len(cohort)} of {COHORT_SIZE} cohort places filled by non-void signals")
    pending = sum(1 for a in cohort if a.status == PENDING)
    holes = sum(1 for a in cohort if a.status == HOLE)
    if pending:
        holds.append(f"{pending} of {len(cohort)} cohort members not yet measured at T+10/T+20")
    if holes:
        holds.append(f"{holes} cohort members unmeasurable (store hole: fix the data)")
    if holds:
        return VERDICT_INSUFFICIENT, "; ".join(holds)
    med10 = statistics.median(a.trade.net[10] for a in cohort)    # type: ignore[union-attr]
    med20 = statistics.median(a.trade.net[20] for a in cohort)    # type: ignore[union-attr]
    cells = (f"cohort median net T+10 {med10:+.4f}% vs {threshold_10:.4f}%, "
             f"T+20 {med20:+.4f}% vs {threshold_20:.4f}%")
    if med10 <= threshold_10 and med20 <= threshold_20:
        return VERDICT_RETIRE, f"n={len(cohort)}; {cells}: at or below the floor at BOTH horizons"
    cleared = [f"T+{k}" for k, med, thr in ((10, med10, threshold_10), (20, med20, threshold_20))
               if med > thr]
    return VERDICT_OWNER_REVIEW, f"n={len(cohort)}; {cells}: clears at {' and '.join(cleared)}"


# =============================================================================== diagnostics
def segment_of(d: date, *, scorer_switch: date) -> str:
    if d < O15_FIRST_SESSION:
        return "pre_o15"
    if d < scorer_switch:
        return "o15_sonnet5"
    return "sonnet55"


def excess_cells(
    assessed: Sequence[Assessed],
    daily: Mapping[date, float | None],
    calendar: NSECalendar,
    horizons: Sequence[int] = HORIZONS,
) -> dict[str, dict[str, Any]]:
    """Net minus the EW benchmark over the same sessions; a window with a missing day is omitted."""
    out: dict[str, dict[str, Any]] = {}
    for k in horizons:
        values: list[float] = []
        omitted = 0
        for a in assessed:
            if a.trade is None or k not in a.trade.net:
                continue
            try:
                b = bench_pct(daily, a.signal.d, a.exits[k], "time", calendar)
            except (ValueError, TypeError, ArithmeticError):
                b = None
            if b is None:
                omitted += 1
            else:
                values.append(a.trade.net[k] - float(b))
        out[str(k)] = {
            "n": len(values),
            "median_excess": round(statistics.median(values), 4) if values else None,
            "mean_excess": round(statistics.fmean(values), 4) if values else None,
            "hit_rate": round(sum(1 for v in values if v > 0) / len(values), 4) if values else None,
            "omitted_missing_ew_day": omitted,
        }
    return out


# =============================================================================== stores and inputs
def _ist_date(value: Any) -> date:
    """The IST date of a DuckDB TIMESTAMPTZ (aware), a naive UTC instant, a date or an ISO string."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(IST).date()
    if isinstance(value, date):
        return value
    raise TypeError(f"not a timestamp: {value!r}")


def scorer_switch_session(
    fetchall: Callable[[str, list[Any]], Sequence[Sequence[Any]]],
    calendar: NSECalendar,
    *,
    as_of: date,
) -> tuple[date, str]:
    """``(S, provenance)`` — the first session after the earliest sonnet-5.5 ``scored_at``.

    Bounded by ``as_of`` (end of that IST day). Any read failure, or no such row, falls back to
    :data:`SCORER_SWITCH_FALLBACK` and says so.
    """
    bound = datetime.combine(as_of + timedelta(days=1), time(0, 0), tzinfo=IST)
    try:
        rows = fetchall(
            "SELECT MIN(scored_at) FROM news_clusters WHERE scorer_model = ? AND scored_at < ?",
            [SCORER_SWITCH_MODEL, bound],
        )
        first = rows[0][0] if rows else None
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one stated fallback
        return SCORER_SWITCH_FALLBACK, (
            f"PINNED FALLBACK ({type(exc).__name__} reading news_clusters.scored_at)")
    if first is None:
        return SCORER_SWITCH_FALLBACK, (
            f"PINNED FALLBACK (no news_clusters row with scorer_model='{SCORER_SWITCH_MODEL}' "
            f"scored on or before {as_of})")
    try:
        day = _ist_date(first)
        switch = calendar.next_trading_day(day)
    except (TypeError, ValueError) as exc:
        return SCORER_SWITCH_FALLBACK, f"PINNED FALLBACK ({type(exc).__name__}: {exc})"
    return switch, (f"first session after {day} (earliest news_clusters.scored_at with "
                    f"scorer_model='{SCORER_SWITCH_MODEL}', IST)")


def load_ew_daily(
    conn: sqlite3.Connection, *, as_of: date
) -> tuple[dict[date, float | None], str | None]:
    """``(universe_ew_returns d -> ret for d <= as_of, note)``; an unreadable table is a note."""
    try:
        rows = conn.execute(
            "SELECT d, ret FROM universe_ew_returns WHERE d <= ?", (as_of.isoformat(),)
        ).fetchall()
    except sqlite3.Error as exc:
        return {}, f"universe_ew_returns unreadable ({type(exc).__name__}: {exc})"
    out: dict[date, float | None] = {}
    for d, ret in rows:
        try:
            out[date.fromisoformat(str(d))] = None if ret is None else float(ret)
        except (TypeError, ValueError):
            continue
    return out, None


def load_stop_pct(cfg_dir: Path | None = None) -> tuple[Decimal, str]:
    """``(cat.stop_pct, provenance)`` from ``settings.yaml`` via the repo loader, else the rule's
    own default — stated."""
    try:
        value = Decimal(str(load_settings(cfg_dir).cat.stop_pct))
        if not value.is_finite() or value <= 0 or value >= _HUNDRED:
            raise ValueError(f"implausible cat.stop_pct {value}")
        return value, "settings.yaml cat.stop_pct"
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one stated fallback
        return (Decimal(str(cat_rule.DEFAULT_PARAMS["stop_pct"])),
                f"cat.DEFAULT_PARAMS fallback ({type(exc).__name__} reading settings.yaml)")


def cat_cost_floor(
    *,
    limits_path: Path,
    equity_override: Decimal | None,
    min_equity: Decimal | None,
    notional_override: Decimal | None = None,
    stop_pct: Decimal,
    stop_source: str = "",
) -> tuple[float, dict[str, Any]]:
    """The hi52 verdict's ``resolve_cost_floor`` at ``cat``'s stop, with the ``equity_floor_rung``
    as the fallback equity; adds ``stop_source`` to the derivation dict."""
    cost_pct, sizing = h52v.resolve_cost_floor(
        limits_path=limits_path, equity_override=equity_override, min_equity=min_equity,
        notional_override=notional_override, stop_pct=stop_pct,
        fallback_equity=h52v.load_registration_equity(limits_path),
    )
    return cost_pct, {**sizing, "stop_source": stop_source}


def load_ex_dates(
    fetchall: Callable[[str, list[Any]], Sequence[Sequence[Any]]],
    symbols: Sequence[str],
    *,
    start: date,
    as_of: date,
) -> dict[str, list[tuple[date, str]]]:
    """``symbol -> [(ex_date, kind)]`` for ``UNADJUSTED_KINDS`` with ``start < ex_date <= as_of``.

    An ex-date on or before ``start`` can void no signal (``d >= start`` and voiding needs
    ``x > d``). An unreadable table is :class:`StoreUnreadable`: voids move cohort membership, so a
    verdict without them would be a verdict on unchecked data.
    """
    wanted = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    if not wanted:
        return {}
    placeholders = ", ".join("?" for _ in wanted)
    sql = (f"SELECT symbol, ex_date, kind FROM corp_actions WHERE symbol IN ({placeholders}) "
           "AND ex_date > ? AND ex_date <= ? ORDER BY symbol, ex_date, kind")
    try:
        rows = fetchall(sql, [*wanted, start, as_of])
    except Exception as exc:  # noqa: BLE001 - a missing/renamed table is one clear refusal
        raise StoreUnreadable(f"corp_actions: {type(exc).__name__}: {exc}") from exc
    out: dict[str, list[tuple[date, str]]] = {}
    for symbol, x, kind in rows:
        if kind not in UNADJUSTED_KINDS:
            continue
        x = x.date() if isinstance(x, datetime) else x
        out.setdefault(str(symbol), []).append((x, str(kind)))
    return out


# =============================================================================== report
def _clip(series: Series, as_of: date) -> Series:
    n = sum(1 for d in series.dates if d <= as_of)   # dates ascend (load_series ORDER BY d)
    if n == len(series):
        return series
    return Series(series.symbol, series.dates[:n], series.open[:n], series.close[:n])


def build_report(
    signals: Sequence[Signal],
    series_by_symbol: Mapping[str, Series],
    *,
    cost_pct: float,
    as_of: date,
    start: date,
    calendar: NSECalendar,
    scorer_switch: tuple[date, str] = (SCORER_SWITCH_FALLBACK, "PINNED FALLBACK (not supplied)"),
    daily: Mapping[date, float | None] | None = None,
    ew_note: str | None = None,
    sizing: Mapping[str, Any] | None = None,
    ex_dates: Mapping[str, Sequence[tuple[date, str]]] | None = None,
) -> dict[str, Any]:
    kept, excluded = apply_exclusion([s for s in signals if start <= s.d <= as_of])
    kept.sort(key=lambda s: (s.d, s.symbol))
    # The loaders already bound bars and ex-dates at as_of; clipping again keeps a pure caller
    # lookahead-free too.
    series_by_symbol = {sym: _clip(s, as_of) for sym, s in series_by_symbol.items()}
    ex_by_symbol = {sym: [(x, k) for x, k in rows if x <= as_of]
                    for sym, rows in (ex_dates or {}).items()}
    ends = [s.dates[-1] for s in series_by_symbol.values() if len(s)]
    stored_through = max(ends) if ends else None
    assessed = [
        assess(s, series_by_symbol.get(s.symbol), cost_pct=cost_pct, calendar=calendar,
               stored_through=stored_through, ex_dates=ex_by_symbol.get(s.symbol, ()))
        for s in kept
    ]
    # VOID is final (no_stop_verdict.look_cohort's _FINAL rows): out of the cohort and every cell.
    valid = [a for a in assessed if a.status != VOID]
    by_signal = {a.signal: a for a in valid}
    cohort = [by_signal[s] for s in fixed_cohort([a.signal for a in valid])]
    thr = {k: margin_threshold_pct(cost_pct, k) for k in HORIZONS}
    sessions = count_sessions(calendar, start, as_of)
    v, reason = verdict(sessions=sessions, admitted=len(kept), cohort=cohort,
                        threshold_10=thr[10], threshold_20=thr[20])
    trades = [a.trade for a in valid if a.trade is not None]
    switch_day, switch_src = scorer_switch
    segments: dict[str, Any] = {}
    for seg in SEGMENTS:
        members = [a for a in valid if segment_of(a.signal.d, scorer_switch=switch_day) == seg]
        segments[seg] = {
            "signals": len(members),
            "horizons": h52v.summarize([a.trade for a in members if a.trade is not None], HORIZONS),
        }
    status_counts = {st: sum(1 for a in assessed if a.status == st)
                     for st in (MEASURED, PENDING, HOLE, VOID)}
    return {
        "meta": {
            "script": "scripts/cat_shadow_verdict.py",
            "strategy_id": STRATEGY_ID,
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "window": {"start": start.isoformat(), "as_of": as_of.isoformat()},
            "registered_window": start == CLOCK_RESTART_FROM,
            "excluded_span": [EXCLUDED_FROM.isoformat(), EXCLUDED_THROUGH.isoformat()],
            "stored_through": None if stored_through is None else stored_through.isoformat(),
            "horizons_sessions": list(HORIZONS),
            "min_sessions": MIN_SESSIONS,
            "min_signals": MIN_SIGNALS,
            "cohort_size": COHORT_SIZE,
            "product": PRODUCT,
            "cost_round_trip_pct": round(cost_pct, 4),
            "sizing": dict(sizing or {"derivation": "not supplied (pure call)", "override": False}),
            "thresholds_pct": {str(k): round(t, 4) for k, t in thr.items()},
            "threshold_derivation": (
                f"WO-3: net expectancy >= cost floor / {MARGIN_FLOOR_DAYS} per day => threshold(H) "
                f"= {cost_pct:.4f}% x H / {MARGIN_FLOOR_DAYS}: T+10 {thr[10]:.4f}%, "
                f"T+20 {thr[20]:.4f}%"
            ),
            "entry_convention": "OPEN of the journal day d (next-session-open fill after the "
                                "prior-session-close anchor)",
            "exit_convention": "CLOSE of the k-th session of the hold, entry session first: "
                               "close(d + k-1 sessions) / open(d) - 1",
            "segment_bounds": {
                "o15_first_session": O15_FIRST_SESSION.isoformat(),
                "o15_provenance": O15_PROVENANCE,
                "scorer_switch_session": switch_day.isoformat(),
                "scorer_switch_provenance": switch_src,
                "note": "both boundaries are +-1-session approximations",
            },
        },
        "population": {
            "signals_admitted": len(kept),
            "signals_excluded_clock_restart": excluded,
            "sessions": sessions,
            "status": status_counts,
            "symbols": sorted({s.symbol for s in kept}),
        },
        "cohort": {
            "members": [
                {
                    "d": a.signal.d.isoformat(), "symbol": a.signal.symbol, "status": a.status,
                    "detail": a.detail,
                    "net": {} if a.trade is None
                    else {str(k): round(x, 4) for k, x in sorted(a.trade.net.items())},
                }
                for a in cohort
            ],
            "measured": sum(1 for a in cohort if a.status == MEASURED),
            "pending": [f"{a.signal.d} {a.signal.symbol}: {a.detail}" for a in cohort
                        if a.status == PENDING],
            "holes": [f"{a.signal.d} {a.signal.symbol}: {a.detail}" for a in cohort
                      if a.status == HOLE],
        },
        "voids": [f"{a.signal.d} {a.signal.symbol}: {a.detail}" for a in assessed
                  if a.status == VOID],
        "verdict": v,
        "reason": reason,
        "diagnostics": {
            "running": h52v.summarize(trades, HORIZONS),
            "segments": segments,
            "excess_vs_ew": excess_cells(valid, daily or {}, calendar),
            "ew_note": ew_note,
        },
    }


def _pct(v: Any) -> str:
    return "     n/a" if v is None else f"{float(v):+8.4f}"


def _hit(v: Any) -> str:
    return "   n/a" if v is None else f"{float(v):6.1%}"


def render(doc: Mapping[str, Any]) -> str:
    m, pop, coh, dg = doc["meta"], doc["population"], doc["cohort"], doc["diagnostics"]
    sb = m["segment_bounds"]
    bar = "=" * 96
    lines = [
        bar,
        "cat v2 SHADOW VERDICT (pre-registered 2026-08-18, WO-18; clock restarted 2026-08-28)",
        bar,
        f"window          : {m['window']['start']} -> {m['window']['as_of']}   "
        f"(excluded always: {m['excluded_span'][0]} .. {m['excluded_span'][1]})"
        + ("" if m["registered_window"] else
           f"   [NON-REGISTERED WINDOW: --from differs from {CLOCK_RESTART_FROM}]"),
        f"store           : bars_1d through {m['stored_through'] or 'n/a'}",
        f"cost floor      : {m['cost_round_trip_pct']:.4f}% one CNC round trip"
        + ("   [OVERRIDE]" if m["sizing"].get("override") else ""),
        f"  notional      : {m['sizing'].get('derivation', 'n/a')}",
        f"  stop          : {m['sizing'].get('stop_pct', 'n/a')}% "
        f"({m['sizing'].get('stop_source') or 'n/a'})",
        f"thresholds      : {m['threshold_derivation']}",
        f"entry           : {m['entry_convention']}",
        f"exit            : {m['exit_convention']}",
        f"population      : {pop['signals_admitted']} admitted signals "
        f"({pop['signals_excluded_clock_restart']} excluded by the clock restart), "
        f"{pop['sessions']} sessions; measured {pop['status'][MEASURED]}, pending "
        f"{pop['status'][PENDING]}, holes {pop['status'][HOLE]}, void {pop['status'][VOID]}",
    ]
    if doc["voids"]:
        lines.append("  VOID (unadjusted corp action inside the hold; out of the cohort and every "
                     "cell):")
        lines += [f"    {x}" for x in doc["voids"]]
    lines += [
        "-" * 96,
        f"COHORT (first {m['cohort_size']} non-void by (d, symbol); fixed once cut): "
        f"{coh['measured']} of {len(coh['members'])} measured at T+10 and T+20",
    ]
    for mem in coh["members"]:
        nets = "  ".join(f"T+{k} {_pct(mem['net'].get(str(k)))}" for k in m["horizons_sessions"])
        lines.append(f"  {mem['d']}  {mem['symbol']:<14} {mem['status']:<9} {nets}"
                     + (f"   {mem['detail']}" if mem["detail"] else ""))
    if coh["holes"]:
        lines.append("  HOLES (fix the data; each one holds the verdict):")
        lines += [f"    {h}" for h in coh["holes"]]
    lines += [
        "-" * 96,
        f"RULE            : INSUFFICIENT unless sessions >= {m['min_sessions']} AND admitted >= "
        f"{m['min_signals']} AND every cohort member measured; then RETIRE iff cohort median net "
        f"T+10 <= {m['thresholds_pct']['10']:.4f}% AND T+20 <= {m['thresholds_pct']['20']:.4f}%, "
        "else OWNER REVIEW",
        f"VERDICT         : {doc['verdict']}   ({doc['reason']})",
    ]
    if doc["verdict"] == VERDICT_RETIRE:
        lines.append("  => cat origination is retired; the watchlist stays context-only permanently "
                     "(an owner-visible change citing this report).")
    elif doc["verdict"] == VERDICT_OWNER_REVIEW:
        lines.append("  => §8.6 owner review (R4) before any live wiring; this result wires nothing.")
    lines += [
        "-" * 96,
        "DIAGNOSTICS — each one decides nothing",
        f"{'cell':<34} {'horizon':>7}  {'n':>4}  {'median net':>10}  {'mean net':>9}  {'hit':>6}",
    ]

    def _cells(label: str, cells: Mapping[str, Any]) -> None:
        for k in m["horizons_sessions"]:
            c = cells.get(str(k), {})
            lines.append(f"{label:<34} {'T+' + str(k):>7}  {c.get('n', 0):>4}  "
                         f"{_pct(c.get('median_net')):>10}  {_pct(c.get('mean_net')):>9}  "
                         f"{_hit(c.get('hit_rate'))}   (decides nothing)")

    _cells("running, all measured signals", dg["running"])
    for seg in SEGMENTS:
        s = dg["segments"][seg]
        _cells(f"{SEGMENT_LABELS[seg]} ({s['signals']} sig)", s["horizons"])
    lines.append(f"  segments      : O15 from {sb['o15_first_session']} ({sb['o15_provenance']}); "
                 f"scorer switch S = {sb['scorer_switch_session']} ({sb['scorer_switch_provenance']}); "
                 f"{sb['note']}")
    for k in m["horizons_sessions"]:
        c = dg["excess_vs_ew"][str(k)]
        lines.append(f"{'excess vs EW benchmark':<34} {'T+' + str(k):>7}  {c['n']:>4}  "
                     f"{_pct(c['median_excess']):>10}  {_pct(c['mean_excess']):>9}  "
                     f"{_hit(c['hit_rate'])}   (decides nothing; omitted, missing EW day: "
                     f"{c['omitted_missing_ew_day']})")
    if dg.get("ew_note"):
        lines.append(f"  ew            : {dg['ew_note']}")
    lines.append(bar)
    return "\n".join(lines)


# =============================================================================== CLI
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="cat v2 shadow verdict (§2.7 2026-08-18 amendment, WO-18). "
                    "Read-only against state.db + market.duckdb; always exits 0.",
    )
    ap.add_argument("--state-db", type=Path, default=repo_root() / "data" / "state.db",
                    help="path to state.db (prescreen_day_slots, equity_snapshots, "
                         "universe_ew_returns)")
    ap.add_argument("--db", type=Path, default=repo_root() / "data" / "market.duckdb",
                    help="path to market.duckdb (bars_1d, news_clusters)")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="bound BOTH stores at this date (default: today IST)")
    ap.add_argument("--from", dest="start", type=date.fromisoformat, default=CLOCK_RESTART_FROM,
                    help=f"first signal day (default {CLOCK_RESTART_FROM}, the restarted clock; "
                         f"{EXCLUDED_FROM}..{EXCLUDED_THROUGH} is excluded whatever this says)")
    ap.add_argument("--json", dest="json_path", type=Path, default=None,
                    help="also write the raw numbers to this JSON path")
    ap.add_argument("--limits", type=Path, default=repo_root() / "config" / "limits.yaml",
                    help="path to limits.yaml (§7.1 per_trade_risk — the notional derivation)")
    ap.add_argument("--equity", type=h52v._positive, default=None, metavar="INR",
                    help="size the cost floor at this equity instead of the smallest snapshot in "
                         "the window (a what-if; the derivation is printed either way)")
    ap.add_argument("--notional", type=h52v._positive, default=None, metavar="INR",
                    help="charge the round trip at this notional and SKIP the §7.1 derivation "
                         "(a what-if; the report says OVERRIDE)")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    as_of = args.as_of or datetime.now(IST).date()
    start = args.start
    if as_of < start:
        print(f"--as-of {as_of} is before --from {start}: no population.", file=sys.stderr)
    calendar = NSECalendar(config_dir() / "calendar", Clock(), include_special=False)

    try:
        state = h52v.open_state_db(Path(args.state_db))
        try:
            signals = load_signal_rows(state, start=start, as_of=as_of, strategy_id=STRATEGY_ID)
            min_equity = h52v.load_min_equity(state, start=start, as_of=as_of)
            daily, ew_note = load_ew_daily(state, as_of=as_of)
        finally:
            state.close()
        market = h52v.open_market_db(Path(args.db))
        try:
            def fetch(sql: str, params: list[Any]) -> Sequence[Sequence[Any]]:
                return market.execute(sql, params).fetchall()

            symbols = [s.symbol for s in signals]
            series = load_series(fetch, symbols, start=start, end=as_of)
            ex_dates = load_ex_dates(fetch, symbols, start=start, as_of=as_of)
            switch = scorer_switch_session(fetch, calendar, as_of=as_of)
        finally:
            market.close()
    except StoreUnreadable as exc:
        print(f"VERDICT         : {VERDICT_UNAVAILABLE}   ({exc})")
        return 0

    stop_pct, stop_src = load_stop_pct()
    cost_pct, sizing = cat_cost_floor(
        limits_path=Path(args.limits), equity_override=args.equity, min_equity=min_equity,
        notional_override=args.notional, stop_pct=stop_pct, stop_source=stop_src,
    )
    doc = build_report(signals, series, cost_pct=cost_pct, as_of=as_of, start=start,
                       calendar=calendar, scorer_switch=switch, daily=daily, ew_note=ew_note,
                       sizing=sizing, ex_dates=ex_dates)
    print(render(doc))
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(doc, indent=2), encoding="utf-8")
        print(f"json            : {args.json_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
