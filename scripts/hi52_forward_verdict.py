#!/usr/bin/env python
"""``hi52`` FORWARD-TEST VERDICT — the mechanical kill criterion for the 2026-09-12 promotion.

``hi52`` v2 left SHADOW on 2026-09-12 (IMPLEMENTATION_PLAN.md, "hi52 v2 promotion" §8.6 addendum):
an expected edge is registered, the §7.1 C3 check has a basis, and its candidates can reach
RECOMMEND. The v2 backtest that justified that is CPCV-promotable (n=1,794; T+20 median net +1.71%,
58.2% hit; fold pass 86.7% under N=2) but its edge lives in an INDEX cell labelled by CURRENT
membership applied backwards — a survivorship-tainted proxy. So RECOMMEND mode **is** the forward
test, and a forward test with real money needs a written, mechanical way to end it. This script is
that way. It changes nothing: it reads two stores read-only and prints a verdict.

===============================================================================================
THE RULE (pre-registered 2026-09-12 — do not re-derive it after seeing the numbers)
===============================================================================================
* **Population:** every ``prescreen_day_slots`` row with ``strategy_id='hi52'`` and ``d >=``
  :data:`PROMOTION_DATE` — i.e. every signal the promoted rule PUBLISHED. Not the subset that was
  forwarded, evaluated, recommended or bought: the claim under test is the RULE's drift, and
  conditioning on the analyst's or the gate's later choices would measure them instead.
* **Verdict:** ``DEMOTE`` if ``n >=`` :data:`MIN_SIGNALS` AND (median net at T+20 ``<= 0`` OR hit
  rate at T+20 ``< 50%``); ``HOLD`` otherwise; ``INSUFFICIENT`` below :data:`MIN_SIGNALS`.
  Only the T+20 cell decides — it is the horizon the registered edge was measured over.
* **Demotion is two edits, both or neither:** re-add ``hi52.STRATEGY_ID`` to
  ``engine.ops.main.NO_EDGE_SHADOW_STRATEGIES`` and delete ``hi52.expected_edge_pct`` from
  ``config/settings.yaml``. An owner-visible commit, never a silent config nudge.

===============================================================================================
CONVENTIONS (pinned; each one is a choice, so each one is stated)
===============================================================================================
* **Entry — the OPEN of the journal day ``d`` itself**, which is exactly
  ``scripts/backtest_hi52.py``'s anchor (2026-09-12 manager decision). The study's signal day is the
  TRIGGER session ``y`` and it enters at the next open; the sweep that journals ``d`` runs on the
  session AFTER ``y``, so ``d`` IS the study's entry session and ``open(d)`` IS the study's entry
  price. Measuring the same quantity the registered edge was measured as is the whole point: a kill
  criterion that tests a different quantity from the one being paid for cannot refute it.
  Stated consequence: the LIVE fill happens LATER on ``d`` — the recommendation lands in the
  morning window and the owner executes by hand — so realized results trail this metric by that
  intraday drift. The metric is the RULE's drift, not the owner's execution; a persistent gap
  between them is an execution finding, never a reason to re-anchor the rule after the fact.
* **Exit — the CLOSE of the ``k``-th session of the HOLD, the entry session counting as the first**:
  ``return = close(d + (k-1) sessions) / open(d) - 1``. In the study's trigger-indexed notation
  (``y = d - 1 session``) that is ``close(y + k) / open(y + 1)`` — the same k-session hold, the same
  repo event-study convention (``scripts/event_study.py``, ``backtest_hi52.py``). Sessions are
  positions in THAT symbol's own ``bars_1d`` series, so weekends, exchange holidays and a symbol's
  own trading halts are handled by construction and never by calendar arithmetic.
* **Costs — one full CNC round trip from the repo's own ``CostModel``**, spread included
  (``breakeven_pct``, never the fees-only view), charged at the notional **§7.1 sizing actually
  produces for this rule** — never a round number chosen by hand. A swing position is capped by
  ``per_trade_risk``: ``swing_position_pct`` (2.0%) of equity, charged at ``overnight_gap_mult``
  (2.5×) the stop distance, and ``hi52``'s stop is 6% — so ``notional <= equity × 2% / (2.5 × 6%)``
  = ``equity / 7.5`` (:func:`sizing_notional`). It matters: at ₹8,000 the round trip is 0.4343% and
  at the ₹5,234 the rule really gets it is 0.5357%, and under-charging by 0.1 pp biases BOTH demote
  clauses toward keeping an unvalidated rule trading. The equity is the SMALLEST snapshot in the
  measured window (``state.db``), i.e. the tightest capital base any measured signal was sized
  under — the conservative reading, so a shrinking book raises the floor rather than hiding behind
  a registration-day number. Residual, stated: integer-qty rounding lands the real notional AT OR
  BELOW that cap, so the charged floor is a lower bound by a few hundredths of a pp; read a verdict
  sitting exactly on the line with that margin in mind. ``--notional`` overrides the derivation
  outright for a what-if and the report says ``OVERRIDE`` when it was used.
  **ONE derivation, two readings:** :func:`registered_edge_pct` computes ``settings.hi52
  .expected_edge_pct`` from the SAME :func:`sizing_notional` + :func:`cost_floor_pct` pair, at the
  smallest equity the §7.1 table still lets the book open a position at (the ``equity_floor_rung``,
  −10% of the ₹40,000 base ⇒ ₹36,000 ⇒ ₹4,800 ⇒ 0.5619% ⇒ 2.0336 − 0.5619 = **1.47**, rounded
  DOWN). The registered edge and this script's floor therefore cannot drift apart: a unit test pins
  ``registered_edge_pct() == load_settings().hi52.expected_edge_pct``, so an owner change to
  ``limits.yaml``, to the stop, or to the cost tables fails that test instead of silently leaving a
  stale edge registered.
* **Horizons are measured on their OWN event sets.** A signal old enough for T+10 but not yet for
  T+20 counts in the T+10 cell only. A forward test reads as its data arrives, so the cells have
  different ``n`` and are not comparable with each other — which is also why the verdict reads T+20
  and nothing else. **T+5 and T+10 are printed as DIAGNOSTICS** (so marked): the v2 study measured
  T+5 dead and the registered edge is the T+20 number, so neither may be read as a verdict.
* **A signal whose ``d`` is missing from that symbol's ``bars_1d`` series is NOT measured.** Told
  apart two ways, because they are two different things: ``d`` AFTER the last stored session is
  ``no_entry_yet`` — the ordinary state of a forward test on the morning it runs, nothing to chase —
  while ``d`` missing from INSIDE the stored range is ``unanchored``, a hole in the series that also
  corrupts the session count the horizons are measured in.
* **``--as-of`` bounds BOTH stores** — journal rows and bars — so a verdict for a past date is
  reproducible and can never read a bar that did not exist then.

Read-only by construction: SQLite through a ``file:...?mode=ro`` URI, DuckDB through a read-only
attach (which a running engine will refuse — that refusal is correct, not a bug to work around).

Exit code is ALWAYS 0: this script is a report, and a missing store or an empty population is a
finding to print, never a build-breaking failure. The verdict line carries the information —
``UNAVAILABLE`` when a store could not be read at all.

Usage:
    python scripts/hi52_forward_verdict.py
    python scripts/hi52_forward_verdict.py --as-of 2026-10-31 --json out.json
    python scripts/hi52_forward_verdict.py --notional 8000     # what-if; reports OVERRIDE
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import statistics
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any, NamedTuple

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.clock import IST  # noqa: E402
from engine.core.config import load_yaml, repo_root  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.scanners.hi52 import DEFAULT_PARAMS, STRATEGY_ID  # noqa: E402

# =============================================================================== pinned constants
#: First session of the PROMOTED population. 2026-09-12 is the promotion commit (a Saturday); the
#: first sweep that could originate under it is Monday 2026-09-14. Rows before this are the SHADOW
#: population on a different rule (v1 filters were diagnostics then) and are never pooled with it.
#: It is an ASSUMPTION, not a measurement — see :data:`PROMOTION_DATE_PROVENANCE`.
PROMOTION_DATE = date(2026, 9, 14)

#: Printed with every verdict, because the boundary is a PREDICTION about when the tranche deploys
#: and nothing in either store records the deploy. If the engine runs a window_open sweep on or after
#: 2026-09-14 on code that still has ``hi52`` in ``NO_EDGE_SHADOW_STRATEGIES`` (the v1 rule: smooth
#: and gap were diagnostics, not gates), those rows land in ``prescreen_day_slots`` looking exactly
#: like promoted ones and the kill rule would measure a rule the engine was not running. Confirm the
#: first promoted session against the deploy (``git log`` on the tranche, the engine's boot line) and
#: pass ``--from`` explicitly whenever it differs.
PROMOTION_DATE_PROVENANCE = (
    "ASSUMED = first session after the 2026-09-12 promotion commit; confirm against the deploy date "
    "and pass --from if the tranche shipped later"
)

#: Horizons measured, in trading sessions of the HOLD (the entry session counts as the first).
#: T+5 is printed because the manager asked to see it (2026-09-12) — the v2 study measured it DEAD
#: (median gross +0.24% against a 0.32% floor), so it is a DIAGNOSTIC and can never be a verdict:
#: a cell the promoted rule makes no claim about is exactly the cell post-hoc reading reaches for.
HORIZONS: tuple[int, ...] = (5, 10, 20)

#: The ONE horizon the verdict reads — the horizon the registered edge was measured over.
VERDICT_HORIZON = 20

#: Everything else is labelled "diagnostic" in the report, so no one can quote it as an outcome.
DIAGNOSTIC_HORIZONS: tuple[int, ...] = tuple(k for k in HORIZONS if k != VERDICT_HORIZON)

#: Minimum measured T+20 signals before the rule can say anything. Below it the verdict is
#: INSUFFICIENT, never HOLD: "not yet refuted" and "no evidence" must not read the same.
MIN_SIGNALS = 20

#: §7.1 ``per_trade_risk`` inputs of the notional derivation, as ``config/limits.yaml`` carries them
#: on 2026-09-12. FALLBACKS only: :func:`load_sizing_limits` reads the live file, so an owner change
#: to either number moves this script's cost floor without a code edit; these values are what it
#: charges when that file cannot be read (a report never fails on a missing input, it states it).
SWING_POSITION_PCT = Decimal("2.0")
OVERNIGHT_GAP_MULT = Decimal("2.5")

#: Equity the notional is derived at when ``state.db`` shows no usable snapshot in the window — the
#: live reading at the promotion (``equity_snapshots``, 2026-09-11 13:00 IST). Overridable with
#: ``--equity``, which is also how a "what if the book were X" run is done.
REFERENCE_EQUITY_INR = Decimal("39256.65")

#: §7.1 capital anchor and the go-flat rung, as ``config/limits.yaml`` carries them on 2026-09-12.
#: FALLBACKS only (:func:`load_registration_equity` reads the live file). ₹40,000 × (1 − 10%) =
#: ₹36,000 is the SMALLEST equity at which the risk table still lets a new position be opened: at or
#: below the rung the platform goes flat and CLOSE_ONLY, so no hi52 entry is sized under it, ever.
CAPITAL_BASE_INR = Decimal("40000")
EQUITY_FLOOR_RUNG_PCT = Decimal("-10.0")

#: The v2 study's measured T+20 median GROSS drift — ``data/reports/backtest_hi52_v2_2026-09-09.json``
#: (``geometry/discrete_fresh_cross/horizons/20/median_gross_pct``, n=1,794). GROSS on purpose: the
#: study charged its own ₹20,000-reference round trip, a size this book never trades, so the net it
#: reports is not this book's net. The cost floor below is what makes it one.
MEASURED_T20_MEDIAN_GROSS_PCT = Decimal("2.0336")

#: The rule's own disaster stop — read from the scanner, never re-typed: it is half the derivation
#: (a wider stop shrinks the notional and RAISES the cost floor), so a change there must move this.
STOP_PCT = Decimal(str(DEFAULT_PARAMS["stop_pct"]))

PRODUCT = "CNC"          # swing/delivery; NSE cash cannot be held overnight under MIS

_HUNDRED = Decimal("100")


def sizing_notional(
    equity: Decimal, *, position_pct: Decimal, gap_mult: Decimal, stop_pct: Decimal
) -> Decimal:
    """The §7.1 ``per_trade_risk`` cap on a swing position's NOTIONAL, in rupees.

    ``qty_max = floor(position_pct% × equity / (gap_mult × stop_pct% × price))`` (``gate.py``
    ``_rule_per_trade_risk``), so ``qty_max × price <= position_pct% × equity / (gap_mult ×
    stop_pct%)`` — the price cancels and the cap is a pure function of these three numbers. The
    other sizing rules do not bind here: ``capital_cap`` allows ₹40,000 deployed against this ₹5k.

    Returns ``REFERENCE_EQUITY_INR``-based arithmetic only; a non-positive or non-finite input is a
    refusal (``ValueError``) rather than a silently tiny notional, because a tiny notional means a
    huge cost floor and a spurious DEMOTE.
    """
    if equity <= 0 or position_pct <= 0 or gap_mult <= 0 or stop_pct <= 0:
        raise ValueError(
            f"sizing inputs must be positive: equity={equity} position_pct={position_pct} "
            f"gap_mult={gap_mult} stop_pct={stop_pct}"
        )
    return (equity * position_pct / _HUNDRED) / (gap_mult * stop_pct / _HUNDRED)


#: The notional at the registration equity — what the report charges when the store shows nothing.
#: ₹5,234 at ₹39,256.65 equity, a 2% swing budget, the 2.5× overnight gap multiplier and a 6% stop.
REFERENCE_NOTIONAL = sizing_notional(
    REFERENCE_EQUITY_INR, position_pct=SWING_POSITION_PCT,
    gap_mult=OVERNIGHT_GAP_MULT, stop_pct=STOP_PCT,
)


def cost_floor_pct(notional: Decimal) -> Decimal:
    """One full CNC round trip at ``notional``, in percent — the repo's own ``CostModel``.

    ``breakeven_pct``, never the fees-only view: the spread is the largest single component at this
    size and a floor that omits it is not a floor. THE one place this script turns a notional into a
    cost, so the registered edge (:func:`registered_edge_pct`) and the forward nets are charged by
    the same code against the same tables.
    """
    return Decimal(str(CostModel.from_config().breakeven_pct(notional, PRODUCT)))


def registered_edge_pct(limits_path: Path | None = None) -> Decimal:
    """The number ``config/settings.yaml`` registers as ``hi52.expected_edge_pct`` — DERIVED here.

    ``measured T+20 median GROSS − one CNC round trip at the §7.1-sized notional``, rounded DOWN to
    two decimals (``ROUND_FLOOR``, never nearest: rounding an edge UP is the one direction that can
    buy a candidate a C3 pass it did not earn — and FLOOR rather than DOWN so a negative result, a
    rule that cannot clear its own costs, rounds away from zero too instead of toward it).

    The equity is the **``equity_floor_rung``**, not today's book: ``capital_base_inr`` × (1 +
    ``equity_pct_of_base``/100) = ₹36,000, the smallest equity the risk table lets a new position be
    opened at. A registered edge is a constant in a protected config file, read on every candidate
    for months; the cost floor it must clear RISES as the book shrinks (₹5,333 ⇒ 0.5303% at the
    ₹40,000 base, ₹5,234 ⇒ 0.5357% at the 09-11 equity, ₹4,800 ⇒ 0.5619% at the rung). Deriving it
    at the largest permitted book would register the edge that flatters the rule at exactly the
    moment the book is smallest — the drawdown — so it is derived at the worst one instead:
    2.0336 − 0.5619 = 1.4717 ⇒ **1.47**.

    This function IS the derivation; ``settings.yaml`` carries only its output, and
    ``tests/unit/test_hi52_forward_verdict.py`` pins the two together so they cannot drift apart.
    """
    path = limits_path or (repo_root() / "config" / "limits.yaml")
    position_pct, gap_mult, _ = load_sizing_limits(path)
    equity, _ = load_registration_equity(path)
    notional = sizing_notional(
        equity, position_pct=position_pct, gap_mult=gap_mult, stop_pct=STOP_PCT
    )
    edge = MEASURED_T20_MEDIAN_GROSS_PCT - cost_floor_pct(notional)
    return edge.quantize(Decimal("0.01"), rounding=ROUND_FLOOR)


VERDICT_DEMOTE = "DEMOTE"
VERDICT_HOLD = "HOLD"
VERDICT_INSUFFICIENT = "INSUFFICIENT"
VERDICT_UNAVAILABLE = "UNAVAILABLE"

#: Review cadence the plan addendum fixes: run this at 20 and at 40 signals.
REVIEW_AT_SIGNALS: tuple[int, ...] = (20, 40)


class Signal(NamedTuple):
    """One published hi52 signal: the journal day and its symbol."""

    d: date
    symbol: str


@dataclass(frozen=True)
class Series:
    """One symbol's ascending ``bars_1d`` sessions. Only opens and closes are read."""

    symbol: str
    dates: list[date]
    open: list[float]
    close: list[float]

    def __len__(self) -> int:
        return len(self.dates)


@dataclass
class Measured:
    """One measured signal. ``gross``/``net`` are PERCENT returns per horizon, equal notional."""

    symbol: str
    signal_date: date
    entry_date: date
    entry_px: float
    gross: dict[int, float] = field(default_factory=dict)
    net: dict[int, float] = field(default_factory=dict)


# =============================================================================== measurement (pure)
def measure_signal(
    series: Series, signal_date: date, *, cost_pct: float, horizons: Sequence[int] = HORIZONS
) -> Measured | None:
    """Measure one signal against one symbol's series, or ``None`` when it cannot be anchored.

    ``None`` means: the signal day is not a session in this series, or its open is unusable. The
    entry is ``open(d)`` — the journal day's OWN open, the study's anchor (module docstring) — so
    the entry bar exists whenever the anchor does, and the exit for horizon ``k`` is the close of
    the ``k``-th session of the hold, ``close[i + k - 1]``, the entry session counting as the first.
    A signal that HAS an entry but has not reached a horizon is returned with that horizon simply
    absent from ``gross``/``net`` — horizons are measured on their own event sets (module
    docstring), so a young signal contributes to the cells it has reached and to no others.
    Never raises on ordinary or malformed bars: a non-finite/non-positive price drops that cell.
    """
    try:
        i = series.dates.index(signal_date)
    except ValueError:
        return None
    entry_px = series.open[i]
    if not math.isfinite(entry_px) or entry_px <= 0.0:
        return None
    out = Measured(
        symbol=series.symbol, signal_date=signal_date,
        entry_date=series.dates[i], entry_px=float(entry_px),
    )
    for k in horizons:
        if k < 1 or i + k - 1 >= len(series.dates):
            continue                      # not yet held that long — not a refusal, just not yet
        exit_px = series.close[i + k - 1]
        if not math.isfinite(exit_px) or exit_px <= 0.0:
            continue
        gross = (float(exit_px) / float(entry_px) - 1.0) * 100.0
        out.gross[k] = gross
        out.net[k] = gross - cost_pct
    return out


def measure_all(
    signals: Iterable[Signal],
    series_by_symbol: Mapping[str, Series],
    *,
    cost_pct: float,
    horizons: Sequence[int] = HORIZONS,
) -> tuple[list[Measured], dict[str, int]]:
    """Measure every signal. Returns the trades and the skip tally.

    The tally is not cosmetic, and its FOUR classes are not one class: an ``n`` that shrank because
    the newest signals have no entry bar YET (``no_entry_yet`` — the ordinary state of a forward test
    at every run) reads nothing like one that shrank because a symbol's bhavcopy row is missing
    (``no_bars``/``unanchored``, a store defect worth chasing) or because a bar is malformed
    (``bad_entry_bar``). Collapsing them would send an operator after a phantom data gap.

    Since the entry is ``open(d)`` itself (2026-09-12), the two anchor failures are told apart by
    WHERE ``d`` sits relative to the stored series: past its last session ⇒ the day's bhavcopy has
    not landed yet (``no_entry_yet``, and on the morning of every run the newest rows are exactly
    that); inside the stored range but absent ⇒ a genuine hole (``unanchored``). Guessing between
    them would send an operator chasing a gap that is really this morning.
    """
    trades: list[Measured] = []
    skipped = {"no_bars": 0, "unanchored": 0, "no_entry_yet": 0, "bad_entry_bar": 0}
    for sig in signals:
        series = series_by_symbol.get(sig.symbol)
        if series is None or not len(series):
            skipped["no_bars"] += 1
            continue
        # The two reasons measure_signal refuses an anchor, told apart here rather than guessed at.
        if sig.d not in series.dates:
            skipped["no_entry_yet" if sig.d > series.dates[-1] else "unanchored"] += 1
            continue
        m = measure_signal(series, sig.d, cost_pct=cost_pct, horizons=horizons)
        if m is None:
            skipped["bad_entry_bar"] += 1
            continue
        trades.append(m)
    return trades, skipped


def _cell(gross: list[float], net: list[float]) -> dict[str, Any]:
    """One horizon's statistics. ``hit_rate`` is on NET returns and strictly ``> 0`` — the same
    definition ``backtest_hi52._stats`` uses, so the forward and backtest cells read alike."""
    if not net:
        return {"n": 0, "median_net": None, "mean_net": None, "hit_rate": None,
                "median_gross": None}
    return {
        "n": len(net),
        "median_net": round(statistics.median(net), 4),
        "mean_net": round(statistics.fmean(net), 4),
        "hit_rate": round(sum(1 for v in net if v > 0) / len(net), 4),
        "median_gross": round(statistics.median(gross), 4),
    }


def summarize(
    trades: Sequence[Measured], horizons: Sequence[int] = HORIZONS
) -> dict[str, dict[str, Any]]:
    """``{"10": cell, "20": cell}`` — each horizon over the trades that have REACHED it."""
    return {
        str(k): _cell([t.gross[k] for t in trades if k in t.gross],
                      [t.net[k] for t in trades if k in t.net])
        for k in horizons
    }


def verdict(cells: Mapping[str, Mapping[str, Any]]) -> tuple[str, str]:
    """``(verdict, reason)`` against the pre-registered rule — the module docstring's THE RULE.

    Reads ONLY the T+``VERDICT_HORIZON`` cell. Written as one expression of that rule and nothing
    else: no severity ladder, no "close to the line" wording, no second horizon casting a vote.
    """
    cell = cells.get(str(VERDICT_HORIZON)) or {}
    n = int(cell.get("n") or 0)
    if n < MIN_SIGNALS:
        return (VERDICT_INSUFFICIENT,
                f"{n} measured T+{VERDICT_HORIZON} signals, rule needs {MIN_SIGNALS}")
    median_net = cell.get("median_net")
    hit_rate = cell.get("hit_rate")
    if median_net is None or hit_rate is None:      # unreachable with n > 0; fail to the safe side
        return VERDICT_DEMOTE, f"T+{VERDICT_HORIZON} statistics unavailable on {n} signals"
    failures = []
    if median_net <= 0:
        failures.append(f"median net {median_net:+.4f}% <= 0")
    if hit_rate < 0.50:
        failures.append(f"hit rate {hit_rate:.1%} < 50%")
    if failures:
        return VERDICT_DEMOTE, f"n={n}; " + " AND ".join(failures)
    return (VERDICT_HOLD,
            f"n={n}; median net {median_net:+.4f}% > 0 and hit rate {hit_rate:.1%} >= 50%")


# =============================================================================== read-only stores
class StoreUnreadable(RuntimeError):
    """A store could not be opened/read. Carries the operator-facing reason."""


def load_signal_rows(conn: sqlite3.Connection, *, start: date, as_of: date) -> list[Signal]:
    """Published ``hi52`` signals in ``[start, as_of]``, ascending by (day, symbol).

    Every row in ``prescreen_day_slots`` is a PUBLICATION (one per (day, symbol, strategy)), so the
    row set IS the signal population — ``evaluated``/``forwarded`` describe what the analyst budget
    then did with it and are deliberately not filtered on (module docstring).
    """
    try:
        rows = conn.execute(
            "SELECT d, symbol FROM prescreen_day_slots "
            "WHERE strategy_id = ? AND d >= ? AND d <= ? ORDER BY d, symbol",
            (STRATEGY_ID, start.isoformat(), as_of.isoformat()),
        ).fetchall()
    except sqlite3.Error as exc:
        raise StoreUnreadable(f"state.db: {type(exc).__name__}: {exc}") from exc
    out: list[Signal] = []
    for d, symbol in rows:
        try:
            out.append(Signal(date.fromisoformat(str(d)), str(symbol)))
        except ValueError:                 # a malformed day is a corrupt row, never a measurement
            continue
    return out


def load_sizing_limits(path: Path) -> tuple[Decimal, Decimal, str]:
    """``(swing_position_pct, overnight_gap_mult, provenance)`` from ``config/limits.yaml``.

    Read through the repo's own loader (``engine.core.config.load_yaml``) and deliberately NOT
    through ``LimitsEngine``: that path verifies the protected-store hash
    against a DB the running engine owns, and a read-only report must never need it — the hash is the
    gate's business, not a report's. Any failure (missing file, bad yaml, absent or non-numeric keys)
    falls back to the pinned registration values and SAYS so in the report: a silent fallback to a
    stale capital rule is precisely the failure this derivation exists to prevent.
    """
    try:
        raw = load_yaml(path)
        ptr = raw["limits"]["per_trade_risk"]
        pos, gap = Decimal(str(ptr["swing_position_pct"])), Decimal(str(ptr["overnight_gap_mult"]))
        if pos <= 0 or gap <= 0:
            raise ValueError(f"non-positive sizing limits: {pos}/{gap}")
        return pos, gap, f"{path}"
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one stated fallback
        return (SWING_POSITION_PCT, OVERNIGHT_GAP_MULT,
                f"pinned defaults ({type(exc).__name__} reading {path})")


def load_registration_equity(path: Path) -> tuple[Decimal, str]:
    """``(equity, provenance)`` the REGISTERED edge is derived at: the ``equity_floor_rung``.

    ``capital_base_inr × (1 + limits.equity_floor_rung.equity_pct_of_base/100)`` through the same
    repo loader :func:`load_sizing_limits` uses (and for the same reason — a report must never need
    the protected-store hash). A rung that is not a real floor (non-positive, or at/above the base,
    which would mean the book may never draw down at all) falls back to the pinned registration
    values and says so: the point of this equity is that it is the WORST one, and a bad read must
    not quietly make it the best one.
    """
    fallback = CAPITAL_BASE_INR * (Decimal("1") + EQUITY_FLOOR_RUNG_PCT / _HUNDRED)
    try:
        raw = load_yaml(path)
        base = Decimal(str(raw["capital_base_inr"]))
        rung_pct = Decimal(str(raw["limits"]["equity_floor_rung"]["equity_pct_of_base"]))
        equity = base * (Decimal("1") + rung_pct / _HUNDRED)
        if base <= 0 or equity <= 0 or equity >= base:
            raise ValueError(f"implausible equity floor rung: base={base} pct={rung_pct}")
        return equity, f"{path} equity_floor_rung ({base} x {100 + rung_pct}%)"
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one stated fallback
        return fallback, f"pinned default ({type(exc).__name__} reading {path})"


def load_min_equity(conn: sqlite3.Connection, *, start: date, as_of: date) -> Decimal | None:
    """The SMALLEST positive ``equity_snapshots.equity`` in ``[start, as_of]``, or ``None``.

    The smallest, not the latest: it is the tightest capital base any measured signal could have been
    sized under, so the notional it implies is the smallest and the cost floor the LARGEST — the
    conservative side, which for a kill criterion is the side that can only make DEMOTE easier. Rows
    are filtered on the DATE part of ``at`` so the stored IST offset never has to be parsed, and a
    row whose ``equity`` will not convert is skipped rather than crashing the report.
    """
    try:
        rows = conn.execute(
            "SELECT equity FROM equity_snapshots "
            "WHERE substr(at, 1, 10) >= ? AND substr(at, 1, 10) <= ?",
            (start.isoformat(), as_of.isoformat()),
        ).fetchall()
    except sqlite3.Error:          # no such table on a fresh/partial store — a stated fallback
        return None
    best: Decimal | None = None
    for (value,) in rows:
        try:
            eq = Decimal(str(value))
        except (ArithmeticError, ValueError):
            continue
        if eq > 0 and (best is None or eq < best):
            best = eq
    return best


def open_state_db(path: Path) -> sqlite3.Connection:
    """``state.db``, strictly read-only (``mode=ro`` URI)."""
    if not path.exists():
        raise StoreUnreadable(f"no such state.db: {path}")
    try:
        return sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        raise StoreUnreadable(f"cannot open {path} read-only: {exc}") from exc


def open_market_db(path: Path):
    """``market.duckdb``, read-only. A RUNNING engine holds the single-writer lock and this fails —
    the correct refusal, never something to route around."""
    import duckdb  # imported here so the pure measurement path needs no DuckDB at all

    if not path.exists():
        raise StoreUnreadable(f"no such market.duckdb: {path}")
    try:
        return duckdb.connect(str(path), read_only=True)
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one clear refusal
        raise StoreUnreadable(
            f"cannot open {path} read-only: {type(exc).__name__}: {exc}\n"
            "  DuckDB is single-writer: if the mt-engine service is running it owns this file."
        ) from exc


def load_series(conn, symbols: Sequence[str], *, start: date, end: date) -> dict[str, Series]:
    """Ascending ``bars_1d`` opens/closes in ``[start, end]`` for ``symbols``.

    ``start`` must sit far enough BEFORE the first signal for nothing — the signal day itself is the
    anchor — but ``end`` is the ``--as-of`` bound and is what keeps a past verdict reproducible.
    """
    if not symbols:
        return {}
    wanted = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    if not wanted:
        return {}
    placeholders = ", ".join("?" for _ in wanted)
    sql = (
        f'SELECT symbol, d, "open", "close" FROM bars_1d '
        f"WHERE symbol IN ({placeholders}) AND d >= ? AND d <= ? ORDER BY symbol, d"
    )
    try:
        rows = conn.execute(sql, [*wanted, start, end]).fetchall()
    except Exception as exc:  # noqa: BLE001 - a missing/renamed table is one clear refusal
        raise StoreUnreadable(f"bars_1d: {type(exc).__name__}: {exc}") from exc
    out: dict[str, Series] = {}
    for symbol, d, o, c in rows:
        sym = str(symbol)
        s = out.get(sym)
        if s is None:
            s = out[sym] = Series(symbol=sym, dates=[], open=[], close=[])
        s.dates.append(d.date() if isinstance(d, datetime) else d)
        s.open.append(float(o) if o is not None else math.nan)
        s.close.append(float(c) if c is not None else math.nan)
    return out


# =============================================================================== report
def build_report(
    signals: Sequence[Signal],
    series_by_symbol: Mapping[str, Series],
    *,
    cost_pct: float,
    as_of: date,
    start: date,
    horizons: Sequence[int] = HORIZONS,
    sizing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    trades, skipped = measure_all(
        signals, series_by_symbol, cost_pct=cost_pct, horizons=horizons
    )
    cells = summarize(trades, horizons)
    v, reason = verdict(cells)
    sizing = dict(sizing or {"notional_inr": str(REFERENCE_NOTIONAL),
                             "derivation": "registration defaults (no run-time inputs supplied)"})
    return {
        "meta": {
            "script": "scripts/hi52_forward_verdict.py",
            "strategy_id": STRATEGY_ID,
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "promotion_date": PROMOTION_DATE.isoformat(),
            "promotion_date_provenance": PROMOTION_DATE_PROVENANCE,
            "window": {"start": start.isoformat(), "as_of": as_of.isoformat()},
            "horizons_sessions": list(horizons),
            "verdict_horizon": VERDICT_HORIZON,
            "diagnostic_horizons": [k for k in horizons if k != VERDICT_HORIZON],
            "min_signals": MIN_SIGNALS,
            "reference_notional_inr": sizing.get("notional_inr", str(REFERENCE_NOTIONAL)),
            "sizing": sizing,
            "product": PRODUCT,
            "cost_round_trip_pct": round(cost_pct, 4),
            "entry_convention": "the OPEN of the journal day d itself (= the backtest's anchor: d is "
                                "the session after the trigger y; live fills land LATER on d, so "
                                "realized results trail this metric by that intraday drift)",
            "exit_convention": "CLOSE of the k-th session of the hold, entry session first: "
                               "close(d + k-1 sessions) / open(d) - 1  [= close(y+k)/open(y+1)]",
        },
        "population": {
            "signals_journalled": len(signals),
            "signals_measured": len(trades),
            "skipped": skipped,
            "symbols": sorted({s.symbol for s in signals}),
        },
        "horizons": cells,
        "verdict": v,
        "reason": reason,
        "trades": [
            {
                "symbol": t.symbol, "signal_date": t.signal_date.isoformat(),
                "entry_date": t.entry_date.isoformat(), "entry_px": round(t.entry_px, 2),
                "net": {str(k): round(val, 4) for k, val in sorted(t.net.items())},
            }
            for t in trades
        ],
    }


def _pct(v: Any) -> str:
    return "     n/a" if v is None else f"{float(v):+8.4f}"


def render(doc: Mapping[str, Any]) -> str:
    m, pop = doc["meta"], doc["population"]
    lines = [
        "=" * 92,
        "hi52 FORWARD-TEST VERDICT (promoted 2026-09-12; RECOMMEND mode IS the soak)",
        "=" * 92,
        f"window          : {m['window']['start']} -> {m['window']['as_of']}   "
        f"(promoted population from {m['promotion_date']})",
        f"  boundary      : {m['promotion_date_provenance']}",
        f"cost floor      : {m['cost_round_trip_pct']:.4f}% one CNC round trip at "
        f"Rs {m['reference_notional_inr']}"
        + ("   [OVERRIDE]" if m["sizing"].get("override") else ""),
        f"  notional      : {m['sizing'].get('derivation', 'n/a')}",
        f"entry           : {m['entry_convention']}",
        f"exit            : {m['exit_convention']}",
        f"signals         : {pop['signals_journalled']} journalled, {pop['signals_measured']} "
        "measured (skipped: "
        + ", ".join(f"{k} {v}" for k, v in pop["skipped"].items()) + ")",
        "-" * 92,
        f"{'horizon':>8}  {'n':>5}  {'median net':>11}  {'mean net':>10}  {'hit':>6}  "
        f"{'median gross':>13}",
    ]
    diagnostics = set(m.get("diagnostic_horizons") or ())
    for k in m["horizons_sessions"]:
        c = doc["horizons"].get(str(k), {})
        hit = "   n/a" if c.get("hit_rate") is None else f"{float(c['hit_rate']):6.1%}"
        # Every non-verdict cell is LABELLED, on its own row: the rule votes on T+20 alone and a
        # cell nobody pre-registered must never be quotable as an outcome.
        lines.append(
            f"{'T+' + str(k):>8}  {c.get('n', 0):>5}  {_pct(c.get('median_net')):>11}  "
            f"{_pct(c.get('mean_net')):>10}  {hit}  {_pct(c.get('median_gross')):>13}"
            + ("   (diagnostic only — not part of the rule)" if k in diagnostics else "")
        )
    lines += [
        "-" * 92,
        f"RULE            : DEMOTE if n >= {m['min_signals']} AND (median net at "
        f"T+{m['verdict_horizon']} <= 0 OR hit rate at T+{m['verdict_horizon']} < 50%)",
        f"VERDICT         : {doc['verdict']}   ({doc['reason']})",
    ]
    if doc["verdict"] == VERDICT_DEMOTE:
        lines += [
            "",
            "DEMOTION (both edits, or neither — a dangling edge key reads as a live registration):",
            "  1. re-add hi52.STRATEGY_ID to engine.ops.main.NO_EDGE_SHADOW_STRATEGIES",
            "  2. delete hi52.expected_edge_pct from config/settings.yaml",
            "  …as one owner-visible commit citing this report.",
        ]
    elif doc["verdict"] == VERDICT_INSUFFICIENT:
        lines.append(
            f"  Review cadence: re-run at {' and '.join(str(n) for n in REVIEW_AT_SIGNALS)} "
            "signals. First-20-signal outcomes are VALIDATION, not income."
        )
    lines.append("=" * 92)
    return "\n".join(lines)


# =============================================================================== CLI
def _date(s: str) -> date:
    return date.fromisoformat(s)


def _positive(s: str) -> Decimal:
    """A positive, finite rupee amount — argparse rejects anything else as a usage error rather than
    letting it reach the cost model (``--equity 0`` would divide by zero in the sizing cap)."""
    try:
        value = Decimal(s)
    except ArithmeticError as exc:
        raise argparse.ArgumentTypeError(f"not a number: {s!r}") from exc
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive amount in rupees, got {s!r}")
    return value


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="hi52 forward-test verdict (IMPLEMENTATION_PLAN.md 8.6 addendum). "
                    "Read-only against state.db + market.duckdb; always exits 0.",
    )
    ap.add_argument("--state-db", type=Path, default=repo_root() / "data" / "state.db",
                    help="path to state.db (prescreen_day_slots)")
    ap.add_argument("--db", type=Path, default=repo_root() / "data" / "market.duckdb",
                    help="path to market.duckdb (bars_1d)")
    ap.add_argument("--as-of", type=_date, default=None, metavar="YYYY-MM-DD",
                    help="bound BOTH stores at this date (default: today) — a past verdict stays "
                         "reproducible because no later bar is read")
    ap.add_argument("--from", dest="start", type=_date, default=PROMOTION_DATE,
                    help=f"first signal day (default {PROMOTION_DATE.isoformat()}, the promotion; "
                         "earlier rows are the SHADOW population on a different rule)")
    ap.add_argument("--json", dest="json_path", type=Path, default=None,
                    help="also write the raw numbers to this JSON path")
    ap.add_argument("--limits", type=Path, default=repo_root() / "config" / "limits.yaml",
                    help="path to limits.yaml (§7.1 per_trade_risk — the notional derivation)")
    ap.add_argument("--equity", type=_positive, default=None, metavar="INR",
                    help="size the cost floor at this equity instead of the smallest snapshot in "
                         "the window (a what-if; the derivation is printed either way)")
    ap.add_argument("--notional", type=_positive, default=None, metavar="INR",
                    help="charge the round trip at this notional and SKIP the §7.1 derivation "
                         "entirely (a what-if; the report says OVERRIDE and the verdict it prints "
                         "is not the kill criterion's)")
    return ap


def resolve_cost_floor(
    *, limits_path: Path, equity_override: Decimal | None, min_equity: Decimal | None,
    notional_override: Decimal | None = None,
) -> tuple[float, dict[str, Any]]:
    """``(round-trip %, the derivation as a dict)`` — the ONE place the cost floor is decided.

    Falls back, never fails: an unreadable ``limits.yaml`` uses the pinned registration values and a
    refused equity (non-positive, or absent from the store) uses :data:`REFERENCE_EQUITY_INR`. Every
    fallback names itself in ``derivation``, which the report prints — an unexplained cost floor on a
    kill criterion is worse than a stated approximate one.

    ``notional_override`` (``--notional``) bypasses the §7.1 derivation entirely. It exists for
    what-ifs ("what would this verdict have been at ₹8,000?") and it SHOUTS: the derivation string
    starts with ``OVERRIDE`` and the report prints it, because a hand-picked notional is exactly the
    defect the derivation was introduced to remove and a quoted number must never hide it.
    """
    position_pct, gap_mult, limits_src = load_sizing_limits(limits_path)
    if notional_override is not None:
        # Rejected at the CLI by `_positive` before it ever reaches here; this is the programming
        # -error guard, because a zero notional would divide by zero inside the cost model and a
        # negative one would print a nonsense floor with a straight face.
        if not notional_override.is_finite() or notional_override <= 0:
            raise ValueError(f"notional override must be positive, got {notional_override}")
        return float(cost_floor_pct(notional_override)), {
            "notional_inr": str(round(notional_override, 2)),
            "equity_inr": None,
            "equity_source": "OVERRIDE (--notional): no equity read",
            "swing_position_pct": str(position_pct),
            "overnight_gap_mult": str(gap_mult),
            "stop_pct": str(STOP_PCT),
            "limits_source": limits_src,
            "override": True,
            "derivation": (
                f"OVERRIDE (--notional Rs {round(notional_override, 2)}): the §7.1 sizing "
                f"derivation was NOT used — this is a what-if, not the kill criterion's floor"
            ),
        }
    if equity_override is not None:
        equity, equity_src = equity_override, "--equity override"
    elif min_equity is not None:
        equity, equity_src = min_equity, "smallest equity_snapshots reading in the window"
    else:
        equity, equity_src = REFERENCE_EQUITY_INR, "registration default (no snapshot in window)"
    try:
        notional = sizing_notional(
            equity, position_pct=position_pct, gap_mult=gap_mult, stop_pct=STOP_PCT
        )
    except ValueError:
        equity, equity_src = REFERENCE_EQUITY_INR, f"{equity_src} REFUSED (non-positive); default"
        notional = REFERENCE_NOTIONAL
    cost_pct = float(cost_floor_pct(notional))
    return cost_pct, {
        "notional_inr": str(round(notional, 2)),
        "equity_inr": str(equity),
        "equity_source": equity_src,
        "swing_position_pct": str(position_pct),
        "overnight_gap_mult": str(gap_mult),
        "stop_pct": str(STOP_PCT),
        "limits_source": limits_src,
        "override": False,
        "derivation": (
            f"§7.1 per_trade_risk cap: equity Rs {equity} x {position_pct}% / "
            f"({gap_mult}x {STOP_PCT}% stop) = Rs {round(notional, 2)}  "
            f"[equity: {equity_src}; limits: {limits_src}] — integer-qty rounding lands the real "
            f"notional at or below this, so the floor is a lower bound"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # IST, never the host's local date: the sessions being counted are NSE's, and a run from a
    # machine in another timezone must not silently shift the --as-of bound by a day.
    as_of = args.as_of or datetime.now(IST).date()
    start = args.start
    if as_of < start:
        print(f"--as-of {as_of} is before --from {start}: no population.", file=sys.stderr)

    try:
        state = open_state_db(Path(args.state_db))
        try:
            signals = load_signal_rows(state, start=start, as_of=as_of)
            # Bounded by --as-of like everything else, so a past verdict stays reproducible.
            min_equity = load_min_equity(state, start=start, as_of=as_of)
        finally:
            state.close()
        market = open_market_db(Path(args.db))
        try:
            series = load_series(
                market, [s.symbol for s in signals], start=start, end=as_of
            )
        finally:
            market.close()
    except StoreUnreadable as exc:
        # A store that cannot be read is not an INSUFFICIENT population — saying so would put a
        # verdict on evidence nobody looked at. Report it as its own state and still exit 0.
        print(f"VERDICT         : {VERDICT_UNAVAILABLE}   ({exc})")
        return 0

    cost_pct, sizing = resolve_cost_floor(
        limits_path=Path(args.limits), equity_override=args.equity, min_equity=min_equity,
        notional_override=args.notional,
    )
    doc = build_report(signals, series, cost_pct=cost_pct, as_of=as_of, start=start, sizing=sizing)
    print(render(doc))
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(doc, indent=2), encoding="utf-8")
        print(f"json            : {args.json_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
