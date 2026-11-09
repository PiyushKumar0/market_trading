#!/usr/bin/env python
"""``hi52`` FORWARD-TEST VERDICT — the mechanical kill criterion for the 2026-09-12 promotion.

``hi52`` left SHADOW on 2026-09-12: an expected edge is registered and its candidates can reach
RECOMMEND. The backtest behind that has its edge in an INDEX cell labelled by CURRENT membership
applied backwards - a survivorship-tainted proxy - so RECOMMEND mode **is** the forward test, and
this script is its written, mechanical end. It changes nothing: it reads two stores read-only and
prints a verdict.

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
* **Entry — the OPEN of the journal day ``d`` itself**, exactly ``scripts/backtest_hi52.py``'s
  anchor. The study's signal day is the TRIGGER session ``y`` and it enters at the next open; the
  sweep that journals ``d`` runs on the session AFTER ``y``, so ``open(d)`` IS the study's entry
  price. A kill criterion must test the quantity the registered edge was measured as. Consequence:
  the LIVE fill happens later on ``d`` (the owner executes by hand), so realized results trail this
  metric by that intraday drift; a persistent gap is an execution finding, never a reason to
  re-anchor the rule.
* **Exit — the CLOSE of the ``k``-th session of the HOLD, the entry session counting as the first**:
  ``return = close(d + (k-1) sessions) / open(d) - 1`` (the study's ``close(y + k) / open(y + 1)``,
  the repo event-study convention of ``scripts/event_study.py``, ``backtest_hi52.py``). Sessions are
  positions in THAT symbol's own ``bars_1d`` series, so holidays and halts are handled by
  construction, never by calendar arithmetic.
* **Costs — one full CNC round trip from the repo's own ``CostModel``**, spread included
  (``breakeven_pct``, never the fees-only view), charged at the notional **per-trade risk sizing
  produces for this rule**. A swing position is capped by ``per_trade_risk``: ``swing_position_pct``
  (2.0%) of equity at ``overnight_gap_mult`` (2.0×; 2.5× at registration) the stop distance, and
  ``hi52``'s stop is 6% - so ``notional <= equity × 2% / (2.0 × 6%)`` = ``equity / 6``
  (:func:`sizing_notional`). Under-charging biases BOTH demote clauses toward keeping an unvalidated
  rule trading. The equity is the SMALLEST snapshot in the measured window (``state.db``): a
  shrinking book raises the floor. Residual: integer-qty rounding lands the real notional AT OR
  BELOW the cap, so the charged floor is a lower bound by a few hundredths of a pp; read a verdict
  sitting on the line with that margin in mind. ``--notional`` overrides outright for a what-if and
  the report says ``OVERRIDE``.
  **ONE derivation, two readings:** :func:`registered_edge_pct` computes ``settings.hi52
  .expected_edge_pct`` from the SAME :func:`sizing_notional` + :func:`cost_floor_pct` pair, at the
  smallest equity the risk table lets the book open a position at (the ``equity_floor_rung``: −10%
  of the ₹40,000 base ⇒ ₹36,000 ⇒ ₹6,000 ⇒ 0.4982% ⇒ 2.0336 − 0.4982 = **1.53**, rounded DOWN). The
  unit tests (``test_hi52_edge.py``, ``test_hi52_forward_verdict.py``) pin the registration INPUTS
  (base, rung, stop, cost tables), so a change fails a test instead of leaving a stale edge registered.
* **Horizons are measured on their OWN event sets.** A signal old enough for T+10 but not T+20
  counts in the T+10 cell only, so cells have different ``n`` and are not comparable - which is why
  the verdict reads T+20 and nothing else. **T+5 and T+10 are printed as DIAGNOSTICS**: the registered
  edge is the T+20 number, so neither may be read as a verdict.
* **A signal whose ``d`` is missing from that symbol's ``bars_1d`` series is NOT measured.** ``d``
  AFTER the last stored session is ``no_entry_yet`` (the ordinary forward-test state); ``d`` missing
  from INSIDE the stored range is ``unanchored``, a hole that also corrupts the horizons' session count.
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
import os
import sqlite3
import statistics
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

from engine.core.clock import IST  # noqa: E402
from engine.core.config import load_yaml, repo_root  # noqa: E402
from engine.learning.hi52_forward import (  # noqa: E402
    HORIZONS,
    MIN_SIGNALS,
    PROMOTION_DATE,
    VERDICT_HORIZON,
    Measured,
    Series,
    Signal,
    StoreUnreadable,
    load_series,
    load_signal_rows,
    measure_all,
    measure_signal,  # noqa: F401 - re-exported
)
from engine.strategy import hi52_edge  # noqa: E402
from engine.strategy.hi52_edge import (  # noqa: E402,F401 - re-exported; ONE derivation, in the engine
    MEASURED_T20_MEDIAN_GROSS_PCT,
    PRODUCT,
    STOP_PCT,
    sizing_notional,
)
from engine.strategy.scanners.hi52 import STRATEGY_ID  # noqa: E402

# =============================================================================== pinned constants
#: Printed with every verdict: the boundary is a PREDICTION about when the change deploys and neither
#: store records the deploy. Rows from a sweep on pre-promotion code look exactly like promoted ones
#: and the kill rule would measure a rule the engine was not running. Confirm the first promoted
#: session against the deploy and pass ``--from`` whenever it differs.
PROMOTION_DATE_PROVENANCE = (
    "ASSUMED = first session after the 2026-09-12 promotion commit; confirm against the deploy date "
    "and pass --from if the promotion shipped later"
)

#: Everything else is labelled "diagnostic" in the report, so no one can quote it as an outcome.
DIAGNOSTIC_HORIZONS: tuple[int, ...] = tuple(k for k in HORIZONS if k != VERDICT_HORIZON)

#: ``per_trade_risk`` inputs of the notional derivation. FALLBACKS only: :func:`load_sizing_limits`
#: reads the live ``config/limits.yaml``; these are charged when it cannot be read (a report never
#: fails on a missing input, it states it).
SWING_POSITION_PCT = Decimal("2.0")
OVERNIGHT_GAP_MULT = Decimal("2.0")

#: Equity the notional is derived at when ``state.db`` shows no usable snapshot in the window.
#: Overridable with ``--equity`` (also the "what if the book were X" run).
REFERENCE_EQUITY_INR = Decimal("39256.65")

#: Capital anchor and the go-flat rung. FALLBACKS only (:func:`load_registration_equity` reads the
#: live file). ₹40,000 × (1 − 10%) = ₹36,000 is the SMALLEST equity at which the risk table still
#: lets a new position open: at or below the rung the platform goes flat and CLOSE_ONLY.
CAPITAL_BASE_INR = Decimal("40000")
EQUITY_FLOOR_RUNG_PCT = Decimal("-10.0")

_HUNDRED = Decimal("100")


#: The notional at the reference equity — what the report charges when the store shows nothing.
REFERENCE_NOTIONAL = sizing_notional(
    REFERENCE_EQUITY_INR, position_pct=SWING_POSITION_PCT,
    gap_mult=OVERNIGHT_GAP_MULT, stop_pct=STOP_PCT,
)


def cost_floor_pct(notional: Decimal) -> Decimal:
    """One full CNC round trip at ``notional``, in percent — the repo's own ``CostModel``.

    Delegates to :func:`engine.strategy.hi52_edge.cost_floor_pct`: THE one place a notional becomes a
    cost, so the registered edge (:func:`registered_edge_pct`) and the forward nets are charged by
    the same code against the same tables.
    """
    return hi52_edge.cost_floor_pct(notional)


def registered_edge_pct(limits_path: Path | None = None) -> Decimal:
    """The number ``config/settings.yaml`` registers as ``hi52.expected_edge_pct`` — DERIVED here.

    ``measured T+20 median GROSS − one CNC round trip at the risk-sized notional``, rounded with
    ``ROUND_FLOOR`` to two decimals: rounding an edge UP can buy a candidate a cost-gate pass it did
    not earn, and FLOOR (not DOWN) rounds a negative result away from zero too.

    The equity is the **``equity_floor_rung``**, not today's book: ``capital_base_inr`` × (1 +
    ``equity_pct_of_base``/100) = ₹36,000. A registered edge is a constant read on every candidate
    for months and the cost floor RISES as the book shrinks, so it is derived at the worst permitted
    book, not the largest. At the rung: ₹6,000 ⇒ 0.4982%, 2.0336 − 0.4982 = 1.5354 ⇒ **1.53**.

    The arithmetic lives in :func:`engine.strategy.hi52_edge.edge_at` (the engine's live cap,
    ``StrategyEdges``, uses it too); this function supplies the file reads and their fallbacks.
    ``tests/unit/test_hi52_edge.py`` pins ``settings.yaml``'s value to it.
    """
    path = limits_path or (repo_root() / "config" / "limits.yaml")
    position_pct, gap_mult, _ = load_sizing_limits(path)
    base, rung_pct, _ = _load_base_and_rung(path)
    return hi52_edge.edge_at(base, rung_pct, position_pct, gap_mult, stop_pct=STOP_PCT)


VERDICT_DEMOTE = "DEMOTE"
VERDICT_HOLD = "HOLD"
VERDICT_INSUFFICIENT = "INSUFFICIENT"
VERDICT_UNAVAILABLE = "UNAVAILABLE"

#: Review cadence: run this at 20 and at 40 signals.
REVIEW_AT_SIGNALS: tuple[int, ...] = (20, 40)


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

    Reads ONLY the T+``VERDICT_HORIZON`` cell; no severity ladder, no second horizon voting.
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
def load_sizing_limits(path: Path) -> tuple[Decimal, Decimal, str]:
    """``(swing_position_pct, overnight_gap_mult, provenance)`` from ``config/limits.yaml``.

    Read through ``engine.core.config.load_yaml``, deliberately NOT ``LimitsEngine`` (its
    protected-store hash check needs a DB the running engine owns). Any failure falls back to the
    pinned registration values and SAYS so in the report: a silent fallback to a stale capital rule
    is what this derivation exists to prevent.
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


def _load_base_and_rung(path: Path) -> tuple[Decimal, Decimal, str]:
    """``(capital_base_inr, equity_floor_rung pct, provenance)``, or the pinned registration values.

    Same repo loader and same fallback discipline as :func:`load_sizing_limits`. A rung that is not a
    real floor (non-positive equity, or at/above the base) falls back and says so.
    """
    try:
        raw = load_yaml(path)
        base = Decimal(str(raw["capital_base_inr"]))
        rung_pct = Decimal(str(raw["limits"]["equity_floor_rung"]["equity_pct_of_base"]))
        equity = base * (Decimal("1") + rung_pct / _HUNDRED)
        if base <= 0 or equity <= 0 or equity >= base:
            raise ValueError(f"implausible equity floor rung: base={base} pct={rung_pct}")
        return base, rung_pct, f"{path}"
    except Exception as exc:  # noqa: BLE001 - every failure mode becomes one stated fallback
        return (CAPITAL_BASE_INR, EQUITY_FLOOR_RUNG_PCT,
                f"pinned default ({type(exc).__name__} reading {path})")


def load_registration_equity(path: Path) -> tuple[Decimal, str]:
    """``(equity, provenance)`` the REGISTERED edge is derived at: the ``equity_floor_rung``.

    ``capital_base_inr × (1 + limits.equity_floor_rung.equity_pct_of_base/100)``, read as in
    :func:`load_sizing_limits`. A rung that is not a real floor falls back to the pinned values and
    says so: this equity is the WORST one, and a bad read must not quietly make it the best.
    """
    base, rung_pct, src = _load_base_and_rung(path)
    equity = base * (Decimal("1") + rung_pct / _HUNDRED)
    if src.startswith("pinned default"):
        return equity, src
    return equity, f"{path} equity_floor_rung ({base} x {100 + rung_pct}%)"


def load_min_equity(conn: sqlite3.Connection, *, start: date, as_of: date) -> Decimal | None:
    """The SMALLEST positive ``equity_snapshots.equity`` in ``[start, as_of]``, or ``None``.

    The smallest, not the latest: the tightest base any measured signal was sized under, hence the
    LARGEST cost floor (the side that can only make DEMOTE easier). Rows are filtered on the DATE
    part of ``at`` so the stored IST offset is never parsed; an unconvertible ``equity`` is skipped.
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
        # Non-verdict cells are labelled on their own row: only T+20 votes.
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
    """A positive, finite rupee amount; anything else is a usage error (``--equity 0`` would divide by zero)."""
    try:
        value = Decimal(s)
    except ArithmeticError as exc:
        raise argparse.ArgumentTypeError(f"not a number: {s!r}") from exc
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive amount in rupees, got {s!r}")
    return value


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="hi52 forward-test verdict. "
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
                    help="path to limits.yaml (per_trade_risk — the notional derivation)")
    ap.add_argument("--equity", type=_positive, default=None, metavar="INR",
                    help="size the cost floor at this equity instead of the smallest snapshot in "
                         "the window (a what-if; the derivation is printed either way)")
    ap.add_argument("--notional", type=_positive, default=None, metavar="INR",
                    help="charge the round trip at this notional and SKIP the per_trade_risk derivation "
                         "entirely (a what-if; the report says OVERRIDE and the verdict it prints "
                         "is not the kill criterion's)")
    return ap


def resolve_cost_floor(
    *, limits_path: Path, equity_override: Decimal | None, min_equity: Decimal | None,
    notional_override: Decimal | None = None,
    stop_pct: Decimal = STOP_PCT,
    fallback_equity: tuple[Decimal, str] | None = None,
) -> tuple[float, dict[str, Any]]:
    """``(round-trip %, the derivation as a dict)`` — the ONE place the cost floor is decided.

    Falls back, never fails: an unreadable ``limits.yaml`` uses the pinned values and a refused
    equity (non-positive, or absent from the store) uses :data:`REFERENCE_EQUITY_INR`. Every
    fallback names itself in ``derivation``, which the report prints.

    ``notional_override`` (``--notional``) bypasses the sizing derivation for what-ifs and SHOUTS:
    the derivation string starts with ``OVERRIDE``, so a hand-picked notional never hides.

    ``stop_pct`` and ``fallback_equity`` let another rule (``scripts/cat_shadow_verdict.py``) be
    charged by this SAME derivation at its own stop; ``fallback_equity`` = ``(equity, source)``
    replaces the default in both the no-snapshot case and the non-positive refusal.
    """
    position_pct, gap_mult, limits_src = load_sizing_limits(limits_path)
    if notional_override is not None:
        # Programming-error guard (the CLI's `_positive` rejects these first): zero would divide by
        # zero in the cost model, a negative would print a nonsense floor.
        if not notional_override.is_finite() or notional_override <= 0:
            raise ValueError(f"notional override must be positive, got {notional_override}")
        return float(cost_floor_pct(notional_override)), {
            "notional_inr": str(round(notional_override, 2)),
            "equity_inr": None,
            "equity_source": "OVERRIDE (--notional): no equity read",
            "swing_position_pct": str(position_pct),
            "overnight_gap_mult": str(gap_mult),
            "stop_pct": str(stop_pct),
            "limits_source": limits_src,
            "override": True,
            "derivation": (
                f"OVERRIDE (--notional Rs {round(notional_override, 2)}): the per-trade risk sizing "
                f"derivation was NOT used — this is a what-if, not the kill criterion's floor"
            ),
        }
    if equity_override is not None:
        equity, equity_src = equity_override, "--equity override"
    elif min_equity is not None:
        equity, equity_src = min_equity, "smallest equity_snapshots reading in the window"
    elif fallback_equity is not None:
        equity, equity_src = fallback_equity
    else:
        equity, equity_src = REFERENCE_EQUITY_INR, "registration default (no snapshot in window)"
    try:
        notional = sizing_notional(
            equity, position_pct=position_pct, gap_mult=gap_mult, stop_pct=stop_pct
        )
    except ValueError:
        if fallback_equity is None:
            equity, equity_src = REFERENCE_EQUITY_INR, f"{equity_src} REFUSED (non-positive); default"
            notional = REFERENCE_NOTIONAL
        else:
            equity = fallback_equity[0]
            equity_src = f"{equity_src} REFUSED (non-positive); {fallback_equity[1]}"
            notional = sizing_notional(
                equity, position_pct=position_pct, gap_mult=gap_mult, stop_pct=stop_pct
            )
    cost_pct = float(cost_floor_pct(notional))
    return cost_pct, {
        "notional_inr": str(round(notional, 2)),
        "equity_inr": str(equity),
        "equity_source": equity_src,
        "swing_position_pct": str(position_pct),
        "overnight_gap_mult": str(gap_mult),
        "stop_pct": str(stop_pct),
        "limits_source": limits_src,
        "override": False,
        "derivation": (
            f"per_trade_risk cap: equity Rs {equity} x {position_pct}% / "
            f"({gap_mult}x {stop_pct}% stop) = Rs {round(notional, 2)}  "
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
                lambda sql, params: market.execute(sql, params).fetchall(),
                [s.symbol for s in signals], start=start, end=as_of,
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
