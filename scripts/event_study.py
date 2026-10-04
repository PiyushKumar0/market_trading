#!/usr/bin/env python
"""§2.7 event-study proxy backtest + §2.8.4 corporate-filings legs — the `cat`/filings validation
prerequisite before any headline corpus or filings-typed origination.

No historical archive of ET/Moneycontrol RSS headlines exists, so `cat` cannot be conventionally
backtested at design time (§2.7 "validation reality"). Interim evidence comes from an event study on
signals that DO have deep history — it validates the two mechanical legs `cat` relies on WITHOUT
headlines:

* **PEAD leg** — post-event drift conditioned on the T-day reaction sign. Event set = historical
  earnings dates (``earnings_calendar``) + bhavcopy gap/volume events (|open-gap vs prior close| ≥ 2%
  with day volume ≥ 2× the trailing 20-day median). Reaction sign = ``sign(close_T − open_T)`` (the
  §6.1/O13 convention; ``close_T == open_T`` ⇒ reaction 0 ⇒ event dropped, conservative). We measure
  the signed drift T+1..T+5 (positive ⇒ the reaction continued).
* **Confirmation leg** — the §6.1 `cat` price/volume confirmation filter: does entering only on a T+1
  bar that closes beyond ``cat.confirm_move_pct`` in the reaction direction with volume ≥
  ``cat.confirm_vol_mult`` × the 20-day median improve the forward drift? Entry at close_{T+1},
  horizons T+2..T+5.

**§2.8.4 filings legs (unlike news, these ARE conventionally backtestable — archives exist):**

* **Insider-buy leg** — events where the trailing-10-session sum of OPEN-MARKET insider BUY value
  (``insider_trades``; ESOP/Gift/inter-se/pledge-invocation/preferential/rights/bonus excluded by the
  §2.8.2 acq_mode taxonomy) first crosses ≥ ``filings.insider_min_value_inr``. Directional (long)
  forward drift T+1,2,3,5,10,20 at close_T (the first close at which the filing was knowable, PIT).
* **Results-filing leg** — reaction-sign PEAD (as above) sourced from ``results_filings`` broadcast
  dates (fallback: historical ``earnings_calendar`` rows) — replaces the perpetually-n=0 earnings leg.
* **Pledge-delta leg** — events where the promoter-category pledged-% changes QoQ by
  ≥ ``filings.pledge_delta_min_pct``; increase = negative-catalyst cohort, decrease = positive.
  Directional forward drift T+1,5,10,20 keyed off the quarter row's ``broadcast_dt`` (PIT, not qtr end).

Every leg reports BOTH **gross** and **net** (net = gross − one round-trip CNC cost, ``CostModel``
breakeven at the reference notional) with honest stats — hit rate, mean/median drift. Negative/flat
results are surfaced, not massaged (C9); every filings leg degrades to an honest ``n=0`` section when
its table is empty. Output: ``data/reports/event_study_<ts>.md`` + ``.json`` (timestamped since
WO-16 — the pre-WO-16 fixed name overwrote the previous run's evidence). Standalone/blocking
(offline research tool). Exit codes: 0 = ran; 2 = no symbols/bars resolved.

**Entry-fill mechanics (WO-16, 2026-08-14).** Every leg's signal is knowable at the CLOSE of its
event session T (the filings legs by construction — :func:`entry_session_index` maps a broadcast to
the first close at which it was public; the PEAD legs because the reaction sign is read off T's own
open/close). ``--entry-fill`` decides where that signal is FILLED:

* ``next_open`` (**default**) — fill at ``open_(T+1)``, the first price actually reachable by an
  order placed after observing close_T. Horizon T+k then measures ``close_(T+k) / open_(T+1)``.
* ``close_t`` — the pre-WO-16 convention: fill at ``close_T`` itself. For the filings legs this is
  defensible (the broadcast timestamp precedes the close, so the information was genuinely in hand
  before the auction) but it still books the T→T+1 overnight gap that no post-close order can
  capture; for the reaction-sign PEAD/confirmation legs it is the WO-2 same-bar class outright (the
  signal is DERIVED from close_T and filled AT close_T). Kept only for old-vs-new comparison.

The cost subtracted from every NET column is the full round-trip friction — statutory fees **plus**
the measured bid-ask spread (WO-2) — via ``CostModel.breakeven_pct``; the report header prints the
two components separately so the spread is never invisible.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401  native import-order guard
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402

# --- the §2.8.2/§2.8.4 crossing primitives this script VALIDATED, now owned by the engine package.
#     Promoted 2026-08-17 with the §6.1 `ins` leg: a LIVE EOD job computes crossings on the decision
#     path, and engine code may not path-load a loose script at runtime (the shim
#     engine.datafeeds.filings_events used to carry, flagged in its own docstring). The dependency is
#     inverted, not copied — this module still exports the same names at module scope, so
#     `es.insider_buy_events(...)` and friends resolve exactly as before for
#     scripts/filings_experiments.py and tests/unit/test_event_study_filings.py, and the study and the
#     live `ins_crossings` job run the SAME bytes. Editing the rule means editing
#     engine/datafeeds/insider_crossings.py and re-running this study (§2.8.4 / WO-3 margin floor).
from engine.datafeeds.insider_crossings import (  # noqa: E402
    INSIDER_ACQ_MODE_EXCLUSIONS,  # noqa: F401  - re-exported for `es.`-style consumers
    INSIDER_TRAILING_SESSIONS,
    MARKET_CLOSE_IST,  # noqa: F401  - re-exported for `es.`-style consumers
    after_hours,  # noqa: F401  - re-exported for `es.`-style consumers
    entry_session_index,
    insider_buy_events,
    insider_cluster_events,  # noqa: F401  - re-exported for `es.`-style consumers
    is_open_market_buy,  # noqa: F401  - re-exported for `es.`-style consumers
    is_open_market_sell,  # noqa: F401  - re-exported for `es.`-style consumers
    to_ist,
)
from engine.marketdata.store import DailyBar, MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

_log = get_logger("scripts.event_study")

HORIZONS = (1, 2, 3, 4, 5)              # T+1 .. T+5 drift horizons (PEAD / results-filing legs)
INSIDER_HORIZONS = (1, 2, 3, 5, 10, 20)  # §2.8.4 insider-buy directional drift horizons
PLEDGE_HORIZONS = (1, 5, 10, 20)         # §2.8.4 pledge-delta directional drift horizons
GAP_MIN = 0.02                          # |open-gap vs prior close| ≥ 2% (documented threshold)
VOL_MULT = 2.0                          # day volume ≥ 2× trailing 20d median
VOL_WINDOW = 20
CAT_CONFIRM_MOVE_PCT = 1.0              # §6.3 cat.confirm_move_pct default
CAT_CONFIRM_VOL_MULT = 1.5             # §6.3 cat.confirm_vol_mult default
REFERENCE_NOTIONAL = Decimal("20000")

#: WO-16 entry-fill conventions (see the module docstring). ``next_open`` is the corrected default.
ENTRY_FILL_NEXT_OPEN = "next_open"
ENTRY_FILL_CLOSE_T = "close_t"
ENTRY_FILLS = (ENTRY_FILL_NEXT_OPEN, ENTRY_FILL_CLOSE_T)
DEFAULT_ENTRY_FILL = ENTRY_FILL_NEXT_OPEN

# --------------------------------------------------------------------------- pure detection/measurement
@dataclass
class _Row:
    d: date
    open: float
    high: float
    low: float
    close: float
    volume: float


def _rows(bars: list[DailyBar]) -> list[_Row]:
    return [
        _Row(b.d, float(b.open), float(b.high), float(b.low), float(b.close), float(b.volume))
        for b in bars
    ]


def _median(vals: list[float]) -> float:
    return statistics.median(vals) if vals else float("nan")


#: Back-compat alias for the pre-promotion private name. ``scripts/filings_experiments.py`` reaches
#: it as ``es._to_ist`` (the loose-script consumers address this module by attribute); keeping the
#: alias means the 2026-08-17 promotion changed no consumer.
_to_ist = to_ist


def gap_volume_event_days(bars: list[DailyBar]) -> list[date]:
    """Days with |open-gap vs prior close| ≥ ``GAP_MIN`` AND volume ≥ ``VOL_MULT`` × 20d median.

    Pure over an ascending daily series. The 20-day median EXCLUDES the event day itself (its own
    spike must not inflate the baseline). Needs a prior close (gap) and 20 prior days (median).
    """
    rows = _rows(bars)
    out: list[date] = []
    for i in range(1, len(rows)):
        if i < VOL_WINDOW:
            continue
        prev_close = rows[i - 1].close
        if prev_close <= 0:
            continue
        gap = (rows[i].open - prev_close) / prev_close
        med = _median([r.volume for r in rows[i - VOL_WINDOW:i]])
        if med <= 0:
            continue
        if abs(gap) >= GAP_MIN and rows[i].volume >= VOL_MULT * med:
            out.append(rows[i].d)
    return out


# ------------------------------------------------------------------ §2.8.4 point-in-time entry mapping
# ``after_hours`` / ``entry_session_index`` / ``is_open_market_buy`` / ``is_open_market_sell`` /
# ``insider_cluster_events`` / ``insider_buy_events`` moved to engine.datafeeds.insider_crossings on
# 2026-08-17 (see the import block above) — imported back, not reimplemented, so this study and the
# live §6.1 `ins` leg cannot diverge.
def event_session_indices(sessions: list[date], broadcast_dts: list[datetime]) -> list[int]:
    """Unique, ascending PIT entry-session indices for a list of broadcast timestamps (§2.8.4). Two
    filings mapping to the same session (e.g. a result's standalone + consolidated rows share a
    broadcast_dt) collapse to one event."""
    idxs: set[int] = set()
    for bdt in broadcast_dts:
        if bdt is None:
            continue
        si = entry_session_index(sessions, bdt)
        if si is not None:
            idxs.add(si)
    return sorted(idxs)


def promoter_pledge_by_quarter(rows: list[dict]) -> list[dict]:
    """One promoter-category pledged-% record per quarter, ascending by ``qtr_end`` (§2.8.2). Promoter
    = a category string containing 'promoter' (case-insensitive — the SEBI aggregate '(A) Promoter &
    Promoter Group' row). If a quarter carries several promoter rows the last-seen wins (deterministic;
    the PK (symbol, qtr_end, category) makes duplicates unusual)."""
    by_q: dict = {}
    for r in rows:
        cat = str(r.get("category") or "")
        if "promoter" not in cat.lower():
            continue
        q = r.get("qtr_end")
        if q is None:
            continue
        by_q[q] = {
            "qtr_end": q,
            "pledged_pct": r.get("pledged_pct"),
            "broadcast_dt": r.get("broadcast_dt"),
        }
    return [by_q[q] for q in sorted(by_q)]


def pledge_delta_events(rows: list[dict], threshold: float) -> list[dict]:
    """QoQ promoter pledged-% crossings (§2.8.2). One dict per event:
    ``{qtr_end, broadcast_dt, direction, delta}`` where ``direction`` is 'increase' (pledge rose —
    the negative-catalyst cohort) or 'decrease' (fell — the positive cohort). Event time = the LATER
    quarter's ``broadcast_dt`` (point-in-time, NOT the quarter end). Pairs with a missing pledged_pct
    on either side are skipped."""
    quarters = promoter_pledge_by_quarter(rows)
    out: list[dict] = []
    for prev, cur in zip(quarters, quarters[1:], strict=False):
        p, c = prev["pledged_pct"], cur["pledged_pct"]
        if p is None or c is None:
            continue
        delta = float(c) - float(p)
        if abs(delta) >= threshold:
            out.append({
                "qtr_end": cur["qtr_end"],
                "broadcast_dt": cur["broadcast_dt"],
                "direction": "increase" if delta > 0 else "decrease",
                "delta": delta,
            })
    return out


@dataclass
class Observation:
    """One reaction-sign event: sign + signed drift (%) per horizon for both PEAD and confirmation legs,
    each reported gross AND net of one round-trip CNC cost."""

    symbol: str
    event_date: date
    kind: str                          # 'earnings' | 'gap_volume' | 'results_filing'
    reaction_sign: int                 # +1 / -1 (0 events are dropped)
    pead_net: dict[int, float] = field(default_factory=dict)          # T+k signed drift, %, net
    pead_gross: dict[int, float] = field(default_factory=dict)
    confirmed: bool = False
    confirm_net: dict[int, float] = field(default_factory=dict)       # T+k (k≥2) net, only if confirmed
    confirm_gross: dict[int, float] = field(default_factory=dict)     # T+k (k≥2) gross, only if confirmed


@dataclass
class DirectionalObservation:
    """One directional (long) event — insider-buy or pledge-delta. Raw forward drift per horizon, gross
    and net; the cohort's direction is the event TYPE, not a T-day reaction sign (signs are not
    flipped, C9)."""

    symbol: str
    event_date: date
    kind: str                          # 'insider_buy' | 'pledge_increase' | 'pledge_decrease'
    gross: dict[int, float] = field(default_factory=dict)
    net: dict[int, float] = field(default_factory=dict)


def entry_price(rows: list[_Row], signal_idx: int, entry_fill: str = DEFAULT_ENTRY_FILL) -> float | None:
    """Fill price for a signal that became knowable at the CLOSE of ``signal_idx`` (WO-16).

    ``next_open`` ⇒ ``rows[signal_idx + 1].open`` (the first reachable price after that close);
    ``close_t`` ⇒ ``rows[signal_idx].close`` (the pre-WO-16 convention). ``None`` when the fill bar
    does not exist or the price is non-positive — the event is then unmeasurable and dropped.
    """
    if entry_fill not in ENTRY_FILLS:
        raise ValueError(f"entry_fill must be one of {ENTRY_FILLS}, got {entry_fill!r}")
    if entry_fill == ENTRY_FILL_CLOSE_T:
        px = rows[signal_idx].close
    else:
        j = signal_idx + 1
        if j >= len(rows):
            return None
        px = rows[j].open
    return px if px > 0 else None


def measure_event(
    rows: list[_Row], idx: int, *, symbol: str, kind: str, cost_pct: float,
    entry_fill: str = DEFAULT_ENTRY_FILL,
) -> Observation | None:
    """Measure one reaction-sign event at series position ``idx`` (day T). ``None`` if unmeasurable.

    ``cost_pct`` is one round-trip CNC breakeven (%), subtracted once from every signed drift to give
    the net. The gross (pre-cost) drift is retained alongside (§2.8.4: net-only hid the diagnosis).
    ``entry_fill`` (WO-16) picks the fill price the drift is measured FROM — ``open_(T+1)`` by
    default, ``close_T`` under the superseded ``close_t`` convention (see the module docstring).
    """
    T = rows[idx]
    if T.close == T.open:
        return None                    # reaction sign 0 ⇒ ineligible (conservative, O13)
    sign = 1 if T.close > T.open else -1
    if idx + max(HORIZONS) >= len(rows):
        return None                    # not enough forward bars for T+5
    base = entry_price(rows, idx, entry_fill)
    if base is None:
        return None                    # no reachable fill bar / non-positive fill price
    obs = Observation(symbol=symbol, event_date=T.d, kind=kind, reaction_sign=sign)
    for k in HORIZONS:
        raw = rows[idx + k].close / base - 1.0
        gross = sign * raw * 100.0
        obs.pead_gross[k] = gross
        obs.pead_net[k] = gross - cost_pct

    # ---- cat-style price/volume confirmation on T+1 -----------------------------------------
    t1 = rows[idx + 1]
    med = _median([r.volume for r in rows[max(0, idx + 1 - VOL_WINDOW):idx + 1]])
    move_ok = (
        t1.close >= T.close * (1.0 + CAT_CONFIRM_MOVE_PCT / 100.0)
        if sign > 0
        else t1.close <= T.close * (1.0 - CAT_CONFIRM_MOVE_PCT / 100.0)
    )
    vol_ok = med > 0 and t1.volume >= CAT_CONFIRM_VOL_MULT * med
    obs.confirmed = bool(move_ok and vol_ok)
    if obs.confirmed:
        # The confirmation signal is read off close_(T+1), so it fills on the SAME convention one
        # session later (open_(T+2) by default; close_(T+1) under close_t — which is same-bar, WO-2).
        c_base = entry_price(rows, idx + 1, entry_fill)
        for k in HORIZONS:
            if k < 2 or c_base is None:
                continue
            raw = rows[idx + k].close / c_base - 1.0
            gross = sign * raw * 100.0
            obs.confirm_gross[k] = gross
            obs.confirm_net[k] = gross - cost_pct
    return obs


def measure_directional(
    rows: list[_Row], idx: int, *, symbol: str, kind: str, horizons: tuple[int, ...], cost_pct: float,
    entry_fill: str = DEFAULT_ENTRY_FILL,
) -> DirectionalObservation | None:
    """Long forward drift for an event knowable at close_T (series position ``idx``). Raw (unsigned)
    return — the cohort direction is the event type, never a T-day reaction sign. ``None`` if there
    are not enough forward bars for the longest horizon. ``cost_pct`` (one CNC round trip) is
    subtracted once per horizon to give net.

    ``entry_fill`` (WO-16): ``next_open`` measures ``close_(T+k) / open_(T+1)`` — the T+1 horizon is
    then a single intraday session — while ``close_t`` measures ``close_(T+k) / close_T``, booking
    the T→T+1 overnight gap that a post-close order cannot reach.
    """
    if idx + max(horizons) >= len(rows):
        return None
    base = entry_price(rows, idx, entry_fill)
    if base is None:
        return None
    obs = DirectionalObservation(symbol=symbol, event_date=rows[idx].d, kind=kind)
    for k in horizons:
        gross = (rows[idx + k].close / base - 1.0) * 100.0
        obs.gross[k] = gross
        obs.net[k] = gross - cost_pct
    return obs


# --------------------------------------------------------------------------- aggregation + render
def _leg_stats(gross_values: list[float], net_values: list[float]) -> dict[str, float | int | None]:
    """Per-horizon stats reporting BOTH gross and net (§2.8.4). ``gross_values``/``net_values`` are
    paired (same event set, same length); the hit rate is reported for each."""
    if not net_values:
        return {
            "n": 0, "hit_rate_net": None, "hit_rate_gross": None,
            "mean_gross": None, "median_gross": None, "mean_net": None, "median_net": None,
        }
    return {
        "n": len(net_values),
        "hit_rate_net": round(sum(1 for v in net_values if v > 0) / len(net_values), 4),
        "hit_rate_gross": round(sum(1 for v in gross_values if v > 0) / len(gross_values), 4),
        "mean_gross": round(statistics.fmean(gross_values), 4),
        "median_gross": round(statistics.median(gross_values), 4),
        "mean_net": round(statistics.fmean(net_values), 4),
        "median_net": round(statistics.median(net_values), 4),
    }


def _horizon_stats(
    obs_list: list, horizons: tuple[int, ...], gross_attr: str, net_attr: str, *, min_k: int | None = None
) -> dict[int, dict]:
    per_h: dict[int, dict] = {}
    for k in horizons:
        if min_k is not None and k < min_k:
            continue
        gross = [getattr(o, gross_attr)[k] for o in obs_list if k in getattr(o, gross_attr)]
        net = [getattr(o, net_attr)[k] for o in obs_list if k in getattr(o, net_attr)]
        per_h[k] = _leg_stats(gross, net)
    return per_h


def _leg(obs_list: list, horizons: tuple[int, ...], gross_attr: str, net_attr: str, *, min_k: int | None = None) -> dict:
    return {"n_events": len(obs_list), "horizons": _horizon_stats(obs_list, horizons, gross_attr, net_attr, min_k=min_k)}


def aggregate(
    pead_obs: list[Observation],
    results_obs: list[Observation],
    directional_obs: list[DirectionalObservation],
) -> dict:
    """Aggregate every leg into per-horizon gross+net hit-rate / mean / median tables."""
    earnings = [o for o in pead_obs if o.kind == "earnings"]
    gap_volume = [o for o in pead_obs if o.kind == "gap_volume"]
    insider = [o for o in directional_obs if o.kind == "insider_buy"]
    pledge_inc = [o for o in directional_obs if o.kind == "pledge_increase"]
    pledge_dec = [o for o in directional_obs if o.kind == "pledge_decrease"]
    legs = {
        # existing PEAD / confirmation legs (now gross + net)
        "pead_all": _leg(pead_obs, HORIZONS, "pead_gross", "pead_net"),
        "pead_earnings": _leg(earnings, HORIZONS, "pead_gross", "pead_net"),
        "pead_gap_volume": _leg(gap_volume, HORIZONS, "pead_gross", "pead_net"),
        "confirmation": _leg([o for o in pead_obs if o.confirmed], HORIZONS, "confirm_gross", "confirm_net", min_k=2),
        # §2.8.4 filings legs
        "results_filing": _leg(results_obs, HORIZONS, "pead_gross", "pead_net"),
        "results_confirmation": _leg([o for o in results_obs if o.confirmed], HORIZONS, "confirm_gross", "confirm_net", min_k=2),
        "insider_buy": _leg(insider, INSIDER_HORIZONS, "gross", "net"),
        "pledge_increase": _leg(pledge_inc, PLEDGE_HORIZONS, "gross", "net"),
        "pledge_decrease": _leg(pledge_dec, PLEDGE_HORIZONS, "gross", "net"),
    }
    n_events = len(pead_obs) + len(results_obs) + len(directional_obs)
    return {"n_events": n_events, "legs": legs}


def _fmt(v) -> str:
    return "—" if v is None else (f"{v:+.4f}" if isinstance(v, float) else str(v))


def _render_leg(lines: list[str], title: str, leg: dict, empty_note: str) -> None:
    """Render one leg's gross+net table, or an honest ``n=0`` note (C9). Shared by every leg."""
    lines.append(f"## {title}")
    lines.append("")
    lines.append(f"_events: {leg['n_events']}_")
    lines.append("")
    horizons = leg["horizons"]
    if leg["n_events"] == 0 or not horizons or all(h["n"] == 0 for h in horizons.values()):
        lines.append(f"> {empty_note}")
        lines.append("")
        return
    lines.append("| horizon | n | hit rate (net) | mean gross % | median gross % | mean net % | median net % |")
    lines.append("|:--------|--:|---------------:|-------------:|---------------:|-----------:|-------------:|")
    for k in sorted(horizons):
        s = horizons[k]
        hit = "—" if s["hit_rate_net"] is None else f"{s['hit_rate_net']:.1%}"
        lines.append(
            f"| T+{k} | {s['n']} | {hit} | {_fmt(s['mean_gross'])} | {_fmt(s['median_gross'])} | "
            f"{_fmt(s['mean_net'])} | {_fmt(s['median_net'])} |"
        )
    lines.append("")
    means = [horizons[k]["mean_net"] for k in horizons if horizons[k]["mean_net"] is not None]
    if means and max(means) <= 0:
        lines.append(
            "> **No positive net drift at any horizon for this leg** — the mechanical edge does "
            "not survive costs here (C9, reported not massaged)."
        )
        lines.append("")


def render_markdown(agg: dict, meta: dict) -> str:
    filings_on = not meta.get("skip_filings")
    lines: list[str] = []
    lines.append("# Event-study proxy backtest (§2.7 `cat` prerequisite + §2.8.4 filings legs)")
    lines.append("")
    lines.append(f"_Generated {meta['generated_at']}_")
    lines.append("")
    lines.append(
        f"- Symbols: {meta['n_symbols']}  ·  window: {meta['start']} → {meta['end']}  ·  "
        f"events: {agg['n_events']}"
    )
    fees_pct, spread_pct = meta.get("cost_fees_pct"), meta.get("cost_spread_pct")
    split = (
        f" = {fees_pct:.4f}% statutory fees + {spread_pct:.4f}% measured bid-ask SPREAD (WO-2)"
        if fees_pct is not None and spread_pct is not None
        else ""
    )
    lines.append(
        f"- Round-trip CNC cost subtracted from every NET drift: {meta['cost_pct']:.4f}%{split} "
        f"(breakeven at ₹{meta['reference_notional']}); GROSS columns are pre-cost. The spread is "
        "charged on BOTH legs (half each) and the CNC DP charge is included once — one round trip "
        "per event, regardless of horizon."
    )
    entry_fill = meta.get("entry_fill", ENTRY_FILL_CLOSE_T)
    lines.append(
        "- Entry fill (WO-16): **"
        + (
            "next session's OPEN** — every drift is measured from `open_(T+1)`, the first price "
            "reachable by an order placed after the signal was knowable at `close_T`."
            if entry_fill == ENTRY_FILL_NEXT_OPEN
            else "`close_T`** (SUPERSEDED pre-WO-16 convention) — books the T→T+1 overnight gap no "
            "post-close order can capture; for the reaction-sign legs it is same-bar (WO-2 class)."
        )
    )
    lines.append(
        "- Survivorship caveat: the symbol set is the universe as of the RUN date applied over the "
        "whole window, not as-of each event date — names that left the index (or delisted) are "
        "absent, which biases every leg optimistically. Not corrected here (no historical index "
        "membership is stored); stated so it is weighed."
    )
    lines.append(
        f"- Gap/volume event rule: |open-gap vs prior close| ≥ {GAP_MIN:.0%} AND volume ≥ "
        f"{VOL_MULT:g}× 20d median (documented)."
    )
    lines.append(
        f"- Confirmation leg: T+1 close beyond {CAT_CONFIRM_MOVE_PCT:g}% in the reaction direction "
        f"AND volume ≥ {CAT_CONFIRM_VOL_MULT:g}× 20d median; entry at close_(T+1)."
    )
    if filings_on:
        lines.append(
            f"- §2.8.4 insider-buy leg: trailing-{INSIDER_TRAILING_SESSIONS}-session open-market "
            f"insider BUY value ≥ ₹{meta['insider_min_value_inr']} (re-arm below); directional long "
            "drift. Event session T = the first close at which the DISCLOSURE was public — keyed on "
            "the exchange broadcast timestamp (`insider_trades.broadcast_dt`), NEVER the "
            "`txn_from`/`txn_to` transaction dates; broadcast after 15:30 IST (or with an unknown "
            "time) ⇒ next session."
        )
        lines.append(
            f"- §2.8.4 pledge-delta leg: promoter pledged-% QoQ change ≥ "
            f"{meta['pledge_delta_min_pct']:g} pts; increase = negative-catalyst cohort, decrease = "
            "positive; entry keyed off the quarter row's broadcast_dt (NOT quarter end)."
        )
        lines.append(
            "- §2.8.4 results-filing leg: reaction-sign PEAD on results_filings broadcast dates "
            "(fallback: historical earnings_calendar rows) — replaces the n=0 earnings leg."
        )
    lines.append("")
    if agg["n_events"] == 0:
        lines.append("> **NO EVENTS DETECTED** for the requested symbols/window — no evidence either "
                     "way. Reported honestly (C9); backfill more history or widen the window.")
        lines.append("")
        return "\n".join(lines)

    _render_leg(lines, "PEAD leg — all events (drift conditioned on T-day reaction sign)",
                agg["legs"]["pead_all"], "no reaction-sign events measured — honest n=0 (C9).")
    _render_leg(lines, "PEAD leg — earnings events only",
                agg["legs"]["pead_earnings"], "no earnings-calendar events in window — honest n=0 (C9).")
    _render_leg(lines, "PEAD leg — gap/volume events only",
                agg["legs"]["pead_gap_volume"], "no gap/volume events measured — honest n=0 (C9).")
    _render_leg(lines, "Confirmation leg — cat-style price/volume filter (entry at T+1)",
                agg["legs"]["confirmation"], "no events passed the cat-style confirmation filter — honest n=0 (C9).")

    if filings_on:
        _render_leg(
            lines, "§2.8.4 Results-filing leg — reaction-sign PEAD on typed broadcast dates",
            agg["legs"]["results_filing"],
            "results_filings empty and no historical earnings_calendar fallback rows — honest n=0 (C9).",
        )
        _render_leg(
            lines, "§2.8.4 Results-filing confirmation sub-leg (entry at T+1)",
            agg["legs"]["results_confirmation"],
            "no results events passed the cat-style confirmation filter — honest n=0 (C9).",
        )
        _render_leg(
            lines, "§2.8.4 Insider-buy leg — trailing-10-session open-market buy crossings (long drift)",
            agg["legs"]["insider_buy"],
            "insider_trades empty (or no trailing-10-session crossing reached the ₹ threshold) — honest n=0 (C9).",
        )
        lines.append(
            "_Pledge cohorts report RAW long forward drift; signs are not flipped (C9). A working "
            "negative catalyst (pledge INCREASE) shows NEGATIVE net drift; a working positive catalyst "
            "(pledge DECREASE) shows POSITIVE net drift._"
        )
        lines.append("")
        _render_leg(
            lines, "§2.8.4 Pledge-delta leg — promoter pledge INCREASE cohort (negative catalyst)",
            agg["legs"]["pledge_increase"],
            "shp_quarterly empty (or no QoQ promoter-pledge increase reached the threshold) — honest n=0 (C9).",
        )
        _render_leg(
            lines, "§2.8.4 Pledge-delta leg — promoter pledge DECREASE cohort (positive catalyst)",
            agg["legs"]["pledge_decrease"],
            "shp_quarterly empty (or no QoQ promoter-pledge decrease reached the threshold) — honest n=0 (C9).",
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- driver
def _resolve_symbols(store: MarketStore, end: date, *, lookback: int = 60) -> list[str]:
    for i in range(lookback + 1):
        rows = store.get_universe_daily(end - timedelta(days=i), included_only=True)
        if rows:
            return [r["symbol"] for r in rows]
    return []


def run_study(
    store: MarketStore, cost_model: CostModel, symbols: list[str], start: date, end: date, *,
    insider_min_value_inr: int = 10_000_000, pledge_delta_min_pct: float = 5.0,
    skip_filings: bool = False, today: date | None = None,
    entry_fill: str = DEFAULT_ENTRY_FILL,
) -> tuple[list[Observation], list[Observation], list[DirectionalObservation], dict]:
    if entry_fill not in ENTRY_FILLS:
        raise ValueError(f"entry_fill must be one of {ENTRY_FILLS}, got {entry_fill!r}")
    cost_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
    fees_pct = float(cost_model.fee_breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
    spread_pct = float(cost_model.spread_pct)
    today = today or Clock().today()
    pead_obs: list[Observation] = []
    results_obs: list[Observation] = []
    directional_obs: list[DirectionalObservation] = []
    for sym in symbols:
        bars = store.get_bars_1d(sym, start, end)
        if len(bars) < VOL_WINDOW + max(HORIZONS) + 2:
            continue
        rows = _rows(bars)
        pos_by_date = {r.d: i for i, r in enumerate(rows)}
        sessions = [r.d for r in rows]

        # ----- existing PEAD legs (earnings_calendar + gap/volume) — UNCHANGED --------------------
        earnings_dates = {
            r["event_date"] for r in store.get_earnings_calendar(start, end, symbol=sym)
        }
        gap_dates = set(gap_volume_event_days(bars))
        # kind precedence: an earnings day is labelled 'earnings' even if it also gapped.
        for d in sorted(earnings_dates | gap_dates):
            idx = pos_by_date.get(d)
            if idx is None:
                continue
            kind = "earnings" if d in earnings_dates else "gap_volume"
            obs = measure_event(rows, idx, symbol=sym, kind=kind, cost_pct=cost_pct, entry_fill=entry_fill)
            if obs is not None:
                pead_obs.append(obs)

        if skip_filings:
            continue

        # ----- §2.8.4 results-filing leg (typed broadcast dates; fallback historical earnings) -----
        result_bdts = [
            r["broadcast_dt"] for r in store.get_results_filings(symbol=sym)
            if r.get("broadcast_dt") is not None
        ]
        if not result_bdts:
            result_bdts = [
                datetime(ed.year, ed.month, ed.day, tzinfo=IST)      # date-only ⇒ same-session entry
                for r in store.get_earnings_calendar(start, end, symbol=sym)
                if (ed := r.get("event_date")) is not None and ed < today
            ]
        for idx in event_session_indices(sessions, result_bdts):
            obs = measure_event(
                rows, idx, symbol=sym, kind="results_filing", cost_pct=cost_pct, entry_fill=entry_fill
            )
            if obs is not None:
                results_obs.append(obs)

        # ----- §2.8.4 insider-buy leg -------------------------------------------------------------
        insider_rows = store.get_insider_trades(symbol=sym)
        for idx in insider_buy_events(sessions, insider_rows, insider_min_value_inr):
            obs = measure_directional(
                rows, idx, symbol=sym, kind="insider_buy", horizons=INSIDER_HORIZONS,
                cost_pct=cost_pct, entry_fill=entry_fill,
            )
            if obs is not None:
                directional_obs.append(obs)

        # ----- §2.8.4 pledge-delta leg ------------------------------------------------------------
        shp_rows = store.get_shp_quarterly(symbol=sym)
        for ev in pledge_delta_events(shp_rows, pledge_delta_min_pct):
            bdt = ev["broadcast_dt"]
            si = entry_session_index(sessions, bdt) if bdt is not None else None
            if si is None:
                continue
            kind = "pledge_increase" if ev["direction"] == "increase" else "pledge_decrease"
            obs = measure_directional(
                rows, si, symbol=sym, kind=kind, horizons=PLEDGE_HORIZONS, cost_pct=cost_pct,
                entry_fill=entry_fill,
            )
            if obs is not None:
                directional_obs.append(obs)

    meta = {
        "generated_at": Clock().now().isoformat(),
        "n_symbols": len(symbols),
        "start": str(start),
        "end": str(end),
        "cost_pct": cost_pct,
        "cost_fees_pct": fees_pct,
        "cost_spread_pct": spread_pct,
        "entry_fill": entry_fill,
        "reference_notional": str(REFERENCE_NOTIONAL),
        "skip_filings": skip_filings,
        "insider_min_value_inr": insider_min_value_inr,
        "pledge_delta_min_pct": pledge_delta_min_pct,
    }
    return pead_obs, results_obs, directional_obs, meta


def _write(agg: dict, meta: dict, reports_dir: Path) -> Path:
    """Write ``event_study_<ts>.{md,json}`` (WO-16: TIMESTAMPED, like every other report artifact).

    Until WO-16 this wrote a FIXED ``event_study.md``/``.json``, so each run destroyed the previous
    one — including the 2026-07-17 stage-2 artifact the ``insider_net_buy`` verdict (T+10 +0.75%,
    T+20 +1.61%, n=110) is recorded against, which is exactly the history a re-run must be compared
    with. The old files are left untouched as the pre-WO-16 record.
    """
    import json

    reports_dir.mkdir(parents=True, exist_ok=True)
    try:
        stamp = datetime.fromisoformat(str(meta["generated_at"])).strftime("%Y%m%dT%H%M%S")
    except (KeyError, TypeError, ValueError):  # pragma: no cover - defensive: never lose a report
        stamp = Clock().now().strftime("%Y%m%dT%H%M%S")
    md_path = reports_dir / f"event_study_{stamp}.md"
    json_path = reports_dir / f"event_study_{stamp}.json"
    md_path.write_text(render_markdown(agg, meta), encoding="utf-8")
    json_path.write_text(json.dumps({"meta": meta, "aggregate": agg}, indent=2), encoding="utf-8")
    return md_path


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="§2.7 event-study proxy backtest + §2.8.4 filings legs.")
    parser.add_argument("--from", dest="start", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    parser.add_argument("--to", dest="end", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date())
    parser.add_argument("--symbols", default=None, help="comma-separated override of the universe")
    parser.add_argument("--reports-dir", default=None)
    parser.add_argument(
        "--skip-filings-legs", dest="skip_filings", action="store_true",
        help="run only the original PEAD + confirmation legs (no §2.8.4 filings legs)",
    )
    parser.add_argument(
        "--entry-fill", dest="entry_fill", default=DEFAULT_ENTRY_FILL, choices=list(ENTRY_FILLS),
        help=(
            "WO-16 fill convention: 'next_open' (default) fills at open_(T+1); 'close_t' is the "
            "superseded pre-WO-16 close_T fill, kept for old-vs-new comparison"
        ),
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    end = args.end or clock.today()
    start = args.start or (end - timedelta(days=730))
    reports_dir = Path(args.reports_dir) if args.reports_dir else settings.resolved_data_dir() / "reports"
    try:
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else _resolve_symbols(store, end)
        )
        if not symbols:
            print("no symbols resolved -- nothing to study", file=sys.stderr)
            return 2
        cost_model = CostModel.from_config()
        pead_obs, results_obs, directional_obs, meta = run_study(
            store, cost_model, symbols, start, end,
            insider_min_value_inr=settings.filings.insider_min_value_inr,
            pledge_delta_min_pct=settings.filings.pledge_delta_min_pct,
            skip_filings=args.skip_filings, today=clock.today(),
            entry_fill=args.entry_fill,
        )
        agg = aggregate(pead_obs, results_obs, directional_obs)
        md_path = _write(agg, meta, reports_dir)
        # ASCII only (Windows console may be cp1252).
        print(
            f"event study: {agg['n_events']} events over {len(symbols)} symbols "
            f"[entry_fill={args.entry_fill}, round-trip cost {meta['cost_pct']:.4f}% "
            f"= {meta['cost_fees_pct']:.4f}% fees + {meta['cost_spread_pct']:.4f}% spread] -> {md_path}"
        )
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
