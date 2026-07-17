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
its table is empty. Output: ``data/reports/event_study.md`` + ``.json``. Standalone/blocking (offline
research tool). Exit codes: 0 = ran; 2 = no symbols/bars resolved.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401  native import-order guard
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
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

INSIDER_TRAILING_SESSIONS = 10          # §2.8.2 trailing-session window for the insider BUY value sum
MARKET_CLOSE_IST = time(15, 30)         # a filing broadcast after this is NOT knowable at that close

# §2.8.2 taxonomy — acq_mode values that are NOT open-market purchases and are excluded from the
# insider_net_buy aggregation. Case-insensitive SUBSTRING match (defensive: NSE varies the exact
# label — 'ESOP' / 'ESOPs' / 'ESOP Allotment' all contain 'esop'; 'Inter-se Transfer' vs 'Inter se
# Transfer'; 'Preferential Offer' vs 'Preferential Allotment' — the substrings below cover the family).
INSIDER_ACQ_MODE_EXCLUSIONS = (
    "esop", "gift", "inter-se", "inter se", "pledge invocation",
    "preferential", "rights", "bonus",
)


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


def _to_ist(dt: datetime) -> datetime:
    """Coerce to tz-aware IST. Store rows are TIMESTAMPTZ (already IST); a naive dt is assumed IST."""
    return dt.astimezone(IST) if dt.tzinfo is not None else dt.replace(tzinfo=IST)


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
def after_hours(broadcast_dt: datetime) -> bool:
    """A broadcast strictly after 15:30 IST was NOT knowable at that session's close (§2.8.4 PIT)."""
    return _to_ist(broadcast_dt).time() > MARKET_CLOSE_IST


def entry_session_index(sessions: list[date], broadcast_dt: datetime) -> int | None:
    """Index into ascending ``sessions`` of the first session at whose CLOSE ``broadcast_dt`` was
    knowable (§2.8.4). After 15:30 IST ⇒ first session strictly AFTER the broadcast date; at/before
    15:30 ⇒ first session on/after it. ``None`` if no such session is in the series (filing past the
    last bar). This is the single PIT entry rule shared by the insider-buy, results and pledge legs.
    """
    bdate = _to_ist(broadcast_dt).date()
    late = after_hours(broadcast_dt)
    for i, d in enumerate(sessions):
        if (d > bdate) if late else (d >= bdate):
            return i
    return None


def is_open_market_buy(txn_type: object, acq_mode: object) -> bool:
    """True iff an ``insider_trades`` row is an OPEN-MARKET buy (§2.8.2): ``txn_type`` == 'Buy' AND
    ``acq_mode`` is not one of the non-market modes (:data:`INSIDER_ACQ_MODE_EXCLUSIONS`). The acq_mode
    test is a case-insensitive substring; a blank/None acq_mode on a Buy defaults to open-market
    (a market purchase often carries no explicit mode label — defensive toward inclusion)."""
    if str(txn_type or "").strip().lower() != "buy":
        return False
    mode = str(acq_mode or "").strip().lower()
    return not any(excl in mode for excl in INSIDER_ACQ_MODE_EXCLUSIONS)


def is_open_market_sell(txn_type: object, acq_mode: object) -> bool:
    """Mirror of :func:`is_open_market_buy` for the SELL side (the filings_experiments E2 adverse-context
    leg). True iff ``txn_type`` == 'Sell' AND ``acq_mode`` is not one of the non-market modes
    (:data:`INSIDER_ACQ_MODE_EXCLUSIONS`). The SAME §2.8.2 exclusion taxonomy applies, so an
    ESOP-exercise SELL (acq_mode contains 'esop') is deliberately NOT counted as an open-market sell —
    an insider disposing of just-exercised ESOP stock is not the bearish open-market signal the adverse
    cohort is after. A blank/None acq_mode on a Sell defaults to open-market (symmetry with the buy
    side)."""
    if str(txn_type or "").strip().lower() != "sell":
        return False
    mode = str(acq_mode or "").strip().lower()
    return not any(excl in mode for excl in INSIDER_ACQ_MODE_EXCLUSIONS)


def insider_cluster_events(
    sessions: list[date], filings: list[dict], threshold: object, predicate=is_open_market_buy
) -> list[int]:
    """Session indices at which the trailing-``INSIDER_TRAILING_SESSIONS`` sum of the value of filings
    matching ``predicate`` first crosses ≥ ``threshold``, re-arming only after the trailing sum falls
    back below (one event per crossing). Generalizes the transaction SIDE via ``predicate``:
    :func:`is_open_market_buy` for the BUY cluster (the §2.8.4 ``insider_net_buy`` proxy),
    :func:`is_open_market_sell` for the SELL cluster (E2 adverse context).

    Pure. ``sessions`` = ascending session dates; ``filings`` = plain row dicts (``txn_type``,
    ``acq_mode``, ``value``, ``broadcast_dt``). Each eligible filing's value lands on its point-in-time
    knowable session (:func:`entry_session_index`); the crossing session is the entry T (close_T).
    Only the absolute ₹ floor is applied here — the §2.8.2 value/20d-ADV floor is deliberately NOT part
    of this proxy (see the module report's ambiguity note).
    """
    thr = Decimal(str(threshold))
    per_session = [Decimal("0")] * len(sessions)
    for f in filings:
        if not predicate(f.get("txn_type"), f.get("acq_mode")):
            continue
        value = f.get("value")
        bdt = f.get("broadcast_dt")
        if value is None or bdt is None:
            continue
        v = Decimal(str(value))
        if v <= 0:
            continue
        si = entry_session_index(sessions, bdt)
        if si is None:
            continue
        per_session[si] += v
    events: list[int] = []
    armed = True
    for i in range(len(sessions)):
        lo = max(0, i - INSIDER_TRAILING_SESSIONS + 1)
        trailing = sum(per_session[lo:i + 1], Decimal("0"))
        if armed and trailing >= thr:
            events.append(i)
            armed = False
        elif not armed and trailing < thr:
            armed = True
    return events


def insider_buy_events(sessions: list[date], filings: list[dict], threshold: object) -> list[int]:
    """Session indices T at which the trailing-``INSIDER_TRAILING_SESSIONS`` sum of open-market insider
    BUY value first crosses ≥ ``threshold`` (§2.8.2/§2.8.4). Re-arms only after the trailing sum falls
    back below the threshold, so one event per crossing. Thin wrapper over
    :func:`insider_cluster_events` with the open-market BUY predicate (kept as the pinned §2.8.4 API)."""
    return insider_cluster_events(sessions, filings, threshold, is_open_market_buy)


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


def measure_event(
    rows: list[_Row], idx: int, *, symbol: str, kind: str, cost_pct: float
) -> Observation | None:
    """Measure one reaction-sign event at series position ``idx`` (day T). ``None`` if unmeasurable.

    ``cost_pct`` is one round-trip CNC breakeven (%), subtracted once from every signed drift to give
    the net. The gross (pre-cost) drift is retained alongside (§2.8.4: net-only hid the diagnosis).
    """
    T = rows[idx]
    if T.close == T.open:
        return None                    # reaction sign 0 ⇒ ineligible (conservative, O13)
    sign = 1 if T.close > T.open else -1
    if idx + max(HORIZONS) >= len(rows):
        return None                    # not enough forward bars for T+5
    obs = Observation(symbol=symbol, event_date=T.d, kind=kind, reaction_sign=sign)
    for k in HORIZONS:
        raw = rows[idx + k].close / T.close - 1.0
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
        for k in HORIZONS:
            if k < 2:
                continue
            raw = rows[idx + k].close / t1.close - 1.0     # entry at the confirmation bar close
            gross = sign * raw * 100.0
            obs.confirm_gross[k] = gross
            obs.confirm_net[k] = gross - cost_pct
    return obs


def measure_directional(
    rows: list[_Row], idx: int, *, symbol: str, kind: str, horizons: tuple[int, ...], cost_pct: float
) -> DirectionalObservation | None:
    """Long forward drift at series position ``idx`` (entry at close_T). Raw (unsigned) return — the
    cohort direction is the event type, never a T-day reaction sign. ``None`` if there are not enough
    forward bars for the longest horizon. ``cost_pct`` (one CNC round trip) is subtracted once per
    horizon to give net."""
    if idx + max(horizons) >= len(rows):
        return None
    base = rows[idx].close
    if base <= 0:
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
    lines.append(
        f"- Round-trip CNC cost subtracted from every NET drift: {meta['cost_pct']:.4f}% "
        f"(breakeven at ₹{meta['reference_notional']}); GROSS columns are pre-cost."
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
            "drift, entry at close_T = first close at which the filing was knowable (point-in-time; "
            "broadcast after 15:30 IST ⇒ next session)."
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
) -> tuple[list[Observation], list[Observation], list[DirectionalObservation], dict]:
    cost_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
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
            obs = measure_event(rows, idx, symbol=sym, kind=kind, cost_pct=cost_pct)
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
            obs = measure_event(rows, idx, symbol=sym, kind="results_filing", cost_pct=cost_pct)
            if obs is not None:
                results_obs.append(obs)

        # ----- §2.8.4 insider-buy leg -------------------------------------------------------------
        insider_rows = store.get_insider_trades(symbol=sym)
        for idx in insider_buy_events(sessions, insider_rows, insider_min_value_inr):
            obs = measure_directional(
                rows, idx, symbol=sym, kind="insider_buy", horizons=INSIDER_HORIZONS, cost_pct=cost_pct
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
                rows, si, symbol=sym, kind=kind, horizons=PLEDGE_HORIZONS, cost_pct=cost_pct
            )
            if obs is not None:
                directional_obs.append(obs)

    meta = {
        "generated_at": Clock().now().isoformat(),
        "n_symbols": len(symbols),
        "start": str(start),
        "end": str(end),
        "cost_pct": cost_pct,
        "reference_notional": str(REFERENCE_NOTIONAL),
        "skip_filings": skip_filings,
        "insider_min_value_inr": insider_min_value_inr,
        "pledge_delta_min_pct": pledge_delta_min_pct,
    }
    return pead_obs, results_obs, directional_obs, meta


def _write(agg: dict, meta: dict, reports_dir: Path) -> Path:
    import json

    reports_dir.mkdir(parents=True, exist_ok=True)
    md_path = reports_dir / "event_study.md"
    json_path = reports_dir / "event_study.json"
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
        )
        agg = aggregate(pead_obs, results_obs, directional_obs)
        md_path = _write(agg, meta, reports_dir)
        print(f"event study: {agg['n_events']} events over {len(symbols)} symbols -> {md_path}")
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
