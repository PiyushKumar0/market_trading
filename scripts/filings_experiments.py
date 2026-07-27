#!/usr/bin/env python
"""Pre-registered filings x price-baseline experiments E1-E3 (runbooks/WORKLOG.md 2026-07-17).

Single-pass, honest, offline analysis tools. Each experiment was PRE-REGISTERED (hypothesis +
discriminator pinned in the worklog before it ran) so there is no in-sample tuning here: the ORB
config is the section-6.3 ENVELOPE DEFAULTS (deliberately NOT the sweep-best, to avoid the
in-sample trap), the RSI2 config is the already-promoted champion, and the insider leg is
event_study.py's verbatim. The only outputs are gross AND net stats per cohort/slice, reported as
found (C9) with honest n=0 sections when a cohort/slice is empty.

    uv run python scripts/filings_experiments.py <e1|e2|e3> --from YYYY-MM-DD --to YYYY-MM-DD \
        --symbols RELIANCE,TCS[,...]  [--reports-dir data/reports]

E1  catalyst-conditioned ORB. H: ORB breakouts that carry a fresh exchange-verified event trend
    (insider-buy cluster <=5 sessions old, OR results T+1/T+2) hold, while the unconditioned ones
    fade at the pinned -0.02% GROSS/trade base. ORB-v2 signals at the section-6.3 envelope defaults,
    ONE vbt Portfolio.from_signals (mechanics identical to sweep._backtest, fees=0 so the per-trade
    return is pure GROSS); each trade flagged into cohort A (insider-buy cluster), B (results T+1/2),
    or C (unconditioned). net = gross - one MIS round trip (CostModel at Rs 20,000).

E2  RSI2 catalyst-veto split. H: the losing tail of the 70%-win champion concentrates in adverse
    filings context. RSI2 signals at the promoted config with the NIFTY 50 regime filter; each entry
    flagged ADVERSE (open-market insider SELL cluster <=5 sessions old, OR promoter pledge INCREASE
    >=+5pp broadcast within 90 calendar days), FAVORABLE (insider-buy cluster, as E1), or NEUTRAL.
    net = gross - one CNC round trip.

E3  insider_net_buy robustness slices (stage-3 gate input). The event_study insider-buy leg VERBATIM
    (same crossings, same T+10/T+20 gross/net/hit) sliced three ways: (a) calendar year of entry,
    (b) dominant person_category of the filings that made up the crossing value (by value share),
    (c) liquidity tercile by the symbol's 20d median close*volume at entry.

Reuse contract: the point-in-time / cluster / drift / cost plumbing is IMPORTED from event_study.py
(insider_buy_events, insider_cluster_events, is_open_market_buy/sell, entry_session_index,
measure_directional, pledge_delta_events, _leg_stats, _horizon_stats, _rows), and the frame loading +
signal builders are IMPORTED from engine.learning.sweep (SweepRunner._load_frames / _per_side_fee,
_signals_orb, _signals_rsi2) -- nothing is duplicated. Output: data/reports/experiment_<id>_<UTCts>
.md + .json. Exit codes: 0 = ran; 2 = no symbols resolved. HARD CONSTRAINT: every print() string is
ASCII-only (Windows cp1252 console); the markdown files are UTF-8 but also kept ASCII for safety.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import statistics
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import pandas as pd  # noqa: E402

import engine  # noqa: E402,F401  native import-order guard (sklearn before numba/vectorbt/cvxpy)
from engine.core.clock import Clock  # noqa: E402
from engine.core.config import load_settings  # noqa: E402
from engine.core.log import configure_logging, get_logger  # noqa: E402
from engine.learning.sweep import (  # noqa: E402
    SweepRunner,
    _signals_orb,
    _signals_rsi2,
)
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402

# Load the event_study.py loose script by path (same pattern as tests/unit/test_event_study_filings.py)
# and reuse its PURE point-in-time / cluster / drift / cost functions -- import, never duplicate.
_ES_PATH = Path(__file__).resolve().parent / "event_study.py"
_spec = importlib.util.spec_from_file_location("mt_event_study", _ES_PATH)
es = importlib.util.module_from_spec(_spec)
sys.modules["mt_event_study"] = es
_spec.loader.exec_module(es)

_log = get_logger("scripts.filings_experiments")

# --------------------------------------------------------------------------- pinned experiment config
#: E1 ORB signals: the section-6.3 ENVELOPE DEFAULTS ONLY (NOT the sweep-best -- pre-registration
#: avoids the in-sample trap). Mirrors config/envelope.yaml orb.* defaults.
E1_ORB_PARAMS: dict[str, float] = {
    "orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5,
}
#: E2 RSI2 signals: the already-PROMOTED champion config (worklog 2026-07-16: 102 trades / 70% win /
#: +0.58% net per trade) -- a pre-registered fixed config, not re-swept here.
E2_RSI2_PARAMS: dict[str, float] = {
    "rsi_entry": 2.0, "rsi_exit": 65.0, "stop_pct": 6.0, "max_hold_days": 15.0,
}
#: Pre-registered unconditioned ORB fade base (worklog): -0.02% GROSS per trade.
UNCONDITIONED_ORB_BASE_GROSS_PCT = -0.02
#: Pre-registered RSI2 champion base win rate: 70%.
RSI2_WIN_BASE = 0.70
#: Canonical rsi2 regime index (mirrors scripts/backtest._DEFAULT_INDEX_SYMBOL / ops.main INDEX_SYMBOL).
DEFAULT_INDEX_SYMBOL = "NIFTY 50"

#: A cluster crossing at session e is treated as ACTIVE for the FORWARD sessions {e+1 .. e+N} only --
#: never session e itself. Point-in-time: an ORB/RSI2 entry on session e cannot assume a filing that
#: only became knowable at e's CLOSE was known intraday/at-entry, so the cluster arms from the NEXT
#: session. "<=5 sessions before entry" <=> entry session in {e+1 .. e+5}.
CLUSTER_FORWARD_SESSIONS = 5
#: Results T+1/T+2: entry 1 or 2 sessions after the results-broadcast knowable session.
RESULTS_OFFSETS = (1, 2)
#: Adverse pledge-increase look-back window (calendar days before entry).
PLEDGE_WINDOW_DAYS = 90
#: E3 liquidity slice: trailing sessions for the median-traded-value liquidity proxy.
LIQUIDITY_MEDIAN_WINDOW = 20
#: Daily-session context buffer before `start` so an event that crossed shortly before the window
#: still flags the in-window entries it should. (E1/E2 cohort flagging only; E3 events are verbatim.)
SESSION_LOOKBACK_DAYS = 60

# --------------------------------------------------------------------------- E3 person_category groups
#: Pinned person_category -> group mapping (E3 slice b). Case-insensitive SUBSTRING match, checked in
#: order, first match wins; anything unmatched (employee / designated person / relative / blank) is
#: 'employee_other'. NSE PIT personCategory strings vary ('Promoters', 'Promoter Group', 'Director',
#: 'Key Managerial Personnel', 'Employees', ...); the substrings below cover the families.
CATEGORY_GROUP_PROMOTER = "promoter"
CATEGORY_GROUP_DIRECTOR_KMP = "director_kmp"
CATEGORY_GROUP_OTHER = "employee_other"
CATEGORY_GROUPS: tuple[str, ...] = (
    CATEGORY_GROUP_PROMOTER, CATEGORY_GROUP_DIRECTOR_KMP, CATEGORY_GROUP_OTHER,
)
CATEGORY_GROUP_RULES: tuple[tuple[str, str], ...] = (
    ("promoter", CATEGORY_GROUP_PROMOTER),
    ("director", CATEGORY_GROUP_DIRECTOR_KMP),
    ("key managerial", CATEGORY_GROUP_DIRECTOR_KMP),
    ("kmp", CATEGORY_GROUP_DIRECTOR_KMP),
)

REFERENCE_NOTIONAL = es.REFERENCE_NOTIONAL  # Decimal("20000")


# =========================================================================== pure flag/slice builders
def cluster_active_dates(
    sessions: list[date], event_indices: list[int], forward: int = CLUSTER_FORWARD_SESSIONS
) -> set[date]:
    """Session dates on which a cluster crossing is ACTIVE: the ``forward`` sessions strictly AFTER
    each crossing session index, i.e. {sessions[e+1] .. sessions[e+forward]} (clamped to the series).
    Session ``e`` itself is excluded (point-in-time; see :data:`CLUSTER_FORWARD_SESSIONS`)."""
    out: set[date] = set()
    n = len(sessions)
    for e in event_indices:
        for k in range(1, forward + 1):
            j = e + k
            if 0 <= j < n:
                out.add(sessions[j])
    return out


def results_active_dates(
    sessions: list[date], broadcast_dts: list, offsets: tuple[int, ...] = RESULTS_OFFSETS
) -> set[date]:
    """Session dates that are T+offset after a results broadcast's point-in-time knowable session
    (:func:`event_study.entry_session_index`). With ``offsets=(1,2)`` this is the results T+1/T+2
    cohort: entries 1 or 2 sessions after the filing became knowable."""
    out: set[date] = set()
    n = len(sessions)
    for bdt in broadcast_dts:
        if bdt is None:
            continue
        r = es.entry_session_index(sessions, bdt)
        if r is None:
            continue
        for k in offsets:
            j = r + k
            if 0 <= j < n:
                out.add(sessions[j])
    return out


def pledge_increase_active(
    entry_date: date, sessions: list[date], increase_events: list[dict],
    window_days: int = PLEDGE_WINDOW_DAYS,
) -> bool:
    """True iff some promoter pledge-INCREASE event is knowable by ``entry_date`` (point-in-time:
    its :func:`event_study.entry_session_index` session is on/before the entry session) AND its
    broadcast date falls within ``window_days`` CALENDAR days before ``entry_date``. ``increase_events``
    are :func:`event_study.pledge_delta_events` dicts with direction 'increase'."""
    for ev in increase_events:
        bdt = ev.get("broadcast_dt")
        if bdt is None:
            continue
        p = es.entry_session_index(sessions, bdt)
        if p is None or sessions[p] > entry_date:
            continue  # not yet knowable at the entry session (PIT)
        b_date = es._to_ist(bdt).date()
        if 0 <= (entry_date - b_date).days <= window_days:
            return True
    return False


def person_category_group(category: object) -> str:
    """Map an NSE PIT ``person_category`` string onto one of :data:`CATEGORY_GROUPS` via the pinned
    :data:`CATEGORY_GROUP_RULES` (case-insensitive substring, first match wins; default employee_other)."""
    c = str(category or "").strip().lower()
    if not c:
        return CATEGORY_GROUP_OTHER
    for needle, group in CATEGORY_GROUP_RULES:
        if needle in c:
            return group
    return CATEGORY_GROUP_OTHER


def contributing_filings(
    sessions: list[date], filings: list[dict], event_index: int,
    window: int = es.INSIDER_TRAILING_SESSIONS, predicate=None,
) -> list[dict]:
    """The open-market filings whose value made up the trailing-``window``-session crossing sum at
    ``event_index`` -- i.e. eligible filings whose point-in-time session lands in
    [event_index-window+1, event_index]. This is exactly the set summed by
    :func:`event_study.insider_cluster_events` at the crossing session (E3 slice-b input)."""
    predicate = predicate or es.is_open_market_buy
    lo = max(0, event_index - window + 1)
    out: list[dict] = []
    for f in filings:
        if not predicate(f.get("txn_type"), f.get("acq_mode")):
            continue
        v = f.get("value")
        bdt = f.get("broadcast_dt")
        if v is None or bdt is None or Decimal(str(v)) <= 0:
            continue
        si = es.entry_session_index(sessions, bdt)
        if si is not None and lo <= si <= event_index:
            out.append(f)
    return out


def dominant_category(filings: list[dict]) -> str:
    """The :func:`person_category_group` with the largest VALUE share among ``filings``. Ties break to
    the earliest group name (deterministic). Empty -> 'unknown'."""
    totals: dict[str, Decimal] = {}
    for f in filings:
        g = person_category_group(f.get("person_category"))
        totals[g] = totals.get(g, Decimal("0")) + Decimal(str(f.get("value") or 0))
    if not totals:
        return "unknown"
    return max(sorted(totals), key=lambda g: totals[g])


def trailing_median_traded_value(rows, idx: int, window: int = LIQUIDITY_MEDIAN_WINDOW) -> float:
    """Median of ``close * volume`` over the ``window`` sessions ending at ``idx`` (inclusive) --
    the E3 liquidity proxy at entry. ``rows`` are :class:`event_study._Row`."""
    lo = max(0, idx - window + 1)
    vals = [r.close * r.volume for r in rows[lo:idx + 1]]
    return statistics.median(vals) if vals else float("nan")


def liquidity_tercile_labels(values: list[float]) -> tuple[list[str], tuple[float, float] | None]:
    """Assign each value a liquidity tercile ('low'|'mid'|'high') by the 1/3 and 2/3 quantiles of the
    non-NaN values (inclusive method). NaN -> 'unknown'. Fewer than 3 usable values -> every value
    'all' (too few to tercile; honest single bucket) and ``None`` bounds. Boundaries are inclusive on
    the low side: v <= q1 -> low, v <= q2 -> mid, else high."""
    clean = [v for v in values if v == v]  # drop NaN
    if len(clean) < 3:
        return ["all" if v == v else "unknown" for v in values], None
    q1, q2 = statistics.quantiles(clean, n=3, method="inclusive")
    labels: list[str] = []
    for v in values:
        if v != v:
            labels.append("unknown")
        elif v <= q1:
            labels.append("low")
        elif v <= q2:
            labels.append("mid")
        else:
            labels.append("high")
    return labels, (q1, q2)


def year_slice(d: date) -> str:
    """Calendar-year-of-entry bucket. Canonical buckets: '2023H2', '2024', '2025', '2026H1'; any
    stragglers get an honest '<year>H1'/'<year>H2' label rather than being dropped."""
    y = d.year
    if y == 2023:
        return "2023H2" if d.month >= 7 else "2023H1"
    if y == 2026:
        return "2026H1" if d.month <= 6 else "2026H2"
    return str(y)


def _year_sort_key(label: str) -> tuple[int, int]:
    if label.endswith("H1"):
        return (int(label[:4]), 0)
    if label.endswith("H2"):
        return (int(label[:4]), 1)
    return (int(label), 0)


# =========================================================================== vbt run + trade extraction
def _run_portfolio(frames, sig, *, init_cash: float = 100_000.0):
    """ONE vbt Portfolio.from_signals with mechanics IDENTICAL to sweep.SweepRunner._backtest, except
    fees=0.0 so ``pf.trades.records_readable['Return']`` is the pure GROSS per-trade return (net is
    then gross - one CostModel round trip, applied uniformly; see the module report). The per-side fee
    still feeds the ORB C3 cost floor via the signal builder -- only the vbt fee drag is zeroed here."""
    import vectorbt as vbt  # function-level: engine._preload native import-order guard

    kwargs: dict = dict(
        close=frames.close, entries=sig.entries, exits=sig.exits, fees=0.0,
        init_cash=init_cash, direction="longonly",
        freq="1min" if frames.intraday else "1D",
    )
    if sig.sl_stop is not None:
        kwargs["sl_stop"] = sig.sl_stop
    if sig.tp_stop is not None:
        kwargs["tp_stop"] = sig.tp_stop
    if sig.sl_trail:
        kwargs["sl_trail"] = True
    if frames.intraday:
        kwargs["high"] = frames.high
        kwargs["low"] = frames.low
    return vbt.Portfolio.from_signals(**kwargs)


def _extract_trades(pf) -> list[tuple[str, date, float]]:
    """(symbol, entry-session date, GROSS return %) per closed/open trade from records_readable."""
    tr = pf.trades.records_readable
    out: list[tuple[str, date, float]] = []
    for sym, ets, ret in zip(tr["Column"], tr["Entry Timestamp"], tr["Return"], strict=False):
        out.append((str(sym), pd.Timestamp(ets).date(), float(ret) * 100.0))
    return out


def _run_and_extract(strategy_id: str, frames, params: dict, fee: float) -> list[tuple[str, date, float]]:
    builder = _signals_orb if strategy_id == "orb" else _signals_rsi2
    sig = builder(frames, params, fee)
    return _extract_trades(_run_portfolio(frames, sig))


def _sessions_for(store: MarketStore, symbol: str, start: date, end: date) -> list[date]:
    df = store.get_bars_1d_frame(symbol, start, end)
    return [pd.Timestamp(ix).date() for ix in df.index]


def _data_span(frames) -> tuple[str | None, str | None]:
    if frames.empty:
        return None, None
    idx = frames.close.index
    return str(pd.Timestamp(idx[0]).date()), str(pd.Timestamp(idx[-1]).date())


# =========================================================================== E1
def run_e1(
    store: MarketStore, cost_model: CostModel, clock: Clock, symbols: list[str],
    start: date, end: date, *, insider_min_value_inr: int,
) -> dict:
    """Catalyst-conditioned ORB. Returns the structured result dict (rendered by :func:`render_e1`)."""
    round_trip_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "MIS"))
    runner = SweepRunner(store, cost_model, clock, index_symbol=None)  # orb needs no regime index
    frames = runner._load_frames("orb", start, end, symbols)
    fee = runner._per_side_fee("orb")

    buf_start = start - timedelta(days=SESSION_LOOKBACK_DAYS)
    a_dates: dict[str, set[date]] = {}
    b_dates: dict[str, set[date]] = {}
    for sym in symbols:
        sessions = _sessions_for(store, sym, buf_start, end)
        if not sessions:
            a_dates[sym] = set()
            b_dates[sym] = set()
            continue
        insider_rows = store.get_insider_trades(symbol=sym)
        a_dates[sym] = cluster_active_dates(
            sessions, es.insider_buy_events(sessions, insider_rows, insider_min_value_inr)
        )
        res_bdts = [
            r["broadcast_dt"] for r in store.get_results_filings(symbol=sym)
            if r.get("broadcast_dt") is not None
        ]
        b_dates[sym] = results_active_dates(sessions, res_bdts)

    trades = [] if frames.empty else _run_and_extract("orb", frames, E1_ORB_PARAMS, fee)
    cohorts = {c: {"gross": [], "net": []} for c in ("A", "B", "C")}
    overlap = 0
    for sym, edate, gross in trades:
        net = gross - round_trip_pct
        in_a = edate in a_dates.get(sym, ())
        in_b = edate in b_dates.get(sym, ())
        if in_a:
            cohorts["A"]["gross"].append(gross)
            cohorts["A"]["net"].append(net)
        if in_b:
            cohorts["B"]["gross"].append(gross)
            cohorts["B"]["net"].append(net)
        if in_a and in_b:
            overlap += 1
        if not in_a and not in_b:
            cohorts["C"]["gross"].append(gross)
            cohorts["C"]["net"].append(net)

    stats = {c: es._leg_stats(v["gross"], v["net"]) for c, v in cohorts.items()}
    d0, d1 = _data_span(frames)
    return {
        "experiment": "e1",
        "title": "Catalyst-conditioned ORB (envelope-default v2 signals)",
        "hypothesis": "ORB breakouts with a fresh exchange-verified event trend hold; unconditioned "
                      "ones fade at the pinned -0.02% GROSS/trade base.",
        "params": E1_ORB_PARAMS,
        "product": "MIS",
        "round_trip_pct": round_trip_pct,
        "n_symbols": len(frames.symbols),
        "n_trades_total": len(trades),
        "data_start": d0,
        "data_end": d1,
        "unconditioned_base_gross_pct": UNCONDITIONED_ORB_BASE_GROSS_PCT,
        "overlap_A_B": overlap,
        "cohorts": {
            "A_insider_buy_cluster": stats["A"],
            "B_results_t1_t2": stats["B"],
            "C_unconditioned": stats["C"],
        },
    }


# =========================================================================== E2
def run_e2(
    store: MarketStore, cost_model: CostModel, clock: Clock, symbols: list[str],
    start: date, end: date, *, insider_min_value_inr: int, pledge_delta_min_pct: float,
    index_symbol: str | None = DEFAULT_INDEX_SYMBOL,
) -> dict:
    """RSI2 catalyst-veto split. Returns the structured result dict (rendered by :func:`render_e2`)."""
    round_trip_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
    runner = SweepRunner(store, cost_model, clock, index_symbol=index_symbol)
    frames = runner._load_frames("rsi2", start, end, symbols)
    fee = runner._per_side_fee("rsi2")
    regime_on = frames.index_closes is not None

    buf_start = start - timedelta(days=SESSION_LOOKBACK_DAYS)
    flags: dict[str, dict] = {}
    for sym in symbols:
        sessions = _sessions_for(store, sym, buf_start, end)
        if not sessions:
            flags[sym] = {"sessions": [], "sell_dates": set(), "buy_dates": set(), "inc_events": []}
            continue
        insider_rows = store.get_insider_trades(symbol=sym)
        sell_events = es.insider_cluster_events(
            sessions, insider_rows, insider_min_value_inr, es.is_open_market_sell
        )
        buy_events = es.insider_buy_events(sessions, insider_rows, insider_min_value_inr)
        inc_events = [
            e for e in es.pledge_delta_events(store.get_shp_quarterly(symbol=sym), pledge_delta_min_pct)
            if e["direction"] == "increase"
        ]
        flags[sym] = {
            "sessions": sessions,
            "sell_dates": cluster_active_dates(sessions, sell_events),
            "buy_dates": cluster_active_dates(sessions, buy_events),
            "inc_events": inc_events,
        }

    trades = [] if frames.empty else _run_and_extract("rsi2", frames, E2_RSI2_PARAMS, fee)
    cohorts = {c: {"gross": [], "net": []} for c in ("ADVERSE", "FAVORABLE", "NEUTRAL")}
    overlap = 0
    for sym, edate, gross in trades:
        net = gross - round_trip_pct
        f = flags.get(sym)
        if f is None:
            adverse = favorable = False
        else:
            sell_adv = edate in f["sell_dates"]
            pledge_adv = pledge_increase_active(edate, f["sessions"], f["inc_events"])
            adverse = sell_adv or pledge_adv
            favorable = edate in f["buy_dates"]
        neutral = not adverse and not favorable
        if adverse:
            cohorts["ADVERSE"]["gross"].append(gross)
            cohorts["ADVERSE"]["net"].append(net)
        if favorable:
            cohorts["FAVORABLE"]["gross"].append(gross)
            cohorts["FAVORABLE"]["net"].append(net)
        if adverse and favorable:
            overlap += 1
        if neutral:
            cohorts["NEUTRAL"]["gross"].append(gross)
            cohorts["NEUTRAL"]["net"].append(net)

    stats = {c: es._leg_stats(v["gross"], v["net"]) for c, v in cohorts.items()}
    d0, d1 = _data_span(frames)
    return {
        "experiment": "e2",
        "title": "RSI2 catalyst-veto split (promoted champion config)",
        "hypothesis": "The losing tail of the 70%-win RSI2 champion concentrates in adverse filings "
                      "context (insider selling / pledge increase).",
        "params": E2_RSI2_PARAMS,
        "product": "CNC",
        "regime_filter": index_symbol if regime_on else None,
        "regime_applied": regime_on,
        "round_trip_pct": round_trip_pct,
        "n_symbols": len(frames.symbols),
        "n_trades_total": len(trades),
        "data_start": d0,
        "data_end": d1,
        "win_base": RSI2_WIN_BASE,
        "overlap_adverse_favorable": overlap,
        "cohorts": {
            "ADVERSE": stats["ADVERSE"],
            "FAVORABLE": stats["FAVORABLE"],
            "NEUTRAL": stats["NEUTRAL"],
        },
    }


# =========================================================================== E3
_E3_HORIZONS = (10, 20)


def run_e3(
    store: MarketStore, cost_model: CostModel, clock: Clock, symbols: list[str],
    start: date, end: date, *, insider_min_value_inr: int,
) -> dict:
    """insider_net_buy robustness slices. The event_study insider-buy leg VERBATIM (bars over
    [start, end], same crossings, same measure_directional), sliced by year / dominant category /
    liquidity tercile. Returns the structured result dict (rendered by :func:`render_e3`)."""
    cost_pct = float(cost_model.breakeven_pct(REFERENCE_NOTIONAL, "CNC"))
    events: list[dict] = []
    for sym in symbols:
        bars = store.get_bars_1d(sym, start, end)
        if not bars:
            continue
        rows = es._rows(bars)
        sessions = [r.d for r in rows]
        insider_rows = store.get_insider_trades(symbol=sym)
        for e in es.insider_buy_events(sessions, insider_rows, insider_min_value_inr):
            obs = es.measure_directional(
                rows, e, symbol=sym, kind="insider_buy", horizons=es.INSIDER_HORIZONS, cost_pct=cost_pct
            )
            if obs is None:
                continue
            contrib = contributing_filings(sessions, insider_rows, e)
            events.append({
                "obs": obs,
                "entry_date": rows[e].d,
                "category": dominant_category(contrib),
                "liquidity": trailing_median_traded_value(rows, e),
            })

    # liquidity terciles computed across ALL events (pooled), then labelled back
    liq_labels, liq_bounds = liquidity_tercile_labels([ev["liquidity"] for ev in events])
    for ev, lab in zip(events, liq_labels, strict=False):
        ev["liq_label"] = lab

    def _slice(key_fn) -> dict:
        buckets: dict[str, list] = {}
        for ev in events:
            buckets.setdefault(key_fn(ev), []).append(ev["obs"])
        return {
            b: es._horizon_stats(obs_list, _E3_HORIZONS, "gross", "net")
            for b, obs_list in buckets.items()
        }

    by_year = _slice(lambda ev: year_slice(ev["entry_date"]))
    by_category = _slice(lambda ev: ev["category"])
    by_liquidity = _slice(lambda ev: ev["liq_label"])

    return {
        "experiment": "e3",
        "title": "insider_net_buy robustness slices (stage-3 gate input)",
        "hypothesis": "The stage-2 insider_net_buy edge (T+10 +0.75% / T+20 +1.61% net, n=110) is "
                      "broad-based, not concentrated in one year / person-category / liquidity tier.",
        "product": "CNC",
        "round_trip_pct": cost_pct,
        "horizons": list(_E3_HORIZONS),
        "n_symbols": len(symbols),
        "n_events": len(events),
        "data_start": str(start),
        "data_end": str(end),
        "liquidity_bounds": (
            None if liq_bounds is None else [round(liq_bounds[0], 2), round(liq_bounds[1], 2)]
        ),
        "slices": {"by_year": by_year, "by_category": by_category, "by_liquidity": by_liquidity},
    }


# =========================================================================== rendering (ASCII markdown)
def _fmt_pct(v) -> str:
    return "n/a" if v is None else f"{v:+.4f}"


def _fmt_rate(v) -> str:
    return "n/a" if v is None else f"{v * 100.0:.1f}%"


_COHORT_HEADER = (
    "| cohort | n | win rate (net) | mean gross % | median gross % | mean net % | median net % |\n"
    "|:-------|--:|---------------:|-------------:|---------------:|-----------:|-------------:|"
)


def _cohort_row(name: str, s: dict) -> str:
    return (
        f"| {name} | {s['n']} | {_fmt_rate(s['hit_rate_net'])} | {_fmt_pct(s['mean_gross'])} | "
        f"{_fmt_pct(s['median_gross'])} | {_fmt_pct(s['mean_net'])} | {_fmt_pct(s['median_net'])} |"
    )


def _meta_lines(res: dict) -> list[str]:
    lines = [
        f"- window: {res.get('data_start')} -> {res.get('data_end')}  ;  "
        f"symbols with bars: {res.get('n_symbols')}",
        f"- round-trip cost subtracted from NET ({res['product']}): {res['round_trip_pct']:.4f}% "
        f"(breakeven at Rs {REFERENCE_NOTIONAL}); GROSS columns are pre-cost.",
    ]
    return lines


def render_e1(res: dict) -> str:
    L: list[str] = []
    L.append("# Experiment E1 -- catalyst-conditioned ORB")
    L.append("")
    L.append(f"_H: {res['hypothesis']}_")
    L.append("")
    L += _meta_lines(res)
    L.append(f"- ORB v2 signals at the section-6.3 ENVELOPE DEFAULTS (not sweep-best): {res['params']}")
    L.append(f"- total ORB trades in window: {res['n_trades_total']}  ;  cohort A&B overlap: "
             f"{res['overlap_A_B']} trades")
    L.append("- cohort A = insider-buy cluster active (crossing <=5 sessions before entry); "
             "cohort B = results T+1/T+2; cohort C = unconditioned (neither).")
    L.append("")
    if res["n_trades_total"] == 0:
        L.append("> NO ORB TRADES for the requested symbols/window -- no evidence either way. "
                 "Reported honestly (C9); check intraday coverage or widen the window.")
        L.append("")
    L.append("## Cohort stats (gross AND net per trade)")
    L.append("")
    L.append(_COHORT_HEADER)
    L.append(_cohort_row("A insider-buy cluster", res["cohorts"]["A_insider_buy_cluster"]))
    L.append(_cohort_row("B results T+1/T+2", res["cohorts"]["B_results_t1_t2"]))
    L.append(_cohort_row("C unconditioned", res["cohorts"]["C_unconditioned"]))
    L.append("")
    for label, key in (("A", "A_insider_buy_cluster"), ("B", "B_results_t1_t2"),
                       ("C", "C_unconditioned")):
        if res["cohorts"][key]["n"] == 0:
            L.append(f"> cohort {label}: honest n=0 -- no ORB trades carried this flag in the window (C9).")
    L.append("")
    a = res["cohorts"]["A_insider_buy_cluster"]["mean_gross"]
    b = res["cohorts"]["B_results_t1_t2"]["mean_gross"]
    c = res["cohorts"]["C_unconditioned"]["mean_gross"]
    L.append("## Discriminator")
    L.append("")
    L.append(f"- cohort A mean GROSS: {_fmt_pct(a)}%  ;  cohort B mean GROSS: {_fmt_pct(b)}%")
    L.append(f"- unconditioned cohort C mean GROSS: {_fmt_pct(c)}%  ;  "
             f"pre-registered unconditioned fade base: {UNCONDITIONED_ORB_BASE_GROSS_PCT:+.4f}%/trade")
    L.append("- Read: a conditioned cohort supports H only if its mean GROSS clears BOTH the empirical "
             "cohort-C base and the pinned -0.02% base by a margin; reported as found, not massaged (C9).")
    L.append("")
    return "\n".join(L)


def render_e2(res: dict) -> str:
    L: list[str] = []
    L.append("# Experiment E2 -- RSI2 catalyst-veto split")
    L.append("")
    L.append(f"_H: {res['hypothesis']}_")
    L.append("")
    L += _meta_lines(res)
    L.append(f"- RSI2 signals at the PROMOTED champion config: {res['params']}")
    regime = res.get("regime_filter")
    L.append(f"- regime filter: {'NIFTY 50 (' + str(regime) + ')' if res.get('regime_applied') else 'DISABLED (no index frame -- disclosed)'}")
    L.append(f"- total RSI2 trades in window: {res['n_trades_total']}  ;  ADVERSE&FAVORABLE overlap: "
             f"{res['overlap_adverse_favorable']} trades")
    L.append("- ADVERSE = open-market insider SELL cluster <=5 sessions old (ESOP-exercise sells "
             "excluded by the same acq_mode taxonomy as buys) OR promoter pledge INCREASE >=+5pp "
             "broadcast within 90 calendar days; FAVORABLE = insider-buy cluster; NEUTRAL = neither.")
    L.append("")
    if res["n_trades_total"] == 0:
        L.append("> NO RSI2 TRADES for the requested symbols/window -- no evidence either way (C9).")
        L.append("")
    L.append("## Cohort stats (gross AND net per trade)")
    L.append("")
    L.append(_COHORT_HEADER)
    L.append(_cohort_row("ADVERSE", res["cohorts"]["ADVERSE"]))
    L.append(_cohort_row("FAVORABLE", res["cohorts"]["FAVORABLE"]))
    L.append(_cohort_row("NEUTRAL", res["cohorts"]["NEUTRAL"]))
    L.append("")
    for label in ("ADVERSE", "FAVORABLE", "NEUTRAL"):
        if res["cohorts"][label]["n"] == 0:
            L.append(f"> cohort {label}: honest n=0 -- no RSI2 trades carried this flag in the window (C9).")
    L.append("")
    adv = res["cohorts"]["ADVERSE"]["hit_rate_net"]
    fav = res["cohorts"]["FAVORABLE"]["hit_rate_net"]
    neu = res["cohorts"]["NEUTRAL"]["hit_rate_net"]
    L.append("## Discriminator")
    L.append("")
    L.append(f"- ADVERSE win rate (net): {_fmt_rate(adv)}  vs  the {RSI2_WIN_BASE * 100:.0f}% champion base")
    L.append(f"- FAVORABLE win rate (net): {_fmt_rate(fav)}  ;  NEUTRAL win rate (net): {_fmt_rate(neu)}")
    L.append("- Read: H holds only if the ADVERSE cohort's win rate sits materially BELOW the 70% base "
             "(the losing tail concentrating in adverse context); reported as found (C9).")
    L.append("")
    return "\n".join(L)


_E3_SLICE_HEADER = (
    "| slice | n | mean gross % | median gross % | mean net % | median net % | hit rate (net) |\n"
    "|:------|--:|-------------:|---------------:|-----------:|-------------:|---------------:|"
)


def _e3_bucket_order(family: str, buckets: dict) -> list[str]:
    keys = list(buckets)
    if family == "by_year":
        return sorted(keys, key=_year_sort_key)
    if family == "by_category":
        order = {g: i for i, g in enumerate(CATEGORY_GROUPS + ("unknown",))}
        return sorted(keys, key=lambda k: order.get(k, len(order)))
    if family == "by_liquidity":
        order = {g: i for i, g in enumerate(("low", "mid", "high", "all", "unknown"))}
        return sorted(keys, key=lambda k: order.get(k, len(order)))
    return sorted(keys)


def _render_e3_family(L: list[str], title: str, family: str, buckets: dict) -> None:
    L.append(f"## {title}")
    L.append("")
    if not buckets:
        L.append("> honest n=0 -- no insider_net_buy crossings measured in the window (C9).")
        L.append("")
        return
    order = _e3_bucket_order(family, buckets)
    for h in _E3_HORIZONS:
        L.append(f"### T+{h}")
        L.append("")
        L.append(_E3_SLICE_HEADER)
        for b in order:
            s = buckets[b][h]
            L.append(
                f"| {b} | {s['n']} | {_fmt_pct(s['mean_gross'])} | {_fmt_pct(s['median_gross'])} | "
                f"{_fmt_pct(s['mean_net'])} | {_fmt_pct(s['median_net'])} | {_fmt_rate(s['hit_rate_net'])} |"
            )
        L.append("")


def render_e3(res: dict) -> str:
    L: list[str] = []
    L.append("# Experiment E3 -- insider_net_buy robustness slices")
    L.append("")
    L.append(f"_H: {res['hypothesis']}_")
    L.append("")
    L.append(f"- window: {res['data_start']} -> {res['data_end']}  ;  symbols: {res['n_symbols']}  ;  "
             f"insider_net_buy events: {res['n_events']}")
    L.append(f"- round-trip cost subtracted from NET (CNC): {res['round_trip_pct']:.4f}% "
             f"(breakeven at Rs {REFERENCE_NOTIONAL}); GROSS columns are pre-cost.")
    L.append("- Events are the event_study insider-buy leg VERBATIM (trailing-10-session open-market "
             "buy crossings >= Rs 1cr, entry at close_T, directional long drift).")
    L.append(f"- person_category groups (value-share dominant): {list(CATEGORY_GROUPS)}")
    if res["liquidity_bounds"] is not None:
        L.append(f"- liquidity tercile bounds (20d median close*volume): "
                 f"low<= {res['liquidity_bounds'][0]} <mid<= {res['liquidity_bounds'][1]} <high")
    L.append("")
    if res["n_events"] == 0:
        L.append("> NO insider_net_buy crossings for the requested symbols/window -- no evidence "
                 "either way. Reported honestly (C9); backfill more history or widen the window.")
        L.append("")
    _render_e3_family(L, "Slice (a) -- by calendar year of entry", "by_year", res["slices"]["by_year"])
    _render_e3_family(L, "Slice (b) -- by dominant person_category (value share)", "by_category",
                      res["slices"]["by_category"])
    _render_e3_family(L, "Slice (c) -- by 20d median-traded-value liquidity tercile", "by_liquidity",
                      res["slices"]["by_liquidity"])
    L.append("## Discriminator")
    L.append("")
    L += _e3_discriminator_lines(res)
    L.append("")
    return "\n".join(L)


def _e3_discriminator_lines(res: dict) -> list[str]:
    def _t20_net(buckets: dict, order_family: str) -> list[str]:
        if not buckets:
            return ["  (none)"]
        out = []
        for b in _e3_bucket_order(order_family, buckets):
            s = buckets[b][20]
            out.append(f"  {b}: n={s['n']} T+20 net mean={_fmt_pct(s['mean_net'])}%")
        return out

    lines = ["- T+20 NET mean by calendar year:"]
    lines += _t20_net(res["slices"]["by_year"], "by_year")
    lines.append("- T+20 NET mean by dominant person_category:")
    lines += _t20_net(res["slices"]["by_category"], "by_category")
    lines.append("- T+20 NET mean by liquidity tercile:")
    lines += _t20_net(res["slices"]["by_liquidity"], "by_liquidity")
    lines.append("- Read: the edge is ROBUST only if T+20 net stays positive across every year and "
                 "category slice; a positive that lives in one year or one category is FRAGILE (C9).")
    return lines


_RENDERERS = {"e1": render_e1, "e2": render_e2, "e3": render_e3}


# =========================================================================== driver
def _write(res: dict, md: str, reports_dir: Path, ts: str) -> tuple[Path, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    eid = res["experiment"]
    md_path = reports_dir / f"experiment_{eid}_{ts}.md"
    json_path = reports_dir / f"experiment_{eid}_{ts}.json"
    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    return md_path, json_path


def _resolve_symbols(store: MarketStore, end: date, *, lookback: int = 60) -> list[str]:
    for i in range(lookback + 1):
        rows = store.get_universe_daily(end - timedelta(days=i), included_only=True)
        if rows:
            return [r["symbol"] for r in rows]
    return []


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Pre-registered filings experiments E1-E3 (WORKLOG 2026-07-17).")
    parser.add_argument("experiment", choices=("e1", "e2", "e3"))
    parser.add_argument("--from", dest="start", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="end", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--symbols", default=None, help="comma-separated symbols (else the latest universe_daily)")
    parser.add_argument("--reports-dir", default=None, help="default: <data_dir>/reports")
    args = parser.parse_args(argv)

    settings = load_settings()
    clock = Clock()
    store = MarketStore.from_settings(settings, clock).open()
    reports_dir = Path(args.reports_dir) if args.reports_dir else settings.resolved_data_dir() / "reports"
    try:
        symbols = (
            [s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols
            else _resolve_symbols(store, args.end)
        )
        suspicious = [s for s in symbols if s.startswith(("$", "%")) or "%" in s]
        if suspicious:
            print(
                f"ERROR: --symbols contains unexpanded shell variable(s): {suspicious} -- run from "
                "PowerShell or pass the comma-separated list explicitly; nothing was run",
                file=sys.stderr,
            )
            return 2
        if not symbols:
            print("no symbols resolved (empty universe_daily and no --symbols) -- nothing to run",
                  file=sys.stderr)
            return 2

        cost_model = CostModel.from_config()
        f = settings.filings
        if args.experiment == "e1":
            res = run_e1(store, cost_model, clock, symbols, args.start, args.end,
                         insider_min_value_inr=f.insider_min_value_inr)
        elif args.experiment == "e2":
            res = run_e2(store, cost_model, clock, symbols, args.start, args.end,
                         insider_min_value_inr=f.insider_min_value_inr,
                         pledge_delta_min_pct=f.pledge_delta_min_pct)
        else:
            res = run_e3(store, cost_model, clock, symbols, args.start, args.end,
                         insider_min_value_inr=f.insider_min_value_inr)

        md = _RENDERERS[args.experiment](res)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        md_path, json_path = _write(res, md, reports_dir, ts)
        # ASCII-only console summary.
        headline = res.get("n_trades_total", res.get("n_events"))
        print(f"experiment {args.experiment}: {headline} trades/events over {len(symbols)} symbols "
              f"-> {md_path}")
        print(f"  json: {json_path}")
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
