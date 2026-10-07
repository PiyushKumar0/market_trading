"""Shared loaders and conventions for the plan-2026-10-06 M5 studies (IMPLEMENTATION_PLAN §6.1, C1/C2/C3).

Every population is built by the registered study's own helpers, imported from the sibling scripts.
Reads a research snapshot read-only; refuses the live store and refuses to run in market hours.
"""

from __future__ import annotations

import importlib.util
import json
import os
import statistics
import sys
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

_REPO_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _REPO_SRC not in sys.path:  # pragma: no cover - loose-script shim
    sys.path.insert(0, _REPO_SRC)

import engine  # noqa: E402,F401,I001  native import-order guard (engine._preload), keep FIRST
import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import yaml  # noqa: E402
from engine.core.calendar import NSECalendar  # noqa: E402
from engine.core.clock import IST, Clock  # noqa: E402
from engine.core.config import config_dir, load_settings  # noqa: E402
from engine.learning.exit_sim import Held, Outcome, SimBar, simulate  # noqa: E402
from engine.learning.validate import margin_floor_pct_per_day  # noqa: E402
from engine.marketdata.store import MarketStore  # noqa: E402
from engine.risk.limits import LimitTable  # noqa: E402
from engine.strategy.cost_model import CostModel  # noqa: E402
from engine.strategy.indicators import wilder_atr  # noqa: E402
from engine.strategy.scanners import brk20, hi52  # noqa: E402
from engine.strategy.types import RawLevels  # noqa: E402


def _load_script(name: str) -> ModuleType:
    mod_name = f"mt_{name}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, Path(__file__).with_name(f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


bb = _load_script("backtest_brk20")
bh = _load_script("backtest_hi52")
es = _load_script("event_study")

PRODUCT = "CNC"
NOTIONAL = Decimal("20000")
HORIZON = 20                      # T+20: the close of the 20th session counting the fill session as 1 (D2)
PURGE = EMBARGO = HORIZON
HELD_OUT_FROM = date(2026, 1, 1)
FULL_HISTORY_START = date(2000, 1, 1)
BRK20_UNIVERSE_AS_OF = date(2026, 9, 11)   # universe_daily day the 2026-09-12 registration read
INS_REGISTERED_WINDOW = (date(2023, 8, 1), date(2026, 7, 16))   # event_study_20260814T023244 (C1 "the event study's window")
ATR_PERIOD = 14
MARKET_OPEN, MARKET_CLOSE = time(9, 15), time(15, 30)
SURVIVORSHIP_LABEL = "SURVIVORSHIP-TAINTED PROXY (current membership applied backwards)"

# Sessions after the signal an admitted event must have usable bars for: the fill window plus the
# longest exit any study reads (brk20's registered close(fill+20); T+20 elsewhere).
SPAN = {"brk20": bb.MAX_FILL_WINDOW + HORIZON, "hi52": HORIZON, "ins": HORIZON}

ascii_text = bb._ascii
stats = bb._stats


class Refusal(RuntimeError):
    """The run must not proceed; the CLI exits 2."""


@dataclass(frozen=True)
class Event:
    """One admitted signal with its registered fill. ``atr`` is ATR14 through the signal session."""

    symbol: str
    series: Any
    signal_idx: int
    fill_idx: int
    fill_px: Decimal
    atr: float
    levels: RawLevels | None = None

    @property
    def signal_date(self) -> date:
        return self.series.dates[self.signal_idx]

    @property
    def fill_date(self) -> date:
        return self.series.dates[self.fill_idx]


@dataclass
class Population:
    strategy: str
    source: str
    n_symbols: int
    events: list[Event]
    counts: dict[str, int]
    structural: dict[str, list[date]]


@dataclass(frozen=True)
class Trade:
    symbol: str
    signal_date: date
    fill_date: date
    reason: str
    gross: float
    net: float
    net_sized: float | None = None


# ----------------------------------------------------------------------------- refusals + access
def market_hours_refusal(now: datetime | None = None) -> str | None:
    now = (now or datetime.now(IST)).astimezone(IST)
    if NSECalendar(config_dir() / "calendar", Clock()).is_trading_day(now.date()) and (
        MARKET_OPEN <= now.time() < MARKET_CLOSE
    ):
        return f"market hours ({now:%Y-%m-%d %H:%M} IST); run outside 09:15-15:30 on a trading day"
    return None


def open_snapshot(db: Path) -> duckdb.DuckDBPyConnection:
    live = load_settings().duckdb_path()
    p = Path(db)
    if p.name == live.name or p.resolve() == live.resolve():
        raise Refusal(f"{p} is the live store; pass a research snapshot (data/research/market_<date>.duckdb)")
    try:
        conn = bb.open_readonly(p)
    except bb.DbUnopenable as exc:
        raise Refusal(str(exc)) from exc
    conn.execute("SET TimeZone='Asia/Kolkata'")
    return conn


def store_reader(conn: duckdb.DuckDBPyConnection) -> MarketStore:
    """A MarketStore whose reads run on ``conn``. ``open()`` is never called, so no DDL runs."""
    store = MarketStore(Path(), Path(), Clock())
    store._con = conn
    return store


# ----------------------------------------------------------------------------- populations
def _series(conn, names, end: date, max_symbols: int | None) -> dict[str, Any]:
    return bb.load_series(conn, FULL_HISTORY_START, end, symbols=sorted(names), max_symbols=max_symbols)


def _restrict(names, symbols) -> set[str]:
    names = {n.upper() for n in names}
    return names & {s.upper() for s in symbols} if symbols else names


def _admitted(s, i: int, span: int, atr: np.ndarray, counts: dict[str, int]) -> bool:
    if not bb.bars_usable(s, i, span):
        brk20._bump(counts, "short_forward_window")
        return False
    if not np.isfinite(atr[i]):
        brk20._bump(counts, "no_atr14")
        return False
    return True


def _atr(s) -> np.ndarray:
    return wilder_atr(s.high, s.low, s.close, ATR_PERIOD).to_numpy()


def px(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"))


def brk20_population(conn, end: date, *, symbols=None, max_symbols=None) -> Population:
    """brk20 V2-N5 fills: ``backtest_brk20.signals_for`` + ``fill_for`` (touch, 5 sessions)."""
    eligible = store_reader(conn).get_universe_eligible_symbols(BRK20_UNIVERSE_AS_OF)
    if not eligible:
        raise Refusal(f"universe_daily has no rows on {BRK20_UNIVERSE_AS_OF} (the brk20 registration's day)")
    series = _series(conn, _restrict(eligible, symbols), end, max_symbols)
    ex, structural = bb.load_ex_dates(conn), bb.load_structural_ex_dates(conn)
    params, floor = bb.PRE_REGISTERED_PARAMS, bb.PRE_REGISTERED_FLOOR_PARAMS
    width = bb._scan_window_rows(params, floor)
    counts: dict[str, int] = {}
    events: list[Event] = []
    for sym, s in series.items():
        atr = _atr(s)
        for sig in bb.signals_for(s, params, ex.get(sym, []), structural.get(sym, []),
                                  floor_params=floor, veto_counts=counts):
            brk20._bump(counts, "signals")
            if not _admitted(s, sig.idx, SPAN["brk20"], atr, counts):
                continue
            fill = bb.fill_for(s, sig, bb.VARIANT_V2_N5)
            if fill is None:
                brk20._bump(counts, "unfilled")
                continue
            cand = brk20.scan_daily(sym, bb._window(s, sig.idx, width), today=s.dates[sig.idx + 1],
                                    upcoming_ex_dates=ex.get(sym, []), params=params)
            events.append(Event(sym, s, sig.idx, fill[0], px(fill[1]), float(atr[sig.idx]), cand.raw_levels))
    source = f"universe_daily eligible set as of {BRK20_UNIVERSE_AS_OF}"
    return Population("brk20", source, len(series), events, counts, structural)


def hi52_population(conn, db: Path, end: date, *, symbols=None, max_symbols=None) -> Population:
    """hi52 v2: ``backtest_hi52.discrete_signals`` at ``hi52.DEFAULT_PARAMS`` over the index members."""
    # The snapshot sits in <data>/research; the ladder looks for <data>/universe/index_cached.csv.
    members, source = bh.load_index_members(Path(db).resolve().parent)
    if not members:
        raise Refusal("no index membership list (hi52 population)")
    series = _series(conn, _restrict(members, symbols), end, max_symbols)
    ex, structural = bb.load_ex_dates(conn), bb.load_structural_ex_dates(conn)
    counts: dict[str, int] = {}
    events: list[Event] = []
    for sym, s in series.items():
        atr = _atr(s)
        for i, _score in bh.discrete_signals(s, hi52.DEFAULT_PARAMS, ex.get(sym, []),
                                             structural.get(sym, []), veto_counts=counts):
            brk20._bump(counts, "signals")
            if _admitted(s, i, SPAN["hi52"], atr, counts):
                events.append(Event(sym, s, i, i + 1, px(s.open[i + 1]), float(atr[i])))
    return Population("hi52", source, len(series), events, counts, structural)


def ins_population(conn, end: date, *, symbols=None, max_symbols=None) -> Population:
    """``insider_net_buy`` events as the registered event study built them: its universe, its signal
    window (``INS_REGISTERED_WINDOW``) and crossing; bars run to ``end`` so late signals keep T+20."""
    store = store_reader(conn)
    start, last = INS_REGISTERED_WINDOW
    last = min(last, end)
    names = es._resolve_symbols(store, last)
    threshold = load_settings().filings.insider_min_value_inr
    series = _series(conn, _restrict(names, symbols), end, max_symbols)
    structural = bb.load_structural_ex_dates(conn)
    counts: dict[str, int] = {}
    events: list[Event] = []
    for sym, s in series.items():
        w0, w1 = bisect_left(s.dates, start), bisect_right(s.dates, last)
        if len(s) - w0 < es.VOL_WINDOW + max(es.HORIZONS) + 2:
            continue
        atr = _atr(s)
        for t in es.insider_buy_events(s.dates[w0:w1], store.get_insider_trades(symbol=sym), threshold):
            i = w0 + t
            brk20._bump(counts, "signals")
            if i + 1 < len(s) and bb._unadjusted_at(structural.get(sym, []), s.dates[i + 1]):
                brk20._bump(counts, hi52.VETO_UNADJUSTED_HISTORY)
                continue
            if _admitted(s, i, SPAN["ins"], atr, counts):
                events.append(Event(sym, s, i, i + 1, px(s.open[i + 1]), float(atr[i])))
    source = f"universe_daily included set as of the latest day <= {last}, signals {start} -> {last}"
    return Population("ins", source, len(series), events, counts, structural)


# ----------------------------------------------------------------------------- measurement
def walk(ev: Event, *, stop: Decimal | None, target: Decimal | None, horizon: int,
         cost_pct: Decimal, ex_dates) -> Outcome:
    """``exit_sim`` research convention: bar positions, Held from the fill, fill session not evaluated.

    ``horizon`` counts sessions after the fill session (T+k is ``k - 1``)."""
    s = ev.series
    bars = [SimBar(s.dates[j], px(s.open[j]), px(s.high[j]), px(s.low[j]), px(s.close[j]))
            for j in range(ev.fill_idx, ev.fill_idx + horizon + 1)]
    return simulate(bars, Held(ev.fill_px, datetime.combine(ev.fill_date, time())), stop=stop,
                    target=target, horizon=horizon, end=bars[-1].d, cost_pct=cost_pct, ex_dates=ex_dates)


def load_limits() -> LimitTable:
    return LimitTable.model_validate(yaml.safe_load((config_dir() / "limits.yaml").read_text(encoding="utf-8")))


def sized_notional(fill: Decimal, stop: Decimal | None, limits: LimitTable) -> Decimal | None:
    """§7.1 swing sizing at the capital base: risk budget over gap-multiplied stop distance, capped
    by the per-stock CNC notional and the deployed-capital cap. ``None`` when unsizable."""
    if stop is None or stop >= fill:
        return None
    lim = limits.limits
    budget = Decimal(str(lim.per_trade_risk.swing_position_pct)) / 100 * limits.capital_base_inr
    unit = Decimal(str(lim.per_trade_risk.overnight_gap_mult)) * (fill - stop)
    qty = min(budget // unit, lim.per_stock_exposure.cnc_notional_inr // fill,
              lim.capital_cap.max_deployed_capital_inr // fill)
    return qty * fill if qty >= 1 else None


def cost_pct(cost_model: CostModel, notional: Decimal = NOTIONAL) -> Decimal:
    return cost_model.breakeven_pct(notional, PRODUCT)


def trade_stats(trades: list[Trade]) -> dict[str, Any]:
    out = stats([t.gross for t in trades], [t.net for t in trades])
    sized = [t.net_sized for t in trades if t.net_sized is not None]
    out["n_sized"] = len(sized)
    out["median_net_sized"] = round(statistics.median(sized), 4) if sized else None
    return out


def geometry(trades: list[Trade], cost: float) -> dict[str, Any]:
    """Median GROSS at T+20 against one round trip, with the WO-3 margin floor per session."""
    gross = [t.gross for t in trades]
    med = statistics.median(gross) if gross else None
    return {
        "n": len(gross),
        "median_gross_pct": None if med is None else round(med, 4),
        "cost_floor_pct": round(cost, 6),
        "margin_floor_pct_per_session": margin_floor_pct_per_day(cost, margin_floor_days=HORIZON),
        "verdict": "unknown" if med is None else ("viable" if med > cost else "dead"),
    }


def cpcv(trades: list[Trade], n_trials: int, cost: float) -> dict[str, Any]:
    """CPCV on the per-fill-day net series (purge = embargo = 20) + ``promotion_decision`` at ``n_trials``."""
    rows = [SimpleNamespace(fill_date=t.fill_date, net={HORIZON: t.net}) for t in trades]
    return bb.cpcv_report(rows, HORIZON, cost, trial_count=n_trials)


def held_out(trades: list[Trade]) -> list[Trade]:
    return [t for t in trades if t.signal_date >= HELD_OUT_FROM]


# ----------------------------------------------------------------------------- reports
def default_out(db: Path, prefix: str) -> Path:
    """``<data>/reports/<prefix>_<date>.json`` beside the snapshot's ``<data>/research`` dir."""
    stem = Path(db).stem
    tag = stem.removeprefix("market_") if stem.startswith("market_") else date.today().isoformat()
    return Path(db).resolve().parent.parent / "reports" / f"{prefix}_{tag}.json"


def write_reports(doc: dict[str, Any], markdown: str, out_json: Path) -> Path:
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    md = out_json.with_suffix(".md")
    md.write_text(markdown, encoding="utf-8")
    return md
