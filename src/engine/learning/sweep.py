"""Phase-1 vectorbt parameter sweep (§6.4 step 1, §8.2) — the multiple-testing trial-count source.

Vectorized backtests of the four §6.1 **price-only** baselines over the §6.3 envelope grid, with
round-trip costs from :class:`~engine.strategy.cost_model.CostModel`. The sweep's job is threefold:

1. score every grid configuration (per-param-set trade stats), and
2. report the **trial count N** = the number of configurations evaluated (grid cardinality) — the
   §6.4 multiple-testing deflation input the ``ValidationReport`` must cite (a 200-point sweep can
   NOT log ``N=1`` and keep the promotion bar lenient — that is the gaming hole §6.4 step 1 closes), and
3. expose :meth:`SweepRunner.returns_for` — the cost-adjusted **daily** net-return series a candidate
   feeds into :class:`~engine.learning.validate.ValidationPipeline` (walk-forward + CPCV).

The indicator math is the SAME pure ``engine.strategy.indicators`` the live scanners use (§6.1), so
research and live cannot diverge (§9.6). vectorbt is imported **function-level** (native import-order
guard, ``engine._preload`` — ``import engine`` establishes sklearn's OpenMP runtime before numba/vectorbt
load); the module top level is pandas/numpy + stdlib only, so ``import engine.learning.sweep`` stays
cheap for the pure-Python test tier.

Documented modelling choices (Phase-1 backtests are **vectorbt-vectorized only**, §8.2; the
event-driven ``ReplayHarness`` + ``PaperBroker`` re-validate these baselines in Phase 3):

* **Long-only.** Every Phase-1 baseline is long-only (the §1.4.9 shorts gate is not open); ``orb``
  models upside breakouts only. The scanner records SELL/short candidates for §6.1 attribution, but
  they are not tradeable and are not backtested here.
* **Per-symbol, equal-weight.** Each symbol is an independent single-name backtest (its own cash);
  the strategy's daily return is the cross-sectional **mean** of the per-symbol daily returns (0 on a
  day a name is flat) — the return of an equal-weight allocation running the rule across the frame.
  §7.1 portfolio limits / concurrent-position caps / sizing are the gate + paper layer's job (Phase
  2/3), deliberately NOT modelled in the raw-edge sweep.
* **Costs.** A constant proportional per-side fee = ½ × ``CostModel`` round-trip breakeven at a
  reference notional (``reference_notional``, default ₹20,000), charged by vectorbt on BOTH legs so a
  round trip pays ≈ the full breakeven. This is an **approximation**: the fixed cost components (DP
  flat, delivery brokerage flat, MIS per-order cap) do not scale linearly with notional, so the fee
  is exact only near the reference book size — documented in every report's notes. ``orb`` is priced
  MIS; ``rsi2``/``trend``/``mom`` are priced CNC (delivery).
* **Stops/exits.** ``orb`` uses vectorbt ``sl_stop``/``tp_stop`` per-signal fractions (intrabar via
  high/low) with risk anchored at the OPPOSITE opening-range edge (§6.1 v2 2026-07-12:
  ``stop_range_frac × (entry − range_low)``; sub-cost-floor breakouts — risk < 2× round-trip
  breakeven, C3 — are skipped, mirroring the live §7.1 cost gate) plus a forced session-end
  square-off (MIS). ``rsi2`` exits on RSI>``rsi_exit`` OR a ``max_hold_days`` scheduled time-exit
  (modelled 2026-07-12) with a constant ``stop_pct`` protective stop; ``trend`` exits on the 20/50
  EMA death cross with an ATR-fraction trailing stop; ``mom`` turns the book over on the rebalance
  cadence. The A12 ex-date skip and ``flagged_instrument_days`` suppression are live/Phase-3
  concerns (the sweep runs on corp-action-adjusted bars, A11) — noted honestly, never silently
  dropped.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.log import get_logger
from engine.strategy.indicators import (
    cross_sectional_rank,
    ema,
    rolling_median_volume,
    sma,
    wilder_adx,
    wilder_atr,
    wilder_rsi,
)

if TYPE_CHECKING:
    from engine.marketdata.store import MarketStore
    from engine.strategy.cost_model import CostModel

_log = get_logger("engine.learning.sweep")

PRICE_BASELINES: tuple[str, ...] = ("orb", "rsi2", "trend", "mom")

#: product each baseline is costed under (§6.1: orb intraday MIS; the rest delivery CNC).
_PRODUCT: dict[str, str] = {"orb": "MIS", "rsi2": "CNC", "trend": "CNC", "mom": "CNC"}

#: grid density → target points per parameter (before the default is unioned in). Configurable via
#: the CLI ``--grid-density``; the grid cardinality is the trial count N (§6.4 step 1).
DENSITY_POINTS: dict[str, int] = {"coarse": 2, "medium": 3, "fine": 5}

_ORB_ENTRY_START = time(9, 30)      # §6.1 orb base entry window (intersect owner window live)
_ORB_ENTRY_END = time(14, 30)
_MEDIAN_WINDOW = 20                 # §6.1 20-bar volume median
_ATR_PERIOD = 14
_RSI_PERIOD = 2
_STOCK_DMA = 200
_MOM_LOOKBACK = 20                  # 4 weeks × 5 sessions (indicators.momentum)
_TRADING_DAYS_Y = 252


# --------------------------------------------------------------------------- report models
class ParamSetStat(BaseModel):
    """Trade-level stats for ONE grid configuration (statistics, not ledger money → floats)."""

    model_config = ConfigDict(frozen=True)

    params: dict[str, float]
    n_trades: int
    win_rate: float | None                 # fraction of trades with net return > 0
    expectancy_pct: float | None           # mean per-trade net return, %
    total_return_pct: float | None         # compounded net return of the equal-weight daily series, %
    sharpe: float | None                   # annualized (252) Sharpe of the daily series
    max_drawdown_pct: float | None         # positive magnitude


class SweepReport(BaseModel):
    """Output of one strategy sweep. ``trial_count_n`` = grid cardinality (§6.4 step 1)."""

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    product: str
    grid_density: str
    trial_count_n: int                     # THE §6.4 multiple-testing input (every config evaluated)
    n_symbols: int
    symbols: list[str]
    data_start: date | None
    data_end: date | None
    reference_notional: str                # Decimal as string (money convention)
    per_side_fee_pct: float                # the modelled constant per-side fee, %
    stats: list[ParamSetStat]
    best_params: dict[str, float] | None   # ranked by expectancy_pct then total_return_pct
    notes: list[str] = Field(default_factory=list)
    generated_at: datetime


# --------------------------------------------------------------------------- envelope + grid (§6.3)
def load_envelope(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """Parse ``config/envelope.yaml`` → ``{param_name: {min, max, default, used_by}}`` (§6.3 bounds)."""
    p = Path(path) if path is not None else config_dir() / "envelope.yaml"
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    return dict(raw["parameters"])


def _is_integer_param(spec: Mapping[str, Any]) -> bool:
    return all(float(spec[k]).is_integer() for k in ("min", "max", "default"))


def _param_points(spec: Mapping[str, Any], points: int) -> list[float]:
    """``points`` values spanning ``[min, max]`` (endpoints inclusive) with the default always in.

    Integer-valued envelope rows (e.g. ``orb.orb_minutes``, ``mom.top_n``) collapse to unique integer
    steps; float rows round to 6 dp to keep grid keys stable/deterministic (§9.6).
    """
    lo, hi, default = float(spec["min"]), float(spec["max"]), float(spec["default"])
    k = max(1, int(points))
    raw = [lo] if k == 1 else list(np.linspace(lo, hi, k))
    raw.append(default)
    if _is_integer_param(spec):
        vals = sorted({int(round(v)) for v in raw})
        return [float(v) for v in vals]
    vals_f = sorted({round(float(v), 6) for v in raw})
    return vals_f


def build_param_grid(
    strategy_id: str, *, points: int = 2, envelope: Mapping[str, dict[str, Any]] | None = None
) -> list[dict[str, float]]:
    """Cartesian product of §6.3 per-parameter value lists for ``strategy_id`` (bare, un-namespaced).

    ``points`` is the target values-per-parameter (grid density); the strategy default is always
    unioned in so the champion baseline is one of the evaluated configs. Returns the full grid; its
    length is the trial count N (§6.4 step 1).
    """
    env = dict(envelope) if envelope is not None else load_envelope()
    prefix = strategy_id + "."
    rows = {k[len(prefix):]: v for k, v in env.items() if k.startswith(prefix)}
    if not rows:
        raise ValueError(f"no §6.3 envelope parameters for strategy {strategy_id!r}")
    names = sorted(rows)
    axes = [_param_points(rows[n], points) for n in names]
    return [dict(zip(names, combo, strict=True)) for combo in itertools.product(*axes)]


# --------------------------------------------------------------------------- loaded price frames
@dataclass
class _Frames:
    """Wide OHLCV frames (rows = time, cols = symbol) for one strategy over one window."""

    close: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    open: pd.DataFrame
    volume: pd.DataFrame
    intraday: bool
    auction_open: pd.DataFrame | None = None       # 1m only; 09:15 seed value per session
    index_closes: pd.Series | None = None          # rsi2 regime input (NIFTY 50 daily closes)
    # Period-FIXED indicator series cached across grid configs (no swept param changes them; the
    # grid loop otherwise re-runs the same Wilder recursions ~N-configs times per symbol).
    cache: dict[str, Any] = field(default_factory=dict)

    @property
    def symbols(self) -> list[str]:
        return list(self.close.columns)

    @property
    def empty(self) -> bool:
        return self.close.shape[1] == 0 or self.close.shape[0] == 0


def _wide(per_symbol: dict[str, pd.DataFrame], field: str) -> pd.DataFrame:
    """Assemble a wide frame for one OHLCV ``field`` across symbols on the union index."""
    cols = {sym: df[field] for sym, df in per_symbol.items()}
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(cols).sort_index()


def _daily_symbol_frame(store: MarketStore, symbol: str, start: date, end: date) -> pd.DataFrame | None:
    # Bulk float read (get_bars_1d_frame) — NOT the pydantic get_bars_1d path: at backtest scale the
    # per-row Bar construction + float(Decimal) passes measured ~50 µs/row (~7.5 min per 9M-row load).
    df = store.get_bars_1d_frame(symbol, start, end)
    return df if len(df) else None


def _intraday_symbol_frame(
    store: MarketStore, symbol: str, start: date, end: date
) -> pd.DataFrame | None:
    start_dt = datetime.combine(start, time(0, 0), tzinfo=IST)
    end_dt = datetime.combine(end + timedelta(days=1), time(0, 0), tzinfo=IST)
    df = store.get_bars_1m_frame(symbol, start_dt, end_dt)
    return df if len(df) else None


# --------------------------------------------------------------------------- the runner
class SweepRunner:
    """Runs §6.1-baseline vectorbt sweeps and exposes the validate-ready returns provider.

    Parameters
    ----------
    store:
        The read surface for bars (``get_bars_1d`` / ``get_bars_1m``). Opened by the caller.
    cost_model:
        Round-trip cost source (C1–C4). The per-side fee is derived once per strategy from its
        breakeven at ``reference_notional``.
    clock:
        Single "now" (§3.2) — stamps ``generated_at``.
    reference_notional / init_cash:
        The book size the constant per-side fee is calibrated at, and per-symbol backtest cash.
    index_symbol:
        Optional reference-index symbol whose daily closes drive the ``rsi2`` regime filter. ``None``
        ⇒ the regime filter is DISABLED for the sweep (noted in the report); the live scanner always
        applies it.
    """

    def __init__(
        self,
        store: MarketStore,
        cost_model: CostModel,
        clock: Clock,
        *,
        reference_notional: Decimal = Decimal("20000"),
        init_cash: float = 100_000.0,
        index_symbol: str | None = None,
    ) -> None:
        self._store = store
        self._cost_model = cost_model
        self._clock = clock
        self._reference_notional = Decimal(reference_notional)
        self._init_cash = float(init_cash)
        self._index_symbol = index_symbol
        # frames cached per strategy so returns_for() recomputes without re-reading the store.
        self._frames: dict[str, _Frames] = {}
        self._fee: dict[str, float] = {}
        # daily-return series memo per (strategy, params) — run()'s grid loop already computed the
        # winning config's series; returns_for() must not silently re-run that full backtest.
        self._returns_cache: dict[tuple[str, tuple[tuple[str, float], ...]], pd.Series] = {}

    # ------------------------------------------------------------------ costs
    def _per_side_fee(self, strategy_id: str) -> float:
        """½ × round-trip breakeven at the reference notional, as a fraction (charged both legs)."""
        product = _PRODUCT[strategy_id]
        be_pct = float(self._cost_model.breakeven_pct(self._reference_notional, product))
        return be_pct / 100.0 / 2.0

    # ------------------------------------------------------------------ public surface
    def run(
        self,
        strategy_id: str,
        start: date,
        end: date,
        *,
        symbols: Sequence[str],
        grid_density: str = "coarse",
        param_grid: Sequence[Mapping[str, float]] | None = None,
    ) -> SweepReport:
        """Backtest every grid config of ``strategy_id`` over ``[start, end]`` on ``symbols``.

        ``param_grid`` overrides the §6.3-derived grid (used by the smoke test for a tiny grid); when
        omitted the grid is :func:`build_param_grid` at the ``grid_density`` point count. The report's
        ``trial_count_n`` is ALWAYS the number of configurations actually evaluated.
        """
        if strategy_id not in PRICE_BASELINES:
            raise ValueError(f"unknown baseline {strategy_id!r}; expected one of {PRICE_BASELINES}")
        if grid_density not in DENSITY_POINTS and param_grid is None:
            raise ValueError(f"grid_density must be one of {sorted(DENSITY_POINTS)}, got {grid_density!r}")

        frames = self._load_frames(strategy_id, start, end, symbols)
        self._frames[strategy_id] = frames
        fee = self._per_side_fee(strategy_id)
        self._fee[strategy_id] = fee
        # New frames ⇒ prior memoized series for this strategy are stale.
        self._returns_cache = {k: v for k, v in self._returns_cache.items() if k[0] != strategy_id}

        grid: list[dict[str, float]] = (
            [dict(p) for p in param_grid]
            if param_grid is not None
            else build_param_grid(strategy_id, points=DENSITY_POINTS[grid_density])
        )

        notes = self._modelling_notes(strategy_id, frames)
        stats: list[ParamSetStat] = []
        if frames.empty:
            notes.append("NO BARS for the requested symbols/window — every config scored zero trades.")
        for params in grid:
            stats.append(self._score(strategy_id, frames, params, fee))

        best = self._rank_best(stats)
        d_start, d_end = self._data_span(frames)
        report = SweepReport(
            strategy_id=strategy_id,
            product=_PRODUCT[strategy_id],
            grid_density=grid_density if param_grid is None else "custom",
            trial_count_n=len(grid),
            n_symbols=len(frames.symbols),
            symbols=frames.symbols,
            data_start=d_start,
            data_end=d_end,
            reference_notional=str(self._reference_notional),
            per_side_fee_pct=round(fee * 100.0, 6),
            stats=stats,
            best_params=best,
            notes=notes,
            generated_at=self._clock.now(),
        )
        _log.info(
            "sweep_done",
            strategy=strategy_id,
            trial_count_n=report.trial_count_n,
            n_symbols=report.n_symbols,
            best=best,
        )
        return report

    def returns_for(self, strategy_id: str, params: Mapping[str, float]) -> pd.Series:
        """Cost-adjusted **daily** net-return series for ``params`` — the ValidationPipeline provider.

        Reuses the frames cached by the most recent :meth:`run` for ``strategy_id`` (deterministic:
        same frames + params ⇒ same series, §9.6). Index is ``datetime.date``, ascending.
        """
        frames = self._frames.get(strategy_id)
        if frames is None:
            raise RuntimeError(
                f"no frames cached for {strategy_id!r} — call run() before returns_for()"
            )
        cached = self._returns_cache.get((strategy_id, _params_key(params)))
        if cached is not None:      # run()'s grid loop already backtested this exact config
            return cached.copy()
        fee = self._fee.get(strategy_id, self._per_side_fee(strategy_id))
        pf = self._backtest(strategy_id, frames, dict(params), fee)
        return self._daily_returns(pf, frames.intraday)

    # ------------------------------------------------------------------ loading
    def _load_frames(
        self, strategy_id: str, start: date, end: date, symbols: Sequence[str]
    ) -> _Frames:
        syms = list(dict.fromkeys(symbols))  # dedupe, keep order
        loader = self._intraday_frame if strategy_id == "orb" else self._daily_frame
        per: dict[str, pd.DataFrame] = {}
        for sym in syms:
            df = loader(sym, start, end)
            if df is not None and not df.empty:
                per[sym] = df
        intraday = strategy_id == "orb"
        frames = _Frames(
            close=_wide(per, "close"),
            high=_wide(per, "high"),
            low=_wide(per, "low"),
            open=_wide(per, "open"),
            volume=_wide(per, "volume"),
            intraday=intraday,
            auction_open=_wide(per, "auction_open") if intraday and per else None,
        )
        if strategy_id == "rsi2" and self._index_symbol is not None:
            idx_df = self._daily_frame(self._index_symbol, start, end)
            if idx_df is not None and not idx_df.empty:
                frames.index_closes = idx_df["close"]
        return frames

    def _daily_frame(self, symbol: str, start: date, end: date) -> pd.DataFrame | None:
        return _daily_symbol_frame(self._store, symbol, start, end)

    def _intraday_frame(self, symbol: str, start: date, end: date) -> pd.DataFrame | None:
        return _intraday_symbol_frame(self._store, symbol, start, end)

    # ------------------------------------------------------------------ scoring
    def _score(
        self, strategy_id: str, frames: _Frames, params: Mapping[str, float], fee: float
    ) -> ParamSetStat:
        if frames.empty:
            return ParamSetStat(
                params=dict(params), n_trades=0, win_rate=None, expectancy_pct=None,
                total_return_pct=None, sharpe=None, max_drawdown_pct=None,
            )
        pf = self._backtest(strategy_id, frames, dict(params), fee)
        daily = self._daily_returns(pf, frames.intraday)
        self._returns_cache[(strategy_id, _params_key(params))] = daily
        trades = pf.trades.records_readable
        n = int(len(trades))
        win_rate: float | None = None
        expectancy: float | None = None
        if n:
            tr_ret = trades["Return"].astype(float)
            win_rate = float((tr_ret > 0.0).mean())
            expectancy = float(tr_ret.mean() * 100.0)
        total, max_dd = _equity_stats(daily)
        return ParamSetStat(
            params=dict(params),
            n_trades=n,
            win_rate=win_rate,
            expectancy_pct=expectancy,
            total_return_pct=total,
            sharpe=_sharpe(daily),
            max_drawdown_pct=max_dd,
        )

    @staticmethod
    def _rank_best(stats: Sequence[ParamSetStat]) -> dict[str, float] | None:
        traded = [s for s in stats if s.n_trades > 0 and s.expectancy_pct is not None]
        if not traded:
            return None
        best = max(
            traded,
            key=lambda s: (s.expectancy_pct or 0.0, s.total_return_pct or 0.0, -(s.max_drawdown_pct or 0.0)),
        )
        return dict(best.params)

    @staticmethod
    def _data_span(frames: _Frames) -> tuple[date | None, date | None]:
        if frames.empty:
            return None, None
        idx = frames.close.index
        return pd.Timestamp(idx[0]).date(), pd.Timestamp(idx[-1]).date()

    def _modelling_notes(self, strategy_id: str, frames: _Frames) -> list[str]:
        notes = [
            f"Costs: constant per-side fee {self._fee.get(strategy_id, 0.0) * 100:.4f}% "
            f"(= ½ × round-trip breakeven at ₹{self._reference_notional} {_PRODUCT[strategy_id]}), "
            "charged on both legs — an approximation (fixed DP/brokerage components do not scale "
            "linearly; exact only near the reference notional).",
            "Long-only (Phase-1 §1.4.9 shorts gate); per-symbol equal-weight, no §7.1 portfolio "
            "limits (gate/paper layer, Phase 2/3).",
            "Vectorbt-vectorized only (§8.2); the event-driven ReplayHarness re-validates in Phase 3.",
        ]
        if strategy_id == "orb":
            notes.append(
                "orb v2 (2026-07-12): stop anchored at the OPPOSITE opening-range edge — risk = "
                "stop_range_frac × (entry − range low) — replacing the sub-cost-floor ATR(14,1m) "
                "unit; breakouts with risk < 2× round-trip breakeven are skipped (C3 — live this "
                "suppression is the §7.1 cost gate). Intrabar stops via high/low + forced "
                "session-end square-off; flagged_instrument_days suppression is live-only."
            )
        if strategy_id == "rsi2":
            if frames.index_closes is None:
                notes.append(
                    "rsi2 REGIME FILTER DISABLED — no reference index in the sweep frame; the live "
                    "scanner applies the 'above rising 50-DMA index' gate. max_hold_days modelled "
                    "as a scheduled time-exit (see next note)."
                )
            else:
                notes.append("rsi2: uptrend = index close > rising 50-DMA.")
            notes.append(
                "rsi2 max_hold_days is MODELLED (2026-07-12) as a time-exit scheduled max_hold_days "
                "sessions after each entry signal (previously omitted — the swept axis was a no-op). "
                "Approximation: a schedule left by an in-position entry signal can clip a subsequent "
                "re-entry early by ≤ max_hold_days sessions (rare; true semantics are the Phase-3 "
                "position manager's)."
            )
        if strategy_id == "trend":
            notes.append("trend: entry 20/50 EMA golden cross ∧ ADX>adx_min; exit death cross + ATR trail.")
        if strategy_id == "mom":
            notes.append(
                "mom: cross-sectional top_n by 4-week momentum, rebalanced every rebalance_days "
                "sessions; A12 ex-date skip is live-only (bars are corp-action-adjusted, A11)."
            )
        return notes

    # ------------------------------------------------------------------ backtest core
    def _backtest(self, strategy_id: str, frames: _Frames, params: dict[str, float], fee: float):
        import vectorbt as vbt  # function-level: engine._preload native import-order guard

        builder = {
            "orb": _signals_orb,
            "rsi2": _signals_rsi2,
            "trend": _signals_trend,
            "mom": _signals_mom,
        }[strategy_id]
        sig = builder(frames, params, fee)
        kwargs: dict[str, Any] = dict(
            close=frames.close,
            entries=sig.entries,
            exits=sig.exits,
            fees=fee,
            init_cash=self._init_cash,
            direction="longonly",
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

    @staticmethod
    def _daily_returns(pf, intraday: bool) -> pd.Series:
        r = pf.returns()
        if isinstance(r, pd.Series):
            r = r.to_frame()
        if r.shape[1] == 0:
            return pd.Series(dtype="float64")
        if intraday:
            grouped = (1.0 + r).groupby(r.index.date).prod() - 1.0
            sd = grouped.mean(axis=1)
            sd.index = pd.Index(list(grouped.index))
        else:
            sd = r.mean(axis=1)
            sd.index = pd.Index([pd.Timestamp(ts).date() for ts in sd.index])
        return sd.sort_index()


# --------------------------------------------------------------------------- signal builder outputs
@dataclass
class _Signals:
    entries: pd.DataFrame
    exits: pd.DataFrame
    sl_stop: pd.DataFrame | float | None = None
    tp_stop: pd.DataFrame | float | None = None
    sl_trail: bool = False


def _params_key(params: Mapping[str, float]) -> tuple[tuple[str, float], ...]:
    """Deterministic memo key for one grid config (§9.6)."""
    return tuple(sorted((k, float(v)) for k, v in params.items()))


def _bool_like(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(False, index=frame.index, columns=frame.columns)


def _nan_like(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(np.nan, index=frame.index, columns=frame.columns)


# --------------------------------------------------------------------------- rsi2 (§6.1 row 2)
def _signals_rsi2(frames: _Frames, params: Mapping[str, float], fee: float = 0.0) -> _Signals:
    del fee  # CNC costing enters via vectorbt ``fees=``; only orb consumes the fee in its builder
    rsi_entry = float(params["rsi_entry"])
    rsi_exit = float(params["rsi_exit"])
    stop_pct = float(params["stop_pct"])
    # §6.1 pins "exit RSI(2) > rsi_exit OR max_hold_days" — the time-exit is MODELLED (2026-07-12;
    # previously omitted, so the swept max_hold_days axis was a no-op and the reported expectancy
    # belonged to a rule the plan does not pin). 0/absent ⇒ no time-exit.
    max_hold = int(params.get("max_hold_days", 0) or 0)
    close = frames.close
    entries = _bool_like(close)
    exits = _bool_like(close)

    # RSI(2)/SMA(200)/regime are period-FIXED (no swept param touches them) — computed once per
    # frames and cached; each config then only re-does the cheap threshold comparisons.
    cached = frames.cache.get("rsi2")
    if cached is None:
        regime = (
            _index_uptrend(frames.index_closes, close.index)
            if frames.index_closes is not None
            else None
        )
        cached = (
            {sym: wilder_rsi(close[sym], _RSI_PERIOD) for sym in close.columns},
            {sym: sma(close[sym], _STOCK_DMA) for sym in close.columns},
            regime,
        )
        frames.cache["rsi2"] = cached
    rsi_by_sym, sma_by_sym, regime = cached
    for sym in close.columns:
        c = close[sym]
        rsi = rsi_by_sym[sym]
        sma200 = sma_by_sym[sym]
        entry = (rsi < rsi_entry) & (c > sma200)
        if regime is not None:
            entry = entry & regime.reindex(c.index).fillna(False)
        ent = entry.fillna(False)
        entries[sym] = ent
        ex = (rsi > rsi_exit).fillna(False)
        if max_hold > 0:
            # Time-exit scheduled max_hold SESSIONS after each entry signal. Approximation
            # (documented in the report notes): the schedule is per-SIGNAL, so a signal that fired
            # while already in position leaves a stale exit that can clip a subsequent re-entry
            # early by ≤ max_hold sessions — rare (oversold signals cluster; vectorbt ignores exits
            # while flat), and the true §6.1 semantics live in the Phase-3 position manager.
            ex_np = ex.to_numpy().copy()
            sched = np.flatnonzero(ent.to_numpy()) + max_hold
            sched = sched[sched < ex_np.size]
            ex_np[sched] = True
            exits[sym] = pd.Series(ex_np, index=c.index)
        else:
            exits[sym] = ex
    return _Signals(entries=entries, exits=exits, sl_stop=stop_pct / 100.0)


def _index_uptrend(index_closes: pd.Series, target_index: pd.Index) -> pd.Series:
    """Uptrend = index close > its 50-DMA AND the 50-DMA is rising over 20 sessions (§6.1 rsi2)."""
    sma50 = sma(index_closes, 50)
    rising = sma50 > sma50.shift(20)
    up = (index_closes > sma50) & rising
    up.index = pd.Index([pd.Timestamp(ts).date() for ts in up.index])
    tgt = pd.Index([pd.Timestamp(ts).date() for ts in target_index])
    return pd.Series(up.reindex(tgt).fillna(False).to_numpy(), index=target_index)


# --------------------------------------------------------------------------- trend (§6.1 row 3)
def _signals_trend(frames: _Frames, params: Mapping[str, float], fee: float = 0.0) -> _Signals:
    del fee  # CNC costing enters via vectorbt ``fees=``; only orb consumes the fee in its builder
    adx_min = float(params["adx_min"])
    trail_mult = float(params["trail_atr_mult"])
    close, high, low = frames.close, frames.high, frames.low
    entries = _bool_like(close)
    exits = _bool_like(close)
    sl = _nan_like(close)
    # EMA cross / ADX / ATR are period-FIXED (no swept param touches them) — one Wilder-recursion
    # pass per symbol per frames, cached across grid configs.
    cached = frames.cache.get("trend")
    if cached is None:
        cached = {}
        for sym in close.columns:
            c = close[sym]
            fast, slow = ema(c, 20), ema(c, 50)
            cached[sym] = (
                (fast.shift(1) <= slow.shift(1)) & (fast > slow),      # golden
                (fast.shift(1) >= slow.shift(1)) & (fast < slow),      # death
                wilder_adx(high[sym], low[sym], c, _ATR_PERIOD),
                wilder_atr(high[sym], low[sym], c, _ATR_PERIOD),
            )
        frames.cache["trend"] = cached
    for sym in close.columns:
        c = close[sym]
        golden, death, adx, atr = cached[sym]
        entry = golden & (adx > adx_min)
        entries[sym] = entry.fillna(False)
        exits[sym] = death.fillna(False)
        # trailing-stop fraction = trail_atr_mult × ATR/price at the entry bar (approximation).
        frac = (trail_mult * atr / c).where(entry)
        sl[sym] = frac
    return _Signals(entries=entries, exits=exits, sl_stop=sl, sl_trail=True)


# --------------------------------------------------------------------------- mom (§6.1 row 4)
def _signals_mom(frames: _Frames, params: Mapping[str, float], fee: float = 0.0) -> _Signals:
    del fee  # CNC costing enters via vectorbt ``fees=``; only orb consumes the fee in its builder
    top_n = int(params["top_n"])
    rebalance_days = int(params["rebalance_days"])
    close = frames.close
    entries = _bool_like(close)
    exits = _bool_like(close)

    mom = close / close.shift(_MOM_LOOKBACK) - 1.0
    first_valid = mom.dropna(how="all")
    if first_valid.empty:
        return _Signals(entries=entries, exits=exits)
    valid_positions = [close.index.get_loc(ix) for ix in first_valid.index]
    rebal_rows = valid_positions[::rebalance_days] if rebalance_days > 0 else valid_positions
    for pos in rebal_rows:
        ts = close.index[pos]
        row = mom.loc[ts]
        ranks = cross_sectional_rank({s: float(v) for s, v in row.items() if not np.isnan(float(v))})
        chosen = {s for s, rk in ranks.items() if rk <= top_n}
        for sym in close.columns:
            if sym in chosen:
                entries.loc[ts, sym] = True
            else:
                exits.loc[ts, sym] = True
    return _Signals(entries=entries, exits=exits)


# --------------------------------------------------------------------------- orb (§6.1 row 1)
def _signals_orb(frames: _Frames, params: Mapping[str, float], fee: float = 0.0) -> _Signals:
    orb_minutes = int(params["orb_minutes"])
    vol_mult = float(params["vol_mult"])
    frac = float(params["stop_range_frac"])   # v2 2026-07-12: stop at the opposite range edge
    rr = float(params["rr_target"])
    # §6.1 v2 cost floor (C3): a breakout whose risk cannot pay for itself twice is not a viable
    # candidate — skip entries with risk/price < 2 × round-trip breakeven (round trip = 2 × the
    # per-side fee). Live this suppression is the §7.1 cost gate's job; modelling it here keeps the
    # Phase-1 sweep aligned with the stack the signal actually flows through (scanner → gate).
    min_risk_frac = 4.0 * fee
    close = frames.close
    entries = _bool_like(close)
    exits = _bool_like(close)
    sl = _nan_like(close)
    tp = _nan_like(close)

    ts_index = pd.DatetimeIndex(close.index)
    session_dates = pd.Index([t.date() for t in ts_index])
    # Hot path (§8.2): this builder runs once per grid config over ~9M cells. Everything below is
    # positional numpy on int64-ns timestamps — the original per-(symbol, day) index masks and
    # boxed per-bar ``ts_index[p]`` comparisons cost ~8 min per config (~10 h per coarse sweep).
    # Semantics are bit-identical to the loop it replaces (verified old-vs-new on real frames).
    ts_i8 = ts_index.asi8
    day_codes, unique_days = pd.factorize(session_dates)
    day_positions = [np.flatnonzero(day_codes == k) for k in range(len(unique_days))]
    minute_ns = 60_000_000_000

    for sym in close.columns:
        c_np = close[sym].to_numpy(dtype="float64")
        h_np = frames.high[sym].to_numpy(dtype="float64")
        lo_np = frames.low[sym].to_numpy(dtype="float64")
        vol_np = frames.volume[sym].to_numpy(dtype="float64")
        auc_np = (
            frames.auction_open[sym].to_numpy(dtype="float64")
            if frames.auction_open is not None
            else None
        )
        ent_col = entries.columns.get_loc(sym)
        sl_col = sl.columns.get_loc(sym)
        tp_col = tp.columns.get_loc(sym)
        ex_col = exits.columns.get_loc(sym)
        for k, day in enumerate(unique_days):
            day_pos = day_positions[k]
            if day_pos.size < _MEDIAN_WINDOW + 1:
                continue
            day_i8 = ts_i8[day_pos]
            range_end_i8 = day_i8[0] + orb_minutes * minute_ns
            tzinfo = ts_index[day_pos[0]].tzinfo
            entry_lo_i8 = pd.Timestamp(datetime.combine(day, _ORB_ENTRY_START, tzinfo=tzinfo)).value
            entry_hi_i8 = pd.Timestamp(datetime.combine(day, _ORB_ENTRY_END, tzinfo=tzinfo)).value

            # Opening range over ``session_open <= t < range_end``. NaN bars (index-union rows this
            # symbol has no bar for) are skipped — python's ``max(x, nan)`` in the old loop kept x,
            # so a plain ``.max()`` (NaN-poisoning) would NOT be equivalent.
            in_range = day_i8 < range_end_i8
            seg_h = h_np[day_pos[in_range]]
            seg_lo = lo_np[day_pos[in_range]]
            hi_ok, lo_ok = ~np.isnan(seg_h), ~np.isnan(seg_lo)
            range_hi = float(seg_h[hi_ok].max()) if hi_ok.any() else -np.inf
            range_lo = float(seg_lo[lo_ok].min()) if lo_ok.any() else np.inf
            if auc_np is not None and in_range.size and in_range[0]:
                a0 = float(auc_np[day_pos[0]])
                if not np.isnan(a0):
                    range_hi, range_lo = max(range_hi, a0), min(range_lo, a0)
            # v2 needs BOTH edges: range_hi is the breakout trigger, range_lo anchors the risk.
            if not (np.isfinite(range_hi) and np.isfinite(range_lo)):
                continue

            # The rolling volume median is computed lazily: most (symbol, day) pairs never produce
            # a breakout candidate, and the old loop's per-candidate Series build was a top-3 cost.
            med_arr: np.ndarray | None = None
            candidates = np.flatnonzero(
                (day_i8 >= range_end_i8) & (day_i8 >= entry_lo_i8) & (day_i8 <= entry_hi_i8)
            )
            entered = False
            for local_i in candidates:
                if local_i < _MEDIAN_WINDOW:
                    continue
                if med_arr is None:
                    med_arr = (
                        rolling_median_volume(vol_np[day_pos], _MEDIAN_WINDOW).to_numpy()
                    )
                med = float(med_arr[local_i - 1])   # window = the _MEDIAN_WINDOW bars before local_i
                if not np.isfinite(med) or med <= 0.0:
                    continue
                p = day_pos[local_i]
                if float(vol_np[p]) < vol_mult * med:
                    continue
                if not float(c_np[p]) > range_hi:      # long-only upside breakout
                    continue
                # v2 (2026-07-12): risk anchored at the opposite range edge — the structural
                # invalidation level — instead of the sub-cost-floor ATR(14,1m) noise unit.
                price = float(c_np[p])
                risk = frac * (price - range_lo)
                if risk <= 0.0 or risk / price < min_risk_frac:
                    continue  # sub-cost-floor breakout (C3) — live, the §7.1 cost gate kills it
                entries.iloc[p, ent_col] = True
                sl.iloc[p, sl_col] = risk / price
                tp.iloc[p, tp_col] = rr * risk / price
                entered = True
                break
            if entered:
                # Forced session-end square-off (MIS): exit at the day's LAST bar, NOT the 14:30
                # entry-window end. §6.1 pins "squared off by window end" and this module's own notes
                # say "forced session-end square-off"; a breakout still open at 14:30 that has hit
                # neither its range-anchored stop nor its RR target rides to the end-of-session MIS
                # square-off (~15:15–15:29). Truncating at 14:30 biased the §6.1 CPCV baselines.
                # The exit must sit on a bar THIS symbol actually has: union-index rows the symbol
                # is missing carry NaN close, and vectorbt silently ignores NaN-price orders
                # (OrderStatusInfo.PriceNaN), so an exit on the union last bar could be dropped and
                # the position would ride overnight with a stale stop — breaking MIS semantics.
                # Non-empty by construction: the entry bar's close was a real (non-NaN) price.
                sym_valid = day_pos[~np.isnan(c_np[day_pos])]
                exits.iloc[sym_valid[-1], ex_col] = True
    return _Signals(entries=entries, exits=exits, sl_stop=sl, tp_stop=tp)


# --------------------------------------------------------------------------- stats helpers
def _equity_stats(daily: pd.Series) -> tuple[float | None, float | None]:
    if daily.empty:
        return None, None
    eq = (1.0 + daily.astype(float)).cumprod()
    total = float(eq.iloc[-1] - 1.0) * 100.0
    dd = float(-(eq / eq.cummax() - 1.0).min()) * 100.0
    return total, dd


def _sharpe(daily: pd.Series) -> float | None:
    if daily.empty:
        return None
    sd = daily.astype(float)
    vol = float(sd.std(ddof=0))
    if vol == 0.0 or not np.isfinite(vol):
        return None
    return float(sd.mean() / vol * np.sqrt(_TRADING_DAYS_Y))


__all__ = [
    "PRICE_BASELINES",
    "DENSITY_POINTS",
    "ParamSetStat",
    "SweepReport",
    "SweepRunner",
    "build_param_grid",
    "load_envelope",
]
