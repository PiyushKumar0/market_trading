"""WO-17 stop-geometry harness — mechanics pinned on SYNTHETIC 1m fixtures only.

Nothing in this file opens ``data/market.duckdb`` (or any store): every test builds its own 1m bars in
memory and injects them through ``StopGeometryExperiment(loader=...)`` or hand-builds an
``EntryContext``. The pre-registration is what is under test — if one of these changes, WO-17 has
drifted:

* the **axis**: {1.0, 1.5, 2.5, 4.0} x ATR_10m + NO-STOP, x 2 exit ladders = exactly 10 configs;
* the **headline case**: a path that dips below the tight stop and then recovers to a positive sell —
  the tight stop books the loss AND counts as a dodged winner, the wide stop books the win;
* **fills at the NEXT bar's OPEN**, entry and exit alike, pinned so a same-bar fill (at the stop
  price, or at the trigger bar's own close) produces a detectably different number;
* the **no-target ladder's session-end exit** (last bar's OPEN, reason ``session_end``);
* **adverse-first**: a bar touching stop AND target in the same 1m bar is booked as a STOP;
* the **MAE / stop-out invariant** — "MAE >= w x ATR_10m" and "stopped at width w" are the same event
  — and the hand-computable p25/p50/p75/p90 aggregation;
* the **ATR switch point**: WO-10b prior-session seeding is what makes a 09:46 orb entry stoppable at
  all; unseeded, the same entry falls inside the 140-minute warm-up and is dropped;
* **promotion is NOT sought**: the module never names ``ValidationPipeline``.

FIXTURE CONVENTION. Bars are ``high = max(open, close) + 0.25`` / ``low = min(open, close) - 0.25``
(0.25 is binary-exact — no float drift in hand-computed levels). Baseline bars carry close 100.0 and
volume 1000, so a 30-minute opening range is exactly [99.75, 100.25] and orb's 20-bar volume median is
exactly 1000.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from engine.core.clock import IST, Clock
from engine.learning.stop_geometry import (
    BANNER,
    EXPERIMENT_ID,
    LADDERS,
    N_CONFIGS,
    NULL_HYPOTHESIS,
    PRODUCT,
    REFERENCE_CONFIG,
    REFERENCE_NOTIONAL,
    SEED_ATR_PRIOR_SESSION,
    STOP_ATR_MULTIPLES,
    STOP_AXIS,
    STOP_SOURCE_ATR,
    STOP_SOURCE_NONE,
    STOP_SOURCE_ORB_RANGE,
    TARGET_SOURCE,
    ConfigAccumulator,
    EntryContext,
    StopConfig,
    StopGeometryExperiment,
    all_configs,
    build_entry_context,
    entry_contexts_for_symbol,
    grid_configs,
    orb_default_params,
    orb_signal_stream,
    render_markdown,
    simulate_entry,
    status_quo_config,
    stop_level,
    target_level,
    write_report,
)
from engine.learning.vwap_reversion import compute_session_features

_SYM = "ZZ"
_DAY1 = date(2026, 1, 5)            # a Monday; this module carries no calendar
_DAY2 = date(2026, 1, 6)
_SESSION_BARS = 375                 # 09:15 .. 15:29 inclusive
_HALF_RANGE = 0.25
_COST = 0.1                         # a stand-in round-trip floor %; the real one is pinned separately
_ORB_PARAMS = {"orb_minutes": 30.0, "vol_mult": 1.5, "stop_range_frac": 1.0, "rr_target": 1.5}

#: 09:15 + 31 min = 09:46 — the breakout SIGNAL bar in the end-to-end fixture (the first bar past the
#: 30-minute opening range that carries the volume surge). Its FILL is 09:47 = positional index 32.
_BREAKOUT_POS = 31
_FILL_POS = 32


# --------------------------------------------------------------------------- fixture builders
class _FixedClock(Clock):
    def __init__(self, ts: datetime) -> None:
        super().__init__(time_source=lambda: ts)


def _clock() -> Clock:
    return _FixedClock(datetime(2026, 8, 14, 21, 30, tzinfo=IST))


def _index(day: date, n: int) -> pd.DatetimeIndex:
    first = datetime.combine(day, time(9, 15), tzinfo=IST)
    return pd.DatetimeIndex([first + timedelta(minutes=i) for i in range(n)])


def _session_frame(
    day: date,
    *,
    closes: list[float] | None = None,
    opens: list[float] | None = None,
    volumes: list[float] | None = None,
) -> pd.DataFrame:
    """One session's 1m OHLCV frame; ``high/low`` bracket the open/close by ``_HALF_RANGE``."""
    close = list(closes) if closes is not None else [100.0] * _SESSION_BARS
    n = len(close)
    open_ = list(opens) if opens is not None else list(close)
    vol = list(volumes) if volumes is not None else [1000.0] * n
    high = [max(o, c) + _HALF_RANGE for o, c in zip(open_, close, strict=True)]
    low = [min(o, c) - _HALF_RANGE for o, c in zip(open_, close, strict=True)]
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=_index(day, n),
    )


def _flat_session(day: date) -> pd.DataFrame:
    """A completely flat session: no breakout, and a 10m ATR of exactly 0.5 for the seeding chain."""
    return _session_frame(day)


def _breakout_session(day: date) -> pd.DataFrame:
    """A session orb breaks out of at 09:46, dips at 09:56, then recovers and holds to the close.

    Hand-computable: opening range [99.75, 100.25]; breakout close 101.0 on 5x volume at index 31;
    the FILL is index 32's OPEN = 100.5 (the signal bar's close 101.0 is deliberately different, so a
    same-bar fill is detectable); the dip low is 99.65 at index 41 and the square-off is 101.5.
    """
    closes = [100.0] * _SESSION_BARS
    vols = [1000.0] * _SESSION_BARS
    closes[_BREAKOUT_POS] = 101.0
    vols[_BREAKOUT_POS] = 5000.0
    for i in range(_FILL_POS, 41):
        closes[i] = 100.5
    closes[41] = 99.9                                   # the dip: low 99.65
    for i in range(42, _SESSION_BARS):
        closes[i] = 101.5
    closes[42] = 100.4                                  # the stop's next-bar-open fill
    return _session_frame(day, closes=closes, volumes=vols)


def _late_dip_session(day: date) -> pd.DataFrame:
    """:func:`_breakout_session` whose FINAL bar collapses AFTER its open (open 101.50, low 89.75).

    The position is squared off at that bar's OPEN, so the collapse is unreachable: it must not enter
    the MAE and must not trigger any stop. Pins the exact bar range both diagnostics are measured over.
    """
    frame = _breakout_session(day).copy()
    last = frame.index[-1]
    frame.loc[last, ["open", "high", "low", "close"]] = [101.5, 101.75, 89.75, 90.0]
    return frame


def _ctx(
    *,
    highs: list[float],
    lows: list[float],
    fill_opens: list[float],
    entry_price: float = 100.0,
    atr: float = 1.0,
    orb_stop_frac: float = 0.0125,
    orb_target_frac: float = 0.05,
) -> EntryContext:
    """A hand-built entry path. ``highs``/``lows`` are trigger bars; ``fill_opens[k]`` fills a
    trigger on bar ``k``, and ``fill_opens[-1]`` is the session-end square-off price."""
    n = len(highs)
    assert len(lows) == n and len(fill_opens) == n
    first = datetime.combine(_DAY1, time(10, 0), tzinfo=IST)
    trigger_ts = pd.DatetimeIndex([first + timedelta(minutes=i) for i in range(n)])
    fill_ts = pd.DatetimeIndex([first + timedelta(minutes=i + 1) for i in range(n)])
    return EntryContext(
        symbol=_SYM,
        session=_DAY1,
        signal_ts=first - timedelta(minutes=1),
        entry_ts=first,
        entry_price=entry_price,
        atr_10m=atr,
        orb_stop_frac=orb_stop_frac,
        orb_target_frac=orb_target_frac,
        trigger_ts=trigger_ts,
        trigger_high=np.asarray(highs, dtype="float64"),
        trigger_low=np.asarray(lows, dtype="float64"),
        fill_ts=fill_ts,
        fill_open=np.asarray(fill_opens, dtype="float64"),
    )


def _dip_then_recover() -> EntryContext:
    """THE headline fixture: entry 100.0, ATR 1.0, a dip to 99.65 low, recovery, square-off 101.0.

    * 1.0 x ATR stop = 99.00 — HIT on bar 0 (low 98.80), filled at bar 1's open 98.90 ⇒ a booked loss;
    * 1.5 / 2.5 / 4.0 x ATR stops = 98.50 / 97.50 / 96.00 — never hit ⇒ held to the square-off at
      101.00 ⇒ a booked win;
    * the target (orb's 1.5R, 5% here = 105.00) is out of reach, so both ladders agree.
    """
    return _ctx(
        highs=[100.20, 100.50, 101.20],
        lows=[98.80, 99.50, 100.50],
        fill_opens=[98.90, 99.60, 101.00],
    )


def _experiment(loader, **kwargs) -> StopGeometryExperiment:
    params = {
        "loader": loader,
        "cost_floor_pct": _COST,
        "orb_fee": 0.0,
        "clock": _clock(),
        "orb_params": _ORB_PARAMS,
    }
    params.update(kwargs)
    return StopGeometryExperiment(**params)


# --------------------------------------------------------------------------- the fixed axis
def test_the_axis_is_the_pre_registered_ten_configs_and_nothing_else():
    """{1.0, 1.5, 2.5, 4.0} x ATR_10m + NO-STOP, x 2 ladders. Drift here = WO-17 drift."""
    assert STOP_ATR_MULTIPLES == (1.0, 1.5, 2.5, 4.0)
    assert STOP_AXIS == (1.0, 1.5, 2.5, 4.0, None)
    assert LADDERS == (True, False)
    assert N_CONFIGS == 10

    configs = grid_configs()
    assert len(configs) == 10
    assert {c.stop_atr_mult for c in configs} == {1.0, 1.5, 2.5, 4.0, None}
    assert all(c.pre_registered for c in configs)
    no_stop = [c for c in configs if c.stop_source == STOP_SOURCE_NONE]
    assert len(no_stop) == 2 and {c.use_target for c in no_stop} == {True, False}
    assert len({c.label for c in configs}) == 10           # every row is distinguishable in the report


def test_status_quo_row_is_not_one_of_the_pre_registered_ten():
    ref = status_quo_config()
    assert ref.pre_registered is False
    assert ref.stop_source == STOP_SOURCE_ORB_RANGE
    assert len([c for c in all_configs() if c.pre_registered]) == 10


def test_reference_config_is_no_stop_no_target():
    """Both diagnostics are measured against this exact configuration — it must stay the pure path."""
    assert REFERENCE_CONFIG.stop_source == STOP_SOURCE_NONE
    assert REFERENCE_CONFIG.use_target is False


def test_orb_population_params_are_the_envelope_defaults():
    """WO-17 says 'the orb entry-signal stream' — the §6.3 default baseline, read from the envelope."""
    assert orb_default_params() == {
        "orb_minutes": 30.0,
        "rr_target": 1.5,
        "stop_range_frac": 1.0,
        "vol_mult": 1.5,
    }


# --------------------------------------------------------------------------- THE headline case
def test_tight_stop_books_the_loss_wide_stop_books_the_win_on_the_same_dip_and_recover_path():
    """The owner's hypothesis, made a unit test: one path, two geometries, opposite outcomes."""
    ctx = _dip_then_recover()

    tight = simulate_entry(
        ctx,
        StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=False),
        cost_floor_pct=_COST,
    )
    wide = simulate_entry(
        ctx,
        StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.5, use_target=False),
        cost_floor_pct=_COST,
    )

    # Tight: stopped on bar 0's 98.80 low against a 99.00 stop, filled at bar 1's OPEN.
    assert tight.exit_reason == "stop"
    assert tight.stop_price == pytest.approx(99.00)
    assert tight.exit_price == pytest.approx(98.90)
    assert tight.gross_return_pct == pytest.approx(-1.10)
    assert tight.net_return_pct == pytest.approx(-1.20)

    # Wide: 98.50 was never touched, so the position rode to the session-end square-off at 101.00.
    assert wide.exit_reason == "session_end"
    assert wide.stop_price == pytest.approx(98.50)
    assert wide.exit_price == pytest.approx(101.00)
    assert wide.gross_return_pct == pytest.approx(1.00)
    assert wide.net_return_pct == pytest.approx(0.90)

    # …and the same entry with NO stop is the reference path both trades carry.
    assert tight.reference_net_pct == pytest.approx(0.90) == wide.reference_net_pct


def test_the_tight_stop_out_is_counted_as_a_dodged_winner_and_the_wide_one_is_not():
    """DODGED WINNER = stopped out, AND the unstopped twin ended the session NET-positive."""
    ctx = _dip_then_recover()
    tight_cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=False)
    wide_cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=2.5, use_target=False)

    tight_acc = ConfigAccumulator(config=tight_cfg)
    tight_acc.add(simulate_entry(ctx, tight_cfg, cost_floor_pct=_COST))
    assert tight_acc.n_stopped == 1
    assert tight_acc.n_dodged_winners == 1
    assert tight_acc.dodged_winner_fraction == pytest.approx(1.0)

    wide_acc = ConfigAccumulator(config=wide_cfg)
    wide_acc.add(simulate_entry(ctx, wide_cfg, cost_floor_pct=_COST))
    assert wide_acc.n_stopped == 0
    assert wide_acc.n_dodged_winners == 0
    assert wide_acc.dodged_winner_fraction is None         # no denominator ⇒ no fraction, never 0.0


def test_a_stop_out_whose_unstopped_twin_still_loses_is_not_a_dodged_winner():
    """The denominator is stop-outs; the numerator is RECOVERIES. A stop that saved money is not one."""
    ctx = _ctx(highs=[100.2, 99.0, 98.0], lows=[98.8, 98.0, 97.0], fill_opens=[98.9, 98.1, 97.5])
    cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=False)
    trade = simulate_entry(ctx, cfg, cost_floor_pct=_COST)
    assert trade.exit_reason == "stop"
    assert trade.reference_net_pct < 0.0                   # unstopped, it ended DOWN at 97.50
    acc = ConfigAccumulator(config=cfg)
    acc.add(trade)
    assert acc.n_stopped == 1 and acc.n_dodged_winners == 0
    assert acc.dodged_winner_fraction == pytest.approx(0.0)


def test_dodged_winner_net_and_gross_readings_can_disagree_and_both_are_reported():
    """A recovery that does not cover its own round trip is a GROSS dodge, not a NET one."""
    # Square-off 100.05 ⇒ gross +0.05% (positive) but net −0.05% (below the 0.1% floor).
    ctx = _ctx(highs=[100.2, 100.1], lows=[98.8, 99.9], fill_opens=[98.9, 100.05])
    cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=False)
    trade = simulate_entry(ctx, cfg, cost_floor_pct=_COST)
    assert trade.exit_reason == "stop"
    assert trade.reference_gross_pct == pytest.approx(0.05)
    assert trade.reference_net_pct == pytest.approx(-0.05)
    acc = ConfigAccumulator(config=cfg)
    acc.add(trade)
    assert acc.dodged_winner_fraction == pytest.approx(0.0)        # NET is the headline
    assert acc.dodged_winner_fraction_gross == pytest.approx(1.0)  # …and the gross reading is visible


# --------------------------------------------------------------------------- fills (WO-2)
def test_exit_fills_at_the_next_bars_open_never_at_the_stop_price_or_the_trigger_bars_close():
    """WO-2's fill rule, pinned so every wrong-but-plausible fill price is detectably different."""
    ctx = _dip_then_recover()
    trade = simulate_entry(
        ctx,
        StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=False),
        cost_floor_pct=_COST,
    )
    assert trade.exit_price == pytest.approx(98.90)        # bar 1's OPEN
    assert trade.exit_price != pytest.approx(99.00)        # NOT the stop level (an intrabar fill)
    assert trade.exit_price != pytest.approx(98.80)        # NOT the trigger bar's own low
    assert trade.exit_ts == ctx.fill_ts[0] == ctx.trigger_ts[1]


def test_entry_fills_at_the_next_bars_open_never_the_signal_bars_close():
    """The breakout is read from bar t's CLOSE (101.0) and filled at bar t+1's OPEN (100.5)."""
    frame = _breakout_session(_DAY1)
    feats = compute_session_features(_SYM, frame, atr_seed=0.5)
    assert feats is not None
    assert float(feats.close[_BREAKOUT_POS]) == pytest.approx(101.0)
    ctx = build_entry_context(feats, _BREAKOUT_POS, orb_stop_frac=0.0125, orb_target_frac=0.05)
    assert ctx is not None
    assert ctx.entry_price == pytest.approx(100.5)         # bar 32's OPEN
    assert ctx.entry_price != pytest.approx(101.0)         # NOT the signal bar's close
    assert ctx.entry_ts == feats.ts[_FILL_POS]


def test_a_signal_on_the_sessions_last_usable_bar_is_dropped():
    """There is no bar left to fill it in — never carried into the next session."""
    frame = _breakout_session(_DAY1)
    feats = compute_session_features(_SYM, frame, atr_seed=0.5)
    assert feats is not None
    assert build_entry_context(feats, feats.n - 1, orb_stop_frac=0.01, orb_target_frac=0.05) is None
    assert build_entry_context(feats, feats.n - 2, orb_stop_frac=0.01, orb_target_frac=0.05) is None


# --------------------------------------------------------------------------- the ladders
def test_no_target_ladder_exits_at_the_session_end_square_off():
    """Ladder (b) with NO-STOP has exactly one exit: the last bar's OPEN, labelled session_end."""
    ctx = _dip_then_recover()
    trade = simulate_entry(ctx, REFERENCE_CONFIG, cost_floor_pct=_COST)
    assert trade.exit_reason == "session_end"
    assert trade.stop_price is None and trade.target_price is None
    assert trade.exit_price == pytest.approx(101.00) == ctx.session_end_price
    assert trade.exit_ts == ctx.fill_ts[-1]
    assert trade.gross_return_pct == pytest.approx(1.00)


def test_target_ladder_takes_orbs_own_1_5R_target_and_it_does_not_move_with_the_stop_width():
    """The flagged interpretation: R is ORB'S risk unit, so one target level serves all five widths."""
    assert TARGET_SOURCE == "orb_rr_target"
    ctx = _ctx(highs=[101.0, 103.0], lows=[99.9, 100.5], fill_opens=[100.4, 102.5], orb_target_frac=0.02)
    levels = {
        stop: target_level(ctx, StopConfig(
            stop_source=STOP_SOURCE_ATR if stop is not None else STOP_SOURCE_NONE,
            stop_atr_mult=stop,
            use_target=True,
        ))
        for stop in STOP_AXIS
    }
    assert set(levels) == set(STOP_AXIS)                    # …including the NO-STOP width
    for stop, level in levels.items():
        assert level == pytest.approx(102.0), stop          # ONE level, identical at every width
    trade = simulate_entry(
        ctx, StopConfig(stop_source=STOP_SOURCE_NONE, stop_atr_mult=None, use_target=True),
        cost_floor_pct=_COST,
    )
    assert trade.exit_reason == "target"
    assert trade.exit_price == pytest.approx(102.5)         # bar 1's trigger, bar 2's OPEN
    assert target_level(ctx, REFERENCE_CONFIG) is None      # ladder (b) arms no target at all


def test_adverse_first_a_bar_touching_both_levels_is_booked_as_a_stop():
    """1m bars carry no intrabar path — the house convention gives the tie to the adverse event."""
    ctx = _ctx(highs=[102.5, 100.1], lows=[98.5, 99.9], fill_opens=[100.5, 100.0], orb_target_frac=0.02)
    stopped = simulate_entry(
        ctx, StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=1.0, use_target=True),
        cost_floor_pct=_COST,
    )
    assert stopped.stop_price == pytest.approx(99.0) and stopped.target_price == pytest.approx(102.0)
    assert stopped.exit_reason == "stop"                    # NOT "target", though both were touched

    # Same bar, same target, no stop armed ⇒ the target is what the bar is credited with.
    targeted = simulate_entry(
        ctx, StopConfig(stop_source=STOP_SOURCE_NONE, stop_atr_mult=None, use_target=True),
        cost_floor_pct=_COST,
    )
    assert targeted.exit_reason == "target"
    assert targeted.exit_price == stopped.exit_price == pytest.approx(100.5)


def test_stop_levels_are_anchored_at_the_entry_fill_in_atr_units():
    ctx = _dip_then_recover()
    for mult in STOP_ATR_MULTIPLES:
        cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=mult, use_target=False)
        assert stop_level(ctx, cfg) == pytest.approx(100.0 - mult * 1.0)
    assert stop_level(ctx, REFERENCE_CONFIG) is None
    orb_cfg = status_quo_config()
    assert stop_level(ctx, orb_cfg) == pytest.approx(100.0 * (1.0 - 0.0125))


# --------------------------------------------------------------------------- MAE (diagnostic 2)
def test_mae_is_hand_computable_in_both_percent_and_atr_multiples():
    ctx = _dip_then_recover()                               # entry 100.0, worst low 98.80, ATR 1.0
    assert ctx.worst_low == pytest.approx(98.80)
    assert ctx.mae_pct == pytest.approx(1.20)
    assert ctx.mae_atr_mult == pytest.approx(1.20)

    half_atr = _ctx(highs=[100.2, 101.2], lows=[98.8, 100.5], fill_opens=[98.9, 101.0], atr=0.5)
    assert half_atr.mae_pct == pytest.approx(1.20)          # same dip …
    assert half_atr.mae_atr_mult == pytest.approx(2.40)     # … twice as many ATRs


def test_mae_never_goes_negative_when_the_path_never_dips_below_the_entry():
    ctx = _ctx(highs=[101.0, 102.0], lows=[100.5, 101.5], fill_opens=[101.0, 102.0])
    assert ctx.mae_pct == pytest.approx(0.0)
    assert ctx.mae_atr_mult == pytest.approx(0.0)


@pytest.mark.parametrize("mult", STOP_ATR_MULTIPLES)
def test_mae_and_stop_out_are_the_same_event(mult: float):
    """THE consistency invariant the two diagnostics rest on: stopped at w  <=>  MAE >= w x ATR_10m.

    If this ever fails, the MAE table and the stop-out counts are describing different things and the
    report's central claim ("a stop tighter than the p90 figure cuts off a tenth of the winners")
    becomes unsupportable.
    """
    for ctx in (
        _dip_then_recover(),
        _ctx(highs=[100.2, 101.2], lows=[95.0, 100.5], fill_opens=[95.5, 101.0]),
        _ctx(highs=[101.0, 102.0], lows=[100.5, 101.5], fill_opens=[101.0, 102.0]),
    ):
        cfg = StopConfig(stop_source=STOP_SOURCE_ATR, stop_atr_mult=mult, use_target=False)
        stopped = simulate_entry(ctx, cfg, cost_floor_pct=_COST).exit_reason == "stop"
        assert stopped == (ctx.mae_atr_mult >= mult)


def test_the_unreachable_square_off_bar_enters_neither_the_mae_nor_any_stop():
    """The trigger/MAE range is bars ``e .. hard-1``: the exit-fill bar's own low is never held.

    The fixture's last bar opens at 101.50 (the square-off price) and then collapses to an 89.75 low.
    Widening the range by one bar would drag that collapse into the MAE (1.70 -> 21.40 ATR) and stop
    out every width — with no bar left to fill the stop in.
    """
    frame = pd.concat([_flat_session(_DAY1), _late_dip_session(_DAY2)])
    feats = compute_session_features(_SYM, _late_dip_session(_DAY2), atr_seed=0.5)
    assert feats is not None
    ctx = build_entry_context(feats, _BREAKOUT_POS, orb_stop_frac=0.0125, orb_target_frac=0.05)
    assert ctx is not None
    # Index alignment: a trigger on bar k fills at fill_open[k]; the arrays must be the same length.
    assert len(ctx.trigger_low) == len(ctx.trigger_high) == len(ctx.fill_open) == len(ctx.fill_ts)
    assert ctx.worst_low == pytest.approx(99.65)           # the 09:56 dip, NOT the 15:29 collapse
    assert ctx.mae_atr_mult == pytest.approx((100.50 - 99.65) / 0.5)
    assert ctx.session_end_price == pytest.approx(101.50)  # the last bar's OPEN, not its 90.0 close

    report = _experiment(lambda _sym: frame).run([_SYM], start=_DAY1, end=_DAY2)
    widest = next(
        c for c in report.configs if c.stop_atr_mult == 4.0 and not c.use_target
    )
    assert widest.n_stopped == 0
    assert widest.exit_counts["session_end"] == 1
    assert widest.expectancy_pct == pytest.approx((101.50 - 100.50) / 100.50 * 100.0 - _COST)


def test_mae_quantiles_are_the_hand_computed_p25_p50_p75_p90():
    """Aggregation pinned on four values whose linear-interpolated quantiles are exact by hand."""
    winners = [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0)]
    losers = [(9.0, 9.0)]
    dists = StopGeometryExperiment._mae_distributions(winners, losers)
    by_label = {d.label: d for d in dists}

    win = by_label["eventual winners (NO-STOP net-positive)"]
    assert win.n == 4
    assert win.mean_pct == pytest.approx(2.5)
    assert win.pct == {"p25": pytest.approx(1.75), "p50": pytest.approx(2.5),
                       "p75": pytest.approx(3.25), "p90": pytest.approx(3.7)}
    assert win.atr_mult == win.pct                          # same numbers in this fixture, by design
    assert dists[0].label.startswith("eventual winners")    # winners FIRST — the pre-registered slice
    assert by_label["all entries [context]"].n == 5


def test_mae_distribution_of_an_empty_slice_is_none_not_nan():
    """NaN is not valid JSON — an empty slice must serialise as null."""
    dists = StopGeometryExperiment._mae_distributions([], [])
    for d in dists:
        assert d.n == 0 and d.pct is None and d.atr_mult is None and d.mean_pct is None


# --------------------------------------------------------------------------- the ATR switch point
def test_seeded_atr_is_the_switch_point_and_is_what_makes_a_0946_orb_entry_stoppable():
    """WO-17 depends on WO-10b's shared unit: unseeded, orb's morning entry has no stop and is dropped.

    Same frame, same signal bar, one flag: with the prior-session seed the 09:46 breakout carries an
    ATR (0.5, the flat prior session's value) and is bookable; unseeded it sits inside the 140-minute
    warm-up, has no stop, and is not booked at all — for EVERY config, so the population stays common.
    """
    assert SEED_ATR_PRIOR_SESSION is True
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])

    seeded, sessions = entry_contexts_for_symbol(
        _SYM, frame, orb_params=_ORB_PARAMS, orb_fee=0.0, seed_atr_prior_session=True
    )
    assert sessions == [_DAY1, _DAY2]
    assert len(seeded) == 1
    assert seeded[0].session == _DAY2
    assert seeded[0].signal_ts == _index(_DAY2, _SESSION_BARS)[_BREAKOUT_POS]
    assert seeded[0].atr_10m == pytest.approx(0.5)          # carried from the flat prior session

    unseeded, _ = entry_contexts_for_symbol(
        _SYM, frame, orb_params=_ORB_PARAMS, orb_fee=0.0, seed_atr_prior_session=False
    )
    assert unseeded == []                                   # no stop unit ⇒ no trade (fail-closed)


def test_an_entry_without_a_defined_atr_is_dropped_for_every_config_including_no_stop():
    """The common-population rule: NO-STOP must not see entries the stopped configs cannot."""
    frame = _breakout_session(_DAY1)
    feats = compute_session_features(_SYM, frame, atr_seed=None)   # unseeded ⇒ NaN before 11:35
    assert feats is not None
    assert not np.isfinite(feats.atr_10m[_BREAKOUT_POS])
    assert build_entry_context(feats, _BREAKOUT_POS, orb_stop_frac=0.01, orb_target_frac=0.05) is None


# --------------------------------------------------------------------------- the orb population
def test_the_population_comes_from_orbs_own_builder_one_entry_per_symbol_session():
    """Reuse, not re-derivation: sweep._signals_orb produces the stream, and orb takes ONE per day."""
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])
    signals = orb_signal_stream(_SYM, frame, params=_ORB_PARAMS, fee=0.0)
    assert len(signals) == 1                               # the flat session never breaks out
    assert signals.index[0] == _index(_DAY2, _SESSION_BARS)[_BREAKOUT_POS]
    # orb's own risk unit: stop_range_frac x (close − range_low) / close = (101.0 − 99.75)/101.0.
    assert float(signals["stop_frac"].iloc[0]) == pytest.approx(1.25 / 101.0)
    assert float(signals["target_frac"].iloc[0]) == pytest.approx(1.5 * 1.25 / 101.0)


def test_every_configuration_is_evaluated_on_exactly_the_same_entries():
    """Requirement 1: the geometry changes the EXIT, never the entry. Different n_trades = broken."""
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])
    report = _experiment(lambda sym: frame if sym == _SYM else None).run(
        [_SYM], start=_DAY1, end=_DAY2
    )
    assert report.status == "COMPLETED"
    assert report.n_entries == 1
    assert {c.n_trades for c in report.configs} == {1}


# --------------------------------------------------------------------------- end-to-end
def test_end_to_end_geometry_table_on_the_dip_and_recover_session():
    """The whole harness on one hand-computable symbol-session, checked row by row.

    Entry 100.50 (09:47 open), ATR_10m 0.50 ⇒ stops at 100.00 / 99.75 / 99.25 / 98.50. The 09:56 dip
    low is 99.65, so the two tight widths stop (filled at 09:57's open 100.40) and the two wide ones
    plus NO-STOP ride to the 101.50 square-off. MAE = (100.50 − 99.65)/100.50 = 0.8458% = 1.70 ATR.
    """
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])
    report = _experiment(lambda sym: frame if sym == _SYM else None).run(
        [_SYM], start=_DAY1, end=_DAY2
    )
    by_key = {(c.stop_atr_mult, c.use_target): c for c in report.configs if c.pre_registered}

    for mult in (1.0, 1.5):
        row = by_key[(mult, False)]
        assert row.n_stopped == 1, mult
        assert row.exit_counts["stop"] == 1
        assert row.expectancy_pct == pytest.approx((100.40 - 100.50) / 100.50 * 100.0 - _COST)
        assert row.dodged_winner_fraction == pytest.approx(1.0), mult
        assert row.win_rate == pytest.approx(0.0)

    for mult in (2.5, 4.0, None):
        row = by_key[(mult, False)]
        assert row.n_stopped == 0, mult
        assert row.exit_counts["session_end"] == 1
        assert row.expectancy_pct == pytest.approx((101.50 - 100.50) / 100.50 * 100.0 - _COST)
        assert row.dodged_winner_fraction is None, mult
        assert row.win_rate == pytest.approx(1.0)

    # orb's 1.5R target (≈102.37) is out of reach here, so both ladders agree on every width.
    for mult in STOP_AXIS:
        assert by_key[(mult, True)].expectancy_pct == pytest.approx(by_key[(mult, False)].expectancy_pct)

    winners = report.mae_distributions[0]
    assert winners.label.startswith("eventual winners")
    assert winners.n == 1
    assert winners.pct["p50"] == pytest.approx((100.50 - 99.65) / 100.50 * 100.0)
    assert winners.atr_mult["p50"] == pytest.approx((100.50 - 99.65) / 0.5)
    assert report.reference_win_rate == pytest.approx(1.0)


def test_no_data_and_no_entries_are_distinct_reported_outcomes():
    empty = _experiment(lambda _sym: None).run([_SYM], start=_DAY1, end=_DAY2)
    assert empty.status == "NO_DATA" and empty.n_entries == 0

    flat = pd.concat([_flat_session(_DAY1), _flat_session(_DAY2)])
    none = _experiment(lambda _sym: flat).run([_SYM], start=_DAY1, end=_DAY2)
    assert none.status == "NO_ENTRIES" and none.n_entries == 0
    assert {c.n_trades for c in none.configs} == {0}


# --------------------------------------------------------------------------- the report
def test_report_carries_the_banner_the_verbatim_null_hypothesis_and_the_owner_answer(tmp_path: Path):
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])
    report = _experiment(lambda _sym: frame).run([_SYM], start=_DAY1, end=_DAY2)

    assert report.banner == BANNER and "STOPS FOR OWNER REVIEW" in report.banner
    assert report.null_hypothesis == NULL_HYPOTHESIS
    assert "optional stopping" in report.null_hypothesis
    assert report.plain_language_answer and "QUESTION:" in report.plain_language_answer[0]
    assert report.product == PRODUCT == "MIS"
    assert float(REFERENCE_NOTIONAL) == 20_000.0
    assert report.fill_mechanics == "next_bar_open"

    md = render_markdown(report)
    assert md.startswith("# WO-17 diagnostic")
    assert BANNER in md
    assert NULL_HYPOTHESIS in md
    assert "DODGED-WINNER FRACTION" in md
    assert "what dip depth do winners actually survive" in md
    for note in report.modelling_notes:
        assert note in md

    artifacts = write_report(report, tmp_path)
    assert artifacts.markdown.name.startswith(f"{EXPERIMENT_ID}_")
    assert artifacts.json.exists()
    # NaN is not valid JSON — a report full of empty slices must still parse.
    import json

    parsed = json.loads(artifacts.json.read_text(encoding="utf-8"))
    assert parsed["experiment_id"] == EXPERIMENT_ID
    assert len([c for c in parsed["configs"] if c["pre_registered"]]) == 10


def test_owner_facing_strings_survive_a_cp1252_console():
    """The CLI prints these verbatim to a Windows console that may be cp1252 — no em-dashes there."""
    frame = pd.concat([_flat_session(_DAY1), _breakout_session(_DAY2)])
    report = _experiment(lambda _sym: frame).run([_SYM], start=_DAY1, end=_DAY2)
    printed = [
        *report.plain_language_answer,
        report.null_hypothesis,
        report.owner_question,
        report.banner,
        *(c.label for c in report.configs),
        *(m.label for m in report.mae_distributions),
    ]
    for text in printed:
        text.encode("cp1252")                              # raises if the console could not print it
        assert all(ord(ch) < 128 for ch in text), text


def test_promotion_is_never_sought_the_module_cannot_reach_the_validation_pipeline():
    """WO-17: promotion is NOT sought — so the promotion path must be UNREACHABLE, not merely unused.

    Checked structurally (the module's prose is allowed to DISCUSS the absence): no import of
    ``engine.learning.validate``, and no ``ValidationPipeline`` / ``ParamSet`` name bound anywhere in
    the module namespace. Nothing here can construct a candidate row or a CPCV verdict.
    """
    import ast

    from engine.learning import stop_geometry

    tree = ast.parse(Path(stop_geometry.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module != "engine.learning.validate", node.module
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
    assert "engine.learning.validate" not in imported
    for banned in ("ValidationPipeline", "ParamSet", "ValidationReport"):
        assert banned not in imported, banned
        assert not hasattr(stop_geometry, banned), banned


def test_the_full_round_trip_is_charged_on_every_trade_including_stop_outs():
    """WO-17: 'each stop-out realizes the full round-trip cost floor.'"""
    ctx = _dip_then_recover()
    for cfg in all_configs():
        trade = simulate_entry(ctx, cfg, cost_floor_pct=_COST)
        assert trade.net_return_pct == pytest.approx(trade.gross_return_pct - _COST), cfg.label
