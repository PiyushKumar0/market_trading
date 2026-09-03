"""``hi52`` — 52-week-high-proximity continuation, SHADOW (owner-directed 2026-09-01 design).

No expected edge is registered for this rule — measuring one is the entire point of the shadow,
identically to ``cat`` (§2.7/WO-18): there is no ``hi52.expected_edge_pct`` anywhere (not in
``settings.yaml``, not in the gate's ``strategy_expected_edge_pct`` map), so the §7.1 C3 cost check
has no edge basis and FAIL-CLOSED-REJECTS every ``hi52`` candidate — the funnel still journals the
signal as its validation population and the analyst still writes its thesis, but nothing reaches
RECOMMEND. Wiring an edge, and with it RECOMMEND eligibility, is the §8.6 owner gate, contingent on
backtest + shadow validation, exactly as for ``cat``. This module does not touch either gate; it
only originates.

**Architecture — a pure batch rule over the BATCH universe, like ``brk20``.** It is deliberately NOT
a :class:`~engine.strategy.scanners.base.Scanner`: the §3.2.5 scanner protocol is (1m bar,
ScanContext)-driven and only watched symbols ever produce bars, but a swing-horizon 52wk-proximity
read has nothing per-bar to key off. This module scans COMPLETED daily bars (bhavcopy-final
``bars_1d``) for every ELIGIBLE universe symbol — watchlist membership is irrelevant. The composition
root runs it from the window-open/``scan_now`` sweep and admits its candidates through
``SignalPreScreen.admit`` so the §3.2.5 dedupe/caps bind exactly as for bar-driven candidates.

**Thesis.** George & Hwang (2004) find proximity to the 52-week high is a stronger continuation
predictor than conventional price momentum: the high acts as an anchor investors under-react to, so
a FRESH crossing into the high-proximity band tends to keep drifting over a multi-week (T+5..T+20)
horizon. The Frog-in-the-Pan hypothesis (Da, Gurun & Warachka) refines this further: GRADUAL,
continuous information diffusion is under-reacted to more than a single salient jump, so a smooth
climb into new highs should drift further than a spiky one. This rule ORIGINATES on the proximity
crossing alone (PINNED, below); the path-smoothness read is carried as a DIAGNOSTIC for that later
analysis and never gates a candidate.

PINNED rule (long-only — NSE cash equities cannot be shorted overnight):

* Let ``y`` be the last COMPLETED session. Fewer than ``min_sessions`` rows of history ⇒ no
  candidate (a thin listing has no meaningful 52wk read). ``hi`` = max high of the last
  ``min(lookback_sessions, len(rows))`` sessions INCLUDING ``y``. ``prox = close(y) / hi``.
* Fire iff ``prox >= proximity_min`` AND FRESH-CROSS — the PREVIOUS session's own ``prox``, computed
  the same way over its own window ending at ``y-1``, was ``< proximity_min`` (a stock already parked
  above the band does not re-fire daily; same-day refire is separately impossible via the prescreen's
  (symbol, strategy) day-dedupe) — AND VOLUME confirmation, ``volume(y) >= vol_mult × mean(volume of
  the 20 sessions immediately before y)`` (``vol_mult = 1.0`` is the neutral default; applied
  unconditionally, never skipped, because ``min_sessions`` already guarantees at least 20 prior rows)
  — AND no known ex-date within ``ex_skip_days`` CALENDAR days of ``today``.
* The ex-date skip mirrors ``brk20``'s A12 calendar-horizon check, with one deliberate difference:
  it is COUNTED here as :data:`VETO_EX_DATE_SKIP`. ``brk20`` itself does NOT count its own ex-date
  refusal (only its WO-19 stop-geometry-floor vetoes are counted, on the theory that an ordinary
  universe-shape refusal isn't worth a counter) — but a brand-new SHADOW's veto classes need to be
  observable from day one, not bolted on after a starvation scare the way ``cat``'s visibility line
  was, so ``hi52`` counts it. This is a deliberate divergence from ``brk20``'s literal pattern, not
  an oversight.
* Unadjusted-history veto (2026-09-03): a symbol with a :data:`UNADJUSTED_KINDS` ex-date inside the
  lookback is not read at all (counted as :data:`VETO_UNADJUSTED_HISTORY`) — stored ``bars_1d``
  history is never re-adjusted across an ex-date, so its window holds bars in two units and the
  proximity read is meaningless until the window rolls past. The composition root supplies the set
  from ``corp_actions`` over the frame window (:func:`unadjusted_history`).
* Levels: ``entry = round_to_tick(close(y))`` — the trigger session's own close. Unlike ``brk20``
  there is no "broken level" to retest (a proximity read isn't a level break), so the close IS the
  reference. ``stop = round_to_tick(entry × (1 − stop_pct/100))``, default **6%** — a disaster stop
  sized the same way as ``cat``'s (§7.1 ``per_trade_risk.overnight_gap_mult`` applies to a swing
  entry). ``target = None``: no ATR/RR level is fabricated onto an unvalidated shadow rule — identical
  reasoning to ``cat``. Degenerate tick-rounding (``stop >= entry``) emits nothing, as for
  ``brk20``/``cat``.
* Score = ``prox``, clamped to ``[0, 1]``: the proximity margin is the strength signal. Scores are
  only ever compared WITHIN a strategy (§5.2(a) per-strategy score quantile), so this scale need not
  agree with any other scanner's.
* Exit is TIME, not price, exactly as for ``cat``: ``hold_sessions`` (= the §7.1 swing ``max_holding``
  cap) is carried in :data:`DEFAULT_PARAMS` purely as documentation of the intended cap — the
  EXISTING max-holding machinery (``RecommendationPipeline.check_aged_positions``) is the exit path,
  and no exit code lives here, mirroring ``cat.DEFAULT_PARAMS["hold_sessions"]`` exactly.
* Diagnostics (Frog-in-the-Pan path-smoothness — DIAGNOSTIC ONLY, never gates a candidate):
  ``up_day_frac`` = fraction of the last 20 sessions with ``close > prev close``; ``max_day_move`` =
  the largest single-session ``|close/prev − 1|`` over the same 20 sessions. **Not attached to**
  :class:`~engine.strategy.types.SignalCandidate` — that type's own docstring pins it to EXACTLY its
  §3.2.5 field set (no metadata/notes field exists there, and neither ``brk20`` nor ``cat`` attach
  one), and ``engine/strategy/types.py`` is outside this module's edit scope. They are exposed
  instead through the standalone :func:`diagnostics_for` helper, computable on the same rows
  :func:`scan_daily` scans, so the eventual Frog-in-the-Pan analysis (and this module's own tests)
  can read them without a schema change. :func:`scan_daily` does not call it internally — nothing
  pays for a statistic today's arithmetic doesn't need (the same "don't cost every universe symbol
  for an unused stat" discipline ``brk20`` applies to its own gap-floor median).

Evidence caveat rides along (§6.1, identical framing to ``cat``): this rule ORIGINATES candidates for
Tier-1 judgement and the owner's decision; it carries no presumption of positive expectancy.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Iterable, Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, NamedTuple

from ulid import ULID

from engine.strategy.scanners.brk20 import DailyRow
from engine.strategy.types import RawLevels, SignalCandidate, round_to_tick

STRATEGY_ID = "hi52"

DEFAULT_PARAMS: dict[str, float] = {   # §6.3-shaped learnable envelope, frozen for the shadow window
    "proximity_min": 0.95,
    "lookback_sessions": 252,
    "min_sessions": 126,
    "vol_mult": 1.0,
    "ex_skip_days": 10,
    "hold_sessions": 20,          # documentation only — see module docstring; not read below
    "stop_pct": 6.0,
}

#: Veto class surfaced through the optional accumulator (§6.1 observability). See the module
#: docstring for why hi52 counts this where brk20 does not.
VETO_EX_DATE_SKIP = "ex_date_skip"

#: Corp-action kinds that RESCALE the price series: after a bonus/split/rights/demerger ex-date every
#: earlier bar is in a different unit. Neither ``bars_1d`` source re-adjusts STORED history — Kite
#: candles are adjusted at fetch time (A11) but the series is seeded once and then extended one
#: session at a time, and bhavcopy rows are raw — so ANY symbol with one of these inside its
#: lookback carries phantom pre-ex highs until the window rolls past (2026-09-03 review).
UNADJUSTED_KINDS = frozenset({"bonus", "split", "rights", "demerger"})
VETO_UNADJUSTED_HISTORY = "unadjusted_history"


def unadjusted_history(corp_rows: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    """Symbols with a structural action among ``corp_rows`` (``MarketStore.get_corp_actions`` rows
    for the lookback window). Pure; the caller picks the window."""
    return frozenset(r["symbol"] for r in corp_rows if r["kind"] in UNADJUSTED_KINDS)


class Diagnostics(NamedTuple):
    """Frog-in-the-Pan path-smoothness read on the SAME rows :func:`scan_daily` gates on — DIAGNOSTIC
    ONLY, never a gating input (see the module docstring)."""

    prox: float
    high_52wk: float
    up_day_frac: float
    max_day_move: float


def _bump(counts: dict[str, int] | None, key: str) -> None:
    """Record one veto on the CALLER's accumulator. Mirrors ``brk20._bump`` field-for-field (kept as
    its own copy, not imported, so a change to hi52's observability semantics cannot silently ride on
    brk20's): ``None`` — the default — keeps :func:`scan_daily` a pure function of its arguments."""
    if counts is not None:
        counts[key] = counts.get(key, 0) + 1


def _proximity(rows: Sequence[DailyRow], *, lookback: int) -> tuple[float, float] | None:
    """``(prox, hi)`` for the window of ``min(lookback, len(rows))`` rows ENDING AT ``rows[-1]``, or
    ``None`` on empty input or non-finite/non-positive ``close``/``hi``.

    Shared by the live proximity check, the fresh-cross check (called on ``rows[:-1]`` so its own
    ``rows[-1]`` is ``y-1``), and :func:`diagnostics_for` — one piece of arithmetic computing the same
    windowed high for all three callers.
    """
    if not rows:
        return None
    window = rows[-min(lookback, len(rows)):]
    close_y = rows[-1].close
    hi = max(r.high for r in window)
    if not (math.isfinite(close_y) and math.isfinite(hi)) or hi <= 0.0:
        return None
    return close_y / hi, hi


def diagnostics_for(
    rows: Sequence[DailyRow], *, params: Mapping[str, float] | None = None
) -> Diagnostics | None:
    """Compute the Frog-in-the-Pan diagnostics for ``rows`` standalone — the same proximity/window
    arithmetic :func:`scan_daily` uses, exposed because :class:`SignalCandidate` has no field to carry
    it on (see the module docstring). ``None`` on the same degenerate input that would make the
    proximity read itself impossible; never raises.
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    lookback = int(p["lookback_sessions"])
    if lookback < 1:
        return None
    prox_hi = _proximity(rows, lookback=lookback)
    if prox_hi is None:
        return None
    prox, hi = prox_hi

    tail = rows[-21:] if len(rows) >= 21 else rows
    pairs = list(zip(tail, tail[1:]))
    if not pairs:
        return Diagnostics(prox=round(prox, 4), high_52wk=hi, up_day_frac=0.0, max_day_move=0.0)

    up_days = sum(1 for prev, cur in pairs if cur.close > prev.close)
    moves = [
        abs(cur.close / prev.close - 1.0)
        for prev, cur in pairs
        if math.isfinite(prev.close) and math.isfinite(cur.close) and prev.close != 0.0
    ]
    max_move = max(moves) if moves else 0.0
    return Diagnostics(
        prox=round(prox, 4),
        high_52wk=hi,
        up_day_frac=round(up_days / len(pairs), 2),
        max_day_move=round(max_move, 4),
    )


def scan_daily(
    symbol: str,
    rows: Sequence[DailyRow],
    *,
    today: date,
    upcoming_ex_dates: Sequence[date] = (),
    params: Mapping[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
) -> SignalCandidate | None:
    """Apply the pinned hi52 rule to one symbol's ascending completed-session history.

    ``rows[-1]`` must be the last COMPLETED session (never today's forming bar). Fails to ``None`` on
    thin history, sub-threshold proximity, a stale (non-fresh) crossing, unconfirmed volume, ex-date
    proximity, or degenerate tick-rounded levels — never raises on ordinary OR malformed data (§3.2.5
    fail-to-zero posture).

    ``veto_counts``, when supplied, is incremented in place with :data:`VETO_EX_DATE_SKIP`; the other
    refusals above are this rule's ordinary silence and are not counted (see the module docstring for
    why the ex-date class alone is counted here).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    min_sessions = int(p["min_sessions"])
    lookback = int(p["lookback_sessions"])
    if min_sessions < 2 or lookback < 1 or len(rows) < min_sessions:
        return None

    proximity_min = float(p["proximity_min"])

    live = _proximity(rows, lookback=lookback)
    if live is None:
        return None
    prox, _hi = live
    if prox < proximity_min:
        return None

    # ---- fresh cross: yesterday's OWN window (ending at y-1) must not already have cleared the band.
    prev = _proximity(rows[:-1], lookback=lookback)
    if prev is None:
        return None
    prev_prox, _prev_hi = prev
    if prev_prox >= proximity_min:
        return None

    # ---- volume confirmation vs the mean of the 20 sessions immediately before y.
    vol_window = rows[:-1][-20:]
    vols = [r.volume for r in vol_window]
    avg_vol = sum(vols) / len(vols)
    y_volume = rows[-1].volume
    vol_mult = float(p["vol_mult"])
    if not (math.isfinite(y_volume) and math.isfinite(avg_vol)) or avg_vol < 0.0 or y_volume < vol_mult * avg_vol:
        return None

    # ---- ex-date skip (mirrors brk20's A12 calendar horizon); COUNTED here (see module docstring).
    horizon = today + timedelta(days=int(p["ex_skip_days"]))
    if any(today <= xd <= horizon for xd in upcoming_ex_dates):
        _bump(veto_counts, VETO_EX_DATE_SKIP)
        return None

    # ---- levels: entry is the trigger session's OWN close (no "broken level" to retest here);
    #      disaster stop, no fabricated target — identical reasoning to `cat`.
    stop_pct = Decimal(str(p["stop_pct"]))
    if stop_pct <= 0 or stop_pct >= 100:
        return None  # a non-positive or total stop is not a risk distance
    entry = round_to_tick(rows[-1].close)
    stop = round_to_tick(entry * (Decimal(1) - stop_pct / Decimal(100)))
    if stop >= entry:
        return None  # tick-rounding degenerate — no structural risk distance

    score = min(1.0, max(0.0, prox))
    return SignalCandidate(
        signal_id=str(ULID()),
        strategy_id=STRATEGY_ID,
        symbol=symbol,
        side="BUY",
        style="swing",
        raw_levels=RawLevels(entry=entry, stop=stop, target=None),
        score=score,
    )


def sweep_daily(
    histories: Mapping[str, Sequence[DailyRow]],
    *,
    today: date,
    ex_dates_by_symbol: Mapping[str, Sequence[date]] | None = None,
    unadjusted_symbols: Collection[str] = (),
    params: Mapping[str, float] | None = None,
    veto_counts: dict[str, int] | None = None,
) -> list[SignalCandidate]:
    """Run :func:`scan_daily` over every symbol; deterministic order (§9.6): score desc, symbol asc.

    ``unadjusted_symbols`` (see :func:`unadjusted_history`) are not read at all — their window
    contains bars in two units — and each is counted as :data:`VETO_UNADJUSTED_HISTORY`.
    ``veto_counts`` is threaded straight through — the caller owns the accumulator and reads the
    veto classes off it after the sweep returns (§6.1 observability)."""
    ex_map = ex_dates_by_symbol or {}
    out: list[SignalCandidate] = []
    for symbol in sorted(histories):
        if symbol in unadjusted_symbols:
            _bump(veto_counts, VETO_UNADJUSTED_HISTORY)
            continue
        cand = scan_daily(
            symbol,
            histories[symbol],
            today=today,
            upcoming_ex_dates=ex_map.get(symbol, ()),
            params=params,
            veto_counts=veto_counts,
        )
        if cand is not None:
            out.append(cand)
    out.sort(key=lambda c: (-c.score, c.symbol))
    return out
