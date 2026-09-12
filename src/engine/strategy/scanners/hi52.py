"""``hi52`` — 52-week-high-proximity continuation, v2, a FORWARD TEST (promoted 2026-09-12).

Promoted out of SHADOW on 2026-09-12 (plan §8.6 addendum; owner-directed "proceed with the changes
you deem will improve the engine's performance"). ``settings.yaml`` registers
``hi52.expected_edge_pct`` and the composition root feeds it to the gate's
``strategy_expected_edge_pct`` map exactly as it does ``ins``'s, so the §7.1 C3 cost check now has
an edge basis and a candidate can reach RECOMMEND. The promotion is NOT a claim of a validated
edge: the v2 backtest is CPCV-promotable (1,794 trades, T+20 median net +1.71% / 58.2% hit, fold
pass 86.7% under N=2) but its edge lives in the INDEX cell of a survivorship-tainted population
proxy, so **RECOMMEND mode IS the forward soak** and it carries a written kill rule —
``scripts/hi52_forward_verdict.py`` measures the promoted population and DEMOTE is mechanical
(n >= 20 signals AND (median net at T+20 <= 0 OR T+20 hit rate < 50%)). Demotion re-adds the id to
``ops.main.NO_EDGE_SHADOW_STRATEGIES`` and removes the settings key. This module does not touch
either gate; it only originates.

**v2 IS the rule as of the promotion**: the three signal-time filters pre-registered on 2026-09-09
(smooth approach, no gap day, eligible population) are GATING here, where until 09-12 the first two
were diagnostics only. Their thresholds are the registered ones, frozen in :data:`DEFAULT_PARAMS`
and owner-only — never learnable, never retuned without a new pre-registration.

**Architecture — a pure batch rule over the ELIGIBLE universe, like ``brk20``** (scoped down from
the batch universe on 2026-09-09: the 09-03 full-market backtest measured the edge as index-class
only, extended names −0.26% net at T+20). It is deliberately NOT
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
climb into new highs should drift further than a spiky one. The v2 registration turns that second
reading into part of the trigger (PINNED, below): the path-smoothness read GATES since 2026-09-12,
through the same :func:`diagnostics_for` the study reads, and the surviving candidates' own values
ride the composition root's ``hi52_sweep`` log line so the promoted population's smooth distribution
stays checkable against the 09-09 study at the 20- and 40-signal reviews.

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
  — AND no known ex-date within ``ex_skip_days`` CALENDAR days of ``today`` — AND the two v2
  signal-time filters below.
* v2 filters (PRE-REGISTERED 2026-09-09, GATING since the 2026-09-12 promotion; thresholds fixed
  from the 09-03 full-sample medians BEFORE the run that measured them, so they are knowable at
  signal time): SMOOTH APPROACH — ``up_day_frac >= smooth_up_day_frac_min`` (0.55) AND
  ``max_day_move <= smooth_max_day_move`` (0.07) over the 20 completed sessions ending at ``y``,
  both read from :func:`diagnostics_for` so the live gate and the study consume ONE definition;
  NO GAP DAY — the trigger session's own ``|close(y)/close(y-1) - 1| <= gap_day_max`` (0.05),
  rounded to the same 4 dp ``max_day_move`` is rounded to (when the trigger day is the window's
  biggest mover the two are the SAME physical number and must read identically). Counted as
  :data:`VETO_SMOOTH` / :data:`VETO_GAP`, evaluated in the registration's own order and FIRST-MATCH,
  so the two counts partition the v2 rejects exactly as ``scripts/backtest_hi52.py`` tallies them —
  a trigger move above ``smooth_max_day_move`` books as ``smooth``, never as ``gap``. A diagnostic
  that cannot be computed at all is a REFUSAL, never a pass: the rule's claim is that the filters
  were SATISFIED at signal time, and an unknowable value did not satisfy them. A filter NEUTRALIZED
  to an infinite ceiling (:data:`V1_PARAMS`, the backtest's v1 path only) is not in the registration
  at all and so refuses nothing — an absent filter and an unfailable one are different things.
* The third v2 filter — population = the eligible (index) universe — is NOT enforced here: the
  composition root has fed this sweep the eligible set since 2026-09-10 (§3.2.4), and a scanner that
  re-derived universe membership from its own rows would be a second, drifting definition of it.
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
  reasoning to ``cat``. That reasoning SURVIVES the promotion inverted: the drift this rule captures
  was MEASURED over a fixed horizon, never predicted to a level, so the gate consumes the REGISTERED
  edge (``settings.hi52.expected_edge_pct``, the `ins` seam) instead of an invented target.
  Degenerate tick-rounding (``stop >= entry``) emits nothing, as for ``brk20``/``cat``.
* Score = ``prox``, clamped to ``[0, 1]``: the proximity margin is the strength signal. The scale
  does not agree with any other scanner's and is not meant to — cross-strategy STANDING is read as a
  per-strategy quantile at the analyst forward slot (§5.2(a), ``pipeline._quantile_band``). The one
  place a raw score IS compared across strategies is ``prescreen._rank`` inside one batch admission,
  where (its own docstring) the per-strategy sub-caps and not the sort are what bound a strategy's
  share of the day; the ordering consequence of a high scale there is handled at the composition
  root (``ops.main._publication_order``), not by rescaling here. Rescaling is therefore NOT free:
  it would move this rule's own quantile history and its place in that batch sort.
* Exit is TIME, not price, exactly as for ``cat``: ``hold_sessions`` (= the §7.1 swing ``max_holding``
  cap) is carried in :data:`DEFAULT_PARAMS` purely as documentation of the intended cap — the
  EXISTING max-holding machinery (``RecommendationPipeline.check_aged_positions``) is the exit path,
  and no exit code lives here, mirroring ``cat.DEFAULT_PARAMS["hold_sessions"]`` exactly.
* Diagnostics (Frog-in-the-Pan path-smoothness — the v2 GATE's own inputs since 2026-09-12, and
  logged per surviving candidate by the composition root's ``hi52_sweep`` line):
  ``up_day_frac`` = fraction of the last 20 sessions with ``close > prev close``; ``max_day_move`` =
  the largest single-session ``|close/prev − 1|`` over the same 20 sessions. **Not attached to**
  :class:`~engine.strategy.types.SignalCandidate` — that type's own docstring pins it to EXACTLY its
  §3.2.5 field set (no metadata/notes field exists there, and neither ``brk20`` nor ``cat`` attach
  one), and ``engine/strategy/types.py`` is outside this module's edit scope. They are exposed
  instead through the standalone :func:`diagnostics_for` helper, computable on the same rows
  :func:`scan_daily` scans, so the Frog-in-the-Pan analysis (and this module's own tests) can read
  them without a schema change. :func:`scan_daily` now calls it — the v2 smooth filter IS these two
  numbers, and it is called only AFTER every v1 test has passed, so the "don't cost every universe
  symbol for an unused stat" discipline still holds: the cost is paid per CANDIDATE, not per symbol.

Evidence caveat rides along (§6.1): the forward test is the measurement. The v2 edge was measured on
a population labelled by CURRENT index membership applied backwards — a survivorship-tainted proxy
(09-03 and 09-09 runs both carry the label) — so a live candidate carries the registered edge for
the gate's arithmetic and no presumption of expectancy for the analyst's judgement.
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

DEFAULT_PARAMS: dict[str, float] = {   # §6.3-shaped envelope, frozen for the forward-test window
    "proximity_min": 0.95,
    "lookback_sessions": 252,
    "min_sessions": 126,
    "vol_mult": 1.0,
    "ex_skip_days": 10,
    "hold_sessions": 20,          # documentation only — see module docstring; not read below
    "stop_pct": 6.0,
    # v2 signal-time filters (pre-registered 2026-09-09, gating since the 2026-09-12 promotion).
    # OWNER-ONLY and NEVER learnable — not now, not when §6.3 learning lands: these three numbers
    # DEFINE the measured v2 population, so moving one re-opens multiplicity, invalidates the
    # backtest that justified the promotion and restarts the forward clock. A change here is a NEW
    # pre-registration, never a tuning.
    "smooth_up_day_frac_min": 0.55,
    "smooth_max_day_move": 0.07,
    "gap_day_max": 0.05,
}

#: The v2 filter keys, as one set: what :data:`V1_PARAMS` neutralizes and what the owner-only rule
#: above applies to. Named so a caller never has to re-list the three literals.
V2_FILTER_PARAMS: frozenset[str] = frozenset(
    {"smooth_up_day_frac_min", "smooth_max_day_move", "gap_day_max"}
)

#: The v1 (pre-2026-09-12) parameter set: :data:`DEFAULT_PARAMS` with the v2 filters NEUTRALIZED
#: (a floor nothing can fail, ceilings nothing finite can exceed) rather than removed — every key
#: must stay present, because :func:`scan_daily` merges over :data:`DEFAULT_PARAMS` and a deleted key
#: would simply come back at its gating value.
#:
#: ONE caller: ``scripts/backtest_hi52.py``'s ``--registration v1`` path, whose whole point is to
#: reproduce the 2026-09-03/09-09 v1 numbers on the rule AS REGISTERED THEN. Without this the
#: promotion would silently convert that registration into v2 and make N=1 unreproducible. It is not
#: a live operating mode: nothing in ``engine.ops`` reads it. The neutralization is EXACT, including
#: the fail-closed refusals: an infinite ceiling turns its filter off outright rather than making it
#: unfailable, so a row v1 emitted is not now dropped as a non-computable gap (see :func:`scan_daily`).
#: That relaxation is reachable ONLY through an infinite threshold, which no live params set.
V1_PARAMS: dict[str, float] = {
    **DEFAULT_PARAMS,
    "smooth_up_day_frac_min": 0.0,
    "smooth_max_day_move": math.inf,
    "gap_day_max": math.inf,
}

#: Veto class surfaced through the optional accumulator (§6.1 observability). See the module
#: docstring for why hi52 counts this where brk20 does not.
VETO_EX_DATE_SKIP = "ex_date_skip"

#: v2 filter vetoes (2026-09-12), counted FIRST-MATCH in the registration's own order so the two
#: partition the v2 rejects — the same counting ``scripts/backtest_hi52.py`` does, so a live count
#: and a study count mean the same thing. Read :data:`VETO_GAP` as "trigger moves in the half-open
#: band (``gap_day_max``, ``smooth_max_day_move``], plus any non-computable gap": a bigger trigger
#: move already failed the smooth ceiling and books as :data:`VETO_SMOOTH`.
VETO_SMOOTH = "smooth"
VETO_GAP = "gap"

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
    """Frog-in-the-Pan path-smoothness read on the SAME rows :func:`scan_daily` gates on.

    ``up_day_frac``/``max_day_move`` ARE the v2 smooth filter's inputs since the 2026-09-12
    promotion (the module docstring's PINNED rule); ``prox``/``high_52wk`` stay reporting fields."""

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

    The ONE definition of ``up_day_frac``/``max_day_move``: :func:`scan_daily`'s v2 smooth filter,
    ``scripts/backtest_hi52.py``'s ``signal_diag`` and the journalled diagnostics all call THIS, so
    the live gate and the study that justified it can never drift apart.
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
    proximity, a v2 filter refusal, or degenerate tick-rounded levels — never raises on ordinary OR
    malformed data (§3.2.5 fail-to-zero posture).

    ``veto_counts``, when supplied, is incremented in place with :data:`VETO_EX_DATE_SKIP`,
    :data:`VETO_SMOOTH` or :data:`VETO_GAP`; the other refusals above are this rule's ordinary
    silence and are not counted (see the module docstring for why the ex-date class alone is counted
    here).
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

    # ---- v2 signal-time filters (pre-registered 2026-09-09, GATING since 2026-09-12). Placed AFTER
    #      every v1 test and BEFORE the levels, so the order matches `backtest_hi52.v2_admits`
    #      applying them to a v1 signal: a candidate the ex-date horizon already refused never
    #      reaches these counters. Smooth first, then gap, first-match (see VETO_GAP's note).
    diag = diagnostics_for(rows, params=p)
    if diag is None or not (
        math.isfinite(diag.up_day_frac)
        and math.isfinite(diag.max_day_move)
        and diag.up_day_frac >= float(p["smooth_up_day_frac_min"])
        and diag.max_day_move <= float(p["smooth_max_day_move"])
    ):
        _bump(veto_counts, VETO_SMOOTH)
        return None
    # The trigger session's OWN move, rounded to the 4 dp `diagnostics_for` rounds max_day_move to:
    # when y is the window's biggest mover the two ARE the same physical number. rows[-2] exists —
    # min_sessions >= 2 is enforced above — and a zero/non-finite prior close cannot divide.
    # An INFINITE `gap_day_max` turns the filter OFF rather than making it unfailable: the fail-closed
    # refusal below is the claim "the filter was SATISFIED at signal time", and a filter that is not
    # in the registration has nothing to satisfy. V1_PARAMS is the only caller that sets it, and this
    # is what makes that neutralization exact — otherwise a symbol whose close(y-1) is 0/NaN (a
    # suspended session, which the archive contains) would be dropped and booked VETO_GAP on a v1
    # re-run that never looked at rows[-2], silently changing the population behind the N=1 numbers.
    # The LIVE default (0.05) is finite, so the live rule is unchanged and still fails closed.
    gap_max = float(p["gap_day_max"])
    prev_close = rows[-2].close
    gap_move = (
        round(abs(rows[-1].close / prev_close - 1.0), 4)
        if math.isfinite(prev_close) and prev_close > 0.0
        else math.nan
    )
    if math.isfinite(gap_max) and not (math.isfinite(gap_move) and gap_move <= gap_max):
        _bump(veto_counts, VETO_GAP)
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
