"""hi52 forward-test measurement — shared by ``scripts/hi52_forward_verdict.py`` and the rec card.

Pure: signals and per-symbol ``bars_1d`` series are arguments, so callers own the store access.
Conventions (entry = the OPEN of journal day ``d``; exit = the CLOSE of the ``k``-th session of the
hold, entry session first) are pinned in the script's module docstring.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import NamedTuple

#: First session of the PROMOTED population (ASSUMED — see the script's PROMOTION_DATE_PROVENANCE).
PROMOTION_DATE = date(2026, 9, 14)

#: The promotion commit's date, as quoted to the owner.
PROMOTED_ON = date(2026, 9, 12)

#: Horizons measured, in trading sessions of the HOLD. Only the verdict horizon decides; the rest
#: are diagnostics.
HORIZONS: tuple[int, ...] = (5, 10, 20)
VERDICT_HORIZON = 20

#: Minimum measured T+20 signals before the rule can say anything.
MIN_SIGNALS = 20


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


def measure_signal(
    series: Series, signal_date: date, *, cost_pct: float, horizons: Sequence[int] = HORIZONS
) -> Measured | None:
    """Measure one signal against one symbol's series, or ``None`` when it cannot be anchored.

    ``None`` means: the signal day is not a session in this series, or its open is unusable. The
    entry is ``open(d)`` — the journal day's OWN open, the study's anchor — so the entry bar exists
    whenever the anchor does, and the exit for horizon ``k`` is the close of the ``k``-th session of
    the hold, ``close[i + k - 1]``, the entry session counting as the first. A signal that HAS an
    entry but has not reached a horizon is returned with that horizon simply absent from
    ``gross``/``net`` — horizons are measured on their own event sets. Never raises on ordinary or
    malformed bars: a non-finite/non-positive price drops that cell.
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

    The tally's four classes are different things and must not be collapsed: ``no_entry_yet`` (``d``
    is past the symbol's last stored session — the ordinary state of a forward test) versus
    ``no_bars``/``unanchored`` (a store hole: no series, or ``d`` missing from INSIDE the stored
    range) versus ``bad_entry_bar`` (a malformed open).
    """
    trades: list[Measured] = []
    skipped = {"no_bars": 0, "unanchored": 0, "no_entry_yet": 0, "bad_entry_bar": 0}
    for sig in signals:
        series = series_by_symbol.get(sig.symbol)
        if series is None or not len(series):
            skipped["no_bars"] += 1
            continue
        if sig.d not in series.dates:
            skipped["no_entry_yet" if sig.d > series.dates[-1] else "unanchored"] += 1
            continue
        m = measure_signal(series, sig.d, cost_pct=cost_pct, horizons=horizons)
        if m is None:
            skipped["bad_entry_bar"] += 1
            continue
        trades.append(m)
    return trades, skipped


def forward_progress(
    signals: Sequence[Signal], series_by_symbol: Mapping[str, Series]
) -> tuple[int, int]:
    """``(k_matured, m_signals)``: signals with a measured T+20, and signals journalled."""
    trades, _ = measure_all(
        signals, series_by_symbol, cost_pct=0.0, horizons=(VERDICT_HORIZON,)
    )
    return sum(1 for t in trades if VERDICT_HORIZON in t.gross), len(signals)


def forward_line(k: int, m: int) -> str:
    return f"forward test: {k} of {MIN_SIGNALS} matured ({m} signals since {PROMOTED_ON})"
