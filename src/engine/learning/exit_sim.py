"""Pure exit simulator shared by rec outcomes, the paper catch-up and research (plan Q2.1).

Each session in the stream is either 1m bars (``at`` a tz-aware minute start) or one daily bar
(``at`` a date). Percentages are percent of the fill price.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

_PCT_Q = Decimal("0.000001")
_HUNDRED = Decimal("100")


@dataclass(frozen=True, slots=True)
class SimBar:
    at: date | datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    @property
    def intraday(self) -> bool:
        return isinstance(self.at, datetime)

    @property
    def d(self) -> date:
        return self.at.date() if isinstance(self.at, datetime) else self.at


@dataclass(frozen=True, slots=True)
class Pending:
    """An entry order live from session ``placed`` through session ``until``.

    ``market`` fills at the first bar's open, except on a daily bar of ``placed`` (the open preceded
    the order), where it fills at the close. ``trade_through`` needs ``low < price``, ``touch``
    ``low <= price``; both fill at ``min(open, price)``.
    """

    fill: Literal["market", "trade_through", "touch"]
    price: Decimal | None
    placed: date
    until: date

    def __post_init__(self) -> None:
        if (self.fill == "market") != (self.price is None):
            raise ValueError("price is required for a limit fill and forbidden for market")


@dataclass(frozen=True, slots=True)
class Held:
    price: Decimal
    at: datetime


@dataclass(frozen=True, slots=True)
class Outcome:
    status: Literal["exit", "void_ca", "open", "unfilled"]
    fill_d: date | None = None
    fill_px: Decimal | None = None
    fill_basis: Literal["1m", "daily"] | None = None
    exit_d: date | None = None
    exit_px: Decimal | None = None
    reason: Literal["stop", "target", "time"] | None = None
    gross_pct: Decimal | None = None
    cost_pct: Decimal | None = None
    net_pct: Decimal | None = None


def simulate(
    bars: Sequence[SimBar],
    start: Pending | Held,
    *,
    stop: Decimal | None,
    target: Decimal | None,
    horizon: date | int | None,
    end: date | datetime,
    cost_pct: Decimal,
    ex_dates: Iterable[date] = (),
    add_sessions: Callable[[date, int], date] | None = None,
) -> Outcome:
    """Walk ``bars`` (ordered) from ``start`` to an outcome.

    ``horizon``: the exit session's date, or the number of sessions after the fill session (T+k is
    ``k - 1``), or None for no time exit. ``end``: a date covers that whole session; a datetime
    admits 1m bars starting before it and daily bars of earlier sessions only. ``ex_dates``: the
    unadjusted-kind ex-dates; one in (fill session, bar session] voids. With ``add_sessions``
    (``NSECalendar.add_sessions``) sessions are calendar sessions: a missing session or a calendar
    ``ValueError`` ends the walk ``open``, and bars on uncounted sessions are skipped. Without it,
    sessions are the stream's own dates (bar positions). Stop and target are not evaluated on the
    fill bar nor on a daily bar of the fill session; both touched in one bar is a stop.
    """
    walk = [b for b in bars if _admitted(b, end)]
    ex = tuple(ex_dates)
    fill_d = fill_px = basis = None
    fill_i = -1
    if isinstance(start, Held):
        fill_d, fill_px = start.at.date(), start.price
        walk = [b for b in walk if (b.at > start.at if b.intraday else b.d >= fill_d)]

    def result(status, b=None, px=None, reason=None) -> Outcome:
        if px is None:
            return Outcome(status, fill_d, fill_px, basis)
        gross = ((px - fill_px) / fill_px * _HUNDRED).quantize(_PCT_Q, rounding=ROUND_HALF_UP)
        return Outcome(status, fill_d, fill_px, basis, b.d, px, reason, gross, cost_pct, gross - cost_pct)

    pending = start if isinstance(start, Pending) else None
    first = start.placed if isinstance(start, Pending) else start.at.date()
    prev_d = fill_d
    held = 0
    for i, b in enumerate(walk):
        if b.d != prev_d:
            if add_sessions is not None:
                try:
                    want = add_sessions(first, 0) if prev_d is None else add_sessions(prev_d, 1)
                except ValueError:
                    return result("open")
                if b.d < want:
                    continue
                if b.d > want:
                    return result("open")
            prev_d = b.d
            if fill_d is not None and b.d > fill_d:
                held += 1
                if any(fill_d < x <= b.d for x in ex):
                    return result("void_ca")
                if horizon is not None and (held > horizon if isinstance(horizon, int) else b.d > horizon):
                    return result("open")
        if pending is not None and fill_px is None:
            if b.d > pending.until:
                return result("unfilled")
            px = _entry_px(pending, b)
            if px is None:
                continue
            fill_d, fill_px, basis, fill_i = b.d, px, "1m" if b.intraday else "daily", i
        if i != fill_i and (b.intraday or b.d != fill_d):
            if stop is not None and b.low <= stop:
                return result("exit", b, min(b.open, stop), "stop")
            if target is not None and b.high >= target:
                return result("exit", b, max(b.open, target), "target")
        nxt = walk[i + 1] if i + 1 < len(walk) else None
        session_closed = nxt.d > b.d if nxt is not None else _covered(b.d, end)
        due = held == horizon if isinstance(horizon, int) else b.d == horizon
        if session_closed and due:
            return result("exit", b, b.close, "time")
    if pending is not None and fill_px is None and prev_d == pending.until and _covered(prev_d, end):
        return result("unfilled")
    return result("open")


def _entry_px(p: Pending, b: SimBar) -> Decimal | None:
    if p.price is None:
        return b.close if not b.intraday and b.d == p.placed else b.open
    hit = b.low < p.price if p.fill == "trade_through" else b.low <= p.price
    return min(b.open, p.price) if hit else None


def _admitted(b: SimBar, end: date | datetime) -> bool:
    if isinstance(end, datetime):
        return b.at < end if b.intraday else b.d < end.date()
    return b.d <= end


def _covered(d: date, end: date | datetime) -> bool:
    return d < end.date() if isinstance(end, datetime) else d <= end
