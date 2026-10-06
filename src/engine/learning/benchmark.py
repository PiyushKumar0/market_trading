"""Equal-weight universe benchmark for rec outcomes (plan Q2.2)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from engine.core.calendar import NSECalendar
from engine.marketdata.store import MarketStore
from engine.strategy.scanners.hi52 import unadjusted_history

BENCH_TIME = "time: fill+1..exit"
BENCH_INTRASESSION = "intrasession: fill+1..exit-1"

_MAX_ABS_RET = 0.25
_ROW_LOOKBACK_SESSIONS = 5
_PCT_Q = Decimal("0.000001")


def compute_ew_return(store: MarketStore, calendar: NSECalendar, d: date) -> tuple[float, int] | None:
    """Mean ``close(d)/close(prev session) - 1`` over the eligible universe of the latest
    ``universe_daily`` row on or before ``d``, and the number of names averaged.

    ``None`` when no row exists within 5 sessions before ``d``. Raises ``ValueError`` when the
    universe, or every name's return, is empty.
    """
    u = d
    for _ in range(_ROW_LOOKBACK_SESSIONS + 1):
        if store.get_universe_daily(u):
            break
        u = calendar.previous_trading_day(u)
    else:
        return None
    symbols = store.get_universe_eligible_symbols(u)
    if not symbols:
        raise ValueError(f"universe_daily {u}: no eligible symbols")
    prev = calendar.previous_trading_day(d)
    closes_d = {b.symbol: b.close for b in store.get_bars_1d_for_day(d)}
    closes_prev = {b.symbol: b.close for b in store.get_bars_1d_for_day(prev)}
    structural = unadjusted_history(store.get_corp_actions(ex_from=d, ex_to=d))
    rets = []
    for s in symbols:
        c, p = closes_d.get(s), closes_prev.get(s)
        if c is None or not p or s in structural:
            continue
        r = float(c / p - 1)
        if abs(r) <= _MAX_ABS_RET:
            rets.append(r)
    if not rets:
        raise ValueError(f"no usable returns for {d} over {len(symbols)} eligible symbols")
    return sum(rets) / len(rets), len(rets)


def bench_pct(
    daily: Mapping[date, float | None],
    fill_d: date,
    exit_d: date,
    exit_reason: str,
    calendar: NSECalendar,
) -> Decimal | None:
    """Compounded benchmark return in percent over the rec's window (:data:`BENCH_TIME` for a
    ``time`` exit, else :data:`BENCH_INTRASESSION`). ``None`` if any session in it is missing or
    ``None`` in ``daily``; an empty window (same-day fill and stop) is 0.
    """
    include_exit = exit_reason == "time"
    growth = 1.0
    s = calendar.add_sessions(fill_d, 1)
    while s < exit_d or (include_exit and s == exit_d):
        r = daily.get(s)
        if r is None:
            return None
        growth *= 1 + r
        s = calendar.add_sessions(s, 1)
    return (Decimal(growth - 1) * 100).quantize(_PCT_Q, rounding=ROUND_HALF_UP)
