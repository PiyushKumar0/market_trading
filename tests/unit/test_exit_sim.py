from bisect import bisect_left
from datetime import date, datetime
from decimal import Decimal as D

import pytest

from engine.core.clock import IST
from engine.learning.exit_sim import Held, Pending, SimBar, simulate

SESSIONS = tuple(date(2026, 10, d) for d in (5, 6, 7, 8, 9, 12, 13, 14))


def cal(d: date, n: int) -> date:
    i = bisect_left(SESSIONS, d) + n
    if i >= len(SESSIONS):
        raise ValueError("beyond calendar")
    return SESSIONS[i]


def day(d, o, h, lo, c):
    return SimBar(date(2026, 10, d), D(o), D(h), D(lo), D(c))


def m1(d, hm, o, h, lo, c):
    return SimBar(datetime(2026, 10, d, hm // 100, hm % 100, tzinfo=IST), D(o), D(h), D(lo), D(c))


def run(bars, start, **kw):
    args = {"stop": None, "target": None, "horizon": None, "end": date(2026, 10, 30), "cost_pct": D("0.5")}
    return simulate(bars, start, **(args | kw))


MKT_5 = Pending("market", None, date(2026, 10, 5), date(2026, 10, 5))


def test_recs_mixed_stream_market_fill_time_exit():
    bars = [m1(5, 1001, 100, 101, 99, 100), m1(5, 1002, 100, 100, 96, 97),
            day(6, 98, 103, 97, 102), day(7, 102, 106, 101, 105)]
    out = run(bars, MKT_5, stop=D(95), target=D(110), horizon=date(2026, 10, 7), add_sessions=cal)
    assert (out.status, out.fill_basis, out.fill_px, out.exit_d, out.exit_px, out.reason) == (
        "exit", "1m", D(100), date(2026, 10, 7), D(105), "time")
    assert (out.gross_pct, out.net_pct) == (D("5"), D("4.5"))


@pytest.mark.parametrize("bar, px, reason", [
    (day(7, 92, 93, 90, 91), D(92), "stop"),        # gap-through
    (day(7, 98, 99, 95, 97), D(95), "stop"),        # exact touch
    (day(7, 112, 113, 111, 112), D(112), "target"),  # gap-up
    (day(7, 100, 111, 94, 100), D(95), "stop"),     # double touch
])
def test_stop_target_per_bar(bar, px, reason):
    bars = [day(6, 100, 101, 99, 100), bar]
    start = Pending("market", None, date(2026, 10, 5), date(2026, 10, 6))
    out = run(bars, start, stop=D(95), target=D(110), horizon=5)
    assert (out.status, out.exit_px, out.reason) == ("exit", px, reason)


@pytest.mark.parametrize("fill, price, bar, px, basis", [
    ("trade_through", D(99), day(5, 101, 102, 99, 100), None, None),   # exact touch, no trade-through
    ("touch", D(99), day(5, 101, 102, 99, 100), D(99), "daily"),
    ("trade_through", D(99), m1(5, 1001, 97, 98, 96, 97), D(97), "1m"),  # gap below the limit
    ("market", None, day(5, 100, 105, 90, 104), D(104), "daily"),       # daily-only: delivery close
])
def test_entry_fill_rules(fill, price, bar, px, basis):
    start = Pending(fill, price, date(2026, 10, 5), date(2026, 10, 5))
    out = run([bar], start, stop=D(95), horizon=0, end=date(2026, 10, 5))
    assert (out.fill_px, out.fill_basis) == (px, basis)
    assert out.status == ("unfilled" if px is None else "exit")
    if px is not None:
        assert (out.reason, out.exit_px) == ("time", bar.close)  # fill bar never stop-evaluated


@pytest.mark.parametrize("bars, end, status", [
    ([day(5, 101, 102, 100, 101), day(6, 101, 102, 100, 101)], date(2026, 10, 6), "unfilled"),
    ([day(5, 101, 102, 100, 101)], date(2026, 10, 2), "open"),  # window not yet covered
    ([day(6, 101, 102, 90, 101)], date(2026, 10, 6), "open"),   # window session bar missing
])
def test_unfilled_vs_unknown(bars, end, status):
    start = Pending("touch", D(99), date(2026, 10, 5), date(2026, 10, 5))
    assert run(bars, start, end=end, add_sessions=cal).status == status


@pytest.mark.parametrize("calendar, bars, status, exit_d", [
    (cal, [day(5, 100, 100, 100, 100), day(6, 100, 100, 100, 100), day(8, 100, 100, 100, 100)],
     "open", None),                                                                  # missing middle bar
    (None, [day(5, 100, 100, 100, 100), day(6, 100, 100, 100, 100), day(8, 100, 100, 100, 100)],
     "exit", date(2026, 10, 8)),                                                     # bar positions
    (cal, [day(9, 100, 100, 100, 100), day(10, 100, 100, 80, 100), day(12, 100, 100, 100, 100),
           day(13, 100, 100, 100, 100)], "exit", date(2026, 10, 13)),               # uncounted session
])
def test_sessions_calendar_vs_positions(calendar, bars, status, exit_d):
    start = Pending("market", None, bars[0].d, bars[0].d)
    out = run(bars, start, stop=D(95), horizon=2, add_sessions=calendar)
    assert (out.status, out.exit_d, out.fill_d) == (status, exit_d, bars[0].d)


@pytest.mark.parametrize("ex, status", [
    (date(2026, 10, 7), "void_ca"),
    (date(2026, 10, 8), "void_ca"),   # ex-date on the exit session
    (date(2026, 10, 9), "exit"),      # after the exit
    (date(2026, 10, 5), "exit"),      # on the fill session
])
def test_corporate_action_window(ex, status):
    bars = [day(d, 100, 100, 100, 100) for d in (5, 6, 7, 8, 9)]
    out = run(bars, MKT_5, horizon=date(2026, 10, 8), ex_dates=[ex], add_sessions=cal)
    assert out.status == status


@pytest.mark.parametrize("end, status, exit_px", [
    (datetime(2026, 10, 5, 15, 0, tzinfo=IST), "open", None),     # end mid exit session
    (datetime(2026, 10, 6, 9, 20, tzinfo=IST), "exit", D(101)),   # downtime across the close
])
def test_catch_up_open_position_mid_session(end, status, exit_px):
    bars = [m1(5, 1359, 90, 90, 80, 90), m1(5, 1430, 100, 102, 99, 101),
            m1(5, 1529, 101, 101, 100, 101), m1(6, 915, 90, 90, 80, 90)]
    start = Held(D(100), datetime(2026, 10, 5, 14, 0, tzinfo=IST))
    out = run(bars, start, stop=D(95), horizon=date(2026, 10, 5), end=end)
    assert (out.status, out.exit_px, out.fill_d) == (status, exit_px, date(2026, 10, 5))


@pytest.mark.parametrize("k", [1, 2, 3])
def test_research_touch_window_and_fixed_horizon(k):
    bars = [day(6, 105, 106, 101, 104), day(7, 103, 104, 99, 102), day(8, 102, 108, 101, 107),
            day(9, 107, 110, 106, 109)]
    start = Pending("touch", D(100), date(2026, 10, 5), date(2026, 10, 9))
    out = run(bars, start, horizon=k - 1)
    assert (out.fill_d, out.fill_px) == (date(2026, 10, 7), D(100))
    assert (out.exit_d, out.exit_px) == (bars[k].d, bars[k].close)
