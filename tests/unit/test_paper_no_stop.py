"""D14 paper no-stop test (plan 2026-10-15): listed strategies open with no stop or target and exit on time."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from engine.oms.state import CloseReason
from tests.unit.test_protection_manager import SYM, Rig, at, orders, position
from tests.unit.test_session_prep import bar, closed, flat, gross, stacks

R = CloseReason
EXIT_DAY = dt.date(2026, 6, 23)            # session 5 of a hold entered Wed 2026-06-17 (weekdays calendar)


@pytest.fixture
async def make_rig(conn, bus):
    rigs: list[Rig] = []

    def build(**kwargs) -> Rig:
        rigs.append(Rig(conn, bus, **kwargs))
        return rigs[-1]

    yield build
    for r in rigs:
        await r.mgr.stop()


@pytest.fixture
async def rig(make_rig) -> Rig:
    return make_rig()


@pytest.fixture
async def make_stack(conn, bus):
    async with stacks(conn, bus) as build:
        yield build


def levels(conn, pid: str) -> tuple:
    pos = position(conn, pid)
    return pos["stop"], pos["target"], pos["exit_policy"], pos["protection_state"]


# ------------------------------------------------------------------------------------- opening
async def test_a_listed_strategy_opens_without_levels_and_gets_no_gtt(conn, make_rig) -> None:
    rig = make_rig(no_stop={"hi52"})
    pid = await rig.enter()

    assert levels(conn, pid) == (None, None, "time_only", "TIME_EXIT_ONLY")
    assert position(conn, pid)["exit_session"] is not None
    assert await rig.gtts() == [] and conn.execute("SELECT COUNT(*) FROM gtts").fetchone()[0] == 0


async def test_an_unlisted_strategy_keeps_its_stop_and_target(conn, make_rig) -> None:
    rig = make_rig(no_stop={"ins"})
    pid = await rig.enter()

    assert levels(conn, pid) == ("95", "110", None, "PROTECTED")
    assert len(await rig.gtts()) == 1


async def test_a_listed_strategy_without_a_known_hold_keeps_its_stop(conn, make_rig) -> None:
    rig = make_rig(no_stop={"hi52"}, hold=None)
    pid = await rig.enter()

    assert levels(conn, pid) == ("95", "110", None, "PROTECTED")
    assert len(await rig.gtts()) == 1


async def test_a_listed_strategy_whose_exit_date_is_beyond_the_calendar_keeps_its_stop(conn, make_rig) -> None:
    """From 2026-12-04 the 20th session is past the loaded 2026 calendar: a time-only position would have
    no exit date, so it opens on the shipped levels instead (and is outside the C5 population)."""
    rig = make_rig(no_stop={"hi52"})

    def beyond_calendar(_d: dt.date, _n: int) -> dt.date:
        raise ValueError("beyond the loaded NSE calendar")

    rig.book._add_sessions = beyond_calendar
    pid = await rig.enter()

    assert levels(conn, pid) == ("95", "110", None, "PROTECTED") and position(conn, pid)["exit_session"] is None
    assert len(await rig.gtts()) == 1


# ------------------------------------------------------------------------------------- protection
async def test_a_no_stop_position_holds_through_a_fall_below_the_shipped_stop(conn, make_rig) -> None:
    rig = make_rig(no_stop={"hi52"})
    pid = await rig.enter()
    await rig.px(at(10, 5), "80.00")
    await rig.pm.tick()
    await rig.pm.verify_all()

    assert (position(conn, pid)["state"], levels(conn, pid)[3]) == ("OPEN", "TIME_EXIT_ONLY")
    assert (rig.exits, rig.alerts, await rig.gtts()) == ([], [], [])


async def test_a_missing_stop_without_the_policy_still_fails_closed(conn, rig) -> None:
    pid = await rig.enter()
    [g] = await rig.gtts()
    await rig.broker.delete_gtt(g["id"])
    conn.execute("UPDATE positions SET stop = NULL WHERE position_id = ?", (pid,))
    rig.now.value = at(10, 5)
    await rig.pm.verify_all()

    pos = position(conn, pid)
    assert (pos["protection_state"], pos["state"]) == ("PROTECTION_FAILED", "PENDING_EXIT")
    assert rig.exits == [(R.GTT_FAILURE_EXIT, "protection_failed")]


# ------------------------------------------------------------------------------------- catch-up and the time exit
async def test_catch_up_ignores_the_shipped_stop_and_closes_on_the_exit_session(conn, make_stack) -> None:
    stack = make_stack(no_stop={"hi52"})
    pid = await stack.enter()
    assert position(conn, pid)["exit_session"] == EXIT_DAY.isoformat()
    dip = bar(at(10, 30), open_="96", high="96", low="80", close="85")
    last = at(15, 29, d=EXIT_DAY)
    stack.bars[SYM] = flat(at(10, 0), 30) + [dip] + flat(at(10, 31), 28) + flat(last - dt.timedelta(minutes=4), 5)

    await stack.prep.run(at(10, 0, 1), at(11, 0))
    assert position(conn, pid)["state"] == "OPEN"

    await stack.prep.run(at(11, 0), at(9, 30, d=EXIT_DAY + dt.timedelta(days=1)))
    pos = position(conn, pid)
    assert closed(conn, pid) == ("CLOSED", "time_stop", "downtime_replay")
    assert (Decimal(pos["realized_pnl"]), pos["closed_at"]) == (gross(conn, pid, "100"), at(15, 30, d=EXIT_DAY).isoformat())


async def test_a_no_stop_position_sells_near_the_close_of_its_exit_session(conn, make_stack) -> None:
    stack = make_stack(no_stop={"hi52"})
    pid = await stack.enter()
    await stack.px(at(10, 5), "80.00")
    await stack.pm.tick()
    await stack.em.tick()
    assert position(conn, pid)["state"] == "OPEN" and orders(conn, "exit") == []

    stack.now.value = at(15, 20, d=EXIT_DAY)
    await stack.em.tick()
    await stack.px(at(15, 20, 2, EXIT_DAY), "90.00")

    pos = position(conn, pid)
    assert (pos["state"], pos["close_reason"], pos["close_basis"]) == ("CLOSED", "time_stop", "time")
    assert len(orders(conn, "exit")) == 1 and orders(conn, "gtt_leg") == [] and stack.alerts == []
