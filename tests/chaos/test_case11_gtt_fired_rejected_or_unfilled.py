"""§9.4 chaos case 11 — a GTT fired and its leg was rejected or never filled, rewritten for paper (plan Q4.5).

Must hold: the leg's failure (a reject, or no fill by ``paper.exit_working_timeout_min``) hands the open
qty to the exit routine as ``gtt_failure_exit``; one exit is sent, the GTT is never re-placed, and the
position closes. Real components: OrderManager, PositionBook, ProtectionManager and PaperBroker; the
Q4.6 exit routine is the unit suite's stand-in.
"""

from __future__ import annotations

import pytest

from engine.oms.state import CloseReason
from tests.unit.test_protection_manager import Rig, at, orders, position


@pytest.mark.parametrize("leg", ["rejected", "unfilled"])
async def test_case11_a_failed_leg_ends_in_one_gtt_failure_exit(conn, bus, monkeypatch, leg) -> None:
    rig = Rig(conn, bus)
    try:
        pid = await rig.enter()
        if leg == "rejected":
            match = rig.broker._match

            def reject_sells(order, *args):
                if order.transaction_type == "SELL":
                    raise RuntimeError("leg cannot book")
                return match(order, *args)

            monkeypatch.setattr(rig.broker, "_match", reject_sells)
            await rig.px(at(10, 5), "94.50")
            await rig.px(at(10, 5, 2), "94.50")
            monkeypatch.setattr(rig.broker, "_match", match)
        else:
            await rig.px(at(10, 5), "90.00")
            rig.now.value = at(10, 10)
            await rig.pm.tick()
            await rig.settle()

        [leg_row] = orders(conn, "gtt_leg")
        ended = "REJECTED" if leg == "rejected" else "CANCELLED"
        assert leg_row["state"] == ended and rig.exits == [(CloseReason.GTT_FAILURE_EXIT, f"leg_{ended.lower()}")]
        await rig.px(at(10, 10, 2), "90.00")
        await rig.pm.tick()

        pos = position(conn, pid)
        assert (pos["state"], pos["close_reason"]) == ("CLOSED", "gtt_failure_exit")
        assert len(orders(conn, "exit")) == 1 and len(await rig.gtts()) == 1 and rig.alerts == []
    finally:
        await rig.mgr.stop()
