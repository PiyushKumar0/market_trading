"""Platform equity, day-scoped counters, and the §7.1 floor ladder (ExposureTracker)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from engine.core.enums import Actor, Mode, RiskState
from engine.core.types import OwnerConfirmation
from engine.risk.exposure import ExposureTracker, FloorLimits
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager

CAPITAL = Decimal("20000")
TODAY = date(2026, 6, 17)          # conftest FIXED_NOW
YESTERDAY = date(2026, 6, 16)


def _position(
    conn,
    position_id: str,
    *,
    symbol: str = "AAA",
    side: str = "BUY",
    product: str = "MIS",
    qty: int = 10,
    avg_entry: str = "100",
    state: str = "OPEN",
    origin: str = "platform",
    opened_at: str | None = f"{TODAY.isoformat()}T09:30:00+05:30",
    closed_at: str | None = None,
    realized_pnl: str | None = None,
    costs: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin,
                               opened_at, closed_at, realized_pnl, costs)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (position_id, symbol, side, product, qty, avg_entry, state, origin,
         opened_at, closed_at, realized_pnl, costs),
    )


def _ledger(conn, entry_id: str, outcome: str, closed_at: str) -> None:
    conn.execute(
        "INSERT INTO learning_ledger (entry_id, outcome_label, closed_at) VALUES (?, ?, ?)",
        (entry_id, outcome, closed_at),
    )


def _snapshot(conn, at: str, equity: str) -> None:
    conn.execute(
        "INSERT INTO equity_snapshots (at, equity, realized_pnl, open_mtm, day_mtm, positions_open) "
        "VALUES (?, ?, '0', '0', '0', 0)",
        (at, equity),
    )


def _mixed_book(conn) -> None:
    """Closed + open + external positions; the §7.1 worked example used across the equity tests."""
    _position(conn, "c1", state="CLOSED", origin="platform", symbol="AAA",
              closed_at=f"{TODAY.isoformat()}T11:00:00+05:30", realized_pnl="500", costs="40")
    _position(conn, "c2", state="CLOSED", origin="recommended", symbol="BBB",
              closed_at=f"{TODAY.isoformat()}T12:00:00+05:30", realized_pnl="-200", costs="35")
    _position(conn, "c3", state="CLOSED", origin="external", symbol="EXT",
              closed_at=f"{TODAY.isoformat()}T12:30:00+05:30", realized_pnl="10000", costs="0")
    _position(conn, "o1", origin="platform", symbol="AAA", side="BUY", product="MIS",
              qty=10, avg_entry="100")
    _position(conn, "o2", origin="recommended", symbol="BBB", side="SELL", product="CNC",
              qty=5, avg_entry="200")
    _position(conn, "o3", origin="external", symbol="EXT", side="BUY", product="CNC",
              qty=100, avg_entry="50")


MARKS = {"AAA": Decimal("110"), "BBB": Decimal("190"), "EXT": Decimal("60")}


# ----------------------------------------------------------------- platform equity (§7.1)
def test_equity_worked_example_excludes_external(conn, clock):
    _mixed_book(conn)
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get)
    # realized net: (500-40) + (-200-35) = 225; external's +10000 excluded (O5)
    assert tracker.realized_net() == Decimal("225")
    # open MTM: long 10 @100 -> 110 = +100; short 5 @200 -> 190 = +50; external excluded
    assert tracker.open_mtm() == Decimal("150")
    assert tracker.equity() == Decimal("20375")


def test_open_mtm_falls_back_to_avg_entry_without_marks(conn, clock):
    _mixed_book(conn)
    # No callback at all (cold start before the WARMING broker last_price is wired).
    assert ExposureTracker(conn, clock, CAPITAL).open_mtm() == Decimal("0")
    # Callback present but returning None for one symbol: that leg marks flat, the other still marks.
    partial = ExposureTracker(conn, clock, CAPITAL, mark_price=lambda s: MARKS.get(s) if s == "AAA" else None)
    assert partial.open_mtm() == Decimal("100")
    assert partial.equity() == Decimal("20325")


# ----------------------------------------------------------------- day MTM baseline (§2.6)
def test_day_mtm_uses_explicit_baseline(conn, clock):
    _mixed_book(conn)
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get)
    tracker.set_day_baseline(Decimal("20000"))
    assert tracker.day_mtm() == Decimal("375")


def test_day_baseline_rebuilds_from_last_prior_session_snapshot(conn, clock):
    _mixed_book(conn)
    _snapshot(conn, f"{YESTERDAY.isoformat()}T15:25:00+05:30", "20500")
    _snapshot(conn, "2026-06-15T15:25:00+05:30", "19000")          # older; must not win
    _snapshot(conn, f"{TODAY.isoformat()}T09:20:00+05:30", "20900")  # today's own; must not be the baseline
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get)
    assert tracker.day_baseline() == Decimal("20500")
    assert tracker.day_mtm() == Decimal("-125")


def test_day_baseline_without_prior_snapshot_backs_out_todays_closes(conn, clock):
    """First-ever run with positions already closed today (§2.6 step 2): the baseline is current
    equity MINUS today's realized net, so an offline-realized P&L still counts toward day-MTM —
    a loss closed while the engine was down must be able to trip daily_loss_soft/hard."""
    _mixed_book(conn)
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get)
    # today's realized net: (500-40) + (-200-35) = +225 (external excluded)
    assert tracker.day_baseline() == Decimal("20150")
    assert tracker.day_mtm() == Decimal("225")


def test_day_mtm_counts_offline_loss_on_first_run(conn, clock):
    """An MIS loss realized by the broker while the engine was off, discovered on the first run of
    the day with no prior snapshots: day_mtm reports the loss, not zero."""
    _position(conn, "off1", state="CLOSED", origin="platform", symbol="AAA",
              closed_at=f"{TODAY.isoformat()}T09:20:00+05:30", realized_pnl="-900", costs="45")
    tracker = ExposureTracker(conn, clock, CAPITAL)
    assert tracker.day_mtm() == Decimal("-945")


# ----------------------------------------------------------------- consecutive_losses rebuild (§7.1)
@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        (["loss", "loss", "win", "loss"], 1),   # count back from the latest until a non-loss
        (["loss", "loss", "loss", "win"], 0),   # a win at the end resets the run
        (["win", "loss", "loss", "loss"], 3),   # three straight closes ⇒ FROZEN
        ([], 0),
    ],
)
def test_consecutive_losses_rebuilt_from_ledger_order(conn, clock, labels, expected):
    for i, label in enumerate(labels):
        _ledger(conn, f"e{i}", label, f"{TODAY.isoformat()}T{10 + i:02d}:00:00+05:30")
    assert ExposureTracker(conn, clock, CAPITAL).consecutive_losses() == expected


def test_consecutive_losses_is_session_scoped_and_ignores_no_action(conn, clock):
    _ledger(conn, "y1", "loss", f"{YESTERDAY.isoformat()}T14:00:00+05:30")
    _ledger(conn, "y2", "loss", f"{YESTERDAY.isoformat()}T15:00:00+05:30")
    _ledger(conn, "t1", "loss", f"{TODAY.isoformat()}T10:00:00+05:30")
    # An expired recommendation is not a close — it must not break the run (§3.6).
    _ledger(conn, "t2", "no_action", f"{TODAY.isoformat()}T10:30:00+05:30")
    _ledger(conn, "t3", "loss", f"{TODAY.isoformat()}T11:00:00+05:30")
    tracker = ExposureTracker(conn, clock, CAPITAL)
    assert tracker.consecutive_losses() == 2                 # yesterday's two excluded
    assert tracker.consecutive_losses(YESTERDAY) == 2


# ----------------------------------------------------------------- day + open counters
def test_trades_opened_today_counts_platform_origins_only(conn, clock):
    _position(conn, "p1", origin="platform")
    _position(conn, "p2", origin="recommended")
    _position(conn, "p3", origin="external")
    _position(conn, "p4", origin="platform", opened_at=f"{YESTERDAY.isoformat()}T09:30:00+05:30")
    tracker = ExposureTracker(conn, clock, CAPITAL)
    assert tracker.trades_opened_today() == 2
    assert tracker.trades_opened_today(YESTERDAY) == 1


def test_open_exposure_counters(conn, clock):
    _position(conn, "o1", symbol="AAA", product="MIS", qty=10, avg_entry="100")
    _position(conn, "o2", symbol="BBB", product="CNC", qty=20, avg_entry="150", origin="recommended")
    _position(conn, "o3", symbol="BBB", product="CNC", qty=5, avg_entry="150")
    _position(conn, "o4", symbol="EXT", product="CNC", qty=100, avg_entry="50", origin="external")
    _position(conn, "o5", symbol="CCC", product="MIS", qty=1, avg_entry="900", state="CLOSED")
    tracker = ExposureTracker(conn, clock, CAPITAL)

    counts = tracker.open_position_counts()
    assert (counts.total, counts.mis, counts.cnc) == (3, 1, 2)   # external + closed excluded
    assert tracker.per_symbol_open("BBB") == 2
    assert tracker.per_symbol_open("EXT") == 0
    assert tracker.cnc_notional("BBB") == Decimal("3750")        # (20+5) × 150
    assert tracker.cnc_notional("AAA") == Decimal("0")           # MIS leg is not CNC notional
    assert tracker.deployed_capital() == Decimal("4750")         # 1000 + 3750, MIS at full notional
    assert tracker.per_sector_open({"AAA": "IT", "BBB": "BANK"}) == {"IT": 1, "BANK": 2}
    assert tracker.per_sector_open({"AAA": "IT"}) == {"IT": 1, "UNCLASSIFIED": 2}


# ----------------------------------------------------------------- minute persistence (§7.1)
def test_persist_snapshot_writes_minute_row_and_replaces(conn, clock):
    _mixed_book(conn)
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get)
    tracker.set_day_baseline(Decimal("20000"))
    snap = tracker.persist_snapshot()

    assert snap.at.isoformat() == "2026-06-17T10:05:00+05:30"    # minute-truncated
    assert (snap.equity, snap.day_mtm, snap.positions_open) == (Decimal("20375"), Decimal("375"), 2)
    rows = conn.execute("SELECT * FROM equity_snapshots").fetchall()
    assert len(rows) == 1
    assert rows[0]["at"] == "2026-06-17T10:05:00+05:30"
    assert Decimal(rows[0]["equity"]) == Decimal("20375")
    assert Decimal(rows[0]["realized_pnl"]) == Decimal("225")
    assert Decimal(rows[0]["open_mtm"]) == Decimal("150")

    # Same minute, moved marks ⇒ REPLACE, not a second row.
    tracker2 = ExposureTracker(conn, clock, CAPITAL, mark_price=lambda s: Decimal("120") if s == "AAA" else MARKS.get(s))
    tracker2.set_day_baseline(Decimal("20000"))
    tracker2.persist_snapshot()
    rows = conn.execute("SELECT equity FROM equity_snapshots").fetchall()
    assert len(rows) == 1
    assert Decimal(rows[0]["equity"]) == Decimal("20475")   # AAA marked 100→120: +100 on 10 shares


# ----------------------------------------------------------------- floor ladder (§7.1)
def _equity_at(conn, equity: Decimal) -> None:
    """One closed platform position that lands equity exactly on ``equity`` (costs zero)."""
    _position(conn, "pnl", state="CLOSED", closed_at=f"{TODAY.isoformat()}T11:00:00+05:30",
              realized_pnl=str(equity - CAPITAL), costs="0")


def test_equity_floor_rung_boundary_is_inclusive(conn, clock):
    _equity_at(conn, Decimal("18000"))                # exactly −10%
    tracker = ExposureTracker(conn, clock, CAPITAL)
    breaches = tracker.evaluate_floors(FloorLimits())
    assert [b.rung for b in breaches] == ["equity_floor_rung"]
    assert breaches[0].threshold == Decimal("18000")
    assert breaches[0].action == "forced_exit_close_only"


def test_equity_floor_rung_just_above_does_not_trip(conn, clock):
    _equity_at(conn, Decimal("18000.01"))
    assert ExposureTracker(conn, clock, CAPITAL).evaluate_floors(FloorLimits()) == []


def test_cumulative_floor_boundary_is_strict(conn, clock):
    _equity_at(conn, Decimal("17000"))                # exactly −15%: "< ₹17,000" ⇒ NOT killed
    tracker = ExposureTracker(conn, clock, CAPITAL)
    assert [b.rung for b in tracker.evaluate_floors(FloorLimits())] == ["equity_floor_rung"]


def test_cumulative_floor_below_boundary_trips(conn, clock):
    _equity_at(conn, Decimal("16999.99"))
    breaches = ExposureTracker(conn, clock, CAPITAL).evaluate_floors(FloorLimits())
    # most-restrictive first
    assert [b.rung for b in breaches] == ["cumulative_floor", "equity_floor_rung"]
    assert breaches[0].threshold == Decimal("17000")
    assert breaches[0].action == "kill_forced_off"


def test_weekly_drawdown_peak_window_is_five_sessions(conn, clock):
    # Six snapshotted sessions; the 25,000 peak is the 6th back and must fall out of the window.
    for d, eq in [("2026-06-09", "25000"), ("2026-06-10", "21000"), ("2026-06-11", "20800"),
                  ("2026-06-12", "20600"), ("2026-06-15", "20400"), ("2026-06-16", "20200")]:
        _snapshot(conn, f"{d}T15:25:00+05:30", eq)
    _equity_at(conn, Decimal("20000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    assert tracker.weekly_drawdown_peak() == Decimal("21000")
    # 1000/21000 = 4.8% < 8% ⇒ no breach (against the dropped 25,000 it would be 20%).
    assert tracker.evaluate_floors(FloorLimits()) == []


def test_weekly_drawdown_breach(conn, clock):
    for d, eq in [("2026-06-10", "22000"), ("2026-06-11", "21500"), ("2026-06-12", "21000"),
                  ("2026-06-15", "20800"), ("2026-06-16", "20400")]:
        _snapshot(conn, f"{d}T15:25:00+05:30", eq)
    _equity_at(conn, Decimal("20000"))               # 2000/22000 = 9.1% ≥ 8%
    breaches = ExposureTracker(conn, clock, CAPITAL).evaluate_floors(FloorLimits())
    assert [b.rung for b in breaches] == ["weekly_drawdown"]
    assert breaches[0].threshold == Decimal("20240")  # 22000 × (1 − 0.08)
    assert breaches[0].action == "close_only_downgrade"


def test_floor_limits_capital_base_override(conn, clock):
    _equity_at(conn, Decimal("18000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    limits = FloorLimits(capital_base=Decimal("10000"))   # a smaller base ⇒ 18,000 is above every rung
    assert tracker.evaluate_floors(limits) == []


# ----------------------------------------------------------------- applying the ladder (§3.5.3)
async def _auto(mm: ModeManager) -> None:
    await mm.request_transition(Mode.AUTO, Actor.OWNER, OwnerConfirmation(actor=Actor.OWNER, confirmed=True))


@pytest.mark.asyncio
async def test_apply_floor_breach_routes_through_latch_when_wired(conn, clock):
    """With a RiskStateLatch wired, a floor rung latches as a per-rung CAUSE (§3.5.3) — so clearing
    an unrelated cause (owner /resume_entries) can never relax the floor-set CLOSE_ONLY."""
    from engine.risk.causes import CAUSE_OWNER_PAUSE, RiskStateLatch

    _equity_at(conn, Decimal("18000"))                 # −10% rung
    tracker = ExposureTracker(conn, clock, CAPITAL)
    mm, ks = ModeManager(conn, clock), KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mm)
    await _auto(mm)
    await latch.set_cause(CAUSE_OWNER_PAUSE, RiskState.FROZEN, "owner pause", Actor.OWNER)

    await tracker.apply_floor_breaches(tracker.evaluate_floors(FloorLimits()), mm, ks, latch=latch)
    assert mm.risk_state() == RiskState.CLOSE_ONLY
    assert any(c == "floor_equity_floor_rung" for c, _s, _d in latch.active_causes())

    # Clearing the unrelated owner_pause must NOT relax the floor's CLOSE_ONLY.
    await latch.clear_cause(CAUSE_OWNER_PAUSE, Actor.OWNER)
    assert mm.risk_state() == RiskState.CLOSE_ONLY


@pytest.mark.asyncio
async def test_apply_weekly_drawdown_forces_close_only_and_recommend(conn, clock):
    for d, eq in [("2026-06-10", "22000"), ("2026-06-16", "20400")]:
        _snapshot(conn, f"{d}T15:25:00+05:30", eq)
    _equity_at(conn, Decimal("20000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    mm, ks = ModeManager(conn, clock), KillSwitch(conn, clock)
    await _auto(mm)

    alerts: list[str] = []

    async def alert(message: str) -> None:
        alerts.append(message)

    breaches = tracker.evaluate_floors(FloorLimits())
    await tracker.apply_floor_breaches(breaches, mm, ks, alert=alert)

    assert mm.risk_state() == RiskState.CLOSE_ONLY
    assert mm.mode() == Mode.RECOMMEND
    assert ks.is_killed() is False
    assert len(alerts) == 1 and "weekly_drawdown" in alerts[0]


@pytest.mark.asyncio
async def test_apply_equity_floor_rung_flattens(conn, clock):
    _equity_at(conn, Decimal("18000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    mm, ks = ModeManager(conn, clock), KillSwitch(conn, clock)
    await _auto(mm)

    flattened: list[str] = []

    async def flatten() -> None:
        flattened.append("go_flat")

    await tracker.apply_floor_breaches(tracker.evaluate_floors(FloorLimits()), mm, ks, flatten=flatten)
    assert flattened == ["go_flat"]
    assert mm.risk_state() == RiskState.CLOSE_ONLY
    assert mm.mode() == Mode.RECOMMEND
    assert ks.is_killed() is False


@pytest.mark.asyncio
async def test_apply_cumulative_floor_kills_and_forces_off(conn, clock):
    _equity_at(conn, Decimal("16000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    mm, ks = ModeManager(conn, clock), KillSwitch(conn, clock)
    await _auto(mm)

    flattened: list[str] = []

    async def flatten() -> None:
        flattened.append("go_flat")

    breaches = tracker.evaluate_floors(FloorLimits())
    await tracker.apply_floor_breaches(breaches, mm, ks, flatten=flatten)

    assert ks.is_killed() is True
    assert "cumulative_floor" in (ks.reason() or "")
    assert mm.mode() == Mode.OFF               # kill forces OFF, harsher than the rung's RECOMMEND
    assert mm.risk_state() == RiskState.CLOSE_ONLY   # the −10% rung still applied its state edge
    assert flattened == ["go_flat"]


@pytest.mark.asyncio
async def test_apply_is_idempotent_and_never_relaxes_state(conn, clock):
    _equity_at(conn, Decimal("16000"))
    tracker = ExposureTracker(conn, clock, CAPITAL)
    mm, ks = ModeManager(conn, clock), KillSwitch(conn, clock)
    await _auto(mm)
    breaches = tracker.evaluate_floors(FloorLimits())

    await tracker.apply_floor_breaches(breaches, mm, ks)
    await tracker.apply_floor_breaches(breaches, mm, ks)   # a startup re-evaluation (§2.6)
    assert (mm.mode(), mm.risk_state(), ks.is_killed()) == (Mode.OFF, RiskState.CLOSE_ONLY, True)

    # A milder rung firing under an already-KILLED state must not downgrade it to CLOSE_ONLY.
    await mm.set_risk_state(RiskState.KILLED, "kill_switch", Actor.RISK_GATE)
    await tracker.apply_floor_breaches(breaches, mm, ks)
    assert mm.risk_state() == RiskState.KILLED
