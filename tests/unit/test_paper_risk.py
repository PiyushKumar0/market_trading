"""Paper equity and halts (plan Q4.8)."""

from __future__ import annotations

import ast
import datetime as dt
import inspect
import shutil
from decimal import Decimal

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.core.contracts import PAPER_ORDER_UPDATE_TOPIC
from engine.core.enums import Actor, RiskState
from engine.core.protected_store import ProtectedStore
from engine.core.types import OwnerConfirmation
from engine.ops import main as opsmain
from engine.ops.paper_control import PaperControl
from engine.ops.paper_risk import PaperRisk, active_halts, paper_effective_state
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from engine.risk.causes import RiskStateLatch
from engine.risk.exposure import ExposureTracker
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine, floor_limits_from
from engine.risk.mode import ModeManager

D = dt.date(2026, 6, 17)
NEXT = dt.date(2026, 6, 18)
R = RiskState


def at(h: int, m: int, d: dt.date = D) -> dt.datetime:
    return dt.datetime.combine(d, dt.time(h, m), tzinfo=IST)


class _Now:
    value = at(10, 0)

    def __call__(self) -> dt.datetime:
        return self.value


@pytest.fixture
def now() -> _Now:
    return _Now()


@pytest.fixture
def clock(now) -> Clock:
    return Clock(time_source=now)


@pytest.fixture
def limits(conn, clock, tmp_path) -> LimitsEngine:
    shutil.copyfile(config_dir() / "limits.yaml", tmp_path / "limits.yaml")
    store = ProtectedStore(tmp_path, conn, clock)
    store.register_initial("limits.yaml", OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test"))
    return LimitsEngine(store)


@pytest.fixture
def marks() -> dict[str, Decimal]:
    return {}


@pytest.fixture
def flattens() -> list[str]:
    return []


@pytest.fixture
def broker(clock) -> PaperBroker:
    return PaperBroker(clock, lambda *_: None, FillModelConfig(), lambda _s: Decimal("0.05"), 7,
                       available_margin=Decimal("1"), topic=PAPER_ORDER_UPDATE_TOPIC)


@pytest.fixture
def make_risk(conn, clock, limits, marks, flattens, broker):
    """A fresh PaperRisk is a fresh process: nothing in memory survives, the tables do."""
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False)
    PaperControl(conn, clock)
    conn.execute("INSERT INTO paper_equity_snapshots (at, equity) VALUES (?, '40000')",
                 (at(15, 29, D - dt.timedelta(days=1)).isoformat(),))

    async def flatten(basis: str) -> int:
        flattens.append(basis)
        return 1

    def build() -> PaperRisk:
        return PaperRisk(conn, clock, calendar.session, broker, capital_base=Decimal("40000"),
                         mark_price=marks.get, limits_fn=limits.load, flatten=flatten)

    return build


def position(conn, pid: str = "P1", *, paper: bool = True) -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, stop, state, is_paper, origin, "
        "opened_at) VALUES (?, 'TCS', 'BUY', 'CNC', 100, '400', '380', 'OPEN', ?, ?, ?)",
        (pid, int(paper), "platform" if paper else "recommended", at(9, 30).isoformat()),
    )


def halts(conn) -> dict[str, tuple[str, int]]:
    rows = conn.execute("SELECT cause, rung, latched FROM paper_halts WHERE cleared_at IS NULL")
    return {r["cause"]: (r["rung"], r["latched"]) for r in rows}


def real_state_rows(conn) -> list[tuple]:
    return [tuple(r) for table in ("mode_state", "risk_state_causes", "kill_state")
            for r in conn.execute(f"SELECT * FROM {table}")]


ELEVEN_PCT_DOWN = {
    "daily_loss_soft": ("FROZEN", 0),
    "daily_loss_hard": ("CLOSE_ONLY", 0),
    "floor_weekly_drawdown": ("CLOSE_ONLY", 1),
    "floor_equity_floor_rung": ("CLOSE_ONLY", 1),
}


# ---------------------------------------------------------------- isolation
@pytest.mark.parametrize(("mark", "expected", "effective"), [
    ("355", ELEVEN_PCT_DOWN, R.CLOSE_ONLY),
    ("335", {**ELEVEN_PCT_DOWN, "floor_cumulative_floor": ("KILLED", 1)}, R.KILLED),
])
async def test_a_paper_floor_breach_latches_paper_halts_only_and_exits_the_book_once(
    conn, clock, make_risk, marks, flattens, mark, expected, effective
) -> None:
    mode, kill = ModeManager(conn, clock, paper_only=False), KillSwitch(conn, clock)
    RiskStateLatch(conn, clock, mode)
    position(conn)
    marks["TCS"] = Decimal(mark)
    before = real_state_rows(conn)
    risk = make_risk()

    for _ in range(2):
        await risk.tick()

    assert halts(conn) == expected
    assert flattens == ["equity_floor"]
    assert paper_effective_state(conn, mode.risk_state)() is effective
    assert real_state_rows(conn) == before and mode.risk_state() is R.NORMAL and not kill.is_killed()
    assert conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0] == 0
    assert conn.execute("SELECT equity FROM paper_equity_snapshots ORDER BY at DESC").fetchone()[0] == str(
        Decimal(40000) + 100 * (Decimal(mark) - 400))

    position(conn, "P2")                     # a resting entry filled after the breach
    await risk.tick()
    assert flattens == ["equity_floor"] * 2


async def test_no_snapshot_or_halt_while_a_held_paper_symbol_is_unmarked(conn, make_risk, marks) -> None:
    conn.execute("UPDATE paper_equity_snapshots SET equity = '44000'")   # the baseline carried TCS at 440
    position(conn)
    risk = make_risk()

    await risk.tick()
    assert halts(conn) == {} and conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0] == 1
    marks["TCS"] = Decimal("440")
    await risk.tick()
    assert halts(conn) == {} and conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0] == 2


async def test_a_real_position_never_moves_paper_equity(conn, make_risk, marks) -> None:
    position(conn, paper=False)
    marks["TCS"] = Decimal("300")
    await make_risk().tick()
    assert halts(conn) == {}


async def test_a_raising_paper_tracker_still_lets_the_real_apply_run(
    conn, clock, limits, make_risk, monkeypatch
) -> None:
    risk = make_risk()

    def broken() -> None:
        raise RuntimeError("paper tracker broke")

    monkeypatch.setattr(risk.tracker, "persist_snapshot", broken)
    await risk.tick()

    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin, opened_at, "
        "closed_at, realized_pnl, costs) VALUES ('R1', 'INFY', 'BUY', 'CNC', 1, '100', 'CLOSED', 'recommended', ?, ?, "
        "'-5000', '0')",
        (at(9, 30).isoformat(), at(9, 45).isoformat()),
    )
    real = ExposureTracker(conn, clock, Decimal("40000"))
    mode, kill = ModeManager(conn, clock, paper_only=False), KillSwitch(conn, clock)
    latch = RiskStateLatch(conn, clock, mode)
    await real.apply_floor_breaches(real.evaluate_floors(floor_limits_from(limits.load())), mode, kill, latch=latch)
    assert mode.risk_state() is R.CLOSE_ONLY and not conn.in_transaction


def test_paper_risk_is_wired_under_the_flag_and_never_inside_equity_tick() -> None:
    # ``_compose_paper`` runs only under the flag (test_ops_main_wiring pins that).
    compose = ast.parse(inspect.getsource(opsmain._compose_paper))
    calls = {ast.unparse(n.func): n for n in ast.walk(compose) if isinstance(n, ast.Call)}
    guard = next(k.value for k in calls["PaperRuntime"].keywords if k.arg == "order_guard")
    assert ast.unparse(guard) == "paper_order_guard(control.enabled, paper_effective_state(conn, real_risk_state))"
    flatten = next(k.value for k in calls["PaperRisk"].keywords if k.arg == "flatten")
    assert ast.unparse(flatten) == "exits.flatten_all"
    tree = ast.parse(inspect.getsource(opsmain.run))
    assert "real_risk_state=mode.risk_state" in ast.unparse(tree)
    equity_tick = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "equity_tick")
    assert "paper" not in ast.unparse(equity_tick).lower()


# ---------------------------------------------------------------- clearing and reset
async def test_daily_halts_auto_clear_the_next_session_and_the_others_latch(conn, now, make_risk, marks) -> None:
    position(conn)
    marks["TCS"] = Decimal("368")            # -8% on the day and the week, above the -10% rung
    risk = make_risk()
    await risk.tick()
    assert halts(conn) == {k: ELEVEN_PCT_DOWN[k] for k in ("daily_loss_soft", "daily_loss_hard",
                                                           "floor_weekly_drawdown")}

    marks["TCS"] = Decimal("400")
    snapshots = conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0]
    now.value = at(16, 0)
    await risk.tick()
    assert set(halts(conn)) == {"daily_loss_soft", "daily_loss_hard", "floor_weekly_drawdown"}
    assert conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0] == snapshots

    now.value = at(9, 20, NEXT)
    await make_risk().tick()
    assert halts(conn) == {"floor_weekly_drawdown": ("CLOSE_ONLY", 1)}


def working_order(conn, order_id: str, state: str = "ACKED") -> None:
    conn.execute("INSERT OR REPLACE INTO orders (order_id, role, is_paper, state, product, side, qty, created_at, "
                 "updated_at) VALUES (?, 'entry', 1, ?, 'CNC', 'BUY', 5, ?, ?)",
                 (order_id, state, *(at(9, 50).isoformat(),) * 2))


async def test_a_reset_after_a_floor_breach_exits_the_book_and_opens_the_epoch_once_flat_across_a_restart(
    conn, clock, now, make_risk, marks, flattens, broker
) -> None:
    conn.execute("UPDATE paper_equity_snapshots SET equity = '44000'")   # the old epoch's day baseline
    position(conn)
    marks["TCS"] = Decimal("355")
    await make_risk().tick()
    PaperControl(conn, clock).request_reset("owner")

    now.value = at(10, 1)
    risk = make_risk()                       # a restart: the request and the halts are on disk
    await risk.tick()
    assert flattens == ["equity_floor"] * 2 and halts(conn)["reset_pending"] == ("CLOSE_ONLY", 1)

    def epoch() -> str | None:
        return conn.execute("SELECT epoch_started_at FROM paper_state").fetchone()[0]

    now.value = at(10, 2)
    await risk.tick()                        # still held
    assert epoch() is None

    working_order(conn, "O1")
    conn.execute("UPDATE positions SET state = 'CLOSED', closed_at = ?, realized_pnl = '-4500', costs = '50'",
                 (at(10, 3).isoformat(),))
    now.value = at(10, 4)
    await risk.tick()                        # flat, but an entry is still working
    assert epoch() is None

    working_order(conn, "O1", "CANCELLED")
    now.value = at(10, 5)
    await risk.tick()
    state = conn.execute("SELECT epoch_started_at, reset_requested_at FROM paper_state").fetchone()
    assert tuple(state) == (at(10, 5).isoformat(), None)
    assert halts(conn) == {} and paper_effective_state(conn, lambda: R.NORMAL)() is R.NORMAL
    assert (await broker.margins())["equity"]["available"]["cash"] == Decimal("40000")

    for wall in (at(10, 6), at(9, 20, NEXT)):
        now.value = wall
        await make_risk().tick()
    assert halts(conn) == {} and make_risk().tracker.equity() == Decimal("40000")
    assert flattens == ["equity_floor"] * 2


async def test_a_reset_on_a_flat_book_opens_the_epoch_without_a_halt(conn, clock, make_risk, flattens) -> None:
    PaperControl(conn, clock).request_reset("owner")
    await make_risk().tick()
    assert conn.execute("SELECT epoch_started_at FROM paper_state").fetchone()[0] == at(10, 0).isoformat()
    assert conn.execute("SELECT COUNT(*) FROM paper_halts").fetchone()[0] == 0 and flattens == []


# ---------------------------------------------------------------- effective state
@pytest.mark.parametrize(("real", "rungs", "expected"), [
    (R.NORMAL, [], R.NORMAL),
    (R.FROZEN, [], R.FROZEN),
    (R.NORMAL, ["FROZEN"], R.FROZEN),
    (R.CLOSE_ONLY, ["FROZEN"], R.CLOSE_ONLY),
    (R.FROZEN, ["KILLED", "CLOSE_ONLY"], R.KILLED),
    (R.NORMAL, ["HALTED?"], R.KILLED),
])
def test_the_effective_paper_state_is_the_worse_of_real_and_paper_halts(conn, real, rungs, expected) -> None:
    for i, rung in enumerate(rungs):
        conn.execute("INSERT INTO paper_halts (cause, rung, set_at) VALUES (?, ?, ?)", (f"c{i}", rung, "x"))
    conn.execute("INSERT INTO paper_halts (cause, rung, set_at, cleared_at) VALUES ('old', 'KILLED', 'x', 'y')")
    assert paper_effective_state(conn, lambda: real)() is expected
    assert set(active_halts(conn)) == {f"c{i}" for i in range(len(rungs))}


def test_paper_risk_takes_a_paper_broker_only(conn, clock, limits) -> None:
    with pytest.raises(TypeError, match="PaperBroker only"):
        PaperRisk(conn, clock, lambda _d: None, object(), capital_base=Decimal("40000"),  # type: ignore[arg-type]
                  mark_price=lambda _s: None, limits_fn=limits.load, flatten=lambda _b: None)  # type: ignore[arg-type]
