"""Scope isolation (plan Q3.2, D1): paper rows move no real number, real rows move no paper number,
and the scope predicate has one source (``engine.core.scope``)."""

from __future__ import annotations

import ast
import inspect
import json
import re
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from engine.core.config import repo_root
from engine.core.enums import Mode, RiskState
from engine.core.scope import scope_sql
from engine.ops import main as opsmain
from engine.ops.holdings_reconcile import _TRACKED_SQL
from engine.ops.lifecycle import SessionLifecycle
from engine.ops.nightly_review import _closed_trades, _proposal_verdict_counts, _verdict_lines
from engine.ops.pipeline import RecommendationBook
from engine.risk.exposure import ExposureTracker, FloorLimits
from engine.risk.gate import GateContextBuilder
from engine.strategy.cost_model import CostModel
from tests.unit.test_reco_pipeline import LEDGER_FIELDS, NOW, SYMBOL, make_rec

TODAY = NOW.date()
CAPITAL = Decimal("20000")
MARKS = {SYMBOL: Decimal("110"), "PAPPART": Decimal("104"), "PAPEXIT": Decimal("90")}


def _at(hhmm: str, days: int = 0) -> str:
    return f"{(TODAY + timedelta(days=days)).isoformat()}T{hhmm}:00+05:30"


def _insert(conn, table: str, **row) -> None:
    conn.execute(
        f"INSERT INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})", tuple(row.values())
    )


def _position(conn, pid, symbol, *, is_paper, state="OPEN", origin=None, qty=10, opened=None,
              **cols) -> None:
    _insert(conn, "positions", position_id=pid, symbol=symbol, side="BUY", product="CNC", qty=qty,
            avg_entry="100", state=state, is_paper=is_paper,
            origin=origin or ("platform" if is_paper else "recommended"), opened_at=opened or _at("09:30"),
            **cols)


def _rec(conn, rec_id, kind, symbol) -> None:
    payload = {"kind": kind, "instrument": symbol, "created_at": NOW.isoformat(),
               "valid_until": (NOW + timedelta(hours=1)).isoformat()}
    _insert(conn, "recommendations", rec_id=rec_id, payload=json.dumps(payload), delivered_at=NOW.isoformat())


# ------------------------------------------------------------------ the three seeders
def seed_shared(conn) -> None:
    """Proposals are unscoped: real and paper both judge the same one (Q4.9)."""
    for pid, symbol in (("pr-real", SYMBOL), ("pr-part", "PAPPART"), ("pr-pend", "PAPPEND"),
                        ("pr-dead", "PAPDEAD")):
        _insert(conn, "proposals", proposal_id=pid, agent_id="a", action="enter", inputs_digest="d",
                payload=json.dumps({"action": "enter", "tradingsymbol": symbol}), created_at=_at("09:20"))


def seed_real(conn) -> None:
    _position(conn, "r-open", SYMBOL, is_paper=0)
    _position(conn, "r-ext", "EXTCO", is_paper=0, origin="external")
    _position(conn, "r-closed", "REALDONE", is_paper=0, state="CLOSED", closed_at=_at("10:00"),
              realized_pnl="200", costs="10")
    _insert(conn, "learning_ledger", entry_id="r-win", rec_id="rec-done", outcome_label="win",
            closed_at=_at("10:02"))
    _rec(conn, "rec-pend", "entry", "REALPEND")
    _rec(conn, "rec-exit", "exit", SYMBOL)                    # a real exit rec on a paper-held symbol
    for pid in ("pr-real", "pr-part", "pr-pend", "pr-dead"):
        _insert(conn, "verdicts", verdict_id=f"v-{pid}", proposal_id=pid, verdict="reject", payload="{}",
                evaluated_at=_at("09:21"))
    _insert(conn, "equity_snapshots", at=_at("15:29", -1), equity="20100")


def seed_paper(conn) -> None:
    """One row of every paper kind, including a partially filled entry and pre-epoch history."""
    _insert(conn, "paper_state", id=1, epoch_started_at=_at("09:00"))
    _position(conn, "p-open", SYMBOL, is_paper=1)
    _position(conn, "p-part", "PAPPART", is_paper=1, qty=5)
    _position(conn, "p-exit", "PAPEXIT", is_paper=1, state="PENDING_EXIT")
    _position(conn, "p-closed", "PAPDONE", is_paper=1, state="CLOSED", closed_at=_at("10:01"),
              realized_pnl="-300", costs="20")
    _position(conn, "p-old", "PAPOLD", is_paper=1, state="CLOSED", opened=_at("10:00", -1),
              closed_at=_at("11:00", -1), realized_pnl="-1000", costs="0")
    for oid, pid, role, state, filled, pos in (
        ("o-part", "pr-part", "entry", "PARTIALLY_FILLED", 5, "p-part"),
        ("o-pend", "pr-pend", "entry", "ACKED", 0, None),
        ("o-dead", "pr-dead", "entry", "CANCELLED", 0, None),
        ("o-leg", None, "gtt_leg", "ACKED", 0, "p-exit"),
    ):
        _insert(conn, "orders", order_id=oid, proposal_id=pid, position_id=pos, role=role, is_paper=1,
                state=state, product="CNC", side="BUY", qty=10, filled_qty=filled, created_at=_at("09:40"))
    _insert(conn, "gtts", gtt_id=900001, position_id="p-open", state="ACTIVE", is_paper=1, symbol=SYMBOL)
    _insert(conn, "gtts", gtt_id=900002, state="active", is_paper=1, symbol="PAPGTT")
    _insert(conn, "learning_ledger", entry_id="p-loss", is_paper=1, outcome_label="loss",
            closed_at=_at("10:01"))
    _insert(conn, "learning_ledger", entry_id="p-void", is_paper=1, outcome_label="void",
            closed_at=_at("10:03"))
    _insert(conn, "verdicts", verdict_id="pv-part", proposal_id="pr-part", verdict="approve",
            payload="{}", evaluated_at=_at("09:22"), is_paper=1)
    _insert(conn, "paper_equity_snapshots", at=_at("09:15"), equity="19000")
    _insert(conn, "paper_equity_snapshots", at=_at("15:29", -1), equity="25000")    # pre-epoch
    _insert(conn, "paper_halts", cause="floor_equity_floor_rung", rung="CLOSE_ONLY", set_at=_at("09:16"),
            latched=1)


# ------------------------------------------------------------------ readers under test
def _builder(conn, clock, exposure, scope="real") -> GateContextBuilder:
    async def arun(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    store = SimpleNamespace(
        arun=arun, get_universe_daily=lambda d: [], get_instruments_daily=lambda d: [],
        get_earnings_calendar=lambda a, b, symbol=None: [], get_bars_1d_frame=lambda *a: None,
        get_sector_map=lambda as_of: [{"symbol": SYMBOL, "sector": "ENERGY"}],
    )
    mode = SimpleNamespace(mode=lambda: Mode.RECOMMEND, risk_state=lambda: RiskState.NORMAL,
                           get_trade_window=lambda: None)
    return GateContextBuilder(
        SimpleNamespace(load=lambda: SimpleNamespace(capital_base_inr=CAPITAL)), exposure,
        SimpleNamespace(is_fno=lambda s: False), store, SimpleNamespace(session=lambda d: None), clock,
        mode, SimpleNamespace(is_killed=lambda: False),
        margins_fn=lambda: Decimal("50000"), missing_holdings_fn=lambda d: set(), scope=scope,
    )


async def _outputs(conn, clock, scope) -> dict:
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get, scope=scope)
    out = {
        "ctx": await _builder(conn, clock, tracker, scope).build(SYMBOL, "BUY", "swing", TODAY),
        "tracker": (tracker.realized_net(), tracker.realized_net_closed_on(TODAY),
                    tracker.trades_opened_today(), tracker.weekly_drawdown_peak(),
                    tracker.evaluate_floors(FloorLimits()), tracker.per_symbol_open(SYMBOL)),
    }
    if scope == "real":
        held = set(opsmain._held_symbols(conn))
        out |= {
            "held": held,
            "retest": opsmain._retest_skip_reasons(
                [SYMBOL, "PAPPART", "PAPPEND"], eligible={SYMBOL, "PAPPART", "PAPPEND"}, ex_skip=(),
                held=held, pending=opsmain._pending_entry_rec_symbols(conn, NOW)),
            "tracked": [tuple(r) for r in conn.execute(_TRACKED_SQL)],
            "lifecycle": SessionLifecycle._open_positions_count(SimpleNamespace(_conn=conn)),
            "nightly": ([tuple(r) for r in _closed_trades(conn, TODAY)], _verdict_lines(conn, TODAY),
                        _proposal_verdict_counts(conn, TODAY.isoformat())),
        }
    return out


# ------------------------------------------------------------------ both directions
async def test_paper_rows_change_no_real_output(conn, clock) -> None:
    seed_shared(conn)
    seed_real(conn)
    before = await _outputs(conn, clock, "real")
    seed_paper(conn)
    assert await _outputs(conn, clock, "real") == before

    ctx = before["ctx"]
    assert ctx.open_symbols == {SYMBOL} and ctx.pending_rec_symbols == {"REALPEND"}
    assert ctx.exiting_symbols == {SYMBOL} and ctx.risk_state is RiskState.NORMAL
    assert ctx.known_order_ids == frozenset() and ctx.available_margin == Decimal("50000")
    assert before["held"] == {SYMBOL} and before["lifecycle"] == 1


async def test_working_paper_entries_commit_paper_capital_only(conn, clock) -> None:
    seed_shared(conn)
    seed_paper(conn)
    tracker = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get, scope="paper")
    base = (await _builder(conn, clock, tracker, "paper").build(SYMBOL, "BUY", "swing", TODAY)).deployed_capital
    conn.execute("UPDATE orders SET price='1200' WHERE order_id IN ('o-part', 'o-pend', 'o-dead')")
    ctx = await _builder(conn, clock, tracker, "paper").build(SYMBOL, "BUY", "swing", TODAY)
    assert ctx.deployed_capital - base == Decimal(5 * 1200 + 10 * 1200)    # remainders; CANCELLED excluded
    real = ExposureTracker(conn, clock, CAPITAL, mark_price=MARKS.get, scope="real")
    assert (await _builder(conn, clock, real).build(SYMBOL, "BUY", "swing", TODAY)).deployed_capital == 0


async def test_real_rows_change_no_paper_output(conn, clock) -> None:
    seed_shared(conn)
    seed_paper(conn)
    before = await _outputs(conn, clock, "paper")
    seed_real(conn)
    assert await _outputs(conn, clock, "paper") == before

    ctx = before["ctx"]
    assert ctx.open_symbols == {SYMBOL, "PAPPART", "PAPEXIT"}
    assert ctx.exiting_symbols == {"PAPEXIT"}                  # PENDING_EXIT, never the real exit rec
    assert (ctx.open_total, ctx.open_cnc) == (2, 2)
    assert ctx.pending_rec_symbols == {"PAPPEND"}             # working, nothing filled
    assert ctx.entry_recs_today == 3                          # every paper entry order submitted today
    assert ctx.known_order_ids == {"o-part", "o-pend", "o-dead", "o-leg"}
    assert ctx.protective_order_ids == {"o-leg"}
    assert ctx.risk_state is RiskState.CLOSE_ONLY             # worse of real NORMAL and the paper halt
    assert ctx.available_margin is None                        # capital_cap binds, not margins
    assert ctx.consecutive_losses == 1                          # the void neither counts nor breaks it
    assert ctx.deployed_capital == Decimal("2500")             # (10 + 5 + 10) x 100
    # Epoch: the pre-epoch close and snapshot are history.
    assert before["tracker"][:4] == (Decimal("-320"), Decimal("-320"), 4, Decimal("19000"))


@pytest.mark.parametrize("rung", ["KILLED", "not-a-state"])
async def test_paper_risk_state_takes_the_worst_halt_and_fails_closed(conn, clock, rung) -> None:
    _insert(conn, "paper_halts", cause="x", rung=rung, set_at=_at("09:16"))
    _insert(conn, "paper_halts", cause="cleared", rung="KILLED", set_at=_at("09:00"), cleared_at=_at("09:05"))
    tracker = ExposureTracker(conn, clock, CAPITAL, scope="paper")
    ctx = await _builder(conn, clock, tracker, "paper").build(SYMBOL, "BUY", "swing", TODAY)
    assert ctx.risk_state is RiskState.KILLED


async def test_the_paper_tracker_never_applies_a_halt(conn, clock) -> None:
    tracker = ExposureTracker(conn, clock, CAPITAL, scope="paper")
    with pytest.raises(RuntimeError):
        await tracker.apply_day_loss(["daily_loss_hard"], None, None)
    with pytest.raises(RuntimeError):
        await tracker.apply_floor_breaches([], None, None)
    tracker.persist_snapshot()
    assert conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM paper_equity_snapshots").fetchone()[0] == 1


def test_a_builder_refuses_a_tracker_of_the_other_scope(conn, clock) -> None:
    with pytest.raises(ValueError):
        _builder(conn, clock, ExposureTracker(conn, clock, CAPITAL), scope="paper")
    with pytest.raises(ValueError):
        _builder(conn, clock, ExposureTracker(conn, clock, CAPITAL, scope="paper"))


async def test_owner_commands_and_expiry_leave_paper_rows_byte_identical(conn, clock) -> None:
    seed_shared(conn)
    seed_paper(conn)

    def paper_rows() -> list:
        return [tuple(r) for t in ("positions", "orders", "gtts", "learning_ledger", "verdicts")
                for r in conn.execute(f"SELECT * FROM {t} WHERE {scope_sql('paper')} ORDER BY 1")]

    before = paper_rows()
    cost_model = CostModel.from_config()
    book = RecommendationBook(conn, clock, cost_model)
    taken, vetoed, stale = (make_rec(cost_model) for _ in range(3))        # all on the paper-held SYMBOL
    for rec in (taken, vetoed, stale):
        book.deliver(rec, ledger_fields=dict(LEDGER_FIELDS))
    await book.take(taken.rec_id, 10, Decimal("100"))
    await book.close(taken.rec_id, Decimal("101"))
    await book.veto(vetoed.rec_id)
    assert book.expire_stale(NOW + timedelta(hours=6)) == [stale.rec_id]
    assert paper_rows() == before


def test_every_paper_position_working_order_and_active_gtt_is_in_the_feed(conn) -> None:
    seed_shared(conn)
    seed_real(conn)
    seed_paper(conn)
    tokens = {s: i for i, s in enumerate(
        [SYMBOL, "PAPPART", "PAPEXIT", "PAPPEND", "PAPGTT", "PAPDEAD", "PAPDONE", "NIFTY 50", "INDIA VIX"])}
    subscribed = opsmain._ticker_tokens(
        watchlist=[], held=opsmain._feed_symbols(conn), batch_state={"day": None, "symbols": set()},
        today=TODAY, token_for_symbol=tokens.get)
    names = {s for s, t in tokens.items() if t in subscribed}
    assert {SYMBOL, "PAPPART", "PAPEXIT", "PAPPEND", "PAPGTT"} <= names
    assert not names & {"PAPDEAD", "PAPDONE"}                  # terminal order, closed position
    assert opsmain._held_symbols(conn) == [SYMBOL]


def test_the_retest_and_the_sweep_read_real_held_symbols_and_only_the_ticker_reads_the_feed() -> None:
    src = inspect.getsource(opsmain.run)
    assert "return _held_symbols(conn)" in src
    assert "held=set(held_symbols())," in src and "held = set(held_symbols())" in src
    assert src.count("_feed_symbols(") == 1


# ------------------------------------------------------------------ scope_sql
_KEYS = {"positions": "position_id", "orders": "order_id", "learning_ledger": "entry_id",
         "verdicts": "verdict_id", "gtts": "gtt_id"}
_REQUIRED = {
    "positions": {"symbol": "X"}, "orders": {"role": "entry", "state": "ACKED", "product": "CNC"},
    "learning_ledger": {}, "gtts": {},
    "verdicts": {"proposal_id": "p", "verdict": "approve", "payload": "{}", "evaluated_at": _at("09:00")},
}
#: key -> (is_paper or None for a legacy insert that omits the column, origin)
_ROWS = {"1": (0, "recommended"), "2": (None, "platform"), "3": (0, "external"), "4": (1, "platform"),
         "5": (1, "recommended")}


@pytest.mark.parametrize("alias", [None, "t"])
@pytest.mark.parametrize(("table", "scope", "has_origin", "expected"), [
    ("positions", "real", False, {"1", "2", "3"}),
    ("positions", "real", True, {"1", "2"}),
    ("positions", "paper", False, {"4", "5"}),
    ("positions", "paper", True, {"4"}),
    *[(t, s, False, e) for t in ("orders", "learning_ledger", "verdicts", "gtts")
      for s, e in (("real", {"1", "2", "3"}), ("paper", {"4", "5"}))],
])
def test_scope_sql_selects_exactly_its_rows(conn, table, scope, has_origin, expected, alias) -> None:
    _insert(conn, "proposals", proposal_id="p", agent_id="a", action="enter", payload="{}",
            inputs_digest="d", created_at=_at("09:00"))
    for key, (is_paper, origin) in _ROWS.items():
        row = {_KEYS[table]: key, **_REQUIRED[table]}
        if table == "positions":
            row["origin"] = origin
        if is_paper is not None:
            row["is_paper"] = is_paper
        _insert(conn, table, **row)
    sql = f"SELECT {_KEYS[table]} FROM {table} {alias or ''} WHERE {scope_sql(scope, alias, has_origin=has_origin)}"
    assert {str(r[0]) for r in conn.execute(sql)} == expected


def test_scope_sql_refuses_an_unknown_scope() -> None:
    with pytest.raises(ValueError):
        scope_sql("both")  # type: ignore[arg-type]


# ------------------------------------------------------------------ the literal ban
_ORIGIN_LIST = re.compile(r"""\(\s*['"]platform['"]\s*,\s*['"]recommended['"]\s*\)""")


def _literal_lines(source: str) -> list[int]:
    """Lines of CODE strings spelling the origin list; docstrings and comments are prose."""
    tree = ast.parse(source)
    docstrings = {
        id(node.body[0].value) for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.body and isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant)
    }
    return [n.lineno for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings
            and _ORIGIN_LIST.search(n.value)]


def test_the_origin_list_is_spelled_only_in_core_scope() -> None:
    root = repo_root() / "src" / "engine"
    home = root / "core" / "scope.py"
    offenders = [f"{p.relative_to(root)}:{line}" for p in sorted(root.rglob("*.py")) if p != home
                 for line in _literal_lines(p.read_text(encoding="utf-8-sig"))]
    assert offenders == []
    assert _literal_lines(home.read_text(encoding="utf-8"))   # the guard sees the one real spelling


@pytest.mark.parametrize(("source", "caught"), [
    ("x = \"origin IN ('platform','recommended')\"\n", True),
    ("x = f\"origin IN ( 'platform' , \\\"recommended\\\" ) {y}\"\n", True),
    ("x = ('a'\n     \"('platform',\"\n     \"'recommended')\")\n", True),
    ("def f():\n    \"\"\"origin IN ('platform','recommended')\"\"\"\n", False),
    ("# origin IN ('platform','recommended')\nx = 1\n", False),
])
def test_the_literal_guard_reads_code_not_prose(source, caught) -> None:
    assert bool(_literal_lines(source)) is caught
