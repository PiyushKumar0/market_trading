"""Phase-2 dashboard API routes (§3.2.11 C3): real reads/writes over risk/oms/learning/intelligence
state via the app's OPTIONAL collaborators (``conn``/``store``/``exposure``/``governor``/
``limits_engine``), with every route degrading to its Phase-0/1 stub shape when its own collaborator is
unwired. Mirrors ``test_api_auth.py``'s ``TestClient`` pattern; inserts rows directly into ``conn``
(migrated by the shared ``conn`` fixture, tests/conftest.py) rather than going through write paths that
don't exist yet."""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from engine.api.app import create_app
from engine.core.calendar import NSECalendar
from engine.core.config import config_dir, load_yaml
from engine.core.enums import Actor, Mode
from engine.core.protected_store import ProtectedStore
from engine.core.secrets import DASHBOARD_TOKEN
from engine.core.types import OwnerConfirmation
from engine.intelligence.governor import BudgetGovernor, TokenUsage
from engine.risk.events import TOPIC_MODE_CHANGED, ModeChanged
from engine.risk.exposure import ExposureTracker
from engine.risk.kill import KillSwitch
from engine.risk.limits import LimitsEngine
from engine.risk.mode import ModeManager

_TOKEN = "s3cret-dash-token"
AUTH = {"Authorization": f"Bearer {_TOKEN}"}
OWNER_OK = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note="test-registration")
REAL_CONFIG = Path(__file__).resolve().parents[2] / "config"


class _FakeSecrets:
    def __init__(self, token: str | None) -> None:
        self._token = token

    def get(self, key: str) -> str:
        if key == DASHBOARD_TOKEN and self._token is not None:
            return self._token
        raise KeyError(key)

    def has(self, key: str) -> bool:
        return key == DASHBOARD_TOKEN and self._token is not None


def _client(**collaborators) -> TestClient:
    return TestClient(create_app(secrets=_FakeSecrets(_TOKEN), **collaborators))


# --------------------------------------------------------------------------- collaborator fixtures
@pytest.fixture
def store(tmp_path, conn, clock) -> ProtectedStore:
    """A ProtectedStore over a config/ dir seeded with the REAL limits.yaml + envelope.yaml, both
    registered (mirrors ``test_limits_engine.py``'s ``store``/``registered_store`` pattern)."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "limits.yaml").write_bytes((REAL_CONFIG / "limits.yaml").read_bytes())
    (cfg / "envelope.yaml").write_bytes((REAL_CONFIG / "envelope.yaml").read_bytes())
    s = ProtectedStore(cfg, conn, clock)
    s.register_initial("limits.yaml", OWNER_OK)
    s.register_initial("envelope.yaml", OWNER_OK)
    return s


@pytest.fixture
def limits_engine(store) -> LimitsEngine:
    return LimitsEngine(store)


@pytest.fixture
def calendar(clock) -> NSECalendar:
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def exposure(conn, clock) -> ExposureTracker:
    return ExposureTracker(conn, clock, Decimal("20000"))


@pytest.fixture
def governor(conn, clock, calendar, bus) -> BudgetGovernor:
    return BudgetGovernor(conn, clock, calendar, load_yaml(config_dir() / "agents.yaml"), bus=bus)


@pytest.fixture
def mode_manager(conn, clock, bus, calendar) -> ModeManager:
    return ModeManager(conn, clock, bus, calendar)


@pytest.fixture
def kill_switch(conn, clock, bus) -> KillSwitch:
    return KillSwitch(conn, clock, bus)


# --------------------------------------------------------------------------- row-insertion helpers
def _insert_proposal(
    conn, proposal_id, *, agent_id="orb_scanner", action="enter", payload=None,
    created_at="2026-06-17T09:20:00+05:30",
) -> None:
    conn.execute(
        "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (proposal_id, agent_id, action, json.dumps(payload or {"action": action, "tradingsymbol": "AAA"}),
         "digest-abc", created_at),
    )


def _insert_verdict(
    conn, verdict_id, proposal_id, *, verdict="approve", payload=None,
    evaluated_at="2026-06-17T09:20:05+05:30",
) -> None:
    conn.execute(
        "INSERT INTO verdicts (verdict_id, proposal_id, verdict, payload, evaluated_at) VALUES (?, ?, ?, ?, ?)",
        (verdict_id, proposal_id, verdict, json.dumps(payload or {"reasons": ["edge_multiple_ok"]}), evaluated_at),
    )


def _insert_position(
    conn, position_id, *, symbol="AAA", side="BUY", product="MIS", qty=10, avg_entry="100",
    state="OPEN", origin="platform", opened_at="2026-06-17T09:30:00+05:30", closed_at=None,
    realized_pnl=None, costs=None,
) -> None:
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, product, qty, avg_entry, state, origin, "
        "opened_at, closed_at, realized_pnl, costs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (position_id, symbol, side, product, qty, avg_entry, state, origin, opened_at, closed_at,
         realized_pnl, costs),
    )


def _insert_order(
    conn, order_id, *, role="entry", state="COMPLETE", product="MIS", created_at="2026-06-17T09:30:00+05:30",
    position_id=None,
) -> None:
    conn.execute(
        "INSERT INTO orders (order_id, role, state, product, created_at, position_id) VALUES (?, ?, ?, ?, ?, ?)",
        (order_id, role, state, product, created_at, position_id),
    )


def _insert_config_audit(conn, name, diff, *, actor="owner", at="2026-06-17T08:00:00+05:30") -> None:
    conn.execute(
        "INSERT INTO config_audit (name, diff, actor, at) VALUES (?, ?, ?, ?)",
        (name, json.dumps(diff), actor, at),
    )


def _insert_nightly_review(conn, d, payload, *, created_at="2026-06-16T21:30:00+05:30") -> None:
    conn.execute(
        "INSERT INTO nightly_reviews (d, payload, created_at) VALUES (?, ?, ?)",
        (d, json.dumps(payload), created_at),
    )


def _insert_param_set(conn, param_set_id, status, *, strategy_id="orb") -> None:
    conn.execute(
        "INSERT INTO param_sets (param_set_id, strategy_id, params, status) VALUES (?, ?, ?, ?)",
        (param_set_id, strategy_id, "{}", status),
    )


# --------------------------------------------------------------------------- unwired => stub shapes
def test_unwired_routes_return_stub_shapes() -> None:
    client = _client()  # no conn/store/exposure/governor/limits_engine/mode_manager/kill_switch

    assert client.get("/positions", headers=AUTH).json() == {"positions": [], "as_of": None}
    assert client.get("/orders", headers=AUTH).json() == {"orders": []}
    assert client.get("/decisions", headers=AUTH).json() == {"decisions": []}
    assert client.get("/verdicts", headers=AUTH).json() == {"verdicts": []}
    assert client.get("/risk/headroom", headers=AUTH).json() == {"headroom": {}}
    assert client.get("/budget", headers=AUTH).json() == {"budget": {}, "degrade_tier": None}
    assert client.get("/learning/status", headers=AUTH).json() == {"learning": {}}
    assert client.get("/config/audit", headers=AUTH).json() == {"config_audit": []}
    assert client.get("/config/params", headers=AUTH).json() == {"params": {}, "suggestions": []}

    params_post = client.post("/config/params", headers=AUTH, json={"name": "orb.vol_mult", "value": 1.5})
    assert params_post.status_code == 200
    assert params_post.json() == {"ok": True, "applied": False, "note": "phase-0 stub — param store lands later"}

    mode_post = client.post("/mode", headers=AUTH, json={"mode": "RECOMMEND"})
    assert mode_post.status_code == 501

    kill_post = client.post("/kill", headers=AUTH, json={})
    assert kill_post.status_code == 501


def test_analyst_confidence_min_and_auto_and_kill_reset_are_409_regardless_of_wiring() -> None:
    # These are business rules, not wiring gaps — 409 even with nothing wired.
    client = _client()
    assert client.post("/config/params", headers=AUTH, json={"name": "analyst_confidence_min", "value": 0.6}).status_code == 409
    assert client.post("/mode", headers=AUTH, json={"mode": "AUTO"}).status_code == 409
    assert client.post("/kill/reset", headers=AUTH).status_code == 409


# --------------------------------------------------------------------------- positions / orders
def test_positions_open_first_with_as_of(conn, clock) -> None:
    _insert_position(conn, "p-closed", state="CLOSED", opened_at="2026-06-17T09:00:00+05:30",
                      closed_at="2026-06-17T09:10:00+05:30")
    _insert_position(conn, "p-open", state="OPEN", opened_at="2026-06-17T09:20:00+05:30")
    client = _client(conn=conn, clock=clock)

    r = client.get("/positions", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert [p["position_id"] for p in body["positions"]] == ["p-open", "p-closed"]
    assert body["as_of"] == clock.now().isoformat()


def test_orders_latest_100_first(conn, clock) -> None:
    _insert_order(conn, "o-1", created_at="2026-06-17T09:00:00+05:30")
    _insert_order(conn, "o-2", created_at="2026-06-17T09:30:00+05:30")
    client = _client(conn=conn, clock=clock)

    r = client.get("/orders", headers=AUTH)
    assert r.status_code == 200
    assert [o["order_id"] for o in r.json()["orders"]] == ["o-2", "o-1"]


# --------------------------------------------------------------------------- decisions / verdicts
def test_decisions_left_join_verdict_and_verdicts_route(conn, clock) -> None:
    _insert_proposal(conn, "prop-1", created_at="2026-06-17T09:20:00+05:30")
    _insert_verdict(conn, "verd-1", "prop-1", verdict="shrink", payload={"reasons": ["per_trade_risk_shrink"]})
    _insert_proposal(conn, "prop-2", created_at="2026-06-17T09:25:00+05:30")  # no verdict yet
    client = _client(conn=conn, clock=clock)

    r = client.get("/decisions", headers=AUTH)
    assert r.status_code == 200
    decisions = r.json()["decisions"]
    assert [d["proposal_id"] for d in decisions] == ["prop-2", "prop-1"]

    judged = next(d for d in decisions if d["proposal_id"] == "prop-1")
    assert judged["verdict"] == "shrink"
    assert judged["reasons"] == ["per_trade_risk_shrink"]
    assert judged["proposal"]["action"] == "enter"

    pending = next(d for d in decisions if d["proposal_id"] == "prop-2")
    assert pending["verdict"] is None
    assert pending["reasons"] == []
    assert pending["verdict_id"] is None

    r2 = client.get("/verdicts", headers=AUTH)
    assert r2.status_code == 200
    verdicts = r2.json()["verdicts"]
    assert len(verdicts) == 1
    assert verdicts[0]["verdict_id"] == "verd-1"
    assert verdicts[0]["payload"]["reasons"] == ["per_trade_risk_shrink"]


def test_decisions_subject_resolves_position_and_order_ids_to_symbols(conn, clock) -> None:
    """Owner-reported 2026-09-02: the decision log printed exit proposals as their position ULID. The
    proposal payload only carries the id (contracts: exit/modify-* → position_id, cancel → order_id), so
    the route resolves each to the position's symbol; an id with no row falls back to the id itself."""
    _insert_position(conn, "pos-1", symbol="HDFCAMC")
    _insert_order(conn, "o-1", role="protective_sl", position_id="pos-1")
    _insert_order(conn, "o-orphan", role="entry")  # no position_id → cannot resolve
    _insert_proposal(conn, "p-enter", action="enter", created_at="2026-06-17T09:20:00+05:30")
    _insert_proposal(conn, "p-exit", action="exit", created_at="2026-06-17T09:21:00+05:30",
                     payload={"action": "exit", "position_id": "pos-1", "exit_type": "MARKET"})
    _insert_proposal(conn, "p-mod", action="modify-stop", created_at="2026-06-17T09:22:00+05:30",
                     payload={"action": "modify-stop", "position_id": "pos-ghost", "new_stop": "100"})
    _insert_proposal(conn, "p-cancel", action="cancel", created_at="2026-06-17T09:23:00+05:30",
                     payload={"action": "cancel", "order_id": "o-1"})
    _insert_proposal(conn, "p-cancel-orphan", action="cancel", created_at="2026-06-17T09:24:00+05:30",
                     payload={"action": "cancel", "order_id": "o-orphan"})
    _insert_proposal(conn, "p-cancel-ghost", action="cancel", created_at="2026-06-17T09:25:00+05:30",
                     payload={"action": "cancel", "order_id": "o-ghost"})
    client = _client(conn=conn, clock=clock)

    subjects = {d["proposal_id"]: d["subject"] for d in client.get("/decisions", headers=AUTH).json()["decisions"]}
    assert subjects == {
        "p-enter": "AAA",               # enter carries its own tradingsymbol
        "p-exit": "HDFCAMC",            # position_id → positions.symbol
        "p-mod": "pos-ghost",           # unknown position → the id, never blank
        "p-cancel": "HDFCAMC",          # order_id → orders.position_id → positions.symbol
        "p-cancel-orphan": "o-orphan",  # order without a position → the order id
        "p-cancel-ghost": "o-ghost",    # unknown order → the id
    }


# --------------------------------------------------------------------------- config/audit
def test_config_audit_latest_first(conn, clock) -> None:
    _insert_config_audit(conn, "mode_state", {"old": "OFF", "new": "RECOMMEND"}, at="2026-06-17T08:00:00+05:30")
    _insert_config_audit(conn, "trade_window_state", {"start": "09:30"}, at="2026-06-17T08:05:00+05:30")
    client = _client(conn=conn, clock=clock)

    r = client.get("/config/audit", headers=AUTH)
    assert r.status_code == 200
    rows = r.json()["config_audit"]
    assert [row["name"] for row in rows] == ["trade_window_state", "mode_state"]
    assert rows[0]["diff"] == {"start": "09:30"}


# --------------------------------------------------------------------------- learning/status
def test_learning_status(conn, clock) -> None:
    conn.execute(
        "INSERT INTO envelope_state (parameter, value, bounds_sha256, set_by, updated_at) "
        "VALUES ('orb.vol_mult', '1.5', 'x', 'default', '2026-06-17T00:00:00+05:30')"
    )
    _insert_param_set(conn, "ps-1", "candidate")
    _insert_param_set(conn, "ps-2", "candidate")
    _insert_param_set(conn, "ps-3", "champion")
    client = _client(conn=conn, clock=clock)

    r = client.get("/learning/status", headers=AUTH)
    assert r.status_code == 200
    learning = r.json()["learning"]
    assert learning["param_sets_by_status"] == {"candidate": 2, "champion": 1}
    assert any(row["parameter"] == "orb.vol_mult" and row["value"] == "1.5" for row in learning["envelope_state"])


# --------------------------------------------------------------------------- risk/headroom
def test_risk_headroom_wired_with_exposure_and_caps(conn, clock, exposure, limits_engine) -> None:
    _insert_position(conn, "o1", symbol="AAA", side="BUY", product="MIS", qty=10, avg_entry="100")
    client = _client(conn=conn, clock=clock, exposure=exposure, limits_engine=limits_engine)

    r = client.get("/risk/headroom", headers=AUTH)
    assert r.status_code == 200
    headroom = r.json()["headroom"]
    assert headroom["equity"] == "20000"          # no mark_price callback => open MTM falls back to 0
    assert headroom["day_mtm"] == "0"
    assert headroom["open_positions"] == {"total": 1, "mis": 1, "cnc": 0}
    assert headroom["consecutive_losses"] == 0
    assert headroom["deployed_capital"] == "1000"  # 10 * 100
    # O16 2026-09-07: base 40000 / caps 6-2-4 — read off the loaded table instead of pinning literals.
    table = limits_engine.table()
    assert headroom["caps"]["max_deployed_capital_inr"] == str(table.limits.capital_cap.max_deployed_capital_inr)
    assert headroom["caps"]["max_open_positions_total"] == table.limits.max_open_positions.total
    assert headroom["caps"]["max_open_positions_mis"] == table.limits.max_open_positions.max_mis
    assert headroom["caps"]["consecutive_losses_max_per_session"] == table.limits.consecutive_losses.max_per_session


def test_risk_headroom_wired_without_limits_engine_omits_caps(conn, clock, exposure) -> None:
    client = _client(conn=conn, clock=clock, exposure=exposure)
    r = client.get("/risk/headroom", headers=AUTH)
    assert "caps" not in r.json()["headroom"]


# --------------------------------------------------------------------------- budget
async def test_budget_wired_per_agent_and_degrade_tier(conn, clock, governor) -> None:
    await governor.record("weekly_researcher", "haiku-4.5", TokenUsage(in_tokens=1_000_000, out_tokens=0))
    client = _client(conn=conn, clock=clock, governor=governor)

    r = client.get("/budget", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["degrade_tier"] == "DG0"
    # Compare as Decimal, not string: the governor's own Decimal formatting (trailing zeros) is not
    # this route's contract to pin down.
    assert Decimal(body["budget"]["window_spend_usd"]) == Decimal("1")
    assert Decimal(body["budget"]["per_agent_spend_usd"]["weekly_researcher"]) == Decimal("1")
    # Amount-AGNOSTIC vs the owner-tunable agents.yaml (2026-08-03 rebalance lesson): the route's
    # contract is that it serves the GOVERNOR's allocation, not any particular dollar figure.
    assert Decimal(body["budget"]["allocations_usd"]["weekly_researcher"]) == governor.allocations()["weekly_researcher"]
    # The quota WINDOW, not a month (§5.6, 2026-09-12): the owner surface must say WHICH week the
    # spend belongs to, and the cap the tier is currently imposing.
    assert body["budget"]["window_key"] == governor.window_key()
    start, end = governor.window_bounds()
    assert (body["budget"]["window_start"], body["budget"]["window_end"]) == (
        start.isoformat(), end.isoformat()
    )
    assert Decimal(body["budget"]["credit_usd"]) == governor.credit()
    assert body["budget"]["forward_cap"] == governor.prescreen_forward_cap()


async def test_budget_per_agent_split_shows_an_agent_that_billed_without_an_allocation(
    conn, clock, governor
) -> None:
    """The split spans the UNION of allocated agents and the ones that billed. `sdk_smoke` exists in
    the live ledger with spend and no `budget_allocations_usd` entry: keyed off allocations alone it
    would sit inside `window_spend_usd` and in no row, so the panel's bars would not add up to the
    total printed above them."""
    await governor.record("sdk_smoke", "haiku-4.5", TokenUsage(in_tokens=200_000, out_tokens=0))
    await governor.record("weekly_researcher", "haiku-4.5", TokenUsage(in_tokens=1_000_000, out_tokens=0))
    body = _client(conn=conn, clock=clock, governor=governor).get("/budget", headers=AUTH).json()

    per_agent = body["budget"]["per_agent_spend_usd"]
    assert "sdk_smoke" not in body["budget"]["allocations_usd"]   # unallocated by construction
    assert Decimal(per_agent["sdk_smoke"]) == Decimal("0.2")
    # Every allocated agent keeps a row even at zero spend, and the rows reconcile to the headline.
    assert set(body["budget"]["allocations_usd"]) <= set(per_agent)
    assert sum((Decimal(v) for v in per_agent.values()), Decimal(0)) == Decimal(
        body["budget"]["window_spend_usd"]
    )


# --------------------------------------------------------------------------- config/params (GET + POST)
def test_post_config_params_bounds_accept_reject_and_audit(conn, clock, store) -> None:
    client = _client(conn=conn, clock=clock, store=store)

    ok = client.post("/config/params", headers=AUTH, json={"name": "edge_multiple_min", "value": 2.5})
    assert ok.status_code == 200
    assert ok.json() == {"ok": True, "applied": True, "value": 2.5}
    row = conn.execute("SELECT value, set_by FROM envelope_state WHERE parameter='edge_multiple_min'").fetchone()
    assert row["value"] == "2.5"
    assert row["set_by"] == "owner"
    audit_rows = conn.execute("SELECT diff FROM config_audit WHERE name='envelope_state'").fetchall()
    assert len(audit_rows) == 1
    assert json.loads(audit_rows[0]["diff"]) == {"parameter": "edge_multiple_min", "value": "2.5"}

    out_of_bounds = client.post("/config/params", headers=AUTH, json={"name": "edge_multiple_min", "value": 10.0})
    assert out_of_bounds.status_code == 422

    unknown = client.post("/config/params", headers=AUTH, json={"name": "nonexistent_param", "value": 1})
    assert unknown.status_code == 422

    protected = client.post("/config/params", headers=AUTH, json={"name": "analyst_confidence_min", "value": 0.6})
    assert protected.status_code == 409
    # The protected rejection must not have written anything (audit count unchanged).
    assert conn.execute("SELECT COUNT(*) AS n FROM config_audit WHERE name='envelope_state'").fetchone()["n"] == 1


def test_get_config_params_wired_with_bounds_and_suggestions(conn, clock, store, limits_engine) -> None:
    client = _client(conn=conn, clock=clock, store=store, limits_engine=limits_engine)
    client.post("/config/params", headers=AUTH, json={"name": "edge_multiple_min", "value": 2.5})
    _insert_nightly_review(
        conn, "2026-06-16",
        {"param_suggestions": [{"name": "edge_multiple_min", "suggested": 2.2, "reasoning": "cost floor"}]},
    )

    r = client.get("/config/params", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    edge = body["params"]["edge_multiple_min"]
    assert edge["value"] == "2.5"
    assert edge["min"] == 1.5
    assert edge["max"] == 3.0
    assert edge["set_by"] == "owner"
    assert body["params"]["analyst_confidence_min"]["value"] == 0.55
    assert body["suggestions"] == [{"name": "edge_multiple_min", "suggested": 2.2, "reasoning": "cost floor"}]


# --------------------------------------------------------------------------- mode / kill
def test_post_mode_recommend_single_step_flips_state(conn, clock, mode_manager) -> None:
    client = _client(mode_manager=mode_manager)

    r = client.post("/mode", headers=AUTH, json={"mode": "RECOMMEND"})
    assert r.status_code == 200
    assert r.json() == {"ok": True, "mode": "RECOMMEND"}
    assert mode_manager.mode() == Mode.RECOMMEND

    auto = client.post("/mode", headers=AUTH, json={"mode": "AUTO"})
    assert auto.status_code == 409
    assert mode_manager.mode() == Mode.RECOMMEND  # unchanged — AUTO never applied via this route

    bad = client.post("/mode", headers=AUTH, json={"mode": "NOT_A_MODE"})
    assert bad.status_code == 422


def test_post_kill_single_step_flips_state_reset_stays_409(conn, clock, kill_switch) -> None:
    client = _client(kill_switch=kill_switch)

    r = client.post("/kill", headers=AUTH, json={"reason": "manual owner kill"})
    assert r.status_code == 200
    assert r.json() == {"ok": True, "killed": True, "reason": "manual owner kill"}
    assert kill_switch.is_killed() is True
    assert kill_switch.reason() == "manual owner kill"

    reset = client.post("/kill/reset", headers=AUTH)
    assert reset.status_code == 409
    assert kill_switch.is_killed() is True  # unchanged — no dashboard two-step in Phase 2


# --------------------------------------------------------------------------- /ws/live
def test_ws_live_rejects_missing_token() -> None:
    client = _client()
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/live"):
            pass


def test_ws_live_broadcasts_bus_event(conn, clock, bus) -> None:
    app = create_app(secrets=_FakeSecrets(_TOKEN), bus=bus, clock=clock)
    client = TestClient(app)

    with client.websocket_connect(f"/ws/live?token={_TOKEN}") as ws:
        hello = ws.receive_json()
        assert hello["kind"] == "hello"

        event = ModeChanged(
            old_mode=Mode.OFF, new_mode=Mode.RECOMMEND, routing=None,
            actor=Actor.OWNER, reason="test", at=clock.now(),
        )
        # Publish on the SAME event loop the websocket connection runs on (this ad-hoc portal), so the
        # relay handler's `websocket.send_json` lands on the right loop (anyio memory streams are not
        # cross-loop safe).
        ws.portal.call(bus.apublish, TOPIC_MODE_CHANGED, event)

        frame = ws.receive_json()
        assert frame["kind"] == TOPIC_MODE_CHANGED
        assert frame["payload"]["new_mode"] == "RECOMMEND"
        assert frame["payload"]["old_mode"] == "OFF"
        assert frame["payload"]["reason"] == "test"
        assert frame["at"] is not None


# --------------------------------------------------------------------------- /news/watchlist (D3)
# The dashboard's news panel (§3.2.11 O8/R8) reads today's §2.7 catalyst watchlist + the digest
# freshness label. The route is duck-typed on ``store``: a MarketStore answers it, a ProtectedStore (or
# no store) degrades to the empty shape with ``status: null``.
@pytest.fixture
def market_store(tmp_path, clock):
    from engine.marketdata.store import MarketStore

    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


def _seed_watchlist(market_store, d) -> None:
    market_store.replace_catalyst_watchlist(
        d,
        [
            {
                "entry_id": "wl-orig", "symbol": "AAA", "grade": "originating", "direction": "long",
                "event_type": "order_win", "materiality": 0.72, "source_domain_count": 3,
                "event_age_h": 4.0, "event_age_sessions": 0,
                "confirm_trigger": Decimal("1410.50"), "invalidation": Decimal("1385.00"),
                "stop_band_low": Decimal("1380.00"), "stop_band_high": Decimal("1390.00"),
                "target_band_low": Decimal("1440.00"), "target_band_high": Decimal("1460.00"),
            },
            {
                "entry_id": "wl-ctx", "symbol": "BBB", "grade": "context", "direction": None,
                "event_type": "guidance", "materiality": 0.31, "source_domain_count": 1,
            },
        ],
    )


def test_news_watchlist_unwired_and_non_market_store_return_empty_shape(conn, clock, store) -> None:
    empty = {"d": None, "watchlist": [], "digest": {"as_of": None, "age_h": None, "stale_max_h": None, "status": None}}

    # (a) nothing wired at all
    assert _client().get("/news/watchlist", headers=AUTH).json() == empty
    # (b) a ProtectedStore is wired (today's real composition): it has no news-layer readers, so the
    #     route must degrade exactly like an unwired one rather than 500 the dashboard poll.
    assert _client(conn=conn, clock=clock, store=store).get("/news/watchlist", headers=AUTH).json() == empty


def test_news_watchlist_wired_rows_levels_and_digest_freshness(clock, market_store, limits_engine) -> None:
    d = clock.today()
    _seed_watchlist(market_store, d)
    market_store.upsert_sentiment_agg(
        [{"scope": "market", "scope_key": "market", "as_of": clock.now(), "value": 0.0}]
    )
    client = _client(clock=clock, store=market_store, limits_engine=limits_engine)

    r = client.get("/news/watchlist", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["d"] == d.isoformat()

    rows = {row["symbol"]: row for row in body["watchlist"]}
    assert {row["grade"] for row in body["watchlist"]} == {"originating", "context"}
    orig = rows["AAA"]
    assert orig["event_type"] == "order_win"
    assert orig["materiality"] == 0.72
    # Levels are Decimal in the store and must cross the wire as STRINGS (§8.1), never floats.
    assert orig["confirm_trigger"] == "1410.50"
    assert orig["stop_band_low"] == "1380.00"
    assert orig["target_band_high"] == "1460.00"
    assert rows["BBB"]["confirm_trigger"] is None

    digest = body["digest"]
    assert digest["status"] == "fresh"          # stamp == frozen now => age 0
    assert digest["age_h"] == 0.0
    assert digest["as_of"] == clock.now().isoformat()
    assert digest["stale_max_h"] == 20          # config/limits.yaml catalyst_guard (§7.1)


def test_news_watchlist_digest_stale_and_missing(clock, market_store, limits_engine) -> None:
    client = _client(clock=clock, store=market_store, limits_engine=limits_engine)

    # No sentiment_agg stamp and no watchlist rows => the digest never ran for today.
    assert client.get("/news/watchlist", headers=AUTH).json()["digest"]["status"] == "missing"

    # Rows but no stamp => an age that cannot be established is never "fresh" (mirrors digest_status).
    _seed_watchlist(market_store, clock.today())
    assert client.get("/news/watchlist", headers=AUTH).json()["digest"]["status"] == "stale"

    # Stamp older than catalyst_guard.digest_stale_max_h (20h) => stale.
    market_store.upsert_sentiment_agg(
        [{"scope": "market", "scope_key": "market", "as_of": clock.now() - timedelta(hours=30),
          "value": 0.0}]
    )
    stale = client.get("/news/watchlist", headers=AUTH).json()["digest"]
    assert stale["status"] == "stale"
    assert stale["age_h"] == 30.0


# --------------------------------------------------------------------------- /recommendations (D3)
# /decisions is the proposal→verdict provenance view and carries no Recommendation payload; the
# dashboard's recommendation panel reads the delivered artifact + the owner's human_action from here.
def test_recommendations_unwired_returns_stub_shape() -> None:
    assert _client().get("/recommendations", headers=AUTH).json() == {"recommendations": []}


def test_recommendations_latest_delivered_first_with_payload_and_human_action(conn, clock) -> None:
    payload = {
        "rec_id": "rec-1", "kind": "entry", "instrument": "AAA", "side": "BUY", "style": "intraday",
        "product": "MIS", "entry_zone": ["1400.00", "1402.50"], "stop": "1390.00",
        "targets": ["1430.00"], "qty": 10, "notional": "14000.00", "thesis": "ORB with volume",
        "confidence": 0.62, "manual_checklist": ["place the stop-loss order first"],
        "gate": {"verdict": "approve", "reasons": ["edge_multiple_ok"]},
    }
    conn.execute(
        "INSERT INTO recommendations (rec_id, payload, delivered_at, human_action, human_fill_price, "
        "outcome) VALUES (?, ?, ?, ?, ?, ?)",
        ("rec-1", json.dumps(payload), "2026-06-17T09:20:00+05:30", "taken", "1401.00",
         json.dumps({"pnl": "120.00"})),
    )
    conn.execute(
        "INSERT INTO recommendations (rec_id, payload, delivered_at, human_action) VALUES (?, ?, ?, ?)",
        ("rec-2", json.dumps({**payload, "rec_id": "rec-2"}), "2026-06-17T10:00:00+05:30", None),
    )
    client = _client(conn=conn, clock=clock)

    r = client.get("/recommendations", headers=AUTH)
    assert r.status_code == 200
    recs = r.json()["recommendations"]
    assert [rec["rec_id"] for rec in recs] == ["rec-2", "rec-1"]

    taken = next(rec for rec in recs if rec["rec_id"] == "rec-1")
    assert taken["human_action"] == "taken"
    assert taken["human_fill_price"] == "1401.00"
    assert taken["outcome"] == {"pnl": "120.00"}
    assert taken["recommendation"]["thesis"] == "ORB with volume"
    assert taken["recommendation"]["entry_zone"] == ["1400.00", "1402.50"]
    assert taken["recommendation"]["manual_checklist"] == ["place the stop-loss order first"]
    assert taken["recommendation"]["gate"]["verdict"] == "approve"

    assert next(rec for rec in recs if rec["rec_id"] == "rec-2")["human_action"] is None


# --------------------------------------------------------------------------- POST /db/query (§3.2.11)
# Owner ad-hoc read surface (owner-directed 2026-08-19): the engine process HOLDS market.duckdb, so it
# answers read-only questions instead of being STOPPED for them. Read-only is enforced by statement
# TYPE (DuckDB's parser) / authorizer action (SQLite) — never by inspecting the SQL text, so these
# rejection cases must hold for anything the parser classifies as a write, not just the literal strings.
_REJECTED = [
    ("market", "INSERT INTO catalyst_watchlist (entry_id, d, symbol, grade) "
               "VALUES ('x', DATE '2026-08-19', 'AAA', 'context')", "insert"),
    ("market", "COPY (SELECT 1) TO 'pwned.csv'", "copy-to"),
    ("market", "SET memory_limit='64GB'", "set"),
    ("market", "ATTACH 'elsewhere.duckdb' AS other", "attach"),
    ("market", "SELECT 1; SELECT 2", "two-statements"),
    ("market", "", "empty"),
    ("market", "   ", "blank"),
    ("state", "INSERT INTO proposals (proposal_id, agent_id, action, payload, inputs_digest) "
              "VALUES ('x', 'a', 'enter', '{}', 'd')", "insert"),
    ("state", "COPY (SELECT 1) TO 'pwned.csv'", "copy-to"),
    ("state", "SET memory_limit='64GB'", "set"),
    ("state", "ATTACH DATABASE 'elsewhere.db' AS other", "attach"),
    ("state", "SELECT 1; SELECT 2", "two-statements"),
    ("state", "", "empty"),
    ("state", "   ", "blank"),
]


def test_db_query_requires_the_bearer_token(conn, clock, market_store) -> None:
    client = _client(conn=conn, clock=clock, market_store=market_store)
    payload = {"db": "market", "sql": "SELECT 1"}

    assert client.post("/db/query", json=payload).status_code == 401
    assert client.post("/db/query", headers={"Authorization": "Bearer wrong"}, json=payload).status_code == 401


def test_db_query_unwired_dbs_answer_503_not_500() -> None:
    client = _client()  # no market_store / conn

    assert client.post("/db/query", headers=AUTH, json={"db": "market", "sql": "SELECT 1"}).status_code == 503
    assert client.post("/db/query", headers=AUTH, json={"db": "state", "sql": "SELECT 1"}).status_code == 503


def test_db_query_state_allows_recursive_cte(conn, clock) -> None:
    """SQLITE_RECURSIVE is in the allow-set (2026-08-19 review): WITH RECURSIVE is a read shape
    (walking order_events chains) and authorizes recursion only — it must not be denied."""
    client = _client(conn=conn, clock=clock)
    body = {
        "db": "state",
        "sql": "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) "
               "SELECT n FROM seq",
    }

    resp = client.post("/db/query", headers=AUTH, json=body)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["columns"] == ["n"]
    assert [r[0] for r in data["rows"]] == [1, 2, 3, 4, 5]
    assert data["truncated"] is False


@pytest.mark.parametrize("db, sql, why", _REJECTED, ids=[f"{d}-{w}" for d, _s, w in _REJECTED])
def test_db_query_rejects_everything_but_one_read_statement(conn, clock, market_store, db, sql, why) -> None:
    client = _client(conn=conn, clock=clock, market_store=market_store)

    r = client.post("/db/query", headers=AUTH, json={"db": db, "sql": sql})
    assert r.status_code == 400, (db, why, r.status_code, r.text)


def test_db_query_market_roundtrip_encodes_decimal_and_date(clock, market_store) -> None:
    d = clock.today()
    _seed_watchlist(market_store, d)
    client = _client(clock=clock, market_store=market_store)

    r = client.post("/db/query", headers=AUTH, json={
        "db": "market",
        "sql": "SELECT symbol, d, confirm_trigger FROM catalyst_watchlist ORDER BY symbol",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["symbol", "d", "confirm_trigger"]
    assert (body["row_count"], body["truncated"]) == (2, False)
    assert body["elapsed_ms"] >= 0
    # DECIMAL crosses as a STRING (§8.1 — a float would corrupt the level), DATE as ISO-8601.
    assert body["rows"] == [["AAA", d.isoformat(), "1410.50"], ["BBB", d.isoformat(), None]]

    # EXPLAIN is the one non-SELECT statement type enumerated as read-only (a plan is what the owner
    # asks for next when a read is slow).
    assert client.post("/db/query", headers=AUTH,
                       json={"db": "market", "sql": "EXPLAIN SELECT 1"}).status_code == 200


def test_db_query_state_reads_the_engines_own_database(conn, clock) -> None:
    _insert_proposal(conn, "p-1", agent_id="orb_scanner")
    _insert_proposal(conn, "p-2", agent_id="cat_scanner")
    client = _client(conn=conn, clock=clock)

    r = client.post("/db/query", headers=AUTH, json={
        "db": "state",
        "sql": "SELECT proposal_id, agent_id FROM proposals ORDER BY proposal_id",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["proposal_id", "agent_id"]
    assert body["rows"] == [["p-1", "orb_scanner"], ["p-2", "cat_scanner"]]
    assert (body["row_count"], body["truncated"]) == (2, False)


def test_db_query_row_cap_flags_truncation(conn, clock, market_store) -> None:
    client = _client(conn=conn, clock=clock, market_store=market_store)

    over = client.post("/db/query", headers=AUTH,
                       json={"db": "market", "sql": "SELECT * FROM range(50)", "max_rows": 10}).json()
    assert (over["row_count"], over["truncated"]) == (10, True)
    assert over["rows"][0] == [0]

    # Exactly at the cap is NOT truncated — the +1 over-fetch probe must never become a false flag.
    exact = client.post("/db/query", headers=AUTH,
                        json={"db": "market", "sql": "SELECT * FROM range(10)", "max_rows": 10}).json()
    assert (exact["row_count"], exact["truncated"]) == (10, False)

    for i in range(6):
        _insert_proposal(conn, f"cap-{i}")
    state = client.post("/db/query", headers=AUTH,
                        json={"db": "state", "sql": "SELECT proposal_id FROM proposals", "max_rows": 4}).json()
    assert (state["row_count"], state["truncated"]) == (4, True)

    # Past the hard ceilings the request is REFUSED, never silently clamped.
    assert client.post("/db/query", headers=AUTH,
                       json={"db": "market", "sql": "SELECT 1", "max_rows": 200_000}).status_code == 422
    assert client.post("/db/query", headers=AUTH,
                       json={"db": "market", "sql": "SELECT 1", "timeout_s": 120}).status_code == 422


def test_db_query_timeout_interrupts_the_query_and_answers_504(clock, market_store) -> None:
    """A runaway owner query is interrupted at ITS OWN cursor: this is a trading process first, and the
    store's connection must survive the interrupt untouched."""
    client = _client(clock=clock, market_store=market_store)

    r = client.post("/db/query", headers=AUTH, json={
        "db": "market",
        "sql": "SELECT count(*) FROM range(100000000000) a, range(100) b",
        "timeout_s": 1,
    })
    assert r.status_code == 504, r.text
    assert "timeout_s=1" in r.json()["detail"]
    assert market_store.table_names()          # the store's own connection still answers


def test_db_query_market_connection_is_memory_bounded(clock, market_store) -> None:
    """2026-08-18 (the 53 GB incident) generalized by §3.2.11: the LIVE store connection carries an
    explicit DuckDB memory_limit, so nothing — this endpoint's arbitrary SELECTs included — can commit
    the machine. Asserted numerically because DuckDB renders the limit in GiB (mirrors
    test_tick_compaction.py::test_compaction_connection_is_memory_bounded)."""
    client = _client(clock=clock, market_store=market_store)

    r = client.post("/db/query", headers=AUTH,
                    json={"db": "market", "sql": "SELECT current_setting('memory_limit')"})
    assert r.status_code == 200, r.text
    limit = r.json()["rows"][0][0]
    value, unit = limit.split()
    assert unit in ("GiB", "GB"), limit
    assert float(value) <= 8.0, limit
