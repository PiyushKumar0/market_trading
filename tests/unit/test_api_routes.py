"""Phase-2 dashboard API routes (§3.2.11 C3): real reads/writes over risk/oms/learning/intelligence
state via the app's OPTIONAL collaborators (``conn``/``store``/``exposure``/``governor``/
``limits_engine``), with every route degrading to its Phase-0/1 stub shape when its own collaborator is
unwired. Mirrors ``test_api_auth.py``'s ``TestClient`` pattern; inserts rows directly into ``conn``
(migrated by the shared ``conn`` fixture, tests/conftest.py) rather than going through write paths that
don't exist yet."""

from __future__ import annotations

import json
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
) -> None:
    conn.execute(
        "INSERT INTO orders (order_id, role, state, product, created_at) VALUES (?, ?, ?, ?, ?)",
        (order_id, role, state, product, created_at),
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
    assert headroom["caps"]["max_deployed_capital_inr"] == "20000"
    assert headroom["caps"]["max_open_positions_total"] == 3
    assert headroom["caps"]["max_open_positions_mis"] == 2
    assert headroom["caps"]["consecutive_losses_max_per_session"] == 3


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
    assert Decimal(body["budget"]["month_spend_usd"]) == Decimal("1")
    assert Decimal(body["budget"]["per_agent_spend_usd"]["weekly_researcher"]) == Decimal("1")
    assert Decimal(body["budget"]["allocations_usd"]["weekly_researcher"]) == Decimal("12")


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
