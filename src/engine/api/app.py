"""FastAPI LAN dashboard API + Kite login callback (§3.2.11; R6/R8/R10).

The owner I/O surface over read-only views of risk / OMS / learning / intelligence state. This module
is the composition point for the HTTP/WS surface; it NEVER originates orders — owner commands route
through ``ModeManager`` / ``KillSwitch`` / ``OrderManager`` interfaces (§3.2.11).

Security model (R10):
- EVERY route is behind a ``Authorization: Bearer <token>`` check against ``Secrets[DASHBOARD_TOKEN]``,
  EXCEPT the unauthenticated ``GET /kite/callback`` (R6 — Kite redirects the browser there with a
  ``request_token`` to complete the daily login; it cannot carry our bearer header).
- The bind address (LAN ``settings.api.host:port``, default ``0.0.0.0:8400``) is the caller's concern
  (uvicorn); this module only constructs the ``FastAPI`` app.

Phase 0 scope shipped REAL wiring + REAL auth with shape-correct read-route placeholders. Phase 2
(§3.2.11 C3) wires the read routes to real SQLite state + the risk/intelligence collaborators
(``conn``/``store``/``exposure``/``governor``/``limits_engine``, all OPTIONAL keyword args on
:func:`create_app`) and lands ``POST /mode``/``POST /kill`` as real SINGLE-step owner actions (the
bearer token itself is the owner authentication, §2.4). Every collaborator degrades to its Phase-0/1
stub response shape when left ``None`` — composition (``engine.ops``) wires them for real. ``POST /mode
AUTO`` and ``POST /kill/reset`` still require the owner TWO-STEP pattern, which has no dashboard path
yet (Telegram-only, §3.5.3/§7.2) — they answer 409, not a stub.
"""

from __future__ import annotations

import asyncio
import json
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from engine.api.kite_callback import build_kite_callback_router
from engine.core.config import Settings, load_settings
from engine.core.db import transaction
from engine.core.enums import Actor, Mode
from engine.core.log import get_logger
from engine.core.secrets import DASHBOARD_TOKEN, Secrets
from engine.intelligence.events import TOPIC_BUDGET_STATE
from engine.risk.events import TOPIC_KILL_STATE, TOPIC_MODE_CHANGED, TOPIC_RISK_STATE, TOPIC_TRADE_WINDOW

_log = get_logger("engine.api.app")

# Path to the built React dashboard (served when present). Owner builds it into ``web/dist`` (R8).
_DASHBOARD_DIST_DIRNAME = "dist"

#: The protected-store name for the learnable-parameter envelope (§6.3), matching ``limits.py``'s
#: module-level ``LIMITS_FILE`` convention.
ENVELOPE_FILE = "envelope.yaml"

#: Bus topics relayed onto every connected ``/ws/live`` socket (R8). Tick fan-out is Phase 3.
_WS_RELAY_TOPICS = (TOPIC_MODE_CHANGED, TOPIC_RISK_STATE, TOPIC_KILL_STATE, TOPIC_TRADE_WINDOW, TOPIC_BUDGET_STATE)

#: Keepalive ping cadence for a live ``/ws/live`` socket (LAN dashboard / proxies see it as alive).
_WS_PING_INTERVAL_S = 15


# --------------------------------------------------------------------------- request bodies
class TradeWindowBody(BaseModel):
    """Owner-set trade-window payload for ``POST /config/trade_window`` (§3.2.7/§7.1)."""

    start: str = Field(description="IST start time, 'HH:MM'")
    end: str = Field(description="IST end time, 'HH:MM'")
    squareoff_buffer_min: int | None = Field(default=None, ge=0)


class ParamBody(BaseModel):
    """Owner-applied tunable for ``POST /config/params`` — the platform SUGGESTS, the owner SETS (§6.3)."""

    name: str
    value: Any


class ModeBody(BaseModel):
    """Owner mode-change payload for ``POST /mode`` (two-step; §3.5.3/R10)."""

    mode: str
    confirmation_phrase: str | None = None
    routing: str | None = None


class KillBody(BaseModel):
    """Owner kill payload for ``POST /kill`` (single-step, fast; §7.2)."""

    reason: str | None = None


# --------------------------------------------------------------------------- WS fan-out hub (R8)
class WSHub:
    """Tracks connected ``/ws/live`` sockets and fans a JSON frame out to all of them.

    Tick fan-out (held + watch instruments) is Phase 3 — this hub only relays the risk/mode/kill/
    trade-window/budget state-change events the bus already publishes (§3.2.11). A socket that fails a
    send (client gone) is dropped rather than raising into the publisher (mirrors ``EventBus``'s
    isolate-a-bad-handler stance, R8).
    """

    def __init__(self, clock: Any = None) -> None:
        self._clock = clock
        self._sockets: set[WebSocket] = set()

    def register(self, websocket: WebSocket) -> None:
        self._sockets.add(websocket)

    def unregister(self, websocket: WebSocket) -> None:
        self._sockets.discard(websocket)

    def now_iso(self) -> str | None:
        return self._clock.now().isoformat() if self._clock is not None else None

    async def broadcast(self, kind: str, payload: dict[str, Any]) -> None:
        frame = {"kind": kind, "payload": payload, "at": self.now_iso()}
        dead: list[WebSocket] = []
        for websocket in list(self._sockets):
            try:
                await websocket.send_json(frame)
            except Exception:  # noqa: BLE001 - a dead socket must not break the broadcast
                dead.append(websocket)
        for websocket in dead:
            self._sockets.discard(websocket)


# --------------------------------------------------------------------------- app factory
def create_app(
    *,
    session_manager: Any = None,
    mode_manager: Any = None,
    kill_switch: Any = None,
    secrets: Secrets | None = None,
    clock: Any = None,
    bus: Any = None,
    conn: Any = None,
    store: Any = None,
    exposure: Any = None,
    governor: Any = None,
    limits_engine: Any = None,
) -> FastAPI:
    """Construct the dashboard ``FastAPI`` app (§3.2.11).

    All dependencies are injected by the composition root (``engine.ops``) so the app is testable and
    holds no module-level singletons. ``secrets`` supplies the bearer token (R10); ``mode_manager``
    backs the trade-window read/write path (§3.2.7). ``conn``/``store``/``exposure``/``governor``/
    ``limits_engine`` back the Phase-2 read/write routes (proposals/verdicts/positions/orders/config
    audit/envelope params/risk headroom/budget) — every one is OPTIONAL and each route degrades to its
    Phase-0/1 stub response shape when its own collaborator is ``None`` (duck-typed calls; no isinstance
    checks), so this factory stays usable before composition wires the real objects. The caller binds it
    to ``settings.api.host:port`` via uvicorn — this factory does not start a server.
    """
    settings: Settings = load_settings()
    secrets = secrets or Secrets()

    app = FastAPI(title="market_trading dashboard", version="0", docs_url=None, redoc_url=None)
    # Stash injected collaborators on app.state for handlers / future phases (no globals).
    app.state.session_manager = session_manager
    app.state.mode_manager = mode_manager
    app.state.kill_switch = kill_switch
    app.state.secrets = secrets
    app.state.clock = clock
    app.state.bus = bus
    app.state.conn = conn
    app.state.store = store
    app.state.exposure = exposure
    app.state.governor = governor
    app.state.limits_engine = limits_engine
    app.state.settings = settings
    app.state.ws_hub = WSHub(clock)

    # ----------------------------------------------------------------- unauthenticated: Kite login (R6)
    # MUST be mounted WITHOUT the bearer dependency: Kite redirects the owner's browser here with a
    # ``request_token`` to complete the daily login, and that redirect cannot carry our bearer header.
    app.include_router(build_kite_callback_router(session_manager, clock))

    # ----------------------------------------------------------------- WS relay subscriptions (R8)
    # Subscribe the hub to every canonical state-change topic so a connected dashboard sees mode/risk/
    # kill/trade-window/budget changes live. Tick fan-out is Phase 3 (deliberately not faked here).
    if bus is not None:
        for topic in _WS_RELAY_TOPICS:
            bus.subscribe(topic, _make_ws_relay_handler(app.state.ws_hub, topic))

    # ----------------------------------------------------------------- read routes (bearer-auth stubs)
    # Phase 0: each returns a shape-correct placeholder; live views over risk/oms/learning/intelligence
    # state arrive in later phases. Every one is gated by ``Owner`` (R10).
    @app.get("/positions")
    async def positions(_: Owner) -> dict[str, Any]:
        """All-origin positions (platform/external/recommended), OPEN first, + ``as_of`` (R8/O5)."""
        conn = app.state.conn
        if conn is None:
            return {"positions": [], "as_of": None}
        rows = conn.execute(
            "SELECT * FROM positions "
            "ORDER BY CASE WHEN state='OPEN' THEN 0 ELSE 1 END, COALESCE(opened_at, '') DESC"
        ).fetchall()
        clock = app.state.clock
        return {"positions": [dict(r) for r in rows], "as_of": clock.now().isoformat() if clock is not None else None}

    @app.get("/orders")
    async def orders(_: Owner) -> dict[str, Any]:
        """Latest 100 orders, most recent first (Phase 3 will add live working-order state)."""
        conn = app.state.conn
        if conn is None:
            return {"orders": []}
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY COALESCE(created_at, '') DESC LIMIT 100"
        ).fetchall()
        return {"orders": [dict(r) for r in rows]}

    @app.get("/decisions")
    async def decisions(_: Owner) -> dict[str, Any]:
        """Latest 100 Tier-1 action proposals LEFT JOIN their verdict (R8 provenance-chain audit view)."""
        conn = app.state.conn
        if conn is None:
            return {"decisions": []}
        rows = conn.execute(
            """
            SELECT p.proposal_id, p.agent_id, p.action, p.payload AS proposal_payload, p.created_at,
                   v.verdict_id, v.verdict AS verdict_outcome, v.payload AS verdict_payload, v.evaluated_at
            FROM proposals p
            LEFT JOIN verdicts v ON v.proposal_id = p.proposal_id
            ORDER BY p.created_at DESC
            LIMIT 100
            """
        ).fetchall()
        out = []
        for row in rows:
            verdict_payload = json.loads(row["verdict_payload"]) if row["verdict_payload"] else None
            out.append(
                {
                    "proposal_id": row["proposal_id"],
                    "agent_id": row["agent_id"],
                    "action": row["action"],
                    "proposal": json.loads(row["proposal_payload"]),
                    "created_at": row["created_at"],
                    "verdict_id": row["verdict_id"],
                    "verdict": row["verdict_outcome"],
                    "reasons": (verdict_payload or {}).get("reasons", []),
                    "evaluated_at": row["evaluated_at"],
                }
            )
        return {"decisions": out}

    @app.get("/verdicts")
    async def verdicts(_: Owner) -> dict[str, Any]:
        """Latest 100 gate-verdict payloads, most recently evaluated first (R8)."""
        conn = app.state.conn
        if conn is None:
            return {"verdicts": []}
        rows = conn.execute(
            "SELECT verdict_id, proposal_id, verdict, payload, evaluated_at FROM verdicts "
            "ORDER BY evaluated_at DESC LIMIT 100"
        ).fetchall()
        out = [
            {
                "verdict_id": r["verdict_id"],
                "proposal_id": r["proposal_id"],
                "verdict": r["verdict"],
                "payload": json.loads(r["payload"]),
                "evaluated_at": r["evaluated_at"],
            }
            for r in rows
        ]
        return {"verdicts": out}

    @app.get("/risk/headroom")
    async def risk_headroom(_: Owner) -> dict[str, Any]:
        """Platform equity/day-MTM/open-count/consecutive-loss headroom (§7.1) + limit caps when wired."""
        exposure = app.state.exposure
        if exposure is None:
            return {"headroom": {}}
        counts = exposure.open_position_counts()
        headroom: dict[str, Any] = {
            "equity": str(exposure.equity()),
            "day_mtm": str(exposure.day_mtm()),
            "open_positions": {"total": counts.total, "mis": counts.mis, "cnc": counts.cnc},
            "consecutive_losses": exposure.consecutive_losses(),
            "deployed_capital": str(exposure.deployed_capital()),
        }
        limits_engine = app.state.limits_engine
        if limits_engine is not None:
            lim = limits_engine.table().limits
            headroom["caps"] = {
                "max_deployed_capital_inr": str(lim.capital_cap.max_deployed_capital_inr),
                "max_open_positions_total": lim.max_open_positions.total,
                "max_open_positions_mis": lim.max_open_positions.max_mis,
                "max_open_positions_cnc": lim.max_open_positions.max_cnc,
                "consecutive_losses_max_per_session": lim.consecutive_losses.max_per_session,
                "max_new_trades_day": lim.max_new_trades_day.count,
            }
        return {"headroom": headroom}

    @app.get("/budget")
    async def budget(_: Owner) -> dict[str, Any]:
        """SDK-call budget governor state: degrade tier + month-spend per agent + allocations (§5.6)."""
        governor = app.state.governor
        if governor is None:
            return {"budget": {}, "degrade_tier": None}
        allocations = {agent: str(usd) for agent, usd in governor.allocations().items()}
        return {
            "budget": {
                "month_spend_usd": str(governor.month_spend()),
                "per_agent_spend_usd": {agent: str(governor.agent_spend(agent)) for agent in allocations},
                "allocations_usd": allocations,
            },
            "degrade_tier": governor.degrade_tier().value,
        }

    @app.get("/learning/status")
    async def learning_status(_: Owner) -> dict[str, Any]:
        """Live envelope_state values + param_sets counts by status (§5.5/§6.5; champ/chall UI is Phase 4+)."""
        conn = app.state.conn
        if conn is None:
            return {"learning": {}}
        envelope_rows = conn.execute(
            "SELECT parameter, value, set_by, updated_at FROM envelope_state"
        ).fetchall()
        status_rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM param_sets GROUP BY status"
        ).fetchall()
        return {
            "learning": {
                "envelope_state": [dict(r) for r in envelope_rows],
                "param_sets_by_status": {r["status"]: r["n"] for r in status_rows},
            }
        }

    @app.get("/config/audit")
    async def config_audit(_: Owner) -> dict[str, Any]:
        """Latest 100 ``config_audit`` rows, most recent first (NEW route; R8 owner-change trail)."""
        conn = app.state.conn
        if conn is None:
            return {"config_audit": []}
        rows = conn.execute(
            "SELECT id, name, diff, actor, at FROM config_audit ORDER BY id DESC LIMIT 100"
        ).fetchall()
        out = []
        for r in rows:
            diff: Any = r["diff"]
            if diff is not None:
                try:
                    diff = json.loads(diff)
                except (TypeError, ValueError):
                    pass  # legacy/non-JSON diff text — surface as-is rather than 500 the whole route
            out.append({"id": r["id"], "name": r["name"], "diff": diff, "actor": r["actor"], "at": r["at"]})
        return {"config_audit": out}

    @app.get("/mode")
    async def get_mode(_: Owner) -> dict[str, Any]:
        """Current mode / routing / risk-state (sticky; live via ``mode_manager`` when wired)."""
        mm = app.state.mode_manager
        if mm is None:
            return {"mode": None, "routing": None, "risk_state": None}
        routing = mm.routing()
        return {
            "mode": mm.mode().value,
            "routing": routing.value if routing else None,
            "risk_state": mm.risk_state().value,
        }

    # ----------------------------------------------------------------- trade window (GET + POST, §3.2.7)
    @app.get("/config/trade_window")
    async def get_trade_window(_: Owner) -> dict[str, Any]:
        """Current owner-set trade window (read via ``mode_manager.get_trade_window``; §7.1)."""
        mm = app.state.mode_manager
        window = mm.get_trade_window() if mm is not None else None
        if window is None:
            return {"trade_window": None}
        return {
            "trade_window": {
                "start": window.start.strftime("%H:%M"),
                "end": window.end.strftime("%H:%M"),
                "squareoff_buffer_min": window.squareoff_buffer_min,
            }
        }

    @app.post("/config/trade_window")
    async def set_trade_window(body: TradeWindowBody, _: Owner) -> dict[str, Any]:
        """Owner SINGLE-step trade-window setter (§3.2.7): validate → persist sticky → audit → publish →
        apply. Bearer auth IS the owner authentication (R10), so the call is made as ``Actor.OWNER``;
        ``ModeManager.set_trade_window`` does the validation + ``config_audit`` write + alert publish."""
        mm = app.state.mode_manager
        if mm is None:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "mode manager unavailable")
        start, end = _parse_hhmm(body.start), _parse_hhmm(body.end)
        ok = await mm.set_trade_window(
            start, end, Actor.OWNER, squareoff_buffer_min=body.squareoff_buffer_min
        )
        if not ok:
            # Validation failed (e.g. start>=end, empty MIS sub-window, outside session) — value unchanged.
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid trade window (rejected, unchanged)")
        _log.warning("trade_window_set_via_api", start=body.start, end=body.end)
        return {"ok": True}

    # ----------------------------------------------------------------- params (GET + POST, §6.3)
    @app.get("/config/params")
    async def get_params(_: Owner) -> dict[str, Any]:
        """Live ``envelope_state`` values + ``envelope.yaml`` bounds (when ``store`` wired) +
        ``analyst_confidence_min`` (when ``limits_engine`` wired) + the latest nightly-reviewer
        SUGGESTED values — the platform SUGGESTS, the owner SETS (§6.3/§5.5)."""
        conn = app.state.conn
        if conn is None:
            return {"params": {}, "suggestions": []}
        store = app.state.store
        bounds: dict[str, Any] = {}
        if store is not None:
            bounds = store.load_verified(ENVELOPE_FILE).get("parameters") or {}
        env_rows = {
            r["parameter"]: r
            for r in conn.execute("SELECT parameter, value, set_by, updated_at FROM envelope_state").fetchall()
        }
        params: dict[str, Any] = {}
        for name in set(bounds) | set(env_rows):
            b = bounds.get(name, {})
            row = env_rows.get(name)
            params[name] = {
                "value": row["value"] if row is not None else b.get("default"),
                "min": b.get("min"),
                "max": b.get("max"),
                "default": b.get("default"),
                "used_by": b.get("used_by"),
                "set_by": row["set_by"] if row is not None else "default",
                "updated_at": row["updated_at"] if row is not None else None,
            }
        limits_engine = app.state.limits_engine
        if limits_engine is not None:
            params["analyst_confidence_min"] = {
                "value": limits_engine.table().analyst_confidence_min,
                "min": None,
                "max": None,
                "default": None,
                "used_by": "analyst_gate",
                "set_by": "owner",
                "updated_at": None,
            }
        suggestions: list[Any] = []
        latest_review = conn.execute("SELECT payload FROM nightly_reviews ORDER BY d DESC LIMIT 1").fetchone()
        if latest_review is not None:
            suggestions = json.loads(latest_review["payload"]).get("param_suggestions") or []
        return {"params": params, "suggestions": suggestions}

    @app.post("/config/params")
    async def set_params(body: ParamBody, _: Owner) -> dict[str, Any]:
        """Owner applies ONE envelope tunable — every change audited to ``config_audit`` (§6.3). The
        platform never self-applies a suggestion; only this owner-authenticated path SETS.
        ``analyst_confidence_min`` is owner-only via the PROTECTED ``limits.yaml`` flow, not this route
        (§6.3) — rejected 409 regardless of wiring. Unknown name / out-of-[min,max] value => 422."""
        if body.name == "analyst_confidence_min":
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={
                    "ok": False,
                    "note": (
                        "analyst_confidence_min is owner-only via the protected limits.yaml flow "
                        "(ProtectedStore.owner_update / Telegram two-step) — not this route (§6.3)"
                    ),
                },
            )
        conn = app.state.conn
        store = app.state.store
        if conn is None or store is None:
            _log.warning("param_set_via_api", name=body.name)
            return {"ok": True, "applied": False, "note": "phase-0 stub — param store lands later"}
        bounds = (store.load_verified(ENVELOPE_FILE).get("parameters") or {}).get(body.name)
        if bounds is None:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"unknown envelope parameter {body.name!r}")
        try:
            value = float(body.value)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "value must be numeric")
        lo, hi = float(bounds["min"]), float(bounds["max"])
        if not (lo <= value <= hi):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY, f"{body.name} must be within [{lo}, {hi}] (got {value})"
            )
        clock = app.state.clock
        now = clock.now().isoformat() if clock is not None else None
        sig_row = conn.execute("SELECT sha256 FROM protected_config WHERE name=?", (ENVELOPE_FILE,)).fetchone()
        bounds_sha256 = sig_row["sha256"] if sig_row is not None else ""
        with transaction(conn):
            conn.execute(
                """
                INSERT INTO envelope_state (parameter, value, bounds_sha256, set_by, param_set_id, updated_at)
                VALUES (?, ?, ?, 'owner', NULL, ?)
                ON CONFLICT(parameter) DO UPDATE SET
                    value=excluded.value, bounds_sha256=excluded.bounds_sha256,
                    set_by=excluded.set_by, param_set_id=NULL, updated_at=excluded.updated_at
                """,
                (body.name, str(value), bounds_sha256, now),
            )
            conn.execute(
                "INSERT INTO config_audit (name, diff, actor, at) VALUES (?, ?, ?, ?)",
                ("envelope_state", json.dumps({"parameter": body.name, "value": str(value)}), Actor.OWNER.value, now),
            )
        _log.warning("param_set_via_api", name=body.name, value=str(value))
        return {"ok": True, "applied": True, "value": value}

    # ----------------------------------------------------------------- mode (POST, two-step; R10)
    @app.post("/mode")
    async def set_mode(body: ModeBody, _: Owner) -> dict[str, Any]:
        """Owner mode change. OFF/RECOMMEND are SINGLE-step (the bearer token itself IS the owner
        authentication, §2.4) — ``ModeManager.request_transition`` applies them directly as
        ``Actor.OWNER``. →AUTO requires a TWO-STEP owner confirmation (R10/§3.5.3) that has no dashboard
        path in Phase 2 (Telegram ``/mode AUTO`` + ``/confirm`` only) — rejected 409, not a stub."""
        try:
            target = Mode(body.mode)
        except ValueError:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"unknown mode {body.mode!r}")
        if target == Mode.AUTO:
            # Business rule, independent of wiring (mirrors /kill/reset and analyst_confidence_min).
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={"ok": False, "note": "two-step via Telegram /mode AUTO + /confirm"},
            )
        mm = app.state.mode_manager
        if mm is None:
            return JSONResponse(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                content={"ok": False, "note": "phase-0 stub — mode manager not wired"},
            )
        ok = await mm.request_transition(target, Actor.OWNER)
        return {"ok": ok, "mode": target.value}

    # ----------------------------------------------------------------- kill (POST, two-step; R10/§7.2)
    @app.post("/kill")
    async def kill(_: Owner, body: KillBody = KillBody()) -> dict[str, Any]:
        """Engage the kill switch — SINGLE-step trigger (fast), owner-authenticated via bearer (§7.2).
        ``KillSwitch.trigger`` persists KILLED before acting (R10); the OMS flatten sequence is
        Phase 3 and runs via its own injected callback when wired."""
        ks = app.state.kill_switch
        if ks is None:
            return JSONResponse(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                content={"ok": False, "note": "phase-0 stub — kill switch not wired"},
            )
        reason = body.reason or "owner_triggered_via_dashboard"
        await ks.trigger(reason, actor=Actor.OWNER)
        return {"ok": True, "killed": True, "reason": reason}

    @app.post("/kill/reset")
    async def kill_reset(_: Owner) -> dict[str, Any]:
        """Reset the kill switch — owner TWO-STEP authenticated flow (R10/§7.2). No dashboard two-step
        exists in Phase 2 (Telegram ``/kill_reset`` + ``/confirm`` only) — always 409, not a stub."""
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "ok": False,
                "note": "two-step via Telegram /kill_reset + /confirm (no dashboard two-step in Phase 2)",
            },
        )

    # ----------------------------------------------------------------- WS live stream (R8; Phase 2)
    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        """Live risk/mode/kill/trade-window/budget state stream (R8): the socket is registered on
        ``app.state.ws_hub`` and receives ``{kind, payload, at}`` frames relayed off the bus, plus a
        keepalive ping every 15s. Tick fan-out (held + watch instruments) is Phase 3 — not faked here.

        NOTE: the bearer check is enforced manually here (WS handshakes can't use the HTTP ``Depends``
        bearer dependency the same way) — the token may arrive as the ``Authorization`` header or a
        ``token`` query param."""
        if not _ws_authorized(websocket, secrets):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await websocket.accept()
        hub: WSHub = app.state.ws_hub
        hub.register(websocket)
        await websocket.send_json({"kind": "hello", "payload": {"stream": "live"}, "at": hub.now_iso()})
        keepalive = asyncio.create_task(_ws_keepalive(hub, websocket))
        try:
            while True:
                await websocket.receive_text()  # blocks until the client sends/disconnects
        except WebSocketDisconnect:
            pass
        finally:
            keepalive.cancel()
            hub.unregister(websocket)

    # ----------------------------------------------------------------- static dashboard (R8)
    _mount_dashboard_if_present(app, settings)

    _log.info("api_app_created", host=settings.api.host, port=settings.api.port)
    return app


# --------------------------------------------------------------------------- auth (R10)
async def _require_owner(request: Request) -> None:
    """Enforce ``Authorization: Bearer <DASHBOARD_TOKEN>`` on every protected route (R10).

    MODULE-LEVEL by design: with ``from __future__ import annotations`` the ``_: Owner`` route
    annotations are strings, and FastAPI resolves them against the route function's MODULE globals — a
    dependency alias defined inside ``create_app`` would be unresolvable (FastAPI would then treat the
    param as a query field and every route would 422 instead of authenticating). The ``Secrets`` come
    off ``request.app.state`` (set by ``create_app``). A missing/garbled token store fails CLOSED (401),
    never open (see ``_token_matches``)."""
    secrets: Secrets = request.app.state.secrets
    token = _extract_bearer(request.headers.get("authorization"))
    if token is None or not _token_matches(secrets, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


#: Route dependency marker: ``_: Owner`` on a handler gates it behind :func:`_require_owner` (R10).
Owner = Annotated[None, Depends(_require_owner)]


def _extract_bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        return None
    return parts[1].strip()


def _token_matches(secrets: Secrets, presented: str) -> bool:
    """Fail-closed comparison of the presented token against the stored dashboard token (R10)."""
    import hmac

    try:
        expected = secrets.get(DASHBOARD_TOKEN)
    except Exception:  # missing secret store / key ⇒ deny (never open)
        return False
    return hmac.compare_digest(expected, presented)


def _ws_authorized(websocket: WebSocket, secrets: Secrets) -> bool:
    """Bearer check for the WS handshake — accepts the token via header or ``?token=`` query (R10)."""
    token = _extract_bearer(websocket.headers.get("authorization"))
    if token is None:
        token = websocket.query_params.get("token")
    return token is not None and _token_matches(secrets, token)


def _make_ws_relay_handler(hub: WSHub, topic: str) -> Any:
    """Bind ``topic`` by value (default-arg trick) so every subscribed handler broadcasts under its
    OWN topic name, not whichever topic the enclosing loop last iterated to (R8)."""

    async def _handler(event: Any) -> None:
        payload = event.model_dump(mode="json") if hasattr(event, "model_dump") else event
        await hub.broadcast(topic, payload)

    return _handler


async def _ws_keepalive(hub: WSHub, websocket: WebSocket) -> None:
    """Ping frame every 15s (§3.2.11) so LAN clients/proxies see the socket as alive. A failed send
    (client already gone) just ends the task — the connection's own receive loop handles cleanup."""
    while True:
        await asyncio.sleep(_WS_PING_INTERVAL_S)
        try:
            await websocket.send_json({"kind": "ping", "payload": {}, "at": hub.now_iso()})
        except Exception:  # noqa: BLE001 - end the loop quietly; not a broadcast-breaking failure
            return


# --------------------------------------------------------------------------- helpers
def _parse_hhmm(value: str):
    """Parse an 'HH:MM' IST clock string into a ``datetime.time`` (owner input; not a trading 'now')."""
    from datetime import time as _time

    hh, mm = value.split(":")[:2]
    return _time(int(hh), int(mm))


def _mount_dashboard_if_present(app: FastAPI, settings: Settings) -> None:
    """Serve the built React dashboard at ``/`` if a ``web/dist`` build exists (R8). No-op otherwise so
    the API runs headless in dev / before the front-end is built."""
    from pathlib import Path

    data_dir = settings.logs_dir().parent if hasattr(settings, "logs_dir") else Path.cwd()
    candidates = [Path.cwd() / "web" / _DASHBOARD_DIST_DIRNAME, data_dir / "web" / _DASHBOARD_DIST_DIRNAME]
    for dist in candidates:
        if dist.is_dir():
            app.mount("/", StaticFiles(directory=str(dist), html=True), name="dashboard")
            _log.info("dashboard_mounted", path=str(dist))
            return
    _log.info("dashboard_not_built", note="no web/dist found; serving API only")
