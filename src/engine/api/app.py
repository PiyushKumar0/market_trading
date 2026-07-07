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

Phase 0 scope: REAL wiring + REAL auth. The read routes return shape-correct placeholders (the live
views land in later phases); the owner WRITE routes that mutate control-plane state are wired to the
real ``ModeManager`` setters where the plan ships them in Phase 0 (``/config/trade_window``). ``POST
/mode`` and the kill routes require the owner two-step pattern — documented here and stubbed for Phase 0.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from engine.api.kite_callback import build_kite_callback_router
from engine.core.config import Settings, load_settings
from engine.core.enums import Actor
from engine.core.log import get_logger
from engine.core.secrets import DASHBOARD_TOKEN, Secrets

_log = get_logger("engine.api.app")

# Path to the built React dashboard (served when present). Owner builds it into ``web/dist`` (R8).
_DASHBOARD_DIST_DIRNAME = "dist"


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


# --------------------------------------------------------------------------- app factory
def create_app(
    *,
    session_manager: Any = None,
    mode_manager: Any = None,
    kill_switch: Any = None,
    secrets: Secrets | None = None,
    clock: Any = None,
    bus: Any = None,
) -> FastAPI:
    """Construct the dashboard ``FastAPI`` app (§3.2.11).

    All dependencies are injected by the composition root (``engine.ops``) so the app is testable and
    holds no module-level singletons. ``secrets`` supplies the bearer token (R10); ``mode_manager``
    backs the trade-window read/write path (§3.2.7). The caller binds it to ``settings.api.host:port``
    via uvicorn — this factory does not start a server.
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
    app.state.settings = settings

    # ----------------------------------------------------------------- unauthenticated: Kite login (R6)
    # MUST be mounted WITHOUT the bearer dependency: Kite redirects the owner's browser here with a
    # ``request_token`` to complete the daily login, and that redirect cannot carry our bearer header.
    app.include_router(build_kite_callback_router(session_manager, clock))

    # ----------------------------------------------------------------- read routes (bearer-auth stubs)
    # Phase 0: each returns a shape-correct placeholder; live views over risk/oms/learning/intelligence
    # state arrive in later phases. Every one is gated by ``Owner`` (R10).
    @app.get("/positions")
    async def positions(_: Owner) -> dict[str, Any]:
        """Open positions with live P&L (read-only view; Phase 2+)."""
        return {"positions": [], "as_of": None}

    @app.get("/orders")
    async def orders(_: Owner) -> dict[str, Any]:
        """Working + recent orders (read-only view; Phase 3+)."""
        return {"orders": []}

    @app.get("/decisions")
    async def decisions(_: Owner) -> dict[str, Any]:
        """Recent Tier-1 action proposals / recommendations (R8 audit view; Phase 2+)."""
        return {"decisions": []}

    @app.get("/verdicts")
    async def verdicts(_: Owner) -> dict[str, Any]:
        """Recent gate verdicts with the mode/risk-state they were issued under (R8; Phase 2+)."""
        return {"verdicts": []}

    @app.get("/risk/headroom")
    async def risk_headroom(_: Owner) -> dict[str, Any]:
        """Per-§7.1 budget/limit headroom (Phase 2+)."""
        return {"headroom": {}}

    @app.get("/budget")
    async def budget(_: Owner) -> dict[str, Any]:
        """SDK-call budget governor state / degrade tier (§5.6; Phase 2+)."""
        return {"budget": {}, "degrade_tier": None}

    @app.get("/learning/status")
    async def learning_status(_: Owner) -> dict[str, Any]:
        """Champion/challenger + nightly-review status (§5.5; Phase 4+)."""
        return {"learning": {}}

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
        """Owner-only tunables (incl. ``analyst_confidence_min``) plus any nightly-reviewer SUGGESTED
        value with its reasoning — the platform SUGGESTS, the owner SETS (§6.3/§5.5). Phase 0 stub."""
        return {"params": {}, "suggestions": []}

    @app.post("/config/params")
    async def set_params(body: ParamBody, _: Owner) -> dict[str, Any]:
        """Owner applies a tunable — every change audited to ``config_audit`` (§6.3). The platform never
        self-applies a suggestion; only this owner-authenticated path SETS. Phase 0 stub (the protected
        param store + audit write land with ``intelligence``/``ProtectedStore`` in a later phase)."""
        _log.warning("param_set_via_api", name=body.name)
        return {"ok": True, "applied": False, "note": "phase-0 stub — param store lands later"}

    # ----------------------------------------------------------------- mode (POST, two-step; R10)
    @app.post("/mode")
    async def set_mode(body: ModeBody, _: Owner) -> dict[str, Any]:
        """Owner mode change. →AUTO requires a TWO-STEP owner confirmation (R10/§3.5.3): the bearer
        token authenticates the request, and a second factor (one-time confirmation phrase) gates the
        AUTO upgrade — ``ModeManager.request_transition`` enforces an authenticated ``OwnerConfirmation``.
        Phase 0 stub: the second-step phrase issue/verify flow lands with ``notify`` (§3.2.11)."""
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"ok": False, "note": "phase-0 stub — two-step mode change lands with notify"},
        )

    # ----------------------------------------------------------------- kill (POST, two-step; R10/§7.2)
    @app.post("/kill")
    async def kill(_: Owner) -> dict[str, Any]:
        """Engage the kill switch — SINGLE-step trigger (fast), owner-authenticated via bearer (§7.2).
        Phase 0 stub: wired to ``KillSwitch.trigger`` in a later phase once the flatten path exists."""
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"ok": False, "note": "phase-0 stub — kill trigger wires with OMS flatten"},
        )

    @app.post("/kill/reset")
    async def kill_reset(_: Owner) -> dict[str, Any]:
        """Reset the kill switch — owner TWO-STEP authenticated flow (R10/§7.2): bearer + a one-time
        confirmation phrase, passed down as an ``OwnerConfirmation`` to ``KillSwitch.owner_reset``.
        Phase 0 stub: the second-step phrase flow lands with ``notify``."""
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"ok": False, "note": "phase-0 stub — two-step kill reset lands with notify"},
        )

    # ----------------------------------------------------------------- WS live stream (R8; Phase 2)
    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        """Live tick / position-P&L / alert stream (R8). Phase 0: accept + send a hello frame; the full
        tick fan-out (held + watch instruments) lands in Phase 2.

        NOTE: the bearer check is enforced manually here (WS handshakes can't use the HTTP ``Depends``
        bearer dependency the same way) — the token may arrive as the ``Authorization`` header or a
        ``token`` query param."""
        if not _ws_authorized(websocket, secrets):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await websocket.accept()
        await websocket.send_json({"event": "hello", "phase": 0, "stream": "live"})
        await websocket.close()

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
