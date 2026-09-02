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
import sqlite3
import threading
import time
from typing import Annotated, Any, Literal

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from engine.api.kite_callback import build_kite_callback_router
from engine.core.config import Settings, load_settings, repo_root
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

#: §7.1 ``catalyst_guard.digest_stale_max_h`` fallback for the READ-ONLY freshness label on
#: ``GET /news/watchlist``, used only when ``limits_engine`` is unwired. Mirrors the pinned fallback in
#: ``engine.datafeeds.news_pipeline`` (duplicated rather than imported: this API module must not drag
#: the whole news pipeline into its import graph for one number, and nothing here can license a trade).
_DIGEST_STALE_MAX_H_FALLBACK = 20

#: Bus topics relayed onto every connected ``/ws/live`` socket (R8). Tick fan-out is Phase 3.
_WS_RELAY_TOPICS = (TOPIC_MODE_CHANGED, TOPIC_RISK_STATE, TOPIC_KILL_STATE, TOPIC_TRADE_WINDOW, TOPIC_BUDGET_STATE)

#: Keepalive ping cadence for a live ``/ws/live`` socket (LAN dashboard / proxies see it as alive).
_WS_PING_INTERVAL_S = 15

#: ``POST /db/query`` row cap: the default an owner gets, and the ceiling a request may ask for
#: (§3.2.11). Enforced with ``fetchmany(max_rows + 1)`` on the result STREAM — the extra row IS the
#: ``truncated`` flag — so an unbounded scan is answered without materializing it in this process.
_DB_QUERY_MAX_ROWS_DEFAULT = 10_000
_DB_QUERY_MAX_ROWS_LIMIT = 100_000

#: ``POST /db/query`` interrupt deadline in seconds: the default, and the longest an owner may ask
#: for (§3.2.11). Past it the query is interrupted at its own connection and answered 504 — this is a
#: trading process first, and an ad-hoc read never holds a worker thread indefinitely.
_DB_QUERY_TIMEOUT_S_DEFAULT = 5.0
_DB_QUERY_TIMEOUT_S_LIMIT = 60.0

#: DuckDB statement types ``POST /db/query`` accepts. Read-only is decided by the PARSER's statement
#: TYPE, never by inspecting the SQL text (§3.2.11). DuckDB 1.5 types ``SHOW``/``DESCRIBE``/
#: ``SUMMARIZE``/``VALUES``/``FROM x``/read-only ``PRAGMA``s as SELECT, and everything that can mutate
#: data, instance settings or the filesystem as its own type (INSERT/UPDATE/DELETE/CREATE/COPY/SET/
#: ATTACH/CALL/PRAGMA/…), so SELECT + EXPLAIN is the whole read-only surface — EXPLAIN is enumerated
#: because a query plan is exactly what an owner debugging a slow read asks for next.
_DB_QUERY_ALLOWED_STATEMENTS = frozenset({duckdb.StatementType.SELECT, duckdb.StatementType.EXPLAIN})

#: SQLite authorizer actions ``POST /db/query`` permits — sqlite's equivalent of DuckDB statement
#: typing (§3.2.11): sqlite asks per action while preparing, and everything outside this set is DENIED
#: before a byte is read, so INSERT/UPDATE/DELETE/ATTACH/PRAGMA/DDL never reach the file.
#: SQLITE_RECURSIVE (2026-08-19 review): WITH RECURSIVE is a read shape (walking order_events
#: chains); the action authorizes recursion only, no write capability — allowed.
_DB_QUERY_SQLITE_ALLOWED_ACTIONS = frozenset(
    {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
)

#: ``POST /db/query`` log-line outcome per HTTP status (§6.5 — every query is logged, including the
#: ones that never ran).
_DB_QUERY_OUTCOMES = {400: "rejected", 503: "unavailable", 504: "timeout"}

#: Prefix of the owner's SQL carried into the log line: enough to identify the query, bounded so a
#: pasted 50 KB statement cannot flood the log.
_DB_QUERY_LOG_SQL_CHARS = 200


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


class DbQueryBody(BaseModel):
    """Owner ad-hoc read payload for ``POST /db/query`` (§3.2.11; owner-directed 2026-08-19).

    ``max_rows``/``timeout_s`` are BOUNDED here rather than clamped silently: an owner who asks for
    more than the ceiling gets a 422 naming it, not a truncated answer they did not ask for."""

    db: Literal["market", "state"]
    sql: str
    max_rows: int = Field(default=_DB_QUERY_MAX_ROWS_DEFAULT, ge=1, le=_DB_QUERY_MAX_ROWS_LIMIT)
    timeout_s: float = Field(default=_DB_QUERY_TIMEOUT_S_DEFAULT, gt=0, le=_DB_QUERY_TIMEOUT_S_LIMIT)


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
    market_store: Any = None,
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
    app.state.market_store = market_store   # MarketStore (news watchlist); `store` is the ProtectedStore
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
        proposals = [json.loads(row["proposal_payload"]) for row in rows]
        subjects = _decision_subjects(conn, proposals)
        out = []
        for row, proposal, subject in zip(rows, proposals, subjects, strict=True):
            verdict_payload = json.loads(row["verdict_payload"]) if row["verdict_payload"] else None
            out.append(
                {
                    "proposal_id": row["proposal_id"],
                    "agent_id": row["agent_id"],
                    "action": row["action"],
                    "proposal": proposal,
                    "subject": subject,
                    "created_at": row["created_at"],
                    "verdict_id": row["verdict_id"],
                    "verdict": row["verdict_outcome"],
                    "reasons": (verdict_payload or {}).get("reasons", []),
                    "evaluated_at": row["evaluated_at"],
                }
            )
        return {"decisions": out}

    @app.get("/recommendations")
    async def recommendations(_: Owner) -> dict[str, Any]:
        """Latest 100 RECOMMEND-mode recommendations, most recently delivered first (§3.6/R8).

        ``/decisions`` is the proposal→verdict PROVENANCE view and carries no ``Recommendation``
        payload; the dashboard's recommendation panel needs the delivered artifact itself (thesis,
        entry zone, stop/targets, gate verdict, ``manual_checklist``) plus the owner's ``human_action``
        (taken | expired | dismissed | closed), which only this table holds."""
        conn = app.state.conn
        if conn is None:
            return {"recommendations": []}
        rows = conn.execute(
            "SELECT rec_id, payload, delivered_at, human_action, human_fill_price, outcome "
            "FROM recommendations ORDER BY COALESCE(delivered_at, '') DESC LIMIT 100"
        ).fetchall()
        out = [
            {
                "rec_id": r["rec_id"],
                "recommendation": json.loads(r["payload"]) if r["payload"] else None,
                "delivered_at": r["delivered_at"],
                "human_action": r["human_action"],
                "human_fill_price": r["human_fill_price"],
                "outcome": json.loads(r["outcome"]) if r["outcome"] else None,
            }
            for r in rows
        ]
        return {"recommendations": out}

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

    @app.get("/news/watchlist")
    async def news_watchlist(_: Owner) -> dict[str, Any]:
        """Today's §2.7 catalyst watchlist — ``originating`` vs ``context`` rows with their event type,
        materiality and DETERMINISTIC §6.1 levels — plus the digest freshness the `cat` fail-safe ladder
        keys off (§2.7). READ-ONLY: this route mirrors the freshness label, it never gates anything.

        Duck-typed like every other read route: rows come off ``store`` when it exposes the news-layer
        readers, and ``digest_stale_max_h`` off ``limits_engine``'s hash-verified ``catalyst_guard``
        block (§7.1). An unwired store / clock degrades to the empty shape with ``status: null`` — an
        UNKNOWN freshness is never reported as one of the three real ``digest_status`` values."""
        store = app.state.market_store if app.state.market_store is not None else app.state.store
        clock = app.state.clock
        get_watchlist = getattr(store, "get_catalyst_watchlist", None)
        if get_watchlist is None or clock is None:
            return {"d": None, "watchlist": [], "digest": _digest_block(None, None, None)}
        d = clock.today()
        rows = [{k: _jsonable(v) for k, v in row.items()} for row in get_watchlist(d)]
        latest_as_of = getattr(store, "latest_sentiment_as_of", None)
        as_of = latest_as_of() if latest_as_of is not None else None
        limits_engine = app.state.limits_engine
        stale_max_h = (
            int(limits_engine.catalyst_guard().digest_stale_max_h)
            if limits_engine is not None
            else _DIGEST_STALE_MAX_H_FALLBACK
        )
        age_h = (clock.now() - as_of).total_seconds() / 3600.0 if as_of is not None else None
        return {"d": d.isoformat(), "watchlist": rows, "digest": _digest_block(as_of, age_h, stale_max_h, rows)}

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

    @app.get("/notifications")
    async def notifications(_: Owner, d: str | None = None) -> dict[str, Any]:
        """One IST day of owner notifications, CHRONOLOGICAL (WO-24d; owner-directed 2026-08-21).

        Reads the ``notifications`` journal every ``TelegramBot.send`` writes to (migration 0011) —
        the same rows the retry outbox drains, so the page can never show a "delivered" the wire never
        saw. Ascending on purpose: this is the day's transcript, read top-to-bottom, not a
        newest-first feed like ``/orders``.

        ``d`` defaults to today off the app's clock. The day is a ``created_at`` RANGE rather than a
        ``substr``/``LIKE`` match so the ``idx_notifications_created_at`` index is usable — every
        timestamp is IST-stamped by :class:`~engine.core.clock.Clock`, and ISO-8601 with a fixed
        ``+05:30`` offset sorts lexicographically, so the range IS the day. Read-only, on a
        per-request cursor that is always closed."""
        clock = app.state.clock
        raw = d if d else (clock.today().isoformat() if clock is not None else None)
        if raw is None:
            return {"d": None, "rows": []}
        try:
            day, next_day = _day_bounds(raw)
        except ValueError:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY, f"invalid date {raw!r}; expected ISO YYYY-MM-DD"
            ) from None
        conn = app.state.conn
        if conn is None:
            return {"d": day, "rows": []}
        cursor = conn.cursor()
        try:
            rows = cursor.execute(
                "SELECT * FROM notifications WHERE created_at >= ? AND created_at < ? "
                "ORDER BY created_at ASC",
                (day, next_day),
            ).fetchall()
        finally:
            cursor.close()
        return {"d": day, "rows": [dict(r) for r in rows]}

    # ----------------------------------------------------------------- ad-hoc DB read (POST, §3.2.11)
    @app.post("/db/query")
    async def db_query(body: DbQueryBody, _: Owner) -> dict[str, Any]:
        """Owner ad-hoc SELECT over the live databases (owner-directed 2026-08-19). Motivated by the
        2026-08-18 incidents: DuckDB's one-writer-PROCESS model made read-only questions cost engine
        STOPS (three in one day, one of them mid-session), so the process that already HOLDS the
        database answers them instead.

        Read-only is decided by STATEMENT TYPE, never by inspecting the SQL text: ``market`` takes
        DuckDB's parser verdict (:data:`_DB_QUERY_ALLOWED_STATEMENTS`), ``state`` runs under SQLite's
        authorizer (:data:`_DB_QUERY_SQLITE_ALLOWED_ACTIONS`) on a ``mode=ro`` connection —
        INSERT/COPY/SET/ATTACH/DDL and multi-statement bodies are 400, so instance-global settings and
        the filesystem write path are unreachable from here. Both dbs execute on a worker thread
        against a PER-REQUEST cursor/connection (never the loop — §2.2 heartbeat invariant; never a
        shared cursor), under the row cap and an interrupt deadline (504).

        Accepted residual (owner-directed): a genuine SELECT can still read local files through
        DuckDB's ``read_*`` table functions — the single-owner bearer token IS the boundary (R10)."""
        started = time.perf_counter()
        try:
            if not body.sql.strip():
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "sql is empty")
            if body.db == "market":
                columns, rows, truncated = await _db_query_market(app.state, body)
            else:
                columns, rows, truncated = await _db_query_state(app.state.conn, body)
        except HTTPException as exc:
            outcome = _DB_QUERY_OUTCOMES.get(exc.status_code, "error")
            _log_db_query(body, outcome, _elapsed_ms(started), detail=str(exc.detail))
            raise
        elapsed_ms = _elapsed_ms(started)
        _log_db_query(body, "ok", elapsed_ms, row_count=len(rows), truncated=truncated)
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "elapsed_ms": elapsed_ms,
        }

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
    # ORDER MATTERS: the dashboard mount is a catch-all at "/", so every narrower route has to be
    # registered before it or the SPA swallows the path (Starlette takes the FIRST matching route).
    _add_notifications_ui_route(app)
    _mount_dashboard_if_present(app, settings)

    _log.info("api_app_created", host=settings.api.host, port=settings.api.port)
    return app


# --------------------------------------------------------------------------- /decisions subject
def _decision_subjects(conn: Any, proposals: list[dict[str, Any]]) -> list[str | None]:
    """The human subject of each stored ``ActionProposal``, positionally aligned with ``proposals``.

    Only ``enter`` names its instrument; ``exit`` / ``modify-*`` carry a ``position_id`` and ``cancel``
    an ``order_id`` (``engine.core.contracts``), which is what the decision log used to print for the
    owner's own positions (reported 2026-09-02). Resolved here, in two batched lookups, rather than in
    the dashboard: ``/positions`` is unbounded today but need not stay so, and the provenance view
    should carry its subject. An id whose row is gone stays visible as the id — never blank."""
    order_ids = sorted({p["order_id"] for p in proposals if isinstance(p.get("order_id"), str)})
    position_of_order: dict[str, str] = {}
    if order_ids:
        marks = ",".join("?" * len(order_ids))
        position_of_order = {
            r["order_id"]: r["position_id"]
            for r in conn.execute(
                f"SELECT order_id, position_id FROM orders WHERE order_id IN ({marks})", order_ids
            )
            if r["position_id"]
        }
    position_ids = sorted(
        {p["position_id"] for p in proposals if isinstance(p.get("position_id"), str)}
        | set(position_of_order.values())
    )
    symbol_of_position: dict[str, str] = {}
    if position_ids:
        marks = ",".join("?" * len(position_ids))
        symbol_of_position = {
            r["position_id"]: r["symbol"]
            for r in conn.execute(
                f"SELECT position_id, symbol FROM positions WHERE position_id IN ({marks})", position_ids
            )
        }

    out: list[str | None] = []
    for p in proposals:
        symbol = p.get("tradingsymbol")
        if isinstance(symbol, str) and symbol:
            out.append(symbol)
            continue
        order_id = p.get("order_id") if isinstance(p.get("order_id"), str) else None
        position_id = p.get("position_id") if isinstance(p.get("position_id"), str) else None
        position_id = position_id or (position_of_order.get(order_id) if order_id else None)
        if position_id:
            out.append(symbol_of_position.get(position_id, position_id))
        else:
            out.append(order_id)
    return out


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
    OWN topic name, not whichever topic the enclosing loop last iterated to (R8).

    The broadcast is FIRE-AND-FORGET (2026-07-28 review): ``KillSwitch.trigger`` and the mode/risk
    setters ``apublish`` their events — i.e. they AWAIT every subscribed handler — so an awaited
    broadcast would let one stalled dashboard socket wedge the KILL sequence. A dropped frame costs
    a dashboard refresh; a wedged kill path costs capital (R10)."""

    async def _handler(event: Any) -> None:
        payload = event.model_dump(mode="json") if hasattr(event, "model_dump") else event
        task = asyncio.ensure_future(hub.broadcast(topic, payload))
        task.add_done_callback(_log_ws_relay_result)

    return _handler


def _log_ws_relay_result(task: "asyncio.Task[Any]") -> None:
    exc = task.exception() if not task.cancelled() else None
    if exc is not None:
        _log.warning("ws_relay_broadcast_failed", error=str(exc))


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
def _jsonable(value: Any) -> Any:
    """Render one store row value for the wire. Store rows are DuckDB-native, so prices arrive as
    ``Decimal`` and days/timestamps as ``date``/``datetime``: Decimals cross as STRINGS (§8.1
    decimal-as-string — FastAPI's default encoder would coerce them to float and corrupt a level).

    ``bytes``/``dict`` are handled for ``POST /db/query``, which returns whatever column the owner
    asked for: FastAPI's encoder UTF-8-decodes bytes (a 500 on any real BLOB — ``agent_calls.context_gz``
    is gzip) and never looks inside a STRUCT for the Decimals it would then float."""
    from datetime import date as _date
    from decimal import Decimal as _Decimal

    if isinstance(value, _Decimal):
        return str(value)
    if isinstance(value, _date):  # covers datetime (a date subclass) — both go out ISO-8601
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


# --------------------------------------------------------------------------- /db/query (§3.2.11)
async def _db_query_market(app_state: Any, body: DbQueryBody) -> tuple[list[str], list[list[Any]], bool]:
    """Validate + run one owner SELECT on a PER-REQUEST cursor of the LIVE ``MarketStore`` connection.

    ``con.cursor()`` is a fresh DuckDB connection onto the SAME instance: it sees every committed row
    the engine has written, contends for none of the store's write lock, and its ``interrupt()`` cancels
    only its own query (verified 2026-08-19 — the parent connection's in-flight work completes
    untouched). Riding the store INSTANCE is forced rather than preferred: §4.1 gives exactly one
    process ``market.duckdb`` and DuckDB's file lock refuses a second ``connect()``. The store is
    duck-typed off ``market_store`` then ``store``, exactly like ``/news/watchlist``; its connection
    accessor is private because there is no public one and widening the store API is not this
    endpoint's business."""
    store = app_state.market_store if app_state.market_store is not None else app_state.store
    require_con = getattr(store, "_require_con", None)
    if require_con is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "market store unavailable")
    try:
        con = require_con()
    except RuntimeError as exc:  # store constructed but never open()ed
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from None
    cursor = con.cursor()
    try:
        _reject_non_read_duckdb(cursor, body.sql)
        return await asyncio.to_thread(_run_duckdb, cursor, body)
    finally:
        cursor.close()


def _reject_non_read_duckdb(cursor: Any, sql: str) -> None:
    """400 anything that is not EXACTLY ONE read-only DuckDB statement, by asking the PARSER for the
    statement TYPE (§3.2.11). Text inspection is not a mechanism here — it cannot see through comments,
    quoting or CTEs, and DuckDB already exposes the parser's own verdict."""
    try:
        statements = cursor.extract_statements(sql)
    except duckdb.Error as exc:  # a parse failure is the owner's typo, not a server fault
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"sql did not parse: {exc}") from None
    if len(statements) != 1:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"exactly one statement required (parsed {len(statements)})"
        )
    kind = statements[0].type
    if kind not in _DB_QUERY_ALLOWED_STATEMENTS:
        allowed = ", ".join(sorted(t.name for t in _DB_QUERY_ALLOWED_STATEMENTS))
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"statement type {kind.name} is not read-only (allowed: {allowed})",
        )


def _run_duckdb(cursor: Any, body: DbQueryBody) -> tuple[list[str], list[list[Any]], bool]:
    """Execute the validated statement on a worker thread under an interrupt deadline.

    ``fetchmany(max_rows + 1)`` — never ``fetchall`` — enforces the cap on the RESULT STREAM: the one
    extra row IS the ``truncated`` flag, so an unbounded scan answers without landing in this process's
    heap. The timer fires ``interrupt()`` on this request's own cursor, so a slow owner query cannot
    take anything else down with it."""
    deadline = threading.Timer(body.timeout_s, cursor.interrupt)
    deadline.start()
    try:
        cursor.execute(body.sql)
        columns = [d[0] for d in cursor.description or ()]
        rows = cursor.fetchmany(body.max_rows + 1)
    except duckdb.InterruptException:
        raise HTTPException(
            status.HTTP_504_GATEWAY_TIMEOUT, f"query interrupted after timeout_s={body.timeout_s:g}"
        ) from None
    except duckdb.Error as exc:  # unknown table/column, type error — the owner's SQL, answered as 400
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"query failed: {exc}") from None
    finally:
        deadline.cancel()
    return _capped(columns, rows, body.max_rows)


async def _db_query_state(conn: Any, body: DbQueryBody) -> tuple[list[str], list[list[Any]], bool]:
    """Validate + run one owner SELECT on a PER-REQUEST read-only SQLite connection (worker thread).

    The file comes off the app's OWN connection (``PRAGMA database_list``), never ``settings``, so this
    surface can never answer from a different database than the engine is running on."""
    if conn is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "state db unavailable")
    if not sqlite3.complete_statement(_semicolon_terminated(body.sql)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "sql is not a complete statement")
    return await asyncio.to_thread(_run_sqlite, _state_db_uri_path(conn), body)


def _semicolon_terminated(sql: str) -> str:
    """``sqlite3.complete_statement`` judges a SEMICOLON-terminated string (the stdlib REPL's own
    idiom), so an owner query without the trailing ';' is given one for the completeness check only."""
    return sql if sql.rstrip().endswith(";") else sql + ";"


def _state_db_uri_path(conn: Any) -> str:
    """The file the app's SQLite connection is attached to, percent-encoded for a ``file:`` URI."""
    from pathlib import Path
    from urllib.parse import quote

    for row in conn.execute("PRAGMA database_list").fetchall():
        if row["name"] == "main" and row["file"]:
            return quote(Path(row["file"]).as_posix(), safe="/:")
    raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "state db is not file-backed")


def _run_sqlite(db_uri_path: str, body: DbQueryBody) -> tuple[list[str], list[list[Any]], bool]:
    """Execute the owner statement on a fresh ``mode=ro`` connection under an interrupt deadline.

    Three independent read-only guarantees, none of them text inspection (§3.2.11): the connection is
    opened READ-ONLY (a WAL reader — the engine keeps writing throughout), the authorizer DENIES every
    action outside :data:`_DB_QUERY_SQLITE_ALLOWED_ACTIONS` while sqlite prepares the statement, and
    sqlite3 itself refuses more than one statement per ``execute``. All three answer 400.

    ``check_same_thread=False`` because the deadline timer touches the connection from its own thread.
    The deadline is FLAGGED rather than sniffed out of the message: sqlite3 reports an interrupt as a
    plain ``OperationalError('interrupted')``, indistinguishable by type from a real SQL error."""
    con = sqlite3.connect(f"file:{db_uri_path}?mode=ro", uri=True, check_same_thread=False)
    con.row_factory = None  # plain tuples — the wire shape is rows-as-lists, not rows-as-objects
    con.set_authorizer(_db_query_sqlite_authorizer)
    timed_out = threading.Event()

    def _on_deadline() -> None:
        timed_out.set()
        con.interrupt()

    deadline = threading.Timer(body.timeout_s, _on_deadline)
    deadline.start()
    try:
        cursor = con.execute(body.sql)
        columns = [d[0] for d in cursor.description or ()]
        rows = cursor.fetchmany(body.max_rows + 1)
    except sqlite3.Error as exc:
        if timed_out.is_set():
            raise HTTPException(
                status.HTTP_504_GATEWAY_TIMEOUT, f"query interrupted after timeout_s={body.timeout_s:g}"
            ) from None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"query rejected: {exc}") from None
    finally:
        deadline.cancel()
        con.close()
    return _capped(columns, rows, body.max_rows)


def _db_query_sqlite_authorizer(action: int, *_args: Any) -> int:
    """SQLite's per-action gate for ``POST /db/query`` — sqlite's equivalent of DuckDB statement typing
    (§3.2.11). Everything outside the read set is DENIED as the statement is prepared, so
    INSERT/UPDATE/DELETE/ATTACH/PRAGMA/DDL never reach the (already read-only) file."""
    return sqlite3.SQLITE_OK if action in _DB_QUERY_SQLITE_ALLOWED_ACTIONS else sqlite3.SQLITE_DENY


def _capped(columns: list[str], rows: list[Any], max_rows: int) -> tuple[list[str], list[list[Any]], bool]:
    """Shape one over-fetched result for the wire: drop the cap-probe row, flag the truncation."""
    return columns, [[_jsonable(v) for v in row] for row in rows[:max_rows]], len(rows) > max_rows


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 1)


def _log_db_query(
    body: DbQueryBody,
    outcome: str,
    elapsed_ms: float,
    *,
    row_count: int | None = None,
    truncated: bool | None = None,
    detail: str | None = None,
) -> None:
    """One INFO line per owner query, run or rejected (§3.2.11/§6.5) — the audit trail for a surface
    that can read any row in either database."""
    _log.info(
        "db_query",
        db=body.db,
        sql=body.sql[:_DB_QUERY_LOG_SQL_CHARS],
        outcome=outcome,
        row_count=row_count,
        truncated=truncated,
        elapsed_ms=elapsed_ms,
        detail=detail,
    )


def _digest_block(
    as_of: Any, age_h: float | None, stale_max_h: int | None, rows: list[Any] | None = None
) -> dict[str, Any]:
    """The ``/news/watchlist`` freshness sub-object, mirroring ``CatalystDigestJob.digest_status``:
    no ``sentiment_agg`` stamp at all ⇒ ``missing`` (or ``stale`` when day ``d`` HAS watchlist rows —
    an age that cannot be established is never "fresh"); otherwise ``stale`` past
    ``catalyst_guard.digest_stale_max_h``. ``status`` is ``None`` only when the store is unwired."""
    if stale_max_h is None:
        return {"as_of": None, "age_h": None, "stale_max_h": None, "status": None}
    if as_of is None:
        status = "stale" if rows else "missing"
    else:
        status = "stale" if (age_h or 0.0) > float(stale_max_h) else "fresh"
    return {
        "as_of": as_of.isoformat() if as_of is not None else None,
        "age_h": round(age_h, 3) if age_h is not None else None,
        "stale_max_h": stale_max_h,
        "status": status,
    }


def _day_bounds(value: str) -> tuple[str, str]:
    """``'2026-08-21'`` → ``('2026-08-21', '2026-08-22')``, the half-open range one IST day of
    ISO-8601 ``created_at`` strings falls in. Raises ``ValueError`` on anything that is not an ISO
    date, which the caller answers 422 — a malformed ``?d=`` must never silently return "no rows"."""
    from datetime import date as _date
    from datetime import timedelta as _timedelta

    day = _date.fromisoformat(value)
    return day.isoformat(), (day + _timedelta(days=1)).isoformat()


def _parse_hhmm(value: str):
    """Parse an 'HH:MM' IST clock string into a ``datetime.time`` (owner input; not a trading 'now')."""
    from datetime import time as _time

    hh, mm = value.split(":")[:2]
    return _time(int(hh), int(mm))


#: URL path of the WO-24d notifications page, served as an explicit ROUTE rather than as a candidate
#: of the "/" dashboard mount below. Two reasons, both load-bearing:
#:
#: * ``<repo>/dashboard/dist`` already exists on the LAN box and is the FIRST candidate of that
#:   mount, so ``web/dist`` would never be reached — and promoting it above the React build would
#:   silently replace the whole dashboard with this one page. A separate path also keeps a
#:   hand-written, dependency-free page out of reach of the next ``npm run build``.
#: * A ``StaticFiles`` MOUNT here would answer ``/notifications-ui/`` but 404 the bare
#:   ``/notifications-ui`` the owner actually types: Starlette only issues its add-the-slash redirect
#:   when NO route matched, and the catch-all dashboard mount at "/" always matches. Registering the
#:   real path (and its trailing-slash twin) sidesteps that entirely — the page is ONE self-contained
#:   file, so a static-directory mount buys nothing anyway.
_NOTIFICATIONS_UI_PATHS = ("/notifications-ui", "/notifications-ui/")


def _add_notifications_ui_route(app: FastAPI) -> None:
    """Serve the self-contained notifications page (``<repo>/web/dist/index.html``) at
    :data:`_NOTIFICATIONS_UI_PATHS`. 404s when the file is absent, so a checkout without it still
    runs the API — same degrade-quietly posture as the dashboard mount.

    UNAUTHENTICATED, by necessity and by design: a browser navigating to a URL cannot attach an
    ``Authorization`` header, and this app has no auth MIDDLEWARE — auth is per-route via
    ``Depends(_require_owner)``, which this route deliberately does not carry. What ships here is
    therefore an empty SHELL: it holds no state, prompts for the dashboard token itself, and every
    byte it displays comes from bearer-authed ``GET /notifications`` (R10). Same posture as the
    existing "/" dashboard mount — the LAN bind plus the token is the boundary."""
    from pathlib import Path

    from fastapi.responses import FileResponse

    page: Path = repo_root() / "web" / _DASHBOARD_DIST_DIRNAME / "index.html"

    async def notifications_ui() -> Any:
        if not page.is_file():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "notifications page is not installed")
        # no-store: the page is the owner's live view of an outage; a cached shell pointing at a
        # stale token prompt is worse than a round trip on a LAN.
        return FileResponse(page, media_type="text/html", headers={"Cache-Control": "no-store"})

    for path in _NOTIFICATIONS_UI_PATHS:
        app.add_api_route(path, notifications_ui, methods=["GET"], include_in_schema=False)
    _log.info("notifications_ui_route", path=str(page), url=_NOTIFICATIONS_UI_PATHS[0])


def _mount_dashboard_if_present(app: FastAPI, settings: Settings) -> None:
    """Serve the built React dashboard at ``/`` if a build exists (R8). No-op otherwise so the API runs
    headless in dev / before the front-end is built.

    ``<repo>/dashboard/dist`` is the FIRST candidate and the real one (D3): the front-end source lives
    in ``dashboard/`` and ``npm run build`` writes there. It is resolved from ``repo_root()`` — NOT from
    ``Path.cwd()`` — because the engine is normally launched as a service whose working directory is
    not the repo. The legacy ``web/dist`` locations stay as fallbacks."""
    from pathlib import Path

    data_dir = settings.logs_dir().parent if hasattr(settings, "logs_dir") else Path.cwd()
    candidates = [
        repo_root() / "dashboard" / _DASHBOARD_DIST_DIRNAME,
        Path.cwd() / "web" / _DASHBOARD_DIST_DIRNAME,
        data_dir / "web" / _DASHBOARD_DIST_DIRNAME,
    ]
    for dist in candidates:
        if dist.is_dir():
            app.mount("/", StaticFiles(directory=str(dist), html=True), name="dashboard")
            _log.info("dashboard_mounted", path=str(dist))
            return
    _log.info("dashboard_not_built", note="no dashboard/dist or web/dist found; serving API only")
