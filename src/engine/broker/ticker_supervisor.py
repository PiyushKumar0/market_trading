"""Ticker subprocess supervisor (§3.2.2, A4/A3/R2, §2.2, §2.4, §2.6).

``TickerSupervisor`` owns the **mt-ticker** Twisted child process (``ticker/main.py``) and the
localhost TCP link it ships data on. The Twisted reactor cannot be restarted in-process, so A4 forces
the ticker into a *separate process*: when the feed has to be respawned (heartbeat silence, A3 KiteTicker
death), the supervisor kills and re-launches a fresh child rather than trying to restart a reactor inside
the engine. This module is the only thing that spawns / terminates that child and the single owner of its
health signal.

Data path (A3/§2.5):
    KiteTicker → mt-ticker child → length-prefixed msgpack frames on 127.0.0.1:<tcp_port> → here.
The child forwards both **ticks** and **order updates** (``type:"order"``, A3) on the same socket, plus a
**heartbeat** frame every 1 s. Because order updates drive the OMS state machine, a fabricated frame must
not be able to inject a phantom fill — hence the §2.4 trust boundary below.

Trust boundary (§2.4):
    * the TCP socket binds ``127.0.0.1`` only (loopback, not routable);
    * a per-spawn **shared-secret handshake** — a fresh random secret is generated for each spawn and
      handed to the child out-of-band (env/stdin), and the child must echo it on its first frame before
      any tick/order frame is accepted;
    * a **parent-PID check** — the child verifies it is still owned by this engine process and exits if
      orphaned, and the supervisor closes the child's stdin on stop so an orphaned child dies on its own.

Health / stale-data guard (R2, §2.6):
    ``health()`` reports a :class:`FeedHealth` whose ``state`` drives the §7.1 ``stale_data_guard``:
        * ``STOPPED``  — no child running.
        * ``WARMING``  — child just spawned / reconnecting + warm-up backfilling; feed-stale alarm and
          respawn are **suppressed** (§2.6/§3.2.12) to avoid false alarms on a fresh startup, distinct
          from feed-lost-while-running.
        * ``HEALTHY``  — heartbeats arriving within the silence budget.
        * ``STALE``    — heartbeat silence exceeded ``settings.ticker.heartbeat_silence_kill_s`` (10 s)
          while *running* (not WARMING) ⇒ the supervisor kills + respawns the child and publishes a
          ``feed.health`` transition to ``STALE`` (R2/A4).

Subscription set (§3.2.2):
    universe watchlist + held symbols + **NIFTY 50 index** + **India VIX** tokens — the index/VIX feed
    powers §6.2 market-context features and the §7.1 stale-data guard; advance-decline is derived from
    the universe ticks. Changes go over a **control frame** on the live TCP link via
    :meth:`update_subscriptions`, not a respawn.

Phase 1 scope: real subprocess spawn/terminate (``asyncio.create_subprocess_exec``), per-spawn
secret + stdin orphan-protection, a real :meth:`health` computed from ``clock.now()``, AND the real
framing: the loopback ``asyncio`` server, the §2.4 handshake validation, length-prefixed msgpack
frame parsing (tick → :class:`~engine.core.types.Tick` → ``bus.publish("tick", …)``; order →
``order.update``; heartbeat → health), the ``subscribe`` control frame, and the stdin credentials
frame.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets as _secrets
import struct
import sys
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgpack
from pydantic import BaseModel, Field

from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Tick

_log = get_logger("engine.broker.ticker_supervisor")

#: Canonical event bus topic for feed-health transitions (§3.2.1).
FEED_HEALTH_TOPIC = "feed.health"
#: Canonical event bus topic for parsed live ticks (§3.2.1) — consumed by ``BarBuilder`` (§3.2.3).
TICK_TOPIC = "tick"
#: Canonical event bus topic for broker order postbacks (§3.2.1, A3) — drives the OMS (§3.5.1).
ORDER_UPDATE_TOPIC = "order.update"

#: Length prefix on every frame: 4-byte big-endian unsigned int (mirror of ticker/main.py).
_LEN_PREFIX = struct.Struct(">I")
#: Wire-protocol version we accept from the child's ``hello`` (ticker/main.py ``_PROTOCOL_VERSION``).
PROTOCOL_VERSION = 1


class OrderUpdateFrame(BaseModel):
    """A verbatim Kite order postback (A3) as forwarded by the mt-ticker child.

    ``data`` is the raw ``on_order_update`` payload, untouched — the OMS correlates it against
    platform orders on the broker's own field names (§3.5.1). Published on ``order.update``.
    """

    data: dict[str, Any] = Field(default_factory=dict)


def _wire_decimal(value: Any) -> Decimal | None:
    """A wire decimal-string (or number) → ``Decimal``; None/empty passes through (§3.2 money)."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _wire_timestamp(value: Any) -> datetime | None:
    """A wire ISO-8601 NAIVE-IST timestamp → tz-aware IST ``datetime`` (§3.2 convention).

    ticker/main.py forwards KiteTicker's naive IST wall time as an ISO string; we attach
    ``Asia/Kolkata`` here. A tz-aware value (defensive) is converted, not re-stamped.
    """
    if value is None or value == "":
        return None
    ts = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(ts, datetime):
        return None
    return ts.replace(tzinfo=IST) if ts.tzinfo is None else ts.astimezone(IST)


def parse_tick_frame(frame: dict[str, Any], tradingsymbol: str) -> Tick:
    """Parse one PINNED wire tick frame (ticker/main.py ``_frame_tick``) into a core ``Tick``.

    The exact mirror of the child's serializer: prices arrive as decimal strings and re-wrap to
    ``Decimal`` exactly; ``volume_traded`` is the broker's CUMULATIVE day volume, verbatim (A13);
    ``exchange_timestamp`` is naive-IST ISO and becomes tz-aware IST. ``tradingsymbol`` is resolved
    by the caller (the wire carries only the instrument token). Raises ``ValueError`` on a frame
    missing its load-bearing fields — the caller logs and drops it (never crashes the read loop).
    """
    ltp = _wire_decimal(frame.get("last_price"))
    if ltp is None:
        raise ValueError("tick frame missing last_price")
    exchange_ts = _wire_timestamp(frame.get("exchange_timestamp"))
    if exchange_ts is None:
        raise ValueError("tick frame missing exchange_timestamp")
    token = frame.get("instrument_token")
    if token is None:
        raise ValueError("tick frame missing instrument_token")
    ohlc = frame.get("ohlc") or {}
    depth = frame.get("depth") or {}
    buy = depth.get("buy") or []
    sell = depth.get("sell") or []
    return Tick(
        instrument_token=int(token),
        tradingsymbol=tradingsymbol,
        ltp=ltp,
        volume_traded=int(frame.get("volume_traded") or 0),
        exchange_ts=exchange_ts,
        ohlc_open=_wire_decimal(ohlc.get("open")),
        ohlc_high=_wire_decimal(ohlc.get("high")),
        ohlc_low=_wire_decimal(ohlc.get("low")),
        ohlc_close=_wire_decimal(ohlc.get("close")),
        avg_price=_wire_decimal(frame.get("average_traded_price")),
        bid=_wire_decimal(buy[0].get("price")) if buy else None,
        ask=_wire_decimal(sell[0].get("price")) if sell else None,
    )

# WIRE CONTRACT (single source of truth — ticker/main.py implements exactly this):
#   * TOPOLOGY: the ENGINE is the TCP SERVER — it listens on 127.0.0.1:<tcp_port> (loopback-only, §2.4)
#     and the child is the CLIENT that connects on spawn (ticker/main.py: reactor.connectTCP). Phase 1
#     runs an asyncio server here (asyncio.start_server) that accepts the child's connection.
#   * SECRETS CHANNEL: shared_secret + parent_pid travel via ENV (below); the Kite api_key + access_token
#     travel via STDIN as the first framed line (§2.4 — never env, never a routable frame — so they never
#     land in the process table). The child reads that one-time credentials line before connecting KiteTicker.
#: Env var carrying the per-spawn shared secret to the child (§2.4 handshake).
_ENV_SHARED_SECRET = "MT_TICKER_SHARED_SECRET"
#: Env var carrying the parent PID so the child can detect orphaning (§2.4).
_ENV_PARENT_PID = "MT_TICKER_PARENT_PID"
#: Env vars carrying the loopback TCP endpoint the ENGINE listens on and the child CONNECTS to (§2.4).
_ENV_TCP_HOST = "MT_TICKER_TCP_HOST"
_ENV_TCP_PORT = "MT_TICKER_TCP_PORT"


class FeedHealth(BaseModel):
    """Health snapshot of the ticker feed; drives the §7.1 ``stale_data_guard`` (R2).

    Attributes
    ----------
    last_tick_age_s:
        Seconds since the last tick frame, or ``None`` if no tick has been seen yet (e.g. WARMING /
        STOPPED). The §7.1 per-symbol max-tick-age check reads this alongside per-symbol ages.
    heartbeat_age_s:
        Seconds since the last 1 s heartbeat frame, or ``None`` if none seen. Silence beyond
        ``heartbeat_silence_kill_s`` (10 s) while running ⇒ kill + respawn.
    state:
        One of ``{"STOPPED", "WARMING", "HEALTHY", "STALE"}``. ``WARMING`` suppresses false feed-stale
        alarms on startup (§2.6).
    """

    last_tick_age_s: float | None = None
    heartbeat_age_s: float | None = None
    state: str = "STOPPED"


class TickerSupervisor:
    """Owns the mt-ticker Twisted subprocess + its localhost TCP link (A4/A3/R2).

    Parameters
    ----------
    settings:
        Provides ``ticker.tcp_host`` / ``ticker.tcp_port`` / ``ticker.heartbeat_silence_kill_s`` /
        ``ticker.max_instruments_per_conn`` (A3 cap).
    clock:
        The single source of "now" — every age in :meth:`health` is derived from ``clock.now()``
        (never a bare ``datetime.now()``; §3.2 convention / R6).
    bus:
        Event bus for ``feed.health`` transitions and the parsed ``tick`` / ``order.update``
        streams. May be ``None`` in bare harnesses/tests — publishing is then skipped.
    symbol_for_token:
        Resolver from instrument token → tradingsymbol (the wire carries only the token; the core
        ``Tick`` requires the symbol). The composition root wires ``InstrumentStore``. A tick whose
        token cannot be resolved is dropped (logged once per token) — downstream consumers are
        keyed by symbol, so an unresolvable tick is unusable.
    api_key:
        Kite api_key handed to the child over stdin together with the access token (§2.4 — never
        env, never a routable frame).
    """

    def __init__(
        self,
        settings: Settings,
        clock: Clock,
        bus: EventBus | None,
        *,
        symbol_for_token: Callable[[int], str | None] | None = None,
        api_key: str = "",
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._bus = bus
        self._symbol_for_token = symbol_for_token
        self._api_key = api_key

        # --- child process state ---
        self._proc: asyncio.subprocess.Process | None = None
        self._shared_secret: str | None = None
        self._access_token: str | None = None
        self._tokens: list[int] = []

        # --- the loopback server + the (single) authenticated child link ---
        self._server: asyncio.AbstractServer | None = None
        self._child_writer: asyncio.StreamWriter | None = None
        self._unresolved_tokens_logged: set[int] = set()

        # --- supervision / read-loop tasks ---
        self._read_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None

        # --- health signal (timestamps as tz-aware IST via clock.now(), R6) ---
        self._state: str = "STOPPED"
        self._last_tick_at = None  # type: ignore[var-annotated]
        self._last_heartbeat_at = None  # type: ignore[var-annotated]
        self._started_at = None  # type: ignore[var-annotated]

        self._lock = asyncio.Lock()  # serialize start/stop/respawn

    # ------------------------------------------------------------------ public API

    async def start(self, tokens: list[int], access_token: str) -> None:
        """Spawn ``ticker/main.py`` as a child and enter the WARMING state (§2.6).

        The subscription ``tokens`` (universe watchlist + held symbols + NIFTY 50 + India VIX) and the
        Kite ``access_token`` are handed to the child; the access token + the per-spawn shared secret
        cross the §2.4 trust boundary out-of-band (env + stdin), never as a routable frame.
        """
        async with self._lock:
            if self._proc is not None and self._proc.returncode is None:
                _log.info("ticker_start_noop_already_running", pid=self._proc.pid)
                return
            self._tokens = list(tokens)
            self._access_token = access_token
            await self._spawn_child()

    async def update_subscriptions(self, tokens: list[int]) -> None:
        """Update the live subscription set via a control frame on the TCP link (no respawn).

        Writes a length-prefixed msgpack ``{"type":"subscribe","tokens":[…]}`` frame; the child
        diffs the set, (un)subscribes, and re-asserts FULL mode (ticker/main.py). The set is capped
        at ``settings.ticker.max_instruments_per_conn`` (A3, ≤3,000/conn).
        """
        cap = self._settings.ticker.max_instruments_per_conn
        if len(tokens) > cap:
            _log.warning("ticker_subscription_over_cap", requested=len(tokens), cap=cap)
        self._tokens = list(tokens)
        if self._proc is None or self._proc.returncode is not None:
            _log.info("ticker_update_subscriptions_deferred_not_running", count=len(self._tokens))
            return
        await self._write_control_frame({"type": "subscribe", "tokens": self._tokens})
        _log.info("ticker_update_subscriptions", count=len(self._tokens))

    async def _write_control_frame(self, obj: dict[str, Any]) -> None:
        """Send one control frame to the connected child (deferred+logged if the link is down)."""
        writer = self._child_writer
        if writer is None:
            _log.info("ticker_control_frame_deferred_no_link", frame_type=obj.get("type"))
            return
        try:
            body = msgpack.packb(obj, use_bin_type=True)
            writer.write(_LEN_PREFIX.pack(len(body)) + body)
            await writer.drain()
        except (ConnectionError, RuntimeError) as exc:
            _log.warning("ticker_control_frame_write_failed", error=str(exc))

    async def stop(self) -> None:
        """Stop the child: close stdin (orphan protection, §2.4) then terminate (A4)."""
        async with self._lock:
            await self._cancel_supervision()
            await self._terminate_child()
            self._set_state("STOPPED")
            _log.info("ticker_stopped")

    def health(self) -> FeedHealth:
        """Return the current :class:`FeedHealth`; ages computed from ``clock.now()`` (R2/R6)."""
        now = self._clock.now()
        last_tick_age = (
            (now - self._last_tick_at).total_seconds() if self._last_tick_at is not None else None
        )
        heartbeat_age = (
            (now - self._last_heartbeat_at).total_seconds()
            if self._last_heartbeat_at is not None
            else None
        )
        return FeedHealth(
            last_tick_age_s=last_tick_age,
            heartbeat_age_s=heartbeat_age,
            state=self._state,
        )

    # ------------------------------------------------------------------ spawn / terminate (real)

    def _ticker_entrypoint(self) -> Path:
        """Resolve ``ticker/main.py`` — the separate Twisted program at the REPO ROOT (§3.2.2).

        ``ticker/`` deliberately lives outside ``src/engine`` (it must never import ``engine.*``,
        §2.2): src/engine/broker/ticker_supervisor.py → parents[3] == the repo root.
        """
        repo_root = Path(__file__).resolve().parents[3]
        return repo_root / "ticker" / "main.py"

    async def _spawn_child(self) -> None:
        """Launch a fresh child with a new per-spawn secret; arm the read + monitor loops.

        The reactor cannot restart in-process (A4), so every (re)spawn is a brand-new OS process. A new
        shared secret per spawn (§2.4) means a frame minted against a previous child's secret is rejected.
        """
        self._shared_secret = _secrets.token_hex(32)
        entrypoint = self._ticker_entrypoint()

        # The ENGINE is the TCP server (WIRE CONTRACT at module top): bind the loopback listener
        # BEFORE spawning the child, or the child's immediate connect would be refused.
        self._read_task = asyncio.create_task(self._read_loop(), name="ticker-read-loop")
        await self._wait_server_ready()

        env = {
            _ENV_SHARED_SECRET: self._shared_secret,
            _ENV_PARENT_PID: str(_current_pid()),
            _ENV_TCP_HOST: self._settings.ticker.tcp_host,
            _ENV_TCP_PORT: str(self._settings.ticker.tcp_port),
        }
        # Inherit the parent environment (PATH, credential-manager access, OAuth token injected at
        # startup per §2.4) and overlay the handshake/loopback vars.
        full_env = {**_os_environ(), **env}

        self._proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(entrypoint),
            stdin=asyncio.subprocess.PIPE,  # kept open = liveness; closing it orphan-kills the child (§2.4)
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=full_env,
        )

        # The access token crosses the trust boundary on stdin (not env, not a frame) so it never lands
        # in the process table; Phase 1 hands the token + initial subscription set here.
        await self._send_startup_handshake()

        self._started_at = self._clock.now()
        self._last_tick_at = None
        self._last_heartbeat_at = None
        self._set_state("WARMING")  # suppress false feed-stale alarms until first ticks (§2.6)

        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="ticker-monitor-loop")

        _log.info(
            "ticker_spawned",
            pid=self._proc.pid,
            entrypoint=str(entrypoint),
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
            tokens=len(self._tokens),
        )

    async def _send_startup_handshake(self) -> None:
        """Deliver the credentials + initial subscriptions to the child over stdin (§2.4).

        One length-prefixed msgpack frame ``{"api_key", "access_token", "tokens"}`` — the SECRETS
        CHANNEL of the wire contract: never env, never a routable frame, so credentials never land
        in the process table. The child reads it before connecting KiteTicker
        (ticker/main.py ``_read_stdin_credentials``). Stdin then stays OPEN as the liveness signal
        (closing it is the orphan-protection kill, §2.4) — the child ignores further stdin bytes.
        """
        proc = self._proc
        if proc is None or proc.stdin is None:
            return
        payload = msgpack.packb(
            {
                "api_key": self._api_key,
                "access_token": self._access_token or "",
                "tokens": list(self._tokens),
            },
            use_bin_type=True,
        )
        try:
            proc.stdin.write(_LEN_PREFIX.pack(len(payload)) + payload)
            await proc.stdin.drain()
        except (ConnectionError, RuntimeError) as exc:  # pragma: no cover - child died mid-spawn
            _log.warning("ticker_stdin_handshake_failed", error=str(exc))
        # Leave stdin OPEN (do NOT close) so the child stays adopted (§2.4 orphan protection).

    async def _terminate_child(self) -> None:
        """Close stdin (orphan protection) then terminate, escalating to kill if it does not exit."""
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.returncode is not None:
            return

        # 1) Close stdin: a well-behaved child treats stdin EOF as "parent gone" and exits (§2.4).
        if proc.stdin is not None and not proc.stdin.is_closing():
            try:
                proc.stdin.close()
            except (OSError, RuntimeError):  # pragma: no cover - stdin already torn down
                pass

        # 2) Graceful terminate, then hard kill if it ignores us (A4 — a wedged reactor must still die).
        try:
            proc.terminate()
        except ProcessLookupError:  # pragma: no cover - exited between checks
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            _log.warning("ticker_terminate_timeout_killing", pid=proc.pid)
            try:
                proc.kill()
            except ProcessLookupError:  # pragma: no cover
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:  # pragma: no cover - OS-level wedge
                _log.error("ticker_kill_timeout", pid=proc.pid)

    # ------------------------------------------------------------------ read loop (the loopback server)

    async def _wait_server_ready(self, timeout_s: float = 5.0) -> None:
        """Poll until :meth:`_read_loop` has bound its listener (or it died / timed out).

        Uses real short sleeps (not Clock) — this is I/O readiness, not trading time.
        """
        for _ in range(int(timeout_s / 0.01)):
            if self._server is not None:
                return
            task = self._read_task
            if task is not None and task.done():
                exc = task.exception() if not task.cancelled() else None
                _log.error("ticker_server_bind_failed", error=str(exc))
                return
            await asyncio.sleep(0.01)
        _log.error("ticker_server_bind_timeout", timeout_s=timeout_s)

    async def _read_loop(self) -> None:
        """The loopback TCP server owning the child's data link (WIRE CONTRACT at module top).

        The ENGINE is the server: ``asyncio.start_server`` on ``127.0.0.1:<tcp_port>`` accepts the
        child's inbound connection (the child is the client, ``reactor.connectTCP``). Frame parsing
        and dispatch live in :meth:`_handle_child_connection`. Cancellation (stop/respawn) closes
        the listener so a respawn can rebind the port.
        """
        server = await asyncio.start_server(
            self._handle_child_connection,
            host=self._settings.ticker.tcp_host,
            port=int(self._settings.ticker.tcp_port),
        )
        self._server = server
        _log.info(
            "ticker_server_listening",
            tcp=f"{self._settings.ticker.tcp_host}:{self._settings.ticker.tcp_port}",
        )
        try:
            await asyncio.Event().wait()  # serve until cancelled (stop/respawn)
        finally:
            self._server = None
            self._child_writer = None
            server.close()
            with contextlib.suppress(Exception):
                await server.wait_closed()

    async def _handle_child_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Authenticate one inbound child connection (§2.4) and pump its frames.

        The FIRST frame must be a ``hello`` echoing this spawn's shared secret — else the
        connection is dropped before any tick/order frame is honoured (a fabricated local frame
        must not be able to inject a phantom fill; loopback bind + per-spawn secret + REST
        reconciliation backstop, §2.4).
        """
        peer = writer.get_extra_info("peername")
        try:
            hello = await self._read_frame(reader)
            if (
                hello is None
                or hello.get("type") != "hello"
                or not self._shared_secret
                or hello.get("secret") != self._shared_secret
            ):
                _log.warning(
                    "ticker_handshake_rejected",
                    peer=str(peer),
                    frame_type=None if hello is None else hello.get("type"),
                )
                return
            _log.info(
                "ticker_handshake_ok",
                child_pid=hello.get("pid"),
                protocol_v=hello.get("v"),
                tokens=hello.get("tokens"),
            )
            self._child_writer = writer
            while True:
                frame = await self._read_frame(reader)
                if frame is None:
                    _log.info("ticker_link_closed", peer=str(peer))
                    return
                await self._handle_frame(frame)
        except asyncio.CancelledError:  # pragma: no cover - normal on stop/respawn
            raise
        except Exception:  # noqa: BLE001 - never let a link error kill the server silently
            _log.exception("ticker_link_error", peer=str(peer))
        finally:
            if self._child_writer is writer:
                self._child_writer = None
            with contextlib.suppress(Exception):
                writer.close()

    @staticmethod
    async def _read_frame(reader: asyncio.StreamReader) -> dict[str, Any] | None:
        """Read one length-prefixed msgpack frame; ``None`` on EOF/connection loss.

        An undecodable body yields ``{}`` (logged) so one corrupt frame never tears the link down —
        the heartbeat-silence guard is the backstop for a systematically broken stream.
        """
        try:
            header = await reader.readexactly(_LEN_PREFIX.size)
            (length,) = _LEN_PREFIX.unpack(header)
            body = await reader.readexactly(length)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None
        try:
            frame = msgpack.unpackb(body, raw=False)
        except Exception as exc:  # noqa: BLE001 - corrupt frame is dropped, not fatal
            _log.warning("ticker_frame_decode_error", error=str(exc))
            return {}
        return frame if isinstance(frame, dict) else {}

    async def _handle_frame(self, frame: dict[str, Any]) -> None:
        """Dispatch one authenticated frame by ``type`` (WIRE CONTRACT at module top)."""
        ftype = frame.get("type")
        if ftype == "heartbeat":
            self._last_heartbeat_at = self._clock.now()
            if self._state == "WARMING":
                # First heartbeat proves the link + child are live: promote WARMING → HEALTHY (§2.6).
                # The §7.1 warm-up ENTRY gate (ops.warmup) is separate — this is feed health only.
                self._set_state("HEALTHY")
                await self._publish_health()
        elif ftype == "tick":
            self._last_tick_at = self._clock.now()
            tick = self._parse_tick(frame)
            if tick is not None and self._bus is not None:
                self._bus.publish(TICK_TOPIC, tick)
        elif ftype == "order":
            # Verbatim Kite postback (A3); the OMS correlates it (§3.5.1).
            if self._bus is not None:
                self._bus.publish(ORDER_UPDATE_TOPIC, OrderUpdateFrame(data=frame.get("data") or {}))
        else:
            _log.warning("ticker_unknown_frame", frame_type=str(ftype))

    def _parse_tick(self, frame: dict[str, Any]) -> Tick | None:
        """Wire tick frame → core ``Tick`` (symbol resolved via the injected resolver); None = drop."""
        token = frame.get("instrument_token")
        symbol: str | None = None
        if token is not None and self._symbol_for_token is not None:
            symbol = self._symbol_for_token(int(token))
        if symbol is None:
            tok = -1 if token is None else int(token)
            if tok not in self._unresolved_tokens_logged:   # log once per token, not per tick
                self._unresolved_tokens_logged.add(tok)
                _log.warning("ticker_tick_symbol_unresolved", instrument_token=tok)
            return None
        try:
            return parse_tick_frame(frame, symbol)
        except Exception:  # noqa: BLE001 - malformed frame is dropped, never crashes the read loop
            _log.exception("ticker_tick_parse_error", instrument_token=token)
            return None

    async def _monitor_loop(self) -> None:
        """Heartbeat-silence watchdog: kill + respawn on >``heartbeat_silence_kill_s`` (R2/A4).

        Runs while a child is alive. Each tick it re-derives the health state from ``clock.now()``; when
        the feed goes STALE while *running* (not WARMING — §2.6 suppression) it triggers a respawn and
        publishes the ``feed.health`` STALE transition. Also reaps an unexpectedly dead child.
        """
        kill_after = float(self._settings.ticker.heartbeat_silence_kill_s)
        try:
            while True:
                await asyncio.sleep(1.0)
                proc = self._proc
                if proc is None:
                    return

                # Child died on its own (KiteTicker crash, A3) — respawn.
                if proc.returncode is not None:
                    _log.warning("ticker_child_exited", pid=proc.pid, code=proc.returncode)
                    await self._respawn(reason="child_exited")
                    return

                heartbeat_age = self._heartbeat_age_s()
                if self._state == "WARMING":
                    # Suppress feed-stale during warm-up; promotion to HEALTHY happens on first
                    # heartbeat in the Phase-1 read loop. Nothing to enforce here yet.
                    continue
                if heartbeat_age is not None and heartbeat_age > kill_after:
                    _log.error(
                        "ticker_heartbeat_silence",
                        heartbeat_age_s=round(heartbeat_age, 3),
                        kill_after_s=kill_after,
                    )
                    self._set_state("STALE")
                    await self._publish_health()  # R2 — STALE transition for the stale-data guard
                    await self._respawn(reason="heartbeat_silence")
                    return
        except asyncio.CancelledError:  # pragma: no cover - normal on stop
            raise

    async def _respawn(self, *, reason: str) -> None:
        """Kill the current child and launch a fresh one (A4 — reactor cannot restart in-process).

        Called from the monitor loop. Cancel the OLD read loop FIRST so it is not leaked when
        :meth:`_spawn_child` overwrites ``self._read_task`` — otherwise the parked read task runs
        forever. The monitor task is NOT cancelled here: the caller *is* the monitor task and returns
        immediately after this awaits, and ``_spawn_child`` installs a fresh monitor task (cancelling
        self would abort the respawn mid-flight).
        """
        async with self._lock:
            _log.warning("ticker_respawn", reason=reason)
            await self._cancel_read_task()
            await self._terminate_child()
            if self._access_token is None:
                _log.error("ticker_respawn_no_access_token")
                self._set_state("STOPPED")
                return
            await self._spawn_child()

    async def _cancel_read_task(self) -> None:
        """Cancel and await only the read loop (idempotent). Used on respawn, where cancelling the
        monitor task — the current caller — would abort the respawn itself."""
        task = self._read_task
        self._read_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _cancel_supervision(self) -> None:
        """Cancel and await the read + monitor tasks (idempotent). Used by :meth:`stop`, never from
        inside the monitor loop (see :meth:`_cancel_read_task`)."""
        for task in (self._read_task, self._monitor_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._read_task = None
        self._monitor_task = None

    # ------------------------------------------------------------------ health helpers

    def _heartbeat_age_s(self) -> float | None:
        if self._last_heartbeat_at is None:
            return None
        return (self._clock.now() - self._last_heartbeat_at).total_seconds()

    def _set_state(self, state: str) -> None:
        if state != self._state:
            _log.info("feed_health_transition", frm=self._state, to=state)
        self._state = state

    async def _publish_health(self) -> None:
        """Publish the current :class:`FeedHealth` on ``feed.health`` (R2). No-op without a bus."""
        if self._bus is None:
            return
        await self._bus.apublish(FEED_HEALTH_TOPIC, self.health())


# --------------------------------------------------------------------------- tiny stdlib indirections
# Wrapped so the rest of the module reads cleanly and so tests can monkeypatch the process identity /
# environment without importing ``os`` at call sites.

def _current_pid() -> int:
    import os

    return os.getpid()


def _os_environ() -> dict[str, str]:
    import os

    return dict(os.environ)
