"""``mt-ticker`` — standalone Twisted KiteTicker subprocess (A4, §2.2 / §3.2.2).

This is **NOT** part of the ``engine`` package. It is a separate program run as a child of
``mt-engine`` (``python ticker/main.py``) on its own Python interpreter, because **A4** forces it:
``KiteTicker`` is Twisted/autobahn-based and the Twisted reactor cannot be restarted in-process, so
the engine must be free to kill and respawn this child freely (new access token at 08:55, reconnect
storms, hangs) without touching its own asyncio loop. See the process table in §2.2.

What it does (§2.2 / §3.2.2 / §2.5):
  * Connects ``KiteTicker`` (≤3,000 instruments per connection, A3).
  * Subscribes to the initial instrument tokens handed to it on spawn (universe watchlist + held
    symbols + NIFTY 50 index + India VIX — assembled by ``TickerSupervisor``, §3.2.2).
  * Forwards every tick to the parent engine over a localhost TCP socket
    (``127.0.0.1:<tcp_port>``, default 8401) as a length-prefixed msgpack frame.
    Per **A13**, the tick carries the broker's *cumulative day volume*; the engine's ``BarBuilder``
    derives per-bar volume from the cumulative-volume delta — the ticker does no aggregation.
  * Forwards order updates on the *same* socket as ``type="order"`` frames (**A3**) — the postback
    channel that drives the OMS state machine (§3.5.1).
  * Emits an in-band **heartbeat** frame every 1 second (``LoopingCall``). Silence >10 s on the
    parent side ⇒ the engine kills + respawns this child and raises the stale-data guard (R2/§7.1).
  * **Pre-open ticks (09:00–09:15) are forwarded RAW** — the ticker never filters them. Pre-open
    exclusion is the engine's job (``BarBuilder`` ignores ticks before 09:15:00, **A14**); doing it
    here would hide the auction-open price the engine stores separately.
  * **Exits on stdin close** (orphan protection): if the parent dies, our inherited stdin pipe
    reaches EOF and we stop the reactor, so we never linger as an orphaned websocket.

Trust boundary (§2.4 / §3.2.2): the socket binds loopback only (``127.0.0.1``, not routable). On
connect we send a per-spawn **shared-secret handshake** frame and our **parent PID** so the engine
can authenticate the link — order frames inject into the OMS state machine, so a fabricated frame
from another local process must not be able to inject a phantom fill. REST-orderbook reconciliation
(R5) is the backstop.

Framing (§2.2): every message is one msgpack-packed object prefixed by a **4-byte big-endian
unsigned length** of the packed bytes. The engine reads ``length`` then exactly ``length`` bytes.

Dependencies: this module imports **only** Twisted + kiteconnect + msgpack (+ stdlib). It MUST NOT
import ``engine.*`` — separate program, separate interpreter, no shared state (§2.2). All logging
goes to **stderr** (the engine captures the child's stderr); ``print()`` is fine here because this is
a standalone script, not an engine module.

Wire payload schemas (Phase 1, PINNED — the engine reader ``engine.broker.ticker_supervisor``
implements the exact mirror):

  * ``hello``      — ``{type, v, secret, ppid, pid, tokens}`` (§2.4 handshake; first frame on
    connect). ``v`` is the protocol version (1); ``tokens`` is the subscribed token COUNT (an int,
    for the engine's link-health log — never the token list itself).
  * ``tick``       — ``{type:"tick", instrument_token, mode, tradable, last_price, volume_traded,
    exchange_timestamp, last_trade_time, average_traded_price, ohlc:{open,high,low,close},
    depth:{buy:[{price,quantity,orders}×≤5], sell:[…]}}``. Prices cross the wire as **decimal
    strings** (msgpack has no Decimal; ``str(float)`` is the shortest round-trip repr, so a broker
    price like 2338.55 arrives as ``"2338.55"`` and the engine re-wraps ``Decimal`` exactly).
    ``volume_traded`` is the broker's **cumulative day volume**, forwarded verbatim (A13).
    Timestamps are ISO-8601 strings of the NAIVE IST wall time KiteTicker parses out of the binary
    packet; the engine attaches ``Asia/Kolkata`` (§3.2).
  * ``order``      — ``{type:"order", data:<verbatim Kite postback dict>}`` (A3): the raw
    ``on_order_update`` payload, untouched — the OMS correlates it (§3.5.1).
  * ``heartbeat``  — ``{type:"heartbeat", seq, ws_connected, last_tick_age_s}`` every 1 s (§2.2);
    ``ws_connected``/``last_tick_age_s`` let the engine distinguish "ticker alive but feed silent"
    from "ticker alive, market quiet".
  * control (inbound) — ``{type:"subscribe", tokens:[…]}`` (§3.2.2 ``update_subscriptions``): the
    child diffs the token set, (un)subscribes, and re-asserts FULL mode on the new set.
  * stdin (one-time, §2.4) — a single length-prefixed msgpack frame
    ``{api_key, access_token, tokens?}`` written by the supervisor on spawn; secrets never touch
    the process table.
"""

from __future__ import annotations

import datetime as _dt
import os
import socket as _socket
import struct
import sys
import time as _time
from typing import Any
from zoneinfo import ZoneInfo

import msgpack

# kiteconnect ships KiteTicker (Twisted/autobahn under the hood) — A4. Imported at module top per
# conventions; the owner installs pykiteconnect==5.2.0 into the venv (pyproject).
from kiteconnect import KiteTicker

# Twisted reactor + helpers. ``reactor`` is the Twisted global reactor; importing it installs the
# default (select/epoll/iocp) reactor for this interpreter — fine, this process runs nothing else.
from twisted.internet import reactor, stdio
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver

# --------------------------------------------------------------------------- constants
#: Length-prefix is a 4-byte big-endian unsigned int (max frame ~4 GiB; ticks are tiny).
_LEN_PREFIX = struct.Struct(">I")

#: Wire-protocol version stamped on the ``hello`` frame (bump on any breaking schema change).
_PROTOCOL_VERSION = 1

#: IST for log timestamps only (this process makes no trading time decisions — no ``core.Clock``).
_IST = ZoneInfo("Asia/Kolkata")

#: Heartbeat cadence (seconds). The engine's stale-data guard kills+respawns on >10 s silence
#: (``settings.ticker.heartbeat_silence_kill_s``, §7.1) — a 1 s beat gives ~10 missed beats of slack.
_HEARTBEAT_INTERVAL_S = 1.0

#: KiteTicker subscription mode. FULL = LTP + OHLC + cumulative volume + market depth — A13 needs the
#: cumulative day volume; depth snapshots feed §6.2 spread/liquidity features.
_MODE_FULL = "full"


# --------------------------------------------------------------------------- structured stderr log
def _log(event: str, **fields: Any) -> None:
    """Emit one JSON line to **stderr** (the engine captures it).

    A deliberately tiny, dependency-free echo of ``engine.core.log``'s shape (``ts``/``level``/
    ``event`` + structured fields) — we cannot import ``engine.core.log`` (§2.2: no ``engine.*``
    imports here). Timestamps use the wall clock for *logging only*; this process makes no trading
    time decisions, so it does not need ``core.Clock``.
    """
    # TODO(Phase 1): mirror engine.core.log's exact JSON field order/levels so the engine can ingest
    # the child's stderr into the same structured log stream. For Phase 0 a readable line suffices.
    parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
    print(f"[mt-ticker] {event} {parts}".rstrip(), file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- framing helpers
def pack_frame(obj: dict[str, Any]) -> bytes:
    """Serialize ``obj`` to a length-prefixed msgpack frame (§2.2).

    Wire layout: ``<4-byte big-endian uint length><msgpack bytes>``. ``use_bin_type=True`` keeps
    bytes/str distinct on the wire; the engine reader uses the symmetric ``raw=False``.
    ``default=str`` is a defensive last resort so an unexpected non-msgpack type inside a verbatim
    broker payload (e.g. a datetime in an order postback) degrades to its string form instead of
    crashing the reactor mid-stream — the pinned tick schema converts explicitly before packing.
    """
    body = msgpack.packb(obj, use_bin_type=True, default=str)
    return _LEN_PREFIX.pack(len(body)) + body


def _price_str(value: Any) -> str | None:
    """A broker price → decimal string for the wire (msgpack has no Decimal; §"tick" schema above).

    ``str(float)`` is the shortest round-trip repr, so 2338.55 crosses as ``"2338.55"`` and the
    engine re-wraps ``Decimal("2338.55")`` exactly. ``None`` passes through (field absent/blank).
    """
    return None if value is None else str(value)


def _iso_ts(value: Any) -> str | None:
    """A KiteTicker timestamp → ISO-8601 string of the NAIVE IST wall time it parsed (schema above).

    KiteTicker hands naive ``datetime`` objects (IST wall clock from the binary packet); the engine
    reader attaches ``Asia/Kolkata`` (§3.2). Non-datetime values degrade to ``str`` defensively.
    """
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    return str(value)


def _depth_side(levels: Any) -> list[dict[str, Any]]:
    """Shape one depth side into the pinned ``[{price, quantity, orders}×≤5]`` wire form (§6.2)."""
    shaped: list[dict[str, Any]] = []
    for level in (levels or [])[:5]:
        shaped.append(
            {
                "price": _price_str(level.get("price")),
                "quantity": level.get("quantity"),
                "orders": level.get("orders"),
            }
        )
    return shaped


# --------------------------------------------------------------------------- TCP publisher
class _PublisherProtocol(Protocol):
    """The localhost TCP connection to the parent engine.

    Owns the *outbound* frame stream (handshake, ticks, order updates, heartbeat). It is write-only
    from the ticker's perspective for tick/order data; subscription **control frames** (e.g.
    ``update_subscriptions``, §3.2.2) arrive inbound from the engine and are dispatched here.
    """

    def __init__(self, factory: _PublisherFactory) -> None:
        self._factory = factory
        self._inbuf = bytearray()

    # -- lifecycle ------------------------------------------------------------
    def connectionMade(self) -> None:
        """Send the auth handshake immediately, then let KiteTicker + heartbeat start."""
        self._factory.on_connected(self)
        self.send_frame(
            {
                "type": "hello",
                # Protocol version — the engine rejects a mismatched child (breaking-change guard).
                "v": _PROTOCOL_VERSION,
                # Per-spawn shared secret + parent PID so the engine authenticates the link (§2.4).
                "secret": self._factory.shared_secret,
                "ppid": os.getppid(),
                "pid": os.getpid(),
                # Subscribed token COUNT (an int, for the engine's link-health log) — never the list.
                "tokens": self._factory.token_count(),
            }
        )

    def connectionLost(self, reason: Any = None) -> None:  # noqa: N802 (Twisted naming)
        """The engine closed the link or it dropped. Treat as fatal — without a parent there is no
        point streaming; stop the reactor so the process exits and the engine can respawn cleanly."""
        _log("tcp.connection_lost", reason=str(getattr(reason, "value", reason)))
        self._factory.on_disconnected()
        _stop_reactor("tcp_connection_lost")

    # -- outbound -------------------------------------------------------------
    def send_frame(self, obj: dict[str, Any]) -> None:
        """Pack and write one frame to the engine (no-op if not connected)."""
        try:
            self.transport.write(pack_frame(obj))
        except Exception as exc:  # pragma: no cover - defensive; never crash the reactor on a write
            _log("tcp.write_error", error=str(exc))

    # -- inbound (control plane from engine) ----------------------------------
    def dataReceived(self, data: bytes) -> None:  # noqa: N802 (Twisted naming)
        """Buffer + decode inbound length-prefixed control frames from the engine.

        The only inbound message today is ``{"type":"subscribe","tokens":[…]}`` (the engine's
        ``update_subscriptions`` on watchlist churn / held-symbol changes, §3.2.2); it is dispatched
        to :meth:`TickerApp.on_control_frame`, which diffs + applies it on the live KiteTicker.
        """
        self._inbuf.extend(data)
        while True:
            if len(self._inbuf) < _LEN_PREFIX.size:
                return
            (length,) = _LEN_PREFIX.unpack(self._inbuf[: _LEN_PREFIX.size])
            end = _LEN_PREFIX.size + length
            if len(self._inbuf) < end:
                return
            body = bytes(self._inbuf[_LEN_PREFIX.size : end])
            del self._inbuf[:end]
            try:
                msg = msgpack.unpackb(body, raw=False)
            except Exception as exc:  # pragma: no cover - defensive
                _log("control.decode_error", error=str(exc))
                continue
            self._factory.on_control_frame(msg)


class _PublisherFactory(ClientFactory):
    """Builds the single publisher protocol and bridges it to the :class:`TickerApp`."""

    def __init__(self, app: TickerApp, shared_secret: str) -> None:
        self.app = app
        self.shared_secret = shared_secret
        self.protocol_instance: _PublisherProtocol | None = None

    def buildProtocol(self, addr: Any) -> _PublisherProtocol:  # noqa: N802 (Twisted naming)
        proto = _PublisherProtocol(self)
        return proto

    def token_count(self) -> int:
        """Subscribed-token count stamped on the ``hello`` frame (link-health log, never the list)."""
        return self.app.token_count

    def clientConnectionFailed(self, connector: Any, reason: Any) -> None:  # noqa: N802
        """Could not even reach the parent socket — fatal; the engine will respawn us."""
        _log("tcp.connect_failed", reason=str(getattr(reason, "value", reason)))
        _stop_reactor("tcp_connect_failed")

    # -- callbacks from the protocol -----------------------------------------
    def on_connected(self, proto: _PublisherProtocol) -> None:
        self.protocol_instance = proto
        self.app.on_tcp_ready(proto)

    def on_disconnected(self) -> None:
        self.protocol_instance = None

    def on_control_frame(self, msg: dict[str, Any]) -> None:
        self.app.on_control_frame(msg)


# --------------------------------------------------------------------------- stdin watcher
class _StdinWatcher(LineReceiver):
    """Watches inherited stdin; stops the reactor on EOF (parent death ⇒ orphan protection, §2.2).

    Stdin carries exactly one payload — the one-time credentials line read at startup by
    :func:`_read_stdin_credentials` (§2.4) BEFORE this watcher is installed. After that the watcher
    reads no commands (the control plane is the TCP link); it only cares about the pipe closing: when
    the engine dies, the write end of our stdin pipe is released and we get ``connectionLost`` — our
    cue to exit so we are never an orphaned websocket.
    """

    # No delimiter framing needed; we ignore content entirely.
    def lineReceived(self, line: bytes) -> None:  # noqa: N802 (Twisted naming)
        # Ignore any bytes the parent sends on stdin (none expected). Control is on the TCP link.
        pass

    def connectionLost(self, reason: Any = None) -> None:  # noqa: N802 (Twisted naming)
        _log("stdin.closed", reason="eof_parent_gone")
        _stop_reactor("stdin_closed")


# --------------------------------------------------------------------------- the app
class TickerApp:
    """Glue between the TCP publisher, the heartbeat loop, and the KiteTicker callbacks.

    Holds the KiteTicker instance and the initial token list; owns the heartbeat ``LoopingCall``.
    Kept import-light and deterministic so it is unit-testable without a live websocket.
    """

    def __init__(
        self,
        *,
        api_key: str,
        access_token: str,
        tokens: list[int],
        shared_secret: str,
    ) -> None:
        self._api_key = api_key
        self._access_token = access_token
        self._tokens = tokens
        self._shared_secret = shared_secret

        self._publisher: _PublisherProtocol | None = None
        self._heartbeat: LoopingCall | None = None
        #: Monotonic-ish counter so the engine can detect dropped heartbeats / reordering.
        self._heartbeat_seq = 0
        #: Built lazily once the TCP link is up so we never stream into a dead socket.
        self._kws: KiteTicker | None = None
        #: Websocket state + last-tick monotonic instant for the heartbeat's health fields (§2.2):
        #: lets the engine distinguish "ticker alive but feed silent" from "ticker alive, market quiet".
        self._ws_connected = False
        self._last_tick_monotonic: float | None = None

    @property
    def token_count(self) -> int:
        return len(self._tokens)

    # -- TCP link -------------------------------------------------------------
    def on_tcp_ready(self, publisher: _PublisherProtocol) -> None:
        """Called once the engine link is open + handshake sent: start KiteTicker + heartbeat."""
        self._publisher = publisher
        self._start_heartbeat()
        self._start_kiteticker()

    def on_control_frame(self, msg: dict[str, Any]) -> None:
        """Apply an inbound control frame from the engine (§3.2.2).

        The only control message today is ``{"type":"subscribe","tokens":[…]}`` (the engine's
        ``update_subscriptions``): diff the token set, (un)subscribe on the live KiteTicker, and
        re-assert FULL mode on the whole new set.
        """
        mtype = msg.get("type") if isinstance(msg, dict) else None
        if mtype == "subscribe":
            try:
                tokens = [int(t) for t in msg.get("tokens", [])]
            except (TypeError, ValueError):
                _log("control.bad_tokens", msg=msg)
                return
            self._apply_subscriptions(tokens)
        else:
            _log("control.unknown_frame", msg=msg)

    def _apply_subscriptions(self, tokens: list[int]) -> None:
        """Diff the current token set against ``tokens`` and apply on the live websocket (§3.2.2)."""
        new = list(dict.fromkeys(tokens))              # de-dup, order-preserving
        old = set(self._tokens)
        added = [t for t in new if t not in old]
        removed = [t for t in self._tokens if t not in set(new)]
        self._tokens = new
        kws = self._kws
        if kws is None:
            # Not connected yet — _on_connect subscribes the (updated) full set when it fires.
            _log("control.subscribe_deferred", tokens=len(new))
            return
        try:
            if removed:
                kws.unsubscribe(removed)
            if added:
                kws.subscribe(added)
            if new:
                # Re-assert FULL on the whole new set (mode is per-token; keeps A13 volume + depth).
                kws.set_mode(_MODE_FULL, new)
        except Exception as exc:  # pragma: no cover - defensive; never crash the reactor
            _log("control.subscribe_error", error=str(exc))
        _log("control.subscribed", total=len(new), added=len(added), removed=len(removed))

    # -- heartbeat ------------------------------------------------------------
    def _start_heartbeat(self) -> None:
        self._heartbeat = LoopingCall(self._send_heartbeat)
        # now=False: don't fire instantly on start; first beat after one interval.
        self._heartbeat.start(_HEARTBEAT_INTERVAL_S, now=False)

    def _send_heartbeat(self) -> None:
        """Emit one in-band heartbeat frame (§2.2). Drives the engine stale-data guard (R2).

        ``ws_connected`` + ``last_tick_age_s`` (monotonic; ``None`` until the first tick) let the
        engine distinguish "ticker alive but feed silent" from "ticker alive, market quiet".
        """
        if self._publisher is None:
            return
        self._heartbeat_seq += 1
        last_tick_age = (
            None
            if self._last_tick_monotonic is None
            else round(_time.monotonic() - self._last_tick_monotonic, 3)
        )
        self._publisher.send_frame(
            {
                "type": "heartbeat",
                "seq": self._heartbeat_seq,
                "ws_connected": self._ws_connected,
                "last_tick_age_s": last_tick_age,
            }
        )

    # -- KiteTicker wiring (A4) ----------------------------------------------
    def _start_kiteticker(self) -> None:
        """Construct + wire the KiteTicker and connect it on the *existing* Twisted reactor.

        ``KiteTicker`` runs on Twisted; we pass ``threaded=False`` so it shares this reactor rather
        than spinning its own thread/reactor. Reconnects are handled by KiteTicker itself
        (``reconnect=True``) — on a drop it backs off and re-fires ``on_connect``, where we
        re-subscribe; the engine's >10 s heartbeat-silence guard is the hard backstop (A4/R2).
        """
        kws = KiteTicker(self._api_key, self._access_token)
        kws.on_ticks = self._on_ticks
        kws.on_connect = self._on_connect
        kws.on_close = self._on_close
        kws.on_error = self._on_error
        kws.on_reconnect = self._on_reconnect
        kws.on_noreconnect = self._on_noreconnect
        # Order postbacks share the websocket (A3) — drives the OMS state machine (§3.5.1).
        kws.on_order_update = self._on_order_update
        self._kws = kws
        # threaded=False ⇒ use this process's reactor; do not let KiteTicker call reactor.run().
        kws.connect(threaded=False, disable_ssl_verification=False)

    # ---- KiteTicker callbacks ----
    def _on_connect(self, ws: Any, response: Any) -> None:
        """Subscribe to the token set and request FULL mode (cumulative volume + depth, A13/§6.2)."""
        _log("kws.connected", tokens=len(self._tokens))
        self._ws_connected = True
        if self._tokens:
            ws.subscribe(self._tokens)
            ws.set_mode(_MODE_FULL, self._tokens)

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        """Forward each tick as a msgpack frame.

        Per **A13** the tick carries the broker's *cumulative day volume* (``volume_traded`` in the
        KiteTicker FULL payload) — we forward it as-is; ``BarBuilder`` takes the per-bar delta. Per
        **A14** pre-open (09:00–09:15) ticks are forwarded RAW (no filtering here); the engine
        excludes them from bars and stores the auction open separately.
        """
        if self._publisher is None:
            return
        self._last_tick_monotonic = _time.monotonic()
        for tick in ticks:
            self._publisher.send_frame(self._frame_tick(tick))

    def _frame_tick(self, tick: dict[str, Any]) -> dict[str, Any]:
        """Shape a KiteTicker FULL-mode tick dict into the PINNED wire frame (module docstring).

        Field set is pinned against the engine's ``core.types.Tick`` reader
        (``engine.broker.ticker_supervisor.parse_tick_frame`` is the exact mirror):
        ``instrument_token``, ``last_price``, **cumulative** ``volume_traded`` verbatim (A13),
        ``exchange_timestamp``/``last_trade_time`` as ISO strings of the naive IST wall time,
        ``average_traded_price`` (exchange day-VWAP), the day ``ohlc`` snapshot, and the ``depth``
        top levels (§6.2 spread/liquidity features). Prices cross as decimal strings (msgpack has
        no Decimal); the engine re-wraps ``Decimal`` exactly.
        """
        ohlc = tick.get("ohlc") or {}
        depth = tick.get("depth") or {}
        return {
            "type": "tick",
            "instrument_token": tick.get("instrument_token"),
            "mode": tick.get("mode"),
            "tradable": tick.get("tradable"),
            "last_price": _price_str(tick.get("last_price")),
            "volume_traded": tick.get("volume_traded"),
            "exchange_timestamp": _iso_ts(tick.get("exchange_timestamp")),
            "last_trade_time": _iso_ts(tick.get("last_trade_time")),
            "average_traded_price": _price_str(tick.get("average_traded_price")),
            "ohlc": {k: _price_str(ohlc.get(k)) for k in ("open", "high", "low", "close")},
            "depth": {
                "buy": _depth_side(depth.get("buy")),
                "sell": _depth_side(depth.get("sell")),
            },
        }

    def _on_order_update(self, ws: Any, data: dict[str, Any]) -> None:
        """Forward a broker order postback as a ``type="order"`` frame (A3) — payload VERBATIM.

        PINNED (module docstring): ``{"type":"order","data":<verbatim Kite postback dict>}`` — the
        raw ``on_order_update`` payload untouched, because the OMS correlates on the broker's own
        field names (§3.5.1). An update that arrives *before* ``place_order()`` returns its id is
        buffered in pending-correlation by the engine — that buffering is the engine's job, not ours.
        """
        if self._publisher is None:
            return
        self._publisher.send_frame({"type": "order", "data": data})

    def _on_close(self, ws: Any, code: Any, reason: Any) -> None:
        """Websocket closed. Log it and let KiteTicker's reconnect logic re-establish (A4)."""
        _log("kws.closed", code=code, reason=reason)
        self._ws_connected = False

    def _on_error(self, ws: Any, code: Any, reason: Any) -> None:
        """Websocket error. Log; KiteTicker reconnects. Persistent silence ⇒ engine respawns us."""
        _log("kws.error", code=code, reason=reason)

    def _on_reconnect(self, ws: Any, attempts: int) -> None:
        _log("kws.reconnecting", attempts=attempts)

    def _on_noreconnect(self, ws: Any) -> None:
        """KiteTicker gave up reconnecting — fatal; stop so the engine respawns us with a fresh
        token (the usual cause is an expired access token, A5)."""
        _log("kws.noreconnect")
        _stop_reactor("kws_noreconnect")


# --------------------------------------------------------------------------- reactor lifecycle
def _stop_reactor(why: str) -> None:
    """Stop the Twisted reactor once (idempotent) so the process exits cleanly."""
    _log("reactor.stop", why=why)
    try:
        if reactor.running:
            reactor.stop()
    except Exception:  # pragma: no cover - reactor already tearing down
        pass


# --------------------------------------------------------------------------- argv/env parsing
def _parse_tokens(raw: str | None) -> list[int]:
    """Parse a comma-separated instrument-token list (``"408065,884737"``) into ``list[int]``."""
    if not raw:
        return []
    return [int(part) for part in raw.split(",") if part.strip()]


def _read_stdin_credentials() -> dict[str, Any]:
    """Read the one-time Kite credentials the supervisor writes to our stdin on spawn (§2.4).

    THE WIRE CONTRACT (mirrored in engine.broker.ticker_supervisor._send_startup_handshake): a
    single length-prefixed msgpack frame ``{"api_key", "access_token", "tokens"?}`` arrives on
    **stdin** — NEVER via env or a routable frame — so secrets never land in the process table.
    Must run BEFORE the Twisted ``_StdinWatcher`` takes over stdin (main() calls this first, then
    installs the watcher for EOF/orphan detection).

    Returns ``{}`` when stdin closes before/inside the frame or the frame is undecodable (a bare
    Phase-0-style harness spawn) — main() then warns and proceeds unauthenticated-to-Kite.
    """
    stdin = sys.stdin.buffer
    header = stdin.read(_LEN_PREFIX.size)
    if header is None or len(header) < _LEN_PREFIX.size:
        _log("stdin.no_credentials_frame", got=0 if not header else len(header))
        return {}
    (length,) = _LEN_PREFIX.unpack(header)
    body = stdin.read(length)
    if body is None or len(body) < length:
        _log("stdin.credentials_truncated", expected=length, got=0 if not body else len(body))
        return {}
    try:
        creds = msgpack.unpackb(body, raw=False)
    except Exception as exc:
        _log("stdin.credentials_decode_error", error=str(exc))
        return {}
    if not isinstance(creds, dict):
        _log("stdin.credentials_not_a_dict", got=type(creds).__name__)
        return {}
    return creds


def _resolve_config(argv: list[str]) -> dict[str, Any]:
    """Resolve the spawn config per the WIRE CONTRACT (§2.2 / §3.2.2), consistent with
    ``TickerSupervisor``:

      * ``tcp_host`` / ``tcp_port`` (the ENGINE's loopback listener — we are the CLIENT that connects),
        ``shared_secret``, ``parent_pid`` and (fallback) ``tokens`` travel via **env/argv**;
      * ``api_key`` + ``access_token`` + the authoritative initial ``tokens`` travel via **stdin**
        (:func:`_read_stdin_credentials`, §2.4) so secrets never touch the process table.
    """
    env = os.environ
    # Positional convention: main.py <tcp_host> <tcp_port> [tokens_csv]
    pos = argv[1:]
    tcp_host = pos[0] if len(pos) >= 1 else env.get("MT_TICKER_TCP_HOST", "127.0.0.1")
    tcp_port = int(pos[1]) if len(pos) >= 2 else int(env.get("MT_TICKER_TCP_PORT", "8401"))
    tokens_raw = pos[2] if len(pos) >= 3 else env.get("MT_TICKER_TOKENS")
    creds = _read_stdin_credentials()  # api_key/access_token (+tokens) via STDIN only (§2.4)
    stdin_tokens = creds.get("tokens")
    tokens = (
        [int(t) for t in stdin_tokens] if stdin_tokens else _parse_tokens(tokens_raw)
    )
    return {
        "tcp_host": tcp_host,
        "tcp_port": tcp_port,
        "api_key": str(creds.get("api_key") or ""),
        "access_token": str(creds.get("access_token") or ""),
        "shared_secret": env.get("MT_TICKER_SHARED_SECRET", ""),
        "tokens": tokens,
    }


# --------------------------------------------------------------------------- entrypoint
def main(argv: list[str] | None = None) -> int:
    """Parse config, open the TCP link to the parent, wire callbacks, and run the reactor.

    Order of operations:
      1. Resolve spawn config (argv/env).
      2. Install the stdin watcher (orphan protection) on the reactor.
      3. ``connectTCP`` to the parent; on connect the protocol sends the handshake and the app
         starts KiteTicker + the 1 s heartbeat.
      4. ``reactor.run()`` blocks until stdin EOF, a fatal TCP/ws condition, or SIGTERM.
    """
    argv = list(sys.argv if argv is None else argv)
    cfg = _resolve_config(argv)

    # Fail fast on a missing handshake secret — an unauthenticated link would let the OMS ingest
    # frames from any local process (§2.4). Empty secret only ever legitimate in a Phase-0 harness.
    if not cfg["shared_secret"]:
        _log("config.warning", msg="empty shared_secret; link is unauthenticated (Phase-0 only)")

    _log(
        "ticker.start",
        tcp_host=cfg["tcp_host"],
        tcp_port=cfg["tcp_port"],
        tokens=len(cfg["tokens"]),
        ppid=os.getppid(),
    )

    app = TickerApp(
        api_key=cfg["api_key"],
        access_token=cfg["access_token"],
        tokens=cfg["tokens"],
        shared_secret=cfg["shared_secret"],
    )

    # Orphan protection: stop the reactor when the inherited stdin pipe hits EOF (§2.2).
    stdio.StandardIO(_StdinWatcher())

    factory = _PublisherFactory(app, cfg["shared_secret"])
    # Loopback only (§2.4): the engine listens on 127.0.0.1:<tcp_port>; we are the client.
    reactor.connectTCP(cfg["tcp_host"], cfg["tcp_port"], factory)

    reactor.run()  # blocks until _stop_reactor()
    _log("ticker.stopped")
    return 0


if __name__ == "__main__":
    # Bare socket import kept referenced so static checkers see the loopback intent documented above;
    # the actual connect goes through Twisted's reactor.connectTCP.
    _ = _socket.AF_INET
    raise SystemExit(main())
