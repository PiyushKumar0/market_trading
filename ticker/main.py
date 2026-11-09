"""``mt-ticker`` — standalone Twisted KiteTicker subprocess.

NOT part of the ``engine`` package: a separate program run as a child of ``mt-engine``
(``python ticker/main.py``) on its own interpreter. ``KiteTicker`` is Twisted-based and the reactor cannot be
restarted in-process, so the engine must be free to kill and respawn this child (new token, reconnect
storms, hangs) without touching its asyncio loop.

What it does:
  * Connects ``KiteTicker`` (≤3,000 instruments per connection) and subscribes to the tokens handed to it on
    spawn (assembled by ``TickerSupervisor``).
  * Forwards every tick to the engine over a localhost TCP socket (``127.0.0.1:<tcp_port>``, default 8401)
    as a length-prefixed msgpack frame. Ticks carry the broker's *cumulative day volume*; ``BarBuilder``
    derives per-bar volume from its delta — the ticker does no aggregation.
  * Forwards order updates on the same socket as ``type="order"`` frames (the OMS postback channel).
  * Emits an in-band **heartbeat** every 1 s (``LoopingCall``). Silence >10 s on the parent side ⇒ the
    engine kills + respawns this child and raises the stale-data guard.
  * **Pre-open ticks (09:00–09:15) are forwarded RAW.** Excluding them is the engine's job (``BarBuilder``
    ignores ticks before 09:15:00); filtering here would hide the auction-open price the engine stores.
  * **Exits on stdin close** (orphan protection): when the parent dies our inherited stdin reaches EOF and
    we stop the reactor, so we never linger as an orphaned websocket.

Trust boundary: loopback only. On connect we send a per-spawn **shared-secret handshake** frame and our
**parent PID** so the engine can authenticate the link — order frames inject into the OMS state machine, so
another local process must not be able to inject a phantom fill. REST-orderbook reconciliation is the
backstop.

Framing: one msgpack-packed object prefixed by a **4-byte big-endian unsigned length** of the packed bytes.

Dependencies: only Twisted + kiteconnect + msgpack (+ stdlib). It MUST NOT import ``engine.*``. All logging
goes to **stderr** (the engine captures it); ``print()`` is fine in this standalone script.

Wire payload schemas (PINNED — ``engine.broker.ticker_supervisor`` implements the exact mirror):

  * ``hello``      — ``{type, v, secret, ppid, pid, tokens}`` (first frame on connect). ``v`` is the
    protocol version (1); ``tokens`` is the subscribed token COUNT (an int, for the link-health log).
  * ``tick``       — ``{type:"tick", instrument_token, mode, tradable, last_price, volume_traded,
    exchange_timestamp, last_trade_time, average_traded_price, ohlc:{open,high,low,close},
    depth:{buy:[{price,quantity,orders}×≤5], sell:[…]}}``. Prices cross as **decimal strings** (msgpack has
    no Decimal; ``str(float)`` is the shortest round-trip repr, so 2338.55 arrives as ``"2338.55"`` and the
    engine re-wraps ``Decimal`` exactly). ``volume_traded`` is the **cumulative day volume**, verbatim.
    Timestamps are ISO-8601 strings of the NAIVE IST wall time KiteTicker parses; the engine attaches
    ``Asia/Kolkata``.
  * ``order``      — ``{type:"order", data:<verbatim Kite postback dict>}``: the raw ``on_order_update``
    payload, untouched — the OMS correlates it.
  * ``heartbeat``  — ``{type:"heartbeat", seq, ws_connected, last_tick_age_s, connect_seq}`` every 1 s;
    ``ws_connected``/``last_tick_age_s`` let the engine tell "ticker alive but feed silent" from "ticker
    alive, market quiet". ``connect_seq`` is a per-child counter bumped on every ``_on_connect`` — the
    engine's IN-BAND reconnect signal, since KiteTicker reconnects inside this process. It is ALSO emitted
    out of cadence: :meth:`TickerApp._on_connect` sends one heartbeat BEFORE ``ws.subscribe``, so the bumped
    value precedes Kite's connect-time snapshot ticks in the same ordered stream. It is an additive optional
    field, so ``v`` stays 1 per the :data:`_PROTOCOL_VERSION` rule.
  * control (inbound) — ``{type:"subscribe", tokens:[…]}`` (``update_subscriptions``): the child diffs the
    token set, (un)subscribes, and re-asserts FULL mode on the new set.
  * stdin (one-time) — a single length-prefixed msgpack frame ``{api_key, access_token, tokens?}`` written
    by the supervisor on spawn; secrets never touch the process table.
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

from kiteconnect import KiteTicker

# Importing ``reactor`` installs the default reactor for this interpreter — fine, this process runs nothing else.
from twisted.internet import reactor, stdio
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver

# --------------------------------------------------------------------------- constants
#: Length-prefix is a 4-byte big-endian unsigned int (max frame ~4 GiB; ticks are tiny).
_LEN_PREFIX = struct.Struct(">I")

#: Wire-protocol version stamped on the ``hello`` frame (bump on any breaking schema change).
#: BREAKING = a reader pinned to the current version would mis-parse: a removed/renamed field, a changed
#: type/meaning, a new REQUIRED field. Adding an OPTIONAL field is additive and does not bump. The engine's
#: mirror constant (``engine.broker.ticker_supervisor.PROTOCOL_VERSION``) stays pinned to the same value.
_PROTOCOL_VERSION = 1

#: IST for log timestamps only (no trading time decisions here — no ``core.Clock``).
_IST = ZoneInfo("Asia/Kolkata")

#: Heartbeat cadence (seconds). The engine kills+respawns on >10 s silence
#: (``settings.ticker.heartbeat_silence_kill_s``) — a 1 s beat gives ~10 missed beats of slack.
_HEARTBEAT_INTERVAL_S = 1.0

#: KiteTicker subscription mode. FULL = LTP + OHLC + cumulative volume + depth (the engine needs the
#: cumulative volume; depth feeds spread/liquidity features).
_MODE_FULL = "full"


# --------------------------------------------------------------------------- structured stderr log
def _log(event: str, **fields: Any) -> None:
    """Emit one plain ``[mt-ticker] <event> k=v ...`` line to **stderr** (no JSON, level or timestamp).

    The supervisor drains it verbatim into ``ticker_child_output`` at WARNING
    (``ticker_supervisor._drain_child_stream``); ``engine.core.log`` cannot be imported here.
    """
    parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
    print(f"[mt-ticker] {event} {parts}".rstrip(), file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- framing helpers
def pack_frame(obj: dict[str, Any]) -> bytes:
    """Serialize ``obj`` to a length-prefixed msgpack frame.

    ``use_bin_type=True`` keeps bytes/str distinct (the engine reader uses ``raw=False``). ``default=str``
    is a last resort so a non-msgpack type inside a verbatim broker payload (e.g. a datetime in an order
    postback) degrades to a string instead of crashing the reactor; the tick schema converts explicitly.
    """
    body = msgpack.packb(obj, use_bin_type=True, default=str)
    return _LEN_PREFIX.pack(len(body)) + body


def _price_str(value: Any) -> str | None:
    """A broker price → decimal string for the wire (see the "tick" schema). ``None`` passes through."""
    return None if value is None else str(value)


def _iso_ts(value: Any) -> str | None:
    """A KiteTicker timestamp → ISO-8601 string of the NAIVE IST wall time (the engine attaches the zone).

    Non-datetime values degrade to ``str``.
    """
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    return str(value)


def _depth_side(levels: Any) -> list[dict[str, Any]]:
    """Shape one depth side into the pinned ``[{price, quantity, orders}×≤5]`` wire form."""
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

    Owns the outbound frame stream (handshake, ticks, orders, heartbeat); inbound **control frames** from the
    engine are dispatched here.
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
                # The engine rejects a mismatched version.
                "v": _PROTOCOL_VERSION,
                "secret": self._factory.shared_secret,
                "ppid": os.getppid(),
                "pid": os.getpid(),
                "tokens": self._factory.token_count(),
            }
        )

    def connectionLost(self, reason: Any = None) -> None:  # noqa: N802 (Twisted naming)
        """The link dropped. Fatal: stop the reactor so the engine can respawn cleanly."""
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
        """Buffer + decode inbound length-prefixed control frames, dispatched to :meth:`TickerApp.on_control_frame`."""
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
    """Watches inherited stdin; stops the reactor on EOF (parent death ⇒ orphan protection).

    The one-time credentials frame is read by :func:`_read_stdin_credentials` BEFORE this watcher is
    installed; afterwards stdin content is ignored (control is on the TCP link) and only the pipe closing
    matters.
    """

    def lineReceived(self, line: bytes) -> None:  # noqa: N802 (Twisted naming)
        pass

    def connectionLost(self, reason: Any = None) -> None:  # noqa: N802 (Twisted naming)
        _log("stdin.closed", reason="eof_parent_gone")
        _stop_reactor("stdin_closed")


# --------------------------------------------------------------------------- the app
class TickerApp:
    """Glue between the TCP publisher, the heartbeat loop, and the KiteTicker callbacks.

    Holds the KiteTicker instance and token list; owns the heartbeat ``LoopingCall``. Unit-testable without
    a live websocket.
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
        #: Lets the engine detect dropped heartbeats / reordering.
        self._heartbeat_seq = 0
        #: Count of KiteTicker ``on_connect`` callbacks in THIS child's life (0 = not yet connected). Rides
        #: every heartbeat; a CHANGE is the engine's only in-band evidence of an in-child reconnect.
        self._connect_seq = 0
        #: Built lazily once the TCP link is up so we never stream into a dead socket.
        self._kws: KiteTicker | None = None
        #: Heartbeat health fields (feed silent vs market quiet).
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
        """Apply an inbound control frame; the only one is ``{"type":"subscribe","tokens":[…]}`` (diff,
        (un)subscribe, re-assert FULL mode on the new set)."""
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
        """Diff the current token set against ``tokens`` and apply on the live websocket."""
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
                # Re-assert FULL on the whole new set (mode is per-token; keeps volume + depth).
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
        """Emit one in-band heartbeat frame (drives the engine stale-data guard).

        ``last_tick_age_s`` is monotonic, ``None`` until the first tick. Also called directly by
        :meth:`_on_connect`, out of cadence; ``seq`` increments either way so it stays a truthful "frames
        emitted" count for gap detection.
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
                "connect_seq": self._connect_seq,
            }
        )

    # -- KiteTicker wiring ----------------------------------------------
    def _start_kiteticker(self) -> None:
        """Construct + wire the KiteTicker and connect it on this process's reactor (``threaded=False``).

        KiteTicker reconnects itself: on a drop it backs off and re-fires ``on_connect``, where we
        re-subscribe; the engine's >10 s heartbeat-silence guard is the hard backstop.
        """
        kws = KiteTicker(self._api_key, self._access_token)
        kws.on_ticks = self._on_ticks
        kws.on_connect = self._on_connect
        kws.on_close = self._on_close
        kws.on_error = self._on_error
        kws.on_reconnect = self._on_reconnect
        kws.on_noreconnect = self._on_noreconnect
        kws.on_order_update = self._on_order_update
        self._kws = kws
        # threaded=False: KiteTicker must not call reactor.run() itself.
        kws.connect(threaded=False, disable_ssl_verification=False)

    # ---- KiteTicker callbacks ----
    def _on_connect(self, ws: Any, response: Any) -> None:
        """Announce the (re)connect in-band, THEN subscribe + request FULL mode.

        ORDER IS LOAD-BEARING. The engine never sees a supervisor state change on an in-child reconnect;
        without this signal its ``BarBuilder`` lag watchdog reads Kite's connect-time snapshot (one tick per
        instrument, carrying its LAST-TRADE timestamp) as consumption lag and flaps an alarm per tick. The
        snapshot lands within ~50 ms of ``ws.subscribe``, so a signal on the next 1 s beat is too late;
        bumping :attr:`_connect_seq` and flushing a heartbeat FIRST puts it ahead of the snapshot in the same
        ordered stream. ``_ws_connected`` is set before the flush so that frame is self-consistent. A drop
        that never reported itself is covered: the counter is bumped by the RE-CONNECT, not the drop.
        """
        self._connect_seq += 1
        self._ws_connected = True
        _log("kws.connected", tokens=len(self._tokens), connect_seq=self._connect_seq)
        self._send_heartbeat()          # MUST precede the subscribe below
        if self._tokens:
            ws.subscribe(self._tokens)
            ws.set_mode(_MODE_FULL, self._tokens)

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        """Forward each tick as a msgpack frame, cumulative ``volume_traded`` as-is and pre-open ticks RAW."""
        if self._publisher is None:
            return
        self._last_tick_monotonic = _time.monotonic()
        for tick in ticks:
            self._publisher.send_frame(self._frame_tick(tick))

    def _frame_tick(self, tick: dict[str, Any]) -> dict[str, Any]:
        """Shape a KiteTicker FULL-mode tick dict into the PINNED wire frame (module docstring);
        ``engine.broker.ticker_supervisor.parse_tick_frame`` is the exact mirror."""
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
        """Forward a broker order postback as a ``type="order"`` frame — payload VERBATIM.

        PINNED (module docstring): untouched, because the OMS correlates on the broker's own field names.
        Buffering an update that arrives before ``place_order()`` returns its id is the engine's job.
        """
        if self._publisher is None:
            return
        self._publisher.send_frame({"type": "order", "data": data})

    def _on_close(self, ws: Any, code: Any, reason: Any) -> None:
        """Websocket closed. Log it and let KiteTicker's reconnect logic re-establish."""
        _log("kws.closed", code=code, reason=reason)
        self._ws_connected = False

    def _on_error(self, ws: Any, code: Any, reason: Any) -> None:
        """Websocket error. Log; KiteTicker reconnects. Persistent silence ⇒ engine respawns us."""
        _log("kws.error", code=code, reason=reason)

    def _on_reconnect(self, ws: Any, attempts: int) -> None:
        _log("kws.reconnecting", attempts=attempts)

    def _on_noreconnect(self, ws: Any) -> None:
        """KiteTicker gave up reconnecting (usually an expired token) — fatal; the engine respawns us."""
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
    """Read the one-time Kite credentials the supervisor writes to our stdin on spawn.

    THE WIRE CONTRACT (mirrored in engine.broker.ticker_supervisor._send_startup_handshake): one
    length-prefixed msgpack frame ``{"api_key", "access_token", "tokens"?}`` on **stdin** — NEVER env — so
    secrets stay out of the process table. Must run BEFORE ``_StdinWatcher`` takes over stdin.

    Returns ``{}`` when stdin closes before/inside the frame or it is undecodable (a bare harness spawn).
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
    """Resolve the spawn config as ``TickerSupervisor`` sends it: ``tcp_host``/``tcp_port`` (the ENGINE's
    loopback listener; we are the client), ``shared_secret`` and fallback ``tokens`` via **env/argv**;
    ``api_key`` + ``access_token`` + the authoritative ``tokens`` via **stdin**."""
    env = os.environ
    # Positional convention: main.py <tcp_host> <tcp_port> [tokens_csv]
    pos = argv[1:]
    tcp_host = pos[0] if len(pos) >= 1 else env.get("MT_TICKER_TCP_HOST", "127.0.0.1")
    tcp_port = int(pos[1]) if len(pos) >= 2 else int(env.get("MT_TICKER_TCP_PORT", "8401"))
    tokens_raw = pos[2] if len(pos) >= 3 else env.get("MT_TICKER_TOKENS")
    creds = _read_stdin_credentials()  # api_key/access_token (+tokens) via STDIN only
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
    """Resolve config, install the stdin watcher, ``connectTCP`` to the parent (on connect: handshake, then
    KiteTicker + heartbeat), and run the reactor until stdin EOF, a fatal TCP/ws condition, or SIGTERM."""
    argv = list(sys.argv if argv is None else argv)
    cfg = _resolve_config(argv)

    # Warn, do not exit: an empty secret is only legitimate in a bare harness. The engine enforces auth by
    # dropping any hello whose secret is not this spawn's (ticker_supervisor._handle_child_connection).
    if not cfg["shared_secret"]:
        _log("config.warning", msg="empty shared_secret; link is unauthenticated")

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

    stdio.StandardIO(_StdinWatcher())   # orphan protection

    factory = _PublisherFactory(app, cfg["shared_secret"])
    reactor.connectTCP(cfg["tcp_host"], cfg["tcp_port"], factory)

    reactor.run()  # blocks until _stop_reactor()
    _log("ticker.stopped")
    return 0


if __name__ == "__main__":
    _ = _socket.AF_INET   # keeps the socket import referenced; the connect goes through reactor.connectTCP
    raise SystemExit(main())
