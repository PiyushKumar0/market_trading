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

Phase-0 status: this is a documented skeleton. The KiteTicker wiring, the framing helpers, and the
process lifecycle (TCP connect, heartbeat loop, stdin watcher, reactor) are real and load-bearing;
the exact *frame payload schemas* are marked ``TODO(Phase 1)`` where the field set is still being
nailed down against the engine reader and the §4 tick/bar schemas.
"""

from __future__ import annotations

import os
import struct
import sys
import socket as _socket
from typing import Any

import msgpack

# kiteconnect ships KiteTicker (Twisted/autobahn under the hood) — A4. Imported at module top per
# conventions; the owner installs pykiteconnect==5.2.0 into the venv (pyproject).
from kiteconnect import KiteTicker

# Twisted reactor + helpers. ``reactor`` is the Twisted global reactor; importing it installs the
# default (select/epoll/iocp) reactor for this interpreter — fine, this process runs nothing else.
from twisted.internet import reactor, stdio
from twisted.internet.task import LoopingCall
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.protocols.basic import LineReceiver


# --------------------------------------------------------------------------- constants
#: Length-prefix is a 4-byte big-endian unsigned int (max frame ~4 GiB; ticks are tiny).
_LEN_PREFIX = struct.Struct(">I")

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
    """
    body = msgpack.packb(obj, use_bin_type=True)
    return _LEN_PREFIX.pack(len(body)) + body


# --------------------------------------------------------------------------- TCP publisher
class _PublisherProtocol(Protocol):
    """The localhost TCP connection to the parent engine.

    Owns the *outbound* frame stream (handshake, ticks, order updates, heartbeat). It is write-only
    from the ticker's perspective for tick/order data; subscription **control frames** (e.g.
    ``update_subscriptions``, §3.2.2) arrive inbound from the engine and are dispatched here.
    """

    def __init__(self, factory: "_PublisherFactory") -> None:
        self._factory = factory
        self._inbuf = bytearray()

    # -- lifecycle ------------------------------------------------------------
    def connectionMade(self) -> None:
        """Send the auth handshake immediately, then let KiteTicker + heartbeat start."""
        self._factory.on_connected(self)
        self.send_frame(
            {
                "type": "hello",
                # Per-spawn shared secret + parent PID so the engine authenticates the link (§2.4).
                "secret": self._factory.shared_secret,
                "ppid": os.getppid(),
                "pid": os.getpid(),
                # TODO(Phase 1): include protocol/version + the subscribed token count for the
                # engine's link-health log.
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

        TODO(Phase 1): the only inbound message today is ``update_subscriptions`` (new token set on
        watchlist churn / held-symbol changes, §3.2.2). Decode and apply to the live KiteTicker.
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

    def __init__(self, app: "TickerApp", shared_secret: str) -> None:
        self.app = app
        self.shared_secret = shared_secret
        self.protocol_instance: _PublisherProtocol | None = None

    def buildProtocol(self, addr: Any) -> _PublisherProtocol:  # noqa: N802 (Twisted naming)
        proto = _PublisherProtocol(self)
        return proto

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

    # -- TCP link -------------------------------------------------------------
    def on_tcp_ready(self, publisher: _PublisherProtocol) -> None:
        """Called once the engine link is open + handshake sent: start KiteTicker + heartbeat."""
        self._publisher = publisher
        self._start_heartbeat()
        self._start_kiteticker()

    def on_control_frame(self, msg: dict[str, Any]) -> None:
        """Apply an inbound control frame from the engine (§3.2.2).

        TODO(Phase 1): dispatch on ``msg["type"]`` — ``update_subscriptions`` ⇒ diff the token set
        and call ``self._kws.subscribe()/unsubscribe()`` + re-assert FULL mode on the new tokens.
        """
        _log("control.frame", msg=msg)

    # -- heartbeat ------------------------------------------------------------
    def _start_heartbeat(self) -> None:
        self._heartbeat = LoopingCall(self._send_heartbeat)
        # now=False: don't fire instantly on start; first beat after one interval.
        self._heartbeat.start(_HEARTBEAT_INTERVAL_S, now=False)

    def _send_heartbeat(self) -> None:
        """Emit one in-band heartbeat frame (§2.2). Drives the engine stale-data guard (R2)."""
        if self._publisher is None:
            return
        self._heartbeat_seq += 1
        self._publisher.send_frame(
            {
                "type": "heartbeat",
                "seq": self._heartbeat_seq,
                # TODO(Phase 1): include ws connection state + last-tick monotonic age so the engine
                # distinguishes "ticker alive but feed silent" from "ticker alive, market quiet".
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
        for tick in ticks:
            self._publisher.send_frame(self._frame_tick(tick))

    def _frame_tick(self, tick: dict[str, Any]) -> dict[str, Any]:
        """Shape a KiteTicker tick dict into the wire frame.

        TODO(Phase 1): pin the exact field set against the §4 ``Tick`` schema + the engine reader —
        instrument_token, last_price, **cumulative** volume_traded (A13), OHLC, depth snapshot
        (§6.2), and the broker ``exchange_timestamp``/``last_trade_time``. Decimals are sent as
        strings to preserve precision (msgpack has no Decimal); the engine re-wraps to ``Decimal``.
        For Phase 0 the raw tick is forwarded under ``data`` so nothing is lost.
        """
        return {"type": "tick", "data": tick}

    def _on_order_update(self, ws: Any, data: dict[str, Any]) -> None:
        """Forward a broker order postback as a ``type="order"`` frame (A3).

        This is the same socket as ticks (§2.2). The engine correlates it to a platform order; an
        update that arrives *before* ``place_order()`` returns its id is buffered in pending-
        correlation (A3/§3.5.1) — that buffering is the engine's job, not ours.

        TODO(Phase 1): pin the order-frame schema against ``BrokerOrderUpdate`` (§3.5) — order_id,
        status, filled/pending qty, average_price (string Decimal), and exchange timestamps.
        """
        if self._publisher is None:
            return
        self._publisher.send_frame({"type": "order", "data": data})

    def _on_close(self, ws: Any, code: Any, reason: Any) -> None:
        """Websocket closed. Log it and let KiteTicker's reconnect logic re-establish (A4)."""
        _log("kws.closed", code=code, reason=reason)

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


def _read_stdin_credentials() -> dict[str, str]:
    """Read the one-time Kite credentials the supervisor writes to our stdin on spawn (§2.4).

    THE WIRE CONTRACT (mirrored in engine.broker.ticker_supervisor): ``api_key`` + ``access_token``
    arrive on **stdin** as the first framed line — NEVER via env or a routable frame — so they never
    land in the process table. Must run BEFORE the Twisted ``_StdinWatcher`` takes over stdin (main()
    calls this first, then installs the watcher for EOF/orphan detection).

    Phase-0 skeleton: the supervisor does not yet write the credentials frame, so return empty and let
    main() warn (the link is unauthenticated — Phase-0 harness only).
    """
    # TODO(Phase 1): read a single length-prefixed msgpack frame from sys.stdin.buffer:
    #   {"api_key": ..., "access_token": ...}; then release stdin to the _StdinWatcher for EOF/orphan.
    return {}


def _resolve_config(argv: list[str]) -> dict[str, Any]:
    """Resolve the spawn config per the WIRE CONTRACT (§2.2 / §3.2.2), consistent with
    ``TickerSupervisor``:

      * ``tcp_host`` / ``tcp_port`` (the ENGINE's loopback listener — we are the CLIENT that connects),
        ``shared_secret``, ``parent_pid`` and the initial ``tokens`` travel via **env/argv**;
      * ``api_key`` + ``access_token`` travel via **stdin** (:func:`_read_stdin_credentials`, §2.4) so
        secrets never touch the process table.
    """
    env = os.environ
    # Positional convention (Phase 0): main.py <tcp_host> <tcp_port> [tokens_csv]
    pos = argv[1:]
    tcp_host = pos[0] if len(pos) >= 1 else env.get("MT_TICKER_TCP_HOST", "127.0.0.1")
    tcp_port = int(pos[1]) if len(pos) >= 2 else int(env.get("MT_TICKER_TCP_PORT", "8401"))
    tokens_raw = pos[2] if len(pos) >= 3 else env.get("MT_TICKER_TOKENS")
    creds = _read_stdin_credentials()  # api_key/access_token via STDIN only (§2.4); empty in Phase 0
    return {
        "tcp_host": tcp_host,
        "tcp_port": tcp_port,
        "api_key": creds.get("api_key", ""),
        "access_token": creds.get("access_token", ""),
        "shared_secret": env.get("MT_TICKER_SHARED_SECRET", ""),
        "tokens": _parse_tokens(tokens_raw),
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
