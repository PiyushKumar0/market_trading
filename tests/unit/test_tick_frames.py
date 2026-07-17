"""Wire-frame schema round-trip (§2.2/§3.2.2): ticker/main.py serializer ↔ ticker_supervisor reader.

The tick/order/hello/heartbeat payload schemas are PINNED (ticker/main.py module docstring);
``engine.broker.ticker_supervisor.parse_tick_frame`` is the exact mirror. These tests drive the real
child-side serializer and the real engine-side parser against each other — prices must round-trip
Decimal-exact (A13 cumulative volume verbatim; §3.2 money convention), timestamps naive-IST-ISO on
the wire → tz-aware IST in core. Plus the §2.4 handshake gate on the supervisor's loopback server.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import struct
import types
from decimal import Decimal

import msgpack
import pytest
import ticker.main as ticker_main

from engine.broker.ticker_supervisor import (
    ORDER_UPDATE_TOPIC,
    TICK_TOPIC,
    OrderUpdateFrame,
    TickerSupervisor,
    parse_tick_frame,
)
from engine.core.clock import IST
from engine.core.types import Tick

_LEN = struct.Struct(">I")


# --------------------------------------------------------------------------- fakes / helpers
class _FakeTickerCfg:
    tcp_host = "127.0.0.1"
    tcp_port = 0                      # ephemeral — tests read the bound port off the server
    heartbeat_silence_kill_s = 10
    max_instruments_per_conn = 3000


class _FakeSettings:
    ticker = _FakeTickerCfg()


class _CollectingTransport:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.chunks.append(data)


class _CollectingPublisher:
    def __init__(self) -> None:
        self.frames: list[dict] = []

    def send_frame(self, obj: dict) -> None:
        self.frames.append(obj)


class _FakeKws:
    def __init__(self) -> None:
        self.subscribed: list[list[int]] = []
        self.unsubscribed: list[list[int]] = []
        self.modes: list[tuple[str, list[int]]] = []

    def subscribe(self, tokens):
        self.subscribed.append(list(tokens))

    def unsubscribe(self, tokens):
        self.unsubscribed.append(list(tokens))

    def set_mode(self, mode, tokens):
        self.modes.append((mode, list(tokens)))


def _decode_frames(chunks: list[bytes]) -> list[dict]:
    buf = b"".join(chunks)
    frames = []
    while buf:
        (length,) = _LEN.unpack(buf[: _LEN.size])
        frames.append(msgpack.unpackb(buf[_LEN.size : _LEN.size + length], raw=False))
        buf = buf[_LEN.size + length :]
    return frames


def _app(tokens: list[int] | None = None) -> ticker_main.TickerApp:
    return ticker_main.TickerApp(
        api_key="k", access_token="t", tokens=tokens or [], shared_secret="sec"
    )


#: A realistic KiteTicker FULL-mode tick dict (naive IST datetimes, float prices — broker-shaped).
_KITE_TICK = {
    "tradable": True,
    "mode": "full",
    "instrument_token": 408065,
    "last_price": 2338.55,
    "last_traded_quantity": 5,
    "average_traded_price": 2337.1234,
    "volume_traded": 1_368_065,                        # CUMULATIVE day volume (A13)
    "total_buy_quantity": 260622,
    "total_sell_quantity": 429140,
    "ohlc": {"open": 2320.0, "high": 2340.1, "low": 2318.05, "close": 2325.35},
    "change": 0.57,
    "last_trade_time": dt.datetime(2026, 6, 17, 10, 4, 58),
    "exchange_timestamp": dt.datetime(2026, 6, 17, 10, 4, 59),
    "oi": 0,
    "depth": {
        "buy": [{"quantity": 10, "price": 2338.5, "orders": 2},
                {"quantity": 12, "price": 2338.45, "orders": 1}],
        "sell": [{"quantity": 7, "price": 2338.6, "orders": 3},
                 {"quantity": 9, "price": 2338.65, "orders": 2}],
    },
}


# --------------------------------------------------------------------------- child-side serializer
def test_frame_tick_pins_the_wire_schema():
    frame = _app()._frame_tick(_KITE_TICK)
    assert frame["type"] == "tick"
    assert frame["instrument_token"] == 408065
    assert frame["mode"] == "full"
    assert frame["tradable"] is True
    # Prices cross as decimal strings (msgpack has no Decimal) — str(float) shortest repr.
    assert frame["last_price"] == "2338.55"
    assert frame["average_traded_price"] == "2337.1234"
    assert frame["ohlc"] == {"open": "2320.0", "high": "2340.1", "low": "2318.05", "close": "2325.35"}
    # Cumulative day volume verbatim (A13) — an int, never stringified.
    assert frame["volume_traded"] == 1_368_065
    # Timestamps: ISO strings of the NAIVE IST wall time KiteTicker parsed.
    assert frame["exchange_timestamp"] == "2026-06-17T10:04:59"
    assert frame["last_trade_time"] == "2026-06-17T10:04:58"
    # Depth top levels, ≤5 per side, prices as strings.
    assert frame["depth"]["buy"][0] == {"price": "2338.5", "quantity": 10, "orders": 2}
    assert frame["depth"]["sell"][0] == {"price": "2338.6", "quantity": 7, "orders": 3}


def test_frame_tick_missing_optional_fields():
    minimal = {
        "instrument_token": 1,
        "last_price": 99.95,
        "volume_traded": 0,
        "exchange_timestamp": dt.datetime(2026, 6, 17, 9, 20, 0),
    }
    frame = _app()._frame_tick(minimal)
    assert frame["last_trade_time"] is None
    assert frame["average_traded_price"] is None
    assert frame["ohlc"] == {"open": None, "high": None, "low": None, "close": None}
    assert frame["depth"] == {"buy": [], "sell": []}


# --------------------------------------------------------------------------- full round-trip
def test_tick_round_trip_wire_to_core_tick():
    """child _frame_tick → pack_frame → msgpack decode → parse_tick_frame ⇒ Decimal-exact Tick."""
    packed = ticker_main.pack_frame(_app()._frame_tick(_KITE_TICK))
    (length,) = _LEN.unpack(packed[: _LEN.size])
    assert length == len(packed) - _LEN.size
    frame = msgpack.unpackb(packed[_LEN.size :], raw=False)

    tick = parse_tick_frame(frame, "RELIANCE")
    assert isinstance(tick, Tick)
    assert tick.tradingsymbol == "RELIANCE"
    assert tick.instrument_token == 408065
    assert tick.ltp == Decimal("2338.55")                      # exact, not float-drifted
    assert tick.volume_traded == 1_368_065                     # cumulative, verbatim (A13)
    assert tick.exchange_ts == dt.datetime(2026, 6, 17, 10, 4, 59, tzinfo=IST)
    assert tick.exchange_ts.tzinfo is not None
    assert tick.ohlc_open == Decimal("2320.0")
    assert tick.ohlc_close == Decimal("2325.35")
    assert tick.avg_price == Decimal("2337.1234")
    assert tick.bid == Decimal("2338.5")                       # depth top-of-book
    assert tick.ask == Decimal("2338.6")


def test_parse_tick_frame_rejects_missing_load_bearing_fields():
    good = _app()._frame_tick(_KITE_TICK)
    for missing in ("last_price", "exchange_timestamp", "instrument_token"):
        broken = {**good, missing: None}
        with pytest.raises(ValueError):
            parse_tick_frame(broken, "X")


# --------------------------------------------------------------------------- hello / heartbeat
def test_hello_frame_carries_version_secret_and_token_count():
    app = _app(tokens=[1, 2, 3])
    # Stub the ready-callback: the real one starts KiteTicker + reactor.run() (blocks forever).
    app.on_tcp_ready = lambda proto: None
    factory = ticker_main._PublisherFactory(app, "s3cr3t")
    proto = factory.buildProtocol(None)
    proto.transport = _CollectingTransport()
    proto.connectionMade()
    (hello,) = _decode_frames(proto.transport.chunks)
    assert hello["type"] == "hello"
    assert hello["v"] == 1
    assert hello["secret"] == "s3cr3t"
    assert hello["tokens"] == 3                 # the COUNT, never the token list
    assert isinstance(hello["pid"], int) and isinstance(hello["ppid"], int)


def test_heartbeat_carries_ws_state_and_tick_age():
    app = _app()
    pub = _CollectingPublisher()
    app._publisher = pub
    app._send_heartbeat()
    hb1 = pub.frames[-1]
    assert hb1["type"] == "heartbeat" and hb1["seq"] == 1
    assert hb1["ws_connected"] is False
    assert hb1["last_tick_age_s"] is None       # no tick yet: "market quiet" is distinguishable

    app._on_connect(_FakeKws(), None)           # ws up (no tokens ⇒ no subscribe call)
    app._on_ticks(None, [_KITE_TICK])           # a tick flowed
    app._send_heartbeat()
    hb2 = pub.frames[-1]
    assert hb2["seq"] == 2
    assert hb2["ws_connected"] is True
    assert isinstance(hb2["last_tick_age_s"], float) and hb2["last_tick_age_s"] >= 0.0


# --------------------------------------------------------------------------- control plane
def test_subscribe_control_frame_diffs_the_token_set():
    app = _app(tokens=[1, 2])
    kws = _FakeKws()
    app._kws = kws
    app.on_control_frame({"type": "subscribe", "tokens": [2, 3]})
    assert kws.unsubscribed == [[1]]
    assert kws.subscribed == [[3]]
    assert kws.modes == [("full", [2, 3])]      # FULL re-asserted on the whole new set
    assert app._tokens == [2, 3]


def test_stdin_credentials_frame_round_trip(monkeypatch):
    payload = msgpack.packb(
        {"api_key": "ak", "access_token": "at", "tokens": [7, 8]}, use_bin_type=True
    )
    fake_stdin = types.SimpleNamespace(buffer=io.BytesIO(_LEN.pack(len(payload)) + payload))
    monkeypatch.setattr(ticker_main.sys, "stdin", fake_stdin)
    creds = ticker_main._read_stdin_credentials()
    assert creds == {"api_key": "ak", "access_token": "at", "tokens": [7, 8]}

    monkeypatch.setattr(ticker_main.sys, "stdin", types.SimpleNamespace(buffer=io.BytesIO(b"")))
    assert ticker_main._read_stdin_credentials() == {}   # EOF ⇒ empty, never a crash


# --------------------------------------------------------------------------- supervisor server side
async def _send(writer: asyncio.StreamWriter, obj: dict) -> None:
    body = msgpack.packb(obj, use_bin_type=True)
    writer.write(_LEN.pack(len(body)) + body)
    await writer.drain()


async def _wait_for(predicate, timeout_s: float = 2.0) -> None:
    for _ in range(int(timeout_s / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


@pytest.mark.asyncio
async def test_supervisor_parses_frames_and_publishes(clock, bus):
    """End-to-end over a real loopback socket: hello handshake → heartbeat promotes WARMING →
    HEALTHY; tick frames become core Ticks on the ``tick`` topic; order frames pass verbatim."""
    ticks: list[Tick] = []
    orders: list[OrderUpdateFrame] = []

    async def on_tick(evt):
        ticks.append(evt)

    async def on_order(evt):
        orders.append(evt)

    bus.subscribe(TICK_TOPIC, on_tick)
    bus.subscribe(ORDER_UPDATE_TOPIC, on_order)

    sup = TickerSupervisor(
        _FakeSettings(), clock, bus,
        symbol_for_token=lambda t: {408065: "RELIANCE"}.get(t),
    )
    sup._shared_secret = "topsecret"
    sup._state = "WARMING"
    read_task = asyncio.create_task(sup._read_loop())
    try:
        await _wait_for(lambda: sup._server is not None)
        port = sup._server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)

        await _send(writer, {"type": "hello", "v": 1, "secret": "topsecret",
                             "ppid": 1, "pid": 2, "tokens": 1})
        await _send(writer, {"type": "heartbeat", "seq": 1, "ws_connected": True,
                             "last_tick_age_s": None})
        await _wait_for(lambda: sup.health().state == "HEALTHY")
        assert sup.health().heartbeat_age_s is not None

        # The REAL child serializer produces the frame — true producer/consumer round-trip.
        await _send(writer, _app()._frame_tick(_KITE_TICK))
        await _wait_for(lambda: len(ticks) == 1)
        assert ticks[0].tradingsymbol == "RELIANCE"
        assert ticks[0].ltp == Decimal("2338.55")
        assert ticks[0].volume_traded == 1_368_065
        assert sup.health().last_tick_age_s is not None

        # Unresolvable token ⇒ dropped (logged), never published, never crashes the loop.
        unknown = {**_app()._frame_tick(_KITE_TICK), "instrument_token": 999}
        await _send(writer, unknown)
        # Order frames pass VERBATIM (A3) on order.update.
        postback = {"order_id": "230817000000001", "status": "COMPLETE", "filled_quantity": 10}
        await _send(writer, {"type": "order", "data": postback})
        await _wait_for(lambda: len(orders) == 1)
        assert orders[0].data == postback
        assert len(ticks) == 1

        # Control plane back to the child: update_subscriptions writes a subscribe frame.
        sup._proc = types.SimpleNamespace(returncode=None, pid=1234)
        await sup.update_subscriptions([1, 2])
        header = await reader.readexactly(_LEN.size)
        (length,) = _LEN.unpack(header)
        ctl = msgpack.unpackb(await reader.readexactly(length), raw=False)
        assert ctl == {"type": "subscribe", "tokens": [1, 2]}

        writer.close()
    finally:
        read_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read_task


@pytest.mark.asyncio
async def test_supervisor_rejects_bad_handshake(clock, bus):
    """A frame minted without this spawn's secret is dropped before any tick is honoured (§2.4)."""
    ticks: list[Tick] = []

    async def on_tick(evt):
        ticks.append(evt)

    bus.subscribe(TICK_TOPIC, on_tick)
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "X")
    sup._shared_secret = "right"
    read_task = asyncio.create_task(sup._read_loop())
    try:
        await _wait_for(lambda: sup._server is not None)
        port = sup._server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        await _send(writer, {"type": "hello", "v": 1, "secret": "WRONG", "ppid": 1, "pid": 2,
                             "tokens": 0})
        data = await reader.read(1)          # server closes the link on a bad handshake
        assert data == b""
        assert ticks == []
        assert sup._child_writer is None
        writer.close()
    finally:
        read_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read_task
