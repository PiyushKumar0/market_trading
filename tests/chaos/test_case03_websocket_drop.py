"""Plan §9.4 chaos case 3 — WebSocket drop mid-session (mid-position).

Must hold (§9.4 row 3, verbatim): "stale-data guard FROZEN at 10 s (R2); ticker respawned (A4);
positions remain broker-protected; entries resume only after feed healthy".

§7.1 ``stale_data_guard`` row (the limit this case exercises): "max tick age 5 s (entry-time, per
symbol + index); feed heartbeat silence 10 s ⇒ FROZEN + ticker respawn (A4/R2)" — ``on_breach:
reject_entry_or_frozen``.

Scenario (10:05 IST, a trading day, RECOMMEND, entries open): the REAL ``TickerSupervisor`` owns a
fake mt-ticker child — the only fake, standing in for the ``ticker/main.py`` Twisted process +
KiteTicker websocket. The fake child is spawned through the supervisor's own
``asyncio.create_subprocess_exec`` call, connects to the supervisor's REAL loopback server (ephemeral
port — never 8400/8401), passes the REAL §2.4 shared-secret handshake and speaks the pinned wire
contract (length-prefixed msgpack hello / heartbeat / tick frames). Ticks flow through the supervisor's
real parser onto the bus, into the composition root's live tick cache (``engine.ops.main`` ``last_ticks``
/ ``tick_age_s``, replicated verbatim) and from there into the real ``GateContextBuilder`` → real
``RiskGate`` over the shipped limits. The real ``_monitor_loop`` watchdog runs, paced one 1-second
cycle at a time on the scenario clock (its ``_monitor_sleep`` test seam); the real kill + respawn
path (``_respawn`` → ``_terminate_child`` → ``_cancel_read_task`` → ``_spawn_child``) runs for real.

Clauses:
* "stale-data guard ... at 10 s" + "ticker respawned (A4)" + "entries resume only after feed
  healthy" — ``test_total_feed_silence_respawns_at_10s_and_entries_resume_only_when_healthy``
  (the child wedges / link dies: frame silence 10 s is NOT stale, 11 s is ⇒ kill + respawn; entries are
  refused by the 5 s tick-age rule from second 6; they resume only once the respawned feed is
  HEALTHY *and* fresh ticks arrive).
* The literal websocket drop — KiteTicker dies INSIDE a still-heartbeating child, then gives up
  (``on_noreconnect`` ⇒ child exits) ⇒ ``test_websocket_drop_inside_the_child_refuses_entries_then_respawns_on_exit``.
* "FROZEN" — ``test_feed_silence_latches_a_frozen_cause_that_clears_on_recovery``: STALE latches the
  ``feed_stale`` cause and pages the owner once; the next HEALTHY clears it (CD-1, fixed 2026-09-24).
* "positions remain broker-protected" — Phase-3-gated (``test_positions_remain_broker_protected``).
"""

from __future__ import annotations

import asyncio
import itertools
import struct
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import msgpack
import pytest
import yaml

import engine.broker.ticker_supervisor as ticker_mod
from engine.broker.ticker_supervisor import FEED_HEALTH_TOPIC, PROTOCOL_VERSION, TickerSupervisor
from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir, load_settings
from engine.core.enums import Actor, Mode, RiskState
from engine.core.eventbus import EventBus
from engine.marketdata.store import MarketStore
from engine.notify.catalog import MessageKind
from engine.risk.causes import RiskStateLatch, feed_health_to_latch
from engine.risk.kill import KillSwitch
from engine.risk.mode import ModeManager
from tests.chaos._entry_gate_rig import INDEX_SYMBOL, LIMITS_YAML, build_entry_gate, entry_checks
from tests.chaos.conftest import PHASE3_GATED

SYMBOL = "RELIANCE"
TOKENS = {738561: SYMBOL, 256265: INDEX_SYMBOL}
START_AT = datetime(2026, 6, 17, 10, 5, 0, tzinfo=IST)
_LEN = struct.Struct(">I")
_PIDS = itertools.count(51_000)


class _Now:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


async def _until(pred, what: str, timeout: float = 5.0) -> None:
    """Wait (real time, bounded) for loopback I/O to deliver — never a fixed sleep."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for: {what}")
        await asyncio.sleep(0.005)


# --------------------------------------------------------------------------- the fake mt-ticker child
class _FakeStdin:
    def __init__(self) -> None:
        self.data = bytearray()
        self._closing = False

    def write(self, b: bytes) -> None:
        self.data += b

    async def drain(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True


class _EofStream:
    async def readline(self) -> bytes:
        return b""


class _FakeTickerChild:
    """The mt-ticker process (ticker/main.py): a client of the supervisor's loopback server that
    sends ``hello`` (the per-spawn secret it got via env) on connect, then whatever frames the test
    scripts. Silence is simply the test sending nothing."""

    def __init__(self, env: dict[str, str]) -> None:
        self.pid = next(_PIDS)
        self.returncode: int | None = None
        self.stdin = _FakeStdin()
        self.stdout = _EofStream()
        self.stderr = _EofStream()
        self.secret = env["MT_TICKER_SHARED_SECRET"]
        self.terminated = False
        self._writer: asyncio.StreamWriter | None = None
        self._exited = asyncio.Event()
        self.hello_sent = asyncio.Event()

    async def connect(self, port: int) -> None:
        _reader, self._writer = await asyncio.open_connection("127.0.0.1", port)
        await self.send({"type": "hello", "v": PROTOCOL_VERSION, "secret": self.secret, "ppid": 0,
                         "pid": self.pid, "tokens": len(TOKENS)})
        self.hello_sent.set()

    async def send(self, frame: dict[str, Any]) -> None:
        body = msgpack.packb(frame, use_bin_type=True)
        self._writer.write(_LEN.pack(len(body)) + body)
        await self._writer.drain()

    async def heartbeat(self, *, ws_connected: bool = True, connect_seq: int = 1) -> None:
        await self.send({"type": "heartbeat", "seq": 0, "ws_connected": ws_connected,
                         "last_tick_age_s": None, "connect_seq": connect_seq})

    async def ticks(self, at: datetime) -> None:
        for token in TOKENS:
            await self.send({"type": "tick", "instrument_token": token, "last_price": "2500.00",
                             "volume_traded": 1000, "exchange_timestamp": at.replace(tzinfo=None).isoformat()})

    def _die(self, code: int) -> None:
        self.returncode = code
        if self._writer is not None:
            self._writer.close()
        self._exited.set()

    def exit(self, code: int = 0) -> None:          # the child's own exit (on_noreconnect → reactor.stop)
        self._die(code)

    def terminate(self) -> None:
        self.terminated = True
        self._die(1)

    def kill(self) -> None:
        self.terminated = True
        self._die(1)

    async def wait(self) -> int:
        await self._exited.wait()
        return int(self.returncode)


class _Pacer:
    """Stands in for ``ticker_supervisor._monitor_sleep``: the REAL ``_monitor_loop`` runs one cycle
    per :meth:`cycle`, with the scenario clock advanced by exactly 1 s (zero overshoot ⇒ the WO-26b
    loop-starvation discount never applies — this is genuine silence, not a starved loop)."""

    def __init__(self, now: _Now) -> None:
        self.now = now
        self._release: asyncio.Future | None = None
        self._parked = asyncio.Event()

    async def sleep(self, _seconds: float) -> None:
        fut = asyncio.get_running_loop().create_future()
        self._release = fut
        self._parked.set()
        await fut

    async def cycle(self) -> None:
        """Advance 1 s, run exactly one monitor cycle, and return once a monitor is parked again —
        the same task, or the fresh one a respawn installed (so a respawn has fully completed)."""
        await asyncio.wait_for(self._parked.wait(), timeout=5)
        self._parked.clear()
        self.now.at += timedelta(seconds=1)
        fut, self._release = self._release, None
        fut.set_result(None)
        await asyncio.wait_for(self._parked.wait(), timeout=10)


@pytest.fixture
async def feed_rig(conn, tmp_path, monkeypatch):
    monkeypatch.setenv("MT_DATA_DIR", str(tmp_path / "data"))
    settings = load_settings()
    assert settings.ticker.heartbeat_silence_kill_s == 10                    # shipped R2 budget
    limits = yaml.safe_load(LIMITS_YAML.read_text(encoding="utf-8"))["limits"]["stale_data_guard"]
    assert limits["feed_heartbeat_silence_s"] == 10 and limits["max_tick_age_s"] == 5
    ticker_settings = SimpleNamespace(ticker=settings.ticker.model_copy(update={"tcp_port": 0}))

    now = _Now(START_AT)
    clock = Clock(time_source=now)
    calendar = NSECalendar(config_dir() / "calendar", clock, strict=False, sqlite_conn=conn)
    bus = EventBus()
    mode = ModeManager(conn, clock, bus, calendar)
    kill = KillSwitch(conn, clock, bus)
    latch = RiskStateLatch(conn, clock, mode)
    await mode.request_transition(Mode.RECOMMEND, Actor.OWNER)

    # ---- engine.ops.main live tick cache (``last_ticks`` / ``mark_price`` / ``tick_age_s``), verbatim ----
    last_ticks: dict[str, tuple[Decimal, Any]] = {}

    async def _cache_tick(evt: Any) -> None:
        last_ticks[evt.tradingsymbol] = (evt.ltp, evt.exchange_ts)

    bus.subscribe("tick", _cache_tick)

    def mark_price(symbol: str) -> Decimal | None:
        cached = last_ticks.get(symbol)
        return cached[0] if cached else None

    def tick_age_s(symbol: str) -> float | None:
        cached = last_ticks.get(symbol)
        if cached is None:
            return None
        return max(0.0, (clock.now() - cached[1]).total_seconds())

    health_events: list[str] = []

    async def _record_health(fh) -> None:
        health_events.append(fh.state)

    bus.subscribe(FEED_HEALTH_TOPIC, _record_health)

    notified: list = []

    async def notify(msg) -> None:
        notified.append(msg)

    sup = TickerSupervisor(ticker_settings, clock, bus, symbol_for_token=TOKENS.get, api_key="ak",
                           calendar=calendar, notify=notify)
    bus.subscribe(FEED_HEALTH_TOPIC, feed_health_to_latch(latch, sup.in_market_hours))   # as main wires it
    children: list[_FakeTickerChild] = []

    async def fake_create_subprocess_exec(*_args, env=None, **_kw):   # the child-process boundary
        child = _FakeTickerChild(env)
        port = sup._server.sockets[0].getsockname()[1]
        children.append(child)
        asyncio.get_running_loop().create_task(child.connect(port))
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    pacer = _Pacer(now)
    monkeypatch.setattr(ticker_mod, "_monitor_sleep", pacer.sleep)

    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    builder, gate = build_entry_gate(
        conn=conn, clock=clock, calendar=calendar, mode=mode, kill=kill, store=store,
        ltp_fn=mark_price, tick_age_fn=tick_age_s, clock_skew_ok_fn=lambda: True,
    )

    async def checks():
        return await entry_checks(builder, gate, clock, SYMBOL)

    async def spawned(n: int) -> _FakeTickerChild:
        """Wait until the n-th child exists, is connected and its hello was ACCEPTED (the link is
        authenticated only once the supervisor adopted the writer)."""
        await _until(lambda: len(children) >= n, f"child #{n} spawned")
        child = children[n - 1]
        await asyncio.wait_for(child.hello_sent.wait(), timeout=5)
        await _until(lambda: sup._child_writer is not None, f"child #{n} link authenticated")
        await _until(lambda: pacer._parked.is_set(), f"child #{n} monitor armed")
        return child

    async def delivered(at: datetime) -> None:
        await _until(lambda: all(last_ticks.get(s, (None, None))[1] == at for s in TOKENS.values()),
                     "ticks delivered to the tick cache")

    rig = SimpleNamespace(now=now, clock=clock, mode=mode, latch=latch, sup=sup, children=children,
                          pacer=pacer, health_events=health_events, notified=notified, checks=checks,
                          spawned=spawned, delivered=delivered, tick_age_s=tick_age_s)
    try:
        yield rig
    finally:
        await asyncio.wait_for(sup.stop(), timeout=10)
        store.close()


async def _healthy_feed(rig) -> _FakeTickerChild:
    """Start the feed and bring it to HEALTHY with fresh ticks for the symbol and the index."""
    await rig.sup.start(list(TOKENS), "tok-day1")
    child = await rig.spawned(1)
    await child.heartbeat()
    await _until(lambda: rig.sup.health().state == "HEALTHY", "WARMING → HEALTHY")
    await child.ticks(rig.now.at)
    await rig.delivered(rig.now.at)
    return child


# ------------------------------------------------------ total silence: 10 s budget, kill + respawn
async def test_total_feed_silence_respawns_at_10s_and_entries_resume_only_when_healthy(feed_rig):
    rig = feed_rig
    child1 = await _healthy_feed(rig)
    _v, checks = await rig.checks()
    assert checks["stale_data_guard"].passed and checks["mode_risk_state"].passed   # entries open

    # The feed flows for a few seconds (heartbeat + ticks every second) ...
    for _ in range(3):
        await rig.pacer.cycle()
        await child1.heartbeat()
        await child1.ticks(rig.now.at)
        await rig.delivered(rig.now.at)
    assert rig.sup.health().state == "HEALTHY"

    # ... then the link goes dead: not one more frame of any kind.
    for second in range(1, 11):
        await rig.pacer.cycle()
        if second == 6:                                   # tick age 6 s > the 5 s entry-time limit
            verdict, checks = await rig.checks()
            assert verdict == "reject" and not checks["stale_data_guard"].passed
    # Exactly 10 s of frame silence is still within budget — no STALE, no kill (R2 is "> 10 s").
    assert rig.sup.health().state == "HEALTHY" and len(rig.children) == 1
    assert rig.sup.health().last_frame_age_s == 10.0

    await rig.pacer.cycle()                               # 11 s ⇒ STALE published, kill + respawn
    child2 = await rig.spawned(2)
    assert "STALE" in rig.health_events
    assert child1.terminated and child1.returncode is not None      # the wedged child was killed
    assert child2.pid != child1.pid and child2.secret != child1.secret   # fresh process, fresh secret
    assert rig.sup.health().state == "WARMING"            # a fresh spawn warms up again (§2.6)

    # Entries do NOT resume on the respawn alone ...
    _v, checks = await rig.checks()
    assert not checks["stale_data_guard"].passed
    # ... nor on the new child's first heartbeat (HEALTHY link, but no fresh tick yet) ...
    await child2.heartbeat()
    await _until(lambda: rig.sup.health().state == "HEALTHY", "respawned feed HEALTHY")
    _v, checks = await rig.checks()
    assert not checks["stale_data_guard"].passed
    # ... only once the healthy feed delivers fresh ticks for the symbol AND the index.
    await child2.ticks(rig.now.at)
    await rig.delivered(rig.now.at)
    _v, checks = await rig.checks()
    assert checks["stale_data_guard"].passed and checks["mode_risk_state"].passed
    assert rig.health_events[-1] == "HEALTHY"


# --------------------------------------------------- the websocket itself drops inside the child
async def test_websocket_drop_inside_the_child_refuses_entries_then_respawns_on_exit(feed_rig):
    rig = feed_rig
    child1 = await _healthy_feed(rig)

    # KiteTicker's websocket drops; the child's reactor is fine, so it keeps heartbeating
    # (ws_connected=False) while KiteTicker retries internally — liveness is intact, ticks are not.
    for _ in range(6):
        await rig.pacer.cycle()
        await child1.heartbeat(ws_connected=False)
    await _until(lambda: rig.sup.health().last_frame_age_s == 0.0, "heartbeat delivered")
    assert rig.sup.health().state == "HEALTHY" and len(rig.children) == 1    # no kill: child alive
    verdict, checks = await rig.checks()
    assert verdict == "reject" and not checks["stale_data_guard"].passed     # 6 s tick age > 5 s

    # KiteTicker gives up (on_noreconnect ⇒ reactor.stop): the child exits on its own ⇒ respawn.
    child1.exit(0)
    await rig.pacer.cycle()
    child2 = await rig.spawned(2)
    assert child2.pid != child1.pid and not child1.terminated               # it died; not killed
    _v, checks = await rig.checks()
    assert not checks["stale_data_guard"].passed                             # not until healthy + fresh

    await child2.heartbeat()
    await _until(lambda: rig.sup.health().state == "HEALTHY", "respawned feed HEALTHY")
    await child2.ticks(rig.now.at)
    await rig.delivered(rig.now.at)
    _v, checks = await rig.checks()
    assert checks["stale_data_guard"].passed


# ----------------------------------------------------------------------------------- FROZEN (R2)
async def test_feed_silence_latches_a_frozen_cause_that_clears_on_recovery(feed_rig):
    """CD-1 (fixed 2026-09-24): STALE used to latch nothing and page nobody."""
    rig = feed_rig
    child1 = await _healthy_feed(rig)
    assert rig.mode.risk_state() == RiskState.NORMAL

    for _ in range(11):                                   # > 10 s of frame silence
        await rig.pacer.cycle()
    child2 = await rig.spawned(2)
    assert "STALE" in rig.health_events and child1.terminated

    # §9.4 / §7.1: "feed heartbeat silence 10 s ⇒ FROZEN" — and it must still hold while the
    # respawned feed is only WARMING (entries resume only after the feed is healthy).
    assert rig.mode.risk_state() == RiskState.FROZEN
    assert [c for c, _s, _d in rig.latch.active_causes()] == ["feed_stale"]
    assert [m.kind for m in rig.notified] == [MessageKind.FEED_STALE]     # one in-session page

    await child2.heartbeat()
    await _until(lambda: rig.sup.health().state == "HEALTHY", "respawned feed HEALTHY")
    await child2.ticks(rig.now.at)
    await rig.delivered(rig.now.at)
    await asyncio.sleep(0.05)
    # Symmetric clear (§3.5.3 data-quality class): no latch left set once the feed is healthy.
    assert rig.mode.risk_state() == RiskState.NORMAL
    assert rig.latch.active_causes() == []


# ------------------------------------------------------------------------------- Phase-3-gated
@pytest.mark.skip(reason=f"'positions remain broker-protected' — {PHASE3_GATED}; missing: "
                         "ProtectionManager (resting SL-M/GTT) — RECOMMEND holds no platform positions")
async def test_positions_remain_broker_protected():
    raise AssertionError("unreachable — skipped")
