"""TickerSupervisor respawn lifecycle: a respawn must cancel the old read loop
before installing a fresh child, or the parked read task leaks (it runs forever).

Also covers the 2026-07-22 cold-start defect family: the tick frame → bus → BarBuilder → store path
end-to-end (proof the pipeline is sound, so a tickless session is a CHILD/feed problem, not this path),
the child-stderr drain (the discarded-diagnostics root cause), the in-session tick-silence DEGRADED
guard (a tickless-but-heartbeating feed must never present HEALTHY through a session), and feed_stats.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from decimal import Decimal

import pytest

from engine.broker.ticker_supervisor import FEED_HEALTH_TOPIC, TICK_TOPIC, TickerSupervisor
from engine.core.clock import IST, Clock
from engine.core.eventbus import EventBus
from engine.marketdata.bar_builder import BarBuilder
from engine.marketdata.store import MarketStore
from engine.notify.catalog import MessageKind


class _FakeTickerCfg:
    tcp_host = "127.0.0.1"
    tcp_port = 8401
    heartbeat_silence_kill_s = 10
    max_instruments_per_conn = 3000
    tick_silence_degrade_s = 120
    tick_silence_respawn_s = 300
    max_tick_silence_respawns = 3
    feed_stats_interval_s = 300
    warming_timeout_s = 60
    warming_backoff_cap_s = 300
    max_wedge_respawns = 5
    keep_awake_in_session = True


class _FakeSettings:
    ticker = _FakeTickerCfg()


# --------------------------------------------------------------------------- helpers for the new tests
class _Now:
    """Mutable IST time source (tests move time explicitly)."""

    def __init__(self, value: dt.datetime) -> None:
        self.value = value

    def __call__(self) -> dt.datetime:
        return self.value

    def set(self, value: dt.datetime) -> None:
        self.value = value


def _at(h: int, m: int, s: int = 0) -> dt.datetime:
    return dt.datetime(2026, 6, 17, h, m, s, tzinfo=IST)   # a real 2026 trading day (conftest)


class _FakeSession:
    def __init__(self, open_dt: dt.datetime, close_dt: dt.datetime) -> None:
        self.open = open_dt
        self.close = close_dt


class _FakeCalendar:
    """Minimal NSECalendar stand-in exposing only .session(date) — what the guard uses."""

    def __init__(self, session: _FakeSession | None) -> None:
        self._session = session

    def session(self, d: dt.date):  # noqa: ANN001 - test double
        return self._session


class _NotifyRec:
    def __init__(self) -> None:
        self.msgs: list = []

    async def __call__(self, msg) -> None:  # noqa: ANN001 - CatalogMessage
        self.msgs.append(msg)


class _FakeStream:
    """Stand-in for asyncio subprocess StreamReader: yields lines then EOF (b'')."""

    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""


class _LogRec:
    """Records structured log calls so a drain test needs no log-capture plumbing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def _mk(self, level: str):
        def _fn(event: str, **kw) -> None:
            self.calls.append((level, event, kw))
        return _fn

    def __getattr__(self, level: str):
        return self._mk(level)


def _tick_frame(token: int = 1, ltp: str = "101.00", cum: int = 500,
                ts: dt.datetime | None = None) -> dict:
    ts = ts or _at(9, 15, 10)
    return {
        "type": "tick",
        "instrument_token": token,
        "last_price": ltp,
        "volume_traded": cum,
        "exchange_timestamp": ts.replace(tzinfo=None).isoformat(),  # naive-IST wire form
    }


def _collect(sink: list):
    """An async ``tick`` subscriber that appends every published Tick to ``sink``."""
    async def _h(tick) -> None:
        sink.append(tick)

    return _h


@pytest.mark.asyncio
async def test_start_refuses_without_api_key(clock, monkeypatch):
    # 2026-07-23 root cause: api_key defaulted to "" and the child dialed the WS with it forever
    # (400-BadRequest reject loop, HEALTHY-by-heartbeat, zero ticks ever). start() must fail LOUD.
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)          # api_key omitted -> ""
    spawned = []
    monkeypatch.setattr(sup, "_spawn_child", lambda: spawned.append(1))
    await sup.start([1, 2], "tok")
    assert spawned == []                                              # refused, never spawned


@pytest.mark.asyncio
async def test_start_spawns_with_api_key(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None, api_key="k123")
    spawned = []

    async def fake_spawn():
        spawned.append(1)

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    await sup.start([1, 2], "tok")
    assert spawned == [1]


@pytest.mark.asyncio
async def test_respawn_cancels_old_read_task(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    # Stand in for a live child + its parked read loop (as _spawn_child would have created).
    old_read = asyncio.create_task(asyncio.Event().wait())
    sup._read_task = old_read
    sup._access_token = "tok"

    spawned = {"n": 0}

    async def fake_spawn():
        spawned["n"] += 1

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)

    await sup._respawn(reason="heartbeat_silence")

    assert old_read.cancelled()          # the old read loop is cancelled, not leaked
    assert sup._read_task is None
    assert spawned["n"] == 1             # a fresh child was spawned


@pytest.mark.asyncio
async def test_respawn_without_token_stops(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    old_read = asyncio.create_task(asyncio.Event().wait())
    sup._read_task = old_read
    sup._access_token = None            # cannot respawn without a token

    async def fake_spawn():
        raise AssertionError("must not spawn without an access token")

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)

    await sup._respawn(reason="child_exited")
    assert old_read.cancelled()
    assert sup.health().state == "STOPPED"


# ---------------------------------------------------------------- root-cause regression: full pipeline
@pytest.mark.asyncio
async def test_tick_frame_end_to_end_supervisor_to_store(tmp_path):
    """A wire tick frame → supervisor parse → bus → BarBuilder → tmp-store 1m bar (src='self').

    Discriminating proof: this whole chain WORKS. The 2026-07-22 zero-bars outage therefore lies
    UPSTREAM of it (the child produced no ticks), not in socket/parse/bus/builder/store — every one
    of which this test exercises for real."""
    now = _Now(_at(9, 15, 10))
    clock = Clock(time_source=now)
    store = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    store.open()
    try:
        bus = EventBus()
        bar_builder = BarBuilder(store, clock, bus)
        bus.subscribe(TICK_TOPIC, bar_builder.on_tick_event)
        sup = TickerSupervisor(_FakeSettings(), clock, bus,
                               symbol_for_token=lambda t: "R" if t == 1 else None)

        await sup._handle_frame(_tick_frame(token=1, ltp="101.00", cum=500, ts=_at(9, 15, 10)))
        for _ in range(20):                 # let the fire-and-forget bus task deliver to the builder
            await asyncio.sleep(0)
        assert sup._ticks_received == 1

        now.set(_at(9, 16, 5))              # minute close + grace ⇒ 09:15 bar finalizes
        bars = bar_builder.advance()
        assert len(bars) == 1 and bars[0].src == "self"

        stored = store.get_bars_1m("R", _at(9, 15), _at(9, 16))
        assert len(stored) == 1
        assert stored[0].close == Decimal("101.00")
        assert stored[0].src == "self"
    finally:
        store.close()


# ---------------------------------------------------------------- root-cause fix: child stderr drained
@pytest.mark.asyncio
async def test_child_stderr_is_drained_to_engine_log(clock, monkeypatch):
    """The child's stderr (its ONLY diagnostic channel) must reach the engine log — un-drained, the
    2026-07-22 session had zero explanation for the tickless feed AND risked wedging the child on a
    full pipe. Drain reads every line until EOF and logs it."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)

    stream = _FakeStream([b"[mt-ticker] kws.error code=1006 reason='handshake'\n",
                          b"[mt-ticker] kws.noreconnect\n"])
    await sup._drain_child_stream(stream, "stderr", "warning")

    logged = [kw.get("line") for (lvl, ev, kw) in rec.calls if ev == "ticker_child_output"]
    assert "[mt-ticker] kws.error code=1006 reason='handshake'" in logged
    assert "[mt-ticker] kws.noreconnect" in logged
    assert all(lvl == "warning" for (lvl, ev, _kw) in rec.calls if ev == "ticker_child_output")


# ---------------------------------------------------------------- hardening 1: in-session tick silence
def _healthy_sup(clock, *, calendar, notify=None, silence_ref: dt.datetime | None = None):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(),
                           calendar=calendar, notify=notify)
    sup._state = "HEALTHY"
    sup._healthy_since = silence_ref
    sup._last_tick_at = None                 # NEVER saw a tick — the exact outage shape
    return sup


@pytest.mark.asyncio
async def test_tick_silence_degrades_in_session(clock):
    """In market hours, HEALTHY + no ticks past the budget ⇒ DEGRADED + WARNING + owner notify."""
    cal = _FakeCalendar(_FakeSession(_at(9, 15), _at(15, 30)))  # clock (conftest) is 10:05 ⇒ in-session
    notify = _NotifyRec()
    sup = _healthy_sup(clock, calendar=cal, notify=notify,
                       silence_ref=clock.now() - dt.timedelta(seconds=200))

    await sup._check_tick_silence(budget_s=120)

    assert sup.health().state == "DEGRADED"
    assert len(notify.msgs) == 1
    assert notify.msgs[0].kind == MessageKind.FEED_DEGRADED
    assert notify.msgs[0].severity == "warning"


@pytest.mark.asyncio
async def test_tick_silence_not_degraded_off_hours():
    """Off-hours (night), the SAME tick silence must NOT alarm — heartbeat-only semantics at night."""
    night = _Now(dt.datetime(2026, 6, 17, 22, 30, tzinfo=IST))
    clock = Clock(time_source=night)
    cal = _FakeCalendar(_FakeSession(_at(9, 15), _at(15, 30)))   # session exists, but now is 22:30
    notify = _NotifyRec()
    sup = _healthy_sup(clock, calendar=cal, notify=notify,
                       silence_ref=clock.now() - dt.timedelta(seconds=10_000))

    await sup._check_tick_silence(budget_s=120)

    assert sup.health().state == "HEALTHY"    # unchanged off-hours
    assert notify.msgs == []


@pytest.mark.asyncio
async def test_tick_silence_no_alarm_on_non_trading_day(clock):
    """A calendar returning no session (holiday/weekend) disables the guard even inside 'hours'."""
    sup = _healthy_sup(clock, calendar=_FakeCalendar(None), notify=_NotifyRec(),
                       silence_ref=clock.now() - dt.timedelta(seconds=9999))
    await sup._check_tick_silence(budget_s=120)
    assert sup.health().state == "HEALTHY"


@pytest.mark.asyncio
async def test_degraded_recovers_to_healthy_on_tick(clock):
    """Once ticks resume, DEGRADED recovers to HEALTHY (visible), publishing the feed.health event."""
    cal = _FakeCalendar(_FakeSession(_at(9, 15), _at(15, 30)))
    bus = EventBus()
    events: list = []

    async def _h(fh) -> None:
        events.append(fh.state)

    bus.subscribe(FEED_HEALTH_TOPIC, _h)
    sup = TickerSupervisor(_FakeSettings(), clock, bus, calendar=cal,
                           symbol_for_token=lambda t: "R")
    sup._state = "DEGRADED"

    await sup._handle_frame(_tick_frame(token=1, ts=_at(10, 5, 0)))
    for _ in range(10):
        await asyncio.sleep(0)

    assert sup.health().state == "HEALTHY"
    assert "HEALTHY" in events                # feed.health transition was published on recovery


# ---------------------------------------------------------------- hardening 2: feed_stats counters
@pytest.mark.asyncio
async def test_feed_stats_snapshot_counts_and_resets(clock):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(),
                           symbol_for_token=lambda t: "R" if t == 1 else None)

    await sup._handle_frame(_tick_frame(token=1))                 # ticks_received += 1
    await sup._handle_frame(_tick_frame(token=999))               # unresolved_symbol drop
    await sup._handle_frame({"type": "mystery"})                  # unknown_frame drop

    snap = sup.stats_snapshot()
    assert snap["ticks_received"] == 2                            # both tick frames counted on the wire
    assert snap["frames_dropped"]["unresolved_symbol"] == 1
    assert snap["frames_dropped"]["unknown_frame"] == 1

    assert sup.stats_snapshot() == {"ticks_received": 0, "frames_dropped": {}}  # reset-on-read


# ------------------------------------------------- epoch-0 exchange_timestamp guard (2026-08-20)
# A tick whose wire ``exchange_timestamp`` is Unix epoch 0 (a zeroed broker field — kiteconnect
# returns ``datetime.fromtimestamp(0)``) sailed through every layer: the child ISO-serializes it,
# ``_wire_timestamp`` IST-localizes it, and the Tick validator only checks tz-awareness — so it
# landed in a ``date=1970-01-01`` tick partition (103 rows before they were quarantined by hand).
# It is dropped: one WARNING, stream untouched. The 2026-08-21 change split it onto its OWN drop
# path/counter (``implausible_timestamp``, no ERROR traceback) — distinct from a genuinely missing
# timestamp, which still logs+counts as ``parse_error`` via the generic exception handler.

#: What ``datetime.fromtimestamp(0)`` produces on an IST box, as the child would serialize it.
_EPOCH_IST = dt.datetime(1970, 1, 1, 5, 30, 0)


@pytest.mark.asyncio
async def test_epoch_timestamp_tick_is_dropped_with_a_warning(clock, monkeypatch):
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    bus = EventBus()
    published: list = []
    bus.subscribe(TICK_TOPIC, _collect(published))
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "R")

    await sup._handle_frame(_tick_frame(token=1, ts=_EPOCH_IST.replace(tzinfo=IST)))
    for _ in range(10):
        await asyncio.sleep(0)

    assert published == []                                     # never reaches BarBuilder/the store
    assert sup.stats_snapshot()["frames_dropped"] == {"implausible_timestamp": 1}  # own drop path
    warns = [kw for (lvl, ev, kw) in rec.calls if ev == "tick_timestamp_implausible"]
    assert len(warns) == 1
    assert warns[0]["symbol"] == "R"
    assert warns[0]["parsed"].startswith("1970-01-01")
    assert all(lvl == "warning" for (lvl, ev, _kw) in rec.calls if ev == "tick_timestamp_implausible")
    # No ERROR-level traceback for this working-as-designed drop — the WARNING is the ONLY log line.
    assert [ev for (_lvl, ev, _kw) in rec.calls if ev == "ticker_tick_parse_error"] == []
    assert len(rec.calls) == 1


@pytest.mark.asyncio
async def test_normal_tick_is_unaffected_by_the_guard(clock, monkeypatch):
    """The guard is a floor far below any real data: a live 2026 tick still publishes, unlogged."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    bus = EventBus()
    published: list = []
    bus.subscribe(TICK_TOPIC, _collect(published))
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "R")

    await sup._handle_frame(_tick_frame(token=1, ts=_at(10, 5, 0)))
    for _ in range(10):
        await asyncio.sleep(0)

    assert len(published) == 1
    assert published[0].exchange_ts == _at(10, 5, 0)
    assert sup.stats_snapshot()["frames_dropped"] == {}
    assert [ev for (_lvl, ev, _kw) in rec.calls if ev == "tick_timestamp_implausible"] == []


@pytest.mark.asyncio
async def test_the_epoch_tick_never_stops_the_stream(clock):
    """One bad tick is dropped, not the feed: the very next good tick flows normally."""
    bus = EventBus()
    published: list = []
    bus.subscribe(TICK_TOPIC, _collect(published))
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "R")

    await sup._handle_frame(_tick_frame(token=1, ts=_EPOCH_IST.replace(tzinfo=IST)))
    await sup._handle_frame(_tick_frame(token=1, ltp="102.50", ts=_at(10, 5, 1)))
    for _ in range(10):
        await asyncio.sleep(0)

    assert len(published) == 1 and published[0].ltp == Decimal("102.50")
    # Both frames were consumed off the wire (the drop is per-TICK, never a read-loop abort) and
    # exactly one of them was dropped, on its own counter.
    snap = sup.stats_snapshot()
    assert snap["ticks_received"] == 2
    assert snap["frames_dropped"] == {"implausible_timestamp": 1}


@pytest.mark.asyncio
async def test_missing_timestamp_tick_still_takes_the_generic_parse_error_path(clock, monkeypatch):
    """A genuinely ABSENT ``exchange_timestamp`` is a different defect than epoch-0 (a later change split
    them): it still raises the plain ``ValueError`` from :func:`parse_tick_frame`, still logs
    ``ticker_tick_parse_error`` with a traceback, and still counts as ``parse_error`` — unchanged."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    bus = EventBus()
    published: list = []
    bus.subscribe(TICK_TOPIC, _collect(published))
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "R")

    frame = _tick_frame(token=1)
    frame["exchange_timestamp"] = None
    await sup._handle_frame(frame)
    for _ in range(10):
        await asyncio.sleep(0)

    assert published == []
    assert sup.stats_snapshot()["frames_dropped"] == {"parse_error": 1}     # NOT implausible_timestamp
    exc_levels = [lvl for (lvl, ev, _kw) in rec.calls if ev == "ticker_tick_parse_error"]
    assert exc_levels == ["exception"]                                      # ERROR-level w/ traceback, fired
    assert [ev for (_lvl, ev, _kw) in rec.calls if ev == "tick_timestamp_implausible"] == []


# --------------------------------------------------------- sleep/resume WARMING-wedge (2026-07-23 13:41)
def _warming_sup(clock, *, notify=None):
    """A supervisor parked in WARMING with a fresh spawn instant — the wedge shape after resume."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(), notify=notify)
    sup._state = "WARMING"
    sup._started_at = clock.now()
    sup._last_heartbeat_at = None
    return sup


@pytest.mark.asyncio
async def test_warming_timeout_respawns_after_budget(monkeypatch):
    """WARMING with no heartbeat past warming_timeout_s ⇒ kill+respawn (the wedge is broken)."""
    now = _Now(_at(9, 15, 0))
    clock = Clock(time_source=now)
    sup = _warming_sup(clock)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    now.set(_at(9, 15, 30))                                   # 30 s < 60 s budget — no respawn
    assert await sup._check_warming_timeout(60.0, 300.0, 5) is False
    assert respawns == []

    now.set(_at(9, 16, 5))                                    # 65 s > 60 s budget — respawn fires
    assert await sup._check_warming_timeout(60.0, 300.0, 5) is True
    assert respawns == ["warming_timeout"]
    assert sup._wedge_respawns == 1
    assert sup.health().state == "STALE"                     # published a visible feed-lost transition


@pytest.mark.asyncio
async def test_warming_heartbeat_within_budget_no_respawn(monkeypatch):
    """A heartbeat within the budget promotes WARMING→HEALTHY — no wedge respawn, counters clean."""
    now = _Now(_at(9, 15, 0))
    clock = Clock(time_source=now)
    sup = _warming_sup(clock)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    now.set(_at(9, 15, 30))                                   # 30 s < 60 s — no fire
    assert await sup._check_warming_timeout(60.0, 300.0, 5) is False

    await sup._handle_frame({"type": "heartbeat"})           # first heartbeat ⇒ HEALTHY
    assert sup.health().state == "HEALTHY"
    assert respawns == []
    assert sup._wedge_respawns == 0
    assert sup._wedge_escalated is False


@pytest.mark.asyncio
async def test_warming_backoff_caps_and_escalates_once(monkeypatch):
    """Consecutive wedge respawns back off exponentially, cap at cap_s, and escalate ONCE."""
    now = _Now(_at(9, 15, 0))
    clock = Clock(time_source=now)
    notify = _NotifyRec()
    sup = _warming_sup(clock, notify=notify)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)
        sup._state = "WARMING"                               # a fresh child re-enters WARMING
        sup._started_at = clock.now()
        sup._last_heartbeat_at = None

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def advance(sec):
        now.set(now.value + dt.timedelta(seconds=sec))
        return await sup._check_warming_timeout(60.0, 300.0, 3)  # max_respawns=3 for a short episode

    assert await advance(61) is True                         # base budget 60 s
    assert sup._wedge_respawns == 1
    assert await advance(90) is False                        # backoff grew to 120 s: 90 s must NOT fire
    assert await advance(40) is True                         # 130 s total > 120 s: fires
    assert sup._wedge_respawns == 2
    assert await advance(241) is True                        # budget 240 s; 3rd respawn hits max ⇒ escalate
    assert sup._wedge_respawns == 3
    assert len(notify.msgs) == 1
    assert notify.msgs[0].kind == MessageKind.FEED_WEDGED
    assert notify.msgs[0].severity == "warning"
    # Cap proof: uncapped budget would be 60×2³=480 s, but cap_s=300 ⇒ a 350 s gap still fires ...
    assert await advance(350) is True
    assert sup._wedge_respawns == 4
    assert len(notify.msgs) == 1                             # ... and escalation is one-shot

    # Recovery resets the backoff + escalation so a later episode can escalate again.
    sup._set_state("HEALTHY")
    assert sup._wedge_respawns == 0
    assert sup._wedge_escalated is False


# ================================================================== the false-STALE wedge
# Three occurrences (2026-08-20 pm, 08-24 10:01, 08-25 11:28) of the same shape: HEALTHY→STALE while
# the feed was perfect — ~46,000 ticks per 5 min and 1,010 bars written per interval kept flowing all
# through a "STALE" that lasted 40 min / 2 h 14 min. Two independent defects produced it:
#   1. Liveness was stamped ONLY on the dedicated heartbeat frame, so a stall on the shared event
#      loop (the reader and the watchdog live on it together) read as a dead child.
#   2. STALE was a one-way door AND the advertised kill+respawn self-deadlocked (_respawn cancelled
#      the read loop first, whose `server.wait_closed()` — CPython ≥3.12 — waits for the live child
#      link to drop, which only `_terminate_child()` two lines later could cause).


class _FakeProc:
    """Stand-in for a live child process: alive (returncode None) with a pid to log."""

    returncode = None
    pid = 4242


class _StopMonitor(Exception):
    """Ends a scripted `_monitor_loop` run (the loop has no bare `except`, so it propagates)."""


def _monitor_sup(clock, *, state="HEALTHY", **kw):
    """A supervisor parked in a running state with a live child — the watchdog's normal world."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(),
                           symbol_for_token=lambda t: "R", **kw)
    sup._state = state
    sup._proc = _FakeProc()
    sup._started_at = clock.now()
    sup._last_frame_at = clock.now()
    sup._last_heartbeat_at = clock.now()
    sup._healthy_since = clock.now()
    return sup


async def _run_monitor(sup, monkeypatch, cycle, cycles):
    """Drive the REAL ``_monitor_loop`` for ``cycles`` iterations, deterministically.

    ``cycle(i)`` stands in for one ``await _monitor_sleep(1.0)``: it advances the fake clock by
    however much wall time that cycle actually consumed (1 s = on time, more = the loop was starved)
    and may deliver frames. Nothing really sleeps, so a 30 s starvation costs a microsecond.
    """
    import engine.broker.ticker_supervisor as mod

    state = {"i": 0}

    async def fake_sleep(_seconds):
        i = state["i"]
        state["i"] += 1
        if i >= cycles:
            raise _StopMonitor
        await cycle(i)

    monkeypatch.setattr(mod, "_monitor_sleep", fake_sleep)
    try:
        await sup._monitor_loop()
    except _StopMonitor:
        pass


# ------------------------------------------------------------------ 1. a tick IS a heartbeat
@pytest.mark.asyncio
async def test_ticks_keep_the_feed_healthy_without_any_heartbeat_frame(monkeypatch):
    """Ticks flowing but ZERO heartbeat frames must stay HEALTHY — no STALE, no respawn.

    The heartbeat frame is a supplement for a quiet tape, not the sole evidence of life: a tick
    proves the child is alive and the link is carrying bytes just as well. Under the old rule the
    heartbeat age was the only input, so this exact situation (data perfect, heartbeat stamp stale)
    is what declared a healthy feed dead."""
    now = _Now(_at(10, 0, 0))
    clock = Clock(time_source=now)
    sup = _monitor_sup(clock)
    sup._last_heartbeat_at = _at(9, 0, 0)          # an HOUR of heartbeat silence ...

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def cycle(_i):
        now.set(now.value + dt.timedelta(seconds=1))
        await sup._handle_frame(_tick_frame(token=1, ts=now.value))   # ... but ticks every second

    await _run_monitor(sup, monkeypatch, cycle, cycles=30)            # 30 s > kill_after (10 s)

    assert respawns == []                                             # never killed a live child
    assert sup.health().state == "HEALTHY"
    assert sup.health().heartbeat_age_s > 3600                        # heartbeat genuinely ancient ...
    assert sup.health().last_frame_age_s <= 1.0                       # ... liveness genuinely fresh


# ------------------------------------------------------------------ 2. the kill_after contract is real
@pytest.mark.asyncio
async def test_true_silence_past_kill_after_restarts_the_child(monkeypatch):
    """NO ticks and NO heartbeats past kill_after_s ⇒ the child is actually restarted, logged once.

    This is the contract the module advertises and did not honour: on 08-24/08-25 the supervisor
    logged `ticker_respawn` and then did nothing at all for hours."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    now = _Now(_at(10, 0, 0))
    clock = Clock(time_source=now)
    sup = _monitor_sup(clock)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def cycle(_i):
        now.set(now.value + dt.timedelta(seconds=1))   # on-time cycles: no starvation to discount
        # ... and not a single frame of any kind.

    await _run_monitor(sup, monkeypatch, cycle, cycles=30)

    assert respawns == ["heartbeat_silence"]           # the child was really restarted, once
    silence = [kw for (lvl, ev, kw) in rec.calls if ev == "ticker_heartbeat_silence"]
    assert len(silence) == 1                           # one line per episode, not one per second
    assert silence[0]["frame_silence_s"] > 10.0
    assert silence[0]["loop_starved_s"] == 0.0         # honestly attributed to the child
    assert sup.health().state == "STALE"


# ------------------------------------------------------------------ 3. STALE recovers on frames
@pytest.mark.asyncio
async def test_stale_recovers_to_healthy_when_frames_resume(clock):
    """A STALE feed must recover on the very next frame — never wait for a reconnect that never comes.

    On 08-24 10:01 STALE held for 40 minutes while ~46,000 ticks per 5 min flowed through
    `_handle_frame` itself; nothing in the state machine could act on that evidence."""
    bus = EventBus()
    events: list = []

    async def _h(fh) -> None:
        events.append(fh.state)

    bus.subscribe(FEED_HEALTH_TOPIC, _h)
    sup = TickerSupervisor(_FakeSettings(), clock, bus, symbol_for_token=lambda t: "R")
    sup._state = "STALE"

    await sup._handle_frame(_tick_frame(token=1, ts=_at(10, 5, 0)))
    for _ in range(10):
        await asyncio.sleep(0)

    assert sup.health().state == "HEALTHY"
    assert events == ["HEALTHY"]           # the feed.health transition was published, not just set


@pytest.mark.asyncio
async def test_a_tick_promotes_warming_to_healthy(clock):
    """WARMING is the other place the heartbeat frame used to be privileged.

    A child whose heartbeat LoopingCall never started but whose ticks flow perfectly would sit in
    WARMING and be respawned every warming_timeout_s forever — throwing away a working feed."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(), symbol_for_token=lambda t: "R")
    sup._state = "WARMING"
    sup._started_at = clock.now()

    await sup._handle_frame(_tick_frame(token=1, ts=_at(10, 5, 0)))

    assert sup.health().state == "HEALTHY"
    assert sup._wedge_respawns == 0            # and the wedge backoff is reset, as on any promotion


@pytest.mark.asyncio
async def test_stale_recovers_on_a_heartbeat_frame_too(clock):
    """Recovery is keyed on FRAMES, not on tick frames specifically — a quiet tape recovers as well."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus())
    sup._state = "STALE"

    await sup._handle_frame({"type": "heartbeat"})

    assert sup.health().state == "HEALTHY"


# ------------------------------------------- 3b. in-child KiteTicker reconnect (2026-09-09)
# KiteTicker reconnects INSIDE the child (`reconnect=True`; ticker/main.py `TickerApp._on_connect`
# re-subscribes and re-asserts FULL mode, so Kite re-delivers the connect-time snapshot with
# last-trade stamps) while the child keeps heartbeating. The supervisor therefore sees NO state
# transition and publishes nothing, so BarBuilder's reconnect grace never arms and the 2026-09-03
# lag flap recurs.
#
# The signal is `connect_seq` (ticker/main.py `TickerApp._send_heartbeat`), a per-child counter bumped
# at the TOP of `_on_connect` and shipped on a heartbeat emitted THERE, before `ws.subscribe` — so it
# precedes the snapshot burst in the pipe. A CHANGE of that value (never the first value after a
# spawn, which is the spawn's own WARMING->HEALTHY publish) is the reconnect, and it must publish
# feed.health exactly once. The `ws_connected` False->True edge stays as the fallback for a heartbeat
# that carries no `connect_seq`, and no frame may ever publish twice.
def _hb(ws_connected: bool | None = None, connect_seq: int | None = None) -> dict:
    """The child's heartbeat frame (ticker/main.py `TickerApp._send_heartbeat`).

    `connect_seq` is the pinned reconnect field, `ws_connected` the fallback; both are optional on
    the wire, so omitting them here reproduces the pre-2026-09-09 frame shape verbatim."""
    frame: dict = {"type": "heartbeat", "seq": 1, "last_tick_age_s": 0.4}
    if ws_connected is not None:
        frame["ws_connected"] = ws_connected
    if connect_seq is not None:
        frame["connect_seq"] = connect_seq
    return frame


def _health_sink(bus: EventBus) -> list[str]:
    states: list[str] = []

    async def _h(fh) -> None:
        states.append(fh.state)

    bus.subscribe(FEED_HEALTH_TOPIC, _h)
    return states


@pytest.mark.asyncio
async def test_in_child_reconnect_publishes_feed_health_on_the_ws_connected_edge(clock):
    """True -> False -> True heartbeats: exactly ONE extra publish, on the False->True edge only.

    The state never leaves HEALTHY (no transition exists for an in-child reconnect), so this edge is
    the only evidence the engine gets that a connect-time snapshot is about to land."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "HEALTHY"

    await sup._handle_frame(_hb(True))          # first frame reports connected — NOT an edge
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == []

    await sup._handle_frame(_hb(False))         # the websocket dropped; child still heartbeating
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == []                          # no state transition, nothing to publish

    await sup._handle_frame(_hb(True))          # reconnected inside the child -> publish
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == ["HEALTHY"]

    await sup._handle_frame(_hb(True))          # steady state: never publish per heartbeat
    await sup._handle_frame(_hb(True))
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == ["HEALTHY"]
    assert sup.health().state == "HEALTHY"       # the state machine itself is untouched


@pytest.mark.asyncio
async def test_the_first_heartbeat_after_spawn_publishes_exactly_once(clock):
    """A fresh spawn's first heartbeat must keep publishing ONE event (WARMING->HEALTHY), not two.

    `ws_connected` starts unknown (None) and the first frame reports True — that is a spawn, not a
    reconnect, and the WARMING promotion already publishes the HEALTHY the grace needs."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "WARMING"
    sup._started_at = clock.now()

    await sup._handle_frame(_hb(True))
    for _ in range(10):
        await asyncio.sleep(0)

    assert states == ["HEALTHY"]
    assert sup.health().state == "HEALTHY"


@pytest.mark.asyncio
async def test_a_heartbeat_without_ws_connected_never_fabricates_an_edge(clock):
    """An older/degenerate heartbeat frame with no `ws_connected` leaves the tracked value alone.

    Absence is not a disconnect: treating a missing field as False would manufacture a reconnect
    edge on the next normal heartbeat and arm the grace for nothing."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "HEALTHY"

    await sup._handle_frame(_hb(False))
    await sup._handle_frame(_hb(None))          # field absent — must not clear the tracked False
    await sup._handle_frame(_hb(True))          # ... so this is still the reconnect edge
    for _ in range(10):
        await asyncio.sleep(0)

    assert states == ["HEALTHY"]


@pytest.mark.asyncio
async def test_a_connect_seq_change_publishes_feed_health_exactly_once(clock, monkeypatch):
    """1, 1, 2 — only the CHANGE is a reconnect (2026-09-09 review, gap (a)).

    The `ws_connected` edge is only observable on the NEXT 1 s heartbeat, but Kite's connect-time
    snapshot ticks land within ~50 ms of the child's re-subscribe, so the burst flaps BarBuilder's lag
    episode before the grace can arm. `connect_seq` rides a heartbeat emitted inside `_on_connect`
    itself, ahead of the subscribe — in-band and ordering-safe. Here: the first value seen is this
    spawn's baseline, the repeat is steady state, and the bump is the reconnect. Every heartbeat carries
    ``ws_connected=True``, so this is also gap (b): a sub-second drop (or an ``on_error`` path with no
    ``on_close``) never produces a ``ws_connected=False`` heartbeat, and the seq bump alone must fire."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "HEALTHY"

    await sup._handle_frame(_hb(True, connect_seq=1))   # first value: baseline, not an edge
    await sup._handle_frame(_hb(True, connect_seq=1))   # steady state
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == []

    await sup._handle_frame(_hb(True, connect_seq=2))   # the in-child reconnect
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == ["HEALTHY"]

    await sup._handle_frame(_hb(True, connect_seq=2))   # ... and never again on the same seq
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == ["HEALTHY"]
    assert sup.health().state == "HEALTHY"              # the state machine itself is untouched

    reconnects = [kw for (_lvl, ev, kw) in rec.calls if ev == "ticker_ws_reconnected"]
    assert len(reconnects) == 1
    assert reconnects[0]["seq"] == 2 and reconnects[0]["via"] == "connect_seq"


@pytest.mark.asyncio
async def test_one_frame_that_recovers_stale_and_changes_seq_publishes_once(clock):
    """A reconnect heartbeat landing on a STALE feed has TWO reasons to publish — `_mark_alive`'s
    STALE->HEALTHY recovery and the seq edge — and must still emit exactly ONE `feed.health`.

    Two events for one frame would arm BarBuilder's grace twice and make the bus lie about how many
    transitions happened."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "HEALTHY"

    await sup._handle_frame(_hb(True, connect_seq=7))   # baseline seq for this spawn
    for _ in range(10):
        await asyncio.sleep(0)
    assert states == []

    sup._state = "STALE"                                # ... then the feed went silent
    await sup._handle_frame(_hb(True, connect_seq=8))   # recovery AND seq edge, one frame
    for _ in range(10):
        await asyncio.sleep(0)

    assert states == ["HEALTHY"]
    assert sup.health().state == "HEALTHY"


@pytest.mark.asyncio
async def test_the_first_connect_seq_after_spawn_is_not_a_reconnect(clock):
    """The child's `_on_connect` heartbeat is the FIRST frame of a fresh spawn, so its `connect_seq`
    has no predecessor here: the WARMING->HEALTHY promotion is the one publish, not two."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "WARMING"
    sup._started_at = clock.now()

    await sup._handle_frame(_hb(True, connect_seq=1))
    for _ in range(10):
        await asyncio.sleep(0)

    assert states == ["HEALTHY"]
    assert sup.health().state == "HEALTHY"


@pytest.mark.asyncio
async def test_a_respawned_childs_first_seq_is_not_a_reconnect(clock):
    """`_spawn_child` resets the tracked seq like every other per-spawn stamp.

    A fresh child restarts its counter at 1, so a supervisor still holding the dead child's last seq
    (say 6) would read 1 as a "change" and publish a phantom reconnect on top of the respawn's own
    WARMING->HEALTHY."""
    bus = EventBus()
    states = _health_sink(bus)
    sup = TickerSupervisor(_FakeSettings(), clock, bus)
    sup._state = "HEALTHY"

    await sup._handle_frame(_hb(True, connect_seq=6))   # the dying child's last heartbeat
    sup._reset_spawn_stamps()                           # what _spawn_child does for a fresh child
    sup._state = "WARMING"
    sup._started_at = clock.now()

    await sup._handle_frame(_hb(True, connect_seq=1))   # new child, counter back at 1
    for _ in range(10):
        await asyncio.sleep(0)

    assert states == ["HEALTHY"]                        # the promotion only — no phantom reconnect


# ------------------------------------------------------------------ 4. stamping lives on the frame path
@pytest.mark.asyncio
async def test_every_frame_type_stamps_liveness_on_the_frame_path(clock):
    """Liveness is stamped in `_handle_frame` — the SAME seam every frame goes through.

    That is the point of the fix: liveness cannot be starved separately from the data it guards,
    because it is refreshed by the data. Any frame counts, including an order postback and a frame
    `_read_frame` could not decode (reduced to `{}`): bytes off the link ARE proof of life."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(), symbol_for_token=lambda t: "R")

    for frame in ({"type": "tick", **_tick_frame(token=1)},
                  {"type": "order", "data": {"order_id": "x"}},
                  {"type": "heartbeat"},
                  {}):                                   # undecodable frame -> still liveness
        sup._last_frame_at = None
        await sup._handle_frame(frame)
        assert sup._last_frame_at == clock.now(), f"no liveness stamp for {frame.get('type')!r}"


# ------------------------------------------------------------------ 5. don't blame the child for our stall
@pytest.mark.asyncio
async def test_silence_measured_across_a_starved_loop_does_not_kill(monkeypatch):
    """A silence the watchdog only 'observed' because its own loop was frozen must not kill the child.

    This is the 08-24/08-25 mechanism: the feature engine blocked the shared event loop for ~30 s at
    a stretch (no `ticks_flushed` for 34 s, then 3,124 ticks in one flush; `store_stalled` reporting
    `seconds_since_last_success=900.154`). Nothing was read during the freeze, so the wall-clock
    frame age looked fatal — while the child was fine and its frames were queued in TCP."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    now = _Now(_at(10, 0, 0))
    clock = Clock(time_source=now)
    sup = _monitor_sup(clock)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def cycle(i):
        # Cycle 0 takes 31 wall-seconds instead of 1: the loop was blocked, not the feed.
        now.set(now.value + dt.timedelta(seconds=31 if i == 0 else 1))

    await _run_monitor(sup, monkeypatch, cycle, cycles=6)

    assert respawns == []                                     # a live child was NOT killed
    discounted = [kw for (lvl, ev, kw) in rec.calls if ev == "ticker_silence_discounted_loop_starved"]
    assert len(discounted) == 1                               # said once per episode, not per cycle
    assert discounted[0]["loop_starved_s"] == pytest.approx(30.0)
    assert discounted[0]["frame_silence_s"] > 10.0             # the raw age really did look fatal
    assert [ev for (_l, ev, _k) in rec.calls if ev == "ticker_heartbeat_silence"] == []


@pytest.mark.asyncio
async def test_starvation_discount_still_kills_a_genuinely_dead_child(monkeypatch):
    """The discount defers one episode; it never grants immunity. Once the loop runs normally again
    and the child is STILL silent, the kill fires for real."""
    now = _Now(_at(10, 0, 0))
    clock = Clock(time_source=now)
    sup = _monitor_sup(clock)

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def cycle(i):
        now.set(now.value + dt.timedelta(seconds=31 if i == 0 else 1))   # starved, then healthy loop

    await _run_monitor(sup, monkeypatch, cycle, cycles=30)

    assert respawns == ["heartbeat_silence"]      # silence outran the starvation credit ⇒ restarted


# ------------------------------------------------------------------ 6. the respawn deadlock itself
@pytest.mark.asyncio
async def test_respawn_terminates_the_child_before_cancelling_the_read_loop(clock, monkeypatch):
    """Regression for the wedge that made kill_after a lie.

    `_read_loop`'s cleanup awaits `server.wait_closed()`, which under CPython ≥3.12 blocks until
    every accepted connection has dropped — the child link included. Cancelling the read loop BEFORE
    killing the child therefore waited on an event only the not-yet-run `_terminate_child()` could
    produce, behind the same lock: the supervisor logged `ticker_respawn` and froze for 40 min
    (08-24 10:01), 2 h 14 min (08-25 11:28) and 4 h 53 min (08-25 03:37), each ending only when an
    unrelated `ticker_link_closed` released the wait. Order is the fix."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    sup._read_task = asyncio.create_task(asyncio.Event().wait())
    sup._access_token = "tok"

    order: list[str] = []

    async def fake_terminate():
        order.append("terminate_child")
        sup._proc = None

    async def fake_cancel_read():
        order.append("cancel_read_task")

    async def fake_spawn():
        order.append("spawn_child")

    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)
    monkeypatch.setattr(sup, "_cancel_read_task", fake_cancel_read)
    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)

    await sup._respawn(reason="heartbeat_silence")

    assert order == ["terminate_child", "cancel_read_task", "spawn_child"]
    sup._read_task.cancel()


@pytest.mark.asyncio
async def test_stop_kills_the_child_between_the_two_cancels(clock, monkeypatch):
    """`stop()` carries the same constraint plus one of its own: the monitor must go FIRST so it
    cannot respawn the child being killed, then the child, then the read loop."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    order: list[str] = []

    async def rec(name):
        order.append(name)

    monkeypatch.setattr(sup, "_cancel_monitor_task", lambda: rec("cancel_monitor"))
    monkeypatch.setattr(sup, "_terminate_child", lambda: rec("terminate_child"))
    monkeypatch.setattr(sup, "_cancel_read_task", lambda: rec("cancel_read_task"))

    await sup.stop()

    assert order == ["cancel_monitor", "terminate_child", "cancel_read_task"]
    assert sup.health().state == "STOPPED"


@pytest.mark.asyncio
async def test_stop_cancels_the_real_read_task(clock, monkeypatch):
    """`_cancel_monitor_task` must touch only the monitor task. Two leftover lines from the
    earlier combined `_cancel_supervision` (`self._read_task = None` plus a redundant
    `self._monitor_task = None`) cleared `_read_task` before step 3 ever ran, so `stop()`'s own
    `_cancel_read_task()` found nothing to cancel and the real read-loop task leaked on every
    normal stop — the opposite of what `stop()`'s docstring promises."""
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    old_read = asyncio.create_task(asyncio.Event().wait())
    sup._read_task = old_read
    sup._monitor_task = asyncio.create_task(asyncio.Event().wait())

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)

    await sup.stop()

    assert old_read.cancelled()          # the real read-loop task was cancelled, not leaked
    assert sup._read_task is None


@pytest.mark.asyncio
async def test_read_loop_cleanup_cannot_hang_on_a_child_that_will_not_let_go(clock, monkeypatch):
    """Belt-and-braces: even with the child link still open, cancelling the read loop must COMPLETE.

    `server.wait_closed()` is bounded and the timeout is logged, so a child that survives both
    SIGTERM and SIGKILL can no longer freeze supervision the way it did for hours."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    monkeypatch.setattr(mod, "_SERVER_CLOSE_TIMEOUT_S", 0.05)

    class _NeverClosingServer:
        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            await asyncio.Event().wait()          # a live connection that never drops

    async def fake_start_server(*_a, **_kw):
        return _NeverClosingServer()

    monkeypatch.setattr(mod.asyncio, "start_server", fake_start_server)
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    sup._read_task = asyncio.create_task(sup._read_loop())
    for _ in range(20):
        await asyncio.sleep(0)

    await asyncio.wait_for(sup._cancel_read_task(), timeout=5.0)      # must not hang

    assert [ev for (_l, ev, _k) in rec.calls if ev == "ticker_server_close_timeout"]


# --------------------------------------------------------------------------- a spawn that raises (2026-10-12)
def _failing_spawn_sup(clock, monkeypatch, *, fail_first: int, bus: EventBus | None = None):
    """A HEALTHY supervisor whose next ``fail_first`` spawns raise CreateProcess's WinError 5 (Modern
    Standby, 2026-10-12 20:12); records spawn attempts and retry sleeps, and never touches a real process."""
    import engine.broker.ticker_supervisor as mod

    sup = TickerSupervisor(_FakeSettings(), clock, bus=bus, api_key="k")
    sup._access_token = "tok"
    sup._set_state("HEALTHY")
    attempts: list[int] = []
    sleeps: list[float] = []

    async def fake_spawn():
        attempts.append(1)
        if len(attempts) <= fail_first:
            raise PermissionError(13, "Access is denied")

    async def fake_terminate():
        sup._proc = None

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)
    monkeypatch.setattr(mod, "_monitor_sleep", fake_sleep)
    return sup, attempts, sleeps


@pytest.mark.asyncio
async def test_a_respawn_whose_spawn_raises_keeps_supervising_and_retries_with_backoff(clock, monkeypatch):
    bus = EventBus()
    states = _health_sink(bus)
    sup, attempts, sleeps = _failing_spawn_sup(clock, monkeypatch, fail_first=2, bus=bus)

    await sup._respawn(reason="heartbeat_silence")                     # must not raise
    await bus.drain(1.0)
    assert (sup.health().state, len(attempts), states) == ("STALE", 1, ["STALE"])
    first = sup._monitor_task
    assert first is not None and not first.done()                      # supervision survives the failure

    await first                                                        # retry 1 fails, arms retry 2
    await sup._monitor_task                                            # retry 2 spawns
    assert (len(attempts), sleeps, sup._spawn_failures) == (3, [60.0, 120.0], 0)


@pytest.mark.asyncio
async def test_stop_cancels_a_pending_spawn_retry(clock, monkeypatch):
    import engine.broker.ticker_supervisor as mod

    sup, attempts, _ = _failing_spawn_sup(clock, monkeypatch, fail_first=99)

    async def parked_sleep(_seconds: float) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(mod, "_monitor_sleep", parked_sleep)
    await sup._respawn(reason="child_exited")
    retry = sup._monitor_task

    await sup.stop()
    assert retry.cancelled() and len(attempts) == 1 and sup.health().state == "STOPPED"


@pytest.mark.asyncio
async def test_a_long_failure_streak_backs_off_at_the_cap_without_overflow(clock, monkeypatch):
    sup, _, sleeps = _failing_spawn_sup(clock, monkeypatch, fail_first=1)
    sup._spawn_failures = 5000                                         # ~17 days at the cap

    await sup._respawn(reason="spawn_retry")
    await sup._monitor_task
    assert sleeps == [300.0]


@pytest.mark.asyncio
async def test_the_incident_through_the_real_monitor_terminate_and_spawn_paths(monkeypatch):
    """10-12 end to end: heartbeat silence in the REAL monitor loop -> the old child's TerminateProcess is
    denied -> the REAL _spawn_child binds its listener, then CreateProcess is denied. Supervision survives
    as a retry, STALE is published and the half-made spawn's listener is cancelled."""
    import engine.broker.ticker_supervisor as mod

    now = _Now(_at(20, 12, 30))
    sup = _monitor_sup(Clock(time_source=now), api_key="k")
    sup._access_token = "tok"
    states = _health_sink(sup._bus)

    class _DeniedProc(_FakeProc):
        stdin = None

        def terminate(self) -> None:
            raise PermissionError(13, "Access is denied")

    class _Server:
        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def fake_start_server(*_a, **_kw):
        return _Server()

    async def denied_exec(*_a, **_kw):
        raise PermissionError(13, "Access is denied", None, 5, None)

    sup._proc = _DeniedProc()
    monkeypatch.setattr(mod.asyncio, "start_server", fake_start_server)
    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", denied_exec)

    async def silent(_i: int) -> None:
        now.set(now.value + dt.timedelta(seconds=1))

    await _run_monitor(sup, monkeypatch, silent, cycles=30)

    retry = sup._monitor_task
    assert (sup.health().state, states[-1], sup._spawn_failures) == ("STALE", "STALE", 1)
    assert sup._read_task is None and sup._proc is None
    assert retry is not None and retry.get_name() == "ticker-spawn-retry" and not retry.done()
    await sup.stop()
    assert retry.cancelled() and sup.health().state == "STOPPED"


class _LinkWriter:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_read_loop_teardown_closes_the_child_link(clock, monkeypatch):
    """A child that outlives a denied terminate/kill keeps its authenticated connection, which
    server.close() does not drop: the read loop's teardown (every stop/respawn cancels it right after
    _terminate_child) must close it, or its frames keep stamping liveness for the next spawn."""
    import engine.broker.ticker_supervisor as mod

    class _Server:
        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def fake_start_server(*_a, **_kw):
        return _Server()

    monkeypatch.setattr(mod.asyncio, "start_server", fake_start_server)
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None)
    sup._read_task = asyncio.create_task(sup._read_loop())
    await sup._wait_server_ready()
    sup._child_writer = writer = _LinkWriter()

    await sup._cancel_read_task()
    assert writer.closed and sup._child_writer is None


@pytest.mark.asyncio
async def test_a_start_whose_spawn_raises_keeps_supervising(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None, api_key="k")

    async def denied_spawn():
        raise PermissionError(13, "Access is denied")

    monkeypatch.setattr(sup, "_spawn_child", denied_spawn)
    await sup.start([1, 2], "tok")                                     # must not raise

    retry = sup._monitor_task
    assert (sup.health().state, sup._spawn_failures) == ("STALE", 1)
    assert retry is not None and retry.get_name() == "ticker-spawn-retry"
    await sup.stop()
    assert retry.cancelled()


@pytest.mark.asyncio
async def test_a_start_cancels_a_pending_spawn_retry(clock, monkeypatch):
    import engine.broker.ticker_supervisor as mod

    sup, attempts, _ = _failing_spawn_sup(clock, monkeypatch, fail_first=1)

    async def parked_sleep(_seconds: float) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(mod, "_monitor_sleep", parked_sleep)
    await sup._respawn(reason="child_exited")
    retry = sup._monitor_task

    await sup.start([1], "tok2")
    assert retry.cancelled() and len(attempts) == 2 and sup._access_token == "tok2"


@pytest.mark.asyncio
async def test_a_long_wedge_streak_backs_off_at_the_cap_without_overflow(clock, monkeypatch):
    sup = TickerSupervisor(_FakeSettings(), clock, bus=None, api_key="k")
    sup._set_state("WARMING")
    sup._started_at = clock.now() - dt.timedelta(seconds=299)
    sup._wedge_respawns = 5000

    respawned = []

    async def fake_respawn(*, reason: str) -> None:
        respawned.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)
    assert await sup._check_warming_timeout(60.0, 300.0, 5) is False   # 299 s is inside the 300 s cap


# ================================================================== silent connected feed (2026-11-03)
# Kite held the WebSocket open and answered pings while delivering no ticks (11-03 13:46–15:30, 11-02
# 14:51–15:22, 10-07, 09-30). The child heartbeats throughout, so only tick silence can see it, and
# every such silence since 09-15 ended with a fresh connection. DEGRADED past 300 s in session now
# respawns, at most 3 times per episode, then escalates once.
class _DayCalendar:
    """NSECalendar stand-in with a 09:15–15:30 session on EVERY date (the rollover test needs two)."""

    def session(self, d: dt.date):  # noqa: ANN001 - test double
        return _FakeSession(dt.datetime(d.year, d.month, d.day, 9, 15, tzinfo=IST),
                            dt.datetime(d.year, d.month, d.day, 15, 30, tzinfo=IST))


def _silent_feed_rig(monkeypatch, start: dt.datetime, *, calendar=None, spawn=None, notify=None):
    """A HEALTHY in-session supervisor whose last tick is ``start``; heartbeats keep coming, ticks don't.

    The REAL ``_respawn`` runs; only the OS edges are faked: terminate drops the proc, and spawn does to
    supervisor state exactly what ``_spawn_child`` does (per-spawn stamps reset, WARMING). ``spawn`` is an
    optional hook run first, so a test can make the spawn raise."""
    import engine.broker.ticker_supervisor as mod

    rec = _LogRec()
    monkeypatch.setattr(mod, "_log", rec)
    now = _Now(start)
    clock = Clock(time_source=now)
    notify = notify if notify is not None else _NotifyRec()
    sup = TickerSupervisor(_FakeSettings(), clock, bus=EventBus(), calendar=calendar or _DayCalendar(),
                           notify=notify, symbol_for_token=lambda t: "R")
    sup._access_token = "tok"
    sup._state = "HEALTHY"
    sup._healthy_since = start - dt.timedelta(hours=1)
    sup._last_tick_at = start
    spawns: list[dt.datetime] = []

    async def fake_spawn():
        spawns.append(clock.now())
        if spawn is not None:
            await spawn()
        sup._reset_spawn_stamps()
        sup._set_state("WARMING")

    async def fake_terminate():
        sup._proc = None

    monkeypatch.setattr(sup, "_spawn_child", fake_spawn)
    monkeypatch.setattr(sup, "_terminate_child", fake_terminate)
    return sup, now, notify, rec, spawns


async def _drive(sup, now: _Now, seconds: int, *, beat: dict | None = None) -> None:
    """``seconds`` one-second monitor cycles of the tick-silence path: a heartbeat lands each second (it
    promotes a respawned child out of WARMING, as the real first heartbeat does), and no tick ever does."""
    frame = beat or _hb(True, connect_seq=1)
    for _ in range(seconds):
        now.set(now.value + dt.timedelta(seconds=1))
        await sup._handle_frame(frame)
        await sup._check_tick_silence(120.0)
        await sup._check_tick_silence_respawn(300.0, 3)


def _events(rec: _LogRec, name: str) -> list[tuple[str, dict]]:
    return [(lvl, kw) for (lvl, ev, kw) in rec.calls if ev == name]


def _kinds(notify: _NotifyRec) -> list[MessageKind]:
    return [m.kind for m in notify.msgs]


@pytest.mark.asyncio
async def test_silent_connected_feed_respawns_once_past_300s_through_the_real_monitor(monkeypatch):
    """The REAL monitor loop: DEGRADED at 121 s, no respawn at 299 s or 300 s, exactly one
    ``tick_silence`` respawn at 301 s, and the loop returns after it (a fresh monitor takes over)."""
    now = _Now(_at(10, 0, 0))
    clock = Clock(time_source=now)
    notify = _NotifyRec()
    sup = _monitor_sup(clock, calendar=_DayCalendar(), notify=notify)
    sup._last_tick_at = clock.now()

    respawns: list[str] = []

    async def fake_respawn(*, reason):
        respawns.append(reason)

    monkeypatch.setattr(sup, "_respawn", fake_respawn)

    async def cycle(_i):
        now.set(now.value + dt.timedelta(seconds=1))
        await sup._handle_frame(_hb(True, connect_seq=1))      # the child heartbeats; Kite sends nothing

    await _run_monitor(sup, monkeypatch, cycle, cycles=299)
    assert sup.health().state == "DEGRADED"
    assert respawns == []                                       # 299 s: inside the budget
    assert _kinds(notify) == [MessageKind.FEED_DEGRADED]

    await _run_monitor(sup, monkeypatch, cycle, cycles=10)
    assert respawns == ["tick_silence"]                         # 301 s: once, and the loop returned
    assert now.value == _at(10, 5, 1)


@pytest.mark.asyncio
async def test_tick_silence_never_respawns_outside_market_hours(monkeypatch):
    """DEGRADED left over after the 15:30 close, or on a holiday, never respawns."""
    sup, now, notify, _rec, spawns = _silent_feed_rig(monkeypatch, _at(15, 0, 0))
    sup._state = "DEGRADED"
    now.set(_at(15, 31, 0))                                     # 31 min of silence, but the session is shut
    await _drive(sup, now, 600)
    assert spawns == [] and sup.health().state == "DEGRADED"

    holiday, hnow, _n, _r, hspawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0), calendar=_FakeCalendar(None))
    holiday._state = "DEGRADED"
    await _drive(holiday, hnow, 900)
    assert hspawns == [] and holiday.health().state == "DEGRADED"
    assert notify.msgs == []


@pytest.mark.asyncio
async def test_each_fresh_connection_gets_the_full_budget_from_its_healthy_entry(monkeypatch):
    """After a tick-silence respawn the fresh child goes HEALTHY on its first heartbeat and re-enters
    DEGRADED 120 s later; its own respawn waits a full 300 s from that HEALTHY entry."""
    sup, now, notify, rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))

    await _drive(sup, now, 301)
    assert spawns == [_at(10, 5, 1)]                            # first connection: 301 s after the last tick
    await _drive(sup, now, 1)
    assert sup.health().state == "HEALTHY" and sup._healthy_since == _at(10, 5, 2)

    await _drive(sup, now, 150)
    assert sup.health().state == "DEGRADED"                     # the re-entry ...
    assert _kinds(notify) == [MessageKind.FEED_DEGRADED]        # ... does not page again

    await _drive(sup, now, 150)                                 # 10:10:02: 300 s after the HEALTHY entry
    assert len(spawns) == 1
    await _drive(sup, now, 1)
    assert spawns[1] == _at(10, 10, 3)                          # 301 s after it, not 301 s after the old tick
    assert [kw["attempt"] for _lvl, kw in _events(rec, "ticker_tick_silence_respawn")] == [1, 2]


@pytest.mark.asyncio
async def test_tick_silence_respawn_times_from_the_later_of_last_tick_and_healthy_entry(monkeypatch):
    """The respawn clock is the LATER of the last tick and the HEALTHY entry, not
    ``_effective_tick_silence_s`` (last tick first): a feed that recovered from STALE at 10:04 with a 10:00
    tick on file has had 120 s of silence at 10:06, not 360 s."""
    sup, now, _notify, _rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))
    sup._state = "DEGRADED"
    sup._healthy_since = _at(10, 4, 0)
    now.set(_at(10, 6, 0))

    assert await sup._check_tick_silence_respawn(300.0, 3) is False
    now.set(_at(10, 9, 0))
    assert await sup._check_tick_silence_respawn(300.0, 3) is False
    now.set(_at(10, 9, 1))
    assert await sup._check_tick_silence_respawn(300.0, 3) is True
    assert spawns == [_at(10, 9, 1)]


@pytest.mark.asyncio
async def test_three_respawns_then_one_escalation_and_one_degraded_alert(monkeypatch):
    """The cap: 3 respawns, then none; one ERROR and one ``feed_silent_unrecovered`` notify; and the
    ``feed_degraded`` notify fired once over the episode despite four DEGRADED entries."""
    sup, now, notify, rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))

    await _drive(sup, now, 1500)
    assert len(spawns) == 3
    assert [kw["attempt"] for _lvl, kw in _events(rec, "ticker_tick_silence_respawn")] == [1, 2, 3]
    assert all(kw["max_respawns"] == 3 for _lvl, kw in _events(rec, "ticker_tick_silence_respawn"))
    assert len(_events(rec, "feed_tick_silence_degraded")) == 4   # every entry still logs ...
    assert _kinds(notify) == [MessageKind.FEED_DEGRADED, MessageKind.FEED_SILENT_UNRECOVERED]  # ... pages don't

    exhausted = _events(rec, "ticker_tick_silence_respawns_exhausted")
    assert [lvl for lvl, _kw in exhausted] == ["error"]
    assert exhausted[0][1]["respawns"] == 3
    assert exhausted[0][1]["since"] == _at(10, 0, 0).isoformat()    # the episode's last real tick
    msg = notify.msgs[1]
    assert msg.severity == "warning" and msg.data["respawns"] == 3
    assert "since 10:00 IST" in msg.body and "3 fresh" in msg.body
    assert msg.dedupe_key == f"feed_silent_unrecovered:{_at(10, 0, 0).isoformat()}"

    await _drive(sup, now, 1800)                                    # cap spent: DEGRADED, quiet, no respawns
    assert len(spawns) == 3 and len(notify.msgs) == 2
    assert len(_events(rec, "ticker_tick_silence_respawns_exhausted")) == 1
    assert sup.health().state == "DEGRADED"


@pytest.mark.asyncio
async def test_a_tick_after_respawns_starts_a_new_episode_with_a_full_cap(monkeypatch):
    """A tick after two respawns (DEGRADED → HEALTHY via the tick) ends the episode: a later silence
    gets a fresh ``feed_degraded`` page and three more respawns before it escalates."""
    sup, now, notify, rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))

    await _drive(sup, now, 800)                                     # third child DEGRADED since t=725
    assert len(spawns) == 2 and sup.health().state == "DEGRADED"
    await sup._handle_frame(_tick_frame(token=1, ts=now.value))     # Kite delivers again
    assert sup.health().state == "HEALTHY"
    assert (sup._silence_respawns, sup._silence_notified, sup._silence_episode_date) == (0, False, None)

    await _drive(sup, now, 2000)
    assert len(spawns) == 5                                         # 2 + a full cap of 3
    assert _kinds(notify) == [MessageKind.FEED_DEGRADED, MessageKind.FEED_DEGRADED,
                              MessageKind.FEED_SILENT_UNRECOVERED]
    attempts = [kw["attempt"] for _lvl, kw in _events(rec, "ticker_tick_silence_respawn")]
    assert attempts == [1, 2, 1, 2, 3]


@pytest.mark.asyncio
async def test_a_tick_on_a_heartbeat_promoted_child_also_ends_the_episode(monkeypatch):
    """The fresh child is HEALTHY (heartbeat-promoted) when its first tick lands, not DEGRADED; that
    tick must end the episode too, and the HEALTHY promotion itself must not."""
    sup, now, _notify, _rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))

    await _drive(sup, now, 302)
    assert len(spawns) == 1 and sup.health().state == "HEALTHY"
    assert sup._silence_respawns == 1                               # the promotion kept the episode
    await sup._handle_frame(_tick_frame(token=1, ts=now.value))
    assert sup._silence_respawns == 0 and sup._silence_episode_date is None


@pytest.mark.asyncio
async def test_a_new_session_date_resets_a_spent_cap(monkeypatch):
    """Capped and escalated yesterday, still DEGRADED at today's open: today's session gets a full
    cap, timed from today's 09:15 open (not yesterday's silence), and its own ``feed_degraded`` page."""
    sup, now, notify, _rec, spawns = _silent_feed_rig(monkeypatch, _at(14, 0, 0))
    await _drive(sup, now, 1500)
    assert len(spawns) == 3 and sup._silence_escalated and sup.health().state == "DEGRADED"

    now.set(dt.datetime(2026, 6, 18, 9, 0, 0, tzinfo=IST))         # next morning, pre-open
    await _drive(sup, now, 15 * 60 + 300)                           # to 09:20:00
    assert len(spawns) == 3
    await _drive(sup, now, 1)
    assert spawns[3] == dt.datetime(2026, 6, 18, 9, 20, 1, tzinfo=IST)
    assert sup._silence_respawns == 1 and not sup._silence_escalated

    await _drive(sup, now, 200)                                     # the fresh child re-enters DEGRADED
    assert _kinds(notify) == [MessageKind.FEED_DEGRADED, MessageKind.FEED_SILENT_UNRECOVERED,
                              MessageKind.FEED_DEGRADED]


@pytest.mark.asyncio
async def test_a_tick_silence_respawn_whose_spawn_raises_keeps_supervising_and_counts(monkeypatch):
    """A spawn that raises takes the existing ``_spawn_failed`` path (STALE + a retry task standing in
    for the monitor) and still spends an attempt."""
    import engine.broker.ticker_supervisor as mod

    async def denied():
        raise PermissionError(13, "Access is denied")

    async def parked(_seconds: float) -> None:
        await asyncio.Event().wait()

    sup, now, _notify, _rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0), spawn=denied)
    monkeypatch.setattr(mod, "_monitor_sleep", parked)
    sup._state = "DEGRADED"
    now.set(_at(10, 5, 1))

    assert await sup._check_tick_silence_respawn(300.0, 3) is True   # must not raise
    retry = sup._monitor_task
    assert len(spawns) == 1 and sup._silence_respawns == 1
    assert sup.health().state == "STALE" and sup._spawn_failures == 1
    assert retry is not None and retry.get_name() == "ticker-spawn-retry" and not retry.done()
    await sup.stop()
    assert retry.cancelled()


@pytest.mark.asyncio
async def test_a_tick_silence_respawn_without_a_token_stops_and_counts(monkeypatch):
    """No access token: the existing STOPPED path, no spawn, and the attempt is still spent."""
    sup, now, _notify, rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))
    sup._access_token = None
    sup._state = "DEGRADED"
    now.set(_at(10, 5, 1))

    assert await sup._check_tick_silence_respawn(300.0, 3) is True
    assert spawns == [] and sup._silence_respawns == 1
    assert sup.health().state == "STOPPED"
    assert _events(rec, "ticker_respawn_no_access_token")


@pytest.mark.asyncio
async def test_tick_silence_logs_carry_the_childs_own_link_state(monkeypatch):
    """The DEGRADED and respawn WARNINGs carry the child's ``ws_connected`` and its OWN
    ``last_tick_age_s``: a small child-side age while the supervisor sees silence is a forwarding fault."""
    sup, now, _notify, rec, _spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0))
    beat = {"type": "heartbeat", "seq": 1, "ws_connected": True, "last_tick_age_s": 1234.5, "connect_seq": 1}

    await _drive(sup, now, 301, beat=beat)
    (_lvl, degraded), = _events(rec, "feed_tick_silence_degraded")
    assert (degraded["ws_connected"], degraded["child_last_tick_age_s"], degraded["silence_respawns"]) == (
        True, 1234.5, 0)
    (lvl, respawn), = _events(rec, "ticker_tick_silence_respawn")
    assert lvl == "warning"
    assert (respawn["ws_connected"], respawn["child_last_tick_age_s"]) == (True, 1234.5)
    assert respawn["tick_silence_s"] > 300
    assert sup._child_tick_age_s is None                            # per-spawn: the fresh child reported nothing

    await sup._handle_frame({"type": "heartbeat", "last_tick_age_s": None})   # "no tick yet" is kept as None
    assert sup._child_tick_age_s is None
    await sup._handle_frame({"type": "heartbeat", "last_tick_age_s": 2.5})
    await sup._handle_frame({"type": "heartbeat"})                  # absent: leaves the value alone
    assert sup._child_tick_age_s == 2.5


@pytest.mark.asyncio
async def test_a_failing_escalation_notify_never_crashes_the_monitor(monkeypatch):
    """The owner sink raising (Telegram down) is logged; the escalation is still marked once."""

    class _Boom:
        async def __call__(self, _msg) -> None:
            raise RuntimeError("telegram down")

    sup, now, _notify, rec, spawns = _silent_feed_rig(monkeypatch, _at(10, 0, 0), notify=_Boom())
    await _drive(sup, now, 1500)                                    # must not raise
    assert len(spawns) == 3 and sup._silence_escalated
    assert len(_events(rec, "feed_silent_unrecovered_notify_failed")) == 1


def test_feed_silent_unrecovered_dedupe_key_is_per_episode():
    """The next session's (or a later episode's) escalation must not be swallowed by today's key."""
    from engine.notify.catalog import feed_silent_unrecovered

    today = feed_silent_unrecovered(respawns=3, since=_at(13, 46, 0), silence_s=1200.0)
    later = feed_silent_unrecovered(respawns=3, since=_at(14, 51, 0), silence_s=1200.0)
    tomorrow = feed_silent_unrecovered(
        respawns=3, since=dt.datetime(2026, 6, 18, 9, 15, tzinfo=IST), silence_s=1200.0)
    assert len({today.dedupe_key, later.dedupe_key, tomorrow.dedupe_key}) == 3
    assert today.kind == MessageKind.FEED_SILENT_UNRECOVERED and today.severity == "warning"
    for phrase in ("since 13:46 IST", "3 fresh Kite connections", "Intraday detection and live marks are blind",
                   "Kite-side outage", "exchange halt", "network"):
        assert phrase in today.body


def test_tick_silence_respawn_keys_load_from_settings_yaml():
    """The real config/settings.yaml through the real loader: the new knobs carry the seeded values."""
    from engine.core.config import load_settings

    s = load_settings()
    assert (s.ticker.tick_silence_respawn_s, s.ticker.max_tick_silence_respawns) == (300, 3)
