"""TickerSupervisor respawn lifecycle (§2.2/§3.2.2, A4/R2): a respawn must cancel the old read loop
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
# It is dropped: one WARNING, stream untouched. WO-24e (2026-08-21) split it onto its OWN drop
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
    assert sup.stats_snapshot()["frames_dropped"] == {"implausible_timestamp": 1}  # own drop path (WO-24e)
    warns = [kw for (lvl, ev, kw) in rec.calls if ev == "tick_timestamp_implausible"]
    assert len(warns) == 1
    assert warns[0]["symbol"] == "R"
    assert warns[0]["parsed"].startswith("1970-01-01")
    assert all(lvl == "warning" for (lvl, ev, _kw) in rec.calls if ev == "tick_timestamp_implausible")
    # WO-24e: no ERROR-level traceback for this working-as-designed drop — the WARNING is the ONLY log line.
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
    # exactly one of them was dropped, on its own counter (WO-24e).
    snap = sup.stats_snapshot()
    assert snap["ticks_received"] == 2
    assert snap["frames_dropped"] == {"implausible_timestamp": 1}


@pytest.mark.asyncio
async def test_missing_timestamp_tick_still_takes_the_generic_parse_error_path(clock, monkeypatch):
    """A genuinely ABSENT ``exchange_timestamp`` is a different defect than epoch-0 (WO-24e split
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

    # Recovery resets the backoff + escalation so a later episode can escalate again (§2.6).
    sup._set_state("HEALTHY")
    assert sup._wedge_respawns == 0
    assert sup._wedge_escalated is False
