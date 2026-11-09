"""Golden replay day — ``engine.paper.replay.ReplayHarness``.

Contract: *same inputs ⇒ byte-identical decision log*. The "decision log" is the harness digest — the
finalized bars plus the ``order.update`` frame stream the harness collected, with platform-minted
ULIDs masked (the ONE accepted non-determinism). Also asserted, because the digest would hide drift:

- two runs over the same partitions ⇒ identical digest and bars; the day's sort key is TOTAL, so rows
  tying on ``(exchange_ts, tradingsymbol, volume_traded, ltp)`` and differing only in their L1 quote
  still deliver in one fixed order
- pre-open ticks build no bar; the open bar's open is the first IN-SESSION print
- the post-close tick builds no bar and is counted
- the attached broker sees every in-session tick and bar, in order, with the clock standing at the
  tick's own ``exchange_ts`` — nothing in a replay may read wall time
- the harness OWNS the publish collector: a real ``PaperBroker`` built with ``harness.publish`` puts
  postbacks/fills on the report and moves the digest; a broker whose book GROWS while it published
  nothing is a hard error; a changing ULID does not move the digest
- replay actions are a function of the STREAM, not task scheduling: ``harness.schedule`` at a NAMED
  minute gives the same fill count and digest on every run
- the day is streamed: the tick loop yields once per ``_YIELD_EVERY_TICKS`` slice (counted, not
  timed) and never holds the loop for a second
- multi-day: per-day bar counts, disjoint days, day order irrelevant
- the live ``market.duckdb`` is never opened, even when one sits in the archive root
- warm-run performance smoke ("a session-day in minutes")
"""

from __future__ import annotations

import asyncio
import sys
import time as _time
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import PaperSettings, config_dir
from engine.core.contracts import ORDER_UPDATE_TOPIC, PAPER_ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.db import connect
from engine.core.enums import RiskState
from engine.core.eventbus import EventBus
from engine.core.migrations import apply_migrations
from engine.core.types import Bar, Tick
from engine.marketdata.store import _TICK_COLUMNS
from engine.oms.manager import paper_order_guard
from engine.ops.paper_control import PaperControl
from engine.ops.paper_runtime import PaperRuntime
from engine.paper.broker import PaperBroker
from engine.paper.fill_model import FillModelConfig
from engine.paper.replay import (
    ORDER_BY_COLUMNS,
    ReplayContractError,
    ReplayHarness,
    ReplayReport,
    mask_ulids,
)
from tests.replay.conftest import REPLAY_DAY, REPLAY_DAY_2
from tests.replay.fixtures.synthetic_day import TICK_COLUMNS, TIE_BIDS, TIE_SYMBOL, SyntheticDay

# ULID-shaped tokens standing in for OMS order ids; two different ones so masking is proven, not luck.
ULID_A = "01JBQ7X2K3MNP4RSTV5WXY6Z7A"
ULID_B = "01JBQ8Y3M4NPQ5STVW6XYZ7A8B"


class StubBroker:
    """The duck-typed broker seam the harness drives (``on_tick`` / ``on_bar``).

    Deliberately NOT ``PaperBroker``: the harness must stay usable with a two-method stub (the
    :class:`~engine.paper.replay.ReplayBroker` contract). It publishes ONE ``order.update`` frame on
    its first tick through the harness's publish seam, the only way anything reaches the digest.
    """

    def __init__(self, clock: Clock, publish: Any, order_id: str) -> None:
        self._clock = clock
        self._publish = publish
        self._order_id = order_id
        self.ticks: list[Tick] = []
        self.tick_clock: list[datetime] = []
        self.bars: list[Bar] = []
        self.sequence: list[tuple[str, str, str]] = []      # (kind, symbol, iso ts) in arrival order
        self._published = False

    def on_tick(self, tick: Tick) -> None:
        self.ticks.append(tick)
        self.tick_clock.append(self._clock.now())
        self.sequence.append(("tick", tick.tradingsymbol, tick.exchange_ts.isoformat()))
        if not self._published:
            self._published = True
            self._publish(
                ORDER_UPDATE_TOPIC,
                OrderUpdateFrame(data={
                    "order_id": self._order_id, "status": "OPEN", "filled_quantity": 0,
                    "tradingsymbol": tick.tradingsymbol, "tag": self._order_id,
                }),
            )

    def on_bar(self, bar: Bar) -> None:
        self.bars.append(bar)
        self.sequence.append(("bar", bar.symbol, bar.ts_minute.isoformat()))


def _run(day: SyntheticDay, scratch: Path, order_id: str | None = None) -> tuple[
    ReplayHarness, ReplayReport, StubBroker | None
]:
    """Replay the synthetic day into a fresh scratch dir; attach a stub broker when ``order_id`` given."""
    harness = ReplayHarness(day.root, scratch)
    broker: StubBroker | None = None
    if order_id is not None:
        broker = StubBroker(harness.clock, harness.publish, order_id)
        harness.attach_broker(broker)
    try:
        report = _await(harness.run([day.day]))
    finally:
        harness.close()
    return harness, report, broker


def _await(coro):
    """Drive the harness's async ``run`` from a sync test (no pytest-asyncio in this repo's deps)."""
    return asyncio.run(coro)


# ------------------------------------------------------------------ lockstep with the store's schema
def test_tick_column_lockstep() -> None:
    """The harness and the fixture read/write exactly the columns the STORE writes."""
    from engine.paper import replay

    assert replay.TICK_COLUMNS == _TICK_COLUMNS
    assert TICK_COLUMNS == _TICK_COLUMNS


def test_sort_key_covers_every_tick_column() -> None:
    """The day's ``ORDER BY`` is a permutation of the columns — that is what makes it TOTAL."""
    assert sorted(ORDER_BY_COLUMNS) == sorted(TICK_COLUMNS)
    assert ORDER_BY_COLUMNS[:2] == ("exchange_ts", "tradingsymbol")


# ------------------------------------------------------------------ determinism (golden day)
def test_two_runs_are_byte_identical(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    h1, r1, _ = _run(synthetic_day, tmp_path / "run1")
    h2, r2, _ = _run(synthetic_day, tmp_path / "run2")

    assert r1.digest == r2.digest
    assert len(r1.digest) == 64                                   # sha256 hex
    assert h1.bars(synthetic_day.day) == h2.bars(synthetic_day.day)
    assert r1.ticks_read == r2.ticks_read == synthetic_day.total_ticks
    assert r1.bars_built == r2.bars_built


def test_tied_rows_deliver_in_total_key_order(tie_day: tuple[Path, Any], tmp_path: Path) -> None:
    """Two prints tying on ``(exchange_ts, tradingsymbol, volume_traded, ltp)`` and differing only in
    ``bid``/``ask`` deliver in ONE fixed order (by bid), run after run.

    The fixture writes them highest-bid-first, so a four-key sort would return file order. The L1
    quote drives the broker's half-spread, so a flip is a different fill price for identical input.
    """
    root, day = tie_day
    seen: list[list[Decimal]] = []
    for run in ("tie1", "tie2"):
        harness = ReplayHarness(root, tmp_path / run)
        broker = StubBroker(harness.clock, harness.publish, ULID_A)
        harness.attach_broker(broker)
        try:
            _await(harness.run([day]))
        finally:
            harness.close()
        tied = [t.bid for t in broker.ticks if t.tradingsymbol == TIE_SYMBOL][: len(TIE_BIDS)]
        seen.append(tied)

    assert seen[0] == list(TIE_BIDS)         # ascending by bid == the total key's order
    assert seen[0] == seen[1]                # …and stable across runs


def test_report_counts_partition_the_stream(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    _, report, _ = _run(synthetic_day, tmp_path / "counts")

    assert report.pre_open_excluded == synthetic_day.pre_open_ticks
    assert report.post_close_dropped == synthetic_day.post_close_ticks
    assert report.ticks_delivered == synthetic_day.session_ticks
    # The counters partition the rows read.
    assert (
        report.pre_open_excluded + report.post_close_dropped + report.ticks_delivered
        == report.ticks_read
    )
    # One bar per (symbol, minute-with-prints); the gap minute contributes none.
    assert report.bars_built[synthetic_day.day] == synthetic_day.session_minutes * len(
        synthetic_day.symbols
    )


def test_symbol_filter_restricts_the_read(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    harness = ReplayHarness(synthetic_day.root, tmp_path / "one_symbol")
    try:
        report = _await(harness.run([synthetic_day.day], symbols=["SYNB"]))
    finally:
        harness.close()
    assert {b.symbol for b in harness.bars(synthetic_day.day)} == {"SYNB"}
    assert report.ticks_read == synthetic_day.total_ticks // len(synthetic_day.symbols)


# ------------------------------------------------------------------ pre-open semantics
def test_pre_open_builds_no_bar_and_open_is_first_session_print(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    harness, _, _ = _run(synthetic_day, tmp_path / "preopen")
    bars = harness.bars(synthetic_day.day)

    open_at = datetime.combine(synthetic_day.day, time(9, 15), tzinfo=IST)
    assert [b for b in bars if b.ts_minute < open_at] == []       # zero pre-open contamination

    for symbol in synthetic_day.symbols:
        first = next(b for b in bars if b.symbol == symbol and b.ts_minute == open_at)
        # The open is the first IN-SESSION print; the auction price rides in its own column on the
        # 09:15 row only.
        assert first.open == synthetic_day.first_session_ltp[symbol]
        assert first.open != synthetic_day.auction_open[symbol]
        assert first.auction_open == synthetic_day.auction_open[symbol]
        assert first.src == "self"
        # Engine up from the open ⇒ the WHOLE cumulative belongs to the 09:15 bar (a delta-only
        # builder would report cum_last − cum_first).
        assert first.volume == synthetic_day.open_bar_volume[symbol]
        assert first.volume > synthetic_day.first_session_cum[symbol]
        later = next(b for b in bars if b.symbol == symbol and b.ts_minute > open_at)
        assert later.auction_open is None                          # stamped on the 09:15 row ONLY


def test_gap_minute_produces_no_bar(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    harness, _, _ = _run(synthetic_day, tmp_path / "gap")
    bars = harness.bars(synthetic_day.day)
    assert [b for b in bars if b.ts_minute == synthetic_day.gap_minute] == []
    # The minutes either side exist, so the gap is a hole and not a truncation.
    assert [b for b in bars if b.ts_minute == synthetic_day.gap_minute - timedelta(minutes=1)]
    assert [b for b in bars if b.ts_minute == synthetic_day.gap_minute + timedelta(minutes=1)]


# ------------------------------------------------------------------ post-close symmetry
def test_post_close_tick_builds_no_bar_and_is_counted(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    harness, report, broker = _run(synthetic_day, tmp_path / "postclose", order_id=ULID_A)
    bars = harness.bars(synthetic_day.day)
    close_at = datetime.combine(synthetic_day.day, time(15, 30), tzinfo=IST)

    assert [b for b in bars if b.ts_minute >= close_at] == []
    assert report.post_close_dropped == len(synthetic_day.symbols)
    assert broker is not None
    assert [t for t in broker.ticks if t.exchange_ts >= close_at] == []


# ------------------------------------------------------------------ broker seam + replay clock
def test_broker_sees_every_session_tick_and_bar_in_order(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    harness, report, broker = _run(synthetic_day, tmp_path / "broker", order_id=ULID_A)
    assert broker is not None

    assert len(broker.ticks) == report.ticks_delivered == synthetic_day.session_ticks
    # At every delivery ``Clock.now()`` IS this tick's exchange_ts.
    assert broker.tick_clock == [t.exchange_ts for t in broker.ticks]
    assert all(ts.tzinfo is not None for ts in broker.tick_clock)
    stamps = [t.exchange_ts for t in broker.ticks]
    assert stamps == sorted(stamps)
    assert broker.bars == harness.bars(synthetic_day.day)

    # Bars arrive interleaved with ticks (at the minute boundary at or after minute+grace), not
    # batched at the end of the day.
    kinds = [kind for kind, _, _ in broker.sequence]
    assert kinds[0] == "tick"
    assert "bar" in kinds[: len(kinds) // 2]


def test_harness_clock_never_reads_wall_time(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    """After the run the clock stands at the LAST tick of the day, not at wall time."""
    harness, _, _ = _run(synthetic_day, tmp_path / "clock")
    assert harness.clock.now() == synthetic_day.post_close_at


# ------------------------------------------------------------------ the harness owns the collector
#: The named minute the scripted order goes in at. The fixture prints at seconds 1/11/21/31/41/51 of
#: every session minute, so the first tick at or after 10:00:00 is SYNA's 10:00:01 print.
ORDER_AT = time(10, 0)
ORDER_TICK_TS = "10:00:01"


class ScriptedTrader:
    """Wraps the REAL :class:`~engine.paper.broker.PaperBroker` and places ONE SCRIPTED order.

    :meth:`place` is handed to ``harness.schedule``, so the harness calls it inside the tick loop,
    immediately before a named tick, with the replay clock already standing at that tick. Nothing in a
    replay may depend on task timing; ``on_tick`` is a pure pass-through.
    """

    def __init__(self, broker: PaperBroker, symbol: str, quantity: int) -> None:
        self._broker = broker
        self._symbol = symbol
        self._quantity = quantity
        self.order_id: str | None = None

    def on_tick(self, tick: Tick) -> None:
        self._broker.on_tick(tick)

    def on_bar(self, bar: Bar) -> None:
        self._broker.on_bar(bar)

    async def orders(self) -> list:
        return await self._broker.orders()

    async def place(self) -> None:
        self.order_id = await self._broker.place_order({
            "variety": "regular", "exchange": "NSE", "tradingsymbol": self._symbol,
            "transaction_type": "BUY", "order_type": "MARKET", "product": "MIS",
            "quantity": self._quantity,
        })


def _paper_broker(harness: ReplayHarness) -> PaperBroker:
    """The real PaperBroker, wired to the harness's clock and publish."""
    return PaperBroker(
        clock=harness.clock,
        publish=harness.publish,
        fill_model=FillModelConfig(),
        tick_size=lambda _symbol: Decimal("0.05"),
        rng_seed=20260910,
        rejection_rate=0.0,          # injected rejections have their own tests; this one is about wiring
    )


def _scripted_paper_run(
    day: SyntheticDay, scratch: Path
) -> tuple[ReplayHarness, ReplayReport, ScriptedTrader]:
    """One replay of ``day`` with the real PaperBroker and ONE order scripted at :data:`ORDER_AT`."""
    harness = ReplayHarness(day.root, scratch)
    trader = ScriptedTrader(_paper_broker(harness), symbol="SYNA", quantity=5)
    harness.attach_broker(trader)
    harness.schedule(datetime.combine(day.day, ORDER_AT, tzinfo=IST), trader.place)
    try:
        report = _await(harness.run([day.day]))
    finally:
        harness.close()
    return harness, report, trader


def test_real_paper_broker_postbacks_reach_the_report_and_digest(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """The harness OWNS the publish collector AND drives the order itself: three consecutive runs
    produce identical digests, and the real broker's postbacks reach the report and move the digest."""
    digests: list[str] = []
    bars_built: dict | None = None
    for run in ("paper1", "paper2", "paper3"):
        harness, report, trader = _scripted_paper_run(synthetic_day, tmp_path / run)

        assert report.actions_fired == 1               # the scripted seam really ran
        assert trader.order_id is not None             # the order really went through the surface
        assert report.postbacks > 0                    # …and its postbacks reached the harness
        assert report.orders == 1
        assert report.fills == 1                       # exactly one, every run
        frames = harness.postbacks()
        assert len(frames) == report.postbacks
        assert {f.data["order_id"] for f in frames} == {trader.order_id}
        # The order is stamped by the STREAM: the clock stood on the scheduled tick.
        assert frames[0].data["order_timestamp"] == f"{synthetic_day.day.isoformat()} {ORDER_TICK_TS}"
        assert frames[-1].data["status"] == "COMPLETE"
        assert frames[-1].data["filled_quantity"] == 5

        digests.append(report.digest)
        bars_built = report.bars_built

    assert len(set(digests)) == 1, f"three runs of identical input produced {len(set(digests))} digests"

    # The digest covers the frames: the same day with no order placed hashes differently, while the
    # bars are untouched.
    _, quiet, _ = _run(synthetic_day, tmp_path / "paper_quiet")
    assert digests[0] != quiet.digest
    assert bars_built == quiet.bars_built


def test_at_tick_action_fires_before_that_stream_ordinal(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """``at_tick(n, fn)`` runs ``fn`` immediately before the n-th tick of the stream is fed.

    The ordinal counts every tick READ (pre-open included): a property of the archive, not of what
    the broker was shown.
    """
    harness = ReplayHarness(synthetic_day.root, tmp_path / "at_tick")
    broker = StubBroker(harness.clock, harness.publish, ULID_A)
    harness.attach_broker(broker)
    seen: list[datetime] = []
    harness.at_tick(0, lambda: seen.append(harness.clock.now()))
    harness.at_tick(100, lambda: seen.append(harness.clock.now()))
    try:
        report = _await(harness.run([synthetic_day.day]))
    finally:
        harness.close()

    assert report.actions_fired == 2
    # Tick 0 is the first PRE-OPEN print (09:00:00). Same ordinals ⇒ same instants, run after run.
    assert seen[0] == datetime.combine(synthetic_day.day, time(9, 0), tzinfo=IST)
    assert seen[1] > seen[0]
    harness2 = ReplayHarness(synthetic_day.root, tmp_path / "at_tick2")
    again: list[datetime] = []
    harness2.at_tick(0, lambda: again.append(harness2.clock.now()))
    harness2.at_tick(100, lambda: again.append(harness2.clock.now()))
    try:
        _await(harness2.run([synthetic_day.day]))
    finally:
        harness2.close()
    assert again == seen


def test_scripted_action_the_stream_never_reaches_is_an_error(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """An action scheduled past the end of the replayed stream is loud, not a silent no-op."""
    harness = ReplayHarness(synthetic_day.root, tmp_path / "unreached")
    harness.schedule(
        datetime.combine(synthetic_day.day, time(23, 59), tzinfo=IST), lambda: None
    )
    with pytest.raises(ReplayContractError, match="never reached by the stream"):
        try:
            _await(harness.run([synthetic_day.day]))
        finally:
            harness.close()


class MuteBroker:
    """A broker that BOOKS an order during the run and publishes NOTHING — the wiring bug.

    The check is about order work THIS run did, so the book must GROW during the replay; one already
    full at the start is the ordinary second-run case.
    """

    def __init__(self, booked: list | None = None) -> None:
        self.book: list = list(booked or ())

    def on_tick(self, tick: Tick) -> None:
        if not self.book:
            self.book.append({"order_id": "251009000000001", "status": "OPEN", "filled_quantity": 0})

    def on_bar(self, bar: Bar) -> None: ...

    async def orders(self) -> list:
        return list(self.book)


def test_broker_with_orders_but_no_postbacks_is_a_hard_error(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """A bars-only digest from a run that DID order work is refused, never quietly reported."""
    harness = ReplayHarness(synthetic_day.root, tmp_path / "mute")
    harness.attach_broker(MuteBroker())
    with pytest.raises(ReplayContractError, match="publish=harness.publish"):
        try:
            _await(harness.run([synthetic_day.day]))
        finally:
            harness.close()


def test_book_already_full_at_run_start_is_not_a_hard_error(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """The check is GROWTH, not emptiness: a book that did not change during the run is silent."""
    harness = ReplayHarness(synthetic_day.root, tmp_path / "prefilled")
    harness.attach_broker(
        MuteBroker([{"order_id": "251009000000001", "status": "COMPLETE", "filled_quantity": 1}])
    )
    try:
        report = _await(harness.run([synthetic_day.day]))
    finally:
        harness.close()
    assert report.postbacks == 0


@pytest.mark.parametrize("topic", [ORDER_UPDATE_TOPIC, PAPER_ORDER_UPDATE_TOPIC])
def test_the_harness_collects_only_the_attached_brokers_topic(
    synthetic_day: SyntheticDay, tmp_path: Path, topic: str
) -> None:
    harness = ReplayHarness(synthetic_day.root, tmp_path / "topic")
    broker = PaperBroker(
        clock=harness.clock, publish=harness.publish, fill_model=FillModelConfig(),
        tick_size=lambda _symbol: Decimal("0.05"), rng_seed=1, topic=topic,
    )
    harness.attach_broker(broker)
    frames = {
        t: OrderUpdateFrame(data={"order_id": t}) for t in (ORDER_UPDATE_TOPIC, PAPER_ORDER_UPDATE_TOPIC)
    }
    for t, frame in frames.items():
        harness.publish(t, frame)
    assert harness.postbacks() == [frames[topic]]


def test_second_run_on_the_same_broker_is_not_a_false_positive(
    two_day_archive: tuple[SyntheticDay, SyntheticDay], tmp_path: Path
) -> None:
    """A REAL PaperBroker that ordered on day 1 and did nothing on day 2 must not raise: the check
    compares the book's growth, not its cumulative size, against the current run's frames.

    Two days rather than one twice because the :class:`ReplayClock` is monotonic: a replay cannot rewind.
    """
    day1, day2 = two_day_archive
    harness = ReplayHarness(day1.root, tmp_path / "two_runs")
    trader = ScriptedTrader(_paper_broker(harness), symbol="SYNA", quantity=5)
    harness.attach_broker(trader)
    harness.schedule(datetime.combine(day1.day, ORDER_AT, tzinfo=IST), trader.place)
    try:
        first = _await(harness.run([day1.day]))
        # Day 2: nothing new is ordered, but the broker still holds day 1's.
        second = _await(harness.run([day2.day]))
        book = _await(trader.orders())
    finally:
        harness.close()

    assert first.actions_fired == 1 and first.orders == 1 and first.fills == 1
    assert second.actions_fired == 0
    assert len(book) == 1, "the broker's book really did carry across the two runs"
    assert second.ticks_read == day2.total_ticks


def test_broker_without_orders_is_not_an_error(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    """A silent broker with an empty book is the NORMAL case."""
    _, report, broker = _run(synthetic_day, tmp_path / "silent", order_id=ULID_A)
    assert broker is not None
    assert report.postbacks == 1 and report.orders == 1 and report.fills == 0


def test_ulid_regex_masks_a_crockford_token() -> None:
    assert mask_ulids(ULID_A) == "<ULID>"
    assert mask_ulids(f"order {ULID_A} ok") == "order <ULID> ok"
    assert mask_ulids("SYNA") == "SYNA"                            # short tokens untouched
    assert mask_ulids("2026-06-17T09:15:00+05:30") == "2026-06-17T09:15:00+05:30"


def test_changing_ulid_does_not_move_the_digest(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    _, r1, b1 = _run(synthetic_day, tmp_path / "ulid_a", order_id=ULID_A)
    _, r2, b2 = _run(synthetic_day, tmp_path / "ulid_b", order_id=ULID_B)
    assert b1 is not None and b2 is not None

    assert r1.postbacks == r2.postbacks == 1
    assert r1.digest == r2.digest

    # Control: the frame stream IS in the digest — a non-ULID change must move it.
    _, r3, _ = _run(synthetic_day, tmp_path / "no_broker")
    assert r3.digest != r1.digest


# ------------------------------------------------------------------ streamed, loop not blocked
def test_run_never_blocks_the_event_loop(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    """A heartbeat coroutine ticking every 50 ms alongside ``run()`` sees no gap over 1 s.

    The 1 s bound is the contract, not a discriminating measurement (the synthetic day replays in
    under a second); the yields are counted in ``test_tick_loop_yields_once_per_slice_and_never_blocks_long``.
    """
    harness = ReplayHarness(synthetic_day.root, tmp_path / "heartbeat")
    beats: list[float] = []

    async def _heartbeat(stop: asyncio.Event) -> None:
        while not stop.is_set():
            beats.append(_time.perf_counter())
            await asyncio.sleep(0.05)

    async def _drive() -> ReplayReport:
        stop = asyncio.Event()
        pulse = asyncio.create_task(_heartbeat(stop))
        try:
            return await harness.run([synthetic_day.day])
        finally:
            stop.set()
            await pulse

    try:
        report = _await(_drive())
    finally:
        harness.close()

    assert report.ticks_read == synthetic_day.total_ticks
    # The beat COUNT depends on box speed; the assertion is the GAP, the longest the replay held the loop.
    assert len(beats) >= 3, "the heartbeat never interleaved with the replay at all"
    gaps = [b - a for a, b in zip(beats, beats[1:], strict=False)]
    assert max(gaps) < 1.0, f"event loop blocked for {max(gaps):.2f}s during replay"


class _LoopTurn:
    """Yield control to the event loop WITHOUT ``asyncio.sleep``, which the test counts."""

    def __await__(self):
        yield


def test_tick_loop_yields_once_per_slice_and_never_blocks_long(
    synthetic_day: SyntheticDay, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tick loop yields once per ``_YIELD_EVERY_TICKS`` slice — COUNTED, not timed.

    Counts every ``asyncio.sleep(0)`` made from inside ``engine.paper.replay``: with the slice patched
    down to 250 rows, a day of N ticks must produce at least ``ceil(N / 250)``, a number that is a
    function of the fixture and cannot drift with machine load. The remaining timing assertion is an
    absolute one-second ceiling. The shipped constant is pinned separately, so patching the slice
    down only widens the margin.
    """
    from engine.paper import replay as replay_mod

    assert replay_mod._YIELD_EVERY_TICKS == 2_000       # the shipped value, pinned
    monkeypatch.setattr(replay_mod, "_YIELD_EVERY_TICKS", 250)

    yields = [0]
    real_sleep = asyncio.sleep

    def _counting_sleep(delay: float, *args: Any, **kwargs: Any):
        # Frame-filtered so only the harness's own yields are counted.
        if delay == 0 and sys._getframe(1).f_globals.get("__name__") == "engine.paper.replay":
            yields[0] += 1
        return real_sleep(delay, *args, **kwargs)

    monkeypatch.setattr(asyncio, "sleep", _counting_sleep)

    harness = ReplayHarness(synthetic_day.root, tmp_path / "yields")
    worst = [0.0]

    async def _spin(stop: asyncio.Event) -> None:
        last = _time.perf_counter()
        while not stop.is_set():
            now = _time.perf_counter()
            worst[0] = max(worst[0], now - last)
            last = now
            await _LoopTurn()               # one turn per event-loop iteration

    async def _drive() -> ReplayReport:
        stop = asyncio.Event()
        spinner = asyncio.create_task(_spin(stop))
        try:
            return await harness.run([synthetic_day.day])
        finally:
            stop.set()
            await spinner

    try:
        report = _await(_drive())
    finally:
        harness.close()

    assert report.ticks_read > 6_000
    expected = -(-report.ticks_read // 250)             # ceil
    assert yields[0] >= expected, (
        f"{yields[0]} cooperative yields for {report.ticks_read} ticks at a 250-tick slice — at "
        f"least {expected} are required, so the loop is converting or feeding more than one slice "
        "between yields"
    )
    assert worst[0] < 1.0, (
        f"longest event-loop block was {worst[0]:.3f}s — the tick loop is holding the loop"
    )


# ------------------------------------------------------------------ multi-day
def test_multi_day_run_is_per_day_and_order_independent(
    two_day_archive: tuple[SyntheticDay, SyntheticDay], tmp_path: Path
) -> None:
    day1, day2 = two_day_archive
    forward = ReplayHarness(day1.root, tmp_path / "d1d2")
    try:
        r_fwd = _await(forward.run([day1.day, day2.day]))
        bars1 = forward.bars(day1.day)
        bars2 = forward.bars(day2.day)
    finally:
        forward.close()

    expected = day1.session_minutes * len(day1.symbols)
    assert r_fwd.bars_built == {day1.day: expected, day2.day: expected}
    assert r_fwd.ticks_read == day1.total_ticks + day2.total_ticks
    assert len(bars1) == len(bars2) == expected

    # Every bar sits on its own day; no minute leaks across the rollover.
    assert {b.ts_minute.date() for b in bars1} == {day1.day}
    assert {b.ts_minute.date() for b in bars2} == {day2.day}
    assert len({(b.symbol, b.ts_minute) for b in bars1 + bars2}) == 2 * expected

    # Day ORDER is not an input: run() sorts.
    reverse = ReplayHarness(day1.root, tmp_path / "d2d1")
    try:
        r_rev = _await(reverse.run([day2.day, day1.day]))
    finally:
        reverse.close()
    assert r_rev.days == r_fwd.days == [day1.day, day2.day]
    assert r_rev.digest == r_fwd.digest


# ------------------------------------------------------------------ never the live store
def test_replay_never_opens_the_live_market_duckdb(
    synthetic_day: SyntheticDay, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every DuckDB connection a RUN opens is ``:memory:`` (single-writer).

    A real ``market.duckdb`` is planted in the archive root first. The throwaway scratch store is
    opened by ``harness.open()`` before the spy goes on; only connections opened during the replay
    are checked.
    """
    live = synthetic_day.root / "market.duckdb"
    duckdb.connect(str(live)).close()
    assert live.exists()

    harness = ReplayHarness(synthetic_day.root, tmp_path / "no_live_db")
    harness.open()                                   # scratch store connects here, before the patch

    seen: list[str] = []
    real_connect = duckdb.connect

    def _spy(database: str = ":memory:", *args: Any, **kwargs: Any):
        seen.append(str(database))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", _spy)
    try:
        report = _await(harness.run([synthetic_day.day]))
    finally:
        harness.close()

    assert report.ticks_read == synthetic_day.total_ticks
    assert seen, "the replay opened no DuckDB connection at all — the spy is not wired"
    assert set(seen) == {":memory:"}, f"replay opened a file-backed DuckDB: {seen}"


# ------------------------------------------------------------------ performance smoke
def test_replay_is_fast_enough(synthetic_day: SyntheticDay, tmp_path: Path) -> None:
    """WARM-run wall-clock bound; the session ``_warm_duckdb`` fixture burns DuckDB start-up and DDL.

    The 25 s bound is the "a session-day in minutes" contract with room for a loaded box, deliberately
    NOT the regression pin; the per-minute cost shape is pinned by counting in
    ``test_hot_path_work_is_per_minute_not_per_tick``.
    """
    _, report, _ = _run(synthetic_day, tmp_path / "perf", order_id=ULID_A)
    assert report.ticks_read >= 6_000
    assert report.elapsed_s < 25.0


def test_paper_bridge_keeps_the_day_fast_and_the_loop_free(
    synthetic_day: SyntheticDay, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The PaperRuntime bridge in front of a real PaperBroker, driven by the golden day, stays inside
    the wall-clock bound and never holds the loop for a second.

    The slice is cut to ~3 stream minutes so session prep, which completes on a loop turn, ends
    inside the 5-minute gap tolerance; at the shipped slice every prep spans hours of stream."""
    from engine.paper import replay as replay_mod

    monkeypatch.setattr(replay_mod, "_YIELD_EVERY_TICKS", 50)
    harness = ReplayHarness(synthetic_day.root, tmp_path / "bridge")
    conn = connect(str(tmp_path / "state.db"))
    apply_migrations(conn)
    PaperControl(conn, harness.clock)
    preps: list[datetime] = []

    async def prep(_since: datetime | None, until: datetime) -> None:
        preps.append(until)

    runtime = PaperRuntime(
        conn, harness.clock, NSECalendar(config_dir() / "calendar", harness.clock, strict=False),
        EventBus(), PaperSettings(),
        capital_base_fn=lambda: Decimal("40000"), tick_size_fn=lambda _symbol: Decimal("0.05"),
        order_guard=paper_order_guard(lambda: True, lambda: RiskState.NORMAL), fill_model_fn=FillModelConfig,
        prep=prep,
    )
    harness.attach_broker(runtime)
    forwarded: list[datetime] = []
    beats: list[float] = []

    async def _drive() -> ReplayReport:
        await runtime.start()
        broker_on_tick = runtime.broker.on_tick
        runtime.broker.on_tick = lambda tick: (forwarded.append(tick.exchange_ts), broker_on_tick(tick))
        stop = asyncio.Event()

        async def _heartbeat() -> None:
            while not stop.is_set():
                beats.append(_time.perf_counter())
                await asyncio.sleep(0.05)

        pulse = asyncio.create_task(_heartbeat())
        try:
            return await harness.run([synthetic_day.day])
        finally:
            stop.set()
            await pulse
            await runtime.stop()

    try:
        report = _await(_drive())
    finally:
        harness.close()
        conn.close()

    assert report.elapsed_s < 25.0
    assert max(b - a for a, b in zip(beats, beats[1:], strict=False)) < 1.0
    assert len(preps) == 1
    assert 0 < len(forwarded) < synthetic_day.session_ticks        # ticks during the prep are dropped
    assert runtime.prep_ready() and runtime.last_observed_at == forwarded[-1]


def test_hot_path_work_is_per_minute_not_per_tick(
    synthetic_day: SyntheticDay, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``advance()`` runs once per distinct minute, not per tick, and bar writes are batched rather
    than one per minute. Counted rather than timed, so the numbers are functions of the fixture.
    """
    from engine.marketdata.bar_builder import BarBuilder
    from engine.marketdata.store import MarketStore

    advances = [0]
    writes = [0]
    real_advance = BarBuilder.advance
    real_insert = MarketStore.insert_bars_1m

    def _count_advance(self: BarBuilder) -> Any:
        advances[0] += 1
        return real_advance(self)

    def _count_insert(self: MarketStore, bars: Any) -> int:
        writes[0] += 1
        return real_insert(self, bars)

    monkeypatch.setattr(BarBuilder, "advance", _count_advance)
    monkeypatch.setattr(MarketStore, "insert_bars_1m", _count_insert)

    _, report, _ = _run(synthetic_day, tmp_path / "hotpath")
    bars = report.bars_built[synthetic_day.day]
    assert bars == 1_122 and report.ticks_read == 6_795          # the fixture, pinned

    # One sweep per distinct minute in the stream (~390 here), not one per tick (6,795).
    assert advances[0] < report.ticks_read // 10, (
        f"advance() ran {advances[0]} times over {report.ticks_read} ticks — it is back on the "
        "per-tick path"
    )
    # Bars are buffered 500 at a time, so 1,122 bars cost 3 round trips, not one per minute (375).
    assert writes[0] <= -(-bars // 500) + 1, (
        f"{writes[0]} store writes for {bars} bars — the bar-write buffer is not batching"
    )


# ------------------------------------------------------------------ guardrails
def test_digest_reflects_bar_content_but_not_auction_open() -> None:
    """The digest moves when a bar moves, and does NOT move for ``auction_open``.

    ``auction_open`` is deliberately outside the canonical serialisation (symbol/minute/OHLC/volume/src);
    the pre-open rule is asserted directly in ``test_pre_open_…``."""
    from engine.paper.replay import _digest

    minute = datetime.combine(REPLAY_DAY, time(9, 15), tzinfo=IST)
    bar = Bar(
        symbol="SYNA", ts_minute=minute, open=Decimal("100.00"), high=Decimal("101.00"),
        low=Decimal("99.00"), close=Decimal("100.50"), volume=512, src="self",
        auction_open=Decimal("98.89"),
    )
    assert _digest([bar], []) == _digest([bar], [])
    assert _digest([bar], []) != _digest([bar.model_copy(update={"close": Decimal("100.51")})], [])
    assert _digest([bar], []) != _digest([bar.model_copy(update={"volume": 513})], [])
    assert _digest([bar], []) != _digest([], [])
    assert _digest([bar], []) == _digest(
        [bar.model_copy(update={"auction_open": Decimal("1.00")})], []
    )


def test_scratch_dir_inside_the_archive_is_refused(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """The harness must never be able to write into the archive it is reading (it runs beside the
    live engine)."""
    with pytest.raises(ValueError, match="scratch"):
        ReplayHarness(synthetic_day.root, synthetic_day.root / "scratch")


def test_missing_day_replays_to_an_empty_report(tmp_path: Path) -> None:
    harness = ReplayHarness(tmp_path / "empty_archive", tmp_path / "scratch")
    try:
        report = _await(harness.run([REPLAY_DAY]))
    finally:
        harness.close()
    assert report.ticks_read == 0
    assert report.bars_built[REPLAY_DAY] == 0
    assert report.postbacks == report.orders == report.fills == 0
    assert harness.bars(REPLAY_DAY) == []
    assert report.digest == ReplayHarness.EMPTY_DIGEST


def test_buffered_bar_writes_match_a_direct_write(
    synthetic_day: SyntheticDay, tmp_path: Path
) -> None:
    """The harness's bar-write buffer does not change a byte of what lands in the real MarketStore.

    The reference store is written in batches of 37, falling on different boundaries than the
    harness's 500, so batch boundaries are shown irrelevant to the stored result.
    """
    from engine.marketdata.store import MarketStore

    scratch = tmp_path / "persisted"
    harness = ReplayHarness(synthetic_day.root, scratch)
    window = (
        datetime.combine(synthetic_day.day, time(9, 0), tzinfo=IST),
        datetime.combine(synthetic_day.day, time(16, 0), tzinfo=IST),
    )
    reference = MarketStore(tmp_path / "ref" / "market.duckdb", tmp_path / "ref" / "parquet",
                            harness.clock)
    try:
        _await(harness.run([synthetic_day.day]))
        built = harness.bars(synthetic_day.day)
        reference.open()
        for start in range(0, len(built), 37):
            reference.insert_bars_1m(built[start:start + 37])
        for symbol in synthetic_day.symbols:
            stored = harness.store.get_bars_1m(symbol, *window)
            assert stored == reference.get_bars_1m(symbol, *window)
            assert stored == [b for b in built if b.symbol == symbol]
            assert all(isinstance(b.close, Decimal) for b in stored)
    finally:
        reference.close()
        harness.close()
    assert not (scratch / "market.duckdb").is_dir()
    assert (scratch / "market.duckdb").exists()


def test_report_day_two_bars_are_absent_when_only_day_one_is_replayed(
    two_day_archive: tuple[SyntheticDay, SyntheticDay], tmp_path: Path
) -> None:
    """A single-day run out of a multi-day archive reads only that day's partitions."""
    day1, _ = two_day_archive
    harness = ReplayHarness(day1.root, tmp_path / "d1_only")
    try:
        report = _await(harness.run([day1.day]))
    finally:
        harness.close()
    assert report.days == [day1.day]
    assert report.ticks_read == day1.total_ticks
    assert harness.bars(REPLAY_DAY_2) == []
