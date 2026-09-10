"""Recorded ticks → the real BarBuilder → a scratch store, deterministically (§3.2.9, §8.4, §9.6).

``ReplayHarness`` is WO-P3-3 of the §8.4 Phase-3 decomposition addendum (2026-09-10): the foundation
leg of "backtest = paper = live, one code path" (E1/R9). It replays a recorded trading day out of the
§4.3 tick Parquet dataset through the SAME :class:`~engine.marketdata.bar_builder.BarBuilder` the live
engine runs, into a SCRATCH :class:`~engine.marketdata.store.MarketStore`, with an optional broker
attached, and emits a :class:`ReplayReport` whose ``digest`` is the §9.6 golden-day artefact: *same
inputs ⇒ byte-identical output*.

Seven properties are load-bearing, and each is a deliberate design constraint rather than an
implementation detail:

1. **It never opens ``market.duckdb``.** The archive is read through the harness's OWN
   ``duckdb.connect(":memory:")`` against ``read_parquet`` globs. DuckDB is single-writer and the live
   engine holds that file (§4.1); a replay that took the store's lock could not run beside a trading
   session, which is exactly when you want to run one. ``MarketStore.get_ticks`` is therefore NOT used
   here despite doing the same job — it reads through the live connection and its lock.
2. **The day is STREAMED in bounded chunks, never materialised whole** (§3.2 hot-path invariants,
   §8.4 performance bar). The read is still ONE ordered query, but it is consumed with
   ``fetchmany(20_000)`` on a private single-worker executor thread that hands chunks to the event
   loop through a bounded queue; peak resident memory is O(chunk), not O(day). A recorded tick costs
   ~2.2 KB as a live ``Tick`` — a 300-symbol session-day materialised twice (rows, then models) is
   ~17 GB, which is not a slow replay, it is a dead box.
3. **The tick loop runs ON the event-loop thread, yielding cooperatively.** Every
   :data:`_YIELD_EVERY_TICKS` ticks the loop awaits ``asyncio.sleep(0)``, so timers and bus handlers
   scheduled by anything downstream get to run and the loop is never blocked for longer than
   :data:`_YIELD_EVERY_TICKS` ticks. The rows of a fetch chunk are turned into ``Tick`` models one
   yield-slice at a time (fix round, 2026-09-10) rather than a whole chunk up front: converting
   20,000 rows is ~0.4 s of uninterruptible loop-thread work, so a chunk-at-a-time conversion made
   the yield granularity a lie — the block was bounded by ``_FETCH_ROWS``, not by
   ``_YIELD_EVERY_TICKS``. The loop is deliberately NOT offloaded to a worker thread:
   ``EventBus.publish`` schedules handler tasks on the RUNNING loop (``call_soon``/``create_task``),
   so a tick loop on another thread would either raise or silently queue a day's worth of work onto
   a loop that is idle-waiting for it — which is the same batching hazard as the bus-collector seam
   rejected below.
4. **The clock IS the tick stream.** :class:`ReplayClock` is a ``Clock`` whose ``time_source`` closes
   over the current tick's ``exchange_ts``, advanced BEFORE the tick is delivered. Nothing in a replay
   may read wall time — that is what makes the golden decision log reproducible (§9.1), and the
   ``Clock`` seam is precisely why the platform has never scattered ``datetime.now()`` calls.
5. **The harness OWNS the publish collector.** :meth:`ReplayHarness.publish` is the
   ``publish(topic, event)`` callable a broker is constructed with (``PaperBroker(publish=...)``);
   every ``order.update`` frame it sees is recorded, counted on the report and hashed into the
   digest. The harness never reaches into the broker for a postback log: a getattr-shaped seam that
   matches nothing degrades to an empty list and reports a bars-only digest as though it covered
   orders. If a broker ends the run with a non-empty orderbook and the harness collected zero frames,
   that is a wiring bug and :class:`ReplayContractError` is raised — a silent bars-only digest is
   refused (§8.4 addendum).
6. **ULIDs are the one accepted non-determinism** (§8.4 addendum). Platform-minted ids differ between
   two runs of identical inputs by construction, so every ULID-shaped token is masked before hashing
   (:func:`mask_ulids`).
7. **Replay behaviour is a function of the STREAM, never of task scheduling** (fix round,
   2026-09-10). Anything that must HAPPEN during a replay — placing an order, injecting a gap,
   flipping a config — is registered with :meth:`ReplayHarness.schedule` (at an instant) or
   :meth:`ReplayHarness.at_tick` (at a stream ordinal) and is invoked SYNCHRONOUSLY inside the tick
   loop, immediately before its tick is delivered, with the clock already standing at that tick.
   The rejected alternative was a fire-and-forget ``create_task`` that placed its order at whichever
   cooperative yield the OS happened to schedule it on: measured over the golden day, that produced
   0 or 1 fills between runs of identical input — a replay whose digest depends on thread timing is
   not a replay. Fire-and-forget bus tasks created by code under replay may still run at the yields
   (nothing forbids them), but nothing the digest covers is allowed to depend on WHEN they do.

**Scope of v1.** The strategy → gate → OMS leg joins in WO-P3-5; there is no order origination here
and nothing in this module is reachable from the RECOMMEND pipeline. ``config_overlay`` is accepted
and recorded on the report for provenance but has no consumer yet — it is the seam that tranche will
plug into, and carrying it now keeps the plan-pinned signature honest instead of adding it later.
Raw ticks are never re-persisted: they were just read FROM the Parquet dataset, so staging them back
into a scratch copy of it is a pure round trip, and ``BarBuilder.on_tick`` only *stages* — the flush
lives on the async ``on_tick_event`` seam the harness does not use, so a staged buffer would simply
grow for the whole day. There is no flag for it (the v1 flag deferred every byte of that work to
teardown, which is a memory leak wearing a feature's clothes).

**Bar capture — which seam, and why.** The work order allowed either a bus subscriber or the
builder's finalize path. A bus subscriber is out: ``BarBuilder`` publishes bars with
``EventBus.publish``, which is *fire-and-forget* — inside a running loop it schedules handler tasks
that do not run until the next ``await``, so a collector on the bus would batch a chunk's bars at the
harness's cooperative yield and destroy the tick↔bar interleaving the broker's fill model depends on.
The builder is therefore constructed with ``bus=None``.

Which leaves the finalize path — and specifically the **store write**, ``insert_bars_1m``, not
``advance()``'s return value. That distinction is load-bearing: ``BarBuilder.on_tick`` calls
``_advance()`` internally on EVERY path, so most bars are finalized inside ``on_tick`` and never come
back through the harness's own ``advance()`` call at all. Capturing ``advance()``'s return value
alone loses them (measured: 6 of 1,122 bars survived). ``_write_and_publish`` persists the batch
BEFORE notifying anyone (§3.2.3), so the store seam sees every finalized bar exactly once, in
finalization order, at the instant of finalization — which is precisely the capture point the
interleaving needs. :class:`_BufferedBarStore` is therefore both the write buffer and the capture
hook, and the broker gets each bar from there, still ahead of the tick that triggered it.

``advance()`` itself is called once per MINUTE (when the tick being fed belongs to a different minute
than the last one), not once per tick — the per-tick call was ~26 k redundant scans of the open-bar
dict on a single-symbol day, on top of the one ``on_tick`` already does. It is ADDITIVE either way (a
tick can never finalize its OWN minute: that minute closes 65 s after it), so it changes no bar's
content; what it buys is finalization for a symbol that has stopped printing while others keep
ticking, and what the conditional changes is only how often that sweep runs. ``flush_all()`` at day
end catches whatever the last boundary left open.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import queue
import re
import threading
import time as _time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Protocol

import duckdb
from pydantic import BaseModel, ConfigDict

# The order-postback wire contract now lives in ``engine.core.contracts`` (WO-P3-2 fix round,
# 2026-09-10): the move this import block anticipated has landed, so ``engine.paper`` imports it from
# ``core`` like everything else and the package no longer reaches into ``engine.broker`` at all
# (§3.2.9, enforced by tests/unit/test_import_graph.py).
from engine.core.clock import IST, Clock
from engine.core.contracts import ORDER_UPDATE_TOPIC, OrderUpdateFrame
from engine.core.log import get_logger
from engine.core.types import Bar, Tick
from engine.marketdata.bar_builder import (
    FINALIZE_GRACE_S,
    SESSION_CLOSE_IST,
    SESSION_OPEN_IST,
    BarBuilder,
)
from engine.marketdata.store import MarketStore

_log = get_logger("engine.paper.replay")

#: Columns read from every tick Parquet fragment, in the order the store WRITES them. Mirrors
#: ``store._TICK_COLUMNS`` (§4.3); ``tests/replay/test_golden_day.py`` asserts the two are equal, so a
#: column added to the dataset cannot silently narrow what a replay sees.
TICK_COLUMNS: tuple[str, ...] = (
    "instrument_token", "tradingsymbol", "ltp", "volume_traded", "exchange_ts",
    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "avg_price", "bid", "ask",
)

#: The day stream's sort key: a TOTAL order over the tick row. ``exchange_ts, tradingsymbol`` alone
#: is not one — the archive routinely holds several prints for one symbol in the same second, and two
#: rows tying on those two keys are free to swap between runs (DuckDB's sort is not stable across a
#: differently-parallelised scan). Two such rows differing only in ``bid``/``ask`` would flip the
#: broker's half-spread, and therefore its fill price, between runs of identical input. Every
#: remaining column is therefore part of the key, so the only rows left interchangeable are ones
#: identical in all twelve columns — interchangeable by definition. ``test_golden_day`` asserts this
#: is a permutation of :data:`TICK_COLUMNS`, so a new column cannot quietly de-totalise the order.
ORDER_BY_COLUMNS: tuple[str, ...] = (
    "exchange_ts", "tradingsymbol", "volume_traded", "ltp", "bid", "ask", "avg_price",
    "instrument_token", "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close",
)

#: Memory ceiling for the harness's private DuckDB instance (§3.2.11 — "every DuckDB instance in this
#: platform carries a stated ceiling", generalizing the 53 GB incident of 2026-08-17). Half the live
#: store's 8 GB on purpose: a replay is a maintenance-class job and must never be able to compete for
#: RAM with the session that may be trading beside it. Past the limit DuckDB spills to its temp dir.
_MEMORY_LIMIT = "4GB"

#: Rows per ``fetchmany`` from the reader thread. ~20 k rows of the 12-column tuple shape is a few MB
#: — big enough that the per-call overhead disappears, small enough that peak resident memory is a
#: constant rather than a function of the day's size.
_FETCH_ROWS = 20_000

#: Chunks the reader thread may run ahead of the tick loop. Bounded on purpose: an unbounded hand-off
#: is just whole-day materialisation with extra steps. Peak = (this + 1) chunks in flight.
_QUEUE_CHUNKS = 2

#: Poll interval for the reader's blocking ``put``. It exists only so a producer parked on a full
#: queue can notice a consumer-side abort (an exception in the tick loop) and exit instead of
#: wedging the executor thread for the life of the process.
_PUT_POLL_S = 0.25

#: Ticks fed between cooperative ``await asyncio.sleep(0)`` yields. This is the upper bound on how
#: long the harness blocks the event loop in one go; ``tests/replay`` pins it with a heartbeat
#: coroutine that must never observe a gap over 1 s.
_YIELD_EVERY_TICKS = 2_000

#: Finalized bars buffered before a scratch-store round trip (see :class:`_BufferedBarStore`).
_BAR_WRITE_BATCH = 500

#: The instant the :class:`ReplayClock` reads before the first tick advances it. A pinned epoch, not
#: ``datetime.now()``: constructing the scratch store reads the clock (``_last_flush_at``), and a
#: wall-clock read there would be the first crack in "nothing in a replay reads wall time".
_EPOCH = datetime(1970, 1, 1, tzinfo=IST)

#: ULID as text: 26 Crockford base32 characters (alphabet 0-9 A-Z minus I, L, O, U), the first of
#: which encodes the high bits of a 48-bit millisecond timestamp and is therefore 0-7 for every date
#: this platform will ever see. The leading-char restriction is what keeps the mask from eating an
#: unrelated 26-character uppercase token. Bounded by non-alphanumerics so it cannot match INSIDE a
#: longer identifier. Accepted residual: masking is lossy by construction — two logs differing only
#: in a masked token hash the same, which is the entire point (§8.4 "accepted non-determinism").
_ULID_RE = re.compile(r"(?<![0-9A-Za-z])[0-7][0-9A-HJKMNP-TV-Z]{25}(?![0-9A-Za-z])")

#: Version tag hashed into every digest. Bump it when the canonical serialisation changes, so a
#: golden-day mismatch reads as "the format moved" rather than "behaviour drifted" (§9.6).
DIGEST_VERSION = "replay-digest-v1"


class ReplayContractError(RuntimeError):
    """The replay produced a report that would MISREPRESENT what it covered (§8.4 addendum).

    Raised when an attached broker ends the run holding orders while the harness collected zero
    ``order.update`` frames: the broker was constructed with someone else's ``publish``, so the
    digest covers bars only. Loud, because the failure mode this replaces was a golden digest that
    silently stopped asserting anything about orders.
    """


def mask_ulids(text: str) -> str:
    """Replace every ULID-shaped token in ``text`` with ``<ULID>`` (§8.4 accepted non-determinism)."""
    return _ULID_RE.sub("<ULID>", text)


class ReplayBroker(Protocol):
    """The duck-typed broker seam the harness drives.

    Deliberately a Protocol and NOT an import of ``engine.paper.broker``: the PaperBroker (WO-P3-2)
    is built concurrently, and the harness must be usable — and testable — with a stub. Two methods,
    and the contract around them:

    * ``on_tick`` receives **in-session prints only**. Pre-open (A14) and post-close (WO-5) ticks
      reach the ``BarBuilder`` — the pre-open ones carry the auction open — but never the broker: a
      print that cannot legally trade in the continuous session must not be able to fill an order.
      The report's three tick counters partition the stream so this is auditable, not assumed.
    * ``on_bar`` receives finalized bars in finalization order, interleaved with the ticks (see the
      module docstring on ``advance()`` cadence).

    Postbacks are NOT pulled from the broker. A broker publishes them through the ``publish``
    callable it was constructed with, which is :meth:`ReplayHarness.publish`.

    **Known blind spot (§9.3).** A time-sorted replay can never exercise ``BarBuilder``'s late-tick /
    amendment path (A13): a minute is finalized only once the clock passes ``minute + 1 min +
    grace``, and every later tick in a sorted stream carries a strictly later timestamp, so no tick
    can ever arrive for an already-finalized minute. That path needs its own §9.3 fixture with a
    deliberately out-of-order stream; it is not reachable from here and this harness must not be read
    as covering it.
    """

    def on_tick(self, tick: Tick) -> None: ...

    def on_bar(self, bar: Bar) -> None: ...


class ReplayClock(Clock):
    """A :class:`~engine.core.clock.Clock` whose "now" is the replayed tick stream (§9.1).

    The ``time_source`` closes over :attr:`_at`, which :meth:`advance_to` moves forward before each
    tick is delivered. Monotonic by contract: the harness sorts the requested days and orders each
    day's stream by :data:`ORDER_BY_COLUMNS`, so a backwards step would mean the stream itself is
    unsorted — which would silently rewind ``BarBuilder``'s finalization and is therefore raised,
    not clamped.
    """

    def __init__(self, start: datetime = _EPOCH) -> None:
        self._at = start.astimezone(IST)
        super().__init__(time_source=lambda: self._at)

    def advance_to(self, ts: datetime) -> None:
        at = ts.astimezone(IST)
        if at < self._at:
            raise ValueError(
                f"replay clock cannot go backwards: {self._at.isoformat()} -> {at.isoformat()}"
            )
        self._at = at


class ReplayReport(BaseModel):
    """Outcome of one :meth:`ReplayHarness.run` (§3.2.9 ``ReplayReport``).

    ``ticks_read`` is every row read from the archive; ``ticks_delivered`` is the subset handed to the
    attached broker, i.e. the IN-SESSION prints. The three tick counters partition ``ticks_read``
    exactly (``pre_open_excluded + post_close_dropped + ticks_delivered == ticks_read``) — the
    BarBuilder still sees every tick, because the pre-open prints carry the A14 auction open.

    ``postbacks`` / ``orders`` / ``fills`` are derived from the ``order.update`` frames the harness
    collected (see :meth:`ReplayHarness.publish`), NOT read off the broker: ``postbacks`` counts
    frames, ``orders`` counts distinct broker ``order_id``s in them, and ``fills`` counts the frames
    that reported a HIGHER ``filled_quantity`` than that order had reported before — so a market
    order that fills in one go is one fill, and a partial-fill ladder is one per rung.

    ``actions_fired`` counts the scripted actions (:meth:`ReplayHarness.schedule` /
    :meth:`ReplayHarness.at_tick`) the stream reached. It is reported rather than merely assumed
    because a scripted action that silently never ran is exactly the failure mode the seam replaces
    (fix round, 2026-09-10); an action the stream never reaches is a hard error at the end of
    :meth:`ReplayHarness.run`, so this is a positive count, never a shortfall.

    ``elapsed_s`` is wall time and is deliberately NOT part of ``digest``.
    """

    model_config = ConfigDict(frozen=True)

    days: list[date]
    ticks_read: int
    ticks_delivered: int
    bars_built: dict[date, int]
    pre_open_excluded: int
    post_close_dropped: int
    postbacks: int
    orders: int
    fills: int
    actions_fired: int
    elapsed_s: float
    digest: str
    symbols: list[str] | None = None
    config_overlay: dict[str, Any] | None = None   # carried for provenance; consumed in WO-P3-5


def _canonical_bar(bar: Bar) -> str:
    """The §9.6 canonical serialisation of one built bar.

    Field list pinned by the work order: symbol, minute, OHLC, volume, src. ``auction_open`` is
    deliberately outside the digest — it is asserted directly by the golden test (A14) rather than
    folded into a hash where a drift would be unreadable. ``str(Decimal)`` preserves the stored scale
    exactly (``Decimal('101.00')`` → ``"101.00"``), so no float ever reaches the hash (§3.2 money).
    """
    return "|".join((
        bar.symbol, bar.ts_minute.isoformat(), str(bar.open), str(bar.high),
        str(bar.low), str(bar.close), str(bar.volume), bar.src,
    ))


def _canonical_postback(entry: Any) -> str:
    """Canonical text for one ``order.update`` frame: key-sorted JSON where possible, ``str`` else.

    Key-sorted because a dict's insertion order is an implementation detail of the broker, not part of
    what the golden day is asserting; ``default=str`` so Decimals and datetimes serialise exactly
    rather than through a float.
    """
    if isinstance(entry, BaseModel):
        entry = entry.model_dump(mode="json")
    if isinstance(entry, dict):
        return json.dumps(entry, sort_keys=True, default=str, separators=(",", ":"))
    return str(entry)


def _frame_data(frame: Any) -> dict[str, Any]:
    """The Kite-shaped payload inside an ``order.update`` frame, or ``{}`` for anything else."""
    data = getattr(frame, "data", None)
    return data if isinstance(data, dict) else {}


def _order_counts(frames: Sequence[Any]) -> tuple[int, int]:
    """``(distinct orders, fill events)`` over a frame stream — see :class:`ReplayReport`."""
    high_water: dict[str, int] = {}
    fills = 0
    for frame in frames:
        data = _frame_data(frame)
        order_id = str(data.get("order_id", ""))
        try:
            filled = int(data.get("filled_quantity") or 0)
        except (TypeError, ValueError):        # a broker that puts something odd on the wire
            filled = 0
        if filled > high_water.get(order_id, 0):
            fills += 1
        high_water[order_id] = max(high_water.get(order_id, 0), filled)
    return len(high_water), fills


def _digest(bars: Sequence[Bar], postbacks: Sequence[Any]) -> str:
    """sha256 over the canonical bar stream + ``order.update`` frame stream, ULIDs masked (§9.6)."""
    lines = [DIGEST_VERSION, f"bars:{len(bars)}"]
    lines.extend(_canonical_bar(b) for b in bars)
    lines.append(f"postbacks:{len(postbacks)}")
    lines.extend(_canonical_postback(p) for p in postbacks)
    payload = mask_ulids("\n".join(lines))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class _BufferedBarStore:
    """The harness's seam onto ``BarBuilder``'s bar output: capture hook + write buffer.

    **Capture.** ``_write_and_publish`` calls ``insert_bars_1m`` with every finalized batch, before
    notifying anything (§3.2.3) — so this is where the harness sees each bar, exactly once, in
    finalization order. ``on_bars`` is invoked synchronously with the batch, ahead of the buffering,
    which is what keeps the tick↔bar interleaving the broker's fill model reads. See the module
    docstring for why ``advance()``'s return value is NOT that seam.

    **Buffering.** The builder persists on every finalization batch — correct for a live session,
    where a batch arrives once a minute and durability matters. Under replay it is one DuckDB upsert
    round trip per replayed minute: measured on this box at ~14 ms each, so a 375-minute day spends
    ~5 s on nothing but transaction overhead, and it scales with MINUTES not symbols — a 300-symbol
    day pays the same 375 round trips for 100× the rows. So rows are batched to
    :data:`_BAR_WRITE_BATCH` and flushed at each day's end. The ``BarBuilder`` is UNCHANGED and
    unaware; this is a harness-local adapter, not a store or builder feature. Durability mid-replay
    buys nothing (the scratch store is thrown away), while in a live session it is the whole point.

    Every other store call passes through untouched, including ``amend_bar_1m_extremes``. That is
    safe rather than lucky: the late-tick path that would read a not-yet-written bar is unreachable
    under a sorted replay (see :class:`ReplayBroker`). Were it reachable, this adapter would have to
    flush before delegating.
    """

    def __init__(
        self,
        store: MarketStore,
        on_bars: Callable[[Sequence[Bar]], None],
        batch: int = _BAR_WRITE_BATCH,
    ) -> None:
        self._store = store
        self._on_bars = on_bars
        self._batch = batch
        self._pending: list[Bar] = []
        self.store_writes = 0                   # round trips actually paid (observability only)

    def __getattr__(self, name: str) -> Any:
        # __dict__ lookup, not self._store: an attribute touched before __init__ finished would
        # otherwise recurse through __getattr__ forever instead of failing.
        return getattr(self.__dict__["_store"], name)

    def insert_bars_1m(self, bars: Sequence[Bar]) -> int:
        self._on_bars(bars)
        self._pending.extend(bars)
        if len(self._pending) >= self._batch:
            self.flush()
        return len(bars)

    def flush(self) -> None:
        """Write everything buffered. Called at each day's end and whenever the batch fills."""
        if not self._pending:
            return
        self._store.insert_bars_1m(self._pending)
        self.store_writes += 1
        self._pending.clear()


class ReplayHarness:
    """Replay recorded tick days through the real bar path into a scratch store (§3.2.9).

    Parameters
    ----------
    parquet_root:
        Root of the §4.3 Parquet datasets — the harness reads ``<root>/ticks/date=…/symbol=…``. This
        is the LIVE archive in normal use and is opened **read-only, through the harness's own
        in-memory DuckDB**; nothing here writes to it.
    scratch_dir:
        Where this run's throwaway ``market.duckdb`` + ``parquet/`` live. Owned by the caller (a
        ``tmp_path``, or the CLI's temp dir) and validated on construction to be outside
        ``parquet_root`` — a replay that could write into the archive it is reading would be able to
        corrupt live data while the engine trades.
    session_open / session_close / grace_s:
        Passed straight through to :class:`BarBuilder`; override for a muhurat/shortened day exactly
        as the live composition root does.

    Wiring a broker takes two steps, and both are required: construct it with :meth:`publish` as its
    event-bus publish callable, then :meth:`attach_broker` it so the tick/bar stream reaches it.
    Attaching without publishing is caught at the end of :meth:`run` (:class:`ReplayContractError`).

    Making something HAPPEN mid-replay — placing an order, flipping a config — goes through
    :meth:`schedule` / :meth:`at_tick`, never through a task the caller creates on the loop
    (module docstring property 7).
    """

    #: Digest of a run that built no bars and collected no frames (the empty-archive case).
    EMPTY_DIGEST = _digest((), ())

    def __init__(
        self,
        parquet_root: str | Path,
        scratch_dir: str | Path,
        *,
        session_open: time = SESSION_OPEN_IST,
        session_close: time = SESSION_CLOSE_IST,
        grace_s: int = FINALIZE_GRACE_S,
    ) -> None:
        self._parquet_root = Path(parquet_root)
        self._scratch_dir = Path(scratch_dir)
        self._guard_scratch()

        self._session_open = session_open
        self._session_close = session_close
        self._grace_s = grace_s

        self.clock = ReplayClock()
        self.store = MarketStore(
            self._scratch_dir / "market.duckdb", self._scratch_dir / "parquet", self.clock
        )
        self._opened = False
        self._broker: ReplayBroker | None = None
        self._bars: dict[date, list[Bar]] = {}
        self._frames: list[OrderUpdateFrame] = []
        self._reader: ThreadPoolExecutor | None = None

        # Scripted actions, kept in fire order. ``_seq`` breaks ties by registration order so two
        # actions scheduled at the same instant fire in the order they were written down, run after
        # run. ``_stream_index`` is the 0-based ordinal of the tick about to be fed, counted across
        # the WHOLE run (every day, every tick read — pre-open and post-close included).
        self._timed: list[tuple[datetime, int, Callable[[], Any]]] = []
        self._indexed: list[tuple[int, int, Callable[[], Any]]] = []
        self._seq = 0
        self._stream_index = 0
        self._actions_fired = 0

    def _guard_scratch(self) -> None:
        """Refuse a scratch dir that is inside the archive (or vice versa) — see ``scratch_dir``."""
        archive = self._parquet_root.resolve()
        scratch = self._scratch_dir.resolve()
        if scratch == archive or archive in scratch.parents or scratch in archive.parents:
            raise ValueError(
                f"replay scratch dir {scratch} overlaps the parquet archive {archive}; "
                "a replay must never be able to write into the data it reads"
            )

    # ------------------------------------------------------------------ lifecycle
    def attach_broker(self, broker: ReplayBroker) -> None:
        """Attach the duck-typed broker driven by :meth:`run` (``on_tick`` / ``on_bar``)."""
        self._broker = broker

    def publish(self, topic: str, event: BaseModel) -> None:
        """The event-bus ``publish`` seam a replayed broker is constructed with (§3.2.9).

        ``PaperBroker(publish=harness.publish)``. Every :data:`ORDER_UPDATE_TOPIC` frame is recorded
        here — this collector is what the digest hashes and what ``ReplayReport.postbacks`` counts.
        Frames on other topics are accepted and ignored: the harness asserts the ORDER path, and a
        broker publishing something else must not crash a replay over it.

        Synchronous and allocation-cheap by design: it is called from inside the broker's ``on_tick``,
        i.e. on the hot path, once per order state change.
        """
        if topic == ORDER_UPDATE_TOPIC:
            self._frames.append(event)  # type: ignore[arg-type]

    # ------------------------------------------------------------------ scripted actions (§9.6)
    def schedule(self, at: datetime, fn: Callable[[], Any]) -> None:
        """Run ``fn`` immediately before the first tick whose ``exchange_ts >= at`` is fed.

        This is the ONLY sanctioned way to make something happen during a replay (fix round,
        2026-09-10, module docstring property 7). ``fn`` is called synchronously on the event-loop
        thread from inside the tick loop, with :attr:`clock` ALREADY advanced to that tick's
        ``exchange_ts`` — so an order placed here is stamped with the instant the print arrived, not
        with whatever the loop got round to. If ``fn`` returns an awaitable (``PaperBroker`` methods
        are ``async``, and their bodies never suspend) it is awaited inline, before the tick is fed:
        the action is complete before anything downstream sees the tick, whatever the awaitable does.

        The predicate is over the STREAM, which includes the pre-open and post-close prints the
        broker never sees — ``at`` earlier than 09:15 therefore fires against an auction print.

        Each registration fires at most once. An action the replayed stream never reaches is a
        :class:`ReplayContractError` at the end of :meth:`run`: a scripted action that silently did
        nothing is the same class of bug as a bars-only digest reported as a golden day.
        """
        self._timed.append((at.astimezone(IST), self._seq, fn))
        self._seq += 1
        self._timed.sort(key=lambda entry: (entry[0], entry[1]))

    def at_tick(self, index: int, fn: Callable[[], Any]) -> None:
        """Run ``fn`` immediately before the tick at 0-based stream ordinal ``index`` is fed.

        The ordinal counts every tick READ in this run, across all replayed days, in delivery
        order — the same stream :meth:`schedule` predicates on. Same synchronous, clock-advanced,
        awaitable-aware contract; same "never reached is an error" rule.
        """
        if index < 0:
            raise ValueError(f"at_tick index must be >= 0, got {index}")
        self._indexed.append((index, self._seq, fn))
        self._seq += 1
        self._indexed.sort(key=lambda entry: (entry[0], entry[1]))

    async def _fire_due_actions(self, ts: datetime, index: int) -> None:
        """Fire every scripted action due at stream ordinal ``index`` / instant ``ts``.

        Ordinal actions go first: an ``at_tick`` names one exact tick, while a ``schedule`` names
        "at or after", so running the exact match first is the only ordering that does not depend on
        which of the two forms happened to be registered first.
        """
        while self._indexed and self._indexed[0][0] <= index:
            _, _, fn = self._indexed.pop(0)
            await self._invoke(fn)
        while self._timed and self._timed[0][0] <= ts:
            _, _, fn = self._timed.pop(0)
            await self._invoke(fn)

    async def _invoke(self, fn: Callable[[], Any]) -> None:
        result = fn()
        if inspect.isawaitable(result):
            await result
        self._actions_fired += 1

    def _assert_actions_fired(self) -> None:
        pending = len(self._timed) + len(self._indexed)
        if pending:
            raise ReplayContractError(
                f"{pending} scripted replay action(s) were never reached by the stream "
                f"(fired {self._actions_fired}) — an action the replay never runs asserts nothing; "
                "schedule it at an instant/ordinal the replayed days actually contain"
            )

    def open(self) -> ReplayHarness:
        if not self._opened:
            self.store.open()
            self._opened = True
        return self

    def close(self) -> None:
        if self._reader is not None:
            self._reader.shutdown(wait=True)
            self._reader = None
        if self._opened:
            self.store.close()
            self._opened = False

    def __enter__(self) -> ReplayHarness:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def bars(self, day: date) -> list[Bar]:
        """Bars built for ``day`` by the most recent :meth:`run`, in finalization order."""
        return list(self._bars.get(day, ()))

    def postbacks(self) -> list[OrderUpdateFrame]:
        """The ``order.update`` frames collected by :meth:`publish` during the most recent run."""
        return list(self._frames)

    # ------------------------------------------------------------------ the run (§3.2.9)
    async def run(
        self,
        days: list[date],
        *,
        symbols: Sequence[str] | None = None,
        config_overlay: dict[str, Any] | None = None,
    ) -> ReplayReport:
        """Replay ``days`` (ascending, deduplicated) and return the :class:`ReplayReport`.

        Days are sorted and deduplicated so the :class:`ReplayClock` stays monotonic whatever order
        the caller passes — ``run([d2, d1])`` and ``run([d1, d2])`` are the same replay and produce
        the same digest.
        """
        self.open()
        started = _time.perf_counter()
        ordered = sorted(set(days))
        wanted = tuple(dict.fromkeys(symbols)) if symbols is not None else None

        self._bars = {}
        self._frames = []
        self._stream_index = 0
        self._actions_fired = 0
        # Snapshot the broker's book BEFORE the first tick (fix round, 2026-09-10). The
        # postbacks-collected check is about orders THIS run produced; a second run() against the
        # same broker inherits the first run's orderbook, and comparing the cumulative book against
        # this run's (empty) frame list would fail a correctly-wired harness.
        book_at_start = await self._broker_book_size()

        ticks_read = pre_open = post_close = delivered = 0
        bars_built: dict[date, int] = {}

        for day in ordered:
            day_read, day_pre, day_post, day_delivered = await self._replay_day(day, wanted)
            ticks_read += day_read
            pre_open += day_pre
            post_close += day_post
            delivered += day_delivered
            bars_built[day] = len(self._bars.get(day, ()))

        self._assert_actions_fired()
        await self._assert_postbacks_collected(book_at_start)

        all_bars = [bar for day in ordered for bar in self._bars.get(day, ())]
        order_count, fill_count = _order_counts(self._frames)
        report = ReplayReport(
            days=ordered,
            ticks_read=ticks_read,
            ticks_delivered=delivered,
            bars_built=bars_built,
            pre_open_excluded=pre_open,
            post_close_dropped=post_close,
            postbacks=len(self._frames),
            orders=order_count,
            fills=fill_count,
            actions_fired=self._actions_fired,
            elapsed_s=_time.perf_counter() - started,
            digest=_digest(all_bars, self._frames),
            symbols=list(wanted) if wanted is not None else None,
            config_overlay=config_overlay,
        )
        _log.info(
            "replay_done",
            days=[d.isoformat() for d in ordered],
            ticks_read=ticks_read,
            bars=sum(bars_built.values()),
            postbacks=report.postbacks,
            fills=report.fills,
            elapsed_s=round(report.elapsed_s, 3),
            digest=report.digest,
        )
        return report

    async def _broker_book_size(self) -> int | None:
        """Size of the attached broker's orderbook, or ``None`` when there is nothing to ask.

        ``None`` and ``0`` are deliberately different answers: ``None`` means the seam is absent (no
        broker, or a stub with no ``orders``) and the postbacks check has nothing to say, while
        ``0`` is a real, empty book.
        """
        if self._broker is None:
            return None
        orders = getattr(self._broker, "orders", None)
        if orders is None:
            return None
        book = orders()
        if inspect.isawaitable(book):        # the BrokerSurface is async; a stub may be sync
            book = await book
        try:
            return len(book)
        except TypeError:                    # a broker whose orders() returns something unsized
            return None

    async def _assert_postbacks_collected(self, book_at_start: int | None) -> None:
        """Refuse a bars-only digest from a run that had a broker doing order work (§8.4 addendum).

        The check is "the broker's orderbook GREW during this run but nothing was published", which
        is exactly the wiring bug: a broker constructed with someone else's ``publish``. A broker
        that simply never received an order (the common case — v1 has no order origination) is
        silent here, as it should be.

        Growth, not emptiness (fix round, 2026-09-10). The predecessor compared the broker's
        CUMULATIVE book against THIS run's frames, so a second ``run()`` on a broker that had
        ordered in the first one raised even though the harness was wired correctly and this run
        simply had no order work — a false positive that would have made the multi-run golden
        assertion below impossible to write.
        """
        if self._broker is None or self._frames:
            return
        size = await self._broker_book_size()
        if size is None:
            return
        baseline = book_at_start or 0
        if size > baseline:
            raise ReplayContractError(
                f"broker's orderbook grew from {baseline} to {size} order(s) during the replay but "
                f"the harness collected zero {ORDER_UPDATE_TOPIC!r} frames — construct it with "
                "publish=harness.publish; a bars-only digest would silently stop asserting anything "
                "about orders (§8.4)"
            )

    # ------------------------------------------------------------------ data path (streamed, §8.4)
    def _day_globs(self, day: date, symbols: tuple[str, ...] | None) -> list[str]:
        """Parquet globs for ``day``, narrowed to ``symbols`` when given.

        Narrowing at the PATH level rather than with a SQL ``WHERE`` matters at archive scale: the
        live dataset holds hundreds of thousands of per-flush fragments per day, and a symbol filter
        that still opened all of them would pay the whole day's file-listing cost to throw most of it
        away. Non-existent / empty partitions are dropped here because ``read_parquet`` raises on a
        glob that matches nothing.
        """
        day_dir = self._parquet_root / "ticks" / f"date={day.isoformat()}"
        if not day_dir.is_dir():
            return []
        if symbols is None:
            return [(day_dir / "*" / "*.parquet").as_posix()] if any(day_dir.glob("*/*.parquet")) else []
        globs: list[str] = []
        for symbol in symbols:
            part = day_dir / f"symbol={symbol}"
            if part.is_dir() and any(part.glob("*.parquet")):
                globs.append((part / "*.parquet").as_posix())
        return globs

    def _pool(self) -> ThreadPoolExecutor:
        """The harness's PRIVATE single-worker reader pool.

        Single worker on purpose: a DuckDB connection is not safe to drive from several threads at
        once, and with one worker every statement of a day's read happens on the same thread, so the
        connection is never shared. Created lazily and released by :meth:`close`.
        """
        if self._reader is None:
            self._reader = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mt-replay-read")
        return self._reader

    @staticmethod
    def _offer(chunks: queue.Queue[Any], item: Any, stop: threading.Event) -> bool:
        """Hand ``item`` to the consumer, blocking while the bounded queue is full.

        Returns ``False`` once the consumer has set ``stop`` — the reader thread then exits instead
        of parking on a queue nobody will drain again (an exception in the tick loop).
        """
        while not stop.is_set():
            try:
                chunks.put(item, timeout=_PUT_POLL_S)
                return True
            except queue.Full:
                continue
        return False

    def _make_reader(
        self, globs: list[str], chunks: queue.Queue[Any], stop: threading.Event
    ) -> Any:
        """Build the reader-thread body: ONE ordered query, consumed ``_FETCH_ROWS`` at a time.

        The query is still a single ``read_parquet`` scan — DuckDB does the sort and the file
        handling — but the ROWS cross into Python in bounded chunks, so nothing here is ever
        proportional to the size of the day. An empty list is the end-of-stream sentinel; an
        exception is forwarded as a value and re-raised on the loop thread, where it has a traceback
        someone can read.
        """

        def _read() -> None:
            try:
                con = duckdb.connect(":memory:")
            except Exception as exc:                        # noqa: BLE001 - re-raised on the loop
                self._offer(chunks, exc, stop)
                return
            try:
                con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
                con.execute("SET TimeZone='Asia/Kolkata'")
                con.execute("SET enable_progress_bar=false")
                con.execute(
                    f"SELECT {', '.join(TICK_COLUMNS)} FROM read_parquet(?, hive_partitioning=false) "
                    f"ORDER BY {', '.join(ORDER_BY_COLUMNS)}",
                    [globs],
                )
                while True:
                    rows = con.fetchmany(_FETCH_ROWS)
                    if not self._offer(chunks, rows, stop) or not rows:
                        return
            except Exception as exc:                        # noqa: BLE001 - re-raised on the loop
                self._offer(chunks, exc, stop)
            finally:
                con.close()

        return _read

    @staticmethod
    def _to_ticks(rows: Sequence[tuple]) -> list[Tick]:
        """One fetched chunk → ``Tick`` models.

        Built with ``Tick.model_construct`` (validation skipped). Measured on this box over a real
        26,164-row symbol-day: ``model_construct`` 0.46 s vs full validation 0.43 s — i.e. the two
        are within noise and speed is NOT the reason. The reason is that a replay must be able to
        replay whatever the archive actually holds: a validation error on one recorded row would
        abort the replay of exactly the day someone is investigating. The two invariants validation
        would have enforced are re-established here directly — ``exchange_ts`` is normalised to
        zoneinfo IST (DuckDB hands back a pytz ``Asia/Kolkata``; 0.035 s / 26 k rows), and every price
        column is a native ``Decimal`` because the columns are ``DECIMAL`` in Parquet and the fetch
        binds them as such. That last point is why this does NOT go through a pandas frame as first
        sketched: ``duckdb`` `.df()` converts ``DECIMAL`` to ``float64`` (verified 2026-09-10 —
        ``CAST(101.05 AS DECIMAL(12,2))`` came back ``np.float64``), which would put a float on every
        price and break §3.2's money convention on the way in.
        """
        build = Tick.model_construct
        return [
            build(
                instrument_token=r[0], tradingsymbol=r[1], ltp=r[2], volume_traded=r[3],
                exchange_ts=r[4].astimezone(IST), ohlc_open=r[5], ohlc_high=r[6], ohlc_low=r[7],
                ohlc_close=r[8], avg_price=r[9], bid=r[10], ask=r[11],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ delivery
    async def _replay_day(
        self, day: date, symbols: tuple[str, ...] | None
    ) -> tuple[int, int, int, int]:
        """Stream one day through the builder; return ``(read, pre_open, post_close, delivered)``.

        A FRESH ``BarBuilder`` per day, force-flushed at the end: the builder's day-rollover path
        finalizes stale bars internally, and a builder carried across days would run that path
        mid-stream instead of under the harness's own flush.
        """
        day_bars: list[Bar] = self._bars.setdefault(day, [])
        globs = self._day_globs(day, symbols)
        if not globs:
            _log.info("replay_day_empty", day=day.isoformat())
            return 0, 0, 0, 0

        def _captured(bars: Sequence[Bar]) -> None:
            for bar in bars:
                self._emit(day_bars, bar)

        buffered = _BufferedBarStore(self.store, _captured)
        builder = BarBuilder(
            buffered,                       # type: ignore[arg-type]  # harness-local write buffer
            self.clock,
            bus=None,                       # see the module docstring: publish() is fire-and-forget
            session_open=self._session_open,
            session_close=self._session_close,
            grace_s=self._grace_s,
            persist_raw_ticks=False,        # the ticks came FROM the archive; see the module docstring
        )

        loop = asyncio.get_running_loop()
        chunks: queue.Queue[Any] = queue.Queue(maxsize=_QUEUE_CHUNKS)
        stop = threading.Event()
        reader = loop.run_in_executor(self._pool(), self._make_reader(globs, chunks, stop))

        read = pre_open = post_close = delivered = 0
        last_minute: datetime | None = None
        try:
            while True:
                item = await asyncio.to_thread(chunks.get)
                if isinstance(item, BaseException):
                    raise item
                if not item:
                    break
                read += len(item)
                # ONE yield-slice converted at a time, not the whole fetch chunk: _to_ticks over
                # _FETCH_ROWS rows is ~0.4 s of uninterruptible loop-thread work, so converting up
                # front made _YIELD_EVERY_TICKS a fiction — the real bound on how long the loop was
                # held was _FETCH_ROWS (fix round, 2026-09-10, module docstring property 3).
                for offset in range(0, len(item), _YIELD_EVERY_TICKS):
                    for tick in self._to_ticks(item[offset:offset + _YIELD_EVERY_TICKS]):
                        # The clock advances BEFORE anything sees the tick — bar finalization, and
                        # every consumer downstream of it, must observe the instant this print
                        # arrived, never wall time.
                        ts = tick.exchange_ts
                        self.clock.advance_to(ts)
                        # …and scripted actions run here, with the clock already on this tick and
                        # before any consumer has seen it: replay behaviour is a function of the
                        # stream, never of when the OS scheduled a task (property 7).
                        if self._timed or self._indexed:
                            await self._fire_due_actions(ts, self._stream_index)
                        self._stream_index += 1
                        minute = ts.replace(second=0, microsecond=0)
                        if minute != last_minute:
                            # Once per minute, not per tick: the coarse sweep that finalizes a
                            # symbol which has stopped printing. Its RETURN VALUE is discarded —
                            # capture is the store seam (see the module docstring), and reading both
                            # would deliver those bars twice.
                            last_minute = minute
                            builder.advance()
                        builder.on_tick(tick)

                        tod = ts.time()
                        if tod < self._session_open:
                            pre_open += 1       # A14: no bar, but it carries the auction open
                        elif tod > self._session_close:
                            post_close += 1     # WO-5: no bar; counted, never traded
                        else:
                            delivered += 1
                            if self._broker is not None:
                                self._broker.on_tick(tick)
                    # Cooperative yield: bus handlers, timers and anything else scheduled on this
                    # loop run here. One chunk is the longest the harness ever blocks the loop.
                    await asyncio.sleep(0)
        finally:
            # An exception in the tick loop leaves the reader parked on a full queue; ``stop`` plus
            # the bounded ``put`` timeout is what lets it notice and exit rather than stranding the
            # executor's only worker for the life of the process. Joined here so the run never
            # outlives its own reader thread.
            stop.set()
            await reader

        builder.flush_all()             # captured through the store seam, like every other bar
        buffered.flush()
        return read, pre_open, post_close, delivered

    def _emit(self, day_bars: list[Bar], bar: Bar) -> None:
        day_bars.append(bar)
        if self._broker is not None:
            self._broker.on_bar(bar)
