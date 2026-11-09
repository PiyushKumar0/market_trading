"""Live ticks → finalized 1-minute bars.

``BarBuilder`` consumes the parsed :class:`~engine.core.types.Tick` stream (the ``tick`` bus topic
published by ``TickerSupervisor``) and produces canonical :class:`~engine.core.types.Bar` rows
(``src='self'``) — batch-written through :class:`~engine.marketdata.store.MarketStore` (the single
DuckDB writer) and published on ``bar.1m``.

Pinned behaviors:

* **Pre-open exclusion:** ticks with ``exchange_ts`` before 09:15:00 IST are dropped from
  bars. The **auction-derived open** — the last pre-open ``ltp`` seen for the symbol (the pre-open
  ticks carry the equilibrium/auction price discovery) — is captured separately and stamped as
  ``auction_open`` on that day's 09:15 bar row ONLY. Pre-open ticks ARE still persisted raw to the
  tick Parquet dataset (raw frames are stored; the exclusion is a *bar* rule).
* **Post-close exclusion (symmetric):** ticks with ``exchange_ts`` time-of-day strictly after
  the session close (15:30:00 IST regular session) are likewise dropped from bars and counted
  (``ticks_dropped['post_close']`` in :meth:`stats_snapshot`; a side-channel counter, never a
  per-tick log). Rationale: a post-close minute bar has NO official candle to be compared against —
  it sits outside the nightly reconcile's window forever, so nothing can ever heal it. Raw
  persistence is unaffected, exactly as in the pre-open case.
* **Volume = Δ(cumulative day volume):** ``Tick.volume_traded`` is the broker's cumulative
  day volume; per-bar volume is the delta against the previous cumulative value.
    - *Day rollover:* per-symbol state (cumulative baseline, auction open, finalized watermark)
      resets when the tick date advances; any still-open prior-day bars are force-finalized first.
    - *Symbol first-tick:* if the first tick we see for a symbol today falls in the session-open
      minute (09:15), the engine has been up from the open, so the delta is the full cumulative —
      which correctly attributes the opening-auction volume to the 09:15 bar (matching the official
      candle). A first tick seen mid-session means an unseen span: the delta is unknowable, so it
      is 0 and ``BackfillJob.warmup_gap`` owns filling ``[last-bar-seen .. now)``.
    - *Cumulative DECREASE (restatement/glitch):* logged to ``corrections_log`` and contributes 0
      volume — bar volume is NEVER negative. The baseline keeps its high-water mark so a transient
      downward glitch does not double-count when the feed recovers; a genuine downward restatement
      converges via the nightly official reconcile.
* **Finalization = minute close + 5 s grace, Clock-driven:** a bar for minute M finalizes when
  ``clock.now() >= M+00:01:05``. :meth:`advance` applies that rule explicitly (tests drive a fake
  Clock); :meth:`on_tick` calls it opportunistically so live finalization needs no separate timer
  at tick rates, and :meth:`flush_all` force-finalizes (EOD / shutdown / day rollover).
* **Late ticks past grace — in-range is FREE, only amendments touch the store:** a tick for
  an already-finalized minute is checked against an IN-MEMORY copy of that bar's range (the last
  :data:`RECENT_BARS_PER_SYMBOL` finalized bars per symbol, kept by :meth:`_finalize`). A print that
  already sits inside the stored range has nothing to widen and nothing to correct, so it costs a
  dict lookup and two ``Decimal`` compares — **zero DuckDB statements, zero store-lock acquisitions,
  no ``corrections_log`` row**. Only a print OUTSIDE the remembered range (or one for a minute no
  longer remembered) pays the store round-trip: it amends the bar's high/low in place and writes the
  ``corrections_log`` row (``symbol, minute, tick_ts, value=ltp, cumulative_volume``,
  ``amended=True`` when applied). Close and volume are never restated post-finalize (the
  cumulative-delta chain must stay consistent — the nightly reconcile is the canonical fix). Amended bars are NOT re-published on ``bar.1m`` (downstream consumers already acted on the
  original; divergence is the reconcile job's concern).
    - *Why:* amending on every late tick costs ~4 DuckDB statements and 2 store-lock acquisitions
      plus a log line. Under processing lag every minute's bar finalizes off its FIRST tick, so
      every later tick of that minute is "late"; the slow path then drives throughput below real
      time and the lag grows without bound. The newest finalized minute is exactly the one memory
      holds, which is why an N=5 window covers the pathological case.
    - *Log volume is part of the cost:* at most ONE ``late_tick_past_grace`` line per
      ``(symbol, minute)``, plus a per-wall-minute ``late_ticks_summary`` aggregate
      (count / distinct symbols / max lag / how many actually touched the store).
    - *Watchdog:* processing lag (``clock.now() - newest exchange_ts``) past
      :data:`LAG_THRESHOLD_S` logs ERROR ``tick_processing_lagging`` (re-logged at most every
      :data:`LAG_LOG_INTERVAL_S`) and raises ONE owner alert per episode through the injected
      ``notify`` sink; ``tick_processing_recovered`` (INFO) closes the episode. **Reconnect grace:**
      :meth:`BarBuilder.on_feed_health` (bus topic ``feed.health``) suspends the watchdog for
      :data:`LAG_RECONNECT_GRACE_S` after a WARMING/HEALTHY transition, because Kite's connect-time
      snapshot carries each instrument's LAST-TRADE stamp and would flap the single global episode.
      In-child KiteTicker reconnects (no supervisor state transition) arrive via the heartbeat's
      ``connect_seq`` — a per-connect counter the child bumps and flushes BEFORE it re-subscribes, so
      the signal precedes the snapshot burst; the ``ws_connected`` False→True edge is only its
      fallback. **Bound:** one (re)connect delays a genuine alarm by up to 30 s, and a feed that
      re-transitions faster than the grace keeps re-arming it; that loop is the supervisor's alarm
      (``warming_timeout`` respawns, DEGRADED/HEALTHY cycles), not this one's.
    - *Only ``src='self'`` rows are amendable:* a ``kite_official``/``gap_backfilled`` row is
      CANONICAL, and reconcile never revisits a checkpointed day — an amendment there would be a
      permanent, invisible rewrite of an official candle. Such a late tick is recorded with
      ``amended=False, reason='official_bar_untouchable'`` and the stored row is left byte-intact.
    - *No read-decide-write window:* the amendment runs through
      :meth:`MarketStore.amend_bar_1m_extremes`, which re-reads, decides and writes under a single
      store lock + transaction and compare-and-swaps on the row it read — so the 15:50 reconcile
      write can never be clobbered by a decision taken against the pre-reconcile row.

Dependencies: ``core`` + the in-package ``MarketStore`` + the transport-free
``notify.catalog`` shapes (same as ``ReconcileJob``). No broker access — official candles are
:class:`~engine.marketdata.backfill.BackfillJob` / ``ReconcileJob`` business.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from engine.core.clock import IST, Clock
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Bar, Tick
from engine.marketdata.store import (
    AMEND_APPLIED,
    AMEND_FOREIGN_SRC,
    AMEND_IN_RANGE,
    AMEND_NO_BAR,
    AMEND_RACE_LOST,
    MarketStore,
)
from engine.notify.catalog import CatalogMessage, MessageKind

#: Injected async owner-alert sink (identical shape to ``ReconcileJob``/``TickerSupervisor``).
NotifySink = Callable[[CatalogMessage], Awaitable[None]]

_log = get_logger("engine.marketdata.bar_builder")

#: Canonical bus topic for finalized 1-minute bars.
BAR_1M_TOPIC = "bar.1m"

#: NSE continuous-session open: ticks strictly before this wall time are pre-open.
#: Constructor-overridable, but the composition root passes no override and per-day bounds are not
#: wired. Right on every regular day; special sessions (muhurat) are excluded by the live calendar.
#: Shortened sessions stay trading days: none is configured today, but wire per-day bounds before
#: adding one.
SESSION_OPEN_IST = time(9, 15)

#: NSE continuous-session close: ticks strictly AFTER this wall time are post-close and build
#: no bar. Same trust model as ``SESSION_OPEN_IST``.
SESSION_CLOSE_IST = time(15, 30)

#: Finalize grace after minute close, seconds (a constant, not a knob).
FINALIZE_GRACE_S = 5

#: Finalized bars retained IN MEMORY per symbol so the late-tick range check never reads DuckDB.
#: Under processing lag the late minute IS the newest finalized one, so the window only has to
#: survive out-of-order jitter, not a long backlog.
RECENT_BARS_PER_SYMBOL = 5

#: Processing-lag alarm threshold, seconds: ``clock.now() - newest exchange_ts`` past this means the
#: tick stream is no longer consumed in real time. A constant, not a knob — far outside anything a
#: healthy feed produces.
LAG_THRESHOLD_S = 120

#: Minimum seconds between two ``tick_processing_lagging`` ERROR lines inside ONE episode. The owner
#: alert is sent once per episode regardless; this only paces the log.
LAG_LOG_INTERVAL_S = 300

#: Seconds after a feed-health transition into WARMING/HEALTHY during which processing lag is NOT
#: evaluated (see :meth:`BarBuilder.on_feed_health`). It only has to cover the connect-time snapshot
#: burst, which lands in ONE batch within milliseconds of the connect; a real backlog is reported at
#: most 30 s later, not suppressed. The bound holds per (re)connect: a feed re-transitioning faster
#: than 30 s keeps the watchdog silent through that loop — deliberately, because a reconnect loop is
#: the supervisor's alarm and duplicating it here would page twice for one fault.
LAG_RECONNECT_GRACE_S = 30

#: ``FeedHealth.state`` values that arm the reconnect grace: the two states a (re)connect passes
#: through. STOPPED / DEGRADED / STALE must never arm — a degraded or stale feed is exactly when the
#: lag alarm has to stay live.
_LAG_GRACE_ARMING_STATES = frozenset({"WARMING", "HEALTHY"})

#: Buffer past session_close the lag watchdog keeps watching (the settlement tail) before treating a
#: stale timestamp as a snapshot echo rather than a backlog (see :meth:`BarBuilder._watch_lag`).
#: Added to the (possibly overridden) session close, so the watch window follows ITS close.
_LAG_WATCH_END_BUFFER = timedelta(minutes=15)

#: ``amend_bar_1m_extremes`` outcome → ``corrections_log.reason`` for the rows we could NOT amend.
#: ``AMEND_IN_RANGE`` keeps a NULL reason: it is not a refusal, there was simply nothing to widen.
_REFUSAL_REASON: dict[str, str | None] = {
    AMEND_IN_RANGE: None,
    AMEND_FOREIGN_SRC: "official_bar_untouchable",   # reconciled/backfilled row — canonical
    AMEND_NO_BAR: "no_stored_bar",
    AMEND_RACE_LOST: "amend_race_lost",
}


@dataclass
class _OpenBar:
    """A still-accumulating minute bar. ``first_ts``/``last_ts`` keep open/close correct under
    out-of-order ticks within the (minute + grace) window."""

    symbol: str
    minute: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    first_ts: datetime
    last_ts: datetime
    auction_open: Decimal | None = None


@dataclass(slots=True)
class _RecentBar:
    """What the late-tick path needs to remember about a finalized minute.

    ``high``/``low`` mirror the row we wrote and are widened in step with any applied amendment (a
    narrow memory would only cost a store round-trip, never a wrong answer). ``ranged`` is False for
    a placeholder for a minute we no longer remember: it exists purely to carry ``logged``, and every
    such tick still goes to the store because its true range is unknown. ``logged`` enforces the
    one-``late_tick_past_grace``-line-per-(symbol, minute) budget.
    """

    high: Decimal
    low: Decimal
    ranged: bool = True
    logged: bool = False


@dataclass(slots=True)
class _LateWindow:
    """Per-wall-minute late-tick aggregate drained into the ``late_ticks_summary`` line."""

    ticks: int = 0
    store_calls: int = 0
    amended: int = 0
    max_lag_s: float = 0.0
    symbols: set[str] = field(default_factory=set)


class BarBuilder:
    """Ticks → 1m bars: pre-open drop + auction open, cumulative-volume deltas,
    minute+grace finalization, late-tick corrections; batch-writes via ``MarketStore`` and
    publishes ``bar.1m``.

    Parameters
    ----------
    store:
        The single-writer :class:`MarketStore` (raw tick buffering + finalized-bar upserts +
        corrections log).
    clock:
        The single source of "now" — drives finalization; tests inject a controlled source.
    bus:
        Event bus for ``bar.1m``; ``None`` skips publishing (bare/offline contexts).
    session_open:
        The day's continuous-session open (default 09:15 IST regular session).
    session_close:
        The day's continuous-session close (default 15:30 IST regular session); ticks strictly after
        it build no bar. Same trust model as ``session_open`` (see ``SESSION_OPEN_IST``).
    grace_s:
        Finalize grace after minute close (pinned ~5 s; constructor param for tests only).
    persist_raw_ticks:
        Buffer every tick (incl. pre-open) into the tick Parquet dataset via
        ``store.stage_tick`` (flushed off-loop by ``aflush_ticks``). Disable only where another
        component owns raw persistence.
    notify:
        Injected async owner-alert sink (``CatalogMessage`` consumer, e.g. ``TelegramBot.send``);
        ``None`` leaves the lag watchdog log-only. Dispatched from :meth:`on_tick_event` (the
        async seam) — the synchronous :meth:`on_tick` only stages the message. A notify failure is
        logged, never raised.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        bus: EventBus | None = None,
        *,
        session_open: time = SESSION_OPEN_IST,
        session_close: time = SESSION_CLOSE_IST,
        grace_s: int = FINALIZE_GRACE_S,
        persist_raw_ticks: bool = True,
        notify: NotifySink | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._bus = bus
        self._session_open = session_open
        self._session_close = session_close
        # Lag watchdog window end: session_close + the settlement-tail buffer.
        self._lag_watch_end = (
            datetime.combine(date.min, session_close) + _LAG_WATCH_END_BUFFER
        ).time()
        self._grace = timedelta(minutes=1, seconds=int(grace_s))
        self._persist_raw_ticks = persist_raw_ticks
        self._notify = notify

        # --- per-(symbol, minute) accumulators + per-symbol state ---
        self._open: dict[tuple[str, datetime], _OpenBar] = {}
        self._last_cum: dict[str, int] = {}                 # cumulative-volume high-water
        self._day: dict[str, date] = {}                     # tick date being built per symbol
        self._auction_open: dict[str, Decimal] = {}         # last pre-open ltp per symbol
        self._finalized_through: dict[str, datetime] = {}   # last finalized minute per symbol
        # symbol -> newest RECENT_BARS_PER_SYMBOL finalized minutes (the late-tick range check
        # is answered from HERE, so an in-range late tick issues zero DuckDB statements).
        self._recent: dict[str, OrderedDict[datetime, _RecentBar]] = {}

        # --- late-tick observability: one line per (symbol, minute) + a per-wall-minute aggregate ---
        self._late_window_minute: datetime | None = None
        self._late_window = _LateWindow()

        # --- processing-lag watchdog ---
        self._lagging = False
        self._lag_logged_at: datetime | None = None
        self._pending_alert: CatalogMessage | None = None
        # Reconnect grace deadline: while set and in the future, _watch_lag does not evaluate.
        # Armed by on_feed_health; cleared lazily by _watch_lag once it has passed.
        self._lag_grace_until: datetime | None = None

        # --- feed_stats counters: hot-path increments, drained + reset by stats_snapshot() ---
        self._bars_finalized = 0
        self._bars_written = 0
        self._ticks_dropped: dict[str, int] = {}   # bar-exclusion reason -> count (post_close, …)
        self._late_ticks = 0                       # late ticks seen since the last snapshot
        self._late_store_calls = 0                 # …of which paid a store round-trip

    # ------------------------------------------------------------------ tick path

    def on_tick(self, tick: Tick) -> None:
        """Ingest one live tick (spec-pinned entry point; see the module docstring for the rules)."""
        ts = tick.exchange_ts.astimezone(IST)
        symbol = tick.tradingsymbol
        # ONE clock read per tick, shared by finalization, the late-tick summary window and the lag
        # watchdog so they cannot disagree.
        now = self._clock.now()
        self._watch_lag(now, ts)
        self._roll_day(symbol, ts.date())

        if self._persist_raw_ticks:
            # Stage only — the flush is DuckDB+Parquet work and must never run on the event loop;
            # on_tick_event offloads it via aflush_ticks.
            self._store.stage_tick(tick)

        if ts.time() < self._session_open:
            # The LAST pre-open print is the auction-derived open, stamped on the 09:15 row.
            self._auction_open[symbol] = tick.ltp
            self._advance(now)
            return

        if ts.time() > self._session_close:
            # Symmetric to pre-open: a post-close print must never build a bar (no official candle
            # exists for it, so nothing could ever heal it). Raw ticks are already staged above;
            # the drop is counted for feed_stats, never logged per tick.
            self._drop("post_close")
            self._advance(now)
            return

        minute = ts.replace(second=0, microsecond=0)
        finalized_through = self._finalized_through.get(symbol)
        if finalized_through is not None and minute <= finalized_through:
            self._handle_late_tick(tick, minute, now)
            self._advance(now)
            return

        delta = self._volume_delta(tick, minute)
        self._merge(tick, minute, delta, ts)
        self._advance(now)

    async def on_tick_event(self, tick: Tick) -> None:
        """Async adapter matching the event-bus ``Handler`` signature (subscribe to ``"tick"``).

        The tick-Parquet flush runs here, thread-offloaded, so the event loop (heartbeat, order
        updates, bar finalization) is never blocked by DuckDB/Parquet work; concurrent due-checks
        are safe (flushes serialize on the store's flush lock; a loser sees an empty buffer).

        This is also where the lag watchdog's owner alert is dispatched: ``on_tick`` is
        synchronous and must stay that way, so it only STAGES the message and this async seam sends
        it (at most once per lag episode)."""
        self.on_tick(tick)
        alert, self._pending_alert = self._pending_alert, None
        if alert is not None:
            await self._send_alert(alert)
        if self._persist_raw_ticks and self._store.tick_flush_due():
            await self._store.aflush_ticks()

    async def _send_alert(self, msg: CatalogMessage) -> None:
        if self._notify is None:
            return
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - alerting must never break the tick path
            _log.exception("tick_lag_notify_failed")

    # ------------------------------------------------------------------ finalization (Clock-driven)

    def advance(self) -> list[Bar]:
        """Finalize every open bar whose minute close + grace has passed per ``clock.now()``.

        The explicit time-control seam: tests drive a fake Clock and call this directly; live
        operation calls it on every tick (and the scheduler may call it on a coarse timer for
        symbols that stop ticking).
        """
        return self._advance(self._clock.now())

    def _advance(self, now: datetime) -> list[Bar]:
        """:meth:`advance` against a caller-supplied ``now``: the per-tick path already read the clock
        once and must not read it again. Also rolls the late-tick summary window, so the
        aggregate line lands on a coarse-timer ``advance()`` even after the late ticks stop."""
        due = [key for key, ob in self._open.items() if ob.minute + self._grace <= now]
        bars = [self._finalize(self._open.pop(key)) for key in sorted(due)]
        self._write_and_publish(bars)
        self._roll_late_window(now)
        return bars

    def flush_all(self) -> list[Bar]:
        """Force-finalize every open bar regardless of grace (EOD / shutdown / day rollover)."""
        keys = sorted(self._open.keys())
        bars = [self._finalize(self._open.pop(key)) for key in keys]
        self._write_and_publish(bars)
        self._roll_late_window(self._clock.now(), force=True)   # never strand a partial summary
        return bars

    # ------------------------------------------------------------------ internals

    def _roll_day(self, symbol: str, d: date) -> None:
        """Reset per-symbol state when the tick date advances (day rollover)."""
        prev = self._day.get(symbol)
        if prev == d:
            return
        if prev is not None:
            # Finalize anything still open from the prior day before dropping state.
            stale = [k for k in self._open if k[0] == symbol]
            bars = [self._finalize(self._open.pop(k)) for k in sorted(stale)]
            self._write_and_publish(bars)
            self._last_cum.pop(symbol, None)
            self._auction_open.pop(symbol, None)
            self._finalized_through.pop(symbol, None)
            self._recent.pop(symbol, None)      # yesterday's ranges can never answer today's late tick
            _log.info("bar_builder_day_rollover", symbol=symbol, frm=prev.isoformat(), to=d.isoformat())
        self._day[symbol] = d

    def _volume_delta(self, tick: Tick, minute: datetime) -> int:
        """Per-bar volume contribution of this tick (see the module docstring rules)."""
        symbol = tick.tradingsymbol
        cum = tick.volume_traded
        last = self._last_cum.get(symbol)
        if last is None:
            self._last_cum[symbol] = cum
            if minute.time() == self._session_open:
                # Up from the open: the whole cumulative (incl. opening-auction volume) belongs to
                # the session-open bar, matching the official 09:15 candle.
                return cum
            # Mid-session first sight: the unseen span's delta is unknowable; warmup_gap fills it.
            return 0
        if cum < last:
            # Never a negative bar volume; the high-water baseline is kept.
            self._store.append_correction(
                symbol, minute, tick.exchange_ts, tick.ltp, cumulative_volume=cum, amended=False
            )
            _log.warning(
                "cumulative_volume_decrease", symbol=symbol, last=last, got=cum,
                minute=minute.isoformat(),
            )
            return 0
        self._last_cum[symbol] = cum
        return cum - last

    def _merge(self, tick: Tick, minute: datetime, delta: int, ts: datetime) -> None:
        key = (tick.tradingsymbol, minute)
        ob = self._open.get(key)
        if ob is None:
            ob = _OpenBar(
                symbol=tick.tradingsymbol, minute=minute,
                open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp,
                volume=delta, first_ts=ts, last_ts=ts,
            )
            if minute.time() == self._session_open:
                ob.auction_open = self._auction_open.get(tick.tradingsymbol)
            self._open[key] = ob
            return
        ob.high = max(ob.high, tick.ltp)
        ob.low = min(ob.low, tick.ltp)
        ob.volume += delta
        if ts >= ob.last_ts:        # out-of-order-safe close (last print by exchange_ts wins)
            ob.close = tick.ltp
            ob.last_ts = ts
        if ts < ob.first_ts:        # out-of-order-safe open (first print by exchange_ts wins)
            ob.open = tick.ltp
            ob.first_ts = ts

    def _finalize(self, ob: _OpenBar) -> Bar:
        prev = self._finalized_through.get(ob.symbol)
        if prev is None or ob.minute > prev:
            self._finalized_through[ob.symbol] = ob.minute
        self._remember(ob.symbol, ob.minute, _RecentBar(high=ob.high, low=ob.low))
        self._bars_finalized += 1
        return Bar(
            symbol=ob.symbol, ts_minute=ob.minute,
            open=ob.open, high=ob.high, low=ob.low, close=ob.close,
            volume=ob.volume, src="self", auction_open=ob.auction_open,
        )

    def _write_and_publish(self, bars: list[Bar]) -> None:
        if not bars:
            return
        self._store.insert_bars_1m(bars)          # persist BEFORE notifying (batch upsert)
        self._bars_written += len(bars)
        if self._bus is not None:
            for bar in bars:
                self._bus.publish(BAR_1M_TOPIC, bar)

    def stats_snapshot(self) -> dict[str, Any]:
        """Return + reset the since-last-call bar counters for the periodic ``feed_stats`` line.

        ``bars_finalized`` counts minute bars closed (minute+grace or force-flush); ``bars_written`` is
        the batch-upsert count. ``ticks_dropped`` maps bar-exclusion reason → count (``post_close``).
        Reset-on-read gives the emitter clean per-interval deltas without a second clock."""
        snap: dict[str, Any] = {
            "bars_finalized": self._bars_finalized,
            "bars_written": self._bars_written,
            "ticks_dropped": dict(self._ticks_dropped),
            # late_ticks >> late_store_calls means the memory range check is absorbing them;
            # late_store_calls climbing means real amendments.
            "late_ticks": self._late_ticks,
            "late_store_calls": self._late_store_calls,
        }
        self._bars_finalized = 0
        self._bars_written = 0
        self._ticks_dropped = {}
        self._late_ticks = 0
        self._late_store_calls = 0
        return snap

    def _drop(self, reason: str) -> None:
        """Count one tick excluded from bar building (feed_stats; zero-cost, no hot-path logging)."""
        self._ticks_dropped[reason] = self._ticks_dropped.get(reason, 0) + 1

    def _remember(self, symbol: str, minute: datetime, bar: _RecentBar) -> None:
        """Insert/overwrite ``bar`` at ``minute`` in the per-symbol LRU, evicting past
        :data:`RECENT_BARS_PER_SYMBOL`. Shared by :meth:`_finalize` (a real range) and
        :meth:`_handle_late_tick` (a ``ranged=False`` placeholder).

        A placeholder never evicts a REAL range: a reconnect replaying 5+ stale minutes would
        otherwise wipe the cache and put every current-minute tick back on the store path during
        the lag episode. A cache full of real bars skips the placeholder; that stale minute keeps
        asking the store, as ``ranged=False`` means anyway."""
        recent = self._recent.get(symbol)
        if recent is None:
            recent = self._recent[symbol] = OrderedDict()
        if (
            not bar.ranged
            and minute not in recent
            and len(recent) >= RECENT_BARS_PER_SYMBOL
            and next(iter(recent.values())).ranged
        ):
            return
        recent[minute] = bar
        recent.move_to_end(minute)
        while len(recent) > RECENT_BARS_PER_SYMBOL:
            recent.popitem(last=False)

    def _handle_late_tick(self, tick: Tick, minute: datetime, now: datetime) -> None:
        """A tick for an already-finalized minute (past minute+grace).

        **The in-range case is free.** The finalized bar's range is answered from :attr:`_recent`.
        A print already inside it has nothing to widen or correct, so it issues NO store call (no
        ``amend_bar_1m_extremes``, no ``append_correction``) — see the module docstring for why.

        Only a print OUTSIDE the remembered range — or one for a minute we no longer remember, whose
        true range is unknown — takes the store path. Two guarantees live in the single
        ``amend_bar_1m_extremes`` call: only a ``src='self'`` row is amendable (an official/backfilled
        row is canonical — recorded with ``reason='official_bar_untouchable'``, bar left
        byte-intact), and the read-decide-write runs under one store lock + transaction with a
        compare-and-swap on the row it read, so the 15:50 reconcile write cannot be clobbered.
        Close/volume are never restated post-finalize (the nightly reconcile is the canonical fix).

        An applied amendment widens the remembered range in the same step; memory is only ever
        widened from values the store accepted, so it can never make an out-of-range print look
        in-range.
        """
        symbol = tick.tradingsymbol
        recent = self._recent.get(symbol)
        known = recent.get(minute) if recent is not None else None

        if known is not None and known.ranged and known.low <= tick.ltp <= known.high:
            self._note_late(known, symbol, minute, tick, now, outcome=AMEND_IN_RANGE, touched=False)
            return

        outcome = self._store.amend_bar_1m_extremes(symbol, minute, tick.ltp, require_src="self")
        amended = outcome == AMEND_APPLIED
        reason = None if amended else _REFUSAL_REASON.get(outcome, outcome)
        self._store.append_correction(
            symbol, minute, tick.exchange_ts, tick.ltp,
            cumulative_volume=tick.volume_traded, amended=amended, reason=reason,
        )
        if known is None:
            # A minute we no longer remember: a ``ranged=False`` placeholder keeps the log-dedup
            # budget, and every later tick for it keeps asking the store. _remember may skip the
            # insert when the cache is full of real ranges; the local instance still feeds _note_late.
            placeholder = _RecentBar(high=Decimal(0), low=Decimal(0), ranged=False)
            self._remember(symbol, minute, placeholder)
            known = self._recent[symbol].get(minute, placeholder)
        elif known.ranged and outcome in (AMEND_APPLIED, AMEND_IN_RANGE):
            # The store accepted this print into the row's range; widen memory to match.
            known.high = max(known.high, tick.ltp)
            known.low = min(known.low, tick.ltp)
        self._note_late(known, symbol, minute, tick, now, outcome=outcome, touched=True)

    # ------------------------------------------------------- late-tick observability

    def _note_late(
        self, state: _RecentBar, symbol: str, minute: datetime, tick: Tick, now: datetime,
        *, outcome: str, touched: bool,
    ) -> None:
        """Count one late tick, and log AT MOST ONE line per (symbol, minute).

        A per-tick log line is itself part of the cost under lag, so the detail collapses to one
        line per (symbol, minute); volume lives in the per-wall-minute ``late_ticks_summary``."""
        self._late_ticks += 1
        window = self._late_window
        window.ticks += 1
        window.symbols.add(symbol)
        lag_s = (now - tick.exchange_ts).total_seconds()
        if lag_s > window.max_lag_s:
            window.max_lag_s = lag_s
        if touched:
            self._late_store_calls += 1
            window.store_calls += 1
            if outcome == AMEND_APPLIED:
                window.amended += 1
        if state.logged:
            return
        state.logged = True
        _log.info(
            "late_tick_past_grace", symbol=symbol, minute=minute.isoformat(),
            tick_ts=tick.exchange_ts.isoformat(), amended=outcome == AMEND_APPLIED, outcome=outcome,
            lag_s=round(lag_s, 1),
        )

    def _roll_late_window(self, now: datetime, *, force: bool = False) -> None:
        """Emit the ``late_ticks_summary`` aggregate when the wall minute turns over (or on flush)."""
        minute = now.replace(second=0, microsecond=0)
        if self._late_window_minute is None:
            self._late_window_minute = minute
            if not force:
                return
        if minute == self._late_window_minute and not force:
            return
        window = self._late_window
        if window.ticks:
            _log.info(
                "late_ticks_summary", window=self._late_window_minute.isoformat(),
                late_ticks=window.ticks, symbols=len(window.symbols),
                store_calls=window.store_calls, amended=window.amended,
                max_lag_s=round(window.max_lag_s, 1),
            )
        self._late_window_minute = minute
        self._late_window = _LateWindow()

    # ------------------------------------------------------- processing-lag watchdog

    async def on_feed_health(self, event: Any) -> None:
        """Arm the reconnect grace on a feed-health transition (bus topic ``feed.health``).

        On ``kws.connect`` Kite delivers one snapshot tick per subscribed instrument carrying its
        **last-trade** ``exchange_timestamp``, interleaved with near-fresh ticks. :meth:`_watch_lag`
        keeps ONE global episode, so each stale tick would re-open it and the next fresh tick close
        it (a flap and an owner page per pair). Nothing on the wire marks a snapshot frame; the
        reconnect itself is the signal. A transition into WARMING/HEALTHY therefore suspends lag
        evaluation for :data:`LAG_RECONNECT_GRACE_S`. An episode already open is closed here, so its
        staged owner page is dropped before :meth:`on_tick_event` can dispatch it.

        KiteTicker also reconnects INSIDE the child (re-subscribes and re-delivers the snapshot)
        with no supervisor state change. The supervisor publishes ``feed.health`` when the
        heartbeat's ``connect_seq`` CHANGES (``ticker_supervisor._handle_frame``), arriving here as
        a HEALTHY event. ``connect_seq`` is bumped at the top of the child's ``_on_connect`` and
        shipped on a heartbeat emitted THERE, before ``ws.subscribe``: the signal is in-band and
        precedes the snapshot burst, which a ``ws_connected`` False→True edge (seen only on the
        next 1 s heartbeat) cannot do, and it does not depend on how the drop was reported. The
        ``ws_connected`` edge is only the fallback for a heartbeat carrying no ``connect_seq``.

        Bound: one (re)connect delays a genuine alarm by up to 30 s, and a feed reconnecting faster
        than the grace keeps it armed — that loop is the supervisor's alarm, not this one's.

        Payload: ``TickerSupervisor._publish_health`` publishes a ``FeedHealth`` model whose
        ``state`` is one of ``{"STOPPED","WARMING","HEALTHY","DEGRADED","STALE"}``. ``state`` is the
        ONLY field read, duck-typed rather than imported: ``marketdata`` does not depend on
        ``broker`` (module docstring "Dependencies"), and an unexpected payload must degrade to
        "no grace", never raise on the bus."""
        state = getattr(event, "state", None)
        if state not in _LAG_GRACE_ARMING_STATES:
            return                              # STOPPED / DEGRADED / STALE keep the watchdog live
        until = self._clock.now() + timedelta(seconds=LAG_RECONNECT_GRACE_S)
        self._lag_grace_until = until
        _log.info("tick_lag_watch_grace_armed", state=state, until=until.isoformat())
        if self._lagging:
            self._close_lag_episode("tick_lag_watch_reconnect_grace")

    def _watch_lag(self, now: datetime, ts: datetime) -> None:
        """Track ``now - newest exchange_ts`` and alarm past :data:`LAG_THRESHOLD_S`.

        Evaluated on the TICK path only, deliberately: lag is a property of tick consumption, so an
        idle overnight engine must not page anyone. The ERROR line repeats at most every
        :data:`LAG_LOG_INTERVAL_S` while the episode lasts; the owner alert is staged ONCE per
        episode and dispatched from :meth:`on_tick_event`.

        Plausibility guards: (i) a tick stamped on a PREVIOUS day is a snapshot echo (a reconnect
        after hours replays old stamps), never consumption lag — it neither alarms nor recovers an
        episode. (ii) The alarm only evaluates inside the session window (``self._session_open``
        to ``self._lag_watch_end``) — outside it an active episode is reset quietly, because the
        spiral this watchdog exists for can only grow while the exchange is producing ticks.
        Accepted residual: a holiday-morning reconnect echoing SAME-day stamps inside the window
        could page once; the calendar is deliberately not threaded in for that rare case.
        (iii) Inside the grace armed by :meth:`on_feed_health` nothing is evaluated; the deadline
        is cleared LAZILY on the first tick that outlives it, not by a timer."""
        if ts.date() != now.date():
            return                              # previous-day snapshot echo — not consumption lag
        if not (self._session_open <= now.time() <= self._lag_watch_end):
            if self._lagging:
                self._close_lag_episode("tick_lag_watch_suspended_out_of_session")
            return
        grace_until = self._lag_grace_until
        if grace_until is not None:
            if now < grace_until:
                # EventBus.publish is fire-and-forget, so a tick task queued BEFORE the arming can
                # still land here with an episode open.
                if self._lagging:
                    self._close_lag_episode("tick_lag_watch_reconnect_grace")
                return
            self._lag_grace_until = None        # lapsed — resume normal evaluation from this tick on
        lag_s = (now - ts).total_seconds()
        if lag_s >= LAG_THRESHOLD_S:
            if not self._lagging:
                self._lagging = True
                self._lag_logged_at = None
                self._pending_alert = _lagging_alert(lag_s)
            last = self._lag_logged_at
            if last is None or (now - last).total_seconds() >= LAG_LOG_INTERVAL_S:
                self._lag_logged_at = now
                _log.error(
                    "tick_processing_lagging", lag_s=round(lag_s, 1), threshold_s=LAG_THRESHOLD_S,
                    newest_tick_ts=ts.isoformat(),
                )
        elif self._lagging:
            self._close_lag_episode("tick_processing_recovered", lag_s=round(lag_s, 1))

    def _close_lag_episode(self, event: str, **fields: Any) -> None:
        """Reset lag-episode state and log ``event``: shared by :meth:`_watch_lag`'s
        out-of-session-suspend and in-session-recovery branches, which differ only in which line
        closes the episode — both must never send a stale page for an episode already over."""
        self._lagging = False
        self._lag_logged_at = None
        self._pending_alert = None
        _log.info(event, **fields)


def _lagging_alert(lag_s: float) -> CatalogMessage:
    """The one-per-episode owner page for :data:`LAG_THRESHOLD_S` processing lag."""
    return CatalogMessage(
        kind=MessageKind.FEED_DEGRADED,   # closest catalog kind (closed catalog here)
        title="Tick processing is falling behind",
        body=(
            f"Live ticks are being consumed {lag_s:.0f}s behind the exchange clock (threshold "
            f"{LAG_THRESHOLD_S}s). Bars are finalizing off partial minutes and every later print of "
            "the same minute takes the late-tick path — the lag grows on its own from here. Check "
            "engine.log for tick_processing_lagging / late_ticks_summary; a restart clears it."
        ),
        severity="warning",
        data={"lag_s": round(lag_s, 1), "threshold_s": LAG_THRESHOLD_S},
    )
