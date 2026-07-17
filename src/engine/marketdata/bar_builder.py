"""Live ticks → finalized 1-minute bars (§3.2.3, §4.4 job 1, A13/A14).

``BarBuilder`` consumes the parsed :class:`~engine.core.types.Tick` stream (the ``tick`` bus topic
published by ``TickerSupervisor``) and produces canonical :class:`~engine.core.types.Bar` rows
(``src='self'``) — batch-written through :class:`~engine.marketdata.store.MarketStore` (the single
DuckDB writer) and published on ``bar.1m``.

Pinned behaviors (the plan is law):

* **Pre-open exclusion (A14):** ticks with ``exchange_ts`` before 09:15:00 IST are dropped from
  bars. The **auction-derived open** — the last pre-open ``ltp`` seen for the symbol (the pre-open
  ticks carry the equilibrium/auction price discovery) — is captured separately and stamped as
  ``auction_open`` on that day's 09:15 bar row ONLY. Pre-open ticks ARE still persisted raw to the
  tick Parquet dataset (§4.3 stores raw frames; the exclusion is a *bar* rule).
* **Volume = Δ(cumulative day volume) (A13):** ``Tick.volume_traded`` is the broker's cumulative
  day volume; per-bar volume is the delta against the previous cumulative value.
    - *Day rollover:* per-symbol state (cumulative baseline, auction open, finalized watermark)
      resets when the tick date advances; any still-open prior-day bars are force-finalized first.
    - *Symbol first-tick:* if the first tick we see for a symbol today falls in the session-open
      minute (09:15), the engine has been up from the open, so the delta is the full cumulative —
      which correctly attributes the opening-auction volume to the 09:15 bar (matching the official
      candle). A first tick seen mid-session means an unseen span: the delta is unknowable, so it
      is 0 and ``BackfillJob.warmup_gap`` owns filling ``[last-bar-seen .. now)`` (§2.6 step 4 /
      §4.4 job 1).
    - *Cumulative DECREASE (restatement/glitch):* logged to ``corrections_log`` and contributes 0
      volume — bar volume is NEVER negative. The baseline keeps its high-water mark so a transient
      downward glitch does not double-count when the feed recovers; a genuine downward restatement
      converges via the nightly official reconcile (A13 backstop).
* **Finalization = minute close + 5 s grace, Clock-driven:** a bar for minute M finalizes when
  ``clock.now() >= M+00:01:05``. :meth:`advance` applies that rule explicitly (tests drive a fake
  Clock); :meth:`on_tick` calls it opportunistically so live finalization needs no separate timer
  at tick rates, and :meth:`flush_all` force-finalizes (EOD / shutdown / day rollover).
* **Late ticks past grace → corrections_log:** a tick for an already-finalized minute is recorded
  (``symbol, minute, tick_ts, value=ltp, cumulative_volume``). If its print falls outside the
  finalized bar's range the bar's high/low are amended in place (upsert) and the correction row is
  marked ``amended=True``; close and volume are never restated post-finalize (the cumulative-delta
  chain must stay consistent — the nightly reconcile is the canonical fix, §4.4 job 2). Amended
  bars are NOT re-published on ``bar.1m`` (downstream consumers already acted on the original;
  divergence is the reconcile job's concern).

Dependencies: ``core`` + the in-package ``MarketStore`` (§3.2.3). No broker access — official
candles are :class:`~engine.marketdata.backfill.BackfillJob` / ``ReconcileJob`` business.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from engine.core.clock import IST, Clock
from engine.core.eventbus import EventBus
from engine.core.log import get_logger
from engine.core.types import Bar, Tick
from engine.marketdata.store import MarketStore

_log = get_logger("engine.marketdata.bar_builder")

#: Canonical bus topic for finalized 1-minute bars (§3.2.1).
BAR_1M_TOPIC = "bar.1m"

#: NSE continuous-session open (A14, pinned): ticks strictly before this wall time are pre-open.
#: Constructor-overridable for special sessions (muhurat) — the composition root passes the
#: NSECalendar session open for the day.
SESSION_OPEN_IST = time(9, 15)

#: Finalize grace after minute close, seconds (§4.4 job 1, pinned "~5 s" — a constant, not a knob).
FINALIZE_GRACE_S = 5


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


class BarBuilder:
    """Ticks → 1m bars (§3.2.3): pre-open drop + auction open (A14), cumulative-volume deltas
    (A13), minute+grace finalization, late-tick corrections; batch-writes via ``MarketStore`` and
    publishes ``bar.1m``.

    Parameters
    ----------
    store:
        The single-writer :class:`MarketStore` (raw tick buffering + finalized-bar upserts +
        corrections log).
    clock:
        The single source of "now" (§3.2) — drives finalization; tests inject a controlled source.
    bus:
        Event bus for ``bar.1m``; ``None`` skips publishing (bare/offline contexts).
    session_open:
        The day's continuous-session open (default 09:15 IST regular session, A14).
    grace_s:
        Finalize grace after minute close (plan-pinned ~5 s; constructor param for tests only).
    persist_raw_ticks:
        Buffer every tick (incl. pre-open) into the §4.3 tick Parquet dataset via
        ``store.buffer_tick``. Disable only where another component owns raw persistence.
    """

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        bus: EventBus | None = None,
        *,
        session_open: time = SESSION_OPEN_IST,
        grace_s: int = FINALIZE_GRACE_S,
        persist_raw_ticks: bool = True,
    ) -> None:
        self._store = store
        self._clock = clock
        self._bus = bus
        self._session_open = session_open
        self._grace = timedelta(minutes=1, seconds=int(grace_s))
        self._persist_raw_ticks = persist_raw_ticks

        # --- per-(symbol, minute) accumulators + per-symbol state ---
        self._open: dict[tuple[str, datetime], _OpenBar] = {}
        self._last_cum: dict[str, int] = {}                 # cumulative-volume high-water (A13)
        self._day: dict[str, date] = {}                     # tick date being built per symbol
        self._auction_open: dict[str, Decimal] = {}         # last pre-open ltp per symbol (A14)
        self._finalized_through: dict[str, datetime] = {}   # last finalized minute per symbol

    # ------------------------------------------------------------------ tick path (§4.4 job 1)

    def on_tick(self, tick: Tick) -> None:
        """Ingest one live tick (spec-pinned entry point; see the module docstring for the rules)."""
        ts = tick.exchange_ts.astimezone(IST)
        symbol = tick.tradingsymbol
        self._roll_day(symbol, ts.date())

        if self._persist_raw_ticks:
            # Raw frames (incl. pre-open) persist to the tick Parquet dataset (§4.3, ~5 s batches).
            # Stage only — the flush is DuckDB+Parquet work (~seconds at watchlist scale) and must
            # never run on the event loop (§2.2); on_tick_event offloads it via aflush_ticks.
            self._store.stage_tick(tick)

        if ts.time() < self._session_open:
            # A14: pre-open ticks never contaminate bars; the LAST pre-open print is the
            # auction-derived open, stamped on the 09:15 row at finalize.
            self._auction_open[symbol] = tick.ltp
            self.advance()
            return

        minute = ts.replace(second=0, microsecond=0)
        finalized_through = self._finalized_through.get(symbol)
        if finalized_through is not None and minute <= finalized_through:
            self._handle_late_tick(tick, minute)
            self.advance()
            return

        delta = self._volume_delta(tick, minute)
        self._merge(tick, minute, delta, ts)
        self.advance()

    async def on_tick_event(self, tick: Tick) -> None:
        """Async adapter matching the event-bus ``Handler`` signature (subscribe to ``"tick"``).

        The tick-Parquet flush runs here, thread-offloaded, so the event loop (heartbeat, order
        updates, bar finalization) is never blocked by DuckDB/Parquet work; concurrent due-checks
        are safe (flushes serialize on the store's flush lock; a loser sees an empty buffer)."""
        self.on_tick(tick)
        if self._persist_raw_ticks and self._store.tick_flush_due():
            await self._store.aflush_ticks()

    # ------------------------------------------------------------------ finalization (Clock-driven)

    def advance(self) -> list[Bar]:
        """Finalize every open bar whose minute close + grace has passed per ``clock.now()``.

        The explicit time-control seam (§4.4 job 1 "finalize at minute close + 5 s grace"): tests
        drive a fake Clock and call this directly; live operation calls it on every tick (and the
        scheduler may call it on a coarse timer for symbols that simply stop ticking).
        """
        now = self._clock.now()
        due = [key for key, ob in self._open.items() if ob.minute + self._grace <= now]
        bars = [self._finalize(self._open.pop(key)) for key in sorted(due)]
        self._write_and_publish(bars)
        return bars

    def flush_all(self) -> list[Bar]:
        """Force-finalize every open bar regardless of grace (EOD / shutdown / day rollover)."""
        keys = sorted(self._open.keys())
        bars = [self._finalize(self._open.pop(key)) for key in keys]
        self._write_and_publish(bars)
        return bars

    # ------------------------------------------------------------------ internals

    def _roll_day(self, symbol: str, d: date) -> None:
        """Reset per-symbol state when the tick date advances (A13 day rollover)."""
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
            _log.info("bar_builder_day_rollover", symbol=symbol, frm=prev.isoformat(), to=d.isoformat())
        self._day[symbol] = d

    def _volume_delta(self, tick: Tick, minute: datetime) -> int:
        """Per-bar volume contribution of this tick (A13 — see the module docstring rules)."""
        symbol = tick.tradingsymbol
        cum = tick.volume_traded
        last = self._last_cum.get(symbol)
        if last is None:
            self._last_cum[symbol] = cum
            if minute.time() == self._session_open:
                # Up from the open: the whole cumulative (incl. opening-auction volume) belongs to
                # the session-open bar — matches the official 09:15 candle.
                return cum
            # Mid-session first sight: the delta over the unseen span is unknowable — contribute 0;
            # warmup_gap fills [last-bar-seen .. now) from official candles (§2.6 step 4).
            return 0
        if cum < last:
            # Restatement/glitch guard (A13): never a negative bar volume. High-water baseline kept.
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
                ob.auction_open = self._auction_open.get(tick.tradingsymbol)  # A14
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
        return Bar(
            symbol=ob.symbol, ts_minute=ob.minute,
            open=ob.open, high=ob.high, low=ob.low, close=ob.close,
            volume=ob.volume, src="self", auction_open=ob.auction_open,
        )

    def _write_and_publish(self, bars: list[Bar]) -> None:
        if not bars:
            return
        self._store.insert_bars_1m(bars)          # persist BEFORE notifying (batch upsert)
        if self._bus is not None:
            for bar in bars:
                self._bus.publish(BAR_1M_TOPIC, bar)

    def _handle_late_tick(self, tick: Tick, minute: datetime) -> None:
        """A tick for an already-finalized minute (past minute+grace): corrections_log (§4.4 job 1).

        Amends the finalized bar's high/low in place when the late print falls outside its range
        (``amended=True`` on the correction row); close/volume are never restated post-finalize —
        the nightly official reconcile is the canonical fix (module docstring).
        """
        symbol = tick.tradingsymbol
        amended = False
        existing = self._store.get_bars_1m(symbol, minute, minute + timedelta(minutes=1))
        if existing:
            bar = existing[0]
            if tick.ltp > bar.high or tick.ltp < bar.low:
                self._store.insert_bars_1m([
                    bar.model_copy(
                        update={"high": max(bar.high, tick.ltp), "low": min(bar.low, tick.ltp)}
                    )
                ])
                amended = True
        self._store.append_correction(
            symbol, minute, tick.exchange_ts, tick.ltp,
            cumulative_volume=tick.volume_traded, amended=amended,
        )
        _log.info(
            "late_tick_past_grace", symbol=symbol, minute=minute.isoformat(),
            tick_ts=tick.exchange_ts.isoformat(), amended=amended,
        )
