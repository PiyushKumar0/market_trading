"""Historical candle backfill + startup warm-up gap fill.

``BackfillJob`` pulls official Kite candles through :meth:`KiteClient.historical`; the client's
shared ``historical`` rate-limiter bucket paces every request, so this job never sleeps on its own.
Requests are **chunked** per Kite's per-request range caps (pinned constants below),
**checkpointed** per ``(symbol, interval)`` in the SQLite ``backfill_checkpoints`` table, and
**resumable**: a re-run skips everything at or behind the checkpoint.

**No re-adjustment:** Kite minute and daily candles are ALREADY corp-action adjusted
(``settings data.minute_candles_adjusted=true``; ``scripts/kite_candle_adjustment_check.py``).
Candles are written EXACTLY as fetched — never apply split/bonus factors across ex-dates.

Provenance (``bars_1m.src``):
    * :meth:`run` writes ``src='kite_official'`` (canonical historical rows).
    * :meth:`warmup_gap` writes ``src='gap_backfilled'`` — the offline-span fill, which the
      nightly reconcile EXCLUDES from its drift denominator (they are not self-built bars).
    * :meth:`warmup_gap` with ``confirm_until`` ALSO writes ``bars_1m_no_trade`` rows —
      upstream-confirmed tradeless minutes, which are evidence and not bars: no price is synthesized
      and ``bars_1m`` is never touched by that path. See the method's contract.

Dependencies: ``core`` + ``broker``. The SQLite checkpoint table lives in ``state.db``;
the bars land in DuckDB via the single-writer :class:`MarketStore`.
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from kiteconnect.exceptions import TokenException
from pydantic import BaseModel, ConfigDict, Field

from engine.broker.kite_client import KiteClient
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.core.types import Bar, BarSrc
from engine.marketdata.store import DailyBar, MarketStore

_log = get_logger("engine.marketdata.backfill")

# --- Kite historical per-request range caps (pinned) --------------------------------
# These are BROKER limits, not tunables: a request spanning more days than this is rejected by the
# API. settings.backfill.{minute,day}_chunk_days may tighten them but can never exceed them.
KITE_MINUTE_CHUNK_DAYS = 60     # max span per minute-interval historical request
KITE_DAY_CHUNK_DAYS = 2000      # max span per day-interval historical request

#: The failed-span error of a symbol with no instrument token: no request is made, and no retry can
#: fetch it until the universe or the instrument dump changes.
_UNKNOWN_TOKEN = "unknown_instrument_token"


class BackfillSpan(BaseModel):
    """One per-symbol span in a :class:`BackfillReport` (ISO strings; a report, not a query key)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    frm: str
    to: str
    bars: int = 0
    error: str | None = None


class BackfillReport(BaseModel):
    """Outcome of one backfill run: requested/fetched/failed spans + bars written.

    ``requested`` is the caller-asked span per symbol; ``fetched`` the chunk spans actually pulled
    (a resumed symbol shows only the remainder); ``failed`` the spans abandoned with their error —
    those symbols stay behind their checkpoint and the next run resumes them (resumable).
    """

    interval: str
    requested: list[BackfillSpan] = Field(default_factory=list)
    fetched: list[BackfillSpan] = Field(default_factory=list)
    failed: list[BackfillSpan] = Field(default_factory=list)
    bars_written: int = 0
    #: Symbols that needed no request at all — every requested session was already present.
    skipped_covered: int = 0
    #: Minutes recorded in ``bars_1m_no_trade`` by :meth:`BackfillJob.warmup_gap` — upstream-confirmed
    #: tradeless minutes, NOT bars.
    no_trade_confirmed: int = 0
    #: (symbol, minute) confirmations DROPPED by the correlation guard: the same minute was missing
    #: across enough of the swept symbols to be a feed/vendor gap rather than thin symbols going
    #: quiet together, so it is left a hole.
    no_trade_correlated_skipped: int = 0

    @property
    def ok(self) -> bool:
        """The job watermark's verdict (``ops.jobs._job_result_ok``): False when any span failed for
        a reason other than a missing instrument token (a rejected token, a broker or network error),
        so catch-up retries the day, until the 7-day give-up. One symbol with no token never sinks
        the day (no retry can fetch it, and a failed ``daily_bars`` day holds ``rec_outcomes``), but
        EVERY requested symbol without one is an empty or degraded token map that a later
        instruments refresh rebuilds, so that does."""
        if any(span.error != _UNKNOWN_TOKEN for span in self.failed):
            return False
        unknown = {span.symbol for span in self.failed}
        return not self.requested or not {span.symbol for span in self.requested} <= unknown


def _candle_field(candle: Any, name: str) -> Any:
    return candle[name] if isinstance(candle, dict) else getattr(candle, name)


def _candle_ts(candle: Any) -> datetime:
    """Candle timestamp → tz-aware IST (pykiteconnect returns tz-aware +05:30 datetimes)."""
    value = _candle_field(candle, "date")
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)


def _dec(value: Any) -> Decimal:
    """Broker float → exact Decimal via the shortest repr (prices are never floats downstream)."""
    return Decimal(str(value))


def _abort_remaining(
    report: BackfillReport,
    remaining: Sequence[str],
    frm: str,
    to: str,
    event: str,
    **log_fields: Any,
) -> None:
    """Common tail of a ``TokenException`` catch in :meth:`BackfillJob.run`/``warmup_gap``/``daily_gap``:
    a rejected token fails every subsequent historical call identically, so mark every un-attempted
    symbol ``aborted_token_rejected`` in one pass and log once. ``frm``/``to`` are the caller's
    already-``isoformat()``-ed WHOLE requested span. Callers set their own ``aborted`` flag and
    break their own loops."""
    for rem in remaining:
        report.failed.append(BackfillSpan(symbol=rem, frm=frm, to=to, error="aborted_token_rejected"))
    _log.warning(event, symbols_remaining=len(remaining), **log_fields)


class BackfillJob:
    """Chunked, checkpointed, resumable official-candle backfill.

    Parameters
    ----------
    store:
        Single-writer :class:`MarketStore` (bars_1m upserts / bars_1d upserts).
    kite:
        :class:`KiteClient` — ALL pacing happens inside it (the shared RateLimiter ``historical``
        bucket, 3 req/s). This job issues requests back-to-back and lets the limiter pace.
    clock:
        The single source of "now"/tz-aware combination.
    settings:
        ``settings.backfill`` chunk knobs (clamped to the pinned Kite caps above).
    conn:
        The SQLite ``state.db`` connection (WAL) holding ``backfill_checkpoints``.
    token_for_symbol:
        tradingsymbol → instrument_token resolver (the composition root wires ``InstrumentStore``).
        ``None`` for a symbol ⇒ that symbol is reported failed, never guessed.
    """

    def __init__(
        self,
        store: MarketStore,
        kite: KiteClient,
        clock: Clock,
        settings: Settings,
        conn: sqlite3.Connection,
        token_for_symbol: Callable[[str], int | None],
    ) -> None:
        self._store = store
        self._kite = kite
        self._clock = clock
        self._settings = settings
        self._conn = conn
        self._token_for_symbol = token_for_symbol

    # ------------------------------------------------------------------ history

    async def run(
        self, symbols: Sequence[str], interval: str, start: date, end: date
    ) -> BackfillReport:
        """Backfill ``[start, end]`` (dates inclusive) of ``interval`` candles for ``symbols``.

        ``interval`` is the Kite interval string (``"minute"`` | ``"day"``). Chunked per the pinned
        range caps; each successfully written chunk advances the ``(symbol, interval)`` checkpoint
        so an interrupted run resumes exactly where it stopped. Rows are written
        ``src='kite_official'`` — already corp-action adjusted, NO re-adjustment (module
        docstring). A per-symbol failure abandons that symbol's remaining chunks (checkpoint left
        at the last success) and continues with the next symbol.
        """
        report = BackfillReport(interval=interval)
        chunk_days = self._chunk_days(interval)
        symbols = list(symbols)
        for i, symbol in enumerate(symbols):
            report.requested.append(
                BackfillSpan(symbol=symbol, frm=start.isoformat(), to=end.isoformat())
            )
            checkpoint = self._checkpoint(symbol, interval)
            eff_start = start if checkpoint is None else max(start, checkpoint + timedelta(days=1))
            if eff_start > end:
                _log.info(
                    "backfill_symbol_already_complete", symbol=symbol, interval=interval,
                    checkpoint=None if checkpoint is None else checkpoint.isoformat(),
                )
                continue
            token = self._token_for_symbol(symbol)
            if token is None:
                report.failed.append(
                    BackfillSpan(
                        symbol=symbol, frm=eff_start.isoformat(), to=end.isoformat(),
                        error=_UNKNOWN_TOKEN,
                    )
                )
                _log.warning("backfill_unknown_token", symbol=symbol)
                continue
            cur = eff_start
            aborted = False
            while cur <= end:
                chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
                try:
                    candles = await self._kite.historical(
                        token,
                        self._clock.combine(cur, time(0, 0)),
                        self._clock.combine(chunk_end, time(23, 59, 59)),
                        interval,
                    )
                    written = await self._write_candles(
                        symbol, interval, candles, src="kite_official"
                    )
                except Exception as exc:  # noqa: BLE001 - record + resume next run
                    report.failed.append(
                        BackfillSpan(
                            symbol=symbol, frm=cur.isoformat(), to=chunk_end.isoformat(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    if isinstance(exc, TokenException):
                        # Abort the whole run; un-attempted symbols are reported failed and the
                        # resume (via PostLoginRecovery) refills them.
                        _abort_remaining(
                            report, symbols[i + 1:], start.isoformat(), end.isoformat(),
                            "backfill_aborted_token_rejected", interval=interval,
                        )
                        aborted = True
                        break
                    _log.warning(
                        "backfill_chunk_failed", symbol=symbol, interval=interval,
                        frm=cur.isoformat(), to=chunk_end.isoformat(), error=str(exc),
                    )
                    break  # keep the checkpoint at the last success; next run resumes here
                report.fetched.append(
                    BackfillSpan(
                        symbol=symbol, frm=cur.isoformat(), to=chunk_end.isoformat(), bars=written
                    )
                )
                report.bars_written += written
                if candles:
                    # OBSERVED-through, never requested-through: a span can end on days whose
                    # candles don't exist yet (a "today" requested pre-close, a weekend tail);
                    # checkpointing the requested end would record them complete forever.
                    observed = max(_candle_ts(c).date() for c in candles)
                    self._advance_checkpoint(symbol, interval, min(chunk_end, observed))
                else:
                    _log.info(
                        "backfill_chunk_empty", symbol=symbol, interval=interval,
                        frm=cur.isoformat(), to=chunk_end.isoformat(),
                    )
                cur = chunk_end + timedelta(days=1)
            if aborted:
                break
        _log.info(
            "backfill_run_done", interval=interval, symbols=len(symbols),
            bars_written=report.bars_written, failed=len(report.failed),
        )
        return report

    # ------------------------------------------------------------------ warm-up gap

    async def warmup_gap(
        self,
        symbols: Sequence[str],
        frm: datetime,
        to: datetime,
        *,
        confirm_until: datetime | None = None,
    ) -> BackfillReport:
        """Fill the ``[frm, to)`` minute-bar gap from official candles.

        ``frm`` is last-bar-seen, so warm-up never requires live tick capture since 09:15. Rows are
        written ``src='gap_backfilled'`` (excluded from the nightly reconcile's drift denominator),
        as fetched. NOT checkpointed: every startup computes its own gap.

        ``confirm_until`` closes an UNFILLABLE hole. A thin, high-priced symbol has minutes in which
        nothing trades: no tick ⇒ no self-built bar, and Kite publishes no candle for a tradeless
        minute ⇒ this fill writes nothing, the minute stays a hole and the warm-up gate refuses the
        symbol for the session. When ``confirm_until`` is set, a still-missing minute that Kite did
        not return, for a symbol whose fetch completed, is recorded in ``bars_1m_no_trade``
        (:meth:`MarketStore.mark_no_trade`) and :meth:`MarketStore.coverage_gaps` counts it covered.

        UPSTREAM-CONFIRMED ONLY — never a synthetic bar or invented price. ``bars_1m`` is untouched
        by this path, so every feature input is unchanged: only the warm-up gate's verdict moves.

        Confirmation runs in TWO PASSES because the last guard is cross-symbol: the collect pass
        (inside the per-symbol loop) only collects candidates; the write pass (after the loop)
        applies the correlation guard and writes. A minute must clear all four guards:

        * **A — Kite answered.** The symbol's ``returned`` set (every in-span minute Kite published,
          across chunks) must be NON-EMPTY. If Kite returned no candles at all for the span, a broker
          outage and a genuinely dead symbol are indistinguishable — confirm nothing, leave the hole.
        * **B — Kite reached past the minute.** ``m < max(returned)``. A Kite-side outage that
          truncates a day must not let the rest be confirmed wholesale; a tradeless minute at the
          END of the window waits for a later pass.
        * **C — published.** ``m < min(confirm_until, now - 2 min)``: a minute Kite has not
          published YET must never be confirmed.
        * **D — uncorrelated.** Across the symbols that answered, a minute missing for
          ``max(3, ceil(0.05 × answered))`` or more of them is a FEED gap, not thin symbols going
          quiet together — dropped for every symbol and counted in
          ``report.no_trade_correlated_skipped``.

        A token-rejection abort discards the whole pending set: the sweep stopped early, so the
        correlation denominator is not the one the guard was calibrated on.

        ``coverage_gaps`` then subtracts confirmed minutes, so the NEXT ``warmup_gap`` makes no Kite
        request for that gap. ``confirm_until=None`` (the default) confirms nothing and writes no
        ``bars_1m_no_trade`` row.
        """
        frm = frm.astimezone(IST)
        to = to.astimezone(IST)
        report = BackfillReport(interval="minute")
        chunk_days = self._chunk_days("minute")
        symbols = list(symbols)
        # ``cutoff`` is taken ONCE, before the sweep: a long sweep must not confirm later minutes
        # for the symbols it happens to reach last.
        cutoff: datetime | None = (
            None if confirm_until is None
            else min(confirm_until.astimezone(IST), self._clock.now().astimezone(IST)
                     - timedelta(minutes=2))
        )
        pending: dict[str, list[datetime]] = {}
        answered = 0                        # symbols Kite returned ≥1 candle for — guard D's denominator
        aborted = False
        for i, symbol in enumerate(symbols):
            report.requested.append(
                BackfillSpan(symbol=symbol, frm=frm.isoformat(), to=to.isoformat())
            )
            token = self._token_for_symbol(symbol)
            if token is None:
                report.failed.append(
                    BackfillSpan(
                        symbol=symbol, frm=frm.isoformat(), to=to.isoformat(),
                        error=_UNKNOWN_TOKEN,
                    )
                )
                _log.warning("warmup_gap_unknown_token", symbol=symbol)
                continue
            # Fill GAPS ONLY — never overwrite an existing bar of ANY src (it would destroy the
            # day's reconcile evidence). Coverage gaps also handle INTERIOR holes, which no single
            # last-bar-seen ``frm`` can express.
            gap_minutes = set(await self._store.acoverage_gaps(symbol, frm, to))
            if not gap_minutes:
                continue      # fully covered — nothing to fill, nothing to touch
            written = 0
            failed = False
            returned: set[datetime] = set()      # every in-span minute Kite DID publish (all chunks)
            cur = frm
            while cur < to:
                chunk_to = min(cur + timedelta(days=chunk_days), to)
                try:
                    candles = await self._kite.historical(token, cur, chunk_to, "minute")
                    returned.update(ts for ts in map(_candle_ts, candles) if frm <= ts < to)
                    written += await self._write_candles(
                        symbol, "minute", candles, src="gap_backfilled", frm=frm, to=to,
                        only_minutes=gap_minutes,
                    )
                except Exception as exc:  # noqa: BLE001 - a symbol's gap failure never blocks others
                    report.failed.append(
                        BackfillSpan(
                            symbol=symbol, frm=cur.isoformat(), to=chunk_to.isoformat(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    if isinstance(exc, TokenException):
                        # Abort the whole fill; the post-login re-trigger recomputes the gap.
                        _abort_remaining(
                            report, symbols[i + 1:], frm.isoformat(), to.isoformat(),
                            "warmup_gap_aborted_token_rejected",
                        )
                        aborted = True
                    else:
                        _log.warning("warmup_gap_chunk_failed", symbol=symbol, error=str(exc))
                    failed = True
                    break
                cur = chunk_to
            if not failed:
                report.fetched.append(
                    BackfillSpan(symbol=symbol, frm=frm.isoformat(), to=to.isoformat(), bars=written)
                )
                report.bars_written += written
                # COLLECT PASS — no writes. Guard A gates candidates and the correlation
                # denominator; guards B and C filter the candidates.
                if cutoff is not None and returned:
                    answered += 1
                    latest = max(returned)
                    candidates = [
                        m for m in gap_minutes
                        if m not in returned and m < latest and m < cutoff
                    ]
                    if candidates:
                        pending[symbol] = candidates
            if aborted:
                break
        # WRITE PASS — guard D and the writes. An aborted sweep confirms NOTHING.
        if pending and not aborted:
            miss_count: dict[datetime, int] = {}
            for mins in pending.values():
                for m in mins:
                    miss_count[m] = miss_count.get(m, 0) + 1
            threshold = max(3, math.ceil(0.05 * answered))
            correlated = {m for m, n in miss_count.items() if n >= threshold}
            if correlated:
                dropped = 0
                affected = 0
                for sym, mins in list(pending.items()):
                    keep = [m for m in mins if m not in correlated]
                    if len(keep) != len(mins):
                        affected += 1
                        dropped += len(mins) - len(keep)
                    if keep:
                        pending[sym] = keep
                    else:
                        del pending[sym]
                report.no_trade_correlated_skipped += dropped
                _log.warning(
                    "warmup_gap_no_trade_correlated",
                    minutes=[m.isoformat() for m in sorted(correlated)[:6]],
                    count=len(correlated), symbols_affected=affected, threshold=threshold,
                )
            for sym, mins in pending.items():
                confirmed = sorted(mins)
                await self._store.amark_no_trade(sym, confirmed, confirmed_at=self._clock.now())
                report.no_trade_confirmed += len(confirmed)
                _log.info(
                    "warmup_gap_no_trade_confirmed", symbol=sym, count=len(confirmed),
                    minutes=[m.isoformat() for m in confirmed[:4]],
                )
        _log.info(
            "warmup_gap_done", symbols=len(symbols), frm=frm.isoformat(), to=to.isoformat(),
            bars_written=report.bars_written, no_trade_confirmed=report.no_trade_confirmed,
            no_trade_correlated_skipped=report.no_trade_correlated_skipped,
            failed=len(report.failed),
        )
        return report

    # ------------------------------------------------------------------ daily gap

    async def daily_gap(self, symbols: Sequence[str], sessions: Sequence[date]) -> BackfillReport:
        """Fill missing ``bars_1d`` rows for ``symbols`` on exactly ``sessions`` — the warm-up gate's
        own daily window (:meth:`engine.ops.warmup.WarmupGate.daily_window`), newest-first or any
        order — from official Kite day candles.

        Coverage is checked from the store FIRST, per symbol, with NO network call: a symbol already
        holding every requested session costs nothing (``report.skipped_covered``). Written rows are
        filtered to exactly the missing dates (:meth:`_write_candles`'s ``only_dates``) so a covered
        session's existing row — which may be a ``src='bhavcopy'`` cross-check row — is never
        clobbered with ``kite_official``.

        NOT checkpointed: coverage is recomputed from the store on each call, so a fill is
        idempotent and self-healing. A monotonic checkpoint would skip exactly the BACKWARD holes
        this repairs.
        """
        report = BackfillReport(interval="day")
        sessions = list(sessions)
        if not sessions:
            _log.info("daily_gap_no_sessions")
            return report
        oldest, newest = min(sessions), max(sessions)
        symbols = list(symbols)
        chunk_days = self._chunk_days("day")
        for i, symbol in enumerate(symbols):
            report.requested.append(
                BackfillSpan(symbol=symbol, frm=oldest.isoformat(), to=newest.isoformat())
            )
            present = {b.d for b in await self._store.aget_bars_1d(symbol, oldest, newest)}
            missing = {d for d in sessions if d not in present}
            if not missing:
                report.skipped_covered += 1
                continue      # fully covered — nothing to fill, no Kite request at all
            token = self._token_for_symbol(symbol)
            if token is None:
                report.failed.append(
                    BackfillSpan(
                        symbol=symbol, frm=oldest.isoformat(), to=newest.isoformat(),
                        error=_UNKNOWN_TOKEN,
                    )
                )
                _log.warning("daily_gap_unknown_token", symbol=symbol)
                continue
            written = 0
            failed = False
            aborted = False
            cur = oldest
            while cur <= newest:
                chunk_end = min(cur + timedelta(days=chunk_days - 1), newest)
                try:
                    candles = await self._kite.historical(
                        token,
                        self._clock.combine(cur, time(0, 0)),
                        self._clock.combine(chunk_end, time(23, 59, 59)),
                        "day",
                    )
                    written += await self._write_candles(
                        symbol, "day", candles, src="kite_official", only_dates=missing
                    )
                except Exception as exc:  # noqa: BLE001 - a symbol's gap failure never blocks others
                    report.failed.append(
                        BackfillSpan(
                            symbol=symbol, frm=cur.isoformat(), to=chunk_end.isoformat(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    if isinstance(exc, TokenException):
                        _abort_remaining(
                            report, symbols[i + 1:], oldest.isoformat(), newest.isoformat(),
                            "daily_gap_aborted_token_rejected",
                        )
                        aborted = True
                    else:
                        _log.warning("daily_gap_chunk_failed", symbol=symbol, error=str(exc))
                    failed = True
                    break
                cur = chunk_end + timedelta(days=1)
            if not failed:
                report.fetched.append(
                    BackfillSpan(
                        symbol=symbol, frm=oldest.isoformat(), to=newest.isoformat(), bars=written
                    )
                )
                report.bars_written += written
            if aborted:
                break
        _log.info(
            "daily_gap_done", symbols=len(symbols), sessions=len(sessions),
            oldest=oldest.isoformat(), newest=newest.isoformat(),
            bars_written=report.bars_written, skipped_covered=report.skipped_covered,
            failed=len(report.failed),
        )
        return report

    # ------------------------------------------------------------------ internals

    def _chunk_days(self, interval: str) -> int:
        """Per-request span: the settings knob clamped to the pinned Kite cap (never above it)."""
        cfg = self._settings.backfill
        if interval == "day":
            return max(1, min(int(cfg.day_chunk_days), KITE_DAY_CHUNK_DAYS))
        return max(1, min(int(cfg.minute_chunk_days), KITE_MINUTE_CHUNK_DAYS))

    async def _write_candles(
        self,
        symbol: str,
        interval: str,
        candles: Sequence[Any],
        *,
        src: str,
        frm: datetime | None = None,
        to: datetime | None = None,
        only_minutes: set | None = None,
        only_dates: set[date] | None = None,
    ) -> int:
        """Write fetched candles (as-is) to bars_1d / bars_1m; optional ``[frm, to)`` filter.

        ``only_minutes`` (minute interval only): write ONLY candles whose ts is in the set — the
        warmup-gap fill-gaps-never-overwrite contract (see :meth:`warmup_gap`).

        ``only_dates`` (day interval only): write ONLY candles on those dates — the daily gap fill's
        fill-gaps-never-overwrite contract (bars_1d rows may be ``src='bhavcopy'`` cross-check rows;
        a gap fill must not clobber them with ``kite_official``)."""
        if not candles:
            return 0
        if interval == "day":
            day_candles = candles
            if only_dates is not None:
                day_candles = [c for c in candles if _candle_ts(c).date() in only_dates]
            rows = [
                DailyBar(
                    symbol=symbol, d=_candle_ts(c).date(),
                    open=_dec(_candle_field(c, "open")), high=_dec(_candle_field(c, "high")),
                    low=_dec(_candle_field(c, "low")), close=_dec(_candle_field(c, "close")),
                    volume=int(_candle_field(c, "volume")), src="kite_official",
                )
                for c in day_candles
            ]
            return await self._store.aupsert_bars_1d(rows)
        bar_src: BarSrc = "gap_backfilled" if src == "gap_backfilled" else "kite_official"
        bars: list[Bar] = []
        for c in candles:
            ts = _candle_ts(c)
            if frm is not None and ts < frm:
                continue
            if to is not None and ts >= to:
                continue
            if only_minutes is not None and ts not in only_minutes:
                continue      # existing bar (any src) — never overwritten by a gap fill
            bars.append(
                Bar(
                    symbol=symbol, ts_minute=ts,
                    open=_dec(_candle_field(c, "open")), high=_dec(_candle_field(c, "high")),
                    low=_dec(_candle_field(c, "low")), close=_dec(_candle_field(c, "close")),
                    volume=int(_candle_field(c, "volume")), src=bar_src,
                )
            )
        if not bars:
            return 0
        return await self._store.ainsert_bars_1m(bars)

    # ---- SQLite checkpoints (``backfill_checkpoints``; resumable) ----

    def _checkpoint(self, symbol: str, interval: str) -> date | None:
        row = self._conn.execute(
            "SELECT through_date FROM backfill_checkpoints WHERE symbol = ? AND interval = ?",
            (symbol, interval),
        ).fetchone()
        if row is None or row["through_date"] is None:
            return None
        return date.fromisoformat(row["through_date"])

    def _advance_checkpoint(self, symbol: str, interval: str, through: date) -> None:
        """Monotonically advance the checkpoint (MAX keeps it from ever moving backwards —
        ISO date strings compare correctly as TEXT)."""
        self._conn.execute(
            "INSERT INTO backfill_checkpoints (symbol, interval, through_date) VALUES (?,?,?) "
            "ON CONFLICT(symbol, interval) DO UPDATE SET "
            "through_date = MAX(through_date, excluded.through_date)",
            (symbol, interval, through.isoformat()),
        )
