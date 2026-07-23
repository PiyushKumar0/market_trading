"""Historical candle backfill + startup warm-up gap fill (§3.2.3, §4.4 jobs 1 & 3, §2.6 step 4, A2).

``BackfillJob`` pulls official Kite candles through :meth:`KiteClient.historical` — every request is
paced by the shared :class:`~engine.broker.rate_limiter.RateLimiter` ``historical`` bucket (≤3 req/s,
A2) inside the client, so this job never sleeps or paces on its own. Requests are **chunked** per
Kite's per-request range caps (pinned constants below), **checkpointed** per ``(symbol, interval)``
in the SQLite ``backfill_checkpoints`` table, and **resumable**: a re-run skips everything at or
behind the checkpoint and continues from the day after it (A2 — the initial minute history is a
multi-evening job).

**A11 RESOLVED — NO re-adjustment:** Kite minute (and daily) candles are ALREADY corp-action
adjusted (``settings data.minute_candles_adjusted=true``; verified 2026-07-05 against the BSE 2:1
bonus via ``scripts/a11_check.py``). Candles are therefore written EXACTLY as fetched — this job
must never apply split/bonus factors across ex-dates.

Provenance (§4.3 ``bars_1m.src``):
    * :meth:`run` writes ``src='kite_official'`` (canonical historical rows; §4.4 jobs 2/3).
    * :meth:`warmup_gap` writes ``src='gap_backfilled'`` — the §2.6 offline-span fill, which the
      nightly reconcile EXCLUDES from its drift denominator (they are not self-built bars).

Dependencies: ``core`` + ``broker`` (§3.2.3). The SQLite checkpoint table lives in ``state.db``
(§4.2); the bars land in DuckDB via the single-writer :class:`MarketStore`.
"""

from __future__ import annotations

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

# --- Kite historical per-request range caps (A2/§3.2.3, pinned) --------------------------------
# These are BROKER limits, not tunables: a request spanning more days than this is rejected by the
# API. settings.backfill.{minute,day}_chunk_days may tighten them but can never exceed them.
KITE_MINUTE_CHUNK_DAYS = 60     # max span per minute-interval historical request
KITE_DAY_CHUNK_DAYS = 2000      # max span per day-interval historical request


class BackfillSpan(BaseModel):
    """One per-symbol span in a :class:`BackfillReport` (ISO strings; a report, not a query key)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    frm: str
    to: str
    bars: int = 0
    error: str | None = None


class BackfillReport(BaseModel):
    """Outcome of one backfill run (§3.2.3): requested/fetched/failed spans + bars written.

    ``requested`` is the caller-asked span per symbol; ``fetched`` the chunk spans actually pulled
    (a resumed symbol shows only the remainder); ``failed`` the spans abandoned with their error —
    those symbols stay behind their checkpoint and the next run resumes them (A2 resumable).
    """

    interval: str
    requested: list[BackfillSpan] = Field(default_factory=list)
    fetched: list[BackfillSpan] = Field(default_factory=list)
    failed: list[BackfillSpan] = Field(default_factory=list)
    bars_written: int = 0


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


class BackfillJob:
    """Chunked, checkpointed, resumable official-candle backfill (§3.2.3, A2/A11).

    Parameters
    ----------
    store:
        Single-writer :class:`MarketStore` (bars_1m upserts / bars_1d upserts).
    kite:
        :class:`KiteClient` — ALL pacing happens inside it (the shared RateLimiter ``historical``
        bucket, 3 req/s, A2). This job issues requests back-to-back and lets the limiter pace.
    clock:
        The single source of "now"/tz-aware combination (§3.2).
    settings:
        ``settings.backfill`` chunk knobs (clamped to the pinned Kite caps above).
    conn:
        The SQLite ``state.db`` connection (WAL; §4.2) holding ``backfill_checkpoints``.
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

    # ------------------------------------------------------------------ §4.4 job 3: history

    async def run(
        self, symbols: Sequence[str], interval: str, start: date, end: date
    ) -> BackfillReport:
        """Backfill ``[start, end]`` (dates inclusive) of ``interval`` candles for ``symbols``.

        ``interval`` is the Kite interval string (``"minute"`` | ``"day"``). Chunked per the pinned
        range caps; each successfully written chunk advances the ``(symbol, interval)`` checkpoint
        so an interrupted run resumes exactly where it stopped (A2). Rows are written
        ``src='kite_official'`` — already corp-action adjusted, NO re-adjustment (A11, module
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
                        error="unknown_instrument_token",
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
                except Exception as exc:  # noqa: BLE001 - record + resume next run (A2)
                    report.failed.append(
                        BackfillSpan(
                            symbol=symbol, frm=cur.isoformat(), to=chunk_end.isoformat(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    if isinstance(exc, TokenException):
                        # 2026-07-21: a rejected token fails EVERY subsequent historical call
                        # identically — abort the whole run instead of hammering the broker (and
                        # spamming a per-symbol warning) once per remaining symbol. The KiteClient
                        # on_token_rejected circuit breaker has already frozen entries; the
                        # un-attempted symbols are reported failed so the caller sees the full
                        # picture and the resume (via PostLoginRecovery) refills them.
                        remaining = symbols[i + 1:]
                        for rem in remaining:
                            report.failed.append(
                                BackfillSpan(
                                    symbol=rem, frm=start.isoformat(), to=end.isoformat(),
                                    error="aborted_token_rejected",
                                )
                            )
                        _log.warning(
                            "backfill_aborted_token_rejected", interval=interval,
                            symbols_remaining=len(remaining),
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
                self._advance_checkpoint(symbol, interval, chunk_end)
                cur = chunk_end + timedelta(days=1)
            if aborted:
                break
        _log.info(
            "backfill_run_done", interval=interval, symbols=len(symbols),
            bars_written=report.bars_written, failed=len(report.failed),
        )
        return report

    # ------------------------------------------------------------------ §2.6 step 4: warm-up gap

    async def warmup_gap(
        self, symbols: Sequence[str], frm: datetime, to: datetime
    ) -> BackfillReport:
        """Fill the ``[frm, to)`` minute-bar gap from official candles (§2.6 step 4 / §4.4 job 1).

        The cold-start/restart same-session (or multi-day) gap fill: ``frm`` is last-bar-seen, so
        warm-up never requires live tick capture since 09:15 — the reference catch-up pattern.
        Rows are written ``src='gap_backfilled'`` (§4.3): an offline-span fill that the nightly
        reconcile EXCLUDES from its drift denominator (§2.6). Candles are already corp-action
        adjusted (A11) — written as fetched. NOT checkpointed: every startup computes its own gap.
        """
        frm = frm.astimezone(IST)
        to = to.astimezone(IST)
        report = BackfillReport(interval="minute")
        chunk_days = self._chunk_days("minute")
        symbols = list(symbols)
        for i, symbol in enumerate(symbols):
            report.requested.append(
                BackfillSpan(symbol=symbol, frm=frm.isoformat(), to=to.isoformat())
            )
            token = self._token_for_symbol(symbol)
            if token is None:
                report.failed.append(
                    BackfillSpan(
                        symbol=symbol, frm=frm.isoformat(), to=to.isoformat(),
                        error="unknown_instrument_token",
                    )
                )
                _log.warning("warmup_gap_unknown_token", symbol=symbol)
                continue
            # Fill GAPS ONLY — never overwrite an existing bar of ANY src (2026-07-23: a mid-session
            # restart passed frm=09:15 and the fill CLOBBERED the morning's live self-built bars with
            # official candles, destroying the day's reconcile evidence — 1,311 of ~9,500 self bars
            # survived). Coverage gaps also handle INTERIOR holes (a sleep window between two live
            # stretches), which no single last-bar-seen ``frm`` can express.
            gap_minutes = set(await self._store.acoverage_gaps(symbol, frm, to))
            if not gap_minutes:
                continue      # fully covered — nothing to fill, nothing to touch
            written = 0
            failed = False
            aborted = False
            cur = frm
            while cur < to:
                chunk_to = min(cur + timedelta(days=chunk_days), to)
                try:
                    candles = await self._kite.historical(token, cur, chunk_to, "minute")
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
                        # 2026-07-21: a rejected token fails every gap call identically — abort the
                        # whole warm-up fill (the circuit breaker already froze entries) rather than
                        # retry per remaining symbol. Un-attempted symbols reported failed; the
                        # post-login re-trigger recomputes and refills the gap once the token is good.
                        for rem in symbols[i + 1:]:
                            report.failed.append(
                                BackfillSpan(
                                    symbol=rem, frm=frm.isoformat(), to=to.isoformat(),
                                    error="aborted_token_rejected",
                                )
                            )
                        _log.warning(
                            "warmup_gap_aborted_token_rejected",
                            symbols_remaining=len(symbols) - i - 1,
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
            if aborted:
                break
        _log.info(
            "warmup_gap_done", symbols=len(symbols), frm=frm.isoformat(), to=to.isoformat(),
            bars_written=report.bars_written, failed=len(report.failed),
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
    ) -> int:
        """Write fetched candles (as-is, A11) to bars_1d / bars_1m; optional ``[frm, to)`` filter.

        ``only_minutes`` (minute interval only): write ONLY candles whose ts is in the set — the
        warmup-gap fill-gaps-never-overwrite contract (2026-07-23; see :meth:`warmup_gap`)."""
        if not candles:
            return 0
        if interval == "day":
            rows = [
                DailyBar(
                    symbol=symbol, d=_candle_ts(c).date(),
                    open=_dec(_candle_field(c, "open")), high=_dec(_candle_field(c, "high")),
                    low=_dec(_candle_field(c, "low")), close=_dec(_candle_field(c, "close")),
                    volume=int(_candle_field(c, "volume")), src="kite_official",
                )
                for c in candles
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

    # ---- SQLite checkpoints (§4.2 ``backfill_checkpoints``; A2 resumable) ----

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
