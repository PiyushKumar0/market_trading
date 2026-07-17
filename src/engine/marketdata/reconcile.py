"""Daily self-built-vs-official 1-minute bar reconciliation (§3.2.3, §4.4 job 2, A13, §2.6).

Nightly (15:50) — and as the §2.6 startup catch-up for ANY past trading day lacking a
``reconcile_log`` entry (``MarketStore.has_reconcile_entry`` is the per-day checkpoint; the
scheduler/CatchUpRunner enumerates the missing days and calls :meth:`ReconcileJob.run` per day).

For each actively-ticked symbol of day ``d``:

* Fetch the official Kite 1m candles for the session (paced ≤3 req/s by the shared RateLimiter
  inside :class:`KiteClient`, A2).
* **Official candles become the canonical ``bars_1m`` rows** (``src='kite_official'``) wherever
  they exist (§4.4 job 2); self-built bars remain only where official has no candle ("self-built
  fill gaps"). The ``auction_open`` stamped on the self 09:15 row (A14) is carried over onto the
  canonical row — the official candle does not know it.
* **Drift** is measured ONLY over minutes where a ``src='self'`` bar exists to compare
  (the *compared* set): a bar drifts when ``|Δvolume| > reconcile.vol_drift_pct`` of the official
  volume OR ``|Δclose| > reconcile.close_drift_ticks × tick_size`` (both strict ``>``; thresholds
  are ``settings.reconcile`` [tunable]). The day flags a symbol when its bad-bar fraction exceeds
  ``reconcile.max_bad_bar_fraction`` (strict ``>`` — "on >1% of bars").
* **OFFLINE spans are NOT drift (§2.6):** an official minute with no self-built bar (the engine was
  off, or the row is already ``gap_backfilled``/official) is backfilled from the official candle
  and counted in ``offline_bars`` — EXCLUDED from the drift denominator, never a failure.
* Results land in ``reconcile_log`` (per (d, symbol) — the catch-up checkpoint) and a
  ``reconcile_drift`` catalog alert goes to the injected async ``notify`` sink when any symbol
  flags (A13 "alert on drift").

Dependencies: ``core`` + ``broker`` (+ the transport-free ``notify.catalog`` shapes, same as the
datafeeds jobs). All DuckDB access goes through the single-writer :class:`MarketStore`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.broker.kite_client import KiteClient
from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.core.types import Bar
from engine.marketdata.store import MarketStore
from engine.notify.catalog import CatalogMessage
from engine.notify.catalog import reconcile_drift as _reconcile_drift_msg

_log = get_logger("engine.marketdata.reconcile")

NotifySink = Callable[[CatalogMessage], Awaitable[None]]

#: Fallback tick size when no per-instrument resolver is injected (NSE minimum tick, A10). The
#: composition root wires ``InstrumentStore``-backed resolution; the fallback only loosens the
#: close-drift check for higher-band instruments (never tightens it).
DEFAULT_TICK_SIZE = Decimal("0.05")


class SymbolReconcile(BaseModel):
    """Per-symbol reconcile outcome for day ``d`` (mirrors a ``reconcile_log`` row)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    bars_self: int
    bars_official: int
    bars_compared: int
    vol_drift_bars: int
    close_drift_bars: int
    offline_bars: int
    bad_bar_fraction: float
    flagged: bool


class ReconcileReport(BaseModel):
    """Outcome of one :meth:`ReconcileJob.run` (§3.2.3)."""

    d: date
    symbols: list[SymbolReconcile] = Field(default_factory=list)
    bars_compared: int = 0
    offline_bars: int = 0
    symbols_flagged: list[str] = Field(default_factory=list)
    bad_bar_fraction: float = 0.0        # aggregate: total bad / total compared
    alerted: bool = False


class ReconcileJob:
    """Self-vs-official 1m reconciliation + offline-span fill (§4.4 job 2; module docstring).

    Parameters
    ----------
    store / kite / clock / settings:
        The usual seams; thresholds come from ``settings.reconcile`` [tunable].
    token_for_symbol:
        tradingsymbol → instrument_token resolver. A symbol with no token gets an empty official
        set (logged) — its row still lands in ``reconcile_log`` with ``bars_official=0``.
    tick_size_for:
        Optional per-symbol tick size (A10 price-banded); defaults to ``DEFAULT_TICK_SIZE``.
    notify:
        Injected async alert sink (``CatalogMessage`` consumer, e.g. ``TelegramBot.send``);
        ``None`` disables alerting. A notify failure is logged, never raised (the reconcile result
        is already persisted).
    session_open / session_close:
        The session bounds used for the official fetch (defaults: regular 09:15–15:30; the
        composition root passes the day's NSECalendar session for special days).
    """

    def __init__(
        self,
        store: MarketStore,
        kite: KiteClient,
        clock: Clock,
        settings: Settings,
        *,
        token_for_symbol: Callable[[str], int | None],
        tick_size_for: Callable[[str], Decimal] | None = None,
        notify: NotifySink | None = None,
        session_open: time = time(9, 15),
        session_close: time = time(15, 30),
    ) -> None:
        self._store = store
        self._kite = kite
        self._clock = clock
        self._settings = settings
        self._token_for_symbol = token_for_symbol
        self._tick_size_for = tick_size_for
        self._notify = notify
        self._session_open = session_open
        self._session_close = session_close

    # ------------------------------------------------------------------ per-day entry point

    async def run(self, d: date, symbols: Sequence[str] | None = None) -> ReconcileReport:
        """Reconcile day ``d`` (idempotent — safe to re-run; §2.6 catch-up calls this per missing day).

        ``symbols`` defaults to every symbol holding a ``bars_1m`` row on ``d`` (the
        actively-ticked set, plus anything already backfilled for the day).
        """
        if symbols is None:
            symbols = await self._store.arun(self._symbols_for_day, d)
        cfg = self._settings.reconcile
        vol_pct = Decimal(str(cfg.vol_drift_pct))
        max_bad = float(cfg.max_bad_bar_fraction)

        report = ReconcileReport(d=d)
        log_rows: list[dict[str, Any]] = []
        total_bad = 0
        day_start = self._clock.combine(d, time(0, 0))
        day_end = self._clock.combine(d + timedelta(days=1), time(0, 0))
        open_minute = self._clock.combine(d, self._session_open)

        for symbol in sorted(set(symbols)):
            existing = await self._store.aget_bars_1m(symbol, day_start, day_end)
            official = await self._fetch_official(symbol, d)

            by_minute: dict[datetime, Bar] = {b.ts_minute: b for b in existing}
            self_bars = {m: b for m, b in by_minute.items() if b.src == "self"}
            tick_size = (
                self._tick_size_for(symbol) if self._tick_size_for is not None else DEFAULT_TICK_SIZE
            )
            close_threshold = tick_size * cfg.close_drift_ticks

            compared = vol_drift = close_drift = bad = offline = 0
            canonical: list[Bar] = []
            for minute in sorted(official):
                oc = official[minute]
                mine = self_bars.get(minute)
                if mine is None:
                    # §2.6: no self-built bar ⇒ offline span (or already gap-backfilled/official) —
                    # fill from official, EXCLUDE from the drift denominator.
                    offline += 1
                else:
                    compared += 1
                    v_bad = (
                        abs(mine.volume - oc.volume) * Decimal(100) > vol_pct * oc.volume
                    )
                    c_bad = abs(mine.close - oc.close) > close_threshold
                    vol_drift += int(v_bad)
                    close_drift += int(c_bad)
                    bad += int(v_bad or c_bad)
                # Official becomes canonical everywhere it exists (§4.4 job 2). Preserve the A14
                # auction_open stamped on the (self-built) session-open row.
                prev = by_minute.get(minute)
                canonical.append(
                    oc.model_copy(
                        update={
                            "auction_open": prev.auction_open
                            if prev is not None and minute == open_minute
                            else None
                        }
                    )
                )

            if canonical:
                await self._store.ainsert_bars_1m(canonical)

            fraction = (bad / compared) if compared else 0.0
            flagged = compared > 0 and fraction > max_bad
            total_bad += bad
            report.bars_compared += compared
            report.offline_bars += offline
            report.symbols.append(
                SymbolReconcile(
                    symbol=symbol, bars_self=len(self_bars), bars_official=len(official),
                    bars_compared=compared, vol_drift_bars=vol_drift,
                    close_drift_bars=close_drift, offline_bars=offline,
                    bad_bar_fraction=fraction, flagged=flagged,
                )
            )
            if flagged:
                report.symbols_flagged.append(symbol)
            log_rows.append(
                {
                    "d": d, "symbol": symbol, "bars_self": len(self_bars),
                    "bars_official": len(official), "bars_compared": compared,
                    "vol_drift_bars": vol_drift, "close_drift_bars": close_drift,
                    "offline_bars": offline, "bad_bar_fraction": fraction,
                    "alerted": flagged,
                }
            )

        report.bad_bar_fraction = (total_bad / report.bars_compared) if report.bars_compared else 0.0
        report.alerted = bool(report.symbols_flagged)
        await self._store.arun(self._store.append_reconcile_log, log_rows)

        if report.alerted:
            _log.warning(
                "reconcile_drift", d=d.isoformat(), symbols_flagged=report.symbols_flagged,
                bad_bar_fraction=report.bad_bar_fraction,
            )
            await self._send_alert(report, max_bad)
        _log.info(
            "reconcile_done", d=d.isoformat(), symbols=len(report.symbols),
            bars_compared=report.bars_compared, offline_bars=report.offline_bars,
            flagged=len(report.symbols_flagged),
        )
        return report

    # ------------------------------------------------------------------ internals

    def _symbols_for_day(self, d: date) -> list[str]:
        """Symbols holding any bars_1m row on ``d`` — the default actively-ticked set.

        Package-internal read through the single-writer store's own connection (convention 12:
        only ``MarketStore`` opens ``market.duckdb``; this never opens a second handle).
        """
        rows = self._store._fetchall(  # noqa: SLF001 - in-package consumer (§3.2.3 / store module doc)
            "SELECT DISTINCT symbol FROM bars_1m WHERE CAST(ts_minute AS DATE) = ? ORDER BY symbol",
            [d],
        )
        return [r[0] for r in rows]

    async def _fetch_official(self, symbol: str, d: date) -> dict[datetime, Bar]:
        """Official 1m candles for the session of ``d`` keyed by minute (``src='kite_official'``)."""
        token = self._token_for_symbol(symbol)
        if token is None:
            _log.warning("reconcile_unknown_token", symbol=symbol)
            return {}
        candles = await self._kite.historical(
            token,
            self._clock.combine(d, self._session_open),
            self._clock.combine(d, self._session_close),
            "minute",
        )
        out: dict[datetime, Bar] = {}
        for c in candles or []:
            ts = self._candle_ts(c)
            out[ts] = Bar(
                symbol=symbol, ts_minute=ts,
                open=self._dec(self._field(c, "open")), high=self._dec(self._field(c, "high")),
                low=self._dec(self._field(c, "low")), close=self._dec(self._field(c, "close")),
                volume=int(self._field(c, "volume")), src="kite_official",
            )
        return out

    async def _send_alert(self, report: ReconcileReport, max_bad: float) -> None:
        if self._notify is None:
            return
        msg = _reconcile_drift_msg(
            d=report.d.isoformat(),
            symbols_flagged=list(report.symbols_flagged),
            bars_compared=report.bars_compared,
            bad_bar_fraction=report.bad_bar_fraction,
            max_bad_bar_fraction=max_bad,
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - alerting must never fail the (already-persisted) job
            _log.exception("reconcile_notify_failed", d=report.d.isoformat())

    @staticmethod
    def _field(candle: Any, name: str) -> Any:
        return candle[name] if isinstance(candle, dict) else getattr(candle, name)

    @staticmethod
    def _dec(value: Any) -> Decimal:
        return Decimal(str(value))

    def _candle_ts(self, candle: Any) -> datetime:
        value = self._field(candle, "date")
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)
