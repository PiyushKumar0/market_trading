"""Daily self-built-vs-official 1-minute bar reconciliation (§3.2.3, §4.4 job 2, A13, §2.6).

Nightly (15:50) — and as the §2.6 startup catch-up for past trading days, driven by the
``job_runs`` DATE_KEYED watermark of ``JOB_RECONCILE`` (the scheduler/CatchUpRunner calls
:meth:`ReconcileJob.run` per missing day). ``reconcile_log`` is the per-(day, symbol) audit trail
AND the per-(day, symbol) resume checkpoint: a run skips every symbol already logged for ``d`` with
official bars. A logged symbol's bars are already canonical official, so re-comparing it finds no self
bars and would overwrite its drift row with ``bars_compared=0`` (2026-09-15: 119 symbols after a
mid-run 502; 08-28, 10-05, 10-08: every symbol, the 15:50 fire racing the catch-up sweep). A row with
``bars_official=0`` converted nothing, so it is no checkpoint: the next run of ``d`` fetches it again.
Runs are serialized per job, so a concurrent caller waits and then finds the day logged.
``MarketStore.has_reconcile_entry`` / ``reconciled_days`` have no production caller.

Each symbol's canonical bars and its log row commit in ONE store transaction
(``MarketStore.commit_reconciled_symbol``), so no exception, cancel, store close or kill can leave a
symbol official but unlogged. A row's ``alerted`` is set only after the drift alert naming it was
handed to the notify sink; a run that dies first (or whose send fails) leaves flagged rows
``alerted=False``, and the next run of ``d`` carries them into its alert (``symbols_alert_carried``).

The report is ok-bearing (E5): a run stopped by a rejected token reports ``ok=False``
(``token_rejected``), so the DATE_KEYED watermark records the day ``failed`` and the §2.6 sweep
retries it after re-login, resuming only the unlogged symbols.

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
* Results land in ``reconcile_log`` (per (d, symbol) — the audit trail) and a
  ``reconcile_drift`` catalog alert goes to the injected async ``notify`` sink when any symbol
  flags (A13 "alert on drift").

Dependencies: ``core`` + ``broker`` (+ the transport-free ``notify.catalog`` shapes, same as the
datafeeds jobs). All DuckDB access goes through the single-writer :class:`MarketStore`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from kiteconnect.exceptions import TokenException
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
    """Outcome of one :meth:`ReconcileJob.run` (§3.2.3). Totals cover THIS run's symbols only.

    ``ok`` is the watermark verdict (``_job_result_ok``): False only when the run stopped before
    attempting every unlogged symbol — ``aborted_reason`` is ``'token_rejected'`` (a fetch raised
    ``TokenException``) or ``'no_valid_token'`` (the composition-root gate; no broker call, no
    store read, so ``symbols_remaining`` stays 0). Any other failure raises instead.
    """

    d: date
    symbols: list[SymbolReconcile] = Field(default_factory=list)
    bars_compared: int = 0
    offline_bars: int = 0
    symbols_flagged: list[str] = Field(default_factory=list)
    bad_bar_fraction: float = 0.0        # aggregate: total bad / total compared
    #: Flagged symbols an EARLIER run of ``d`` logged but never alerted, included in this run's alert.
    symbols_alert_carried: list[str] = Field(default_factory=list)
    alerted: bool = False                # a drift alert went to the notify sink and its rows are marked
    ok: bool = True
    aborted_reason: str | None = None
    symbols_remaining: int = 0           # unlogged symbols left unattempted by a token abort
    symbols_already_done: int = 0        # requested symbols already logged with official bars, skipped


class ReconcileJob:
    """Self-vs-official 1m reconciliation + offline-span fill (§4.4 job 2; module docstring).

    Parameters
    ----------
    store / kite / clock / settings:
        The usual seams; thresholds come from ``settings.reconcile`` [tunable].
    token_for_symbol:
        tradingsymbol → instrument_token resolver. A symbol with no token gets an empty official
        set (logged) — its row still lands in ``reconcile_log`` with ``bars_official=0``, which the
        next run of the day retries.
    tick_size_for:
        Optional per-symbol tick size (A10 price-banded); defaults to ``DEFAULT_TICK_SIZE``.
    notify:
        Injected async alert sink (``CatalogMessage`` consumer, e.g. ``TelegramBot.send``);
        ``None`` disables alerting. A notify failure is logged, never raised (the reconcile result
        is already persisted).
    session_open / session_close:
        The session bounds used for the official fetch (defaults: regular 09:15–15:30). The
        composition root passes neither: regular-session constants; the live calendar excludes
        special sessions (include_special=False, 2026-10-11), so they hold on every day the engine
        reconciles.
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
        # Blocking, not skip-if-busy: the 15:50 fire and a catch-up pass reach run(d) without knowing
        # about each other, and a skipped caller would record a watermark for work it never saw end.
        self._run_lock = asyncio.Lock()

    # ------------------------------------------------------------------ per-day entry point

    async def run(self, d: date, symbols: Sequence[str] | None = None) -> ReconcileReport:
        """Reconcile day ``d`` (§2.6 catch-up calls this per missing day). Idempotent per (day,
        symbol): symbols already in ``reconcile_log`` for ``d`` with official bars are skipped,
        never re-fetched.

        ``symbols`` defaults to every symbol holding a ``bars_1m`` row on ``d`` (the
        actively-ticked set, plus anything already backfilled for the day).
        """
        async with self._run_lock:
            return await self._run_locked(d, symbols)

    async def _run_locked(self, d: date, symbols: Sequence[str] | None) -> ReconcileReport:
        if symbols is None:
            symbols = await self._store.arun(self._symbols_for_day, d)
        cfg = self._settings.reconcile
        vol_pct = Decimal(str(cfg.vol_drift_pct))
        max_bad = float(cfg.max_bad_bar_fraction)

        requested = set(symbols)
        logged = await self._store.arun(self._store.get_reconcile_log, d)
        # A row with no official bars converted nothing (unknown token, or an empty fetch): its bars
        # are still self-built, so it is retried, not checkpointed.
        done = {r["symbol"] for r in logged if r["bars_official"]}
        # Flagged by an earlier run of d whose alert never went out (it raised, was stopped, or the
        # send failed): this run's alert covers them, once.
        carried = [r for r in logged if not r["alerted"] and self._row_flagged(r, max_bad)]
        report = ReconcileReport(
            d=d, symbols_already_done=len(requested & done),
            symbols_alert_carried=sorted(r["symbol"] for r in carried),
        )
        total_bad = 0
        day_start = self._clock.combine(d, time(0, 0))
        day_end = self._clock.combine(d + timedelta(days=1), time(0, 0))
        open_minute = self._clock.combine(d, self._session_open)

        ordered = sorted(requested - done)
        for i, symbol in enumerate(ordered):
            existing = await self._store.aget_bars_1m(symbol, day_start, day_end)
            try:
                official = await self._fetch_official(symbol, d)
            except TokenException:
                # 2026-07-21: a rejected token fails every official fetch identically — stop
                # instead of hammering the broker (the KiteClient circuit breaker has already
                # frozen entries). ok=False sinks the DATE_KEYED watermark, so the §2.6 sweep
                # retries the day after re-login and resumes at the first unlogged symbol.
                report.ok = False
                report.aborted_reason = "token_rejected"
                report.symbols_remaining = len(ordered) - i
                _log.warning(
                    "reconcile_aborted_token_rejected", d=d.isoformat(),
                    symbols_remaining=report.symbols_remaining,
                )
                break

            by_minute: dict[datetime, Bar] = {b.ts_minute: b for b in existing}
            self_bars = {m: b for m, b in by_minute.items() if b.src == "self"}
            tick_size = (
                self._tick_size_for(symbol) if self._tick_size_for is not None
                else DEFAULT_TICK_SIZE
            )
            close_threshold = tick_size * cfg.close_drift_ticks

            compared = vol_drift = close_drift = bad = offline = 0
            canonical: list[Bar] = []
            for minute in sorted(official):
                oc = official[minute]
                mine = self_bars.get(minute)
                if mine is None:
                    # §2.6: no self-built bar ⇒ offline span (or already gap-backfilled/official)
                    # — fill from official, EXCLUDE from the drift denominator.
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

            fraction = (bad / compared) if compared else 0.0
            flagged = compared > 0 and fraction > max_bad
            # Bars and checkpoint row in one transaction: an unlogged official symbol would be
            # re-compared on the retry against no self bars (compared=0).
            await self._store.arun(
                self._store.commit_reconciled_symbol, canonical,
                {
                    "d": d, "symbol": symbol, "bars_self": len(self_bars),
                    "bars_official": len(official), "bars_compared": compared,
                    "vol_drift_bars": vol_drift, "close_drift_bars": close_drift,
                    "offline_bars": offline, "bad_bar_fraction": fraction, "alerted": False,
                },
            )
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

        report.bad_bar_fraction = (total_bad / report.bars_compared) if report.bars_compared else 0.0

        to_alert = sorted({*report.symbols_alert_carried, *report.symbols_flagged})
        if to_alert:
            # The message aggregate spans this run's symbols plus the carried rows it names.
            alert_compared = report.bars_compared + sum(int(r["bars_compared"]) for r in carried)
            alert_bad = total_bad + sum(self._row_bad_bars(r) for r in carried)
            alert_fraction = (alert_bad / alert_compared) if alert_compared else 0.0
            _log.warning(
                "reconcile_drift", d=d.isoformat(), symbols_flagged=report.symbols_flagged,
                symbols_alert_carried=report.symbols_alert_carried, bad_bar_fraction=alert_fraction,
            )
            if await self._send_alert(d, to_alert, alert_compared, alert_fraction, max_bad):
                await self._store.arun(self._store.mark_reconcile_alerted, d, to_alert)
                report.alerted = True
        _log.info(
            "reconcile_done", d=d.isoformat(), symbols=len(report.symbols),
            bars_compared=report.bars_compared, offline_bars=report.offline_bars,
            flagged=len(report.symbols_flagged), alert_carried=len(report.symbols_alert_carried),
            alerted=report.alerted, ok=report.ok, aborted_reason=report.aborted_reason,
            symbols_remaining=report.symbols_remaining,
            symbols_already_done=report.symbols_already_done,
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

    @staticmethod
    def _row_flagged(row: dict[str, Any], max_bad: float) -> bool:
        """The in-run flag rule (``compared > 0 and fraction > max_bad``) re-applied to a logged row:
        the stored fraction is the same float, so the verdict is identical at equal thresholds."""
        compared = row["bars_compared"] or 0
        return compared > 0 and float(row["bad_bar_fraction"] or 0.0) > max_bad

    @staticmethod
    def _row_bad_bars(row: dict[str, Any]) -> int:
        # bad_bar_fraction = bad / compared with both small ints, so the product rounds back exactly.
        return round(float(row["bad_bar_fraction"] or 0.0) * int(row["bars_compared"] or 0))

    async def _send_alert(
        self, d: date, symbols: list[str], bars_compared: int, bad_bar_fraction: float,
        max_bad: float,
    ) -> bool:
        """Hand the drift alert to the sink; True only when it accepted the message."""
        if self._notify is None:
            return False
        msg = _reconcile_drift_msg(
            d=d.isoformat(),
            symbols_flagged=list(symbols),
            bars_compared=bars_compared,
            bad_bar_fraction=bad_bar_fraction,
            max_bad_bar_fraction=max_bad,
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - alerting must never fail the (already-persisted) job
            _log.exception("reconcile_notify_failed", d=d.isoformat())
            return False
        return True

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
