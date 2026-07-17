"""NSE bhavcopy (UDiFF) ingest — §4.4 job 6 (18:00), E5 best-effort.

Downloads the day's cash-market bhavcopy in the CURRENT UDiFF format/URL scheme (NSE cut over from
the legacy ``EQ_...bhav.csv.zip`` in 2024 — the [VERIFY]-marked template below is the UDiFF archive
path) and uses it two ways per the plan:

1. **Cross-check input for ``bars_1d``** (§4.3): where a Kite-official row already exists for
   (symbol, day) it is CANONICAL (adjusted per A11) — the bhavcopy row only cross-checks it
   (close/volume drift beyond tolerance is logged + reported, never overwritten). Where no row
   exists, the bhavcopy row is written with ``src='bhavcopy'`` (fills the daily-bar history the
   traded-value filter needs even when Kite backfill lags).
2. **Traded-value universe-filter feed**: the written ``bars_1d`` rows are exactly what
   ``UniverseBuilder`` medians over (``close × volume``).

Failure model (E5): injected ``httpx.AsyncClient``, defensive parsing (UDiFF column names with
legacy aliases; malformed rows skipped-and-counted), and on ANY failure the job alerts + returns a
degraded result — ``bars_1d`` keeps yesterday's data and the §2.6 date-keyed catch-up re-runs the
missed day. Never raises into the scheduler.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import httpx
from pydantic import BaseModel, ConfigDict

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.marketdata.store import DailyBar, MarketStore
from engine.notify.catalog import CatalogMessage, MessageKind

_log = get_logger("engine.datafeeds.bhavcopy")

#: UDiFF cash-market bhavcopy archive URL (current scheme). [VERIFY Phase-1] — NSE moves these.
BHAVCOPY_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
)

#: Only the rolling-settlement equity series feeds bars_1d / the traded-value filter; T2T (BE/BZ)
#: names are universe-excluded anyway (§3.2.4) and SME/other series are out of scope.
_SERIES_KEPT = frozenset({"EQ"})

#: Cross-check tolerances (module-pinned; the §3.2.3 reconcile thresholds govern 1m bars, not this):
#: close within one minimum tick (A10 floor ₹0.05), volume within 2%.
CLOSE_TOLERANCE = Decimal("0.05")
VOLUME_TOLERANCE_PCT = 2.0

_PAISE = Decimal("0.01")
_ZIP_MAGIC = b"PK\x03\x04"

NotifySink = Callable[[CatalogMessage], Awaitable[None]]


class BhavcopyResult(BaseModel):
    """One run's outcome (returned to the scheduler/catch-up; never an exception, E5)."""

    model_config = ConfigDict(frozen=True)

    d: date
    ok: bool
    degraded: bool = False
    rows_parsed: int = 0
    rows_written: int = 0
    rows_cross_checked: int = 0
    mismatched_symbols: tuple[str, ...] = ()
    reason: str | None = None


def _parse_date(raw: str) -> date | None:
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_bhavcopy_csv(text: str, d: date) -> list[DailyBar]:
    """UDiFF (or legacy) bhavcopy CSV → ``DailyBar`` rows for day ``d`` (``src='bhavcopy'``).

    Defensive (E5): column names are located case-insensitively with UDiFF-first aliases
    (``TckrSymb``/``SYMBOL``, ``OpnPric``/``OPEN``, …); non-EQ series are dropped; a row whose
    ``TradDt`` parses to a DIFFERENT day is dropped (wrong file); malformed rows are
    skipped-and-counted, never fatal.
    """
    reader = csv.DictReader(io.StringIO(text))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}

    def col(row: dict, *aliases: str) -> str:
        for alias in aliases:
            key = norm.get(alias)
            if key is not None:
                value = (row.get(key) or "").strip()
                if value:
                    return value
        return ""

    bars: list[DailyBar] = []
    malformed = 0
    for row in reader:
        try:
            series = col(row, "sctysrs", "series").upper()
            if series not in _SERIES_KEPT:
                continue
            symbol = col(row, "tckrsymb", "symbol").upper()
            if not symbol:
                malformed += 1
                continue
            trad_dt = col(row, "traddt", "date1", "timestamp")
            if trad_dt:
                parsed = _parse_date(trad_dt)
                if parsed is not None and parsed != d:
                    continue  # a row for another day means the wrong file — never mis-key bars_1d
            bars.append(
                DailyBar(
                    symbol=symbol,
                    d=d,
                    open=Decimal(col(row, "opnpric", "open")).quantize(_PAISE),
                    high=Decimal(col(row, "hghpric", "high")).quantize(_PAISE),
                    low=Decimal(col(row, "lwpric", "low")).quantize(_PAISE),
                    close=Decimal(col(row, "clspric", "close")).quantize(_PAISE),
                    volume=int(Decimal(col(row, "ttltradgvol", "tottrdqty") or "0")),
                    src="bhavcopy",
                )
            )
        except (InvalidOperation, ValueError, TypeError, KeyError):
            malformed += 1
    if malformed:
        _log.warning("bhavcopy_malformed_rows", d=d.isoformat(), skipped=malformed)
    return bars


def _unwrap_zip(content: bytes) -> str:
    """Bhavcopy payload → CSV text (the archive URL serves ``.csv.zip``; tolerate plain CSV too)."""
    if content[:4] == _ZIP_MAGIC:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")] or zf.namelist()
            if not names:
                raise ValueError("bhavcopy zip contains no members")
            return zf.read(names[0]).decode("utf-8-sig")
    return content.decode("utf-8-sig")


class BhavcopyJob:
    """§4.4 job 6 — daily UDiFF bhavcopy → ``bars_1d`` cross-check/fill + traded-value feed (E5)."""

    def __init__(
        self,
        store: MarketStore,
        clock: Clock,
        http: httpx.AsyncClient,
        *,
        notify: NotifySink | None = None,
        request_timeout_s: float = 30.0,
    ) -> None:
        self._store = store
        self._clock = clock
        self._http = http
        self._notify = notify
        self._timeout = float(request_timeout_s)

    async def run(self, d: date) -> BhavcopyResult:
        """Fetch + parse + persist day ``d``'s bhavcopy. Never raises into the scheduler (E5)."""
        url = BHAVCOPY_URL_TEMPLATE.format(d=d)
        try:
            # Archives host (nsearchives): browser UA + bounded retry, no cookie priming (A4).
            resp = await nse_get(self._http, url, timeout=self._timeout)
            bars = parse_bhavcopy_csv(_unwrap_zip(resp.content), d)
            if not bars:
                raise ValueError("bhavcopy parsed to zero EQ rows")
        except Exception as exc:  # noqa: BLE001 - E5: degrade + alert, never raise
            reason = f"{type(exc).__name__}: {exc}"
            _log.warning("bhavcopy_fetch_failed", d=d.isoformat(), url=url, error=reason)
            await self._alert(d, reason)
            return BhavcopyResult(d=d, ok=False, degraded=True, reason=reason)

        written, checked, mismatches = await self._store.arun(self._persist, d, bars)
        if mismatches:
            _log.warning(
                "bhavcopy_cross_check_mismatch", d=d.isoformat(),
                symbols=mismatches[:20], count=len(mismatches),
            )
        result = BhavcopyResult(
            d=d, ok=True, rows_parsed=len(bars), rows_written=written,
            rows_cross_checked=checked, mismatched_symbols=tuple(mismatches),
        )
        _log.info(
            "bhavcopy_ingested", d=d.isoformat(), parsed=len(bars), written=written,
            cross_checked=checked, mismatches=len(mismatches),
        )
        return result

    # ------------------------------------------------------------------ persistence (sync, via arun)
    def _persist(self, d: date, bars: list[DailyBar]) -> tuple[int, int, list[str]]:
        """Kite-official rows are canonical (A11-adjusted): cross-check them, never overwrite;
        write bhavcopy rows only where no row exists yet.

        One day-scan read + one batched write — NOT per-symbol round trips: ~2000 single-row
        store calls cost ~13.5 ms each (≈45 s nightly) vs sub-second batched."""
        existing_by_symbol = {b.symbol: b for b in self._store.get_bars_1d_for_day(d)}
        written = 0
        checked = 0
        mismatches: list[str] = []
        new_bars: list[DailyBar] = []
        for bar in bars:
            current = existing_by_symbol.get(bar.symbol)
            if current is not None:
                checked += 1
                close_off = abs(current.close - bar.close) > CLOSE_TOLERANCE
                if current.volume:
                    vol_off = abs(current.volume - bar.volume) * 100.0 / current.volume > VOLUME_TOLERANCE_PCT
                else:
                    vol_off = bar.volume != 0
                if close_off or vol_off:
                    mismatches.append(bar.symbol)
            else:
                new_bars.append(bar)
                written += 1
                # A duplicate-symbol row later in this batch must cross-check against this one
                # (the old per-row read-after-write behavior), not double-count as written.
                existing_by_symbol[bar.symbol] = bar
        if new_bars:
            self._store.upsert_bars_1d(new_bars)
        return written, checked, mismatches

    async def _alert(self, d: date, reason: str) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            kind=MessageKind.DATA_FRESHNESS_FROZEN,  # closest catalog kind (closed catalog here)
            title="Bhavcopy ingest degraded",
            body=(
                f"Bhavcopy for {d.isoformat()} failed: {reason}. bars_1d keeps prior data; the "
                "§2.6 date-keyed catch-up will retry (E5)."
            ),
            severity="warning",
            data={"job_id": "bhavcopy", "d": d.isoformat(), "reason": reason},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("bhavcopy_notify_failed")
