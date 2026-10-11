"""Surveillance-list ingest — GSM/ASM/T2T/ESM + unsolicited-SMS lists (§3.2.4 / §4.4 job 5, A8).

``SurveillanceIngest.refresh()`` (08:20 daily, §10.1) pulls the exchange surveillance lists that the
§3.2.4 universe rule excludes ("not in GSM/ASM/T2T/ESM") plus the unsolicited-SMS pump-suspect list
(consumed by scanners/gate context, not the universe rule). All sources are best-effort scrapes of
NSE pages/APIs with the plan's ``[likely]`` anti-bot caveat (E5): injected ``httpx.AsyncClient``,
[VERIFY]-marked URLs, defensive parsing.

Failure model (§4.4 job 5, pinned): on a per-source failure, REUSE yesterday's list for that source
(runtime JSON cache under ``data/``, survives restarts) + alert + expose the affected symbols via
``SurveillanceLists.unconfirmed_symbols``. Unconfirmed symbols are refused new entries INDIRECTLY:
the reused list keeps them flagged, so the 08:30 universe build excludes them and the Tier-2 gate
refuses anything outside the eligible universe. No production code reads ``unconfirmed_symbols``;
there is no direct gate seam. A failed source can therefore never *shrink* the exclusion set:
yesterday's flagged symbols stay excluded, and they are additionally unconfirmed.

Held-position migration (A8, plan §2.6 step 5 / §4.4 job 5): every refresh alerts the owner, through
``notify``, for each OPEN real position (the injected ``held_positions``) whose symbol is on a list.
The alert is state-driven, not diff-driven: it fires on the first refresh that finds a held symbol
listed, and the catalog ``dedupe_key`` (position, list) makes it once-only across restarts. The
per-list diff (:meth:`SurveillanceLists.new_listings`) against the previous snapshot — the in-process
one, else the on-disk cache as loaded, which spans any offline gap — only sets the wording. So a
cache advanced by another writer, a missing cache, or a position opened on an already-listed symbol
still alerts. Without ``held_positions`` (``scripts/build_universe.py``) nothing is sent.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from engine.core.clock import Clock
from engine.core.log import get_logger
from engine.core.nse_http import nse_get
from engine.notify.catalog import CatalogMessage, MessageKind, surveillance_migration

_log = get_logger("engine.universe.surveillance")

# --------------------------------------------------------------------------- sources ([VERIFY Phase-1])
# NSE JSON report endpoints ([likely] anti-bot: cookie/UA-gated — best-effort per plan §4.4 job 5).
NSE_GSM_URL = "https://www.nseindia.com/api/reportGSM"                # [VERIFY Phase-1]
NSE_ASM_URL = "https://www.nseindia.com/api/reportASM"                # [VERIFY Phase-1]
NSE_ESM_URL = "https://www.nseindia.com/api/reportESM"                # [VERIFY Phase-1]
#: T2T from the listed-securities master (archives host, less bot-gated): SERIES BE/BZ = trade-to-trade.
NSE_T2T_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"   # [VERIFY Phase-1]
#: Unsolicited-SMS watch list — RETIRED 2026-07-28: NSE removed /api/unsolicited-sms (hard 404 while
#: the sibling report endpoints above still serve, so removal, not anti-bot; probed variants all 404).
#: A permanently-dead fetch would re-flag "degraded" + critical-alert every refresh for a list the
#: §3.2.4 universe rules never consumed. The ``sms`` field stays on the snapshot as the seam for a
#: replacement source, pinned empty until one exists.

#: T2T series codes in the securities master (BE = rolling T2T, BZ = T2T + other restrictions).
_T2T_SERIES = frozenset({"BE", "BZ"})

NotifySink = Callable[[CatalogMessage], Awaitable[None]]
#: OPEN real positions as ``(position_id, symbol, qty)`` — paper is never passed in (paper plan D4).
HeldFn = Callable[[], Sequence[tuple[str, str, int]]]

#: Universe-rule sources (§3.2.4 pins these four) in canonical order; ``sms`` is carried separately.
SURVEILLANCE_KEYS: tuple[str, ...] = ("gsm", "asm", "t2t", "esm")


class SurveillanceLists(BaseModel):
    """One refresh's surveillance snapshot (A8) — the §3.2.4 universe-rule exclusion input.

    ``unconfirmed_symbols``: symbols whose CURRENT status could not be confirmed because their
    source failed and yesterday's list was reused. They are already inside :meth:`flagged`, so the
    universe exclusion refuses them; nothing reads this field directly. ``degraded_sources`` names
    the failed sources (the universe build marks itself degraded on any).
    """

    model_config = ConfigDict(frozen=True)

    as_of: date
    gsm: frozenset[str] = Field(default_factory=frozenset)
    asm: frozenset[str] = Field(default_factory=frozenset)
    t2t: frozenset[str] = Field(default_factory=frozenset)
    esm: frozenset[str] = Field(default_factory=frozenset)
    sms: frozenset[str] = Field(default_factory=frozenset)
    degraded_sources: tuple[str, ...] = ()
    unconfirmed_symbols: frozenset[str] = Field(default_factory=frozenset)
    #: EQ-series rows of the same EQUITY_L.csv the T2T parse reads — the NSE listed-EQUITY master
    #: (companies only; ETFs/SME series are absent). The §3.2.4 batch-universe extended leg
    #: (2026-09-01) intersects candidates with this set to keep ETFs/SME boards out. Informational —
    #: never part of ``flagged()``/``reasons_for``. Empty when the source AND cache are both
    #: unavailable (the extended leg then builds nothing — fail closed).
    equity_master: frozenset[str] = Field(default_factory=frozenset)

    def flagged(self) -> frozenset[str]:
        """Union of the four universe-rule lists (GSM/ASM/T2T/ESM — not SMS, §3.2.4)."""
        return frozenset(self.gsm | self.asm | self.t2t | self.esm)

    def reasons_for(self, symbol: str) -> list[str]:
        """Per-list exclusion reasons for ``symbol`` (auditable ``universe_daily`` strings, §4.3)."""
        sym = symbol.upper()
        return [f"surveillance_{key}" for key in SURVEILLANCE_KEYS if sym in getattr(self, key)]

    def new_entries(self, previous: SurveillanceLists) -> frozenset[str]:
        """Symbols newly flagged vs ``previous`` on the UNION of the lists — a move from one list to
        another (ASM→GSM) is not in it; the A8 alert uses :meth:`new_listings`."""
        return frozenset(self.flagged() - previous.flagged())

    def new_listings(self, previous: SurveillanceLists) -> dict[str, frozenset[str]]:
        """Per list, symbols on it now and not on ``previous``'s copy (A8 wording input)."""
        return {key: frozenset(getattr(self, key) - getattr(previous, key)) for key in SURVEILLANCE_KEYS}


def _symbols_from_json(payload: Any) -> set[str]:
    """Recursively collect ``"symbol"`` values from an arbitrarily nested NSE JSON payload.

    The NSE report endpoints wrap rows differently per list (``{"data": [...]}``,
    ``{"longterm": {"data": [...]}, "shortterm": {...}}``, plain arrays…) — walking the whole tree
    for ``symbol`` keys is robust to all of them (parse-defensively, E5).
    """
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if str(key).lower() == "symbol" and isinstance(value, str) and value.strip():
                    found.add(value.strip().upper())
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return found


def _parse_equity_master(text: str) -> tuple[set[str], set[str]]:
    """One pass over EQUITY_L.csv → ``(t2t_symbols, eq_master)`` (defensive column lookup).

    T2T = rows whose SERIES is BE/BZ (the §3.2.4 exclusion input, unchanged). ``eq_master`` = rows
    whose SERIES is EQ — the listed-equity master the batch-universe extended leg intersects with
    (2026-09-01): ETFs and SME-board series never appear in it, so it doubles as the
    keep-companies-only filter without a second download."""
    reader = csv.DictReader(io.StringIO(text))
    norm = {(name or "").strip().lower(): name for name in (reader.fieldnames or [])}
    sym_col, series_col = norm.get("symbol"), norm.get("series")
    if sym_col is None or series_col is None:
        raise ValueError(f"EQUITY_L.csv missing SYMBOL/SERIES columns: {reader.fieldnames}")
    t2t: set[str] = set()
    master: set[str] = set()
    for row in reader:
        series = (row.get(series_col) or "").strip().upper()
        symbol = (row.get(sym_col) or "").strip().upper()
        if not symbol:
            continue
        if series in _T2T_SERIES:
            t2t.add(symbol)
        elif series == "EQ":
            master.add(symbol)
    return t2t, master


class SurveillanceIngest:
    """§3.2.4 ``SurveillanceIngest`` — best-effort surveillance-list refresh with reuse-yesterday (E5).

    Parameters
    ----------
    clock:
        Single source of "now" (§3.2) — stamps ``as_of`` and the cache entries.
    http:
        Injected ``httpx.AsyncClient`` (convention 11). Owned by the caller; never closed here.
    cache_path:
        Runtime last-good JSON cache (e.g. ``data/universe/surveillance.json``), one entry per
        source — the "yesterday's lists" reused per-source on failure, surviving restarts.
    notify:
        Optional owner-alert sink; any degraded source alerts through it (critical — A8 safety).
    held_positions:
        Optional OPEN real positions reader; each refresh alerts per held symbol on a list (A8, see
        the module docstring). ``None`` = no held-position alert.
    """

    def __init__(
        self,
        clock: Clock,
        http: httpx.AsyncClient,
        cache_path: str | Path,
        *,
        notify: NotifySink | None = None,
        held_positions: HeldFn | None = None,
        request_timeout_s: float = 15.0,
    ) -> None:
        self._clock = clock
        self._http = http
        self._cache_path = Path(cache_path)
        self._notify = notify
        self._held_positions = held_positions
        self._timeout = float(request_timeout_s)
        self._current: SurveillanceLists | None = None
        # Per-list date of the list ``_current`` actually holds (the cached copy's date for a failed
        # source), the dating of the next refresh's in-process comparison.
        self._current_used_as_of: dict[str, str | None] = {}

    async def current(self) -> SurveillanceLists:
        """The latest refreshed lists, refreshing once if never refreshed this process (§2.6)."""
        if self._current is None:
            return await self.refresh()
        return self._current

    async def refresh(self) -> SurveillanceLists:
        """Fetch every source under its own guard; failed sources reuse yesterday + alert (§4.4 job 5).

        Never raises into the caller's loop (E5). Returns (and retains for :meth:`current`) the
        snapshot with per-source results, degraded-source names, and the unconfirmed-symbol set.
        """
        today = self._clock.today()
        cache = self._load_cache()
        # Captured before the fetch loop replaces cache entries: the over-the-gap comparison. Named
        # ``prior`` because the failure branch below binds ``previous`` to one list.
        try:
            prior, prior_as_of = self._previous_snapshot(cache, today)
        except Exception:  # noqa: BLE001 - E5: wording input only; every list then reads unknown
            _log.exception("surveillance_previous_snapshot_failed")
            prior, prior_as_of = SurveillanceLists(as_of=today), dict.fromkeys(SURVEILLANCE_KEYS)
        results: dict[str, frozenset[str]] = {}
        degraded: list[str] = []
        unconfirmed: set[str] = set()
        equity_master: frozenset[str] = frozenset()
        # Date of the list each result actually is: today for a fresh download, the reused cache
        # entry's date for a failed one (None = no dated copy).
        used_as_of: dict[str, str | None] = dict.fromkeys(SURVEILLANCE_KEYS, today.isoformat())

        for key, coro in (
            ("gsm", self._fetch_json_symbols(NSE_GSM_URL)),
            ("asm", self._fetch_json_symbols(NSE_ASM_URL)),
            ("t2t", self._fetch_t2t()),
            ("esm", self._fetch_json_symbols(NSE_ESM_URL)),
        ):
            try:
                if key == "t2t":
                    # One EQUITY_L.csv download serves both the T2T exclusion list and the
                    # listed-equity master (batch-universe extended leg, 2026-09-01) — see
                    # _parse_equity_master. The master rides the same cache entry so a failed
                    # download reuses yesterday's master exactly like it reuses yesterday's list.
                    symbols, master = await coro
                    equity_master = frozenset(master)
                    cache["equity_master"] = {"as_of": today.isoformat(),
                                              "symbols": sorted(equity_master)}
                else:
                    symbols = await coro
                results[key] = frozenset(symbols)
                cache[key] = {"as_of": today.isoformat(), "symbols": sorted(symbols)}
            except Exception as exc:  # noqa: BLE001 - E5: per-source degrade, never raise
                previous = frozenset(cache.get(key, {}).get("symbols") or [])
                _log.warning(
                    "surveillance_source_failed",
                    source=key,
                    error=f"{type(exc).__name__}: {exc}",
                    reused=len(previous),
                )
                results[key] = previous
                degraded.append(key)
                entry = cache.get(key)
                stamp = entry.get("as_of") if isinstance(entry, dict) else None
                used_as_of[key] = stamp if isinstance(stamp, str) and stamp else None
                unconfirmed |= previous
                if key == "t2t":
                    equity_master = frozenset(cache.get("equity_master", {}).get("symbols") or [])

        self._save_cache(cache)
        lists = SurveillanceLists(
            as_of=today,
            gsm=results["gsm"],
            asm=results["asm"],
            t2t=results["t2t"],
            esm=results["esm"],
            # sms retired (source removed by NSE 2026-07-28) — pinned empty, see the note at the top.
            degraded_sources=tuple(degraded),
            unconfirmed_symbols=frozenset(unconfirmed),
            equity_master=equity_master,
        )
        self._current = lists
        self._current_used_as_of = used_as_of
        _log.info(
            "surveillance_refreshed",
            gsm=len(lists.gsm), asm=len(lists.asm), t2t=len(lists.t2t), esm=len(lists.esm),
            sms=len(lists.sms), equity_master=len(lists.equity_master), degraded=degraded,
        )
        if degraded:
            await self._alert(degraded=degraded, unconfirmed=len(unconfirmed))
        try:
            await self._alert_held_listings(lists, prior, prior_as_of, used_as_of)
        except Exception:  # noqa: BLE001 - E5: a SAFETY_CRITICAL job's refresh never raises
            _log.exception("surveillance_migration_check_failed")
        return lists

    def _previous_snapshot(
        self, cache: dict[str, Any], today: date
    ) -> tuple[SurveillanceLists, dict[str, str | None]]:
        """The snapshot this refresh is compared against, with each list's date (``None`` = no
        earlier copy of that list, so its status is unknown). In-process snapshot first; after a
        restart the loaded cache, whose per-list dates span any offline gap."""
        cur = self._current
        if cur is not None:
            # Each list's own date: a failed source reused the cached copy, so naming ``cur.as_of``
            # would date a list that was never downloaded. A failed source with nothing to reuse was
            # never known (None), not known-empty.
            return cur, {key: None if key in cur.degraded_sources and not getattr(cur, key)
                         else self._current_used_as_of.get(key, cur.as_of.isoformat())
                         for key in SURVEILLANCE_KEYS}
        lists: dict[str, frozenset[str]] = {}
        as_of: dict[str, str | None] = {}
        for key in SURVEILLANCE_KEYS:
            entry = cache.get(key)
            symbols = entry.get("symbols") if isinstance(entry, dict) else None
            stamp = entry.get("as_of") if isinstance(entry, dict) else None
            if not isinstance(symbols, list) or not isinstance(stamp, str) or not stamp:
                as_of[key] = None
                continue
            lists[key] = frozenset(str(s).upper() for s in symbols)
            as_of[key] = stamp
        # ``as_of`` here is a placeholder: the per-list dates above are the comparison's real dates.
        return SurveillanceLists(as_of=today, **lists), as_of

    async def _alert_held_listings(
        self, lists: SurveillanceLists, previous: SurveillanceLists, previous_as_of: dict[str, str | None],
        used_as_of: dict[str, str | None],
    ) -> None:
        """One critical alert per (held position, list it is on) — A8. Repeats every refresh by
        design; the journal's ``dedupe_key`` index delivers each (position, list) once."""
        if self._held_positions is None:
            return
        held = self._held_positions()
        if not held:
            return
        new = lists.new_listings(previous)
        for position_id, symbol, qty in held:
            sym = str(symbol).upper()
            for key in SURVEILLANCE_KEYS:
                if sym not in getattr(lists, key):
                    continue
                prev_as_of = previous_as_of.get(key)
                unconfirmed = key in lists.degraded_sources
                if unconfirmed and prev_as_of == used_as_of.get(key):
                    # Both sides are the same reused copy: a list compared with itself says nothing.
                    prev_as_of = None
                status = "unknown" if prev_as_of is None else ("new" if sym in new[key] else "listed")
                _log.warning(
                    "surveillance_held_listed", position_id=position_id, symbol=sym, list=key,
                    status=status, previous_as_of=prev_as_of, unconfirmed=unconfirmed,
                )
                if self._notify is None:
                    continue
                msg = surveillance_migration(
                    symbol=sym, position_id=str(position_id), qty=int(qty or 0), list_key=key,
                    as_of=lists.as_of.isoformat(), previous_as_of=prev_as_of, status=status,
                    unconfirmed=unconfirmed, list_as_of=used_as_of.get(key),
                )
                try:
                    await self._notify(msg)
                except Exception:  # noqa: BLE001 - one failed send must not skip the other holdings
                    _log.exception("surveillance_migration_notify_failed", position_id=position_id, list=key)

    # ------------------------------------------------------------------ fetchers
    async def _fetch_json_symbols(self, url: str) -> set[str]:
        # www.nseindia.com API host: cookie priming + browser headers + bounded retry (A3, E5).
        resp = await nse_get(self._http, url, timeout=self._timeout)
        return _symbols_from_json(json.loads(resp.content))

    async def _fetch_t2t(self) -> tuple[set[str], set[str]]:
        # nsearchives host: browser headers + retry, no priming (no cookie gate on archives, A4).
        # Returns (t2t_symbols, eq_master) from one EQUITY_L.csv pass — see _parse_equity_master.
        resp = await nse_get(self._http, NSE_T2T_URL, timeout=self._timeout)
        return _parse_equity_master(resp.text)

    # ------------------------------------------------------------------ cache (reuse-yesterday, E5)
    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self._cache_path.exists():
            return {}
        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except (OSError, ValueError):
            _log.exception("surveillance_cache_unreadable", path=str(self._cache_path))
            return {}

    def _save_cache(self, cache: dict[str, dict[str, Any]]) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(cache), encoding="utf-8")
        except OSError:  # not fatal — next failure just has an older fallback
            _log.exception("surveillance_cache_write_failed", path=str(self._cache_path))

    async def _alert(self, *, degraded: list[str], unconfirmed: int) -> None:
        if self._notify is None:
            return
        msg = CatalogMessage(
            # Closest catalog kind for "safety-critical daily list not fresh" (§2.6 step 5 names
            # surveillance in the safety set); the freeze itself is the gate/catch-up's decision.
            kind=MessageKind.DATA_FRESHNESS_FROZEN,
            title="Surveillance lists degraded — reusing yesterday",
            body=(
                f"Source(s) failed: {', '.join(degraded)}. Yesterday's lists reused; "
                f"{unconfirmed} symbol(s) unconfirmed — gate refuses NEW entries there (A8/E5)."
            ),
            severity="critical",
            data={"job_id": "surveillance", "degraded_sources": degraded, "unconfirmed": unconfirmed},
        )
        try:
            await self._notify(msg)
        except Exception:  # noqa: BLE001 - best-effort alert; a failed send never propagates
            _log.exception("surveillance_notify_failed")
