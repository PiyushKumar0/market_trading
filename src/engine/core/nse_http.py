"""Shared NSE HTTP hardening — cookie priming, browser headers, bounded retry (A3/A4, E5).

NSE's ``www.nseindia.com/api/*`` JSON endpoints mint anti-bot cookies (``nsit``/``nseappid``/
``bm_sv``/``ak_bmsc``) only after the homepage ``https://www.nseindia.com/`` has been fetched on the
same client; without those cookies the edge returns a **deliberately-misleading 404** (not 403), which
is exactly how ``corp_actions`` failed (see ``runbooks/boot-log-investigation-2026-07-10.md`` §A3). The
archives host ``nsearchives.nseindia.com`` tarpits the default python-httpx UA and has no anti-bot
cookie gate, but a slow file-drop can time out with no retry (``bhavcopy``, §A4).

:func:`nse_get` is the single funnel every best-effort NSE feed (``engine.datafeeds.*`` and
``engine.universe.surveillance``) fetches through. It lives in ``engine.core`` — the foundation layer
both consumer packages already import (``engine.core.clock``/``engine.core.log``) — so the module is
importable from either without introducing a ``universe -> datafeeds`` edge (the import-graph guard,
``tests/unit/test_import_graph.py``, only forbids ``engine.risk``/``engine.oms`` -> ``engine.intelligence``).

Contract (per feed's outer try/except degrade+alert path is left intact — this helper only re-raises):
  * ``www.nseindia.com`` host: prime the client's cookie jar once (best-effort homepage GET), then on a
    401/403/404 re-prime once and retry (the anti-bot deception); a second 4xx surfaces via
    ``raise_for_status`` so a genuine 404 is not masked.
  * Any host: bounded exponential-backoff retry on transient failures (timeouts, transport errors, 5xx).
  * ``nsearchives.nseindia.com`` (and any non-www host): headers + retry, but NO priming (no cookie gate).

``_sleep`` is a module-level indirection (defaults to :func:`asyncio.sleep`) so tests can monkeypatch it
and observe the backoff schedule without actually waiting.
"""

from __future__ import annotations

import asyncio
import weakref

import httpx

from engine.core.log import get_logger

_log = get_logger("engine.core.nse_http")

#: Homepage that mints the anti-bot cookie jar; the www API host that is cookie-gated.
_NSE_HOME_URL = "https://www.nseindia.com/"
_NSE_WWW_HOST = "www.nseindia.com"

#: Canonical browser-shaped headers — the single source of truth (each feed imports THIS; no per-feed
#: copy drifts). NSE rejects the default ``python-httpx`` UA outright (anti-bot [likely], §A3/A4).
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

#: The "deliberately-misleading" statuses NSE returns for an un-cookied ``/api/*`` request (§A3): a
#: 404 that is really "no cookies", not "no such resource" — so we re-prime + retry once before trusting it.
_ANTIBOT_STATUSES = frozenset({401, 403, 404})

#: Homepage-prime timeout (short — it never returns a body we parse) and backoff base (1s, 2s, 4s, …).
_PRIME_TIMEOUT_S = 10.0
_BACKOFF_BASE_S = 1.0

#: Clients whose cookie jar has already been primed this process (identity-keyed, GC-friendly).
_PRIMED: weakref.WeakSet[httpx.AsyncClient] = weakref.WeakSet()

#: Injectable sleep so tests observe the backoff schedule without waiting (monkeypatch this attribute).
_sleep = asyncio.sleep


async def _prime(http: httpx.AsyncClient) -> None:
    """Best-effort homepage GET to mint the anti-bot cookie jar; marks the client primed regardless.

    A failed prime is swallowed + logged (never blocks the real fetch, E5): the subsequent ``/api``
    GET may still 4xx, which the caller re-primes on. The client is marked primed either way so a
    down homepage cannot trigger a prime storm — the anti-bot retry path is the recovery route.
    """
    if http in _PRIMED:
        return
    try:
        await http.get(_NSE_HOME_URL, headers=NSE_HEADERS, timeout=_PRIME_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - priming is best-effort; a failed prime never blocks the fetch
        _log.warning("nse_prime_failed", error=f"{type(exc).__name__}: {exc}")
    _PRIMED.add(http)


async def nse_get(
    http: httpx.AsyncClient, url: str, *, timeout: float, attempts: int = 3
) -> httpx.Response:
    """GET ``url`` with browser headers, one-time cookie priming (www only), and bounded retry (A3/A4).

    ``attempts`` bounds the number of GETs of ``url`` (the anti-bot re-prime retry and the transient
    backoff retries all draw from this same budget). The final failure re-raises unchanged — the
    ``httpx`` exception on a persistent transient error, or ``HTTPStatusError`` via ``raise_for_status``
    on a persistent 4xx/5xx — so each feed's existing except-degrade-alert path (E5) fires as before.
    """
    is_www = httpx.URL(url).host == _NSE_WWW_HOST
    reprimed = False  # the anti-bot re-prime fires at most once per call

    if is_www:
        await _prime(http)

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = await http.get(url, headers=NSE_HEADERS, timeout=timeout)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt >= attempts:
                raise
            _log.warning(
                "nse_get_transient", url=url, attempt=attempt, error=f"{type(exc).__name__}: {exc}"
            )
            await _sleep(_BACKOFF_BASE_S * 2 ** (attempt - 1))
            continue

        status = resp.status_code
        # Anti-bot deception: an un-cookied www ``/api`` 4xx is "re-prime and try once more", not a real
        # 404. Re-prime (no backoff — the cookie jar is the fix) and retry; a second 4xx surfaces below.
        if is_www and status in _ANTIBOT_STATUSES and not reprimed and attempt < attempts:
            reprimed = True
            _PRIMED.discard(http)
            _log.warning("nse_get_reprime", url=url, status=status, attempt=attempt)
            await _prime(http)
            continue
        # Transient server error: bounded exponential-backoff retry within the attempts budget.
        if status >= 500 and attempt < attempts:
            _log.warning("nse_get_server_error", url=url, status=status, attempt=attempt)
            await _sleep(_BACKOFF_BASE_S * 2 ** (attempt - 1))
            continue

        resp.raise_for_status()  # 2xx/3xx: no-op; a genuine 4xx/5xx surfaces to the feed's guard
        return resp
