"""Shared BSE HTTP hardening — browser headers, bounded retry, error-page detection (§2.8, E5).

BSE's ``api.bseindia.com/BseIndiaAPI/api/*`` JSON endpoints are the §2.8 SHP/pledge-history source
(``SHPQNewFormat/w`` quarter index + ``CorporatesSHPSecuritybeta/w`` detail) and the ISIN→scrip-code
utility (``PeerSmartSearch/w``). They gate on a browser User-Agent and the ``Origin``/``Referer``
site headers (the default ``python-httpx`` UA is rejected), but — unlike NSE — carry NO anti-bot
cookie priming step.

**The documented BSE quirk (§2.8 source verdicts):** a bad request does NOT 404. It returns HTTP
**200** that either redirects to ``…/error_Bse.html`` (final URL) or serves that error HTML with a
200 status. Either way the body is HTML, not JSON. A health check that trusts the status code alone
is fooled; the ONLY reliable signal is "did the body parse as JSON?" (probe evidence: the guessed
``ShareholdingPattern/w`` endpoints both returned ``200`` + ``error_Bse.html``). :func:`bse_get`
therefore treats a 200-whose-final-URL-is-``error_Bse.html`` OR a 200-whose-body-fails-``json.loads``
as a failure and raises :class:`BseError`, so each feed's outer except-degrade-alert path (E5) fires
exactly as it does for a genuine transport error.

Contract (mirrors :mod:`engine.core.nse_http` in shape — this helper only re-raises; the per-feed
degrade+alert path is left intact):
  * Any host: bounded exponential-backoff retry on transient failures (timeouts, transport errors,
    5xx). A persistent transient error re-raises the underlying ``httpx`` exception; a persistent
    4xx/5xx surfaces via ``raise_for_status``.
  * A 200 that is really the BSE error page (redirect target or non-JSON body) raises
    :class:`BseError` — NOT retried (it is a "no such resource / bad params", not transient).

``_sleep`` is a module-level indirection (defaults to :func:`asyncio.sleep`) so tests can monkeypatch
it and observe the backoff schedule without actually waiting (same pattern as ``nse_http``).

Note the JSON-parse health check is on parse-ABILITY, not shape: some BSE endpoints legitimately
return a JSON-encoded STRING rather than an object (``PeerSmartSearch/w`` wraps its ``<li>`` HTML as a
JSON string — probe-verified) — that still parses, so it passes; only the ``error_Bse.html`` page (not
valid JSON at all) is rejected. The caller ``json.loads`` the body and handles str-vs-dict itself.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from engine.core.log import get_logger

_log = get_logger("engine.core.bse_http")

#: Canonical browser-shaped headers for the BSE API host — the single source of truth (each feed
#: imports THIS; no per-feed copy drifts). BSE rejects the default ``python-httpx`` UA and requires
#: the site ``Origin``/``Referer`` (probe evidence, §2.8).
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/",
}

#: The BSE error page a bad request masquerades as a 200 with (final URL OR served body), §2.8.
_BSE_ERROR_PAGE = "error_Bse.html"

#: Backoff base (1s, 2s, 4s, …) — mirrors ``nse_http`` so both hosts pace transient retries alike.
_BACKOFF_BASE_S = 1.0

#: Injectable sleep so tests observe the backoff schedule without waiting (monkeypatch this attribute).
_sleep = asyncio.sleep


class BseError(RuntimeError):
    """A BSE response that is a 200 but not real JSON (the ``error_Bse.html`` quirk, §2.8).

    Raised by :func:`bse_get` so the caller's E5 except-degrade-alert path treats it exactly like a
    transport failure — never a silently-empty success that would be mistaken for "no data".
    """


def _is_error_page(resp: httpx.Response) -> bool:
    """True when this 200 is really the BSE error page — by final-URL redirect target OR by a body
    that does not parse as JSON (§2.8: status alone is not trustworthy; the JSON parse is)."""
    if str(resp.url).rsplit("/", 1)[-1].split("?", 1)[0].lower() == _BSE_ERROR_PAGE.lower():
        return True
    try:
        json.loads(resp.content)
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return True
    return False


async def bse_get(
    http: httpx.AsyncClient, url: str, *, timeout: float, attempts: int = 3
) -> httpx.Response:
    """GET ``url`` from the BSE API with browser headers, bounded retry, and error-page detection (§2.8).

    ``attempts`` bounds the number of GETs of ``url`` (the transient backoff retries draw from this
    budget). Returns the raw :class:`httpx.Response` (JSON-parseable, verified) — the caller
    re-parses with ``json.loads`` exactly like the NSE feeds do. Raises on: a persistent transient
    error (the underlying ``httpx`` exception), a persistent 4xx/5xx (``HTTPStatusError`` via
    ``raise_for_status``), or the BSE error-page quirk (:class:`BseError`).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = await http.get(url, headers=BSE_HEADERS, timeout=timeout, follow_redirects=True)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt >= attempts:
                raise
            _log.warning(
                "bse_get_transient", url=url, attempt=attempt, error=f"{type(exc).__name__}: {exc}"
            )
            await _sleep(_BACKOFF_BASE_S * 2 ** (attempt - 1))
            continue

        # Transient server error: bounded exponential-backoff retry within the attempts budget.
        if resp.status_code >= 500 and attempt < attempts:
            _log.warning("bse_get_server_error", url=url, status=resp.status_code, attempt=attempt)
            await _sleep(_BACKOFF_BASE_S * 2 ** (attempt - 1))
            continue

        resp.raise_for_status()  # a genuine 4xx/5xx surfaces to the feed's guard (rare — BSE prefers 200s)
        # The BSE quirk: a 200 that is really the error page. NOT transient (bad params / no such
        # resource) — raise immediately so the feed's degrade path fires, no retry.
        if _is_error_page(resp):
            raise BseError(f"BSE returned error_Bse.html / non-JSON body for {url} (final={resp.url})")
        return resp
