"""engine.core.nse_http (A3/A4): cookie priming, anti-bot re-prime, and bounded backoff retry.

Every case drives :func:`nse_get` through an ``httpx.MockTransport`` that records the exact request
sequence, so the priming/re-prime/backoff contract is asserted on observed traffic — not internals.
``_sleep`` is monkeypatched to a recorder so the backoff schedule is observed without waiting.
"""

from __future__ import annotations

import httpx
import pytest

from engine.core import nse_http
from engine.core.nse_http import nse_get

WWW_API = "https://www.nseindia.com/api/foo"
HOMEPAGE = "https://www.nseindia.com/"
ARCHIVES = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"


@pytest.fixture(autouse=True)
def _clear_primed():
    """The primed-clients WeakSet is module-global — reset it around each test for determinism."""
    nse_http._PRIMED.clear()
    yield
    nse_http._PRIMED.clear()


def recording_client(handler):
    """An AsyncClient over a MockTransport that records every request URL into a shared list."""
    seen: list[str] = []

    def transport(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return handler(request)

    return httpx.AsyncClient(transport=httpx.MockTransport(transport)), seen


def recording_sleep(monkeypatch):
    """Replace nse_http._sleep with an async no-op that records the delays it was asked to wait."""
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(nse_http, "_sleep", fake_sleep)
    return slept


# --------------------------------------------------------------------------- (a) priming once
async def test_www_api_primes_homepage_once_across_two_calls():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(200)                      # homepage mints cookies
        return httpx.Response(200, json={"ok": True})

    client, seen = recording_client(handler)
    async with client:
        r1 = await nse_get(client, WWW_API, timeout=5.0)
        r2 = await nse_get(client, WWW_API, timeout=5.0)

    assert r1.status_code == 200 and r2.status_code == 200
    # Homepage fetched exactly once, BEFORE the first api GET; the second call skips priming.
    assert seen == [HOMEPAGE, WWW_API, WWW_API]
    # The browser UA + Referer from NSE_HEADERS are applied on the fetch (single-source headers).
    assert nse_http.NSE_HEADERS["Referer"] == HOMEPAGE
    assert nse_http.NSE_HEADERS["User-Agent"].startswith("Mozilla/5.0")


# --------------------------------------------------------------------------- (b) anti-bot 4xx
async def test_www_api_404_once_reprimes_and_succeeds(monkeypatch):
    slept = recording_sleep(monkeypatch)
    calls = {"api": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(200)
        calls["api"] += 1
        return httpx.Response(404) if calls["api"] == 1 else httpx.Response(200, json={"ok": True})

    client, seen = recording_client(handler)
    async with client:
        resp = await nse_get(client, WWW_API, timeout=5.0)

    assert resp.status_code == 200
    # prime → api(404) → RE-prime (second homepage) → api(200): the anti-bot deception recovery.
    assert seen == [HOMEPAGE, WWW_API, HOMEPAGE, WWW_API]
    assert slept == []                                      # re-prime does NOT back off (cookie is the fix)


async def test_www_api_404_twice_raises_http_status_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200) if request.url.path == "/" else httpx.Response(404)

    client, seen = recording_client(handler)
    async with client:
        with pytest.raises(httpx.HTTPStatusError):
            await nse_get(client, WWW_API, timeout=5.0)     # a genuine (post-reprime) 404 surfaces

    assert seen == [HOMEPAGE, WWW_API, HOMEPAGE, WWW_API]   # primed + one re-prime, then it gives up


# --------------------------------------------------------------------------- (c) transient backoff
async def test_transient_timeout_twice_then_success_with_exponential_backoff(monkeypatch):
    slept = recording_sleep(monkeypatch)
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise httpx.ReadTimeout("archives slow", request=request)
        return httpx.Response(200, content=b"csv")

    # Archives host: no priming, so every recorded request is a target GET (clean attempt count).
    client, seen = recording_client(handler)
    async with client:
        resp = await nse_get(client, ARCHIVES, timeout=5.0)

    assert resp.status_code == 200
    assert calls["n"] == 3                                  # two timeouts + one success
    assert seen == [ARCHIVES, ARCHIVES, ARCHIVES]
    assert slept == [1.0, 2.0]                              # bounded exponential backoff 1s, then 2s


async def test_transient_timeout_every_attempt_raises(monkeypatch):
    slept = recording_sleep(monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("archives down", request=request)

    client, seen = recording_client(handler)
    async with client:
        with pytest.raises(httpx.ReadTimeout):
            await nse_get(client, ARCHIVES, timeout=5.0)    # final failure re-raises for the feed's guard

    assert len(seen) == 3                                   # attempts=3 total tries, then re-raise
    assert slept == [1.0, 2.0]                              # slept before the 2nd and 3rd try only


# --------------------------------------------------------------------------- (d) archives: no priming
async def test_archives_url_never_fetches_homepage():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"csv")

    client, seen = recording_client(handler)
    async with client:
        resp = await nse_get(client, ARCHIVES, timeout=5.0)

    assert resp.status_code == 200
    assert seen == [ARCHIVES]                               # no cookie gate on the archives host
    assert all("www.nseindia.com" not in url for url in seen)
