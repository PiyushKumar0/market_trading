"""Where the warm-up/backfill fetch runs, and what bounds it.

* ``BackfillJob.warmup_gap`` awaits ``kite.historical`` as its own statement; the store is touched
  only by the separate ``acoverage_gaps`` / ``ainsert_bars_1m`` awaits around it, so the store's
  ``threading.RLock`` is never held across the network call. The first test proves that with a fetch
  that hangs a worker thread, as a blocked ``requests`` call does, while a concurrent store read is issued.
* The fetch IS bounded: ``KiteClient.historical`` runs ``kc.historical_data`` under
  ``loop.run_in_executor``, and pykiteconnect passes its own ``_default_timeout`` (7 s) to ``requests``
  on every request. ``SessionManager`` builds ``KiteConnect`` with no ``timeout`` override, so that
  default is what the engine runs with. The second test pins it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import threading

import pytest
from kiteconnect import KiteConnect

from engine.broker.session import SessionManager
from engine.core.config import Settings
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY
from engine.marketdata.backfill import BackfillJob
from engine.marketdata.store import MarketStore

TOKEN = 408065
SYMBOL = "RELIANCE"
SESSION_DAY = dt.date(2026, 6, 17)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "pq", clock)
    s.open()
    yield s
    s.close()


class HangingKite:
    """A ``KiteClient`` stand-in whose historical fetch blocks a worker thread until released.

    Like ``KiteClient._call``, a wedged HTTP call holds an executor thread, not the event loop.
    """

    def __init__(self, release: threading.Event, entered: asyncio.Event) -> None:
        self._release = release
        self._entered = entered
        self.calls = 0

    async def historical(self, token, frm, to, interval):
        self.calls += 1
        self._entered.set()
        await asyncio.get_running_loop().run_in_executor(None, self._release.wait)
        return []


async def test_a_hung_historical_fetch_never_blocks_a_concurrent_store_read(store, clock, conn):
    """The fetch is OUTSIDE the store's serialized section: with the warm-up fill parked in a fetch
    that never returns, an unrelated store read still answers.
    """
    release = threading.Event()
    entered = asyncio.Event()
    kite = HangingKite(release, entered)
    job = BackfillJob(store, kite, clock, Settings(), conn, lambda s: TOKEN)
    frm = clock.combine(SESSION_DAY, dt.time(9, 15))
    to = clock.combine(SESSION_DAY, dt.time(10, 0))

    task = asyncio.create_task(job.warmup_gap([SYMBOL], frm, to))
    try:
        await asyncio.wait_for(entered.wait(), 10)

        # An empty store means every minute in the span is a gap, which is what got warmup_gap
        # past its own coverage check.
        gaps = await asyncio.wait_for(store.acoverage_gaps(SYMBOL, frm, to), 10)
        assert len(gaps) == 45
        assert not task.done()                       # the fill is still stuck in the fetch
    finally:
        release.set()

    report = await asyncio.wait_for(task, 10)
    assert kite.calls == 1
    assert report.bars_written == 0


class _KeyOnlySecrets:
    """Just enough of ``Secrets`` for ``SessionManager``: an api_key, no access token yet."""

    def get(self, key: str) -> str:
        if key == KITE_API_KEY:
            return "k"
        raise KeyError(key)

    def has(self, key: str) -> bool:
        return key == KITE_API_KEY

    def get_optional(self, key: str) -> str | None:
        assert key == KITE_ACCESS_TOKEN
        return None


def test_every_kite_historical_request_carries_an_explicit_timeout(monkeypatch, clock):
    """pykiteconnect hands ``requests`` a timeout on every call. Pinned through the ENGINE's own
    client (``SessionManager.kite_connect()``) so an engine-side override, or pykiteconnect dropping
    the pass-through, fails here."""
    kc = SessionManager(_KeyOnlySecrets(), clock).kite_connect()
    kc.set_access_token("t")
    seen: dict = {}

    class Response:
        status_code = 200
        headers = {"content-type": "application/json"}
        content = b""

        @staticmethod
        def json() -> dict:
            return {"status": "success", "data": {"candles": []}}

    def fake_request(method, url, **kwargs):
        seen["method"] = method
        seen.update(kwargs)
        return Response()

    monkeypatch.setattr(kc.reqsession, "request", fake_request)

    candles = kc.historical_data(
        TOKEN, dt.datetime(2026, 6, 17, 9, 15), dt.datetime(2026, 6, 17, 10, 0), "minute"
    )

    assert candles == []
    assert seen["method"] == "GET"
    assert seen["timeout"] == kc.timeout
    assert 0 < float(seen["timeout"]) <= 30          # bounded, and bounded at a sane order
    assert KiteConnect._default_timeout == 7         # the value the engine actually runs with
