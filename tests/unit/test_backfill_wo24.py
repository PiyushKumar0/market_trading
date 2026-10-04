"""WO-24b (2026-08-21): where the warm-up/backfill fetch runs, and what bounds it.

The WO-24 working hypothesis was that the 09:56 freeze came from a Kite historical call made while
holding the market store's serialization, with no timeout on it. The code says otherwise on both
counts, and these two tests are what pin the shape that was actually found, so a future refactor
cannot quietly introduce the hypothesised one:

* ``BackfillJob.warmup_gap`` awaits ``kite.historical`` as its own statement
  (``src/engine/marketdata/backfill.py:294``); the store is touched only by the separate
  ``acoverage_gaps`` / ``ainsert_bars_1m`` awaits around it (lines 284 and 296). The store's
  ``threading.RLock`` (``store.py:685``) is therefore never held across the network call -- the
  first test proves that behaviourally, with a fetch that hangs a worker thread exactly the way a
  blocked ``requests`` call does while a concurrent store read is issued.
* The fetch IS bounded: ``KiteClient.historical`` calls ``kc.historical_data`` under
  ``loop.run_in_executor`` (``kite_client.py:138``), and pykiteconnect passes its own
  ``_default_timeout`` (7 s) to ``requests`` on every request (``kiteconnect/connect.py:963``).
  ``SessionManager._connect`` (``session.py:301``) builds ``KiteConnect(api_key=...)`` with no
  ``timeout`` override, so that default is what the engine runs with. The second test pins it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import threading

import pytest
from kiteconnect import KiteConnect

from engine.core.config import Settings
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
    """A ``KiteClient`` stand-in whose historical fetch blocks a worker thread and stays there.

    Shaped like the real thing on purpose: ``KiteClient._call`` runs the synchronous pykiteconnect
    method through ``loop.run_in_executor``, so a wedged HTTP call holds an executor thread, not the
    event loop. That is the failure mode being simulated.
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
    """The fetch is OUTSIDE the store's serialized section, and this is what that buys.

    With the warm-up fill parked inside a fetch that will never return, an unrelated store read
    still answers. Had the fetch been made under the store's lock -- the WO-24 hypothesis -- this
    read would have queued behind it and the assertion below would time out, which is exactly the
    14-minute freeze the incident produced.
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

        # The store answers WHILE the fetch is wedged. (An empty store means every minute in the
        # span is a gap, which is also what got warmup_gap past its own coverage check above.)
        gaps = await asyncio.wait_for(store.acoverage_gaps(SYMBOL, frm, to), 10)
        assert len(gaps) == 45
        assert not task.done()                       # the fill really is still stuck in the fetch
    finally:
        release.set()

    report = await asyncio.wait_for(task, 10)
    assert kite.calls == 1
    assert report.bars_written == 0


def test_every_kite_historical_request_carries_an_explicit_timeout(monkeypatch):
    """A hung fetch has to end by itself, and it does: pykiteconnect hands ``requests`` a timeout on
    every call. Pinned end-to-end through ``historical_data`` rather than by reading the constant,
    so removing the pass-through (or constructing the client with ``timeout=None``) fails here."""
    kc = KiteConnect(api_key="k")
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
