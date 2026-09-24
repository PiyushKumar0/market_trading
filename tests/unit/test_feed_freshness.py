"""feed_freshness — the daily census that catches feeds which succeed but deliver nothing new."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from engine.core.calendar import NSECalendar
from engine.core.clock import IST, Clock
from engine.core.config import config_dir
from engine.marketdata.store import MarketStore
from engine.ops.feed_freshness import MAX_LAG, FeedFreshnessJob, feed_lag

#: Real 2026 calendar: Fri 06-19, Sat 06-20, Sun 06-21, Mon 06-22 (config/calendar/2026.yaml).
FRI, SAT, MON, TUE = date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 22), date(2026, 6, 23)


def _clock(d: date) -> Clock:
    return Clock(time_source=lambda: datetime(d.year, d.month, d.day, 21, 30, tzinfo=IST))


@pytest.fixture
def calendar(clock):
    return NSECalendar(config_dir() / "calendar", clock, strict=False)


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock).open()
    yield s
    s.close()


def _seed_all_fresh(store: MarketStore, d: date, *, news_day: date) -> None:
    """Every feed delivered on ``d``; news (a calendar-day feed, polled all day) on ``news_day``."""
    at = datetime(d.year, d.month, d.day, 18, 0, tzinfo=IST)
    news_at = datetime(news_day.year, news_day.month, news_day.day, 9, 0, tzinfo=IST)
    store._execute("INSERT INTO bars_1d (symbol, d, open, high, low, close, volume, src) "
                   "VALUES ('RELIANCE', ?, 1, 1, 1, 1, 1, 'bhavcopy')", [d])
    store._execute("INSERT INTO flagged_instrument_days (d, symbol, reason) VALUES (?, 'RELIANCE', 'bulk_deal')", [d])
    store._execute("INSERT INTO corp_actions (symbol, ex_date, kind, recorded_at) VALUES ('RELIANCE', ?, 'dividend', ?)",
                   [d, at])
    store.upsert_earnings_calendar(
        [{"symbol": "RELIANCE", "event_date": d, "kind": "results", "source": "nse", "recorded_at": at}]
    )
    for tag in ("", "bse:"):
        store._execute("INSERT INTO insider_trades (id, symbol, broadcast_dt, ingested_at) VALUES (?, 'RELIANCE', ?, ?)",
                       [f"{tag}x", at, at])
    store._execute("INSERT INTO shp_quarterly (symbol, qtr_end, category, ingested_at) "
                   "VALUES ('RELIANCE', ?, 'Grand Total', ?)", [date(2026, 3, 31), at])
    store._execute("INSERT INTO news (headline_id, title, source_domain, url, published_at, untrusted, ingested_at) "
                   "VALUES ('h1', 't', 'nseindia.com', 'https://x', ?, TRUE, ?)", [news_at, news_at])
    store._execute("INSERT INTO sector_map (as_of, symbol, sector) VALUES (?, 'RELIANCE', 'ENERGY')", [d])
    store.upsert_symbol_isin([{"symbol": "RELIANCE", "isin": "INE002A01018", "bse_scrip_code": "500325", "as_of": d}])


def test_every_censused_feed_has_a_lag_limit(store):
    assert set(store.feed_newest()) == set(MAX_LAG)


def test_trading_lag_skips_weekends_and_calendar_lag_does_not(calendar):
    assert feed_lag(FRI, MON, "trading", calendar) == 1          # Sat/Sun never count
    assert feed_lag(FRI, SAT, "trading", calendar) == 0
    assert feed_lag(FRI, MON, "calendar", calendar) == 3
    assert feed_lag(MON, FRI, "trading", calendar) == 0          # a future stamp is not "behind"


async def test_fresh_feeds_raise_nothing(store, calendar):
    _seed_all_fresh(store, FRI, news_day=MON)
    sent: list = []

    async def sink(msg):
        sent.append(msg)

    result = await FeedFreshnessJob(store, _clock(MON), calendar, notify=sink).run()   # Friday's data, Monday
    assert result.stale == () and sent == []


async def test_stale_and_empty_feeds_alert_once_per_streak(store, calendar):
    _seed_all_fresh(store, FRI, news_day=TUE)
    store._execute("DELETE FROM insider_trades WHERE id NOT LIKE 'bse:%'")        # the NSE route delivers nothing
    store._execute("DELETE FROM bars_1d")
    store._execute("INSERT INTO bars_1d (symbol, d, open, high, low, close, volume, src) "
                   "VALUES ('RELIANCE', DATE '2026-06-17', 1, 1, 1, 1, 1, 'bhavcopy')")   # Wed: Thu+Fri+Mon+Tue missed
    sent: list = []

    async def sink(msg):
        sent.append(msg)

    job = FeedFreshnessJob(store, _clock(TUE), calendar, notify=sink)
    result = await job.run()
    by_feed = {s.feed: s for s in result.stale}
    assert set(by_feed) == {"bhavcopy", "insider_nse"}
    assert by_feed["bhavcopy"].lag == 4 and by_feed["insider_nse"].newest is None
    assert len(sent) == 1 and "insider_nse: never delivered" in sent[0].body
    await job.run()                                               # same streak: no second alert
    assert len(sent) == 1

    store._execute("INSERT INTO bars_1d (symbol, d, open, high, low, close, volume, src) "
                   "VALUES ('RELIANCE', ?, 1, 1, 1, 1, 1, 'bhavcopy')", [TUE])
    await job.run()                                               # bars recover; NSE still dead: silent
    assert len(sent) == 1
    store._execute("DELETE FROM bars_1d WHERE d = ?", [TUE])
    await job.run()                                               # bars stale again: a new streak
    assert len(sent) == 2 and "bhavcopy" in sent[1].body
