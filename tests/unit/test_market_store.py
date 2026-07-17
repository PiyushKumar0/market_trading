"""MarketStore (§4.1/§4.3/§4.5, E4): idempotent schema init for the full §4.3 table set, Decimal-exact
bar/tick round-trips (incl. ``src`` provenance + the A14 ``auction_open``), partitioned tick Parquet
batches, the §2.6 contiguous-coverage warm-up check, retention purges, and the Phase-1 additive
settings keys + notify-catalog messages."""

from __future__ import annotations

import datetime as dt
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import duckdb
import pytest
from pydantic import ValidationError

from engine.core.clock import Clock
from engine.core.config import load_settings
from engine.core.types import Bar, Tick
from engine.marketdata.store import EXPECTED_TABLES, MarketStore
from engine.notify.catalog import (
    MessageKind,
    backfill_report,
    catchup_report,
    data_freshness_frozen,
    engine_crashloop,
    reconcile_drift,
    warmup_frozen,
)
from tests.conftest import FIXED_NOW

# The complete §4.3 DuckDB table inventory. Adding a table to the schema without adding it here fails
# CI and vice-versa (the same lockstep guard as test_migrations for SQLite).
PLAN_TABLES = {
    "bars_1m", "corrections_log", "bars_1d", "reconcile_log", "instruments_daily",
    "universe_daily", "features_daily", "feature_snapshots", "news", "news_clusters",
    "entity_aliases", "unresolved_entities", "theme_map", "sentiment_agg", "catalyst_watchlist",
    "calendar", "corp_actions", "earnings_calendar", "flagged_instrument_days", "sector_map",
    # §2.8 corporate-filings layer (O14)
    "symbol_isin", "insider_trades", "shp_quarterly", "results_filings",
}


@pytest.fixture
def store(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    yield s
    s.close()


def _bar(minute: datetime, *, symbol: str = "RELIANCE", src: str = "self", **kw) -> Bar:
    return Bar(
        symbol=symbol, ts_minute=minute,
        open=kw.get("open", Decimal("2338.55")), high=kw.get("high", Decimal("2340.00")),
        low=kw.get("low", Decimal("2337.05")), close=kw.get("close", Decimal("2339.90")),
        volume=kw.get("volume", 12500), src=src, auction_open=kw.get("auction_open"),
    )


def _tick(ts: datetime, *, symbol: str = "RELIANCE", ltp: str = "2338.55", vol: int = 100) -> Tick:
    return Tick(
        instrument_token=738561, tradingsymbol=symbol, ltp=Decimal(ltp), volume_traded=vol,
        exchange_ts=ts, avg_price=Decimal("2338.1234"), bid=Decimal("2338.50"), ask=Decimal("2338.60"),
    )


# --------------------------------------------------------------------------- schema init
def test_double_init_idempotent_and_persistent(tmp_path, clock):
    s = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s.open()
    s.init_schema()                                   # second explicit init must be a no-op
    assert PLAN_TABLES <= s.table_names()
    assert PLAN_TABLES == set(EXPECTED_TABLES)        # lockstep: code inventory == plan inventory
    s.insert_bars_1m([_bar(FIXED_NOW.replace(second=0, microsecond=0))])
    s.close()

    # Re-open the same file: schema init is IF NOT EXISTS — existing data survives.
    s2 = MarketStore(tmp_path / "market.duckdb", tmp_path / "parquet", clock)
    s2.open()
    assert PLAN_TABLES <= s2.table_names()
    assert len(s2.get_bars_1m("RELIANCE", FIXED_NOW - timedelta(days=1), FIXED_NOW + timedelta(days=1))) == 1
    s2.close()


# --------------------------------------------------------------------------- bars_1m
def test_bar_roundtrip_decimal_fidelity_src_auction_open(store, clock):
    open_915 = clock.combine(clock.today(), time(9, 15))
    bars = [
        _bar(open_915, auction_open=Decimal("2338.10")),      # A14: auction open on the 09:15 row
        _bar(open_915 + timedelta(minutes=1), src="kite_official", open=Decimal("2339.05")),
    ]
    assert store.insert_bars_1m(bars) == 2

    got = store.get_bars_1m("RELIANCE", open_915, open_915 + timedelta(minutes=2))
    assert len(got) == 2
    b0, b1 = got
    # Decimal-exact round-trip (§3.2 money convention; DECIMAL(12,2) documented choice).
    assert isinstance(b0.open, Decimal) and b0.open == Decimal("2338.55")
    assert b0.close == Decimal("2339.90") and b0.volume == 12500
    assert b0.auction_open == Decimal("2338.10") and b1.auction_open is None
    assert b0.src == "self" and b1.src == "kite_official"
    # tz-aware IST timestamps, instant-equal to what went in.
    assert b0.ts_minute.tzinfo is not None and b0.ts_minute == open_915
    assert b1.ts_minute == open_915 + timedelta(minutes=1)


def test_bars_1m_upsert_official_replaces_self(store, clock):
    minute = clock.combine(clock.today(), time(10, 0))
    store.insert_bars_1m([_bar(minute, src="self")])
    store.insert_bars_1m([_bar(minute, src="kite_official", close=Decimal("2341.15"))])
    got = store.get_bars_1m("RELIANCE", minute, minute + timedelta(minutes=1))
    assert len(got) == 1                               # upsert, not duplicate (§4.4 job 2 canonical)
    assert got[0].src == "kite_official" and got[0].close == Decimal("2341.15")


def test_bar_src_is_constrained(store, clock):
    # Model-level: BarSrc is a closed Literal.
    with pytest.raises(ValidationError):
        Bar(symbol="X", ts_minute=FIXED_NOW, open=Decimal("1"), high=Decimal("1"),
            low=Decimal("1"), close=Decimal("1"), volume=0, src="bogus")
    # Store-level: the CHECK constraint rejects a rogue writer bypassing the model.
    with pytest.raises(duckdb.ConstraintException):
        store._execute(
            "INSERT INTO bars_1m (symbol, ts_minute, \"open\", high, low, \"close\", volume, src) "
            "VALUES ('X', ?, 1, 1, 1, 1, 0, 'bogus')", [FIXED_NOW],
        )


def test_naive_datetime_rejected_by_models():
    with pytest.raises(ValidationError):
        _bar(datetime(2026, 6, 17, 9, 15))             # naive ts_minute is a bug (§3.2)
    with pytest.raises(ValidationError):
        _tick(datetime(2026, 6, 17, 9, 15))            # naive exchange_ts is a bug (§3.2)


def test_last_bar_time_and_coverage_gap_check(store, clock):
    start = clock.combine(clock.today(), time(9, 15))
    minutes = [start + timedelta(minutes=i) for i in range(5) if i != 2]   # hole at 09:17
    store.insert_bars_1m([_bar(m) for m in minutes])

    assert store.last_bar_time("RELIANCE") == start + timedelta(minutes=4)
    assert store.last_bar_time("NOSUCH") is None

    end = start + timedelta(minutes=5)
    assert store.coverage_gaps("RELIANCE", start, end) == [start + timedelta(minutes=2)]
    assert store.has_contiguous_coverage("RELIANCE", start, end) is False  # §2.6 step-6 warm-up gate
    store.insert_bars_1m([_bar(start + timedelta(minutes=2))])
    assert store.has_contiguous_coverage("RELIANCE", start, end) is True


# --------------------------------------------------------------------------- bars_1d
def test_bars_1d_upsert_and_query(store):
    from engine.marketdata.store import DailyBar

    row = DailyBar(symbol="TCS", d=date(2026, 6, 16), open=Decimal("4100.00"), high=Decimal("4150.55"),
                   low=Decimal("4088.10"), close=Decimal("4120.35"), volume=1_000_000)
    store.upsert_bars_1d([row])
    # bhavcopy cross-check overwrites the same (symbol, d).
    store.upsert_bars_1d([row.model_copy(update={"close": Decimal("4121.00"), "src": "bhavcopy"})])
    got = store.get_bars_1d("TCS", date(2026, 6, 1), date(2026, 6, 30))
    assert len(got) == 1
    assert got[0].close == Decimal("4121.00") and got[0].src == "bhavcopy"


# --------------------------------------------------------------------------- ticks -> parquet (§4.3)
def test_tick_flush_writes_partitioned_parquet(store, tmp_path, clock):
    ts = clock.now()
    d = clock.today().isoformat()
    for i in range(3):
        store.buffer_tick(_tick(ts + timedelta(seconds=i), vol=100 + i))
    store.buffer_tick(_tick(ts, symbol="TCS", ltp="4100.05", vol=50))
    assert store.pending_tick_count == 4

    files = store.flush_ticks()
    assert store.pending_tick_count == 0
    # Partition layout: <root>/ticks/date=YYYY-MM-DD/symbol=X/<ulid>.parquet (§4.3).
    rel_dirs = {f.parent.relative_to(tmp_path / "parquet").as_posix() for f in files}
    assert rel_dirs == {f"ticks/date={d}/symbol=RELIANCE", f"ticks/date={d}/symbol=TCS"}
    assert all(f.suffix == ".parquet" and f.exists() for f in files)

    # Decimal + cumulative-volume + tz fidelity through the parquet round-trip (A13).
    back = store.get_ticks("RELIANCE", clock.today())
    assert [t.volume_traded for t in back] == [100, 101, 102]      # cumulative day volume preserved
    assert back[0].ltp == Decimal("2338.55") and isinstance(back[0].ltp, Decimal)
    assert back[0].avg_price == Decimal("2338.1234")
    assert back[0].exchange_ts == ts and back[0].exchange_ts.tzinfo is not None
    assert store.get_ticks("RELIANCE", date(2020, 1, 1)) == []


def test_tick_autoflush_on_batch_size(tmp_path, clock):
    s = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock, max_buffered_ticks=3)
    s.open()
    try:
        assert s.buffer_tick(_tick(clock.now())) == []
        assert s.buffer_tick(_tick(clock.now())) == []
        files = s.buffer_tick(_tick(clock.now()))              # 3rd tick trips the batch cap
        assert files and s.pending_tick_count == 0
    finally:
        s.close()


def test_tick_autoflush_on_interval(tmp_path):
    current = {"now": FIXED_NOW}
    clock = Clock(time_source=lambda: current["now"])
    s = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock, flush_interval_s=5.0)
    s.open()
    try:
        assert s.buffer_tick(_tick(FIXED_NOW)) == []           # 0s elapsed — buffered
        current["now"] = FIXED_NOW + timedelta(seconds=6)      # past the ~5s batch window (§4.3)
        files = s.buffer_tick(_tick(current["now"]))
        assert files and s.pending_tick_count == 0
    finally:
        s.close()


def test_compact_tick_partitions(store, clock):
    ts = clock.now()
    for i in range(3):                                          # 3 separate flushes = 3 small files
        store.buffer_tick(_tick(ts + timedelta(seconds=i), vol=i + 1))
        store.flush_ticks()
    day_dir = store._parquet_root / "ticks" / f"date={clock.today().isoformat()}" / "symbol=RELIANCE"
    assert len(list(day_dir.glob("*.parquet"))) == 3
    store.compact_tick_partitions(clock.today())
    assert len(list(day_dir.glob("*.parquet"))) == 1            # coalesced, data intact
    assert [t.volume_traded for t in store.get_ticks("RELIANCE", clock.today())] == [1, 2, 3]


# --------------------------------------------------------------------------- retention (§4.5)
def test_retention_purges_expired_only(tmp_path):
    current = {"now": FIXED_NOW - timedelta(days=120)}          # start 120 days in the past
    clock = Clock(time_source=lambda: current["now"])
    s = MarketStore(tmp_path / "m.duckdb", tmp_path / "pq", clock)
    s.open()
    try:
        old_day = current["now"]
        s.buffer_tick(_tick(old_day))
        s.flush_ticks()                                          # old tick partition (120d)
        s.append_correction("RELIANCE", old_day, old_day, Decimal("1.00"))   # old correction (120d)
        s.insert_news([{"title": "old", "source_domain": "et.com", "url": "https://e/1",
                        "published_at": FIXED_NOW - timedelta(days=400)}])   # >1y old headline

        current["now"] = FIXED_NOW                               # jump to "today"
        s.buffer_tick(_tick(FIXED_NOW))
        s.flush_ticks()                                          # fresh tick partition
        s.append_correction("RELIANCE", FIXED_NOW, FIXED_NOW, Decimal("2.00"))
        s.insert_news([{"title": "fresh", "source_domain": "et.com", "url": "https://e/2",
                        "published_at": FIXED_NOW - timedelta(days=1)}])

        report = s.apply_retention()
        assert report["tick_partitions"] == 1                    # 30d rolling: old day dir removed
        assert report["corrections_log"] == 1                    # 90d: only the old row purged
        assert report["news"] == 1                               # 1y: only the >365d headline purged

        ticks_root = tmp_path / "pq" / "ticks"
        assert not (ticks_root / f"date={old_day.date().isoformat()}").exists()
        assert (ticks_root / f"date={FIXED_NOW.date().isoformat()}").exists()
        titles = {n["title"] for n in s.get_news()}
        assert titles == {"fresh"}
        assert len(s.get_corrections(FIXED_NOW.date())) == 1
    finally:
        s.close()


# --------------------------------------------------------------------------- news pipeline surfaces
def test_news_insert_dedupes_on_url_and_clusters(store, clock):
    rows = [
        {"title": "TCS wins deal", "source_domain": "economictimes.com",
         "url": "https://et/1", "published_at": clock.now()},
        {"title": "TCS wins big deal", "source_domain": "moneycontrol.com",
         "url": "https://mc/1", "published_at": clock.now()},
    ]
    assert store.insert_news(rows) == 2
    assert store.insert_news(rows) == 0                          # idempotent backfill (§4.4 job 10)

    headlines = store.get_news(unclustered_only=True)
    assert len(headlines) == 2 and all(h["untrusted"] for h in headlines)   # §2.4: always untrusted

    store.set_news_cluster([h["headline_id"] for h in headlines], "01CLUSTER")
    assert store.get_news(unclustered_only=True) == []

    store.upsert_news_clusters([{
        "cluster_id": "01CLUSTER", "representative": "TCS wins deal",
        "source_domains": ["economictimes.com", "moneycontrol.com"],
        "first_seen": clock.now(), "last_seen": clock.now(),
    }])
    unscored = store.get_news_clusters(scored=False)
    assert len(unscored) == 1
    assert sorted(unscored[0]["source_domains"]) == ["economictimes.com", "moneycontrol.com"]
    assert store.get_news_clusters(scored=True) == []

    store.log_unresolved_entity("TRENT", "ambiguous", cluster_id="01CLUSTER",
                                candidate_symbols=["TRENT", "TRENTLTD"])
    logged = store.get_unresolved_entities()
    assert logged[0]["reason"] == "ambiguous" and logged[0]["candidate_symbols"] == ["TRENT", "TRENTLTD"]


def test_catalyst_watchlist_replace_is_idempotent_with_decimal_levels(store, clock):
    d = clock.today()
    row = {
        "symbol": "TCS", "grade": "originating", "direction": "long", "event_type": "earnings_beat",
        "cluster_refs": ["01CLUSTER"], "materiality": 0.8, "source_domain_count": 2,
        "confirm_trigger": Decimal("4130.00"), "invalidation": Decimal("4080.50"),
        "stop_band_low": Decimal("4075.00"), "stop_band_high": Decimal("4085.00"),
        "target_band_low": Decimal("4200.00"), "target_band_high": Decimal("4260.00"),
        "expires_at": d + timedelta(days=2),
    }
    store.replace_catalyst_watchlist(d, [row])
    store.replace_catalyst_watchlist(d, [row])                   # digest re-run: no duplicates (§2.6)
    got = store.get_catalyst_watchlist(d)
    assert len(got) == 1
    assert got[0]["grade"] == "originating" and got[0]["entry_id"]          # catalyst_ref minted
    assert got[0]["confirm_trigger"] == Decimal("4130.00")                  # deterministic levels exact
    assert store.get_catalyst_watchlist(d, grade="context") == []

    store.replace_catalyst_watchlist(d, [])                       # empty-but-fresh digest (§2.7)
    assert store.get_catalyst_watchlist(d) == []


# --------------------------------------------------------------------------- ancillary tables
def test_sector_map_snapshots_and_latest(store):
    store.upsert_sector_map(date(2026, 6, 7), [{"symbol": "TCS", "sector": "NIFTY IT"}])
    store.upsert_sector_map(date(2026, 6, 14), [{"symbol": "TCS", "sector": "NIFTY IT"},
                                                {"symbol": "SBIN", "sector": "NIFTY PSU BANK"}])
    assert len(store.get_sector_map()) == 2                       # overall latest snapshot
    assert len(store.get_sector_map(as_of=date(2026, 6, 10))) == 1  # latest snapshot <= as_of
    assert store.get_sector_map(as_of=date(2026, 1, 1)) == []


def test_reconcile_log_checkpoints(store, clock):
    d = date(2026, 6, 16)
    assert store.has_reconcile_entry(d) is False                  # §2.6: un-reconciled day detected
    store.append_reconcile_log([{
        "d": d, "symbol": "RELIANCE", "bars_self": 375, "bars_official": 375, "bars_compared": 370,
        "vol_drift_bars": 1, "close_drift_bars": 0, "offline_bars": 5, "bad_bar_fraction": 0.0027,
    }])
    assert store.has_reconcile_entry(d) is True
    assert store.reconciled_days(date(2026, 6, 15), date(2026, 6, 17)) == {d}
    entry = store.get_reconcile_log(d)[0]
    assert entry["offline_bars"] == 5 and entry["ran_at"] == clock.now()   # stamped via Clock

    store.append_correction("RELIANCE", clock.now(), clock.now(), Decimal("2339.90"),
                            cumulative_volume=125000, amended=True)
    corr = store.get_corrections(clock.today())[0]
    assert corr["value"] == Decimal("2339.90") and corr["amended"] is True


def test_generic_upserts_reject_unknown_columns(store):
    with pytest.raises(ValueError, match="unknown column"):
        store.upsert_universe_daily([{"d": date(2026, 6, 17), "symbol": "TCS", "included": True,
                                      "typo_col": 1}])


def test_instruments_universe_features_roundtrip(store, clock):
    d = clock.today()
    store.upsert_instruments_daily([{
        "d": d, "instrument_token": 738561, "tradingsymbol": "RELIANCE", "name": "RELIANCE INDUSTRIES",
        "tick_size": Decimal("0.05"), "lot_size": 1, "mis_eligible": True, "fno": True,
    }])
    inst = store.get_instruments_daily(d)[0]
    assert inst["tick_size"] == Decimal("0.05") and inst["surveillance"] is None   # clean = NULL (A8)

    store.upsert_universe_daily([
        {"d": d, "symbol": "RELIANCE", "included": True, "mis_candidate": True},
        {"d": d, "symbol": "PENNYCO", "included": False,
         "exclusion_reasons": ["below_traded_value", "asm_stage_1"]},
    ])
    assert [u["symbol"] for u in store.get_universe_daily(d, included_only=True)] == ["RELIANCE"]
    excluded = [u for u in store.get_universe_daily(d) if not u["included"]][0]
    assert excluded["exclusion_reasons"] == ["below_traded_value", "asm_stage_1"]

    store.upsert_features_daily([{"d": d, "symbol": "RELIANCE", "feature_set_version": 1,
                                  "features": '{"atr14": "12.35"}'}])
    assert store.get_features_daily(d, feature_set_version=1)[0]["features"] == '{"atr14": "12.35"}'
    store.insert_feature_snapshot("01SNAP", "RELIANCE", clock.now(), 1, '{"vwap_dist": "-0.2"}')
    snap = store.get_feature_snapshot("01SNAP")
    assert snap is not None and snap["ts"] == clock.now()
    assert store.get_feature_snapshot("NOPE") is None


# --------------------------------------------------------------------------- async wrappers
async def test_async_wrappers_offload_sync_core(store, clock):
    minute = clock.combine(clock.today(), time(10, 0))
    assert await store.ainsert_bars_1m([_bar(minute)]) == 1
    got = await store.aget_bars_1m("RELIANCE", minute, minute + timedelta(minutes=1))
    assert got[0].open == Decimal("2338.55")
    assert await store.alast_bar_time("RELIANCE") == minute
    assert await store.ahas_contiguous_coverage("RELIANCE", minute, minute + timedelta(minutes=1))
    assert await store.arun(store.table_names) >= PLAN_TABLES


# --------------------------------------------------------------------------- settings (additive keys)
def test_settings_load_with_new_phase1_keys():
    s = load_settings()
    assert (s.news.et_poll_s, s.news.mc_poll_s, s.news.gdelt_poll_s) == (300, 900, 900)
    assert s.news.backfill_lookback_h == 72 and s.news.gdelt_backfill_max_days == 90
    assert s.news.cluster_sim_threshold == 0.75 and s.news.feeds.et_markets_rss.startswith("https://")
    assert s.cat.fanout_weight == 0.5
    assert (s.reconcile.vol_drift_pct, s.reconcile.close_drift_ticks) == (2.0, 1)
    assert s.reconcile.max_bad_bar_fraction == 0.01
    assert (s.backfill.req_per_s, s.backfill.minute_chunk_days, s.backfill.day_chunk_days) == (3, 60, 2000)
    assert s.lifecycle.active_period_starts == [time(8, 0)]
    assert (s.lifecycle.start_grace_s, s.lifecycle.catchup_grace_s) == (900, 900)
    assert s.lifecycle.crashloop_window_s == 600
    assert s.universe.nifty200_seed_path == "config/universe/nifty200_seed.csv"
    assert s.universe.nifty200_source_url.startswith("https://")
    assert s.jobs.reconcile_ist == time(15, 50) and s.jobs.catalyst_digest_ist == time(8, 35)
    assert s.jobs.sector_map_weekly_day == "SUN"
    # Pre-existing keys still load (additive-only change).
    assert s.trade_window.start_ist == dt.time(10, 0) and s.data.minute_candles_adjusted is True


# --------------------------------------------------------------------------- notify catalog additions
def test_new_catalog_messages_shapes():
    cases = [
        (reconcile_drift(d="2026-06-16", symbols_flagged=["RELIANCE"], bars_compared=370,
                         bad_bar_fraction=0.02, max_bad_bar_fraction=0.01),
         MessageKind.RECONCILE_DRIFT, "warning"),
        (backfill_report(interval="minute", symbols=50, bars_written=18750, frm="2026-06-17 09:15",
                         to="2026-06-17 10:05", duration_s=42.0, failures=[]),
         MessageKind.BACKFILL_REPORT, "info"),
        (backfill_report(interval="minute", symbols=2, bars_written=0, frm="a", to="b",
                         duration_s=1.0, failures=["TCS"]),
         MessageKind.BACKFILL_REPORT, "warning"),
        (warmup_frozen(blockers=["orb:RELIANCE bars 12/30"]), MessageKind.WARMUP_FROZEN, "warning"),
        (catchup_report(off_duration_s=7200.0, jobs_caught_up=["bhavcopy", "reconcile"],
                        jobs_failed=[]),
         MessageKind.CATCHUP_REPORT, "info"),
        (engine_crashloop(restarts=4, window_s=600), MessageKind.ENGINE_CRASHLOOP, "critical"),
        (data_freshness_frozen(job_id="surveillance", last_success=None, reason="NSE page anti-bot"),
         MessageKind.DATA_FRESHNESS_FROZEN, "critical"),
    ]
    for msg, kind, severity in cases:
        assert msg.kind == kind and msg.severity == severity
        rendered = msg.render()
        for leak in ("kind=", "data={", "reply_keyboard=", "MessageKind."):
            assert leak not in rendered                     # no raw-model leak into owner prose (R8)
        assert msg.data                                     # structured fields preserved for audit
