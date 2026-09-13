"""DuckDB + Parquet market-data store (§4.1/§4.3/§4.5, E4) — the SINGLE WRITER for ``data/market.duckdb``.

``MarketStore`` owns every byte of the analytical store: the DuckDB tables of §4.3 (bars, features,
news, universe history, calendars, maps) and the tick Parquet dataset. **All DuckDB access in the
platform goes through this class** — DuckDB is a single-writer database and the §4.1 storage split
makes the engine that writer; no other module may open ``market.duckdb`` (the dashboard reads via the
engine API, never the file). Enforced socially by convention item 12 and physically by DuckDB's file
lock.

Price representation (documented choice per §4.3): **``DECIMAL(12,2)``** for every price column
(NSE cash equities quote at paise = 2 decimal places; the minimum tick is 0.05, A10), except the tick
``avg_price`` (exchange day-VWAP) which is ``DECIMAL(14,4)`` because Kite reports it at sub-paise
precision. DuckDB binds and returns Python ``decimal.Decimal`` natively for DECIMAL columns, so
prices round-trip through this API **exactly** — no float ever touches a price (§3.2 money
convention). JSON payload columns (``features``, ``extra``) store Decimals as strings (callers
serialize; mirrors the SQLite convention).

Threading / async model (§3.2 convention 4): the core is **synchronous** (DuckDB is native/CPU-bound
work) with every call serialized by an internal lock — safe because there is exactly one writer and
readers go through the same connection. Scan-heavy/bulky calls have thin ``a``-prefixed async
wrappers that offload to the store's OWN bounded worker pool so the asyncio loop is never blocked
(§2.2 heartbeat invariant); anything without a dedicated wrapper can be offloaded with
:meth:`MarketStore.arun`. Those wrappers deliberately do **not** use ``asyncio.to_thread``: that is
the process-wide default executor shared with every other offload, and on 2026-08-25 a slow tick
flush filled it with blocked threads and starved the entire intelligence layer for 4.4 h (WO-26a).
Store work now lives in ``mt-store`` (4 workers, reads/writes) and ``mt-flush`` (1 worker, the tick
flush), so neither can starve the other and nothing outside can starve either.

Tick Parquet dataset (§4.3 ``ticks``): raw FULL-mode frames (cumulative volume + depth top, A13) are
buffered in memory and flushed every ``flush_interval_s`` (60 s default) or ``max_buffered_ticks``, whichever
first, into ``<parquet_root>/ticks/date=YYYY-MM-DD/symbol=<SYM>/<ulid>.parquet``. Each flush writes
one file per (date, symbol) present in the batch; :meth:`compact_tick_partitions` coalesces a day's
small batch files into one file per symbol (EOD job) so the 30-day retention window stays a sane file
count. Timestamps are ``TIMESTAMPTZ`` with the session timezone pinned to Asia/Kolkata; every
datetime returned by this API is normalized tz-aware IST (§3.2).

Retention (§4.5): ticks 30 days rolling, news + clusters + sentiment 1 year, corrections 90 days —
:meth:`apply_retention` (plan-pinned constants, not tunables). 1-minute bars are kept 5 years and
daily bars indefinitely — no purge implemented for them here.
"""

from __future__ import annotations

import asyncio
import functools
import shutil
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from ulid import ULID

from engine.core.clock import IST, Clock
from engine.core.config import Settings
from engine.core.log import get_logger
from engine.core.types import Bar, Tick

_log = get_logger("engine.marketdata.store")

T = TypeVar("T")

#: Hard ceiling on the LIVE store connection's DuckDB memory (§3.2.11, generalizing the 53 GB incident
#: of 2026-08-17). DuckDB's default is ~80% of RAM (~25 GB here), so one bad scan can commit the whole
#: machine while the engine is trading — and ``POST /db/query`` now runs owner SQL on this very
#: instance. Past the limit DuckDB spills to its temp directory instead of taking the RAM. Higher than
#: ``tick_compact._MEMORY_LIMIT`` (4 GB) on purpose: this connection serves the session, that one is a
#: maintenance job that must never compete with it.
_MEMORY_LIMIT = "8GB"

#: Width of the store's private worker pool (``mt-store``), WO-26a. Every DuckDB call serializes on
#: ``_lock`` anyway, so this is not a parallelism knob — it is a QUEUE depth: enough that the health
#: probe and a scan read are never behind the same single worker, small enough that a wedged store
#: cannot bloom threads. 4 is two concurrent readers plus headroom for the probe and one job.
_STORE_EXECUTOR_WORKERS = 4

#: Quiet window between ``flush_skipped_in_flight`` lines (seconds). Skips are the DESIGNED response
#: to a burst (see :meth:`MarketStore.flush_ticks`), so one line per skip would mean one line per
#: tick during exactly the storm the skip exists to survive. The counter carries the real signal.
_FLUSH_SKIP_LOG_EVERY_S = 60.0

#: How long :meth:`MarketStore.close` waits for an in-flight background flush before giving up on it
#: (seconds). It is the one caller that must not skip — the connection is about to go — but shutdown
#: must not hang on a wedged flush either, so the wait is bounded and the give-up is logged.
_CLOSE_FLUSH_WAIT_S = 15.0

#: Slow-statement telemetry threshold (seconds), §2.6 "Store slow-statement telemetry" (WO-24b
#: follow-up). The 2026-09-08 11:47 mid-session stall's thread dump (store_stall_stacks) is taken
#: sequentially and could not tell whether a feature_snapshot INSERT (``_execute``) or a tick-flush
#: partition COPY (``_flush_locked``) was the statement actually holding ``_lock`` for 59 s. Timed
#: (see :func:`_note_slow_statement`): every hold taken by the statement helpers —
#: :meth:`MarketStore._execute`, :meth:`MarketStore._fetchall`, :meth:`MarketStore._fetch_dicts`,
#: :meth:`MarketStore._bulk_write`, :meth:`MarketStore._upsert_rows` and the per-partition COPY in
#: :meth:`MarketStore._flush_locked` — PLUS the hand-rolled holds that run SQL of their own:
#: :meth:`MarketStore.amend_bar_1m_extremes`, the ``_tick_stage`` DELETE at the head of
#: :meth:`MarketStore._flush_locked`, :meth:`MarketStore.insert_news`,
#: :meth:`MarketStore.set_news_cluster` and :meth:`MarketStore.compact_tick_partitions`.
#: :meth:`MarketStore.init_schema` is the ONE hold left
#: deliberately untimed (with the :meth:`MarketStore.open` hold that wraps it) — boot-only DDL that no
#: trading-session caller can ever be queued behind, so a slow hold there has no victim to name.
#: :meth:`MarketStore.ping` is not a site either: it IS the wedge detector (a ``SELECT 1`` that does
#: not return is the signal), so timing it would only re-report what the health monitor already sees.
#: A single hold at or above this threshold logs ``store_slow_statement`` so the NEXT stall names its
#: statement without needing a lucky thread dump.
_SLOW_STATEMENT_S = 5.0

# ---------------------------------------------------------------------- retention (§4.5, plan-pinned)
TICKS_RETENTION_DAYS = 30          # raw tick Parquet — enough to calibrate the fill model (R9)
NEWS_RETENTION_DAYS = 365          # news + sentiment scores
CORRECTIONS_RETENTION_DAYS = 90    # late-tick corrections log — bounded, not unbounded (§4.3)


class DailyBar(BaseModel):
    """A daily OHLCV bar (§4.3 ``bars_1d``) from Kite historical (adjusted per A11 finding: Kite
    minute+daily candles ARE corp-action adjusted) with bhavcopy cross-check (``src``)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    d: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = Field(ge=0)
    src: str = "kite_official"     # kite_official | bhavcopy


# ---------------------------------------------------------------------- §4.3 DDL (idempotent)
# Every statement is CREATE ... IF NOT EXISTS so init_schema() is safely re-runnable on every open.
_SCHEMA: tuple[str, ...] = (
    # bars_1m — OHLCV from cumulative-volume deltas (A13); src provenance; auction_open on the
    # 09:15 row only (A14). PK (symbol, ts_minute) so reconcile can upsert official rows (§4.4 job 2).
    """
    CREATE TABLE IF NOT EXISTS bars_1m (
        symbol       TEXT NOT NULL,
        ts_minute    TIMESTAMPTZ NOT NULL,
        "open"       DECIMAL(12,2) NOT NULL,
        high         DECIMAL(12,2) NOT NULL,
        low          DECIMAL(12,2) NOT NULL,
        "close"      DECIMAL(12,2) NOT NULL,
        volume       BIGINT NOT NULL,
        src          TEXT NOT NULL CHECK (src IN ('self','kite_official','gap_backfilled')),
        auction_open DECIMAL(12,2),
        PRIMARY KEY (symbol, ts_minute)
    )
    """,
    # corrections_log — late ticks past minute+5s grace (§4.4 job 1): symbol, minute, tick_ts, value,
    # plus whether the late tick amended its bar before the nightly reconcile. 90-day retention.
    # ``reason`` (nullable) records WHY an unamended late tick was refused — notably
    # 'official_bar_untouchable' when the target row is no longer src='self' (§3.2.3 amendment rules).
    # It is the LAST column so a fresh DB and a DB widened by _migrate_corrections_reason (which can
    # only append) carry identical column order.
    """
    CREATE TABLE IF NOT EXISTS corrections_log (
        symbol            TEXT NOT NULL,
        minute            TIMESTAMPTZ NOT NULL,
        tick_ts           TIMESTAMPTZ NOT NULL,
        value             DECIMAL(12,2),
        cumulative_volume BIGINT,
        amended           BOOLEAN NOT NULL DEFAULT FALSE,
        logged_at         TIMESTAMPTZ NOT NULL,
        reason            TEXT
    )
    """,
    # bars_1d — Kite historical (adjusted, A11) + bhavcopy cross-check.
    """
    CREATE TABLE IF NOT EXISTS bars_1d (
        symbol  TEXT NOT NULL,
        d       DATE NOT NULL,
        "open"  DECIMAL(12,2) NOT NULL,
        high    DECIMAL(12,2) NOT NULL,
        low     DECIMAL(12,2) NOT NULL,
        "close" DECIMAL(12,2) NOT NULL,
        volume  BIGINT NOT NULL,
        src     TEXT NOT NULL DEFAULT 'kite_official',
        PRIMARY KEY (symbol, d)
    )
    """,
    # reconcile_log — nightly self-vs-official drift per (day, symbol) (A13); the per-day checkpoint
    # driving the §2.6 startup catch-up ("any past trading day lacking a reconcile_log entry").
    # offline_bars counts the span excluded from the drift denominator (gap-backfilled, not drift).
    """
    CREATE TABLE IF NOT EXISTS reconcile_log (
        d                DATE NOT NULL,
        symbol           TEXT NOT NULL,
        bars_self        INTEGER,
        bars_official    INTEGER,
        bars_compared    INTEGER,
        vol_drift_bars   INTEGER,
        close_drift_bars INTEGER,
        offline_bars     INTEGER,
        bad_bar_fraction DOUBLE,
        alerted          BOOLEAN DEFAULT FALSE,
        ran_at           TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (d, symbol)
    )
    """,
    # instruments_daily — full Kite dump snapshot per day incl. tick_size (A10), MIS leverage,
    # surveillance flags (A8), F&O membership (C7). `extra` = JSON overflow of the raw dump row.
    # tick_size is DECIMAL(18,6), NOT (10,2) (2026-07-21 lossless-hydrate incident): the daily Kite dump
    # carries sub-₹0.01 ticks (currency/commodity derivatives at ₹0.0025) that a (10,2) column silently
    # truncated to 0.00 on write; hydrate() then rebuilt an Instrument from 0.00 and the `tick_size > 0`
    # model REJECTED it (8072 skips, any legacy tradable-lane index row among them). Scale 6 represents
    # every published NSE tick. Existing (10,2) DBs are widened once by _migrate_instruments_tick_scale.
    """
    CREATE TABLE IF NOT EXISTS instruments_daily (
        d                DATE NOT NULL,
        instrument_token BIGINT NOT NULL,
        tradingsymbol    TEXT NOT NULL,
        name             TEXT,
        exchange         TEXT,
        segment          TEXT,
        instrument_type  TEXT,
        tick_size        DECIMAL(18,6),
        lot_size         INTEGER,
        mis_leverage     DOUBLE,
        mis_eligible     BOOLEAN,
        surveillance     TEXT,
        fno              BOOLEAN,
        extra            TEXT,
        PRIMARY KEY (d, instrument_token)
    )
    """,
    # universe_daily — resolved universe + mis_candidates + exclusion reasons (auditable, §4.3).
    """
    CREATE TABLE IF NOT EXISTS universe_daily (
        d                   DATE NOT NULL,
        symbol              TEXT NOT NULL,
        included            BOOLEAN NOT NULL,
        mis_candidate       BOOLEAN NOT NULL DEFAULT FALSE,
        exclusion_reasons   TEXT[],
        median_traded_value DECIMAL(18,2),
        PRIMARY KEY (d, symbol)
    )
    """,
    # features_daily — §6.2 feature set as JSON (Decimals as strings), versioned (feature_set_version).
    """
    CREATE TABLE IF NOT EXISTS features_daily (
        d                   DATE NOT NULL,
        symbol              TEXT NOT NULL,
        feature_set_version INTEGER NOT NULL,
        features            TEXT NOT NULL,
        PRIMARY KEY (d, symbol, feature_set_version)
    )
    """,
    # feature_snapshots — keyed by features_snapshot_id referenced in proposals/ledger (§4.3).
    """
    CREATE TABLE IF NOT EXISTS feature_snapshots (
        snapshot_id         TEXT PRIMARY KEY,
        symbol              TEXT NOT NULL,
        ts                  TIMESTAMPTZ NOT NULL,
        feature_set_version INTEGER NOT NULL,
        features            TEXT NOT NULL
    )
    """,
    # news — raw headlines, HEADLINE-LEVEL ONLY (§2.7 step 1); untrusted=true always (§2.4);
    # LLM scores live on the CLUSTER, never the headline (§5.4). 1-year retention.
    """
    CREATE TABLE IF NOT EXISTS news (
        headline_id   TEXT PRIMARY KEY,
        title         TEXT NOT NULL,
        source_domain TEXT NOT NULL,
        url           TEXT NOT NULL,
        published_at  TIMESTAMPTZ NOT NULL,
        cluster_id    TEXT,
        untrusted     BOOLEAN NOT NULL DEFAULT TRUE,
        ingested_at   TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_news_url ON news(url)",
    "CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at)",
    # news_clusters — §2.7 step 2 output + step-4 LLM scores. source_domains is the DISTINCT set
    # (the §7.1 catalyst_guard.min_source_domains corroboration input); symbols[] is EntityResolver
    # output ONLY (the LLM never assigns a symbol, §2.7 step 3). Replay/backtest consume these
    # persisted scores — the LLM is never re-invoked for a past cluster (R8/§9.6).
    """
    CREATE TABLE IF NOT EXISTS news_clusters (
        cluster_id     TEXT PRIMARY KEY,
        representative TEXT NOT NULL,
        source_domains TEXT[] NOT NULL,
        first_seen     TIMESTAMPTZ NOT NULL,
        last_seen      TIMESTAMPTZ NOT NULL,
        scope          TEXT,
        entities       TEXT[],
        symbols        TEXT[],
        sectors        TEXT[],
        themes         TEXT[],
        sentiment      DOUBLE,
        materiality    DOUBLE,
        event_type     TEXT,
        novelty        DOUBLE,
        scored_at      TIMESTAMPTZ,
        scorer_model   TEXT,
        untrusted      BOOLEAN NOT NULL DEFAULT TRUE
    )
    """,
    # entity_aliases — alias -> tradingsymbol seed + curated additions (§4.3). PK allows one alias
    # mapping to MULTIPLE symbols; the EntityResolver treats that as AMBIGUOUS ⇒ no match (§3.2.4).
    """
    CREATE TABLE IF NOT EXISTS entity_aliases (
        alias         TEXT NOT NULL,
        tradingsymbol TEXT NOT NULL,
        source        TEXT NOT NULL DEFAULT 'seed',
        added_at      TIMESTAMPTZ,
        PRIMARY KEY (alias, tradingsymbol)
    )
    """,
    # unresolved_entities — §3.2.4 EntityResolver log target for ambiguous / out-of-universe /
    # unmatched entity strings; feeds the weekly alias-map suggestion loop (§5.5). Append-only.
    """
    CREATE TABLE IF NOT EXISTS unresolved_entities (
        entity_text       TEXT NOT NULL,
        cluster_id        TEXT,
        reason            TEXT NOT NULL CHECK (reason IN ('ambiguous','out_of_universe','no_match')),
        candidate_symbols TEXT[],
        logged_at         TIMESTAMPTZ NOT NULL
    )
    """,
    # theme_map — theme -> {keywords[], symbols[]} (seed config/themes.yaml; owner-approved updates).
    """
    CREATE TABLE IF NOT EXISTS theme_map (
        theme      TEXT PRIMARY KEY,
        keywords   TEXT[] NOT NULL,
        symbols    TEXT[] NOT NULL,
        updated_at TIMESTAMPTZ
    )
    """,
    # sentiment_agg — clipped decay-weighted SUM per (scope, scope_key, as_of) (§2.7 step 5(i));
    # the §6.2 features-v2 source. Not money ⇒ DOUBLE.
    # ``raw_sum``/``n_clusters`` (both nullable, WO-22) are the SATURATION MEASUREMENT: the UNCLIPPED
    # sum and how many clusters produced it. ``value`` stays clip(raw_sum, −1, +1) — the rail told a
    # reader nothing about how far past it the flow ran, and it railed on 7 of 17 digest days. They
    # are the LAST columns so a fresh DB and a DB widened by _migrate_sentiment_measures (which can
    # only append) carry identical column order.
    """
    CREATE TABLE IF NOT EXISTS sentiment_agg (
        scope      TEXT NOT NULL CHECK (scope IN ('symbol','sector','theme','market')),
        scope_key  TEXT NOT NULL,
        as_of      TIMESTAMPTZ NOT NULL,
        value      DOUBLE NOT NULL,
        raw_sum    DOUBLE,
        n_clusters INTEGER,
        PRIMARY KEY (scope, scope_key, as_of)
    )
    """,
    # catalyst_watchlist — §2.7 step-5(ii) output, per trading day; the `cat` scanner's ONLY news
    # input (single seam, O11). entry_id is the catalyst_ref carried on SignalCandidate (§3.2.5/§6.5).
    # Levels are DETERMINISTIC (§6.1 `cat` rules) and exact — DECIMAL(12,2).
    """
    CREATE TABLE IF NOT EXISTS catalyst_watchlist (
        entry_id            TEXT PRIMARY KEY,
        d                   DATE NOT NULL,
        symbol              TEXT NOT NULL,
        grade               TEXT NOT NULL CHECK (grade IN ('originating','context')),
        direction           TEXT,
        event_type          TEXT,
        cluster_refs        TEXT[],
        materiality         DOUBLE,
        source_domain_count INTEGER,
        event_age_h         DOUBLE,
        event_age_sessions  INTEGER,
        confirm_trigger     DECIMAL(12,2),
        invalidation        DECIMAL(12,2),
        stop_band_low       DECIMAL(12,2),
        stop_band_high      DECIMAL(12,2),
        target_band_low     DECIMAL(12,2),
        target_band_high    DECIMAL(12,2),
        expires_at          DATE,
        -- §2.7 `cat_reversal` (2026-08-27): cluster_id of the EARLIER opposite-direction (short)
        -- cluster of this same (symbol, event_type) story that the row's winning cluster reverses;
        -- NULL on every ordinary row. Nullable and LAST so a fresh DB and a DB widened by
        -- _migrate_watchlist_reversal share one column order.
        reversal_of         TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_watchlist_day ON catalyst_watchlist(d, symbol)",
    # calendar — trading days + session times + muhurat/shortened flags (R6); YAML-sourced.
    """
    CREATE TABLE IF NOT EXISTS calendar (
        d              DATE PRIMARY KEY,
        is_trading_day BOOLEAN NOT NULL,
        session_open   TEXT,
        session_close  TEXT,
        is_muhurat     BOOLEAN DEFAULT FALSE,
        is_shortened   BOOLEAN DEFAULT FALSE,
        note           TEXT
    )
    """,
    # corp_actions — ex-dates/splits/bonuses/dividends -> GTT adjustment + ledger attribution (A12).
    """
    CREATE TABLE IF NOT EXISTS corp_actions (
        symbol      TEXT NOT NULL,
        ex_date     DATE NOT NULL,
        kind        TEXT NOT NULL,
        ratio       TEXT,
        amount      DECIMAL(12,2),
        source      TEXT,
        recorded_at TIMESTAMPTZ,
        PRIMARY KEY (symbol, ex_date, kind)
    )
    """,
    # earnings_calendar — results/board-meeting dates -> R2 no-trade windows + §6.1 `cat` T+1 PEAD (O13).
    """
    CREATE TABLE IF NOT EXISTS earnings_calendar (
        symbol      TEXT NOT NULL,
        event_date  DATE NOT NULL,
        kind        TEXT NOT NULL DEFAULT 'results',
        source      TEXT,
        recorded_at TIMESTAMPTZ,
        PRIMARY KEY (symbol, event_date, kind)
    )
    """,
    # flagged_instrument_days — bulk/block-deal days; scanners suppress volume-breakout signals (§4).
    """
    CREATE TABLE IF NOT EXISTS flagged_instrument_days (
        symbol  TEXT NOT NULL,
        d       DATE NOT NULL,
        reason  TEXT NOT NULL,
        details TEXT,
        PRIMARY KEY (symbol, d, reason)
    )
    """,
    # sector_map — weekly snapshot; the deterministic source for §7.1 per_sector_exposure and the
    # §2.7 sector fan-out (the Kite dump carries no sector field). UNCLASSIFIED handled upstream.
    """
    CREATE TABLE IF NOT EXISTS sector_map (
        as_of  DATE NOT NULL,
        symbol TEXT NOT NULL,
        sector TEXT NOT NULL,
        PRIMARY KEY (as_of, symbol)
    )
    """,
    # ============================================================ §2.8 corporate-filings layer (O14)
    # Every row carries broadcast/dissemination point-in-time timestamps + an ``ingested_at`` stamp;
    # money is DECIMAL (never float), consistent with the price columns above (§2.8.1, §3.2 money).
    # symbol_isin — the stable cross-exchange join key: NIFTY-constituents ISIN + resolved BSE scrip
    # code (nullable until PeerSmartSearch resolves it). ISINs survive symbol renames (§2.8.1).
    """
    CREATE TABLE IF NOT EXISTS symbol_isin (
        symbol         TEXT PRIMARY KEY,
        isin           TEXT NOT NULL,
        bse_scrip_code TEXT,
        as_of          DATE NOT NULL,
        ingested_at    TIMESTAMPTZ NOT NULL
    )
    """,
    # insider_trades — NSE PIT structured rows. PK ``id`` = sha256 content hash of
    # (symbol, person_name, broadcast_dt, txn_type, qty, value) so amended/duplicate broadcasts of
    # the SAME transaction collapse to one row and a genuinely different transaction never collides
    # (§2.8.1 / edge case: content-hash PKs, latest wins).
    """
    CREATE TABLE IF NOT EXISTS insider_trades (
        id              TEXT PRIMARY KEY,
        symbol          TEXT NOT NULL,
        person_name     TEXT,
        person_category TEXT,
        acq_mode        TEXT,
        txn_type        TEXT,
        qty             BIGINT,
        value           DECIMAL(16,2),
        before_pct      DOUBLE,
        after_pct       DOUBLE,
        txn_from        DATE,
        txn_to          DATE,
        intim_dt        DATE,
        broadcast_dt    TIMESTAMPTZ,
        xbrl            TEXT,
        ingested_at     TIMESTAMPTZ NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_insider_symbol ON insider_trades(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_insider_broadcast ON insider_trades(broadcast_dt)",
    # shp_quarterly — SEBI-format shareholding pattern per (symbol, quarter, category), incl. the
    # per-category pledged/encumbered + locked shares (§2.8.1). Source ``bse`` (detail stack) or
    # ``nse`` (master, freshness only). ``revised`` marks a re-filed quarter (latest wins).
    """
    CREATE TABLE IF NOT EXISTS shp_quarterly (
        symbol         TEXT NOT NULL,
        qtr_end        DATE NOT NULL,
        category       TEXT NOT NULL,
        holders        BIGINT,
        shares         BIGINT,
        pct            DOUBLE,
        pledged_shares BIGINT,
        pledged_pct    DOUBLE,
        locked_shares  BIGINT,
        broadcast_dt   TIMESTAMPTZ,
        source         TEXT,
        revised        BOOLEAN DEFAULT FALSE,
        ingested_at    TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (symbol, qtr_end, category)
    )
    """,
    # results_filings — NSE financial-results filing METADATA (line items NULL in stage 1; §2.8.4
    # stages 2). PK (symbol, period_end, consolidated) keeps standalone + consolidated as distinct
    # rows (both stored, consolidated preferred downstream — §2.8 edge cases). ``broadcast_dt`` is the
    # point-in-time timestamp every as-of join keys on (never the period label — period labels lie).
    """
    CREATE TABLE IF NOT EXISTS results_filings (
        symbol       TEXT NOT NULL,
        period_end   DATE NOT NULL,
        consolidated BOOLEAN NOT NULL,
        audited      BOOLEAN,
        broadcast_dt TIMESTAMPTZ,
        exchdiss_dt  TIMESTAMPTZ,
        xbrl         TEXT,
        revenue      DECIMAL(18,2),
        pat          DECIMAL(18,2),
        eps          DECIMAL(12,4),
        ingested_at  TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (symbol, period_end, consolidated)
    )
    """,
)

#: All §4.3 DuckDB tables created by :meth:`MarketStore.init_schema` (kept in lockstep with tests).
EXPECTED_TABLES: frozenset[str] = frozenset({
    "bars_1m", "corrections_log", "bars_1d", "reconcile_log", "instruments_daily",
    "universe_daily", "features_daily", "feature_snapshots", "news", "news_clusters",
    "entity_aliases", "unresolved_entities", "theme_map", "sentiment_agg", "catalyst_watchlist",
    "calendar", "corp_actions", "earnings_calendar", "flagged_instrument_days", "sector_map",
    # §2.8 corporate-filings layer (O14)
    "symbol_isin", "insider_trades", "shp_quarterly", "results_filings",
})

# Pinned column sets for the dict-row upsert/read APIs: table -> (columns, pk_columns).
# A row dict may omit non-PK columns (NULL) but an unknown key is a hard error (catches typos).
_TABLE_SPEC: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "instruments_daily": (
        ("d", "instrument_token", "tradingsymbol", "name", "exchange", "segment", "instrument_type",
         "tick_size", "lot_size", "mis_leverage", "mis_eligible", "surveillance", "fno", "extra"),
        ("d", "instrument_token"),
    ),
    "universe_daily": (
        ("d", "symbol", "included", "mis_candidate", "exclusion_reasons", "median_traded_value"),
        ("d", "symbol"),
    ),
    "features_daily": (
        ("d", "symbol", "feature_set_version", "features"),
        ("d", "symbol", "feature_set_version"),
    ),
    "news_clusters": (
        ("cluster_id", "representative", "source_domains", "first_seen", "last_seen", "scope",
         "entities", "symbols", "sectors", "themes", "sentiment", "materiality", "event_type",
         "novelty", "scored_at", "scorer_model", "untrusted"),
        ("cluster_id",),
    ),
    "entity_aliases": (
        ("alias", "tradingsymbol", "source", "added_at"),
        ("alias", "tradingsymbol"),
    ),
    "theme_map": (
        ("theme", "keywords", "symbols", "updated_at"),
        ("theme",),
    ),
    "sentiment_agg": (
        ("scope", "scope_key", "as_of", "value", "raw_sum", "n_clusters"),
        ("scope", "scope_key", "as_of"),
    ),
    "catalyst_watchlist": (
        ("entry_id", "d", "symbol", "grade", "direction", "event_type", "cluster_refs",
         "materiality", "source_domain_count", "event_age_h", "event_age_sessions",
         "confirm_trigger", "invalidation", "stop_band_low", "stop_band_high",
         "target_band_low", "target_band_high", "expires_at", "reversal_of"),
        ("entry_id",),
    ),
    "calendar": (
        ("d", "is_trading_day", "session_open", "session_close", "is_muhurat", "is_shortened", "note"),
        ("d",),
    ),
    "corp_actions": (
        ("symbol", "ex_date", "kind", "ratio", "amount", "source", "recorded_at"),
        ("symbol", "ex_date", "kind"),
    ),
    "earnings_calendar": (
        ("symbol", "event_date", "kind", "source", "recorded_at"),
        ("symbol", "event_date", "kind"),
    ),
    "flagged_instrument_days": (
        ("symbol", "d", "reason", "details"),
        ("symbol", "d", "reason"),
    ),
    "sector_map": (
        ("as_of", "symbol", "sector"),
        ("as_of", "symbol"),
    ),
    "reconcile_log": (
        ("d", "symbol", "bars_self", "bars_official", "bars_compared", "vol_drift_bars",
         "close_drift_bars", "offline_bars", "bad_bar_fraction", "alerted", "ran_at"),
        ("d", "symbol"),
    ),
    # ------------------------------------------------------------------ §2.8 corporate-filings layer
    "symbol_isin": (
        ("symbol", "isin", "bse_scrip_code", "as_of", "ingested_at"),
        ("symbol",),
    ),
    "insider_trades": (
        ("id", "symbol", "person_name", "person_category", "acq_mode", "txn_type", "qty", "value",
         "before_pct", "after_pct", "txn_from", "txn_to", "intim_dt", "broadcast_dt", "xbrl",
         "ingested_at"),
        ("id",),
    ),
    "shp_quarterly": (
        ("symbol", "qtr_end", "category", "holders", "shares", "pct", "pledged_shares", "pledged_pct",
         "locked_shares", "broadcast_dt", "source", "revised", "ingested_at"),
        ("symbol", "qtr_end", "category"),
    ),
    "results_filings": (
        ("symbol", "period_end", "consolidated", "audited", "broadcast_dt", "exchdiss_dt", "xbrl",
         "revenue", "pat", "eps", "ingested_at"),
        ("symbol", "period_end", "consolidated"),
    ),
}

# Tables whose PK is a CONTENT HASH of the row (identical id ⇒ identical row). Conflict action for
# these is DO NOTHING, never DO UPDATE: the update would rewrite equal values, and that no-op
# rewrite tripped DuckDB's ART index fault on 2026-07-23 (insider_trades catch-up re-upsert →
# "Failed to delete all rows from index" → connection-invalidating FATAL).
_CONTENT_HASH_PK_TABLES: frozenset[str] = frozenset({"insider_trades"})

# Values applied for keys OMITTED from a row dict (an explicit ``?`` NULL would otherwise override
# the column DEFAULT). Mirrors the DDL defaults above — keep the two in lockstep.
_TABLE_DEFAULTS: dict[str, dict[str, Any]] = {
    "universe_daily": {"mis_candidate": False},
    "news_clusters": {"untrusted": True},
    "entity_aliases": {"source": "seed"},
    "calendar": {"is_muhurat": False, "is_shortened": False},
    "earnings_calendar": {"kind": "results"},
    "reconcile_log": {"alerted": False},
    "shp_quarterly": {"revised": False},          # mirrors the DDL default (§2.8.1)
}

# ---------------------------------------------------------------------- late-tick amendment outcomes
# Returned by :meth:`MarketStore.amend_bar_1m_extremes` (the §3.2.3 read-decide-write seam). The STORE
# owns atomicity and the src precondition; the CALLER owns what each outcome means in corrections_log.
AMEND_APPLIED = "amended"          # high/low widened to include the late print
AMEND_IN_RANGE = "in_range"        # print already inside [low, high] — nothing to do
AMEND_NO_BAR = "no_bar"            # no stored row for (symbol, minute)
AMEND_FOREIGN_SRC = "foreign_src"  # row is no longer src=<require_src> (official/backfilled): untouchable
AMEND_RACE_LOST = "race_lost"      # CAS predicate missed: the row changed under us; nothing written

# Duplicated from ``engine.universe.builder.EXCL_CAP``/``EXCL_INDEX``: this module CANNOT import
# that one (builder imports the store — a store->builder import would cycle). Must stay equal to
# the builder constants; ``tests/unit/test_market_store.py`` asserts the pairs match.
_EXCL_CAP = "watchlist_cap"
_EXCL_INDEX = "not_in_index"
#: The pre-O15 spelling of ``_EXCL_INDEX``. The extended leg shipped 2026-09-01 writing
#: ``not_nifty200``; O15 (2026-09-04) renamed the marker when the index became config. Rows written
#: 2026-09-01…09-04 still carry the legacy string and NOTHING rewrites history (universe_daily is an
#: append-per-day audit trail, and a day's rows are the record of what that day's build decided), so
#: every read of the extended leg accepts EITHER marker and the day-d replace deletes both. Not a
#: permanent widening: it can be dropped once no retained universe_daily day predates 2026-09-05.
_EXCL_INDEX_LEGACY = "not_nifty200"

# Duplicated from ``engine.datafeeds.filings_pit_fresh.BSE_ID_PREFIX``: this module CANNOT import
# that one (every datafeed imports the store — a store->datafeeds import would cycle). Must stay
# equal to it; ``tests/unit/test_filings_feeds.py`` asserts the pair matches. NSE corporates-pit
# rows carry the bare content hash, BSE fresh-feed rows the tagged one, so the id prefix IS the
# source partition of ``insider_trades`` (§2.8.5) — no source column exists. A THIRD source must add
# its tag here AND be excluded from the 'nse' predicate: bare-id means NSE only while 'bse:' is the
# only tag (verified 2026-09-13 on the live store: 44,187 bare / 405 'bse:' / no other prefix).
_INSIDER_SOURCE_PREDICATE = {
    "nse": "id NOT LIKE 'bse:%'",
    "bse": "id LIKE 'bse:%'",
}

_TICK_STAGE_DDL = """
    CREATE OR REPLACE TEMP TABLE _tick_stage (
        instrument_token BIGINT,
        tradingsymbol    TEXT,
        ltp              DECIMAL(12,2),
        volume_traded    BIGINT,
        exchange_ts      TIMESTAMPTZ,
        ohlc_open        DECIMAL(12,2),
        ohlc_high        DECIMAL(12,2),
        ohlc_low         DECIMAL(12,2),
        ohlc_close       DECIMAL(12,2),
        avg_price        DECIMAL(14,4),
        bid              DECIMAL(12,2),
        ask              DECIMAL(12,2)
    )
"""

_TICK_COLUMNS = (
    "instrument_token", "tradingsymbol", "ltp", "volume_traded", "exchange_ts",
    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "avg_price", "bid", "ask",
)


def _ist(value: Any) -> Any:
    """Normalize a DuckDB-returned datetime to tz-aware IST (§3.2); pass everything else through."""
    if isinstance(value, datetime):
        return value.astimezone(IST)
    return value


def _note_slow_statement(label: str, elapsed: float, *, failed: bool = False) -> None:
    """Log ``store_slow_statement`` (§2.6 hardening (iii), WO-24b follow-up) when ONE ``_lock`` hold
    lasted ``elapsed`` >= :data:`_SLOW_STATEMENT_S` seconds — so the next mid-session stall names the
    statement the thread dump could not.

    Contract for every instrumented site (2026-09-09 review): take ``t0`` right AFTER acquiring
    ``_lock``, compute ``elapsed`` in a ``finally`` right BEFORE releasing it — so a statement that
    RAISES after a long hold is reported too, with ``failed=True`` — and call this AFTER the ``with
    self._lock:`` block, where the WARNING can no longer add to the hold it is reporting. One hold
    must emit exactly ONE event: that is why the read helpers run their statement through
    :meth:`MarketStore._execute_locked` (silent) instead of nesting :meth:`MarketStore._execute`,
    which used to log twice per read under identical labels and never timed the fetch phase at all.

    HONEST EXCEPTION: ``_lock`` is an ``RLock``, and a few methods call an instrumented helper from
    INSIDE their own hold — the delete-then-insert rewrites (:meth:`MarketStore.replace_universe_daily_index_markers`,
    :meth:`MarketStore.replace_catalyst_watchlist`), the ``_execute``-based frame reads and the
    Parquet exports. Those notes DO fire while the outer hold is still held, and such a hold emits
    one event per inner statement rather than one for the hold. At least one of them IS a
    trading-session path (2026-09-09 review corrected the earlier "none of these runs in-session"
    claim): :meth:`MarketStore.get_bars_1d_frame` wraps a logging ``_execute``, and the risk gate's
    co-movement rule reads it live, once per open position per candidate
    (``src/engine/risk/gate.py`` ~1547). Accepted anyway, not plumbed around: the nested note only
    fires when the inner statement ALREADY took >= 5 s, and one WARNING costs milliseconds beside
    that — it cannot meaningfully lengthen the hold it is reporting."""
    if elapsed < _SLOW_STATEMENT_S:
        return
    fields: dict[str, Any] = {
        "label": label,
        "elapsed_s": round(elapsed, 3),
        "thread": threading.current_thread().name,
    }
    if failed:
        fields["failed"] = True        # the hold ended in an exception — the statement never returned
    _log.warning("store_slow_statement", **fields)


def _norm_scrip_code(raw: Any) -> str:
    """Normalize a BSE scrip code (int/str, possibly ``'500325.0'``) to a bare-int string; ``''`` for
    blank/None (§2.8 fresh-insider reverse lookup). Keeps a non-numeric code as its stripped self."""
    s = str(raw if raw is not None else "").strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except ValueError:
        return s


class MarketStore:
    """Single-writer DuckDB/Parquet store for all §4.3 analytical data (E4).

    Parameters
    ----------
    db_path:
        The ``market.duckdb`` file (created on :meth:`open`).
    parquet_root:
        Root of the Parquet datasets (``<root>/ticks/date=…/symbol=…``).
    clock:
        The single source of "now" (§3.2) — stamps ``ingested_at``/``logged_at`` and drives the tick
        flush timer and retention cutoffs.
    flush_interval_s / max_buffered_ticks:
        Tick batching knobs (§4.3). Whichever trips first flushes the buffer. 60 s default
        (WO-7, 2026-08-13; was 5 s): 5 s batches produced ~752K parquet fragments/day at
        ~1.9 KB each; 60 s cuts the fragment count ~10× and the nightly compaction merges the
        remainder to one file per symbol-day. Cost: the raw-TICK loss window on a hard crash
        widens 5→60 s — bars_1m is built and persisted independently, so the exposure is R9
        fill-model raw ticks only, never bar/decision data.
    """

    def __init__(
        self,
        db_path: str | Path,
        parquet_root: str | Path,
        clock: Clock,
        *,
        flush_interval_s: float = 60.0,
        max_buffered_ticks: int = 2000,
    ) -> None:
        self._db_path = Path(db_path)
        self._parquet_root = Path(parquet_root)
        self._clock = clock
        self._flush_interval_s = float(flush_interval_s)
        self._max_buffered_ticks = int(max_buffered_ticks)

        self._con: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.RLock()          # serializes ALL DuckDB access (single writer, §4.1)
        self._tick_lock = threading.Lock()      # tick buffer only — appends never wait on DuckDB
        self._flush_lock = threading.Lock()     # single-flight gate for flushes — try-acquired, never queued
        self._tick_buffer: list[Tick] = []
        self._last_flush_at: datetime = clock.now()

        # --- WO-26a (2026-08-25) flush single-flight telemetry: skipped flushes are normal under
        #     load, so they are COUNTED always and logged at most once per minute.
        self._flush_skip_lock = threading.Lock()          # counter only — never held across I/O
        self._flush_skips = 0
        self._flush_skip_logged_at: datetime | None = None
        # --- WO-26a partition-dir cache: (see _tick_partition_dir) symbols whose ticks directory is
        #     known to exist for _tick_dirs_day. Touched only under _flush_lock.
        self._tick_dirs: set[str] = set()
        self._tick_dirs_day: date | None = None
        # --- WO-26a private worker pools (see _pool / _flush_pool). Created lazily so a sync-only
        #     user (tests, offline jobs) never spawns a thread; released by close().
        self._pool_lock = threading.Lock()
        self._store_executor: ThreadPoolExecutor | None = None
        self._flush_executor: ThreadPoolExecutor | None = None

    @classmethod
    def from_settings(cls, settings: Settings, clock: Clock, **kwargs: Any) -> MarketStore:
        """Build against the configured paths (``paths.duckdb`` / ``paths.parquet_dir``)."""
        return cls(settings.duckdb_path(), settings.parquet_dir(), clock, **kwargs)

    # ------------------------------------------------------------------ lifecycle
    def open(self) -> MarketStore:
        """Open the DuckDB connection (creating the file), pin the session to IST, init the schema."""
        with self._lock:
            if self._con is not None:
                return self
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._parquet_root.mkdir(parents=True, exist_ok=True)
            self._con = duckdb.connect(str(self._db_path))
            # Every DuckDB instance in this platform carries a stated ceiling (§3.2.11) — set BEFORE
            # any query so no job or endpoint can commit the machine's memory.
            self._con.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
            # Session timezone pinned so TIMESTAMPTZ round-trips as IST wall time (§3.2 convention).
            self._con.execute("SET TimeZone='Asia/Kolkata'")
            self.init_schema()
            self._con.execute(_TICK_STAGE_DDL)
            self._last_flush_at = self._clock.now()
        _log.info("market_store_opened", db=str(self._db_path), parquet=str(self._parquet_root))
        return self

    def close(self) -> None:
        """Flush any buffered ticks, then close. Idempotent.

        The flush runs BEFORE taking ``_lock``: flush_ticks acquires ``_flush_lock`` then ``_lock``
        per statement, so calling it while already holding ``_lock`` inverts the order against any
        in-flight background flush (aflush_ticks worker) — a reproducible AB-BA deadlock.

        This is also the ONE caller that waits on an in-flight flush instead of skipping it
        (WO-26a): after this returns the connection is gone, so a skipped batch would have nowhere
        left to go. The wait is bounded by ``_CLOSE_FLUSH_WAIT_S`` — shutdown must not hang on a
        wedged flush, which is the failure mode this work order exists to survive."""
        if self._con is None:
            self._shutdown_pools()
            return
        try:
            self.flush_ticks(wait_s=_CLOSE_FLUSH_WAIT_S)
        finally:
            with self._lock:
                if self._con is not None:
                    self._con.close()
                    self._con = None
            self._shutdown_pools()
        _log.info("market_store_closed", db=str(self._db_path))

    # ------------------------------------------------------------------ worker pools (WO-26a)
    def _pool(self) -> ThreadPoolExecutor:
        """The store's PRIVATE worker pool (``mt-store``) — where every async wrapper below runs.

        2026-08-25 root cause: the wrappers offloaded via ``asyncio.to_thread``, i.e. the ONE
        default executor shared by every offload in the process. A slow tick flush parked ~20
        threads on ``_flush_lock`` inside that pool, and from then on nothing else could get a
        worker: the health probe stayed pending 09:15→13:39 (consecutive=264) and the entire
        intelligence layer ran empty — zero candidates, zero analyst calls, a whole session.

        A pool the store owns makes that structurally impossible in both directions: store work
        cannot starve the rest of the process, and no SDK call, HTTP parse or other ``to_thread``
        user can starve a store read. Created lazily, released by :meth:`close`."""
        with self._pool_lock:
            if self._store_executor is None:
                self._store_executor = ThreadPoolExecutor(
                    max_workers=_STORE_EXECUTOR_WORKERS, thread_name_prefix="mt-store"
                )
            return self._store_executor

    def _flush_pool(self) -> ThreadPoolExecutor:
        """The tick flush's own single-thread pool (``mt-flush``), separate from :meth:`_pool`.

        One thread is exactly the right width because flushes are single-flight already
        (:meth:`flush_ticks` skips rather than queues). Keeping it out of ``mt-store`` means a long
        flush can never consume a worker that a store read — or the health probe — needs, which is
        the other half of the 08-25 lesson."""
        with self._pool_lock:
            if self._flush_executor is None:
                self._flush_executor = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="mt-flush"
                )
            return self._flush_executor

    def _shutdown_pools(self) -> None:
        """Release both pools. ``wait=False`` on purpose: :meth:`close` runs on the event loop at
        engine shutdown (and inside another pool's worker for the CLI jobs), so joining here would
        trade a clean stop for a hang on exactly the wedged flush this design exists to survive.
        Already-queued work still runs; a flush that lands after the connection is gone re-stages
        its batch and warns (``tick_flush_skipped_store_closed``)."""
        with self._pool_lock:
            pools = [p for p in (self._store_executor, self._flush_executor) if p is not None]
            self._store_executor = None
            self._flush_executor = None
        for pool in pools:
            pool.shutdown(wait=False)

    def __enter__(self) -> MarketStore:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ping(self) -> bool:
        """Cheapest possible proof that this store is ALIVE: take ``_lock``, run ``SELECT 1``, return.

        WO-24b-prime (2026-08-21). The 09:56 freeze stalled every store-touching path at once —
        feature snapshots, the ``warmup_refresh`` job, the gate-context read — for 14 minutes, and
        afterwards the platform could not say WHICH shared resource had seized, because nothing was
        probing any of them. This is that probe. It acquires the same ``_lock`` every other method
        acquires and runs the smallest statement DuckDB has, so a call that does not RETURN means
        precisely one thing: nobody can get the lock, or the connection itself is wedged.

        Deliberately not wrapped in try/except — raising is a different and equally useful answer
        from hanging, and the caller (:class:`~engine.ops.health.HealthMonitor`) distinguishes them.
        """
        with self._lock:
            self._require_con().execute("SELECT 1").fetchone()
        return True

    #: Scale ``instruments_daily.tick_size`` must carry (2026-07-21 lossless-hydrate incident). The
    #: ``DECIMAL(18,6)`` DDL applies to fresh DBs; a DB created under the old ``DECIMAL(10,2)`` is
    #: widened once by :meth:`_migrate_instruments_tick_scale` on the next open.
    _INSTRUMENTS_TICK_SCALE = 6

    def init_schema(self) -> None:
        """Create every §4.3 table + index. Idempotent (IF NOT EXISTS) — safe on every startup.

        Also runs the one-shot ``instruments_daily.tick_size`` widen (2026-07-21): ``CREATE TABLE IF
        NOT EXISTS`` never alters an existing column, so a legacy DB would keep truncating sub-paisa
        ticks to 0.00 and losing them on hydrate. Same reason for the ``corrections_log.reason`` add
        and the ``sentiment_agg`` saturation-measure adds.
        """
        with self._lock:
            con = self._require_con()
            for stmt in _SCHEMA:
                con.execute(stmt)
            self._migrate_instruments_tick_scale(con)
            self._migrate_corrections_reason(con)
            self._migrate_sentiment_measures(con)
            self._migrate_watchlist_reversal(con)

    def _migrate_instruments_tick_scale(self, con: duckdb.DuckDBPyConnection) -> None:
        """Idempotently widen a legacy ``instruments_daily.tick_size DECIMAL(10,2)`` to ``DECIMAL(18,6)``
        (2026-07-21 lossless-hydrate incident). Guarded on the live column scale via ``information_schema``
        so the ALTER runs EXACTLY ONCE (never a per-open column rewrite): a fresh DB already carries the
        wide DDL, so ``numeric_scale`` is 6 and this no-ops."""
        row = con.execute(
            "SELECT numeric_scale FROM information_schema.columns "
            "WHERE table_name = 'instruments_daily' AND column_name = 'tick_size'"
        ).fetchone()
        scale = row[0] if row and row[0] is not None else self._INSTRUMENTS_TICK_SCALE
        if scale < self._INSTRUMENTS_TICK_SCALE:
            con.execute("ALTER TABLE instruments_daily ALTER tick_size SET DATA TYPE DECIMAL(18,6)")
            _log.info("instruments_tick_scale_migrated", frm=scale, to=self._INSTRUMENTS_TICK_SCALE)

    def _migrate_corrections_reason(self, con: duckdb.DuckDBPyConnection) -> None:
        """Idempotently add the nullable ``corrections_log.reason`` column to a legacy DB (WO-5):
        ``CREATE TABLE IF NOT EXISTS`` never alters an existing table, so a DB created before the
        column existed would reject every ``append_correction`` carrying a reason. Guarded on
        ``information_schema`` so the ALTER runs EXACTLY ONCE; a fresh DB already has it and no-ops.
        Nullable + appended-last ⇒ existing rows keep their values and read back unchanged."""
        row = con.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'corrections_log' AND column_name = 'reason'"
        ).fetchone()
        if row is None:
            con.execute("ALTER TABLE corrections_log ADD COLUMN reason TEXT")
            _log.info("corrections_log_reason_column_added")

    #: Nullable ``sentiment_agg`` saturation measures appended by :meth:`_migrate_sentiment_measures`,
    #: in the order the ALTERs must run to match the fresh-DB column order (WO-22).
    _SENTIMENT_MEASURE_COLUMNS: tuple[tuple[str, str], ...] = (
        ("raw_sum", "DOUBLE"), ("n_clusters", "INTEGER"),
    )

    def _migrate_sentiment_measures(self, con: duckdb.DuckDBPyConnection) -> None:
        """Idempotently add the nullable ``sentiment_agg.raw_sum``/``n_clusters`` columns to a legacy
        DB (WO-22), mirroring :meth:`_migrate_corrections_reason`: ``CREATE TABLE IF NOT EXISTS``
        never alters an existing table, so a DB written before the measurement existed would reject
        every digest upsert carrying it. Guarded per column on ``information_schema`` so each ALTER
        runs EXACTLY ONCE; a fresh DB already has both and this no-ops. Nullable + appended-last ⇒
        digest rows written BEFORE WO-22 keep reading back with NULL measures, which is exactly what
        the analyst render treats as "no measurement for this row"."""
        present = {
            r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'sentiment_agg'"
            ).fetchall()
        }
        for column, sql_type in self._SENTIMENT_MEASURE_COLUMNS:
            if column not in present:
                con.execute(f"ALTER TABLE sentiment_agg ADD COLUMN {column} {sql_type}")
                _log.info("sentiment_agg_column_added", column=column)

    def _migrate_watchlist_reversal(self, con: duckdb.DuckDBPyConnection) -> None:
        """Idempotently add the nullable ``catalyst_watchlist.reversal_of`` column (§2.7
        ``cat_reversal``, 2026-08-27), mirroring :meth:`_migrate_sentiment_measures`.

        ``CREATE TABLE IF NOT EXISTS`` never alters an existing table, so a DB written before the
        reversal flag existed would reject every digest upsert carrying it — and the digest is the
        08:35 job the whole news chain hangs off. Guarded on ``information_schema`` so the ALTER runs
        EXACTLY ONCE; a fresh DB already has the column and this no-ops. Nullable + appended-last ⇒
        every watchlist row written before today reads back with ``reversal_of = NULL``, i.e. "not a
        reversal", which is the correct answer for rows graded before the detector existed."""
        present = {
            r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'catalyst_watchlist'"
            ).fetchall()
        }
        if "reversal_of" not in present:
            con.execute("ALTER TABLE catalyst_watchlist ADD COLUMN reversal_of TEXT")
            _log.info("catalyst_watchlist_column_added", column="reversal_of")

    def table_names(self) -> set[str]:
        """Names of the persistent tables in the store (for self-tests / the schema lockstep test)."""
        rows = self._fetchall(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = current_catalog() AND table_schema = 'main'"
        )
        return {r[0] for r in rows}

    # ------------------------------------------------------------------ low-level helpers
    def _require_con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("MarketStore is not open — call open() first")
        return self._con

    def _execute_locked(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        params: Sequence[Any] | None = None,
    ) -> tuple[duckdb.DuckDBPyConnection, float]:
        """Run ONE statement on ``con`` and return ``(cursor, elapsed_s)``. Deliberately SILENT: the
        caller is already inside its ``with self._lock:`` block, so it — not this helper — decides
        what the hold's single :func:`_note_slow_statement` says and emits it after the release
        (2026-09-09 review: nesting the logging :meth:`_execute` inside a hold logged twice per read
        under identical labels and left the fetch phase untimed)."""
        t0 = time.perf_counter()
        cur = con.execute(sql, params) if params is not None else con.execute(sql)
        return cur, time.perf_counter() - t0

    def _execute(self, sql: str, params: Sequence[Any] | None = None) -> duckdb.DuckDBPyConnection:
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    result, elapsed = self._execute_locked(self._require_con(), sql, params)
                    failed = False
                finally:
                    if failed:      # raised: _execute_locked never handed back its measurement
                        elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(sql[:80], elapsed, failed=failed)
        return result

    def _fetchall(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        # The WHOLE hold is one measurement (execute + fetch): a 5 s read split 3 s/2.5 s across the
        # two phases went unreported when each phase was timed on its own (2026-09-09 review).
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    cur, _ = self._execute_locked(self._require_con(), sql, params)
                    rows = cur.fetchall()
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(sql[:80], elapsed, failed=failed)
        return rows

    def _fetch_dicts(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        # As _fetchall, and the per-row dict/_ist conversion counts too — it runs under the hold.
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    cur, _ = self._execute_locked(self._require_con(), sql, params)
                    cols = [d[0] for d in cur.description]
                    out = [
                        {c: _ist(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()
                    ]
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(sql[:80], elapsed, failed=failed)
        return out

    #: Row count at which _upsert_rows switches from executemany (~128 rows/s) to the vectorized
    #: _bulk_write path (2026-07-21). In practice only instruments_daily (~113k rows/day) crosses it.
    _VECTORIZED_UPSERT_MIN_ROWS = 2000

    def _bulk_write(
        self,
        table: str,
        cols: Sequence[str],
        rows: Sequence[Sequence[Any]],
        *,
        pk: Sequence[str] = (),
        conflict: str = "",
    ) -> None:
        """Vectorized multi-row write: register the batch as a DataFrame view, run ONE
        ``INSERT … SELECT`` — DuckDB executes ``executemany`` row-by-row (~6 ms/row; a 15k-bar
        backfill chunk took ~90 s), this path is ~200x faster. ``dtype=object`` keeps executemany's
        per-element binding semantics (Decimals exact, no int→float/NaN coercion of None-bearing
        columns). ``pk`` keeps its last-wins semantics for duplicate keys within one batch, which a
        single INSERT…SELECT would otherwise reject ("cannot update the same row twice").

        CONSTRAINT: each column's values must be type-homogeneous (modulo None). The whole column
        gets ONE inferred type — a date/datetime or naive/aware-datetime mix that per-row binding
        would coerce per element instead breaks dedupe or raises. The pydantic-typed bulk writers
        (``Bar``/``DailyBar``/``Tick``) guarantee this; free-form dict rows (``_upsert_rows``) stay
        on ``executemany``, whose small batches never needed the speed."""
        if not rows:
            return
        if pk:
            col_index = {c: i for i, c in enumerate(cols)}
            idx = [col_index[k] for k in pk]
            rows = list({tuple(r[i] for i in idx): r for r in rows}.values())
        collist = ", ".join(f'"{c}"' for c in cols)
        # Decimals go in as strings: DuckDB infers ONE DECIMAL(width,scale) per object column from a
        # ~1000-row stride sample, so a rare wider value that dodges the sample fails the cast (a
        # GMRAIRPORT ₹100.06 spike among ₹8x.xx bars → 'cast "100.06" to DECIMAL(4,2)' aborting the
        # chunk). VARCHAR scans cast per value against the real table column type — exact and safe.
        df = pd.DataFrame(
            [[str(v) if isinstance(v, Decimal) else v for v in r] for r in rows],
            columns=list(cols),
            dtype=object,
        )
        view = "_mt_bulk_stage"
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                # t0 is the FIRST thing inside the hold and elapsed is taken around the whole
                # register/insert/unregister trio (2026-09-09 review): register materializes the frame
                # as a DuckDB view and unregister tears it down, both under _lock — timing only the
                # INSERT under-reported the hold every other caller actually waited on.
                t0 = time.perf_counter()
                try:
                    con = self._require_con()
                    con.register(view, df)
                    try:
                        self._execute_locked(
                            con,
                            f"INSERT INTO {table} ({collist}) SELECT {collist} FROM {view} {conflict}",
                        )
                        failed = False
                    finally:
                        con.unregister(view)
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"bulk_write:{table}:{len(rows)}", elapsed, failed=failed)

    def _upsert_rows(self, table: str, rows: Sequence[dict[str, Any]]) -> int:
        """Generic pinned-column upsert: unknown keys are a hard error; missing keys insert NULL."""
        if not rows:
            return 0
        cols, pk = _TABLE_SPEC[table]
        colset = set(cols)
        defaults = _TABLE_DEFAULTS.get(table, {})
        for row in rows:
            unknown = set(row) - colset
            if unknown:
                raise ValueError(f"{table}: unknown column(s) {sorted(unknown)}; allowed: {cols}")
        if defaults:
            rows = [{**defaults, **row} for row in rows]
        non_pk = [c for c in cols if c not in pk]
        if non_pk and table not in _CONTENT_HASH_PK_TABLES:
            updates = ", ".join(f'"{c}" = excluded."{c}"' for c in non_pk)
            conflict = f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {updates}"
        else:
            # Content-hash-PK tables (id derives from the row's content): identical id ⇒ identical
            # row, so a conflict UPDATE only rewrites equal values — and that no-op rewrite is what
            # tripped DuckDB's ART fault on 2026-07-23 ("Failed to delete all rows from index",
            # insider_trades re-upsert during catch-up; the FATAL invalidated the whole connection).
            # DO NOTHING is semantically exact for these tables and never touches the delete path.
            conflict = f"ON CONFLICT ({', '.join(pk)}) DO NOTHING"
        # 2026-07-21 (third finding of the incident day): instruments_daily is ~113k rows/day and
        # executemany binds row-at-a-time (~128 rows/s measured here) — the 08:15 persist and the
        # degraded-snapshot heal each held the single-writer lock for ~12 MINUTES, starving every
        # other store consumer (the 13:33 backfill silence). Large batches therefore take
        # _bulk_write's registered-view INSERT…SELECT (~200x): its dtype=object frame keeps
        # executemany's per-element semantics (Decimals stringified and cast per value against the
        # REAL column type — the GMRAIRPORT lesson; None → NULL; ints stay ints), and its pk dedupe
        # keeps last-wins for intra-batch duplicate keys, exactly like sequential upserts. The
        # single INSERT…SELECT is one statement, so the torn-write guarantee holds (statement
        # atomicity; a kill leaves the prior complete day).
        if len(rows) >= self._VECTORIZED_UPSERT_MIN_ROWS:
            self._bulk_write(
                table, cols, [[row.get(c) for c in cols] for row in rows], pk=pk, conflict=conflict
            )
            return len(rows)
        # SMALL free-form batches stay on executemany: dict rows can mix date/datetime or
        # naive/aware values in one column, which per-row binding coerces per element but a single
        # DataFrame column cannot (one inferred type per column). No small-batch caller needed the
        # speed; a heterogeneous column that ever DID cross the threshold fails LOUD (ConversionException
        # + rollback), never silently.
        placeholders = ", ".join("?" for _ in cols)
        collist = ", ".join(f'"{c}"' for c in cols)
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) {conflict}"
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()      # first thing inside the hold — see _bulk_write
                con = self._require_con()
                # Torn-write guard (2026-07-21): a taskkill mid-executemany left instruments_daily HALF-
                # written (112,297 of 112,826 rows — snapshot_rows appends the 233 index rows LAST, so
                # exactly the regime tokens were lost and every later boot hydrated a broken MAX(d) day).
                # Autocommit applies per statement; one explicit transaction makes the batch all-or-nothing —
                # a killed process leaves the PRIOR complete snapshot, never a torn one. BaseException so a
                # KeyboardInterrupt/CancelledError mid-batch also rolls back and keeps the connection usable.
                try:
                    con.execute("BEGIN TRANSACTION")
                    try:
                        con.executemany(sql, [[row.get(c) for c in cols] for row in rows])
                    except BaseException:
                        con.execute("ROLLBACK")
                        raise
                    con.execute("COMMIT")
                    failed = False
                finally:
                    # The whole transaction is the hold (2026-09-09 review): a batch that rolls back
                    # after 30 s starved every other caller for 30 s and must still name itself.
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"upsert_rows:{table}:{len(rows)}", elapsed, failed=failed)
        return len(rows)

    # ================================================================== bars_1m (§3.2.3, A13/A14)
    def insert_bars_1m(self, bars: Sequence[Bar]) -> int:
        """Upsert finalized 1m bars. Upsert (not insert) because official candles become the
        canonical rows where they exist (§4.4 job 2) and a late-tick amendment rewrites a bar."""
        if not bars:
            return 0
        self._bulk_write(
            "bars_1m",
            ("symbol", "ts_minute", "open", "high", "low", "close", "volume", "src", "auction_open"),
            [
                [b.symbol, b.ts_minute, b.open, b.high, b.low, b.close, b.volume, b.src, b.auction_open]
                for b in bars
            ],
            pk=("symbol", "ts_minute"),
            conflict=(
                'ON CONFLICT (symbol, ts_minute) DO UPDATE SET "open"=excluded."open", high=excluded.high, '
                'low=excluded.low, "close"=excluded."close", volume=excluded.volume, src=excluded.src, '
                "auction_open=excluded.auction_open"
            ),
        )
        return len(bars)

    def get_bars_1m(self, symbol: str, start: datetime, end: datetime) -> list[Bar]:
        """Bars for ``symbol`` with ``start <= ts_minute < end``, ascending. Decimal-exact."""
        rows = self._fetchall(
            'SELECT symbol, ts_minute, "open", high, low, "close", volume, src, auction_open '
            "FROM bars_1m WHERE symbol = ? AND ts_minute >= ? AND ts_minute < ? ORDER BY ts_minute",
            [symbol, start, end],
        )
        return [
            Bar(
                symbol=r[0], ts_minute=_ist(r[1]), open=r[2], high=r[3], low=r[4], close=r[5],
                volume=r[6], src=r[7], auction_open=r[8],
            )
            for r in rows
        ]

    def amend_bar_1m_extremes(
        self, symbol: str, minute: datetime, value: Decimal, *, require_src: str = "self"
    ) -> str:
        """Atomically widen ONE stored 1m bar's high/low to include ``value`` (§3.2.3 late-tick
        amendment). Returns one of the module-level ``AMEND_*`` outcomes; writes nothing except on
        ``AMEND_APPLIED``, and never touches open/close/volume/src/auction_open.

        This is the read-decide-write seam the late-tick path needs (WO-5): ``BarBuilder`` used to
        ``get_bars_1m`` then ``insert_bars_1m`` as two independent lock acquisitions, so the 15:50
        ReconcileJob could upsert the canonical official candle in between and have the amendment —
        computed against the pre-reconcile row and issued as a whole-row upsert — silently overwrite
        it. Here the SELECT, the decision and the UPDATE happen inside ONE ``_lock`` acquisition and
        ONE transaction, and the UPDATE re-states the row it read as its own predicate
        (``src``/``high``/``low`` compare-and-swap) so even a lock-free future caller can only write
        against the row it decided on — otherwise ``AMEND_RACE_LOST``, write skipped.

        ``require_src`` is a hard precondition, not a filter: a row whose ``src`` has become
        ``kite_official``/``gap_backfilled`` is CANONICAL (§4.4 job 2) and a stray live tick may never
        rewrite it — it reports ``AMEND_FOREIGN_SRC`` and leaves the row byte-intact.

        Lock note: this holds ``_lock`` across 2-3 statements (µs-scale point reads/writes on the PK),
        marginally longer than the one-statement discipline of the flush path — the cost of atomicity.
        The whole transaction is therefore ONE timed hold (§2.6 hardening (iii), 2026-09-09): a 2-3
        statement hold that stalls starves every other store caller exactly like a one-statement one.
        This runs on the LIVE late-tick path, so the instrumentation is deliberately the cheap shape —
        two ``perf_counter`` reads and one f-string per amendment, nothing allocated per statement.
        """
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    con = self._require_con()
                    con.execute("BEGIN TRANSACTION")
                    try:
                        outcome = self._amend_bar_1m_locked(con, symbol, minute, value, require_src)
                    except BaseException:
                        # Mirrors _upsert_rows: a KeyboardInterrupt/CancelledError mid-amendment must
                        # leave no open transaction behind (the connection stays usable for everyone).
                        con.execute("ROLLBACK")
                        raise
                    con.execute("COMMIT")
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"amend_bar_1m:{symbol}", elapsed, failed=failed)
        return outcome

    def _amend_bar_1m_locked(
        self,
        con: duckdb.DuckDBPyConnection,
        symbol: str,
        minute: datetime,
        value: Decimal,
        require_src: str,
    ) -> str:
        """The read-decide-write body of :meth:`amend_bar_1m_extremes` — callers hold ``_lock`` and an
        open transaction."""
        row = self._read_bar_extremes(con, symbol, minute)
        if row is None:
            return AMEND_NO_BAR
        src, high, low = row
        if src != require_src:
            return AMEND_FOREIGN_SRC
        if low <= value <= high:
            return AMEND_IN_RANGE
        changed = con.execute(
            "UPDATE bars_1m SET high = ?, low = ? "
            "WHERE symbol = ? AND ts_minute = ? AND src = ? AND high = ? AND low = ?",
            [max(high, value), min(low, value), symbol, minute, src, high, low],
        ).fetchone()[0]
        return AMEND_APPLIED if changed else AMEND_RACE_LOST

    @staticmethod
    def _read_bar_extremes(
        con: duckdb.DuckDBPyConnection, symbol: str, minute: datetime
    ) -> tuple[str, Decimal, Decimal] | None:
        """``(src, high, low)`` of one stored bar, or ``None``. Its own method so the CAS predicate in
        :meth:`_amend_bar_1m_locked` is testable: a test double returning a STALE snapshot must lose
        the compare-and-swap (``AMEND_RACE_LOST``) and write nothing."""
        row = con.execute(
            "SELECT src, high, low FROM bars_1m WHERE symbol = ? AND ts_minute = ?", [symbol, minute]
        ).fetchone()
        return (row[0], row[1], row[2]) if row is not None else None

    def get_bars_1m_frame(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Bulk float OHLCV frame of 1m bars (``start <= ts_minute < end``), ascending.

        The ANALYTICAL read path (backtest frame loading): one vectorized DuckDB→pandas fetch with
        DOUBLE casts instead of per-row pydantic ``Bar`` construction (~50 µs/row — ~7.5 min for a
        9M-row orb backtest load). Exactness note: DECIMAL→DOUBLE is the same correctly-rounded
        value as ``float(Decimal)``. Ledger/live consumers keep :meth:`get_bars_1m` (Decimal)."""
        with self._lock:
            df = self._execute(
                'SELECT ts_minute, CAST("open" AS DOUBLE) AS "open", CAST(high AS DOUBLE) AS high, '
                'CAST(low AS DOUBLE) AS low, CAST("close" AS DOUBLE) AS "close", '
                "CAST(volume AS DOUBLE) AS volume, CAST(auction_open AS DOUBLE) AS auction_open "
                "FROM bars_1m WHERE symbol = ? AND ts_minute >= ? AND ts_minute < ? ORDER BY ts_minute",
                [symbol, start, end],
            ).df()
        df = df.set_index("ts_minute")
        # ns resolution, NOT DuckDB's native us: downstream int64-ns index arithmetic
        # (sweep._signals_orb .asi8 math) and every pd.Timestamp-built index assume ns.
        df.index = pd.DatetimeIndex(df.index).as_unit("ns").tz_convert(IST)
        df.index.name = None
        return df

    def get_bars_1d_frame(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        """Bulk float OHLCV frame of daily bars (``start <= d <= end``), ascending (see
        :meth:`get_bars_1m_frame` — the analytical read path; Decimal consumers use get_bars_1d)."""
        with self._lock:
            df = self._execute(
                'SELECT d, CAST("open" AS DOUBLE) AS "open", CAST(high AS DOUBLE) AS high, '
                'CAST(low AS DOUBLE) AS low, CAST("close" AS DOUBLE) AS "close", '
                "CAST(volume AS DOUBLE) AS volume "
                "FROM bars_1d WHERE symbol = ? AND d >= ? AND d <= ? ORDER BY d",
                [symbol, start, end],
            ).df()
        df = df.set_index("d")
        df.index = pd.DatetimeIndex(df.index).as_unit("ns")   # ns, matching pd.Timestamp-built indexes
        df.index.name = None
        return df

    def get_bars_1d_for_day(self, d: date) -> list[DailyBar]:
        """Every symbol's daily bar for day ``d`` — one scan for batch cross-checks (bhavcopy job)."""
        rows = self._fetchall(
            'SELECT symbol, d, "open", high, low, "close", volume, src FROM bars_1d '
            "WHERE d = ? ORDER BY symbol",
            [d],
        )
        return [
            DailyBar(symbol=r[0], d=r[1], open=r[2], high=r[3], low=r[4], close=r[5], volume=r[6], src=r[7])
            for r in rows
        ]

    def last_bar_time(self, symbol: str) -> datetime | None:
        """Latest ``ts_minute`` seen for ``symbol`` — the warm-up gap-fill `frm` anchor (§2.6/§4.4 job 1)."""
        row = self._fetchall("SELECT max(ts_minute) FROM bars_1m WHERE symbol = ?", [symbol])[0]
        return _ist(row[0]) if row[0] is not None else None

    def coverage_gaps(self, symbol: str, start: datetime, end: datetime) -> list[datetime]:
        """Missing minute-starts in ``[start, end)`` for ``symbol`` (§2.6 step 6 / §7.1 ``warmup_ready``).

        Expects a WITHIN-SESSION range (the caller clamps to session minutes via ``NSECalendar``);
        every whole minute in the range is expected to have a bar. Returns the missing minutes
        ascending — empty list ⇒ contiguous coverage.
        """
        start = start.astimezone(IST).replace(second=0, microsecond=0)
        end = end.astimezone(IST)
        expected: list[datetime] = []
        cur = start
        while cur < end:
            expected.append(cur)
            cur += timedelta(minutes=1)
        if not expected:
            return []
        rows = self._fetchall(
            "SELECT ts_minute FROM bars_1m WHERE symbol = ? AND ts_minute >= ? AND ts_minute < ?",
            [symbol, start, end],
        )
        present = {_ist(r[0]) for r in rows}
        return [m for m in expected if m not in present]

    def has_contiguous_coverage(self, symbol: str, start: datetime, end: datetime) -> bool:
        """True iff every minute in ``[start, end)`` has a bar — the §2.6/§7.1 warm-up gate check."""
        return not self.coverage_gaps(symbol, start, end)

    def export_bars_1m_month(self, year: int, month: int) -> Path:
        """Write the month's bars_1m to the Parquet monthly archive (§4.3) and return the file path."""
        first = date(int(year), int(month), 1)
        nxt = date(first.year + (first.month == 12), (first.month % 12) + 1, 1)
        out_dir = self._parquet_root / "bars_1m"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{first:%Y-%m}.parquet"
        with self._lock:
            self._execute(
                "COPY (SELECT * FROM bars_1m WHERE ts_minute >= CAST(? AS TIMESTAMPTZ) "
                "AND ts_minute < CAST(? AS TIMESTAMPTZ) ORDER BY symbol, ts_minute) "
                f"TO '{out.as_posix()}' (FORMAT PARQUET)",
                [self._clock.combine(first, datetime.min.time()),
                 self._clock.combine(nxt, datetime.min.time())],
            )
        return out

    # ================================================================== bars_1d
    def upsert_bars_1d(self, bars: Sequence[DailyBar]) -> int:
        if not bars:
            return 0
        self._bulk_write(
            "bars_1d",
            ("symbol", "d", "open", "high", "low", "close", "volume", "src"),
            [[b.symbol, b.d, b.open, b.high, b.low, b.close, b.volume, b.src] for b in bars],
            pk=("symbol", "d"),
            conflict=(
                'ON CONFLICT (symbol, d) DO UPDATE SET "open"=excluded."open", high=excluded.high, '
                'low=excluded.low, "close"=excluded."close", volume=excluded.volume, src=excluded.src'
            ),
        )
        return len(bars)

    def get_bars_1d(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        """Daily bars with ``start <= d <= end``, ascending."""
        rows = self._fetchall(
            'SELECT symbol, d, "open", high, low, "close", volume, src FROM bars_1d '
            "WHERE symbol = ? AND d >= ? AND d <= ? ORDER BY d",
            [symbol, start, end],
        )
        return [
            DailyBar(symbol=r[0], d=r[1], open=r[2], high=r[3], low=r[4], close=r[5], volume=r[6], src=r[7])
            for r in rows
        ]

    def daily_bar_span(self, symbol: str) -> tuple[date | None, int]:
        """``(earliest d, total bar count)`` across ALL of ``symbol``'s ``bars_1d`` — the warm-up
        gate's young-listing probe (§2.6 step 6). ``(None, 0)`` when the symbol has no daily bars.

        The gate compares ``total`` to the number of sessions since ``earliest d``: a symbol with a bar
        for every session since its (recent) first bar is a fresh LISTING short of the lookback, not a
        gap — the gate excludes it from the freeze rather than blocking entries forever. A shortfall
        with ``total`` below that span is a real gap and still blocks."""
        first, count = self._fetchall("SELECT MIN(d), COUNT(*) FROM bars_1d WHERE symbol = ?", [symbol])[0]
        return (first, int(count))

    # ================================================================== corrections_log (§4.4 job 1)
    def append_correction(
        self,
        symbol: str,
        minute: datetime,
        tick_ts: datetime,
        value: Decimal | None,
        *,
        cumulative_volume: int | None = None,
        amended: bool = False,
        reason: str | None = None,
    ) -> None:
        """Log a late tick that arrived past the minute+5s finalize grace (§4.3 corrections_log).

        ``reason`` (optional) records why an ``amended=False`` row was NOT applied — notably
        ``'official_bar_untouchable'`` for a late tick aimed at a reconciled/backfilled row (§3.2.3).
        """
        self._execute(
            "INSERT INTO corrections_log "
            "(symbol, minute, tick_ts, value, cumulative_volume, amended, logged_at, reason) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [symbol, minute, tick_ts, value, cumulative_volume, amended, self._clock.now(), reason],
        )

    def get_corrections(self, d: date) -> list[dict[str, Any]]:
        return self._fetch_dicts(
            "SELECT * FROM corrections_log WHERE CAST(minute AS DATE) = ? ORDER BY tick_ts", [d]
        )

    # ================================================================== reconcile_log (A13/§2.6)
    def append_reconcile_log(self, rows: Sequence[dict[str, Any]]) -> int:
        """Upsert per-(day, symbol) reconcile results; ``ran_at`` defaults to now if omitted."""
        stamped = [{**row, "ran_at": row.get("ran_at") or self._clock.now()} for row in rows]
        return self._upsert_rows("reconcile_log", stamped)

    def has_reconcile_entry(self, d: date) -> bool:
        """§2.6 catch-up checkpoint: has day ``d`` been reconciled at all?"""
        return bool(self._fetchall("SELECT 1 FROM reconcile_log WHERE d = ? LIMIT 1", [d]))

    def reconciled_days(self, start: date, end: date) -> set[date]:
        """Distinct reconciled days in ``[start, end]`` — the startup scan for un-reconciled days."""
        return {r[0] for r in self._fetchall(
            "SELECT DISTINCT d FROM reconcile_log WHERE d >= ? AND d <= ?", [start, end]
        )}

    def get_reconcile_log(self, d: date) -> list[dict[str, Any]]:
        return self._fetch_dicts("SELECT * FROM reconcile_log WHERE d = ? ORDER BY symbol", [d])

    # ================================================================== instruments / universe (A8/A10)
    def upsert_instruments_daily(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("instruments_daily", rows)

    def get_instruments_daily(self, d: date) -> list[dict[str, Any]]:
        return self._fetch_dicts("SELECT * FROM instruments_daily WHERE d = ? ORDER BY tradingsymbol", [d])

    def get_latest_instruments_daily(self) -> tuple[date, list[dict[str, Any]]] | None:
        """Most recent persisted ``instruments_daily`` snapshot on-or-before today (the F2 cold-start
        hydrate source). Returns ``(d, rows)`` for the latest day carrying rows, or ``None`` when the
        table is empty. A pure DuckDB read — usable pre-login, before any Kite session exists (§2.6)."""
        row = self._fetchall("SELECT MAX(d) FROM instruments_daily WHERE d <= ?", [self._clock.today()])
        d = row[0][0] if row else None
        if d is None:
            return None
        return d, self.get_instruments_daily(d)

    def upsert_universe_daily(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("universe_daily", rows)

    def replace_universe_daily(self, d: date, rows: Sequence[dict[str, Any]]) -> int:
        """Day-``d`` universe write with extended-leg hygiene (2026-09-01 review finding): plain
        upserts never delete, so a same-day re-build with ``batch_universe_enabled`` flipped off —
        or a shrunken extended candidate set — would leave stale extended rows feeding
        :meth:`get_batch_universe_symbols` for the rest of the day, defeating the flag's documented
        rollback guarantee. Extended rows are therefore delete-then-inserted under one lock hold
        (the ``catalyst_watchlist`` idempotent-rewrite precedent); index-member rows stay pure
        upserts — every build re-writes their full audit rows by construction.

        BOTH index markers are deleted (O15, 2026-09-04): a re-build on a day whose earlier build
        wrote the legacy ``not_nifty200`` marker must clear those rows too, or the rollback
        guarantee holds only for rows the renamed build happened to write."""
        with self._lock:
            self._execute(
                "DELETE FROM universe_daily WHERE d = ? AND len(exclusion_reasons) = 1 "
                "AND exclusion_reasons[1] IN (?, ?)",
                [d, _EXCL_INDEX, _EXCL_INDEX_LEGACY],
            )
            return self._upsert_rows("universe_daily", rows)

    def get_universe_daily(self, d: date, *, included_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM universe_daily WHERE d = ?"
        if included_only:
            sql += " AND included"
        return self._fetch_dicts(sql + " ORDER BY symbol", [d])

    def get_universe_eligible_symbols(self, d: date) -> list[str]:
        """The ELIGIBLE universe for ``d``: every symbol that passed every §3.2.4 rule, whether or
        not it made today's top-N focus watchlist — i.e. ``included`` rows PLUS rows excluded for
        the cap alone (``exclusion_reasons == ['watchlist_cap']``). This is the brk20/ins batch-rule
        universe (``src/engine/ops/main.py`` ``run_scan_sweep``), not the ``included_only`` focus
        watchlist — use it wherever "did this symbol clear the rules" matters more than "is it in
        today's top 100" (the 2026-08-04 BPCL lesson: watchlist-cap-contaminated visibility silently
        drops otherwise-eligible symbols).
        """
        rows = self._fetch_dicts("SELECT * FROM universe_daily WHERE d = ?", [d])
        return sorted(
            r["symbol"] for r in rows
            if r["included"] or list(r["exclusion_reasons"] or []) == [_EXCL_CAP]
        )

    def get_batch_universe_symbols(self, d: date) -> list[str]:
        """The BATCH universe for ``d`` (§3.2.4 extended-leg addendum, 2026-09-01): the eligible set
        (see :meth:`get_universe_eligible_symbols`) PLUS criteria-passing NON-index symbols persisted
        with the extended-leg marker alone. This is the widest rule-passing scan set — news
        resolver/digest shadow and the pre-open breakout advisory. NO scanner reads it today: ``hi52``
        read it at birth (2026-09-01), was scoped back to the eligible set 2026-09-10 and PROMOTED
        out of shadow 2026-09-12 (plan §8.6).
        NEVER feed it to anything RECOMMEND-capable: the risk gate approves ``included`` rows only,
        so an actionable strategy scanning this set would originate un-approvable candidates.

        Either index marker counts (O15, 2026-09-04) — see :data:`_EXCL_INDEX_LEGACY`: rows written
        2026-09-01…09-04 spell it ``not_nifty200``, and reading only the new spelling would silently
        drop every pre-rename extended name out of the batch scan set."""
        rows = self._fetch_dicts("SELECT * FROM universe_daily WHERE d = ?", [d])
        return sorted(
            r["symbol"] for r in rows
            if r["included"]
            or list(r["exclusion_reasons"] or []) in ([_EXCL_CAP], [_EXCL_INDEX], [_EXCL_INDEX_LEGACY])
        )

    # ================================================================== features (§3.2.5/§6.2)
    def upsert_features_daily(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("features_daily", rows)

    def get_features_daily(self, d: date, *, feature_set_version: int | None = None) -> list[dict[str, Any]]:
        if feature_set_version is None:
            return self._fetch_dicts("SELECT * FROM features_daily WHERE d = ? ORDER BY symbol", [d])
        return self._fetch_dicts(
            "SELECT * FROM features_daily WHERE d = ? AND feature_set_version = ? ORDER BY symbol",
            [d, feature_set_version],
        )

    def insert_feature_snapshot(
        self, snapshot_id: str, symbol: str, ts: datetime, feature_set_version: int, features_json: str
    ) -> None:
        """Persist an intraday snapshot keyed by ``features_snapshot_id`` (proposals/ledger, §4.3)."""
        self._execute(
            "INSERT INTO feature_snapshots (snapshot_id, symbol, ts, feature_set_version, features) "
            "VALUES (?,?,?,?,?)",
            [snapshot_id, symbol, ts, feature_set_version, features_json],
        )

    def get_feature_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        rows = self._fetch_dicts("SELECT * FROM feature_snapshots WHERE snapshot_id = ?", [snapshot_id])
        return rows[0] if rows else None

    # ================================================================== news (§2.7 steps 1-4)
    def insert_news(self, rows: Sequence[dict[str, Any]]) -> int:
        """Insert headlines, deduping on ``url`` (idempotent off-period backfill, §4.4 job 10).

        Row keys: ``title, source_domain, url, published_at`` (+ optional ``headline_id, cluster_id``).
        ``untrusted`` is forced TRUE (§2.4); ``ingested_at`` stamped from Clock. Returns rows inserted.
        """
        inserted = 0
        now = self._clock.now()
        # ONE hold spans the whole row loop, so the note names the batch, not a statement (§2.6
        # hardening (iii), 2026-09-09): a backfill of hundreds of headlines is row-at-a-time INSERT
        # under a single _lock acquisition — the batch IS the hold every other caller waits behind.
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    con = self._require_con()
                    for row in rows:
                        hid = row.get("headline_id") or str(ULID())
                        cur = con.execute(
                            "INSERT INTO news (headline_id, title, source_domain, url, published_at, "
                            "cluster_id, untrusted, ingested_at) "
                            "SELECT ?,?,?,?,?,?,TRUE,? WHERE NOT EXISTS (SELECT 1 FROM news WHERE url = ?)",
                            [hid, row["title"], row["source_domain"], row["url"], row["published_at"],
                             row.get("cluster_id"), now, row["url"]],
                        )
                        inserted += cur.fetchone()[0]
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"insert_news:{len(rows)}", elapsed, failed=failed)
        return inserted

    def set_news_cluster(self, headline_ids: Sequence[str], cluster_id: str) -> None:
        """Assign headlines to a cluster (§2.7 step 2 output)."""
        # executemany binds row-at-a-time, so the hold scales with the cluster's size — one note for
        # the whole hold (§2.6 hardening (iii), 2026-09-09).
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    con = self._require_con()
                    con.executemany(
                        "UPDATE news SET cluster_id = ? WHERE headline_id = ?",
                        [[cluster_id, hid] for hid in headline_ids],
                    )
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"set_news_cluster:{len(headline_ids)}", elapsed, failed=failed)

    def get_news(
        self, *, published_after: datetime | None = None, unclustered_only: bool = False
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM news WHERE TRUE"
        params: list[Any] = []
        if published_after is not None:
            sql += " AND published_at >= ?"
            params.append(published_after)
        if unclustered_only:
            sql += " AND cluster_id IS NULL"
        return self._fetch_dicts(sql + " ORDER BY published_at", params)

    def upsert_news_clusters(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("news_clusters", rows)

    def get_news_clusters(
        self,
        *,
        first_seen_after: datetime | None = None,
        last_seen_after: datetime | None = None,
        scored: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Clusters filtered for the scorer batch (``scored=False``) / digest (``scored=True``) (§2.7)."""
        sql = "SELECT * FROM news_clusters WHERE TRUE"
        params: list[Any] = []
        if first_seen_after is not None:
            sql += " AND first_seen >= ?"
            params.append(first_seen_after)
        if last_seen_after is not None:
            sql += " AND last_seen >= ?"
            params.append(last_seen_after)
        if scored is True:
            sql += " AND scored_at IS NOT NULL"
        elif scored is False:
            sql += " AND scored_at IS NULL"
        return self._fetch_dicts(sql + " ORDER BY first_seen", params)

    # ================================================================== aliases / unresolved (§3.2.4)
    def upsert_entity_aliases(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("entity_aliases", rows)

    def get_entity_aliases(self) -> list[dict[str, Any]]:
        return self._fetch_dicts("SELECT * FROM entity_aliases ORDER BY alias, tradingsymbol")

    def log_unresolved_entity(
        self,
        entity_text: str,
        reason: str,
        *,
        cluster_id: str | None = None,
        candidate_symbols: Sequence[str] | None = None,
    ) -> None:
        """Record an ambiguous / out-of-universe / unmatched entity (§3.2.4 — never a guess; feeds
        the weekly alias-suggestion loop, §5.5)."""
        self._execute(
            "INSERT INTO unresolved_entities (entity_text, cluster_id, reason, candidate_symbols, logged_at) "
            "VALUES (?,?,?,?,?)",
            [entity_text, cluster_id, reason, list(candidate_symbols or []), self._clock.now()],
        )

    def get_unresolved_entities(self, *, since: datetime | None = None) -> list[dict[str, Any]]:
        if since is None:
            return self._fetch_dicts("SELECT * FROM unresolved_entities ORDER BY logged_at")
        return self._fetch_dicts(
            "SELECT * FROM unresolved_entities WHERE logged_at >= ? ORDER BY logged_at", [since]
        )

    # ================================================================== theme map / sentiment (§2.7 step 5)
    def upsert_theme_map(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("theme_map", rows)

    def get_theme_map(self) -> list[dict[str, Any]]:
        return self._fetch_dicts("SELECT * FROM theme_map ORDER BY theme")

    def upsert_sentiment_agg(self, rows: Sequence[dict[str, Any]]) -> int:
        """Write digest rows. ``raw_sum``/``n_clusters`` (the WO-22 saturation measure) are OPTIONAL:
        a row omitting them stores NULL, so a caller written before the measure existed still works."""
        return self._upsert_rows("sentiment_agg", rows)

    def get_sentiment_agg(self, as_of: datetime) -> list[dict[str, Any]]:
        """One run's rows, including ``raw_sum``/``n_clusters`` — NULL on rows digested before WO-22."""
        return self._fetch_dicts(
            "SELECT * FROM sentiment_agg WHERE as_of = ? ORDER BY scope, scope_key", [as_of]
        )

    def latest_sentiment_as_of(self) -> datetime | None:
        """Latest digest run time — the §2.7 digest-staleness input (``digest_stale_max_h``)."""
        row = self._fetchall("SELECT max(as_of) FROM sentiment_agg")[0]
        return _ist(row[0]) if row[0] is not None else None

    # ================================================================== catalyst watchlist (§2.7 step 5(ii))
    def replace_catalyst_watchlist(self, d: date, rows: Sequence[dict[str, Any]]) -> int:
        """Idempotently (re)write day ``d``'s watchlist (the digest is run-latest-once, §2.6):
        delete-then-insert so a re-run never duplicates. ``entry_id`` is minted if absent."""
        stamped = [{**row, "d": d, "entry_id": row.get("entry_id") or str(ULID())} for row in rows]
        with self._lock:
            self._execute("DELETE FROM catalyst_watchlist WHERE d = ?", [d])
            return self._upsert_rows("catalyst_watchlist", stamped)

    def get_catalyst_watchlist(self, d: date, *, grade: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM catalyst_watchlist WHERE d = ?"
        params: list[Any] = [d]
        if grade is not None:
            sql += " AND grade = ?"
            params.append(grade)
        return self._fetch_dicts(sql + " ORDER BY symbol", params)

    # ================================================================== calendar / corp / earnings / deals
    def upsert_calendar(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("calendar", rows)

    def get_calendar(self, start: date, end: date) -> list[dict[str, Any]]:
        return self._fetch_dicts(
            "SELECT * FROM calendar WHERE d >= ? AND d <= ? ORDER BY d", [start, end]
        )

    def upsert_corp_actions(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("corp_actions", rows)

    def get_corp_actions(
        self, *, symbol: str | None = None, ex_from: date | None = None, ex_to: date | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM corp_actions WHERE TRUE"
        params: list[Any] = []
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        if ex_from is not None:
            sql += " AND ex_date >= ?"
            params.append(ex_from)
        if ex_to is not None:
            sql += " AND ex_date <= ?"
            params.append(ex_to)
        return self._fetch_dicts(sql + " ORDER BY ex_date, symbol", params)

    def upsert_earnings_calendar(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("earnings_calendar", rows)

    def get_earnings_calendar(
        self, start: date, end: date, *, symbol: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM earnings_calendar WHERE event_date >= ? AND event_date <= ?"
        params: list[Any] = [start, end]
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        return self._fetch_dicts(sql + " ORDER BY event_date, symbol", params)

    def upsert_flagged_instrument_days(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("flagged_instrument_days", rows)

    def get_flagged_instrument_days(self, d: date) -> list[dict[str, Any]]:
        return self._fetch_dicts("SELECT * FROM flagged_instrument_days WHERE d = ? ORDER BY symbol", [d])

    # ================================================================== sector map (§4.4 job 13, R1)
    def upsert_sector_map(self, as_of: date, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("sector_map", [{**row, "as_of": as_of} for row in rows])

    def get_sector_map(self, *, as_of: date | None = None) -> list[dict[str, Any]]:
        """The snapshot at ``as_of`` (latest snapshot ≤ as_of), or the overall latest if None."""
        if as_of is None:
            row = self._fetchall("SELECT max(as_of) FROM sector_map")[0]
        else:
            row = self._fetchall("SELECT max(as_of) FROM sector_map WHERE as_of <= ?", [as_of])[0]
        if row[0] is None:
            return []
        return self._fetch_dicts("SELECT * FROM sector_map WHERE as_of = ? ORDER BY symbol", [row[0]])

    # ================================================================== §2.8 corporate filings (O14)
    def upsert_symbol_isin(self, rows: Sequence[dict[str, Any]]) -> int:
        """Upsert ISIN / BSE-scrip-code mappings (idempotent on ``symbol``; latest as_of wins)."""
        now = self._clock.now()
        stamped = [{"ingested_at": now, **row} for row in rows]
        return self._upsert_rows("symbol_isin", stamped)

    def get_symbol_isin(self, *, symbol: str | None = None) -> list[dict[str, Any]]:
        if symbol is not None:
            return self._fetch_dicts("SELECT * FROM symbol_isin WHERE symbol = ?", [symbol])
        return self._fetch_dicts("SELECT * FROM symbol_isin ORDER BY symbol")

    def symbol_isin_map(self) -> dict[str, dict[str, Any]]:
        """``symbol -> row`` map — the filings_shp job's BSE-scrip-code lookup (§2.8 filings_shp)."""
        return {r["symbol"]: r for r in self.get_symbol_isin()}

    def bse_scrip_symbol_map(self) -> dict[str, str]:
        """``bse_scrip_code -> symbol`` REVERSE map — the §2.8 fresh-insider feed resolves a BSE
        ``Fld_ScripCode`` back to our symbol (the whole market is served, so most scrips are
        out-of-universe and simply absent). Codes are normalized to a bare-int string so ``500325``,
        ``'500325'`` and ``'500325.0'`` all collide (idempotent with the stored TEXT column). If two
        symbols ever share a scrip code the last-seen wins (deterministic; scrip codes are unique in
        practice)."""
        out: dict[str, str] = {}
        for r in self.get_symbol_isin():
            key = _norm_scrip_code(r.get("bse_scrip_code"))
            if key:
                out[key] = r["symbol"]
        return out

    def upsert_insider_trades(self, rows: Sequence[dict[str, Any]]) -> int:
        """Upsert NSE-PIT insider trades (idempotent on the content-hash ``id``, §2.8.1)."""
        now = self._clock.now()
        stamped = [{"ingested_at": now, **row} for row in rows]
        return self._upsert_rows("insider_trades", stamped)

    def get_insider_trades(
        self, *, symbol: str | None = None,
        broadcast_from: datetime | None = None, broadcast_to: datetime | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM insider_trades WHERE TRUE"
        params: list[Any] = []
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        if broadcast_from is not None:
            sql += " AND broadcast_dt >= ?"
            params.append(broadcast_from)
        if broadcast_to is not None:
            sql += " AND broadcast_dt <= ?"
            params.append(broadcast_to)
        return self._fetch_dicts(sql + " ORDER BY broadcast_dt, id", params)

    def latest_insider_broadcast(self, source: str | None = None) -> datetime | None:
        """Latest stored PIT ``broadcast_dt`` — the filings_pit incremental-window watermark (§2.8).

        ``source`` selects the partition by id tag (§2.8.5, 2026-09-13): ``'nse'`` = the bare-id
        ``corporates-pit`` rows, ``'bse'`` = the ``bse:``-prefixed fresh-feed rows, ``None`` = the
        whole table (the pre-2026-09-13 reading, kept for callers that want "any insider row").
        Only ``'nse'`` has a production caller: ``filings_pit_fresh`` windows on DATES, never on
        a watermark, so ``'bse'`` is the symmetric reading kept for a future one.
        WHOLE-TABLE IS WRONG FOR A PER-JOB WINDOW: the BSE feed writes same-day rows, which pins the
        NSE job's window to ``[d-1, d]`` and leaves a revived NSE route unable to reopen its own gap.
        An unknown tag RAISES rather than falling back to the whole table — a silent fallback is
        exactly that bug again.
        """
        sql = "SELECT max(broadcast_dt) FROM insider_trades"
        if source is not None:
            predicate = _INSIDER_SOURCE_PREDICATE.get(source.strip().lower())
            if predicate is None:
                raise ValueError(
                    f"unknown insider source {source!r} "
                    f"(expected one of {sorted(_INSIDER_SOURCE_PREDICATE)} or None)"
                )
            sql += f" WHERE {predicate}"
        row = self._fetchall(sql)[0]
        return _ist(row[0]) if row[0] is not None else None

    def upsert_shp_quarterly(self, rows: Sequence[dict[str, Any]]) -> int:
        """Upsert SEBI-format SHP rows (idempotent on (symbol, qtr_end, category); latest wins)."""
        now = self._clock.now()
        stamped = [{"ingested_at": now, **row} for row in rows]
        return self._upsert_rows("shp_quarterly", stamped)

    def get_shp_quarterly(
        self, *, symbol: str | None = None, qtr_end: date | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM shp_quarterly WHERE TRUE"
        params: list[Any] = []
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        if qtr_end is not None:
            sql += " AND qtr_end = ?"
            params.append(qtr_end)
        return self._fetch_dicts(sql + " ORDER BY symbol, qtr_end, category", params)

    def latest_shp_broadcast(self) -> datetime | None:
        """Latest stored SHP ``broadcast_dt`` — the filings_shp new-submission watermark (§2.8)."""
        row = self._fetchall("SELECT max(broadcast_dt) FROM shp_quarterly")[0]
        return _ist(row[0]) if row[0] is not None else None

    def upsert_results_filings(self, rows: Sequence[dict[str, Any]]) -> int:
        """Upsert results-filing metadata (idempotent on (symbol, period_end, consolidated), §2.8.1)."""
        now = self._clock.now()
        stamped = [{"ingested_at": now, **row} for row in rows]
        return self._upsert_rows("results_filings", stamped)

    def get_results_filings(
        self, *, symbol: str | None = None,
        broadcast_from: datetime | None = None, broadcast_to: datetime | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM results_filings WHERE TRUE"
        params: list[Any] = []
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(symbol)
        if broadcast_from is not None:
            sql += " AND broadcast_dt >= ?"
            params.append(broadcast_from)
        if broadcast_to is not None:
            sql += " AND broadcast_dt <= ?"
            params.append(broadcast_to)
        return self._fetch_dicts(sql + " ORDER BY broadcast_dt, symbol", params)

    def latest_results_broadcast(self) -> datetime | None:
        """Latest stored results ``broadcast_dt`` — the filings_results incremental watermark (§2.8)."""
        row = self._fetchall("SELECT max(broadcast_dt) FROM results_filings")[0]
        return _ist(row[0]) if row[0] is not None else None

    # ================================================================== tick Parquet writer (§4.3)
    @property
    def pending_tick_count(self) -> int:
        with self._tick_lock:
            return len(self._tick_buffer)

    @property
    def tick_flush_skips(self) -> int:
        """Flushes that returned immediately because another was already running (WO-26a).

        A steadily climbing count is not an error — it is the pile-up that used to happen instead,
        now costing nothing. A count climbing while ``ticks_flushed`` does NOT is the shape worth
        alerting on: it means one flush never finished."""
        with self._flush_skip_lock:
            return self._flush_skips

    def stage_tick(self, tick: Tick) -> bool:
        """Append one tick to the buffer WITHOUT flushing; True when a flush is due.

        The live tick path stages here (never touching DuckDB — ``_tick_lock`` only, ~µs) and
        offloads the actual flush via :meth:`aflush_ticks` so the asyncio event loop never runs
        Parquet/DuckDB work (§2.2 heartbeat invariant; a 200-symbol flush measured ~2 s inline).
        A backlog past 4× the batch cap only WARNS — flushing inline here would run DuckDB work
        on the caller's thread (the event loop, live), the exact freeze this split exists to
        prevent; the flusher (on_tick_event / buffer_tick) is responsible for draining.
        """
        with self._tick_lock:
            self._tick_buffer.append(tick)
            n = len(self._tick_buffer)
            due = (
                n >= self._max_buffered_ticks
                or (self._clock.now() - self._last_flush_at).total_seconds() >= self._flush_interval_s
            )
        if n >= 4 * self._max_buffered_ticks and n % self._max_buffered_ticks == 0:
            _log.warning("tick_buffer_backlog", ticks=n, cap=self._max_buffered_ticks)
        return due

    def tick_flush_due(self) -> bool:
        """True when the buffered batch is due for a flush (age or size), without staging."""
        with self._tick_lock:
            return bool(self._tick_buffer) and (
                len(self._tick_buffer) >= self._max_buffered_ticks
                or (self._clock.now() - self._last_flush_at).total_seconds() >= self._flush_interval_s
            )

    def buffer_tick(self, tick: Tick) -> list[Path]:
        """Buffer one tick; flush when the batch is due (~``flush_interval_s`` s or
        ``max_buffered_ticks``, §4.3 "written in 5 s batches"). Returns files written (usually []).

        Synchronous convenience for offline/test callers; the live path uses
        :meth:`stage_tick` + :meth:`aflush_ticks` so the flush never runs on the event loop."""
        if self.stage_tick(tick):
            return self.flush_ticks()
        return []

    def flush_ticks(self, *, wait_s: float = 0.0) -> list[Path]:
        """Write the buffered batch to ``ticks/date=…/symbol=…/<ulid>.parquet`` (one file per
        (date, symbol) group in the batch) and reset the flush timer. Decimal/tz exact.

        **Single-flight with SKIP (WO-26a, 2026-08-25).** A flush that finds another flush already
        running returns IMMEDIATELY — it never queues behind ``_flush_lock``. The live path calls
        this once per tick event whose batch is due (``BarBuilder.on_tick_event``), so a flush that
        runs long used to convert every subsequent tick into one more BLOCKED worker thread: on
        08-25 a stack dump caught ~20 of them stopped at this lock inside the shared
        ``asyncio.to_thread`` pool, with the whole process starved behind them. Skipping costs
        nothing at all: the staged ticks stay in the buffer, ``_last_flush_at`` is only reset by a
        flush that actually RUNS, so the batch is still due and the next tick event flushes it.

        ``wait_s`` > 0 waits that long for the in-flight flush instead of skipping. Only
        :meth:`close` uses it — see there for why it is the one caller that may not skip.

        Lock discipline: ``_flush_lock`` serializes whole flushes (the stage table is shared
        working space); the DuckDB ``_lock`` is held only per statement — one staged bulk write
        for the WHOLE batch, then one brief COPY per (date, symbol) partition — so concurrent
        bar upserts wait at most one statement (~ms), never a whole flush."""
        acquired = (
            self._flush_lock.acquire(timeout=wait_s)
            if wait_s > 0
            else self._flush_lock.acquire(blocking=False)
        )
        if not acquired:
            self._note_flush_skipped(waited_s=wait_s)
            return []
        try:
            return self._flush_locked()
        finally:
            self._flush_lock.release()

    def _note_flush_skipped(self, *, waited_s: float = 0.0) -> None:
        """Count a skipped flush; log at most one line per ``_FLUSH_SKIP_LOG_EVERY_S`` (WO-26a).

        The counter (:attr:`tick_flush_skips`) is the telemetry; the periodic line is the
        breadcrumb that says the skip path is doing its job, carrying the running total and the
        backlog it left staged."""
        with self._flush_skip_lock:
            self._flush_skips += 1
            total = self._flush_skips
            now = self._clock.now()
            due = (
                self._flush_skip_logged_at is None
                or (now - self._flush_skip_logged_at).total_seconds() >= _FLUSH_SKIP_LOG_EVERY_S
            )
            if due:
                self._flush_skip_logged_at = now
        if due:
            _log.info(
                "flush_skipped_in_flight",
                skipped_total=total,
                pending_ticks=self.pending_tick_count,
                waited_s=waited_s,
            )

    def _tick_partition_dir(self, d: date, symbol: str) -> Path:
        """The ``ticks/date=…/symbol=…`` directory, created ONCE per (date, symbol) per process.

        WO-26a: the flush used to ``mkdir(parents=True, exist_ok=True)`` for every one of the ~200
        symbols in each batch — ~200 filesystem round-trips per pass, all of them inside
        ``_flush_lock``, all of them re-proving what the previous flush 60 s earlier had already
        established. On a busy Windows volume that is a large part of what made a flush long enough
        for callers to pile up behind it in the first place.

        The cache is cleared on DATE rollover, so it stays one trading day wide (~200 entries) and
        a new day still gets its directories created. Called only under ``_flush_lock``, which is
        what makes the unsynchronized set access safe."""
        part_dir = self._parquet_root / "ticks" / f"date={d.isoformat()}" / f"symbol={symbol}"
        if d != self._tick_dirs_day:
            self._tick_dirs_day = d
            self._tick_dirs.clear()
        if symbol not in self._tick_dirs:
            part_dir.mkdir(parents=True, exist_ok=True)
            self._tick_dirs.add(symbol)
        return part_dir

    def _flush_locked(self) -> list[Path]:
        """The flush proper. Callers hold ``_flush_lock`` — see :meth:`flush_ticks`."""
        with self._tick_lock:
            batch, self._tick_buffer = self._tick_buffer, []
            self._last_flush_at = self._clock.now()
        if not batch:
            return []
        group_keys = sorted(
            {(t.exchange_ts.astimezone(IST).date(), t.tradingsymbol) for t in batch}
        )
        # The stage wipe is its own _lock hold and its own note (§2.6 hardening (iii), 2026-09-09):
        # on a big backlog the DELETE is not free, and it runs BEFORE the bulk_write/copy_ticks pair,
        # so leaving it untimed would have blamed the next statement for its share of a stall.
        stage_elapsed = 0.0
        stage_failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    closed = self._con is None
                    if not closed:
                        self._con.execute("DELETE FROM _tick_stage")
                    stage_failed = False
                finally:
                    stage_elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement("flush_stage_delete", stage_elapsed, failed=stage_failed)
        if closed:
            # Orphaned late flush after close() (shutdown edge): restage rather than crash a
            # background worker; nothing can write these post-close — the loss is explicit.
            with self._tick_lock:
                self._tick_buffer[:0] = batch
            _log.warning("tick_flush_skipped_store_closed", ticks=len(batch))
            return []
        self._bulk_write(
            "_tick_stage", _TICK_COLUMNS, [[getattr(t, c) for c in _TICK_COLUMNS] for t in batch]
        )
        written: list[Path] = []
        for d, symbol in group_keys:
            out = self._tick_partition_dir(d, symbol) / f"{ULID()!s}.parquet"
            elapsed = 0.0
            failed = True
            try:
                with self._lock:
                    t0 = time.perf_counter()
                    try:
                        self._execute_locked(
                            self._require_con(),
                            "COPY (SELECT * FROM _tick_stage WHERE tradingsymbol = ? "
                            "AND CAST(exchange_ts AS DATE) = ? ORDER BY exchange_ts) "
                            f"TO '{out.as_posix()}' (FORMAT PARQUET)",
                            [symbol, d],
                        )
                        failed = False
                    finally:
                        elapsed = time.perf_counter() - t0
            finally:
                _note_slow_statement(f"copy_ticks:{symbol}", elapsed, failed=failed)
            written.append(out)
        _log.info("ticks_flushed", ticks=len(batch), files=len(written))
        return written

    def get_ticks(self, symbol: str, d: date) -> list[Tick]:
        """Read back a day's ticks for ``symbol`` from the Parquet dataset (fill-model calibration, R9)."""
        part_dir = self._parquet_root / "ticks" / f"date={d.isoformat()}" / f"symbol={symbol}"
        if not part_dir.exists() or not any(part_dir.glob("*.parquet")):
            return []
        rows = self._fetchall(
            f"SELECT {', '.join(_TICK_COLUMNS)} FROM read_parquet(?) ORDER BY exchange_ts",
            [(part_dir / "*.parquet").as_posix()],
        )
        return [
            Tick(**{c: (_ist(v) if c == "exchange_ts" else v) for c, v in zip(_TICK_COLUMNS, r, strict=True)})
            for r in rows
        ]

    def compact_tick_partitions(self, d: date) -> list[Path]:
        """Coalesce day ``d``'s many small batch files into ONE file per symbol (EOD housekeeping —
        keeps the 30-day window a sane file count). Idempotent; returns the compacted files."""
        day_dir = self._parquet_root / "ticks" / f"date={d.isoformat()}"
        if not day_dir.exists():
            return []
        compacted: list[Path] = []
        # ONE hold spans the whole per-symbol loop — ~200 COPYs plus their unlinks under a single
        # _lock acquisition — so it gets ONE note for the whole hold (§2.6 hardening (iii),
        # 2026-09-09), which is the number a stalled caller experienced. Per-symbol notes would say
        # "everything was fast" about a hold that lasted minutes.
        elapsed = 0.0
        failed = True
        try:
            with self._lock:
                t0 = time.perf_counter()
                try:
                    con = self._require_con()
                    for sym_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
                        files = sorted(sym_dir.glob("*.parquet"))
                        if len(files) <= 1:
                            continue
                        out = sym_dir / f"compact-{ULID()!s}.parquet"
                        con.execute(
                            f"COPY (SELECT * FROM read_parquet(?) ORDER BY exchange_ts) "
                            f"TO '{out.as_posix()}' (FORMAT PARQUET)",
                            [(sym_dir / "*.parquet").as_posix()],
                        )
                        for f in files:
                            f.unlink()
                        compacted.append(out)
                    failed = False
                finally:
                    elapsed = time.perf_counter() - t0
        finally:
            _note_slow_statement(f"compact_ticks:{d.isoformat()}", elapsed, failed=failed)
        return compacted

    # ================================================================== retention (§4.5)
    def apply_retention(self) -> dict[str, int]:
        """Purge expired data per the §4.5 policy (plan-pinned): ticks 30d (partition dirs), news +
        clusters + sentiment 1y, corrections 90d. Returns a per-dataset purge-count report."""
        now = self._clock.now()
        report: dict[str, int] = {}

        ticks_root = self._parquet_root / "ticks"
        cutoff_d = (now - timedelta(days=TICKS_RETENTION_DAYS)).date()
        removed = 0
        if ticks_root.exists():
            for part in sorted(ticks_root.glob("date=*")):
                try:
                    part_date = date.fromisoformat(part.name.split("=", 1)[1])
                except ValueError:
                    continue          # foreign dir — never delete what we can't parse
                if part_date < cutoff_d:
                    shutil.rmtree(part)
                    removed += 1
        report["tick_partitions"] = removed

        def _purge(table: str, ts_col: str, days: int) -> int:
            cutoff = now - timedelta(days=days)
            return self._fetchall(f"DELETE FROM {table} WHERE {ts_col} < ?", [cutoff])[0][0]

        report["corrections_log"] = _purge("corrections_log", "logged_at", CORRECTIONS_RETENTION_DAYS)
        report["news"] = _purge("news", "published_at", NEWS_RETENTION_DAYS)
        report["news_clusters"] = _purge("news_clusters", "last_seen", NEWS_RETENTION_DAYS)
        report["sentiment_agg"] = _purge("sentiment_agg", "as_of", NEWS_RETENTION_DAYS)
        _log.info("retention_applied", **report)
        return report

    # ================================================================== async wrappers (§3.2 conv. 4)
    # Thin offloads so scan-heavy DuckDB work never blocks the asyncio loop (§2.2 heartbeat
    # invariant). The sync core stays the single implementation. Every one of them goes through
    # :meth:`_off` onto the store's PRIVATE ``mt-store`` pool — never ``asyncio.to_thread``, whose
    # shared default executor is what a slow flush drained on 2026-08-25 (WO-26a; see :meth:`_pool`).
    async def _off(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run ``fn`` on the store's own pool — the single offload seam for the wrappers below."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool(), functools.partial(fn, *args, **kwargs))

    async def arun(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run any MarketStore method (or callable) in a worker thread — the generic offload for
        calls without a dedicated wrapper below."""
        return await self._off(fn, *args, **kwargs)

    async def aping(self) -> bool:
        """:meth:`ping` off the loop — the liveness probe the health pulse awaits (WO-24b-prime).

        Goes through the SAME offload as every other wrapper here on purpose: a probe that took a
        private thread would answer "the lock is free" while the path the engine actually uses was
        starved, which is the one lie this watchdog must not be able to tell. Since WO-26a that
        path is the ``mt-store`` pool — so this probe now tests both halves of the real thing, the
        lock AND the ability to get a worker, which is precisely the pair that failed on 08-25."""
        return await self._off(self.ping)

    async def ainsert_bars_1m(self, bars: Sequence[Bar]) -> int:
        return await self._off(self.insert_bars_1m, bars)

    async def aget_bars_1m(self, symbol: str, start: datetime, end: datetime) -> list[Bar]:
        return await self._off(self.get_bars_1m, symbol, start, end)

    async def alast_bar_time(self, symbol: str) -> datetime | None:
        return await self._off(self.last_bar_time, symbol)

    async def acoverage_gaps(self, symbol: str, start: datetime, end: datetime) -> list[datetime]:
        return await self._off(self.coverage_gaps, symbol, start, end)

    async def ahas_contiguous_coverage(self, symbol: str, start: datetime, end: datetime) -> bool:
        return await self._off(self.has_contiguous_coverage, symbol, start, end)

    async def aupsert_bars_1d(self, bars: Sequence[DailyBar]) -> int:
        return await self._off(self.upsert_bars_1d, bars)

    async def aget_bars_1d(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        return await self._off(self.get_bars_1d, symbol, start, end)

    async def adaily_bar_span(self, symbol: str) -> tuple[date | None, int]:
        return await self._off(self.daily_bar_span, symbol)

    async def aflush_ticks(self) -> list[Path]:
        """Flush on the DEDICATED single-thread ``mt-flush`` pool (WO-26a) — never the store read
        pool, never the shared default executor. Combined with the skip semantics of
        :meth:`flush_ticks`, a slow flush can now delay nothing but the next flush.

        The skip is decided HERE, on the loop, when a flush is visibly already running. The live
        caller is ``BarBuilder.on_tick_event``, which awaits this once per due tick: hopping to the
        pool only to discover the lock is taken would put both a queue entry and the tick handler's
        own latency behind a flush that can run for seconds — the pile-up in its last remaining
        form. The check is optimistic and the try-acquire inside :meth:`flush_ticks` remains the
        authority; losing the race costs one skipped cycle and never a tick, because the batch
        stays staged and stays due."""
        if self._flush_lock.locked():
            self._note_flush_skipped()
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._flush_pool(), self.flush_ticks)

    async def aget_ticks(self, symbol: str, d: date) -> list[Tick]:
        return await self._off(self.get_ticks, symbol, d)

    async def aapply_retention(self) -> dict[str, int]:
        return await self._off(self.apply_retention)

    async def ainsert_news(self, rows: Sequence[dict[str, Any]]) -> int:
        return await self._off(self.insert_news, rows)

    async def aupsert_news_clusters(self, rows: Sequence[dict[str, Any]]) -> int:
        return await self._off(self.upsert_news_clusters, rows)

    async def aget_news_clusters(self, **filters: Any) -> list[dict[str, Any]]:
        return await self._off(self.get_news_clusters, **filters)

    async def areplace_catalyst_watchlist(self, d: date, rows: Sequence[dict[str, Any]]) -> int:
        return await self._off(self.replace_catalyst_watchlist, d, rows)

    async def aget_catalyst_watchlist(self, d: date, *, grade: str | None = None) -> list[dict[str, Any]]:
        return await self._off(self.get_catalyst_watchlist, d, grade=grade)

    # --- §2.8 corporate-filings async offloads (the jobs upsert under store.arun; these are the
    #     dedicated wrappers for the read-side watermark checks the jobs/backfill do on the loop).
    #     2026-09-02 review: these five predate WO-26a and were missed by its to_thread→_off
    #     migration — the last shared-executor path into the store, now closed. ---
    async def alatest_insider_broadcast(self, source: str | None = None) -> datetime | None:
        return await self._off(self.latest_insider_broadcast, source)

    async def alatest_shp_broadcast(self) -> datetime | None:
        return await self._off(self.latest_shp_broadcast)

    async def alatest_results_broadcast(self) -> datetime | None:
        return await self._off(self.latest_results_broadcast)

    async def asymbol_isin_map(self) -> dict[str, dict[str, Any]]:
        return await self._off(self.symbol_isin_map)

    async def abse_scrip_symbol_map(self) -> dict[str, str]:
        return await self._off(self.bse_scrip_symbol_map)
