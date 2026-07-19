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
wrappers that offload via ``asyncio.to_thread`` so the asyncio loop is never blocked (§2.2 heartbeat
invariant); anything without a dedicated wrapper can be offloaded with :meth:`MarketStore.arun`.

Tick Parquet dataset (§4.3 ``ticks``): raw FULL-mode frames (cumulative volume + depth top, A13) are
buffered in memory and flushed every ``flush_interval_s`` (~5 s) or ``max_buffered_ticks``, whichever
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
import shutil
import threading
from collections.abc import Callable, Sequence
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
    """
    CREATE TABLE IF NOT EXISTS corrections_log (
        symbol            TEXT NOT NULL,
        minute            TIMESTAMPTZ NOT NULL,
        tick_ts           TIMESTAMPTZ NOT NULL,
        value             DECIMAL(12,2),
        cumulative_volume BIGINT,
        amended           BOOLEAN NOT NULL DEFAULT FALSE,
        logged_at         TIMESTAMPTZ NOT NULL
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
    """
    CREATE TABLE IF NOT EXISTS instruments_daily (
        d                DATE NOT NULL,
        instrument_token BIGINT NOT NULL,
        tradingsymbol    TEXT NOT NULL,
        name             TEXT,
        exchange         TEXT,
        segment          TEXT,
        instrument_type  TEXT,
        tick_size        DECIMAL(10,2),
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
    """
    CREATE TABLE IF NOT EXISTS sentiment_agg (
        scope     TEXT NOT NULL CHECK (scope IN ('symbol','sector','theme','market')),
        scope_key TEXT NOT NULL,
        as_of     TIMESTAMPTZ NOT NULL,
        value     DOUBLE NOT NULL,
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
        expires_at          DATE
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
        ("scope", "scope_key", "as_of", "value"),
        ("scope", "scope_key", "as_of"),
    ),
    "catalyst_watchlist": (
        ("entry_id", "d", "symbol", "grade", "direction", "event_type", "cluster_refs",
         "materiality", "source_domain_count", "event_age_h", "event_age_sessions",
         "confirm_trigger", "invalidation", "stop_band_low", "stop_band_high",
         "target_band_low", "target_band_high", "expires_at"),
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
        Tick batching knobs (§4.3: ~5 s batches). Whichever trips first flushes the buffer.
    """

    def __init__(
        self,
        db_path: str | Path,
        parquet_root: str | Path,
        clock: Clock,
        *,
        flush_interval_s: float = 5.0,
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
        self._flush_lock = threading.Lock()     # serializes whole tick flushes (shared stage table)
        self._tick_buffer: list[Tick] = []
        self._last_flush_at: datetime = clock.now()

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
        in-flight background flush (aflush_ticks worker) — a reproducible AB-BA deadlock."""
        if self._con is None:
            return
        try:
            self.flush_ticks()
        finally:
            with self._lock:
                if self._con is not None:
                    self._con.close()
                    self._con = None
        _log.info("market_store_closed", db=str(self._db_path))

    def __enter__(self) -> MarketStore:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def init_schema(self) -> None:
        """Create every §4.3 table + index. Idempotent (IF NOT EXISTS) — safe on every startup."""
        with self._lock:
            con = self._require_con()
            for stmt in _SCHEMA:
                con.execute(stmt)

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

    def _execute(self, sql: str, params: Sequence[Any] | None = None) -> duckdb.DuckDBPyConnection:
        with self._lock:
            con = self._require_con()
            return con.execute(sql, params) if params is not None else con.execute(sql)

    def _fetchall(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        with self._lock:
            return self._execute(sql, params).fetchall()

    def _fetch_dicts(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [{c: _ist(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]

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
        with self._lock:
            con = self._require_con()
            con.register(view, df)
            try:
                con.execute(f"INSERT INTO {table} ({collist}) SELECT {collist} FROM {view} {conflict}")
            finally:
                con.unregister(view)

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
        if non_pk:
            updates = ", ".join(f'"{c}" = excluded."{c}"' for c in non_pk)
            conflict = f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {updates}"
        else:
            conflict = f"ON CONFLICT ({', '.join(pk)}) DO NOTHING"
        # Deliberately executemany, NOT _bulk_write: free-form dict rows can mix date/datetime or
        # naive/aware values in one column, which per-row binding coerces per element but a single
        # DataFrame column cannot (one inferred type per column — silent first-wins or a
        # ConversionException). These batches are small (≤ a few hundred rows/day); the vectorized
        # path is for the typed high-volume writers (bars_1m / bars_1d / ticks).
        placeholders = ", ".join("?" for _ in cols)
        collist = ", ".join(f'"{c}"' for c in cols)
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) {conflict}"
        with self._lock:
            con = self._require_con()
            con.executemany(sql, [[row.get(c) for c in cols] for row in rows])
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
    ) -> None:
        """Log a late tick that arrived past the minute+5s finalize grace (§4.3 corrections_log)."""
        self._execute(
            "INSERT INTO corrections_log (symbol, minute, tick_ts, value, cumulative_volume, amended, logged_at) "
            "VALUES (?,?,?,?,?,?,?)",
            [symbol, minute, tick_ts, value, cumulative_volume, amended, self._clock.now()],
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

    def upsert_universe_daily(self, rows: Sequence[dict[str, Any]]) -> int:
        return self._upsert_rows("universe_daily", rows)

    def get_universe_daily(self, d: date, *, included_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM universe_daily WHERE d = ?"
        if included_only:
            sql += " AND included"
        return self._fetch_dicts(sql + " ORDER BY symbol", [d])

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
        with self._lock:
            con = self._require_con()
            for row in rows:
                hid = row.get("headline_id") or str(ULID())
                cur = con.execute(
                    "INSERT INTO news (headline_id, title, source_domain, url, published_at, cluster_id, "
                    "untrusted, ingested_at) "
                    "SELECT ?,?,?,?,?,?,TRUE,? WHERE NOT EXISTS (SELECT 1 FROM news WHERE url = ?)",
                    [hid, row["title"], row["source_domain"], row["url"], row["published_at"],
                     row.get("cluster_id"), now, row["url"]],
                )
                inserted += cur.fetchone()[0]
        return inserted

    def set_news_cluster(self, headline_ids: Sequence[str], cluster_id: str) -> None:
        """Assign headlines to a cluster (§2.7 step 2 output)."""
        with self._lock:
            con = self._require_con()
            con.executemany(
                "UPDATE news SET cluster_id = ? WHERE headline_id = ?",
                [[cluster_id, hid] for hid in headline_ids],
            )

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
        return self._upsert_rows("sentiment_agg", rows)

    def get_sentiment_agg(self, as_of: datetime) -> list[dict[str, Any]]:
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

    def latest_insider_broadcast(self) -> datetime | None:
        """Latest stored PIT ``broadcast_dt`` — the filings_pit incremental-window watermark (§2.8)."""
        row = self._fetchall("SELECT max(broadcast_dt) FROM insider_trades")[0]
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

    def flush_ticks(self) -> list[Path]:
        """Write the buffered batch to ``ticks/date=…/symbol=…/<ulid>.parquet`` (one file per
        (date, symbol) group in the batch) and reset the flush timer. Decimal/tz exact.

        Lock discipline: ``_flush_lock`` serializes whole flushes (the stage table is shared
        working space); the DuckDB ``_lock`` is held only per statement — one staged bulk write
        for the WHOLE batch, then one brief COPY per (date, symbol) partition — so concurrent
        bar upserts wait at most one statement (~ms), never a whole flush."""
        with self._flush_lock:
            with self._tick_lock:
                batch, self._tick_buffer = self._tick_buffer, []
                self._last_flush_at = self._clock.now()
            if not batch:
                return []
            group_keys = sorted(
                {(t.exchange_ts.astimezone(IST).date(), t.tradingsymbol) for t in batch}
            )
            with self._lock:
                closed = self._con is None
                if not closed:
                    self._con.execute("DELETE FROM _tick_stage")
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
                part_dir = self._parquet_root / "ticks" / f"date={d.isoformat()}" / f"symbol={symbol}"
                part_dir.mkdir(parents=True, exist_ok=True)
                out = part_dir / f"{ULID()!s}.parquet"
                with self._lock:
                    self._require_con().execute(
                        "COPY (SELECT * FROM _tick_stage WHERE tradingsymbol = ? "
                        "AND CAST(exchange_ts AS DATE) = ? ORDER BY exchange_ts) "
                        f"TO '{out.as_posix()}' (FORMAT PARQUET)",
                        [symbol, d],
                    )
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
        with self._lock:
            con = self._require_con()
            for sym_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
                files = sorted(sym_dir.glob("*.parquet"))
                if len(files) <= 1:
                    continue
                out = sym_dir / f"compact-{ULID()!s}.parquet"
                con.execute(
                    f"COPY (SELECT * FROM read_parquet(?) ORDER BY exchange_ts) TO '{out.as_posix()}' "
                    "(FORMAT PARQUET)",
                    [(sym_dir / "*.parquet").as_posix()],
                )
                for f in files:
                    f.unlink()
                compacted.append(out)
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
    # Thin `asyncio.to_thread` offloads so scan-heavy DuckDB work never blocks the asyncio loop
    # (§2.2 heartbeat invariant). The sync core stays the single implementation.
    async def arun(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run any MarketStore method (or callable) in a worker thread — the generic offload for
        calls without a dedicated wrapper below."""
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def ainsert_bars_1m(self, bars: Sequence[Bar]) -> int:
        return await asyncio.to_thread(self.insert_bars_1m, bars)

    async def aget_bars_1m(self, symbol: str, start: datetime, end: datetime) -> list[Bar]:
        return await asyncio.to_thread(self.get_bars_1m, symbol, start, end)

    async def alast_bar_time(self, symbol: str) -> datetime | None:
        return await asyncio.to_thread(self.last_bar_time, symbol)

    async def acoverage_gaps(self, symbol: str, start: datetime, end: datetime) -> list[datetime]:
        return await asyncio.to_thread(self.coverage_gaps, symbol, start, end)

    async def ahas_contiguous_coverage(self, symbol: str, start: datetime, end: datetime) -> bool:
        return await asyncio.to_thread(self.has_contiguous_coverage, symbol, start, end)

    async def aupsert_bars_1d(self, bars: Sequence[DailyBar]) -> int:
        return await asyncio.to_thread(self.upsert_bars_1d, bars)

    async def aget_bars_1d(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        return await asyncio.to_thread(self.get_bars_1d, symbol, start, end)

    async def aflush_ticks(self) -> list[Path]:
        return await asyncio.to_thread(self.flush_ticks)

    async def aget_ticks(self, symbol: str, d: date) -> list[Tick]:
        return await asyncio.to_thread(self.get_ticks, symbol, d)

    async def aapply_retention(self) -> dict[str, int]:
        return await asyncio.to_thread(self.apply_retention)

    async def ainsert_news(self, rows: Sequence[dict[str, Any]]) -> int:
        return await asyncio.to_thread(self.insert_news, rows)

    async def aupsert_news_clusters(self, rows: Sequence[dict[str, Any]]) -> int:
        return await asyncio.to_thread(self.upsert_news_clusters, rows)

    async def aget_news_clusters(self, **filters: Any) -> list[dict[str, Any]]:
        return await asyncio.to_thread(lambda: self.get_news_clusters(**filters))

    async def areplace_catalyst_watchlist(self, d: date, rows: Sequence[dict[str, Any]]) -> int:
        return await asyncio.to_thread(self.replace_catalyst_watchlist, d, rows)

    async def aget_catalyst_watchlist(self, d: date, *, grade: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(lambda: self.get_catalyst_watchlist(d, grade=grade))

    # --- §2.8 corporate-filings async offloads (the jobs upsert under store.arun; these are the
    #     dedicated wrappers for the read-side watermark checks the jobs/backfill do on the loop) ---
    async def alatest_insider_broadcast(self) -> datetime | None:
        return await asyncio.to_thread(self.latest_insider_broadcast)

    async def alatest_shp_broadcast(self) -> datetime | None:
        return await asyncio.to_thread(self.latest_shp_broadcast)

    async def alatest_results_broadcast(self) -> datetime | None:
        return await asyncio.to_thread(self.latest_results_broadcast)

    async def asymbol_isin_map(self) -> dict[str, dict[str, Any]]:
        return await asyncio.to_thread(self.symbol_isin_map)

    async def abse_scrip_symbol_map(self) -> dict[str, str]:
        return await asyncio.to_thread(self.bse_scrip_symbol_map)
