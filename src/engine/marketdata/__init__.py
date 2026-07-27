"""Ticks -> 1m bars (A13/A14), historical backfill (A2), self-vs-official reconciliation (A13),
DuckDB/Parquet persistence (E4). Phase 1.

``MarketStore`` (store.py) is the SINGLE WRITER for ``data/market.duckdb`` — all DuckDB access in
the platform goes through it (§4.1; convention item 12). ``BarBuilder`` / ``BackfillJob`` /
``ReconcileJob`` (§3.2.3) are its in-package consumers.
"""

from engine.marketdata.store import (
    CORRECTIONS_RETENTION_DAYS,
    EXPECTED_TABLES,
    NEWS_RETENTION_DAYS,
    TICKS_RETENTION_DAYS,
    DailyBar,
    MarketStore,
)

__all__ = [
    "CORRECTIONS_RETENTION_DAYS",
    "EXPECTED_TABLES",
    "NEWS_RETENTION_DAYS",
    "TICKS_RETENTION_DAYS",
    "DailyBar",
    "MarketStore",
]
