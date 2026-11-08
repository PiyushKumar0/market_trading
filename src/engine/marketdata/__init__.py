"""Ticks -> 1m bars, historical backfill, self-vs-official reconciliation,
DuckDB/Parquet persistence.

``MarketStore`` (store.py) is the SINGLE WRITER for ``data/market.duckdb`` — all DuckDB access in
the platform goes through it. ``BarBuilder`` / ``BackfillJob`` /
``ReconcileJob`` are its in-package consumers.
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
