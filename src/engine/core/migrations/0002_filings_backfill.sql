-- 0002_filings_backfill.sql — §2.8 corporate-filings backfill checkpoints (state.db, §4.2).
-- Mirrors backfill_checkpoints' shape (0001): a resumable per-unit watermark so scripts/backfill_filings.py
-- can seed the 3y history in ≤31-day NSE windows / per-symbol BSE SHP quarter loops and resume exactly
-- where it stopped. The checkpoint key is (feed, unit): unit = the window end "YYYY-MM-DD..YYYY-MM-DD"
-- for the NSE date-windowed feeds (pit/results), the symbol for the per-scrip BSE SHP loop.

CREATE TABLE filings_backfill_checkpoints (
    feed         TEXT NOT NULL,          -- pit | results | shp
    unit         TEXT NOT NULL,          -- "<from>..<to>" window (pit/results) OR symbol (shp)
    through_date TEXT,                   -- ISO date this (feed, unit) is completed through
    updated_at   TEXT,
    PRIMARY KEY (feed, unit)
);
