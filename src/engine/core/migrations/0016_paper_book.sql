-- ------------------------- paper book storage (plan Q3.3)
-- Additive only. Prices are TEXT like the rest of the schema. gtts: trigger_low = stop, trigger_high =
-- target (NULL for a single-trigger GTT); paper GTT ids start at 900001 (the broker seeds the counter, Q4.1).
-- positions.close_basis / strategy_id / exit_session are set for paper positions, NULL for real ones.
-- paper_state is NOT seeded: the control module inserts the (disabled) row on first use.
-- Precheck on the live state.db, read-only, 2026-10-07 03:50 IST: orders 0 rows, no duplicate entry
-- verdict_ids, gtts 0 rows -- so the unique index below cannot fail on existing data.
ALTER TABLE verdicts ADD COLUMN is_paper INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gtts ADD COLUMN is_paper INTEGER NOT NULL DEFAULT 0;
ALTER TABLE gtts ADD COLUMN symbol TEXT;
ALTER TABLE gtts ADD COLUMN side TEXT;
ALTER TABLE gtts ADD COLUMN product TEXT;
ALTER TABLE gtts ADD COLUMN qty INTEGER;
ALTER TABLE gtts ADD COLUMN stop_limit TEXT;
ALTER TABLE gtts ADD COLUMN target_limit TEXT;
ALTER TABLE gtts ADD COLUMN last_price TEXT;
ALTER TABLE gtts ADD COLUMN created_at TEXT;

ALTER TABLE positions ADD COLUMN strategy_id TEXT;
ALTER TABLE positions ADD COLUMN exit_session TEXT;
ALTER TABLE positions ADD COLUMN close_basis TEXT;

CREATE UNIQUE INDEX idx_orders_entry_verdict ON orders (verdict_id) WHERE role = 'entry';

CREATE TABLE paper_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    enabled             INTEGER NOT NULL DEFAULT 0,
    changed_at          TEXT,
    changed_by          TEXT,
    last_observed_at    TEXT,
    epoch_started_at    TEXT,
    reset_requested_at  TEXT
);

CREATE TABLE paper_equity_snapshots (
    at             TEXT PRIMARY KEY,
    equity         TEXT NOT NULL,
    realized_pnl   TEXT,
    open_mtm       TEXT,
    day_mtm        TEXT,
    positions_open INTEGER
);

CREATE TABLE paper_halts (
    cause       TEXT PRIMARY KEY,
    rung        TEXT NOT NULL,
    set_at      TEXT NOT NULL,
    cleared_at  TEXT,
    latched     INTEGER NOT NULL DEFAULT 0
);
