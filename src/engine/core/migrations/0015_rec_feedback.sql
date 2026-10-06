-- ------------------------- durable recommendation-feedback state (plan Q0.3)
-- In-memory dedupes re-fire after a restart; these columns/tables make the skip-reason prompt, the
-- reminders, the outcome scorecard and the outbox dedupe survive one. Prices are TEXT like
-- positions/recommendations; pct columns are REAL.
ALTER TABLE recommendations ADD COLUMN skip_reason TEXT
    CHECK (skip_reason IN ('market', 'price', 'size', 'trust', 'away', 'other'));
ALTER TABLE recommendations ADD COLUMN skip_reason_at TEXT;
ALTER TABLE recommendations ADD COLUMN reminder_sent_at TEXT;

ALTER TABLE positions ADD COLUMN owner_protected_at TEXT;
ALTER TABLE positions ADD COLUMN protection_reminders INTEGER NOT NULL DEFAULT 0;
-- Positions already OPEN are grandfathered: no protection reminders for them.
UPDATE positions SET protection_reminders = 2 WHERE state = 'OPEN';

ALTER TABLE notifications ADD COLUMN dedupe_key TEXT;
CREATE UNIQUE INDEX idx_notifications_dedupe_key ON notifications (dedupe_key)
    WHERE dedupe_key IS NOT NULL;

CREATE TABLE rec_outcomes (
    rec_id       TEXT PRIMARY KEY,
    strategy_id  TEXT,
    entry_type   TEXT,
    fill_basis   TEXT CHECK (fill_basis IN ('1m_post_delivery', 'daily')),
    status       TEXT NOT NULL
                 CHECK (status IN ('unfilled', 'open', 'closed', 'void_ca', 'unscorable')),
    fill_d       TEXT,
    fill_px      TEXT,
    exit_d       TEXT,
    exit_px      TEXT,
    exit_reason  TEXT,
    gross_pct    REAL,
    cost_pct     REAL,
    net_pct      REAL,
    net_t5       REAL,
    net_t10      REAL,
    net_t20      REAL,
    bench_pct    REAL,
    excess_pct   REAL,
    excess_t20   REAL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE universe_ew_returns (
    d   TEXT PRIMARY KEY,
    ret REAL,
    n   INTEGER NOT NULL
);
