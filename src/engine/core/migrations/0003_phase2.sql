-- 0003_phase2.sql — Phase-2 tables (§5.1, §5.3, §5.5, §7.1, §3.5.3).
-- Same conventions as 0001: timestamps are ISO-8601 tz-aware IST strings produced via core.Clock (never
-- naive); money is TEXT (Decimal-as-string); JSON payloads stored as TEXT.

-- ---------------------------------------------------------------- DayPlan store (§5.3)
-- One DayPlan JSON row per trading day — distinct from the raw agent-call audit (agent_calls below),
-- which records every Tier-1 invocation whether or not it produced a plan the day acted on.
CREATE TABLE day_plans (
    d          TEXT PRIMARY KEY,           -- trading date (YYYY-MM-DD)
    payload    TEXT NOT NULL,              -- full DayPlan (JSON)
    created_at TEXT NOT NULL
);

-- ---------------------------------------------------------------- Tier-1 agent-call audit (§5.1)
-- Every Tier-1 call persists inputs_digest, gzipped context snapshot, raw structured output, usage and
-- cost so a call is replayable offline without re-hitting the LLM.
CREATE TABLE agent_calls (
    call_id       TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL,
    trigger       TEXT,
    model         TEXT,
    inputs_digest TEXT,
    context_gz    BLOB,                    -- gzipped context snapshot
    output_json   TEXT,                    -- raw structured output (JSON)
    ok            INTEGER NOT NULL,
    fail_reason   TEXT,
    in_tokens     INTEGER,
    out_tokens    INTEGER,
    cache_read    INTEGER,
    cache_write   INTEGER,
    cost_usd      TEXT,
    duration_ms   INTEGER,
    at            TEXT NOT NULL
);
CREATE INDEX idx_agent_calls_agent_at ON agent_calls(agent_id, at);

-- ---------------------------------------------------------------- platform equity time series (§7.1)
-- Persisted each minute by ExposureTracker; basis for the floor ladder + weekly drawdown checks.
CREATE TABLE equity_snapshots (
    at             TEXT PRIMARY KEY,
    equity         TEXT NOT NULL,
    realized_pnl   TEXT,
    open_mtm       TEXT,
    day_mtm        TEXT,
    positions_open INTEGER
);

-- ---------------------------------------------------------------- risk-state per-cause latch ledger (§3.5.3)
-- Risk states are reached by direct per-cause edges, most-restrictive-wins; re-arm requires every
-- latching cause cleared. Active cause = cleared_at IS NULL.
CREATE TABLE risk_state_causes (
    cause      TEXT PRIMARY KEY,
    state      TEXT NOT NULL CHECK (state IN ('FROZEN','CLOSE_ONLY','KILLED')),
    detail     TEXT,
    set_at     TEXT NOT NULL,
    cleared_at TEXT
);

-- ---------------------------------------------------------------- NightlyReview store (§5.5)
-- param_suggestions read by GET /config/params.
CREATE TABLE nightly_reviews (
    d          TEXT PRIMARY KEY,           -- trading date (YYYY-MM-DD)
    payload    TEXT NOT NULL,              -- full NightlyReview (JSON)
    created_at TEXT NOT NULL
);
