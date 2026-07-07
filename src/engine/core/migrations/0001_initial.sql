-- 0001_initial.sql — SQLite transactional state, migrations v1 (§4.2 + §6.5).
-- All timestamps are ISO-8601 tz-aware IST strings produced via core.Clock (never naive). Money/prices
-- are TEXT (Decimal-as-string — JSON/SQLite have no decimal type; avoids float corruption of ticks).
-- JSON payloads stored as TEXT via the SQLite JSON1 functions.

-- ---------------------------------------------------------------- decision provenance chain (R1/R8)
CREATE TABLE proposals (
    proposal_id    TEXT PRIMARY KEY,
    agent_id       TEXT NOT NULL,
    action         TEXT NOT NULL,                  -- enter | exit | modify-stop | modify-target | cancel
    payload        TEXT NOT NULL,                  -- full ActionProposal (JSON)
    inputs_digest  TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE verdicts (
    verdict_id     TEXT PRIMARY KEY,
    proposal_id    TEXT NOT NULL REFERENCES proposals(proposal_id),
    verdict        TEXT NOT NULL,                  -- approve | shrink | reject | owner_approval_required
    payload        TEXT NOT NULL,                  -- full GateVerdict (JSON)
    evaluated_at   TEXT NOT NULL
);
CREATE INDEX idx_verdicts_proposal ON verdicts(proposal_id);

-- ---------------------------------------------------------------- orders + transition audit (§3.5.1)
CREATE TABLE orders (
    order_id          TEXT PRIMARY KEY,            -- platform ULID
    broker_order_id   TEXT,
    position_id       TEXT,
    proposal_id       TEXT,
    verdict_id        TEXT,
    role              TEXT NOT NULL CHECK (role IN
                          ('entry','protective_sl','target','squareoff','exit','gtt_leg')),
    is_paper          INTEGER NOT NULL DEFAULT 0,  -- §3.5.3 routing; R9 live-vs-paper joins
    state             TEXT NOT NULL,
    product           TEXT NOT NULL,
    side              TEXT,
    qty               INTEGER,
    filled_qty        INTEGER DEFAULT 0,
    price             TEXT,
    trigger_price     TEXT,
    modifications     INTEGER DEFAULT 0,           -- self-cap 20 (A2)
    reject_reason     TEXT,
    raw_broker_payload TEXT,
    created_at        TEXT,
    updated_at        TEXT
);
CREATE INDEX idx_orders_broker ON orders(broker_order_id);
CREATE INDEX idx_orders_position ON orders(position_id);
CREATE INDEX idx_orders_state ON orders(state);

CREATE TABLE order_events (
    id          INTEGER PRIMARY KEY,
    order_id    TEXT NOT NULL REFERENCES orders(order_id),
    from_state  TEXT,
    to_state    TEXT,
    payload     TEXT,
    at          TEXT NOT NULL
);
CREATE INDEX idx_order_events_order ON order_events(order_id);

-- ---------------------------------------------------------------- positions (§3.5.2)
CREATE TABLE positions (
    position_id       TEXT PRIMARY KEY,
    symbol            TEXT NOT NULL,
    side              TEXT,
    style             TEXT,                        -- intraday | swing | position
    product           TEXT,                        -- MIS | CNC
    qty               INTEGER,
    avg_entry         TEXT,
    stop              TEXT,
    target            TEXT,
    state             TEXT,                        -- PENDING_ENTRY | OPEN | PENDING_EXIT | CLOSED | DISCARDED
    protection_state  TEXT,                        -- PROTECTED | PROTECTION_PENDING | PROTECTION_FAILED
    is_paper          INTEGER NOT NULL DEFAULT 0,
    origin            TEXT NOT NULL CHECK (origin IN ('platform','external','recommended')),
    close_reason      TEXT,                        -- §3.5.2 reasons (target|stop|square_off|broker_rms|...)
    opened_at         TEXT,
    closed_at         TEXT,
    realized_pnl      TEXT,
    costs             TEXT
);
CREATE INDEX idx_positions_state ON positions(state);
CREATE INDEX idx_positions_symbol ON positions(symbol);

-- ---------------------------------------------------------------- GTT lifecycle (A12)
CREATE TABLE gtts (
    gtt_id              INTEGER PRIMARY KEY,
    position_id         TEXT,
    state               TEXT,
    trigger_low         TEXT,
    trigger_high        TEXT,
    last_verified_at    TEXT,
    ex_date_adjusted_for TEXT
);

-- ---------------------------------------------------------------- sticky control-plane state (R5/R10)
CREATE TABLE mode_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    mode        TEXT NOT NULL,
    routing     TEXT CHECK (routing IN ('paper','live')),   -- valid only with AUTO (§3.5.3)
    risk_state  TEXT NOT NULL,
    reason      TEXT,
    changed_by  TEXT,
    changed_at  TEXT
);

CREATE TABLE kill_state (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    killed    INTEGER NOT NULL DEFAULT 0,                    -- checked before ANY order on startup (R10)
    reason    TEXT,
    at        TEXT,
    reset_by  TEXT,
    reset_at  TEXT
);

CREATE TABLE trade_window_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),  -- sticky owner-set window (§7.1/§3.2.7)
    start_ist           TEXT,
    end_ist             TEXT,
    squareoff_buffer_min INTEGER,
    set_by              TEXT,
    changed_at          TEXT
);

-- ---------------------------------------------------------------- startup catch-up watermarks (§2.6)
CREATE TABLE job_runs (
    job_id          TEXT NOT NULL,
    run_for_date    TEXT NOT NULL,
    last_success_at TEXT,
    last_attempt_at TEXT,
    status          TEXT,
    PRIMARY KEY (job_id, run_for_date)
);

-- ---------------------------------------------------------------- budget governor ledger (D6)
CREATE TABLE budget_ledger (
    id          INTEGER PRIMARY KEY,
    agent_id    TEXT,
    model       TEXT,
    at          TEXT,
    in_tokens   INTEGER,
    out_tokens  INTEGER,
    cache_read  INTEGER,
    cache_write INTEGER,
    cost_usd    TEXT,
    month       TEXT
);
CREATE INDEX idx_budget_month ON budget_ledger(month, agent_id);

-- ---------------------------------------------------------------- protected store integrity (R4)
CREATE TABLE protected_config (
    name             TEXT PRIMARY KEY,             -- limits.yaml | envelope.yaml
    sha256           TEXT NOT NULL,
    updated_by       TEXT,
    updated_at       TEXT,
    content_snapshot TEXT
);

CREATE TABLE config_audit (
    id     INTEGER PRIMARY KEY,
    name   TEXT,
    diff   TEXT,
    actor  TEXT,
    at     TEXT
);

CREATE TABLE owner_approvals (
    approval_id  TEXT PRIMARY KEY,
    kind         TEXT,                             -- stop_widen | target_extend | ... (R1)
    payload      TEXT,
    status       TEXT,                             -- pending | approved | rejected
    requested_at TEXT,
    resolved_at  TEXT
);

-- ---------------------------------------------------------------- learning ledger + satellites (§6.5)
CREATE TABLE learning_ledger (
    entry_id            TEXT PRIMARY KEY,
    position_id         TEXT,
    rec_id              TEXT,
    is_paper            INTEGER NOT NULL DEFAULT 0,
    strategy_id         TEXT,
    param_set_id        TEXT,
    feature_set_version TEXT,
    features_snapshot_id TEXT,
    thesis              TEXT,
    confidence          REAL,
    agent_id            TEXT,
    proposal_id         TEXT,
    verdict_id          TEXT,
    baseline_signal     INTEGER,
    llm_filter_decision TEXT,
    entry_px            TEXT,
    exit_px             TEXT,
    qty                 INTEGER,
    costs               TEXT,
    gross_pnl           TEXT,
    net_pnl             TEXT,
    mae                 TEXT,
    mfe                 TEXT,
    holding_minutes     INTEGER,
    close_reason        TEXT,
    slippage_entry      TEXT,
    slippage_exit       TEXT,
    ex_date_effect      INTEGER,
    flagged_day         INTEGER,
    regime_label        TEXT,
    outcome_label       TEXT,                       -- win|loss|scratch|process_error|no_action (§3.6)
    created_at          TEXT,
    closed_at           TEXT
);
CREATE INDEX idx_ledger_strategy ON learning_ledger(strategy_id);
CREATE INDEX idx_ledger_paper ON learning_ledger(is_paper);

CREATE TABLE param_sets (
    param_set_id      TEXT PRIMARY KEY,
    strategy_id       TEXT,
    params            TEXT,                         -- JSON
    status            TEXT CHECK (status IN ('candidate','shadow','champion','retired','rolled_back')),
    validation_report TEXT,                         -- JSON; must cite trial count N (§6.4)
    evaluated_at      TEXT,
    enabled           INTEGER NOT NULL DEFAULT 1,   -- per-strategy freeze flag (§6.4 step 5)
    promoted_at       TEXT,
    retired_at        TEXT
);

CREATE TABLE model_registry (
    model_id       TEXT PRIMARY KEY,
    kind           TEXT,
    artifact_path  TEXT,
    trained_through TEXT,
    status         TEXT,
    drift_state    TEXT
);

CREATE TABLE envelope_state (
    parameter     TEXT PRIMARY KEY,                 -- the ONE learner-writable config surface (R4)
    value         TEXT,
    bounds_sha256 TEXT NOT NULL,                    -- must match protected_config row for envelope.yaml
    set_by        TEXT CHECK (set_by IN ('default','owner','promotion')),
    param_set_id  TEXT,
    updated_at    TEXT
);

CREATE TABLE shadow_trades (
    id                  INTEGER PRIMARY KEY,
    param_set_id        TEXT,
    symbol              TEXT,
    side                TEXT,
    entry_px            TEXT,
    exit_px             TEXT,
    qty                 INTEGER,
    costs               TEXT,
    gross_pnl           TEXT,
    net_pnl             TEXT,
    outcome_label       TEXT,
    gate_rejected       INTEGER,
    regime_label        TEXT,
    baseline_signal     INTEGER,
    llm_filter_decision TEXT,
    opened_at           TEXT,
    closed_at           TEXT
);
CREATE INDEX idx_shadow_param ON shadow_trades(param_set_id);

-- ---------------------------------------------------------------- RECOMMEND ledger (§3.6)
CREATE TABLE recommendations (
    rec_id           TEXT PRIMARY KEY,
    payload          TEXT,                          -- full Recommendation (JSON)
    delivered_at     TEXT,
    human_action     TEXT,                          -- taken | expired | dismissed | closed
    human_fill_price TEXT,
    outcome          TEXT                           -- JSON
);

-- ---------------------------------------------------------------- backfill checkpoints (A2)
CREATE TABLE backfill_checkpoints (
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    through_date TEXT,
    PRIMARY KEY (symbol, interval)
);

-- ---------------------------------------------------------------- safe-default singletons
-- Fresh install defaults to OFF/NORMAL and NOT killed — the safe no-trading state (O2/R10).
-- trade_window_state is intentionally left empty; lifecycle seeds it from settings.yaml on first run
-- (NSECalendar falls back to the settings seed until a row exists).
INSERT INTO mode_state (id, mode, routing, risk_state, reason, changed_by, changed_at)
VALUES (1, 'OFF', NULL, 'NORMAL', 'fresh_install_default', 'system', NULL);

INSERT INTO kill_state (id, killed, reason, at, reset_by, reset_at)
VALUES (1, 0, NULL, NULL, NULL, NULL);

-- ---------------------------------------------------------------- engine lifecycle (§4.2/§2.2)

-- One row (id=1). Tri-state `state` REPLACES the old boolean "intend to run" sentinel (a boolean cannot
-- distinguish "cleanly shutting down" from "running-and-wedged"); the alias "intend to run" == state!='STOPPED'
-- gates NSSM restart-on-failure. Crash/interrupt is read off `state` (RUNNING|STOPPING + dead pid), never a
-- timestamp compare (§2.2/§2.6 step 0). Watchdog debounce (last_down_alert_at) lives in the watchdog's OWN
-- file (data/watchdog_state.json), NOT here — the watchdog is never a second writer to state.db (§2.2).


CREATE TABLE engine_lifecycle (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    state             TEXT NOT NULL DEFAULT 'STOPPED'
                          CHECK (state IN ('RUNNING','STOPPING','STOPPED')),  -- RUNNING=up; STOPPING=clean
                                                                              --   teardown; STOPPED=clean/idle
    last_alive_at     TEXT,                          -- liveness heartbeat (dedicated OS thread, Phase 1)
    pid               INTEGER,                        -- current owner pid; watchdog cross-checks it is ALIVE
    started_at        TEXT,
    last_clean_stop_at TEXT,
    version           TEXT
);

-- Fresh install: nothing running/intended => STOPPED, the safe no-trading default (watchdog silent, §2.6).
INSERT INTO engine_lifecycle (id, state, last_alive_at, pid, started_at, last_clean_stop_at, version)
VALUES (1, 'STOPPED', NULL, NULL, NULL, NULL, NULL);
