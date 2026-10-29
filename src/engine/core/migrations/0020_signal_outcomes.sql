-- Hindsight outcome of every published brk20/hi52/ins signal (review §9 #5/#6, 2026-10-29). HINDSIGHT:
-- read by owner surfaces, verdict code and research only — never by a scanner, threshold, gate, prompt
-- or promotion path (the rec_outcomes rule). Constructs:
--   rule         — the strategy's measured construct: hi52/ins open of d; brk20 a touch of the resting level
--                  on signal_d..signal_d+4 sessions (V2-5's y+1..y+5), first publication only; no stop/target
--   rule_blocked — the same brk20 construct for a crossing C2 blocked (brk20_regime_blocked)
--   next_open    — open of the first session after d (strictly after the analyst's decision), every row
-- T+k exits at the close of the k-th regular session counting the fill session as 1 (muhurat excluded).
-- cost_pct is fixed when a row is first scored; fill and net_* freeze once set; a later pass may only
-- fill bench_t20/excess_t20.
CREATE TABLE signal_outcomes (
    d           TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    construct   TEXT NOT NULL CHECK (construct IN ('rule', 'rule_blocked', 'next_open')),
    status      TEXT NOT NULL CHECK (status IN ('open', 'closed', 'unfilled', 'void_ca', 'unscorable')),
    fill_d      TEXT,
    fill_px     TEXT,
    exit_d      TEXT,               -- the T+20 exit session, or the voiding ex-date for void_ca
    cost_pct    REAL,
    net_t5      REAL,
    net_t10     REAL,
    net_t20     REAL,
    bench_t20   REAL,
    excess_t20  REAL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (d, symbol, strategy_id, construct)
);

-- C2's blocked arm (T5 writes it on the loop thread; one row per blocked crossing per signal session).
CREATE TABLE brk20_regime_blocked (
    symbol      TEXT NOT NULL,
    signal_d    TEXT NOT NULL,      -- the sweep session y+1, as brk20_resting_levels.signal_d
    entry       TEXT NOT NULL,
    stop        TEXT,
    target      TEXT,
    score       REAL NOT NULL,
    index_close REAL,
    sma50       REAL,
    sma50_then  REAL,
    reason      TEXT NOT NULL,      -- 'down' | 'undefined' | 'index_bar_missing'
    created_at  TEXT NOT NULL,
    PRIMARY KEY (symbol, signal_d)
);
