-- ------------------------- §6.1 `brk20` RETEST re-arm: the broken level RESTS for 5 sessions
-- 2026-09-12 (WO-R). The 2026-09-12 pre-registered entry-mechanism backtest selected
-- `V2_limit_at_H20_N5` under its own decision rule: a limit resting at the broken level for up to
-- FIVE sessions beat both market-on-confirmation (V1) and the three-session limit (V2-3) at T+10,
-- and was the only variant geometry-viable there. The LIVE rule publishes that level exactly ONCE,
-- on the crossing session, under the pre-screen's same-day (symbol, strategy) dedupe — i.e. it ships
-- the N=1 variant, which was not among the registered variants and has no measured return at all
-- (its fill rate reads 31.4% off the registered delay histogram, against V2-5's 59.1%).
--
-- The live cost of the one-shot publication is measured, not hypothetical: of 15 `brk20` proposals on
-- 2026-09-11, SIX died on the §7.1 `entry_sanity_band` because the rested level already sat >2% below
-- LTP by the time the sweep ran (IDEA 5.24%, PAYTM 4.01%, SAIL 2.88%, KOTAKBANK 4.65%, SYRMA 3.96%,
-- CHENNPETRO 6.30%), and the band was the SOLE reject cause for three of them — the three top-scoring
-- candidates of the day, because the score is monotone in breakout margin and a wide margin is
-- exactly what puts the level far below the live price.
--
-- ONE ROW PER (symbol, crossing session) is the smallest durable fact that fixes it. Durable, not
-- in-memory, for the reason the 2026-09-11 forensics recorded against the forward queue: the engine
-- reboots inside the session (a warm-up freeze, a token refresh, a deploy), and a multi-session
-- mechanism whose state lives in process memory silently becomes a single-session one. `signal_d` is
-- the crossing session; `expires_d` is the 5th TRADING session after it, resolved through the NSE
-- calendar at write time (holidays and weekends are not sessions, and a calendar-blind +5 days would
-- quietly shorten the window over every long weekend).
--
-- Prices are TEXT decimal strings (§8.1: money never round-trips through a float) — the `ins_pending`
-- convention. `stop`/`target` are NULLABLE even though `brk20.scan_daily` always ships all three:
-- `RawLevels` permits None, and a schema that cannot represent what the type permits turns a future
-- level shape into an IntegrityError inside the sweep.
--
-- This table grants NO edge and widens NO envelope. It changes WHEN an already-admitted level is
-- offered for judgement, never the level, the band, the caps, the gate or the expectancy claim:
-- every re-publication goes through the SAME `prescreen.admit` (same day cap, same per-strategy cap,
-- same same-day dedupe) and the same §7.1 gate as the original, and `brk20` stays exploratory.
CREATE TABLE brk20_resting_levels (
    symbol     TEXT NOT NULL,
    signal_d   TEXT NOT NULL,              -- IST crossing session, ISO-8601 (sorts chronologically)
    entry      TEXT NOT NULL,              -- the broken level H20 itself, tick-rounded (decimal string)
    stop       TEXT,                       -- the candidate's own stop/target, carried VERBATIM:
    target     TEXT,                       -- a retest re-publication invents no new geometry
    score      REAL NOT NULL,              -- the ORIGINAL breakout-margin score, not a decayed one
    expires_d  TEXT NOT NULL,              -- the Nth trading session after signal_d (calendar-aware)
    status     TEXT NOT NULL DEFAULT 'resting' CHECK (status IN ('resting', 'expired')),
    created_at TEXT NOT NULL,              -- IST timestamp of the crossing-session admission
    updated_at TEXT NOT NULL,              -- IST timestamp of the last status write
    PRIMARY KEY (symbol, signal_d)
);

-- The ONE read this table serves runs every 60 s inside the trade window: "which resting rows are
-- due today?" (`status='resting' AND signal_d < today <= expires_d`) and the expiry sweep that
-- precedes it (`status='resting' AND expires_d < today`). Both lead with `status` and range over a
-- date, which is exactly this index — the per-minute cadence is what makes it worth having on a
-- table that holds a handful of rows a day.
CREATE INDEX idx_brk20_resting_levels_status_expires ON brk20_resting_levels (status, expires_d);
