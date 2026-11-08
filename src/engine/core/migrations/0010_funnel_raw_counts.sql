-- --------------------------------------------------- pre-screen RAW funnel counters
-- 2026-08-21: the TOP of the origination funnel — what the scanners PRODUCED, before the prescreen
-- dedupe and caps touched it — was the one funnel number living purely in ``SignalPreScreen`` process
-- memory. Every restart zeroed it, so the 22:35 ``funnel_utilization`` line reported ``raw=None``
-- ("unmeasured") on 4 of the last 6 trade days — which is exactly the "the analyst never saw it" vs
-- "nothing fired" ambiguity the funnel counters exist to remove, re-introduced by the restart. One row per
-- (session day, strategy); ``fires`` is the day's ABSOLUTE raw count, re-UPSERTed from the 60 s
-- forward-drain tick (engine.ops.pipeline) and loaded back INTO the counters when a new process
-- rolls onto the day, so counting CONTINUES across a restart instead of starting over. Absolute
-- rather than incremental for exactly that reason: an idempotent write cannot double-count a
-- replayed flush. Telemetry only — nothing here gates a trade.
CREATE TABLE funnel_raw_counts (
    d           TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    fires       INTEGER NOT NULL,
    PRIMARY KEY (d, strategy_id)
);
