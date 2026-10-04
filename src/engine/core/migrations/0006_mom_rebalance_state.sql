-- ---------------------------------------------------------------------- §6.1 mom rebalance state
-- WO-13 (2026-08-13, F11): ``ScanContext.mom_sessions_since_rebalance`` was hard-coded ``None`` in
-- ``engine.ops.scan_context`` (LiveScanContextProvider.__call__), so live treated EVERY trading day
-- as rebalance-due while the sweep rebalances the book every ``rebalance_days`` (§6.3 default 15)
-- TRADING SESSIONS (``engine.learning.sweep``: ``valid_positions[::rebalance_days]``) — a live/
-- backtest cadence mismatch. One row (id=1, singleton pattern per mode_state/kill_state/
-- engine_lifecycle in 0001_initial.sql): the trading day the ``mom`` book was last rebalanced.
-- NULL (fresh install) means "never rebalanced" — the provider still reports ``None`` to the
-- scanner for that case (matches the pre-existing "never ⇒ due now" semantics, MomentumScanner)
-- and then stamps this row with that day so subsequent days count sessions from a real reference
-- point instead of forever reporting "never".
CREATE TABLE mom_rebalance_state (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    last_rebalance_d TEXT
);
INSERT INTO mom_rebalance_state (id, last_rebalance_d) VALUES (1, NULL);
