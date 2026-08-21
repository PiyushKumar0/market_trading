-- ------------------------------------- §3.2.11 owner-notification journal + retry outbox (WO-24d)
-- 2026-08-21: ``TelegramBot._send_text`` DROPPED on failure — logged, never queued, never stored.
-- On this box's flaky path that is not a theoretical loss: 223 ConnectTimeouts on 08-20 and 186 on
-- 08-19, so a RECOMMENDATION fired during an outage vanished with nothing on the platform able to
-- say it had ever existed. ONE mechanism answers both halves of that: every outbound owner message
-- is journalled HERE before it is attempted, so this table is simultaneously the RETRY OUTBOX (the
-- 30 s drainer re-attempts ``pending`` rows oldest-first) and the DASHBOARD's data source
-- (``GET /notifications`` reads one day chronologically). A second table would fork the two and let
-- the owner's screen disagree with what was actually delivered.
--
-- ``status`` IS the state machine: ``pending`` (queued, or retrying under backoff) → ``delivered``,
-- or → ``failed`` when a NON-critical row has been pending 6 h. Critical rows (severity=critical, or
-- a kind in ``telegram.CRITICAL_KINDS`` — recommendation / rec-fill / login / kill / freeze) NEVER
-- expire: they retry until delivered or the process ends. ``attempts`` + ``last_attempt_at`` drive
-- the per-row exponential backoff (30 s doubling to a 300 s ceiling); ``last_error`` is the bounded
-- ``ExcClass: first line`` label the dashboard shows on a row that is not getting through.
--
-- Message TEXT is stored as the catalog's own ``title``/``body`` split rather than as one rendered
-- blob: the dashboard needs the two separately (title in the row, body behind a disclosure), and the
-- drainer re-renders the exact wire text from ``severity``+``title``+``body`` — the split into
-- Telegram-sized parts is a transport detail that is deliberately NOT journalled (one notification,
-- one row, however many parts the wire needed).
CREATE TABLE notifications (
    notification_id TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    kind            TEXT,
    severity        TEXT,
    title           TEXT NOT NULL,
    body            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    delivered_at    TEXT,
    last_error      TEXT,
    -- Not in the original column sketch: the drainer needs a per-row "when was this last tried"
    -- clock to apply backoff, and deriving it from ``delivered_at``/``created_at`` cannot express
    -- "attempted at T and failed". One column is cheaper than that ambiguity.
    last_attempt_at TEXT
);

-- Both readers scan by time: the dashboard takes one IST day as a ``created_at`` RANGE (ISO-8601
-- with a fixed +05:30 offset sorts lexicographically, so the range IS the day) and the drainer takes
-- the oldest pending rows ``ORDER BY created_at``. One index serves both.
CREATE INDEX idx_notifications_created_at ON notifications (created_at);
