-- SQLite WAL mode and pragmas are enabled programmatically on connection open.

CREATE TABLE IF NOT EXISTS rules (
    rule_id TEXT PRIMARY KEY,
    keyword_lower TEXT NOT NULL,
    dm_message TEXT NOT NULL,
    created_at REAL NOT NULL
);

-- event_id PK is the redelivery guard.
-- INSERT OR IGNORE; rowcount 0 == duplicate event, drop it.
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    raw_body TEXT NOT NULL,
    signature_valid INTEGER NOT NULL,
    received_at REAL NOT NULL,
    processed_at REAL
);

-- Duplicate raw webhook events received (rowcount == 0 on events insert)
CREATE TABLE IF NOT EXISTS duplicate_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    raw_body TEXT NOT NULL,
    received_at REAL NOT NULL,
    processed_at REAL
);

-- Rejected webhook events failing signature verification
CREATE TABLE IF NOT EXISTS rejected_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    raw_body TEXT NOT NULL,
    received_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    post_id TEXT,
    text TEXT,
    user_id TEXT NOT NULL,
    username TEXT,
    created_at REAL NOT NULL,
    deleted INTEGER DEFAULT 0
);

-- Core DM execution table
-- UNIQUE(rule_id, recipient_user_id) is the primary "never DM same user twice for one rule" guarantee.
CREATE TABLE IF NOT EXISTS dm_jobs (
    job_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    recipient_user_id TEXT NOT NULL,
    comment_id TEXT NOT NULL,
    message TEXT NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL,
    dm_id TEXT,
    attempts INTEGER DEFAULT 0,
    reconcile_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(rule_id, recipient_user_id)
);

-- send_log tracks outbound calls for the rolling 60-second rate limiter (max 9 requests per 60s)
CREATE TABLE IF NOT EXISTS send_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sent_at REAL NOT NULL
);

-- Atomic counter table for global counters (specifically duplicates_blocked)
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

-- Pre-seed duplicates_blocked counter row if it doesn't exist
INSERT OR IGNORE INTO counters (name, value) VALUES ('duplicates_blocked', 0);
