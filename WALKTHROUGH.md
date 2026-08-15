# WALKTHROUGH.md — Module-by-Module Explanation

This document explains every module in the codebase, what it does, and why it works the way it does. Written so I can walk through it verbally on a call.

---

## `app/config.py`

Loads all configuration from environment variables via `pydantic-settings`. Four settings: `PSEUDOGRAM_API_KEY` (outbound API auth and HMAC secret), `PSEUDOGRAM_BASE_URL` (mock API target), `DB_PATH` (SQLite file location), `ENABLE_SIGNATURE_VERIFICATION` (toggles HMAC checking on webhooks). No secrets hardcoded. Falls back to `.env` file locally.

---

## `app/db.py`

Provides `get_db()` — an async context manager that opens an `aiosqlite` connection with WAL mode, `synchronous=NORMAL`, and `busy_timeout=3000`. WAL allows concurrent readers while a writer holds the lock. `busy_timeout=3000` makes writers wait up to 3 seconds for the lock instead of throwing `database is locked` immediately — keeps us under the 5-second webhook ceiling with headroom.

`init_db()` runs `data/schema.sql` at startup and applies an idempotent `ALTER TABLE dm_jobs ADD COLUMN reconcile_attempts` migration for databases created before that column existed.

---

## `app/auth.py`

`verify_signature()` — HMAC-SHA256 verification over the raw request bytes (not re-serialized JSON) using `PSEUDOGRAM_API_KEY` as the secret. Checks the `X-PseudoGram-Signature: sha256=<hex>` header. Uses `hmac.compare_digest` for constant-time comparison to prevent timing attacks. Returns `True` immediately if signature verification is disabled.

---

## `app/main.py` — HTTP Handlers

### `POST /webhook`
1. Read raw bytes via `request.body()`. Accepts any content type.
2. Verify HMAC signature. If invalid → insert into `rejected_events` (not `events` — prevents PK namespace poisoning), return 401.
3. Parse JSON defensively. If unparseable, generate a fallback `event_id`.
4. `INSERT OR IGNORE INTO events` keyed on `event_id` — Layer 1 dedupe. If `rowcount == 0`, the event was already seen; insert into `duplicate_events` for later counting.
5. Return `{"ok": true}` always (except 401). The entire handler does zero outbound HTTP.
6. Wrapped in try/except — on any DB error, logs and returns 200 to prevent upstream retry storms. The event is lost in that case (documented in FAILURES.md §1).

### `POST /rules`
1. Lowercase the keyword. Check if an identical `(keyword_lower, dm_message)` pair already exists — if so, return 201 with the existing `rule_id` (idempotent). Otherwise insert a new rule.
2. Returns exactly `{rule_id, keyword, dm_message}` with keyword in original casing.

### `GET /stats`
1. Four `SELECT COUNT(*)` queries: `delivered` → `sent`, `failed` → `failed`, `pending+accepted` → `queued`. Plus `duplicates_blocked` from the `counters` table.
2. Coerces all values to `int`. Caches last-known values. On any DB error, returns the cached values instead of 500ing.

All three routes have trailing-slash aliases (`/webhook/`, `/rules/`, `/stats/`) to prevent a 307 redirect from dropping a POST body.

---

## `app/workers/ingest.py` — Ingest Worker

Polls every 100ms. Processes up to 50 unprocessed `events` rows per cycle.

For `comment.created`:
- Checks for an existing `comment.deleted` tombstone (handles out-of-order events). If found, skips job creation.
- Upserts into `comments` table.
- Scans all rules. For each keyword match (case-insensitive substring), attempts `INSERT INTO dm_jobs` with `UNIQUE(rule_id, recipient_user_id)` constraint. On `IntegrityError`, increments `duplicates_blocked` counter — this is Layer 2 dedupe.
- The `idempotency_key` is `sha256(rule_id:user_id)`, meaning the same logical DM always gets the same key across retries (until the reconciler generates a fresh one on delivery failure).

For `comment.deleted`:
- Inserts/updates a tombstone in `comments` (handles arriving before `comment.created`).
- Cancels any `pending` jobs for that comment. Does not touch `accepted` or `delivered` jobs — those DMs cannot be recalled.

Also processes `duplicate_events` rows: for redelivered `comment.created` events, checks if the text matches any rule keyword and increments `duplicates_blocked` accordingly.

---

## `app/workers/sender.py` — Sender Worker

Polls continuously. For each cycle:
1. Claims the next `pending` job where `next_attempt_at <= now`.
2. Prunes `send_log` entries older than 300s.
3. Checks rolling 61-second window in `send_log`. If >= 10 sends in window, calculates wait time from the oldest entry and sleeps.
4. **Reserves a rate slot** by inserting into `send_log` *before* the HTTP call. This is the reserve-before-send pattern — if the process crashes mid-request, the slot is still consumed, preventing a rate-limit violation at the cost of one wasted slot per crash.
5. Sends `POST /v1/dm/send` with the `Idempotency-Key` header.

Response handling:
- **202 Accepted**: Store `dm_id`, set status to `accepted`. The reconciler will confirm delivery.
- **429 Rate Limited**: Read `Retry-After` header, reschedule. Does NOT count as a send attempt.
- **400 Bad Request**: Terminal. Mark `failed` immediately.
- **500 / other**: Increment attempts, exponential backoff with jitter, max 6 attempts before terminal `failed`.
- **Network error**: Same retry logic as 500.

---

## `app/workers/reconciler.py` — Reconciler Worker

Polls every 3 seconds. Checks up to 10 `accepted` jobs older than 2 seconds via `GET /v1/dm/{dm_id}`.

- **`delivered`**: Mark `status = 'delivered'`. This is the *only* code path that sets `delivered` — `sent` in `/stats` can only increase from here.
- **`failed`**: Increment `reconcile_attempts`. If <= 3, reset to `pending` with a fresh idempotency key (`sha256(rule_id:user_id:retry{n})`). Fresh key is essential — reusing the original would make the API return the same dead `dm_id`. If > 3, mark terminal `failed`.
- **`queued`**: Touch `updated_at` to check again next cycle.
- **404**: Mark `failed`.

---

## `data/schema.sql`

Plain SQL, no ORM. Tables: `rules`, `events`, `duplicate_events`, `rejected_events`, `comments`, `dm_jobs`, `send_log`, `counters`. Key constraints: `events.event_id` PK (Layer 1), `dm_jobs UNIQUE(rule_id, recipient_user_id)` (Layer 2), `dm_jobs.idempotency_key UNIQUE`.

Includes partial indexes on `events(processed_at) WHERE NULL`, `dm_jobs(status, next_attempt_at)`, and `send_log(sent_at)` for worker scan performance during burst ingestion.

---

## `render.yaml`

Configures a Render web service with a 1GB persistent disk at `/opt/render/project/src/data`. `DB_PATH` points to the mounted disk so the SQLite database survives redeploys.
