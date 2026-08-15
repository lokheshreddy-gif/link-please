# LinkPlease Codebase Walkthrough

This document provides a module-by-module walkthrough of the LinkPlease backend implementation.

---

## 1. Configuration (`app/config.py`)
Loaded via `pydantic-settings`. Configures environment variables:
- `PSEUDOGRAM_API_KEY`: API authentication key.
- `PSEUDOGRAM_BASE_URL`: External API target.
- `DB_PATH`: Location of the SQLite database file (`data/app.db`).
- `ENABLE_SIGNATURE_VERIFICATION`: Feature flag for HMAC webhook signature verification.

---

## 2. Database & Schema (`app/db.py` & `data/schema.sql`)
- Plain SQL tables without an ORM for explicit query auditing.
- `get_db()` provides an async context manager managing `aiosqlite` connections. Enables Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) and normal synchronous mode for fast concurrent operations.
- Tables:
  - `rules`: Keyword-to-DM message mapping (`keyword_lower` for case-insensitive lookup).
  - `events`: Ingested raw webhook payloads (`event_id` PRIMARY KEY acts as Layer 1 event redelivery guard).
  - `comments`: Extracted comment details with `deleted` flag for out-of-order tombstone support.
  - `dm_jobs`: Execution state machine (`pending` -> `accepted` -> `delivered` / `failed`). Enforces `UNIQUE(rule_id, recipient_user_id)` as Layer 2 dedupe.
  - `send_log`: Outbound request timestamps for rolling 60-second rate limiter audit.
  - `counters`: Global counter table for tracking `duplicates_blocked`.

---

## 3. Webhook Signature Verification (`app/auth.py`)
- `verify_signature(raw_bytes, signature_header)` verifies HMAC-SHA256 digests against raw request body bytes.
- Uses `hmac.compare_digest` to prevent timing attack side-channels.
- Verification operates strictly on original raw request bytes (never re-serialized JSON).

---

## 4. FastAPI Routes & Lifespan (`app/main.py`)
- Lifespan context manager runs `init_db()` and spawns three background worker tasks (`ingest_worker_loop`, `sender_worker_loop`, `reconciler_worker_loop`).
- `POST /webhook`: Fast ingest endpoint returning `{"ok": true}` in <5s. Zero outbound network I/O.
- `POST /rules`: Validates and registers new automation rules.
- `GET /stats`: Evaluates `SELECT COUNT(*)` live over `dm_jobs` and reads `duplicates_blocked` from `counters`.
- `GET /healthz`: Standard liveness check.

---

## 5. Ingest Worker (`app/workers/ingest.py`)
- Polls unprocessed events every 100ms.
- Handles `comment.created`: upserts comment metadata, checks for existing `deleted=1` tombstones, performs case-insensitive keyword matching (`keyword_lower in text.lower()`), and inserts jobs into `dm_jobs` with deterministic idempotency keys (`sha256(rule_id + ":" + recipient_user_id)`).
- Handles `comment.deleted`: creates tombstone records and cancels any pending jobs for that comment.
- Increments `duplicates_blocked` counter whenever `UNIQUE(rule_id, recipient_user_id)` constraint prevents duplicate job creation.

---

## 6. Sender Worker (`app/workers/sender.py`)
- Continuous worker loop claiming `pending` jobs (`next_attempt_at <= now`).
- Rolling rate limiter: checks `send_log` count over past 60s. Maintains 1 slot headroom (max 9 sends/60s).
- Reserves `send_log` slot *before* HTTP dispatch for crash resilience.
- Sends `POST /v1/dm/send` with `X-API-Key` and `Idempotency-Key`.
- Handles `202 Accepted` (moves job to `accepted`), `429 Rate Limited` (sleeps `Retry-After`), `400 Bad Request` (terminal `failed`), and `500` / connection errors (exponential backoff up to 6 attempts).

---

## 7. Reconciler Worker (`app/workers/reconciler.py`)
- Runs every 3s checking jobs in `accepted` state older than 2s via `GET /v1/dm/{dm_id}`.
- Confirms `delivered` state (`status = 'delivered'`).
- Handles ~15% flipped failure cases: if external API returns `failed`, resets job to `pending` with a **fresh** idempotency key (`...:retry{n}`). Caps reconcile retries at 3 before marking final `failed`.

---

## 8. Verification & Audit Scripts (`scripts/`)
- `scripts/ratecheck.py`: Audits `send_log` table across all 60-second sliding windows.
- `scripts/verify.py`: Runs simulation tests against running instances, polls `/stats`, and diffs against truth data.
