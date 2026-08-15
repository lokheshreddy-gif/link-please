# LinkPlease Comment→DM Automation Backend

A backend service built with Python 3.11, FastAPI, `aiosqlite` (WAL mode, plain SQL), `httpx`, and background `asyncio` workers. Receives Instagram comment webhooks, matches keywords to rules, and sends DMs via the Pseudogram mock API.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PSEUDOGRAM_API_KEY` | `""` | API key used in `X-API-Key` header for outbound calls and as HMAC secret for webhook signature verification |
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | Base URL of external Pseudogram API |
| `DB_PATH` | `data/app.db` | Path to the single-file SQLite database |
| `ENABLE_SIGNATURE_VERIFICATION` | `false` | Set `true` to enforce HMAC-SHA256 signature validation on `POST /webhook` |

---

## Architecture

1. **`POST /webhook`** — Reads raw bytes, validates HMAC-SHA256 signature (rejects to `rejected_events` with 401 on failure). Valid events: `INSERT OR IGNORE INTO events` keyed on `event_id` (Layer 1 dedupe). Redelivered events (rowcount=0) go to `duplicate_events`. Returns 200 with zero outbound I/O. Accepts any content type.
2. **Database** — Single-file SQLite at `DB_PATH`, opened with `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=3000`. Plain SQL schema in `data/schema.sql` with idempotent migrations in `app/db.py`.
3. **`POST /rules`** — Stores keyword in lowercase. If an existing rule has identical `keyword_lower` AND `dm_message`, returns 201 with the existing `rule_id`. Different message with the same keyword creates a new rule.
4. **Ingest Worker** (`app/workers/ingest.py`) — Polls unprocessed events every 100ms. Upserts comment metadata, handles out-of-order `comment.deleted` tombstones, performs case-insensitive substring matching against all rules, and inserts jobs into `dm_jobs`. Also processes `duplicate_events` rows, incrementing `duplicates_blocked` when a redelivered comment matched a rule.
5. **Deduplication** — `dm_jobs` has `UNIQUE(rule_id, recipient_user_id)`. Constraint violations increment the `duplicates_blocked` counter (Layer 2 dedupe).
6. **Sender Worker** (`app/workers/sender.py`) — Polls pending jobs. Rolling 61-second rate limiter (max 10 sends/61s) via `send_log` table. Reserves the rate slot *before* the HTTP call for crash safety. Sends `POST /v1/dm/send` with `Idempotency-Key`. Retries on 500/429 with backoff; marks 400 as terminal `failed`.
7. **Reconciler Worker** (`app/workers/reconciler.py`) — Polls `accepted` jobs older than 2s via `GET /v1/dm/{dm_id}`. Sets `delivered` on success. On `failed`, increments `reconcile_attempts`, resets to `pending` with a fresh idempotency key. After 3 reconcile failures, marks terminally `failed`.
8. **`GET /stats`** — Live `SELECT COUNT(*)` queries. Returns exactly `{sent, failed, queued, duplicates_blocked}` as integers. Never 500s — falls back to last-known values on DB error.

---

## Stats Definitions

- **`sent`**: `dm_jobs` rows with `status = 'delivered'` — only set after `GET /v1/dm/{dm_id}` confirms delivery.
- **`failed`**: `dm_jobs` rows with `status = 'failed'` — terminal 400, send attempts exhausted, or reconcile attempts > 3.
- **`queued`**: `dm_jobs` rows with `status IN ('pending', 'accepted')` — waiting to send or awaiting delivery confirmation.
- **`duplicates_blocked`**: Atomic counter. Incremented on Layer 2 `UNIQUE(rule_id, recipient_user_id)` violations and on Layer 1 redelivered `comment.created` events matching an active rule.

---

## Duplicate-Keyword Rule Decision

If `POST /rules` receives a keyword+message pair that already exists (case-insensitive keyword match AND identical `dm_message`), it returns 201 with the existing `rule_id`. This prevents accidental rule duplication from retried client calls. A different `dm_message` with the same keyword creates a separate rule — both will trigger on matching comments.

---

## Running Locally & Testing

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run test suite (18 tests)
PYTHONPATH=. pytest -v tests/

# Run application
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Run live verification against deployed target (requires real API key)
PYTHONPATH=. python scripts/verify.py --url https://YOUR-APP.onrender.com --api-key YOUR_KEY
```

---

## Deployment (Render)

`render.yaml` configures a web service with a 1GB persistent disk mounted at `/opt/render/project/src/data`. The SQLite database file survives redeploys. Set `PSEUDOGRAM_API_KEY` and `ENABLE_SIGNATURE_VERIFICATION=true` in the Render dashboard.
