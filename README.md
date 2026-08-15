# LinkPlease Comment→DM Automation Backend

A backend service built with Python 3.11, FastAPI, `aiosqlite` (WAL mode, plain SQL schema), `httpx`, and background `asyncio` workers designed for Instagram comment-to-DM automation.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PSEUDOGRAM_API_KEY` | `""` | Auth API key sent via `X-API-Key` header |
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | Base URL of external Pseudogram API |
| `DB_PATH` | `data/app.db` | Single-file SQLite database path |
| `ENABLE_SIGNATURE_VERIFICATION` | `false` | Set `true` to enforce HMAC-SHA256 signature validation on `POST /webhook` |

---

## Architecture Summary (~15 lines)

1. **Ingest Layer:** `POST /webhook` reads raw request bytes, validates HMAC-SHA256 signature (if enabled), executes `INSERT OR IGNORE` on `events.event_id` (Layer 1 dedupe), and returns HTTP 200 in <5s with zero outbound network calls.
2. **Database Engine:** Single-file SQLite (`data/app.db`) initialized with `PRAGMA journal_mode=WAL;` and plain SQL tables (`rules`, `events`, `comments`, `dm_jobs`, `send_log`, `counters`).
3. **Ingest Worker:** Polls unprocessed events every 100ms. Upserts comment metadata, checks out-of-order `comment.deleted` tombstones, performs case-insensitive keyword substring matching, and inserts jobs into `dm_jobs`.
4. **User Deduplication:** `dm_jobs` enforces `UNIQUE(rule_id, recipient_user_id)` (Layer 2 dedupe). DB constraint violations increment `duplicates_blocked` counter.
5. **Sender Worker:** Polls pending jobs. Enforces client-side rolling 60s rate limiter (max 9 sends/60s) via `send_log`. Reserves rate slot *before* HTTP dispatch for crash safety. Sends `POST /v1/dm/send` with `Idempotency-Key`.
6. **Reconciler Worker:** Polls `accepted` jobs older than 2s via `GET /v1/dm/{dm_id}`. Confirms `delivered` state or resets `failed` DMs with fresh idempotency keys (`...:retry{n}`).

---

## Exact Stats Definitions (`GET /stats`)

Metrics are evaluated live via `SELECT COUNT(*)` queries over SQLite on every request:

- **`sent`**: Count of jobs in `delivered` status (confirmed via `GET /v1/dm/{dm_id}`).
- **`failed`**: Count of jobs in `failed` status (terminal `400`, max retries exceeded, or max reconcile retries reached).
- **`queued`**: Count of non-terminal jobs currently in `pending` or `accepted` status waiting to send or reconcile.
- **`duplicates_blocked`**: Atomic counter value incremented whenever `UNIQUE(rule_id, recipient_user_id)` rejects a duplicate job insert.

---

## Running Locally & Testing

```bash
# Setup virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run test suite (11 tests passing)
PYTHONPATH=. pytest -v tests/

# Run application
uvicorn app.main:app --host 0.0.0.0 --port 8000
```
