# LinkPlease Comment→DM Automation Backend

A production-grade backend service built with Python 3.11, FastAPI, `aiosqlite` (WAL mode, plain SQL schema), `httpx`, and background `asyncio` workers designed for Instagram comment-to-DM automation.

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

1. **Ingest Layer (`POST /webhook`):** Reads raw bytes, validates HMAC-SHA256 signature (invalid signatures stored in `rejected_events` returning 401). Valid events execute `INSERT OR IGNORE` into `events.event_id` (Layer 1 dedupe). Redelivered events are stored in `duplicate_events`. Returns HTTP 200 in <5s with zero network I/O.
2. **Database Engine:** Single-file SQLite (`data/app.db`) with `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=4000;`, plain SQL tables, and automatic idempotent schema migrations (`reconcile_attempts`).
3. **Rule Registration (`POST /rules`):** Keyword stored in lowercase. If an existing rule has identical `keyword_lower` AND `dm_message`, returns HTTP 201 with the existing `rule_id` to prevent duplicate rule pollution. Different message with same keyword creates a new rule.
4. **Ingest Worker:** Polls unprocessed events every 100ms. Upserts comment metadata, handles out-of-order `comment.deleted` tombstones, matches case-insensitive keywords, and inserts jobs into `dm_jobs`. Also processes `duplicate_events` rows (incrementing `duplicates_blocked` if the redelivered comment matched a rule).
5. **User Deduplication:** `dm_jobs` enforces `UNIQUE(rule_id, recipient_user_id)` (Layer 2 dedupe). DB constraint violations increment `duplicates_blocked` counter.
6. **Sender Worker:** Polls pending jobs (`next_attempt_at <= now`). Enforces rolling 61s rate limiter (max 10 sends/61s) via `send_log` with pre-call slot reservation for crash safety and automatic log pruning (>300s). Dispatches `POST /v1/dm/send` with `Idempotency-Key`.
7. **Reconciler Worker:** Polls `accepted` jobs older than 2s via `GET /v1/dm/{dm_id}`. Confirms `delivered` state. If API returns `failed` (~15% flip case), increments `reconcile_attempts` column and resets status to `pending` with a fresh idempotency key (`hashlib.sha256(f"{rule_id}:{user_id}:retry{n}".encode()).hexdigest()`). Caps reconcile retries at 3 before marking terminal `failed`.

---

## Exact Stats Definitions (`GET /stats`)

Metrics are evaluated live via `SELECT COUNT(*)` queries over SQLite on every request:

- **`sent`**: Count of jobs in `delivered` status (confirmed via `GET /v1/dm/{dm_id}`).
- **`failed`**: Count of jobs in `failed` status (terminal `400`, max send attempts >= 6, missing `dm_id`, or `reconcile_attempts > 3` exhausted).
- **`queued`**: Count of non-terminal jobs currently in `pending` or `accepted` status waiting to send or reconcile.
- **`duplicates_blocked`**: Atomic counter value incremented on Layer 2 DB constraint rejections (`UNIQUE(rule_id, recipient_user_id)`) plus Layer 1 redelivered `comment.created` events that matched at least one active rule.

---

## Running Locally & Testing

```bash
# Setup virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run test suite (14 tests passing)
PYTHONPATH=. pytest -v tests/

# Run application
uvicorn app.main:app --host 0.0.0.0 --port 8000
```
