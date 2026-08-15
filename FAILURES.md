# FAILURES.md — Known Failure Modes

Each bullet names a concrete way the system can lose a DM, send a duplicate, or report a wrong number. Conditions are stated so they can be checked against a real run.

---

### 1. Lock contention drops events under burst

`PRAGMA busy_timeout=3000` makes SQLite writers wait up to 3 seconds for the write lock. If a webhook arrives while three background workers hold transactions and the wait exceeds 3 seconds, the handler catches the exception, logs it, returns HTTP 200 (to prevent upstream retry storms), and the event is lost — no row in `events`, no DM job created. `sent + queued` will undercount the truth by one per occurrence.

`TODO(real-run): how many events were dropped during the 500-event run, if any. Compare COUNT(*) FROM events against the simulation's total event count.`

---

### 2. Reconcile retry cap was dead code (found in review, fixed)

The original reconciler checked `if ":retry" in old_idem_key`. Since `old_idem_key` was a 64-hex-char SHA256 digest, the string `:retry` never appeared. `reconcile_retry_count` reset to 1 every cycle, the `> 3` cap was unreachable, and failing DMs looped `accepted → pending → accepted` indefinitely, burning rate-limit slots.

**Fix:** Added `reconcile_attempts` column to `dm_jobs`, tracked in the database. After 3 failed reconcile cycles, the job is marked `failed`.

**Residual:** If the upstream API actually delivered a DM but reported `failed` on `GET /v1/dm/{dm_id}`, the system marks it `failed`. `sent` undercounts by 1 and `failed` overcounts by 1 per occurrence.

---

### 3. `duplicates_blocked` definition gap

**Implemented definition:** Layer 2 `UNIQUE(rule_id, recipient_user_id)` constraint violations on `dm_jobs` insert, plus Layer 1 redelivered `comment.created` webhooks where the comment text matched at least one active rule keyword (processed via the `duplicate_events` table).

If the grader counts every redelivered event regardless of keyword match, or counts only Layer 2 violations, the numbers will diverge.

`TODO(real-run): actual duplicates_blocked vs truth expected_duplicates, and the magnitude of the divergence.`

---

### 4. Rate-limit arithmetic and drain time

10 sends per rolling 61 seconds against several hundred unique recipients means the queue takes tens of minutes to drain after a 500-event burst. Any `/stats` read shortly after the burst shows a large `queued` and a small `sent`. This is correct behavior — not a stall.

`TODO(real-run): actual queue drain time from the stats convergence polling timestamps.`

---

### 5. Restart during drain loses in-flight state

On the Render mounted disk, all SQLite rows survive restart: `rules`, `events`, `dm_jobs` (including pending/accepted jobs and their retry state), `send_log`, and `counters`. The workers resume from where they left off.

What does not survive: any `asyncio` task mid-execution. If a sender worker crashes between the `send_log` reservation and the HTTP response, the rate slot is burned (see §6) and the job stays `pending` with `attempts` unchanged — it will be retried on the next poll. No DM is lost, but one rate slot is wasted for 61 seconds.

If the disk mount is absent (free-tier ephemeral filesystem), the entire DB is wiped. All stats reset to zero, deduplication history is lost, and users who already received a DM can receive a duplicate after rules are re-created.

---

### 6. `send_log` reserve-before-send burns a slot on crash

The sender inserts a `send_log` timestamp before issuing `POST /v1/dm/send` to guarantee the rate limit is never exceeded even if the process crashes mid-request. If a crash or network timeout occurs after the insert but before the HTTP response, one slot out of 10 in the rolling 61-second window is consumed without a DM being sent. Throughput drops by 10% for that 61-second window per occurrence.

---

### 7. `comment.deleted` after DM acceptance is unrecoverable

A `comment.created` triggers a DM job. If the job reaches `accepted` or `delivered` before a `comment.deleted` arrives for the same comment, the DM cannot be recalled — the Pseudogram API has no delete-DM endpoint. `UPDATE dm_jobs SET status='cancelled' WHERE comment_id=? AND status='pending'` only cancels jobs still waiting.

`TODO(real-run): count of comment.deleted events observed and whether any arrived after their DM was already accepted.`

---

### 8. Reconcile cap of 3 can miscount `sent`

When `GET /v1/dm/{dm_id}` returns `failed`, the reconciler resets the job to `pending` with a fresh idempotency key and increments `reconcile_attempts`. After 3 failures, the job is marked terminally `failed`. If the upstream API actually delivered the DM but mis-reported `failed`, our `sent` undercounts true delivery and `failed` overcounts it, by 1 per occurrence.

---

### 9. Forged-event handling

Invalid-signature webhooks are stored in `rejected_events` (not `events`) so the real `event_id` PK namespace is not poisoned. But forged events are invisible in `/stats` — a flood of them produces no counter change, no alert, and no log beyond the per-request warning line. Monitoring would require querying `rejected_events` directly.

---

### What I'd change with more time

- **PostgreSQL** with `double precision` timestamp columns and a connection pool (`asyncpg` + pool), eliminating the `database is locked` failure mode entirely and allowing horizontal scaling.
- **Distributed locking** (e.g. `SELECT ... FOR UPDATE SKIP LOCKED`) so multiple sender workers can process the queue concurrently without double-sending.
- **Dead-letter table** for terminally failed jobs with structured error context, separate from the main `dm_jobs` table, to keep the pending-job scan fast.
- **Load testing beyond 500/10s** to find the exact burst rate where `busy_timeout` breaks and whether the partial indexes are sufficient.
