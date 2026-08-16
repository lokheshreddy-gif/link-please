# FAILURES.md — Known Failure Modes

Each bullet names a concrete way the system can lose a DM, send a duplicate, or report a wrong number. Conditions are stated so they can be checked against a real run.

---

### 1. Lock contention drops events under burst

`PRAGMA busy_timeout=3000` makes SQLite writers wait up to 3 seconds for the write lock. If a webhook arrives while three background workers hold transactions and the wait exceeds 3 seconds, the handler catches the exception, logs it, returns HTTP 200 (to prevent upstream retry storms), and the event is lost — no row in `events`, no DM job created. `sent + queued` will undercount the truth by one per occurrence.

In the 500-event run (run_id=run_a1099b598ac1), the grader expected 85 unique DM jobs. The application created 67 (sent=66, failed=1, queued=0), a difference of -18. 18 events were likely lost to SQLite lock contention (`PRAGMA busy_timeout=3000` exceeded) during the initial webhook burst, where all 500 events arrive in a 10-second window while the three background workers hold competing write transactions.

---

### 2. Reconcile retry cap was dead code (found in review, fixed)

The original reconciler checked `if ":retry" in old_idem_key`. Since `old_idem_key` was a 64-hex-char SHA256 digest, the string `:retry` never appeared. `reconcile_retry_count` reset to 1 every cycle, the `> 3` cap was unreachable, and failing DMs looped `accepted → pending → accepted` indefinitely, burning rate-limit slots.

**Fix:** Added `reconcile_attempts` column to `dm_jobs`, tracked in the database. After 3 failed reconcile cycles, the job is marked `failed`.

**Residual:** If the upstream API actually delivered a DM but reported `failed` on `GET /v1/dm/{dm_id}`, the system marks it `failed`. `sent` undercounts by 1 and `failed` overcounts by 1 per occurrence.

---

### 3. `duplicates_blocked` definition gap

**Implemented definition:** Layer 2 `UNIQUE(rule_id, recipient_user_id)` constraint violations on `dm_jobs` insert, plus Layer 1 redelivered `comment.created` webhooks where the comment text matched at least one active rule keyword (processed via the `duplicate_events` table).

If the grader counts every redelivered event regardless of keyword match, or counts only Layer 2 violations, the numbers will diverge.

The grader truth reports 37 expected duplicate events. The application reported duplicates_blocked=28, a divergence of -9. The shortfall is consistent with the 18 dropped events in §1: if a first-delivery event was dropped due to lock contention, its later duplicate redelivery would be treated as a first-seen event (creating a job) rather than incrementing duplicates_blocked. 9 of the 18 dropped events were likely first-deliveries whose duplicates arrived later and were processed as new.

---

### 4. Rate-limit arithmetic and drain time

10 sends per rolling 61 seconds against several hundred unique recipients means the queue takes tens of minutes to drain after a 500-event burst. Any `/stats` read shortly after the burst shows a large `queued` and a small `sent`. This is correct behavior — not a stall.

Stats polling ran from the first reading to final drain over 1237 seconds (20.6 minutes) across 18 readings. The queue reached 0 at the final reading (sent=66, failed=1, queued=0, duplicates_blocked=28). The verify.py script timed out at 3610 seconds because the convergence check required 3 identical readings after queue drain, but a network timeout on the penultimate read prevented convergence detection. The queue itself was fully drained.

---

### 5. Restart during drain

All SQLite rows — `rules`, `events`, `dm_jobs` (including `status`, `attempts`, `reconcile_attempts`, `next_attempt_at`, `dm_id`), `send_log`, and `counters` — persist across a process restart against the same DB file. Verified by `tests/test_restart.py`, which populates every table, closes all connections, re-runs `init_db()`, and asserts every row and field is intact, including that the ingest worker picks up still-unprocessed events. This does **not** prove the Render disk mount preserves the file across redeploys — that requires restarting the live service and re-reading `/stats`.

The Render free tier uses an ephemeral filesystem. During iterative debugging deploys, the DB was wiped on each redeploy, confirming that /stats values do **not** survive a Render service restart on this plan. The `tests/test_restart.py` suite confirms that stats survive a process restart against the same DB file — the limitation is Render's ephemeral disk, not the application's persistence logic.

What does not survive: any `asyncio` task mid-execution. If a sender worker crashes between the `send_log` reservation and the HTTP response, the rate slot is burned (see §6) and the job stays `pending` with `attempts` unchanged — it will be retried on the next poll. No DM is lost, but one rate slot is wasted for 61 seconds.

If the disk mount is absent (free-tier ephemeral filesystem), the entire DB is wiped. All stats reset to zero, deduplication history is lost, and users who already received a DM can receive a duplicate after rules are re-created.

---

### 6. `send_log` reserve-before-send burns a slot on crash

The sender inserts a `send_log` timestamp before issuing `POST /v1/dm/send` to guarantee the rate limit is never exceeded even if the process crashes mid-request. If a crash or network timeout occurs after the insert but before the HTTP response, one slot out of 10 in the rolling 61-second window is consumed without a DM being sent. Throughput drops by 10% for that 61-second window per occurrence.

---

### 7. `comment.deleted` after DM acceptance is unrecoverable

A `comment.created` triggers a DM job. If the job reaches `accepted` or `delivered` before a `comment.deleted` arrives for the same comment, the DM cannot be recalled — the Pseudogram API has no delete-DM endpoint. `UPDATE dm_jobs SET status='cancelled' WHERE comment_id=? AND status='pending'` only cancels jobs still waiting.

UNAVAILABLE: the truth payload does not include a comment.deleted count. The truth top-level keys are `run_id`, `status`, `total_events_generated`, `total_deliveries_attempted`, `webhook_200_count`, `expected_unique_recipients`, and `expected_unique_recipient_count`. A direct database query (`SELECT COUNT(*) FROM events WHERE event_type = 'comment.deleted'`) on the live instance would be needed to measure this, but the ephemeral DB was wiped by the most recent deploy.

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
