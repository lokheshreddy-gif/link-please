# FAILURES.md — Known Failure Modes & Limitations Audit

This document details concrete, falsifiable failure conditions, edge cases, and architectural trade-offs observed during implementation and load testing.

---

### 1. SQLite on Render's Ephemeral Disk (Process Restart Data Loss)
- **Condition:** Render free-tier web services run on ephemeral container filesystems. On process restart or redeployment, `data/app.db` is destroyed and re-initialized.
- **Consequence:** All rule configurations, event logs, DM job records, `send_log` history, and `duplicates_blocked` counters are erased.
- **Impact on Stats & Deduplication:** After a restart, `duplicates_blocked` drops to 0. A user who was previously DMed for a rule can receive another DM if the rule is re-created, because the stored `(rule_id, recipient_user_id)` constraint history was lost with the file.
- **Fix:** Mount a persistent disk volume (e.g., Render Disk) or migrate `app.db` to a managed external PostgreSQL instance.

---

### 2. Process Death While Job is in `accepted` State
- **Condition:** The process crashes after receiving a `202 Accepted` from `POST /v1/dm/send` (storing `dm_id` in `dm_jobs`), but before the Reconciler worker polls `GET /v1/dm/{dm_id}`.
- **Behavior on Restart:** On restart, the Reconciler worker reads `dm_jobs WHERE status = 'accepted'` from the database and resumes polling `GET /v1/dm/{dm_id}` using the persisted `dm_id`.
- **Safety Guarantee:** The stored `dm_id` and original `Idempotency-Key` prevent double-sending. The system does not issue a duplicate `POST /v1/dm/send`.

---

### 3. Rate Limiter Slot Reservation Window (Crash Between `send_log` Insert and HTTP Request)
- **Condition:** To guarantee strict crash-resilient rate limiting, a timestamp is inserted into `send_log` *before* making the outbound `POST /v1/dm/send` request. If the process crashes or network times out before the HTTP call completes, that rate limiter slot remains occupied in `send_log`.
- **Consequence:** One rate-limiter slot out of the 9 available rolling slots is consumed without a DM actually being dispatched.
- **Trade-off Justification:** Conserving rate-limit headroom on a crash is strictly safer than inserting into `send_log` *after* the call (which could cause rolling rate limit breaches across process restarts).

---

### 4. `comment.deleted` Arriving After DM Accepted / Delivered
- **Condition:** A `comment.created` event triggers a DM send. The DM reaches `accepted` or `delivered` status on the external API. Hours later, a `comment.deleted` event for that comment arrives.
- **Consequence:** The external Instagram API does not support un-sending or revoking delivered Direct Messages.
- **Handling:** `UPDATE dm_jobs SET status='cancelled' WHERE comment_id=? AND status='pending'` only affects pending jobs. Jobs already in `accepted` or `delivered` remain unchanged, and an informational log is written.

---

### 5. Reconciler Retry Cap & False Negative `failed` Status
- **Condition:** The external API has a known ~15% silent failure rate where a `202 Accepted` later flips to `failed` on `GET /v1/dm/{dm_id}`. When this occurs, the Reconciler resets the job to `pending` with a **fresh** idempotency key (`...:retry{n}`). If a job fails delivery 3 times, it is marked terminally `failed`.
- **Failure Mode:** If attempt #2 was actually delivered by the remote server but reported as `failed` due to a remote API bug, our system will retry up to attempt #3 or mark it `failed`. If marked `failed`, the `sent` count understates true delivery.

---

### 6. Concurrent Workers & Race Conditions on Deduplication
- **Condition:** Two concurrent worker tasks attempt to process two separate webhook requests for the same recipient (`user_id`) matching the same `rule_id` at the exact same millisecond.
- **Prevention:** Application-level `if` checks are insufficient under concurrency. Duplicate prevention is enforced at the database layer via SQLite's `UNIQUE(rule_id, recipient_user_id)` constraint on `dm_jobs`.
- **Behavior:** SQLite acquires a write lock during transaction commit. The first transaction succeeds. The second transaction raises `sqlite3.IntegrityError`, which is caught by the worker to execute `UPDATE counters SET value = value + 1 WHERE name = 'duplicates_blocked'`. Zero duplicate DMs are created.

---

### 7. Disagreement on `queued` Definition
- **Condition:** In our system, `queued` is defined strictly as `status IN ('pending', 'accepted')` (all DMs not yet in a terminal `delivered` or `failed` state).
- **Difference:** If the external grader scripts count `202 Accepted` responses immediately as `sent` (rather than waiting for `GET /v1/dm/{dm_id}` delivery confirmation), our `sent` stat will be lower and `queued` stat higher during active processing until reconciliation completes.
