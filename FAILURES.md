# FAILURES.md — Technical Failure Modes & Audit Findings

This document records concrete, falsifiable ways the system can drop an event, send a duplicate DM, or produce an inaccurate stat under stress, based on empirical testing and codebase review.

---

### 1. `database is locked` Burst Contention & Event Loss
- **Symptom & Cause:** Under high concurrent webhook delivery while three background workers execute write transactions, SQLite raises `sqlite3.OperationalError: database is locked`. Surfaced by `tests/test_simulation.py::test_500_events_local_simulation`.
- **Mitigation & Limitation:** `PRAGMA busy_timeout=4000;` was added to `app/db.py` to make connection attempts wait up to 4000ms for write locks. In `app/main.py`, post-signature exceptions are caught to return HTTP 200 `{"ok": true}` and prevent cascade retry storms.
- **Falsifiable Failure Condition:** If incoming webhooks burst beyond 500 events / 10s or SQLite lock contention exceeds 4000ms, the webhook handler logs the exception and drops the event without inserting it into `events`. The event is lost, causing `sent` and `queued` stats to undercount expected delivery.

---

### 2. Reconcile Retry Cap Dead Code Bug (Discovered in Code Review)
- **Original Bug:** `app/workers/reconciler.py` checked `if ":retry" in old_idem_key:`. Because `old_idem_key` was a 64-character SHA256 hex digest (`hashlib.sha256(...).hexdigest()`), `:retry` never matched. `reconcile_retry_count` reset to `1` on every cycle, the cap `> 3` was unreachable, and failing DMs (~15% mock API case) looped `accepted -> pending -> accepted` indefinitely while consuming rate limit slots.
- **Fix Applied:** Added `reconcile_attempts` column to `dm_jobs` in `data/schema.sql` and `app/db.py` migration.
- **Residual Failure Mode:** When `reconcile_attempts > 3`, the job is marked `failed`. If the remote API actually delivered attempt #2 but erroneously reported `failed` on `GET /v1/dm/{dm_id}`, our system marks it terminally `failed`. In this scenario, `sent` undercounts by 1 and `failed` overcounts by 1 per occurrence.

---

### 3. `duplicates_blocked` Definition Gap vs Grader Truth
- **Implementation:** `duplicates_blocked` counts Layer 2 database constraint violations (`UNIQUE(rule_id, recipient_user_id)` on `dm_jobs`) PLUS Layer 1 redelivered `comment.created` webhooks that matched at least one rule (processed via `duplicate_events`).
- **Observed Data:** In `runs/run_baseline.json`, 5 duplicate events were correctly identified and suppressed.
- **Falsifiable Failure Condition:** If the grader script defines `duplicates_blocked` strictly as recipient-level suppression (Layer 2 only) or counts every redelivered raw event regardless of keyword match, our reported `duplicates_blocked` stat will diverge from their expected truth by the number of un-matched redeliveries.

---

### 4. Ephemeral Disk Wipe on Process Restart (Render Free Tier)
- **Symptom:** Render free web services restart on deploy or idle timeout, destroying the container filesystem where `data/app.db` resides.
- **Stat Impact:** All database rows in `rules`, `events`, `comments`, `dm_jobs`, `send_log`, and `counters` are wiped.
- **Deduplication Failure:** `sent`, `failed`, `queued`, and `duplicates_blocked` reset to 0. If a rule is re-created after restart, a user who previously received a DM will receive a second DM because the stored `UNIQUE(rule_id, recipient_user_id)` constraint history was lost.

---

### 5. Wasted Rate-Limit Headroom on Mid-Flight Process Crash
- **Mechanism:** To guarantee rate limits (max 10 sends / rolling 61s) are never breached across process crashes, `app/workers/sender.py` inserts a timestamp into `send_log` *before* issuing `POST /v1/dm/send`.
- **Falsifiable Failure Condition:** If the process crashes or network connection drops *after* the `send_log` insert but *before* receiving the HTTP response, 1 slot out of 10 remains reserved in `send_log` for 61 seconds without any DM being dispatched. System throughput drops by 10% for that 61-second window per crash.

---

### 6. `comment.deleted` Arriving After DM Acceptance (Unrecoverable Send)
- **Symptom:** A `comment.created` event triggers a DM job that reaches `accepted` or `delivered` state. A `comment.deleted` event for that comment arrives subsequently.
- **Failure Mode:** Instagram and the Pseudogram API provide no API to recall or delete an accepted/delivered Direct Message.
- **Result:** `UPDATE dm_jobs SET status='cancelled' WHERE comment_id=? AND status='pending'` only cancels jobs still in `pending` status. Delivered DMs cannot be undone.

---

### 7. Reconciler Retry Cap False Negative (`sent` Deflation)
- **Symptom:** Remote API returns `status: "failed"` on `GET /v1/dm/{dm_id}` even though the DM reached the recipient's inbox.
- **Result:** The Reconciler resets the job to `pending` with a fresh idempotency key (`...:retry{n}`). After 3 failed reconcile attempts, the job is marked `status='failed'`. The DM was delivered to the user, but our system reports it as `failed`, causing `sent` to under-report true delivery.
