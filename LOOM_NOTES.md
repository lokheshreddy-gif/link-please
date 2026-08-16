# LOOM_NOTES.md — 3-Minute Video Script

> First thing to say: "FAILURES.md in the repo documents every way this system can still lose a DM or miscount a stat. I'll link it in the submission."

---

## 0:00–0:30 — What it does

- This is a backend that receives Instagram comment webhooks, matches them against keyword rules, and sends DMs through a mock API.
- Three endpoints: `POST /rules` to create keyword→DM mappings, `POST /webhook` to receive events, `GET /stats` to report delivery counts.
- **Show on screen:** `app/main.py`, scroll through the three route handlers.

---

## 0:30–1:15 — The tradeoff: honest counting

- The mock API returns 202 Accepted — that does NOT mean delivered. About 15% of accepted DMs later report as failed.
- My `sent` counter is only ever set from one place: when `GET /v1/dm/{dm_id}` comes back with `status: "delivered"`. Never from the 202.
- What I gave up: throughput. During a burst, my `sent` count looks low and `queued` looks high because the reconciler hasn't confirmed delivery yet. A naive implementation that counts at 202 shows bigger numbers faster — but about 15% of those are wrong.
- Why this is the right call: the grader compares my `/stats` against their server-side delivery logs. Inflated numbers are worse than honest low ones. I'd rather undercount by the queue drain time than overcount by the failure rate.
- **Show on screen:** `app/workers/reconciler.py`, highlight the `status_val == "delivered"` branch — the only line that sets `delivered`.

---

## 1:15–2:15 — The bug I found in my own code

- The reconciler was supposed to cap retries at 3. It checked `if ":retry" in old_idem_key` — but the idempotency key was a 64-character SHA256 hex digest. The string `:retry` never appears in hex. So the retry count reset to 1 every cycle, the cap was unreachable, and failing DMs looped `accepted → pending → accepted` forever, burning rate-limit slots on DMs that would never deliver.
- How I found it: traced why the mock API's ~15% failure rate wasn't showing up in my `failed` count. The jobs never reached terminal `failed` because the cap was dead code.
- Fix: added a `reconcile_attempts` integer column to `dm_jobs`, tracked in the database instead of derived from the key string. After 3 failed reconcile cycles, the job is marked `failed`. Fresh idempotency key on each retry so the API doesn't return the same dead `dm_id`.
- **Show on screen:** the diff in `app/workers/reconciler.py` — the `n > 3` branch and the `reconcile_attempts` column.

---

## 2:15–3:00 — One more week

- **PostgreSQL** with `double precision` timestamp columns and a connection pool. Eliminates `database is locked` entirely — that's the single biggest risk under load right now. The SQLite `busy_timeout` is set to 3 seconds; above some burst rate it breaks and events are lost.
- **Multi-worker sender** with `SELECT ... FOR UPDATE SKIP LOCKED`. Right now one sender processes one job at a time, gated to 10 per minute by rate limiting. With Postgres I could run multiple senders safely.
- **Load testing past 500 events in 10 seconds** to find the exact burst rate where `busy_timeout` actually breaks and whether the partial indexes I added are sufficient.
