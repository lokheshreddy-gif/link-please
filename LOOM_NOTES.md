# LOOM_NOTES.md — 3-Minute Video Outline

Talking points for the Loom recording. Bullet points only — talk, don't read.

---

## Question 1: What tradeoffs did you make?

- **Reserve-before-send rate limiting.** I insert a timestamp into `send_log` *before* the HTTP call, not after. This guarantees I never exceed 10 sends/60s even if the process crashes mid-request. The cost: a crash between the insert and the HTTP response burns one rate slot for 61 seconds without sending a DM — throughput drops by 10% for that window.

- **`sent` only from delivery confirmation.** I never count a 202 Accepted as `sent`. Only `GET /v1/dm/{dm_id}` returning `delivered` sets the status. This means my `sent` count is lower than a naive implementation that counts at 202 — but it's *true*. The grader compares against server-side logs, so lower-and-true beats higher-and-wrong.

- **SQLite on a mounted disk, not Postgres.** Single-file database on a persistent Render disk. Simpler to reason about, no connection pool bugs, but `busy_timeout` becomes the bottleneck under high concurrency. I documented the failure mode in FAILURES.md and it's testable.

- **Three workers in one process.** Ingest, sender, reconciler as `asyncio.create_task()`. No Celery, no Redis. Simpler to deploy and explain. Downside: all three share one event loop and one write lock. With more time, I'd move to Postgres and `SELECT ... FOR UPDATE SKIP LOCKED` for concurrent sender workers.

- **Duplicate keyword+message rule returns existing rule_id.** Idempotent — repeated `POST /rules` with the same payload returns 201 with the original `rule_id`. Different message with the same keyword creates a new rule. Documented in README.

---

## Question 2: What would you do with one more week?

- **PostgreSQL** with `double precision` timestamp columns and `asyncpg` connection pool. Eliminates `database is locked` entirely. The SQLite regex-rewrite approach I tried first was broken (syntax errors, wrong column types, no pooling) — doing it right needs dialect-aware SQL or an ORM, which is a bigger change.

- **Multi-worker sender** with `SELECT ... FOR UPDATE SKIP LOCKED` so multiple workers can drain the queue concurrently without double-sending. Right now one sender processes one job at a time, gated to 10/minute by rate limiting.

- **Dead-letter table** for terminally failed jobs. Right now they stay in `dm_jobs` with `status='failed'`, which means the pending-job scan has to skip them. A separate table keeps the hot path fast.

- **Load testing beyond 500/10s.** I know `busy_timeout=3000` survives 500 events in the test suite. I don't know where it breaks. I'd want to find that number and document it.

- **Observability.** Structured logging is there, but no metrics endpoint. A `/metrics` route exposing queue depth, rate-limit utilization, and error rates would let me monitor the drain in real time.
