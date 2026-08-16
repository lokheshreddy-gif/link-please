"""
Test that all database rows survive a simulated process restart — closing the DB
connection entirely and re-running init_db() against the same file.

Proves: rows persist across a process restart against the same DB file.
Does NOT prove: Render's disk mount preserves the file across redeploys.
"""
import time
import hashlib
import os
import pytest
import pytest_asyncio
from app.config import settings
from app.db import init_db, get_db
from app.workers.ingest import run_ingest_worker_once


@pytest_asyncio.fixture(autouse=True)
async def setup_restart_db(tmp_path):
    test_db = str(tmp_path / "restart_test.db")
    settings.db_path = test_db
    settings.enable_signature_verification = False
    await init_db()
    yield test_db
    if os.path.exists(test_db):
        os.remove(test_db)


@pytest.mark.asyncio
async def test_rows_survive_restart(setup_restart_db):
    """
    Populate rules, events, dm_jobs (pending + accepted), send_log, counters.
    Close all connections. Re-init against the same file. Assert every row intact.
    """
    db_path = setup_restart_db
    now = time.time()

    # ── Phase 1: populate ────────────────────────────────────────────────
    async with get_db() as db:
        # Rule
        await db.execute(
            "INSERT INTO rules (rule_id, keyword_lower, dm_message, created_at) VALUES (?, ?, ?, ?)",
            ("rule_restart_1", "restart", "restart dm", now)
        )

        # An unprocessed event (should be picked up by ingest after restart)
        await db.execute(
            """INSERT INTO events (event_id, event_type, raw_body, signature_valid, received_at)
               VALUES (?, ?, ?, 1, ?)""",
            ("evt_restart_1", "comment.created",
             '{"event_id":"evt_restart_1","event_type":"comment.created","data":{"comment_id":"cmt_r1","text":"restart keyword","from":{"user_id":"usr_r1","username":"u_r1"}}}',
             now)
        )

        # A pending job with specific attempts and next_attempt_at
        next_at = now + 120.0
        idem_key_pending = hashlib.sha256(b"rule_restart_1:usr_r_pending").hexdigest()
        await db.execute(
            """INSERT INTO dm_jobs (job_id, rule_id, recipient_user_id, comment_id, message,
               idempotency_key, status, dm_id, attempts, reconcile_attempts, next_attempt_at,
               created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, 3, 0, ?, ?, ?)""",
            ("job_pending_r", "rule_restart_1", "usr_r_pending", "cmt_rp", "msg",
             idem_key_pending, next_at, now, now)
        )

        # An accepted job with a stored dm_id and reconcile_attempts
        idem_key_accepted = hashlib.sha256(b"rule_restart_1:usr_r_accepted").hexdigest()
        await db.execute(
            """INSERT INTO dm_jobs (job_id, rule_id, recipient_user_id, comment_id, message,
               idempotency_key, status, dm_id, attempts, reconcile_attempts, next_attempt_at,
               created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?, 1, 2, ?, ?, ?)""",
            ("job_accepted_r", "rule_restart_1", "usr_r_accepted", "cmt_ra", "msg",
             idem_key_accepted, "dm_real_abc123", now, now, now)
        )

        # A send_log entry
        await db.execute("INSERT INTO send_log (sent_at) VALUES (?)", (now,))

        # Set duplicates_blocked counter to a known value
        await db.execute("UPDATE counters SET value = 7 WHERE name = 'duplicates_blocked'")

        await db.commit()

    # ── Phase 2: "restart" — re-init against the same file ───────────────
    # aiosqlite connections are closed when the context manager exits above.
    # Re-running init_db simulates what happens when the process restarts:
    # schema DDL runs with IF NOT EXISTS, migration ALTERs are swallowed.
    await init_db()

    # ── Phase 3: assert every row survived ───────────────────────────────
    async with get_db() as db:
        # Rule
        cursor = await db.execute("SELECT rule_id, keyword_lower, dm_message FROM rules WHERE rule_id = 'rule_restart_1'")
        rule = await cursor.fetchone()
        assert rule is not None, "Rule row lost after restart"
        assert rule["keyword_lower"] == "restart"
        assert rule["dm_message"] == "restart dm"

        # Pending job — check every field that matters for resumption
        cursor = await db.execute("SELECT * FROM dm_jobs WHERE job_id = 'job_pending_r'")
        pj = await cursor.fetchone()
        assert pj is not None, "Pending job lost after restart"
        assert pj["status"] == "pending"
        assert pj["attempts"] == 3
        assert pj["reconcile_attempts"] == 0
        assert abs(pj["next_attempt_at"] - next_at) < 0.01, "next_attempt_at drifted"
        assert pj["dm_id"] is None

        # Accepted job — check dm_id and reconcile_attempts
        cursor = await db.execute("SELECT * FROM dm_jobs WHERE job_id = 'job_accepted_r'")
        aj = await cursor.fetchone()
        assert aj is not None, "Accepted job lost after restart"
        assert aj["status"] == "accepted"
        assert aj["dm_id"] == "dm_real_abc123"
        assert aj["attempts"] == 1
        assert aj["reconcile_attempts"] == 2

        # send_log
        cursor = await db.execute("SELECT COUNT(*) FROM send_log")
        assert (await cursor.fetchone())[0] >= 1, "send_log rows lost after restart"

        # duplicates_blocked counter
        cursor = await db.execute("SELECT value FROM counters WHERE name = 'duplicates_blocked'")
        assert (await cursor.fetchone())[0] == 7, "duplicates_blocked counter lost after restart"

        # Unprocessed event still present and still unprocessed
        cursor = await db.execute("SELECT processed_at FROM events WHERE event_id = 'evt_restart_1'")
        evt = await cursor.fetchone()
        assert evt is not None, "Unprocessed event lost after restart"
        assert evt["processed_at"] is None, "Event was unexpectedly processed"

    # ── Phase 4: ingest worker picks up the unprocessed event ────────────
    await run_ingest_worker_once()

    async with get_db() as db:
        cursor = await db.execute("SELECT processed_at FROM events WHERE event_id = 'evt_restart_1'")
        evt = await cursor.fetchone()
        assert evt["processed_at"] is not None, "Ingest worker did not process the surviving event"

        # The event text contains "restart" which matches rule keyword "restart",
        # so a new dm_job should have been created for usr_r1
        cursor = await db.execute("SELECT status FROM dm_jobs WHERE recipient_user_id = 'usr_r1'")
        new_job = await cursor.fetchone()
        assert new_job is not None, "Ingest worker did not create a job from the surviving event"
        assert new_job["status"] == "pending"
