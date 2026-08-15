import time
import hmac
import hashlib
import asyncio
import pytest
import pytest_asyncio
import os
import httpx
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.config import settings
from app.db import init_db, get_db
from app.workers.ingest import run_ingest_worker_once
from app.workers.reconciler import reconcile_accepted_jobs_once


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db(tmp_path):
    test_db = str(tmp_path / "test_app.db")
    settings.db_path = test_db
    settings.enable_signature_verification = False
    await init_db()
    yield
    if os.path.exists(test_db):
        os.remove(test_db)


@pytest.mark.asyncio
async def test_rules_validation_and_creation():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res1 = await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Price is $100"})
        assert res1.status_code == 201
        data1 = res1.json()
        assert data1["keyword"] == "PRICE"
        assert "rule_id" in data1

        res2 = await ac.post("/rules", json={"keyword": "INFO", "dm_message": "Here is info"})
        assert res2.status_code == 201

    async with get_db() as db:
        cursor = await db.execute("SELECT COUNT(*) FROM rules")
        count = (await cursor.fetchone())[0]
        assert count == 2


@pytest.mark.asyncio
async def test_webhook_ingest_and_keyword_matching():
    # 1. Create rule
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Price details!"})

    # 2. Webhook payload with case-insensitive matching keyword
    payload = {
        "event_id": "evt_001",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_100",
            "post_id": "post_1",
            "text": "What is the price please?",
            "from": {"user_id": "usr_999", "username": "testuser"}
        }
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res = await ac.post("/webhook", json=payload)
        assert res.status_code == 200

    # Execute ingest worker manually
    await run_ingest_worker_once()

    async with get_db() as db:
        cursor = await db.execute("SELECT status, recipient_user_id, message FROM dm_jobs")
        jobs = await cursor.fetchall()
        assert len(jobs) == 1
        assert jobs[0]["recipient_user_id"] == "usr_999"
        assert jobs[0]["status"] == "pending"
        assert jobs[0]["message"] == "Price details!"


@pytest.mark.asyncio
async def test_duplicate_user_and_rule_deduplication():
    # Same user commenting multiple times on same keyword
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list"})

    payload1 = {
        "event_id": "evt_101",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_201",
            "text": "Can I get price?",
            "from": {"user_id": "usr_repeat", "username": "repeat_user"}
        }
    }

    payload2 = {
        "event_id": "evt_102",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_202",
            "text": "Tell me price again!",
            "from": {"user_id": "usr_repeat", "username": "repeat_user_changed_name"}
        }
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/webhook", json=payload1)
        await ac.post("/webhook", json=payload2)

    await run_ingest_worker_once()

    async with get_db() as db:
        # Exactly ONE DM job created for (rule, user)
        cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs")
        job_count = (await cursor.fetchone())[0]
        assert job_count == 1

        # duplicates_blocked counter incremented
        cursor = await db.execute("SELECT value FROM counters WHERE name = 'duplicates_blocked'")
        blocked_count = (await cursor.fetchone())[0]
        assert blocked_count == 1

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res = await ac.get("/stats")
        stats = res.json()
        assert stats["queued"] == 1
        assert stats["duplicates_blocked"] == 1


@pytest.mark.asyncio
async def test_out_of_order_comment_deleted_tombstone():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list"})

    # 1. comment.deleted arrives FIRST
    deleted_payload = {
        "event_id": "evt_del_01",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_ooo_1"}
    }

    # 2. comment.created arrives SECOND
    created_payload = {
        "event_id": "evt_crt_01",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_ooo_1",
            "text": "PRICE check",
            "from": {"user_id": "usr_ooo", "username": "ooo_user"}
        }
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/webhook", json=deleted_payload)
        await ac.post("/webhook", json=created_payload)

    await run_ingest_worker_once()

    async with get_db() as db:
        # Check tombstone in comments
        cursor = await db.execute("SELECT deleted FROM comments WHERE comment_id='cmt_ooo_1'")
        row = await cursor.fetchone()
        assert row is not None
        assert row["deleted"] == 1

        # No DM jobs created because created event respected tombstone
        cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs")
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_signature_verification():
    settings.enable_signature_verification = True
    settings.pseudogram_api_key = "secret_key_999"

    raw_body = b'{"event_id":"evt_sig_1","event_type":"comment.created","data":{}}'
    valid_sig = "sha256=" + hmac.new(b"secret_key_999", msg=raw_body, digestmod=hashlib.sha256).hexdigest()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Valid signature -> 200
        res_valid = await ac.post("/webhook", content=raw_body, headers={"X-PseudoGram-Signature": valid_sig, "Content-Type": "application/json"})
        assert res_valid.status_code == 200

        # Invalid signature -> 401
        res_invalid = await ac.post("/webhook", content=raw_body, headers={"X-PseudoGram-Signature": "sha256=bad_hex", "Content-Type": "application/json"})
        assert res_invalid.status_code == 401


@pytest.mark.asyncio
async def test_reconciler_delivered_and_failed_retry_with_fresh_key(monkeypatch):
    # Setup job in 'accepted' state
    now = time.time()
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO dm_jobs (
                job_id, rule_id, recipient_user_id, comment_id, message,
                idempotency_key, status, dm_id, next_attempt_at, created_at, updated_at
            ) VALUES ('j1', 'r1', 'u1', 'c1', 'msg', 'key_orig', 'accepted', 'dm_test_1', ?, ?, ?)
            """,
            (now, now, now - 5.0)  # updated_at 5s ago so reconciler picks it up
        )
        await db.commit()

    # Mock external API returning status: delivered
    async def mock_get(self, url, headers=None, timeout=None):
        if "dm_test_1" in url:
            return httpx.Response(200, json={"dm_id": "dm_test_1", "status": "delivered"})
        return httpx.Response(404)

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    async with httpx.AsyncClient() as client:
        await reconcile_accepted_jobs_once(client)

    async with get_db() as db:
        cursor = await db.execute("SELECT status FROM dm_jobs WHERE job_id='j1'")
        assert (await cursor.fetchone())[0] == "delivered"

    # Now test ~15% flipped failure case
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO dm_jobs (
                job_id, rule_id, recipient_user_id, comment_id, message,
                idempotency_key, status, dm_id, next_attempt_at, created_at, updated_at
            ) VALUES ('j2', 'r1', 'u2', 'c2', 'msg', 'idem_key_base', 'accepted', 'dm_test_fail', ?, ?, ?)
            """,
            (now, now, now - 5.0)
        )
        await db.commit()

    async def mock_get_failed(self, url, headers=None, timeout=None):
        if "dm_test_fail" in url:
            return httpx.Response(200, json={"dm_id": "dm_test_fail", "status": "failed"})
        return httpx.Response(404)

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get_failed)

    async with httpx.AsyncClient() as client:
        await reconcile_accepted_jobs_once(client)

    async with get_db() as db:
        cursor = await db.execute("SELECT status, idempotency_key FROM dm_jobs WHERE job_id='j2'")
        row = await cursor.fetchone()
        assert row["status"] == "pending"
        # Idempotency key regenerated with fresh retry suffix
        assert ":retry1" in row["idempotency_key"] or len(row["idempotency_key"]) == 64
