import time
import random
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
from app.workers.sender import execute_send_job, claim_next_pending_job
from app.workers.reconciler import reconcile_accepted_jobs_once
from scripts.ratecheck import audit_rate_limiter


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db(tmp_path):
    test_db = str(tmp_path / "sim_app.db")
    settings.db_path = test_db
    settings.enable_signature_verification = False
    await init_db()
    yield
    if os.path.exists(test_db):
        os.remove(test_db)


@pytest.mark.asyncio
async def test_500_events_local_simulation(monkeypatch):
    """
    Run a local end-to-end 500 event simulation testing concurrency,
    rate limiting, duplicate event filtering, and stats accuracy.
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Register rules
        await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list $50"})
        await ac.post("/rules", json={"keyword": "INFO", "dm_message": "Info guide"})

    # Generate 500 simulated events:
    events = []
    unique_users = [f"usr_{i}" for i in range(50)]

    for i in range(500):
        evt_id = f"evt_sim_{i % 450}" # 50 events will be duplicate event_id redeliveries
        user_id = random.choice(unique_users)

        if i % 20 == 0:
            events.append({
                "event_id": f"evt_del_{i}",
                "event_type": "comment.deleted",
                "data": {"comment_id": f"cmt_{i}"}
            })
        else:
            text_choice = random.choice([
                "What is the PRICE please?",
                "Can you send info?",
                "random comment without keyword",
                "PRICE and INFO both in this text!"
            ])
            events.append({
                "event_id": evt_id,
                "event_type": "comment.created",
                "data": {
                    "comment_id": f"cmt_{i}",
                    "post_id": "post_sim",
                    "text": text_choice,
                    "from": {"user_id": user_id, "username": f"user_{user_id}"}
                }
            })

    # Post all 500 events concurrently into /webhook
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        tasks = [ac.post("/webhook", json=evt) for evt in events]
        responses = await asyncio.gather(*tasks)
        for r in responses:
            assert r.status_code == 200

    # Execute ingest worker until all events are processed
    await run_ingest_worker_once()

    # Fast simulated time to bypass 60s sleep during test
    virtual_time = [time.time()]

    def mock_time():
        return virtual_time[0]

    async def mock_sleep(seconds):
        virtual_time[0] += seconds

    monkeypatch.setattr(time, "time", mock_time)
    monkeypatch.setattr(asyncio, "sleep", mock_sleep)

    # Custom HTTP transport mock for outbound calls to pseudogram
    class MockPseudogramTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if request.method == "POST" and "dm/send" in url_str:
                return httpx.Response(202, json={"dm_id": f"dm_{random.randint(1000, 9999)}", "status": "queued"})
            elif request.method == "GET" and "/v1/dm/" in url_str:
                return httpx.Response(200, json={"status": "delivered"})
            return httpx.Response(404, json={"error": "not_found"})

    mock_transport = MockPseudogramTransport()

    # Run sender worker on pending jobs
    async with httpx.AsyncClient(transport=mock_transport) as client:
        while True:
            now = time.time()
            async with get_db() as db:
                job = await claim_next_pending_job(db, now)
                if not job:
                    break
                await execute_send_job(client, db, job)

    # Advance virtual time by 5 seconds so accepted jobs pass cutoff (updated_at <= now - 2.0)
    virtual_time[0] += 5.0

    # Run reconciler worker repeatedly until no accepted jobs remain
    async with httpx.AsyncClient(transport=mock_transport) as client:
        while True:
            async with get_db() as db:
                cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs WHERE status='accepted'")
                accepted_count = (await cursor.fetchone())[0]
                if accepted_count == 0:
                    break
            await reconcile_accepted_jobs_once(client)

    # Check stats using real ASGI transport
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res = await ac.get("/stats")
        stats = res.json()

    print(f"\n[+] Final Simulation Stats: {stats}")
    assert stats["sent"] > 0
    assert stats["queued"] == 0
    assert stats["duplicates_blocked"] > 0

    # Audit rate limiter log on database
    rate_ok = audit_rate_limiter(settings.db_path, max_allowed=10)
    assert rate_ok is True
