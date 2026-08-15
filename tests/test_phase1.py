import pytest
import pytest_asyncio
import os
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.config import settings
from app.db import init_db


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db(tmp_path):
    # Set DB_PATH to isolated temporary database file for each test
    test_db = str(tmp_path / "test_app.db")
    settings.db_path = test_db
    # Initialize schema explicitly for test run
    await init_db()
    yield
    if os.path.exists(test_db):
        os.remove(test_db)


@pytest.mark.asyncio
async def test_healthz():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_create_rule():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/rules", json={"keyword": "PRICE", "dm_message": "Here is the price list!"})
    assert response.status_code == 201
    data = response.json()
    assert "rule_id" in data
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Here is the price list!"


@pytest.mark.asyncio
async def test_webhook_ingest():
    payload = {
        "event_id": "evt_test_123",
        "event_type": "comment.created",
        "data": {
            "comment_id": "c1",
            "text": "Send me the price please",
            "user_id": "u100",
            "username": "user1"
        }
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # First ingest
        res1 = await ac.post("/webhook", json=payload)
        assert res1.status_code == 200
        assert res1.json() == {"ok": True}

        # Duplicate event_id ingest (redelivery)
        res2 = await ac.post("/webhook", json=payload)
        assert res2.status_code == 200
        assert res2.json() == {"ok": True}


@pytest.mark.asyncio
async def test_stats_initial():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/stats")
    assert response.status_code == 200
    assert response.json() == {
        "sent": 0,
        "failed": 0,
        "queued": 0,
        "duplicates_blocked": 0
    }
