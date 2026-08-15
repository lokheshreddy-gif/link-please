import time
import json
import uuid
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.db import get_db, init_db
from app.auth import verify_signature
from app.workers.ingest import ingest_worker_loop
from app.workers.sender import sender_worker_loop
from app.workers.reconciler import reconciler_worker_loop

# Structured JSON logging format setup
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'
)
logger = logging.getLogger("linkplease")

# Cache last-known stats so /stats never 500s even if the DB is locked
_last_known_stats = {"sent": 0, "failed": 0, "queued": 0, "duplicates_blocked": 0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI Lifespan context manager.
    Initializes database schema and starts background worker tasks.
    Gracefully cancels tasks on application shutdown.
    """
    logger.info("Initializing database schema")
    await init_db()

    logger.info("Starting background workers: Ingest, Sender, Reconciler")
    ingest_task = asyncio.create_task(ingest_worker_loop())
    sender_task = asyncio.create_task(sender_worker_loop())
    reconciler_task = asyncio.create_task(reconciler_worker_loop())

    yield

    logger.info("Cancelling background worker tasks")
    ingest_task.cancel()
    sender_task.cancel()
    reconciler_task.cancel()
    await asyncio.gather(ingest_task, sender_task, reconciler_task, return_exceptions=True)


app = FastAPI(title="LinkPlease Comment->DM Automation", lifespan=lifespan)


class RuleCreateRequest(BaseModel):
    keyword: str
    dm_message: str


@app.get("/healthz")
async def healthz():
    """Liveness probe returning 200 OK."""
    return {"status": "ok"}


@app.post("/rules", status_code=status.HTTP_201_CREATED)
@app.post("/rules/", status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def create_rule(req: RuleCreateRequest):
    """
    Register a new keyword-to-DM automation rule.
    Keyword is stored in lowercase for case-insensitive substring matching.
    If an existing rule has identical keyword_lower AND dm_message, return 201 with existing rule_id.
    """
    now = time.time()
    keyword_lower = req.keyword.lower()

    async with get_db() as db:
        cursor = await db.execute(
            "SELECT rule_id FROM rules WHERE keyword_lower = ? AND dm_message = ?",
            (keyword_lower, req.dm_message)
        )
        existing = await cursor.fetchone()
        if existing:
            return {
                "rule_id": existing["rule_id"],
                "keyword": req.keyword,
                "dm_message": req.dm_message
            }

        rule_id = f"rule_{uuid.uuid4().hex[:8]}"
        await db.execute(
            """
            INSERT INTO rules (rule_id, keyword_lower, dm_message, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (rule_id, keyword_lower, req.dm_message, now)
        )
        await db.commit()

    return {
        "rule_id": rule_id,
        "keyword": req.keyword,
        "dm_message": req.dm_message
    }


@app.post("/webhook")
@app.post("/webhook/", include_in_schema=False)
async def webhook(request: Request):
    """
    Ingest webhook events. Must return 200 in under 5 seconds always.
    Performs zero network I/O in this request handler thread.
    Reads raw bytes -> verifies HMAC signature -> stores event row -> returns {"ok": true}.
    Accepts any content type; non-JSON bodies are stored but produce no DM jobs.
    """
    raw_body_bytes = await request.body()
    signature_header = request.headers.get("X-PseudoGram-Signature")
    sig_valid = verify_signature(raw_body_bytes, signature_header)

    now = time.time()
    raw_body_str = raw_body_bytes.decode("utf-8", errors="replace")

    # If signature is invalid, record event in rejected_events table and return 401
    if not sig_valid:
        logger.warning("Rejected webhook due to invalid signature")
        event_id = None
        try:
            parsed = json.loads(raw_body_str)
            if isinstance(parsed, dict) and "event_id" in parsed:
                event_id = parsed["event_id"]
        except Exception:
            pass

        async with get_db() as db:
            await db.execute(
                """
                INSERT INTO rejected_events (event_id, raw_body, received_at)
                VALUES (?, ?, ?)
                """,
                (event_id, raw_body_str, now)
            )
            await db.commit()
        return JSONResponse(status_code=401, content={"error": "invalid_signature"})

    try:
        # Parse payload structure safely; never crash or fail on malformed JSON
        event_id = None
        event_type = "unknown"
        try:
            payload = json.loads(raw_body_str)
            if isinstance(payload, dict):
                event_id = payload.get("event_id")
                event_type = payload.get("event_type", "unknown")
        except Exception as exc:
            logger.error(f"Failed to parse webhook JSON body: {exc}")

        if not event_id:
            event_id = f"evt_fallback_{uuid.uuid4().hex[:8]}"

        # Event ID PK acts as Layer 1 deduplication guard (INSERT OR IGNORE)
        async with get_db() as db:
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO events (event_id, event_type, raw_body, signature_valid, received_at)
                VALUES (?, ?, ?, 1, ?)
                """,
                (event_id, event_type, raw_body_str, now)
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO duplicate_events (event_id, raw_body, received_at)
                    VALUES (?, ?, ?)
                    """,
                    (event_id, raw_body_str, now)
                )
            await db.commit()
    except Exception as exc:
        logger.error(f"Unexpected error in webhook processing: {exc}")

    return {"ok": True}


@app.get("/stats")
@app.get("/stats/", include_in_schema=False)
async def get_stats():
    """
    Return current live statistics computed via SELECT COUNT(*) queries.
    Never relies on in-memory counters which drift after restarts.
    Returns exactly four integer keys. Never 500s — falls back to last-known values on DB error.
    """
    global _last_known_stats
    try:
        async with get_db() as db:
            # sent: delivered state confirmed by GET /v1/dm/{id}
            cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs WHERE status = 'delivered'")
            sent_count = (await cursor.fetchone())[0]

            # failed: terminal failure states
            cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs WHERE status = 'failed'")
            failed_count = (await cursor.fetchone())[0]

            # queued: pending or accepted (unconfirmed) states
            cursor = await db.execute("SELECT COUNT(*) FROM dm_jobs WHERE status IN ('pending', 'accepted')")
            queued_count = (await cursor.fetchone())[0]

            # duplicates_blocked: counter incremented on UNIQUE(rule_id, recipient_user_id) constraint rejection
            cursor = await db.execute("SELECT value FROM counters WHERE name = 'duplicates_blocked'")
            row = await cursor.fetchone()
            duplicates_blocked_count = row[0] if row else 0

        result = {
            "sent": int(sent_count or 0),
            "failed": int(failed_count or 0),
            "queued": int(queued_count or 0),
            "duplicates_blocked": int(duplicates_blocked_count or 0)
        }
        _last_known_stats = result
        return result
    except Exception as exc:
        logger.error(f"Error reading stats from DB, returning last-known values: {exc}")
        return _last_known_stats
