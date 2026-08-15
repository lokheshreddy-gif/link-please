import time
import json
import hashlib
import sqlite3
import logging
import asyncio
from app.db import get_db

logger = logging.getLogger("linkplease.ingest")


async def process_single_event(db, event):
    """
    Process a single webhook event row.
    Handles comment.created, comment.deleted, keyword matching, and dual-layer deduplication.
    """
    event_id = event["event_id"]
    event_type = event["event_type"]
    raw_body = event["raw_body"]
    signature_valid = event["signature_valid"]
    now = time.time()

    # If signature was invalid when ingested, skip processing logic
    if not signature_valid:
        await db.execute("UPDATE events SET processed_at=? WHERE event_id=?", (now, event_id))
        return

    try:
        payload = json.loads(raw_body)
    except Exception as exc:
        logger.error(f"Event {event_id} has invalid JSON payload: {exc}")
        await db.execute("UPDATE events SET processed_at=? WHERE event_id=?", (now, event_id))
        return

    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    if event_type == "comment.created":
        comment_id = data.get("comment_id")
        post_id = data.get("post_id")
        text = data.get("text", "")
        from_user = data.get("from", {}) if isinstance(data.get("from"), dict) else {}
        user_id = from_user.get("user_id")
        username = from_user.get("username")
        created_at_str = data.get("created_at")

        if not comment_id or not user_id:
            logger.warning(f"Event {event_id} missing comment_id or user_id")
            await db.execute("UPDATE events SET processed_at=? WHERE event_id=?", (now, event_id))
            return

        # Check for out-of-order tombstone (comment.deleted arrived before comment.created)
        cursor = await db.execute("SELECT deleted FROM comments WHERE comment_id=?", (comment_id,))
        existing_comment = await cursor.fetchone()

        if existing_comment and existing_comment["deleted"] == 1:
            logger.info(f"Comment {comment_id} was already marked deleted (out-of-order tombstone). Skipping job creation.")
            await db.execute("UPDATE events SET processed_at=? WHERE event_id=?", (now, event_id))
            return

        # Insert/upsert comment details
        await db.execute(
            """
            INSERT INTO comments (comment_id, post_id, text, user_id, username, created_at, deleted)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(comment_id) DO UPDATE SET
                post_id=excluded.post_id,
                text=excluded.text,
                user_id=excluded.user_id,
                username=excluded.username
            """,
            (comment_id, post_id, text, user_id, username, now)
        )

        # Match comment text against all active rules (case-insensitive substring match)
        cursor = await db.execute("SELECT rule_id, keyword_lower, dm_message FROM rules")
        rules = await cursor.fetchall()

        text_lower = text.lower()
        for rule in rules:
            rule_id = rule["rule_id"]
            keyword_lower = rule["keyword_lower"]
            dm_message = rule["dm_message"]

            if keyword_lower in text_lower:
                job_id = f"job_{hashlib.sha256(f'{rule_id}:{comment_id}'.encode()).hexdigest()[:12]}"
                idempotency_key = hashlib.sha256(f"{rule_id}:{user_id}".encode()).hexdigest()

                try:
                    await db.execute(
                        """
                        INSERT INTO dm_jobs (
                            job_id, rule_id, recipient_user_id, comment_id, message,
                            idempotency_key, status, attempts, next_attempt_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                        """,
                        (job_id, rule_id, user_id, comment_id, dm_message, idempotency_key, now, now, now)
                    )
                    logger.info(f"Created DM job {job_id} for user {user_id} on rule {rule_id}")
                except sqlite3.IntegrityError:
                    # Layer 2 deduplication guard caught a duplicate user+rule or idempotency_key attempt.
                    # Increment duplicates_blocked atomic counter.
                    logger.info(f"Duplicate DM blocked for user {user_id} on rule {rule_id}")
                    await db.execute("UPDATE counters SET value = value + 1 WHERE name = 'duplicates_blocked'")

    elif event_type == "comment.deleted":
        comment_id = data.get("comment_id")
        if comment_id:
            # Mark comment deleted (creates tombstone if comment.created has not arrived yet)
            await db.execute(
                """
                INSERT INTO comments (comment_id, user_id, created_at, deleted)
                VALUES (?, 'unknown', ?, 1)
                ON CONFLICT(comment_id) DO UPDATE SET deleted=1
                """,
                (comment_id, now)
            )

            # Cancel any pending jobs for this comment
            cursor = await db.execute(
                "UPDATE dm_jobs SET status='cancelled', updated_at=? WHERE comment_id=? AND status='pending'",
                (now, comment_id)
            )
            cancelled_count = cursor.rowcount
            if cancelled_count > 0:
                logger.info(f"Cancelled {cancelled_count} pending jobs for deleted comment {comment_id}")

    # Mark event row as processed
    await db.execute("UPDATE events SET processed_at=? WHERE event_id=?", (now, event_id))


async def process_duplicate_events(db):
    """
    Process unprocessed duplicate_events rows.
    If event_type == 'comment.created' and text matches any rule, increment duplicates_blocked counter.
    Does NOT create dm_jobs.
    """
    cursor = await db.execute(
        "SELECT id, raw_body FROM duplicate_events WHERE processed_at IS NULL ORDER BY received_at ASC LIMIT 50"
    )
    unprocessed_dups = await cursor.fetchall()
    now = time.time()

    if not unprocessed_dups:
        return

    cursor = await db.execute("SELECT keyword_lower FROM rules")
    rules = await cursor.fetchall()

    for dup in unprocessed_dups:
        dup_id = dup["id"]
        raw_body = dup["raw_body"]
        try:
            payload = json.loads(raw_body)
            if isinstance(payload, dict) and payload.get("event_type") == "comment.created":
                data = payload.get("data", {})
                if isinstance(data, dict):
                    text = data.get("text", "")
                    text_lower = text.lower()

                    matching_rules_count = 0
                    for r in rules:
                        if r["keyword_lower"] in text_lower:
                            matching_rules_count += 1

                    if matching_rules_count > 0:
                        await db.execute(
                            "UPDATE counters SET value = value + ? WHERE name = 'duplicates_blocked'",
                            (matching_rules_count,)
                        )
        except Exception as exc:
            logger.error(f"Error processing duplicate event id {dup_id}: {exc}")

        await db.execute("UPDATE duplicate_events SET processed_at=? WHERE id=?", (now, dup_id))


async def run_ingest_worker_once():
    """
    Execute one batch of unprocessed events ingestion.
    """
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT event_id, event_type, raw_body, signature_valid FROM events WHERE processed_at IS NULL ORDER BY received_at ASC LIMIT 50"
        )
        unprocessed = await cursor.fetchall()

        for event in unprocessed:
            await process_single_event(db, event)

        await process_duplicate_events(db)

        await db.commit()


async def ingest_worker_loop():
    """
    Continuous background worker loop for event ingestion.
    Runs every 100ms with try/except error recovery.
    """
    logger.info("Ingest worker loop started")
    while True:
        try:
            await run_ingest_worker_once()
        except asyncio.CancelledError:
            logger.info("Ingest worker loop cancelled")
            break
        except Exception as exc:
            logger.error(f"Error in ingest worker loop: {exc}", exc_info=True)
        await asyncio.sleep(0.1)
