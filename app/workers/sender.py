import time
import random
import logging
import asyncio
import httpx
from app.db import get_db
from app.config import settings

logger = logging.getLogger("linkplease.sender")


async def get_active_send_count_in_window(db, now: float) -> tuple[int, float | None]:
    """
    Check rolling 61-second window in send_log table.
    Widen window to 61.0s to compensate for potential clock skew while using full 10 requests/60s budget.
    Returns (count_in_window, oldest_timestamp_in_window).
    """
    window_start = now - 61.0
    cursor = await db.execute("SELECT COUNT(*) FROM send_log WHERE sent_at > ?", (window_start,))
    count = (await cursor.fetchone())[0]

    oldest_sent_at = None
    if count > 0:
        cursor = await db.execute(
            "SELECT sent_at FROM send_log WHERE sent_at > ? ORDER BY sent_at ASC LIMIT 1",
            (window_start,)
        )
        row = await cursor.fetchone()
        if row:
            oldest_sent_at = row["sent_at"]

    return count, oldest_sent_at


async def claim_next_pending_job(db, now: float):
    """
    Claim the next pending job ready for execution (next_attempt_at <= now).
    """
    cursor = await db.execute(
        """
        SELECT job_id, rule_id, recipient_user_id, comment_id, message, idempotency_key, attempts
        FROM dm_jobs
        WHERE status = 'pending' AND next_attempt_at <= ?
        ORDER BY next_attempt_at ASC
        LIMIT 1
        """,
        (now,)
    )
    return await cursor.fetchone()


async def execute_send_job(client: httpx.AsyncClient, db, job):
    """
    Execute outbound /v1/dm/send request for a single job with rate limiting,
    pre-call send_log reservation, and error handling.
    """
    now = time.time()
    job_id = job["job_id"]
    recipient_user_id = job["recipient_user_id"]
    comment_id = job["comment_id"]
    message = job["message"]
    idempotency_key = job["idempotency_key"]
    attempts = job["attempts"]

    # Prune send_log rows older than 300s to prevent unbounded DB growth across long runs
    await db.execute("DELETE FROM send_log WHERE sent_at < ?", (now - 300.0,))

    # Rate limiting check: max 10 sends per rolling 61s window
    send_count, oldest_sent_at = await get_active_send_count_in_window(db, now)

    if send_count >= 10:
        if oldest_sent_at:
            wait_time = (oldest_sent_at + 61.0) - now + 0.1
        else:
            wait_time = 1.0
        wait_time = max(0.1, wait_time)
        logger.info(f"Rate limiter threshold reached ({send_count}/10). Sleeping {wait_time:.2f}s")
        await asyncio.sleep(wait_time)
        return

    # Reserve send slot in send_log BEFORE making outbound HTTP call
    # Why: If process crashes or hangs mid-request, rate limit window remains reserved.
    await db.execute("INSERT INTO send_log (sent_at) VALUES (?)", (now,))
    await db.commit()

    url = f"{settings.pseudogram_base_url.rstrip('/')}/v1/dm/send"
    headers = {
        "X-API-Key": settings.pseudogram_api_key,
        "Idempotency-Key": idempotency_key,
        "Content-Type": "application/json"
    }
    payload = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id
    }

    try:
        response = await client.post(url, json=payload, headers=headers, timeout=10.0)
        status_code = response.status_code

        if status_code in (200, 202):
            # 200/202 Accepted: Store dm_id and move to 'accepted' state (unconfirmed)
            resp_data = response.json()
            dm_id = resp_data.get("dm_id")
            await db.execute(
                """
                UPDATE dm_jobs
                SET status = 'accepted', dm_id = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (dm_id, now, job_id)
            )
            await db.commit()
            logger.info(f"Job {job_id} accepted by API with dm_id {dm_id}")

        elif status_code == 429:
            # 429 Rate Limited: Respect Retry-After header, do NOT count as attempt
            retry_after_hdr = response.headers.get("Retry-After")
            try:
                retry_after = float(retry_after_hdr) if retry_after_hdr else 5.0
            except ValueError:
                retry_after = 5.0

            next_attempt = now + max(1.0, retry_after)
            await db.execute(
                """
                UPDATE dm_jobs
                SET next_attempt_at = ?, updated_at = ?, last_error = 'Rate limited 429'
                WHERE job_id = ?
                """,
                (next_attempt, now, job_id)
            )
            await db.commit()
            logger.warning(f"Job {job_id} hit 429 rate limit. Rescheduled for +{retry_after:.1f}s")

        elif status_code == 400:
            # 400 Bad Request: Terminal failure, do not retry
            last_err = f"HTTP 400: {response.text}"
            await db.execute(
                """
                UPDATE dm_jobs
                SET status = 'failed', updated_at = ?, last_error = ?
                WHERE job_id = ?
                """,
                (now, last_err, job_id)
            )
            await db.commit()
            logger.error(f"Job {job_id} failed terminally with HTTP 400: {response.text}")

        else:
            # HTTP 500 or other status: Retry with exponential backoff
            new_attempts = attempts + 1
            if new_attempts >= 6:
                await db.execute(
                    """
                    UPDATE dm_jobs
                    SET status = 'failed', attempts = ?, updated_at = ?, last_error = ?
                    WHERE job_id = ?
                    """,
                    (new_attempts, now, f"HTTP {status_code}: {response.text}", job_id)
                )
                logger.error(f"Job {job_id} failed after {new_attempts} attempts (HTTP {status_code})")
            else:
                backoff = min(2 ** new_attempts + random.uniform(0.0, 1.0), 60.0)
                next_attempt = now + backoff
                await db.execute(
                    """
                    UPDATE dm_jobs
                    SET attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ?
                    WHERE job_id = ?
                    """,
                    (new_attempts, next_attempt, now, f"HTTP {status_code}: {response.text}", job_id)
                )
                logger.warning(f"Job {job_id} retry #{new_attempts} scheduled in {backoff:.2f}s (HTTP {status_code})")
            await db.commit()

    except Exception as exc:
        # Connection error, timeout, or network exception
        new_attempts = attempts + 1
        err_msg = f"Network exception: {str(exc)}"

        if new_attempts >= 6:
            await db.execute(
                """
                UPDATE dm_jobs
                SET status = 'failed', attempts = ?, updated_at = ?, last_error = ?
                WHERE job_id = ?
                """,
                (new_attempts, now, err_msg, job_id)
            )
            logger.error(f"Job {job_id} failed after {new_attempts} attempts due to error: {err_msg}")
        else:
            backoff = min(2 ** new_attempts + random.uniform(0.0, 1.0), 60.0)
            next_attempt = now + backoff
            await db.execute(
                """
                UPDATE dm_jobs
                SET attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ?
                WHERE job_id = ?
                """,
                (new_attempts, next_attempt, now, err_msg, job_id)
            )
            logger.warning(f"Job {job_id} network retry #{new_attempts} scheduled in {backoff:.2f}s: {err_msg}")
        await db.commit()


async def sender_worker_loop():
    """
    Continuous background loop for dispatching DM jobs.
    Uses persistent AsyncClient for connection reuse.
    """
    logger.info("Sender worker loop started")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                now = time.time()
                async with get_db() as db:
                    job = await claim_next_pending_job(db, now)
                    if job:
                        await execute_send_job(client, db, job)
                    else:
                        await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("Sender worker loop cancelled")
                break
            except Exception as exc:
                logger.error(f"Error in sender worker loop: {exc}", exc_info=True)
                await asyncio.sleep(0.5)
