import time
import hashlib
import logging
import asyncio
import httpx
from app.db import get_db
from app.config import settings

logger = logging.getLogger("linkplease.reconciler")


async def reconcile_accepted_jobs_once(client: httpx.AsyncClient):
    """
    Poll jobs in 'accepted' status older than 2s and reconcile delivery state via GET /v1/dm/{dm_id}.
    """
    now = time.time()
    cutoff = now - 2.0

    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT job_id, rule_id, recipient_user_id, dm_id, idempotency_key, attempts, reconcile_attempts
            FROM dm_jobs
            WHERE status = 'accepted' AND updated_at <= ?
            LIMIT 10
            """,
            (cutoff,)
        )
        accepted_jobs = await cursor.fetchall()

        for job in accepted_jobs:
            job_id = job["job_id"]
            rule_id = job["rule_id"]
            user_id = job["recipient_user_id"]
            dm_id = job["dm_id"]
            reconcile_attempts = job["reconcile_attempts"]

            if not dm_id:
                # If dm_id is missing for some reason, mark failed
                await db.execute(
                    "UPDATE dm_jobs SET status='failed', updated_at=?, last_error='Missing dm_id' WHERE job_id=?",
                    (now, job_id)
                )
                continue

            url = f"{settings.pseudogram_base_url.rstrip('/')}/v1/dm/{dm_id}"
            headers = {"X-API-Key": settings.pseudogram_api_key}

            try:
                response = await client.get(url, headers=headers, timeout=5.0)

                if response.status_code == 200:
                    data = response.json()
                    status_val = data.get("status")

                    if status_val == "delivered":
                        # Delivery confirmed by external API!
                        await db.execute(
                            "UPDATE dm_jobs SET status='delivered', updated_at=? WHERE job_id=?",
                            (now, job_id)
                        )
                        logger.info(f"Job {job_id} (dm_id {dm_id}) confirmed delivered")

                    elif status_val == "failed":
                        # ~15% flip case: API accepted DM but later failed to deliver.
                        # Track reconcile attempts via database column.
                        n = reconcile_attempts + 1

                        if n > 3:
                            await db.execute(
                                """
                                UPDATE dm_jobs
                                SET status='failed', reconcile_attempts=?, updated_at=?, last_error='Reconcile retries exhausted (3)'
                                WHERE job_id=?
                                """,
                                (n, now, job_id)
                            )
                            logger.error(f"Job {job_id} failed after reaching max reconcile retries (3)")
                        else:
                            # The fresh idempotency key is essential — reusing the original key would make POST /v1/dm/send return the same dead dm_id.
                            fresh_idem_key = hashlib.sha256(f"{rule_id}:{user_id}:retry{n}".encode()).hexdigest()
                            await db.execute(
                                """
                                UPDATE dm_jobs
                                SET status='pending', dm_id=NULL, reconcile_attempts=?, attempts=0, idempotency_key=?, next_attempt_at=?, updated_at=?, last_error='Reconciler reset after API failed delivery'
                                WHERE job_id=?
                                """,
                                (n, fresh_idem_key, now, now, job_id)
                            )
                            logger.warning(f"Job {job_id} failed on delivery; reset to pending with fresh idempotency key {fresh_idem_key[:8]} (reconcile attempt #{n})")

                    elif status_val == "queued":
                        # Still queued on external server, touch updated_at to retry next cycle
                        await db.execute("UPDATE dm_jobs SET updated_at=? WHERE job_id=?", (now, job_id))

                elif response.status_code == 404:
                    # DM ID not found on remote server, mark failed
                    await db.execute(
                        "UPDATE dm_jobs SET status='failed', updated_at=?, last_error='DM ID 404 Not Found' WHERE job_id=?",
                        (now, job_id)
                    )

            except Exception as exc:
                logger.error(f"Error checking dm_id {dm_id} status for job {job_id}: {exc}")

        if accepted_jobs:
            await db.commit()


async def reconciler_worker_loop():
    """
    Continuous background loop for delivery status reconciliation.
    Runs every 3 seconds.
    """
    logger.info("Reconciler worker loop started")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await reconcile_accepted_jobs_once(client)
            except asyncio.CancelledError:
                logger.info("Reconciler worker loop cancelled")
                break
            except Exception as exc:
                logger.error(f"Error in reconciler worker loop: {exc}", exc_info=True)
            await asyncio.sleep(3.0)
