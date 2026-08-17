import time
import json
import uuid
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, HTMLResponse
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


@app.get("/", response_class=HTMLResponse)
async def root():
    """Welcome landing page for the root URL."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Link Please — Live Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: linear-gradient(135deg, #0b091a, #161233, #0d0b1f);
            color: #f3f4f6;
            min-height: 100vh;
            padding: 40px 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .container {
            max-width: 900px;
            width: 100%;
            display: flex;
            flex-direction: column;
            gap: 24px;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 16px;
            padding: 24px;
            backdrop-filter: blur(8px);
        }
        h1 {
            font-size: 1.8rem;
            background: linear-gradient(90deg, #a78bfa, #3b82f6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
        }
        .subtitle { color: #9ca3af; font-size: 0.85rem; margin-top: 4px; }
        .badge-live {
            background: rgba(34, 197, 94, 0.15);
            color: #4ade80;
            border: 1px solid rgba(34, 197, 94, 0.3);
            font-size: 0.75rem;
            font-weight: 700;
            padding: 4px 12px;
            border-radius: 99px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .badge-live::before {
            content: "";
            display: inline-block;
            width: 8px;
            height: 8px;
            background: #22c55e;
            border-radius: 50%;
            box-shadow: 0 0 8px #22c55e;
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 0.6; }
            50% { transform: scale(1.1); opacity: 1; }
            100% { transform: scale(0.9); opacity: 0.6; }
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 16px;
            padding: 20px;
            text-align: center;
            transition: transform 0.2s, background-color 0.2s;
        }
        .stat-card:hover {
            transform: translateY(-2px);
            background: rgba(255, 255, 255, 0.04);
            border-color: rgba(255, 255, 255, 0.1);
        }
        .stat-label {
            color: #9ca3af;
            font-size: 0.8rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 8px;
        }
        .stat-val {
            font-size: 2.2rem;
            font-weight: 800;
            color: #ffffff;
        }
        .card-sent { border-bottom: 3px solid #10b981; }
        .card-sent .stat-val { color: #34d399; }
        .card-failed { border-bottom: 3px solid #ef4444; }
        .card-failed .stat-val { color: #f87171; }
        .card-queued { border-bottom: 3px solid #f59e0b; }
        .card-queued .stat-val { color: #fbbf24; }
        .card-blocked { border-bottom: 3px solid #8b5cf6; }
        .card-blocked .stat-val { color: #a78bfa; }
        
        /* Main Body Grid */
        .main-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 24px;
        }
        @media (min-width: 768px) {
            .main-grid { grid-template-columns: 1fr 1fr; }
        }
        
        .section-card {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 16px;
            padding: 24px;
        }
        h2 {
            font-size: 1.2rem;
            margin-bottom: 18px;
            color: #ffffff;
            border-left: 3px solid #6366f1;
            padding-left: 10px;
        }
        
        /* Rule Form */
        .form-group { margin-bottom: 16px; }
        .form-group label {
            display: block;
            font-size: 0.8rem;
            color: #9ca3af;
            margin-bottom: 6px;
            font-weight: 600;
        }
        .form-group input, .form-group textarea {
            width: 100%;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 8px;
            padding: 10px 12px;
            color: #ffffff;
            font-family: inherit;
            font-size: 0.9rem;
            outline: none;
            transition: border-color 0.2s, background-color 0.2s;
        }
        .form-group input:focus, .form-group textarea:focus {
            border-color: #6366f1;
            background: rgba(255, 255, 255, 0.08);
        }
        button {
            width: 100%;
            background: #6366f1;
            color: #ffffff;
            border: none;
            border-radius: 8px;
            padding: 12px;
            font-size: 0.95rem;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s, transform 0.1s;
        }
        button:hover { background: #4f46e5; }
        button:active { transform: scale(0.98); }
        .form-feedback {
            margin-top: 12px;
            font-size: 0.85rem;
            display: none;
            padding: 10px;
            border-radius: 6px;
        }
        .feedback-success { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.2); }
        .feedback-error { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.2); }
        
        /* Endpoints list */
        .endpoints-list { list-style: none; }
        .endpoints-list li {
            padding: 12px;
            margin-bottom: 10px;
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 10px;
            display: flex;
            align-items: center;
            gap: 12px;
            transition: background-color 0.2s;
        }
        .endpoints-list li:hover {
            background: rgba(255, 255, 255, 0.04);
        }
        .endpoints-list a {
            text-decoration: none;
            color: inherit;
            display: block;
            width: 100%;
        }
        .method {
            font-size: 0.65rem;
            font-weight: 700;
            padding: 3px 6px;
            border-radius: 4px;
            min-width: 48px;
            text-align: center;
        }
        .get { background: rgba(16, 185, 129, 0.15); color: #34d399; }
        .post { background: rgba(59, 130, 246, 0.15); color: #60a5fa; }
        .path { font-family: monospace; font-size: 0.85rem; font-weight: 600; color: #ffffff; }
        .desc { color: #9ca3af; font-size: 0.8rem; margin-left: auto; }
        
        footer {
            text-align: center;
            margin-top: 40px;
            color: #6b7280;
            font-size: 0.8rem;
            border-top: 1px solid rgba(255, 255, 255, 0.05);
            padding-top: 20px;
            width: 100%;
        }
        .footer-credit {
            font-weight: 600;
            color: #9ca3af;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>🔗 Link Please</h1>
                <p class="subtitle">Instagram Comment-to-DM Automation Engine</p>
            </div>
            <span class="badge-live">Live Monitoring</span>
        </header>
        
        <!-- Live Stats Panel -->
        <section class="stats-grid">
            <div class="stat-card card-sent">
                <div class="stat-label">Sent DMs</div>
                <div class="stat-val" id="stat-sent">0</div>
            </div>
            <div class="stat-card card-failed">
                <div class="stat-label">Failed DMs</div>
                <div class="stat-val" id="stat-failed">0</div>
            </div>
            <div class="stat-card card-queued">
                <div class="stat-label">Queued DMs</div>
                <div class="stat-val" id="stat-queued">0</div>
            </div>
            <div class="stat-card card-blocked">
                <div class="stat-label">Duplicates Blocked</div>
                <div class="stat-val" id="stat-blocked">0</div>
            </div>
        </section>
        
        <div class="main-grid">
            <!-- Left Panel: Create Automation Rule -->
            <section class="section-card">
                <h2>Create Rule</h2>
                <form id="rule-form">
                    <div class="form-group">
                        <label for="keyword">Keyword (Case Insensitive)</label>
                        <input type="text" id="keyword" required placeholder="e.g., PRICE, LINK, INFO">
                    </div>
                    <div class="form-group">
                        <label for="dm-message">DM Message</label>
                        <textarea id="dm-message" rows="3" required placeholder="Type the message that will be sent via direct message..."></textarea>
                    </div>
                    <button type="submit">Create Automation Rule</button>
                </form>
                <div class="form-feedback" id="form-feedback"></div>
            </section>
            
            <!-- Right Panel: API Endpoints -->
            <section class="section-card">
                <h2>API Reference</h2>
                <ul class="endpoints-list">
                    <a href="/healthz" target="_blank">
                        <li>
                            <span class="method get">GET</span>
                            <span class="path">/healthz</span>
                            <span class="desc">Liveness check</span>
                        </li>
                    </a>
                    <a href="/stats" target="_blank">
                        <li>
                            <span class="method get">GET</span>
                            <span class="path">/stats</span>
                            <span class="desc">Live stats counters</span>
                        </li>
                    </a>
                    <li>
                        <span class="method post">POST</span>
                        <span class="path">/rules</span>
                        <span class="desc">Create rule (programmatic)</span>
                    </li>
                    <li>
                        <span class="method post">POST</span>
                        <span class="path">/webhook</span>
                        <span class="desc">Receive webhook events</span>
                    </li>
                </ul>
            </section>
        </div>
        
        <footer>
            <p>Developed by <span class="footer-credit">Mallela Lokesh Reddy</span> &bull; SRM University AP</p>
        </footer>
    </div>

    <script>
        // Fetch stats immediately and update every 2 seconds
        async function fetchStats() {
            try {
                const response = await fetch('/stats');
                if (response.ok) {
                    const stats = await response.json();
                    document.getElementById('stat-sent').textContent = stats.sent;
                    document.getElementById('stat-failed').textContent = stats.failed;
                    document.getElementById('stat-queued').textContent = stats.queued;
                    document.getElementById('stat-blocked').textContent = stats.duplicates_blocked;
                }
            } catch (err) {
                console.error("Failed to fetch stats:", err);
            }
        }

        // Rule creation handling
        const ruleForm = document.getElementById('rule-form');
        const feedbackDiv = document.getElementById('form-feedback');

        ruleForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            feedbackDiv.style.display = 'none';
            feedbackDiv.className = 'form-feedback';
            
            const keyword = document.getElementById('keyword').value;
            const dm_message = document.getElementById('dm-message').value;
            
            try {
                const response = await fetch('/rules', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ keyword, dm_message })
                });
                
                const data = await response.json();
                if (response.ok) {
                    feedbackDiv.textContent = `Success! Rule created with ID: ${data.rule_id}`;
                    feedbackDiv.classList.add('feedback-success');
                    feedbackDiv.style.display = 'block';
                    ruleForm.reset();
                } else {
                    feedbackDiv.textContent = `Error: ${data.detail || 'Failed to create rule'}`;
                    feedbackDiv.classList.add('feedback-error');
                    feedbackDiv.style.display = 'block';
                }
            } catch (err) {
                feedbackDiv.textContent = `Error: Network failure. Could not connect to API.`;
                feedbackDiv.classList.add('feedback-error');
                feedbackDiv.style.display = 'block';
            }
        });

        // Start polling
        fetchStats();
        setInterval(fetchStats, 2000);
    </script>
</body>
</html>"""


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


@app.get("/rules")
@app.get("/rules/", include_in_schema=False)
async def get_rules():
    """Return all active keyword-to-DM rules."""
    async with get_db() as db:
        cursor = await db.execute("SELECT rule_id, keyword_lower AS keyword, dm_message FROM rules")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


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
        import hmac, hashlib
        from app.config import settings
        secret_bytes = settings.pseudogram_api_key.encode("utf-8")
        computed = "sha256=" + hmac.new(secret_bytes, msg=raw_body_bytes, digestmod=hashlib.sha256).hexdigest()
        debug_info = f"EXPECTED: {computed} | RECEIVED: {signature_header} | BODY: {raw_body_str}"
        logger.warning(f"Rejected webhook due to invalid signature. Expected: {computed}, Got: {signature_header}")
        
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
                (event_id, debug_info, now)
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


@app.get("/debug-events")
async def debug_events():
    """Temporary endpoint to check DB state and signature verification details."""
    try:
        async with get_db() as db:
            c1 = await db.execute("SELECT COUNT(*) FROM events")
            events_count = (await c1.fetchone())[0]

            c2 = await db.execute("SELECT COUNT(*) FROM duplicate_events")
            dupes_count = (await c2.fetchone())[0]

            c3 = await db.execute("SELECT COUNT(*) FROM rejected_events")
            rejected_count = (await c3.fetchone())[0]

            # Get the last 5 rejected events to inspect their signatures and bodies
            c4 = await db.execute("SELECT * FROM rejected_events ORDER BY id DESC LIMIT 5")
            last_rejected = [dict(row) for row in await c4.fetchall()]

            # Count of dm_jobs
            c5 = await db.execute("SELECT COUNT(*) FROM dm_jobs")
            jobs_count = (await c5.fetchone())[0]

            # Last 50 dm_jobs (shows the entire table)
            c6 = await db.execute("SELECT job_id, status, attempts, reconcile_attempts, next_attempt_at, updated_at, last_error, dm_id FROM dm_jobs ORDER BY created_at DESC LIMIT 50")
            last_jobs = [dict(row) for row in await c6.fetchall()]

            # Count of send_log
            c7 = await db.execute("SELECT COUNT(*) FROM send_log")
            send_log_count = (await c7.fetchone())[0]

            # Also check settings
            from app.config import settings
            from app.workers.reconciler import last_run as rec_run
            return {
                "now": time.time(),
                "events_count": events_count,
                "duplicate_events_count": dupes_count,
                "rejected_events_count": rejected_count,
                "last_rejected": last_rejected,
                "jobs_count": jobs_count,
                "last_jobs": last_jobs,
                "send_log_count": send_log_count,
                "reconciler_last_run": rec_run,
                "settings": {
                    "enable_signature_verification": settings.enable_signature_verification,
                    "db_path": settings.db_path,
                    "pseudogram_api_key_len": len(settings.pseudogram_api_key) if settings.pseudogram_api_key else 0,
                    "pseudogram_api_key_prefix": settings.pseudogram_api_key[:8] if settings.pseudogram_api_key else ""
                }
            }
    except Exception as exc:
        return {"error": str(exc)}

