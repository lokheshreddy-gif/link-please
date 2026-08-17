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
    <title>LinkPlease Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --primary-accent: #3b82f6;
            --primary-accent-hover: #2563eb;
            --primary-accent-gradient: linear-gradient(to right, #60a5fa, #a78bfa);
            --badge-glow: #22c55e;
            --badge-glow-rgba: rgba(34, 197, 94, 0.2);
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: #0b0c10;
            color: #f3f4f6;
            min-height: 100vh;
            padding: 40px 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
            transition: background-color 0.3s, color 0.3s;
        }
        .container {
            max-width: 1000px;
            width: 100%;
            display: flex;
            flex-direction: column;
            gap: 30px;
        }
        header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background-color: #121620;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 20px 30px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.25);
        }
        .header-title-container {
            display: flex;
            align-items: center;
            gap: 12px;
        }
        h1 {
            font-size: 1.8rem;
            font-weight: 700;
            background: var(--primary-accent-gradient);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            transition: background 0.3s;
        }
        .badge-online {
            background-color: rgba(34, 197, 94, 0.1);
            color: #4ade80;
            border: 1px solid var(--badge-glow-rgba);
            font-size: 0.75rem;
            font-weight: 600;
            padding: 3px 10px;
            border-radius: 99px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .badge-online::before {
            content: "";
            width: 6px;
            height: 6px;
            background-color: var(--badge-glow);
            border-radius: 50%;
            display: inline-block;
        }
        
        /* Team Accents / Themes */
        .themes-container {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .theme-label {
            font-size: 0.75rem;
            color: #9ca3af;
            font-weight: 600;
        }
        .theme-btn {
            width: 20px;
            height: 20px;
            border-radius: 50%;
            border: 2px solid transparent;
            cursor: pointer;
            transition: transform 0.2s, border-color 0.2s;
            margin: 0;
            padding: 0;
            display: inline-block;
        }
        .theme-btn:hover {
            transform: scale(1.15);
        }
        .theme-btn.active {
            border-color: #ffffff;
            transform: scale(1.1);
        }
        .btn-indigo { background-color: #3b82f6; }
        .btn-emerald { background-color: #10b981; }
        .btn-rose { background-color: #f43f5e; }
        .btn-amber { background-color: #f59e0b; }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
        }
        @media (max-width: 768px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        @media (max-width: 480px) {
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
        .stat-card {
            background-color: #121620;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 24px;
            text-align: center;
            box-shadow: 0 4px 20px rgba(0,0,0,0.25);
        }
        .stat-label {
            color: #9ca3af;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            margin-bottom: 12px;
        }
        .stat-val {
            font-size: 2.8rem;
            font-weight: 800;
        }
        .stat-sent { color: #10b981; }
        .stat-failed { color: #f87171; }
        .stat-queued { color: var(--primary-accent); }
        .stat-blocked { color: #f59e0b; }
        
        /* Main Grid */
        .main-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 24px;
        }
        @media (max-width: 768px) {
            .main-grid {
                grid-template-columns: 1fr;
            }
        }
        .section-card {
            background-color: #121620;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.25);
            display: flex;
            flex-direction: column;
            min-height: 400px;
        }
        h2 {
            font-size: 1.3rem;
            font-weight: 700;
            margin-bottom: 24px;
            color: #ffffff;
        }
        
        /* Filter Controls */
        .controls-row {
            display: flex;
            gap: 12px;
            margin-bottom: 18px;
        }
        .search-control {
            flex: 1;
        }
        .sort-control {
            width: 150px;
        }
        .search-control input, .sort-control select {
            width: 100%;
            background-color: #0b0d13;
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 8px;
            padding: 10px 12px;
            color: #ffffff;
            font-family: inherit;
            font-size: 0.85rem;
            outline: none;
            transition: border-color 0.2s;
        }
        .search-control input:focus, .sort-control select:focus {
            border-color: var(--primary-accent);
        }
        
        /* Form inputs */
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            font-size: 0.8rem;
            color: #9ca3af;
            margin-bottom: 8px;
            font-weight: 600;
        }
        .form-group input, .form-group textarea {
            width: 100%;
            background-color: #0b0d13;
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 8px;
            padding: 12px;
            color: #ffffff;
            font-family: inherit;
            font-size: 0.9rem;
            outline: none;
            transition: border-color 0.2s;
        }
        .form-group input:focus, .form-group textarea:focus {
            border-color: var(--primary-accent);
        }
        button.btn-submit {
            background-color: var(--primary-accent);
            color: #ffffff;
            border: none;
            border-radius: 8px;
            padding: 14px;
            font-size: 0.95rem;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            transition: background-color 0.2s;
            margin-top: 10px;
        }
        button.btn-submit:hover {
            background-color: var(--primary-accent-hover);
        }
        
        /* Rules List */
        .rules-list {
            display: flex;
            flex-direction: column;
            gap: 12px;
            overflow-y: auto;
            max-height: 380px;
            padding-right: 4px;
        }
        .rules-list::-webkit-scrollbar {
            width: 6px;
        }
        .rules-list::-webkit-scrollbar-thumb {
            background-color: rgba(255, 255, 255, 0.1);
            border-radius: 3px;
        }
        .rule-item {
            background-color: #181d2a;
            border: 1px solid rgba(255, 255, 255, 0.03);
            border-radius: 10px;
            padding: 18px;
            display: flex;
            flex-direction: column;
            gap: 6px;
            transition: border-color 0.2s;
        }
        .rule-item:hover {
            border-color: rgba(255, 255, 255, 0.08);
        }
        .rule-keyword {
            color: var(--primary-accent);
            font-weight: 700;
            font-size: 0.95rem;
            transition: color 0.3s;
        }
        .rule-message {
            color: #9ca3af;
            font-size: 0.85rem;
            line-height: 1.4;
        }
        .rule-date {
            color: #4b5563;
            font-size: 0.75rem;
            margin-top: 4px;
        }
        .empty-rules {
            color: #6b7280;
            font-size: 0.9rem;
            text-align: center;
            margin: auto;
        }
        
        .form-feedback {
            margin-top: 16px;
            font-size: 0.85rem;
            display: none;
            padding: 10px;
            border-radius: 6px;
        }
        .feedback-success { background: rgba(16, 185, 129, 0.1); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.15); }
        .feedback-error { background: rgba(239, 68, 68, 0.1); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.15); }
        
        footer {
            text-align: center;
            margin-top: 50px;
            color: #4b5563;
            font-size: 0.8rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title-container">
                <h1>LinkPlease Dashboard</h1>
                <span class="badge-online">Online</span>
            </div>
            <!-- Theme / Team switcher controls -->
            <div class="themes-container">
                <span class="theme-label">Accents:</span>
                <button class="theme-btn btn-indigo active" onclick="setTheme('indigo')" title="Indigo Accent"></button>
                <button class="theme-btn btn-emerald" onclick="setTheme('emerald')" title="Emerald Accent"></button>
                <button class="theme-btn btn-rose" onclick="setTheme('rose')" title="Rose Accent"></button>
                <button class="theme-btn btn-amber" onclick="setTheme('amber')" title="Amber Accent"></button>
            </div>
        </header>
        
        <!-- Stats Panel -->
        <section class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">SENT</div>
                <div class="stat-val stat-sent" id="stat-sent">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">FAILED</div>
                <div class="stat-val" id="stat-failed">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">QUEUED</div>
                <div class="stat-val stat-queued" id="stat-queued">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">DUPLICATES BLOCKED</div>
                <div class="stat-val stat-blocked" id="stat-blocked">0</div>
            </div>
        </section>
        
        <div class="main-grid">
            <!-- Create Rule Card -->
            <section class="section-card">
                <h2>Create New Rule</h2>
                <form id="rule-form">
                    <div class="form-group">
                        <label for="keyword">Keyword</label>
                        <input type="text" id="keyword" required placeholder="e.g. PRICE">
                    </div>
                    <div class="form-group">
                        <label for="dm-message">DM Message</label>
                        <textarea id="dm-message" rows="4" required placeholder="e.g. Here is our pricing..."></textarea>
                    </div>
                    <button type="submit" class="btn-submit">Create Rule</button>
                </form>
                <div class="form-feedback" id="form-feedback"></div>
            </section>
            
            <!-- Active Rules Card -->
            <section class="section-card">
                <h2>Active Rules</h2>
                <!-- Filter Controls Row -->
                <div class="controls-row">
                    <div class="search-control">
                        <input type="text" id="search-input" onkeyup="filterRules()" placeholder="Filter by keyword...">
                    </div>
                    <div class="sort-control">
                        <select id="sort-select" onchange="sortAndRenderRules()">
                            <option value="created_desc">Created (Newest)</option>
                            <option value="alpha_asc">Keyword (A-Z)</option>
                            <option value="alpha_desc">Keyword (Z-A)</option>
                        </select>
                    </div>
                </div>
                <div class="rules-list" id="rules-list">
                    <div class="empty-rules">No rules active yet.</div>
                </div>
            </section>
        </div>
        
        <footer>
            <p>Built by Mallela Lokesh Reddy</p>
        </footer>
    </div>

    <script>
        // Global memory storage for fetched rules to allow instant client-side filtering/sorting
        let _allRules = [];

        // Theme Switcher Logic
        function setTheme(theme) {
            // Remove active class from all buttons
            document.querySelectorAll('.theme-btn').forEach(btn => btn.classList.remove('active'));
            
            // Add active to selected button
            const activeBtn = document.querySelector(`.btn-${theme}`);
            if (activeBtn) activeBtn.classList.add('active');
            
            const root = document.documentElement;
            if (theme === 'indigo') {
                root.style.setProperty('--primary-accent', '#3b82f6');
                root.style.setProperty('--primary-accent-hover', '#2563eb');
                root.style.setProperty('--primary-accent-gradient', 'linear-gradient(to right, #60a5fa, #a78bfa)');
            } else if (theme === 'emerald') {
                root.style.setProperty('--primary-accent', '#10b981');
                root.style.setProperty('--primary-accent-hover', '#059669');
                root.style.setProperty('--primary-accent-gradient', 'linear-gradient(to right, #34d399, #6ee7b7)');
            } else if (theme === 'rose') {
                root.style.setProperty('--primary-accent', '#f43f5e');
                root.style.setProperty('--primary-accent-hover', '#e11d48');
                root.style.setProperty('--primary-accent-gradient', 'linear-gradient(to right, #fb7185, #f472b6)');
            } else if (theme === 'amber') {
                root.style.setProperty('--primary-accent', '#f59e0b');
                root.style.setProperty('--primary-accent-hover', '#d97706');
                root.style.setProperty('--primary-accent-gradient', 'linear-gradient(to right, #fbbf24, #fcd34d)');
            }
        }

        // Fetch stats and update UI
        async function fetchStats() {
            try {
                const response = await fetch('/stats');
                if (response.ok) {
                    const stats = await response.json();
                    document.getElementById('stat-sent').textContent = stats.sent;
                    document.getElementById('stat-failed').textContent = stats.failed;
                    document.getElementById('stat-queued').textContent = stats.queued;
                    document.getElementById('stat-blocked').textContent = stats.duplicates_blocked;
                    
                    // Highlight failed count if non-zero
                    const failedEl = document.getElementById('stat-failed');
                    if (stats.failed > 0) {
                        failedEl.className = 'stat-val stat-failed';
                    } else {
                        failedEl.className = 'stat-val';
                    }
                }
            } catch (err) {
                console.error("Failed to fetch stats:", err);
            }
        }

        // Fetch active rules and update list UI
        async function fetchRules() {
            try {
                const response = await fetch('/rules');
                if (response.ok) {
                    _allRules = await response.json();
                    sortAndRenderRules();
                }
            } catch (err) {
                console.error("Failed to fetch rules:", err);
            }
        }

        // Filter and Render Rules
        function filterRules() {
            sortAndRenderRules();
        }

        function sortAndRenderRules() {
            const query = document.getElementById('search-input').value.toLowerCase().trim();
            const sortVal = document.getElementById('sort-select').value;
            const listContainer = document.getElementById('rules-list');

            // Apply filter
            let filtered = _allRules.filter(rule => {
                return rule.keyword.toLowerCase().includes(query);
            });

            if (filtered.length === 0) {
                listContainer.innerHTML = '<div class="empty-rules">No matching rules found.</div>';
                return;
            }

            // Apply sort
            if (sortVal === 'created_desc') {
                filtered.sort((a, b) => (b.created_at || 0) - (a.created_at || 0));
            } else if (sortVal === 'alpha_asc') {
                filtered.sort((a, b) => a.keyword.localeCompare(b.keyword));
            } else if (sortVal === 'alpha_desc') {
                filtered.sort((a, b) => b.keyword.localeCompare(a.keyword));
            }

            // Render
            listContainer.innerHTML = filtered.map(rule => {
                const dateStr = rule.created_at 
                    ? new Date(rule.created_at * 1000).toLocaleString('en-US', {
                        hour: 'numeric',
                        minute: 'numeric',
                        second: 'numeric',
                        hour12: true,
                        month: 'numeric',
                        day: 'numeric',
                        year: 'numeric'
                      })
                    : 'Unknown Date';
                    
                return `
                    <div class="rule-item">
                        <div class="rule-keyword">Keyword: ${escapeHtml(rule.keyword.toUpperCase())}</div>
                        <div class="rule-message">Message: ${escapeHtml(rule.dm_message)}</div>
                        <div class="rule-date">Created: ${dateStr}</div>
                    </div>
                `;
            }).join('');
        }

        function escapeHtml(str) {
            return str
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
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
                    feedbackDiv.textContent = `Success! Rule created.`;
                    feedbackDiv.classList.add('feedback-success');
                    feedbackDiv.style.display = 'block';
                    ruleForm.reset();
                    fetchRules(); // Refresh list immediately
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

        // Initialize and start polling
        fetchStats();
        fetchRules();
        setInterval(fetchStats, 2000);
        setInterval(fetchRules, 5000); // Poll rules every 5s for any updates
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
        cursor = await db.execute("SELECT rule_id, keyword_lower AS keyword, dm_message, created_at FROM rules")
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

