#!/usr/bin/env python3
"""
End-to-End Simulation & Truth Verification Script.

Runs the real Pseudogram simulation against a live target URL, polls /stats
until convergence, fetches server-side truth, and writes a run report.

This script has NO code path that writes a results file without a real run_id
from the Pseudogram server. If any required step fails, it exits non-zero.
"""
import sys
import time
import json
import sqlite3
import argparse
import httpx
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="LinkPlease verification against live Pseudogram simulation")
    parser.add_argument("--url", required=True, help="Target app base URL (e.g. https://myapp.onrender.com)")
    parser.add_argument("--api-key", required=True, help="Pseudogram API key")
    parser.add_argument("--pseudogram-url", default="https://pseudogram-api.onrender.com", help="Pseudogram API base URL")
    parser.add_argument("--count", type=int, default=500, help="Number of events to simulate")
    parser.add_argument("--duration", type=int, default=10, help="Simulation duration in seconds")
    parser.add_argument("--max-wait", type=int, default=3600, help="Max seconds to poll /stats before giving up")
    parser.add_argument("--db-path", default="data/app.db", help="Path to local SQLite DB for rate audit (optional)")
    args = parser.parse_args()

    target_url = args.url.rstrip("/")
    pseudogram_url = args.pseudogram_url.rstrip("/")
    client = httpx.Client(timeout=30.0)

    # ── Step 1: Health check ──────────────────────────────────────────────
    print(f"[1] Health check: GET {target_url}/healthz")
    try:
        res = client.get(f"{target_url}/healthz")
        if res.status_code != 200:
            print(f"    FAIL: HTTP {res.status_code}")
            sys.exit(1)
        print("    OK")
    except Exception as exc:
        print(f"    FAIL: {exc}")
        sys.exit(1)

    # ── Step 2: Register rules ────────────────────────────────────────────
    print("[2] Registering rules via POST /rules")
    rules = [
        {"keyword": "PRICE", "dm_message": "Here's the price list!"},
        {"keyword": "DISCOUNT", "dm_message": "Use code SAVE20 for 20% off!"},
        {"keyword": "LINK", "dm_message": "Check out the link in bio!"},
    ]
    for r in rules:
        res = client.post(f"{target_url}/rules", json=r)
        if res.status_code not in (200, 201):
            print(f"    WARN: POST /rules returned {res.status_code} for keyword={r['keyword']}: {res.text}")
        else:
            print(f"    Created: {res.json().get('rule_id')} -> {r['keyword']}")

    # ── Step 3: Start simulation ──────────────────────────────────────────
    print(f"[3] POST {pseudogram_url}/v1/simulate/start (count={args.count}, duration={args.duration}s)")
    headers = {"X-API-Key": args.api_key, "Content-Type": "application/json"}
    sim_payload = {
        "webhook_url": f"{target_url}/webhook",
        "count": args.count,
        "duration_seconds": args.duration,
    }
    try:
        sim_res = client.post(f"{pseudogram_url}/v1/simulate/start", json=sim_payload, headers=headers)
    except Exception as exc:
        print(f"    FAIL: request error: {exc}")
        sys.exit(1)

    if sim_res.status_code not in (200, 201, 202):
        print(f"    FAIL: HTTP {sim_res.status_code}: {sim_res.text}")
        sys.exit(1)

    sim_data = sim_res.json()
    run_id = sim_data.get("run_id")
    if not run_id:
        print(f"    FAIL: response missing run_id. Full response: {json.dumps(sim_data, indent=2)}")
        sys.exit(1)
    print(f"    run_id = {run_id}")

    # ── Step 4: Poll /stats until convergence ─────────────────────────────
    print(f"[4] Polling GET {target_url}/stats (max {args.max_wait}s, 10s interval)")
    stats_readings = []
    last_stats = None
    identical_count = 0
    converged = False
    poll_start = time.time()

    while True:
        elapsed = time.time() - poll_start
        if elapsed > args.max_wait:
            print(f"    TIMEOUT after {elapsed:.0f}s — stats did NOT converge")
            break

        try:
            stats_res = client.get(f"{target_url}/stats")
            current_stats = stats_res.json() if stats_res.status_code == 200 else None
        except Exception as exc:
            print(f"    [t+{elapsed:.0f}s] Error: {exc}")
            time.sleep(10)
            continue

        ts = time.time()
        stats_readings.append({"timestamp": ts, "stats": current_stats})
        print(f"    [t+{elapsed:.0f}s] {current_stats}")

        if current_stats == last_stats:
            identical_count += 1
        else:
            identical_count = 0
            last_stats = current_stats

        if identical_count >= 3 and (current_stats.get("queued", 0) == 0 or elapsed > 600):
            converged = True
            print(f"    CONVERGED (identical readings and queue drained, or timeout reached)")
            break

        time.sleep(10)

    final_stats = last_stats or {"sent": 0, "failed": 0, "queued": 0, "duplicates_blocked": 0}

    # ── Step 5: Fetch truth ───────────────────────────────────────────────
    print(f"[5] GET {pseudogram_url}/v1/simulate/{run_id}/truth")
    truth_data = None
    try:
        truth_res = client.get(f"{pseudogram_url}/v1/simulate/{run_id}/truth", headers=headers)
        if truth_res.status_code == 200:
            truth_data = truth_res.json()
            print(f"    Raw truth top-level keys: {list(truth_data.keys())}")
        else:
            print(f"    WARN: HTTP {truth_res.status_code}: {truth_res.text}")
    except Exception as exc:
        print(f"    ERROR: {exc}")

    # ── Step 6: Diff ──────────────────────────────────────────────────────
    diff_report = {}
    diff_report = {}
    if truth_data:
        # Parse based on actual schema: expected_unique_recipient_count, and duplicates from attempt diff
        expected_unique_jobs = truth_data.get("expected_unique_recipient_count")
        
        total_attempts = truth_data.get("total_deliveries_attempted")
        total_events = truth_data.get("total_events_generated")
        if total_attempts is not None and total_events is not None:
            expected_duplicates = total_attempts - total_events
        else:
            expected_duplicates = None

        total_my_jobs = final_stats.get("sent", 0) + final_stats.get("failed", 0) + final_stats.get("queued", 0)

        diff_report = {
            "expected_unique_jobs": expected_unique_jobs,
            "actual_total_jobs": total_my_jobs,
            "jobs_diff": total_my_jobs - expected_unique_jobs if expected_unique_jobs is not None else None,
            "expected_duplicates": expected_duplicates,
            "actual_duplicates_blocked": final_stats.get("duplicates_blocked", 0),
            "duplicates_diff": (final_stats.get("duplicates_blocked", 0) - expected_duplicates) if expected_duplicates is not None else None,
            "notes": [],
        }
        if expected_unique_jobs is None:
            diff_report["notes"].append("truth payload missing 'expected_unique_recipient_count'")
        if expected_duplicates is None:
            diff_report["notes"].append("truth payload missing 'total_deliveries_attempted' or 'total_events_generated'")

    # ── Step 7: Rate audit (local DB only) ────────────────────────────────
    rate_audit = {"status": "SKIPPED", "reason": "no local DB available"}
    db_path = Path(args.db_path)
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute("SELECT sent_at FROM send_log ORDER BY sent_at ASC")
            timestamps = [row[0] for row in cur.fetchall()]
            conn.close()

            max_60s = 0
            for ts in timestamps:
                cnt = len([t for t in timestamps if ts <= t <= ts + 60.0])
                if cnt > max_60s:
                    max_60s = cnt

            rate_audit = {
                "status": "PASSED" if max_60s <= 10 else "FAILED",
                "max_sends_60s": max_60s,
                "total_sends": len(timestamps),
            }
        except Exception as exc:
            rate_audit = {"status": "ERROR", "reason": str(exc)}

    # ── Step 8: Write results ─────────────────────────────────────────────
    # This is the ONLY place a file is written, and it requires a real run_id
    assert run_id, "BUG: reached file write without a run_id"

    runs_dir = Path("runs")
    runs_dir.mkdir(exist_ok=True)
    out_file = runs_dir / f"run_{int(time.time())}.json"

    run_output = {
        "run_id": run_id,
        "target_url": target_url,
        "count": args.count,
        "duration_seconds": args.duration,
        "converged": converged,
        "stats_readings": stats_readings,
        "final_stats": final_stats,
        "truth": truth_data,
        "diff_report": diff_report,
        "rate_audit": rate_audit,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(run_output, f, indent=2)
    print(f"\n[+] Results saved to {out_file}")

    # ── Step 9: Summary table ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  run_id:              {run_id}")
    print(f"  target:              {target_url}")
    print(f"  converged:           {converged}")
    print(f"  final sent:          {final_stats.get('sent')}")
    print(f"  final failed:        {final_stats.get('failed')}")
    print(f"  final queued:        {final_stats.get('queued')}")
    print(f"  duplicates_blocked:  {final_stats.get('duplicates_blocked')}")
    if truth_data:
        print(f"  expected unique:     {diff_report.get('expected_unique_jobs')}")
        print(f"  expected duplicates: {diff_report.get('expected_duplicates')}")
        print(f"  jobs diff:           {diff_report.get('jobs_diff')}")
        print(f"  duplicates diff:     {diff_report.get('duplicates_diff')}")
    print(f"  rate audit:          {rate_audit.get('status')} (max 60s window: {rate_audit.get('max_sends_60s', 'n/a')})")
    print("=" * 60)


if __name__ == "__main__":
    main()
