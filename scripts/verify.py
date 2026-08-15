#!/usr/bin/env python3
"""
End-to-End Simulation & Truth Verification Script
Registers rules, triggers POST /v1/simulate/start, polls /stats until stable,
fetches /v1/simulate/{run_id}/truth, performs rate limiting audit, and outputs
detailed results to runs/run_<timestamp>.json.
"""
import sys
import time
import json
import sqlite3
import httpx
from pathlib import Path
from scripts.ratecheck import audit_rate_limiter


def run_verification(target_url: str, pseudogram_url: str, api_key: str, db_path: str = "data/app.db"):
    target_url = target_url.rstrip("/")
    pseudogram_url = pseudogram_url.rstrip("/")

    client = httpx.Client(timeout=15.0)

    print(f"[*] Step 1: Health check on target URL {target_url}...")
    try:
        res = client.get(f"{target_url}/healthz")
        if res.status_code != 200:
            print(f"[!] Health check failed with HTTP {res.status_code}")
            sys.exit(1)
        print("[+] Health check OK.")
    except Exception as exc:
        print(f"[!] Target URL unreachable: {exc}")
        sys.exit(1)

    print("[*] Step 2: Registering automation rules...")
    rules = [
        {"keyword": "PRICE", "dm_message": "Here's the price list!"},
        {"keyword": "DISCOUNT", "dm_message": "Use code SAVE20 for 20% off!"},
        {"keyword": "LINK", "dm_message": "Check out the link in bio!"}
    ]
    created_rules = []
    for r in rules:
        res = client.post(f"{target_url}/rules", json=r)
        if res.status_code in (200, 201):
            rule_data = res.json()
            created_rules.append(rule_data)
            print(f"  - Created rule {rule_data.get('rule_id')} for keyword '{r['keyword']}'")
        else:
            print(f"[!] Failed to create rule: HTTP {res.status_code} {res.text}")

    print(f"[*] Step 3: Starting simulation on {pseudogram_url}/v1/simulate/start...")
    sim_payload = {
        "webhook_url": f"{target_url}/webhook",
        "count": 500,
        "duration_seconds": 10
    }
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    run_id = None
    try:
        sim_res = client.post(f"{pseudogram_url}/v1/simulate/start", json=sim_payload, headers=headers)
        if sim_res.status_code in (200, 201, 202):
            sim_data = sim_res.json()
            run_id = sim_data.get("run_id")
            print(f"[+] Simulation started successfully! Run ID: {run_id}")
        else:
            print(f"[!] Simulation start returned HTTP {sim_res.status_code}: {sim_res.text}")
    except Exception as exc:
        print(f"[!] Simulation start request failed: {exc}")

    print("[*] Step 4: Polling /stats until stable (3 consecutive identical reads)...")
    last_stats = None
    identical_count = 0
    start_poll = time.time()

    while True:
        try:
            stats_res = client.get(f"{target_url}/stats")
            if stats_res.status_code == 200:
                current_stats = stats_res.json()
                print(f"  - Stats: {current_stats} (elapsed {time.time() - start_poll:.1f}s)")
                if current_stats == last_stats:
                    identical_count += 1
                else:
                    identical_count = 0
                    last_stats = current_stats

                if identical_count >= 3:
                    print("[+] Stats stabilized!")
                    break
        except Exception as exc:
            print(f"  - Error reading /stats: {exc}")

        if time.time() - start_poll > 300:
            print("[!] Polling timed out after 300 seconds.")
            break

        time.sleep(5)

    print("\n==================================================")
    print("FINAL APP STATS OVERVIEW")
    print("==================================================")
    print(json.dumps(last_stats, indent=2))

    truth_data = None
    diff_report = {}

    if run_id:
        print(f"\n[*] Step 5: Fetching truth data from {pseudogram_url}/v1/simulate/{run_id}/truth...")
        try:
            truth_res = client.get(f"{pseudogram_url}/v1/simulate/{run_id}/truth", headers=headers)
            if truth_res.status_code == 200:
                truth_data = truth_res.json()

                total_my_jobs = (last_stats.get("sent", 0) +
                                 last_stats.get("failed", 0) +
                                 last_stats.get("queued", 0))

                expected_unique_jobs = truth_data.get("expected_unique_jobs", truth_data.get("expected_sent", 0))
                expected_duplicates = truth_data.get("expected_duplicates", 0)

                diff_report = {
                    "expected_unique_jobs": expected_unique_jobs,
                    "actual_total_jobs": total_my_jobs,
                    "jobs_diff": total_my_jobs - expected_unique_jobs,
                    "expected_duplicates_redelivered": expected_duplicates,
                    "actual_duplicates_blocked": last_stats.get("duplicates_blocked", 0),
                    "duplicates_diff": last_stats.get("duplicates_blocked", 0) - expected_duplicates,
                    "missing_users": []
                }

                # Check for truth users missing job rows in local DB if DB file exists
                if Path(db_path).exists() and isinstance(truth_data.get("expected_recipients"), list):
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()
                    cur.execute("SELECT DISTINCT recipient_user_id FROM dm_jobs")
                    local_users = {row[0] for row in cur.fetchall()}
                    conn.close()

                    expected_recipients = set(truth_data.get("expected_recipients", []))
                    missing = list(expected_recipients - local_users)
                    diff_report["missing_users"] = missing

                print("\n==================================================")
                print("TRUTH VS ACTUAL COMPARISON")
                print("==================================================")
                print(f"Expected Unique DMs:     {expected_unique_jobs}")
                print(f"Actual Sent (delivered): {last_stats.get('sent')}")
                print(f"Actual Failed:           {last_stats.get('failed')}")
                print(f"Actual Queued:           {last_stats.get('queued')}")
                print(f"Actual Total Jobs:       {total_my_jobs}")
                print(f"Expected Duplicates:     {expected_duplicates}")
                print(f"Actual Duplicates Blkd:  {last_stats.get('duplicates_blocked')}")
                print(f"Missing Recipients:      {len(diff_report['missing_users'])}")

            else:
                print(f"[!] Truth endpoint returned HTTP {truth_res.status_code}: {truth_res.text}")
        except Exception as exc:
            print(f"[!] Error fetching truth data: {exc}")

    # Rate check audit
    rate_limiter_audit = {"status": "NOT_CHECKED", "max_sends_60s": 0}
    if Path(db_path).exists():
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT sent_at FROM send_log ORDER BY sent_at ASC")
        timestamps = [row[0] for row in cur.fetchall()]
        conn.close()

        max_60s = 0
        for ts in timestamps:
            cnt = len([t for t in timestamps if ts <= t <= ts + 60.0])
            if cnt > max_60s:
                max_60s = cnt

        rate_limiter_audit = {
            "status": "PASSED" if max_60s <= 10 else "FAILED",
            "max_sends_60s": max_60s,
            "max_allowed": 10
        }
        print(f"\n[*] Rate Limiter Audit: Peak requests in 60s window = {max_60s} (Max allowed: 10)")
        assert max_60s <= 10, f"Rate limit violated! Peak 60s window: {max_60s}"

    # Save run results to runs/ directory
    runs_dir = Path("runs")
    runs_dir.mkdir(exist_ok=True)
    out_file = runs_dir / f"run_{int(time.time())}.json"
    run_output = {
        "timestamp": time.time(),
        "target_url": target_url,
        "run_id": run_id,
        "stats": last_stats,
        "truth": truth_data,
        "diff_report": diff_report,
        "rate_limiter_audit": rate_limiter_audit
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(run_output, f, indent=2)
    print(f"\n[+] Full run evidence saved to {out_file}")


if __name__ == "__main__":
    t_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    p_url = sys.argv[2] if len(sys.argv) > 2 else "https://pseudogram-api.onrender.com"
    key = sys.argv[3] if len(sys.argv) > 3 else "test_api_key"

    run_verification(t_url, p_url, key)
