#!/usr/bin/env python3
"""
End-to-End Simulation & Truth Verification Script
Registers rules, triggers POST /v1/simulate/start, polls /stats until stable,
fetches /v1/simulate/{run_id}/truth, and prints detailed diff table.
"""
import sys
import time
import json
import httpx
from pathlib import Path


def run_verification(target_url: str, pseudogram_url: str, api_key: str):
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

        if time.time() - start_poll > 300: # 5 minute cutoff max
            print("[!] Polling timed out after 300 seconds.")
            break

        time.sleep(5)

    print("\n==================================================")
    print("FINAL APP STATS OVERVIEW")
    print("==================================================")
    print(json.dumps(last_stats, indent=2))

    if run_id:
        print(f"\n[*] Step 5: Fetching truth data from {pseudogram_url}/v1/simulate/{run_id}/truth...")
        try:
            truth_res = client.get(f"{pseudogram_url}/v1/simulate/{run_id}/truth", headers=headers)
            if truth_res.status_code == 200:
                truth_data = truth_res.json()
                print("\n==================================================")
                print("TRUTH VS ACTUAL COMPARISON")
                print("==================================================")
                print(f"Expected Unique DMs:     {truth_data.get('expected_sent')}")
                print(f"Actual Sent (delivered): {last_stats.get('sent')}")
                print(f"Actual Failed:           {last_stats.get('failed')}")
                print(f"Actual Queued:           {last_stats.get('queued')}")
                print(f"Expected Duplicates:     {truth_data.get('expected_duplicates')}")
                print(f"Actual Duplicates Blkd:  {last_stats.get('duplicates_blocked')}")
            else:
                print(f"[!] Truth endpoint returned HTTP {truth_res.status_code}: {truth_res.text}")
        except Exception as exc:
            print(f"[!] Error fetching truth data: {exc}")

    # Save run results to runs/ directory
    runs_dir = Path("runs")
    runs_dir.mkdir(exist_ok=True)
    out_file = runs_dir / f"run_{int(time.time())}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"target_url": target_url, "run_id": run_id, "stats": last_stats}, f, indent=2)
    print(f"\n[+] Results saved to {out_file}")


if __name__ == "__main__":
    t_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    p_url = sys.argv[2] if len(sys.argv) > 2 else "https://pseudogram-api.onrender.com"
    key = sys.argv[3] if len(sys.argv) > 3 else "test_api_key"

    run_verification(t_url, p_url, key)
