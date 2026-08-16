#!/usr/bin/env python3
"""
Pre-submission contract check against a live deployed URL.
Catches every way the grader can score zero before a human sees anything.

Usage: python scripts/preflight.py --url https://your-app.onrender.com --api-key YOUR_KEY
"""
import sys
import time
import json
import hmac
import hashlib
import argparse
import httpx


def sign(body_bytes: bytes, api_key: str) -> str:
    """HMAC-SHA256 over exact body bytes. Never re-serialise."""
    digest = hmac.new(api_key.encode("utf-8"), msg=body_bytes, digestmod=hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def main():
    parser = argparse.ArgumentParser(description="Pre-submission contract check")
    parser.add_argument("--url", required=True, help="Deployed app base URL")
    parser.add_argument("--api-key", required=True, help="Pseudogram API key (HMAC secret)")
    args = parser.parse_args()

    base = args.url.rstrip("/")
    api_key = args.api_key
    client = httpx.Client(timeout=10.0)
    failures = []
    check_num = 0

    def check(name, passed, detail=""):
        nonlocal check_num
        check_num += 1
        status = "PASS" if passed else "FAIL"
        line = f"  [{check_num:2d}] {status}: {name}"
        if detail:
            line += f" — {detail}"
        print(line)
        if not passed:
            failures.append(f"#{check_num} {name}: {detail}")

    # ── 11. Cold-start probe (run first, print latency separately) ────────
    print(f"\nPreflight against {base}\n")
    cold_start = time.monotonic()
    try:
        cold_res = client.get(f"{base}/healthz")
        cold_latency = time.monotonic() - cold_start
        print(f"  Cold-start probe: {cold_latency*1000:.0f}ms (GET /healthz → {cold_res.status_code})\n")
    except Exception as exc:
        cold_latency = time.monotonic() - cold_start
        print(f"  Cold-start probe: UNREACHABLE after {cold_latency*1000:.0f}ms — {exc}\n")
        print("\nFATAL: target unreachable. Cannot continue.")
        sys.exit(1)

    # ── 1. GET /healthz → 200 ─────────────────────────────────────────────
    res = client.get(f"{base}/healthz")
    check("GET /healthz → 200", res.status_code == 200, f"got {res.status_code}")

    # ── 2. POST /rules → 201 with exact shape ────────────────────────────
    res = client.post(f"{base}/rules", json={"keyword": "PREFLIGHT_TEST", "dm_message": "x"})
    keys_ok = set(res.json().keys()) == {"rule_id", "keyword", "dm_message"} if res.status_code == 201 else False
    kw_echo = res.json().get("keyword") == "PREFLIGHT_TEST" if res.status_code == 201 else False
    check("POST /rules → 201, exact keys, keyword echoed",
          res.status_code == 201 and keys_ok and kw_echo,
          f"status={res.status_code}, keys={set(res.json().keys()) if res.status_code == 201 else 'n/a'}, keyword={res.json().get('keyword') if res.status_code == 201 else 'n/a'}")

    # ── 3. POST /rules/ (trailing slash) → 201 ───────────────────────────
    res = client.post(f"{base}/rules/", json={"keyword": "PREFLIGHT_SLASH", "dm_message": "y"})
    check("POST /rules/ (trailing slash) → 201", res.status_code == 201, f"got {res.status_code}")

    # ── 4. GET /stats → 200, exact keys, all int ─────────────────────────
    res = client.get(f"{base}/stats")
    stats_ok = False
    detail = f"status={res.status_code}"
    if res.status_code == 200:
        data = res.json()
        expected_keys = {"sent", "failed", "queued", "duplicates_blocked"}
        keys_match = set(data.keys()) == expected_keys
        all_int = all(isinstance(data.get(k), int) for k in expected_keys)
        stats_ok = keys_match and all_int
        detail = f"keys={set(data.keys())}, all_int={all_int}"
    check("GET /stats → 200, exact 4 int keys", stats_ok, detail)

    # ── 5. GET /stats/ → 200, same shape ─────────────────────────────────
    res = client.get(f"{base}/stats/")
    stats_slash_ok = False
    if res.status_code == 200:
        data = res.json()
        stats_slash_ok = set(data.keys()) == {"sent", "failed", "queued", "duplicates_blocked"}
    check("GET /stats/ (trailing slash) → 200", stats_slash_ok, f"status={res.status_code}")

    # ── Build a signed webhook body (serialise ONCE) ──────────────────────
    event_payload = {
        "event_id": f"evt_preflight_{int(time.time())}",
        "event_type": "comment.created",
        "data": {
            "comment_id": f"cmt_pf_{int(time.time())}",
            "text": "PREFLIGHT_TEST check",
            "from": {"user_id": f"usr_pf_{int(time.time())}", "username": "preflight_user"}
        }
    }
    body_bytes = json.dumps(event_payload).encode("utf-8")
    valid_sig = sign(body_bytes, api_key)

    # ── 6. POST /webhook signed → 200, latency < 2s ──────────────────────
    t0 = time.monotonic()
    res = client.post(f"{base}/webhook", content=body_bytes,
                      headers={"X-PseudoGram-Signature": valid_sig, "Content-Type": "application/json"})
    latency = time.monotonic() - t0
    check("POST /webhook (signed) → 200, latency < 2.0s",
          res.status_code == 200 and latency < 2.0,
          f"status={res.status_code}, latency={latency*1000:.0f}ms")

    # ── 7. POST /webhook corrupted signature → 401 ───────────────────────
    bad_sig = "sha256=" + "0" * 64
    res = client.post(f"{base}/webhook", content=body_bytes,
                      headers={"X-PseudoGram-Signature": bad_sig, "Content-Type": "application/json"})
    check("POST /webhook (bad signature) → 401",
          res.status_code == 401,
          f"got {res.status_code} — if this fails, signature verification may be misconfigured and ALL graded events will be rejected")

    # ── 8. POST /webhook/ trailing slash, signed → 200 ───────────────────
    slash_payload = {
        "event_id": f"evt_pf_slash_{int(time.time())}",
        "event_type": "comment.created",
        "data": {
            "comment_id": f"cmt_pfs_{int(time.time())}",
            "text": "slash test",
            "from": {"user_id": f"usr_pfs_{int(time.time())}", "username": "pf_slash"}
        }
    }
    slash_bytes = json.dumps(slash_payload).encode("utf-8")
    slash_sig = sign(slash_bytes, api_key)
    res = client.post(f"{base}/webhook/", content=slash_bytes,
                      headers={"X-PseudoGram-Signature": slash_sig, "Content-Type": "application/json"})
    check("POST /webhook/ (trailing slash, signed) → 200", res.status_code == 200, f"got {res.status_code}")

    # ── 9. POST /webhook garbage text/plain → 200 ────────────────────────
    garbage_bytes = b"not json at all"
    garbage_sig = sign(garbage_bytes, api_key)
    res = client.post(f"{base}/webhook", content=garbage_bytes,
                      headers={"X-PseudoGram-Signature": garbage_sig, "Content-Type": "text/plain"})
    check("POST /webhook (text/plain garbage) → 200", res.status_code == 200, f"got {res.status_code}")

    # ── 10. Duplicate event → duplicates_blocked increases ────────────────
    # Read baseline
    res_before = client.get(f"{base}/stats")
    dup_before = res_before.json().get("duplicates_blocked", 0) if res_before.status_code == 200 else 0

    # Send the same signed event from check 6 again (same event_id = redelivery)
    client.post(f"{base}/webhook", content=body_bytes,
                headers={"X-PseudoGram-Signature": valid_sig, "Content-Type": "application/json"})

    # Poll /stats for up to 30s waiting for duplicates_blocked to increase
    dup_increased = False
    poll_start = time.monotonic()
    while time.monotonic() - poll_start < 30:
        res_after = client.get(f"{base}/stats")
        if res_after.status_code == 200:
            dup_after = res_after.json().get("duplicates_blocked", 0)
            if dup_after > dup_before:
                dup_increased = True
                break
        time.sleep(2)

    check("Duplicate event → duplicates_blocked increased",
          dup_increased,
          f"before={dup_before}, after={dup_after if 'dup_after' in dir() else '?'}")

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    if failures:
        print(f"PREFLIGHT FAILED — {len(failures)} check(s):")
        for f in failures:
            print(f"  ✗ {f}")
        print(f"{'='*60}")
        sys.exit(1)
    else:
        print(f"PREFLIGHT PASSED — all {check_num} checks OK")
        print(f"{'='*60}")
        sys.exit(0)


if __name__ == "__main__":
    main()
