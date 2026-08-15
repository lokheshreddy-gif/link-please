#!/usr/bin/env python3
"""
Webhook Signature Verification Check Script
Verifies that valid HMAC-SHA256 signatures over raw body bytes return 200 OK,
and invalid signatures return 401 Unauthorized.
"""
import sys
import hmac
import json
import hashlib
import httpx


def test_signature_verification(base_url: str = "http://localhost:8000", api_key: str = "test_api_key"):
    base_url = base_url.rstrip("/")
    client = httpx.Client(timeout=10.0)

    payload = {
        "event_id": f"evt_sigcheck_{int(httpx.__name__.__len__())}",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_sig_1",
            "text": "Checking signature verification",
            "from": {"user_id": "usr_sig_1", "username": "sig_user"}
        }
    }

    # Serialize body ONCE to exact bytes and sign those exact bytes
    raw_bytes = json.dumps(payload).encode("utf-8")

    secret_bytes = api_key.encode("utf-8")
    valid_digest = hmac.new(secret_bytes, msg=raw_bytes, digestmod=hashlib.sha256).hexdigest()
    valid_signature_hdr = f"sha256={valid_digest}"

    print(f"[*] Testing valid signature POST to {base_url}/webhook...")
    res_valid = client.post(
        f"{base_url}/webhook",
        content=raw_bytes,
        headers={"X-PseudoGram-Signature": valid_signature_hdr, "Content-Type": "application/json"}
    )
    print(f"  - Response status: {res_valid.status_code}")
    assert res_valid.status_code == 200, f"Expected 200, got {res_valid.status_code}"

    print("[*] Testing corrupted signature POST to webhook...")
    corrupted_signature_hdr = "sha256=0000000000000000000000000000000000000000000000000000000000000000"
    res_invalid = client.post(
        f"{base_url}/webhook",
        content=raw_bytes,
        headers={"X-PseudoGram-Signature": corrupted_signature_hdr, "Content-Type": "application/json"}
    )
    print(f"  - Response status: {res_invalid.status_code}")
    assert res_invalid.status_code == 401, f"Expected 401, got {res_invalid.status_code}"

    print("[SUCCESS] Webhook signature verification check PASSED!")
    return True


if __name__ == "__main__":
    b_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    key = sys.argv[2] if len(sys.argv) > 2 else "test_api_key"
    success = test_signature_verification(b_url, key)
    sys.exit(0 if success else 1)
