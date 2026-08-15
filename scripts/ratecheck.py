#!/usr/bin/env python3
"""
Rate Limiter Audit Script
Verifies that no rolling 60-second window in send_log exceeded the maximum threshold (10).
"""
import sys
import sqlite3
from pathlib import Path


def audit_rate_limiter(db_path: str = "data/app.db", max_allowed: int = 10):
    if not Path(db_path).exists():
        print(f"[!] Database file '{db_path}' not found.")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT sent_at FROM send_log ORDER BY sent_at ASC")
    timestamps = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"[*] Auditing {len(timestamps)} send_log entries in '{db_path}'...")

    max_in_any_window = 0
    violation_count = 0

    for i, ts in enumerate(timestamps):
        window_start = ts
        window_end = ts + 60.0

        # Count timestamps within [ts, ts + 60.0]
        in_window = [t for t in timestamps if window_start <= t <= window_end]
        count = len(in_window)

        if count > max_in_any_window:
            max_in_any_window = count

        if count > max_allowed:
            violation_count += 1
            print(f"[ERROR] Window starting at t={ts:.2f} s contains {count} requests (Max allowed: {max_allowed})")

    print(f"[*] Peak requests in any rolling 60s window: {max_in_any_window}")

    if violation_count == 0 and max_in_any_window <= max_allowed:
        print(f"[SUCCESS] Rate limit check PASSED! Max rolling 60s window requests: {max_in_any_window} <= {max_allowed}")
        return True
    else:
        print(f"[FAIL] Rate limit check FAILED! Found {violation_count} violating window intervals.")
        return False


if __name__ == "__main__":
    db_file = sys.argv[1] if len(sys.argv) > 1 else "data/app.db"
    success = audit_rate_limiter(db_file)
    sys.exit(0 if success else 1)
