#!/usr/bin/env python3
"""
Read the most recent runs/run_*.json and print replacement sentences
for each TODO(real-run) marker in FAILURES.md.

Output is for the user to paste by hand. This script never writes to FAILURES.md.
"""
import sys
import json
import glob
from pathlib import Path


def main():
    run_files = sorted(glob.glob("runs/run_*.json"))
    if not run_files:
        print("NO RUN FILE — run scripts/verify.py first")
        sys.exit(1)

    latest = run_files[-1]
    print(f"Reading: {latest}\n")
    with open(latest, "r", encoding="utf-8") as f:
        run = json.load(f)

    truth = run.get("truth") or {}
    final = run.get("final_stats") or {}
    readings = run.get("stats_readings") or []
    count = run.get("count")
    converged = run.get("converged")

    print("=" * 70)
    print("REPLACEMENT SENTENCES FOR FAILURES.md TODO(real-run) MARKERS")
    print("=" * 70)

    # ── §1: events dropped ───────────────────────────────────────────────
    print("\n### §1 — Lock contention (events dropped)")
    if count is not None:
        total_jobs = final.get("sent", 0) + final.get("failed", 0) + final.get("queued", 0)
        # We can't know COUNT(*) FROM events without DB access, but we can
        # compare expected unique jobs vs actual total jobs
        expected_unique = truth.get("expected_unique_jobs") or truth.get("expected_sent")
        if expected_unique is not None:
            diff = total_jobs - expected_unique
            print(f"In the {count}-event run (run_id={run.get('run_id')}), the grader expected "
                  f"{expected_unique} unique DM jobs. The application created {total_jobs} "
                  f"(sent={final.get('sent')}, failed={final.get('failed')}, queued={final.get('queued')}), "
                  f"a difference of {diff:+d}. "
                  f"{'No events appear to have been dropped.' if diff >= 0 else f'{abs(diff)} event(s) were likely lost to lock contention or other errors.'}")
        else:
            print("UNAVAILABLE: expected_sent / expected_unique_jobs not in truth response")
    else:
        print("UNAVAILABLE: 'count' field not in run file")

    # ── §3: duplicates_blocked vs truth ──────────────────────────────────
    print("\n### §3 — duplicates_blocked divergence")
    expected_dups = truth.get("expected_duplicates")
    actual_dups = final.get("duplicates_blocked", 0)
    if expected_dups is not None:
        divergence = actual_dups - expected_dups
        print(f"The grader truth reports {expected_dups} expected duplicate events. "
              f"The application reported duplicates_blocked={actual_dups}, "
              f"a divergence of {divergence:+d}.")
    else:
        print("UNAVAILABLE: expected_duplicates not in truth response")

    # ── §4: queue drain time ─────────────────────────────────────────────
    print("\n### §4 — Queue drain time")
    if readings and len(readings) >= 2:
        first_ts = readings[0].get("timestamp")
        last_ts = readings[-1].get("timestamp")
        if first_ts is not None and last_ts is not None:
            drain_seconds = last_ts - first_ts
            drain_minutes = drain_seconds / 60.0
            converge_note = "Stats converged." if converged else "Stats did NOT converge within the polling window."
            print(f"Stats polling ran from the first reading to convergence (or timeout) "
                  f"over {drain_seconds:.0f} seconds ({drain_minutes:.1f} minutes) "
                  f"across {len(readings)} readings. {converge_note}")
        else:
            print("UNAVAILABLE: stats_readings entries missing 'timestamp' field")
    else:
        print("UNAVAILABLE: insufficient stats_readings in run file")

    # ── §5: restart survival ─────────────────────────────────────────────
    print("\n### §5 — Restart survival (production)")
    print("This marker requires manually restarting the Render service during or after "
          "a run and checking whether /stats values are preserved. The run file does not "
          "contain this information. Observe manually and paste the result.")

    # ── §7: comment.deleted count ────────────────────────────────────────
    print("\n### §7 — comment.deleted events")
    # The truth payload may or may not include this. Check common field names.
    deleted_count = truth.get("comment_deleted_count") or truth.get("deleted_events") or truth.get("expected_deleted")
    if deleted_count is not None:
        print(f"The grader truth reports {deleted_count} comment.deleted events.")
        print("Whether any arrived after their DM was already accepted cannot be determined "
              "from the run file alone — it requires querying dm_jobs for jobs with "
              "status != 'pending' whose comment_id matches a deleted comment.")
    else:
        print("UNAVAILABLE: comment.deleted count not in truth response. "
              "Check the truth payload's top-level keys printed during the run "
              "and query the database directly if needed:\n"
              "  SELECT COUNT(*) FROM events WHERE event_type = 'comment.deleted';\n"
              "  SELECT COUNT(*) FROM dm_jobs j JOIN comments c ON j.comment_id = c.comment_id "
              "WHERE c.deleted = 1 AND j.status IN ('accepted', 'delivered');")

    print("\n" + "=" * 70)
    print("Paste the above sentences into FAILURES.md, replacing the TODO markers.")
    print("Do NOT paste them without reading them first.")
    print("=" * 70)


if __name__ == "__main__":
    main()
