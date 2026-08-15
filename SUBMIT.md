# SUBMIT.md — Submission Checklist

## Submission JSON Body

```json
{
  "email": "<YOUR_EMAIL>",
  "github_repo": "https://github.com/lokheshreddy-gif/link-please",
  "working_url": "<YOUR_RENDER_URL>",
  "loom_url": "<YOUR_LOOM_VIDEO_URL>",
  "parts_completed": "A+B+C",
  "start_date": "<YYYY-MM-DD>"
}
```

## Submission Command

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/submit \
  -H "X-API-Key: <YOUR_PSEUDOGRAM_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "<YOUR_EMAIL>",
    "github_repo": "https://github.com/lokheshreddy-gif/link-please",
    "working_url": "<YOUR_RENDER_URL>",
    "loom_url": "<YOUR_LOOM_VIDEO_URL>",
    "parts_completed": "A+B+C",
    "start_date": "<YYYY-MM-DD>"
  }'
```

## Pre-Submission Checklist

- [ ] Deploy to Render and verify `/healthz` returns 200
- [ ] Set `PSEUDOGRAM_API_KEY` and `ENABLE_SIGNATURE_VERIFICATION=true` in Render env vars
- [ ] Run the real 500-event simulation:
  ```bash
  PYTHONPATH=. python scripts/verify.py --url <YOUR_RENDER_URL> --api-key <YOUR_API_KEY>
  ```
- [ ] Fill TODOs in `FAILURES.md` with real numbers from `runs/run_*.json`
- [ ] Commit the `runs/run_*.json` output file
- [ ] Record 3-minute Loom video (see `LOOM_NOTES.md` for outline)
- [ ] Submit via the curl command above
