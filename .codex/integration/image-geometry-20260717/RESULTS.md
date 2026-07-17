# Image geometry rollout results

Date: 2026-07-17

## Delivery

- Feature and follow-up queue-starvation fix are reachable from `origin/main`
  at merge SHA `8e0ebabcd8577563c5b264f62016fcb3484d49dd`.
- PRs: `#62` (feature) and `#63` (production-smoke regression fix); both CI
  runs passed.
- Fly machine `2860d45f312248`, release `1690`, image
  `deployment-01KXRRM020EQ2VFGRDQQEC2H1R`; `/healthz` ready with no issues.

## Normal worker smoke

- A real durable `event_media_review` job processed the three approved images
  for event `698` while the independent semantic-role budget was exhausted.
- Provider reservations rotated `GOOGLE_API_KEY4 → GOOGLE_API_KEY5 →
  GOOGLE_API_KEY4`; all three `gemma-4-31b-it` calls succeeded on attempt one.
- Production rows for posters `11`, `12`, `13` were classified and visually
  inspected. Face boxes and viewer-value regions were correctly aligned.
- Overlay: `artifacts/codex/image-geometry-20260717/production-smoke/event-698-production-smoke.jpg`.

## Backfill and import

- External paced backfill used exactly `400/400` provider reservations, with
  six-second-plus jitter spacing and no `429` responses.
- JSONL contains `401` successful poster results (381 distinct pixel hashes)
  plus two terminal source `404` records. All 21 final contact sheets were
  inspected; no systematic coordinate error was found.
- Production dry-run: `input_successes=401`, `valid=401`, `stale=0`.
- Production apply: `401` poster links, `381` classified cache rows, `0`
  invalid boxes; backup at
  `/data/backups/image-geometry-import-20260717.json`.
- SQLite `quick_check=ok`; the pre-existing 195 unrelated foreign-key
  violations remained exactly 195 after import.

## Validation

- Focused suite: 57 tests collected and passed.
- Broader pre-release suite: 75 passed; a wider 95-test run reproduced one
  unrelated baseline failure on clean `origin/main`.
- Production file-mirror logs show three `reserve_normal_pool_used` and three
  `call_ok` records for `smart_update_image_geometry`, with no geometry warning,
  provider error or rate-limit block.
