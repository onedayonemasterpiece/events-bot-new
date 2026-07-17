# Manual 12-image review deck — corrective recheck

The first exact review correctly returned OVERALL FAIL for two blockers:

1. live copy lost contrast over dark mosaic cells in states 04, 05 and 07;
2. mobile clipped review choices 10–12.

Do not inherit a PASS. Inspect the final committed candidate at full size and
confirm whether those blockers are actually closed without returning opaque
paper slabs or weakening the sample.

## Rejected evidence (`e308c7ed49d9`)

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-e308/04-media_review_writing_kaliningrad.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-e308/05-media_review_swan_lake.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-e308/07-media_review_literary_evening.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-e308/mobile-390.png`

## Final exact candidate (`38425f28bd15`)

Inspect all twelve full-size PNGs, `contact-sheet-12.jpg`, `mobile-390.png`,
`manual-six-scenes.webm` and `facts.json` in:

`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-3842/`

The intended correction caps only mosaic cells geometrically intersecting live
fragment line boxes at `.24` opacity. It must protect letters without creating
rectangular paper slabs; non-intersecting cells retain the irregular mosaic.
On mobile the 1–12 rail must wrap so all twelve direct choices are visible and
reachable without horizontal scrolling.

Retain every independent hard fail from the first gate: at least ten genuinely
different images, direct selection/Back/Replay/Next, no auto-skip, no competing
raster OCR, sharp intentional cover crop, no repetitive one-image demo, no
inner engineering frame, no old stripe/double-underline defect, text-only
mobile with categories/feed, and meaningful manual motion/cursor evidence.

Required response:

- `BLOCKER 1 COPY CONTRAST: CLOSED|OPEN`
- `BLOCKER 2 MOBILE 1–12: CLOSED|OPEN`
- `SAMPLE SIZE/DIVERSITY: PASS|FAIL`
- `MANUAL REVIEWABILITY: PASS|FAIL`
- `IMAGE OCR/QUALITY/CROP: PASS|FAIL`
- `TYPOGRAPHY/MOSAIC: PASS|FAIL`
- `MOBILE/MOTION: PASS|FAIL`
- `OVERALL: PASS|FAIL`
- `PUBLISH FOR USER REVIEW: YES|NO`
- `BLOCKERS: none` or list every blocker.

This can approve only the isolated lab for user review, not production.
