# Manual 12-image briefing review-deck gate

You are the critical final reviewer for an isolated visual lab. The user rejected
the previous handoff because its automatic three-scene chain effectively exposed
only one image, so it was impossible to judge the mosaic mechanic as a sample.
Do not inherit any earlier PASS.

Inspect every full-size PNG below, then the contact sheet, mobile PNG and WebM.
Do not infer quality or OCR safety from filenames or metadata.

## Exact committed candidate (`e308c7ed49d9`)

Directory:
`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-media-review-deck-20260717/evidence-e308/`

- `01-media_review_planet_ocean.png`
- `02-media_review_ivana_kupala.png`
- `03-media_review_region_80.png`
- `04-media_review_writing_kaliningrad.png`
- `05-media_review_swan_lake.png`
- `06-media_review_vertinsky.png`
- `07-media_review_literary_evening.png`
- `08-media_review_hay_day.png`
- `09-media_review_ship_quay.png`
- `10-media_review_admiral.png`
- `11-media_review_flight.png`
- `12-media_review_craft.png`
- `contact-sheet-12.jpg`
- `mobile-390.png`
- `manual-six-scenes.webm`
- `facts.json`

## Intended review contract

- This is a separate `review=media` mode, not the ordinary narrative queue.
- It must expose at least ten visually distinct image scenarios. Candidate has 12
  different event objects and 12 different source URLs.
- The screen must stop after each reveal. The reviewer must be able to select
  any exact state via 1–12 and use Back / Replay / Next without waiting for an
  automatic chain.
- Images may contain ordinary photographed objects, but FAIL if embedded poster
  copy, captions, large logos or signage competes with the live headline.
- Every crop must look intentional, sharp and undistorted. No face/object should
  be carelessly cut or enlarged into visible softness.
- Headline readability, fixed anchor, isolated opacity drama, physical right
  edge and the single lightweight stripe/underline contract remain required.
- The comparison rail must be understandable and secondary; it must not turn
  the hero itself into a framed engineering panel.
- Mobile is intentionally text-only but must still make all 12 review choices,
  Back / Replay / Next, categories and the beginning of the feed reachable.

## Independent hard fails

Return FAIL if any is true:

1. Fewer than ten images are genuinely different enough to evaluate the mechanic.
2. Direct 1–12 selection or Back / Replay / Next is missing/ambiguous, or the
   scenario auto-skips before a reviewer can inspect it.
3. Any shown raster contains competing embedded text/logos/signage.
4. Any raster looks soft, stretched or carelessly cropped at the exact viewport.
5. The set is visually repetitive enough that it still behaves like a one-image demo.
6. The rail dominates the page, creates an inner frame, or obscures categories/feed.
7. The old stripe/double-underline/readability defect returns in any of the 12 states.
8. Mobile downloads/shows a narrative raster, overflows, or makes the review
   controls/categories/feed inaccessible.
9. The WebM does not demonstrate manual state changes or loses the irregular
   reveal/pending horizontal cursor behavior.

## Required response

- `SAMPLE SIZE/DIVERSITY: PASS|FAIL`
- `MANUAL REVIEWABILITY: PASS|FAIL`
- `IMAGE OCR/QUALITY/CROP: PASS|FAIL`
- `TYPOGRAPHY/MOSAIC: PASS|FAIL`
- `MOBILE/MOTION: PASS|FAIL`
- `OVERALL: PASS|FAIL`
- `PUBLISH FOR USER REVIEW: YES|NO`
- List every blocker; if none, write `BLOCKERS: none`.
- Give a concise pixel-specific critique rather than repeating implementation claims.

This gate can approve only publication of the isolated lab for user review. It
cannot approve the production homepage or product desirability.
