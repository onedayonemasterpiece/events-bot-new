# Blind-first stripe / media-quality corrective gate

You are the final critical visual reviewer for an isolated desktop/mobile lab,
not an advocate for the implementation. The previous reviewer passed pixels
that the user correctly rejected. Do not inherit any prior verdict.

First open and inspect every PNG and the WebM below at full size. Do not infer
visual quality from filenames or from implementation claims.

## Rejected baseline

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-90e0/rejected-live-1440.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-90e0/rejected-weather-1440.png`

## Corrective candidate

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/weather-water-1440.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/forwarded-1920.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/named-abstains-1440.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/rare-abstains-1440.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/storm-abstains-1366.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/mobile-390.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-stripe-media-20260717/evidence-c5df/desktop-1366-stripe-ocr-safe.webm`

After the visual pass, reconcile observations against the captured browser
facts in `evidence-c5df/geometry.json`. If a local image/video cannot be opened,
return `BLOCKED: media not inspected`; do not infer from a filename. Do not call
the physical-right-edge criterion failed when the inspected raster reaches the
last screenshot pixel and `mediaRect.right == innerWidth`.

## Independent hard-fail criteria

Return FAIL if **any** item is true:

1. Paper treatment still reads as opaque rectangular slabs, overlaps adjacent
   fragments/lines, or creates a second underline/band that competes with the
   actual event-link underline.
2. Removing those slabs made the headline hard to read at first glance over
   the image.
3. Any candidate hero that shows a raster also shows embedded poster/OCR copy,
   logos, captions or signage that competes with the live headline. Interface
   navigation and the live headline itself are not raster OCR.
4. Any shown raster looks stretched, visibly soft/pixelated, or carelessly
   cropped at 100% review scale.
5. Mosaic alpha still reads as a smooth left-to-right fade or checkerboard,
   rather than a broad directional field with isolated bright and washed cells.
6. The text anchor moves between media and text-only candidates, media creates
   an inner frame/seam, or the image stops before the physical right edge.
7. Mobile requests/shows the narrative raster, overflows horizontally, or hides
   categories/feed context.
8. The WebM does not show irregular reveal/exit, retains the raster after the
   scene should leave, or loses the pending horizontal cursor **after the
   current sentence is complete and while the next scenario timer is active**.
   A vertical/bar cursor during fragment-by-fragment sentence formation is an
   intentional scenario variant and is not itself a failure.

It is intentionally acceptable for named/rare/storm scenes to abstain and be
text-only. Do not penalize fewer images: safety and source quality outrank the
old majority-coverage target.

## Required response

- One compact visual comparison of rejected vs candidate.
- `R01 STRIPE: PASS|FAIL` with pixel-specific reason.
- `R02 QUALITY/CROP: PASS|FAIL`.
- `R03 TILE DRAMA: PASS|FAIL`.
- `R04 OCR/ABSTENTION: PASS|FAIL`.
- `MOBILE/MOTION: PASS|FAIL`.
- `OVERALL: PASS|FAIL`.
- `PUBLISH FOR USER REVIEW: YES|NO`.
- List every remaining blocker. If none, write `BLOCKERS: none`.

This can approve only publication of the isolated lab for user review. It is
not production-home approval or product desirability evidence.
