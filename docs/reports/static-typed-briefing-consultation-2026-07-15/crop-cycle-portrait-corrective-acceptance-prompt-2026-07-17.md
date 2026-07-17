# Corrective crop / sequence acceptance gate after explicit FAIL

You previously returned FAIL on R01 because the WebM showed new live copy over
old media, and FAIL on R02 because a group portrait cut heads at the upper crop
boundary. Those are hard blockers. Do not inherit a PASS from implementation
claims.

Re-open and inspect these **updated files** at full size:

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_writing_kaliningrad.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_single_portrait.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_portrait_collage.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/mobile-review-controls.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media-review-sequential.webm`

The corrective lifecycle contract is precise:
- the old copy and old image may persist together while the next exact media is
  preloaded and decoded;
- only then must the new `{copy, CTA, media}` state commit;
- a manually selected final state must retain its image indefinitely;
- no new headline may type over the prior image and no terminal image may leave
  an empty hero.

The group portrait is now a source-faithful contained contiguous right cluster,
not panoramic cover. Fail R02 if either visible head is cut at the source/crop
boundary.

Apply all independent criteria from:
`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/docs/reports/static-typed-briefing-consultation-2026-07-15/crop-cycle-portrait-acceptance-prompt-2026-07-17.md`.
If any media cannot be opened, return BLOCKED instead of inferring.

Required compact response:
- `R01 LIFECYCLE/SEQUENCE: PASS|FAIL`
- `R02 FACE-SAFE CROP: PASS|FAIL`
- `R03 SINGLE PORTRAIT: PASS|FAIL`
- `R04 MULTI-PORTRAIT: PASS|FAIL`
- `R05 TYPOGRAPHY/OCR: PASS|FAIL`
- `R06 MOBILE: PASS|FAIL`
- `R07 MOTION: PASS|FAIL`
- `OVERALL: PASS|FAIL`
- `PUBLISH ISOLATED LAB FOR USER REVIEW: YES|NO`
- `BLOCKERS: ...`

This does not approve production or prove desirability.
