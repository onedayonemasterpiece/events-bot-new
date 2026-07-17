# Final clarified crop / sequence acceptance gate

This is a third blind-first gate after two valid FAIL rounds. Re-open the latest
files; they have been regenerated after the second verdict:

- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_writing_kaliningrad.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_single_portrait.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media_review_portrait_collage.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/mobile-review-controls.png`
- `/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration/artifacts/codex/typed-briefing-crop-cycle-20260717/evidence/media-review-sequential.webm`

Do not inherit a PASS. If media cannot be opened, return BLOCKED.

## Two clarifications to avoid changing the user's approved mechanic

1. The user explicitly requires irregular per-square opacity, including some
   very washed cells. Therefore missing/transparent-looking cells are not by
   themselves "confetti" or checkerboard. For R03/R04, judge **source
   ownership and composition**: one source must remain one contiguous
   five-column macro-cluster; the three-source case must keep three separate
   contiguous five-column macro-panels. Fail if sources are randomly mixed per
   cell, if the principal subject becomes unreadable, or if a source is
   stretched—not merely because tile alpha varies.
2. An atomic cut from complete old `{copy, CTA, media}` to new
   `{copy, CTA, media}` is acceptable. A crossfade is not required. Fail R01 if
   the WebM still shows a new headline over the old image, an old headline over
   the new image, or a perceptible empty hero. The first initial reveal may use
   the irregular tile animation; later preloaded state commits may be instant.

For R02, inspect the **latest** group screenshot. Both complete heads must be
inside the displayed source with visible source margin above them. Do not infer
from the earlier rejected screenshot.

Apply the remaining typography/OCR/mobile/motion criteria from
`crop-cycle-portrait-acceptance-prompt-2026-07-17.md`.

Required response:
- brief pixel/motion findings;
- R01 through R07 PASS|FAIL;
- OVERALL PASS|FAIL;
- PUBLISH ISOLATED LAB FOR USER REVIEW YES|NO;
- BLOCKERS.

This gate does not approve production and does not prove desirability.
