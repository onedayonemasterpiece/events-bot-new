# Image geometry production repair integration report

Base: `origin/main@c587a0cf86e144a88c0457035866c8325ea59dc5`

## Requirements

| ID | Status | Evidence |
| --- | --- | --- |
| R01 | Done | New poster writes use encoded-byte SHA paths; visual identity changes centrally invalidate geometry and image-dependent semantic evidence; exact-pixel checks guard selection. |
| R02 | Done | Both initial approval and final pair reconciliation enqueue the durable enrichment job; the operational selector includes missing and stale geometry. |
| R03 | Done | Semantic role uses normal `KEY4,KEY5` rotation without overflow/model fallback; RPD, rate-limit and temporary failures receive bounded persisted delays. |
| R04 | Done | Static export joins normalized face/value boxes only for exact-current model/prompt/pixel geometry and includes provenance. |
| R05 | Done | The exact-ratio desktop card solver preserves the protected union when feasible and otherwise uses `contain`; responsive surfaces fail closed rather than guess a crop. |
| R06 | Partial until release | Source integration, tests, docs, changelog and incident contract are complete. Main merge, Fly deploy, paced canary and visual production QA remain release steps. |

## Validation

- Python compile and `git diff --check`: passed.
- Focused/integrated Python: `146 passed` (including exact-v2 early-return and
  Smart Update raw-identity collision regressions).
- Focused Node crop/media/desktop suite: `20 passed` (including reconstruction
  of the serialized CSS crop at a tight protected boundary).
- Astro production build: `380 page(s) built`.
- Broad Node suite: `43/44`; the sole failure is the pre-existing
  `event-detail-runtime-regressions` literal class-token assertion against an
  unchanged `EventLayout.astro`. It is outside this repair and does not fail the
  focused behavior tests or build.
- Production preflight: SQLite `quick_check=ok`; 2026-07-20 usage before canary
  was 9/100 geometry calls and 10/150 semantic-role calls. Event `6956` still
  demonstrated the old-pixel linked geometry defect before deployment.

The independent checklist review found and drove fixes for provider-call
TOCTOU drift, legacy TelegramMonitor writes, CSS specificity/precision,
display/source fallback, audit blind spots and per-event raw-SHA collisions.

## Release guard

No mass backfill is part of this release. After the main-reachable Fly deploy,
run only a paced one-event canary, verify exact poster/geometry hashes and the
new immutable path, inspect runtime mirror evidence, and visually inspect bbox
overlays before expanding the cohort.
