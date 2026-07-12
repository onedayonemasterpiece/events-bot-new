# Desktop media families — lane map

```yaml
mode: serial_integrator_with_parallel_read_only_discovery
repo: events-bot-new
base_ref: origin/integration/event-page-desktop-variants-20260711
base_branch: integration/event-page-desktop-variants-20260711
integration_branch: feature/event-page-desktop-media-families-20260712
global_constraints:
  - desktop lab only; production mobile components and mobile CSS are forbidden
  - non-OCR media may use an assertive cover crop
  - OCR media must retain at least 80 percent of the source image area
  - use real event data and validate both common and short-height viewports
verification_owner: L3-integrator
stop_conditions:
  - source image dimensions or OCR mode cannot be verified
  - an OCR prototype exceeds the 20 percent crop budget
lanes:
  - id: L1-corpus
    role: planner
    requirement_ids: [R05]
    target: Measure the real preview corpus and select representative OCR/non-OCR events.
    depends_on: []
    execution_mode: parallel
    branch: feature/event-page-desktop-media-families-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-media-families
    writable_files: [artifacts/codex/desktop-media-families-20260712]
    forbidden_files: [site/src, docs, CHANGELOG.md]
    expected_output: Corpus statistics and verified specimen IDs.
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: L2-viewport
    role: planner
    requirement_ids: [R04]
    target: Establish representative desktop viewport and short-height acceptance matrix.
    depends_on: []
    execution_mode: parallel
    branch: feature/event-page-desktop-media-families-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-media-families
    writable_files: [artifacts/codex/desktop-media-families-20260712]
    forbidden_files: [site/src, docs, CHANGELOG.md]
    expected_output: Source-backed viewport matrix and title-visibility checks.
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: L3-integrator
    role: worker
    requirement_ids: [R01, R02, R03, R06]
    target: Build the desktop-only OCR/non-OCR lab, enforce media rules, document and validate it.
    depends_on: [L1-corpus, L2-viewport]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-media-families-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-media-families
    writable_files:
      - site/src/pages/lab/event-desktop/index.astro
      - site/src/components/lab/DesktopEventPrototype.astro
      - site/scripts/check-preview.mjs
      - docs/features/static-site-pages
      - CHANGELOG.md
      - .codex/lanes/desktop-media-families-20260712
    forbidden_files:
      - site/src/components/EventHero.astro
      - site/src/layouts/EventLayout.astro
      - production mobile styles
    expected_output: Public desktop comparison page with real specimens and passing viewport/crop gates.
    verification_scope: full_local
    effort: high
    status: completed
```

## Requirements

| ID | Requirement | Done when |
|---|---|---|
| R01 | Separate desktop layouts for images without OCR. | Non-OCR specimens use strong `cover` treatment and are labelled separately. |
| R02 | Separate desktop layouts for OCR images; crop no more than 20%. | OCR specimens use adaptive safe-cover/contain and measured crop is ≤20%. |
| R03 | Minimize black/empty fields through efficient image and text placement. | Poster width follows intrinsic ratio and viewport height; no decorative masking is used. |
| R04 | Account for screen width and height; title remains readable on wide/short displays. | Automated checks pass at common 16:9, QHD, ultrawide, and short-height viewports. |
| R05 | Use real examples and analyze Typographic Lead viability for short titles. | Corpus distribution and representative event IDs are recorded and reflected in the lab. |
| R06 | Desktop only; do not alter mobile. | Diff contains no production mobile component/style changes. |

