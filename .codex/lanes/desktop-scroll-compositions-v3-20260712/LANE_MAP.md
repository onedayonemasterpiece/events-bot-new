# Desktop scroll compositions v3 — lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: c4e6bae98f344c1a7bd06b0a346fc7589bfc73b5
base_branch: feature/event-page-desktop-clean-pages-v2-20260712
integration_branch: feature/event-page-desktop-scroll-compositions-v3-20260712
global_constraints:
  - desktop-only at min-width 1024px
  - no production mobile component/style changes
  - preserve previous preview as rollback/reference
  - real event content; no technical copy inside event pages
verification_owner: integrator
stop_conditions:
  - any production EventHero/EventLayout/mobile diff
  - OCR text crop over the accepted budget without explicit lab policy
  - first-viewport title/date/venue/CTA clipping
lanes:
  - id: scroll-geometry-map
    role: planner
    requirement_ids: [R02, R03, R04, R05]
    target: map current hero/scroll coupling and safe desktop-only extension points
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: exact CSS/DOM/JS change plan
    verification_scope: inspection_only
    effort: medium
    status: merged
  - id: related-normalization-map
    role: planner
    requirement_ids: [R06]
    target: map EventCard OCR/photo media geometry and row normalization options
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: production-compatible row algorithm and selectors
    verification_scope: inspection_only
    effort: medium
    status: merged
  - id: consultants
    role: reviewer
    requirement_ids: [R02, R03, R04, R05, R06]
    target: Gemini Pro and a-opus critical design review
    depends_on: [scroll-geometry-map, related-normalization-map]
    execution_mode: serial_after_dependency
    branch: null
    worktree: current integration worktree
    writable_files: [artifacts/codex/desktop-scroll-compositions-v3-20260712]
    forbidden_files: [site/src]
    expected_output: implementable recommendations and rejection risks
    verification_scope: inspection_only
    effort: high
    status: merged
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R03, R04, R05, R06]
    target: implement, test and publish desktop v3 variants
    depends_on: [scroll-geometry-map, related-normalization-map, consultants]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-scroll-compositions-v3-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis
    writable_files:
      - site/src/components/lab/DesktopEventCleanPage.astro
      - site/src/pages/lab/event-desktop/**
      - site/scripts/check-preview.mjs
      - docs/features/static-site-pages/**
      - CHANGELOG.md
    forbidden_files:
      - site/src/components/EventHero.astro
      - site/src/layouts/EventLayout.astro
      - production mobile components/styles
    expected_output: clean desktop-only preview URLs
    verification_scope: full_local_and_public
    effort: high
    status: spawned
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05, R06]
    target: final requirement and visual regression audit
    depends_on: [integrator]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only final branch
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: Done/Partial/Missing closure matrix
    verification_scope: inspection_only
    effort: high
    status: merged
```

## Requirement matrix

| ID | Requirement | Done when |
|---|---|---|
| R01 | Desktop only; mobile production untouched | No production/mobile file diff; v3 surface hidden below 1024px |
| R02 | Editorial photo scroll model | Background photo moves slowly; content slab rises with document; ticket block stays with media/action layer; thumbnail switcher remains and auto-rotation is controllable |
| R03 | Editorial OCR full-height left poster | Poster fills available height, is flush left and uncropped; right column width follows poster ratio and includes a short digest |
| R04 | Split OCR minimum width and natural page scroll | Poster column never collapses below readable minimum; poster is not shrunk into an unreadable strip and leaves naturally with page scroll |
| R05 | New sticky-media reading-column variant | ~50% left media stays sticky while a long right event story scrolls; additional deduplicated images advance deterministically and reversibly |
| R06 | Media-aware related row normalization | Outer cards/actions align; one OCR item sets row media height; multiple OCR items use the common frame with the lowest bounded crop loss; photos cover the chosen frame |
