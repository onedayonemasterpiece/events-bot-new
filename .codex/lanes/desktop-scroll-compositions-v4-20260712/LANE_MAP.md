# Desktop scroll compositions v4 — lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: 09a21a1da544c51ccfbf1d56733b52150cd74c25
base_branch: feature/event-page-desktop-scroll-compositions-v3-20260712
integration_branch: feature/event-page-desktop-scroll-compositions-v4-20260712
global_constraints:
  - desktop-only at min-width 1024px
  - no production EventHero/EventLayout/mobile edits
  - preserve v3 preview as rollback/reference
  - real event data and real image lists
verification_owner: integrator
stop_conditions:
  - any production mobile diff
  - abrupt source replacement in the scroll-stack candidate
  - sticky media/CTA overlap with related cards
  - OCR crop beyond the documented budget
lanes:
  - id: editorial-map
    role: planner
    requirement_ids: [R01, R02]
    target: map one continuous Editorial content slab, sticky CTA/rail and alternate landscape selection
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: DOM/CSS/media-selection plan
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: vertical-motion-map
    role: planner
    requirement_ids: [R03, R04, R05]
    target: map Split OCR, smooth scroll strip and vertical adaptive Bento geometry
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: deterministic scroll math and Bento sizing plan
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: consultants
    role: reviewer
    requirement_ids: [R01, R02, R03, R04, R05]
    target: Gemini Pro and a-opus design/interaction critique
    depends_on: [editorial-map, vertical-motion-map]
    execution_mode: serial_after_dependency
    branch: null
    worktree: current integration worktree
    writable_files: [artifacts/codex/desktop-scroll-compositions-v4-20260712]
    forbidden_files: [site/src]
    expected_output: implementation gates and rejected mechanics
    verification_scope: inspection_only
    effort: high
    status: completed_with_a_opus_quota_blocker
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R03, R04, R05]
    target: implement, test and publish desktop v4 review pages
    depends_on: [editorial-map, vertical-motion-map, consultants]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-scroll-compositions-v4-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis
    writable_files:
      - site/src/components/lab/DesktopEventCleanPage.astro
      - site/src/components/lab/DesktopEventActionPanel.astro
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
    status: completed_local_pending_public_release
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05]
    target: final requirement and visual-regression audit
    depends_on: [integrator]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only final branch
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: Done/Partial/Missing closure matrix
    verification_scope: inspection_only
    effort: high
    status: completed_after_fixes
```

## Requirement matrix

| ID | Requirement | Done when |
|---|---|---|
| R01 | Editorial photo becomes one continuous information slab | Title/date/venue/medallions and full `О событии` description share one normal-flow block; no duplicate lower description; image exits above by the description end |
| R02 | Editorial side rail behavior and alternate landscape | Thumbnail strip sits above a sticky CTA rail; both release before related; OCR-primary event can intentionally promote a verified horizontal visual alternative while retaining poster access |
| R03 | Split OCR gains meaningful long-flow parallax | Full information/description is on the right; left poster moves slower and leaves before related rather than ending before the text |
| R04 | Reading variant uses a smooth physical image strip | Images coexist in one vertical strip and reveal continuously from below; no abrupt `src` swap or reversed movement; strip is slower than right content and releases before related |
| R05 | New portrait + adaptive Bento composition | Main portrait plus aspect-aware 1×1/2×1/1×2 Bento spans fill the left column within available text height; right column contains full event information; media parallax is bounded and releases before related |
