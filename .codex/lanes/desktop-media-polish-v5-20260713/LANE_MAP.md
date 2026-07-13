# Desktop event media polish v5 — lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: 0d20185c1ae369fb8a65d140e9fa80cedffa9e19
base_branch: feature/event-page-desktop-scroll-compositions-v4-20260712
integration_branch: feature/event-page-desktop-media-polish-v5-20260713
global_constraints:
  - desktop lab only at min-width 1024px
  - do not edit production EventHero/EventLayout or mobile components/styles
  - use real event data and the existing fullscreen gallery
  - preserve the public v4 branch as rollback evidence
verification_owner: integrator
stop_conditions:
  - any production mobile diff
  - downward internal hero motion while document scrolls down
  - any gallery tile opens at index 0 instead of its own image
  - horizontal overflow at desktop acceptance viewports
lanes:
  - id: media-interaction-audit
    role: planner
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07]
    target: audit parallax direction, compact rail, selected-index gallery, Split OCR and Bento geometry
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only integration snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: exact DOM/CSS/JS correction notes
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: related-inversion-audit
    role: planner
    requirement_ids: [R09]
    target: map desktop-lab-only inverse related-card styling without double-card actions
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only integration snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: scoped CSS contract and regression risks
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: ratio-research-and-gemini
    role: reviewer
    requirement_ids: [R08]
    target: research responsive media/text proportions and obtain Gemini Pro implementation critique
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: integration worktree artifacts only
    writable_files: [artifacts/codex/desktop-media-polish-v5-20260713]
    forbidden_files: [site/src]
    expected_output: breakpoint-aware ratio recommendation
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R09]
    target: implement and publish the corrected desktop-only review pages
    depends_on: [media-interaction-audit, related-inversion-audit, ratio-research-and-gemini]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-media-polish-v5-20260713
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis
    writable_files:
      - site/src/components/lab/DesktopEventCleanPage.astro
      - site/src/pages/lab/event-desktop/**
      - site/scripts/check-preview.mjs
      - docs/features/static-site-pages/**
      - CHANGELOG.md
      - .codex/lanes/desktop-media-polish-v5-20260713/**
    forbidden_files:
      - site/src/components/EventHero.astro
      - site/src/layouts/EventLayout.astro
      - production mobile components/styles
    expected_output: public desktop v5 preview URLs
    verification_scope: full_local_and_public
    effort: high
    status: completed
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08, R09]
    target: final requirement, interaction and mobile-isolation audit
    depends_on: [integrator]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only final integration branch
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: Done/Partial/Missing closure matrix
    verification_scope: inspection_only
    effort: high
    status: completed
```

## Requirement matrix

| ID | Requirement | Done when |
|---|---|---|
| R01 | Editorial parallax moves in the same physical direction as downward page scroll | Playwright proves the internal image Y decreases as `scrollY` increases; screenshots show slower upward movement, not downward drift |
| R02 | Restore compact one-line Editorial thumbnail rail | One stable row of compact previews fits the right space at desktop breakpoints without becoming a 2×3 block |
| R03 | Editorial thumbnails open fullscreen at the selected image and duplicate photo-count control disappears | Clicking rail item N opens the existing gallery at counter N; Editorial has no redundant `N фото` pill |
| R04 | Split OCR media occupies approximately half the desktop width with retuned slow travel | 50/50 split at normal desktop widths, readable right column, bounded negative poster motion and full poster reach |
| R05 | Bento uses square base cells and explicit crop policy | Base cells are square; OCR preserves top and crops bottom, visual images crop centrally, wide visuals occupy 2×1 |
| R06 | Bento click opens fullscreen at the selected image | Main and every tile open the existing carousel at their own gallery index |
| R07 | Bento example visibly contains a horizontal 2×1 image | Real event media with landscape natural ratio spans two neighboring square cells |
| R08 | Choose a harmonious responsive media/content ratio using research and Gemini | Breakpoint contract is documented and applied; body measure remains readable |
| R09 | Invert related cards on graphite without a double-card action effect | Card body/utility become light, red remains red, share/like sit transparently on graphite with inverse text/icons |
