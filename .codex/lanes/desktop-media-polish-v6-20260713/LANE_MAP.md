# Desktop event media polish v6 — lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: ae7c61e810091a024af2f885e873bb559a38c55a
base_branch: feature/event-page-desktop-media-polish-v5-20260713
integration_branch: feature/event-page-desktop-media-polish-v6-20260713
global_constraints:
  - desktop lab only at min-width 1024px
  - do not edit production EventHero/EventLayout or mobile components/styles
  - preserve v5 public preview as rollback evidence
  - OCR/document media must be fully readable through contain, not crop
verification_owner: integrator
stop_conditions:
  - any production mobile diff
  - OCR image rendered with cover in fullscreen or Split review surfaces
  - CTA leaves the desktop Split viewport before the related-section release boundary
  - horizontal overflow at desktop acceptance viewports
lanes:
  - id: media-cta-audit
    role: planner
    requirement_ids: [R01, R02, R03, R04, R05]
    target: map stronger Editorial parallax, OCR fullscreen containment, remaining-photo affordance, and sticky Split CTA
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only integration snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: exact DOM/CSS/JS corrections and risks
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: related-media-audit
    role: planner
    requirement_ids: [R06]
    target: design desktop-only square-ish no-OCR related-card normalization without changing OCR cards or mobile
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only integration snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: scoped normalization contract and acceptance assertions
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R03, R04, R05, R06]
    target: implement corrected desktop-only review pages
    depends_on: [media-cta-audit, related-media-audit]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-media-polish-v6-20260713
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis
    writable_files:
      - site/src/components/lab/DesktopEventCleanPage.astro
      - site/src/pages/lab/event-desktop/**
      - site/scripts/check-preview.mjs
      - docs/features/static-site-pages/**
      - CHANGELOG.md
      - .codex/lanes/desktop-media-polish-v6-20260713/**
    forbidden_files:
      - site/src/components/EventHero.astro
      - site/src/layouts/EventLayout.astro
      - production mobile components/styles
    expected_output: public desktop v6 preview URLs
    verification_scope: full_local_and_public
    effort: high
    status: completed
  - id: gemini-browser-audit
    role: reviewer
    requirement_ids: [R07]
    target: visually audit every v6 composition across scroll states with primary focus on CTA, media readability and event comprehension
    depends_on: [integrator]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only screenshots and public URLs
    writable_files: [artifacts/codex/desktop-media-polish-v6-20260713]
    forbidden_files: [site/src]
    expected_output: Gemini Pro design and product assessment with findings grouped by composition
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07]
    target: final requirement, interaction, consultant and mobile-isolation audit
    depends_on: [gemini-browser-audit]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only final integration branch
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: Done/Partial/Missing closure matrix
    verification_scope: inspection_only
    effort: high
    status: in_progress
```

## Requirement matrix

| ID | Requirement | Done when |
|---|---|---|
| R01 | Make Editorial parallax noticeably stronger | Correct-direction travel is materially larger than v5 while bounded, clipped and disabled under reduced motion |
| R02 | Fullscreen gallery must fit OCR images | Known document/poster frames render whole and readable with `contain`; visual photos may still use immersive cover |
| R03 | Editorial preview rail must answer how many photos remain | One compact cell communicates the remaining count without restoring a duplicate floating photo button |
| R04 | Split CTA remains available during long reading | CTA becomes sticky after reaching the header and releases before `Смотрите дальше`; article content may pass behind the deliberately opaque panel, but cannot visibly bleed into the header gap |
| R05 | Split OCR images must fit, not crop | OCR poster and any OCR gallery frame use contain with no text loss |
| R06 | No-OCR related images become more square | Desktop related rows use square-ish crop for visual-only cards, reducing height; OCR/document cards keep readable containment |
| R07 | Gemini visually audits every variant while scrolling | Gemini Pro receives public URLs plus Playwright scroll-state captures and returns separate design/product findings focused on CTA, media and event comprehension |
