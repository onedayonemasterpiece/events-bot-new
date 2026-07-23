# R10 Mobile rail donor restoration plan

```yaml
mode: serial_integrator_after_parallel_read_only_mapping
repo: /home/dev/projects/events-bot-new
base_ref: 43a4b3e5
base_branch: integration/mobile-acceptance-r9-20260723
integration_branch: integration/mobile-rail-donor-restore-r10-20260723
global_constraints:
  - exact donor contract integration/mobile-search-unified-v14-20260722@3f5b88f9
  - no OCR crop weakening; only source-reviewed alternate asset may override event-level OCR
  - shared feedback profile and shared Icon.astro; no private duplicate state store
  - production untouched; immutable noindex preview only
verification_owner: root
stop_conditions:
  - unresolved crop protection regression
  - missing generated event canary
  - Gemini Pro acceptance blocker must be reported, never downgraded to Lite/Flash
lanes:
  - id: R10-DONOR
    role: planner
    requirement_ids: [R1, R4]
    target: exact heart and gesture donor mapping
    depends_on: []
    execution_mode: read_only_parallel
    branch: integration/mobile-rail-donor-restore-r10-20260723
    worktree: current integration worktree
    writable_files: []
    forbidden_files: ['*']
    expected_output: exact donor state machine and shared-icon mapping
    verification_scope: inspection_only
    status: completed
  - id: R10-MEDIA
    role: planner
    requirement_ids: [R2, R3]
    target: real-data crop canaries 5296/6939
    depends_on: []
    execution_mode: read_only_parallel
    branch: integration/mobile-rail-donor-restore-r10-20260723
    worktree: current integration worktree
    writable_files: []
    forbidden_files: ['*']
    expected_output: safe resolver/override patch contract
    verification_scope: inspection_only
    status: completed
  - id: R10-MEDALLION
    role: planner
    requirement_ids: [R5]
    target: structured festival resolver audit for 4211
    depends_on: []
    execution_mode: read_only_parallel
    branch: integration/mobile-rail-donor-restore-r10-20260723
    worktree: current integration worktree
    writable_files: []
    forbidden_files: ['*']
    expected_output: fail-closed manifest fix
    verification_scope: inspection_only
    status: completed
  - id: R10-INTEGRATE
    role: worker
    requirement_ids: [R1, R2, R3, R4, R5]
    target: shared rail row/surface/media/medallion integration
    depends_on: [R10-DONOR, R10-MEDIA, R10-MEDALLION]
    execution_mode: serial_after_dependency
    branch: integration/mobile-rail-donor-restore-r10-20260723
    worktree: current integration worktree
    writable_files:
      - site/src/components/listings/MobileListingRailRow.astro
      - site/src/components/listings/MobileListingRailSurface.astro
      - site/src/lib/mobileListingRailMedia.mjs
      - site/src/lib/listingPresentation.ts
      - site/src/data/listingMediaOverrides.json
      - site/src/data/festivalMedallions.json
      - focused tests and canonical docs
      - CHANGELOG.md
    forbidden_files:
      - production deployment state
    expected_output: one integrated commit and immutable noindex preview
    verification_scope: full_local
    status: completed
  - id: R10-ACCEPT
    role: reviewer
    requirement_ids: [R1, R2, R3, R4, R5]
    target: Gemini Pro critical acceptance plus browser evidence
    depends_on: [R10-INTEGRATE]
    execution_mode: serial_after_dependency
    branch: integration/mobile-rail-donor-restore-r10-20260723
    worktree: current integration worktree
    writable_files: [.codex/lanes/R10/RESULTS.md, .codex/integration/INTEGRATION_REPORT-R10.md]
    forbidden_files: [product implementation]
    expected_output: severity-tagged acceptance and release links
    verification_scope: full_local
    status: completed
```

## Requirements

| ID | Requirement | Done when |
|---|---|---|
| R1 | Restore standard hollow/filled rail like icon | shared Icon component drives proof and CTA states |
| R2 | Pianissimo #5296 has no top/bottom fields | 140×112 cover, face-safe, generated/browser canary |
| R3 | Teremok #6939 uses reviewed visual-only image at vertical 4:5 | 90×112 cover, crop <=20%, OCR fail-closed otherwise |
| R4 | Restore donor edge gestures | right pull at start gives red/confirm negative; left overpull at end likes; pointer/touch/undo |
| R5 | More vnutri #4211 resolves festival medallion in rail | structured festival binding, external token, no image overlay |
