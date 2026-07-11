# Event page desktop variants — lane map

```yaml
mode: serial_integrator_with_parallel_read_only_review
repo: events-bot-new
base_ref: e9966bb1
base_branch: feature/event-page-ux-lab-v3-20260710
integration_branch: integration/event-page-desktop-variants-20260711
global_constraints:
  - Preserve the accepted mobile hero-overlap hierarchy.
  - No fabricated price, registration requirement, calendar count, or email outcome.
  - OCR/unknown posters must remain uncropped.
  - Desktop variants are a review lab, not silent production promotion.
verification_owner: integrator
execution_matrix:
  - {id: R01, requirement: "Correct CTA semantics", lane: L1-product-media-audit, depends_on: [], done_when: "free no-registration uses calendar; paid unknown says Tickets"}
  - {id: R02, requirement: "Remove desktop fact/action duplication", lane: L4-integrator, depends_on: [R06, R07], done_when: "each lab option has one fact owner and one primary CTA"}
  - {id: R03, requirement: "Onboarding is optional and contextual", lane: L1-product-media-audit, depends_on: [], done_when: "default review page does not force first-view education"}
  - {id: R04, requirement: "Enlarge action icons and remove calendar plus", lane: L1-product-media-audit, depends_on: [], done_when: "large SVGs in 48px+ targets with no literal plus"}
  - {id: R05, requirement: "Do not crop OCR posters", lane: L1-product-media-audit, depends_on: [], done_when: "OCR/unknown poster checks use contain"}
  - {id: R06, requirement: "Several horizontal/square desktop concepts", lane: L2-gemini, depends_on: [R01, R05], done_when: "three buildable concepts are reviewed"}
  - {id: R07, requirement: "Several portrait desktop concepts", lane: L3-opus, depends_on: [R01, R05], done_when: "three buildable concepts are reviewed"}
  - {id: R08, requirement: "Preserve accepted mobile layout", lane: L4-integrator, depends_on: [R01, R03, R04, R05], done_when: "320/390 regression passes"}
  - {id: R09, requirement: "Publish a choice surface", lane: L4-integrator, depends_on: [R06, R07], done_when: "noindex public lab and real-event preview return 200"}
stop_conditions:
  - consultant downgrade below Gemini Pro or Opus
  - shared component regression at 320/390 px
lanes:
  - id: L1-product-media-audit
    role: planner
    requirement_ids: [R01, R03, R04, R05]
    target: inspect real event CTA/media/onboarding states
    depends_on: []
    execution_mode: parallel
    branch: integration/event-page-desktop-variants-20260711
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1
    writable_files: []
    forbidden_files: [site/src/components, site/src/layouts]
    expected_output: artifacts/codex/event-page-desktop-20260711/audit.json
    verification_scope: inspection_only
    effort: medium
    status: completed
  - id: L2-gemini
    role: reviewer
    requirement_ids: [R06]
    target: horizontal/square and portrait desktop concepts
    depends_on: [L1-product-media-audit]
    execution_mode: parallel
    branch: integration/event-page-desktop-variants-20260711
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1
    writable_files: []
    forbidden_files: [site/src]
    expected_output: artifacts/codex/event-page-desktop-20260711/gemini.md
    verification_scope: inspection_only
    effort: extra-high
    status: completed
  - id: L3-opus
    role: reviewer
    requirement_ids: [R07]
    target: independent desktop concepts and critique
    depends_on: [L1-product-media-audit]
    execution_mode: parallel
    branch: integration/event-page-desktop-variants-20260711
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1
    writable_files: []
    forbidden_files: [site/src]
    expected_output: artifacts/codex/event-page-desktop-20260711/opus.md
    verification_scope: inspection_only
    effort: extra-high
    status: blocked
  - id: L4-integrator
    role: worker
    requirement_ids: [R02, R08, R09]
    target: fixes plus desktop variant lab and public preview
    depends_on: [L1-product-media-audit, L2-gemini, L3-opus]
    execution_mode: serial_after_dependency
    branch: integration/event-page-desktop-variants-20260711
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1
    writable_files: [site, docs, CHANGELOG.md, .env.example, .codex/lanes]
    forbidden_files: [supabase/migrations, production DB]
    expected_output: committed preview implementation
    verification_scope: full_local
    effort: extra-high
    status: spawned
```
