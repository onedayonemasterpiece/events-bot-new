# Desktop clean event pages v2 — lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: e9a50f61805dd2ae01222e3ddc1eb6c16126cbdb
base_branch: feature/event-page-desktop-multimedia-analysis-20260712
integration_branch: feature/event-page-desktop-clean-pages-v2-20260712
global_constraints:
  - desktop-only at min-width 1024px
  - no production mobile component/layout changes
  - event examples contain no research labels or technical rationale
  - preserve previous media-families geometry and accepted production controls
  - Gemini review must be Pro class
verification_owner: integrator
stop_conditions:
  - any production mobile diff
  - event page contains lab/service explanation
  - gallery or parallax cannot be exercised through Chromium
lanes:
  - id: prior-composition-map
    role: planner
    requirement_ids: [R03, R05]
    target: map exact media-families composition and motion contracts
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: exact components/classes/geometry to preserve
    verification_scope: inspection_only
    effort: medium
    status: merged
  - id: production-ui-map
    role: planner
    requirement_ids: [R04, R06, R08]
    target: map production header, fullscreen gallery, event cards and controls
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: read-only current snapshot
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: reusable production components and integration constraints
    verification_scope: inspection_only
    effort: medium
    status: merged
  - id: gemini-review
    role: reviewer
    requirement_ids: [R02, R07, R10]
    target: critical product and interaction review before implementation
    depends_on: [prior-composition-map, production-ui-map]
    execution_mode: serial_after_dependency
    branch: null
    worktree: current integration worktree
    writable_files: [artifacts/codex/desktop-clean-event-pages-v2-20260712]
    forbidden_files: [site/src]
    expected_output: actionable acceptance/rejection notes
    verification_scope: inspection_only
    effort: high
    status: merged
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R04, R07, R09, R11, R12]
    target: implement and publish clean desktop event pages
    depends_on: [prior-composition-map, production-ui-map, gemini-review]
    execution_mode: serial_after_dependency
    branch: feature/event-page-desktop-clean-pages-v2-20260712
    worktree: /home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis
    writable_files:
      - site/src/components/lab/DesktopEvent*.astro
      - site/src/pages/lab/event-desktop/**
      - site/scripts/check-preview.mjs
      - docs/features/static-site-pages/**
      - CHANGELOG.md
    forbidden_files:
      - site/src/components/EventHero.astro
      - site/src/layouts/EventLayout.astro
      - production mobile styles and pages
    expected_output: separate clean real-event URLs plus overview-only analysis
    verification_scope: full_local_and_public
    effort: high
    status: committed
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08, R09, R10, R11, R12]
    target: requirement-by-requirement diff and UI acceptance audit
    depends_on: [integrator]
    execution_mode: serial_after_dependency
    branch: null
    worktree: read-only final branch
    writable_files: []
    forbidden_files: ['**/*']
    expected_output: closure matrix with missing risks
    verification_scope: inspection_only
    effort: high
    status: merged
```

## Requirement matrix

| ID | Requirement | Done when |
|---|---|---|
| R01 | Desktop only; mobile production untouched | No production/mobile file diff; prototypes hidden below 1024px |
| R02 | Clean event pages without service/research clutter | No lab-note, rationale, prototype or planned-data labels inside event page |
| R03 | Preserve agreed media-families compositions | Geometry/classes are derived from prior Editorial/Split/Gallery variants |
| R04 | Multiple images use fullscreen viewer; Editorial may expose visible carousel | Image CTA opens overlay; keyboard/close/arrows work; Editorial has visible media rail |
| R05 | Clearly visible, bounded parallax | Media visibly moves more slowly than page and stops before related; reduced-motion disables it |
| R06 | Desktop header stays fixed | Production-like header remains sticky during scroll |
| R07 | Full flow includes description, feedback marker, transport and related | Separate event URLs continue cleanly through every applicable section |
| R08 | Related uses full production card/control contract and normalized height | Existing event-card renderer reused or faithfully wrapped, dark inversion only via theme |
| R09 | Separate URLs based on real events | At least 4 directly reviewable event URLs |
| R10 | Constructive Gemini Pro consultation | Successful Pro-class artifact and applied/rejected findings recorded |
| R11 | Prior data findings remain available only in overview/docs | Event page has no audit metrics; overview retains evidence |
| R12 | Desktop UI acceptance | Multi-viewport Chromium, full scroll, gallery, parallax and mobile-diff gates pass |
