# Related events compact unification — integration map

## Baseline

- Base branch: `docs/related-events-consolidation-20260721`
- Base SHA: `d37e224f5a010d5f88b817298678eec8002d81cc`
- Integration branch: `feature/related-events-compact-unified-20260721`
- Verification owner: root integrator
- Global constraints: explicit canonical occurrence links only; no frontend identity inference; preserve accepted patterns 03/04/05/10; do not import rejected 01/02/11/12/13; no production deploy.

## Requirement matrix

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
| --- | --- | --- | --- | --- | --- | --- |
| R01 | Record accepted/rejected screenshot decisions | product contract | none | low | integrator | **Done:** docs name 03/04/05/10 accepted, 01/02/11/12/13 rejected |
| R02 | Canonical compact schedule notation | domain/formatting | occurrence slot model | medium | integrator | **Done:** exact, cross-month, mixed and a11y projections covered by unit tests |
| R03 | Check/rework big cards and left rail date block | UI surfaces | R02 | high | main/occurrence/listing explorers → integrator | **Done:** shared label variants replace local `Также:`; lab covers both rail orders |
| R04 | One resolver for family presence, slots, grouping and card collapse | frontend architecture | explicit exported IDs | high | main/occurrence explorers → integrator | **Done:** pure module owns resolver/formatter/collapse; no title/type/venue inference |
| R05 | Consolidate canonical documentation | docs | R01–R04 | medium | integrator | **Done:** umbrella requirements, inventory, visuals, static-page doc, routes, changelog |
| R06 | Provide branch and short propagation prompt | release/handoff | R01–R05 | low | branch-history/listing explorer → integrator | **Done when pushed:** `handoff.md` contains donor rules, prompt and checks |
| R07 | Independent architecture and closure review | acceptance | R02–R06 | low | Gemini Pro + integrator closure audit | **Done:** Gemini Pro returned ACCEPT WITH REWORK; requested layering/a11y/honest fallback and rollout coverage gate are incorporated |

## Lane map

```yaml
mode: serial_integrator_after_read_only_parallel
repo: events-bot-new
base_ref: d37e224f5a010d5f88b817298678eec8002d81cc
base_branch: docs/related-events-consolidation-20260721
integration_branch: feature/related-events-compact-unified-20260721
global_constraints:
  - explicit occurrence links are the only identity input
  - accepted screenshots 03/04/05/10 are preserved
  - rejected screenshots 01/02/11/12/13 do not become baselines
verification_owner: root
stop_conditions:
  - source worktree changes would be overwritten
  - shared formatter cannot fail honestly on ambiguous slots
lanes:
  - id: current-main-map
    role: planner
    requirement_ids: [R02, R03, R04]
    target: origin/main static site
    execution_mode: parallel
    writable_files: []
    expected_output: symbol and test map
    verification_scope: inspection_only
    status: completed
    effort: medium
  - id: occurrence-candidate-map
    role: planner
    requirement_ids: [R01, R03, R04]
    target: origin/feature/static-related-occurrence-final-templates
    execution_mode: parallel
    writable_files: []
    expected_output: accepted component and commit map
    verification_scope: inspection_only
    status: completed
    effort: medium
  - id: listing-lab-map
    role: planner
    requirement_ids: [R01, R03, R06]
    target: listing/feed lab branches
    execution_mode: parallel
    writable_files: []
    expected_output: surface and branch transfer matrix
    verification_scope: inspection_only
    status: completed
    effort: medium
  - id: integration
    role: worker
    requirement_ids: [R02, R03, R04, R05, R06]
    target: shared occurrence module/component/docs
    depends_on: [current-main-map, occurrence-candidate-map, listing-lab-map]
    execution_mode: serial_after_dependency
    branch: feature/related-events-compact-unified-20260721
    worktree: /home/dev/.codex/worktrees/events-bot-new/related-events-consolidation
    writable_files:
      - site/src/lib/occurrences.*
      - site/src/components/EventOccurrence*.astro
      - relevant card/listing integration and tests
      - docs/features/linked-events/**
      - linked canonical static-site docs
      - CHANGELOG.md
    forbidden_files:
      - production databases
      - deployment configuration
    expected_output: committed and pushed integration branch
    verification_scope: full_local
    status: completed
    effort: high
  - id: closure-review
    role: merge_reviewer
    requirement_ids: [R07]
    target: final diff and requirement matrix
    depends_on: [integration]
    execution_mode: serial_after_dependency
    writable_files: []
    expected_output: requirement-by-requirement verdict
    verification_scope: full_local
    status: completed
    effort: high
```

## Decision record

- Gemini 3.1 Pro (High), artifact
  `artifacts/codex/related-events-compact-contract-20260721/agy-architecture-response.md`:
  **ACCEPT WITH REWORK** — split domain resolver, formatter strategy and dumb
  Astro components; preserve honest mixed fallback and one a11y label.
- Product refinement after review: date-bounded lists use `per-date` rather than
  always keeping every time card; entity/ranked lists use `per-family`. This
  retains temporal intent and makes the user's same-day compact notation useful.
- Occurrence lab uses synthetic reciprocal November fixtures. Screenshot source
  event `5756` is composition-only while
  `INC-2026-07-18-dramteatr-same-day-event-glue` remains open.

## Verification evidence

- `npm run test:occurrences`: **PASS**, 8/8.
- `npm run build:preview`: **PASS**, 383 pages including synthetic occurrence lab.
- generated-output occurrence gates: **PASS** for both exact labels, both rail
  variants, mobile selector, no legacy `Также:`, hydrated family collapse.
- `npm run check:preview`: **BLOCKED by pre-existing unrelated base failure** —
  `Desktop v12 Garage fixture must expose delayed autorotation...`. The same
  failure reproduces on clean base `d37e224f` without this patch; occurrence
  assertions run before it and pass.
- `pytest -q tests/test_smart_update_merge_identity_gate.py`: **PASS**, 15/15;
  includes the incident negative replay and same-event positive control.
- linked export regression tests: **PASS**, 10/10
  (`test_static_site_public_gate` mutual normalization + pgvector export file).
- Playwright visual smoke: **PASS**, desktop 1280 and mobile 390; numbered
  validation screenshots 14/15 delivered to Telegram topic 437 (receipt in
  ignored artifacts).
- Production deploy: intentionally not in scope; no deploy performed.
