# R12 execution matrix and lane map

Base: `69dad0ae38b0f89ced776c2a7faf749bd3c9fbc1` (published R11 noindex prototype).
Production is out of scope until owner acceptance.

| ID | Original requirement | Area | Dependencies | Conflict risk | Lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|
| R12-01 | «Сегодняшнее событие которое уже прошло не saturation» | mobile Today/runtime clock | accepted mobile rail | low | SATURATION | yes | a real past event at the build date is visibly desaturated in Playwright; future event is not |
| R12-02 | «В списке событий на 26 июля … Воскресник Хранителей руин … не вижу их медальона» | event medallion resolution/data | medallion manifests + real event | medium | MEDALLIONS | yes, shared with R12-03 | exact event resolves the accepted organizer/festival token with provenance and browser evidence |
| R12-03 | «У события где есть транспорт … должен быть медальон … РЖД-ласточка; контрольное событие музея курортной моды» | secondary transport medallion | transport payload + layout rules | medium | MEDALLIONS | yes, shared with R12-02 | eligible rail event resolves RZD Lastochka as Secondary/Inline; control event verified |
| R12-04 | Empty item «4.» | unknown | user clarification | none | INTEGRATOR | no code | preserved as needs-interpretation; no inferred requirement |

```yaml
mode: worktree_worker
repo: /home/dev/projects/events-bot-new
base_ref: 69dad0ae38b0f89ced776c2a7faf749bd3c9fbc1
base_branch: integration/unified-corrections-r11-20260724
integration_branch: integration/unified-corrections-r12-20260726
global_constraints:
  - no production generation/deploy
  - preserve OCR fail-closed crop and accepted mobile rail
  - reuse accepted medallion assets; git archaeology first
  - docs and CHANGELOG owned by integrator
verification_owner: integrator
stop_conditions:
  - worker scope collision
  - missing trustworthy medallion provenance
lanes:
  - id: R12-SATURATION
    role: worker
    requirement_ids: [R12-01]
    target: runtime Today temporal state and browser regression
    depends_on: []
    execution_mode: parallel
    branch: agent/unified-r12/saturation
    worktree: /home/dev/.codex/worktrees/events-bot-new/r12-saturation
    writable_files: [site/src/components/listings/DateListingSurface.astro, site/src/components/listings/MobileListingRailSurface.astro, site/tests/*today*, site/tests/mobile-listing-rails*]
    forbidden_files: [CHANGELOG.md, docs/**, medallion manifests/assets]
    expected_output: committed fix plus .codex/lanes/R12-SATURATION/RESULTS.md
    verification_scope: targeted
    status: completed
  - id: R12-MEDALLIONS
    role: worker
    requirement_ids: [R12-02, R12-03]
    target: exact medallion data/resolution/layout for Ruins Keepers and RZD Lastochka
    depends_on: []
    execution_mode: parallel
    branch: agent/unified-r12/medallions
    worktree: /home/dev/.codex/worktrees/events-bot-new/r12-medallions
    writable_files: [site/src/lib/eventMedallions.ts, site/src/components/EventTokenMedallions.astro, site/src/data/*Medallions.json, site/public/assets/**, site/src/assets/**, site/tests/*medallion*]
    forbidden_files: [CHANGELOG.md, docs/**, listing temporal components]
    expected_output: committed fix plus .codex/lanes/R12-MEDALLIONS/RESULTS.md
    verification_scope: targeted
    status: completed
  - id: R12-INTEGRATOR
    role: merge_reviewer
    requirement_ids: [R12-04]
    target: merge, canonical docs, changelog, full preview/build/browser QA
    depends_on: [R12-SATURATION, R12-MEDALLIONS]
    execution_mode: serial_after_dependency
    branch: integration/unified-corrections-r12-20260726
    worktree: /home/dev/.codex/worktrees/events-bot-new/unified-corrections-r12
    writable_files: [docs/**, CHANGELOG.md, .codex/lanes/R12/**]
    forbidden_files: []
    expected_output: integrated noindex preview and closure report
    verification_scope: full_local
    status: completed
```
