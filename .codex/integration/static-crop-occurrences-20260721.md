# Static crop + linked occurrences integration report

```yaml
mode: serial_integrator
repo: events-bot-new
base_ref: origin/main@da95738c55b931780e8dd94baec36b441f434df9
base_branch: main
integration_branch: integration/static-crop-occurrences-20260721
global_constraints:
  - keep stable production root untouched
  - publish only an immutable noindex bearer candidate
  - do not merge the donor branch wholesale
  - family identity comes only from reciprocal explicit other_date_ids
verification_owner: root integrator
stop_conditions:
  - any image band in compact event-detail cards
  - OCR/document crop above 20 percent
  - any inference of occurrence identity from title, type, or venue
  - generated-output or Gemini Pro rejection
lanes:
  - id: integration
    role: worker
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08, R09]
    target: selective donor port plus coupled static-card, search and release-gate implementation
    depends_on: []
    execution_mode: serial_after_dependency
    branch: integration/static-crop-occurrences-20260721
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-crop-occurrences-20260721
    writable_files: [site, scripts, supabase/functions/event-search, docs, CHANGELOG.md]
    forbidden_files: [unrelated dirty root-worktree changes]
    expected_output: one integrated, tested commit
    verification_scope: full_local
    status: spawned
  - id: closure_audit
    role: reviewer
    requirement_ids: [R10]
    target: final requirement-by-requirement read-only audit
    depends_on: [integration]
    execution_mode: read_only_until_dependency
    branch: integration/static-crop-occurrences-20260721
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-crop-occurrences-20260721
    writable_files: []
    forbidden_files: ['*']
    expected_output: ACCEPT or concrete blockers
    verification_scope: inspection_only
    status: spawned
```

## Requirement matrix

| ID | Requirement | Primary owner | Status | Evidence |
|---|---|---|---|---|
| R01 | Non-OCR hero/gallery fills with no fields | integration | Done | generated Chromium `hero_gallery_crop=ok` |
| R02 | Equal media and card heights per compact row | integration | Done | generated Chromium static + hydrated geometry checks |
| R03 | OCR normally uncropped; only very tall OCR may crop <=20% | integration | Done | DP feasibility contract; generated max actual crop `0.200024` |
| R04 | Enumerate combinations, allow reorder, minimize total full-page height | integration | Done | bitmask DP plus independent exhaustive-optimum unit test |
| R05 | Selectively port occurrence resolver/formatter/components | integration | Done | donor contract integrated without branch merge |
| R06 | Reciprocal explicit families; per-date/per-family surface policy | integration | Done | occurrence and producer tests |
| R07 | Always-visible detail selector and exact compact/two-line accessible labels | integration | Done | occurrence formatter/component tests |
| R08 | Search snapshot, Edge response and browser hydration collapse per family | integration | Done | vector-sync/Edge wiring and client tests |
| R09 | Canonical docs, changelog, incident and generated-output gates | integration | Done | docs diff; Node/Python/Astro/Chromium evidence |
| R10 | Independent final review, including agy Gemini 3.1 Pro High | closure_audit | Partial | Codex closure audit pending; agy is a post-publication hard gate |

## Local integration evidence

- Node: `94/94`.
- Python occurrence/search plus `INC-2026-07-18` merge-identity regression:
  `26 passed`.
- Astro preview: `383` pages; preview contract: `303` events.
- Production-family local export: `244` events, `916` root files, `921`
  secret-candidate files.
- Generated Chromium: all seven mandatory checks passed; static related `10`
  cards and hydrated continuation `6` cards; zero unused frame; document crop
  no greater than the `20%` tolerance.
- Retained local evidence (ignored):
  `artifacts/codex/static-crop-occurrences-20260721/local-browser-gate-v2/`.

The release portion stays pending until the implementation is reachable from
`origin/main`, a fresh immutable candidate is published, and agy Gemini 3.1 Pro
High returns an acceptance verdict on that exact candidate.
