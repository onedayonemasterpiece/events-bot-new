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
    status: merged
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
    status: merged
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
| R10 | Independent final review, including agy Gemini 3.1 Pro High | closure_audit | Done | Codex closure audit accepted; live candidate passed and agy returned `SHIP_SECRET_CANDIDATE` |

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

## Release and final acceptance evidence

- PR `#117` merged to exact `origin/main` SHA
  `58440062e7bab708676c378de345c65f19ce91b1`; both required GitHub checks
  passed.
- Fly release `v1741` deployed and pinned that exact SHA; `/healthz` remained
  ready.
- Fresh immutable candidate `D1qL0…` was built as
  `production-secret-20260721T120452-b290f999` from snapshot
  `snapshot-20260721T100452-9c8cd823ac`; result SHA-256 is
  `16c57759c57f1d31cd1a84cf5e4e30556a730abc1f71721d871e5bcb6b7b3f16`
  and manifest SHA-256 is
  `73cb6e4c3ea1ce22e22e29e6974323a17abea2219b34543f6d9e4a247ed5c884`.
- Live Chromium passed nine acceptance groups on the exact candidate: hero and
  gallery crop, static and hydrated compact-card geometry, always-rendered and
  real-family occurrence selectors, cold/mixed-input keyboard, cross-document
  gallery and footer shortcuts.
- Stable root and sitemap hashes remained unchanged. No root promotion occurred.
- Independent `/home/dev/.local/bin/agy` model `gemini-3.1-pro-high`, high
  effort, returned `SHIP_SECRET_CANDIDATE` after inspecting the URL, PNG pixels,
  source and test evidence.

Full bearer URL and detailed evidence stay in the ignored directory
`artifacts/codex/static-crop-occurrences-20260721/`; the secret token is not
committed to Git.
