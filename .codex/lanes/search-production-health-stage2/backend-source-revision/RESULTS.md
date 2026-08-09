# Exact Search backend source revision lane

## Scope and outcome

- Lane ID: `search-production-health-stage2/backend-source-revision`
- Requirement IDs: exact Edge source digest; separate compatibility contract; HEAD/response exposure; exact pre-Search marker gate; response-derived evidence; HEAD-to-POST revision-race blocking; deterministic CI verification; canonical docs.
- Status: complete. No deploy, production network, live Auth, or live Search was performed.

## Revisions

- Base SHA: `9ed2a67e873ffeac160edcf00b44fbb04d2521f4`
- Implementation head SHA before this results receipt: `6c8253aebdf1fba9d36a98d77fd7420f6557b6bb`
- Commits:
  - `f2b6dde0b056cbd751c9a2cab34271542d490ba2` — implementation and tests
  - `3133b27b2bb989e3ef65030154f61fe00852b615` — Stage-2 handoff sync
  - `6c8253aebdf1fba9d36a98d77fd7420f6557b6bb` — canonical product contract sync

## Interface delivered

- `scripts/generate_event_search_revision.mjs`
  - default: regenerate `search-backend-revision.generated.ts`
  - `--check`: fail closed with `event_search_backend_revision_stale`
  - digest format: `sha256:<64 lowercase hex>`
  - deterministic sorted portable paths plus length-framed raw bytes; excludes the generated revision module and `*.test.*`.
- Edge HEAD exposes independent safe headers:
  - `X-KenigEvents-Search-Revision`
  - `X-KenigEvents-Search-Contract`
- Normal Search receipts expose independent fields:
  - `search_backend_revision`
  - `search_contract_version`
- Deploy markers and pre-Search HEAD probes require exact digest syntax and equality.
- The already-received Search response is authoritative after the HEAD gate. A deployment race becomes `BLOCKED_RELEASE_NOT_ACTIVE`, causes no retry, and cannot create a product incident.
- Evidence and aggregate summaries always use response `search_backend_revision`, including schedule/manual runs without an expected marker.

## Test evidence

Red-first regression:

- `node --experimental-strip-types --test site/tests/search-production-health-backend-revision.test.mjs`
  - initially failed with `ERR_MODULE_NOT_FOUND` for the generator.

Green validation:

- `npm run test:search-production-health` — 131/131 passed.
- `npm run test:search-e2e-harness` — 31/31 passed.
- `node --test supabase/functions/event-search/canary-contract.test.mjs supabase/functions/event-search/google-quota.test.mjs supabase/functions/event-search/occurrence-families.test.mjs` — 26/26 passed.
- Focused revision/journey/workflow suite after the race and safe-header fixes — 60/60 passed.
- `node scripts/generate_event_search_revision.mjs --check` — passed, revision `sha256:7ab0bd272925e959531bdcf679e995c65ee672165bd05788d51f7d13d35875d7`.
- `git diff --check` — passed.

## Changed files

- `.github/workflows/ci.yaml`
- `CHANGELOG.md`
- `docs/features/static-site-pages/smart-vector-search/README.md`
- `docs/features/static-site-pages/smart-vector-search/stage-2-production-health-handoff.md`
- `docs/features/unsigned-personalization/authorized-event-search.md`
- `docs/operations/release-governance.md`
- `scripts/generate_event_search_revision.mjs`
- `site/e2e/search/acceptance.mjs`
- `site/e2e/search/adapters/runtime-probe.mjs`
- `site/e2e/search/evidence.mjs`
- `site/e2e/search/production-health-journey.mjs`
- `site/e2e/search/production-health-planner.mjs`
- `site/e2e/search/production-health-report-plan-cli.mjs`
- `site/e2e/search/production-health-run.mjs`
- `site/e2e/search/search-backend-release-probe-cli.mjs`
- `site/e2e/search/search-backend-release-probe.mjs`
- `site/tests/search-e2e-workflow-contract.test.mjs`
- `site/tests/search-production-health-backend-revision.test.mjs`
- `site/tests/search-production-health-deploy-marker.test.mjs`
- `site/tests/search-production-health-journey.test.mjs`
- `site/tests/search-production-health-planner.test.mjs`
- `site/tests/search-production-health-report-cli.test.mjs`
- `supabase/functions/event-search/canary-contract.test.mjs`
- `supabase/functions/event-search/index.ts`
- `supabase/functions/event-search/search-backend-revision.generated.ts`

## Risks / follow-up

- The revision is deliberately a repository-derived deployable-source digest, not a provider deployment ID, git SHA, or compiled-byte hash.
- Release operators must regenerate and commit the generated constant before deploying Edge changes; default PR CI now enforces `--check`.
- Live Edge deployment and live probe validation remain outside this lane by instruction.
