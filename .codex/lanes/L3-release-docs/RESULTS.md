# L3 release docs results

Branch: `agent/interest-clubs/l3-release-docs`

## Requirements

| Requirement | Status | Evidence |
|---|---|---|
| R01 — promote accepted research contract to implementation RC canon | Done | `docs/features/interest-clubs/README.md`, ADR-001, legacy backlog redirect, routes/index updates |
| R06 — staged release plan and explicit postrelease acquisition scenarios | Done | `docs/features/interest-clubs/release-plan.md`, including migration/bootstrap, shadow replay, static preview, canary, full rollout, rollback/catch-up, metrics and Hero Talk concept |

## Decisions recorded

- Owner confirmation is implementation RC GO only; production migration, Smart Update enablement and public promotion remain separate gates.
- Fly SQLite owns club identity/relation; Smart Update only hands off canonical changes; static site consumes an accepted disposable projection.
- LLM-first, grounded, fail-closed relation boundary; no positive Lite fallback.
- Stable identity/slug history, reviewer-only merge/split/slug changes, explicit co-hosting and linked/festival boundaries.
- Active/dormant/archive lifecycle, owner review workflow, bounded provenance and no participant/profile storage.
- Postrelease design backlog covers navigation, search, event detail, recommendations, editorial, deep links and clearly marks Hero Talk as an unimplemented product concept.

## Validation

- `docs/routes.yml` parsed with `python3`/PyYAML.
- Local relative-link existence check passed for all new feature docs and backlog redirects.
- `git diff --check` passed.
- No code, site, CHANGELOG, `.env`, production, provider or database state changed in this lane.
