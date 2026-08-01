# Region Talk cliché-ban results

## Status

- R01: **Partial** — all 23 confirmed candidates were selected for controlled
  backfill attempts; Archi.ru is corrected and delivered, while Gemini RPD and
  strict editorial/media gates leave an explicit retry/review tail.
- R02: **Done** — writer/critic, deterministic validation, readiness/planning,
  render and final caption all fail closed on the banned construction family.
- R03: **Done** — v9/output-v3/backfill-v3 invalidation, exact-URL force,
  URL+candidate-ID published protection, tests, docs and changelog are merged.

## Integration

- Base: `origin/main`
- Core PR: #184, merge `084d2f96c58af6d492ba20ca83a3d0ca03a4bb6b`
- Follow-up PR: #185, merge `a2228b70f0eb3b2ede79f9335a8d0a945a9dd1d2`
- Fly core release: v1855

## Verification

- Focused suite: `167 passed`
- Full Region Talk suite: `705 passed`
- Final checklist review: PASS
- Production audit: 23 confirmed candidates, zero banned-pattern matches
- Archi.ru operator delivery: message `33805`

## Remaining risk

Gemini RPD exhaustion deferred 12 rows; six more remain in explicit manual
grounding review and four await media materialization. The pipeline did not
weaken grounding, switch to a lower model, or publish any candidate.
