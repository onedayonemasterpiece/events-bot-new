# Lane R15-L06-TESTS-DOCS Results

## Status

Committed. Canonical docs, frozen evaluation fixture, source contracts and the
standalone browser journey are complete for this lane. Real Kaggle CPU canary,
immutable candidate browser execution and production-root enablement remain
explicitly pending integration/release gates.

## Lane and requirements

- Lane: `R15-L06-TESTS-DOCS`
- Requirement IDs: `R01`–`R10`
- Branch: `agent/unusual-r15/tests-docs`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/unusual-r15-tests-docs`
- Base SHA: `31b72b93153c094ca16cd564bfdc6b56c2031867`
- Head SHA (implementation commit): `479685ef`

The lane-results metadata is committed immediately after the implementation
commit; its final branch SHA is reported to the integrator at handoff.

## Delivered

- Added the canonical `docs/features/unusual-events/README.md` with:
  - exact 15-family taxonomy;
  - one shared pinned BGE-M3 event-vector contract for public related,
    unusual, family and presentation-concept support;
  - scorer, golden metrics and fail-closed approval gates;
  - atomic vector receipt/cache/last-good and migration suppression;
  - concept dedup and red-dot identity/acknowledgement semantics;
  - `provider_calls=0`, rollout, Kaggle evidence and rollback rules.
- Kept Gemini `search_v3` authorized Search intact and documented Gemini
  `related_v1` only as explicit rollback/comparison canary after BGE enablement.
- Added targeted canonical documentation for R01–R07 and synchronized docs
  indexes, routes, semantic retrieval, builder handoff, E2E index and
  `CHANGELOG.md`.
- Added `unusual_events_golden_v1.json`, derived only from the read-only
  `artifacts/codex/unusual-events-20260727` snapshot/review files. It has 57
  checked rows: 32 positives across all families, 20 hard negatives, 5
  non-events and 6 repeated series/concept groups. Every case includes the
  semantic evaluator's `eligible` and `frozen_tier` contract.
- Added Python fixture integrity tests, an integration-aware Node source
  contract across R01–R10 and a standalone Playwright journey for the mobile
  product/zero-provider contract.

## Evidence and commands run

```text
sha256sum artifacts/codex/unusual-events-20260727/prod-events.json \
  artifacts/codex/unusual-events-20260727/manual_future_taxonomy.csv
# prod-events.json:
# 9971db619edc59c347e4c9d35b473287d338bc59bb9b152bf284185787193b2d
# manual_future_taxonomy.csv:
# 769e67b3222dcf9713124dfa7845d01b8f542ebc925cdeb32dc051ca48365592

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_unusual_events_golden_contract.py
# 4 passed

node --check site/tests/unusual-events-source-contract.test.mjs
node --check site/tests/unusual-events.playwright.mjs
# both passed

node --test site/tests/unusual-events-source-contract.test.mjs
# valid TAP; 3 intentionally skipped in the isolated docs lane because the
# semantic, Astro and Favorites sentinels are absent. Read-only cross-worktree
# execution of the same assertions against the four current implementation
# lanes passed; integration must rerun the committed test without skips.

/home/dev/.codex/venvs/events-bot-new/bin/python - <<'PY'
# yaml.safe_load(docs/routes.yml), exact unusual route/status assertions,
# every fixture frozen_tier in the four-value public tier enum
PY
# passed

git diff --check
# passed
```

## Browser / canary status

- `site/tests/unusual-events.playwright.mjs` parses successfully.
- Browser execution is **not claimed**: this isolated worktree has no installed
  `playwright` module (`ERR_MODULE_NOT_FOUND`) and no immutable integrated
  candidate URL was produced by this lane.
- A real pinned-BGE Kaggle CPU canary is **pending**. No local fixture or source
  test is represented as encoded canary evidence, classifier approval, root
  enablement or production release.

## Risks and integration follow-up

1. Cherry-pick/merge the semantic, builder/share, Astro and Favorites/home lanes,
   then rerun `node --test site/tests/unusual-events-source-contract.test.mjs`;
   any partial integration no longer qualifies for the isolated-lane skip.
2. Run the Playwright journey on the immutable noindex candidate with
   `UNUSUAL_EVENTS_BASE_URL`; set `UNUSUAL_EXPECT_APPROVED=1` only when the real
   quality report is approved.
3. Attach Kaggle heartbeat/result, snapshot/vector/hash identities, cache and
   manifest downloads, `provider_calls=0`, `check:preview` and browser evidence
   before considering owner acceptance or production-root cutover.
4. The frozen tiers are editorial ground truth for measuring the first real
   canary; they are not a claim that the current unencoded fixture passed the
   classifier.

## Changed files

- `CHANGELOG.md`
- `docs/README.md`
- `docs/features/README.md`
- `docs/features/unusual-events/README.md`
- `docs/features/event-favorites-calendar/README.md`
- `docs/features/static-site-pages/README.md`
- `docs/features/static-site-pages/astro-preview.md`
- `docs/features/static-site-pages/image-framing.md`
- `docs/features/static-site-pages/mobile-shell.md`
- `docs/features/static-site-pages/service-sharing.md`
- `docs/features/unsigned-personalization/audience-admission-discovery.md`
- `docs/features/unsigned-personalization/semantic-vector-retrieval.md`
- `docs/operations/e2e-scenarios.md`
- `docs/operations/kaggle-static-site-builder.md`
- `docs/routes.yml`
- `tests/fixtures/unusual_events_golden_v1.json`
- `tests/test_unusual_events_golden_contract.py`
- `site/tests/unusual-events-source-contract.test.mjs`
- `site/tests/unusual-events.playwright.mjs`
- `.codex/lanes/R15-L06-TESTS-DOCS/RESULTS.md`
