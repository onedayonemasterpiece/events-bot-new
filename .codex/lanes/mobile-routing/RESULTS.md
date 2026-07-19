# Mobile routing lane result

## Scope

- rebased same-site dynamic event/detail, absolute/share and local-calendar URLs to the currently open static preview base;
- left foreign organizer/ticket origins unchanged;
- scoped personal-feed cache keys and payload validation by current base path;
- applied the shared boundary to personal/discovery rendering and Authorized Search final/vector cards;
- added generated-preview regression guards, canonical docs, changelog and incident contract updates.

No transport, KAUP, telephone CTA, icon or design-system files were changed.

## Validation

- `PREVIEW_BUILD_ID=preview-20260717t-static-personalization-v10-mobile-routing npm run build:preview` — passed, 373 routes / 303 event pages.
- `PREVIEW_BUILD_ID=preview-20260717t-static-personalization-v10-mobile-routing npm run check:preview` — passed, 303 events.
- `PREVIEW_BUILD_ID=preview-20260717t-static-personalization-v10-mobile-routing npm run check:production-desktop` — passed, 303 event pages.
- `git diff --check` — passed.
- local Playwright routing gate — passed: 36 events at `320×780` and 36 at `390×844`, five actual related-card navigations, zero failures; a compatible stale-v7 cache was consumed and its detail/share/calendar projections were rebased to v10; a foreign ticket URL stayed unchanged.

## Artifacts

Ignored, reproducible evidence:

- `artifacts/codex/static-site-v10-mobile-routing/mobile-routing-acceptance.cjs`
- `artifacts/codex/static-site-v10-mobile-routing/mobile-routing-evidence.json`
- `artifacts/codex/static-site-v10-mobile-routing/run.log`

The lane is source-complete but does not deploy or promote a public preview.
