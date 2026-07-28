# R15-RAIL-PLAYWRIGHT-FINAL results

- **Base SHA:** `11d8c9846432414020cc5201eb650f5cfbf38eba`
- **Validated implementation head:** `3346e3f16a93cc9da8538a87d511ea713b7680ac`
- **Final lane tip:** immediate child containing this record; reported in handoff.
- **Writable implementation scope:** `site/src/lib/mobileListingRailMedia.mjs`, `site/tests/mobile-listing-rail-media.test.mjs`, `site/tests/mobile-listing-rails.test.mjs`, `site/tests/unusual-events.playwright.mjs`.
- **Forbidden scope preserved:** no canonical docs, `CHANGELOG.md`, builder, manifest, route, or component edits.

## Result

1. Every event/asset-consistent, classified crop-safe `visual_only` event photo now resolves to the same horizontal `140×112` (`5:4`) `cover` window, independent of source orientation and gallery size.
2. Contradictory event OCR/unknown state, OCR assets, documents, unknown semantics, and unreviewed media always resolve to `contain`, even if an upstream selector requested adaptive cover.
3. Unusual Playwright uses explicit modes:
   - default `product`: requires only `UNUSUAL_EVENTS_BASE_URL` and never visits lab routes;
   - `lab`: requires only `UNUSUAL_EVENTS_LAB_BASE_URL` and runs the noindex red-dot scenario matrix;
   - `all`: runs both against their independent bases.

## Validation

```text
node --experimental-strip-types --test \
  tests/mobile-listing-rail-media.test.mjs \
  tests/mobile-listing-rails.test.mjs \
  tests/unusual-events.test.mjs
# 22 passed, 0 failed

node --check tests/unusual-events.playwright.mjs
# passed

git diff --check
# passed
```

Packaged exact-candidate product smoke (candidate has no unusual lab routes):

```text
UNUSUAL_EVENTS_PLAYWRIGHT_MODE=product \
UNUSUAL_EVENTS_BASE_URL=http://127.0.0.1:4331/_review/<token> \
node tests/unusual-events.playwright.mjs
# passed
# exact candidate /lab/unusual-unread/new-core/ returned 404 as expected
```

Separate local noindex-lab matrix:

```text
UNUSUAL_EVENTS_PLAYWRIGHT_MODE=lab \
UNUSUAL_EVENTS_LAB_BASE_URL=http://127.0.0.1:4332 \
node tests/unusual-events.playwright.mjs
# passed
# local /lab/unusual-unread/new-core/ returned 200
```

Real multi-image portrait rail acceptance on local Astro dev:

```text
/date-2026-09-06/, event 6823
3 gallery cells: each width=140, height=112, ratio=1.25,
object-fit=cover, reason=safe_visual_landscape_5x4
```

## Changed files

- `.codex/lanes/R15-RAIL-PLAYWRIGHT-FINAL/RESULTS.md`
- `site/src/lib/mobileListingRailMedia.mjs`
- `site/tests/mobile-listing-rail-media.test.mjs`
- `site/tests/mobile-listing-rails.test.mjs`
- `site/tests/unusual-events.playwright.mjs`

## Risks

- The exact candidate used for contract validation predates this resolver commit; it proves the product smoke no longer depends on stripped labs. Integration must rebuild its next candidate to carry the new multi-image portrait geometry.
