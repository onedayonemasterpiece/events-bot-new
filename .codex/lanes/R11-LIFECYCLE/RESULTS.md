# R11-LIFECYCLE Results

## Scope

- Lane: `R11-LIFECYCLE`
- Requirements: `R11-05`, `R11-06`
- Base SHA: `7c34d29a2ad65fc6532d934a49d4d48604f79e82`
- Implementation head SHA: `3b507fcbc1b5a45291d24c430a47c2a335b87045`

## Outcome

- Added one `isPopularEligible` predicate used by both mobile and desktop
  Popular selectors.
- Rejected cancelled, postponed, duplicate, merged, deleted, and inactive
  events.
- Made ranges eligible through `end_date` inclusively, before considering
  any opening-day timestamp; ended ranges are rejected.
- Made one-off eligibility deterministic from an explicit
  `currentDate`/`referenceIso`: future dates remain eligible and same-day
  events must not have elapsed.
- Added a generated-preview assertion that the union of desktop and both
  mobile Popular representations contains zero ineligible event IDs.
- Grouped occurrence dates by stable year/month. Visual month groups use
  commas and aria month groups use the human conjunction.
- Preserved the accepted `2, 9 ноября 19:00`,
  `4 ноября 17:00, 19:00`, and reciprocal-explicit-only family contracts.

## Changed files

- `site/src/lib/events.ts`
- `site/src/lib/eventOccurrences.ts`
- `site/scripts/check-preview.mjs`
- `site/tests/event-occurrences.test.mjs`
- `.codex/lanes/R11-LIFECYCLE/RESULTS.md`

## Validation evidence

- `npm ci`
  - completed; 267 packages installed.
- `node --experimental-strip-types --test tests/event-occurrences.test.mjs`
  - passed: 14 tests, 0 failures.
- `node --test tests/popular-desktop-listing.test.mjs`
  - passed: 3 tests, 0 failures.
- `PREVIEW_BUILD_ID=preview-r11-lifecycle npm run build:preview`
  - passed: 431 pages built.
- `PREVIEW_BUILD_ID=preview-r11-lifecycle npm run check:preview`
  - passed: 288 events, `strict_related=false`.
- `git diff --check`
  - passed.

## Risks

- `check-preview.mjs` mirrors the pure lifecycle rules because the generated
  artifact gate runs under plain Node rather than Astro's TypeScript module
  loader. Pure predicate tests and the generated zero-ineligible-ID assertion
  cover both sides of that boundary.
- The preview snapshot is intentionally evaluated at its exported build
  reference by default; callers can supply deterministic reference/current
  dates for freshness-sensitive builds and tests.
