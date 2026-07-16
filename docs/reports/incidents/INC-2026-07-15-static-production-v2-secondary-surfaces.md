# INC-2026-07-15 Static production v2 secondary-surface regressions

Status: closed
Severity: sev2
Service: KenigEvents static event preview
Opened: 2026-07-15
Closed: 2026-07-15
Owners: static-site integration
Related incidents: `INC-2026-07-15-static-desktop-template-regression`
Related docs: `docs/features/static-site-pages/`, `docs/operations/release-governance.md`

## Summary

The v2 full-catalog preview preserved the accepted primary desktop shells, but its shared production integration regressed three adjacent surfaces: desktop “Смотрите дальше” card geometry, the rail transport illustration, and the mobile event presentation. The mobile route was based on an older production approximation instead of the accepted V8 implementation from `fd8766b1`.

## User / Business Impact

- desktop recommendation rows visibly changed media and card heights and lost the agreed crop/scale rules;
- a rail journey example no longer showed the train image, weakening the transport block’s primary visual cue;
- generated mobile event pages lagged behind the accepted V8 interaction and parallax behavior;
- the full generated preview therefore could not be treated as an integration acceptance build.

## Detection

- detected by direct user review of real generated event routes;
- the prior acceptance covered primary desktop routing but did not assert the three secondary contracts corpus-wide;
- consultant review was not a substitute for browser assertions against the generated production DOM.

## Timeline

- 2026-07-15: v2 full-catalog preview published for review.
- 2026-07-15: user reported variable recommendation geometry, missing train media and stale mobile UI.
- 2026-07-15: incident opened and v3 serial integration repair started.
- 2026-07-15: exact V8 source was integrated, desktop related rows were packed
  by a measured OCR crop interval, and the train breakpoint was removed.
- 2026-07-15: full local corpus and public noindex v3 acceptance passed; the
  production root was intentionally not changed.

## Root Cause

1. Related-card row normalization retained a `document-natural` escape hatch, so an OCR card could opt out of the row ratio and make media geometry vary.
2. The rail illustration was hidden below a container threshold that the accepted desktop split column did not reach.
3. Production mobile markup/styles were maintained independently from the accepted V8 lab implementation, allowing the implementation to drift despite a known accepted commit.

## Contributing Factors

- acceptance emphasized the primary desktop shell and did not encode adjacent surfaces as hard generated-page checks;
- shared lab/production behavior had duplicated selectors and styles;
- “no more than 20% OCR crop” was implemented as an opt-out instead of a row-packing/normalization constraint.

## Automation Contract

### Treat as regression guard when

- changing event recommendation cards, media roles or image geometry;
- changing rail/bus transport components or their container breakpoints;
- changing mobile event hero/parallax/actions or integrating lab revisions into production;
- running a full static-site preview generation.

### Affected surfaces

- `site/src/components/lab/DesktopEventPage.astro`
- `site/src/components/EventTransportSchedule.astro`
- `site/src/components/MobileEventProductionStyles.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/pages/sobytiya/[slug].astro`
- full-catalog generation and public preview deploy

### Mandatory checks before closure or deploy

- generated desktop recommendation rows have one media ratio per row and no letterbox fields;
- non-OCR related images always cover; OCR media uses complete fit when compatible and crops at most 20% only when excessively portrait;
- rail example visibly renders the train asset at desktop and mobile review widths;
- production mobile generated routes carry the exact V8 parallax/profile markers and pass V8 behavior checks;
- complete future-event build passes link integrity and representative desktop/mobile Playwright screenshots;
- the earlier accepted desktop routing regression contract remains green.

### Required evidence

- source commit containing the fix;
- full build event/page counts and check-preview result;
- public HTTP 200 and Playwright evidence for real desktop, rail and mobile routes;
- public noindex preview URL; production root remains unchanged without approval.

## Immediate Mitigation

- v2 remains a noindex preview and is not promoted to the production root;
- work is isolated in the existing pushed integration branch and a new v3 preview will replace the review link.

## Corrective Actions

- remove row-geometry opt-outs and encode the crop contract as normalized row behavior;
- make transport media responsive to the actual accepted column widths;
- transfer accepted mobile V8 source behavior from `fd8766b1` and bind production to the same profile.

## Follow-up Actions

- [x] Add generated-page assertions for recommendation geometry, transport media and mobile revision markers.
- [x] Keep accepted UI revisions in shared production components rather than hand-copied approximations.

## Release And Closure Evidence

- deployed code SHA: `a6e99977` (acceptance-check tip: `0eb677bb`)
- deploy path: `s3://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/`
- public URL: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/__preview/>
- regression checks: Astro built `352` pages / `282` event routes; production
  desktop router, rail directory and bus directory checks passed; local
  Playwright covered all `282` routes, `1090` related rows and `21` rail pages
  with `0` failures; three real mobile V8 cases plus related-card navigation
  passed.
- post-deploy verification: index, Garage, Pianissimo, OCR, rail, V8 lab and
  train asset returned HTTP `200`; public Chromium checked four desktop routes,
  `16` related rows and three mobile routes with zero console errors/overflow,
  exact V8 markers and moving parallax. The decoded mobile train measured
  `324×65.4px`; a related card navigated to its generated route in the same
  prefix. Preview remains `noindex`; stable `/ics/*` and production root were
  not modified.

## Prevention

- this record is a mandatory regression contract for subsequent static event page generations.
