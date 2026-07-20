# INC-2026-07-20-static-listing-desktop-preview-regression

Status: open
Severity: sev2
Service: KenigEvents static-site public review previews
Opened: 2026-07-20
Closed: —
Owners: static-site delivery / Codex incident owner
Related incidents: `INC-2026-07-15-static-desktop-template-regression.md`
Related docs: `docs/features/static-site-pages/listing-surfaces-v27-desktop-recovery.md`

## Summary

Public V26 review routes for Today, Tomorrow and Weekend rendered without the
listing design system, while Popular lost the sticky global header and left its
sticky city rail over scrolling content.

## User / Business Impact

- The owner could not review three date-listing prototypes because their
  consumer composition collapsed into native unstyled controls and oversized
  media.
- Popular remained readable at the top but lost global navigation and brand
  context after scrolling.
- A preview acceptance gate passed despite the user-visible desktop regression.

## Detection

The owner reported the public V26 URL and supplied a desktop screenshot.
Existing static checks validated Popular V26 markers but did not verify that
each listing route emitted the shared CSS bundle or that the header stayed
above the discovery rail.

## Timeline

- 2026-07-19: V26 mobile sticky group preview published.
- 2026-07-20: owner reports broken desktop Popular and date routes.
- 2026-07-20: HTTP, screenshot and built-asset comparison reproduces two
  independent regressions against V18/V22.

## Root Cause

1. Merge `98025aa1` resolved the divergent V22 (`f6c1ab96`) and main
   (`c587a0cf`) layout lineages by replacing the accepted V22
   `EventLayout.astro` with
   a lineage that omitted `design-system.css` and reset `.site-header` from
   sticky to relative.
2. Popular masked the missing shared import through its own route-level import;
   the date pages relied on a transitive component import which later vanished.
3. Preview checks asserted Popular markup/source strings, not compiled styles
   for every listing route or runtime sticky geometry.
4. The same merge removed the existing design-system source check from the
   `check:preview` command chain. That broad checker now also contains
   independent stale primitive assertions, so it cannot serve as this
   incident's only protection.

## Contributing Factors

- CSS ownership existed simultaneously at route, component and layout levels.
- Desktop preservation was asserted by source markers rather than the built
  four-route surface.

## Automation Contract

### Treat as regression guard when

- changing `EventLayout.astro`, `design-system.css`, listing routes/components,
  preview build/check scripts or Astro CSS imports.

### Affected surfaces

- `site/src/layouts/EventLayout.astro`
- `site/src/styles/design-system.css`
- `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`
- `site/scripts/check-preview.mjs`

### Mandatory checks before closure or deploy

- preview build and `npm run check:preview` pass;
- all four routes contain compiled listing CSS;
- desktop Playwright at 1366, 1536 and 1920 widths verifies the header at
  viewport top and the discovery rail directly beneath it after scroll;
- Today, Tomorrow and Weekend retain normal card geometry and no native filters;
- Popular 360/390/430 mobile V26 density/group context remains intact;
- keyboard focus exposes the skip link above the sticky header;
- public preview returns HTTP 200 without browser errors.

### Required evidence

- fix SHA and public V27 preview URLs;
- local and public Playwright JSON/screenshots;
- Gemini Pro critical acceptance artifact;
- Telegram review message receipt.

## Immediate Mitigation

Restore the design-system import and sticky header at the shared layout
boundary, then publish a new immutable preview instead of mutating V26.

## Corrective Actions

- Add compiled CSS coverage for all four listing routes.
- Restore a single shared sticky-header contract above listing rails.
- Add the affected import/header assertions directly to `check:preview`, while
  tracking restoration of the broader design-system checker separately.
- Raise and browser-test the focused skip link above the restored header after
  the first Gemini Pro review found the stacking regression.

## Follow-up Actions

- [ ] Close after public V27 desktop/mobile regression matrix and Telegram receipt.
- [ ] Keep compiled CSS route coverage in every future preview check.
- [ ] Repair the independent `check-design-system.mjs` assertion debt and then
  restore it to the package-level preview chain.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending immutable V27 public preview
- regression checks: pending
- post-deploy verification: pending

## Prevention

Preview acceptance now validates emitted CSS per route instead of trusting
transitive component imports or a single Popular source contract.
