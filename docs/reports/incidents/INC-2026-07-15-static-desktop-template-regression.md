# INC-2026-07-15 Static desktop template regression in production-integration preview

Status: open
Severity: sev2
Service: KenigEvents static-site production-integration preview
Opened: 2026-07-15
Closed: —
Owners: static-site integration
Related incidents: —
Related docs: `docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md`, `docs/features/static-site-pages/presentation-release-checklist.md`

## Summary

The full real-event preview claimed to integrate the accepted desktop event
templates but rendered the legacy production `EventHero + event-grid +
side-col` composition with a small layer of desktop CSS. It therefore looked
only loosely inspired by the fourteen-round accepted lab component. A
portrait `180×320` source was additionally routed into a full-width photo
surface by a one-boolean media-role check.

## User / Business Impact

- every generated desktop event page could diverge from the reviewed product contract;
- low-resolution portrait media could be enlarged into a blurred full-width hero;
- the public review link could not be used to approve production promotion;
- the reported Gemini acceptance did not cover the generated production routes.

The preview prefix is noindex and the production root was not promoted, which
limited external impact, but the candidate itself was user-visible and invalid.

## Detection

The product owner opened the first generated event (`5294`) and then event
`6815`. Both immediately exposed the mismatch. Existing checks validated three
convenient URLs and transport presence but never asserted that all generated
routes mounted the accepted component or that routing covered real media
geometry.

## Timeline

- 2026-07-15 17:xx UTC — full real-event preview published and incorrectly reported accepted.
- 2026-07-15 18:xx UTC — mismatch reported with event `5294` and `6815` screenshots.
- 2026-07-15 19:xx UTC — root cause confirmed by comparing the production route with the accepted lab component.
- 2026-07-15 19:4x UTC — exact component integration and geometry/semantic router implemented locally; full-catalog validation started.
- 2026-07-15 20:5x UTC — local replacement passed the `282/282` catalog gate, `4 × 3` viewport matrix, mobile isolation and interaction suite; public replacement build/consultant review remained pending.

## Root Cause

1. The accepted implementation remained a lab-only component, while the mass
   generation route kept the old production DOM and attempted to imitate the
   accepted design through CSS.
2. Desktop family selection used only `image_media_role`, without source
   geometry, resolution, OCR policy or classified poster semantics.
3. Acceptance tested a curated subset and a transport composition review, not
   every generated production event route.
4. A consultant review of a bounded composition question was incorrectly
   described as acceptance of the later full-catalog integration.

## Contributing Factors

- documentation still described the accepted component as lab-only;
- no build gate asserted exact production component identity;
- pending media semantic rows were treated inconsistently between lab and production;
- no mandatory random/representative real-event matrix was attached to the release claim.

## Automation Contract

### Treat as regression guard when

- changing `/sobytiya/[slug]`, desktop event components, media-role export,
  desktop routing, transport insertion or static preview generation;
- claiming that a lab-approved static-site layout is promoted to generated pages.

### Affected surfaces

- `site/src/pages/sobytiya/[slug].astro`;
- `site/src/components/DesktopEventPage.astro`;
- `site/src/lib/desktopEventPresentation.ts`;
- static preview build/deploy and consultant acceptance evidence;
- all future event pages at desktop widths `>=1024px`.

### Mandatory checks before closure or deploy

- `npm run build`, `npm run check:preview` and `npm run check:production-desktop`;
- generated HTML contract check for every event page;
- Playwright desktop run over the complete generated catalog with no legacy
  mobile composition visible, no missing H1/CTA and no horizontal overflow;
- representative screenshots at `1536×864`, `1920×1080` and `1440×650` for:
  qualified Editorial, low-resolution Split, portrait Split and classified
  poster companion;
- mobile isolation at `390×844`: desktop component hidden and mobile revision
  `v4` still visible;
- consultant review must open the newly generated production URLs/screenshots,
  not only lab fixtures or a different feature lane.

### Required evidence

- branch/SHA and public noindex preview prefix;
- full-catalog check artifact and representative screenshots;
- public HTTP and Playwright evidence for events `5294`, `6815`, `5658`, `4671`;
- explicit statement whether the consultant actually reviewed those generated URLs.

## Immediate Mitigation

Production-root promotion of the invalid preview was stopped. The replacement
is being built in an isolated fix branch from the reproducible integration SHA.

## Corrective Actions

- mount the exact accepted desktop component on every generated event route;
- retain the existing mobile v4 DOM and hide each surface at its own breakpoint;
- route by source geometry, resolution, OCR mode and classified identity-poster
  semantics, with portrait/low-resolution media failing to Split;
- insert transport additively inside the accepted long information flow;
- add a build-time all-page contract checker and full-catalog browser gate.

## Follow-up Actions

- [ ] Run the LLM media-role pass before future static generation and expose completion/coverage in build evidence.
- [ ] Add calibrated visual-quality/focal metadata so low-resolution and face-sensitive media can be routed without guesswork.
- [ ] Require consultant artifacts to list the exact reviewed URLs and viewport matrix.

## Release And Closure Evidence

- deployed SHA: pending commit/push
- deploy path: pending replacement noindex preview `preview-20260715t-production-desktop-contract-v2`
- regression checks: `npm run build` and `check:production-desktop` pass for `282/282`; full-catalog Playwright `282/282`, zero page errors; representative `4 × 3` matrix, mobile-v4 isolation and interaction suite pass
- post-deploy verification: pending public HTTP/Playwright and exact-URL Gemini Pro/Opus review

## Prevention

The accepted component is now a production component rather than a lab copy.
The release gate checks all generated pages and pins four real-event routing
specimens, so future CSS imitations or one-boolean routing cannot pass silently.
