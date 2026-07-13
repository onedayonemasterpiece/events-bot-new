# Medallion visual QA: exhaustive target-surface capture

> **Status:** mandatory public-release QA gate for the **static site only**; planned, not yet completed. The existing medallion lab screenshots and asset-load smoke are useful evidence, but they do not prove cleanliness on every real static-site surface where medallions are rendered.
> **Owner:** release UI/visual acceptance. The canonical visual and semantic contract remains [Event token medallions](event-token-medallions.md).

## Release requirement

At the release-candidate SHA, Playwright must discover and capture **every actual static-site location where a medallion is visible**, not only the dedicated laboratory page or a hand-picked happy-path event.

Telegram custom-emoji medallions are explicitly outside this QA requirement. Their rendering/runtime QA remains a separate feature concern and does not enter the static-site M1-QA inventory, screenshots or GO verdict.

The evidence must prove that every rendered medallion:

- is fully visible and not clipped by its own frame, row, card, section or viewport;
- has no dirty raster matte, dark/light alpha fringe, rectangular background, broken or abruptly cut shadow;
- has no shadow/ring clipped at an edge and no accidental `overflow: hidden` crop;
- keeps the intended circle/pill/composite geometry, optical centering and sufficient internal padding;
- remains legible against the actual surrounding background and next to neighbouring medallions;
- does not overlap the title, poster, CTA, text or another token;
- does not create horizontal page overflow or an unintended scrolling chip row;
- loads the intended SVG or lightweight WebP, with a valid fallback where the contract requires one;
- preserves the expected accessible name and does not render a duplicate token for the same identity.

This is a separate blocking gate. A successful build, `38/38` image-load check or one clean `/lab/medallions/` screenshot cannot close it.

## Inventory: what “every actual location” means

The Playwright run builds its inventory from the frozen RC output and current renderer graph rather than from a manually maintained URL shortlist:

1. Scan the full generated public HTML tree for the medallion container/token selectors and record every matching URL, token slug/kind and count.
2. Cross-check the source tree for every medallion renderer/component use. An unaccounted component invocation or DOM selector fails the inventory step.
3. Capture every generated event-detail URL that actually contains one or more medallions. Do not sample only one page per organizer: combinations, wrapping and surrounding content can create page-specific clipping.
4. Capture `/lab/medallions/` as the exhaustive isolated asset sheet, including organizer/venue, festival/program, source and Pushkin-card variants present in the RC manifests.
5. Assert that listing, search, related-card and personal-feed surfaces contain no medallion row while they remain outside the approved P0 scope. If a release candidate starts rendering medallions there, those URLs automatically become positive capture targets and need product approval.

The run emits a machine-readable `surface-inventory.json`. The gate fails when:

- an inventoried medallion URL has no screenshot;
- a screenshot target has no inventory row;
- the page contains a medallion unknown to the frozen manifests/configuration;
- an approved manifest entry has neither an isolated lab capture nor an explicit `not_rendered_in_rc` reason;
- a newly introduced rendering surface is silently excluded.

## Required Playwright capture matrix

### Static-site product pages

For **every** RC URL containing medallions:

| Viewport | Required evidence |
|---|---|
| Mobile `390×844` | Full contextual page screenshot plus a close section-crop screenshot of each medallion section after fonts and lazy images settle |
| Desktop `1440×1000` | Full contextual page screenshot plus a close section-crop screenshot of each medallion section |

For each distinct medallion layout/token combination, also capture the tablet/breakpoint state at `768×1024`. The laboratory page additionally runs narrow-width boundary checks at `320px` and `375px` so overflow/clipping is discovered before it reaches a real-device review.

The capture waits for:

- `document.fonts.ready`;
- every medallion image to report `complete && naturalWidth > 0`;
- the section to be scrolled into view so lazy assets are decoded;
- two stable animation frames with unchanged section bounds;
- no relevant console error or failed asset request.

Animations and transitions are disabled for deterministic evidence. The full-page screenshot does not replace the token/section crop: both context and edge detail are required to identify clipped shadows and alpha contamination.

## Visual verdict and defect handling

Each target receives one verdict:

- `pass` — clean without qualification;
- `fail_asset` — dirty crop/matte/alpha/shadow or unreadable artwork;
- `fail_layout` — clipping, overflow, overlap, wrong size/wrapping or surrounding-surface conflict;
- `fail_loading` — broken/missing/wrong primary or fallback asset;
- `fail_semantics` — wrong/duplicate medallion or accessible label;
- `blocked_capture` — target could not be rendered; this blocks release and is not a pass.

Automated bounding-box, overflow, image-load and screenshot-diff assertions are necessary but do not replace visual review. Every failed screenshot is routed to the owning asset, alias/data or UI-layout task; after the fix, rerun the affected target and the full inventory check. Do not hide a dirty medallion for one event as the default repair unless the source is genuinely ambiguous and fail-closed omission is the accepted product outcome.

## Evidence pack

Store the ignored RC evidence under an artifact directory such as:

```text
artifacts/codex/medallion-visual-qa/<rc-sha>/
  surface-inventory.json
  verdicts.json
  static/mobile/<url-key>--full.png
  static/mobile/<url-key>--medallions.png
  static/desktop/<url-key>--full.png
  static/desktop/<url-key>--medallions.png
  static/tablet/<layout-key>.png
  lab/<viewport>.png
  console-network.json
  summary.md
```

Every inventory/verdict row records at least:

- RC commit SHA and preview/build id;
- canonical static-site URL;
- page/template/surface kind and event id where applicable;
- viewport, browser/version and screenshot path;
- medallion slugs/kinds, asset URLs and rendered bounding boxes;
- image load/natural-size result, overflow result and console/network failures;
- reviewer verdict, defect link and rerun evidence.

## Release acceptance

- [ ] The RC inventory accounts for 100% of static-site renderer invocations and generated pages with medallions.
- [ ] Every actual static target has required mobile/desktop Playwright context and section screenshots; each distinct layout also has tablet/boundary evidence.
- [ ] Every RC static-site manifest medallion has an isolated lab capture or an explicit reviewed `not_rendered_in_rc` record.
- [ ] Zero `fail_*`, `blocked_capture`, missing screenshots, broken assets, clipped medallions/shadows, dirty alpha/mattes, overlaps or horizontal overflow remain.
- [ ] Negative assertions prove that unapproved listing/search/related/personal-feed surfaces do not display medallions.
- [ ] Final owner review is recorded against the immutable RC SHA/build id; evidence from an older CSS/asset/manifest commit does not carry forward silently.

## Related documentation

- [Event token medallions](event-token-medallions.md)
- [Astro preview and validation](astro-preview.md)
- [E2E scenario index](../../operations/e2e-scenarios.md)
- [Static personal announcements release checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
