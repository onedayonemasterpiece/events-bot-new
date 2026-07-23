# L01-R4 — desktop leather tag

## Decision

Selected `head-skin-desctop (2).png` (blank stitched leather) and retained the existing live `AnnouncementsLockup` DOM/SVG above it.

At the exact desktop tag geometry (`240 × 88 CSS px`), variant 2 is the safer and visually stronger implementation:

- the SVG wordmark remains sharp at device scale factors and future responsive scaling;
- the endorsement remains real DOM text and the anchor keeps its existing accessible name;
- no baked raster glyphs can double with or drift from the current lockup;
- the same complete leather silhouette, side stitches, lower seam, rounded foot and contact shadow from the supplied source remain visible.

Variant 1 was rejected only as the runtime lockup source: its lettering is baked into the bitmap and visibly softens at the small tag size. It remains useful as a visual reference.

## Implementation

- Runtime: `site/public/assets/ui/desktop-head-leather-r4.webp`, RGBA WebP, 960 × 352 (`30:11`, 4× the 240 × 88 CSS box).
- Source-derived master: `site/src/assets/ui/desktop-head-leather-r4-master.webp`, 1920 × 704.
- Provenance and checksums: `site/public/assets/ui/desktop-head-leather-r4.metadata.json`.
- `EventLayout.astro` now points the existing desktop-only CSS variable to the R4 asset.
- The existing immediate `#98401f` terracotta underpaint remains unchanged, so the live white lockup is readable before load and after an image error.
- Mobile CSS and assets were not changed.

## Visual evidence

Playwright comparison at 1440 px viewport, with both alternatives rendered at exactly 240 × 88 CSS px and `deviceScaleFactor: 2`:

- `artifacts/codex/L01-R4/desktop-leather-comparison-1440.png`
- SHA-256: `32cf3d1adcf5e4214231ecbda3e55122af5d3f54a5795b0b648cb4d3d4604921`

The selected live/SVG variant retains crisp letter edges while the source edging and lower shadow remain complete.

## Verification

```text
node --test site/tests/desktop-media-contract.test.mjs
4 passed, 0 failed

git diff --check
passed
```

A full local preview build was attempted, reached event-page generation, and then the shared runner returned `ENOSPC: no space left on device` (filesystem had only ~144 MiB free after removing this lane's partial output). This is an environment-capacity blocker rather than a compilation or implementation error; the integration worktree should perform the full build after shared-space cleanup.
