# Tile mosaic v2 UI lane

- **Base:** `8b22af29008456ec125b1404055a4283ddb2b57a`
- **Head:** committed lane HEAD returned with this report (`agent/static-launch-tile-mosaic-v2/ui`)
- **Requirements:** R01–R16

## Files

- `site/src/components/launch/TileMosaicLaunch.astro`
- `site/src/pages/lab/launch/tile-mosaic/index.astro`
- `.codex/lanes/ui-v2/RESULTS.md`

The existing `site/public/assets/launch/PWA-icon.png` remains the single local brand source and was not modified.

## Outcome

- Desktop geometry now uses 12 square columns and six rows, with `--tile-width: calc((100svh - 5 * var(--tile-gap)) / 6)` and the same width/height. The stage begins at `top: 0; left: 36.5vw`; excess columns intentionally clip beyond the right viewport edge.
- The projection is inside an opaque `#020405` grid plane. A dedicated exact-period seam overlay guarantees that projected pixels never appear in gaps.
- Default `brand` mode crops the PWA source's pale outer field and bounds the leather squircle from 14–80svh. `cover` mode independently fills the complete grid for portrait/cathedral/landscape sources and honors focal position.
- Astro props expose `imageSrc`, `imageMode`, `focalX`, `focalY`. Runtime supports `?mosaicImage=`, `?mosaicMode=` (plus `mosaicImageMode` alias), focal params, and `tile-mosaic:set-image` with `{src, mode|imageMode, focalX, focalY}`. Existing URL validation and preview-prefix boundary remain intact.
- Header uses the cropped square PWA icon at the verdict's desktop size. H1, status, date, four-line description, and hidden email label exactly match the approved Russian copy.
- Desktop form follows the reference envelope: 581–649px total width, 320–368px field, 245–265px text-only button, 16px gap, 76–80px controls, terracotta material, glass input, and envelope SVG inside the input.
- Removed the eyebrow and carbon hatch. Irregular seeded turbulence, per-tile roughness loci, depth, global illumination, richer reveal states, and left ambience provide the material treatment.
- Static distribution is exact: 30 sealed, 11 dim, 9 sleeping, 19 revealed, 3 glint (41.7% / 27.8% dim+rest / 26.4% revealed / 4.2% glint). Dynamic selection uses comparable bands, sparse 1–4 tile updates, repeat avoidance, and reveal/glint caps.
- Desktop remains a clipped 100svh surface at viewports ≥1024×760. Mobile retains 6×12 flow, bounded brand projection, scrolling copy/form, 320px guardrail, pointer behavior, and reduced-motion static behavior.
- Stable QA hooks include `data-image-mode`, `data-tile-size-contract`, `data-seam-policy`, `data-opaque-seams`, `data-projection-surface`, grid column data, and all pre-existing selectors.

## Commands / validation

- UI/UX skill search invocation — helper pointer resolved to missing `/home/dev/src/ui-ux-pro-max/scripts/search.py`; applied the embedded skill guidance directly (focus, 44px+ controls, accessible label/status, reduced motion, mobile overflow protection).
- `@astrojs/compiler` `parse` + `transform` for both owned Astro files — **PASS**, no error diagnostics.
- R01–R16 deterministic source-contract assertions — **PASS**.
- `git diff --check` — **PASS**.
- Full `npm run build` was intentionally not completed in this sparse lane: its base omits `site/scripts/transport-fault-build-contract.mjs`; an isolated `/tmp` config then hit Astro entrypoint resolution outside the project root. After the integration owner issued the storage guard at ~185MB free in `/dev/shm`, no dist/full-build retry was made. Integration owns the canonical build after cherry-pick.

## Risks / merge notes

1. Run the QA-v2 browser matrix after cherry-pick: square geometry, `scrollHeight === innerHeight`, exact form sizes, seams, mode switching, mobile overflow, reduced motion, and 0/5/10s state captures.
2. The projection is deliberately a positioned child of `.mosaic__grid`: this lets the grid retain an opaque computed background while tiles reveal the image and the top seam layer masks every gap. Preserve this stacking order.
3. Generic runtime sources default to `cover`; explicitly pass `mode: 'brand'` only for already-compatible brand artwork.
4. Subscription markup hooks and transport behavior are preserved; this lane did not modify backend, tests, docs, or changelog.
