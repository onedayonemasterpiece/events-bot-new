# Lane L03 Results

## Status

Done.

- Lane ID: `L03`
- Requirement IDs: `R06`, `R07`
- Base SHA: `68576d5b70f57164c00386b05cff126586c3f700`
- Validated implementation head SHA: `cfda7e1bfd67780198f4fb0482da44643fddcf19`
- Branch: `agent/r3-desktop-media/L03`

The implementation SHA above is the tested product commit. This results record is
committed as its immediate child so that the record can contain the exact
validated head without a self-referential commit hash.

## Outcome

### R06 — desktop leather brand tag

- Locally cropped the supplied `head-desctop-skin.png`; no paid or remote image
  generation was used.
- Preserved the lower seam, both stitched side edges, rounded foot, and visible
  outer edging in a transparent 30:11 WebP.
- Kept the existing rendered desktop tag size at `240 × 88`.
- Added an immediate terracotta `#98401f` background fallback beneath the
  transparent asset, plus a restrained border, inset edge, and two-stage shadow.
- Recorded source/crop/output hashes and crop policy in colocated metadata.

### R07 — compact card media invariant

- Investigated the exact Goblin recommendation reachable from event `6529`
  (`event_id=6835`).
- Verified the immutable `691 × 1000` source has no baked bands and no OCR. The
  bands were layout-created by `contain` framing after stale
  `unknown_document` semantics.
- Added an exact-source-keyed reviewed 5:4 derivative and classification. It is
  applied only when the immutable source URL matches and reviewed
  `visual_only`/crop evidence is present.
- Switched server and runtime card framing to explicit `image_text_mode`:
  non-OCR `visual_only` media fills with `cover`; OCR/unknown document media
  remains fail-closed and is not weakened.
- Unified loaded/error shell state on all rendered card branches so the fallback
  cannot paint through a loaded image.

## Regression and browser evidence

Ignored evidence under `artifacts/codex/L03/`:

- `event-6529-desktop-tag.png`
  - SHA-256 `197ba9ae68db3f9004c9d2d31348c0085859e2efb1169d9d3ad152bdcbf36a49`
- `event-6529-desktop-tag-fallback.png`
  - SHA-256 `8112428d0015aeea2fcda37d7801446aac62c321e29aa18929cf1716ff1b6778`
- `event-6529-goblin-card.png`
  - SHA-256 `f47254a9915f3b42329338950d6c065498756254699919a485933992af2c4fef`
- `browser-evidence.json`
  - tag `240 × 88`, correct skin URL, terracotta background, border and shadow;
  - forced skin request failure retains the same geometry and white wordmark;
  - Goblin image natural size `1000 × 800`, rendered image and shell sizes equal,
    `object-fit: cover`, zero unused X/Y frame, loaded fallback hidden.

The base preview discovery manifest exports only the first 30 continuation
entries while Goblin is later in this base fixture. The browser gate therefore
injected the exact immutable Goblin source into the real event-6529 page through
the production `KenigEventsCreateEventCard` path (`fixtureInjected: true`).
Static data-contract tests additionally pin that event 6835 remains in event
6529's full related chain.

Incident regression checks from the known static event keyboard/media records
were preserved: no autofocus/navigation behavior changed, loaded media reserves
geometry, visual-only media has no contain bands, and OCR media remains on the
document-safe path.

## Commands and tests

- `PREVIEW_BUILD_ID=preview-20260723-r3-l03-desktop-media npm --prefix site run build:preview`
  - PASS, 383 pages generated.
- `node --test site/tests/desktop-media-contract.test.mjs site/tests/visual-keyboard-regressions.test.mjs site/tests/event-media-quality.test.mjs`
  - PASS, 21/21.
- `node --test site/tests/*.test.mjs`
  - PASS, 78/78; TAP saved to ignored
    `artifacts/codex/L03/site-tests.tap`.
- Playwright-core browser capture against the built preview using the installed
  Chromium executable.
  - PASS for tag normal state, forced asset-failure state, and exact Goblin
    runtime card.
- `git diff --check`
  - PASS.

## Changed files

- `site/public/assets/card-media/goblin-battle-reviewed-5x4.metadata.json`
- `site/public/assets/card-media/goblin-battle-reviewed-5x4.webp`
- `site/public/assets/ui/desktop-head-leather.metadata.json`
- `site/public/assets/ui/desktop-head-leather.webp`
- `site/src/components/EventCard.astro`
- `site/src/data/listingMediaOverrides.json`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/relatedCardLayout.mjs`
- `site/tests/desktop-media-contract.test.mjs`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `.codex/lanes/L03/RESULTS.md`

## Risks / integration notes

- `site/src/data/listingMediaOverrides.json` is a shared registry. If the
  integration branch already has entries, merge the `items` arrays rather than
  choosing one side wholesale.
- The override is deliberately keyed by the full immutable source URL. A future
  upstream source URL change will fail closed instead of silently applying this
  crop to different pixels.
- Per lane ownership, `docs/` and `CHANGELOG.md` were not modified; the
  integrator owns the shared documentation and changelog updates.
