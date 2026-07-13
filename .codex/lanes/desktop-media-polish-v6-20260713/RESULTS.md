# Desktop event media polish v6 — integration results

## Status
ready-for-commit

## Branch
`feature/event-page-desktop-media-polish-v6-20260713`

## Base SHA
`ae7c61e810091a024af2f885e873bb559a38c55a`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Public Chromium records Editorial internal Y at `+64`, `+4.3`, `-58.8` with zero horizontal overflow and correct downward-reading direction. |
| R02 | Done | Known `5658/image_assets[4]` opens as gallery source `4`, `5 / 7`, `object-fit:contain`, `ocr_text`. |
| R03 | Done | Public rail has six cells: five previews plus `+2`; the count cell opens source/index `5`, counter `6 / 7`; no duplicate floating photo pill. |
| R04 | Done | Split CTA is sticky at `top:85px`, below the `73px` header, remains there through the long text, masks the `12px` header gap so clipped article text cannot bleed above it, and exits at the related-section containing-block boundary. |
| R05 | Done | Split primary poster exactly matches its `720×827` media viewport with `contain`; forced OCR source `2` also opens fullscreen with `contain`. |
| R06 | Done | Public related-card audit shows visual media at square-to-`4:3` with `cover`, document/OCR media with `contain`, aligned card bottoms, and no lab attributes below `1024px`. |
| R07 | Done | Gemini 3.1 Pro (High) used the installed Chromium plus all eight public top/related captures and key gallery/sticky captures; corrected verdict: no blocker, Editorial Photo preferred for photos, Split OCR preferred for posters. |

## Verification

- `npm run build:preview` generated 442 pages with `PREVIEW_BUILD_ID=preview-20260713t-desktop-media-polish-v6`.
- `npm run check:preview` passed with the public personalization environment mapped into the preview build.
- The final v3 Astro run again generated all `442` pages. Its post-build duplicate copy hit host-disk `EIO`, so the already complete base-path build was reparented into `dist/<preview-id>/` with same-filesystem renames; the retained standalone `check-preview-final-v3.log` then passed against that exact published tree.
- Local and public Playwright acceptance both report `failures: []` at the interaction/geometry gate; desktop viewport matrix: `1024×768`, `1440×650`, `1920×600`, `1920×1080`; mobile-isolation probe: `390×844` root hidden and no lab attribute leaks.
- The post-review public polish gate also reports `failures: []`: Split CTA top `85px`, opaque `13.5938px` paper pseudo-mask plus `-12px` paper shadow, and a forced-failed remote visual image exposing the neutral gradient placeholder instead of graphite.
- Public HTTP returned `200` for the overview and all eight direct scenario pages; generated CSS/JS also returned `200`.
- Relevant preview HTML/CSS/JS upload was completed and publicly verified. Only the independent long stable `/ics/*.ics` mirroring tail was cancelled after `236/410` stable mirrors; it does not gate the desktop lab.
- Gemini evidence: `artifacts/codex/desktop-media-polish-v6-20260713/gemini/audit-corrected.md` with model/timestamps in `provenance-correction.txt`. Its slow third-party related-image finding is resolved with a neutral visual placeholder; the sampled VK image resolves successfully after network settling and is not a missing asset.

## Scope audit

- Changed behavior is confined to `site/src/components/lab/DesktopEventCleanPage.astro` and `/lab/event-desktop/**`.
- `EventHero.astro`, `EventLayout.astro` and production mobile components/styles are unchanged.
- V5 remains publicly available as rollback/reference evidence.
