# Favicon and small tag mark

> **Normative status:** approved R6 and installed as `site/public/favicon.svg`. The selected mark is the transparent terracotta hanging tag with one lower-set white wide-`о`.

## Final concept

At browser-tab size the full service name is not recoverable. The favicon reduces the identity to three durable cues:

1. the solid terracotta hanging-tag silhouette;
2. a square top attached to the artboard edge and softly rounded bottom;
3. the expanded white `о` from the «Анонсы» lettering, optically weighted toward the lower field.

The bare-tag alternative was rejected because it became a generic bookmark/colour block at `16px`. The older two-colour `ПК` monogram was rejected because its multiple small shapes produced more noise and weaker continuity with the approved one-colour lockup.

## SVG geometry

| Property | Contract |
|---|---|
| Installed file | `site/public/favicon.svg` |
| Brand master | `site/public/brand/favicon-tag-wide-o.svg` |
| ViewBox | `0 0 64 64` |
| Background | transparent; no full-artboard rectangle |
| Tag | `x=6…58`, top flush at `y=0`, bottom at `y=63` |
| Top / bottom corners | square / approximately `13px` radius |
| Tag colour | `#98401f` |
| Glyph outer | `x=12…52`, `y=24…48`, centre `y=36` |
| Glyph counter | `x=19…45`, `y=30…42` |
| Glyph colour | `#ffffff`, compound path with `fill-rule="evenodd"` |
| Raster dependency | none |

The glyph centre is `4` source units below the artboard centre, so the mark remains visibly bottom-weighted without appearing to fall into the rounded base. The `24…48` vertical bounds map exactly to `6…12px` in a `16px` raster, avoiding an unnecessary fractional vertical shift at the primary browser-tab size. Side transparency keeps the coloured tag from becoming a generic square app tile.

## Installation

Every static HTML surface uses the base-aware equivalent of:

```html
<link rel="icon" href="/favicon.svg" type="image/svg+xml" sizes="any">
```

Do not replace the final master with the bare tag, restore the old monogram, vertically centre the `о`, or introduce a cream backing plate, outline or raster fallback.

## Acceptance

- `16px`: the terracotta silhouette remains distinct from transparent sides; the `о` counter stays open and its outer vertical bounds align to whole pixels.
- `32px`: square top, rounded bottom and lower visual gravity are perceptible.
- `64px`: curves remain smooth; top air is `24u`, bottom air is `15u` to the tag edge.
- Light and dark browser chrome: the white glyph remains legible because the tag owns its coloured field.
- The final SVG contains no `<rect>`, embedded raster or full-canvas background.

## Review record

Review routes remain available as QA/history:

- `/lab/design-system/` — final mark and rejected bare-tag control at `16/32/64/128px`;
- `/lab/design-system/favicon/tag-o/` — final mark actually installed in the browser tab;
- `/lab/design-system/favicon/tag-only/` — rejected control.

On 2026-07-14 product review selected the wide-`о` tag over both the bare tag and old monogram. A final optical-position board compared centres `y=37/36/35/34` at real CSS `16/20/32/64px`. Codex initially preferred `y=35` at display sizes; Gemini 3.1 Pro High preferred `y=36` because it preserved lower gravity while aligning `24…48` to the `16px` grid. The final synthesis selected `y=36` as the more appropriate favicon-first solution.

Ignored raw evidence: `artifacts/codex/favicon-final-choice-20260714/` and `artifacts/codex/favicon-optical-position-20260714/`.
