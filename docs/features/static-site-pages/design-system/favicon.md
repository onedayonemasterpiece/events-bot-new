# Favicon and small tag mark

> **Normative status:** approved and installed as `site/public/favicon.svg`.

## Concept

At browser-tab size the full name is not recoverable. The favicon therefore reduces the identity to two durable cues:

1. a solid terracotta tag attached to the top edge;
2. the single expanded white `о` from the service lettering idea.

This replaces the older two-colour PK monogram favicon. It does not replace the umbrella brand in full lockups; it is the small service mark for constrained static-site surfaces.

## SVG geometry

| Property | Contract |
|---|---|
| File | `site/public/favicon.svg` |
| ViewBox | `0 0 64 64` |
| Background | transparent; no full-artboard rectangle |
| Tag | `x=6…58`, top flush at `y=0`, bottom at `y=63` |
| Top corners | square |
| Bottom corners | approximately `13px` radius |
| Tag colour | `#98401f` |
| Glyph | one compound white wide-`о`, `x=12…52`, optical centre `y=28`, `fill-rule="evenodd"` |
| Raster dependency | none |

The `о` uses a more open counter than the wordmark master. This is an optical small-size drawing, not a crop of the wordmark glyph. Its `6px` source side clear space equals `1.5px` at a `16px` render, while the optical centre is lifted `4px` above the artboard axis to compensate for the rounded-bottom mass. Side transparency prevents the colored tag from becoming a generic square app tile; the flush top and rounded bottom keep the hanging-tag silhouette recognizable.

## HTML installation

Every static HTML surface must use:

```html
<link rel="icon" href="/favicon.svg" type="image/svg+xml" sizes="any">
```

Use the base-aware equivalent in Astro. Do not add a cream background or inline raster fallback to the SVG. Cache-busting may be handled by the deployment path/version, not by duplicating assets.

## Small-size acceptance

- `16px`: terracotta silhouette remains distinct from the transparent sides; the `о` counter does not close.
- `32px`: square top and rounded bottom are both perceptible.
- `64px`: curves are smooth and the glyph is optically centered.
- Light and dark browser chrome: the mark remains legible because the tag owns its colored field.

The visual reference and faux installed browser tab live at `/lab/design-system/`.
