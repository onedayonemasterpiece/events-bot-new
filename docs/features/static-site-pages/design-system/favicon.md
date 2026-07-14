# Favicon and small tag mark

> **Normative status:** choice round R5. The desktop/mobile full-name lockups remain approved; the final favicon has not yet been selected. Candidate A is temporarily installed as `site/public/favicon.svg` so ordinary preview pages expose a real favicon.

## Shared contract

At browser-tab size the full service name is not recoverable. Both current candidates reduce the identity to a transparent `64×64` SVG with one solid terracotta hanging-tag silhouette:

| Property | Contract |
|---|---|
| ViewBox | `0 0 64 64` |
| Background | transparent; no full-artboard rectangle |
| Tag | `x=6…58`, top flush at `y=0`, bottom at `y=63` |
| Top corners | square |
| Bottom corners | approximately `13px` radius |
| Tag colour | `#98401f` |
| Raster dependency | none |

Side transparency prevents the coloured shape from becoming a generic square app tile. The flush top and rounded bottom must continue to read as a hanging tag, not a bookmark floating inside another tile.

## Candidate A — tag + lower wide-`о`

Files:

- comparison master: `site/public/brand/favicon-tag-wide-o.svg`;
- temporarily installed copy: `site/public/favicon.svg`.

The white compound `о` spans `x=12…52`, `y=25…49`; its optical centre is `y=37`, deliberately below the artboard centre. The asymmetric free field—more air above, less below—transfers the approved full-size lockup principle instead of vertically centring the glyph. Its counter spans `x=19…45`, `y=31…43` and remains open at `16px`.

**Strength:** this is a service-specific mnemonic tied directly to the wide-`о` lettering in «Анонсы».

**Risk:** the glyph adds detail at the smallest size and depends on careful rasterisation; it must never drift back to geometric centring.

## Candidate B — tag only

File: `site/public/brand/favicon-tag-only.svg`.

This candidate removes the glyph completely and keeps only colour plus silhouette. It is the cleanest transfer of the physical-tag concept and remains especially stable at `16px`.

**Strength:** maximum simplicity, no closed-counter or off-centre-letter problem, exact continuity with the shared tag silhouette.

**Risk:** a plain hanging tag is less ownable and can be read as a generic bookmark/label without the full-name lockup nearby. Recognition therefore depends more heavily on repeated use of `#98401f` and the precise square-top/rounded-bottom geometry.

## Installation and comparison

Every static HTML surface uses the base-aware equivalent of:

```html
<link rel="icon" href="/favicon.svg" type="image/svg+xml" sizes="any">
```

Until product selection, `/favicon.svg` mirrors candidate A only as a practical installed preview—not as a final brand decision. Do not silently replace it with candidate B or introduce a third geometry.

Review routes:

- `/lab/design-system/` — side-by-side board at `16/32/64/128px`;
- `/lab/design-system/favicon/tag-o/` — candidate A actually installed in the browser tab;
- `/lab/design-system/favicon/tag-only/` — candidate B actually installed in the browser tab.

## Acceptance

For both candidates:

- `16px`: terracotta silhouette remains distinct from transparent sides;
- `32px`: square top and rounded bottom are both perceptible;
- light and dark browser chrome: the mark remains legible because it owns its coloured field;
- no cream plate, outline, raster image or full-canvas background is allowed.

For A, the `о` counter must remain open and the glyph must visibly occupy the lower field. For B, no accidental white path or hidden letter may remain.
