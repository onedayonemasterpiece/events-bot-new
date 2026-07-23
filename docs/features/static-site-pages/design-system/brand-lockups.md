# Brand lockups and lettering

> **Normative status:** approved R3 desktop / R4 mobile optical adaptation.

## Fixed content and hierarchy

The lockup always contains:

1. umbrella endorsement `Полюбить Калининград`;
2. outlined service wordmark `Анонсы` from `site/public/brand/announcements-wordmark-ui.svg`.

The endorsement remains HTML text for accessibility, localization and crisp small text. `Анонсы` is a one-colour outline SVG with one wide `о`; do not typeset it with a normal font or replace it with a mechanically stretched glyph.

`site/src/components/brand/AnnouncementsLockup.astro` is the shared content source. It exposes `desktop` and `mobile` optical variants. The variants share assets and hierarchy, but deliberately use different line breaks and proportions.

## Desktop lockup

| Property | Contract |
|---|---:|
| Tag | `240×88px` |
| Fill / shadow | leather WebP over `#98401f` fallback / none |
| Radius | `0 0 12px 12px` |
| Padding | `18px 24px 16px` |
| Internal rows | `12px auto`, gap `4px` |
| Endorsement | one line, `11/12px`, weight `600`, tracking `0.08em`, uppercase |
| Wordmark | `192px` wide |
| Header bar / overlap | `56px` / `31px` |

The lockup remains the same size from `1024px` through `1920px`; elastic whitespace, not logo scaling, absorbs viewport changes. It anchors to the shared left container edge while the five-link navigation anchors right.

The desktop material layer is
`site/public/assets/ui/desktop-head-leather.webp`, a deterministic `30:11`
crop from the supplied `head-desctop-skin.png`. It changes only the material:
the `240×88` geometry, radius, padding, live endorsement and SVG wordmark stay
unchanged. The real CSS background remains solid `#98401f`; the WebP is an
enhancement painted above it. Therefore the tag is immediately legible before
the image decodes and remains identical in hierarchy if the asset fails. Crop
coordinates, hashes and the Pillow recipe live beside the asset in
`desktop-head-leather.metadata.json`.

## Mobile lockup

The mobile tag is an interaction handle, not a compressed desktop logo. Its extra height makes the drawer relationship legible and gives the small endorsement enough air.

| Property | Contract |
|---|---:|
| Tag | `128×96px` plus `env(safe-area-inset-top)` |
| Visible closed position | top `-7px`; bottom about `89px` without safe-area inset |
| Fill | solid `#98401f` |
| Contextual shadow | `0 12px 28px rgba(72,45,25,.24)` |
| Radius | `0 0 12px 12px` |
| Padding | `21px 12px 16px` before safe-area compensation |
| Lockup | `104px` wide; rows `18px auto`; gap `6px` |
| Endorsement | two explicit lines, `8/9px`, weight `600`, tracking `0.09em`, uppercase, opacity `0.9` |
| Wordmark | `104px` wide, about `20px` visual height |

The two endorsement lines are an intentional mobile composition, not accidental wrapping. Never compress them to one clipped line. The wordmark retains the wide `о` and is not proportionally narrowed to match the tall container.

The prior slow side-to-side title sway is removed. A permanent identity should remain optically anchored while the whole drawer already provides the interaction motion. Hover changes only the solid fill; opening changes only the contextual shadow and drawer position.

## Clear space and prohibited changes

- Keep at least `12px` clear space at the mobile sides and `24px` at desktop sides.
- On desktop the approved `240×88` hanging tag uses the viewport gutter
  `clamp(24px, 3vw, 48px)`, rather than the centered content container. This
  keeps it near the viewport frame at `1536–1920px` while the menu remains
  aligned to the same right gutter. Do not move the tag to the `1440px`
  content max-width: at Full HD that would push it inward to `x=240px`.
- Keep `4–5px` vertical separation between endorsement architecture and service wordmark.
- Do not add a separate PK monogram inside the full-name tag.
- Do not use gradient, bevel, inner cream tile, stroke or outline around the wordmark.
- Do not crop stitching, phone chrome or unrelated page background into the
  desktop leather material.
- Do not center the two tiers or scale `Анонсы` independently per viewport.
- Do not animate letters or infer navigation active state from event taxonomy.

## Accessibility

The outer tag link/summary owns the accessible action label. The outlined wordmark SVG is decorative (`aria-hidden`). The summary remains a native `<summary>` with keyboard activation and at least a `44px` target. Exact listing pages may expose one `aria-current="page"`; individual events expose none.

## Optional section badge

`EventLayout` may receive one optional `headerBadge` for a real navigation
section. It is rendered by the shared header beside that desktop navigation
link and, on mobile, on the discovery handle and matching drawer link. This is
status chrome, not part of `AnnouncementsLockup`: it must not change the tag,
wordmark geometry or base-aware link destinations. The desktop instance owns
live accessible copy; mobile duplicates are visual mirrors (`aria-hidden`) to
avoid repeated announcements. Red communicates unseen/urgent content only
with text such as `3 новых`; the soft neutral state may say `загляните`.
When the optional badge extension is active, its matching mobile drawer link
mirrors `headerCurrent` with `aria-current="page"`; responsive prototype
layouts may compact shared gap/padding locally or switch to the shared drawer,
but must never hide every non-current global destination.
