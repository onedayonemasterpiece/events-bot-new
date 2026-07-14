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
| Fill / shadow | `#98401f` / none |
| Radius | `0 0 12px 12px` |
| Padding | `18px 24px 16px` |
| Internal rows | `12px auto`, gap `4px` |
| Endorsement | one line, `11/12px`, weight `600`, tracking `0.08em`, uppercase |
| Wordmark | `192px` wide |
| Header bar / overlap | `56px` / `31px` |

The lockup remains the same size from `1024px` through `1920px`; elastic whitespace, not logo scaling, absorbs viewport changes. It anchors to the shared left container edge while the five-link navigation anchors right.

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
- Keep `4–5px` vertical separation between endorsement architecture and service wordmark.
- Do not add a separate PK monogram inside the full-name tag.
- Do not use gradient, bevel, inner cream tile, stroke or outline around the wordmark.
- Do not center the two tiers or scale `Анонсы` independently per viewport.
- Do not animate letters or infer navigation active state from event taxonomy.

## Accessibility

The outer tag link/summary owns the accessible action label. The outlined wordmark SVG is decorative (`aria-hidden`). The summary remains a native `<summary>` with keyboard activation and at least a `44px` target. Exact listing pages may expose one `aria-current="page"`; individual events expose none.
