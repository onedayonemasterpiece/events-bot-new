# Desktop header concepts — 2026-07-14

> **Status:** noindex choice lab; not a production-header decision. Mobile remains unchanged.
>
> **Routes:** `/lab/header-desktop/`, `/lab/header-desktop/examples/{text-tag|logo-eyelet|signature-tab}/` and `/lab/header-desktop/listing/`.

## Product problem

The accepted mobile navigation uses a terracotta tag that protrudes below the top edge and opens the discovery rail. Desktop should preserve that recognisable silhouette without hiding five primary destinations behind a menu. The second design round also resolves three review issues: the two-colour mark previously needed a cream inset tile, the `1180px` lab grid made navigation look shifted left on wide screens, and the event-detail control incorrectly highlighted `Выставки`.

The desktop contract is now:

- keep all five global discovery destinations visible and right-aligned;
- use a `56px` working bar with link hit areas of at least `44px`;
- keep the tag as a home link, not as a redundant desktop menu toggle;
- use the existing mark as a one-colour white SVG directly on terracotta, without an inset card, outline or backing tile;
- let the tag overlap the hero by a bounded `12–22px` rather than expanding the header;
- use a responsive header container `min(1440px, 100% - clamp(48px, 5vw, 96px))`;
- expose `aria-current="page"` and a visible indicator only for an exact current listing destination;
- expose no active primary destination on an individual event, home or search page;
- keep the existing mobile `<details>` drawer/tag implementation unchanged.

## Research and UX decisions

### Persistent navigation and alignment

- Carbon treats the header as persistent identity plus global navigation and orders product identity before navigation. For this five-link, no-utility state, a right-aligned navigation cluster gives the light text links a stable counterweight to the heavy tag: <https://carbondesignsystem.com/components/UI-shell-header/usage/>.
- USWDS makes both logo allocation and header maximum width explicit layout tokens, supporting a single shared container rather than independent floating offsets: <https://designsystem.digital.gov/components/header/>.
- GOV.UK separates the home-linked service identity from service navigation and recommends only the most important top-level links: <https://design-system.service.gov.uk/patterns/navigate-a-service/>.

The implementation therefore removes variant-specific `margin-left` values and the absolute-positioned brand group. Brand and navigation participate in one flex row; `margin-left:auto` anchors the menu to the container's right edge. The old `1180px` cap is retained elsewhere in production until a production layout decision, but the design lab uses `1440px` because `1180px` left `370px` outer margins at `1920px` and visually pulled the menu toward the middle.

### Current-state semantics

W3C ARIA26 defines `aria-current` as the item that actually represents the current page; authors should mark only one current item in a set: <https://www.w3.org/WAI/WCAG21/Techniques/aria/ARIA26>. GOV.UK further distinguishes an exact `current` page from an `active` ancestor/group: <https://design-system.service.gov.uk/components/service-navigation/>.

| Page context | Visible menu state | Semantics |
|---|---|---|
| Home | No highlighted destination | Tag/home identity already establishes location. |
| Exact date/category listing (`/vystavki/`, `/segodnya/`) | Matching destination gets stronger text plus a `2px` underline | Matching link has `aria-current="page"`. |
| Individual event | No highlighted destination | Event type is metadata, not proof that the user is on the corresponding listing URL. |
| Search | No highlighted destination | Search is outside this five-link destination set. |

The event-detail routes are the default comparison context and intentionally contain no `aria-current`. `/lab/header-desktop/listing/` is the separate exact-match control.

## Spatial contract

| Viewport | Container result | Outer gutter | Brand treatment | Navigation |
|---|---:|---:|---|---|
| `1024px` | about `973px` | about `26px` each | `72×78px` eyelet; redundant wordmark hidden | right edge at the container boundary; compact `11px` inline link padding |
| `1280px` | `1216px` | `32px` each | eyelet plus wordmark | right-aligned; normal `16px` inline padding |
| `1536px` | `1440px` | `48px` each | eyelet plus wordmark | right-aligned on the wide shared grid |
| `1920px` | `1440px` | `240px` each | same stable brand group | right-aligned inside the capped readable composition |

Shared tokens:

- bar: `56px`;
- eyelet: `72×78px`, `22px` overlap;
- brand-to-navigation flex gap: `clamp(32px, 4vw, 64px)`;
- link gap: `4px` normally, `0` at `<=1120px`;
- link padding: `16px` inline normally, `11px` at `<=1120px`;
- link minimum height: `44px`;
- white mark: `51×44px`, optically lifted `4px` because the tag extends below the working row.

## Compared variants

### 01 — Text tag (`text-tag`)

- `56px` bar; `222×76px` terracotta text tag; about `20px` overlap.
- Strongest literal continuity with mobile.
- Main weakness: it still does not create an independent brand symbol.

### 02 — Logo eyelet (`logo-eyelet`) — recommended

- `56px` bar; `72×78px` terracotta eyelet; `22px` overlap.
- `brand-mark-white.svg` preserves the accepted geometry as one white silhouette directly on the colored tag. There is no inner cream tile.
- The full name remains a calm two-line wordmark at `>=1121px` and yields before navigation is compressed.
- Best separation of permanent identity, readable name and visible navigation.

### 03 — Signature tab (`signature-tab`)

- `56px` bar; `248×68px` combined mark-and-name tag; `12px` overlap.
- Uses the same white mark without an inset tile.
- Strongest immediate identity, but reserves the most horizontal space and is visually heavier.

## Recommendation

Continue with **02 / Logo eyelet**, right-align the menu, and keep active state contextual.

The tag and menu should not be forced into symmetrical halves: they have different visual weights. The correct balance is asymmetric but anchored — tag at the shared left grid edge, navigation at the shared right grid edge, elastic whitespace between them. This preserves the earlier comfortable right alignment while giving the new mark a clear brand role.

Do not implement the tag as an active-section slider. Do not infer current navigation from event taxonomy. Do not use absolute brand positioning plus per-variant `margin-left`, and do not shrink target sizes to preserve the desktop row.

## Consultant evidence

The neutral briefs, screenshots and raw consultant reports are stored under `artifacts/codex/static-desktop-header-20260714/` as ignored runtime evidence.

- **Round 2 — Gemini:** `Gemini 3.1 Pro (High)` through Antigravity `agy` completed successfully on 2026-07-14. It selected the refined logo-eyelet, explicitly recommended right alignment, a `1440px` cap with responsive gutters, a `72×78px` tag, white mark without inset, flex-flow positioning, and no event-detail/search active state. Raw report: `gemini-layout-round2-review.md`.
- **Round 1 — Gemini:** `Gemini 3.1 Pro (High)` through the project Antigravity runner selected the logo-bearing tag and recommended a `48–56px` bar, bounded overlap and hiding secondary wordmark before squeezing navigation. Raw report: `gemini-antigravity-review.md`.
- **Round 1 exact legacy CLI probe:** `gemini-3.1-pro-preview` through `gemini-legacy` returned `IneligibleTierError / UNSUPPORTED_CLIENT` and instructed migration to Antigravity. It was not substituted with a lower model.
- **Round 1 a-opus:** `a-opus` returned `Individual quota reached`; Claude Code `Opus` was logged out. This remains blocker evidence, not a lower-class review.

## Acceptance grid

| Check | Contract |
|---|---|
| `1920×1080` | Header uses the `1440px` cap; tag and menu occupy opposite grid edges. |
| `1536×864` | `48px` outer gutters, wordmark and all five destinations fit on one `56px` bar. |
| `1280×800` | Brand/name and navigation retain a clear elastic gap with no collision. |
| `1024×768` | Eyelet remains, redundant wordmark hides, five links remain visible and right-aligned. |
| Event detail | No navigation link has `aria-current`; no visible underline is forced by event type. |
| Listing | Exactly one link has `aria-current="page"`, stronger text and a visible underline. |
| Keyboard | Every functional link has a visible `3px` focus ring; the duplicate visual wordmark is not in the tab sequence. |
| Reduced motion | Current-state transition is disabled under `prefers-reduced-motion`. |

## Production integration boundary

This branch still adds only noindex lab routes, the white lab mark and validation. It does **not** change `site/src/layouts/EventLayout.astro`, production event/listing routes or the mobile drawer. After product selection, promote the accepted mark and spatial tokens into the shared header, derive exact current state from route identity, test normal/listing/event/search pages at the acceptance grid, and reconcile the production `1180px` page shell separately rather than silently changing every page width.
