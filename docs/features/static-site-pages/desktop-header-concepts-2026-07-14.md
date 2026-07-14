# Desktop header concepts — 2026-07-14

> **Status:** text-tag R3 approved as the static-site brand/header direction; noindex alternatives remain as decision history.
>
> **Routes:** `/lab/header-desktop/`, `/lab/header-desktop/examples/{text-tag|logo-eyelet|signature-tab}/` and `/lab/header-desktop/listing/`.

## Product problem

The accepted mobile navigation uses a terracotta tag that protrudes below the top edge and opens the discovery rail. Desktop should preserve that recognisable silhouette without hiding five primary destinations behind a menu. The second design round also resolves three review issues: the two-colour mark previously needed a cream inset tile, the `1180px` lab grid made navigation look shifted left on wide screens, and the event-detail control incorrectly highlighted `Выставки`.

The desktop contract is now:

- keep all five global discovery destinations visible and right-aligned;
- use a `56px` working bar with link hit areas of at least `44px`;
- keep the tag as a home link, not as a redundant desktop menu toggle;
- use the existing mark as a one-colour white SVG directly on terracotta, without an inset card, outline or backing tile;
- render the service word `Анонсы` with the working wide-`о` UI wordmark while keeping `Полюбить Калининград` as calm HTML endorsement text;
- let the evaluated tag overlap the hero by a bounded `11–31px` rather than expanding the header;
- use a responsive header container `min(1440px, 100% - clamp(48px, 5vw, 96px))`;
- expose `aria-current="page"` and a visible indicator only for an exact current listing destination;
- expose no active primary destination on an individual event, home or search page;
- preserve the mobile `<details>` drawer behavior while moving its content to the shared optically adapted lettering lockup.

## Research and UX decisions

### Full-name lockup and text tag

The focused third round treats `Полюбить Калининград · Анонсы` as one fixed stacked lockup, not as a small caption and an independently stretched SVG:

- GOV.UK defines a lockup as a fixed relationship between a parent wordmark and a service name, derives spacing from an internal brand unit and left-aligns stacked web lockups: <https://brand.design-system.service.gov.uk/logo-system/brand-hierarchy/>.
- Docusign requires a stable relationship and visual weight between identity parts, recommends one-colour reversed artwork on a sufficiently contrasting fill and explicitly rejects arbitrary repositioning, strokes and logo effects: <https://brand.docusign.com/logo>.
- UCSB's official wordmark system derives clear space from x-height and uses reversed white lockups on dark backgrounds while preserving tier legibility: <https://brand.ucsb.edu/visual-identity/university-marks/primary-wordmark-lock-ups>.
- UMD limits the main header to five concise links, prefers SVG identity artwork and requires consistent type/spacing plus reflow and keyboard access: <https://designsystem.umd.edu/components/site-header>.

Applied result: the focused tag is a stable `240×88px` at every review width from `1024` through `1920`. A `4px` internal module yields `24px` side clear space, `18/16px` top/bottom padding, a `4px` inter-tier gap and a `12px` endorsement line. Solid `#98401f` replaces the diagonal gradient and shadow, so the silhouette reads as a bookmark/tab rather than a primary button. White/terracotta contrast is approximately `6.54:1` with the cream-white lettering. The endorsement stays uppercase for architecture but drops from weight `820` to `600`; `white-space:nowrap` makes a broken umbrella name a contract failure rather than an accepted breakpoint.

Gemini proposed shrinking the `1024px` lockup to roughly `195×76px`, but browser measurement showed its proposed `10px / 0.08em` uppercase endorsement is about `175px` wide while the proposed content box is only `155px`. The implementation therefore deliberately keeps the same `240×88px` optical master across all four desktop widths: the `1024px` shared container still leaves about `242px` of elastic space between the tag and the `488px` navigation cluster. This removes the previous breakpoint jump and is safer than copying an internally inconsistent number.

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
| `1024px` | about `973px` | about `26px` each | focused text lockup `240×88px`; one-line endorsement | right edge at the container boundary; compact `11px` inline link padding |
| `1280px` | `1216px` | `32px` each | same `240×88px` lockup | right-aligned; normal `16px` inline padding |
| `1536px` | `1440px` | `48px` each | same `240×88px` lockup | right-aligned on the wide shared grid |
| `1920px` | `1440px` | `240px` each | same `240×88px` lockup | right-aligned inside the capped readable composition |

Shared tokens:

- bar: `56px`;
- focused text tag: `240×88px`, `31px` overlap, `24px` side padding, solid `#98401f`, no shadow;
- endorsement: `11/12px`, weight `600`, tracking `0.08em`, uppercase, no wrapping;
- service wordmark: `192px` wide, `4px` below the endorsement;
- eyelet: `72×78px`, `22px` overlap;
- brand-to-navigation flex gap: `clamp(32px, 4vw, 64px)`;
- link gap: `4px` normally, `0` at `<=1120px`;
- link padding: `16px` inline normally, `11px` at `<=1120px`;
- link minimum height: `44px`;
- white mark: `51×44px`, optically lifted `4px` because the tag extends below the working row.

## Compared variants

### 01 — Text tag (`text-tag`)

- `56px` bar; stable `240×88px` solid terracotta text tag; `31px` overlap.
- The complete name is a fixed left-aligned lockup: `11/12px` lightweight uppercase endorsement, `4px` gap, `192px` service wordmark.
- No breakpoint resize between `1024` and `1920`; no gradient, inset or shadow; hover changes only the solid fill slightly.
- Strongest literal continuity with mobile.
- Approved static-site direction; the independent small mark is being compared as either the matching transparent tag + lower wide-`о` or the bare tag silhouette.

### 02 — Logo eyelet (`logo-eyelet`)

- `56px` bar; `72×78px` terracotta eyelet; `22px` overlap.
- `brand-mark-white.svg` preserves the accepted geometry as one white silhouette directly on the colored tag. There is no inner cream tile.
- The full name remains a calm two-line wordmark at `>=1121px` and yields before navigation is compressed.
- The second line uses `announcements-wordmark-ui.svg`; its single wide `о` is a service accent, not a replacement for the umbrella monogram inside the eyelet.
- Best separation of permanent identity, readable name and visible navigation.

### 03 — Signature tab (`signature-tab`)

- `56px` bar; `248×68px` combined mark-and-name tag; `12px` overlap.
- Uses the same white mark without an inset tile.
- Strongest immediate identity, but reserves the most horizontal space and is visually heavier.

## Recommendation

Use **01 / Text tag** as the approved direction, keep the menu right-aligned, and keep active state contextual.

The tag and menu should not be forced into symmetrical halves: they have different visual weights. The correct balance is asymmetric but anchored — the fixed full-name lockup at the shared left grid edge, navigation at the shared right grid edge, elastic whitespace between them. This preserves the comfortable right alignment while making the umbrella/service relationship explicit without a competing icon.

Do not implement the tag as an active-section slider. Do not infer current navigation from event taxonomy. Do not use absolute brand positioning plus per-variant `margin-left`, and do not shrink target sizes to preserve the desktop row.

## Consultant evidence

The neutral briefs, screenshots and raw consultant reports are stored under `artifacts/codex/static-desktop-header-20260714/` as ignored runtime evidence.

- **Round 2 — Gemini:** `Gemini 3.1 Pro (High)` through Antigravity `agy` completed successfully on 2026-07-14. It selected the refined logo-eyelet, explicitly recommended right alignment, a `1440px` cap with responsive gutters, a `72×78px` tag, white mark without inset, flex-flow positioning, and no event-detail/search active state. Raw report: `gemini-layout-round2-review.md`.
- **Text-tag harmony round — Gemini:** `Gemini 3.1 Pro (High)` through Antigravity `agy` completed successfully on 2026-07-14 after inspecting the current `1024/1280/1536/1920` renders and measured geometry. It marked the `1024px` two-line endorsement and gradient/shadow button affordance as P0, selected a solid modular Editorial/Calm lockup, reduced the endorsement weight and required a fixed no-wrap relationship. The integrator retained the selected direction but corrected the consultant's too-narrow `1024px` dimensions using real browser text measurements. A second review of the implemented four-viewport renders returned **ACCEPT**, no P0/P1/P2 and explicitly approved the stable `240×88px` lockup. Raw ignored evidence: `artifacts/codex/text-tag-harmony-20260714/gemini-harmony-review.md` and `gemini-acceptance-review.md`.
- **Round 1 — Gemini:** `Gemini 3.1 Pro (High)` through the project Antigravity runner selected the logo-bearing tag and recommended a `48–56px` bar, bounded overlap and hiding secondary wordmark before squeezing navigation. Raw report: `gemini-antigravity-review.md`.
- **Round 1 exact legacy CLI probe:** `gemini-3.1-pro-preview` through `gemini-legacy` returned `IneligibleTierError / UNSUPPORTED_CLIENT` and instructed migration to Antigravity. It was not substituted with a lower model.
- **Round 1 a-opus:** `a-opus` returned `Individual quota reached`; Claude Code `Opus` was logged out. This remains blocker evidence, not a lower-class review.

## Acceptance grid

| Check | Contract |
|---|---|
| `1920×1080` | Header uses the `1440px` cap; the `240×88px` lockup and menu occupy opposite grid edges. |
| `1536×864` | `48px` outer gutters, fixed full-name lockup and all five destinations fit around one `56px` working bar. |
| `1280×800` | Brand/name and navigation retain a clear elastic gap with no collision. |
| `1024×768` | The same `240×88px` lockup remains unbroken; five links remain visible and right-aligned. |
| Text-tag optics | Endorsement stays one line; SVG is `192px` wide; fill is solid; computed shadow is `none`; overlap is `31px`. |
| Event detail | No navigation link has `aria-current`; no visible underline is forced by event type. |
| Listing | Exactly one link has `aria-current="page"`, stronger text and a visible underline. |
| Keyboard | Every functional link has a visible `3px` focus ring; the duplicate visual wordmark is not in the tab sequence. |
| Reduced motion | Current-state transition is disabled under `prefers-reduced-motion`. |

## Integration state

The accepted desktop concept and mobile production drawer now share `AnnouncementsLockup.astro`; the desktop concept keeps the fixed R3 geometry while mobile uses a taller R4 optical composition rather than a mechanical resize. Production event/listing route behavior and exact current-state semantics remain unchanged. Favicon selection is intentionally still open between the shared tag silhouette with a lower small-size wide `о` and the bare silhouette; candidate A is installed only for preview practicality. Canonical rules have moved to [`design-system/`](design-system/README.md); this dated document remains the research and decision record.

Remaining rollout work is limited to promoting the accepted desktop header from its noindex comparison component into the production desktop layout after the separate page-shell release gate. That rollout must keep the acceptance grid and reconcile the production `1180px` content shell explicitly.
