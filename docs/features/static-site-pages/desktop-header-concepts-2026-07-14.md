# Desktop header concepts — 2026-07-14

> **Status:** noindex choice lab; not a production-header decision. Mobile remains unchanged.
>
> **Routes:** `/lab/header-desktop/` and `/lab/header-desktop/examples/{text-tag|logo-eyelet|signature-tab}/`.

## Product problem

The accepted mobile navigation uses a terracotta tag that protrudes below the top edge and opens the discovery rail. The current desktop header instead uses a quiet `72px` text row with visible links, but has no independent brand mark, no strong continuity with the mobile tag and no current-section treatment.

The desktop contract is therefore:

- keep global discovery navigation visible rather than hiding it behind a menu;
- compress the working bar to `56px`, while keeping link hit areas at least `44px`;
- keep the tag as a home link, not as a redundant desktop menu toggle;
- let the tag overlap the hero by a bounded `12–22px` rather than expanding the entire header;
- render a visible keyboard focus and `aria-current="page"` state;
- keep the existing mobile `<details>` drawer/tag implementation unchanged;
- reserve future horizontal space for search and account tools.

## Evidence and UX rules

- Nielsen Norman Group's menu checklist recommends visible navigation on desktop rather than hiding primary choices under a hamburger: <https://media.nngroup.com/media/articles/attachments/PDF_Menu-Design-Checklist.pdf>.
- W3C WAI recommends semantic navigation structure, short labels and `aria-current="page"` for orientation: <https://www.w3.org/WAI/tutorials/menus/structure/>.
- W3C page-structure guidance treats a top-level header as the site banner that typically owns persistent logo/name and navigation: <https://www.w3.org/WAI/WCAG21/Techniques/html/H101>.
- GOV.UK separates a home-linked brand identity from service navigation and exposes current navigation state: <https://design-system.service.gov.uk/components/service-navigation/>.
- Material's dense desktop top bar supports the same conclusion: denser is valid on desktop, but branding, navigation and actions still need stable placement: <https://m2.material.io/components/app-bars-top>.

These are structural references, not a visual style to copy. KenigEvents keeps its own warm paper/graphite/terracotta system and physical tag metaphor.

## Compared variants

### 01 — Text tag (`text-tag`)

- `56px` bar; `222×76px` terracotta text tag; `20px` visual overlap after box sizing.
- No symbol. The tag contains `Полюбить Калининград / Анонсы` and is the home link.
- Strongest literal continuity with mobile.
- Main weakness: does not solve the missing independent brand symbol and can still read as a promoted label rather than a mark.

### 02 — Logo eyelet (`logo-eyelet`) — recommended

- `56px` bar; `76×78px` terracotta eyelet; `22px` overlap.
- Existing transparent `brand-mark.svg` is rendered on a small warm paper inset so the two-colour graphite/terracotta drawing stays source-faithful and legible.
- The full name is a separate calm two-line wordmark on wider desktops and disappears before it can collide at `<=1080px`; the sign and all five navigation destinations remain.
- Best separation of roles: permanent brand mark, readable name, visible navigation.
- Best future capacity for search/profile tools because the wordmark can yield first without removing the mark.

### 03 — Signature tab (`signature-tab`)

- `56px` bar; `248×68px` combined logo-and-name tab; `12px` overlap.
- Strongest immediate identity and the shallowest overlap.
- Main weakness: the combined object is visually heavier and reserves more horizontal space; at narrower desktop widths it must collapse to the mark only.

## Recommendation

Choose **02 / Logo eyelet** for the next production-shaped iteration.

It preserves continuity without copying mobile interaction literally: on desktop it is a home link while navigation is already open. It introduces the already accepted graphite/terracotta mark, gives the header a recognisable silhouette, and provides a clean degradation path: hide only the redundant wordmark at `<=1080px`, never the sign or primary destinations.

Do not implement the tag as an active-section slider. That would change its meaning from a brand/menu object on mobile to a navigation tab on desktop. Do not let it add its full height to document flow, and do not compress the bar below the `44px` interaction contract.

## Consultant evidence

The shared neutral brief and baseline screenshot are stored under `artifacts/codex/static-desktop-header-20260714/` (uncommitted runtime evidence).

- **Gemini:** `Gemini 3.1 Pro (High)` through the project Antigravity `gemini` runner completed successfully on 2026-07-14. It independently ranked the logo-bearing square tag first, recommended a `48–56px` bar, a bounded `24–32px` overlap, visible current state and hiding secondary wordmark before squeezing navigation. Raw report: `gemini-antigravity-review.md`.
- **Exact legacy Gemini CLI probe:** `gemini-3.1-pro-preview` through `gemini-legacy` could not authenticate because Google returned `IneligibleTierError / UNSUPPORTED_CLIENT` and instructed migration to Antigravity. This is runner evidence, not a lower-class substitute.
- **a-opus:** the requested `a-opus` / `Claude Opus 4.6 (Thinking)` call was attempted on 2026-07-14 but returned `Individual quota reached`, reset in about `45h45m`. Claude Code `Opus` fallback was also unavailable because `claude auth status` reported `loggedIn:false`. No Sonnet/Haiku/Flash/Lite substitute was used, so a fresh Opus design review remains blocked rather than falsely marked complete.

## Acceptance grid

| Check | Contract |
|---|---|
| `1920×1080` | Header contents stay on the shared `1180px` grid; tag aligns with the hero/content edge. |
| `1536×864` | Wordmark, five destinations and bounded overlap fit on one `56px` bar. |
| `1280×800` | Brand/name and navigation retain a readable gap with no collisions. |
| `1024×768` | Logo eyelet remains; redundant text wordmark is hidden; all five navigation links remain visible. |
| Keyboard | Every functional link is in logical order and has a visible `3px` focus ring. The duplicate decorative wordmark is removed from the tab order. |
| Current page | `aria-current="page"` is present and the state is visible without relying only on colour. |
| 200% text zoom | No text clipping. The implementation may switch to the accepted mobile pattern or wrap outside the normal desktop capture, but must not reduce targets below `44px`. |
| Reduced motion | No required animation; current-state transitions are disabled under `prefers-reduced-motion`. |

## Production integration boundary

This branch intentionally adds only lab routes and validation. It does **not** change `site/src/layouts/EventLayout.astro`, the production event routes or the mobile drawer. After product selection, promote one concept into the shared layout, add URL-aware `aria-current`, test normal/listing/event pages at the acceptance grid, and update this document from choice-lab to accepted contract.
