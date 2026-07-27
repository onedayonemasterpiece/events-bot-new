# Product breadcrumbs contract

Status: accepted for the unified noindex prototype  
Decision date: 2026-07-23

## Product decision

Breadcrumbs are **selective hierarchy**, not permanent page chrome.

- Do not show them on the home/prototype hub or first-level pages: date
  listings, Popular, Exhibitions, Search, For me, Clubs index, Partners and
  Partnership.
- Show them on genuinely deep pages:
  - event detail;
  - a materialized Search collection;
  - a first-class curated collection, using its real product parent rather
    than pretending it is a Search result;
  - club detail;
  - an internal lab only when it needs a route back to the prototype hub.
- Desktop (`>=1024px`) uses the full, deterministic hierarchy and ends with
  the current page as non-interactive text.
- Mobile/tablet (`<1024px`) replaces the chain with one `44px`-high named
  parent link on materialized Search collections and club detail. Event detail
  is the deliberate exception: its immersive hero renders no breadcrumb/back
  row at all. A structural “up” link, where present, is never
  `history.back()`.
- The path never depends on `referrer`, browser history, the listing that
  happened to be used, or a client-only state.

On desktop event Editorial pages the hierarchy is rendered as a quiet overlay
at the lower edge of the hero photograph, ending just above the cream content
sheet. White type and a restrained under-glow preserve legibility without
creating a competing bar. The sheet receives matching rounded top corners.
The overlay must not cover the primary subject or become taller than its single
line. Mobile/tablet event detail omits the overlay and the former separate
named-parent row so primary event information begins immediately after the
hero.

The rule intentionally removes decorative one-hop chains such as
`Афиша / Поиск`: they duplicate the global navigation and cost roughly
`56–87px` of vertical space in the current implementation.

`Бесплатные события` is a first-class curated collection. Its only structural
parent is `Афиша`; it must not render `Поиск` in breadcrumbs, use a Search back
link or highlight Search in the mobile dock. Other genuinely query-derived
materialized collections may keep Search as their parent.

## Event-detail hierarchy

Use only real, materialized parent destinations.

1. `Афиша` links to the current build's home/hub.
2. Add a category parent only when that category has a genuine landing page.
   The current concrete case is `Выставки` → `/vystavki/`.
3. The event title is the final desktop item with `aria-current="page"`.
4. On mobile/tablet event detail render neither the chain nor a substitute
   back row. Browser navigation and the global mobile discovery menu remain
   responsible for returning to listings.

Do not add a city node merely to make the chain longer. A city query is not a
site hierarchy unless it is promoted to a stable, user-facing landing page.
Do not invent links for event types whose category pages do not exist.

## Markup and structured data

- Full chain: `<nav aria-label="Хлебные крошки"><ol><li>…`.
- Current item: non-link element in `li[aria-current="page"]`.
- Separators are presentational and must not be announced.
- Long current titles remain one line and use ellipsis on desktop.
- Parent link has at least a `44px` click target.
- Event `BreadcrumbList` JSON-LD and visible desktop hierarchy are derived
  from the same parent list.
- JSON-LD is useful for crawler understanding and eligible search-result
  presentation; it must not be described as a direct ranking boost.

## Evidence

Primary guidance:

- [GOV.UK Breadcrumbs](https://design-system.service.gov.uk/components/breadcrumbs/)
  recommends breadcrumbs for multiple site levels and not for a flat
  structure; it also documents mobile collapse.
- [U.S. Web Design System Breadcrumb](https://designsystem.digital.gov/components/breadcrumb/)
  recommends breadcrumbs for interior/external-entry pages, permits omission
  from home/section landing pages and describes a direct-parent mobile form.
- [W3C ARIA Authoring Practices breadcrumb pattern](https://www.w3.org/WAI/ARIA/apg/patterns/breadcrumb/)
  defines the labelled navigation landmark and current-page semantics.

The local inventory found an inconsistent pre-decision state: top-level pages
mixed shown/hidden crumbs; event breadcrumbs were hidden on phones, present
only on tablet and absent from the desktop renderer.

An external `agy` consultation was run with CLI slug
`gemini-3.1-pro-low`; retained resolver logs prove the backend label
`Gemini 3.1 Pro (Low)`. It independently recommended removing breadcrumbs
from first-level product pages, using full deterministic desktop hierarchy and
one named parent link on mobile. The `gemini-3.1-pro-high` alias was not used
as review evidence because the current resolver mapped it to
`Gemini 3.6 Flash (High)`; those responses are retained only as supplementary
probe material.

Evidence directory:
`artifacts/codex/static-unified-corrections-20260723/gemini-breadcrumbs/`.

## Acceptance

- No first-level product page renders `data-product-breadcrumbs` or legacy
  `.crumbs`.
- Event detail shows full breadcrumbs at `1024px+` and exactly one parent link
  below `1024px`.
- On desktop Editorial event detail, the breadcrumb overlay ends above the
  rounded content sheet and remains readable over both light and dark photos.
- Search-collection and club-detail pages follow the same responsive rule.
- No visible or JSON-LD path contains a non-materialized category/city URL.
- Keyboard focus, tap target, `aria-current`, ordered-list structure and
  no-horizontal-overflow checks pass at `390`, `800`, `1024` and `1440px`.

## Measurement

Track parent-link clicks separately from global navigation and browser Back.
Compare external-entry event sessions for:

- parent-link click-through;
- continuation into the parent listing;
- first-viewport CTA visibility;
- bounce/exit and scroll depth.

Do not call the contract successful merely because the link is rendered.
