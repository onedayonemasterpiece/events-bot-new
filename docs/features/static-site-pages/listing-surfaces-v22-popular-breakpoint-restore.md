# Listing surfaces V22: breakpoint-isolated Popular

> **Comparison-only correction:** V22 remains the immutable equal two-column
> mobile specimen and accepted desktop baseline. Its mobile large-card claim is
> superseded by V23, which reuses the canonical `EventCard.astro` directly.

> **Status:** immutable preview candidate, 2026-07-19.
> **Corrects:** the V21 desktop replacement and horizontal phone shelf.

## Product contract

`Популярное` retains one ranked allocation of 4–5 behavioral sections, but
uses two mutually exclusive responsive renderers because the accepted desktop
listing composition and the real mobile related-event card are different
components:

- **desktop, 721px and wider:** the accepted V20/V19 `PopularBehaviorRows@2`
  with `ListingEventCard@9`, intrinsic heterogeneous widths, 180px media
  height (190px on wide desktop) and up to five cards in one row;
- **phone, 720px and narrower:** `PopularMobileBehaviorRows@1` with the actual
  shared `EventCard@3` from `Смотрите дальше`.

The two renderers receive the same deduplicated groups in the same order.
Filters update both representations, while totals and density-anchor lookup
are scoped to the active breakpoint representation, so counts do not double.

## Phone densities

Both modes use the same mobile `EventCard` articles and ordinary vertical page
scroll. There is no horizontal card shelf, hidden carousel affordance,
masonry, dense packing or rank change.

- `Крупно`: one centered card per row, width `100vw - 58px` — 302/332/372px
  at viewports 360/390/430px — with 16px vertical gap;
- `2 в ряд`: row-major two-column grid with 12px gap and intended column widths
  150/165/185px at the same viewports.

OCR/document media stays at its authored ratio without fields. Listing cards
show no Calendar, `Не интересно` or mutable Share/Like actions; only non-zero
quiet audience proof may link to the detail page.

## Regression acceptance

- at 1366/1536/1920px the mobile renderer and density dock are `display:none`;
- at 1536px the first accepted desktop row keeps heterogeneous widths near
  220/220/244/280/220px, 180px media height and no row/page X overflow;
- at 360/390/430px the desktop renderer is hidden and exactly 25 visible
  `EventCard@3` articles remain;
- large rows have `scrollWidth == clientWidth`, `overflow-x` is not `auto` or
  `scroll`, and every next card starts below the previous card;
- compact columns and gap are 150/165/185px and 12px, ID order is stable, and
  page/rows have no horizontal overflow;
- city counts remain 25 rather than 50 and the nearest visible mobile card is
  preserved while density changes.

Public immutable preview:
`https://kenigevents.ru/preview-20260719-date-listings-v22-mobile-restore/populyarnoe/`.
