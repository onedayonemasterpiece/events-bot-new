# Listing surfaces V21: real EventCard mobile Popular

> **Additional correction:** the V21 `listing-proof` large card was a
> Popular-only reconstruction and is rejected. V23 large mode directly renders
> the shared canonical `EventCard.astro` `split-actions` component.

> **Rejected and superseded by V22.** V21 incorrectly replaced the accepted
> desktop `ListingEventCard` composition and made the large phone mode a
> horizontally scrolling shelf. Keep this file only as regression evidence;
> the current contract is `listing-surfaces-v22-popular-breakpoint-restore.md`.

> **Status:** immutable preview candidate, 2026-07-18.
> **Surface in focus:** `/populyarnoe/` on phone; desktop behavioral allocation and all date listings keep the V20/V19 regression baseline.
> **Supersedes:** the V20 mobile card implementation, not its labels or evidence ranking.

## Correction

V20 interpreted `Крупно` as a listing card made visually larger and
`Компактно` as a one-item-wide media-left scan row. That was the wrong product
mapping. `Крупно` means the actual mobile card from the event-detail section
`Смотрите дальше`; compact means the same event card in a two-column vertical
feed.

Popular therefore renders the shared `EventCard` component directly. It does
not copy its media/body/meta markup into `ListingEventCard`, does not maintain a
second mobile card component and does not mount separate large and compact DOM
trees. Today, Tomorrow and Weekend continue to use `ListingEventCard` because
their exact-time packing is a separate surface.

## Shared component contract

`EventCard@3` adds the explicit `listing-proof` variant:

- the existing `event-card__media-link`, media-role policy, body, title, date,
  admission and place markup remain component-owned;
- mutable `Не интересно`, Calendar, Share and Like controls are not rendered on
  Popular;
- only non-zero Share/Like values appear as quiet informational proof linked to
  the detail page, in system order Share → Like;
- listing-ready identities are selected inside `EventCard`: a strictly eligible
  photo/no-media identity may occupy the lower-right preview corner, while OCR
  and documents keep identities outside the image;
- medallions on media are opaque and unfiltered; no universal border or shadow
  is added.

The same article exposes listing data attributes so direct city filtering,
linked-date exclusion and full-card keyboard navigation continue to use the
shared runtime.

## Mobile densities

### `Крупно` — default

- each behavioral shelf stays horizontally bounded;
- its cards are the real `EventCard` instances;
- outer width is `100vw - 58px`: 302px at 360, 332px at 390 and 372px at
  430, matching the event-detail mobile card width;
- the shelf owns horizontal overflow; the page never does.

### `2 в ряд`

- the same EventCard articles become a row-major two-column CSS grid;
- no masonry, CSS columns, dense fill, event reorder or alternate rank is used;
- with the 24px page gutters and 12px gap, the intended column widths are about
  150/165/185px at 360/390/430;
- safe photo media may use the component's cover policy; OCR/document media
  remains full-width at its authored natural ratio, with no contain fields or
  equal-height crop;
- unequal protected-image heights create neutral canvas below the shorter grid
  neighbor, not a stretched card or invented media field;
- title is clamped to three visible lines, but the full anchor text remains in
  the DOM and accessible name.

The bottom control is labelled `Крупно / 2 в ряд`, keeps 48px targets, safe-area
clearance, local preference, roving radio keyboard behavior and viewport-anchor
preservation. Native pinch zoom stays enabled and is never used as a hidden
density gesture.

## Critical review

The implementation decision was reviewed against the real public
`Смотрите дальше` DOM and by Gemini 3.1 Pro High. The accepted direction is one
real `EventCard` DOM, a readonly listing variant, a horizontal large shelf and a
two-column compact grid. Rejected alternatives: a `ListingEventCard` visual
facsimile, duplicated hidden card trees, one-column compact rows, masonry/dense
reorder, OCR crop/contain fields and visually hidden but focusable listing CTA.

## Acceptance

- Popular contains `EventCard@3` and contains no `ListingEventCard` instance;
- one article is rendered for each allocated ID; ID count/order does not change
  after density switching;
- event 5130 remains in `frequently_shared` by its 69-share evidence rule;
- at 360/390/430 large card widths are 302/332/372px ±2;
- compact mode has exactly two row-major columns, 12px gap, no page X overflow
  and an odd fifth item starts the next row on the left;
- document image shell and image edges coincide, source ratio delta ≤0.01 and
  no preview fields are visible;
- overlay identity is allowed only by the existing strict photo/no-media gate;
  OCR identities stay outside media; proof is non-zero, quiet and non-focusable
  as an action;
- no Calendar, Share/Like button or `Не интересно` control occurs in Popular;
- the switch preserves the nearest visible event within 16px, respects footer
  and safe area, and does not disable browser zoom.

Public immutable preview:
`https://kenigevents.ru/preview-20260718-date-listings-v21-mobile-grid/populyarnoe/`.
