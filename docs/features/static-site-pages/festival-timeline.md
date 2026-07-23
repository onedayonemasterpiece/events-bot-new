# Festival calendar timeline

> **Status:** public `noindex` review prototype; calendar view only.
> **Route:** `/festivali/` inside an immutable preview prefix.
> **Current candidate:** `preview-20260723-festivals-calendar-r4`.

The page uses the unified Astro header, footer and mobile dock. It presents 21
regional festivals from July through December 2026. The category view remains
deferred. The first `r1` candidate is withdrawn: it shortened the donor
mechanic, used split image/body cards and repeated media from the regional
anniversary aggregator.

R4 is the desktop correction after owner review. The page now consumes the
shared `--ke-font-sans` stack (`Inter` with the same system fallbacks as the
rest of the static site) everywhere; the local Georgia override is removed.
The left month marker, name, mood symbol and copy form one sticky shelf below
the unified header and sticky month switcher. The shelf remains at `126.6px`
while its month is in view and is pushed out by the next section.

The calendar contains no page-local prototype explanation, data-quality note
or link to the prototype hub. Pending dates/programmes remain on their cards
because those are visitor-facing festival facts rather than operator copy.

## Data and honesty contract

The production `festival` table was checked first. Its retained/archive rows
and sparse current dates are not sufficient for this calendar, and individual
programme events cannot be presented as festival-series records. The curated
projection in `site/src/data/festivalTimeline.ts` therefore uses the official
regional calendar only where a festival has no own current announcement, and
otherwise links the festival, organiser or venue directly.

Every item has:

- a published date or an explicitly broad period;
- an announced, programme-pending or date-pending state;
- an exact source label and destination;
- reviewed local media dimensions, semantic class and focal point;
- an optional internal event id only for an exact 2026 match.

Dates are not inferred from prior editions. Broad labels such as `Октябрь`,
`Октябрь — декабрь` and `с 13 декабря` remain visible together with a pending
status.

## Media provenance

`site/src/data/festivalTimelineMedia.ts` is the hash-bound media ledger. For
each of the 21 WebP files it stores:

- the exact festival/organiser/venue post or official gallery;
- owner class and review date;
- `visual`/document semantic classification and confidence;
- SHA-256 of the cached asset;
- an explicit note when the image is archive or contextual venue media.

The behavior test recalculates every hash and rejects a missing/stale record.
No asset comes from `afisha80let.visit-kaliningrad.ru`. Thirteen covers were
replaced during the correction: all nine anniversary-overlay images plus four
contextually wrong covers (`Жили-были`, `Народов много — Родина одна`,
`Тыквенный пир`, `Клуб путешественников`). The remaining covers already came
from exact official posts. Generated and stock art are not used.

## `/617` packing contract

`site/src/lib/festivalTimelineLayout.ts` runs a whole-month bitmask dynamic
program over one-to-four-card candidates. Cost includes real normalized strip
height, crop, source resolution, copy pressure and order changes. Equal-cost
layouts resolve by fewer rows, lower crop and fewer permutations.

The rendered contract is:

- every non-final row fills 100% of the cards plane;
- the only deliberately partial rows are final one/two-card remainders;
- every card in a row has the same visible and complete height;
- the complete card is the media surface: no white body, blank field or
  artificial height reserve;
- all packed images use `object-fit: cover`;
- visual media may crop; wide and low-resolution slots participate in cost;
- a document anchors the row at its natural ratio and is rejected above the
  20% crop budget; unknown semantic media fails closed instead of entering a
  cover row;
- desktop formations follow the donor density (`4+3` for seven, `4+1` for
  five, `3+3` for six), while the optimizer still evaluates legal
  assignments/permutations;
- full-month and remainder solo cards remain compact panoramas;
- at `390px`, the page repacks into two equal columns; only the
  below-`340px` safety fallback becomes one column.

The date and full status are top overlays; title, one-line place and icon-led
theme are bottom overlays. Four-up rows no longer collapse status text to a
bare dot. Source/provenance stays in the data contract and card destination
rather than consuming visible card height.

This is a close implementation of the donor's **card and timeline mechanic**,
not a pixel-for-pixel copy of its unrelated page chrome: the inventory, image
ratios and KenigEvents tokens are real project inputs. The shared visual
contract is nevertheless element-complete: full-bleed media, top date, top
status, bottom title, place, category pill, same-height row, left rail, marker,
month name, mood icon and mood copy.

Desktop card hover must not translate the card or scale the image. It may only
change non-geometric affordances (border and shadow); keyboard focus retains a
visible inset outline.

## Pixel acceptance

The `2026-07-23` Playwright gate uses `887×900`, `1440×900`, `390×844` and
`320×700`. Required envelopes:

- `887px`: card plane `700–710px`, timeline `≤1600px`;
- `1440px`: timeline `≤2300px`; four-up `≤300px`, three-up `≤260px`,
  two-up/solo `≤225px`;
- `390px`: two columns, regular tiles `184–214px`, August `≤950px`, full
  timeline `≤3200px`;
- all viewports: zero horizontal overflow, 21 decoded images, no browser or
  first-party request errors, same-height cards per row.

Measured R4 values are `1488.8px`, `2279.4px` and `3043.6px`
respectively. The previous `r1` timeline measured `5729.6px` at the
reference-width viewport and `10148.8px` on mobile.

The earlier R3 Gemini Pro `KEEP` is retained only as an external review
artifact, not as product acceptance: subsequent owner inspection found the
off-system serif, missing sticky shelf, page-local service note and moving
hover. R4 closes those concrete gaps and is the new review candidate.

## Checks and preview

Run:

```bash
npm --prefix site run test:festival-timeline-layout
PREVIEW_BUILD_ID=preview-20260723-festivals-calendar-r4 npm --prefix site run build:preview
PREVIEW_BUILD_ID=preview-20260723-festivals-calendar-r4 npm --prefix site run check:unified-prototype
```

Review candidate:

- page: <https://kenigevents.ru/preview-20260723-festivals-calendar-r4/festivali/>;
- hub: <https://kenigevents.ru/preview-20260723-festivals-calendar-r4/__preview/>.

The immutable URL is a public bearer link, not authentication. It must not be
promoted to the production root before product acceptance and a refresh owner
are agreed.
