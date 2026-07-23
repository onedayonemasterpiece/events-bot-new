# Festival calendar timeline

> **Status:** public noindex prototype; calendar view only; not promoted to the production root.
> **Route:** `/festivali/` inside an immutable preview prefix.

The festival page extends the current unified Astro shell rather than creating
a parallel header, footer or mobile navigation. Its first prototype is a
chronological July–December 2026 timeline with 21 regional festivals. The
category view is intentionally deferred.

## Data and honesty contract

The production `festival` table was checked first. It contains 58 rows, but the
current projection is too sparse and includes retained 2025 or date-unknown
records. The event table also cannot be treated as a festival directory:
festival-tagged rows often represent individual programme events. Therefore the
prototype uses the regional tourism calendars and organiser announcements as
the canonical calendar layer, then links to the current internal event page
where an exact 2026 match exists.

The curated projection lives in
`site/src/data/festivalTimeline.ts`. Every item carries:

- a real source URL and source label;
- a published date or an explicit broad period;
- an honesty state: announced, programme pending or date pending;
- a real locally cached photograph/poster with original dimensions and media
  semantics;
- an optional production event id for an internal detail-page link.

Unknown dates are never inferred from an annual pattern. `Октябрь`,
`Октябрь — декабрь` and `С 13 декабря` remain broad labels, accompanied by a
visible pending state. The page is permanently `noindex` while it is a review
prototype.

## Card and timeline layout

`site/src/lib/festivalTimelineLayout.ts` performs stable-order dynamic
programming over consecutive cards. It evaluates rows of one to four items and
minimises vertical expansion without reordering calendar chronology.

The rendered contract is:

- every non-terminal row fills the available width;
- all media frames and complete cards are equal-height within a desktop row;
- visual-only portrait media may use the compact `5:4` target; a wide solo card
  is capped at `16:9` rather than becoming a page-height hero;
- documentary/OCR media keeps its natural aspect and uses `contain`, so text is
  not sacrificed to packing;
- cards have no synthetic blank fields or height padding; body height comes
  only from real content and row equalisation;
- mobile uses one card per row, `16:10` visual media, natural protected
  documentary media and the shared mobile shell/dock;
- card order is chronological, and unknown dates stay at the appropriate
  month boundary.

All 21 WebP assets under `site/public/assets/festivals/timeline/` come from
festival organisers, official venues, regional tourism pages or relevant
festival posts. No generated or stock replacement art is used.

## Preview and checks

The unified prototype hub links the route, and
`site/scripts/check-unified-prototype.mjs` checks:

- the immutable prefix and `noindex,nofollow,noarchive`;
- the exact 21-card and six-month inventory;
- pending-date honesty labels;
- prefix-local festival media;
- a valid generated route from the shared header/footer build.

Review build:

- page: <https://kenigevents.ru/preview-20260723-festivals-calendar-r1/festivali/>;
- hub: <https://kenigevents.ru/preview-20260723-festivals-calendar-r1/__preview/>.

The immutable preview is a public bearer link, not authentication. It must not
be listed as a stable production route until the calendar data owner, refresh
path and category view are accepted.
