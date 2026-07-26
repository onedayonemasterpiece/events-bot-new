# Festival calendar timeline

> **Status:** integrated into the checked production generator; calendar view
> only. Public root promotion is still blocked by the shared reader-atomic
> publisher gate.
> **Route:** canonical root-form `/festivali/`; review delivery remains an
> immutable `noindex` candidate under `/_review/<token>/`.
> **Accepted visual donor:** `preview-20260726-festivals-calendar-r9`.

The page uses the unified Astro header, footer and mobile dock. It presents 21
regional festivals from July through December 2026. The category view remains
deferred. The first `r1` candidate is withdrawn: it shortened the donor
mechanic, used split image/body cards and repeated media from the regional
anniversary aggregator.

R4 was the first desktop correction after owner review. The page now consumes the
shared `--ke-font-sans` stack (`Inter` with the same system fallbacks as the
rest of the static site) everywhere; the local Georgia override is removed.
The left month marker, name, category inventory and copy form one sticky shelf below
the unified header and sticky month switcher. The shelf remains at `126.6px`
while its month is in view and is pushed out by the next section.

R6 supersedes the rejected R5 card treatment. R5 was closer than the original
split-body cards but remained a nested-card composition: its large rounded,
bordered glass panel visually cut the photograph in two. R6 restores the donor
mechanic as one uninterrupted photo canvas. A seamless lower gradient protects
the title and place; only the compact category chip keeps a translucent
backdrop. The date is a solid KenigEvents-primary badge, while official and
pending states use quiet green and amber fills without decorative dots.

R8 keeps that accepted single-canvas mechanic and increases the R7 card type by
approximately 20%: desktop titles render at `21–25.2px`, place copy at
`12.6–14.7px`, date/status badges at `12–13.2px` and category copy at
`11.1–12.9px`. Dense `820–1000px` formations use explicit `19.2–19.8px`
titles, `12.3px` places and `11.1–11.7px` badges, with a third title line when
needed rather than truncating a festival name. Place copy may use two lines.

R9 reduces the desktop pre-calendar stack without shrinking the global header
or the accepted cards. At the effective `1536×864` CSS viewport produced by an
FHD display at 125% scaling, the hero is `272.8px` high and the first festival
starts at `517px`; the complete `224.3px` City Jazz card is visible. At a
conservative `1536×700` browser content height its title still appears at
`645px`. The former R8 positions were `380.6px` for the hero and `713.5px` for
the first card, so the useful content now begins `196.5px` earlier.

The top copy is now a literal answer to the page purpose rather than the
campaign-like `Время фестивалей`: the unique H1 says `Фестивали
Калининградской области`, the eyebrow supplies the 2026 period, and the visible
summary explains covered cities/topics, dates, programme status and official
organiser destinations. The compact three-part guide distinguishes a
browser-local heart from future notification work and explains what card
status and destination mean.

The SEO/GEO contract uses one concise Russian title, one page-specific meta
description, aligned H1/OG copy, descriptive image alternatives and an eager
high-priority first card image. JSON-LD describes the page as a Russian
`CollectionPage` with review date, Kaliningrad-region subject, publisher,
primary image and an ascending 21-item list whose entities use Schema.org
`Festival`. The aggregate deliberately does not claim Google Event rich-result
eligibility: Google requires complete leaf pages for individual events. The
preview remains `noindex`; these signals become indexable only after promotion
to the canonical `/festivali/` route.

Each month rail now exposes every represented category, not a sampled
`3 + N` summary. The inventory wraps inside the sticky rail in three
`28×28px` cells per row with a `21×21px` glyph. Category-to-icon assignment is
one canonical map keyed by the displayed category, so the same category cannot
change icon because of card order or month. Both jazz events are explicitly
categorised as `Джаз`. All rail icons use KenigEvents primary terracotta; the
unexplained green decorative month glyphs were removed.

At `820–1000px`, category type and icon geometry tighten together so all labels
remain complete rather than ellipsized. Place and category copy use lighter
weights and category labels render in lowercase, preserving the reference
hierarchy beneath the stronger festival title.

The category chip no longer uses improvised Unicode marks. Its baseline uses
twelve unchanged glyphs from SVG Repo's CC0 Lucide Line family, plus two
visually reviewed semantic exceptions: a saxophone for jazz and comedy/tragedy
masks for theatre. The former dense saxophone was rejected after a ten-candidate
contact-sheet comparison at `18/24/32px`; SVG Repo
`480248-saxophone-2.svg` preserves a legible neck, body and bell at the actual
UI sizes. Meaningful two-axis categories may show two icons at full desktop
size; compact formations keep only the primary glyph. Their durable
project copies and exact item links are recorded in
`site/public/assets/icons/festival-categories/ATTRIBUTION.md`; the same assets
are catalogued in the shared SVG library under
`icons/svgrepo/ui/festival-categories/`. CSS consumes the original SVGs as
alpha masks so their geometry stays source-faithful while colour follows the
chip.

The page contains one visitor-facing usage strip, not an operator/prototype
note. It explains that cards go to official organisers, hearts are saved in
the current browser, and detailed pages/notifications are later work. The
current heart is described as a local bookmark and does not imply an active
subscription.

## Festival hearts and entity boundary

R9 deliberately does not reuse the numeric event-feedback controller. Only
four of 21 cards map to an exact current event, the Fly `festival` rows do not
provide a safe series/edition identity for all cards, and Supabase exposes only
a SELECT-only aggregate event counter with no actor-owned festival write RPC.

The prototype therefore stores a boolean set under
`ke_festival_likes_v1`, keyed as `festival-edition:2026:<slug>`. This state:

- remains only in the current browser;
- changes no public count and makes no Supabase mutation;
- never enters `ke_personalization_profile.liked_event_ids`;
- does not imply consent to or delivery of notifications;
- fails closed when local storage cannot be written.

Server-backed engagement requires a canonical festival-series and
festival-edition split, a complete 21-card mapping, owned state/RLS or a
same-origin idempotent endpoint, separate channel consent, a change detector
and an outbox/unsubscribe path.

## DB projection and honesty contract

The production `festival` table was checked first. Its retained/archive rows,
non-unique series names and sparse current dates are not sufficient for this
calendar, and inserting duplicate yearly `festival.name` rows would break
legacy code paths that expect at most one match.

The public annual editions therefore use a dedicated Fly SQLite table,
`festival_calendar_item`. It is additive: it does not rewrite legacy
`festival` rows or individual programme events. The accepted 2026 catalog is
stored in `site/src/data/festivalTimelineSeed.json` and applied transactionally
with:

```bash
python scripts/backfill_festival_calendar_2026.py --db /data/db.sqlite
python scripts/backfill_festival_calendar_2026.py --db /data/db.sqlite --apply
```

Dry-run is the default. The backfill is idempotent, validates optional
`festival`/`event` foreign keys and refuses year/order identity conflicts.
Broad periods are intentionally nullable dates with a separate precision and
reader-facing label; it never invents exact dates.

`site/scripts/export-production-preview-data.py` is the only production page
projection owner. It writes `site/src/data/festival-timeline.json` with source
`sqlite-festival-calendar-v1`. `site/src/pages/festivali/index.astro` consumes
that generated JSON and has no hardcoded TypeScript fallback. A full production
export fails closed if the table/current-year catalog is absent, incomplete or
contains duplicate slugs/orders. The checked release manifest records the
projection hash, source, catalog versions and DB/rendered row counts.

The bounded JSON committed to the repository is only a review fixture; the
production exporter must overwrite it before `build:production`.

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

## Official social video audit

On 2026-07-24 the 21 festivals were checked read-only through the approved
Telegram human session and authenticated VK API, never through VK public HTML.
The result is an inventory for future click-to-load playback, not permission to
autoplay or crop video blindly:

- 4 strong source/formation candidates: `Территория мира`, `Водная ассамблея`,
  `Клуб путешественников`, `Джаз в Филармонии`;
- 13 conditional candidates needing edition, crop, archive or embed-policy
  review;
- 4 without a usable confirmed clip: `Соседи`,
  `Народов много — Родина одна`, `ВитаЛики`, `В единстве наша сила`.

Only `Гроздь`, `Море внутри` and `Короче` have selected current-2026 assets.
Most other exact videos are archive recaps. Future playback must remain
poster-first and click-to-load, keep the uncropped frame in an overlay/contained
player, avoid autoplay, and revalidate source availability before release.
This R9 candidate does not download or embed those videos.
The exact 21-row source/shape/duration ledger is
[`festival-video-audit-2026.md`](festival-video-audit-2026.md).

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

The date and status are top overlays; title and a one/two-line place sit directly
on the lower image gradient, followed by an icon-led translucent theme chip.
The independent festival heart is aligned to the bottom-right card edge; the
theme chip reserves its footprint, so the heart never interrupts the title or
place and never overlaps the category.
There is no bordered or rounded caption panel inside the card. Four-up rows no
longer collapse status text to a bare dot. Source/provenance stays in the data
contract and card destination rather than consuming visible card height.

This is a close implementation of the donor's **card and timeline mechanic**,
not a pixel-for-pixel copy of its unrelated page chrome: the inventory, image
ratios and KenigEvents tokens are real project inputs. The shared visual
contract is nevertheless element-complete: full-bleed media, top date, top
status, bottom title, place, category pill, same-height row, left rail, marker,
month name, complete category inventory and mood copy.

Desktop card hover must not translate the card or scale the image. It may only
change non-geometric affordances (border and shadow); keyboard focus retains a
visible inset outline.

## Pixel acceptance

The Playwright gate uses `887×900`, `1440×900`, `390×844` and
`320×700`, plus the desktop above-fold probe at `1536×700/864`. Required
envelopes:

- `887px`: card plane `700–710px`, timeline `≤1700px`;
- `1440px`: timeline `≤2300px`; four-up `≤300px`, three-up `≤260px`,
  two-up/solo `≤225px`;
- `1536×700`: first card begins no lower than `530px`, and its title is visible
  without scrolling;
- `1536×864`: the complete first card is visible;
- `390px`: two columns, regular tiles `184–214px`, August `≤950px`, full
  timeline `≤3200px`;
- all viewports: zero horizontal overflow, 21 decoded images, no browser or
  first-party request errors, same-height cards per row.

R8 measures `1698.1px` at the `887px` reference viewport and retains
`2279.4px` at `1440px`. The small height increase at 887 is the bounded cost of
the larger type and complete three-line names. Both widths have zero
topline/caption, date/status, heart/category or horizontal-overflow collisions.
At 1440 the measured title is `22.18–24.48px`, place `14.4px`, date
`12.38–13.10px` and category `12.82px`; at 887 the corresponding sizes are
`19.2–19.8px`, `12.3px`, `11.4–11.7px` and `10.8px`. All 21 hearts remain
outside their card anchors and persist across a same-browser reload.
The reference-width rows reproduce the donor card proportions while remaining
far denser than the withdrawn `r1`, whose timeline measured `5729.6px`.

The earlier R3 Gemini Pro `KEEP` is retained only as an external review
artifact, not as product acceptance: subsequent owner inspection found the
off-system serif, missing sticky shelf, page-local service note and moving
hover. A fresh Gemini 3.1 Pro High comparison scored R4 `2/10` and R5 `5/10`,
explicitly rejecting R5 as still “по мотивам”. After R6 removed the nested
caption card, the acceptance pass scored the mechanic `8.5/10` and returned
`CONDITIONAL KEEP`; its three remaining typography/badge corrections are
included in the final candidate and covered by the overlay behavior gate. The
follow-up gate passed every card/timeline criterion, scored the final candidate
`9/10` and returned `KEEP`.

A fresh R8 visual prescription was requested twice through agy with the exact
listed model id `gemini-3.1-pro-high`. Antigravity rejected the authenticated
account before conversation creation because the service is unavailable for
its current location. A bounded direct `gemini-3.1-pro-preview` request with
the R7 screenshot and ten-candidate jazz contact sheet returned `429
RESOURCE_EXHAUSTED` with zero free-tier quota. R8 therefore does not claim
Gemini participation or approval. The icon choice is the recorded manual
contact-sheet result and the page is accepted only by measured binary gates and
visual inspection.

## Checks and generated release

Run:

```bash
npm --prefix site run test:festival-timeline-layout
pytest -q tests/test_festival_calendar_static_projection.py
PREVIEW_BUILD_ID=preview-20260726-festivals-calendar-r9 npm --prefix site run build:preview
PREVIEW_BUILD_ID=preview-20260726-festivals-calendar-r9 npm --prefix site run check:unified-prototype
```

The production path additionally requires `build:production`,
`check:production`, `build:secret-candidate`, `check:secret-candidate` and the
Chromium release gate. Those gates require `/festivali/`, 21 current DB rows in
the initial 2026 catalog, an indexable root-form route, a `noindex` prefixed
candidate, decoded images and zero horizontal overflow at 1440 and 390 pixels.

Review candidate:

- page: <https://kenigevents.ru/preview-20260726-festivals-calendar-r9/festivali/>;
- hub: <https://kenigevents.ru/preview-20260726-festivals-calendar-r9/__preview/>.

The immutable URL is a public bearer link, not authentication. The repository
still has no safe reader-atomic root publisher; generating and checking the
root-form artifact is not a claim that `kenigevents.ru/festivali/` has been
published.
