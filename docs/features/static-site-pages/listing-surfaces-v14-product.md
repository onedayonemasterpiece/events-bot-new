# Listing surfaces V14: product and composition decisions

> **Status:** desktop preview candidate, 2026-07-18. Mobile is deliberately not the acceptance focus of V14.
> **Surfaces:** `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`.

## What problem the pages solve

The date pages are not catalog grids. They answer, in order: **when can I go, what is available at that exact time, and which option is worth opening?** Popular is a discovery stream: **what has current audience evidence, and can I continue scanning without reaching a horizontal dead end?** Consequently:

- exact time is the primary axis on Today/Tomorrow/Weekend;
- all events at one exact time share one group and wrap into further rows;
- media width, not title length, determines packing;
- Popular uses the same intrinsic cards but wraps vertically for the full list;
- an empty side of a Weekend hour remains empty: it communicates that the other day alone has a start at that time and must not be filled with a misleading duplicate time label.

## Product decisions

### Today and Tomorrow

- Exhibitions and continuing month-scale entities are removed from the primary date stream while there are enough atomic starts. They remain discoverable through Exhibitions/Search; a multi-day festival with a real start is not automatically equivalent to a six-month exhibition.
- Today shows an event as past only when its explicit `end_at` has elapsed. Start time alone is insufficient because a concert or festival may still be in progress.
- Past events stay above the current/future stream in a collapsed `Завершились` section. Their images are desaturated, but text and actions keep normal contrast.
- `Сейчас · HH:MM` is a visible separator. Tomorrow has no false current-time state.
- `Время уточняется` is a bottom content section, not a navigation goal. Missing source data is not a user intent.

### Weekend

- One strong time column is followed by two continuous day lanes. Day identity lives in one sticky header per lane; `Сб`/`Вс` are the only filled chip text, while the date and event count remain normal typography.
- The same time is never repeated independently inside Saturday and Sunday. This prevents the vertical desynchronization seen in V13.
- The current weekend stays one page even with dozens of events. No hidden pagination or `Смотреть все` conversion detour is introduced.
- When the current date is Sunday, a conservative one-shot auto-position may move to the first not-earlier Sunday start only on a fresh navigation. Hash navigation, back/forward, prior scroll, user input and reduced-motion preference cancel it; a visible return-to-top action is provided.
- Adjacent weekends are linked after the schedule with compact smart ranges (`25–26 июля`, `31 октября – 1 ноября`).

### City selection and personalization

- Desktop city selection is one direct sticky horizontal chip rail, not a dropdown. `Все` is the safe default; multi-city selection remains possible and counts are recomputed on the same page.
- The rail begins after the 240×88 brand tag while sticky, so controls do not pass under the tag.
- Full list remains the default. `Для меня` is only enabled when a compatible consented profile produces a real different set; V14 must not present a decorative disabled promise.
- A combined Today+Tomorrow desktop view remains an explicit later experiment, not the default. It needs observed comparison behavior and an easy persistent off switch before implementation.

### Media, medallions and actions

- OCR media keeps its natural geometry; it is never widened by cropping away text.
- A reviewed no-OCR source may crop adaptively to 3:2/4:3. A large no-OCR portrait whose focal review is still pending may use only a conservative square floor; it does not get the stronger 3:2 claim.
- A 180px-wide thumbnail is not enlarged into a 300–400px desktop frame. A source-manifest replacement is applied before this quality gate; otherwise the shared neutral fallback is more honest than visible upscale.
- Available wide alternatives win within the same source inventory (control `6875`: 1280×960 beats 750×1000).
- A source candidate classified `no_event_relevance` fails closed (control `6904`); it cannot become a listing image through generic fallback.
- Identity medallions are recognition and trust aids, not decoration. V14 uses 72px (about 29% above V13's 56px), may show up to three in an external vertical rail for OCR, and may overlay one identity only on a safe no-OCR visual. `Бесплатно` is a first-class 0 ₽ medallion.
- The shared static-site Share icon is shown before Like. Counts are visible only when non-zero. A wide safe no-OCR visual may absorb the action rail; OCR keeps it outside.
- The title may use free copy space only for the actual last card of a rendered row, at lower priority than adaptive media growth. It never changes flex basis or pushes the next card.

## Automatic source-media contract

V13 event `3794` used a manually selected 1024×683 Cathedral article photo keyed to that event. Although the photo was official, this did **not** demonstrate automatic generation from available sources. V14 removes the association.

The only acceptable general path is:

1. traverse canonical `source_url/source_urls` with a host adapter;
2. enumerate source-grounded media candidates;
3. materialize dimensions/hash/derivatives;
4. run the common event-relevance, OCR, role, focal and crop decision;
5. persist a durable `source URL → candidate asset → decision/version` manifest;
6. let every renderer consume that manifest without `event_id` media overrides.

V14 implements the renderer-side fail-closed and stable source-page manifest lookup, but not a complete Cathedral/article crawler. Therefore `3794` correctly uses its 300×174 canonical source only if it passes the general frame gate, otherwise the neutral fallback. The automatic source adapter remains follow-up work and must not be reported as complete.

## Fresh-data evidence

Production snapshot: `2026-07-18T09:52:41Z`, 6549 event rows; preview export: `2026-07-18T09:54:16Z`, 220 bounded real events.

- 18 July: 46 raw date starts; 43 primary non-exhibition/deduplicated Weekend events.
- 19 July: 28 primary starts.
- Weekend consumer surface: 71 events total across the two lanes.
- `6904`: rejected automatically as `no_event_relevance`.
- `6932`: 48×48 candidate is below the desktop quality gate.
- `3794`: only the canonical 300×174 source is present; the rejected listing-only 1024×683 file is not selected.
- Popular: 60 events, vertical wrap, no document horizontal overflow at 1920px; the first measured row contains four different-width cards.

## Reference research and what was rejected

Pinterest research collected 120 candidates from 12 schedule/multi-city/editorial queries. The durable collection is `20260718-kenigevents-desktop-schedules-multi-city-v14`; the 18-candidate contact sheet is stored as a non-committed artifact. Critical review retained only nine mechanics for comparison, including [compact adjacent-date tiles](https://www.pinterest.com/pin/929641548114774296/) and [a strong left time axis with a compact right flow](https://www.pinterest.com/pin/742671794853014098/).

Literal calendar grids, TV schedules, Gantt/timetable matrices, SaaS dashboards and poster walls were rejected. They optimize allocation or decoration, not rapid consumer discovery across exact times, cities and heterogeneous media.

## Deferred questions and measurement

- Does the combined Today+Tomorrow experiment reduce time-to-first-detail without reducing depth on either day?
- At which catalog size should continuing festivals receive a separate compact module instead of Search/Exhibitions only?
- Do city multi-select users commonly compare destinations, or is a single active city plus `Все` sufficient?
- Do medallions improve detail opens and trust, or merely increase visual weight? Measure opens by medallion presence/type and guard against CTR-only conclusions.
- Personalization success requires coverage and satisfaction guardrails: detail opens, saves/likes, return-to-list, diversity, city/date availability and an explicit full-list recovery path.

## Final critical acceptance

The published desktop pages and four FHD screenshots received a final `agy/Opus` review on 2026-07-18. Verdict: **PASS WITH FOLLOW-UPS**, with no blocker before user review. The consultant accepted the time-first composition, continuous Weekend lanes, vertically wrapping Popular stream, 72px medallions and Share → Like rail. It also verified that event `3794` renders a neutral category fallback rather than the rejected manually selected photo.

The follow-ups remain explicit rather than being folded into the acceptance claim: implement the general source-page crawler; browser-test past-image desaturation against events with an explicit elapsed `end_at`; test the maximum three-item identity rail and zero-count action policy; and hide an entire Weekend time row when city filtering leaves both day lanes empty.
