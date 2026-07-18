# Listing surfaces V15: product and composition decisions

> **Status:** desktop preview candidate, 2026-07-18. Mobile is deliberately not the acceptance focus of V15. The file keeps its historical path because it records the V14 → V15 correction without creating a second competing specification.
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
- Past events stay above the current/future stream in a collapsed `Завершились` section. Their images are desaturated, but text and social evidence keep readable contrast.
- `Сейчас · HH:MM` is a visible separator. Tomorrow has no false current-time state.
- The exact-time label is a persistent reading axis: while a long `17:00` group scrolls, `17:00` stays pinned below the discovery rail until the next exact time replaces it. This is a navigation/context function, not decorative sticky behavior.
- `Время уточняется` is a bottom content section, not a navigation goal. Missing source data is not a user intent.

### Weekend

- One strong time column is followed by two continuous day lanes. Day identity lives in one sticky header per lane; `Сб`/`Вс` are the only filled chip text, while the date and event count remain normal typography.
- The same time is never repeated independently inside Saturday and Sunday. This prevents the vertical desynchronization seen in V13.
- The current weekend stays one page even with dozens of events. No hidden pagination or `Смотреть все` conversion detour is introduced.
- When the current date is Sunday, a conservative one-shot auto-position may move to the first not-earlier Sunday start only on a fresh navigation. Hash navigation, back/forward, prior scroll, user input and reduced-motion preference cancel it; a visible return-to-top action is provided.
- Adjacent weekends are linked after the schedule with compact smart ranges (`25–26 июля`, `31 октября – 1 ноября`).

### City selection and personalization

- Desktop city selection is direct and always visible, not a dropdown. V15 uses quiet link-like checkbox labels rather than large pills; `Все` is the safe default, multi-city selection remains possible and counts are recomputed on the same page.
- City counts are secondary to direct access: single-result counts are omitted on compact desktop, and the cities-only Popular rail may omit all counts to keep one balanced line at 1536px. Exact-time/daypart counts remain visible because they directly estimate choice density.
- Cities and time navigation live on one light sticky discovery surface as two semantic rows. The rows may wrap at narrower desktop widths, but must never require horizontal scrolling. The rail begins after the brand tag so controls do not pass under it.
- Full list remains the default. `Для меня` is only enabled when a compatible consented profile produces a real different set; V15 must not present a decorative disabled promise.
- A combined Today+Tomorrow desktop view remains an explicit later experiment, not the default. It needs observed comparison behavior and an easy persistent off switch before implementation.

### Media, medallions and social proof

- OCR media keeps its natural geometry. Missing OCR is `unknown`, not proof of no text; even a short date/name is compositionally meaningful. OCR, unknown documents, identity posters, schedules and attendee instructions never enter adaptive crop. Their vertical retention is `1.0` (and in any later bounded-cover experiment must remain at least `0.8`).
- When the same approved event inventory contains several `unknown` assets, the wider authored candidate may win in its own natural ratio (control `6875`: 1280×960). This is candidate selection, not a crop claim: no pixels are discarded and the mode remains `unknown-natural`.
- Only a classified event photo with event relevance, `safe_crop`, focal evidence and a reviewed/high-confidence media role may crop adaptively. This is fail-closed: no OCR result alone never unlocks crop.
- A 180px-wide thumbnail is not enlarged into a 300–400px desktop frame. A source-manifest replacement is applied before this quality gate; otherwise the shared neutral fallback is more honest than visible upscale.
- Available wide alternatives win within the same source inventory (control `6875`: 1280×960 beats 750×1000).
- A source candidate classified `no_event_relevance` fails closed (control `6904`); it cannot become a listing image through generic fallback.
- Identity medallions are recognition and trust aids, not decoration. V15 shows at most three in a `52…60px` external rail, removes the universal border/shadow from both external and safe-image-overlay medallions so authored rings such as KGD80/KONB are never doubled, and lowers saturation/opacity until hover/focus. Identity precedes the quiet monochrome `0 ₽ / БЕСПЛАТНО` token.
- The listing supports `scan → shortlist → open detail`; final intent/actions belong to the detail page. Therefore listing Calendar is absent and Share/Like are **static social-proof**, not feedback/share buttons. Each metric renders only when its aggregate is non-zero; if both are zero the whole rail is absent and consumes no width. The shared visual order remains `Share → Like`; a locally liked event may tint the existing Like proof but clicking the proof only opens detail. No overlay social controls are used.
- Zero is treated as “no trustworthy signal yet”, not as “unpopular”: there is no `0`, empty placeholder, disabled icon or negative styling. This limits cold-start bias against new/small-city events while still letting real non-zero activity guide what to open.
- The title may use free copy space only for the actual last card of a rendered row, at lower priority than adaptive media growth. It never changes flex basis or pushes the next card.

## Existing source-media contract — no new crawler

V13 event `3794` used a manually selected 1024×683 Cathedral article photo keyed to that event. Although official, it did **not** demonstrate automatic generation from the content already attached to the event. V14 correctly removed that substitution, but its document then incorrectly invented a future “general crawler” as if it were an agreed system requirement.

**Correction:** no additional crawler was planned or approved for these listing pages. The product contract is to consume the existing event media/OCR/Smart Update projection and the already supported Telegram/VK/source adapters. Listing rendering must not compensate for a framing bug by introducing a new ingestion subsystem.

The accepted path is:

1. consume the canonical event's existing approved media inventory;
2. reject `no_event_relevance` and quarantined candidates;
3. preserve exported OCR/media-role uncertainty fail-closed;
4. choose the best eligible existing candidate and its derivatives;
5. render a slightly sub-threshold but event-relevant canonical image as a no-upscale last resort before using a neutral fallback.

Control `3794` therefore uses its existing 300×174 canonical photo, at its true size, rather than the rejected event-specific 1024×683 substitution or an empty card. Control `6904` remains empty because its only candidate is explicitly `no_event_relevance`.

## Fresh-data evidence

Production snapshot: `2026-07-18T09:52:41Z`, 6549 event rows; V15 preview export: `2026-07-18T12:06Z`, 220 bounded real events.

- 18 July: 46 raw date starts; 43 primary non-exhibition/deduplicated Weekend events.
- 19 July: 28 primary starts.
- Weekend consumer surface: 71 events total across the two lanes.
- `6904`: rejected automatically as `no_event_relevance`.
- `6932`: 48×48 candidate is below the desktop quality gate.
- `3794`: the canonical 300×174 source is rendered without upscale; the rejected listing-only 1024×683 file is absent.
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

## Critical consultant gate

Before browser acceptance, the evidence and implementation plan received a full **agy Gemini 3.1 Pro (High)** review on 2026-07-18. It rejected the crawler scope, chose the monochrome Free reference (Ref 2 / option 10), removed universal medallion rings and selected the two-light-row Weekend shell over pills, a dropdown or a graphite bar. A second targeted review accepted the owner's behavioral model and replaced premature listing actions with non-zero-only static Share/Like proof; Calendar remains detail-only.

Browser acceptance is measurable: at 1536×864 the sticky site header plus discovery rows must occupy no more than `150px`; city controls must have no horizontal overflow; each exact-time marker must remain pinned until the next group pushes it away; text-protected cards retain at least 80% vertically (V15 uses 100%); `3794` renders 300×174 without upscale; external medallions have no added border/shadow; and no action control is nested inside a media link.

Final local acceptance on the immutable V15 build checked 202 rendered cards across the four consumer surfaces. All 181 cards using a natural (non-`visual-crop`) mode matched their selected source ratio within `0.012`; controls `3794`, `3795` and `4785` rendered at `300×174`, `300×199` and `300×225` respectively, without a fixed-height `contain` frame. The browser found 184 cards with at least one non-zero static social signal and 18 with the whole signal rail absent. All four pages had zero document/city overflow at 1536×864; the discovery stack measured 145px on date/weekend pages and 101px on Popular. The `17:00` marker remained at `157px` through three scroll positions.
