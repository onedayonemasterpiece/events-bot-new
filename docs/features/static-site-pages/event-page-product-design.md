# Event Page Product & Design Spec — «Полюбить Калининград Анонсы»

> **Status:** production event-page contract; mobile baseline and desktop continuous Editorial composition implemented.
> **Brand/page family:** «Полюбить Калининград Анонсы»
> **Target:** `https://kenigevents.ru/sobytiya/<stable-slug>/`
> **Publish target:** production Yandex Object Storage bucket/domain `kenigevents.ru`; preview prefixes remain `noindex`.

## 0. Answer to “is this already designed?”

Partially, but not enough for implementation.

Existing docs already cover:

- static-first architecture and bucket/deploy target: [static-site-pages/README.md](README.md);
- MVP related-events personalization contract: [unsigned-personalization/event-detail-related.md](../unsigned-personalization/event-detail-related.md);
- personalization production gates/write-path: [production-integration.md](../unsigned-personalization/production-integration.md) and [opus-review-2026-06-27.md](../unsigned-personalization/opus-review-2026-06-27.md);
- reference mechanics/competitors: [interface-references.md](interface-references.md).

Missing before this document:

- a concrete product page anatomy;
- a mobile/desktop visual layout contract;
- a migration mapping from the current Telegraph event page;
- CTA hierarchy beyond “ticket/register”;
- clear placement of “Похожие события”, promo and personalization;
- acceptance gates for the first generated event pages.

This document fills that gap.

## 1. Current Telegraph baseline

Current Telegraph event pages are a compact public card generated from canonical event fields and `event.description`.

Observed/implemented baseline in the Telegraph renderer:

1. cover image/poster when available;
2. logistics infoblock:
   - lifecycle status: cancelled/postponed/etc.;
   - date/time;
   - calendar/ICS link;
   - linked occurrences as “Другие даты”;
   - venue/address/city;
   - Pushkin card marker;
   - ticket/free/sold-out/phone-only contact;
3. `search_digest` as a short “what is this?” paragraph;
4. full event description;
5. video links/media when available;
6. source/footer/provenance.

What Telegraph does well:

- it is simple and reliable inside Telegram previews;
- it already has a useful logistics infoblock;
- it already handles ICS, other dates, sold-out/free/phone-only cases;
- it is generated from the same event source-of-truth.

What the new page must improve:

- owned canonical URL, SEO/GEO, JSON-LD, sitemap;
- real visual hierarchy and mobile CTA;
- share/calendar/copy actions that work naturally on phone;
- related events and later personalization;
- desktop-native layout instead of a single narrow article;
- analytics of impressions/CTA/share without raw firehose;
- brand and trust: “Полюбить Калининград Анонсы”.

## 2. Product principle

The event page is not a blog article and not a ticketing marketplace clone. It is a fast local event decision page:

```text
Can I understand the event in 5–10 seconds?
Can I decide whether to go?
Can I save/share/buy/register without hunting?
Can I continue to something relevant if this event is not right?
```

Primary product metric remains **time-to-interest**: time until the visitor finds a relevant event or performs a meaningful action.

## 3. Brand direction

Working name in UI:

```text
Полюбить Калининград Анонсы
```

Brand meaning:

- local, warm, trustworthy, not “big ticketing platform”;
- editorial enough to feel curated;
- practical enough to be an афиша;
- visually modern but not nightclub-only and not childish.

Tone of voice:

- direct Russian copy;
- no manipulative urgency unless factual (`sold_out`, registration deadline);
- reason chips are useful, not creepy: “похоже по джазу”, “рядом”, “в эти выходные”, not “мы знаем, что вы любите…”.

Recommended visual direction:

- **content-first editorial card UI**;
- light warm background, strong dark text, one accent color;
- generous mobile spacing, dense but calm desktop grid;
- poster/media drives emotion; UI should not fight posters.

Initial design tokens:

| Token | Value | Notes |
| --- | --- | --- |
| Background | warm off-white `#FFF8EF` or neutral `#FAF7F2` | avoids cold SaaS feel |
| Text | deep ink `#1F2933` | high contrast |
| Muted text | slate/stone `#667085` | still accessible |
| Primary accent | Baltic amber `#D97706` | CTA, highlights |
| Secondary accent | sea green/teal `#0F766E` | calendar/share/success |
| Danger | `#B42318` | cancelled/sold out |
| Radius | 16–24px cards, 999px chips | soft local guide feel |
| Shadow | low, warm, content-card only | avoid heavy marketplace clutter |
| Icon style | one SVG stroke set, 1.75–2px | no emoji as structural icons in final UI |

Typography:

- system-first for performance (`Inter`/system sans) or one hosted font pair after asset gate;
- body minimum 16px mobile;
- line-height 1.5–1.65;
- desktop description max line length 65–75 characters.

## 4. Page information architecture

### Required sections

1. **Header / brand bar**
   - brand: «Полюбить Калининград Анонсы»;
   - city/context: Калининград and region;
   - compact nav: Сегодня, Выходные, Бесплатно, Поиск (later);
   - on mobile, header is simple and non-distracting.

2. **Hero / decision block**
   - poster/cover;
   - title;
   - category/type chip;
   - date/time;
   - venue/address/city;
   - price/status/free/sold-out;
   - primary CTA;
   - secondary actions: add to calendar, share, copy link/save.

2a. **Event medallion row**
   - large quick-read medallions of this concrete event, immediately after the title/summary area;
   - P0 examples: organizer avatar, `Пушкинская карта`, charity, kids/family, video/recording status;
   - canonical contract: [event-token-medallions.md](event-token-medallions.md);
   - this is not a listing-card badge row; cards only need weekday + event type formatting unless separately redesigned.

3. **Trust/provenance strip**
   - source/organizer if known;
   - last updated;
   - “проверяем данные по источнику” wording if ticket status unknown.

4. **Short explanation**
   - `search_digest` or one-sentence summary;
   - answers “что это?” before long text.

5. **Details / description**
   - full description from Smart Update;
   - program/people/venue details if known;
   - media/video below or inline depending quality.

6. **CTA repeat / action rail**
   - repeats primary CTA after description;
   - calendar/share/copy remain available;
   - on mobile use sticky bottom action only if it does not hide content.

7. **Расписание этой программы**
   - belongs to the primary date/time owner, not to a separate event-card feed;
   - same event occurrence group, separate from related events;
   - cancelled/postponed marked clearly.

8. **Похожие события**
   - static fallback in HTML;
   - after consent local rerank may reorder within constraints;
   - no visible jump after user has already read the block.

9. **Nearby / same venue / same weekend modules**
   - desktop modules can be separate;
   - mobile can appear as compact continuation after related.

10. **Footer**
    - brand, canonical URL, contact/source policy;
   - Telegraph compatibility link during dual-run if useful for admins, not as primary user CTA.

### Related dates and times in the accepted compositions

The rejected first pass placed a new large `Когда` module into a legacy white
hero/side-panel composition. That changed the accepted hierarchy, duplicated
the date below the description and made the visitor interpret another block
before reaching the ticket action. It is not a valid base for the final
templates.

The accepted solution separates a quiet hero summary from the complete desktop
schedule and never makes the visitor open a disclosure:

- **desktop Split:** the selector stays in the top-right date/time area of the
  cream reading header;
- **desktop Continuous Editorial:** it stays in the date/time area of the cream
  information sheet;
- **mobile accepted-v8:** it stays inside the existing compact weekday/date/time
  strip below the dark action dock and above the venue row;
- the graphite ticket/action dock is never made the owner of event dates;
- with one date and one time, the accepted compact date line remains unchanged;
- mobile preserves the accepted strong current row. Same-day alternatives use
  quiet `Другое время` text links; other dates follow under a non-interactive
  `Другие даты` label as an always-open compact list. Links have 44 px-class hit
  areas without visually heavy pills;
- desktop hero never renders the full date matrix before H1. It shows the current
  slot, same-day `Другое время` links and, when needed, `Другие даты (N) ↓` as a
  native anchor to the lower complete schedule;
- the lower desktop `Когда` groups all occurrences by date. Its current time is
  a filled, non-link chip with `aria-current="page"`; every alternative is an
  underlined canonical `<a>` to that exact occurrence;
- no `<details>`, dropdown, carousel or horizontal scroll is used. Dense dates
  remain visible in the mobile list and lower desktop schedule, not above H1;
- desktop writes the weekday in full (`воскресенье`), preserving the accepted
  typography. Mobile retains the compact two-letter weekday badge;
- mobile alternatives keep at least a 44 px target; long rows wrap naturally;
- the current slot and exact same-slot duplicates are excluded; only active,
  eligible future occurrence URLs are offered;
- the old full-size event-card grid below the description is removed;
- desktop intentionally uses two projections of the same component data: a
  bounded summary above and the exhaustive schedule in
  `Перед посещением → Когда`. The anchor connects them and canonical links are
  generated from the same occurrence set, so they cannot disagree.

This keeps the label explicit without adding instructional prose: the visitor
can compare every available slot immediately, choose the exact occurrence and
only then register or buy its ticket.

The layout is based on linked-occurrence data rather than only the current
preview window. On 2026-07-18 the active production slice contained `305`
eligible events, `38` linked events and `16` mutual groups: `14` groups had two
dates and `2` had three. That slice happened to have one time per day, but the
historical canonical graph contains `79` groups with multiple times on one day.
Real examples reach five times per day (the ship excursion at `11:00`, `12:30`,
`14:00`, `15:30`, `17:00`) and seven-date programs with up to four times per
day. Therefore a permanently collapsed two-date control would optimise for a
temporary sparse window and fail known seasonal schedules.

## 5. CTA contract

Primary CTA is selected by event facts:

| Event state | Primary CTA | Secondary text |
| --- | --- | --- |
| `ticket_link` + paid/available | `Купить билет` | price range if known |
| registration link | `Зарегистрироваться` | “может потребоваться регистрация” |
| free + explicit registration link | `Зарегистрироваться` | admission property must still say `Бесплатно · регистрация` |
| free without registration requirement | `В календарь` | calendar save is also the service saved-event action; source remains secondary |
| phone-only booking | `Позвонить` on mobile, branded `Показать телефон` on desktop | the desktop copy-icon CTA reveals and copies the normalized number in one click, then confirms success without trying to open a dialer |
| paid/ticketed, price unknown | `Билеты` | never invent `Узнать цену`, `Узнать условия` or `По билетам` |
| source link only / unknown tickets | `Источник события` | honest secondary destination, no fake ticket CTA |
| sold out | `Билеты закончились` as disabled/status | offer related events, not fake CTA |
| cancelled/postponed | status banner first | no ticket CTA unless new date/source exists |

Secondary CTAs:

- **Добавить в календарь** — always if date exists; ICS download link; on mobile must open/download predictably.
- On desktop, a secondary calendar action may adapt its visible label from local
  usage history while preserving its accessible name: show `В календарь` for a
  new user, fewer than three uses, or no use in the last 30 days; collapse to the
  calendar icon for a regular recent user. Primary calendar CTA and mobile
  actions do not use this adaptive desktop presentation rule. Successful or
  fallback calendar clicks update a bounded local counter; saved-event expiry
  remains a separate state.
- **Поделиться** — use Web Share API on mobile; fallback to copy link on desktop/unsupported browsers.
- **Скопировать ссылку** — visible fallback; toast “Ссылка скопирована”.
- **Сохранить** — MVP can be local-only after consent; if no consent, show “сохранить в браузере?” prompt or keep for post-MVP.
- **Не интересно / скрыть похожее** — only on related cards, not on the current event hero.

CTA quality rules:

- one primary action per viewport;
- all touch targets at least 44px;
- no icon-only action without label/aria-label;
- sticky mobile CTA must reserve bottom padding and not cover content;
- share/calendar/ticket clicks must attach `event_id`, `surface`, `viewport_class`, `layout_mode`, `served_list_id/hash` when applicable.

## 6. Mobile layout

Mobile goal: decide quickly with one thumb.

```text
[Brand bar: Полюбить Калининград Анонсы]

[Poster / image]
[category]
H1 Event title
[Primary CTA full width]
[Calendar] [Share] [Copy]

[weekday] [current date] [current time]
[Другое время: time link, time link]
[Другие даты]
[weekday] [date] [time link, time link]
[venue/address row]
[price/status row]

[Short summary]
[Details collapsed after 2-3 paragraphs if long]

[Похожие события]
  [vertical related card]
  [vertical related card]
  [vertical related card]
  [Показать ещё]

[More this weekend / same venue]
[Footer]
```

Mobile cards:

- image aspect ratio around 4:3 or 16:10;
- title max 2–3 lines before expansion;
- date/venue/price always visible;
- CTA or “Подробнее” visible without precise tap;
- reason chip max 1–2 chips (`похоже`, `рядом`, `в эти выходные`).

Personalization on mobile:

- first related chunk may rerank after consent;
- if block is already visible/read, do not suddenly reorder; apply rerank to “Показать ещё” chunk or show subtle cue “Подборка обновлена”; no layout jump.
- exploration must be visible enough: for 6 cards, reserve at least 1 novelty/diversity slot; for a future feed, 1–2 slots per 6 depending category density.

## 7. Desktop layout

Desktop goal: feel like a proper афиша page, not a stretched mobile card. These
rules apply only at desktop breakpoints; the accepted mobile composition must
not be reflowed by desktop experiments.

### Desktop production composition

- Primary family is continuous Editorial: a strong non-document image occupies
  the full desktop canvas; its image layer moves upward more slowly than page
  content (continuous positive parallax) and exits naturally, without a late
  acceleration jump or permanent pin.
- One continuous information slab starts with category/date/title and includes
  medallions, place, short digest, full description and other dates. It scrolls
  as one honest block; description is not duplicated below the hero.
- The compact media rail starts in the low-attention top-right image area and
  stays available. CTA begins at the same lower decision position as the
  information slab; it joins the upward movement when the slab reaches it,
  then remains sticky below the fixed header.
- Rail, CTA and optional poster companion are bounded by the end of the **full
  reading shell**, not the hero. They release before `Смотрите дальше` with a
  safe bottom gap, never collide with the graphite continuation block.
- A dedicated poster companion exists only for a successfully classified
  `event_identity_poster`. It preserves the exact source ratio inside a thin
  graphite frame, with no copy, backdrop fields or crop. Other documents
  (services, schedule, wayfinding, sponsors) stay in the gallery/rail and are
  never called an афиша.
- An `unknown_document`, attendee-information or other non-identity document
  must not become the desktop hero merely because it is the first source image.
  When the same event has an explicitly classified, safely coverable
  horizontal `event_photo`, the Editorial family uses that photo and keeps the
  document available in the rail/gallery. This is a role-first rule; OCR
  emptiness, filename order and broad text heuristics do not promote media.
- If no strong horizontal/photo hero is available, the split fallback keeps a
  document/portrait media column and a readable information column. OCR text is
  never horizontally cropped.
- The fullscreen efficient portrait viewer is enabled for galleries dominated
  by vertical images. It fills the viewport with multiple natural-height
  images; next/previous buttons, keyboard arrows and swipe all advance by a
  viewport group in both directions. It never silently falls back to a
  one-image viewer.
- The efficient viewer is quality-aware. When at least four technically strong
  assets exist (`long edge >= 720`, `>= 450k` pixels and imported quality score
  `>= 10`), only that strong set participates in the grouped viewer and
  the header discloses `Показаны N из M изображений в лучшем качестве`.
  Materially weak media remains excluded when a strong set exists; if fewer
  than four strong assets exist, the viewer keeps the full source set rather
  than hiding the only available event evidence. This is a deterministic
  presentation gate over upstream quality metadata, not a semantic substitute
  for the LLM-first media-role pipeline.
- Desktop fullscreen rendering is fail-closed: only a classified `event_photo`
  with `recommended_hero_fit=cover` and `safe_crop=true` may fill by cover.
  Posters and all OCR/document media use `contain`, do not auto-pan, and remain
  fully readable. An un-dragged click on the displayed image or empty backdrop
  closes the viewer in addition to `×` and `Escape`; clicks on navigation,
  links or the terminal recommendation never close it.
- Both desktop and mobile fullscreen galleries render the shared
  `AnnouncementsLockup` (`240×88` desktop, `128×96` mobile). A hand-built copy
  of the retired tag/wordmark is forbidden. This shared brand replacement is
  the only mobile change in the 2026-07-17 desktop remediation.
- The insufficient-evidence feedback placeholder (`Общего вывода пока нет` /
  `Отзывов недостаточно`) is omitted. A future feedback block may render only
  a substantive, evidence-backed aggregate.
- Related cards use a common bounded media height. Documents/OCR are scaled to
  full card width and may overflow only vertically (centered, or shifted by a
  trusted focal Y); source left/right edges are never cut and no side fields are
  introduced. Ordinary cover is unlocked only by the explicit LLM-authored
  `event_photo` role. Missing/unknown roles and legacy `visual_only` remain
  width-fit/no-horizontal-crop until semantic enrichment succeeds.

Desktop header uses the shared announcement lockup at `240×88`, menu on the
right, exact-listing active state only, and no selected item on event details.
Keyboard navigation and visible focus remain required.

The graphite desktop action panel is component-responsive, not only
viewport-responsive. A phone action is a stricter exception: admission,
branded phone CTA, calendar, share and like remain on one explicit grid row at
`1366×768`, `1536×864` and `1920×1080`; it must never fall back to the generic
stacked layout. The calendar is icon-only in this constrained phone panel even
when bounded usage history would normally expand its wording. The desktop phone
journey keeps the established branded primary CTA instead of plain contact text
plus a detached utility icon. Its initial label is `Показать телефон`; the
leading glyph is the standard copy icon, not a redundant handset. One click
reveals the standard-size formatted number inside the same CTA and copies its
normalized value. A transient non-layout toast says `Номер скопирован`, while a
polite live region announces the same result. Subsequent clicks copy again. The
outer CTA and graphite panel retain their dimensions through reveal and success.
Browser acceptance compares child vertical
centres/overlap and before/after-copy geometry; containment-only assertions are
not sufficient.

The exact-venue KAUP journey is not desktop-only. On the accepted phone event
surface it appears after `Коротко` as one flat compact block with the same
actionable order: recommended official transfer, the reviewed `Северный вокзал`
boarding point with estimated `terminal + 15 min` departures, Romanovo-to-venue walk, explicit no-return warning
and the car alternative. Only transfer boarding fine print is collapsed into a
native `details` disclosure; origin, departures and the return risk remain
visible without a tap. Transfer, bus, last mile, warning and car are flat rows,
not nested cards; map actions are icon-only `44×44` pins with accessible names.
This compact variant has no raw-coordinate label, pseudo-map decoration or
button wall, and standard bus/walk/car/pin icons keep mode changes scannable.

## 8. Related events and personalization placement

### Static fallback

`Похожие события` must be useful in plain HTML:

- 6 visible candidates for MVP;
- generated from `static_related_v1`;
- other dates excluded into `Другие даты`;
- cancelled/past excluded;
- sold-out follows documented rule;
- reason codes available in manifest and optionally rendered as compact chips.

### Local personalization after consent

Allowed:

- reorder related cards within the same candidate pool;
- hide explicit `hidden_event_ids`;
- downrank/omit explicit negative interests;
- attach served-list summary to the selected write path.

Forbidden:

- online LLM/vector calls on page view;
- replacing the block with unrelated categories just because profile likes them;
- adding links not present in static manifest for MVP-0;
- visible reorder/jump after user has already engaged with the block.

### Anti-bubble rule

Personalization must not trap users in one category:

- top slots remain page-context-aware;
- diversity slot in first 6 related cards;
- avoid more than 2–3 same-category cards in a row unless the candidate pool is truly narrow;
- explicit hides are hard veto; inferred negative interests decay and should not permanently erase a whole cultural category;
- promo cannot override explicit hide or `audience_exclusion_tags`.

## 9. Promo integration

Promo events are not banner ads; they are event cards with disclosure.

Rules:

- label: `Партнёрское` / `Реклама` / `При поддержке` depending legal/product decision;
- at most 1 promo card in the first 6 related cards for `event_detail_related`;
- never promote cancelled/past/wrong-city events;
- never override `audience_exclusion_tags`;
- explicit hide/not interested suppresses repeated promo exposure;
- frequency cap stored locally and/or compact server summary;
- promo exposure must be measurable separately from organic recommendation impressions.

Good placement:

- desktop: one labelled promo card in related grid or right rail mini-module;
- mobile: one labelled card after 2 organic cards, not as hero takeover;
- never between title and primary CTA on event detail page.

## 10. Analytics and telemetry

The page must collect product evidence without filling Supabase.

MVP event kinds:

- `page_view` / valid detail view summary;
- `primary_cta_click`;
- `calendar_click`;
- `share_click` / `copy_link`;
- `related_served_list_summary`;
- `related_card_click`;
- `hide_event` / `not_interested`.

Do not store raw scroll firehose. For weak signals use compact rollups:

```text
surface = event_detail
viewport_class = mobile | tablet | desktop
layout_mode = detail_mobile | detail_desktop | related_module | related_grid
cta_state = ticket | registration | free | phone | unknown | sold_out | cancelled
related_algorithm_id = static_related_v1 | local_related_rerank_v1
served_list_id/hash = only for related block
```

Metric cuts required:

- mobile vs desktop separately;
- CTA state separately;
- organic vs promo separately;
- static fallback vs local rerank separately;
- Supabase/API unavailable fallback rate.

## 11. SEO/GEO and crawler contract

Every production event page requires:

- unique `<title>`: `Название — дата, Калининград | Полюбить Калининград Анонсы`;
- meta description from `search_digest` + date/place;
- canonical URL;
- OG/Twitter title/description/image;
- JSON-LD `Event` matching visible facts;
- breadcrumbs;
- sitemap entry with `lastmod`;
- status handling for cancelled/postponed/sold-out/completed.

Crawlers and preview bots see the same static fallback content as users before personalization. Personalization cannot remove SEO-critical links or show a materially different page to bots.

Preview prefixes in the production bucket must be `noindex, nofollow, noarchive` and must not leak production canonical URLs unless intentionally testing canonical behavior.

## 12. First vertical slice

Now that the production bucket/domain exists, the first slice should generate a small set of real pages.

Recommended MVP slice:

- 5–10 future active events from production data;
- include varied states:
  - paid with ticket link;
  - free event;
  - registration event;
  - unknown ticket/source-only;
  - event with other dates or same venue alternatives;
- generate both production-capable and preview-capable static trees;
- publish preview first under `preview-<timestamp>-<random>/`;
- production root upload only after checks pass.

Minimum artifacts:

```text
/sobytiya/<stable-slug>/index.html
/sobytiya/<stable-slug>/related.json or embedded manifest
/assets/...
/sitemap.xml
/robots.txt
```

## 13. Acceptance gates for first generated pages

Product/UI:

- 375px: no horizontal scroll; primary CTA visible; touch targets >=44px;
- desktop 1366px: two-column layout, not stretched mobile feed;
- desktop phone/action regressions are additionally measured at `1366×768`,
  `1536×864` and `1920×1080`: every action rectangle must remain inside the
  graphite panel, both the initial label and revealed number must stay on one
  line, and the document width must not exceed the viewport;
- share works on phone via Web Share API or copy fallback;
- calendar ICS works;
- ticket/registration/source CTA matches event facts;
- `Похожие события` visible without JS;
- personalization disabled/unavailable does not break CTA.

SEO/GEO:

- HTML contains title, description, canonical, OG, JSON-LD;
- JSON-LD parses and matches visible date/place/status;
- sitemap contains generated pages;
- preview pages are noindex;
- production pages are indexable.

Personalization:

- no direct browser table writes;
- no raw impression firehose;
- served-list summary compact;
- no visible related-card jump after late rerank;
- current event excluded;
- other dates separated;
- hidden/explicit negative events removed.

Ops:

- build log stores event ids/slugs/content hashes;
- upload target is `kenigevents.ru`, not legacy media bucket;
- rollback is possible by re-uploading previous static tree or disabling new links;
- Telegraph event-detail URL remains a dual-run fallback for 10 days after the
  actual production T0, then new event Telegraph pages stop being created while
  legacy URLs remain available; canonical gates and aggregate-surface boundary:
  [`release-plan.md`](release-plan.md#десятидневный-telegraph-coexistence).

## 14. Implementation order

Do not start with full personalization backend. Start with the static product surface.

1. Static event page vertical slice:
   - export real event payloads from Fly SQLite;
   - generate Astro/static HTML for 5–10 events;
   - render hero, CTA, description, other dates, static related fallback;
   - publish preview to bucket prefix;
   - run visual/SEO checks.

2. Related manifest and client island:
   - wire `personalization.js` to real page DOM;
   - keep local rerank optional and consent-gated;
   - no remote writes yet or `PERSONALIZATION_WRITE_PATH=none`.

3. Write-path spike:
   - `same_origin_endpoint_v1` first;
   - Supabase RPC only after grants/RLS/quota/storage tests.

4. Canary:
   - switch a small set of Telegram/VK links from Telegraph to `kenigevents.ru`;
   - keep Telegraph dual-run;
   - monitor CTA/share/calendar/related clicks and fallback errors.
