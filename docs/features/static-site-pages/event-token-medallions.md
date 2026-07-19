# Event token medallions / quick-read badges

> **Status:** curated organizer/venue/festival/source/Pushkin assets are rendered on event detail pages and projected into Telegram RichMessages. `DATE-LISTING TH-P1 · V15` also has an approved compact recognition rail: at most three quiet identity/fact medallions may sit beside media; a single overlay remains restricted to reviewed no-OCR photo media. SVG is the static-site primary where source-faithful; raster-only items are WebP-first with PNG fallback/QA.
> **Surface:** **страница конкретного события** (`/sobytiya/<slug>/`) and the explicitly bounded V15 date-listing compact rail/overlay. Search/related cards remain unaffected without separate approval.
> **Related docs:** [Event Page Product & Design Spec](event-page-product-design.md), [Listing personal feed](listing-personal-feed.md), [Anonymous Personalization](../unsigned-personalization/README.md).

## Goal

Add a visually large row of **quick-read event medallions** on the **event detail page** so a visitor can scan high-value facts about this конкретное событие before reading the full description:

- who is behind the event;
- whether a known social/cultural program applies (`Пушкинская карта`);
- whether the event is charity-related, kid/family-friendly, recorded/streamed, accessible, free, etc.;
- which properties matter for personalization and later filters without making personalization a first-paint dependency.

The medallion row is informational and is not a replacement for the existing `Коротко` facts or CTA panel. Facts still need source-grounded text in the event body/quick facts; medallions are the fast visual layer. On listings they are deliberately quieter: recognition and trust cues that support the choice to open a card, not CTA or decoration.

## Scope clarification

Current scope:

- **P0:** medallions on the concrete event page only;
- **Approved compact exception:** on V15 Today/Tomorrow/Weekend/Popular, a vertical external rail may show up to three structured medallions. It is allowed for OCR/unknown media because it does not cover the image. Identity tokens precede the `Бесплатно` fact;
- **Overlay exception:** one named venue/festival medallion may be placed at the bottom-right edge only when the selected asset passes the reviewed no-OCR gate below;
- **P0:** listing/card date formatting: show short weekday and render event type without `#`;
- **Not P0:** medallions on generic search/related cards. If added later, they need separate compact-card acceptance.

### `DATE-LISTING TH-P1 · V15` compact rail and venue overlay

This is not a general permission to decorate posters. It is a fail-closed
location cue for the dense exact-time desktop flow:

- external placement: `52…60px`, at most three items in a vertical rail. No universal CSS border or shadow; KGD80/KONB and other identity marks keep their own authored ring. Default listing treatment is `opacity≈.82`, `saturate≈.68`, recovering toward normal on card hover/focus;
- overlay placement: one `60px` circle at the image's bottom-right edge, with at most a neutral 2px contrast edge and no shadow; the title and venue text remain below the image;
- semantics: the matched token must carry `listingStatus=listing_ready` and a
  structured `listingBinding`. Runtime priority is `venue → festival →
  organizer`, exact/bounded aliases only, at most one token;
- required media evidence: the **selected** asset is
  `image_text_mode=visual_only`. The structured binding establishes token
  identity, not crop safety; reviewed crop evidence remains asset-specific;
- forbidden shortcuts: `safe_crop`, OCR length alone, filename, event title or
  an inferred organizer are not sufficient evidence;
- OCR/unknown text mode forbids only the **overlay**. A structured `listing_ready` token may still appear in the external rail. Missing structured match or a manifest status other than `listing_ready` renders no identity medallion;
- an organizer asset with no structured organizer field is marked
  `blocked_missing_binding`, not guessed from event prose/title.

No listing flag treats `safe_crop`, filename or venue name as proof that an
image has no OCR: `visual_only` plus reviewed event-photo/focal evidence remains
mandatory for overlay/crop. The external rail does not need crop evidence
because it never covers the image.

## Visual contract

### Shape

Use two shapes only:

| Shape | Use | Rule |
| --- | --- | --- |
| Circle medallion | organizer avatar, Pushkin-card emblem, icon-only event-page medallion | `border-radius: 999px`, official/local asset centered, accent ring |
| Pill medallion | facts that need text (`Детям`, `Благотворительность`, `Будет запись`) | circle icon + short label, no emoji as structural icons |

Do not create a horizontally scrolling chip carousel. On detail pages the row wraps, but the P0 layout should fit the important 4–6 tokens in the first row on desktop and within roughly one mobile screen of the hero/title area.

### Placement

Detail page order:

```text
Hero / visual decision block
H1 + short meta / summary
Token medallion row
Primary CTA / Коротко facts / description
```

Generic search/related cards are not a medallion surface in P0. Date/Popular listings use only the bounded V15 exception above. Other cards get the separate metadata formatting fix:

```text
Пн, 14 октября · Концерт     # weekday + event type without hashtag
Event title
Admission / actions
```

The medallion row stays in normal document flow on the event detail page, not as a floating overlay over poster text. Overlays are allowed only after a visual QA pass proves they do not cover OCR/poster text.

### Sizes

Runtime event-page medallions are intentionally larger than ordinary chips: they are a visual trust/recognition layer under the hero, not small metadata labels. Organizer medallions should prefer SVG `avatarUrl` assets. If the source is not SVG/vector-safe, use WebP as the primary browser asset and keep PNG only as QA/fallback.

| Surface | Desktop | Tablet | Mobile |
| --- | --- | --- | --- |
| Detail circle/avatar | `clamp(88px, 23vw, 112px)` | same responsive token | about `90px` on a 390px mobile viewport |
| Pushkin-card medallion | visual black circle uses the same `--token-size` as organizer circles; the wordmark may protrude to the right inside one composite image | same responsive token | same circle diameter as organizer medallions; composite width is larger only because of the original wordmark |
| Detail pill height | 56px | 48px | 44px minimum hit/scan area |
| Gap | 12px | 10px | 8px |
| V15 listing external circle | `52…60px`, max 3 | desktop acceptance only | deferred |
| V15 listing overlay circle | `60px`, max 1 | desktop acceptance only | deferred |

Special crop notes:

- `Остров Канта` uses the official cathedral-mark asset enlarged inside the circle (`scale(1.32)`) so the mark is readable at mobile size.
- `Дом китобоя` uses a simplified stacked mark: the source-logo words `дом` and `Китобоя` are placed one above another and fitted into the circle; in v2 `дом` is made larger and shifted left while keeping roughly the same right edge, so the upper word consumes more of the round avatar without drifting right. The blue/ring palette still comes from the public social avatar.

Clickable tokens, if introduced later (for organizer pages or filters), need a real 44×44px hit area and visible focus. P0 tokens are informational spans/list items.

### Priority and overflow

Render tokens in this order:

1. `organizer` — visual trust/recognition;
2. `pushkin_card` — strong decision factor for youth/culture audience;
3. `charity` — social meaning, must be source-grounded;
4. `kids_or_family` — fast audience filter;
5. `video_recording` / `online_stream` — whether recording/stream will exist;
6. `accessible` — accessible environment, only with evidence;
7. `free_or_registration` — only if it adds more than the CTA/fact label;
8. `tourist_friendly`, `outdoor`, `language`, `age_rating` — P1/P2 only.

Overflow:

| Surface | Max visible | Overflow |
| --- | --- | --- |
| Detail page | 6 primary tokens | second line is allowed; avoid more than 2 lines |
| V15 listing | 3 total | no overflow UI; identity first, `Бесплатно` last |

## Token catalog

### P0 tokens

| Token | Data source | Visual | Copy |
| --- | --- | --- | --- |
| Organizer avatar | normalized organizer/venue mapping | official avatar/logo in circle | `Организатор: …` in aria/tooltip; no visible long label on cards |
| Пушкинская карта | existing `pushkin_card=true` plus source evidence | special Pushkin-card medallion | `Пушкинская карта` |
| Благотворительность | LLM-first classification with evidence | heart/hand SVG pill | `Благотворительность` |
| Детям / семейное | age/audience fields + LLM-first classification | child/star/kite SVG pill | `Детям` or `Семейное` |
| Видеозапись | source-grounded video/stream/recording status | play/video SVG pill/circle | `Будет запись`, `Есть видео`, `Онлайн` |

### Starter organizer avatars

The starter organizer avatars are saved as local medallion-ready assets. Runtime code should prefer SVG when available; otherwise prefer the WebP path and keep the PNG as fallback/QA.

| Organization | Slug | Runtime asset | Source/provenance |
| --- | --- | --- | --- |
| Музей Мирового океана | `world-ocean-museum` | `/assets/organizers/world-ocean-museum.svg` (`.png` fallback/QA) | official mobile SVG logo from `world-ocean.ru`, simplified to the large geometric `ММО` mark without raster runtime |
| Историко-художественный музей | `history-art-museum` | `/assets/organizers/history-art-museum.svg` (`.png` fallback/QA) | official white KOIHM PNG from `koihm.ru`; SVG source was not found on the checked public URLs, so the geometric building/`КОИХМ` mark was locally vectorized into SVG on the accepted contrast circle |
| Калининградская филармония | `kaliningrad-philharmonic` | `/assets/organizers/kaliningrad-philharmonic.svg` (`.png` fallback/QA) | official black SVG logo from `filarmonia39.ru`; yellow background `#FAB534` matched to the current Telegram profile avatar at `t.me/filarmonia_39` |
| Остров Канта | `kant-island` | `/assets/organizers/kant-island.svg` (`.png` fallback/QA) | official `sobor39.ru` SVG logo; the exact cathedral-mark path is embedded directly into the medallion SVG |
| Дом китобоя | `dom-kitoboya` | `/assets/organizers/dom-kitoboya-stacked.webp` (`.png` fallback) | source logo snapshot from `domkitoboya.ru` split into two words and recomposed as v2 enlarged/left-shifted `дом` over `Китобоя`; no official/source SVG was found in the checked public candidates, so this medallion intentionally remains WebP-first raster for now |
| Филиал Третьяковской галереи | `tretyakovka-kaliningrad` | `/assets/organizers/tretyakovka-kaliningrad.svg` (`.png` fallback/QA) | public Telegram avatar from `t.me/tretyakovka_kaliningrad`; the simple gold `Т` mark is reconstructed as SVG primitives on a warm light background |
| Калининградский зоопарк | `kldzoo` | `/assets/organizers/kldzoo.webp` (`.png` fallback) | official square PNG mark from `https://kldzoo.ru/local/templates/s1/img/logo.png`; locally converted without redesign |
| Калининградская областная научная библиотека | `konb` | `/assets/organizers/konb.webp` (`.png` fallback) | local reference `docs/reference/лого КОНБ (1)(1).png`; explicit raster exception for the 2026-07-02 SVG pass |
| Театр «Акт Опус» | `act-opus` | `/assets/organizers/act-opus.svg` (`.png` fallback/QA) | official `actop.us/plays` Next image PNG wordmark; medallion stacks `АКТ` over `ОПУС`, replacing the octopus symbol, with `АКТ` inset inside the circle |
| Калининградский драматический театр | `dramteatr39` | `/assets/organizers/dramteatr39.svg` | official `dramteatr39.ru/img/logo.svg?v=2`; the accepted round runtime mark uses the left theatre emblem from the official horizontal SVG and matches venue/source aliases including `Драматический театр` |
| Российское общество «Знание» | `znanie-russia` | `/assets/organizers/znanie-russia.svg` (`.png` fallback/QA) | current official site primary blue `#0501D0` from `znanierussia.ru`; local kgd80 vector supplies the enlarged white internal `З` symbol as a root-clipped group, optically centered and clipped by the lower circle edge |
| Фестиваль «80 историй о главном» | `kgd80` | `/assets/organizers/kgd80.svg` (`.png` fallback/QA) | KGD80 hero lockup from `site/src/assets/partners/source/kgd80.logo-80-istorii-hero.svg`; tighter medallion viewBox with safe margins and a small downward optical nudge; forced for `event.festival=80 историй о главном` |
| Фестиваль «Кантата» | `kantata-festival` | `/assets/organizers/kantata-festival.webp` (`.png` fallback) | official Tilda PNG wordmark `КАНТАТА`; WebP-first because source is raster |
| Поселение викингов «Кауп» | `kaup` | `/assets/festivals/kaup.svg` | official SVG mark from `kaup39.ru`; matched as a curated `venue_brand` by normalized venue/source aliases, including `Поселение викингов Кауп` |

Asset inventory:

- runtime optimized assets: `site/public/assets/organizers/`; primary organizer assets are SVG except explicit raster exceptions;
- source originals + provenance README: `site/src/assets/organizers/source/`;
- browser-facing organizer/venue manifest: `site/src/data/organizerMedallions.json`;
- the current minimal KGD80 festival manifest and runtime tree: `site/src/data/festivalMedallions.json` and `site/public/assets/festivals/`;
- asset/provenance inventory is canonical in `site/src/assets/organizers/source/README.md` and `site/src/assets/festivals/source/README.md` rather than duplicated here.

2026-07-02 SVG pass:

- `kant-island`, `kaliningrad-philharmonic` and `world-ocean-museum` use local/official SVG source material directly;
- `history-art-museum` had no public SVG source in the checked KOIHM candidates, so the existing geometric PNG medallion was locally vectorized into SVG;
- `tretyakovka-kaliningrad` is reconstructed as simple SVG primitives from the geometric Telegram avatar;
- `dom-kitoboya` is intentionally not SVG because no source SVG was found (`logo.svg` candidates returned 404); it is WebP-first with PNG fallback;
- `konb` is intentionally unchanged as the explicit raster exception for this pass;
- `act-opus` is updated to a self-contained SVG medallion that stacks the official `АКТ` / `ОПУС` wordmark instead of using the octopus symbol, with the `АКТ` crop safely inset inside the circle;
- `znanie-russia` is updated to the official blue `#0501D0` full circle with the internal `З` kept white, scaled larger, browser-rendered via a root-clipped group, optically centered and clipped by the lower circle edge;
- `kgd80` is added as an SVG festival medallion from the «80 историй о главном» hero logo, using a tighter lockup viewBox plus a small downward optical nudge for fuller circular occupancy;
- `kantata-festival` remains a WebP-first raster medallion because the available official source is PNG;
- `kaup` restores the accepted official SVG venue-brand medallion for events whose normalized venue/source explicitly names Кауп; it is not inferred from unrelated description prose;
- `znanie-russia` is detected when the event explicitly names «Знание» as organizer/partner/supporter or links to `znanierussia.ru`, and is also forced by curated policy for `event.festival=80 историй о главном`.

No OpenAI image generation/editing was used for these assets; they were produced by local SVG rendering/vectorization, source-faithful cropping and alpha-preserving WebP/PNG fallback export.

### Manifest-inventory regression contract

The runtime manifest and the optimized asset must move together. The July 2026
`dramteatr39` regression happened because the accepted SVG existed on an older
medallion branch but its manifest item was absent from the later integration
base; alias matching itself was not broken. Full-catalog preview acceptance must
therefore render event `5756` and assert both the local
`/assets/organizers/dramteatr39.svg` URL and the theatre label. A visual asset
present only in branch history is not considered integrated.

For unknown organizers use a neutral initials medallion (`МК`, `Ф`, etc.) only after the normalized organizer name is known. Do not guess logos.

### Pushkin-card asset

Source image requested for the first asset: <https://bgtk.org/upload/information_system_15/2/6/3/item_2637/item_2637.jpg>.

Asset pipeline:

1. Download and store source provenance in the asset README.
2. Remove background locally (`rembg`, OpenCV/threshold + manual QA, GIMP/Inkscape); **do not use OpenAI image generation/editing** unless the user gives explicit consent in the current thread.
3. Export optimized source cutouts under `site/public/assets/badges/`; the runtime asset is a **single composite** `pushkin-card-medallion.webp` with `pushkin-card-medallion.png` as fallback/QA, assembled from the high-quality bust cutout and the original source wordmark. The intermediate bust asset may remain for provenance/QA but must not be composed in CSS at runtime.
4. Render the composite so the black circle is the **same visual diameter** as organizer medallion circles; do not enlarge the Pushkin circle relative to other circles. The original `Пушкинская карта` wordmark starts over the lower part of the circle and may protrude to the right. Do **not** add a separate pill/label with duplicated text.
5. Provide a fallback `ПК` purple-ring medallion if the asset fails visual QA.

## Telegram graphical medallion projection

The same curated organizer/festival/source/program assets are projected into
ordinary `@kldevents` event publications by the canonical Telegram publisher;
the transport contract lives in
[`docs/features/tg-publishing/README.md`](../tg-publishing/README.md).

- Telegram uses a deterministic local Pillow render on an opaque brand-graphite
  `1300×330` strip, with visual marks up to `260px`.
- The strip is a standalone bottom image block inside Telegram RichMessage; it
  is not a caption mosaic and does not use custom emoji.
- `organizerMedallions.json`, optional `festivalMedallions.json`, local source
  marks and the Pushkin composite remain the asset/source-of-truth inventory.
- KGD80 festival policy resolves two distinct partner marks (`80 историй` and
  `Знание`), so a KОНБ KGD80 event resolves all three identities.
- The old `TG_MEDALLION_CUSTOM_EMOJI_*` grid remains legacy-only data. Normal
  event publication and repair paths must not enqueue or pass a medallion block
  to the Premium/custom-emoji editor.

No OpenAI image generation/editing is used by the Telegram projection.


## Static export data contract

### Listing placement V18

Date/Popular listing cards use a stricter compact projection than event detail:

- at most three visible identities including `Бесплатно`;
- one medallion may move to a `right:10px; bottom:10px` overlay only on the
  selected source-reviewed `visual_only` safe event photo or a real no-image
  fallback; OCR/unknown media never inherits crop/overlay permission from a
  different raw candidate;
- regular `221px` media packs three `51px` identities and up to two non-zero
  Share/Like proof rows into one `64px` rail; short Weekend `178px` media uses a
  `56px + 36px` split rail (`96px` total);
- medallions have no universal ring/shadow and render with quiet listing
  saturation/opacity, strengthening only on hover/focus;
- proof never evicts venue/festival/Free identity, and a zero proof metric has
  no DOM node or reserved width.

Controls: Tretyakovka photo `6950` and Zoo fallback `6957` prove the lower-right
overlay; event `6811` proves all three identities plus two proof rows in both
regular and Weekend densities. Canonical product contract:
[`listing-surfaces-v18-product.md`](listing-surfaces-v18-product.md).

Future static export should project a compact token model next to existing event fields:

```json
{
  "event_id": 5878,
  "event_type": "концерт",
  "display_date": "2026-07-11",
  "display_weekday": "Сб",
  "organizer_badge": {
    "slug": "kaliningrad-philharmonic",
    "name": "Калининградская филармония",
    "avatar_url": "/assets/organizers/kaliningrad-philharmonic.webp",
    "confidence": 0.95,
    "evidence": "venue_alias"
  },
  "event_badges": [
    {"kind": "organizer", "label": "Филармония", "priority": 10},
    {"kind": "pushkin_card", "label": "Пушкинская карта", "priority": 20},
    {"kind": "kids_or_family", "label": "Детям", "priority": 40},
    {"kind": "video_recording", "label": "Будет запись", "status": "planned", "priority": 50}
  ]
}
```

Notes:

- `event_type` is stored and rendered as plain text (`Концерт`, not `#концерт`) in both event-page metadata and list cards.
- Weekday can be generated at build time, but exporting `display_weekday` keeps static manifests consistent across card renderers.
- `event_badges[]` is a display projection for the event detail page; source-of-truth fields remain on the canonical event/feature snapshot.
- Supabase personalization may duplicate only this compact display projection when needed for browser-facing recommendation cards; it must not become source of truth for event facts.

## LLM-first detection contract

Because these fields affect event meaning and user trust, use the repo's LLM-first policy. Deterministic checks may support or validate narrow signals, but broad semantic classification must stay in LLM prompts / Smart Update enrichment, not ad-hoc regex.

### Single-venue public ceiling

The static projection renders at most one `venue_brand` medallion. Exact
structured evidence (canonical venue, trusted ticket/source identity) outranks
description prose and short aliases. If two venue brands have equal-strength
structured evidence, or several same-host ticket identities conflict, the
renderer fails closed and shows no venue medallion rather than guessing. Unicode
word boundaries are mandatory for short aliases: `ММО` must not match inside
`программой`. Real regressions: `6796` resolves to KAUP only; contradictory
legacy event `5295` resolves to neither venue until canonical data is repaired.

### Desktop edge and shadow contract

The accepted desktop title panel must not clip the circular medallion ring or
its soft shadow at the bounds of the token row. The title-panel wrapper and its
token row therefore expose visual overflow while retaining the existing token
width, order, accessible label and single-venue fail-closed resolution. This is
a presentation-only rule: it must not widen alias matching or add a second
`venue_brand` identity.

### Required output shape for enrichment

```json
{
  "badges": {
    "charity": {"value": true, "confidence": 0.88, "evidence": "часть средств направят в фонд…"},
    "kids_or_family": {"value": true, "confidence": 0.91, "evidence": "6+, семейный спектакль"},
    "video_recording": {"value": "planned", "confidence": 0.82, "evidence": "будет доступна запись трансляции"},
    "pushkin_card": {"value": true, "confidence": 0.98, "evidence": "оплата по Пушкинской карте"},
    "organizer_slug": {"value": "world-ocean-museum", "confidence": 0.94, "evidence": "source account / venue alias"}
  },
  "review_required": false,
  "quality_warnings": []
}
```

### Detection notes

| Field | Accept when | Reject / review when |
| --- | --- | --- |
| `charity` | source says благотворительный, сбор средств, пожертвование, proceeds/support to a named fund/person/cause | generic `бесплатно`, `в поддержку культуры`, venue sponsorship without donation; confidence `<0.80` goes to review |
| `kids_or_family` | explicit age 0+/6+, `для детей`, family event, children's workshop/performance | 12+/16+/18+, adult venue context, fairy-tale title for adult theatre without audience evidence |
| `video_recording` | source promises recording/stream or already links video | event merely has a promo video/poster video; distinguish `available`, `planned`, `livestream_only` |
| `pushkin_card` | existing DB `pushkin_card=true`, source text, ticket system/program evidence | organizer generally participates but this event has excluded ticket category; mark review |
| `organizer_slug` | exact source account, venue alias, official organizer line, manually curated mapping | title mentions an institution as subject but not organizer/venue |

Confidence policy:

- `>=0.80`: auto-accepted when evidence span is present;
- `0.50–0.79`: render only in admin/review tooling, not public token;
- `<0.50`: reject;
- every public `charity`, `kids_or_family`, `video_recording` token must be traceable to source evidence or curated venue/organizer policy.

## Accessibility and SEO/GEO

- Render the row as `<ul class="event-token-row" role="list">` with `<li>` tokens, or equivalent list semantics.
- Every icon-only medallion has a full `aria-label` (`Можно оплатить Пушкинской картой`, `Организатор: Музей Мирового океана`).
- Decorative SVG/icon images use `aria-hidden="true"`; organizer avatars use empty `alt` when the `aria-label` carries the semantic name.
- Keep contrast at WCAG AA; do not rely on color alone (icon + label/aria).
- JSON-LD may include source-grounded `organizer`, `isAccessibleForFree`, `audience`, and `eventAttendanceMode`, but must not hallucinate charitable purpose or video availability.
- Token text is visible HTML, not hidden SEO stuffing.

## Separate card-list formatting contract

Implemented first slice in this branch. This is separate from event-page medallions:

- `site/src/lib/events.ts` exposes weekday-aware formatters for cards.
- `EventCard.astro` renders date labels with a short Russian weekday and renders `event_type` as plain text instead of `#…`.
- `EventListItem.astro` uses weekday-aware dates and includes the plain event type in the listing metadata row.

Acceptance examples:

| Before | After |
| --- | --- |
| `14 октября · 19:00` | `Ср, 14 октября · 19:00` |
| `#концерт` | `концерт` / source-cased event type |

## Roadmap

### P0

1. Keep the weekday/type card formatting from this branch.
2. Add `EventTokenRow.astro` / `EventBadgeRow.astro` with empty-state no-op rendering.
3. Add local asset folders and starter organizer avatar manifest. **Done for the starter organizers above, including social-palette Dom Kitoboya.**
4. Build Pushkin-card medallion asset through the local pipeline and visual QA.
5. Render detail-page token row from existing safe facts (`organizer`, `pushkin_card`, `is_free`, `event_type`) plus curated manual token overrides only.
6. Add preview checks for: no `#` event type in cards, weekday present, token row does not horizontal-scroll, `aria-label`s present.

### P1

1. Add LLM-first badge enrichment for `charity`, `kids_or_family`, `video_recording` with evidence spans and review thresholds.
2. Extend static export/preview fixtures with `event_badges[]`.
3. Feed token tags into personalization feature snapshots as controlled, versioned features only.
4. Add Schema.org enrichment where data is source-grounded.
5. Decide separately whether compact medallions should ever appear inside cards; default is no.

### P2

- Organizer pages/filter facets from token clicks.
- Optional compact medallions in related/search/listing cards, only after separate design approval.
- `accessible`, `online`, `outdoor`, `tourist_friendly`, language and age-rating tokens.
- Tooltip/popover explanations on desktop.
- Personalization controls such as “меньше детских событий” must use existing `negative_interest_tags`, not token click guesses.

## External design review trace

The requested external review was run on 2026-06-29:

- `a-opus` / Opus produced a fuller badge design contract with sizes, priority, `BadgeRow.astro` sketch, LLM thresholds and merge checklist.
- `gemini --model gemini-3.1-pro-preview` independently recommended the same large circle/pill model, local Pushkin-card asset processing, weekday/date formatting and no-hashtag event type rendering.

Synthesis and accepted/deferred decisions are recorded in [event-token-medallions-consultant-notes-2026-06-29.md](event-token-medallions-consultant-notes-2026-06-29.md).

### Source-channel medallions

`MEOW Афиша` uses the public Telegram avatar from <https://t.me/meowafisha>, stored as `/assets/sources/meow-afisha.webp` (`.png` fallback). It is rendered only when the event source URLs include `t.me/meowafisha` and the event has no more than two known source URLs, so the badge means source/provenance rather than a generic topic.

This source-channel badge is a **static event-detail-page surface only**. Source
and aggregator identities are not event attributes and must never be included
in Telegram `@kldevents` graphical-medallion strips. Telegram may still show
source-grounded organizer, venue, festival, program and Pushkin-card marks.
