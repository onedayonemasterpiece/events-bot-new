# Event token medallions / quick-read badges

> **Status:** design + runtime medallion assets; organizer and Pushkin-card medallions are rendered on event detail pages in the static preview. As of 2026-07-02, SVG is the primary runtime format for organizer medallions where an SVG source exists or the mark is safely geometric-vectorized; raster-only sources must be WebP-first with PNG fallback/QA. `dom-kitoboya`, `konb` and `kantata-festival` are WebP-first raster items until source SVGs are found; `act-opus` is a self-contained SVG medallion assembled from the official raster wordmark.
> **Surface:** прежде всего **страница конкретного события** (`/sobytiya/<slug>/`). Listing/search cards are affected only by the separate date/type formatting requirement (weekday + event type without `#`).
> **Related docs:** [Event Page Product & Design Spec](event-page-product-design.md), [Listing personal feed](listing-personal-feed.md), [Anonymous Personalization](../unsigned-personalization/README.md).

## Goal

Add a visually large row of **quick-read event medallions** on the **event detail page** so a visitor can scan high-value facts about this конкретное событие before reading the full description:

- who is behind the event;
- whether a known social/cultural program applies (`Пушкинская карта`);
- whether the event is charity-related, kid/family-friendly, recorded/streamed, accessible, free, etc.;
- which properties matter for personalization and later filters without making personalization a first-paint dependency.

The medallion row is informational and belongs to the event detail page; it is not a replacement for the existing `Коротко` facts or CTA panel. Facts still need source-grounded text in the event body/quick facts; medallions are the fast visual layer. Event-list cards do **not** need this medallion row in the current scope.

## Scope clarification

Current scope:

- **P0:** medallions on the concrete event page only;
- **P0:** listing/card date formatting: show short weekday and render event type without `#`;
- **Not P0:** medallion rows inside listing/search/related cards. If added later, they must be re-approved as a separate compact-card design.

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

Listing/search/related cards are not a medallion surface in P0. They only get the separate metadata formatting fix:

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
| Калининградская областная научная библиотека | `konb` | `/assets/organizers/konb.webp` (`.png` fallback) | local reference `docs/reference/лого КОНБ (1)(1).png`; explicit raster exception for the 2026-07-02 SVG pass |
| Театр «Акт Опус» | `act-opus` | `/assets/organizers/act-opus.svg` (`.png` fallback/QA) | official `actop.us/plays` Next image PNG wordmark; medallion stacks `АКТ` over `ОПУС`, replacing the octopus symbol, with `АКТ` inset inside the circle |
| Российское общество «Знание» | `znanie-russia` | `/assets/organizers/znanie-russia.svg` (`.png` fallback/QA) | current official site primary blue `#0501D0` from `znanierussia.ru`; local kgd80 vector supplies the enlarged white internal `З` symbol as a root-clipped group, optically centered and clipped by the lower circle edge |
| Фестиваль «80 историй о главном» | `kgd80` | `/assets/organizers/kgd80.svg` (`.png` fallback/QA) | KGD80 hero lockup from `site/src/assets/partners/source/kgd80.logo-80-istorii-hero.svg`; tighter medallion viewBox with safe margins and a small downward optical nudge; forced for `event.festival=80 историй о главном` |
| Фестиваль «Кантата» | `kantata-festival` | `/assets/organizers/kantata-festival.webp` (`.png` fallback) | official Tilda PNG wordmark `КАНТАТА`; WebP-first because source is raster |

Asset inventory:

- runtime optimized assets: `site/public/assets/organizers/`; primary organizer assets are SVG except explicit raster exceptions;
- source originals + provenance README: `site/src/assets/organizers/source/`;
- browser-facing manifest for the future `EventTokenRow`: `site/src/data/organizerMedallions.json`.

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
- `znanie-russia` is detected when the event explicitly names «Знание» as organizer/partner/supporter or links to `znanierussia.ru`, and is also forced by curated policy for `event.festival=80 историй о главном`.

No OpenAI image generation/editing was used for these assets; they were produced by local SVG rendering/vectorization, source-faithful cropping and alpha-preserving WebP/PNG fallback export.

For unknown organizers use a neutral initials medallion (`МК`, `Ф`, etc.) only after the normalized organizer name is known. Do not guess logos.

### Pushkin-card asset

Source image requested for the first asset: <https://bgtk.org/upload/information_system_15/2/6/3/item_2637/item_2637.jpg>.

Asset pipeline:

1. Download and store source provenance in the asset README.
2. Remove background locally (`rembg`, OpenCV/threshold + manual QA, GIMP/Inkscape); **do not use OpenAI image generation/editing** unless the user gives explicit consent in the current thread.
3. Export optimized source cutouts under `site/public/assets/badges/`; the runtime asset is a **single composite** `pushkin-card-medallion.webp` with `pushkin-card-medallion.png` as fallback/QA, assembled from the high-quality bust cutout and the original source wordmark. The intermediate bust asset may remain for provenance/QA but must not be composed in CSS at runtime.
4. Render the composite so the black circle is the **same visual diameter** as organizer medallion circles; do not enlarge the Pushkin circle relative to other circles. The original `Пушкинская карта` wordmark starts over the lower part of the circle and may protrude to the right. Do **not** add a separate pill/label with duplicated text.
5. Provide a fallback `ПК` purple-ring medallion if the asset fails visual QA.

## Telegram custom-emoji medallions

The static-site medallion manifest is also used as the source semantics for Telegram custom-emoji medallions in `tg_event_publish` promo posts. Telegram rendering is deliberately configured separately from the static site because each ordinary visual medallion consumes a 4×4 grid of custom emoji documents.

Runtime contract:

- custom emoji pack capacity is 200 stickers; one ordinary 4×4 medallion consumes 16 stickers, while the KGD80+Znanie composite consumes 28 stickers as a 7×4 unit;
- production reads `TG_MEDALLION_CUSTOM_EMOJI_JSON` or `TG_MEDALLION_CUSTOM_EMOJI_PATH`; if no mapping is present, posts are rendered without medallions;
- `TG_MEDALLIONS_ENABLED=0` disables the feature without changing campaign configuration;
- no more than two visual medallion units are rendered for one Telegram event; item-level dimensions are allowed so a 7×4 composite still counts as one unit;
- in public channel event posts the bot sends a clean post without medallion placeholders first, because Bot API custom-emoji entities in channels require a bot with Fragment-purchased usernames; the delayed Premium Telethon editor then inserts the medallion mosaic as real custom emoji before the `Подробнее`/social footer;
- no visible fallback grid such as `🟧🟧🟧` is published during the editor delay; until the editor runs, the post simply has no medallion block;
- album/media-group captions use a compact 8-space visual gap between the `Подробнее` text link and the `Max`/VK social links; ordinary text/photo posts keep the wider 12-space footer gap;
- `Пушкинская карта` is mandatory when `event.pushkin_card=true`;
- events of «80 историй о главном» use the curated `kgd80-znanie` composite when present: `80 историй` is drawn on top and `Знание` is shifted behind it with a one-cell overlap, making the pair seven cells wide instead of two separate 4×4 medallions;
- standalone `znanie-russia` remains available for non-KGD80 events, while standalone KGD80 is intentionally not kept in the Telegram pack/config; `world-ocean-museum` is also omitted from the one Telegram pack to leave capacity for the wider 7×4 KGD80+Znanie composite;
- because `kgd80-znanie` occupies only one visual slot, a matching venue/location medallion can be rendered as the second slot for KGD80 events without reintroducing the broken three-wide mobile layout;
- ordinary Telegram venue/location medallions match aliases only against `location_name`, `location_address` and `city`; descriptions, search digests, film/program text and festival labels must not add venue medallions;
- Telegram alias matching is token/phrase-bounded, not raw substring matching: short acronyms such as `ММО` must appear as standalone tokens or an explicit full alias/venue/source signal, so ordinary words like `программой`, `Эммой` or `фильмом` do not attach the Музей Мирового океана medallion;
- disabled while on design/partner review: `rostec-arena`, `signal`, `locostandup`, `ruin-keepers`, `meow-afisha`, `kaliningrad-art-museum`.

The accepted Telegram mosaic calibration from July 2026 is 4×4 with a mobile source row step of `84.5px` and a `400×353.5` source canvas. Wider composites use the same row step and 100px columns (for example, `kgd80-znanie` is `700×353.5` before slicing into 7×4 cells). Earlier `79–80px` experiments render as vertically stretched ovals on current Telegram mobile clients.


## Static export data contract

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
