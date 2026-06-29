# Event token medallions / quick-read badges

> **Status:** design + first listing-card formatting slice prepared on branch `docs/site-personalization-tokens-20260629`.
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

| Surface | Desktop | Tablet | Mobile |
| --- | --- | --- | --- |
| Detail circle | 56px | 48px | 44px |
| Detail pill height | 56px | 48px | 44px |
| Gap | 12px | 10px | 8px |

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

Start with four locally stored, optimized assets:

| Organization | Suggested slug | Asset rule |
| --- | --- | --- |
| Музей Мирового океана | `world-ocean-museum` | official logo/avatar, cropped into a 1:1 transparent/safe square |
| Историко-художественный музей | `history-art-museum` | official logo/avatar; do not redraw from memory |
| Калининградская филармония | `kaliningrad-philharmonic` | official logo/avatar |
| Остров Канта | `kant-island` | official mark if available; otherwise a deterministic SVG cathedral/island mark designed in-repo, not AI-generated |

For unknown organizers use a neutral initials medallion (`МК`, `Ф`, etc.) only after the normalized organizer name is known. Do not guess logos.

### Pushkin-card asset

Source image requested for the first asset: <https://bgtk.org/upload/information_system_15/2/6/3/item_2637/item_2637.jpg>.

Asset pipeline:

1. Download and store source provenance in the asset README.
2. Remove background locally (`rembg`, OpenCV/threshold + manual QA, GIMP/Inkscape); **do not use OpenAI image generation/editing** unless the user gives explicit consent in the current thread.
3. Export an optimized transparent PNG/WebP under `site/public/assets/badges/`.
4. Render inside a circular medallion where the image fits vertically; text may protrude to the right with `overflow: visible`, but must not protrude upward beyond the medallion.
5. Provide a fallback `ПК` purple-ring medallion if the asset fails visual QA.

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
3. Add local asset folders and starter organizer avatar manifest.
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
