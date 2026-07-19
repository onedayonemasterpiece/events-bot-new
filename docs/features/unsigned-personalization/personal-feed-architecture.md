# Personal feed architecture

Status: static-catalog MVP implemented in the noindex full-catalog preview; backend top-up remains optional/future.

## Goal

Move from static related lists toward a real personal feed without putting LLMs, Supabase recommendation reads or heavy ranking in the page-view hot path.

## Static-first contract

All public pages render useful crawlable HTML without JS. Personalization is progressive enhancement:

```text
static HTML with 10 seeded events
  -> JS activates
  -> compatible local profile filters/reranks current cards
  -> one same-origin manifest/top-up fetch when needed
  -> user presses “Показать ещё” for later chunks
```

## Event-detail discovery sequence

The event page intentionally separates two product questions:

1. **`Смотрите дальше`** — contextually similar events, deterministic and present
   in static HTML for every visitor;
2. **`Для вас / По вашим интересам`** — a separate progressive continuation,
   ranked from the compatible anonymous profile on the current device.

The second block is not a renamed continuation of similarity. It remains hidden
until the profile has at least three bounded strong signals (unique likes,
`Не интересно`/hidden events, plus at most three share signals) and the visitor
reaches the similar-events boundary. An `IntersectionObserver` starts one
same-origin `/data/personal-feed.json` fetch near that boundary; first paint and
visitors who never scroll there pay no feed-request cost.

The static similar row normally contributes ten candidates. Personalized
continuation renders in chunks of six, at most eighteen cards, so a formed
profile can expose roughly 20–28 distinct candidates in the decision journey.
The product target is that a visitor sees at least one relevant event within
20–30 cards; it is a measurement target, not a promise that similarity alone
can satisfy.

Client ranking is profile-first and bounded: affinity, popularity, price/time
fit, static quality and a small exploration term, with negative-interest and
fatigue penalties. Before rendering it removes the current event, every event
already offered in either desktop or mobile similar blocks, linked/hidden
candidates and repeats; diversity permits at most three cards from one category
and two from one venue. The public catalog contains only compact card fields and
no visitor/profile data.

Dynamic cards reserve their media aspect ratio before an image request finishes.
They show a calm shimmer while loading, retain the same frame on failure and
fall back to a generated textual visual instead of collapsing to `0px` and
causing layout shift. `prefers-reduced-motion` disables shimmer animation.

## Success and guardrails

Primary evaluation:

- probability of a meaningful action or relevant-card click within 20/30 cards;
- `Для вас` view rate and CTR compared with the immediately preceding similar row;
- calendar, like, share and primary-CTA conversion after a personalized impression.

Guardrails:

- no duplicate with current/similar cards;
- no first-paint request and no section for an unformed profile;
- no movement of cards already seen or acted on;
- no material primary-CTA cannibalization;
- explain that ranking uses actions stored on this device, without pretending a
server identity/profile was used.

### Sidecar schema compatibility

The personalization pgvector sidecar is a projection, but its document table
must still accept every canonical field emitted by the current exporter. The
2026-07-17 full-catalog sync surfaced PostgREST `PGRST204` because
`event_search_documents` did not yet contain `age_restriction` and
`age_restriction_status`. Migration
`supabase/migrations/20260717074903_event_search_age_fields.sql` adds the two
bounded fields, constraints and an explicit PostgREST schema-cache reload. A
sync must stop on this class of schema drift; silently dropping a new canonical
field would make search and recommendations disagree with event pages.

For date/listing pages, the static page remains the SEO surface. The personal continuation/feed is not a canonical SEO URL and is not put in sitemap.

## Feed sources

Initial MVP sources:

- static date/listing/event-detail candidates;
- `/data/discovery/<event_id>.json` for event details;
- future compact card snapshot: `/data/cards/current.compact.json`;
- future golden-facet manifests: `/data/personalization/facets/v1/...`.

Supabase is not the default browser read path. Supabase stores compact strong-action telemetry and offline aggregates for later batch generation/evaluation.

## Browser behavior

1. Render static page.
2. Read `ke_personalization_profile` from localStorage.
3. Validate schema/taxonomy/profile versions.
4. Apply explicit hard filters (`not_interested`, linked duplicates).
5. Apply local scoring from session/short/mid/long horizons and golden facets.
6. Keep exploration/diversity guardrails.
7. Show `Все / Для меня` only when there is a meaningful difference, and show how many events were hidden.
8. Never make a user action reorder/disappear the acted-on card or cards above it in the current viewport.

## Strong actions

These actions may update local profile and future compact telemetry:

- `like_event` / `unlike_event`;
- `not_interested` / `undo_not_interested`;
- `share_event` after a successful Web Share/copy fallback;
- calendar add click;
- ticket/registration/phone CTA click.

Every persisted strong action must be joinable with `served_list_id`, `served_list_hash`, `surface`, `position`, `algorithm_id` and `build_id`.

## Browser notifications

Browser notifications are a future opt-in channel, not part of the current static page baseline. Planned use cases:

- tomorrow selection is ready;
- Friday lunch/weekend selection;
- explicitly saved/liked event reminder;
- promo campaign message only if it fits the user's opted-in category/facet policy.

Notification permission must be requested only after a clear value explanation and user gesture. Denial must not degrade the site.

## Share-image debt

The production share action is Web Share with image file + plain text + URL, falling back to URL/text copy. Rich hidden links inside share text are not a browser-guaranteed capability.

Technical debt for later: define a server/batch share-image generator. Preferred base ratios are vertical `4:5` and square `1:1`; both should reserve a visible bottom stripe for service identity, date/place/CTA, inspired by the successful transfer-post visual pattern. This is a generator requirement, not a current runtime promise.
