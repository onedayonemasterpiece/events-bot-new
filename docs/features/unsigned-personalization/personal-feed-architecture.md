# Personal feed architecture

Status: design accepted / implementation pending beyond current local preview.

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
