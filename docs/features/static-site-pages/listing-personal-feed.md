# Listing personal feed

> **Status:** client contract and same-origin static-catalog MVP implemented; backend/RPC remains an optional fallback, not the default read path.

Canonical cross-surface relation kinds, family dedupe and naming are defined in
[`../linked-events/README.md`](../linked-events/README.md). This file owns the
listing runtime/cache projection only.

## Product rule

Static date listing pages (`/segodnya/`, `/zavtra/`, `/vyhodnye/`) and
`/vystavki/` may end with a dynamic personal continuation. The section is
**not pre-rendered with personal cards** in static HTML. It stays hidden until
the browser can use a compatible cached personal list or fetch one from the
same-origin catalog/backend. Event detail owns its separate finite continuation
and must not also append the generic listing slot.

`/populyarnoe/` remains excluded from this generic slot. Its primary job is a
bounded behavioral overview. Desktop V28 adds one compact continuation,
`Вам может быть интересно`: it uses the same cross-site local profile only with
explicit consent and at least three strong signals, then reveals exactly four
affinity candidates plus one anti-bubble candidate. Page-wide family
exclusions apply; if the honest 4+1 set cannot be formed, the whole shelf stays
absent. It has no independent Popular profile, backend read or generic personal
response. See
[`listing-surfaces-v28-desktop-popular.md`](listing-surfaces-v28-desktop-popular.md).

This keeps SEO-safe deterministic listing content first, then adds a personal continuation only for real users.

### Review-only `/dlya-menya/` surface

The finite cold-start route uses the same
`OptimizedEventCardGrid`/`EventCard` pair as desktop event-detail
recommendations. Desktop rows are packed in threes by the global crop/height
optimizer; non-final rows are full and every row has equal media/card heights.
Below the desktop breakpoint the component uses one canonical large card per
row rather than reusing desktop row geometry. A bespoke unoptimized 3/2/1 CSS
grid is forbidden because it reintroduces a second crop implementation.

## Runtime behavior

1. Page renders static listing sections only.
2. JS looks for a base-scoped `ke_listing_personal_feed_cache_v1:<base-path>` manifest in `localStorage`.
3. Cache is shared across listing/event pages only inside the same production or immutable-preview base and has a short TTL of 30 minutes. A manifest whose stored `base_path` differs from the current page is rejected.
4. If cache is valid for the current compatible profile hash, the personal section renders immediately without another network call.
5. If there is no cache, the browser first loads the bounded same-origin `/data/personal-feed.json` catalog and ranks it locally. A public Supabase RPC, when explicitly configured, is only a fallback if that static request fails.
6. While browsing across listing pages, the same localStorage list is reused; “Обновить ленту” can force a refresh when backend config exists.
7. If both sources fail, the section remains hidden; static listing UX is not degraded.

Every dynamic event-card boundary rebases same-site `/sobytiya/<slug>/...`
links to the base of the page that is currently open. This applies to cached
personal feeds, discovery load-more manifests and Authorized Search results,
including their absolute/share and local calendar projections. It prevents a
manifest retained from preview v7 (or a search snapshot produced for the root)
from navigating a v10 reviewer back to an older event UI. Absolute URLs on a
different origin remain untouched because they may be real organizer/ticket
destinations rather than KenigEvents event navigation.

### Event-detail desktop continuation

On event-detail pages the same engine is intentionally delayed until the
visitor reaches `Смотрите дальше`; see
[Personal feed architecture](../unsigned-personalization/personal-feed-architecture.md).
It then renders one bounded continuation rather than an infinite feed:

1. the hard limit is exactly six cards; there is no `Показать ещё` action;
2. with at least three compatible local strong signals, cards use the personal
   ranker and the honest heading `По вашим интересам`;
3. before the profile is mature, the same-origin catalog is ranked by a
   deterministic `0.68 × popularity + 0.32 × upcoming-date proximity` fallback,
   labelled `Ещё события` rather than personalized;
4. both modes exclude the current event, linked/hidden events and cards already
   offered in `Смотрите дальше`, then enforce at most three cards per category
   and two per venue;
5. the section ends with `Все анонсы`; no genre/search chip is shown until a
   useful unauthenticated destination exists;
6. on mobile the immature/fallback continuation stays hidden because the
   established discovery feed already provides continuation. Mature personal
   results may still appear without duplicating the fallback.

Three product alternatives were compared with Gemini 3.1 Pro: additional
cards, routing chips and a hybrid. The accepted preproduction rule above passed
the external product gate because it gives most desktop visitors a finite next
step without falsely claiming personalization or routing them into the current
authenticated-search dead end. Future measurement must distinguish
`personal`/`popular_fallback`, rank, impressions, clicks and terminal
`Все анонсы` use before changing the six-card limit.

## Why localStorage cache

The goal is to avoid repeated Supabase reads on every static-page navigation without allowing a build-specific URL to outlive its preview. The feed is a **starter list** for the browsing session, not an exact real-time ranking. Local strong actions (`like`, `not_interested`, `share`) still update the local profile and can filter/reorder cached cards client-side.

## Supabase RPC option

Direct browser → Supabase RPC is acceptable only under this contract:

- browser uses only `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`;
- no table is exposed directly for arbitrary selects;
- RPC returns a compact card projection, not raw canonical events or telemetry;
- request sends a compact profile snapshot (`positive_tags`, `negative_interest_tags`, liked/hidden/seen ids capped), not raw event logs;
- response returns at most 20–30 cards;
- RLS/Data API grants are explicit;
- if a `SECURITY DEFINER` function is ever needed, revoke `PUBLIC` execute and grant only intended roles; prefer `SECURITY INVOKER`/RLS-readable projection when possible.

Recommended function shape:

```sql
select * from get_listing_personal_feed_v1(
  p_surface => 'listing_personal_feed',
  p_limit => 30,
  p_profile => '{... compact profile ...}'::jsonb
);
```

Recommended response shape follows the existing card manifest style:

```json
{
  "schema_version": "listing-personal-feed-v1",
  "feature_schema_version": "event-detail-related-v1",
  "taxonomy_version": "event-taxonomy-v1",
  "surface": "listing_personal_feed",
  "algorithm_id": "supabase_rpc_personal_feed_v1",
  "related_static": [
    {
      "event_id": 5878,
      "title": "Песни СССР",
      "category": "концерт",
      "tags": ["концерт"],
      "base_similarity": 0.93,
      "reason_codes": ["profile:positive_affinity"],
      "display": {
        "href": "/sobytiya/pesni-sssr-svetlogorsk-5878/",
        "absolute_url": "https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/",
        "title": "Песни СССР",
        "image_url": "https://static.kenigevents.ru/p/...webp",
        "image_text_mode": "ocr_text",
        "display_date_time": "11 июля · 21:30",
        "place": "Светлогорск · Янтарь холл",
        "price_label": "Билеты",
        "likes_count": 11,
        "shares_count": 0,
        "calendar_href": "/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics",
        "calendar_eligible": true
      }
    }
  ]
}
```

## Data duplication policy

Supabase may duplicate only the card projection needed for the feed:

- ids, slug/href, title;
- display date/time, city, venue/place;
- image URL + `image_text_mode` + alt;
- ticket/admission label, lifecycle/status;
- aggregate like/share counts;
- tags/features needed for ranking/filtering.

Do not duplicate full descriptions, raw source text, Telegraph HTML, media blobs, raw telemetry or bot operational state.

## CDN note

When CDN is enabled, card projection should already contain CDN-ready image URLs or raw Object Storage URLs resolvable by the frontend `eventImageUrl()` layer. The frontend must not fetch personalized JSON through CDN.

## External references checked

- Supabase Data REST API / browser-accessible PostgREST layer: https://supabase.com/docs/guides/api
- Supabase Database Functions / remotely callable database logic: https://supabase.com/docs/guides/database/functions
- Supabase JavaScript `rpc()` reference: https://supabase.com/docs/reference/javascript/rpc
- Supabase RLS guide: https://supabase.com/docs/guides/database/postgres/row-level-security
- Supabase API keys guide: https://supabase.com/docs/guides/getting-started/api-keys

## Shared desktop row geometry, R11

The crop/height optimizer is not specific to event detail. Every desktop
personal continuation on date, Today, Tomorrow, Weekend and event-detail
surfaces packs complete three-card rows before rendering:

- non-final rows are always full and the only remainder is last;
- every card and media viewport in one row has the same visible height;
- reviewed `visual_only` media uses cover with no fields;
- OCR/protected documents retain authored content and the documented maximum
  crop; unknown media remains fail-closed;
- the optimizer evaluates allowed row combinations and minimizes total document
  height instead of accepting independent intrinsic card heights.

Below the desktop breakpoint the packed coordinates are discarded. The shared
mobile resolver owns the card: reviewed photos use the accepted horizontal
`5:4` cover and protected documents retain natural geometry. A generic personal
slot stays hidden beneath the dedicated mobile rail so two discovery feeds
cannot hydrate on the same page.
