# Listing personal feed

> **Status:** client contract prepared; backend/RPC not enabled yet.

## Product rule

Static listing pages (`/segodnya/`, `/zavtra/`, `/vyhodnye/`) end with a dynamic “Личная лента” section. The section is **not pre-rendered with personal cards** in static HTML. It stays hidden until the browser can use a cached personal list or fetch one from a backend.

This keeps SEO-safe deterministic listing content first, then adds a personal continuation only for real users.

## Runtime behavior

1. Page renders static listing sections only.
2. JS looks for a cached `ke_listing_personal_feed_cache_v1` manifest in `localStorage`.
3. Cache is shared across listing pages and has a short TTL of 30 minutes.
4. If cache is valid for the current compatible profile hash, the personal section renders immediately without another network call.
5. If there is no cache and public Supabase RPC config is present, the page makes one RPC request for up to 30 card projections.
6. While browsing across listing pages, the same localStorage list is reused; “Обновить ленту” can force a refresh when backend config exists.
7. If backend/RPC is absent or fails, the section remains hidden; static listing UX is not degraded.

## Why localStorage cache

The goal is to avoid repeated Supabase reads on every static-page navigation. The feed is a **starter list** for the browsing session, not an exact real-time ranking. Local strong actions (`like`, `not_interested`, `share`) still update the local profile and can filter/reorder cached cards client-side.

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
