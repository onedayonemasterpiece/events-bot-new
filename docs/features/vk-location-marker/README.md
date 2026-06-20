# VK location marker

Canonical requirements: [requirements.md](requirements.md).

## Implemented v1 behavior

- Managed VK event posts (`vk_sync` / `sync_vk_source_post`) resolve an optional location marker from structured `event.city` before creating a new `wall.post`.
- The resolver is conservative and fail-open: if city is missing, outside Kaliningrad Oblast, ambiguous, not present in the internal marker directory, or resolution errors, VK publication continues without marker.
- Region safety reuses `geo_region.py` allowlist/cache primitives and only applies markers for Kaliningrad Oblast decisions.
- `wall.post` receives only VK-supported marker payload keys: `lat`, `long`, and/or `place_id`; `city_id` is never sent.
- If VK rejects marker params as invalid, `post_to_vk` retries the same post without location params.
- Marker decisions are logged as `vk.location_marker decision=...` with statuses such as `applied`, `skipped_no_city`, `skipped_not_region`, `skipped_low_confidence`, and `lookup_error`.

## Cache

`db.init()` creates `vk_location_marker_cache`:

- `query_norm`, `query_display`, `display_title`, `city`
- `is_kaliningrad_oblast`
- `lat`, `long`, `place_id`
- `confidence`, `provenance`, `status`, `details`
- `created_at`, `updated_at`

Positive marker rows are reused by normalized city key. Negative/ambiguous rows are reused for a bounded TTL controlled by `VK_LOCATION_MARKER_NEGATIVE_TTL_SECONDS` (default: 7 days).

## v1 limits / tech debt

- v1 uses a conservative static Kaliningrad Oblast city/settlement marker directory plus existing geo cache; it does not perform live VK/third-party place search at publish time.
- Ambiguous settlement names are skipped unless `location_name`/`location_address` contains supporting Kaliningrad Oblast context.
