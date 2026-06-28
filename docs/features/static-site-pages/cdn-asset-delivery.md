# CDN asset delivery for KenigEvents static pages

> **Status:** CDN host `https://static.kenigevents.ru` is live and verified on 2026-06-28. It fronts the **static-site bucket** (`kenigevents.ru`) and now also serves mirrored event media under `/p/...` plus stable calendar files under `/ics/<event_id>.ics`.

## Goal

Use the CDN for stable static-site assets while keeping canonical pages, APIs/RPC, telemetry and calendar flows on `https://kenigevents.ru`.

CDN is currently safe for:

- Astro-generated code assets under versioned preview/prod prefixes: `/<build_id>/_astro/*`;
- static public assets uploaded with the site prefix, e.g. `/<build_id>/favicon.svg`;
- mirrored event media under content-addressed `/p/...` keys;
- stable calendar files under `/ics/<event_id>.ics`;
- direct QA access to full preview pages at `https://static.kenigevents.ru/<build_id>/...` as an infrastructure smoke check only.

CDN is **not** currently used for:

- canonical event/listing HTML URLs — canonical remains `https://kenigevents.ru/...`;
- personalization RPC/API/telemetry;
- dynamic/personal JSON;
- long-cache `.ics` files (calendar files stay short-cache).

## Environment variables

Two CDN knobs are deliberately separate:

```bash
# Canonical page host. Keep this on the main domain.
PUBLIC_SITE_ORIGIN=https://kenigevents.ru

# Astro CSS/JS/static build assets. For preview, include the versioned build prefix.
# build-preview.mjs replaces {buildId} with the actual PREVIEW_BUILD_ID.
PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}

# Event-media CDN and stable calendar base.
PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru
PUBLIC_ICS_BASE_URL=https://static.kenigevents.ru/ics
```

`PUBLIC_ASTRO_ASSET_BASE_URL` controls Astro `build.assetsPrefix` only. `PUBLIC_ASSET_BASE_URL` controls `eventImageUrl()` / `assetUrl()` for event hero/card/listing/gallery/OG/JSON-LD images. `PUBLIC_ICS_BASE_URL` controls stable calendar CTA links; if omitted and `PUBLIC_ASSET_BASE_URL` is set, pages use `${PUBLIC_ASSET_BASE_URL}/ics/<event_id>.ics`.

## Current verification facts

- `static.kenigevents.ru` resolves to Yandex Cloud CDN (`*.yccdn.ru`).
- `https://static.kenigevents.ru/preview-20260628-event-pages-v47-sparse-fixes/__preview/` and `https://kenigevents.ru/preview-20260628-event-pages-v47-sparse-fixes/__preview/` returned `200` in the current focus preview.
- Migration tool: `scripts/migrate_static_media_to_cdn_bucket.py` copies active legacy media objects from `s3://kenigevents/p/...` to `s3://kenigevents.ru/p/...` without mutating SQLite rows. Astro rewrites legacy raw URLs to CDN URLs at build time when `PUBLIC_ASSET_BASE_URL` is set.
- Deploy script uploads generated `event.ics` both under the versioned preview path and to stable CDN keys `s3://kenigevents.ru/ics/<event_id>.ics`. v47 uploaded 70 stable ICS files and `https://static.kenigevents.ru/ics/5077.ics` returned `200 text/calendar`.


## 2026-06-28 v47 media migration evidence

For the current focus preview (`preview-20260628-event-pages-v47-sparse-fixes`):

- `scripts/migrate_static_media_to_cdn_bucket.py --db artifacts/codex/static-site-builder/prod-db-20260628.sqlite --active-on 2026-06-28 --apply` selected 957 active legacy `kenigevents/p/...` media objects; all 957 were already present in `kenigevents.ru`, copied 0, failed 0;
- sample CDN media under `https://static.kenigevents.ru/p/...webp` returned `200 image/webp`;
- static preview deploy rewrote event media to `https://static.kenigevents.ru/p/...` and uploaded stable calendar files to `/ics/<event_id>.ics`;
- this proves the preview bucket/CDN path, not the full production Smart Update promotion loop. Automatic Smart Update → Kaggle artifact → CDN/Object Storage promotion still remains a separate release gate.

## Preview testing strategy while UI/code still changes

Use a new build id for every focus-group iteration:

```bash
PREVIEW_BUILD_ID=preview-YYYYMMDD-event-pages-vNN \
PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' \
PUBLIC_ASSET_BASE_URL='https://static.kenigevents.ru' \
PUBLIC_ICS_BASE_URL='https://static.kenigevents.ru/ics' \
npm --prefix site run build:preview

PREVIEW_BUILD_ID=preview-YYYYMMDD-event-pages-vNN \
PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' \
PUBLIC_ASSET_BASE_URL='https://static.kenigevents.ru' \
PUBLIC_ICS_BASE_URL='https://static.kenigevents.ru/ics' \
npm --prefix site run check:preview

PREVIEW_BUILD_ID=preview-YYYYMMDD-event-pages-vNN \
npm --prefix site run deploy:preview
```

Give users the main-domain URL (`https://kenigevents.ru/<build_id>/...`) so canonical/SEO behavior is realistic. The HTML will load `_astro/*` from `https://static.kenigevents.ru/<build_id>/_astro/...`. Direct `static.kenigevents.ru/<build_id>/...` is only a CDN smoke URL.

Rationale:

- code can still change: new build id + content-hashed `_astro` filenames avoids stale CDN code without purges;
- images are stable and mirrored into the CDN bucket; every CDN-enabled build must fail if raw `storage.yandexcloud.net/kenigevents/...` leaks into event HTML/JSON-LD;
- preview HTML can keep short cache headers; immutable `_astro` can be cached for a year.

## Cache policy

The deploy script uploads preview files with default short cache and then re-uploads Astro code assets with immutable cache headers:

| Path | Cache-Control | Notes |
| --- | --- | --- |
| `/<build_id>/*.html`, JSON, sitemap/robots | `public, max-age=300` | safe for focus-group preview; new build id for changes |
| `/<build_id>/_astro/*` | `public, max-age=31536000, immutable` | content-hashed and build-prefixed |
| `/<build_id>/event.ics` | `public, max-age=300` + `text/calendar` metadata | build-scoped fallback |
| `/ics/<event_id>.ics` | `public, max-age=300` + `text/calendar` metadata | stable calendar CTA target |
| `/p/**` | object metadata, intended immutable for content-addressed keys | mirrored from legacy media bucket; safe for `PUBLIC_ASSET_BASE_URL` |

## Acceptance gates

`npm run check:preview` verifies both branches:

- when `PUBLIC_ASTRO_ASSET_BASE_URL` is set, Astro CSS/JS links use that host and canonical URLs still stay on `kenigevents.ru`;
- when `PUBLIC_ASSET_BASE_URL` is set, event images/OG/JSON-LD images must use that media host;
- when only `PUBLIC_ASTRO_ASSET_BASE_URL` is set, event media must not be rewritten to `static.kenigevents.ru/p/...`.

## Future media-CDN gate

For every new release/canary with `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru`, verify:

1. `https://<media-cdn>/p/<hash>.webp` returns `200` for production media;
2. CORS is public for image/share/canvas use where needed;
3. `og:image` and JSON-LD `Event.image[]` use stable CDN URLs;
4. cache headers are immutable for hashed media;
5. no private/user/session data can be served from the CDN.

## Current/new media write policy

- Legacy source bucket: `kenigevents`.
- CDN/static-site bucket: `kenigevents.ru`.
- New server-side media uploads prefer `YC_STORAGE_BUCKET`; if it is unset they fall back to `KENIGEVENTS_SITE_YC_BUCKET`, then `kenigevents.ru`.
- New persisted Yandex media URLs use `YC_STORAGE_PUBLIC_BASE_URL` / `PUBLIC_ASSET_BASE_URL`; for bucket `kenigevents.ru` the default public URL is `https://static.kenigevents.ru/<path>`.
- Cleanup remains dual-compatible: stored URLs/paths from both `kenigevents` and `kenigevents.ru` are queued in `supabase_delete_queue`, and `flush_supabase_delete_queue()` deletes Yandex objects from both buckets before falling back to Supabase removal for Supabase buckets.
