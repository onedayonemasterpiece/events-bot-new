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
- Preview deploy uploads generated `event.ics` only below the versioned preview prefix and explicitly refuses to mutate stable `s3://kenigevents.ru/ics/*` keys. Stable calendar promotion is a separate production operation. Historical v47 evidence below predates this safety boundary: that run uploaded 70 stable ICS files and `https://static.kenigevents.ru/ics/5077.ics` returned `200 text/calendar`.


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

python scripts/run_static_site_builder_kaggle.py \
  --db <immutable-production-projection.sqlite> \
  --repo-sha <exact-40-character-SHA> \
  --profile preview --catalog-mode slice \
  --build-id preview-YYYYMMDD-event-pages-vNN \
  --asset-base-url https://static.kenigevents.ru \
  --astro-asset-base-url 'https://static.kenigevents.ru/{buildId}' \
  --download-output --publish-preview
```

Give users the main-domain URL (`https://kenigevents.ru/<build_id>/...`) so canonical/SEO behavior is realistic. The HTML will load `_astro/*` from `https://static.kenigevents.ru/<build_id>/_astro/...`. Direct `static.kenigevents.ru/<build_id>/...` is only a CDN smoke URL.

Rationale:

- code can still change: new build id + content-hashed `_astro` filenames avoids stale CDN code without purges;
- images are stable and mirrored into the CDN bucket; every CDN-enabled build must fail if raw `storage.yandexcloud.net/kenigevents/...` leaks into event HTML/JSON-LD;
- preview HTML can keep short cache headers; immutable `_astro` can be cached for a year.

## Cache policy

The trusted host publisher uploads the checked Kaggle preview artifact with short default cache headers and immutable headers for content-hashed Astro assets:

| Path | Cache-Control | Notes |
| --- | --- | --- |
| `/<build_id>/*.html`, JSON, sitemap/robots | `public, max-age=300` | safe for focus-group preview; new build id for changes |
| `/<build_id>/_astro/*` | `public, max-age=31536000, immutable` | content-hashed and build-prefixed |
| `/<build_id>/assets/*` | `public, max-age=31536000, immutable` | build-prefixed local visual assets, including listing-media derivatives |
| `/<build_id>/event.ics` | `public, max-age=300` + `text/calendar` metadata | build-scoped fallback |
| `/ics/<event_id>.ics` | `public, max-age=300` + `text/calendar` metadata | stable calendar CTA target; never written by the preview publisher |
| `/p/**` | object metadata, intended immutable for content-addressed keys | mirrored from legacy media bucket; safe for `PUBLIC_ASSET_BASE_URL` |
| `/p/thumb/v1/**` | `public, max-age=31536000, immutable` | content-addressed 256/512 WebP derivatives used by rails/cards through `srcset` |

### V19 listing-media profile (2026-07-18)

Public V18 Weekend profiling at `1536×864` measured 67 event images and showed
that the browser cache was already effective: a warm reload and subsequent
scroll transferred **0 image body bytes** (memory cache first, disk cache later).
The perceived disappearance was instead late discovery: only six images had a
real `src` initially because application JS copied URLs from `data-listing-src`
after a narrow `200px` observer intersection. A visible second card received its
URL around 916ms and became ready around 1.15s. V19 therefore emits every
`src/srcset` in parser-visible HTML and uses native `loading=lazy`; the first four
Weekend cards in global chronology are eager/high priority. It also changes the
Weekend `sizes` contract to 320/340px because 15 of 67 V18 cards selected a 256w
file for an actual 257–317px frame at DPR 1.

Build-scoped `assets/**` are re-uploaded immutable in V19; HTML and ICS retain
the short-cache policy. Cross-origin RUM still needs a separate
`Timing-Allow-Origin` CDN configuration before client Resource Timing can be
used as authoritative transfer evidence.

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

Thumbnail rails must not request every full-resolution gallery original. Media
materialization emits independent 256/512 derivatives and the HTML provides
real `srcset`/`sizes`, so the browser chooses one cached object for the rendered
slot. Do not replace this with a single sprite/contact sheet unless a measured
experiment demonstrates lower transferred **and decoded** bytes on the real
gallery distribution; the current production decision is independent immutable
objects because the rail may show a variable subset and opens images by index.

### 2026-07-15 sprite decision: keep independent responsive objects

The decision above is now measured rather than assumed. Three real desktop
gallery families were re-encoded both as independent cells and as one exact-size
WebP contact sheet at the same quality (`q=82`, method `6`). Decoded RGBA volume
was identical for equal cell dimensions; transferred-byte differences stayed
within about two percent:

| Gallery | Cells / rendered slot | Independent 1x | Sprite 1x | Independent 2x | Sprite 2x |
| --- | ---: | ---: | ---: | ---: | ---: |
| Garage `5658` | 7 / `80×52` | 8,462 B | 7,862 B | 23,264 B | 22,604 B |
| Split `5761` | 12 / `112×112` | 45,196 B | 44,636 B | 122,692 B | 122,930 B |
| Mixed portrait `4671` | 12 / `128×153` | 70,930 B | 69,662 B | 163,518 B | 166,790 B |

The sprite therefore saves requests, not meaningful payload. It also forces one
resolution for every cell, downloads hidden/unneeded cells, invalidates the
whole sheet when one image changes and complicates exact-index semantics. Under
HTTP/2/3 the request-count advantage is smaller, while independent URLs retain
granular immutable caching and native `srcset` selection. This matches
[MDN's sprite guidance](https://developer.mozilla.org/en-US/docs/Web/CSS/Guides/Images/Implementing_image_sprites)
and the [web.dev responsive image contract](https://web.dev/learn/images/responsive-images).

Request overhead is not zero, but it is not one operating-system thread per
image: HTTP/2/3 multiplexes many request streams over a small number of
connections. A sprite can still win under high latency when **every** cell is
needed at one resolution. That is not this first-paint rail. Counting only the
visible subset makes independent objects materially smaller even before cache
reuse: Garage 1x is `6,634 B` across five requests versus a `7,862 B` complete
sprite; Split 1x is `22,184 B` across six requests versus `44,636 B`. At 2x the
same comparisons are `18,508 B` versus `22,604 B` and `61,066 B` versus
`122,930 B`. The remaining cells are fetched only if navigation makes them
visible.

The accepted optimization is therefore:

1. keep one immutable CDN WebP per media index with 256/512 `srcset`;
2. emit the measured slot width in `sizes` (not a conservative maximum);
3. assign `src/srcset` only to the subset actually visible in the rail;
4. keep hidden cells as a transparent placeholder until layout exposes them;
5. add a 128 derivative only after a production trace shows at least
   `100–150 KiB` initial-load savings on common DPR/viewport combinations.

At the Full-HD/125% CSS viewport (`1536×864`, DPR 1), the Split cells are
`88–112px`; the previous hard-coded `sizes="196px"` incorrectly selected 512
objects. Exact sizes select 256, and only six visible cells are requested instead
of all eleven image cells. Garage requests five visible thumbnails instead of
seven; the OCR companion requests its one visible preview rather than every
candidate.

Antigravity **Gemini 3.1 Pro (High)** independently reviewed the same measured
table and selected **INDEPENDENT**. It recommended fixing `sizes` and the visible
subset before adding formats or a third derivative. The reproducible report,
exact WebPs, prompt and review remain under ignored
`artifacts/codex/static-desktop-v14-20260715/`.

## Current/new media write policy

- Legacy source bucket: `kenigevents`.
- CDN/static-site bucket: `kenigevents.ru`.
- New server-side media uploads prefer `YC_STORAGE_BUCKET`; if it is unset they fall back to `KENIGEVENTS_SITE_YC_BUCKET`, then `kenigevents.ru`.
- New persisted Yandex media URLs use `YC_STORAGE_PUBLIC_BASE_URL` / `PUBLIC_ASSET_BASE_URL`; for bucket `kenigevents.ru` the default public URL is `https://static.kenigevents.ru/<path>`.
- Provider write exceptions must retain a bounded error type, HTTP status and
  provider code in runtime logs. Returning only `None` without this evidence is
  forbidden because capacity errors such as `BucketMaxSizeExceeded` otherwise
  look like ordinary source-media misses.
- The bucket's immutable `_review/` trees use durable-current-pointer-aware retention before a
  new secret-candidate upload. The durable current candidate is protected by
  token hash; the two newest non-current candidates and every tree younger than
  48 hours are retained. Missing current-prefix evidence fails closed, and no
  bearer token is logged.
- Cleanup remains dual-compatible: stored URLs/paths from both `kenigevents` and `kenigevents.ru` are queued in `supabase_delete_queue`, and `flush_supabase_delete_queue()` deletes Yandex objects from both buckets before falling back to Supabase removal for Supabase buckets.
