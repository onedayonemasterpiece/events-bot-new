# CDN asset delivery for KenigEvents static pages

> **Status:** implementation-prepared, CDN not enabled. The production CDN host will be switched on only after the owner confirms that Yandex Cloud CDN is configured.

## Goal

Serve heavy immutable assets through `https://static.kenigevents.ru` while keeping canonical HTML pages, future APIs/RPC, telemetry and `.ics` files on `https://kenigevents.ru`.

CDN is intended for:

- event hero images and fullscreen gallery images;
- related/listing card thumbnails;
- Astro-generated `/_astro/*` CSS/JS;
- versioned icons/fonts/logo assets;
- SEO image URLs (`og:image`, JSON-LD `Event.image[]`, future image sitemap).

CDN is **not** intended for:

- `/sobytiya/*`, `/segodnya/`, `/zavtra/`, `/vyhodnye/` HTML;
- personalization RPC/API/telemetry;
- dynamic `/data/discovery/*.json` and future personal JSON;
- long-cache `.ics` files;
- query-string versioning.

## Current code preparation

Implemented but disabled by default:

- `site/src/lib/assets.ts` exposes `eventImageUrl()` / `assetUrl()`.
- `PUBLIC_ASSET_BASE_URL` is optional. When empty, preview keeps current image URLs unchanged.
- When `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru`, Yandex Object Storage URLs like `https://storage.yandexcloud.net/kenigevents/p/...webp` are emitted as `https://static.kenigevents.ru/p/...webp`.
- Unknown third-party absolute URLs are preserved as-is; only known Object Storage URLs and relative asset keys are rewritten to the CDN host.
- `astro.config.mjs` sets `build.assetsPrefix` from `PUBLIC_ASSET_BASE_URL` for Astro `/_astro/*` assets.
- Event hero, event cards, listing thumbnails, gallery `data-gallery-src`, `og:image` and JSON-LD `Event.image[]` use the resolver.

## Environment

Production target after CDN setup:

```bash
PUBLIC_SITE_ORIGIN=https://kenigevents.ru
PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru
```

Preview/local default until CDN is explicitly enabled:

```bash
PUBLIC_ASSET_BASE_URL=
```

## URL rules

- Canonical page URLs always stay on `https://kenigevents.ru`.
- CDN URLs are allowed only for static assets.
- Production CDN-enabled HTML must not emit `https://storage.yandexcloud.net/kenigevents/...` for event images.
- Use content-hashed paths for immutable media; do not use query strings as cache-busting.

## Cache policy

| Path | Cache-Control | Purge policy |
| --- | --- | --- |
| `/p/**/<hash>.webp` | `public,max-age=31536000,immutable` | no purge; new hash |
| `/_astro/*` | `public,max-age=31536000,immutable` | no purge; new hash |
| `/fonts/*.<hash>.woff2` | `public,max-age=31536000,immutable` | no purge |
| `/icons/*.<hash>.svg` | `public,max-age=31536000,immutable` | no purge |
| `/favicon.svg` | `public,max-age=3600` unless hashed | partial purge only |
| `/manifest.webmanifest` | `public,max-age=3600` | partial purge only |

## CORS / cookies

CDN assets are public and must not carry user/session state.

Expected response headers:

```text
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, HEAD, OPTIONS
Timing-Allow-Origin: https://kenigevents.ru
X-Content-Type-Options: nosniff
```

`Set-Cookie` from `static.kenigevents.ru` is forbidden.

## SEO/GEO

- `og:image`, `WebPage.primaryImageOfPage` and JSON-LD `Event.image[]` use CDN URLs when CDN is enabled.
- `Event.image[]` includes only current event images, not the final gallery CTA/related-card image.
- Image sitemap may use CDN image URLs.
- `static.kenigevents.ru` must not be blocked by robots.
- Add `static.kenigevents.ru` to Google Search Console and Яндекс.Вебмастер after DNS/CDN activation.

## Deploy outline after CDN is ready

1. Build Astro with `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru`.
2. Upload `dist/_astro/**` to Object Storage `/_astro/**` with immutable cache headers.
3. Ensure event images already exist under `/p/**` with immutable cache headers.
4. Upload mutable favicon/manifest with short TTL.
5. Deploy HTML to the `kenigevents.ru` origin/prefix.
6. Purge only mutable paths if needed; never full-purge on normal deploy.
7. Run CDN smoke checks: image/CSS `curl -I`, no `Set-Cookie`, correct content type, correct cache headers.

## Acceptance gates

`npm run check:preview` includes a CDN-enabled branch: when `PUBLIC_ASSET_BASE_URL` is set, rendered event HTML must use the asset host for event images and JSON-LD images, while canonical URLs remain on `kenigevents.ru`.

## External references checked

- Yandex Cloud CDN CORS settings: https://yandex.cloud/en/docs/cdn/operations/resources/configure-cors
- Yandex Object Storage Cache-Control metadata: https://yandex.cloud/en/docs/troubleshooting/storage/how-to/configure-cache-headings-with-http-queries
- Yandex CDN path purge CLI: https://yandex.cloud/en/docs/cdn/cli-ref/cache/purge
