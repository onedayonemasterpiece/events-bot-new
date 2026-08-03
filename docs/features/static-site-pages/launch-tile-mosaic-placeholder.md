# Launch placeholder — «Кафельная мозаика»

> **Status:** isolated laboratory candidate contract. Implementation and release
> evidence are accepted only after the checks in this document pass; this page
> does not authorize a production-root change.
>
> **Route:** `/lab/launch/tile-mosaic/`
>
> **Launch date shown to visitors:** `2026-09-01` (1 сентября 2026 года).

## Scope and release boundary

The candidate is a launch placeholder for **«Полюбить Калининград Анонсы»**.
It explains a future service for cultural and educational events,
personalized recommendations and an event guide for Kaliningrad and the
Kaliningrad region. Useful copy, the launch date and the subscription form are
server-rendered static HTML; JavaScript enhances the mosaic and submits the
form but must not manufacture the page's meaning.

The implementation is deliberately isolated:

- page: `site/src/pages/lab/launch/tile-mosaic/index.astro`;
- visual component: `site/src/components/launch/TileMosaicLaunch.astro`;
- canonical source image:
  `docs/features/static-site-pages/auto-present/scenario-assets/PWA-icon.png`;
- byte-identical site-served copy: `site/public/assets/launch/PWA-icon.png`;
- subscription migration:
  `supabase/migrations/20260803143000_create_site_launch_subscriptions.sql`;
- laboratory URL only: `/lab/launch/tile-mosaic/`.

The source and served image must both have SHA-256
`7015488739e0296f6c5b04935a16769804aa8bf128436450e8a60eef32ec07dd`;
copying is permitted, visual regeneration is not.

It must not edit or replace `site/src/pages/index.astro`, alter another launch
prototype, change a production route, enter the sitemap, enable indexing, or
promote itself. A local/Astro preview or secret candidate is review evidence,
not production approval. Root rollout, indexing and branch-to-`main` merge all
need separate written approval.

## Visual architecture

The scene represents an image embedded in a rough, dark physical tile/metal
surface, not a photo beneath a decorative grid. Its stacking order is:

| Layer | Purpose |
| --- | --- |
| 00 | solid dark foundation |
| 01 | leather/rough material texture |
| 02 | vignette |
| 03 | one replaceable projected image |
| 04 | warm image grading |
| 05 | slowly moving metallic light gradient |
| 06 | physical HTML/CSS tile matrix |
| 07 | bevels, inset depth and grout |
| 08 | global film/material noise |
| 09 | semantic content and subscription form |
| 10 | restrained bloom around active tiles |

`mix-blend-mode` may blend the light layer, but its stacking context must be
kept local with `isolation`. `backdrop-filter` may blur/saturate/brighten the
projection behind a tile. The tile surface must remain partly transparent or
the backdrop effect is invisible. Readable text and controls do not use blend
modes and stay above decorative layers with stable contrast. The fallback when
either effect is unsupported is an intact dark material surface, not missing
content.

The large image-bearing squircle belongs to the scene: it extends beyond the
right edge on desktop, sits slightly above the vertical center, and receives
an inset shadow and soft orange halo. It must not read as a separate card.

## One projection, 72 real tiles

There is exactly one projection `<img>` below one stable collection of 72
decorative tile elements. The image is never baked into the grid and is never
split into 72 files. This keeps image perspective continuous and makes image,
material, noise, tile depth and light independently replaceable.

The same DOM is used at every viewport:

- desktop/tablet landscape: 12 columns × 6 rows;
- mobile: 6 columns × 12 rows;
- breakpoint changes CSS Grid only; it does not add, remove or reorder tiles;
- grid and tiles are decorative (`aria-hidden="true"`) and are not focusable.

### Tile-state contract

Each tile owns one state and the corresponding CSS custom-property bundle.
Opacity is not the state model by itself.

| State | Intended appearance | Approximate cover alpha |
| --- | --- | ---: |
| `sealed` | almost-black rough closed tile | `0.94` |
| `dim` | weak projected color | `0.78` |
| `sleeping` | neutral intermediate rest state | `0.66` |
| `revealed` | image is legible while material remains | `0.42` |
| `glint` | short, locally bright reflection | `0.16` |

Every state defines, directly or through state tokens:

- `--tile-cover-alpha`;
- `--tile-image-saturation`;
- `--tile-image-brightness`;
- `--tile-blur`;
- `--tile-glint-alpha`;
- `--tile-edge-alpha`;
- `--tile-depth`.

Animation uses a reproducible seed. Every 1.2–2.8 seconds it changes a sparse
group of 1–4 tiles with 2.8–6.5 second easing: one or two become lighter while
others darken. At most 25–35% are strongly revealed and at most 2–4 are
`glint`; the same tile is not selected again for 2–3 cycles. The independent
light source drifts slowly. A pointer may gently bias it and pointer exit
returns to the automatic path. There is no LED-like flash, rapid oscillation
or high-frequency randomized timer.

## Replaceable image API and URL safety

The projection accepts all three equivalent entry points:

1. Astro component prop `imageSrc`;
2. query parameter `?mosaicImage=<URL>` (optional numeric `focalX`/`focalY`
   query values use the same bounds as the event API);
3. browser event `tile-mosaic:set-image` with
   `{ src, focalX?, focalY? }` in `CustomEvent.detail`.

Example:

```js
window.dispatchEvent(
  new CustomEvent("tile-mosaic:set-image", {
    detail: {
      src: "https://example.org/new-image.jpg",
      focalX: 0.5,
      focalY: 0.42,
    },
  }),
);
```

The validator resolves a candidate with `new URL(candidate, location.origin)`
and accepts only:

- local site paths and same-origin URLs; or
- absolute cross-origin URLs whose protocol is exactly `https:`.

It rejects malformed URLs, credentials in URLs, protocol-relative surprises
after resolution, `http:` cross-origin URLs, `data:`, `blob:`, `file:`,
`javascript:` and every other scheme. It changes only the projection's
`HTMLImageElement.src` and bounded numeric focal coordinates. User-controlled
text is never concatenated into an inline `style`, CSS rule, HTML string or
script. Invalid prop/query/event input preserves the last safe image. A remote
host remains subject to the deployed CSP and browser image policy; URL syntax
validation is not permission to weaken those controls.

## Responsive composition

### Desktop

- canvas height: at least `100svh`;
- page inset: `clamp(28px, 4.1vw, 68px)`;
- content occupies roughly 40–43%; scene occupies 57–60%;
- compact logo is upper-left and launch status is upper-right over the scene;
- main copy starts in the lower half of the left column;
- H1 is about three lines and is constrained to roughly 590–630 px;
- email and button share one row; the button is the warm visual accent.

### Mobile

The semantic order is: (1) logo/status, (2) mosaic scene, (3) H1, (4) launch
date, (5) explanation, (6) email, (7) button, (8) privacy note. The scene takes
approximately the upper 42–47% of the first viewport; the heading begins near
the page midpoint and uses `clamp(48px, 11.2vw, 84px)`. The form is a single
column and both controls fill the available width. Tile count remains 72 while
tile size adapts. At 320 px, no copy or decoration may cause horizontal
overflow.

Required viewports are `320`, `360`, `390`, `430`, `768`, `1024`, `1366`,
`1440`, and `1920` px. The layout must also tolerate text zoom and viewport
heights smaller than the design reference; `100svh` is a minimum rather than a
content-clipping fixed height.

## Subscription data and browser behavior

### Database boundary

This feature belongs to the project's separate **personalization
Supabase/Postgres contour**. It does not move or write the Fly SQLite core.
Schema application uses the operator/backend-only
`PERSONALIZATION_DIRECT_CONNECTION_STRING`; it is never rendered into the
site.

`public.site_launch_subscriptions` is an isolated table with UUID `id`,
normalized unique `email`, `source`, `page_path`, `locale`, `status`,
`submission_count`, `created_at`, `updated_at`, and `last_seen_at`. Email is
trimmed/lowercased, at most 254 characters, and syntax-checked in the trusted
database function. `source` is a lowercased bounded token (64 characters),
`page_path` is a bounded absolute path (500 characters, no controls), `locale`
is a lowercased bounded locale token (16 characters), and `status` is
`subscribed` or `unsubscribed`. A repeat submission updates the existing row,
restores `subscribed`, refreshes attribution/timestamps and increments its
positive integer count instead of creating a duplicate.

RLS is enabled. Direct table operations are revoked from `anon` and
`authenticated`; the browser has no direct `INSERT` path. Its only write
surface is:

```text
public.subscribe_site_launch_v1(
  p_email,
  p_source = 'tile-mosaic-launch',
  p_page_path = '/lab/launch/tile-mosaic/',
  p_locale = 'ru'
)
```

The `SECURITY DEFINER` function uses an empty `search_path`, schema-qualified
relations and a security-bounded idempotent upsert. It returns only the
constant minimal result `{ "accepted": true, "status": "subscribed" }`; it
does not expose an ID, submission count, row-existence flag or other account
enumeration side channel. Default/`PUBLIC`, `authenticated`, and `service_role`
function execution are revoked; only `anon` receives the intentional execute
grant. Function grants and the table's RLS/grants are explicit in the
migration.

### Environment and form states

The browser may receive only the existing personalization-contour public env:

- `PUBLIC_PERSONALIZATION_SUPABASE_URL`;
- `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`;
- optionally `PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL` for the already
  approved stateless relay route.

There is no generic `PUBLIC_SUPABASE_*` alias and no anon/service-key fallback.
No `service_role`, direct connection string, secret key or server credential
may enter source, rendered HTML, JS bundles, screenshots or logs. With valid
public env, submit uses the shared `getResilientDataClient` singleton and the
cataloged `selected-once` operation to call the `subscribe_site_launch_v1` RPC
with named `p_email`, `p_source`, `p_page_path`, and `p_locale` arguments.
Feature code must not bypass the singleton with a raw direct fetch. The
operation is not automatically replayed because a successful call increments
`submission_count`: a timeout after dispatch is ambiguous, so the UI preserves
the email and asks the visitor to retry later. If URL or publishable key is
absent, the page remains fully readable and the form reports a clear
unavailable/configuration error without sending a request or clearing the
email.

The form includes a labelled email field, `type="email"`,
`autocomplete="email"`, `inputmode="email"`, a hidden honeypot, and a status
region using `aria-live`. It exposes `idle`, `submitting`, `success`, and
`error` states; prevents accidental double-submit; handles native and RPC
validation; treats the database's idempotent duplicate response as success;
and preserves the entered email after a network/configuration error. Honeypot
traffic is rejected without a database call and without revealing filtering
logic.

The honeypot, input limits and idempotency are abuse-reduction measures, not a
public-endpoint rate limit. Before any indexable/public-route rollout, an owner
must accept and verify a server/gateway rate-limit and abuse-monitoring policy.
That missing production control does not block a bounded secret lab review but
does block presenting the form as generally public-ready.

## SEO, GEO and indexing

The built HTML contains a Russian `lang`, unique `<title>`, meta description,
canonical link, Open Graph fields, Twitter Card fields, a single descriptive
H1, visible explanatory copy and a machine-readable launch `<time
datetime="2026-09-01">`. Copy identifies the service, Kaliningrad, the
Kaliningrad region, cultural and educational events, personalized
recommendations, the event guide, and the date 1 September 2026.

JSON-LD contains `WebSite`, `WebPage`, and `Service`. `Organization` is allowed
only when verified organization identity fields are available; placeholders
must not be invented. JSON-LD and visible content agree.

While this route remains under `/lab/`, both HTML metadata and hosting policy
must keep it out of search:

```html
<meta name="robots" content="noindex, nofollow, noarchive" />
```

It is excluded from sitemap/discovery. Canonical metadata does not override
this noindex boundary. Indexing can be considered only after approval of a
public route and a separate release review.

## Accessibility and motion

- Semantic reading order remains useful when all decorative CSS/JS is removed.
- Logo imagery has appropriate accessible naming; projection and tile visuals
  are decorative and do not duplicate text.
- Form labels, instructions, errors and success state do not rely on color.
- Email and submit controls are keyboard reachable, have visible focus, and
  keep logical focus through async state changes.
- Disabled/submitting behavior is announced without trapping focus.
- Text and controls retain readable contrast against the moving scene.
- At `prefers-reduced-motion: reduce`, random tile cycles, pointer response and
  automatic light travel are disabled. The scene uses a deliberately composed
  static state; essential form feedback remains immediate and non-animated.
- The static state is selected before visible animation begins, avoiding a
  flash of full-motion content.

## Acceptance and QA matrix

| Area | Required evidence |
| --- | --- |
| Build | `npm --prefix site run build` succeeds with and without public Supabase env; project checks pass |
| Route isolation | lab route exists; root, existing prototypes, sitemap and production outputs are unchanged |
| DOM contract | exactly one projection image and 72 tiles; 12×6 desktop and 6×12 mobile computed grids |
| Responsive | screenshots at all nine required widths; no horizontal overflow; text remains readable at 320 px |
| Visual | physical depth, restrained light and continuous projection; no LED flicker or card-like detached squircle |
| Image API | safe prop, same-origin query, HTTPS query and event work; malformed/unsafe schemes are rejected without CSS/HTML injection |
| Motion | normal sparse cycles respect caps; reduced-motion is static and ignores pointer movement |
| Keyboard/a11y | tab/shift-tab, focus ring, label, announcements, submit and error recovery are verified |
| Form | success, repeat/duplicate, invalid email, honeypot, rapid double-submit, selected-once ambiguous timeout/network error and missing env are exercised |
| Data/security | normalized unique upsert; RLS on; direct `anon`/`authenticated` table writes fail; only intended RPC execute grant works; response has no enumeration fields |
| SEO/GEO | static entities and launch date present; `WebSite`/`WebPage`/`Service` JSON-LD valid; no invented organization |
| Noindex | robots meta is exact; route absent from sitemap; secret candidate stays non-indexable |
| Browser fallback | no-backdrop-filter/blending fallback keeps content and form usable |

Before review delivery, run Astro build and project checks, capture desktop and
mobile screenshots, compare proportions with the approved references, and
record the exact build SHA. Publication is limited to a non-indexable secret
candidate. The returned URL is a bearer review link: send it only to the
requested review destination and do not commit it. A draft PR and candidate
preview still do not authorize `main`, the production root, or indexing.

## Engineering references

- MDN: [`backdrop-filter`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/Properties/backdrop-filter) — the filter applies to pixels behind a partly transparent element and backdrop roots affect its result.
- MDN: [`mix-blend-mode`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/Properties/mix-blend-mode) — blending behavior and stacking-context implications.
- MDN: [`prefers-reduced-motion`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/At-rules/%40media/prefers-reduced-motion) — detect and honor the user's request to reduce non-essential motion.
- Supabase: [Database Functions](https://supabase.com/docs/guides/database/functions) — remote database functions, `security invoker`/`definer`, fixed `search_path`, and explicit execute privileges.
- Supabase JavaScript: [`rpc()`](https://supabase.com/docs/reference/javascript/rpc) — invoking a Postgres function from the public client.
- Supabase: [Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security) — RLS and exposed-schema access control.
