# Coding-agent brief: вариант «Кафельная мозаика»

> This is a durable implementation and review brief, not evidence that the
> feature has been built, tested, published, merged, or accepted. Record each
> completed check separately with its command/output, commit SHA and artifact.

## Mission

Implement an isolated Astro launch-placeholder candidate named **«Вариант
кафельной мозаики»** at `/lab/launch/tile-mosaic/`. It must reproduce the
physical dark tile/material composition, provide a replaceable continuous
image projection, and submit launch subscriptions through a narrow Supabase
RPC. Follow the canonical contract in
[`../launch-tile-mosaic-placeholder.md`](../launch-tile-mosaic-placeholder.md).

## Branch, ownership and absolute prohibitions

Work only in a branch created from
`feature/static-launch-tile-mosaic-20260803`.

Do **not**:

- modify the existing parallel launch-placeholder prototype;
- replace or edit `site/src/pages/index.astro` without separate written owner
  approval;
- merge this branch into `main` yourself;
- move the experiment away from `/lab/launch/tile-mosaic/`;
- add the lab route to a sitemap, production navigation or indexable output;
- treat a local build, draft PR, uploaded preview or screenshot as production
  approval;
- bake the image into the CSS grid or split it into per-tile image files;
- put user-controlled image input into inline CSS, HTML, script or
  `innerHTML`;
- expose a Supabase `service_role`/secret key or perform a direct client
  `INSERT` into the subscription table;
- reuse or change unrelated static-site, auth, personalization or database
  contracts.

Use the canonical image source:

```text
docs/features/static-site-pages/auto-present/scenario-assets/PWA-icon.png
```

Expose it to the static page through a byte-identical copy at
`site/public/assets/launch/PWA-icon.png`; do not silently replace the canonical
source with generated or unrelated art. Both files must have SHA-256
`7015488739e0296f6c5b04935a16769804aa8bf128436450e8a60eef32ec07dd`.

## Intended files

```text
site/src/components/launch/TileMosaicLaunch.astro
site/src/pages/lab/launch/tile-mosaic/index.astro
supabase/migrations/20260803143000_create_site_launch_subscriptions.sql
docs/features/static-site-pages/launch-tile-mosaic-placeholder.md
docs/features/static-site-pages/prompts/tile-mosaic-launch-coding-agent.md
CHANGELOG.md
```

Keep implementation changes bounded to this feature. Do not duplicate the
canonical specification into another feature document.

## Required implementation contract

### Scene and tiles

Build a real layered scene in this order: dark foundation, rough/leather
texture, vignette, one projected image, warm grading, metallic light, tile
grid, bevels/grout, noise, semantic UI, restrained bloom. The image is one
`<img>` beneath exactly 72 real HTML/CSS tile elements.

- Desktop: 12 columns × 6 rows.
- Mobile: 6 columns × 12 rows.
- The DOM collection stays the same across the breakpoint.
- Tiles are decorative and hidden from assistive technology.
- Every tile supports at least `sealed`, `dim`, `revealed`, and `glint`;
  implement the canonical `sleeping` rest state too.
- State controls cover alpha, saturation, brightness, blur, glint, edge and
  depth variables—not opacity alone.
- Use reproducibly seeded, slow, sparse changes: 1–4 tiles per 1.2–2.8 s
  iteration, 2.8–6.5 s easing, no more than 25–35% strongly revealed, no more
  than 2–4 glints, and no immediate tile reselection.
- Slowly move light independently; pointer input may gently bias it.
- Never create fast blinking, LED-matrix behavior or high-frequency randomness.

`backdrop-filter` and `mix-blend-mode` may create material/light effects, but
keep content readable and supply a usable fallback. A tile using
`backdrop-filter` must remain partly transparent. Isolate blend stacking and do
not blend semantic text or controls.

### Replaceable image

Support all of:

1. Astro prop `imageSrc`;
2. `?mosaicImage=<URL>`;
3. `tile-mosaic:set-image` `CustomEvent` with
   `{ src, focalX?, focalY? }`.

Allow local/same-origin URLs and absolute HTTPS URLs only. Parse with the URL
API; reject malformed values, credentials, unsafe schemes and cross-origin
HTTP. Update only `img.src` and bounded numeric focal coordinates. Invalid
input must preserve the last safe source. Do not weaken the deployed CSP to
make an arbitrary remote URL work.

### Layout

Desktop uses at least `100svh`, `clamp(28px, 4.1vw, 68px)` page insets, a
roughly 40–43% copy column and 57–60% scene. Place logo/status across the top,
lower the main copy in the left column, keep the H1 near three lines with a
590–630 px cap, and keep email/button in one row.

On mobile use this semantic order: logo/status, scene, H1, date, explanation,
email, button, privacy. Scene height is roughly 42–47% of the first viewport;
H1 uses `clamp(48px, 11.2vw, 84px)`; form controls are full-width in one
column. Validate widths `320`, `360`, `390`, `430`, `768`, `1024`, `1366`,
`1440`, and `1920` px. There must be no horizontal overflow at 320 px or any
required viewport.

### Supabase subscription

Create `public.site_launch_subscriptions` with the canonical identity,
metadata, status, submission-count and timestamp columns. Normalize email with
trim + lowercase, enforce one row per normalized email, and update a repeat
submission rather than inserting a duplicate.

Enable RLS and revoke direct table operations from `anon` and `authenticated`.
Expose only the narrowly granted function:

```text
public.subscribe_site_launch_v1(
  p_email,
  p_source = 'tile-mosaic-launch',
  p_page_path = '/lab/launch/tile-mosaic/',
  p_locale = 'ru'
)
```

It must use `SECURITY DEFINER`, an empty `search_path`, fully schema-qualified
relations and return the constant minimal shape
`{accepted:true,status:'subscribed'}`. Revoke default/`PUBLIC`,
`authenticated`, and `service_role` execution; grant execute only to `anon`.
Do not return a row ID, count, existence flag or other enumeration signal.

This table/function belongs only to the project's separate personalization
Supabase/Postgres contour; do not touch or migrate the Fly SQLite core. Apply
the migration through the backend/operator-only
`PERSONALIZATION_DIRECT_CONNECTION_STRING`.

The static client reads the existing
`PUBLIC_PERSONALIZATION_SUPABASE_URL` and
`PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`, with optional existing
`PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL`. It must call
`subscribe_site_launch_v1` with named arguments through the shared
`getResilientDataClient` singleton. Catalog this write as `selected-once`, not
`idempotent-replay`, because every accepted call increments
`submission_count`. A timeout after dispatch is ambiguous: do not
automatically retry, preserve the email, and tell the visitor to retry later.
Direct raw fetch and table writes are forbidden. Do not add generic
`PUBLIC_SUPABASE_*`, anon-key or service-key compatibility fallbacks. The
direct connection string and every service/secret key are forbidden from the
client. When public env is absent, preserve the useful page and entered email
and show an accessible unavailable/configuration result without issuing a
request.

The form has a labelled `type="email"` control, `autocomplete="email"`,
`inputmode="email"`, honeypot, keyboard-visible focus and an `aria-live`
result. Implement `idle`, `submitting`, `success`, and `error`, prevent
double-submit, treat the idempotent repeat as success, and retain email on
network/configuration failure.

Do not present the honeypot as a security boundary. A separately reviewed
server/gateway rate limit and abuse-monitoring policy is required before a
generally public/indexable rollout; the secret lab candidate must not silently
waive that production gate.

### SEO/GEO, noindex and accessibility

Render meaningful Russian static HTML. Provide title, description, canonical,
Open Graph, Twitter Card, one correct H1, visible service explanation,
`<time datetime="2026-09-01">`, and JSON-LD for `WebSite`, `WebPage`, and
`Service`. Add `Organization` only from verified data.

Visible copy must unambiguously name «Полюбить Калининград Анонсы»,
Kaliningrad, the Kaliningrad region, cultural events, educational events,
personalized recommendations, the event guide, and 1 September 2026.

The lab page must contain exactly the protective intent:

```html
<meta name="robots" content="noindex, nofollow, noarchive" />
```

Keep it out of the sitemap. Do not enable indexing until a separately approved
public-route release.

The content and form remain understandable by keyboard and screen reader when
decoration or JavaScript is absent. Decorative projection/tiles do not create
duplicate announcements. Status/errors do not rely only on color. At
`prefers-reduced-motion: reduce`, disable tile cycling, automatic/pointer light
travel and other non-essential movement, selecting a polished static scene
before animation can flash.

## Definition of done

Do not claim completion until all of the following are recorded:

1. `npm --prefix site run build` passes.
2. Relevant project checks and tests pass.
3. Desktop and mobile screenshots are captured.
4. Proportions are compared against the approved references.
5. Horizontal overflow is absent at every required width.
6. Exactly 72 tiles and the expected 12×6 / 6×12 computed grids are verified.
7. Normal and reduced-motion behavior are verified in a browser.
8. Safe and rejected image prop/query/event cases are exercised.
9. Form success, repeat/duplicate, invalid email, honeypot, double-submit,
   selected-once ambiguous timeout/network error and missing-Supabase-env
   behavior are exercised.
10. Database checks prove normalized idempotency, RLS, denied direct client
    writes, bounded RPC grants and the non-enumerating response.
11. SEO/JSON-LD/static copy are inspected and the lab remains noindex and out
    of the sitemap.
12. `CHANGELOG.md` and the canonical document are synchronized with actual
    behavior.
13. A draft PR is created without merging it to `main`.
14. The build is published only as a secret, non-indexable candidate preview.
15. Return the secret URL, exact build/repository SHA and screenshot set to the
    requested private review destination.

The secret URL is bearer review material: do not commit it or expose it in a
public channel. Candidate publication is not root promotion.

## Required engineering references

Use these official sources when implementing or reviewing the relevant
contracts rather than relying on visual trial-and-error:

- MDN [`backdrop-filter`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/Properties/backdrop-filter)
- MDN [`mix-blend-mode`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/Properties/mix-blend-mode)
- MDN [`prefers-reduced-motion`](https://developer.mozilla.org/en-US/docs/Web/CSS/Reference/At-rules/%40media/prefers-reduced-motion)
- Supabase [Database Functions](https://supabase.com/docs/guides/database/functions)
- Supabase JavaScript [`rpc()`](https://supabase.com/docs/reference/javascript/rpc)
- Supabase [Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security)
