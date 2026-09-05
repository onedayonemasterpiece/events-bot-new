# Astro SSG preview — event pages

> **Status:** accepted v11 full-catalog baseline plus v12 fidelity/idempotency corrections are the primary preproduction event templates; checked immutable secret candidates are refreshed through Smart Update/Kaggle. Production-root rollout remains pending.
> **Accepted template source:** `3b17e536` (`integration/static-event-v11-transport-phone-carousel`), including horizontal photo, photo+OCR companion, document-contain, portrait-series and quality-fallback families.

This is the primary Astro SSG preproduction implementation for `kenigevents.ru`
event-detail pages in `events-bot-new`, not a lab experiment. It remains
noindex and prefix-contained: no Supabase page-view write path, no
personalization telemetry persistence on ordinary views, and no LLM fragments
in rendered HTML. Event-detail discovery hydration is a static same-origin JSON
manifest; pgvector is used only during the offline build/search sidecar
pipeline, not as a live page-view ranking service.

Named `preview-…` URLs in this document are historical evidence only. The
canonical current review link is resolved from the last fully checked published
receipt, never hard-coded:

```bash
.venv/bin/python scripts/request_static_site_build.py \
  --db /data/db.sqlite --show-current-review
```

The durable pointer is internal SQLite state, not a public redirect. A failed,
no-op or artifact-only run preserves the previous immutable target; an
incomplete receipt resolves to unavailable. Production `/`, `current.json` and
stable `/ics/*` stay untouched.

## Local focused route workflow

Use the canonical local command for one route or one named page class. It stages
the exact committed source in a detached worktree, invokes the existing exporter,
page-class selector, Astro preview builder, slice checker and release server, and
creates no repository DB, `dist`, publication or retention state.

Deterministic offline fixture with desktop/mobile browser smoke:

```bash
FIXTURE_DATE=$(node -p "require('./site/src/data/preview-events.json').build.current_date")
npm --prefix site run local:focused -- \
  --route "/date-${FIXTURE_DATE}/" \
  --fixture --offline --open
```

A real event-detail slice accepts `--db`, exact `--entity-id` and
`--entity-slug`. `--page-class date|event|weekend|collection|personal|focus|partner|lab`
keeps the deliberately broader class-wide diagnostic distinct from exact-route
mode. Exact-route materialization retains only the chosen route, its required
event endpoints when applicable, `/__preview/`, `robots.txt`, manifests and
shared assets. Static asset metadata is excluded from the product-route receipt;
same-class neighbour pages are rejected by the receipt gate.

Astro's global `trailingSlash: 'always'` also appends `/` to prerender entry
pathnames for `.json` and `.ics` endpoints. The focused selector canonicalizes
those entries back to extension-final asset paths before matching, so an exact
event slice cannot silently omit its discovery manifest or calendar companion.
The browser smoke ignores only Chromium requests explicitly marked
`sec-purpose: prefetch` for intentionally excluded neighbour pages; HTTP errors
for the selected document, owned runtime endpoints, scripts and fetches remain
fatal.

The command prints source/snapshot/data identities, selected owner source,
generated routes, timings and browser results. It never publishes. Full
owner-facing `real/all` remains on the dedicated Review Preview Kaggle kernel;
production candidates remain on the production kernel. The sole page-class
allowlist remains `site/scripts/static-site-page-classes.v1.json`. A verified
`PLAYWRIGHT_EXECUTABLE_PATH` reuses the installed local Chromium; otherwise the
staged checkout installs its matching Playwright browser.

Browser smoke runs at the artifact's declared Kaliningrad build date. This is
required for deterministic older fixtures: their `/segodnya/` owner is tested
as generated instead of following the intentional live stale-date redirect to
a route outside the exact local slice. Stale/current/redirect behavior remains
covered separately by the date-availability contract tests.

`check:preview` treats a date listing without mobile event rows as valid only
when the generated page retains the mobile rail shell and renders the explicit
no-events marker as visible. This keeps empty current dates buildable without
weakening the missing-markup regression check for non-empty listings.

Popular occurrence validation is likewise driven by the ranked catalog rather
than a hard-coded fixture. A selection with no repeated-date family is valid.
When a selected event has `other_date_ids`, the gate requires its actual
distinct repeat count in the temporal label and fails if a linked occurrence is
also rendered as a second desktop card. Never restore a literal requirement
such as `ещё 1 показ`: production catalog composition is mutable. This is the
regression contract for
[`INC-2026-08-03-static-site-builder-failure-storm`](../../reports/incidents/INC-2026-08-03-static-site-builder-failure-storm.md).

The reviewer enters through `/<id>/__preview/`; that hub owns the page-type
inventory and links only targets inside the same prefix. It must state that the
integrated Search visual is not acceptance of the live auth/backend journey.

## Owner-facing Preview archetype inventory

The existing `/<id>/__preview/` hub must render exactly one prefix-local
representative link for each family below. Extra product links may remain, but
they do not satisfy this coverage gate and do not receive the
`data-owner-archetype-family` marker. `/lab/*` and the Preview directory itself
are non-product infrastructure: they may be generated, but are excluded from
owner review, V0 verdicts, product readiness and A=S=P completion percentages.

| Family ID | Representative route |
|---|---|
| `home` | `/` |
| `today` | `/segodnya/` |
| `tomorrow` | `/zavtra/` |
| `date` | one current materialized `/date-YYYY-MM-DD/` route distinct from Today and Tomorrow |
| `weekend` | `/vyhodnye/` |
| `popular` | `/populyarnoe/` |
| `collections` | `/podborki/besplatnye-sobytiya/` |
| `festivals` | `/festivali/` |
| `exhibitions` | `/vystavki/` |
| `favorites` | `/izbrannoe/` |
| `search` | `/poisk/` |
| `for-me` | `/dlya-menya/` |
| `focus-group` | `/fokus-gruppa/` |
| `artifacts` | `/artefakty/` |
| `interest-clubs` | `/kluby-po-interesam/` |
| `unusual-events` | `/neobychnoe/` |
| `event-detail` | one current real `/sobytiya/{slug}/` route |
| `information` | `/partners/` |

`site/src/pages/[preview]/index.astro` owns the 18-family source registry: all
17 required production-contract archetypes plus the separately useful arbitrary-Date
representative.
`check:unified-prototype` reads that registry and the generated hub, then fails on a
missing, duplicate, misrouted or non-materialized representative. Event Detail uses
one real specimen; the hub does not enumerate the event catalog. The same gate also
reads
`design-system-production-surface-contract.v1.json` and rejects any missing or
duplicate required contract archetype, so the owner directory cannot silently drift
behind the canonical production inventory.
For a real-data occurrence specimen it follows the current `EventOccurrenceNav` contract:
desktop and mobile summaries plus the always-visible practical selector identified by
`data-occurrence-variant="practical"` and its `event-occurrences__rows` content owner.
Other full-real checks likewise choose factual specimens from the exact staged
projection (compatibility grid, breadcrumb, semantic-error media and persisted
duration forecast). Historical named regressions such as `6686` and `6529` remain in
their dedicated deterministic packets; their absence from a bounded current-event
slice is not itself a production defect.

The `/dlya-menya/` page is an honest finite cold-start surface. Production
generation retains it for product navigation but keeps it explicitly noindex
and outside the sitemap; it is not presented as server-personalized SEO
content. A successful local build or prefix upload never authorizes root
promotion or a stable calendar rewrite.

The same Astro page set now includes `/festivali/` in production generation.
Its current catalog is exported from core Fly SQLite rather than
the former hardcoded TypeScript array. Source/status honesty rules, DB/backfill
ownership and compact one-to-four-card packing are canonical in
[`festival-timeline.md`](festival-timeline.md). Immutable review artifacts
remain noindex; the generated root-form page does not imply that the
default-off two-bucket/ALB publisher or its still-missing live infrastructure
has been enabled.
The current `r3` correction withdraws the split-body `r1` candidate: cards are
now full-cover overlays in donor-density rows, the `390px` layout keeps two
columns, and all media is hash-bound to festival/organiser/venue provenance
without the regional anniversary aggregator.

The R15 candidate additionally stages the static unusual-events manifest and
daily service-share assets inside this same immutable snapshot/Kaggle handoff.
It does not add a page-view scorer or another builder job. The shared BGE
artifact, unusual cache/last-good receipt, `provider_calls=0`, migration
notification suppression and real canary/rollback gates are canonical in
[`unusual-events`](../unusual-events/README.md). Final exact code SHA
`123bcee460112ee9fe0b0a0176f51a07c92eed6a` passed build
`production-r15-bge-final5-20260727t221000z` against the immutable
`prod-20260727` snapshot (326 events). The secret candidate is available at
<https://kenigevents.ru/_review/pp1wRctXBd6boYU1EcnBrod3z8MmKpD7SGEufK1t-xw/>;
production root remains untouched.

For `profile=production-candidate`, Kaggle must preserve the legacy preview
contract as a pre-gate rather than jumping directly to root-form output:

1. run `npm run build:preview` and `npm run check:preview` under an isolated
   `preview-gate-<build>` ID;
2. mark that output `archived=false`, `published=false`, then require the
   production build to remove it;
3. build/check the production-root form and run its Chromium browser-release
   gate;
4. build/check the immutable noindex secret candidate and run its separate
   Chromium browser-release gate.

Missing/failed preview evidence or preview-gate files leaking into the root
archive is a release failure. The preview pre-gate is not a third artifact or
publishable URL. If its HTML points Astro runtime assets at the configured
immutable CDN prefix, `check:preview` maps only the bounded `/_astro/…` suffix
back to the local generated tree. It verifies the exact local CSS bytes without
depending on an unpublished CDN prefix or assuming that the ephemeral and
production build IDs are equal. The earlier `11d8c984` canary predated this
sequence and is superseded. The final exact run reported the preview pre-gate as
`ok`, non-archived and non-published, then passed both generated browser gates.
`/neobychnoe/` remains candidate-only and no production-root promotion is
implied until the owner accepts the complete candidate.

The clean preview also pins the generated, reciprocal multi-image browser
journey `6408 → 6407`; both pages must retain a real closed gallery and the
source must retain at least three canonical recommendation cards. Browser media
acceptance distinguishes loaded pixels from the intentional missing-media
fallback. A loaded image must remain inside its shell. A failed image must be
removed from painting and expose the bounded fallback; its zero rectangle is
not an image-shell escape. This distinction preserves geometry enforcement
while making CDN/network failure evidence deterministic.

The production catalog is calendar-day inclusive: every otherwise-public event
whose start date is the current Kaliningrad date remains in the static export
after its start time. This lets `/segodnya/` render elapsed events with the
accepted muted state and keeps its mobile rail structurally present late in the
day. Surfaces that must exclude already-started one-offs, especially Popular,
apply their own start-instant eligibility after export. The catalog eligibility
ledger uses the same inclusive predicate.

Direct event links have a separate, bounded lifecycle. A full-catalog export
writes recently elapsed canonical public events to
`preview-event-archive.json` for **30 days**. That projection may generate only
the event detail route and its ICS response; it is not imported by listings,
Search, Popular, recommendations, personalization, the active catalog ledger
or sitemap. Grace-period detail pages are `noindex,nofollow,noarchive`. This
keeps a shared/reviewed event URL such as `6529` from turning into a next-day
404 without reintroducing an elapsed event into discovery. Silent, aliased,
non-active or public-gate-rejected rows remain excluded fail closed.

The 2026-07-23 correction candidate additionally keeps every reviewed surface
inside one mutually linked prefix: the dynamic Exhibitions presentation,
optimized responsive personal-card feed, Search, current clubs, six-logo Partners,
deep-page breadcrumbs and the two event-detail regressions (`6686`, `6529`).
It is still a review build, not a root rollout.

The R3 correction supersedes the read-only Search specimen and the obsolete
build-time duration provider: Search is built only with a safe public Supabase
configuration and resumes the saved intent after Yandex PKCE; a deterministic
test-only smoke can mock the PKCE/session boundary without adding a production
auth bypass. Missing duration is estimated earlier by Smart Update only for
events that an implemented transport surface can use, stored as the separate
nullable `duration_forecast_minutes` field and merely exported by the builder.
It also restores V7 hotkeys to named immutable previews, applies exact
source-keyed media reviews to compact related rows, moves desktop Editorial
breadcrumbs onto the lower hero edge and progressively paints the unchanged
desktop tag with a border-preserving leather crop over its immediate
terracotta fallback.

R3 additionally audits every organizer/festival manifest entry against current
and retained production events, restores event-detail festival resolution and
Telegram-readable fallbacks, verifies the current Museum of Resort Fashion
event resolves `mumod`, and adapts the clubs catalog with geometry-scoped
hotkeys plus a source-grounded Game Vibes cover. The Goblin recommendation on
event `6529` is an exact-source reviewed `5:4` derivative: as visual-only media
it must fill the row and may not expose letterbox fields.

The R4 desktop review keeps the R3 data/runtime contracts and changes only the
review presentation. The desktop `240×88` brand tag now uses the clean supplied
`head-skin-desctop (2).png` leather as a local alpha WebP under the existing
live SVG/DOM lockup; the stitched side edges, lower seam, rounded foot and
contact shadow remain visible, while `#98401f` paints immediately before load
or after an image failure. Event `6686` uses a genuinely compact breadcrumb
line with an out-of-flow expanded pointer target, so the type, date and title
move into the first viewport. Event `6529` renders its resolved `mumod`
identity in the centered desktop TopSlot and does not manufacture an InlineSlot
from price/audience fact pills. The clubs catalog places a real future-meeting
count in the upper-right media corner with a restrained downward glow. These
are desktop acceptance changes; the existing mobile compositions remain the
review baseline for a later pass.

R5 is a bounded presentation-polish successor to R4. It removes the offset dark
backing and pale extraction remnant that remained visible below/right of the
desktop leather face, while preserving the stitched outer edge and live
SVG/DOM lockup. It also makes the existing `Ближайших встреч` club-card accent
visibly glow below the label on desktop. That glow was the first item entered
in the central presentation debt register (`TD-PRESENTATION-UI-001`) and was
closed in the same R5 pass after the user explicitly allowed the deferred polish
to ship now. Mobile layout and behavior remain unchanged pending the next
explicit review.

Immutable R5 review routes:

- <https://kenigevents.ru/preview-20260723-unified-corrections-r5/__preview/>;
- <https://kenigevents.ru/preview-20260723-unified-corrections-r5/kluby-po-interesam/>;
- <https://kenigevents.ru/preview-20260723-unified-corrections-r5/sobytiya/dekorativnoe-mini-panno-tkanye-uzory-zelenogradsk-6529/>.

The links were delivered to the requested `KenigEvents · UI review` Telegram
forum thread as verified message `608` (topic anchor `548`). Public Chromium
smoke at `1440×900` and `390×844` returned HTTP `200`, preserved
`noindex,nofollow,noarchive`, and found no horizontal overflow, broken images,
console errors or page errors. The 390 px run is technical smoke only, not
mobile product acceptance.

The published R2 review object is
<https://kenigevents.ru/preview-20260723-unified-corrections-r2/__preview/>.
Generated and live browser gates passed; valid `agy`
`gemini-3.1-pro-low` resolved to `Gemini 3.1 Pro (Low)` and returned
**CONDITIONAL** only on the remaining real-user Yandex OAuth round-trip. This
is a manual identity-provider gate, not permission to replace the real result
path with a demo or to promote the production root.

## 2026-07-18 v12 fidelity and production-rail contract

V12 applies the accepted component system to automatic generation rather than
recreating it per candidate:

- transport A/B/C uses the exact Telegram `261–264` hierarchy/copy and shared
  icons; a secret-only noindex specimen remains query-forceable after event
  `4671` elapsed;
- desktop ticket/telephone/information CTAs share the invariant bottom
  calendar/share/like row; the phone remains a branded reveal-and-copy action;
- the immutable candidate retains one Split phone-CTA fixture and one Editorial
  footer/CTA fixture, so the live `1536×864` Playwright geometry gate remains
  executable after the original acceptance events leave the active catalog;
- venue medallions are evidence-aware, capped at one and fail closed on
  conflicting structured identities;
- supplied symphonic/lecture art is presentation-only fallback and never enters
  gallery, Open Graph, JSON-LD, share media or the canonical media ledger;
- `/segodnya/`, export, runner and receipt use one `Europe/Kaliningrad` clock;
- Smart Update requests are coalesced, fingerprinted against the canonical
  public projection and protected by a durable SQLite claim. A crashed Fly
  waiter must reconcile/adopt the exact Kaggle dataset/output before another
  push; automatic/operator requests no-op on an unchanged fingerprint unless
  the operator explicitly requests `force_rebuild`. A terminal status-dataset
  row does not make that claim supersedable: until the exact owner recovery
  persists success/failure and clears the claim, every newer fingerprint stays
  `busy`. This closes the short interval where the callback ledger is terminal
  but Kaggle REST still reports `RUNNING`.
- immutable SQLite inputs live only for the handoff lifecycle. Success,
  no-op, recovered success and failures without a durable remote dataset delete
  the snapshot, manifest and SQLite sidecars. A pushed dataset keeps its exact
  input and claim until adoption/recovery; a pre-build crash guard preserves it
  plus at most `STATIC_SITE_SNAPSHOT_KEEP_LATEST_TERMINAL=1` newest unreferenced
  complete pair. Snapshot accumulation is never allowed to consume the Fly
  `/data` health reserve.
- the 256-bit base64url candidate token is handed to the runner as
  `--candidate-token=<value>`; a leading `-` is valid token entropy, not a new
  CLI option and must not cause a retry loop.
- recognized incomplete backup files are removed only after
  `STATIC_SITE_SNAPSHOT_STALE_INCOMPLETE_SECONDS=900`; unknown files are never
  touched, and an unreadable active handoff disables pruning fail-closed.

Event `6774` is not a valid no-image specimen: incident evidence identifies it
as the later teaser duplicate `6774→2884`. Production is repaired to the
canonical `2884`; its official sources were merged, duplicate Telegram/VK
surfaces removed and the duplicate Telegraph page redirects to the survivor.

## 2026-07-18 v11 regression repair evidence

The next immutable secret candidate supersedes the visually rejected
`5roC…` candidate and must be produced from a fresh read-only Fly SQLite
snapshot through `scripts/run_static_site_builder_kaggle.py`; a local Astro
build is test evidence, not the handoff artifact. Production root, `current` and
stable `/ics/` remain unchanged until explicit product acceptance.

Release acceptance adds `ADD-RECENT-06..10`:

- one quality-admitted photo family is shared by desktop/mobile; weak images are
  removed only when strong event-local alternatives exist;
- a weak-only low-resolution portrait is retained but source-size bounded with
  `contain` in desktop/mobile hero and fullscreen viewer instead of being
  enlarged/cropped;
- stored OCR/document roles remain non-crop and exact structured source
  occurrences fail safe when the canonical aggregate is contradictory;
- editorial leads end at a real sentence or a disclosed ellipsis;
- KAUP keeps all three accepted timetable arms, while the accepted departure
  board replaces the old compact list in off/no-JS/elapsed/automation fallback.

The review handoff must include the ordinary KAUP URL and all three forced
`?ke-exp-transport=` URLs. Forced impressions/actions stay excluded from trusted
experiment telemetry. The current production A/B/C mode remains `off`; a secret
candidate is visual/product acceptance, not permission to promote root or start
live experimentation.

Every HTML document in an immutable secret candidate, including pages that are
already private such as `/dlya-menya/` and `/podborki/*`, uses the stronger
`noindex,nofollow,noarchive,nosnippet` policy plus `no-referrer`. Page-local
`noindex` must not weaken that candidate-wide isolation contract.

Final automated acceptance evidence for the corrected candidate:

- source `a6ad22fba8b63e3dee7a71b8ca0837494c554033` is reachable from
  `origin/main`; GitHub CI passed;
- Kaggle build
  `production-20260718t-static-event-v11-regression-repair-kaggle-v2b`
  used snapshot `snapshot-20260718t-v11-regression-repair`, SHA-256
  `8c784e2d14b34738a89f4cf0101645a46e470a2147c7752f73db7dcf83629972`,
  `quick_check=ok`, and produced `323` event pages / `1172` files with all
  required production and secret-candidate checks green;
- durable docs identify the secret prefix only by token SHA-256
  `4c906f92db3bbf5c448bf6b29fc650a40dbc2f7841008555657e84bdaec10b66`.
  Authenticated inventory and public hash/MIME verification passed for all
  `1173` objects. An interrupted upload was resumed only after verifying the
  `745` already-created single-part objects; the remaining `428` were created
  with `If-None-Match`, and no existing object was overwritten;
- seven public HTTP specimens returned `200` with `noindex` and
  `no-referrer`; Playwright passed `21` focused desktop/mobile checks, all three
  forced transport arms, `36` event pages at both `320` and `390` px, and five
  actual related-event transitions;
- production root and sitemap body hashes stayed byte-identical. Stable ICS was
  observation-only because that prefix has an independent writer; it also did
  not change during this final candidate publication;
- links were sent and read back as Telegram message `300` in chat
  `4337049383`, topic `2`. Production root remains untouched pending visual
  acceptance.

## Public URLs

Required URLs for the historical 2026-07-02 detailed review target:

- Preview index: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/__preview/>
- Desktop/media regression: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sobytiya/spektakl-garazh-kaliningrad-5658/>
- Real photo/mobile V8 regression: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sobytiya/kontsert-festival-pianissimo-maksim-miloslavskiy-kaliningrad-5294/>
- OCR/mobile V8 regression: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sobytiya/epidemiya-ognennaya-rukopis-kaliningrad-4671/>
- Rail transport regression: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sobytiya/joe-lynn-turner-i-j-l-t-band-svetlogorsk-5789/>
- Bus transport regression: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sobytiya/slet-babok-ezhek-romanovo-6710/>
- Today listing: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/segodnya/>
- Tomorrow listing: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/zavtra/>
- Weekend listing: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/vyhodnye/>
- Search page: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/poisk/>
- Exhibitions/long-running listing: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/vystavki/>
- Popular-by-source-engagement listing: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/populyarnoe/>
- Information partnership/reference block page: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/partnerstvo/>
- Information partners directory: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/partners/>
- Event-token medallion QA lab: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/lab/medallions/>
- Historical related discovery JSON: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/data/discovery/5658.json>
- Preview sitemap: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/sitemap.xml>
- Preview robots: <https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/robots.txt>
- Yandex Object Storage website fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260715t-production-mobile-v8-related-transport-v3/__preview/>

CDN media/ICS verification for those historical previews: event images in rendered HTML/JSON-LD use `https://static.kenigevents.ru/p/...`, raw legacy `https://storage.yandexcloud.net/kenigevents/...` image URLs do not leak into HTML, calendar CTAs point to stable `https://static.kenigevents.ru/ics/<event_id>.ics`. v59 discovery JSON uses `event_pgvector_related_chain_v1`; v62 and the 2026-07-02 recovery preview use `event_pgvector_related_chain_v2_two_doc` with `embedding_document_version=related_v1`; the 2026-07-02 recovery preview has Gemma strict verification disabled for the fast end-of-day rebuild and keeps the pgvector chain/audit metadata transparent in `preview-related.json`.

## Code layout

```text
site/
  package.json
  astro.config.mjs
  tsconfig.json
  scripts/build-preview.mjs
  scripts/check-preview.mjs
  scripts/page-class-build-filter.mjs
  scripts/check-page-class-preview.mjs
  src/pages/[preview]/index.astro        # emits /__preview/
  src/pages/segodnya/index.astro
  src/pages/zavtra/index.astro
  src/pages/vyhodnye/index.astro
  src/pages/vystavki/index.astro
  src/pages/populyarnoe/index.astro
  src/pages/poisk/index.astro
  src/pages/partnerstvo/index.astro
  src/pages/partners/index.astro
  src/pages/sobytiya/[slug].astro
  src/pages/sobytiya/[slug]/event.ics.ts
  src/pages/data/discovery/[eventId].json.ts
  src/pages/lab/medallions/index.astro
  src/pages/lab/hero/index.astro
  src/pages/lab/hero/review/index.astro
  src/pages/sitemap.xml.ts
  src/pages/robots.txt.ts
  src/layouts/EventLayout.astro
  src/components/EventHero.astro
  src/components/EventCtaPanel.astro
  src/components/EventFacts.astro
  src/components/EventCard.astro
  src/components/EventListItem.astro
  src/components/PersonalFeedSlot.astro
  src/components/CalendarLink.astro
  src/components/Icon.astro
  src/lib/assets.ts
  public/favicon.svg
  src/data/preview-events.json
  src/data/preview-related.json
```


## 2026-07-02 merged vector-gate + medallion preview

`preview-20260702t1536-merged-vector-medallions` supersedes `preview-20260702t0755-fresh-ui-fixes` for the Static Site MVP review because it merges the parallel medallion SVG upgrade and the Smart Update vector identity gate branch before exporting from the latest 2026-07-02 production SQLite snapshot.

Evidence:

- exported `399` active/future public events, max event id `6613`; the build includes production events `6601`–`6605` created after the vector-identity gate rollout;
- related chains use `event_pgvector_related_chain_v2_two_doc` / `related_v1` over Supabase pgvector; the fresh sync upserted `399` documents and `101` changed/new embeddings, while `697` embeddings were skipped as unchanged;
- build/check target: `PREVIEW_BUILD_ID=preview-20260702t1536-merged-vector-medallions`, `npm --prefix site run build:preview`, `npm --prefix site run check:preview`;
- deploy target: `s3://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/` plus stable `s3://kenigevents.ru/ics/<event_id>.ics` files; deploy verification reported `Public preview verification: ok` and `Stable CDN ICS uploaded: 399`;
- public HTTP smoke returned `200` for `__preview/`, `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/poisk/`, `/partners/`, `/lab/medallions/`, `sitemap.xml`, `robots.txt`, sample event pages `5264`, `6585`, `6601`, `6605`, `6613`, and stable ICS files for those ids;
- authorized-search readiness passed with live Edge CORS, Supabase Auth redirect config, `custom:yandex` provider and `yandex-userinfo` adapter probes; mocked UI smoke and real Edge Playwright smoke passed (`интересно детям` returned rendered cards and quota status);
- `/vystavki/` vector-identity regression audit with `--since-date 2026-07-02` returned `high_confidence_duplicate_count=0`; identity-gate rollout audit on the same snapshot reports `env_readiness.ready=true`, `identity_gate_vector_available_count=13`, and the only `vector_error_count=2` rows are the pre-secret 14:26/14:33 decisions.

OCR/poster text contract for this preview: raw poster OCR is not embedded directly into `search_v3` or `related_v1`. It can affect search only indirectly if Smart Update has already promoted source-grounded poster facts into canonical public fields (`title`, `description`/`search_digest`, venue/address, topics). This prevents commercial venue names printed on posters from dominating `/poisk/` or static related cards unless the venue was canonically accepted by the event extraction/update pipeline.


## 2026-07-02 recovery preview: fresh data + UI repair

`preview-20260702t0755-fresh-ui-fixes` supersedes the earlier same-day UI-only preview because the first rebuild still carried `current_date=2026-07-01` data. The accepted preview is exported from the 2026-07-02 production SQLite snapshot and the personalization Supabase pgvector sidecar:

- exported `376` active/future public events, max event id `6585`, including late ids `6566–6585` from the latest production snapshot;
- related chains use `event_pgvector_related_chain_v2_two_doc` / `related_v1` over Supabase pgvector; the fresh sync upserted `376` documents and `64` changed/new embeddings, while `688` embeddings were skipped as unchanged;
- build/check target: `PREVIEW_BUILD_ID=preview-20260702t0755-fresh-ui-fixes`, `npm --prefix site run build:preview`, `npm --prefix site run check:preview`;
- deploy target: `s3://kenigevents.ru/preview-20260702t0755-fresh-ui-fixes/` plus stable `s3://kenigevents.ru/ics/<event_id>.ics` files; deploy verification reported `Public preview verification: ok` and `Stable CDN ICS uploaded: 376`;
- public HTTP smoke returned `200` for `__preview/`, `/poisk/`, `/partners/`, the Pianissimo regression event `5264`, fresh event `6585`, and stable ICS files `5264.ics` / `6585.ics`;
- public Playwright visual smoke passed: `5` broken upstream related-card images were converted to fallback surfaces with `0` visible broken icons, the mobile tag did not overlap nav links, four partner logos loaded, search submit/progress/avatar geometry matched the recovered UI, and the footer partner item was a transparent plain link;
- UI regression fixes included in the build: broken related-card images fall back to a neutral image surface instead of showing raw alt text/broken icons, the mobile terracotta drawer rail is tall enough not to overlap the top navigation, `/poisk/` restores the account avatar/search-button progress polish, `/partners/` is a logo-first minimalist page, and footer partner navigation is a plain link rather than a pill button.

Yandex Cloud CLI auth was not reinitialized for this recovery: the existing local profile/cache (`/home/dev/yandex-cloud/bin/yc`, `/home/dev/.config/yandex-cloud/`, profile `artkoder`) was verified with a control-plane API call. Static deploy still uses S3/Object Storage credentials from `.env`; no new browser `yc init` flow is part of the static-preview rebuild.

Database routing remains dual-DB: canonical event data comes from Fly SQLite `/data/db.sqlite`; personalization/search data lives in the separate Supabase/Postgres project. No tracked code or `.env.example` wiring for Yandex YDB exists as of this recovery pass.

## v46d regression and related-chain evidence

`preview-20260628-event-pages-v47-sparse-fixes` was generated on Kaggle CPU from the 2026-06-28 production SQLite snapshot with `--current-date 2026-06-29` and 50 real tomorrow/future events. Kaggle result: `ok=true`, event count `50`, archive `preview-20260628-event-pages-v47-sparse-fixes.tar.gz`, runtime 19:40:00–20:07:18 UTC.

UI fixes included in this preview:

- personalization reset now records and displays `Последний сброс: DD.MM, HH:mm`;
- sold-out/unavailable events render `Билеты закончились` and do not expose `Купить билет`;
- hero `Фото` hint is positioned above the decision sheet and no longer overlaps the content below;
- markdown-like source descriptions no longer become multi-paragraph bold blocks;
- opened mobile tag/drawer closes automatically after the user scrolls/continues the page;
- event pages display last update time in Kaliningrad time;
- OCR/poster hero images keep parallax transforms instead of disabling parallax.

Related/discovery changes:

- related chains in this preview are built by `event_sparse_related_chain_v1` with honest lexical/sparse retrieval (`local_tfidf_sparse_v1`) plus deterministic/facet scoring; this is not semantic vector search;
- Gemma 4 26B verification ran only through `GoogleAIClient` + Supabase limiter. The full Kaggle run audited 45 anchors with 45 provider calls and ended `partial` because 4 provider calls timed out at 45s and 1 response was malformed; those anchors fall back to vector chains. The persisted cache is nevertheless usable for rebuild stability; a rerun with the same cache reported `cache_hit_no_provider`, `provider_calls=0`, `cache_hits=50`;
- Kaggle no longer relies on UI secrets for API-started runs: the runner attaches encrypted split secret datasets and deletes them after the waited run.
- Public release flags loaded from that encrypted bundle must be copied into
  Astro's explicit subprocess environment after the export step. In particular,
  `PUBLIC_INTEREST_CLUBS_ENABLED` is bridged after decryption so the DB exporter,
  the clubs index and club-detail `getStaticPaths()` all observe the same
  release decision; the generated-output gate fails if this regresses to an
  empty catalog.

Verification evidence: Kaggle `npm run check:preview` passed; public `curl -I` returned HTTP 200; `artifacts/codex/static-site-builder/playwright-v46d-public-check.cjs` passed against the published URL.

## v48 Supabase pgvector semantic related canary

`preview-20260628-event-pages-v48-pgvector-gemma-kaggle` was the first pgvector focus preview for the 2026-06-28/29 data slice. It was built on Kaggle CPU from a production SQLite snapshot with 70 real events, synced compact search documents/vectors into the separate personalization Supabase project, built related chains through Supabase pgvector and deployed the checked artifact to the `kenigevents.ru` bucket/CDN path.

Retrieval contract:

- `algorithm_id=event_pgvector_related_chain_v1`;
- `strategy=event_pgvector_related_chain_v1_manifest`;
- `retrieval_method=supabase_pgvector_hnsw_cosine_v1`;
- `semantic_embeddings=true`;
- model/dimension: `gemini-embedding-2`, `vector(768)`;
- ordinary event-page views still read static JSON; no page-view Supabase/embedding/LLM call is introduced.

Evidence from 2026-06-29 UTC:

- local vector sync processed 70 docs and wrote 12 changed/new vectors after weekday/category hardening;
- live personalization Supabase contains 76 search documents and 76 embeddings for `gemini-embedding-2/vector(768)`;
- local Gemma 4 26B verifier canary completed `status=ok`, `audited_anchors=15`, `provider_calls=7`, `cache_hits=8`, `errors=[]`;
- Kaggle CPU run `preview-20260628-event-pages-v48-pgvector-gemma-kaggle` completed with `ok=true`, `event_count=70`, vector sync `provider_calls=0` because the Supabase vectors were already current, and `npm run check:preview` passed inside the notebook;
- live public smoke for `/data/discovery/6447.json` returns `6310` “Архитектурно-урбанистическая студия...” as first candidate with `vector_similarity≈0.8592`, `llm_semantic_score=0.92`, fixing the earlier “Музыка нашего города” lexical false-positive.

Remaining production gate after v48 was automatic Smart Update → Kaggle → CDN promotion after artifact checks; v59 below supersedes v48 for strict related-quality review.

## v59 strict pgvector + Gemma 4 related preview

`preview-20260629-event-pages-v59-related-gemma50` is historical strict related-events canary evidence. It was generated from a read-only production SQLite snapshot on 2026-06-29 with `--current-datetime 2026-06-29T21:30`, prioritising events starting on 2026-06-30 and 2026-07-01. The two-day focus window contained 21 eligible one-day/short events, so the exporter supplemented later active future events to reach a 50-event review slice.

Related/publication contract:

- event-to-event retrieval starts with Supabase `pgvector` over `gemini-embedding-2/vector(768)` search documents;
- Gemma 4 26B (`models/gemma-4-26b-a4b-it`) then sees the top retrieved candidates, rejects unrelated events and returns the final order;
- public `similar[]` / `related_static[]` contain only candidates explicitly accepted by Gemma with `llm_semantic_score >= 0.72`; weak 0.55–0.71 candidates may remain only as adjacent/explore metadata, not as “similar” cards;
- the raw pgvector chain is still stored in the manifest for audit/debug, but Astro consumes the strict verified list when `strict_verified_related=true`;
- already-started same-day events and past/cancelled/deleted/duplicate events are excluded during export, so a related card disappears from new builds when it is no longer actionable.

Performance evidence from the v59 local canary:

| Step | Result |
|---|---:|
| Focus export | 50 events |
| Pgvector/vector sync | 44 new/changed embeddings, 6 unchanged; 32.59s wall |
| Pgvector chains | 50 anchors, 40 raw candidates per anchor |
| Gemma strict audit | 50 successful anchors, 60 total attempts after retries |
| Gemma wall time | 22m53s first pass + 3m53s fill-missing |
| Gemma timings | p50 ≈ 18.3s, avg ≈ 17.0s for successful first-pass calls |
| Final cached export | 0 provider calls, 0.47s |
| Astro build | 66 pages, ≈5.7s |

Golden check: event `6447` («Как договориться о будущем города») now shows only event `6310` («Архитектурно-урбанистическая студия...») as strict similar in the first slot (`llm_semantic_score=0.88`), instead of letting a lexical “город” music false-positive into the related feed.

Verification evidence:

- `npm run check:preview` passed for a non-control focused build and verifies strict Gemma score metadata;
- public HTTP checks returned 200 for `__preview/`, event `6447` and `/data/discovery/6447.json`;
- Playwright mobile smoke against the public URL verified that the first visible related card for `6447` is `6310` and that the discovery JSON carries `llm_semantic_score=0.88`.

### v61 full-catalog Gemma verifier prompt audit

The first full future-catalog stress attempt exposed a Gemma verifier I/O issue:
embeddings and pgvector retrieval were cached/reusable, but the old verbose
Gemma prompt produced many invalid/truncated JSON retries. The related verifier
now uses the compact v4 contract documented in
`docs/features/unsigned-personalization/semantic-vector-retrieval.md`:

- Gemma sees compact 10-candidate batches by default (`6..12` allowed);
- the static related audit runs 2 passes by default, so it can inspect up to
  20 strongest pgvector candidates without one large fragile JSON response;
- the model returns `event_id`, `llm_semantic_score`, `similarity_class`,
  `confidence` and `reject`; verbose reason-code explanations stay out of the
  output;
- app code can rescue only fully complete verdict objects from a truncated JSON
  tail;
- strict “Похожие” remains LLM-verified; lower pgvector candidates should be a
  separately headed discovery section, not silently mixed into similar cards.

Evidence is in `artifacts/codex/related-gemma-prompt-audit-20260630/`: Gemini
3.1 Pro review completed, Opus review is explicitly blocked (empty `a-opus`/`agy`
outputs and Claude `401`), and local live smoke on anchors `6447`, `5878`, `5370`
returned valid compact Gemma JSON in `6.22–8.20s`; after restoring
model-provided `similarity_class`/`confidence`, the synthetic smoke returned
valid JSON in `7.00s`. A new full Kaggle run is still needed to measure the
statistical retry/error reduction on all anchors.


## v62 full-catalog two-document pgvector + Gemma run

`preview-20260630-event-pages-v62-two-vector-gemma-full` is the first full future-catalog stress run for the implemented two-document retrieval split:

- `search_v3` vectors remain optimized for authorized `/poisk/`;
- `related_v1` vectors are used by static event-to-event related chains;
- Supabase RPC `event_related_candidates_by_event_id_v1(..., p_embedding_doc_kind => 'related_v1')` is the recall layer;
- Gemma 4 26B validates/reorders the top candidate windows offline;
- public `similar[]` is strict: only candidates with Gemma verdict and `llm_semantic_score >= 0.72`; weak/provisional material belongs only under a separate discovery heading.

Incremental contract:

- `scripts/sync_event_search_vectors_to_supabase.py` skips unchanged vectors independently for `search_v3` and `related_v1`; after the initial related-vector backfill, ordinary rebuilds should generate provider embeddings only for new/changed event documents.
- The related cache stores raw pgvector chains and Gemma verdicts keyed by event/candidate fingerprints and policy signature. If a new event appears, only anchors whose top candidate window changes, plus the new anchor, need new Gemma calls; unchanged anchor/candidate pairs are cache hits.
- The first v62 run is necessarily heavier than a steady-state rebuild because it changes the document kind/cache schema and has to validate anchors not present in the old v59/v61 cache.

Local preflight on 2026-06-30 before the Kaggle run:

- personalization Supabase size: about 25 MiB; `event_search_documents≈3.8 MiB`, `event_embeddings≈9.4 MiB`;
- vectors present: `search_v3=404`, `related_v1=343`;
- full sync from the v61 production snapshot processed 343 future events and created the remaining `related_v1` vectors with `293` embedding provider calls;
- after the sync, reruns skipped unchanged embeddings by kind;
- local Gemma cache preflight verified 20 anchors with valid JSON and reused cached verdicts on rerun; golden `6447` ordered `4759` then `6310`, while unrelated music false positives stayed out of strict similar.

The first Kaggle v62 run produced the expensive reusable related cache but ended with Kaggle `ERROR` before a compact archive/result could be accepted because the failed notebook left a large `node22` dependency tree in `/kaggle/working`. The recovered cache was then reused locally with `--pgvector-max-provider-calls 0` / `--gemma-related-max-anchors 0`: `343` anchors exported, `343` Gemma cache hits, `0` provider calls, `npm run check:preview` passed, and the preview tree was uploaded to `s3://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/`. The Kaggle kernel now cleans transient `node22`/extracted site paths on both success and failure while preserving recoverable outputs such as `event_related_chain_cache.json` and `events.sqlite`. The exporter also has a shrink guard so a 50-event canary cannot overwrite a larger expensive related cache unless `STATIC_SITE_ALLOW_RELATED_CACHE_SHRINK=1` is set.

v62 verification evidence on 2026-06-30:

- S3 listing confirms `1060` uploaded objects for the v62 prefix, including `343` event detail pages and `343` discovery JSON files;
- authenticated S3 listing confirms `__preview/index.html`, `/poisk/index.html`, golden event pages and stable ICS files exist in the bucket;
- public HTTP is currently blocked: `https://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/__preview/` returns `404`; `https://static.kenigevents.ru/ics/5878.ics` fails TLS validation because the CDN certificate is still for `*.yccdn.cloud.yandex.net`; bucket public-read policy and CDN certificate/domain binding must be fixed before user-facing review;
- built discovery JSON for `6447` has `algorithm_id=event_pgvector_related_chain_v2_two_doc`, `strategy=event_pgvector_related_chain_v2_manifest`, and only two strict related cards: `4759` (`llm_semantic_score=0.85`, `llm_confidence=0.95`) and `6310` (`0.75`, `0.90`);
- built discovery JSON for `5878` has music/retro/concert candidates first (`3398`, `5777`, `6488`, `6481`, `5733`);
- built discovery JSON for `5370` has art/exhibition candidates first (`6214`, `5969`, `6080`, `5391`);
- mocked browser smoke: `authorized_search_ui_smoke=ok`, result cards scroll and shared like/share/not-interested/calendar actions render;
- real Edge smoke: `authorized_search_real_edge_smoke=ok`, 12 cards rendered for `концерт классической музыки`, first event `5668`, scrolled event `5667`, quota text returned.

## v47 sparse terminology, related-order and CDN verification

`preview-20260628-event-pages-v47-sparse-fixes` is the historical sparse-baseline preview for the 2026-06-28 data slice. It was generated from the production SQLite snapshot with 70 real events and deployed to the `kenigevents.ru` bucket with CDN asset settings.

User-visible fixes included in this preview:

- the forbidden admission phrase `Платный вход` is no longer emitted by the exporter/runtime UI; paid/ticketed events without a reliable price render `По билетам`, while real price/range values are shown as the value and may be a nofollow ticket link;
- registration event `5077` keeps the expected `kgd80.ru/.../?register=1` registration CTA;
- related/feed cards show a compact event-type hashtag so a title without an explicit type is still understandable;
- source count and last update moved to the end of the event description/details block before the related feed;
- `Показать ещё` is shown only when there are eligible not-yet-rendered candidates and appends from the same static discovery JSON chain;
- the Pianissimo image regression (`5201`) is covered by an explicit valid image override.

Related-chain contract:

- generated manifests now use `schema_version=event_sparse_related_chain_v1`, `algorithm=event_sparse_related_chain_v1`, `retrieval_method=local_tfidf_sparse_v1`, `semantic_embeddings=false`;
- candidates carry `lexical_similarity`, mandatory `slot_type` and reason codes; legacy `event_vector_related_chain_v2` is compatibility-only for reading old artifacts;
- popularity/source likes do not boost candidates into `pure_related`;
- the 6447 golden-anchor regression is fixed in the sparse baseline: `Архитектурно-урбанистическая студия` ranks before `Музыка нашего города`. This is still a lexical/facet fix, not real semantic vector search.

CDN/ICS verification for v47:

- `scripts/migrate_static_media_to_cdn_bucket.py --active-on 2026-06-28 --apply` found 957 referenced legacy `/p/...` keys and 0 missing in bucket `kenigevents.ru`;
- deployed pages load `_astro/*` and rewritten event media from `https://static.kenigevents.ru`;
- stable calendar files are uploaded to `https://static.kenigevents.ru/ics/<event_id>.ics`;
- `npm run check:preview` and public Playwright regression passed (`artifacts/codex/static-site-builder/playwright-v47-public-check.cjs`).

Production caveat: Smart Update currently schedules/runs the Kaggle static-site builder artifact path. Automatic promotion of a checked Kaggle artifact to CDN/Object Storage is still a separate production gate; manual preview deploy verifies the bucket/CDN path but does not close the full Smart Update → CDN publication loop.

## Fixture coverage

The v43 preview uses 80 real production event rows exported read-only from the 2026-06-28 Fly SQLite snapshot under `artifacts/codex/static-site-builder/prod-db-20260628.sqlite` and committed as a bounded production-like static fixture. The export deliberately prioritizes the real same-day slice before long-running continuing events: `/segodnya/` now has 49 events starting on 2026-06-28 across 14 event types, so mobile QA is not limited to exhibitions.

- `5878` — «Песни СССР», paid sale, control slug `pesni-sssr-svetlogorsk-5878`;
- `698` — «Древние воины Янтарного края», multi-image fullscreen-gallery regression event;
- `6438` — «Водные битвы с аниматорами», same-day listing/card/hero QA event;
- free / registration with link;
- registration/source-only without direct ticket link;
- phone-only CTA;
- unknown/source-only CTA;
- long Russian title wrapping in main column;
- no local image hero fallback;
- weak/missing address fallback;
- static `/segodnya/`, `/zavtra/` and `/vyhodnye/` listing pages from the same fixture;
- related “Другие даты” pair `6437`/`6438`;
- one static neutral `Смотрите дальше` discovery feed; diversification is an internal ranking constraint, not a separate user-facing block;
- up to 10 preloaded discovery candidates in static HTML, plus a same-origin `/data/discovery/<event_id>.json` `event_detail_related` manifest (`schema_version=event-detail-related-v1`, `related_static[]`) for one automatic client hydration after JS applies a consented compatible local profile; further expansion is explicit through `Показать ещё`;
- explicit card reactions: like count + toggle like/unlike, “Не интересно”, local compact raw log/report for the current anonymous browser profile;
- honest like baseline: visible `likes_count` is `source_likes_count + service_likes_count`; `source_likes_count` is aggregated from available production TG/VK source-post metrics, while `service_likes_count` is the future first-party KenigEvents counter and remains `0` in this static preview; public HTML/UI shows only the total count; source/service split is technical and must not be rendered as copy or data attributes;
- detail-page calendar action links open `.ics` directly rather than forcing a
  download for every valid date/date range. If an event is free and has no
  registration CTA, `Добавить в календарь` becomes the primary CTA; otherwise
  calendar remains secondary to ticket/registration. Multi-day ranges export
  all-day inclusive public dates with exclusive RFC 5545 `DTEND`. Compact feed
  card utilities retain their stricter one-day eligibility.
- `image_text_mode` (`ocr_text` / `visual_only` / `unknown`) is a required export field. This preview does **not** run OCR during Astro build; it consumes the fixture value that must be produced by the existing media/OCR pipeline in production export. If this field is missing, the safe default is `unknown` → natural-ratio no-crop rendering.
- visible Russian dates omit the current year when both boundaries are in the build current year; cross-year ranges keep both years.
- `/segodnya/` and `/zavtra/` are grouped into `Утро / День / Вечер / Ночь`; no-time events fall into `День`. Mobile list cards use a compact plaque: cropped left photo column at parent-card level, straight separator to the text column, no direct outbound ticket/source links. This keeps production listings indexable and pushes external actions to detail pages where context and `rel` can be controlled.
- v43 keeps property-label polish, the taller mobile drawer tag, the split-card utility/action layout, hidden listing personal-feed slots, local `Все / Для меня` filter, CDN-aware `_astro/*` asset URLs and strengthened Open Graph metadata; temporary share lab controls are removed, the single production `Поделиться` action uses Web Share file + text + URL with generated 1080×1350 fallback, paid `price_label` chips can link directly to the ticket URL with `rel="nofollow"`, and the fullscreen gallery preloads/decodes adjacent slides before auto-advance to avoid black flicker.

No future active sold-out/cancelled/postponed event is intentionally showcased as a product state in this slice, so those optional states still need separate QA when the fixture/export includes reliable examples.

## Build size and timing evidence

Measured locally on 2026-06-28 with `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}`:

| Slice | Events | Static pages | Files | Output size | Build wall time | Max RSS |
|---|---:|---:|---:|---:|---:|---:|
| v43 focus preview | 80 | 95 | 261 | 28 MiB | 0:06.57 | ~387 MiB |
| full active snapshot estimate | 386 | 403 | 1185 | 128 MiB | 0:20.16 | ~522 MiB |

The full estimate was produced by exporting all active future/intersecting events from the same 2026-06-28 snapshot and building locally under a temporary `preview-20260628-event-pages-full-local-estimate` id. It excludes future media mirroring/CDN image transformations; bucket upload time will scale mostly with file count/bytes and is expected to dominate the Astro render time once full publication is enabled.

## SEO/GEO and preview safety

- All preview HTML has `meta name="robots" content="noindex,nofollow,noarchive"`.
- Prefix robots is exactly:

```text
User-agent: *
Disallow: /
```

- Preview canonical and `og:url` include `/preview-20260628-event-pages-v48-pgvector-gemma-kaggle/`; production canonical is not emitted by the preview build.
- Event pages render `schema.org/Event` / `MusicEvent` JSON-LD from visible event facts; for multi-image events, JSON-LD `image[]` includes the hero/gallery image assets even when the fullscreen gallery lazy-loads them after user action, so SEO/GEO crawlers can still tie the images to the event.
- The control `.ics` is a no-JS link and contains `DTSTART:20260711T193000Z`; it deliberately has no `DTEND` because reliable duration/end was not exported for event `5878`.


## Kaggle CPU builder / Smart Update handoff

The static-site production build path now has a Kaggle CPU runner that reuses the existing events-bot Kaggle infrastructure instead of inventing a separate execution path:

- production and owner Review Preview use separate private Kaggle execution
  slugs but the same runner, exporter, kernel source, artifact checks and
  publisher; the runner changes only the isolated staged kernel identity;
- for a first-time Review Preview slug bootstrap, both staged `id` and `title`
  are bound to that slug because Kaggle derives a new kernel URL slug from its
  title. The repository metadata remains the production identity.

```bash
python scripts/run_static_site_builder_kaggle.py \
  --db /data/db.sqlite \
  --status-db /data/db.sqlite \
  --status-callback-url https://events-bot-new-wngqia.fly.dev/internal/kaggle/run-event \
  --limit 50 \
  --current-date YYYY-MM-DD \
  --build-id preview-YYYYMMDDHHMM-event-pages-prod50-kaggle \
  --download-output
```

Contract:

- input data is a unique per-run private Kaggle dataset, matching the CherryFlash/session-dataset pattern;
- the site source is uploaded as `site_source.tarball` (gzip tar content with a neutral extension) because Kaggle dataset ingestion auto-extracts `.tar.gz` and can break Astro dynamic route filenames;
- the bundle also carries the exact repo-level
  `docs/testing/transport-fault-profiles.v1.yml` contract resolved by preview
  scripts as a sibling of `site/`; staging fails closed if it is missing;
- preview provenance uses the runner-bound full `STATIC_SITE_REPO_SHA`; the
  Kaggle archive is not treated as a Git checkout and cannot silently emit a
  synthetic or short SHA;
- the kernel extracts the site to `/tmp/kenigevents-static-site`, not read-only `/kaggle/src`;
- Kaggle CPU currently provides Node 20, while Astro 6 requires Node `>=22.12.0`, so the kernel installs local `node@22.12.0` before build/check;
- output is intentionally minimal: `<build_id>.tar.gz`, `static_site_build_result.json`, and the kernel log; `node_modules` is not left under `/kaggle/working`;
- when `--status-db` and callback URL are provided, the launcher creates `kaggle_run.json` via `create_kaggle_run_config(...)`, uploads a status dataset via `create_kaggle_status_dataset(...)`, and adds it to `dataset_sources`; the kernel explicitly discovers `kaggle_status_client.py` below the mounted `/kaggle/input` tree (dataset sources are not automatically on `sys.path`) and emits `kernel_started`, `preflight_ok`, `alive` progress, and `report_written`;
- after the host downloads and cryptographically validates the immutable result,
  it reconciles a non-terminal ledger to a distinct
  `host_result_validated/done` event. This is a delivery fail-safe, not a fake
  heartbeat: a healthy status-aware run must still contain the real kernel
  callbacks and `static_site:builder` lease lifecycle;
- the resource lease key is `static_site:builder`, so a production status-aware run can block parallel static-site builds.

Verified artifact on 2026-06-28: `preview-20260628-event-pages-prod50-kaggle-v44` built 50 real production-snapshot events on Kaggle CPU and passed `npm run check:preview`. This was a local manual run without production callback env, so status dataset creation was intentionally skipped; the production outbox path must pass `/data/db.sqlite` and the Fly callback to make it visible in `kaggle_run_ledger`/poller.

Outbox integration: `schedule_event_update_tasks(...)` enqueues a coalesced
`JobTask.static_site_build` with key `static_site_build:prod` for 15 minutes
after the last Smart Update when `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`. The
handler launches the runner with `/data/db.sqlite`, status DB/callback,
`--download-output`, CDN asset/ICS base envs, browser-safe AuthorizedEventSearch
public envs, and the configured related mode. Production pgvector uses
`STATIC_SITE_RELATED_MODE=pgvector`, `ENABLE_EVENT_VECTOR_SYNC=1`,
`STATIC_SITE_REQUIRE_VECTOR_BARRIER=1` and
`STATIC_SITE_SYNC_PGVECTOR_VECTORS=0`: the Fly projection receipt must be
current before Kaggle reads compact related candidates. The Kaggle sync switch
is manual canary/backfill only. For focus-group builds that should show Yandex
login/search, also set the public Supabase URL/key and `custom:yandex`; only the
URL and publishable key are exposed to the browser. Full page rebuilds are for
content/lifecycle changes, not every counter tick.

## Local diagnostics and canonical publication

```bash
cd site
npm install
PREVIEW_BUILD_ID=preview-20260628-event-pages-v48-pgvector-gemma-kaggle PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' npm run build:preview
PREVIEW_BUILD_ID=preview-20260628-event-pages-v48-pgvector-gemma-kaggle PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' npm run check:preview
```

These local commands are diagnostics only. They cannot be published. Canonical
preview generation/publication uses the Kaggle runner with
`--download-output --publish-preview`; page-family speedups use repeatable
`--page-class`. The single rail, exact slug mapping and command are maintained
in [`docs/operations/kaggle-static-site-builder.md`](../../operations/kaggle-static-site-builder.md#single-build-and-publish-rail-decision-2026-09-03).

## Visual review passes

The first public preview (`preview-20260627-event-pages-v1`) was superseded after visual review. v43/v47 keep the event page mobile/feed-oriented and feedback-aware, and replaces the v19 safe image block with the v20 hero composition lab from `event-hero-lab-2026-06-27.md`:

The feed-card A/B has been resolved for normal event pages: `split-actions` is now the baseline for all event detail discovery feeds. The old overlay variant remains documented only as a rejected/historical comparison in `event-card-ui-ab-2026-06-27.md`.

- recommendation cards now have large image-led feed cards instead of text-only cards;
- event hero keeps deterministic media modes (`poster-stage` for OCR/unknown, `photo-cover` for verified `visual_only`, `fallback-art` for no image), but now adds explicit composition variants (`poster-billboard`, `poster-attached-card`, `photo-cinematic-sheet`, `photo-parallax-sheet`, `compact-ticketing`); mobile hero visual breaks out to 100vw where appropriate, H1/CTA remain HTML in a decision sheet, OCR/unknown posters are not cropped, and visual-only images may use cover. Non-rail detail/recommendation cards keep the OCR-safe v15 rule: `visual_only` cover/crops inside a strict vertical 4:5 frame; `ocr_text|unknown` renders the actual image at natural aspect ratio with no crop, no fixed cover frame, no duplicate/backdrop underlay and no blur fill. R15 mobile listing rails instead use the horizontal fail-closed contract in [`image-framing.md`](image-framing.md);
- duplicated facts/source/debug notes were removed from the first screen;
- the long description is visible HTML, followed by a compact icon facts block; public source count/views and source links are hidden until auth exists, with a temporary notice that sources, mentions and extended statistics will be available to registered users;
- native mobile share is attempted by one visible `Поделиться` button; duplicate Telegram/VK/WhatsApp share pills were removed, and fallback copies the URL when system share is unavailable.
- footer social navigation mirrors the Telegraph editorial footer and adds Max: Telegram `@kenigevents` + `@kldevents`, VK `kenigeventsofficial` + `klgdevents` + `vk.ru/im/channels/-239844596`, and `max.ru/channel_kenigevents`; site footer uses visible Telegram/VK/Max SVG icons, while Telegraph remains plain links only.
- a favicon is emitted from the two-color calendar/heart/church SVG (`site/public/favicon.svg`) so browser/share surfaces have a site icon;
- footer exposes compact social navigation and `mailto:info@kenigevents.ru`.


After direct product review, `preview-20260628-event-pages-v43` rolls back the UX regressions introduced by the split recommendation rails and adds the first static-seed/client-hydration discovery contract:

- event description is visible HTML again, not hidden behind a collapsed `<details>` block;
- event continuation is one vertical mobile-first discovery feed (`Смотрите дальше`), not two horizontal scroll blocks and not a visible “try something else” module;
- the first continuation surface is static-first: the generated HTML contains up to 10 candidates; after JS activation, only a consented compatible local profile (`ke_personalization_profile`, UUID ids, `event-detail-related-v1` / `event-taxonomy-v1`) may hide/rerank preloaded cards by `hidden_event_ids`, `not_interested_event_ids` and strong `negative_interest_tags`, then the page performs one lightweight same-origin fetch to `/data/discovery/<event_id>.json` and top-ups relevant candidates; after that, more cards are loaded only when the user presses `Показать ещё`;
- desktop keeps the same continuation content as a grid, matching desktop expectations instead of mobile horizontal rails;
- `Поделиться` is visible always: it calls `navigator.share()` when the browser/webview supports native system share and falls back to copying the URL when native share is unavailable.
- diversity/anti-bubble is only a ranking/composition rule inside `Смотрите дальше`; the UI does not label cards as “Попробовать другое” or “Открыть новое”.
- explicit likes are the strongest positive signal: after consent the preview stores a DB-compatible anonymous browser profile in `ke_personalization_profile` and compact local strong-action records in `ke_event_feedback_log_v1`; likes/unlikes update `liked_event_ids` and `positive_tags`, while visible counts increment only for the current visitor.
- “Не интересно” is the explicit negative signal; the preview dims and demotes the card instead of inventing a visible anti-bubble block.
- the bottom sticky CTA is hidden after the discovery feed enters the viewport.
- same-origin event links have lightweight prefetch markers so static page transitions can warm the next HTML document.
- media rendering consumes the same `image_text_mode` export but differs by surface: hero uses `poster-stage` for OCR/unknown and `photo-cover` for `visual_only`; non-rail detail/recommendation cards use `visual_only` cover in a vertical 4:5 frame and `ocr_text|unknown` natural aspect ratio, not `contain` inside a fixed card frame. R15 mobile listing rails are the separate horizontal `5:4` rule in [`image-framing.md`](image-framing.md). Duplicate same-poster underlays, blurred fills, repeated edges and OCR crop are forbidden.
- The share action uses a VK-like outlined repost/share arrow adapted from `@vkontakte/icons` `Icon24ShareOutline` (MIT), accessible `Поделиться` label and share count when count is positive. Zero like/share counts are not rendered as `0`. After a successful like the share action is highlighted instead of showing a floating bubble. Variant A keeps one overlay row with `Не интересно`, share and like; Variant B moves share/like under the card as transparent icon actions and may keep `В календарь` as an inside-card utility only for one-day events. The old explicit `Открыть` card button is removed because media/title links plus full-card JS navigation preserve crawlable SEO/GEO links while reducing UI noise.
- Calendar remains available on the event detail page / primary transaction
  block for a valid date or range. In the feed it is absent from Variant A;
  Variant B may show it as an inside-card utility for one-day/calendar-eligible
  events only.
- The like button shows only the total like count. The source/service split is kept in the fixture/DB for consistency and audit, but is not rendered into the public page.
- single tap/click on a non-interactive part of a card navigates to the event detail page immediately. Double-tap like is intentionally removed because it raced with navigation and could not be made reliable without harming SEO/GEO-friendly full-card navigation; likes are explicit button actions.
- marking `Не интересно` turns the current card into a grey explanatory plate (`Вы пометили: не интересует`) with an explicit `Отменить` button; tapping the plate itself does not navigate, and later personalization may remove/demote similar events on subsequent surfaces.
- explicit-feedback rerank is viewport-stable: after a user action, the acted-on card and all cards above it keep their positions; only cards below the action anchor may be re-ordered.
- same-year visible dates omit the year (`11 июля · 21:30`), while cross-year ranges keep the year on both sides (`12 июня 2026 — до 28 марта 2027`).

After consultant review, `preview-20260628-event-pages-v43` additionally hardens the first discovery layer:

- header links now open real static `/segodnya/`, `/zavtra/` and `/vyhodnye/` pages, not QA anchors;
- related cards use a no-nested-anchor poster-card component with mandatory image/generated visual slot and direct page link; `.ics` calendar action is deliberately kept on the detail page, not in feed cards;
- `6437`/`6438` same-occurrence duplicates are excluded from “Похожие события” and remain only in “Другие даты”;
- source-only paid/ticketed events use honest `По билетам` copy instead of the forbidden generic phrase `Платный вход` or implying direct purchase;
- weak-address pages do not show “Открыть на карте”;
- raw markdown/facts artifacts, hashtags in venue names, `null`/`undefined`/`NaN`, sitemap entries and all event `.ics` files are covered by `npm run check:preview`.



### v37 product/UI corrections

`preview-20260628-event-pages-v43` adds the current product corrections:

- public event pages show only an auth-gate notice for sources, mentions and extended stats; actual source lists/statistics are not rendered until registered-user access exists;
- the event page now has one product-level facts block, not two: hero keeps only a compact date/place meta line, while `Коротко` is the single icon fact block with date, venue+address, entry/status, optional Pushkin card/festival;
- detail CTA hierarchy supports calendar-as-primary only for one-day free/no-purchase events, while paid/registration events keep ticket/registration primary and calendar secondary when eligible;
- hero gallery has an on-image transparent `Фото N` CTA, lazy-loads gallery slides from `data-gallery-src` after open/navigation, shows the service tag in fullscreen, makes visual-only fullscreen photos cover 100% viewport height with no side fields, auto-pans right-to-left and then advances to the next photo, pauses forward auto-advance after a manual backward swipe, keeps OCR/text images in contain, and JSON-LD `image[]` lists the gallery assets for SEO/GEO;
- the mobile top drawer is now one monolithic sliding object: rail and handle move together with no visible gap; the closed handle remains visible after scrolling, instead of disappearing;
- the sources/mentions registered-user notice moved out of `Коротко` and is rendered as the bottom strip of the parent details section, so it no longer visually belongs to the compact facts block;
- event `5370` is a documented fixture override: production currently marks the long-running exhibition «Точка и линия» free because a free curator round-table source was merged into it. The v40 fixture renders it as paid/ticketed for preview correctness, while production DB repair remains a separate source-of-truth task.

### v44 CDN media/ICS and Kaggle-published preview

`preview-20260628-event-pages-v44-cdn-kaggle` is the first public preview built by the Kaggle StaticSiteBuilder after enabling the media CDN path. The run used the 2026-06-28 production SQLite snapshot, exported 80 real active events, passed `npm run check:preview` inside Kaggle, downloaded `static_site_build_result.json` + tar.gz, and was then deployed to Yandex Object Storage bucket `kenigevents.ru`. Before the build, legacy active media keys from `s3://kenigevents/p/...` were mirrored into `s3://kenigevents.ru/p/...`; verification found `957` needed active keys and `0` missing in the target bucket. Deploy also uploaded `80` stable calendar files to `s3://kenigevents.ru/ics/<event_id>.ics`.

Local post-deploy checks on 2026-06-28:

- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/__preview/` → `200`;
- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/segodnya/` → `200`;
- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sobytiya/pesni-sssr-svetlogorsk-5878/` → `200`;
- `https://static.kenigevents.ru/ics/5878.ics` → `200`, `content-type: text/calendar; charset=utf-8`;
- sample CDN poster `https://static.kenigevents.ru/p/...webp` → `200`;
- control event HTML contains CDN `/p/...` media and stable `/ics/5878.ics`, and does not contain old raw `storage.yandexcloud.net/kenigevents/` image URLs.

### v43 share, carousel, price-link and today-fixture corrections

`preview-20260628-event-pages-v43` closes the share experiment. Rich/markdown hidden links are not a Web Share capability, so temporary experiment buttons are removed. The main `Поделиться` action remains the tested production path: image file + plain text + separate URL, with generated 1080×1350 PNG fallback and text/URL copy fallback. Fullscreen visual-photo gallery pan is slowed by ~40% (`17.9s`) while auto-advance now fires after a shorter `8.88s` interval, keeping the slower leftward motion but removing the dead wait. Paid events with missing price must not render `Билеты` as an admission value; they show `Цена уточняется`/`По билетам`, while real `price_label` values render the exact price or range and are also reflected in JSON-LD offers when possible.

v43 adds two user-visible fixes on top: the event-page mobile brand tag now wraps exactly like the fullscreen-gallery tag (`Полюбить / Калининград` on the kicker lines instead of clipping the text), and gallery auto-advance preloads/decodes the next image slides before moving to them so the transition does not begin over a black empty slide. If a paid event has a real price/range and a ticket URL, the price chip in the compact `Вход` fact and CTA panel may itself be the ticket link (`rel="noopener noreferrer nofollow"`, `data-nosnippet`) instead of adding another noisy label. The production export now selects exact same-day events first, then upcoming short events, then continuing long-running events, so `/segodnya/` is diverse and testable.

Local build verification (`npm run build:preview` + `npm run check:preview`) passed for v43; the current public focus preview is v47.

## v16/v17 personalization-contract correction + v18 UI A/B + v20 hero composition lab

`preview-20260628-event-pages-v43` keeps the discovery implementation aligned with the documented `event_detail_related` contract:

- `/data/discovery/<event_id>.json` now returns `schema_version`, `feature_schema_version`, `taxonomy_version`, `surface`, `algorithm_id`, `current_event` and `related_static[]` candidates with `category`, `tags`, `audience_exclusion_tags`, `base_similarity`, `reason_codes` and nested display data.
- Static HTML still preloads up to 10 cards; production target is 10 when enough eligible future events exist.
- Without consent or without a compatible profile, the static order remains the fallback and no profile is created.
- With consent and a compatible profile, browser JS runs the local `rankEventDetailRelated` formula: static related similarity remains dominant, explicit likes boost, `hidden_event_ids`/`not_interested_event_ids` hard-filter, strong `negative_interest_tags` remove unsuitable cards, and one same-origin JSON top-up restores the visible pool before the `Показать ещё` button takes over.
- Browser strong actions carry `served_list_id` / `served_list_hash` in the compact local log, matching the future Supabase `personalization_served_list_summary` write path.

## Verified on 2026-06-29

`npm run check:preview` passed inside the Kaggle CPU run for `preview-20260628-event-pages-v48-pgvector-gemma-kaggle`. The check covers the normal event-page/static contracts from v47 plus the pgvector discovery manifest contract: `algorithm_id=event_pgvector_related_chain_v1`, `strategy=event_pgvector_related_chain_v1_manifest`, `related_static[]`, mandatory `slot_type`, and at least one candidate carrying `vector_similarity`.

Live public smoke additionally verified HTTP 200 for the preview index and the golden anchor JSON `data/discovery/6447.json`; that JSON returns `6310` as the first related candidate with `vector_similarity≈0.8592` and `llm_semantic_score=0.92`. Like/profile writes remain local-only preview behavior; authorized search UI source is present but live search remains gated on Yandex OAuth and Edge Function deployment.


## Verified on 2026-07-01

Recovery preview `preview-20260701t2341-recovery-full` combines the restored full static-site branch work with the `feature/smart-search-quota-key5-site` authorized-search quota/key rollout. It was exported from the refreshed production SQLite snapshot for `2026-07-01`, generated `380` public events, passed `npm run build:preview` and `npm run check:preview`, and was deployed to `https://kenigevents.ru/preview-20260701t2341-recovery-full/__preview/` with `380` stable CDN ICS files. The refresh includes the late production events `6563`, `6564` and `6565`, verified by public HTTP checks against their generated event pages.

The export includes a narrow prompt-leak publication guard: rows whose title is obvious prompt/debug leakage are skipped before static pages and search fixtures are built. This is only a stopgap for preview/publication safety; the canonical production row still needs source/Smart Update repair. The recovery run used this guard to exclude event `6518` from the static preview and removed its stale `event_search_documents` / `event_embeddings` personalization rows.

Live search/auth verification for the same build passed through the deployed Supabase `event-search` Edge Function. The browser smoke on `/poisk/` queried `интересно детям`, rendered `18` cards, and the latest audit row recorded `request_kind=llm_rerank`, `status=ok`, `result_count=12`, `embedding_model=gemini-embedding-2`, `embedding_key_env=GOOGLE_API_KEY5`, `llm_model=gemini-3.1-flash-lite`, `llm_policy=lite_first_gemma_overflow`, and first Lite attempt key `GOOGLE_API_KEY3`. This confirms the recovered branch is not running on a single KEY5 lane.

## Mobile acceptance R6 — 2026-07-23

The immutable noindex review build is published at
<https://kenigevents.ru/preview-20260723-unified-corrections-r6/>. Its complete
mutually linked page-type map is
<https://kenigevents.ru/preview-20260723-unified-corrections-r6/__preview/>.
Production was not changed.

R6 removes the desktop leather tag's black subpixel side rails without clipping
its stitched dimensional edge, unifies mobile club cards with the accepted
desktop media-overlay composition, removes event breadcrumbs on mobile, keeps a
single primary ticket action, labels a wide Share action, restores the dedicated
free-admission medallion, and preserves real glass transparency in the mobile
menu. The footer PWA action is Android-only and remains hidden until the browser
provides a real one-shot `beforeinstallprompt`; it clears after prompting or
`appinstalled`.

Release evidence:

- deployed source SHA: `da92ab4a`;
- Astro export: `389` pages, including `288` event pages;
- full static-site tests: `174/174` passed;
- `check:preview` and `check:unified-prototype` passed;
- public Chromium acceptance passed at mobile `390px` and desktop DPR 1/2 with
  no horizontal overflow or broken images on the checked routes;
- agy `gemini-3.1-pro-high` acceptance verdict: `GO`, `PASS 8/8`;
- the full route catalog was delivered and read back successfully in Telegram
  topic `KenigEvents · UI review`, topic anchor `548`, message `616`.

Related/discovery data in this preview uses the two-document pgvector chain `event_pgvector_related_chain_v2_two_doc` with `embedding_document_version=related_v1`. Gemma strict related verification was not rerun for this full end-of-day refresh, so `strict_verified_related=false`; event pages still read the static related JSON and do not spend online embedding/LLM quota on page view.

## Counter freshness plan

Counter freshness is documented in [Event reaction counters](reaction-counters.md). The decision is manifest-first: static HTML keeps a build-time baseline for SEO/no-JS, while a small same-origin counter manifest should patch counters after first paint. Full page rebuilds are for event content/lifecycle changes, not for every like tick.

## Historical candidate from 2026-07-17 — `TODAY-HOUR A · АФИШНЫЙ ПОТОК · V9`

Public desktop preview:
<https://kenigevents.ru/preview-20260717-today-hour-flow-v9/segodnya/>.

The page was exported from a fresh read-only production Fly SQLite snapshot for
2026-07-17. The preview contains 220 real active events overall; `/segodnya/`
renders 17 unique same-day cards after one exact title/time/venue duplicate is
removed. Events are grouped by exact start time (`18:00` and `18:30` remain
separate). Each group uses intrinsic-width `flex-wrap` rather than an equal
column grid, horizontal scroller or stretched row. Long-running exhibitions do
not enter the primary Today flow, while already-finished events remain available
inside the collapsed `Ранее сегодня` section.

The candidate location overlay is deliberately fail-closed. A bottom-right
medallion is rendered only on a curated venue match whose primary image is a
semantically classified `event_photo`/`photo` and is `visual_only` at both event
and asset levels. `safe_crop` is not an OCR proxy. In this real-data build only
event `4783`, «Мюзикл „Алые паруса“», passed the gate; short-text/poster-like
images were rejected.

Post-deploy browser QA passed at `1920×1080` and `1440×1000`: 17 visible cards,
9 time groups, 10 distinct rendered card widths, `display:flex`,
`flex-wrap:wrap`, card `flex-grow:0`, no horizontal overflow, no broken or
pending images after a full lazy-load scroll, and exactly one eligible venue
medallion. `npm run check:preview` passed for the 242-page/220-event build.

V9 is retained as research evidence, not as an approved listing system. It had
page-local geometry and covered only Today; the shared V10 system below replaces
it for product review.

## Product candidate on 2026-07-17 — `DATE-LISTING TH-P1 · V10`

Immutable review routes:

- <https://kenigevents.ru/preview-20260717-date-listings-v10/segodnya/>;
- <https://kenigevents.ru/preview-20260717-date-listings-v10/zavtra/>;
- <https://kenigevents.ru/preview-20260717-date-listings-v10/vyhodnye/>;
- <https://kenigevents.ru/preview-20260717-date-listings-v10/lab/design-system/>.

V10 implements one desktop-first runtime family for `/segodnya/`, `/zavtra/`
and `/vyhodnye/` from the global static-site design system. It does not create a
page-specific design system. Shared tokens and candidate components live in
`site/src/styles/design-system.css` and `site/src/components/listings/`, are
rendered in `/lab/design-system/`, and are checked by both design-system and
preview contracts.

Product contract:

- exact start times remain the content structure: all events at `18:00` fill
  the available width and wrap to the next line; `18:30` is a separate group;
- the sticky primary navigation is hybrid: `Утро / День / Вечер / Ночь / Без
  времени` with live result counts; an exact-time disclosure appears only for a
  dense period (at least five distinct time groups), so a long weekend remains
  navigable without turning every hour into permanent chrome;
- Today keeps starts before the current Kaliningrad time in one collapsed
  `Начались ранее` block above a visible `Сейчас · HH:MM` marker; a sparse
  upcoming remainder of one to four events receives weighted centering;
- Weekend uses one combined period navigation and two independent day columns.
  Exact-time disclosures label links by day (`Сб 19:00`, `Вс 19:00`), while day
  headers remain visible beneath the sticky time bar;
- city filtering is one compact multi-select disclosure rather than a permanent
  row of ten chips. The full list is the default; the `Для меня / Полный список`
  v2 switch is exposed only when a compatible consented profile produces a real
  difference. All page, period, exact-time, earlier and day counts update after
  filters, and an empty intersection has an explicit reset action;
- long-running exhibitions are excluded from primary date flows, while genuine
  short multi-day festivals starting on the chosen date are not discarded;
  the upstream primary `event_type` is authoritative when present, so a
  secondary `EXHIBITIONS` topic in a mixed festival/concert programme cannot
  silently remove that event from its date rail. The exact exhibition topic is
  used only as a fail-closed fallback when the primary type is absent; titles
  are never reclassified by a render-time keyword rule;
- media selection first prefers a classified wide identity poster (`1.85–2.10`)
  with adequate resolution. Square/wide classified event photos are preferred;
  a crop requires `safe_crop` and focal metadata. Near-wide photos may become
  `3:2`, while a vertical fallback is normalized only to the minimum `4:5`.
  Unknown/poster-like material stays natural and is never stretched or cropped
  merely to fill a card;
- a venue medallion remains a fail-closed photo-only candidate and is evaluated
  against the selected listing asset, not blindly against the first asset;
- the browser keeps the navigation structure stable after filtering: zero-count
  periods become disabled, while zero-count exact-time rows disappear from the
  disclosure. Native anchors, focus transfer and Escape/outside-click closing
  preserve keyboard behavior.

The real-data preview snapshot contains the confirmed `4671/6859` «Эпидемия»
recurrence. V10 removes `6859` only in an auditable preview reconciliation copy
and selects the classified `638×316` identity poster for canonical event `4671`.
Production remediation still belongs to the LLM-first merge flow under
`INC-2026-05-30-active-duplicate-events-recall-gate`; no title regex or hidden
UI mapping was added.

### V10 publication and browser evidence

- published artifact source: branch
  `integration/listing-time-nav-media-v10-20260717`, SHA
  `c29e370486df96a7aff9d1bb5c79993777d0cb8e`;
- checked export/build: `220` real events, `243` generated pages;
  `check:design-system` passed with `20` core tokens, `26` versioned registry
  rows and `8` AA pairs; `check:preview` passed;
- public HTTP returned `200` for Today, Tomorrow, Weekend and the DS catalog;
  the immutable CDN stylesheet returned `200` with one-year immutable caching,
  and the served TLS certificate SAN contains `static.kenigevents.ru`;
- fresh Chromium checks at `1366×1080` and `1920×1080` found no horizontal
  overflow, broken images, console errors or failed first-party requests;
- rendered inventory: Today `17` (`15` earlier, collapsed), Tomorrow `36`,
  Weekend `61` (`36 + 25`); Tomorrow and Weekend expose two dense exact-time
  disclosures each;
- city intersection smoke changed Tomorrow from `36` to `23`, updated the page
  count and retained `scrollWidth=viewport`; exact disclosure closes on Escape;
- canonical «Эпидемия» `4671` renders once as `poster-natural` at ratio
  `2.01899:1` (`351×174` at 1920); `6859` renders zero times;
- labeled visual evidence and the QA summary were delivered to Telegram forum
  topic `KenigEvents · UI review` / `Главная, Популярное, списки — wireframes`,
  message ids `273–278` (topic anchor `122`).

## R12 current-clock preview gate, 2026-07-26

`check:preview` reads `currentDate` and `referenceIso` from the generated
`preview-build.json`, not from stale snapshot metadata. Lifecycle assertions
therefore agree with the pages produced by `build:preview`: an already-ended
Break Summer event is forbidden from Popular after its date, while it remains
required when the same gate runs against an earlier eligible build clock.

Generated-output canaries also respect the generated calendar window. The
executable no-band Pianissimo canary follows upcoming event `5297` on
`date-2026-07-30`; it checks the full rail DOM, horizontal `140×112` cover,
`visual_only`, safe-crop geometry and focal point. A past one-off event is not
used as a required source row: production export is allowed to remove expired
dates, so an obsolete July 24 occurrence cannot break a later valid candidate.
After `5297` itself leaves the generated catalog, the gate does not require the
expired row to reappear. It instead checks a current generated date-rail
`visual_only` specimen for the same `140×112` cover/no-band DOM and runs the
rail resolver against the immutable two-photo `5297` geometry, including its
`65% 35%` focal point. Thus both the current output and the exact historical
regression remain executable without making catalog retention part of the UI
contract.

The same lifecycle rule applies to transport specimens. Romanovo event `6710`
is historical acceptance evidence, not a permanent catalog fixture.
`check:preview` validates the preferred Северный-вокзал boarding UI on a
currently eligible Romanovo event when one exists; the official route-119
terminal times/provenance and focused transport tests remain mandatory even
when no current public event uses that bus route.

## Immutable candidate recovery after publication

Secret-candidate publication is create-only under its random `_review/<token>/`
prefix. A recovery pass never overwrites an object. When the first pass has
already uploaded an object but a concurrent production SQLite writer prevents
the host from persisting the final receipt, the exact retry may adopt that
object only after the normal manifest-bound download verification confirms its
size, SHA-256 and MIME type. Any other S3 error remains terminal/retryable under
the existing failure classifier.

The outbox receipt update itself uses four fresh ORM sessions with bounded
backoff, in addition to SQLite's configured busy timeout. This keeps a
many-minute, already-verified publication from being repeated merely because
one Smart Update transaction temporarily owned the single SQLite writer. The
claim/current-candidate commit remains the authority; root aliases and stable
ICS keys are still inexpressible from this publisher.

Production and secret-candidate Free-collection gates inspect event ids only
inside `<main data-free-collection-surface>`. `EventLayout` deliberately ships
hidden canonical `EventCard` templates after the page slot for client-side
hydration; those templates are not collection results and must not be
misclassified as non-free cards merely because their current fixture is paid.
The gate still validates every actual regular and exhibition card rendered by
the collection.

Local real-data focused runs pass the frozen DB as the canonical exporter `--db` plus snapshot identity/hash/size and clock. Page-class and exact-route filtering belong to the existing Astro adapter, not unsupported exporter flags. The CLI contract regression is `site/scripts/local-focused-preview.behavior.test.mjs`.

### Owner-review packed/runtime regression (2026-09-05)

Packed related cards retain SSR order until the deferred geometry owner is ready;
its readiness signal reapplies ranking and row layout together. Packed media
links reserve the shared row aspect ratio independently of flow subgrid tracks.
The browser gate rejects both unequal internal boundaries and equal but collapsed
media/body anatomy. Exhibitions inline scripts are parsed as plain JavaScript
in regression tests: TypeScript assertions in `is:inline` disable all interaction.
These source tests are not independent review or evidence of a published candidate.

### Free collection owner correction (2026-09-05, #621/5550253003)

`/podborki/besplatnye-sobytiya/` is one ordinary `standard-free-listing`, not
a timed/exhibition landing. Its complete confirmed-free pool reuses the shared
catalog mapper, embeds a typed inline manifest, initially renders 12 cards and
uses the existing EventLayout discovery controller for personalization and
loading. The hard free constraint applies before ranking, appending and profile
visibility; it never admits unknown/paid entries through fallback. Count metadata
distinguishes loaded from total with Russian forms. The source-bound structural
projection labels five inspected entries as a sample, not the catalog total.
This corrects the owner-rejected composition; historical two-group proofs are
not acceptance evidence for the new page. Production is not promoted.

### Immutable Kaggle SQLite input (2026-09-05)

Remote export passes `--db-immutable` only after snapshot identity/hash validation.
SQLite `mode=ro` alone can still require WAL/SHM sidecars on a WAL-format database;
`immutable=1` reads the validated, self-contained file on Kaggle's read-only mount.
The default local/live database path remains ordinary read-only and never infers
immutability. A nonempty adjacent WAL rejects immutable mode. This avoids copying
private DB data into published Kaggle outputs. See SQLite's documented contract:
https://www.sqlite.org/wal.html#read_only_databases .
The d1cc5c7d3 review run failed at this read before Astro generation; no artifact
was published. Its snapshot bytes and UI are retained for the tooling successor.

Remote export also carries the two public Interest Club release gates through
`interest_club_build_env` in the existing input config, independently of provider
secret bundles. Exporter and Astro consume the same allowlisted values; explicit
`0` remains a rollback. Sparse/no-provider builds must not silently disable the
confirmed-club projection. The nonempty real-preview artifact gate is unchanged.

The owner-review inverse related-card action rule crosses the child Astro scope
inside its unique desktop root: a className on AdaptiveEventCardGrid does not
inherit DesktopEventPage's scope attribute. Real CDN no-JS Home checks wait for
stylesheet load before asserting first-scene visibility; DOMContentLoaded alone
can count all unstyled scenes. This is a readiness correction, not an exemption.

The ordinary page-title browser packet includes Search, Favorites, Partners and
Partnership as well as the original twelve routes. Favorites and Partners no
longer retain a separate local H1 scale; standalone Search uses the shared
compact role at the same mobile breakpoint. Entity/card/editorial/form headings
keep their distinct semantic roles; no title copy or route behavior is changed.

### Owner follow-up on2fe28b1f8 (2026-09-05)

The two full transcripts in #621/5551113067 reopen CODE correction, not only
independent REVIEW/MATERIALS. REL-053 and DS1.14.1 jointly require loaded painted
bounds without fields, protected text, preserved admitted IDs and row geometry;
protective contain inside an arbitrary fixed5:4 shell is not accepted output.
Card title/metadata/status/action weights now have shared semantic owners
(700/500/600/600), with page H1 roles unchanged. The existing CSS fallback font
stack is retained: measured Linux Chromium used DejaVu Sans Bold, not a loaded
Inter asset; CSS family text alone is not font-delivery evidence. Listing and
ordinary EventCard titles consume the same card-title role.
The existing lower-stack controller exposes read-only clipped occupied rectangles
through `KenigEventsShellOccupiedSpace()` and its existing state event; separate
islands are not merged into a fictitious opaque rectangle. This adapter does
not send analytics, alter served-list identity/order or create a transport.
Primary action receipts remain independent of optional disposable telemetry;
existing transport failure isolation checks are retained. Free mobile's in-flow
page medallion is doubled without scaling all medallions or covering card text.
The next same-corpus Kaggle handoff must use the product Home URL as its primary
link. Source changes/test packets are not deployed runtime or independent acceptance.

Home no longer adds a second page-local top gap before Hero Talk; existing shell/brand clearance remains the shell owner. This is not a new Hero or palette.

Shell follow-up fixes scope prelaunch document styles to the actual prelaunch root, preserve the full brand while page context is active, restore desktop Today and remove only the rejected mobile All collections link. The shared bottom island remains on desktop and mobile; its terminal clearance is inside SiteFooter, not blank body padding. Browser regression must use real wheel/menu/Back input, not scrollTo alone.
