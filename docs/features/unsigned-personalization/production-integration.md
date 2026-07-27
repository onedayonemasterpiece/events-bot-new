# Thin Runtime Architectural Gate for Static Personalization

> **Status:** engineering-spike gate after external review; **canary and production are blocked** until every P0/P1 gate below has executable evidence.
>
> Scope: event pages on `kenigevents.ru`, first surface `event_detail_related`, later mobile discovery feed and desktop grid/modules.

## Decision summary

The production plan is intentionally constrained for the current infrastructure:

```text
Fly web runtime = static/API minimum only
batch/worker/CI = Smart Update exports, static builds, LLM, ML, embeddings, bulk aggregation
Supabase/Postgres = compact accepted personalization telemetry + aggregates only
Fly SQLite = canonical events, promo state, Smart Update/static rebuild state
```

Static-site deployment is also fixed for the production integration:

```text
static HTML/CSS/JS/manifests/sitemap/robots -> s3://kenigevents.ru/
public canonical origin -> https://kenigevents.ru/
preview prefixes -> s3://kenigevents.ru/preview-<timestamp>-<random>/
Fly/local secret prefix -> KENIGEVENTS_SITE_*
```

Do not reuse the generic media poster bucket default (`kenigevents`) as the site target. The same service-account credentials may be present in prod for poster uploads, but the site publisher must explicitly address bucket `kenigevents.ru`.

Do not turn the Fly web machine into a general Node/Python backend. On the current Fly.io shape (`shared-cpu`, up to 2 GB RAM for the intended web role, with CPU burst/throttle behavior), the web process must survive crawler traffic, preview bots and ordinary CTA clicks even when personalization telemetry is unavailable.

Telemetry has two production-compatible write paths:

```text
write_path = same_origin_endpoint_v1 | supabase_rpc_ingest_v1
```

Default remains `same_origin_endpoint_v1` because it can drop abuse before the DB. `supabase_rpc_ingest_v1` is an allowed lightweight mode for reducing Fly load, but it moves backend validation into Postgres/RPC and must pass its own gates. Direct browser table writes and raw browser JSON stored as DB rows remain forbidden.

## Production data baseline

Live read-only production SQL on 2026-06-26 returned `pragma quick_check = ok` and this current event/promo shape:

| Fact | Value | Design implication |
| --- | ---: | --- |
| Future active event rows by simple production date filter | 364 | Full static/catalog recomputation is small enough for a batch job; per-page manifests of 12–24 candidates are enough. |
| Probe snapshot parseable future active events | 296 | Some prod date/status fields still need normalization; jobs must tolerate uneven data. |
| Main cities | Калининград 263, Светлогорск 60, then smaller cities | City affinity matters, but same-city alone is too generic for similarity. |
| Top event types | концерт 160, спектакль 50, встреча 33, лекция 26, мастер-класс 15, кинопоказ 14, фестиваль 13 | Concerts will dominate unless diversity/anti-bubble caps are explicit. |
| Free events | 46 | Free/paid is a convenience/CTA feature, not semantic similarity. |
| Ticket status unknown | 158 | CTA must fall back to source/details; purchase/register cannot be assumed. |
| Promo campaigns / activities / exposures | 11 / 34 / 539 | Promo must reuse existing `promo_*` state and stay labelled/capped. |

The MVP optimizes for a small, uneven real catalog, not a marketplace/vector-search architecture.

## P0 gates before any canary

### P0.1 Runtime ownership gate

**Fly web process may only:**

- serve static HTML/CSS/JS and same-origin JSON/manifests;
- render/serve a thin event page shell if needed;
- optionally accept a tiny `POST /api/personalization/summary` telemetry request when `same_origin_endpoint_v1` is selected;
- if the endpoint is selected, validate/classify/dedupe the request and insert/call DB RPC with a short timeout;
- return quickly without blocking navigation/CTA.

**Fly web process must never:**

- run LLM, embedding provider calls or local ML inference;
- run Smart Update, static build, image work, sitemap/OG validation or CDN upload;
- scan the whole catalog per user request;
- run analytics aggregation or profile rebuilds;
- keep unbounded catalog/profile/telemetry state in memory.

Required deployment shape before canary:

```text
web process        static delivery + optional tiny telemetry endpoint
builder/job process Smart Update outputs -> event feature snapshots/manifests/static pages
analytics worker   short scheduled aggregation/retention job, not long-running by default
```

For MVP, `builder/job process` may be GitHub Actions, CI, or a one-shot Fly Machine. It must not be the web process.

### P0.2 Client id / DB id compatibility gate

Browser `anon_id` and `session_id` must be PostgreSQL UUID-compatible if SQL uses `uuid` columns. The reference client therefore creates UUID values without `anon-`/`session-` prefixes and rejects legacy local profiles with incompatible ids.

Accepted:

```json
{"anon_id":"7b79d9d9-bba0-4f6e-9b83-1e3c4976d3d1","session_id":"35a2b764-5e3a-4ea9-a392-bb2cbdc1c2d2"}
```

Rejected / reset to static fallback:

```json
{"anon_id":"anon-...","session_id":"session-..."}
```

`served_list_id` and `client_summary_id` remain opaque `text` identifiers; they are not required to be UUIDs.

### P0.3 Browser payload is not the DB row

The browser sends a client telemetry contract. The selected write path owns the server row contract.

```text
browser payload
  -> hard body cap
  -> parse/validate/schema-version/taxonomy-version checks
  -> consent check
  -> actor classification and trust_state assignment
  -> dedupe/rate-limit
  -> compact field mapping
  -> accepted insert OR tiny quarantine/drop
```

The selected write path annotates fields that the browser must not authoritatively set:

- `requested_at` / `received_at`;
- `consent_version` / server consent interpretation;
- `actor_class` / `trust_state`;
- IP/UA-derived abuse evidence;
- compact reason masks / promo flags / metadata truncation;
- `event_pool_hash` / static build version validation.

No design or test may assume that raw browser JSON can be inserted directly into Supabase tables.

### P0.4 Telemetry write-path gate

Default production mode:

```text
browser -> same-origin Fly endpoint -> validate/drop/quarantine before DB -> Supabase/Postgres insert/RPC
```

Allowed lightweight mode:

```text
browser -> Supabase RPC public.ingest_personalization_summary_v1(...) -> validate/compact/dedupe/quota -> accepted row or tiny quarantine/drop
```

Forbidden in production:

```text
browser -> direct table insert/update/select
browser -> RPC that accepts raw JSON and persists it as-is
```

Same-origin endpoint behavior:

```text
POST /api/personalization/summary
  body cap: 8-16 KB absolute; accepted row target <2 KB
  schema/taxonomy/consent/id validation
  actor_class/trust_state classification
  dedupe by served_list_hash/client_summary_id
  bounded rate-limit maps
  DB insert/RPC timeout: target 300-500 ms, max 800 ms
  DB unavailable/timeout: drop disposable telemetry, return 204/202
```

Supabase RPC ingest may be used only if all are true:

- no `anon`/`authenticated` direct INSERT/UPDATE/SELECT grants on telemetry/profile tables;
- only a dedicated append-only ingest function is executable by `anon`;
- function execute is revoked from `PUBLIC` by default and re-granted only for the specific function/role;
- if `security definer` is used, it sets a fixed `search_path` and references relations explicitly;
- input is compact typed parameters or immediately normalized JSONB, never raw JSON persisted as a row;
- function validates UUID ids, schema/taxonomy/consent/surface/layout/algorithm enums and shown-list cardinality;
- client-supplied `actor_class`, `trust_state`, `training_eligible`, quota/debug flags and server timestamps are ignored;
- dedupe, per-anon/session/time-bucket quota and emergency storage caps run before accepted insert;
- return value is void or minimal `{ok:true}` and never returns profiles/recommendations/debug internals.

CTA/navigation must never wait for either write path. Telemetry is disposable; event page UX is not.

## P1 gates before canary

### P1.1 Fly runtime budget

| Budget | Gate target |
| --- | --- |
| Personalization JS | `<20-30 KB gzip` for MVP island/module |
| Per-page related manifest | 12–24 candidates; `<15-30 KB raw`, smaller preferred |
| Local rerank p95 | `<20 ms` on low-end mobile for one related block |
| Request body cap | `8-16 KB` absolute; accepted row target `<2 KB` |
| DB pool | `1-3` connections max, or HTTP insert with strict timeout |
| In-memory dedupe/rate maps | bounded by count + TTL; no per-user unbounded cache |
| Static/build/aggregation | outside web runtime only |
| Raw weak telemetry | off by default |

Failure modes must be safe:

- DB unavailable -> drop telemetry, page/CTA works;
- write path overloaded -> drop/204 for normal telemetry or 429 for abuse, page/CTA works;
- manifest missing -> static HTML fallback or empty related block, no crash;
- localStorage blocked -> static order, no profile mutation;
- crawler/preview -> static fallback and no trusted telemetry.

### P1.2 Compact storage gate

The free-tier personalization DB has roughly 500 MB budget. Therefore served-list and session summaries cannot store JSON-heavy debug payloads by default.

Accepted production row shape for served-list exposure is compact arrays/bitmasks:

```text
shown_event_ids bigint[]
shown_ranks smallint[]
shown_score_0_1000 smallint[]
shown_reason_mask integer[]
promo_event_ids bigint[]
metadata jsonb <= 512-1024 bytes
```

Full `reason_codes` JSON is debug/sample-only (`1-5%` or explicit test mode), not the default accepted telemetry path.

Retention defaults:

| Data | Retention |
| --- | ---: |
| served-list summaries | 14-30 days full, then aggregate |
| strong actions | 30-90 days |
| quarantine | 7-14 days |
| daily aggregates | 12 months |
| raw weak events | off |

### P1.3 Anti-bubble and feature separation gate

Personalization must not trap a user in one topic. Ranking features are separated so “similarity” does not become “both are evening and paid”.

| Feature class | Examples | Ranking use |
| --- | --- | --- |
| Semantic | `music`, `jazz`, `theatre`, `lecture`, `excursion`, `exhibition` | primary similarity |
| Audience | `kids`, `family`, `adults`, `tourist_friendly` | affinity/exclusion, never mixed with event-side exclusions |
| Convenience | `free`, `ticketed`, `evening`, `weekend`, `date_near`, `same_city`, `price_band_match` | lower-weight convenience/context |
| Module reasons | `same_venue`, `this_weekend`, `other_dates` | separate module/label when possible |

For `event_detail_related`:

```text
positions 1-3: current-event context similarity dominates
positions 4-6: context + diversity + one exploration/adjacent slot when eligible
never: hidden/cancelled/current/linked-date duplicate
```

Before canary, the four current `negative_interest_top5_count_le_1` probe WARN anchors must be manually classified as taxonomy bug, generic-tag leakage, penalty weakness, valid context dominance or source data issue.

### P1.4 Promo gate

Promo is an explicit campaign layer, not hidden interest inference.

- Existing core SQLite `promo_*` tables remain source of truth.
- Promo candidates are labelled (`sponsored`, `partner`, `editorial_pick` or configured wording).
- Promo cannot resurrect hidden/cancelled/past/merged/duplicate events.
- `event_detail_related`: at most one promo in the first 6 cards unless shown as a separate labelled module.
- Promo exposure is labelled in telemetry so it does not bias profile/ranker training as organic interest.

### P1.5 Bot/automation write-path gate

The documentation contract must become executable endpoint/RPC tests:

| Scenario | Expected result |
| --- | --- |
| known preview UA | `204` drop, no accepted DB row |
| unverified Googlebot-like UA | `unknown`/`bot_likely`, never `crawler_verified` from UA alone |
| no consent | drop/quarantine, no profile update |
| missing taxonomy/profile version | drop |
| oversized payload | reject/drop before DB |
| repeated served_list_hash | dedupe before DB |
| click before served_list | quarantine/drop |
| many summaries/minute | rate-limit |

`crawler_verified` requires UA plus reverse DNS/IP allowlist where available; UA alone is not verification.

## Static rebuild / Smart Update gate

Smart Update remains the source of truth for event facts. Personalization consumes accepted canonical events and must not affect extraction, matching, dedup, lifecycle or editorial semantics.

After Smart Update commits an event, Fly SQLite/joboutbox owns rebuild requests:

```text
static_page_rebuild_request(event_id, reason, content_hash, requested_at, status)
```

Batch/worker flow only:

```text
Smart Update commit
  -> core SQLite rebuild request
  -> event feature snapshot export
  -> related manifest regeneration for changed event + affected neighbours
  -> Astro/static build or incremental page render
  -> sitemap/JSON-LD/OG validation
  -> object storage/CDN upload with atomic manifest marker
  -> optional cache purge / short cache-control
```

Supabase personalization DB must not own canonical event lifecycle or rebuild queue state.

## Analytics/statistics gate

Analytics starts from compact accepted exposure/action rows, not a scroll firehose.

Minimum daily aggregate cuts:

- page/event id;
- surface (`event_detail_related`, later `home_feed`, `category_page`);
- algorithm id;
- viewport/layout;
- actor class/trust state;
- organic vs promo;
- category/event type/city.

Key product reports:

- related CTR and ticket/register/source click rate;
- calendar/share/map CTA usage;
- hide/not-interest rate;
- exploration slot CTR/hide rate;
- promo exposure/click/fatigue;
- bot quarantine/drop rate;
- fallback rate by write-path/storage/schema incompatibility.

## Future auth/linking gate

Anonymous personalization cannot silently become authenticated identity stitching.

Later login flow:

1. User logs in.
2. UI asks whether to use this browser personalization on the account.
3. Backend records `auth_profile_link` with auth user id, hashed/opaque anon id, consent version and merge timestamp.
4. Backend merges compact profile snapshots, not raw history.
5. User can unlink/delete imported anonymous personalization.

Authenticated explicit actions outrank anonymous inferred actions; old anonymous weak signals decay quickly.

## CTA gate

Personalization must not narrow the CTA system to “buy ticket”. Event pages need a matrix because current production has many unknown ticket states.

| CTA | When | Telemetry |
| --- | --- | --- |
| Купить билет | paid/available sale link | `ticket_click` |
| Зарегистрироваться | registration/free registration | `register_click` |
| Перейти к источнику | unknown ticket status/source-only row | `source_click` |
| Добавить в календарь | every dated event; `.ics` with Europe/Kaliningrad timezone | `calendar_add` / `ics_download` |
| Поделиться | every public event; mobile `navigator.share`, fallback copy canonical URL | `share_native` / `share_copy_link` |
| Маршрут/карта | venue/address known | `map_click` |
| Другие даты | linked dates exist; separate module | `linked_date_click` |
| Не интересно | after consent in related/feed | `hide_event` |

Mobile share must use the canonical event URL, not preview/bucket URL.

## Engineering spike acceptance checklist

The next PR is not another broad concept document. It must prove the thin runtime contract:

- [ ] `anon_id` / `session_id` are DB-compatible UUIDs or SQL explicitly uses text/hash.
- [ ] Client payload -> server accepted row mapping is documented and tested.
- [ ] Static fallback is visible without JS.
- [ ] No consent -> no local profile mutation and no trusted telemetry.
- [ ] localStorage blocked -> static fallback, no crash.
- [ ] Telemetry write path unavailable -> local CTA works, no trusted remote telemetry.
- [ ] Bot/preview/automation payloads are dropped/quarantined before DB insert.
- [ ] Oversized/repeated payloads are dropped before DB insert.
- [ ] Request body cap is enforced.
- [ ] In-memory rate-limit/dedupe maps are bounded.
- [ ] If `supabase_rpc_ingest_v1` is selected: table grants remain closed to `anon`, function execute grants/search_path are audited, quota/dedupe/storage guards are tested.
- [ ] No LLM/vector/embedding call in web runtime.
- [ ] No static rebuild or Smart Update job in web process.
- [ ] DB insert timeout/circuit breaker is implemented.
- [ ] The four negative-interest WARN cases are manually classified.
- [ ] Promo candidates are labelled/capped if included in manifest.
- [ ] `node --check`, Playwright and write-path endpoint/RPC tests pass from a clean checkout.

## Explicit non-goals now

Do not implement yet: server-side ranker, pgvector serving, CatBoost/LightGBM serving, two-tower, online embeddings, online LLM, personalized homepage, infinite feed, browser -> Supabase direct table writes, raw browser payload -> DB row RPC, raw impression firehose, server profile read by `anon_id`, or Smart Update influenced by telemetry.

## R14 regular product readout contract, 2026-07-27

The review thread requires one small recurring product report, not raw browser
event dumps. It must answer:

| Question | Compact accepted signal | Daily aggregate |
|---|---|---|
| Are pages fast? | sampled Web Vitals after consent (`LCP`, `INP`, `CLS`) plus release/build id and surface | p50/p75/p95 by surface and mobile/desktop |
| Do people use date-page rails? | one `rail_exposed` per event row and one `rail_depth_reached` bucket (`25/50/75/100`) | exposed sessions, engaged sessions, max-depth distribution |
| Do they swipe like/dislike? | committed `rail_like` / `rail_not_interested` only after the canonical action succeeds; consent-dialog cancel is not an action | users/sessions/actions and undo rate by surface |
| Do they find artifacts? | `artifact_exposed` once after the assigned control is actually visible and `artifact_collected` once | exposed, collected, collection rate |
| Do they inspect the collection? | `artifact_collection_view` once per visit with only found-count/total-count | viewers and found-count distribution |
| Do they open artifact detail? | `artifact_detail_open` with artifact id, no story text/URL | opens and unique viewers |
| Do they use the date calendar? | `date_calendar_open` and committed `date_calendar_select` with selected day | users/day, opens, selections and conversion |

Privacy/volume constraints:

- no signal is sent before the existing accepted consent; without consent the
  feature remains fully functional and the event is dropped;
- do not store raw swipe coordinates, scroll offsets, referrer, full URL,
  free-form Search text, full UA or artifact story content;
- exposure events are deduplicated in the browser/session and converted to one
  bounded summary; CTA/navigation never waits for telemetry;
- bots, previews and automated acceptance contexts are excluded or marked
  non-training; immutable noindex artifact research may keep telemetry disabled;
- the recurring report reads daily aggregates, not raw user-level rows, and
  reports denominator-aware conversion (for example found/exposed, not only
  number found).

**Implementation status:** event names and aggregation contract are accepted
documentation only. The production ingest/RLS/retention path and consented
browser emitter have not been shipped; therefore no regular live-statistics
claim may be made for the R14 review candidate.
