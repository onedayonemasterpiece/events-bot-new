# Production Integration Plan for Static Personalization

> **Status:** production-shaped design after external review; implementation still pending.
>
> Scope: event pages on `kenigevents.ru`, first surface `event_detail_related`, future mobile discovery feed and desktop grid/modules.

## Production data baseline

Live read-only production SQL on 2026-06-26 returned `pragma quick_check = ok` and this current event/promo shape:

| Fact | Value | Design implication |
| --- | ---: | --- |
| Future active event rows by simple production date filter | 364 | Static build/rebuild must be cheap and frequent; per-page manifests of 12–24 candidates are enough. |
| Probe snapshot parseable future active events | 296 | Some prod date/status fields still need normalization; recommendation jobs must tolerate uneven data. |
| Main cities | Калининград 263, Светлогорск 60, then smaller cities | City/region affinity matters, but same-city alone is too generic for similarity. |
| Top event types | концерт 160, спектакль 50, встреча 33, лекция 26, мастер-класс 15, кинопоказ 14, фестиваль 13 | Music will dominate unless diversity/anti-bubble caps are explicit. |
| Free events | 46 | Free/paid is useful for CTA and affordability, but should be a convenience feature, not semantic similarity. |
| Ticket status unknown | 158 | CTA logic needs robust fallback to source/details; cannot assume purchase/register always known. |
| Promo campaigns / activities / exposures | 11 / 34 / 539 | Static personalization must integrate with existing `promo_*` tables instead of inventing a separate ad system. |

This means the first implementation should optimize for a small real catalog with uneven metadata, not for a huge marketplace/vector-search architecture.

## 1. Anti-bubble policy

Personalization must reduce time-to-interest without trapping the visitor in one topic loop. For Kaliningrad events this is especially important because concerts dominate current inventory.

### Ranking blend

For `event_detail_related`:

```text
positions 1-3: context similarity to current event dominates
positions 4-6: context + diversity + one exploration/adjacent slot when eligible
never: hidden/cancelled/current/linked-date duplicate
```

For future mobile feed/grid:

```text
70-80% relevance / context / stated interests
10-20% exploration and adjacent categories
5-10% editorial or promo slots with disclosure and caps
```

Exploration is not random noise. It should come from controlled reasons:

- adjacent category: jazz concert → classical/live music/exhibition with music context;
- same venue module: “На этой площадке”, not mixed into pure similarity;
- temporal convenience: “Сегодня/на выходных”, separate from semantic similarity;
- editorial/new: fresh or curated events that broaden the catalog;
- city/tourist: relevant if profile/session indicates visitor intent.

### Guardrails

- Keep diversity caps by category and venue for every visible list chunk.
- Keep an entropy floor: if a visitor has one strong interest, still reserve exploration slots unless they explicitly hide that class.
- Do not convert weak skips into hard negative interests until they are valid impressions and repeated.
- Explicit `hide_event` is a hard local filter for that event; category-level negative interest remains soft unless repeated/confirmed.
- Track `exploration_slot=true` and `exploration_reason` in served-list summaries so exploration quality can be measured separately.

### Anti-bubble metrics

- unique categories/venues shown per session;
- exploration CTR and exploration hide rate;
- repeated same-category exposure count;
- return rate after exploration click;
- novelty clicks: first click in a category not seen in the last N sessions;
- long-term profile entropy: alert if one category exceeds a threshold and exploration engagement drops.

## 2. Promo campaign integration

Promos are an explicit campaign layer, not hidden personalization. Existing production tables remain source of truth:

- `promo_campaign` — campaign status, priority, dates, caps/disclosure;
- `promo_target` — concrete event/festival/query targets;
- `promo_activity` — surface/profile/slot policy;
- `promo_exposure` — normalized exposure audit.

### Placement model

Promo candidates enter the same candidate-generation stage as organic candidates, but with a separate score component and hard caps:

```text
candidate_pool = organic_related + eligible_promo_candidates
promo_eligible if campaign active + target event future active + activity surface allowed + caps not exhausted
final_order = organic relevance + promo_boost within caps + diversity + explicit hide veto
```

Rules:

- Promotion must have disclosure: `sponsored`, `partner`, `editorial_pick` or configured campaign wording.
- Promo cannot resurrect hidden, cancelled, past, merged or linked-date duplicate events.
- Promo cannot dominate the first screen: for `event_detail_related`, at most one promo card in the first 6 unless a separately labelled partner module is used.
- Promo does not train user interest by itself. A user clicking/hiding a promoted card is still a user signal, but promo exposure must be labelled to avoid biased training.
- Campaign caps are checked in core SQLite; personalization Supabase stores only view/action summaries and does not own campaign state.

### Surfaces

| Surface | Promo behavior |
| --- | --- |
| Event detail related | one capped labelled promo if contextually eligible; otherwise separate “Партнёрское событие” module |
| Mobile feed later | interleaving with explicit disclosure and frequency caps |
| Desktop grid later | labelled module or row; never hover-only disclosure |
| Existing TG/VK/video surfaces | continue through current promo resolver and `promo_exposure` |

## 3. Smart Update and static page rebuild system

Personalization consumes canonical event facts; it must not change Smart Update extraction/matching semantics.

### Change detection

After Smart Update commits an event, compute whether any static-site field changed:

```text
identity: id, stable slug, linked_event_ids
facts: title, date, time, city, venue, address, price/free, ticket status, lifecycle
content: description, short_description, search_digest, media/poster, source URL
ranking: event_type, topics, tourist_label, tags/enrichment input
```

If changed, enqueue a core SQLite static export job:

```text
static_page_rebuild_request(event_id, reason, content_hash, requested_at, status)
```

The job belongs to Fly SQLite/joboutbox. Supabase personalization DB must not own canonical rebuild state.

### Build/deploy flow

```text
Smart Update commit
  -> static rebuild request
  -> event feature snapshot export
  -> related manifest regeneration for changed event + affected neighbours
  -> Astro/static build or incremental page render
  -> sitemap/JSON-LD/OG validation
  -> object storage/CDN upload with atomic manifest marker
  -> optional cache purge / short cache-control
```

Affected neighbours are events whose related manifests included the changed event or share strong category/venue/date links.

### Lifecycle behavior

- `active` fact update: rebuild page + manifests + sitemap `lastmod`.
- `sold_out`: keep page, update CTA/status, downrank or label in related per policy.
- `cancelled/postponed`: keep page with correct status, remove from active related manifests/lists.
- merged/duplicate: remove from related; redirect/canonical decision follows static-site lifecycle policy.
- past event: remove from active feeds; keep archival page for retention window.

## 4. Analytics and impression statistics

Do not store raw scroll firehose. Store compact, validated, actor-classified summaries.

### Client events

Strong/meaningful actions:

- `served_list_summary` with `served_list_id`, `served_list_hash`, shown event ids/scores/reasons, `algorithm_id`, `viewport_class`, `layout_mode`, `surface`;
- `related_card_click`;
- `ticket_click` / `register_click` / `source_click`;
- `calendar_add` / `ics_download`;
- `share_native` / `share_copy_link`;
- `map_click`;
- `hide_event` / `not_interested`.

Weak signals such as impressions/dwell can be sampled later, but MVP should rely on served-list exposure + strong actions.

### Server path

```text
POST /api/personalization/summary
  validate schema/version/taxonomy
  classify actor_class/trust_state
  dedupe served_list_hash
  validate event ids against manifest/static build version
  accept compact row or write private quarantine evidence
  return 204
```

### Aggregates

Build daily aggregates by:

- page/event id;
- surface (`event_detail_related`, future `home_feed`, `category_page`);
- algorithm id;
- viewport/layout;
- actor class/trust state;
- organic vs promo;
- category/event type/city.

Key reports:

- related card CTR;
- ticket/register/source click rate from related;
- calendar/share/map CTA usage;
- hide/not-interest rate;
- exploration slot CTR/hide rate;
- promo exposure/click/fatigue;
- bot quarantine/drop rate;
- fallback rate caused by endpoint/storage/schema incompatibility.

## 5. Anonymous profile to future authorized profile

Anonymous MVP must not block future login/account features. Use explicit consent and merge, not silent identity stitching.

### Local anonymous profile

```json
{
  "anon_id": "opaque",
  "profile_version": "anon-profile-v1",
  "feature_schema_version": "event-detail-related-v1",
  "taxonomy_version": "event-taxonomy-v1",
  "positive_tags": {},
  "negative_interest_tags": {},
  "hidden_event_ids": []
}
```

### Login/link flow later

1. User logs in.
2. UI asks whether to use this browser's local personalization on the account.
3. Backend creates `auth_profile_link` with `auth_user_id`, hashed/opaque `anon_id`, consent version and merge timestamp.
4. Server merges compact profile snapshots, not raw history.
5. User can unlink/delete imported anonymous personalization.

Merge policy:

- authenticated explicit actions outrank anonymous inferred actions;
- `hide_event` and saved/calendar actions are preserved with timestamps;
- old anonymous weak signals decay quickly;
- no PII is written back to the anonymous browser profile;
- if multiple devices link, merge via server profile snapshots and keep per-device local cache compatible by version.

## 6. CTA system

Primary CTA is not only “buy/register”. Event pages need a small CTA matrix because many production rows have unknown ticket status.

| CTA | When | Implementation notes | Telemetry |
| --- | --- | --- | --- |
| Купить билет | ticket link + paid/available/sale | external link with source trust/utm; never block on personalization | `ticket_click` |
| Зарегистрироваться | registration/free registration | label differs from purchase; preserve source URL | `register_click` |
| Перейти к источнику | unknown ticket status or source-only row | safe fallback for 158 current unknown statuses | `source_click` |
| Добавить в календарь | every dated event | `.ics` with Europe/Kaliningrad timezone, title, location, URL; fallback download | `calendar_add` / `ics_download` |
| Поделиться | every public event | mobile first: `navigator.share({title,text,url})`; fallback `navigator.clipboard.writeText(url)`; final fallback visible URL | `share_native` / `share_copy_link` |
| Маршрут/карта | venue/address known | map link; do not make it primary above ticket CTA | `map_click` |
| Другие даты | linked dates exist | separate module, not related recommendations | `linked_date_click` |
| Не интересно | personalized related/feed after consent | local hide/downrank; compact telemetry only after consent/storage ok | `hide_event` |

Mobile share must use canonical event URL, not the current preview/bucket URL. If native share fails or is unavailable, copy canonical URL and show a short confirmation.

## Implementation gates before canary

No canary until these gates are green:

- static fallback visible without JS;
- taxonomy version in manifest/profile/JS compatibility checks;
- localStorage unavailable/corrupt path tested;
- endpoint unavailable disables trusted telemetry but does not break local CTA/fallback;
- bot/preview/no-consent endpoint tests;
- Smart Update rebuild request contract and stale-page invalidation plan;
- promo candidates labelled and capped;
- 4 negative-interest WARN cases manually classified;
- mobile/desktop visual prototype accepted;
- analytics aggregates defined before raw telemetry expands.
