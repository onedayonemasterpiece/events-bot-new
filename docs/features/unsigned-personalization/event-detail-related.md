# Event Detail Related Recommendations

> **Status:** MVP-0 product/technical contract + hardened browser reference prototype
> **Surface:** `event_detail_related`
> **Primary goal:** validate personalization as a small enhancement to a useful static event page before designing a personalized home feed.

## Why MVP starts here

The first personalization surface is the related-events block on a concrete event page, not a personalized homepage or infinite feed.

Reason: an event detail page already gives strong context. If the visitor opens a jazz concert, the block should mostly remain “similar to this event” and only gently use the local anonymous profile. A homepage must guess what the visitor wants in general; this is a harder product problem and should wait until MVP-0 proves the catalog, taxonomy, ranking and UX on real data.

Product readiness wording:

```text
MVP-0 is ready for an engineering implementation spike; product-quality validation is not passed yet.
```

That means concept churn should stop, but implementation must continue through bounded validation: expanded real-catalog probe, test/golden personas, static related block, browser prototype, bot/automation guardrails and later human top-10 review.


## Current Astro preview implementation (`preview-20260627-event-pages-v32`)

The current static event-page preview implements this contract as a local/browser slice, not as a Supabase write path yet:

1. Static HTML renders the event page and up to 10 `event_detail_related` cards from the build-time manifest. If the compact preview fixture has fewer eligible future events after excluding the current event/linked dates, it renders all available candidates; production target remains 10.
2. `/data/discovery/<event_id>.json` is now an `event-detail-related-v1` manifest with `current_event` and `related_static[]`, not a display-only `events[]` payload.
3. Before consent, no trusted profile is created and the static fallback remains unchanged.
4. After consent, the browser creates/uses `ke_personalization_profile` with UUID-compatible `anon_id` and `session_id`; legacy prefixed ids and profiles missing `feature_schema_version`/`taxonomy_version` are ignored.
5. On activation with a compatible profile, the client removes hard-hidden / `not_interested` / strong negative-interest matches from the preloaded cards, reorders by local `rankEventDetailRelated`, performs one same-origin JSON top-up, then exposes only `Показать ещё` for additional cards.
6. Strong actions (`like_event`, `unlike_event`, `not_interested`, `undo_not_interested`, `share_event`) are written only to compact local debug storage for the preview, with `served_list_id` / `served_list_hash` attached. Mapping this into Supabase remains the production telemetry slice.
7. Static related selection is implemented in `site/src/lib/events.ts` as `static_related_v1`: explicit seeds from `site/src/data/preview-related.json` are merged with all eligible future active events, then scored by category equality, tag Jaccard overlap, same city, date proximity, same venue and price-band match. Hard exclusions remove current event, linked other dates, reverse linked dates, inactive/cancelled/past events. This solves the static fallback for the preview/MVP engineering slice, not final recommendation quality; production still needs expanded real-catalog probes and human/golden top-10 review.
8. Mobile event cards now show `title → time/status → venue` after the image. Product rationale: in a feed/continuation scan the user first decides “what is this?”, then checks date/conditions; this also gives crawlers a cleaner text order inside each card.
9. Service controls (`Не интересно`, share, like/unlike, undo plate) remain real `<button>` controls, not links, and are marked `data-nosnippet`. Ticket/registration/calendar/detail links remain crawlable. This prevents utility UI text from polluting snippets/LLM summaries while keeping event content and internal navigation indexable.

## MVP-0 page contract

URL shape:

```text
/sobytiya/<slug>/
```

Page behavior:

1. Static HTML renders the event page and a fallback continuation block (current public label: `Смотрите дальше`; historical/internal label: “Похожие события”).
2. The fallback block is useful without JS, consent, Supabase, or localStorage profile.
3. After consent:
   - if a compatible localStorage profile exists, the browser reranks the related block locally;
   - if no profile exists, the static context-related order remains;
   - `hidden_event_ids` are hard-filtered;
   - `negative_interest_tags` are removed/downranked;
   - if Supabase/API is unavailable, CTA and fallback related block remain intact.
4. Crawlers, preview bots and suspicious automation receive static fallback only and cannot write trusted telemetry; see `bots-and-automation.md`.

No online LLM call, vector DB call or provider embedding call is allowed in the page-view hot path.

## Static related candidate generation

At static build time, generate `12–24` candidates per event:

```text
current_event -> similar future active events
```

Mandatory filters/guards:

- exclude the current event;
- exclude past events;
- exclude `cancelled` events;
- handle `sold_out` by downranking or explicit product rule, not by silently removing unless decided later;
- put other dates of the same event into “Другие даты”, not into the main related block;
- prefer same city/region first;
- avoid too many cards from one venue;
- avoid filling every slot with concerts when close alternatives exist.

Manifest shape (`docs/features/unsigned-personalization/samples/event-detail-related-manifest.sample.json` is a generated example from the real catalog probe):

```json
{
  "schema_version": "event-detail-related-v1",
  "feature_schema_version": "event-detail-related-v1",
  "taxonomy_version": "event-taxonomy-v1",
  "surface": "event_detail_related",
  "algorithm_id": "static_related_v1",
  "generated_at": "2026-06-26T00:00:00Z",
  "current_event": {
    "event_id": 123,
    "title": "Камерный джаз",
    "category": "music",
    "tags": ["jazz", "live_music", "evening"],
    "city": "Калининград",
    "location_name": "Дом искусств",
    "date": "2026-07-12"
  },
  "related_static": [
    {
      "event_id": 456,
      "title": "Камерный джаз на крыше",
      "category": "music",
      "tags": ["jazz", "live_music", "evening"],
      "audience_exclusion_tags": [],
      "city": "Калининград",
      "location_name": "Roof Hall",
      "date": "2026-07-14",
      "status": "available",
      "lifecycle_status": "active",
      "is_free": false,
      "base_similarity": 0.82,
      "reason_codes": ["same_category:music", "tag:jazz", "same_city"]
    }
  ]
}
```

Candidate fields:

| Field | Required | Notes |
| --- | --- | --- |
| `event_id`, `title` | yes | stable canonical event id from Fly SQLite; no Supabase ids |
| `category`, `tags` | yes | controlled taxonomy values; no free-form LLM tags in serving |
| `audience_exclusion_tags` | yes, can be empty | event-side “not suited for” hints, never user dislikes and never `negative_interest_tags` scoring input |
| `city`, `location_name`, `date` | yes | used for context and display |
| `status`, `lifecycle_status` | yes | cancelled/postponed/duplicate/merged are hard-excluded |
| `is_free` / price band | recommended | used for light price affinity |
| `base_similarity` | yes | static related score normalized to `0..1` |
| `reason_codes` | yes | compact explainability and telemetry evidence |

Static score formula used by the probe/reference contract:

```text
static_related_score =
  0.28 * same_category
+ 0.24 * tag_overlap_jaccard
+ 0.12 * same_city
+ 0.12 * date_proximity
+ 0.06 * same_venue
+ 0.05 * price_band_match
- 0.20 * sold_out_penalty
```

The exact weights are allowed to change after probes, but the invariants are not:
current/cancelled/past/linked-date duplicates are excluded, and `reason_codes`
must explain why a candidate entered the static fallback.

## Local rerank formula

For `event_detail_related`, similarity to the current event dominates. Long-term user profile is a modifier, not the main objective.

Draft formula:

```text
personalized_related_score =
  0.80 * static_related_score
+ 0.10 * local_profile_affinity
+ 0.04 * price_match
+ 0.03 * time_match
+ 0.60 * explicit_like_match
+ 0.02 * exploration_bonus
- 0.55 * negative_interest_match
- 0.18 * fatigue_penalty
- 0.20 * sold_out_penalty
- explicit_not_interested_hard_filter
- explicit_hide_hard_filter
```

Guardrails:

- if the current page is a jazz concert and the long-term profile likes theatre, the block must not become a theatre подборка. It may slightly raise compatible evening/live-music events, but the page context remains primary;
- `audience_exclusion_tags` belong to the event and are not user dislikes. A 18+/adult jazz event with `audience_exclusion_tags: ["kids"]` must not be penalized for a visitor whose `negative_interest_tags.kids` means “do not show me children's events”;
- legacy profiles containing `negative_tags` or missing/mismatched `feature_schema_version` / `taxonomy_version` are incompatible and fall back to static order until reset/migration;
- explicit `like_event` is a direct positive action and may immediately boost that event in the local surface; `unlike_event` only removes this boost and must not be interpreted as negative interest;
- explicit `not_interested` is the direct negative action and should hard-filter/demote that event in the local surface and feed `negative_interest_tags` during rollup;
- explicit feedback must not cause orientation loss in the current viewport: when the user likes/unlikes/marks a card not interesting, the acted-on card and all cards above it stay in place; local rerank may only reorder cards below that anchor until the next page load/refresh;
- selected telemetry write path unavailable disables trusted telemetry/server mutation, but a consented compatible local profile may still run local rerank as `local_related_rerank_v1_fallback`;
- localStorage unavailable/corrupted means no profile mutation and static fallback.
- `anon_id` and `session_id` must be UUID-compatible while the database schema uses `uuid` columns; legacy prefixed ids are incompatible and fall back to static order until reset/migration.

Local profile fields consumed by MVP-0:

```json
{
  "profile_version": "anon-profile-v1",
  "feature_schema_version": "event-detail-related-v1",
  "taxonomy_version": "event-taxonomy-v1",
  "anon_id": "uuid-v4-compatible",
  "session_id": "uuid-v4-compatible",
  "positive_tags": {"jazz": 1.0, "live_music": 0.6},
  "negative_interest_tags": {"kids": 1.0},
  "liked_event_ids": [5878],
  "not_interested_event_ids": [6093],
  "hidden_event_ids": [12345],
  "seen_event_ids": [222],
  "seen_venue_ids": ["venue-or-name"],
  "price_preferences": {"prefer_free": false}
}
```

## Served-list summary

Every rendered/reranked related block should be summarizable without storing raw user history:

```json
{
  "served_list_id": "opaque-text-id",
  "anon_id": "uuid",
  "session_id": "uuid",
  "surface": "event_detail_related",
  "layout_mode": "module",
  "current_event_id": 123,
  "algorithm_id": "local_related_rerank_v1",
  "served_list_hash": "stable-list-hash",
  "shown": [
    {
      "event_id": 456,
      "rank": 0,
      "base_similarity": 0.82,
      "personal_score": 0.91,
      "reason_codes": ["tag:jazz", "same_city", "profile:live_music"]
    }
  ]
}
```

This extends `personalization_served_list_summary` and gives future ranker/eval code exposure context. The selected production write path maps `shown[]` JSON into compact arrays/reason masks before DB insert; full reason JSON is debug/sample-only. The reference client creates `served_list_id` before rendering cards, attaches the same id/hash to strong actions, dedupes repeated `served_list_summary` emissions by `served_list_hash` for the configured window, and keeps that in-memory dedupe map bounded so resize/re-render does not create a raw firehose. When `backendAvailable=false`, the reference client disables trusted telemetry and can emit local debug-only records only through `debugTelemetrySink`.

Session summary and strong actions stay compact:

```json
{
  "event_kind": "session_summary",
  "surface": "event_detail_related",
  "viewport_class": "mobile",
  "layout_mode": "module",
  "presentation_mode": "vertical_related",
  "algorithm_id": "local_related_rerank_v1",
  "event_counts": {
    "served_list_summary": 1,
    "related_card_click": 1,
    "hide_event": 1
  },
  "strong_event_ids": {
    "related_card_click": [456],
    "hide_event": [789]
  }
}
```

```json
{
  "event_kind": "related_card_click",
  "surface": "event_detail_related",
  "event_id": 456,
  "rank": 0,
  "served_list_id": "opaque-text-id",
  "served_list_hash": "stable-list-hash",
  "algorithm_id": "local_related_rerank_v1"
}
```

## Mobile vs desktop presentation

### Mobile

Under the event content:

```text
Смотрите дальше
[card]
[card]
[card]
Показать ещё
```

MVP target: 6 cards + “Показать ещё”, not an infinite feed.

Signals:

- `event_detail_view`;
- `related_card_click`;
- `ticket_click`;
- `share` / `copy_link`;
- `hide_event` / `not_interested`;
- `valid_impression`;
- `dwell_checkpoint`.

`quick_skip` can be considered only after `valid_impression`.

### Desktop

Do not stretch a mobile feed into desktop. Use modules:

- right rail: “Похоже на это”;
- below description: 3–4 card grid;
- optional module: “В эти выходные для вас”.

Personalization on desktop changes:

- card order;
- section choice/order;
- reason chips;
- hiding/downranking explicit negatives.

It must not aggressively rearrange the whole page or learn from hover as a strong signal.

## Rankers to compare before implementation

MVP-0 validation compares three small rankers on the same real catalog:

| Ranker | Description | Required before browser prototype |
| --- | --- | --- |
| `static_related_v1` | current-event similarity + deterministic rules only | yes |
| `local_related_rerank_v1` | static related + localStorage profile + negative interests | yes |
| `semantic_related_v1` | static/local features + semantic embedding similarity | eval only; not online dependency |

Acceptance is not “the model feels smart”. Required checks:

- current event is never in related;
- cancelled events are never in related;
- sold-out behavior follows the documented rule;
- other dates are separated;
- `hidden_event_ids` never show;
- `negative_interest_tags` are absent/downranked in top 5;
- top 10 does not exceed the event-type diversity cap;
- mobile and desktop are evaluated separately.

## SEO/GEO policy for service controls

- Do **not** expose utility actions such as `Не интересно`, like/unlike, share/copy and undo as crawlable `<a href>` links. They are interaction controls and should stay `<button>` with clear `aria-label`.
- Mark those controls/plates `data-nosnippet`, because repeated utility text can otherwise appear in snippets or be over-weighted by AI crawlers even though it is not event content.
- Do not place `data-nosnippet` on title, date, venue, description, ticket/register/calendar links, JSON-LD or the static related card body. Those are useful public content and navigation.
- Calendar/detail/ticket links may remain crawlable where appropriate; native share and negative feedback must not be indexed as destinations.

## Browser prototype before Astro implementation

Before full Astro integration, the reference artifacts are:

- `static_site/personalization/demo.html`;
- `static_site/personalization/personalization.js`;
- `tests/playwright/static_personalization_contract.spec.ts`.

The demo covers only this MVP-0 surface first: static related block, consent,
local profile, hide/not interested, mobile module, desktop module, telemetry
write-path/Supabase timeout fallback, strict profile compatibility, event-exclusion separation from user negative interests, served-list id/hash lifecycle and resize/render telemetry dedupe.

Verification:

```bash
NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules \
PLAYWRIGHT_HTML_OPEN=never \
npx playwright test tests/playwright/static_personalization_contract.spec.ts --browser=chromium --reporter=line
```

Last local run: `8 passed` (Playwright Chromium).

## Real catalog probe

Probe script:

```bash
python3 scripts/probe_event_detail_related.py \
  --db artifacts/db/event_quality_audit_20260624_prod.sqlite \
  --today 2026-06-26
```

Outputs:

- report: `docs/features/unsigned-personalization/event-detail-related-probe.md`;
- manifest example: `docs/features/unsigned-personalization/samples/event-detail-related-manifest.sample.json`.

Current decision: local feature vectors are sufficient for the MVP-0 **engineering spike**, not proven final recommendation quality. Production integration details live in `production-integration.md`. The expanded probe keeps safety invariants green but still has negative-interest quality WARN rows, so taxonomy/ranking tuning and human top-10 review remain required. Do not add a semantic embedding provider to the browser or required build path. Keep `semantic_related_v1` as a later offline comparison only after expanded automated probe plus human/golden top-10 review show a measured quality need.
