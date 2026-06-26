# Event Detail Related Recommendations

> **Status:** MVP-0 product/technical contract, not implemented
> **Surface:** `event_detail_related`
> **Primary goal:** prove personalization as a small enhancement to a useful static event page before designing a personalized home feed.

## Why MVP starts here

The first personalization surface is the related-events block on a concrete event page, not a personalized homepage or infinite feed.

Reason: an event detail page already gives strong context. If the visitor opens a jazz concert, the block should mostly remain “similar to this event” and only gently use the local anonymous profile. A homepage must guess what the visitor wants in general; this is a harder product problem and should wait until MVP-0 proves the catalog, taxonomy, ranking and UX on real data.

Product readiness wording:

```text
Architectural design is stabilized, but product/technical validation is not passed yet.
```

That means concept churn should stop, but implementation must begin with small validation probes: real catalog sample, test personas, static related block, and browser prototype.

## MVP-0 page contract

URL shape:

```text
/sobytiya/<slug>/
```

Page behavior:

1. Static HTML renders the event page and a fallback block “Похожие события”.
2. The fallback block is useful without JS, consent, Supabase, or localStorage profile.
3. After consent:
   - if a compatible localStorage profile exists, the browser reranks the related block locally;
   - if no profile exists, the static context-related order remains;
   - `hidden_event_ids` are hard-filtered;
   - `negative_interest_tags` are removed/downranked;
   - if Supabase/API is unavailable, CTA and fallback related block remain intact.

No online LLM call is allowed in the page-view hot path.

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

Manifest shape:

```json
{
  "event_id": 123,
  "related_static": [
    {
      "event_id": 456,
      "base_similarity": 0.82,
      "reason_codes": ["same_category:music", "tag:jazz", "same_city"]
    },
    {
      "event_id": 789,
      "base_similarity": 0.74,
      "reason_codes": ["tag:evening", "venue_nearby"]
    }
  ]
}
```

## Local rerank formula

For `event_detail_related`, similarity to the current event dominates. Long-term user profile is a modifier, not the main objective.

Draft formula:

```text
personalized_related_score =
  0.45 * similarity_to_current_event
+ 0.20 * user_profile_similarity
+ 0.10 * same_city_or_distance_match
+ 0.08 * date_time_match
+ 0.05 * price_match
+ 0.05 * freshness_or_popularity
+ 0.04 * diversity_bonus
+ 0.03 * exploration_bonus
- negative_interest_penalty
- fatigue_penalty
- explicit_hide_hard_filter
```

Guardrail: if the current page is a jazz concert and the long-term profile likes theatre, the block must not become a theatre подборка. It may slightly raise compatible evening/live-music events, but the page context remains primary.

## Served-list summary

Every rendered/reranked related block should be summarizable without storing raw user history:

```json
{
  "served_list_id": "uuid",
  "anon_id": "uuid",
  "session_id": "uuid",
  "surface": "event_detail_related",
  "layout_mode": "module",
  "current_event_id": 123,
  "algorithm_id": "local_related_rerank_v1",
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

This extends `personalization_served_list_summary` and gives future ranker/eval code exposure context.

## Mobile vs desktop presentation

### Mobile

Under the event content:

```text
Похожие события
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

## Browser prototype before Astro implementation

Before full Astro integration, create the planned reference artifacts:

- `static_site/personalization/demo.html`;
- `static_site/personalization/personalization.js`;
- `tests/playwright/static_personalization_contract.spec.ts`.

The demo should cover only this MVP-0 surface first: static related block, consent, local profile, hide/not interested, mobile module, desktop module, Supabase timeout fallback.
