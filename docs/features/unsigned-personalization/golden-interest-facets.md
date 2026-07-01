# Golden interest facets

Status: design accepted / implementation pending.

## Product decision

Do not model a visitor as one “golden person”. A visitor has a weighted mix of narrow **golden interest facets**. The facets are retrieval shortcuts and ranking hints; they do not replace explicit user control.

Examples:

- `music_evening`;
- `art_exhibitions`;
- `family_weekend`;
- `kids_workshops`;
- `free_city_walk`;
- `theatre_culture`;
- `tourist_one_day`;
- `local_regular`.

## Horizons

Facet weights must be calculated from all personalization horizons already defined in the project, not only from short-term/session signals:

| Horizon | Role |
| --- | --- |
| `session` | immediate context for the current visit; volatile boost only |
| `short` | recent interests over days/weeks |
| `mid` | stable preferences over weeks/months |
| `long` | durable tastes with slow decay |
| `negative` | separate exclusion/demotion axis, never a time period |

Golden-facet scoring combines these horizons, for example:

```text
facet_weight = 0.35 * session + 0.30 * short + 0.22 * mid + 0.13 * long - negative_penalty
```

Exact weights are an algorithm version, not UI copy. Hard hides are allowed only from explicit negative actions.

## Pipeline role

```text
candidate generation
  -> scoring by local profile horizons + facet weights
  -> reranking with diversity/exploration/business rules
```

Candidate generators:

- static related/discovery manifest;
- golden-facet manifests;
- popular/fresh events;
- editorial/promo slots;
- exploration bucket.

Reranking rules:

- explicit `Не интересно` hard-hides exact event/linked dates;
- likes/share/calendar/ticket/phone actions are strong positives;
- implicit dwell/scroll signals are weak and must not hard-hide;
- diversity by category, venue and date/time;
- exploration quota 10–20% to avoid an information bubble;
- user can always switch back to `Все`.

## Local profile shape

```json
{
  "schema_version": "anonymous_profile_v2",
  "anon_id": "uuid",
  "session_id": "uuid",
  "taxonomy_version": "event-taxonomy-v1",
  "feature_schema_version": "event-features-v1",
  "interest_facets": {
    "music_evening": { "weight": 0.74, "confidence": 0.68 },
    "art_exhibitions": { "weight": 0.52, "confidence": 0.61 }
  },
  "disabled_facets": [],
  "negative_actions": {
    "event_ids": [],
    "linked_event_ids": []
  }
}
```

If schema/taxonomy/profile versions are incompatible, the page falls back to static order and offers reset/migration instead of silently using stale preferences.

## Static manifest contract

Facet manifests are static/same-origin/CDN files, not a page-view Supabase read path:

```text
/data/personalization/facets/v1/<facet_id>/home_mobile.json
/data/personalization/facets/v1/<facet_id>/segodnya.json
/data/personalization/facets/v1/<facet_id>/vyhodnye.json
```

```json
{
  "schema_version": "golden_facet_manifest_v1",
  "facet_version": "golden_facets_2026_06_28",
  "facet_id": "music_evening",
  "surface": "segodnya_mobile",
  "build_id": "2026-06-28T14-20-00Z",
  "algorithm_id": "facet_static_v1",
  "items": [
    { "event_id": 5878, "rank": 1, "score_bucket": "high", "reason_codes": ["music", "evening"] }
  ]
}
```

## Kaggle role

Kaggle may generate facet manifests from immutable event/action snapshots as a batch job. It is a compute worker, not the production trust boundary; publication follows `docs/operations/kaggle-static-site-builder.md`.
