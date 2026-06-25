# Personalization Taxonomy v1

Status: design requirement before implementation.

The personalization MVP must not let an LLM invent production tags freely. LLM
output is only a proposal. The serving/ranking contract uses a controlled
`taxonomy_version` and a deterministic normalization layer.

## Contract

Pipeline:

```text
raw event text
→ LLM enrichment with JSON schema
→ alias normalization
→ allowed taxonomy mapping
→ unknown/unmapped tag quarantine
→ feature vector build
→ static event_features manifest
```

Hard rules:

- `category` must be one of `allowed_categories`.
- `tags`, `audience_tags`, `mood_tags`, `format_tags`, `time_tags`, and
  `price_tags` must come from the controlled vocabulary.
- LLM-proposed unknown values go to `raw_tags` / `unmapped_tags`, not to ranking.
- Every feature snapshot stores `taxonomy_version` and `feature_schema_version`.
- Changing aliases, allowed tags, vector dimensions, or weights requires a new
  schema version and a localStorage migration/reset path.

## Naming rule: event exclusions vs user dislikes

Do not use one ambiguous `negative_tags` field for both events and visitors.

Use:

- `event.audience_exclusion_tags` — audiences/interests likely **not suited** for
  this event, e.g. `kids` for an adult jazz night. This is not a dislike.
- `user.negative_interest_tags` — interests explicitly or repeatedly rejected by
  the visitor.
- `user.hidden_event_ids` — explicit event-level hide/not interested actions.

Ranking can penalize an event when `event.tags` or `event.audience_exclusion_tags`
match `user.negative_interest_tags`, but the semantics stay separate.

## Draft allowed categories

```json
[
  "music",
  "theatre",
  "exhibition",
  "kids",
  "sport",
  "excursion",
  "lecture",
  "workshop",
  "cinema",
  "festival",
  "market",
  "nightlife",
  "food",
  "other"
]
```

## Draft allowed tags

This is intentionally small for MVP. Expand only when real event samples show a
repeatable need.

```json
[
  "jazz",
  "classical_music",
  "rock",
  "live_music",
  "instrumental",
  "standup",
  "drama",
  "comedy",
  "museum",
  "lecture",
  "tour",
  "family",
  "kids",
  "adult",
  "tourist_friendly",
  "local_friendly",
  "free",
  "ticketed",
  "outdoor",
  "indoor",
  "evening",
  "weekend",
  "weekday",
  "market",
  "food",
  "nightlife"
]
```

## Draft aliases

```json
{
  "джаз": "jazz",
  "jazz_music": "jazz",
  "джазовый концерт": "jazz",
  "концерт": "live_music",
  "live_concert": "live_music",
  "живой концерт": "live_music",
  "инструментальная музыка": "instrumental",
  "классика": "classical_music",
  "классическая музыка": "classical_music",
  "стендап": "standup",
  "stand-up": "standup",
  "дети": "kids",
  "children": "kids",
  "семейное": "family",
  "бесплатно": "free",
  "free_entry": "free",
  "на улице": "outdoor",
  "open_air": "outdoor",
  "вечер": "evening",
  "выходные": "weekend"
}
```

## Unknown tag quarantine

Each enrichment row should expose quality evidence:

```json
{
  "taxonomy_version": "event-taxonomy-v1",
  "feature_schema_version": "event-features-v1",
  "raw_tags": ["камерный формат", "вечерняя музыка"],
  "normalized_tags": ["jazz", "live_music", "instrumental", "evening"],
  "unmapped_tags": ["камерный формат"],
  "quality_warnings": ["unmapped_tag:камерный формат"]
}
```

Rows with too many `unmapped_tags`, schema violations, or low confidence go to
Gemma/human review instead of silently affecting production ranking.
