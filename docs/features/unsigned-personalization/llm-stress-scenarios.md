# LLM Stress Scenario for Personalization Enrichment

> **Status:** design/eval scenario  
> **Real-data evidence:** `artifacts/codex/static-personalization/real_event_sample_2026-06-24.json` (not committed) from Fly production SQLite read-only probe.

A reproducible prompt pack can be generated offline with:

```bash
python3 scripts/build_personalization_llm_stress_pack.py \
  --input artifacts/codex/static-personalization/real_event_sample_2026-06-24.json \
  --output artifacts/codex/static-personalization/llm_stress_prompt_pack_2026-06-24.json \
  --limit 24 \
  --top-k 12
```

The command performs no provider call; it builds the LLM feature-extraction prompt, persona evaluator prompts, deterministic top-k baselines, and validates the pack shape. Current local validation result: `{"ok": true, "errors": []}`.

## Real data snapshot

Read-only production probe on 2026-06-24 found `377` future events. Top event types in the sample window:

| event_type | count |
| --- | ---: |
| концерт | 156 |
| спектакль | 54 |
| встреча | 36 |
| лекция | 33 |
| мастер-класс | 20 |
| кинопоказ | 20 |
| экскурсия | 13 |
| фестиваль | 10 |
| ярмарка | 9 |
| выставка | 5 |

The real sample includes useful stress cases:

- many concerts vs smaller categories, so diversity guardrails must prevent a concert-only feed;
- mixed city/venue contexts: Калининград, Светлогорск, Гусев, посёлки;
- free and paid events;
- sold-out/registration/sale ticket statuses;
- type/text inconsistencies, for example a kinopokaz row whose digest/description looked like a concert — enrichment must flag, not silently repair;
- non-standard sport/outdoor/excursion events that should not be forced into culture-only tags.

## What the LLM is allowed to do

Offline enrichment may create ranking features:

```json
{
  "event_id": 123,
  "normalized_tags": ["concert", "jazz", "live_music"],
  "audience_tags": ["adults", "tourists"],
  "negative_tags": ["kids"],
  "mood_tags": ["evening", "calm"],
  "format_tags": ["indoor", "ticketed"],
  "embedding_text": "short normalized text for embedding",
  "confidence": 0.0,
  "quality_warnings": ["type_description_mismatch"]
}
```

It must not change canonical event title/date/venue/ticket status. Warnings go to debug/review or Smart Update replay.

## Stress prompt: batch feature extraction

Use a batch of 24–100 real future events. Keep descriptions truncated and source-safe.

```text
You are enriching public event listings for recommendation ranking.
Return strict JSON only, matching the schema.
Do not repair factual fields. If title/type/digest conflict, add quality_warnings.
Do not infer private user data. Do not optimize for clickbait.

Schema:
{
  "events": [
    {
      "event_id": integer,
      "normalized_tags": string[],
      "audience_tags": string[],
      "negative_tags": string[],
      "mood_tags": string[],
      "format_tags": string[],
      "embedding_text": string,
      "confidence": number,
      "quality_warnings": string[]
    }
  ]
}

Allowed tag style: lower_snake_case English tokens.
Prefer 3-8 normalized_tags per event.
negative_tags mean audiences/interests likely not suited for the event, not moral judgement.
quality_warnings examples:
- type_description_mismatch
- missing_time
- weak_description
- location_ambiguous
- ticket_status_unclear
- likely_non_event

Input events:
<JSON array from production sample>
```

Acceptance:

- every input event_id appears exactly once;
- no extra event IDs;
- valid JSON under schema;
- no title/date/venue repairs in output;
- mismatch cases get warnings;
- tags are stable across retry for high-confidence obvious events;
- output can be stored in `event_feature_snapshot` without raw source text.

## Stress prompt: persona top-k evaluator

This prompt is for eval/oracle only, not online ranking.

```text
You are evaluating whether a deterministic recommender ranked events well.
Given a synthetic anonymous persona, event features, and a proposed top 20 list,
return JSON with pass/fail checks. Do not rerank all events; judge the proposal.

Persona:
{
  "persona_key": "mobile_jazz_concerts_negative_kids",
  "viewport_class": "mobile",
  "layout_mode": "feed",
  "positive_tags": {"concert": 0.8, "jazz": 0.7, "evening": 0.4},
  "negative_tags": {"kids": 0.9, "workshop": 0.4},
  "strong_actions": ["ticket_click", "event_detail_view"]
}

Checks:
- top_5_contains_positive_interest
- no_more_than_3_same_event_type_in_top_10
- negative_tags_not_in_top_5_unless_exploration_slot
- sold_out_downranked_unless_exact_interest
- mobile_feed_diversity_ok

Return:
{
  "pass": boolean,
  "failed_checks": string[],
  "notable_event_ids": integer[],
  "reason": string
}
```

Acceptance:

- evaluator cannot see anonymous raw history beyond the synthetic profile;
- it detects diversity failures even when relevance is high;
- it treats mobile and desktop differently via `layout_mode`;
- it is used in CI/eval batches only, never per visitor page view.

## Load/stress plan

1. Build event feature snapshots for 200–500 real future/recent events.
2. Run 8–12 synthetic personas across mobile feed and desktop grid.
3. For each persona, run deterministic local ranker first.
4. Use LLM evaluator only on the proposed top-k and sampled failures.
5. Track:
   - schema validity;
   - retry consistency;
   - cost per 100 events;
   - latency per batch;
   - number of quality warnings;
   - cases where LLM disagrees with deterministic tags.
6. Stop if two similar model/schema failures occur; inspect prompts/docs/model contract before another trial.

## Expected product conclusion

The likely MVP architecture remains:

```text
Smart Update accepted event
  -> static export
  -> offline LLM feature enrichment / embeddings
  -> static feature/recommendation manifests
  -> local-first browser rerank after consent
  -> Supabase append-only telemetry and backend aggregates
```

No online LLM call is required to render a personalized feed.
