# Comment Semantic Retrieval for Subscriber Acquisition

Status: proposed research/MVP stage, not yet production posting  
Stage name: `acq_comment_semantic_retrieval.v1`  
Date: 2026-07-04

## Purpose

Mass LLM analysis of every Telegram/VK comment is too expensive and unstable for
Discovery. The first wide layer should answer a cheaper question: **which
replyable surfaces and comments contain acquisition-relevant meanings, how often,
and where exactly?**

This stage is a semantic-retrieval funnel inside the existing Discovery run:

```text
Discovery scanner
  -> collect human comments/messages from replyable surfaces
  -> compute local embeddings on Kaggle
  -> score comments against intent sets
  -> produce surface semantic profiles + ranked comment candidates
  -> send only top candidates to Gemma/LLM gate
  -> import accepted opportunities and write benchmark/report artifacts
```

It does **not** replace the existing LLM-first acceptance policy. Embeddings own
recall, ranking, monitoring prioritization and LLM-budget reduction. Gemma/LLM
still owns final semantic acceptance and any public wording.

## Sources and scope

Use only current Subscriber Acquisition Discovery sources:

- Telegram groups/chats/supergroups;
- Telegram linked discussions resolved from channels;
- VK community wall comments;
- VK discussion-board topics/comments;
- social VK wall posts only when the post itself is a human question/request;
- existing `acq_surface`, manual `/acq_surface_add`, Telegram Monitoring/VK
  monitoring seed payloads, documented Telega.in/Smartik/search seeds, and newly
  discovered public community links.

Do not treat top-level broadcast channel/community posts as reply candidates.
They may provide container metadata: post URL/id, title/snippet, comment count,
linked discussion target and thread context.

No external sends, joins, comments, reactions or personal-wall crawling are part
of this stage.

## Action classes

The retrieval stage must score more than generic “куда сходить” intent.

1. `trip_route_poi_recommendation` — first-class route/POI acquisition lane.
   Queries include where to go from Kaliningrad for one day/weekend, what to see
   in towns of the oblast, what to combine in one route, train/bus/car hints,
   castles/forts/kirks/coast/museums/parks/viewpoints and “is this place worth
   visiting?”. Routes are not implemented yet, so the current MVP target is
   surface discovery: identify publics/chats where route comments happen and mark
   candidates as `route_needed` / `unknown` instead of trying to match a missing
   route corpus. Later, when route cards/posts exist, the same action class can
   resolve to a concrete route card/post or POI explainer.
2. `event_recommendation_reply` — user asks which event/venue/activity to choose.
3. `event_site_search_or_listing` — user asks for afisha/search/calendar/list of
   exhibitions/popular events.
4. `organizer_submission_or_partnership` — user asks where to add/publish/send an
   event or arrange information partnership.
5. `badge_filter_need` — Pushkin card, kids/family, free entry, charity,
   recording/streaming and other future medallion/filter needs.
6. `organizer_visibility_clarification` — organizer-owned event surfaces where a
   useful clarification question may be asked after event matching and dedupe.
7. Future-only labels may exist for research, but must not become automatic
   publication paths: `organizer_data_gap_clarification`,
   `smart_update_enrichment`, `event_verification_blue_check`,
   `sticker_strategy_suitability`.

## Intent ladder

Use multi-level intent sets instead of one flat phrase list.

### `route_poi_far_context`

A surface/thread generally discusses trips, places, region tourism, impressions,
beautiful places, towns, coast, castles, forts, museums, parks, comparisons of
places, whether a place is worth visiting, and short trips around Kaliningrad
Oblast.

### `route_poi_medium_interest`

A person asks where to go from Kaliningrad, what to see in one day/weekend, which
places are worth visiting, how to get to an interesting place, what to see near a
place, or seeks a route/walk/drive through the oblast.

### `route_poi_close_actionable`

A comment is close enough for a concrete route/POI reply: one-day route from
Kaliningrad, train/car/bus trip, what to see in Svetlogorsk/Zelenogradsk/Baltiysk
/Yantarny/Chernyakhovsk, whether to visit a particular castle/fort/kirk/museum
/park, what to combine nearby, children-friendly oblast trip, weekend route.

### `event_far_context`

The thread discusses events, afisha, concerts, exhibitions, lectures, theatre,
festivals, city activities or weekend events.

### `event_close_question`

A person asks about event start time, duration, venue, tickets, price,
registration, children, cancellation/postponement, programme or whether an event
happens today/tomorrow/weekend.

### `organizer_comment_fit`

Under organizer posts, people ask practical event questions: age limits, entry,
registration, tickets, meeting point, bad-weather plan, children, accessibility,
places left, recording/streaming.

### `negative_intents`

Politics/flood/insults, generic praise/thanks without need, post-event reports,
advertising, unrelated бытовые themes, transport/weather without visit/event
relation, and any question unrelated to place/route/POI/event.

## Embedding benchmark

Run exactly the same comment dataset and preprocessing through both models:

| Model | First config | Notes |
| --- | --- | --- |
| `intfloat/multilingual-e5-base` | normalized cosine, `query: ` intent prefix, `passage: ` comment prefix, `max_length=128`, batch `16/32/64` | dimension 768; optional `max_length=256` only if comment p95/p99 length justifies it |
| `BAAI/bge-m3` | dense embeddings only, normalized cosine/dot, `max_length=128`, batch `4/8/16` | dimension 1024; do not use 8192 context for short comments; optional `max_length=256` only after dry-run |

Do not choose thresholds from absolute cosine values before analyzing score
distributions. Ranking and percentiles are primary.

### Scoring methods

- `max_positive_similarity`
- `top3_positive_mean`
- `centroid_similarity`
- `positive_negative_margin`, where `score = max_positive - max_negative`

### Candidate policies

Evaluate top `0.5%`, `1%`, `3%`, `5%`, `10%`, top `20/50` per surface and top
`500/1000` global.

### Dataset matrix

Dry run:

- up to 20 surfaces;
- up to 5,000 comments;
- include TG and VK if available;
- include social/community surfaces, route/tourism communities, TG chats/linked
  discussions and organizer/event-owned surfaces if commentable;
- include `https://t.me/vKalinigrad_recomendations` as a golden calibration
  Telegram group for route/POI interest extraction, because it is known to have
  real questions suitable for semantic retrieval calibration.

Main run only after dry-run review:

- 20k / 50k / 100k comments depending on dry-run throughput and memory.

## Surface semantic profile

Every scanned surface should receive a `comment_semantic_profile` summary:

```json
{
  "surface_key": "platform:external_id",
  "platform": "tg|vk",
  "surface_type": "group|linked_discussion|community|...",
  "comments_total": 1234,
  "comments_embedded": 1200,
  "period": {"min_created_at": "...", "max_created_at": "..."},
  "semantic_presence": {
    "route_poi_far_context": {
      "present": true,
      "level": "none|low|medium|high",
      "top_score_p95": 0.0,
      "candidate_count_top3pct": 0,
      "example_comment_ids": []
    },
    "route_poi_medium_interest": {},
    "route_poi_close_actionable": {},
    "event_far_context": {},
    "event_close_question": {},
    "organizer_comment_fit": {}
  },
  "dominant_detected_interests": ["route_poi", "event_questions"],
  "monitoring_decision_hint": "monitor|sample_more|low_priority|reject",
  "monitoring_reason": "short grounded reason",
  "llm_budget_recommendation": {
    "send_top_comments_to_llm": 0,
    "reason": "short reason"
  }
}
```

Decision hints:

- `monitor`: repeated route/POI or event-question candidates, fresh comments,
  manageable noise/spam;
- `sample_more`: signal exists but sample is small or scores are unstable;
- `low_priority`: broad interest exists but few actionable close candidates;
- `reject`: no relevant meanings, comments unavailable, unrelated region/spam/flood.

## Per-comment retrieval result

Candidate rows must include at least:

```json
{
  "run_id": "...",
  "surface_key": "...",
  "platform": "tg|vk",
  "surface_type": "...",
  "context_url": "...",
  "comment_id": "...",
  "post_id": "...",
  "topic_id": "...",
  "thread_id": "...",
  "created_at": "...",
  "text_snapshot": "short public snippet",
  "model_name": "intfloat/multilingual-e5-base|BAAI/bge-m3",
  "max_length": 128,
  "batch_size": 32,
  "intent_set": "route_poi_close_actionable",
  "scoring_method": "positive_negative_margin",
  "score": 0.0,
  "positive_score": 0.0,
  "negative_score": 0.0,
  "top_intent_phrase": "...",
  "top_intent_score": 0.0,
  "rank_global": 123,
  "rank_within_surface": 5,
  "funnel_bucket": "top_1pct|top_3pct|top_50_surface",
  "candidate_action_type": "trip_route_poi_recommendation|event_recommendation_reply|organizer_visibility_clarification|...",
  "target_hint": {
    "route_target_status": "matched_existing|published_post_found|route_needed|unknown|not_applicable",
    "destination_hint": "",
    "poi_hints": [],
    "transport_hint": "train|bus|car|unknown",
    "event_ids": []
  }
}
```

## Preprocessing

Keep question marks, place names, emojis, transport words and short meaningful
questions. Remove only null/empty text, whitespace/control characters, obvious
HTML tags, and optionally exact duplicate text within the same surface while
preserving duplicate counts.

Do not aggressively lemmatize, strip punctuation, lowercase in a way that breaks
names/URLs, or use OCR in this stage.

Before LLM gate, apply a conservative **question-first quality layer**:

- boost comments with explicit question signal (`?`, “куда/где/как/когда/что”,
  “подскажите/посоветуйте”, “есть ли/будет ли/можно ли”);
- penalize non-question statements;
- mark explicit offers/ads/cross-posts as not eligible for LLM gate when they
  contain cues such as “сохраняйте”, “записывайтесь”, “приглашаем”,
  “бронируйте”, price/discount language, contact/DM calls, or URL/mention
  cross-posts without a question.

This deterministic layer is a narrow budget/noise guardrail, not the semantic
acceptance decision. It should keep rows visible in artifacts with
`candidate_noise_type`, `question_signal` and `pre_llm_candidate_eligible`, but
only eligible question-like rows should spend Gemma/reply review budget.

## Kaggle integration

Current MVP implementation is a sibling module called from the current runtime
after read-only TG/VK comment collection:

```text
kaggle/SubscriberAcquisitionDiscovery/comment_semantic_retrieval.py
```

It is enabled by `ACQ_ENABLE_COMMENT_SEMANTIC_RETRIEVAL=1`, reuses the existing
encrypted config/secrets, Kaggle status dataset,
`kaggle_status_client.py`, `kaggle_registry`, remote Telegram session guard,
`ACQ_*` config style and no-send/shadow constraints. When enabled, the scanner
collects comment records, skips the old per-comment deterministic/Gemma path,
embeds the collected comments locally, attaches a `comment_semantic_profile` to
each scanned surface that had comments, and sends only top retrieval candidates
to the existing Gemma gate.

Suggested progress payload phase names:

- `loading_comments`
- `embedding_intents`
- `embedding_comments`
- `scoring`
- `surface_profile`
- `report`
- `complete` / `failed`

Progress should include model name, device, batch size, max length,
comments_total/processed, comments/sec, surface count, candidate count, elapsed
seconds and progress percent.

## Storage decision

Do **not** write full raw comments, full per-comment scores or embeddings into
core Fly SQLite. That DB is operational and currently owns bot state, canonical
events, publication state, scheduler/job state and review UI. It may keep only
small compatibility rows needed for operator review: accepted/top opportunities,
small surface profile snippets, artifact pointers and import counters.

Research-stage bulk output is artifact-first:

- `acq_comment_retrieval_run_summary.json`
- `comment_retrieval_candidates.csv`
- `comment_retrieval_surface_profiles.csv`
- `comment_retrieval_surface_decision_summary.csv`
- `comment_retrieval_score_distributions.csv`
- `comment_retrieval_manual_review_sample.xlsx`
- `comment_retrieval_speed_metrics.csv`
- `comment_retrieval_report.md`

The XLSX must include a `surface_summary` sheet. It is the operator-facing map
for “where to work next”: per channel/group/community recommendation, counts of
answerable question candidates, counts of contexts useful for asking organizer
clarification questions, filtered noise count, and clickable examples.

YDB serverless is the preferred project-owned store for sanitized retrieval
summaries once the dry run proves value, because the repo already has an optional
YDB acquisition sink and this workload is append/upsert run/profile/candidate
state rather than critical bot operations. Creating the following YDB summary
tables is an implementation detail of this accepted storage direction and does
not require a separate user approval question:

- `acq_comment_retrieval_runs`
- `acq_comment_retrieval_surface_profiles`
- `acq_comment_retrieval_candidates`

YDB rows should be sanitized summaries only: no Telegram access hashes, no auth
payloads, no secrets, no private `_telegram_access`, and no full embeddings by
default. Full embeddings should remain ephemeral artifacts or be moved to object
storage only after a separate retention/cost decision.

A separate local SQLite file is acceptable only as a dev/offline cache or Kaggle
artifact, not as the production state owner. Personalization Supabase/Postgres is
out of scope for acquisition crawling/review queues.

## Benchmark metrics

Timing:

- data_fetch_sec, preprocessing_sec, model_download_or_cache_sec, model_load_sec;
- intent_embedding_sec, comment_embedding_sec, scoring_sec;
- surface_profile_sec, artifact_write_sec, report_generation_sec, total_sec.

Throughput/stability:

- comments_total, comments_after_filter, comments_embedded;
- comments_per_sec/min/hour;
- average/p50/p90/p95/max batch seconds;
- peak RAM MB, CPU info, GPU info if present.

Funnel:

- candidate counts by top-percent policy;
- candidates per surface p50/p90;
- surfaces with route/POI, event and organizer signal;
- estimated LLM reduction versus all comments;
- LLM candidates/run under current `ACQ_MAX_LLM_CALLS_PER_RUN`.

Quality sample:

- precision@100, precision@500, precision@top1%, precision@top3%;
- actionable_rate@top1%;
- route_poi_actionable_rate, event_actionable_rate, organizer_fit_rate;
- false-positive types, duplicate/noise rate, model disagreement examples.

If labels do not exist, create a manual review table and clearly call it a
visual/manual sample, not recall. The first dry-run deliverable must include a
clickable XLSX table for the operator, with direct TG/VK context links, surface,
action class, score/rank fields, model-disagreement bucket and empty columns for
manual labels. CSV may be produced as a secondary artifact, but XLSX is the
operator-facing file. After the dry-run, send the XLSX artifact to Telegram
Saved Messages/«Избранное» through the approved local human Telegram session
(`TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION`), never through the S22 Kaggle
monitoring session.

## Report requirements

The Russian benchmark report must include:

1. executive summary and selected first-layer model/config;
2. scanned TG/VK surfaces, comment source types and time period;
3. surface semantic profiles and monitoring recommendations;
4. speed table by model/batch/max_length/device;
5. funnel table by model/intent/scoring policy;
6. quality/manual review sample or explicit “unlabeled sample only” note;
7. best route/POI, event and organizer examples plus false positives and model
   disagreement examples;
8. production recommendation: model, max_length, batch size, scoring method,
   thresholds, negative margin use, comments/hour, LLM calls after funnel and
   storage/artifact plan;
9. integration sketch: scanner -> retrieval -> surface profile/report -> top
   candidates to Gemma -> accepted `acq_opportunity`.

## Acceptance criteria

The research stage is complete only when:

1. both models run on the exact same comment dataset;
2. a dry-run report is produced;
3. the operator-facing manual review XLSX is produced and delivered to Telegram
   Saved Messages/«Избранное»;
4. every scanned surface has `comment_semantic_profile`;
5. every candidate has action type, context URL, text snapshot, model, intent
   set, scoring method, score, top intent phrase, global rank and surface rank;
6. route/POI candidates include destination/POI/transport hints and
   `route_target_status`;
7. the report compares speed and candidate quality of e5-base vs bge-m3;
8. the report recommends one model/config for the next MVP run;
9. no full-comment LLM processing is used;
10. no production table is mutated without explicit approval;
11. no external Telegram/VK posting happens.
