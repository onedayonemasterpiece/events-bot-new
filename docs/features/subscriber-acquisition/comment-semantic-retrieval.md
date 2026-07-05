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

Important separation for reports:

- the default LLM gate receives top-N vector semantic rows **without**
  deterministic/regex prefiltering;
- human-facing evidence (`answerable_*`, `ask_contexts_*`, `surface_summary`,
  `question_patterns`, `canonical_questions`) uses a stricter post-vector
  report-quality gate so a noisy vector match is not promoted into an
  “эталонный вопрос”.

The report-quality gate requires a real question for reply examples, supported
intent in the current text, no explicit offer/ad/cross-post/out-of-scope
diagnostic, and a minimal score floor. Source posts may still appear as
`organizer_visibility_clarification` context for “ask the organizer” analysis,
but they must not be shown as user reply questions or canonical question
examples. Therefore `answerable_*` sheets contain only fresh replyable user
questions/messages, while `ask_contexts_*` sheets contain organizer/source-post
contexts where the product may later draft its own clarification question.

## Kaliningrad Oblast region gate

Subscriber Acquisition is region-scoped: event replies, route/POI replies,
organizer clarification questions, event-search replies, organizer submission
replies and badge/filter replies are eligible only when the surface/thread
proves **Kaliningrad Oblast**.

The gate is intentionally separate from the acquisition-intent scorer:

1. The action-intent vectors still decide whether a text is about routes,
   events, organizers, site search, badges, etc.
2. A dedicated region vector embeds a wider context:
   surface title/URL/handle/source, source post/topic, parent comment and
   current comment/post.
3. Narrow deterministic KGD hints (for example `Калининград`, `39` in a
   handle, `Светлогорск`, `Зеленоградск`, `Куршская коса`, known local
   landmarks) are evidence boosts, not a broad replacement for semantic
   scoring. `39`/`kld*` are trusted only in surface metadata/URLs, not inside
   arbitrary comment bodies.
4. Explicit other-region hints such as `visitNavahrudak`/`Новогрудок`,
   `Минск`, `Москва`, `СПб` reject the row only when no Kaliningrad evidence is
   present.

Rows receive `region_confidence`:

- `confirmed` — explicit Kaliningrad Oblast evidence in surface metadata or
  comment/post context;
- `probable` — dedicated region vector is sufficiently closer to the
  Kaliningrad catalog than to the other-region catalog;
- `unknown` — no safe evidence yet;
- `out_of_region` — likely another region.

Only `confirmed`/`probable` rows may enter `goal_*`, `monitoring_targets` and
the top-N Gemma/LLM gate. `unknown` rows remain visible in diagnostics so future
scans can prove the region through newer comments or better surface metadata.
The XLSX includes `region_catalog` and region columns in surface/comment sheets
so operators can audit why a place was selected or rejected.
The semantic-only `probable` path is intentionally conservative: the default
margin is `ACQ_COMMENT_RETRIEVAL_REGION_MARGIN_MIN_SCORE=0.06`, so a generic
“куда съездить?” without local anchors should stay `unknown` rather than spend
LLM budget.

## Future-event and hard out-of-scope gates

Event acquisition is forward-looking. Rows whose future action is to answer an
event question or ask an organizer (`event_recommendation_reply`,
`organizer_visibility_clarification`, `event_site_search_or_listing`,
`badge_filter_need`) must pass an event-time gate before they can appear in
`goal_*`, `monitoring_targets` or the Gemma top-N queue:

- explicit dates in the current text plus source-post/parent context are parsed
  against the run date; if the latest detected event date is before the run date,
  the row is `past_event` and diagnostic-only;
- post-event semantic signals such as отчёт/итоги/спасибо организаторам reject
  as `past_event_signal` when no future/announcement signal is present;
- future/today dates and future announcement signals can pass, but still require
  final LLM validation before any real reply/question.

Fuel availability is outside the product scope: questions like “где есть
бензин/дизель/топливо/АЗС?” are marked
`out_of_scope_gasoline_availability` and are excluded from goal sheets and LLM
budget even if the vector model sees route/transport similarity.

## Sources and scope

Use only current Subscriber Acquisition Discovery sources:

- Telegram groups/chats/supergroups;
- Telegram linked discussions resolved from channels;
- VK community wall comments;
- VK personal profile wall comments when the profile wall is explicitly
  discovered/seeded, publicly readable, actively maintained and has comments;
- VK discussion-board topics/comments;
- VK wall posts only as context/noise/source-post evidence by default; they are
  exported separately as `relation=vk_social_wall_post` and do not spend reply
  budget unless `ACQ_ALLOW_SOURCE_POST_REPLY_CANDIDATES=1` is explicitly set;
- existing `acq_surface`, manual `/acq_surface_add`, Telegram Monitoring/VK
  monitoring seed payloads, documented Telega.in/Smartik/search seeds, newly
  discovered public community links, and explicitly discovered VK profile-wall
  links (`vk:id...` / positive-owner `wall123_...`) when they pass the bounded
  read-only scan.

Do not treat top-level broadcast channel/community posts as reply candidates.
They may provide container metadata: post URL/id, title/snippet, comment count,
linked discussion target and thread context.

No external sends, joins, comments or reactions are part of this stage. VK
personal-wall scanning is allowed only for explicit profile candidates found in
public evidence or optional capped member samples; it remains read-only and
bounded by the same run budgets.

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

Bounded live runs with a hard wall-clock target (for example the current
≤10 minute Kaggle discovery check) may reduce scanned surfaces/messages and the
Gemma top-N gate, but **must not** reduce semantic retrieval to one embedding
model. The runtime always appends both required models
(`intfloat/multilingual-e5-base`, `BAAI/bge-m3`) even when
`ACQ_COMMENT_RETRIEVAL_MODELS_JSON` is misconfigured to a single model; local
overrides may only add experimental models.

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
- every report must state the actual scope/depth: seed payload count, scanned
  surface count, scan budgets, source relations (`tg` group/comment, linked
  discussion, `vk_comment`, `vk_board_comment`, `vk_social_wall_post`), and the
  observed `created_at` period. It must not imply “all DB/all history” unless
  the payload and budgets really covered that.
- freshness policy: historical comments are retained for canonical question
  extraction and semantic-control QA, but only comments within
  `ACQ_COMMENT_RETRIEVAL_MAX_COMMENT_AGE_DAYS=365` may become monitoring/LLM
  candidates. A surface whose latest observed comment is older than
  `ACQ_COMMENT_RETRIEVAL_STALE_ACTIVITY_DAYS=92` is marked
  `reject_stale_inactive` for the current acquisition plan. This is not a
  permanent ban: if the public/chat/profile revives and fresh comments appear, a
  later run can move it back into `candidate`/`selected`.

Main run only after dry-run review:

- 20k / 50k / 100k comments depending on dry-run throughput and memory.

## Surface semantic profile

Every scanned surface should receive a `comment_semantic_profile` summary:

```json
{
  "surface_key": "platform:external_id",
  "platform": "tg|vk",
  "surface_type": "group|linked_discussion|community|...",
  "surface_title": "...",
  "surface_url": "https://...",
  "members_or_subscribers": 0,
  "comments_total": 1234,
  "comments_embedded": 1200,
  "period": {"min_created_at": "...", "max_created_at": "..."},
  "period_days": 30.0,
  "comments_per_day": 0.0,
  "comments_per_week": 0.0,
  "comments_per_30d": 0.0,
  "comments_per_90d": 0.0,
  "latest_100_comments": 100,
  "latest_100_period_days": 7.0,
  "unique_commenters": 0,
  "region_confidence": "confirmed|probable|unknown|out_of_region",
  "region_gate_status": "region_ok|region_unknown|region_out_of_region",
  "region_signal_source": "surface_metadata_keyword|comment_context_keyword|semantic_region_vector|no_region_signal|...",
  "region_evidence_ru": "human-readable reason",
  "relation_counts": {"vk_comment": 100, "vk_board_comment": 10, "vk_social_wall_post": 5},
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
  "relation": "group_message|linked_discussion|telegram_search|vk_comment|vk_board_comment|vk_social_wall_post",
  "author_id": "best-effort public platform id, empty if not collected",
  "is_post": false,
  "context_url": "...",
  "comment_id": "...",
  "post_id": "...",
  "topic_id": "...",
  "thread_id": "...",
  "created_at": "...",
  "text_snapshot": "short public snippet",
  "analysis_context_snapshot": "source post / parent comment context used for interpretation",
  "source_post_text_snapshot": "bounded source post or VK topic text/title",
  "reply_parent_text_snapshot": "bounded parent comment text when the comment is a reply",
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
- mark source posts (`vk_social_wall_post` / Telegram post rows) as
  `source_post_not_comment` and exclude them from reply examples/LLM budget by
  default, because the current MVP is comment-reply acquisition. They remain in
  artifacts for surface context and future organizer-question strategies.
- mark real-estate-only and medicine-only questions as out of scope
  (`out_of_scope_real_estate`, `out_of_scope_medicine`) unless the same text has
  explicit route/event context. Example: “где снять квартиру?” is out of scope,
  while “снимаем квартиру, куда съездить на один день?” remains a route query.

For semantic scoring the embedded text is the comment plus bounded context when
available:

```text
Текущий комментарий/пост: <comment text>

Контекст для понимания:
Исходный пост/тема: ...
Родительский комментарий: ...
```

This is required because many comments only make sense relative to the source
post or reply parent. The original comment remains separately visible in
`text_snapshot`.

The vector-scan funnel must not run deterministic/regex prefilters before LLM
candidate selection. Every collected row is embedded; `candidate_noise_type`,
`question_signal` and `intent_text_supported` are diagnostic columns only and do
not alter `score_for_rank` or `pre_llm_candidate_eligible` in the default path.
`llm_gate_candidates` are the top semantic rows for the gate model within the
monitoring freshness window. The old deterministic shortlist exists only behind
explicit debug flag `ACQ_COMMENT_RETRIEVAL_DETERMINISTIC_PREFILTER=1` and must
not be used for real discovery runs.

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
embeds the collected comments/post contexts locally, attaches a
`comment_semantic_profile` to each scanned surface that had records, and sends
only top vector-retrieval candidates to the existing Gemma gate. TG linked
discussion comments preserve source post and immediate parent comment when
available; VK wall comments preserve source wall post and same-batch parent
comment when available. Source wall/channel posts are also collected as
`is_post=true` context records so organizer clarification opportunities can be
reviewed with full context.

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
- `comment_retrieval_question_patterns.csv`
- `comment_retrieval_canonical_questions.csv`
- `comment_retrieval_score_distributions.csv`
- `comment_retrieval_manual_review_sample.xlsx`
- `comment_retrieval_speed_metrics.csv`
- `comment_retrieval_report.md`

The XLSX must include these operator-facing sheets:

- `summary_ru`: dashboard-style Russian explanation of what the file means,
  how to read it and what the important limitations are. The first visible
  block is the last-run delta: `+` newly found, `−` removed (currently zero
  unless a future importer records removals), touched/analyzed surfaces,
  selected/candidate/rejected within the delta, comments embedded in the delta
  and answerable questions in the delta. Use the term “площадки”, not the
  literal “поверхности”, in Russian operator text.
- `decision_deltas`: the first working sheet after `summary_ru`; it contains
  only surfaces that were newly discovered, newly analyzed or touched by the
  verified last run. If `KAGGLE_RUN_ID`/`ACQ_SOURCE_RUN_ID` is missing, this
  sheet must explicitly say that the last-run delta is not proven instead of
  showing the whole inventory as a delta.
- `processed_comments_last_run`: one best scored row per latest processed
  comment/post-context in the monitoring window, with `criteria_status_ru`
  explaining whether it matched reply/ask-context criteria or why it was
  rejected. This is the place to audit “what comments were processed last time”.
  It must never put a source post into a column named “Комментарий”: operator
  columns are split into `current_comment_text`, `reply_parent_comment_text`,
  `source_post_text`, `current_post_text`, `evidence_type_ru` and
  `future_goal_ru`. This sheet is diagnostic, not the candidate shortlist: it
  shows `models_matched`/`model_count` and a clear `llm_queue_status_ru` so a
  rejected/noise row cannot visually look like a future-goal candidate only
  because it came from the semantic top-N pool.
- `rejected_noise_examples`: diagnostic noise only. “Lost phone / found items”
  style comments may appear here because negative vectors are used after
  collection/embedding as evidence of filtering; they must not be promoted into
  candidate/surface decision sheets as useful opportunities.
- `summary_counts`: counts by platform/surface type and final status:
  total/selected/candidate/rejected, plus the last-run increment:
  `newly_discovered_this_run`, `increment_touched_this_run` and
  `analyzed_comments_this_run`, plus explicit delta columns for removed
  surfaces, selected/candidate/rejected-in-delta, embedded comments and
  answerable questions in the delta.
- `run_delta_sources`: short, first-read version of the last-run delta with
  only changed/newly touched площадки and clickable links. It exists so the
  operator can immediately see what the latest Kaggle run actually changed
  before opening the full inventory.
- `monitoring_targets`: cumulative “selected/candidate for permanent
  monitoring” sheet. It contains only площадки with confirmed useful semantic
  signal or an already selected status, with live links, last analyzed run/time,
  latest event-question and latest route-recommendation dates, answerable/ask
  counts and member/commenter stats when collected.
- `surface_backlog`: separate “where to look next” sheet for selected surfaces,
  candidate surfaces with signal, and known sources that are still waiting for
  linked-discussion/commentability resolve or a bounded scan. Existing
  Telegram/VK event-monitoring sources should be visible here even before they
  have useful comment profiles.
- `goal_ask_event_details`: short candidate sheet for organizer-owned posts or
  contexts where the future action is to ask a useful clarification about an
  event.
- `goal_reply_events`: short candidate sheet for user comments/questions where
  the future action is to answer about events, afisha/search/listing or quick
  event filters.
- `goal_reply_routes`: short candidate sheet for user comments/questions where
  the future action is to answer with a route/POI recommendation.
- `goal_other_acq`: short candidate sheet for organizer submission/partnership
  acquisition questions. A row may repeat across diagnostic/model sheets, but
  goal sheets must contain only useful vector-selected candidates that still
  require final LLM validation before any real public reply.
- `scope`: run id, stage, model list/gate model, actual comment period,
  platform/surface/relation counts and the configured scan budgets; this answers
  whether the file came from all stored surfaces or from a bounded Kaggle sample.
- `full_surface_list`: full payload/run surface inventory, including surfaces
  where nothing was found, no comments were available, or the surface did not
  enter the retrieval stage.
- `surface_summary`: map for “where to work next” with community/group names,
  live links, member/subscriber count if collected, observed period, unique
  commenters if collected, total comments, normalized comments/day/week/30d/90d,
  latest-100 comment window dates/duration for low-volume or old surfaces,
  answerable questions per 30d/90d/per 100 comments, source-post vs comment
  context counts, freshness status (`latest_comment_at`,
  `latest_event_question_at`, `latest_route_recommendation_at`,
  `days_since_latest_activity`, `freshness_status`), last-run increment status
  (`increment_status`, `discovered_at`, `discovered_in_run_id`), filtered noise
  count and clickable examples. Rows with final `selection_status=selected` are
  filled light green; rows newly discovered/touched in the last run are filled
  light yellow when they are not already selected.
- one `answerable_<model>` sheet per embedding model for successful reply-like
  questions, and one `ask_contexts_<model>` sheet per model for source posts /
  organizer-owned contexts where the acquisition action is to ask a clarification
  ourselves. These sheets must use the same split text columns as
  `processed_comments_last_run`, so it is visually clear whether the row is a
  user comment, a reply-parent comment, or the original post/announcement.
- `question_patterns`: grouped question patterns with counts and examples,
  e.g. route with children, route transport, ticket/price, registration/seats,
  age/children, location/entry, Pushkin/free/accessibility and organizer
  submission. This sheet must contain actual question examples and a
  `canonical_question_ru` template, not generic statements. Historical rows are
  allowed here because their purpose is to build future template questions and
  verify the semantic-control process, not to trigger immediate monitoring.
- `canonical_questions`: global catalog of canonical question templates derived
  from real question-like comments across surfaces. It separates fresh
  monitoring examples from historical calibration examples so the operator can
  see whether the template library is growing from current monitoring data or
  only from old QA/calibration material.
- `intent_catalog`: the complete catalog of model phrases/senses embedded as
  retrieval queries. The per-candidate `top_intent_phrase` is the closest phrase
  from this catalog for the candidate’s `intent_set`.
- `manual_review`: mixed calibration sample with empty human-label columns; it
  is intentionally noisier than goal sheets.

The report must expose the planned future acquisition goal for each candidate:
`event_recommendation_reply` = answer event questions,
`trip_route_poi_recommendation` = answer with route/POI recommendation,
`organizer_visibility_clarification` / source-post contexts = ask event
organizers clarifying questions, `event_site_search_or_listing` = point to
event search/afisha/collections, `organizer_submission_or_partnership` = tell
organizers where to add events or discuss info-partnership, and
`badge_filter_need` = answer with quick event filters such as children/free/
Pushkin-card/accessibility. Sticker strategy remains a separate discovery lane,
not a semantic reply candidate in this workbook.

Human-facing XLSX formatting should use Russian column labels, grouped header
bands, `dd.mm.yyyy` date strings and one-decimal normalized rates where possible.
`discovered_at`/report generation timestamps may include `dd.mm.yyyy HH:MM`
because they are used to see the latest run increment quickly.

Important provenance rule: rebuilding a report from an old Kaggle output may
improve formatting and interpretation, but it does not prove new monitoring.
The `scope` and `summary_ru` sheets must show the source `run_id`,
`KAGGLE_RUN_ID`/`ACQ_SOURCE_RUN_ID`, `source_run_provenance`, report generation
timestamp and configured budgets. Last-run increment counts and yellow
highlighting are allowed only when the source run id is verified. If neither
`KAGGLE_RUN_ID` nor `ACQ_SOURCE_RUN_ID` is present, the report must show
`source_run_provenance=missing_run_id_no_increment_claim`, mark rows as
`no_verified_run_id` / `analyzed_comments_unverified_run`, and keep increment
counters at zero. New VK profile-wall support affects only future scans unless
those profile surfaces already existed in the source Kaggle output.

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
3. surface semantic profiles and monitoring recommendations, including
   normalized depth/rates and whether counts were collected from comments,
   board comments or source posts;
4. speed table by model/batch/max_length/device;
5. funnel table by model/intent/scoring policy;
6. quality/manual review sample or explicit “unlabeled sample only” note;
7. best route/POI, event and organizer examples for **both** embedding models,
   false positives, model disagreement examples and collected question-pattern
   groups;
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
8. the report shows both-model output, speed and candidate differences while
   keeping the configured gate model explicit for the bounded Gemma top-N queue;
9. no full-comment LLM processing is used;
10. no production table is mutated without explicit approval;
11. no external Telegram/VK posting happens.
