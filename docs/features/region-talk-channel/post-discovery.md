# Post discovery — Region Talk Channel

Status: design for MVP-1 candidate-report-only run.

## Monitoring rules

- Scan only `accepted` / explicitly allowed `candidate` sources.
- Incremental only: use `region_talk_source_state.last_seen_post_key`, `last_seen_post_published_at` and `next_fetch_after`.
- Do not read infinite history; MVP caps posts per source and comments/reposts if used as evidence.
- One broken source is recorded as `fetch_status=error|forbidden|unavailable` and does not fail the run.
- No public publish side effect during Post Discovery.

## Region relevance anchors

Anchors are recall/guardrails, **not** final classification:

- Калининград, Калининградская область, Кёнигсберг/Кенигсберг/Königsberg;
- Куршская коса, Балтийское море, Светлогорск, Зеленоградск, Янтарный, Балтийск;
- Черняховск, Гусев, Советск, Неман, Правдинск, Гвардейск;
- Рыбная деревня, остров Канта, Кафедральный собор, Фридландские ворота, форты;
- дюна Эфа, Танцующий лес, Балтийская коса, янтарный край, 39 регион.

Final decision is semantic/vector + verifier, not keyword-only.

## Semantic bank v1

For each class keep: `description`, positive prototypes, hard negatives, candidate-score use, publishable flag, verifier requirement.

### Positive classes

| Class | Description | Publishable | Verifier |
|---|---|---|---|
| `kaliningrad_beautiful_city` | city beauty / atmosphere | yes | top candidates |
| `baltic_sea_atmosphere` | sea, coast, weather mood | yes | top candidates |
| `curonian_spit_nature` | dunes, forest, protected nature | yes | top candidates |
| `european_architecture_vibe` | old European city architecture | yes | top candidates |
| `konigsberg_history_layers` | historical layers / former Königsberg | yes | top candidates |
| `underrated_destination` | surprising/underestimated destination | yes | top candidates |
| `weekend_trip_idea` | weekend route idea | yes | top candidates |
| `resort_towns_positive` | resort towns and promenade value | yes | top candidates |
| `amber_identity` | amber identity/craft/landscape | yes | top candidates |
| `photogenic_places` | strong visual travel places | yes | top candidates |
| `family_trip_positive` | family trip angle | yes | manual if children/faces |
| `romantic_trip_positive` | romantic/calm route angle | yes | top candidates |
| `nature_and_city_mix` | nature + city in one trip | yes | top candidates |
| `route_value` | good route composition | yes | top candidates |
| `food_positive` | gastronomy/cafes/local products | yes | top candidates |
| `unusual_geography` | exclave/geographic uniqueness | yes | top candidates |
| `calm_slow_travel` | slow travel/calm atmosphere | yes | top candidates |
| `cultural_route_positive` | museums/culture route | yes | top candidates |

### Neutral useful classes

`itinerary_practical`, `transport_logistics`, `seasonality`, `prices_practical`, `where_to_stay`, `weather_practical`, `crowd_practical`, `border_region_context`, `how_to_get_there`, `day_trip_route`, `multi_day_route`, `museum_route`, `seaside_route`.

These are publishable only when they add useful context and are not complaint-only. Verifier required before queue.

### Constructive-negative classes

`weather_concern`, `crowds_concern`, `price_concern`, `service_concern`, `transport_concern`, `expectation_gap`, `overtourism_concern`, `parking_concern`, `seasonality_limitation`.

Publishable only as mixed-but-valuable context. Never turn into scandal/news. Verifier + manual review required.

### Disqualifier classes

`news_report`, `incident_crime`, `politics_conflict`, `war_military`, `local_regional_news`, `ad_only`, `low_effort_repost`, `meme_trash`, `negative_only`, `not_about_region`, `duplicate_post`, `source_rights_risk`, `personal_private_profile`, `unsafe_visuals`, `weak_media`.

Any accepted disqualifier should block publication and either reject candidate or put it into debug/report-only with reason.

## Vector recall

MVP recall uses both models as enrichment lanes, not as mutually exclusive candidates:

- `intfloat/multilingual-e5-base` — multilingual semantic lane that often gives broad recall and stable Russian/cross-lingual matches.
- `BAAI/bge-m3` — complementary multilingual semantic lane that can surface additional or higher-quality matches.

For every post, write embeddings and top-K semantic matches for both models, then create a fused candidate set by unioning accepted/top matches and recording per-model scores, margins and the fusion reason (`e5_only`, `bge_m3_only`, `both_models`, `model_disagreement`). Do not discard one model because the other scored higher; use disagreement as a review signal. Heavy embedding runs happen offline/Kaggle; no public hot path calls.

### MVP runtime semantic bank cache

The current Kaggle implementation uses a deliberately finite semantic-bank
prototype list for vector gating. It is versioned as `semantic_bank_v1` and
hashed by exact JSON content so edits invalidate caches automatically.

Runtime labels:

- positives: `ko_visit_impression`, `ko_route_useful`, `ko_visual_place_card`;
- negatives: `other_region_travel`, `multi_region_roundup`, `news_report`,
  `event_announcement`, `ad_or_promo`, `low_substance`.

For each model (`intfloat/multilingual-e5-base`, `BAAI/bge-m3`) the prototype
embeddings are loaded from/saved to YDB as `semantic_bank_embedding` rows. This
means the meanings are prepared once per semantic-bank hash and model, then
reused by later Kaggle runs instead of re-embedding the same prototypes. Per-post
query embeddings are still computed during the run because they depend on the
fresh post text and are not persisted for rejected garbage rows.

### BGE-M3 split worker and geo discriminator

After the July 2026 Kaggle memory tests, the target runtime avoids loading E5
and BGE-M3 in the same main notebook process. The main CandidateReport may keep
the E5 lane, then enqueue/mark rows for a no-Telegram BGE worker. The clean
worker is:

- `kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py`;
- launcher: `kaggle/execute_region_talk_bge_m3_enrichment.py`;
- YDB output kind: `text_vector_enrichment_item`;
- BGE encoder contract: `bge_m3_flagembedding_dense_v1`.

The worker is vectorization-only: it reads compact YDB text/excerpt rows,
computes BGE-M3 dense vectors/scores, writes results back to YDB and exits. It
does not use Telethon, images, Gemini or source discovery.

The non-region discriminator is also vectorized. The BGE worker scores text
against:

- the finite semantic bank above;
- a Kaliningrad Oblast geo bank built from `kaliningrad-place-lexicon-v1.csv`;
- an external Russia geo bank;
- an external country/travel geo bank.

For each row/model it stores KO top score, external top score and margin. Later
fusion can reject multi-region/other-region rows without loading BGE again.
Vector search/scoring over already stored vectors is math-only; model loading is
needed only for new text/bank embeddings.

## Candidate scoring

```text
candidate_score =
  region_relevance_score          * 0.20
+ source_quality_score            * 0.12
+ source_novelty_score            * 0.12
+ text_value_score                * 0.16
+ positive_or_useful_tone_score   * 0.12
+ media_postcardness_score        * 0.20
+ engagement_normalized_score     * 0.04
+ diversity_bonus                 * 0.04
- newsiness_penalty
- trash_penalty
- rights_risk_penalty
- duplicate_penalty
- weak_media_penalty
```

MVP hard gate: main candidates require at least one strong image. Good text with weak media goes only to debug sheet `good_text_weak_media`, not to publication queue.

## MVP-1.x strict post gates

A post must be substantively about Kaliningrad Oblast only. Multiple oblast cities, settlements, natural places or landmarks are allowed; multi-region/country lists are rejected even when Kaliningrad is mentioned positively.

Gate order:

1. freshness (rolling `REGION_TALK_HISTORY_MAX_POST_AGE_DAYS=365`; an explicit
   `REGION_TALK_MIN_POST_DATE` is only an operator override);
2. `kaliningrad_oblast_only_scope_gate` as an LLM-owned semantic decision; the place lexicon only supplies recall/evidence;
3. LLM-owned ad/promo/announcement decision, with keyword/lexicon hints passed only as evidence;
4. LLM-owned content substance / visit-impression decision;
5. LLM-owned not-news / not-trash decision;
6. semantic dual-model enrichment (`intfloat/multilingual-e5-base` + `BAAI/bge-m3`) as recall enrichment/fusion, not A/B comparison;
7. image postcardness scoring only after all text gates pass.

Visual quality is evaluated only for selected non-ad posts about the region. Rows skipped by text gates must expose `visual_scoring_stage=skipped_by_text_gate`, `visual_scoring_skip_reason`, and `image_scoring_cost_saved=true`.

### LLM-first semantic gate ownership

Deterministic regex/keyword/place-lexicon checks are evidence only. They may populate `matched_place_names`, `external_geo_mentions`, `ad_promo_hits`, `text_substance_score` and `semantic_evidence_flags`, but they must not be the final reason to accept/reject region scope, ad/promo status, substance, news/trash or candidate quality. If the LLM semantic gate is not configured, fresh posts must remain `semantic_review_required` and image scoring must stay skipped.
