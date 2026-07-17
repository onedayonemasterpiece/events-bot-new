# MVP candidate report / favorites table

Status: MVP-1 target. Canonical export name: `region_talk_candidates_report`. The first practical result is a cumulative XLSX review workbook the product owner can inspect by eye.

## Storage principle

YDB is the target source of truth. Until the YDB sidecar is wired, the runner uses an explicit dry-run JSON state file (`artifacts/region-talk/state/region-talk-state.json`, or `REGION_TALK_STATE_FILE`) and marks the workbook as `baseline run, not real increment` when that state is absent. The XLSX is generated from persisted state plus the current run delta. The workbook must never be the only persistent storage and must not contain secrets, raw tokens or private raw payloads.

## Artifact paths

Latest convenience artifact:

- `artifacts/region-talk/candidates-latest.xlsx`

Immutable per-run artifacts:

- `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.xlsx`
- `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.csv`
- `data/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.json`
- `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.md`
- `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.html`

Artifacts are not committed unless converted into minimal fixtures later.

## Stable ID and dedupe policy

Stable ids:

```text
source_id    = stable hash(platform + canonical_source_url_or_handle)
post_id      = stable hash(platform + platform_post_key_or_canonical_post_url)
media_id     = stable hash(post_id + media_url_or_phash)
candidate_id = stable hash(post_id + semantic_bank_version + selected_media_fingerprint)
```

Dedupe requirements:

- canonicalize URLs;
- normalize Telegram handles (`@Name` / `https://t.me/Name` / case/spacing variants);
- normalize VK/VK Video handles and owner ids;
- store `text_hash` for posts;
- store perceptual hash for images;
- run semantic duplicate checks for near-identical reposts;
- the same source/post must not create duplicate candidates across runs.

## Two separate funnels

The XLSX must show source discovery and post discovery separately. Do not mix source rejection and post rejection.

### A. Source discovery funnel

```text
seed_source
  → source_candidate
  → source_rejected
  → source_monitor_candidate
  → source_monitored
  → source_discovered_from_graph
```

### B. Post discovery funnel

```text
post_fetched
  → region_relevant
  → non_news
  → strong_media
  → semantic_candidate
  → verifier_candidate
  → favorite
  → ready_for_manual_review
```

## Workbook sheets

### `00_readme`

Explains:

- what this file is;
- `generated_at` and `run_id`;
- how to review rows;
- status meanings;
- that MVP-1 never autopublishes.

### `01_run_summary`

Columns:

- `run_id`, `started_at`, `finished_at`, `git_sha`, `branch`, `config_profile`, `dry_run`, `ydb_namespace`;
- `seed_file_version`, `source_count_seeded`, `source_count_scanned`, `posts_fetched`, `posts_region_relevant`, `posts_with_strong_media`;
- `candidates_created`, `favorites_created`;
- `dropped_news`, `dropped_trash`, `dropped_not_region`, `dropped_weak_media`, `dropped_duplicate`, `dropped_rights`;
- `llm_calls`, `image_model_calls`, `errors_count`, `artifact_paths`.
- `increment_state_loaded`, `previous_run_id`, `previous_seen_post_count`, `new_posts_this_run`, `changed_posts_this_run`, `unchanged_posts_this_run`, `state_write_status`.
- `source_count_selected`, `source_count_attempted`, `source_count_ok`, `source_count_skipped`, `source_count_error`, `vk_wall_probe_status`.
- `telegram_governor_enabled`, request/cache/FloodWait counters, `telegram_cooldown_active`, `telegram_degraded_mode`, `telegram_similar_channels_status`, `telegram_similar_channels_*`, `source_frontier_unique_count`, `git_sha`, `git_branch`, `git_dirty`.

### `02_increment`

Purpose: show what changed since the previous run.

Columns:

- `entity_type=source|post|media|candidate`
- `entity_id`
- `source_title`
- `post_url`
- `first_seen_run_id`
- `previous_run_id`
- `current_run_id`
- `first_seen_at`
- `last_seen_at`
- `seen_run_count`
- `previous_stage`
- `current_stage`
- `stage_transition`
- `new_this_run`
- `changed_this_run`
- `change_reason`
- `candidate_score_previous`
- `candidate_score_current`
- `candidate_score_delta`
- `media_score_previous`
- `media_score_current`
- `media_score_delta`
- `manual_review_status`
- `next_action`

It must answer: what appeared for the first time, what changed status, what became candidate/favorite, what dropped and why, which sources are new, which sources gave strong photos, which posts repeated, and which posts need human decision.

### `03_funnel`

Columns:

- `stage`
- `current_run_count`
- `previous_run_count`
- `delta`
- `total_cumulative`
- `top_rejection_reasons`
- `notes`

### `04a_final_shortlist`

Product-facing shortlist for human eye review. It should stay compact: source, date, post URL, short summary, why it is about Kaliningrad Oblast, why it is useful/interesting, image readiness and the main reject/blocking reason. Full engineering/debug columns remain in `04_review_queue` and `04a_final_shortlist_raw`.

Rows may enter this sheet only when the semantic gate accepted the post and image state is honestly labelled. A row with weak media can be useful for review but must not be labelled `reviewable_image`.

### `04a_final_shortlist_raw`

Uncompacted raw rows behind `04a_final_shortlist`, kept for debugging score/gate details without making the main review screen unreadable.

### `04p_publication_queue`

Product-goal queue for the current 20-link objective. Rows are built by joining
candidate memory/text-vector/source evidence with `RegionTalkImageDiagnostic`
actual-image scores. This sheet is ranked top-down by publication score and
shows `publication_candidate_status`, Gemini Lite verifier decision, visual/text
scores, diversity penalty, `goal_stop_candidate`, and `sent_to_chat`.

### `04q_publication_confirmed`

Confirmed subset of `04p_publication_queue`: Gemini-accepted rows and rows that
have already been sent to the operator chat. This is the lightweight shortlist
to inspect when the objective is “find 20 strong post links”, not a public
auto-publish queue.

### `04_review_queue`

Main human review screen. Columns must stay short and readable:

- `rank`
- `status_badge`
- `new_or_seen`
- `source_title`
- `source_type`
- `platform`
- `post_date`
- `post_url`
- `source_url`
- `short_summary`
- `why_this_is_about_kaliningrad`
- `what_positive`
- `what_neutral_or_useful`
- `what_concern`
- `best_photo_preview`
- `postcardness_score`
- `aesthetic_score`
- `technical_quality_score`
- `region_visual_relevance_score`
- `publication_safety_score`
- `image_model_report_short`
- `candidate_score`
- `rights_policy`
- `risk_flags`
- `suggested_action`
- `manual_decision`
- `reviewer_comment`

`status_badge` values:

- `NEW`
- `STILL_GOOD`
- `IMPROVED`
- `DROPPED`
- `NEEDS_REVIEW`
- `READY`
- `BLOCKED_RIGHTS`
- `WEAK_MEDIA`
- `DUPLICATE`

### `05_favorites`

Stable shortlist accumulated across runs.

Columns:

- `favorite_id`, `candidate_id`, `first_seen_run_id`, `last_seen_run_id`, `seen_run_count`;
- `source_title`, `post_url`, `short_summary`, `why_selected`, `best_image_preview`;
- `candidate_score`, `review_status`, `manual_decision`, `publication_readiness`, `notes`.

### `06_candidates_all`

All current and historical candidates from YDB state, including candidates that are no longer active.

### `07_new_posts_this_run`

Only posts whose stable `post_id` was first seen in this run, before later filters remove some of them. Repeated posts from the current fetch do not belong here.

### `07_current_run_posts`

All posts fetched/observed in the current run, including repeats already known from previous state.

### `08_dropped_posts`

Columns:

- `post_id`
- `source_title`
- `post_url`
- `drop_stage`
- `rejection_reason`
- `model_reason`
- `newsiness_score`
- `trash_score`
- `region_relevance_score`
- `media_score`
- `duplicate_of`

### `09_image_quality`

One row per media item. This sheet must show the model report, not only numbers.

Columns:

- `media_id`
- `candidate_id`
- `post_url`
- `image_url_or_local_path`
- `thumbnail`
- `technical_quality_score`
- `aesthetic_score`
- `postcardness_score`
- `region_visual_relevance_score`
- `publication_safety_score`
- `low_noise_score`
- `overall_media_score`
- `is_selected_for_publication`
- `recognized_visual_elements`
- `contains_text_overlay`
- `contains_watermark`
- `contains_large_people_faces`
- `contains_news_or_incident_visuals`
- `model_short_explanation`
- `failure_reason`
- `model_id`
- `model_version`

### `09a_image_candidate_queue`

Single persistent queue for posts/images that need actual media acquisition or
actual-image scoring. It is separate from the source queue and has its own
cursor/batch marker. This queue is downstream of text/vector region gates: rows
are admitted only when the post is text-confirmed as about Kaliningrad Oblast,
non-ad, not a multi-region/other-region roundup, and not vector-rejected. Old
queue rows without this proof are pruned on the next state write, so examples
such as Krasnodar/Buryatia official posts must not be sent to image diagnostics.

Rows include `image_queue_order`, `cursor_marker`,
`selected_for_next_image_batch`, `image_queue_status`,
`text_region_confirmation_status`, `kaliningrad_oblast_only_scope`,
`kaliningrad_mention_role`, `vector_gate_status`, `vector_content_type`,
`matched_place_names`, `external_geo_mentions`, `media_acquisition_status`,
`media_acquisition_error_type`, post/source URLs and the latest scoring fields.
These proof fields are part of the compact YDB allow-list, so queue rows remain
auditable after state round-trips. The default human-sized processing target is
`REGION_TALK_IMAGE_QUEUE_TARGET_PER_RUN=30`.

### `09d_image_driven_top`

Image-first review sheet sorted by actual-image model results, not metadata
fallback. It lists the best scored images with their source post URL so a human
can inspect top visual candidates directly or later compare an alternative
neural scorer. Metadata-only rows must not appear here as successful image
quality decisions.

### `10_good_text_weak_media`

Good semantic candidates blocked because images are weak. These never enter the main publication queue in MVP-1.

### `11_sources_seed`

All rows loaded from [`seed-sources-v1.csv`](seed-sources-v1.csv), with normalized handles/URLs and source ids.

### `12_sources_discovered`

New source candidates discovered from links, mentions, forwards, repost attribution, catalogs, source descriptions, public travel-blogger workbooks and Telegram similar-channel recommendations.

### `12a_source_frontier_unique`

De-duplicated source frontier across seeds, link graph, public travel-blogger workbook imports and Telegram similar-channel recommendations. This is the main place to inspect what could be added to monitoring next. Public fields may include `private_state_key` for matching back to private state, but must not include Telegram `channel_id`, `access_hash`, session strings or tokens.

### `12_source_queue`

Canonical product source queue. This is the single reviewer-facing URL list and
must contain **only** Telegram channels and VK community/wall URLs. It is
deduped by `canonical_source_key`, carries immutable admission `queue_seq`, a
legacy `queue_order` cursor/display field and a separate priority lane, and exposes
`added_at`, `added_from`, `source_queue_status`, `status_color_hint`,
`last_scan_status`, `posts_scanned`, `ko_posts_found`,
`candidate_posts_found`, `actual_images_scored_count`,
`avg_actual_image_score`, `source_image_quality_status`,
`monitoring_exclusion_reason` and `next_action`.

Strict source URL policy: Telegram rows must be public channel roots like
`t.me/<channel>` only; Telegram post/search/view URLs (`t.me/<channel>/<id>`,
`t.me/s/...`, `tgstat.ru/search?...`) are not durable sources. VK rows must be
community/wall roots like `vk.com/<domain>`, `vk.com/club...`,
`vk.com/public...` or wall owner URLs; VK media/search/result pages such as
`vk.com/video`, `vk.com/video-*`, `vk.com/clip-*`, `vk.com/photo-*` are not
durable sources. Non-source URLs are skipped from `12_source_queue` and surfaced
through diagnostics/quarantine only.

Insertion policy:

- existing queue order is preserved;
- new Telegram sources discovered by Telegram keyword search are inserted
  immediately after the saved cursor so they are scanned next;
- catalog/static/similar/link discoveries are appended to the tail;
- non-Telegram/VK URLs and TG/VK non-source pages stay out of this sheet and may
  only appear in diagnostic quarantine sheets.

A source with Kaliningrad-region candidate posts can still be excluded from the
future monitoring-candidate pool when actual-image evidence is systematically
weak. The default rule marks `source_image_quality_status=exclude_low_image_quality`
when at least `REGION_TALK_SOURCE_IMAGE_MIN_ACTUAL_SCORED=3` actual images have
been scored and their average is below
`REGION_TALK_SOURCE_IMAGE_MIN_AVG_SCORE=0.55`; the row remains in the queue for
auditability, with `monitoring_exclusion_reason` explaining the exclusion.

Visual colors are part of the XLSX contract: white = pending scan, yellow =
retry/rescan needed or low image-quality exclusion, green = processed with
Kaliningrad/candidate hits, red = processed with no Kaliningrad hits, blue =
cursor marker.

### `12b_telegram_similar_channels`

Raw Telegram similar/recommended-channel evidence for the current run: seed channel, recommendation rank, public username/title/url when available, method status, errors and whether the row was added to the frontier. If Telethon lacks the required API request, this sheet must still exist with `method_status=not_supported_by_telethon_version`.

### `12d_similar_seed_queue`

Persistent queue of resolved Telegram channels that can become seeds for the next similar-channel discovery pass. It is fed by every resolved/fetched Telegram source, prioritizes sources that yielded candidate-memory rows, and must stay read-only/no-auto-join in MVP.

### `13b_source_delta_scan`

Per-source delta/cursor sheet: previous/current history fetch run, fetch mode, new-source flag, last seen post date and per-run scanned counts. This is the visible audit trail for frontier→scan promotion and cached-history reuse.

### `13_sources_monitored`

Actually scanned sources, cursors, fetch status, errors and next fetch timestamps.

For MVP-1.z this sheet must also include selected-but-not-fetched rows with explicit statuses such as `skipped_vk_wall_not_configured`, `skipped_vk_wall_not_implemented`, `skipped_vkvideo_auxiliary_not_implemented`, or `skipped_unsupported_platform`; enabled non-Telegram sources must not silently disappear from coverage.

### `14_verifier_reports`

Gemini/VLM structured decisions, policy version, model id and cache key.

### `19_image_model_observability`

Per-run image model/runtime summary:

- `image_scoring_mode`
- `image_model_id`
- `image_model_version`
- `image_model_type`
- `image_model_runtime`
- `image_model_input_type`
- `image_model_device`
- `rows`
- `actual_image_bytes_required`
- `fallback_note`

### `20_telegram_rate_observability`

Per-run Telegram request-governor summary: resolve cache hits/network resolves, history requests, media downloads, recommendation requests, FloodWait counts/max seconds, cooldown/degraded-mode flags, configured caps and ledger path. The P0 requirement name `20_telegram_rate_limit_observability` is longer than Excel's 31-character sheet limit, so the implemented workbook uses the Excel-safe `20_telegram_rate_observability`.

### `24_source_yield_metrics`

Yield per scanned source, normalized as `per_1000_scanned_sources`: sources scanned, new sources scanned, sources with fresh Kaliningrad posts, non-ad Kaliningrad posts, candidate-memory posts, actual-image candidates and publication-ready candidates. These metrics are directional because the source sample is priority/frontier-biased, not random.

### `15_manual_decisions`

Manual review import/export contract.

Columns:

- `candidate_id`
- `manual_decision=favorite|reject|approve_for_preview|approve_for_queue|block_source`
- `reviewer`
- `reviewed_at`
- `reviewer_comment`
- `rights_override`
- `source_status_override`

MVP-1 may skip importing this sheet, but the export/import shape must already be documented.

### `16_publish_preview_future`

Future-only preview of Telegram/VK text/card readiness. No real publishing in MVP-1.

## Workbook UX requirements

- Freeze first row.
- Enable filters.
- Make hyperlinks clickable.
- Use short text columns plus references to full JSON reports.
- Apply conditional formatting by `status_badge`.
- Include thumbnail/local preview columns where technically possible.
- Protect formula/header rows if easy.
- Keep sheet names stable.
- No macros.
- No secrets/raw tokens/raw private payloads.
- Do not embed full-size images if the workbook becomes too heavy; use thumbnails/local artifact refs.

## Strong media gate

Main favorites require strong photos. Without strong photos a post may be shown only in `10_good_text_weak_media`, not in main publication queue.

## MVP-1.x workbook additions

`04_review_queue` and candidate sheets include strict gate evidence:

- `kaliningrad_oblast_only_scope`, `matched_place_names`, `matched_place_types`, `matched_place_priority_tiers`, `matched_place_aliases`, `external_geo_mentions`, `region_scope_reason`;
- `is_forwarded_or_repost`, `forwarded_from_source_title`, `forwarded_from_url`, `original_source_candidate_id`, `discovery_edges_count`;
- `text_substance_score`, `visit_impression_score`, `useful_route_score`, `emotion_observation_score`, `memorable_details_score`;
- `visual_scoring_stage`, `visual_scoring_skip_reason`, `image_scoring_cost_saved`.

`08_dropped_posts` includes `drop_gate`, `rejection_reason`, `is_ad_or_promo`, `post_age_days`, `text_substance_score`, and `image_scoring_skipped`.

`12_sources_discovered` includes explicit links, forwarded/repost origins, normalized URL, platform guess, edge type, source status and confidence. `12a_source_frontier_unique` deduplicates next-source candidates and `12b_telegram_similar_channels` isolates the Telegram recommendation evidence.

`13_sources_monitored` includes source profile probe fields: sampled post count, Kaliningrad hit count, ad/news/trash ratios, original-media score, link/forward richness and monitor priority score.

New sheets:

- `12a_source_frontier_unique`;
- `12b_telegram_similar_channels`;
- `17_source_graph_edges`;
- `18_place_lexicon_matches`;
- `20_telegram_rate_observability` (Excel-safe name for the longer P0 rate-limit observability sheet).

## MVP-1.y LLM-first review queue update

`semantic_gate_not_run` is no longer a terminal semantic reject. Fresh rows that need semantic judgment but are not model-reviewed in the current budget become `pre_candidate_needs_llm` and stay visible in `04_review_queue` plus `14b_pre_candidates_needing_llm`.

`03_funnel` is sequential. Independent evidence counts moved to `03b_gate_counts`.

Image rows in `09_image_quality` are created only after `llm_decision=accept`; pre-candidates keep `visual_scoring_stage=skipped_by_text_gate` and `image_scoring_cost_saved=true`.

## MVP-1.z Supabase-limited LLM + product shortlist update

LLM semantic calls must use the shared `google_ai` Supabase limiter (`google_ai_reserve`, `google_ai_mark_sent`, `google_ai_finalize`) with fallback disabled. The report no longer treats `REGION_TALK_MAX_LLM_CALLS` or a direct `REGION_TALK_GOOGLE_API_KEY_ENV=GOOGLE_API_KEY2` path as authoritative. Runtime summary fields must expose `llm_limit_source`, `llm_provider`, `llm_model`, `llm_default_env_var_name`, Supabase model caps, `llm_calls_ok`, `llm_calls_error`, `llm_quota_errors`, and `llm_retry_rows`.

Human review starts at `00_product_summary` and `04a_final_shortlist`. `04_review_queue` remains a wider engineering queue. Obvious pre-LLM cost-guard rejects are moved to `04c_debug_rejects`; quota/provider failures are moved to `04b_needs_llm_retry` and `14c_llm_errors`.

Image quality claims must distinguish:

- `image_model_input_type=actual_image`, `image_model_runtime=kaggle_local`, `image_model_type=clip` — real Kaggle-local neural scoring;
- `image_model_input_type=metadata_only`, `image_model_type=cv_only` — fallback/debug only, not proof of image-quality model success.

Image gating is split into `image_reviewable=true` vs `image_publication_ready=true`; a row can be useful for human review without being auto-ready for publication. Rows with LLM-accepted text but `image_reviewable=false` must not be labelled `reviewable_image` in `04a_final_shortlist`; they go to `10_good_text_weak_media`.

## MVP-1.z2 coverage/increment/gating tightening

- `REGION_TALK_MAX_SOURCES=30` should select up to 30 seed sources by priority, not only the 7 `monitoring_enabled=true` Telegram rows. Telegram sources can be fetched; VK/VKVideo/web rows must appear with honest skipped/not-configured/not-implemented statuses until real fetchers are implemented.
- The ad/promo detector must use bounded/contextual patterns. Examples: `руб` must not match `рубрика/рубрике`, and `тур` must not match `туризм/культура/архитектура`. Hard promo rejects are explicit prices, tickets, registrations, contests, paid tours/services, app downloads and advertising labels. Possible promo rows with strong content can still be sent to the LLM final semantic gate.
- Product-facing reject reasons should be concrete: `reject_old_post`, `reject_not_kaliningrad_oblast_only`, `reject_ad_or_promo`, `reject_multi_region_roundup`, `reject_low_substance`, `reject_news_or_trash`, `reject_source_boilerplate`.
- `03_funnel` is the sequential funnel; independent evidence counts belong in `03b_gate_counts`.
- Ambiguous place names that require context may become region evidence only when nearby context contains strong Kaliningrad-oblast anchors such as `Куршская коса`, `Калининградская область`, municipality/district wording, or route/trip context.


## MVP-1.z3 human-like Telegram discovery + source frontier update

- The runner imports `public_travel_blogger_channel_links.xlsx` as source-frontier evidence only; catalog rows are not automatically monitored or published.
- Telegram fetch/discovery uses a cache-first request governor with conservative defaults: at most 3 network username resolves, 8 history sources and 20 media downloads per run unless explicitly changed. Large FloodWait values create cooldown/degraded-mode evidence instead of long sleeps.
- Similar channels are discovered through Telegram recommendations for already-resolved seed channels and are written as `telegram_similar_channel` graph/frontier rows. Unsupported Telethon versions must be reported as `not_supported_by_telethon_version`, not silently replaced by scraping.
- Product review starts from `00_product_summary`, compact `04a_final_shortlist`, `12a_source_frontier_unique`, `12b_telegram_similar_channels` and `20_telegram_rate_observability`.
- Workbook and companion artifacts include git provenance so Kaggle results can be traced back to the exact runner code.


## MVP-1.z4 cumulative memory and honest increment update

Region Talk is a cumulative discovery/research process, not a single-run-only report. A previously found candidate must not disappear only because its source was not refetched in the current run. The dry-run state now persists `candidate_memory` next to post/source/frontier state.

New/clarified workbook sheets:

- `04a_current_run_shortlist` — compact shortlist from the current run only; `04a_final_shortlist` is the cumulative active candidate-memory shortlist.
- `04p_publication_queue` / `04q_publication_confirmed` — product-goal ranked
  queue and Gemini-confirmed shortlist built after ImageDiagnostic actual-image
  scoring.
- `06a_candidate_memory` — all accumulated candidate-memory rows, including text-accepted/image-pending/good-text-weak-media/publication-ready/manual-keep rows.
- `06b_candidate_memory_top` — active cumulative candidate memory for human review.
- `07b_prev_candidates_not_refetch` — Excel-safe name for previous candidates not refetched this run; the longer requested name does not fit Excel's 31-character sheet limit.
- `12c_source_frontier_queue_next` — ranked next-source queue with P0/P1/P2/P3 promotion class and planned action.
- `21_manual_review_queue` — cumulative human-readable queue across active memory buckets.
- `22_candidate_deltas` — candidate-level delta buckets such as `new_to_system`, `re_seen`, `not_refetched_this_run`, `stage_upgraded`, `stage_downgraded`, `became_candidate`, `expired_by_policy`.

Summary metrics include post memory totals, candidate memory totals/active/new/retained/not-refetched/upgraded/downgraded/expired, source frontier promoted/ready-next/deferred, and cache-first Telegram entity metrics. If `04a_current_run_shortlist` is empty but active candidate memory exists, `00_product_summary` points the reviewer to `06b`/`21`.

Image gate correction: metadata-only image fallback is never a final visual accept/reject. It becomes `image_fetch_retry_needed` / `needs_actual_image_fetch` / `visual_decision=pending` with `next_action=retry_media_download_or_manual_open`.

Semantic bucket correction: the LLM prompt/schema separates multi-region roundup from `single_location_photo_card`. A fresh Kaliningrad-only single-location card may be a research/image-pending candidate even when it is weaker than a firsthand visit impression.

## MVP-1.z5 vector-first / final-LLM correction

Region Talk text quality is now **local/vector-first**. Broad fetched/current-run/review-queue rows must not be classified by LLM. The wide funnel uses freshness, Kaliningrad-only scope, deterministic ad/event evidence and `news_event_vector_gate` / prototype-vector fields; actual image scoring can run locally before any final LLM verifier.

LLM usage is allowed only for compact final verifier/tie-breaker stages (`final_publication_verifier`, `final_human_review_explainer`, `ambiguous_final_tiebreaker`, retry of a previous final verifier) and must remain Supabase-limiter controlled. Default config is `REGION_TALK_ENABLE_EARLY_LLM=0`, `REGION_TALK_ENABLE_VECTOR_GATES=1`, `REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS=1`, `REGION_TALK_MAX_LLM_FINAL_VERIFY=10`, `REGION_TALK_LLM_CALL_TIMEOUT_SECONDS=60`, `REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS=1800`, with `GOOGLE_AI_PROVIDER_TIMEOUT_SEC` propagated to the same value unless explicitly overridden. Timeout is a structured `llm_gate_status=error` row outcome, not a notebook hang. The prompt must use compact text/summary fallback and slim image/vector evidence only. XLSX observability exposes `wide_funnel_llm_calls`, `final_verifier_llm_calls`, vector reject counts, `actual_image_scored_before_llm_count`, and `14d_llm_usage_by_stage`; acceptance requires `wide_funnel_llm_calls=0`.

For the image-queue CandidateReport phase, historical candidate-memory vector
rechecks are disabled by default (`REGION_TALK_MEMORY_VECTOR_RECHECK_MAX_ROWS=0`,
`REGION_TALK_MEMORY_VECTOR_RECHECK_BATCH_EMBEDDINGS=0`) so the job does not run a
second dual-embedding pass after already scoring the current posts. A final
publication pass may explicitly enable a small recheck limit after
ImageDiagnostic has written `actual_scored` rows.

The live-YDB handoff to ImageDiagnostic is earlier than the XLSX/report tail.
After the bounded text/vector scoring loop, CandidateReport writes
`image_queue_item` rows and an image queue cursor immediately
(`REGION_TALK_LIVE_IMAGE_QUEUE_HANDOFF_EARLY=1`). For the bounded discovery
pass, `REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF=0` is the default:
once the image queue is durable in YDB, the notebook still continues through
the source-frontier/source-queue tail so similar-channel and keyword-search hits
are promoted behind the source cursor for real history scans. Operator-only XLSX
artifacts remain best-effort and must not block ImageDiagnostic, but source queue
updates are a production stage. After source queue rows and the cursor are
durable, bounded live runs may exit with
`report_tail_skipped_after_live_source_queue_handoff`
(`REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF=1`).

In live-YDB mode the report tail is not a required product stage. The durable
contract is:

```text
CandidateReport text/vector rows + image_queue_item
  → RegionTalkImageDiagnostic actual_scored image_queue_item rows
     → bounded complete-album visual adjudication for strict dual-vector rows
       stranded only by the uncalibrated legacy image score
  → publication finalizer reads live YDB, verifies top actual-image rows,
     writes publication_candidate_item, and optionally exports a lightweight
     XLSX/CSV review artifact
```

The bounded visual adjudicator is not a replacement for E5+BGE or the final
Gemini text/product verifier. Admission requires current fused
`fused_e5_bge_m3`, `vector_accept_candidate`, current source/text/publication
eligibility, actual images, complete acquisition and an unchanged media
manifest. It sees all album images but no post text. Only a structurally
consistent versioned `accept` can recover the row as `vlm_visual_accept`;
`reject`, `review`, provider failures and budget deferrals remain non-terminal
for operator/calibration review. Calls share the finalizer's durable <=100
Region Talk budget and are capped at two new requests per image run by default.
Completed unchanged requests are replayed by fingerprint and never paid twice.
Repeated YDB polls of the same VLM-review row count once in the run heartbeat.
The ImageDiagnostic eligibility audit also separates inspection from mutation:
historical blocked rows are persisted only for a material status, eligibility
or retained-evidence transition. A changed `checked_at`, current run id or
queue update timestamp is not a reason to rewrite the image ledger.

`build_report(...)` may still be the historical function name around the
bounded scoring code, but product completion must be judged from live YDB rows:
`candidate_memory_item`, `image_queue_item`, `publication_candidate_item`,
queue cursors and business events. A missing or skipped XLSX/build-report tail
is acceptable after `early_image_queue_handoff_done`; a missing
`image_queue_item` handoff is not acceptable because ImageDiagnostic then has no
work item.

Automated orchestration uses the narrower
`REGION_TALK_WRITE_REPORT_ARTIFACTS=0` contract. Unlike the older early-return
flags, it is checked only after publication/source/image queues and compact YDB
state are durable. The run completes with minimal `output.json` and
`stage_status.json`, empty `xlsx_path`/`latest_xlsx`, and no XLSX/CSV/full
JSON/Markdown/HTML export. Standalone/manual audit runs retain the backward-
compatible default `1`.

`04a_final_shortlist` is now the cumulative active candidate-memory shortlist, while `04a_current_run_shortlist` remains current-run-only. Metadata-only/CV-only image fallback is never `publication_ready` or `reviewable`; rows needing real media bytes go to `09b_image_fetch_retry_queue`. `low_priority_defer` is replaced by `frontier_stage` values such as `unresolved`, `probe_due`, `history_due`, `vk_not_configured`, `unsupported`, and `inactive_low_quality`.

## MVP-1.z6 throughput / hard-region / recursive discovery correction

The default Telegram governor is raised for the next MVP run: `REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN=300`, network resolves `8`, history sources `40`, media downloads `60`, recommendation calls `20`, similar-channel seed channels `20`, similar frontier cap `150`, and dynamic frontier probes `30`. The XLSX exposes `history_sources_target`, attempted/ok/new/cached/network counts, runtime seconds and posts-per-source distribution.

Every resolved Telegram channel can be persisted in `12d_similar_seed_queue`; previous frontier rows can be promoted into dynamic seeds for shallow probe/history scan, and `13b_source_delta_scan` records source-level cursor/delta state. The product acceptance target for the next real run is `sources_history_fetched_ok >= 25`; if the run cannot hit it, `00_product_summary`/`20_telegram_rate_observability` must explain the governor/FloodWait/cache blocker.

For bounded live-YDB handoff runs, fetched posts are pre-ordered before the vector pass by Kaliningrad place evidence and media presence (`REGION_TALK_PRIORITIZE_REGION_TEXT_BEFORE_VECTOR=1`). This is only a runtime-budget priority so the embedding/vector scorer sees likely region posts first; it is not the final acceptance rule.

Hard Kaliningrad-only region scope now runs before candidate memory and `04a_final_shortlist`: multi-region/non-Kaliningrad posts are rejected as `reject_not_kaliningrad_oblast_only` and cannot leak into product shortlist unless manually overridden with explicit region evidence. Image scoring remains only for selected non-ad Kaliningrad rows; broad LLM stays disabled (`wide_funnel_llm_calls=0`) and the optional LLM verifier is limited to top-N final rows through the Supabase limiter.

The external-region/country safety evidence is matched in ordinary Russian
case forms, not only in the canonical nominative dictionary form. For example,
`в Дагестане`, `в Новосибирской области`, `в Нижнем Новгороде`, `через Польшу`
map back to their exact allow-listed canonical geo terms and make a mixed
destination post fail the KO-only gate before image/Gemini spend. This is a
narrow inflection expansion of known external geographies, not a regex-based
positive region classifier: adjective-derived unrelated words such as
`дагестанская кухня` or `польская плитка` do not become external destinations.
The policy version is `region_talk_post_processing_v4_external_geo_case_forms`
so active stale rows receive the normal idempotent policy refresh; E5+BGE
remains mandatory and unchanged.

The delayed E5→BGE path has the same pre-image safety contract as the direct
path. Candidate memory retains hard ad/promo and multi-region evidence while it
waits for BGE. When BGE arrives, the promotion pass reuses the complete active
E5 text rather than the 180-character summary; explicit hard registration,
booking, price or promo evidence becomes `vector_reject_ad_promo` and never
enters ImageDiagnostic. This does not replace or weaken dual-vector scoring—it
prevents compact queue state from erasing an already known safety verdict.

## MVP-1.z7 growth discovery / honest increment update

The runner now has explicit process modes:

- `REGION_TALK_DISCOVERY_MODE=mixed|similar_only|keyword_only|off`;
- `REGION_TALK_HISTORY_SCAN_MODE=primary_and_delta|delta_only|primary_only|off`;
- `REGION_TALK_MEDIA_SCORING_MODE=retry_queue_first|top_text_candidates|off`.

Kaggle state is still JSON-sidecar in MVP, but it is versioned as `region-talk-state-v2`, exposes `previous_state_loaded`, `previous_state_source`, `previous_state_run_id`, `previous_state_hash`, and writes a `latest-region-talk-state.json` pointer with `latest_state_hash`. If `REGION_TALK_REQUIRE_PREVIOUS_STATE=1`, a missing previous state fails instead of silently becoming a baseline run.

Similar-channel discovery is cursor-like rather than recursive: processed seeds get `similar_seed_last_used_at`, `similar_seed_use_count`, result/unique counts and `similar_seed_next_allowed_at`; self-loops are rejected. z7 defaults target 100 similar seeds/run, 1000 new frontier rows/run and 100 history sources/run when FloodWait/cooldown allows.

Telegram keyword discovery adds source candidates only, never accepted posts. It searches bounded Kaliningrad toponyms and writes `12e_telegram_keyword_discovery` rows with `edge_type=telegram_keyword_search`; public post text is not retained in the keyword sheet.

Source classification is visible in `12f_source_classification`, `13_sources_monitored`, candidate memory and shortlists: `source_geo_class`, `source_topic_class`, `ko_mention_ratio_recent`, `travel_blogger_score`, `personal_voice_score`, `nonlocal_value_score`, `source_priority_reason`. Current-run/cumulative shortlists are sorted higher for nonlocal blogger visit/impression evidence.

The local/vector-first visit classifier now fills `has_firsthand_visit_evidence`, `visit_evidence_type`, `first_person_markers`, `emotion_or_impression_evidence`, `review_or_opinion_evidence`, `useful_route_evidence`, `nonlocal_blogger_visit_score`, and `publication_story_score` before any final LLM verifier. Metadata-only media is still never publication-ready.

Incremental run-event artifacts are written next to the XLSX: `run_events.jsonl`, `candidate_found.jsonl`, and `stage_status.json`. `05_favorites` and `06_candidates_all` now use `_sheet_note` placeholders and expose `favorites_candidates_consistency_status` so placeholder rows are not confused with real candidates.

## MVP-1.z8 product-acceleration contract

The z8 runner treats storage backend selection as an explicit product signal:

- `REGION_TALK_STATE_BACKEND=ydb` requests authoritative YDB state; JSON files are backup/export only.
- `REGION_TALK_REQUIRE_YDB_STATE=1` fails the run when YDB state cannot be read or written.
- If YDB is requested without the required config and fail-fast is disabled, the workbook must say `state_backend=json_fallback`, `state_fallback_used=true`, and include `ydb_read_status` / `ydb_write_status` / `state_fallback_reason`; it must not silently look like production state.

Source frontier is canonical-key based. Catalog imports, Telegram similar-channel recommendations, keyword discovery, post links and forwarded origins should upsert into one deduplicated frontier/source view. Required KPI fields include `catalog_import_rows_total`, `catalog_import_unique_sources`, `catalog_sources_in_authoritative_frontier`, `frontier_duplicate_canonical_keys`, `frontier_self_loops`, `telegram_similar_seed_used`, `telegram_similar_raw_count`, `telegram_similar_unique_count`, `keyword_queries_processed` and `keyword_unique_sources`.

The JSONL candidate event contract is stage-first. `candidate_found.jsonl` may contain `new_source_with_ko_post`, `fresh_ko_post_found`, `pre_candidate_created`, `image_reviewable_candidate`, `publication_ready_candidate` and `image_fetch_retry_needed` with `run_id`, `event_at`, `source_id`, `source_title`, `source_url`, `post_id`, `post_url`, `post_date`, `matched_place_names`, `content_type`, `candidate_score`, `media_score`, `stage`, `next_action` and `short_summary`. These are internal review/reporting events only; public publication remains disabled.

Every XLSX/summary must expose product acceleration KPIs for state, discovery, scanning, delta, conversion per 1000 scanned sources, product shortlist/memory/favorites and actual-image retry. Conversion metrics must carry `sample_bias_note` because current runs are priority-biased, not random samples.

## MVP-1.z9 unified queue correction

The product source queue is now `12_source_queue`, not the diagnostic frontier
tabs. `12a_source_frontier_unique`, `12c_source_frontier_queue_next`,
`12e_*` and `12g_external_links_quarantine` remain engineering/debug evidence,
but reviewer acceptance starts with `12_source_queue`.

Summary metrics include `source_queue_total`, pending/processed/retry counts,
`source_queue_cursor_position`, `source_queue_keyword_inserted_this_run`,
`source_queue_catalog_sources_total`, `source_queue_telegram_total`,
`source_queue_vk_total`, `source_queue_pending_telegram_total`,
`source_queue_pending_vk_total`, `source_queue_non_target_skipped_this_run`,
`source_queue_low_image_quality_excluded_total`,
`source_queue_rejected_high_volume_total`, `source_queue_only_telegram_vk`,
`source_queue_only_target_source_urls`, `image_queue_total`, `image_queue_cursor_position`,
`image_queue_target_this_run`, `image_queue_selected_next_batch`,
`image_queue_actual_scored_total`, `image_queue_needs_actual_fetch_total`,
`image_queue_pruned_non_region_previous`,
`image_queue_rejected_non_region_inputs` and
`image_queue_text_region_confirmed_total`.

Kaggle launcher safety: per-run private input datasets use hash-suffixed slugs
to avoid stale dataset reuse, copy the public blogger workbook from the
canonical artifact path when running in a linked worktree, and delete temporary
`region-talk-config-*` / `rt-secret-bundle-*` datasets after a waited run has
downloaded output. Use `--keep-input-datasets` only for debugging a still-running
kernel.

YDB business heartbeat: when `REGION_TALK_STATE_BACKEND=ydb`, every
`Status.event(...)` also upserts compact online progress to
`latest_business_heartbeat`, `business_heartbeat:<run_id>` and, by default,
`business_event:<run_id>:<seq>`. Heartbeat is **observability only**: it helps
watch a running Kaggle job when `kernels_logs` are empty, but it is not the
source of truth for queues, scores or cursor state. Heartbeat payloads include
phase/status, `progress_label`, current source title/url/handle, source counters,
fetch status, post counters, vector/report counters (`posts_to_score`,
`posts_scored`, `posts_deferred`) and final YDB/write summary fields; no secrets
are persisted.
Heartbeat stdout is quiet by default
(`REGION_TALK_STDOUT_HEARTBEATS=0`,
`REGION_TALK_KAGGLE_STATUS_STDOUT=0`) so Kaggle Logs remain readable. Events are
still persisted immediately to `/kaggle/working/region_talk_run_events_live.jsonl`
and YDB. `REGION_TALK_ENABLE_STACK_WATCHDOG=1` emits one all-thread Python stack
trace after the default 300-second interval during an opaque startup/stall;
repeated native dumps are opt-in through
`REGION_TALK_STACK_WATCHDOG_REPEAT=1`. A live Python 3.12 Kaggle run crashed at
the second watchdog boundary while inside C `re.search`, after the first dump
had already supplied the useful diagnostic stack. Queue assembly therefore
uses structured `source_queue_build_progress` events every 500 rows instead of
repeated signal-driven dumps. Hugging Face/tqdm progress bars remain disabled
by default so model loading does not drown the stage logs.

The ordinary CandidateReport hot path does not reclassify all ~7.6k durable
source rows on every run. It recomputes rows touched by the current source,
confirmed-blogger/frontier/keyword inputs, current image evidence, or a narrow
legacy repair; unchanged backlog rows are reused from the authoritative YDB
snapshot. Image evidence is indexed once by source id/URL rather than scanned
for every source. Metrics expose `source_queue_rows_recomputed_this_run` and
`source_queue_rows_reused_unchanged_this_run`. A deliberate maintenance sweep
may set `REGION_TALK_SOURCE_QUEUE_RECLASSIFY_FULL=1`; automated product cycles
must leave it disabled.

CandidateReport discovery tail is budget-gated before report assembly:
`REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS` (default `420`)
keeps similar/keyword discovery from consuming the final vector/report window,
and keyword search reports `keyword_discovery_alive` progress every few queries.
CandidateReport report assembly is bounded by
`REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN` (default `180`) with periodic
`REGION_TALK_VECTOR_HEARTBEAT_EVERY_POSTS` (default `5`) heartbeats. The
scoring loop also checks `REGION_TALK_RUNTIME_RESERVE_DURING_SCORING_SECONDS`
(default `120`) before each post and emits
`runtime_budget_vector_scoring_stop` instead of silently overrunning the
20-minute product run budget. Posts fetched beyond the scoring/runtime budget
are not stored as raw YDB payloads; they are visible in the XLSX-only
`02b_runtime_deferred_posts` sheet with links/metadata so the next bounded run
can score them. Final LLM verification and candidate-memory vector recheck have
the same heartbeat/runtime-stop discipline so YDB receives partial queues before
the notebook exits.

Real dual text embeddings remain the required product path. CandidateReport
loads **E5 only**, batch-scores the bounded post set and writes durable E5 rows;
the separate `RegionTalkBgeM3Enrichment` CPU notebook reads those rows, loads
**BGE-M3 only** and writes matching BGE rows. A later CandidateReport pass fuses
both score maps without loading BGE. This preserves the E5+BGE-M3 decision while
never placing both model weights in one Kaggle process. E5's isolation worker
uses multiprocessing `spawn`, not `fork`: forking after Telethon/YDB/native torch
initialization produced a real SIGSEGV (`exitcode=-11`) and deferred most posts
in a product run. `text_embedding_model_pass_alive` is the expected heartbeat
while E5 loads/scores; BGE emits its own model/batch heartbeats. Production runs
fail loud when required current-version dual rows are absent; prototype fallback
is debug/offline only.

YDB compact state is process state, not a mirror of every XLSX/debug tab.
Entity schema `region-talk-ydb-compact-v3` persists the single Telegram/VK
`unified_source_queue`, the downstream text-confirmed `image_candidate_queue`,
compact source/post/candidate lifecycle state and `queue_cursors` as stable
row-level records. `latest_state` uses the small
`region-talk-ydb-checkpoint-v4` singleton contract and does not duplicate those
collections. Parallel notebooks therefore
exchange work without treating Kaggle as a black box:

- `source_queue_item:<canonical_source_key>` / kind `source_queue_item`: one
  durable row per Telegram channel or VK community/wall source, including
  `queue_order`, `source_queue_status`, `status_changed_this_run`,
  `last_status_changed_at`, `ko_posts_found`, `candidate_posts_found`,
  image-quality rollup fields and next action.
- `image_queue_item:<image_queue_id>` / kind `image_queue_item`: one durable row
  per text-confirmed Kaliningrad-only post/image candidate, including lease
  fields, acquisition status, actual-image model scores and final visual status.
- `queue_cursor:source`, `queue_cursor:image` / kind `queue_cursor`: current
  cursor positions, cursor keys and queue totals.
- `queue_metrics:latest` / kind `queue_metrics`: compact latest queue totals for
  quick probes.

`RegionTalkCandidateReport` reads `latest_state` and overlays row-level
`source_queue_item`, `image_queue_item` and `queue_cursor` rows before building
the workbook. It owns source discovery, source/text scanning, vector/text gates
and final workbook assembly; it must not run local image-scoring models by
default. Default config is `REGION_TALK_IMAGE_SCORING_MODE=external_ydb_queue`,
`REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING=0`, and local CandidateReport image
models require explicit `REGION_TALK_CANDIDATE_REPORT_ALLOW_IMAGE_MODEL_SCORING=1`
for a debug-only run.

`RegionTalkImageDiagnostic` is the dedicated image worker. It reads/leases
`image_queue_item` rows, writes actual-image consensus scores back to those rows,
and updates visual rollups on matching `source_queue_item` rows. In YDB mode it
acts as a bounded poller: if the queue is empty at startup it waits up to
`REGION_TALK_IMAGE_DIAG_WAIT_INITIAL_SECONDS` (default 600) checking every
`REGION_TALK_IMAGE_DIAG_POLL_INTERVAL_SECONDS` (default 60); after draining
available rows it waits up to `REGION_TALK_IMAGE_DIAG_WAIT_AFTER_DRAIN_SECONDS`
(default 600) for newly queued candidates. The run is capped by
`REGION_TALK_IMAGE_DIAG_MAX_ITEMS_PER_RUN` and `REGION_TALK_IMAGE_DIAG_BATCH_SIZE`
so it cannot become an infinite worker. This makes visible deltas such as “new
communities added”, “source status changed”, “new image rows appeared” and
“images scored” available online in YDB before the final XLSX is downloaded.
If a leased media item is a video/non-image file or fails image decoding,
ImageDiagnostic writes terminal
`image_queue_status=not_reviewable_unsupported_media` with
`media_acquisition_status=unsupported_media_or_decode_failed`. CandidateReport
preserves that terminal status on later queue rebuilds, so an `.mp4` post cannot
be repeatedly re-leased as `needs_actual_image_fetch` and starve actual photos.
The `.mp4` row is terminal only in ImageDiagnostic. A confirmed-external,
KO-only, fused E5+BGE text candidate may continue to the finalizer as explicit
manual video review; Gemini checks text and the operator watches the linked
video in the review chat. No visual score is fabricated.
The orchestrator exposes terminal image counters
`image_not_reviewable_no_media_total`,
`image_not_reviewable_unsupported_media_total` and
`image_rejected_text_gate_total` alongside pending/in-progress/actual-scored
counts.

Legacy queue-like structures such as `source_frontier_queue_next` and
`similar_seed_queue` must not be durable YDB queue state. Frontier/debug sheets
remain XLSX/report artifacts only. The writer runs a bounded prune pass
(`REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS=1`) that rewrites old compact
snapshots to this contract without deleting source/post or candidate data.
Row-level reads are paginated to avoid YDB `TruncatedResponseError` and preserve
YDB row `updated_at` as `_ydb_updated_at` for scan scheduling. Live row writes
are the authoritative online state; final state snapshots emit
`state_write_started` / `state_write_done` and skip row-level entity rewrites by
default (`REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE=1`) so a 20-minute run does not
spend its tail rewriting thousands of unchanged rows. Full rewrites are an
explicit maintenance mode, not normal notebook behavior. Already processed publics are
not selected again every run: `processed_*` queue rows are held until
`next_history_scan_at`/`next_delta_scan_at`, retry status, or the processed-source
rescan cooldown says they are due. The default processed-source cooldown is two
weeks (`REGION_TALK_SOURCE_DELTA_RESCAN_INTERVAL_SECONDS=1209600`, overridable
by `REGION_TALK_SOURCE_RESCAN_PROCESSED_AFTER_SECONDS`). Even when a processed
public is technically due, CandidateReport first spends scan slots on
`pending_scan` / retry / never-scanned publics; processed-source delta rescans
start only after the primary frontier is exhausted. That later monitoring loop is
cursor-based: it uses the stored per-source history cursor, delta window and
overlap to fetch new or boundary posts, not to repeat the original full scan.
Normal product runs also enforce freshness and high-volume source guardrails:
`REGION_TALK_HISTORY_MAX_POST_AGE_DAYS` defaults to 365, `REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS` is honored when explicitly set by the orchestrator, and `REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD` defaults to 30.
If a source hits the daily text-post threshold, CandidateReport writes
`rejected_high_volume_text_posts_per_day` with the triggering date/count and the
source is not selected for future scans unless manually reset. Source-level
scan counters such as `posts_scanned` are monotonic maxima across rescans; a
shorter delta/rescan must not reduce cumulative monitoring metrics. A partial
source scan that still has `pending_scan` status but already has scan evidence
(`posts_scanned`, history fetch timestamp, or run id) is no longer selected as a
fresh primary source; it waits for the rescan phase after the first queue pass.
Each source fetch also stores history depth from post dates: `history_avg_post_age_days`,
`history_newest_post_age_days`, `history_oldest_post_age_days`,
`history_newest_post_date` and `history_oldest_post_date`. The orchestrator
aggregates these for all known source rows and for the latest run so operators
can tune how fresh/deep scanning should be.

## Vector-first product quality correction

After the z10b workbook review, `04a_final_shortlist` must be treated as product-facing and must not be filled by news, afisha, local institution PR, ads/promos, or posts whose main topic is another region. The mass filter is vector-owned:

- positive classes: Kaliningrad Oblast visit/impression, useful route, visual place card;
- negative classes: other-region primary topic/homonym, multi-region roundup, news/report, event announcement, ad/promo, local institution PR/event report, low-substance chat/test output;
- Russian external-region/city evidence is maintained as a compact gazetteer before vector scoring: multi-region event/place roundups such as “Новосибирская область + Калининградская область” must be rejected even if one listed item is a valid Kaliningrad place;
- deterministic place/regex/keyword checks are only evidence or last-resort safety fallback;
- LLM remains final verifier only and must not be used for broad corpus classification.

Candidate memory may keep historical rows for diagnostics, but before rendering `04a_final_shortlist` / `21_manual_review_queue` it must re-check memory rows with the same vector product gate and push vector-negative rows out of the product shortlist.

The orchestrator also computes a deterministic regex diagnostic over merged post
rows. It reports `regex_ko_raw_posts_total`, `regex_ko_filtered_posts_total`
(after external-region/multiregion/ad/news/substance filters),
`vector_ko_candidate_posts_total`, `regex_filtered_without_vector_posts_total`
and `vector_without_regex_filtered_posts_total`. These regex numbers are a
monitoring comparator only: if filtered regex KO volume is materially higher
than vector KO volume, the vector gates/prototypes need review; regexes do not
accept/reject production candidates.
Keyword-source queue health is monitored separately via
`publics_keyword_discovered_total`, `publics_keyword_scanned_with_posts_total`,
`publics_keyword_with_ko_candidates_total`,
`publics_keyword_pending_after_cursor_total` and
`publics_keyword_ko_yield_percent`; the same block also exposes
`publics_keyword_queue_rows_total`, `publics_keyword_edge_targets_total` and
`publics_keyword_queue_missing_total` so keyword hits that were already present
in the frontier cannot disappear from monitoring. Keyword-discovered rows,
including existing pending frontier rows, historical `source_edge_item` /
`source_candidate_item` keyword evidence, context-only rows with just
`canonical_source_key`+handle, and fake legacy `processed_*` rows without scan
evidence, receive a persisted immediate-priority marker in the same canonical
ledger so the next scan tests the channels where keyword search saw a
Kaliningrad hit without rewriting immutable `queue_seq`. Source queue cursor
calculation treats the cursor as the point
before the next primary `pending_scan` gap, not simply the largest processed
order; otherwise a later processed row could hide unscanned keyword rows behind
the cursor. The source selector also prioritizes keyword-evidence rows even when
legacy queue drift placed them before the stored cursor. The orchestrator also
reports keyword-sourced post diagnostics:
`publics_keyword_post_rows_with_text_total`, `publics_keyword_regex_ko_raw_posts_total`,
`publics_keyword_regex_ko_filtered_posts_total`,
`publics_keyword_vector_ko_candidate_posts_total`, source-level regex hit counts,
and regex/vector deltas. If keyword-discovered channels do not show a high KO
yield after real scans, or regex KO hits materially exceed vector KO candidates,
the keyword query/frontier insertion or vector prototype tuning needs review.

For live YDB runs CandidateReport does not rewrite the entire source queue on
every handoff. `REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS` defaults to 500 and
the orchestrator currently sets it to 80 together with
`REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL=0`. The bounded payload
keeps changed/current-run rows, keyword-evidence rows, the forward cursor
neighbourhood and pending/retry backlog, but a keyword reorder is no longer
allowed to force a full queue rewrite. This keeps the 30-minute run contract
realistic without hiding queue size: `source_queue_total` stays the full queue
count, while `source_queue_handoff_rows` reports how many rows were actually
written to `source_queue_item`. The legacy `source_status_item` mirror is off by
default in orchestrated runs (`REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR=0`)
to avoid duplicate transactional YDB upserts for the same queue handoff.
The exceptional one-time immutable `queue_seq` migration still persists the
complete ledger, but uses YDB `BulkUpsert` in bounded chunks (default 500 via
`REGION_TALK_SOURCE_QUEUE_REPAIR_BULK_CHUNK_SIZE`) so migration cost is based on
bulk requests rather than thousands of per-row transactional executions.
Likewise, the final compact-state row projection uses bounded YDB bulk writes
(`REGION_TALK_YDB_SNAPSHOT_BULK_CHUNK_SIZE`, default 500) because those
independent stable-key snapshots do not require one cross-row transaction.
Live queue transitions and heartbeats keep the transactional OLTP writer.
After the terminal `report_written` event, CandidateReport explicitly stops the
status heartbeat client and closes its shared YDB heartbeat driver before the
cell returns. This prevents an otherwise finished notebook from retaining a
Kaggle `RUNNING` slot while SDK background clients drain only at `atexit`.
For strict pre-image eligibility, an authoritative source is also considered
confirmed external when its canonical source-ledger row passed the surface
guard (`source_quick_class=candidate_keep`), has at least five actually scanned
posts, reached `processed_found_ko_candidate`, and has a non-zero KO ratio below
the local-source cutoff of `0.7`. This is a durable scan-evidence fallback for
rows where the optional profile projection did not persist `source_scope`; an
unscanned, sparse, all-KO, local or spam row remains fail-closed.
When image inputs join multiple source projections, the canonical
`unified_source_queue` row has precedence over thinner frontier/candidate rows
with the same URL; otherwise scan counters can be lost and a confirmed external
source incorrectly falls back to `source_verdict_unknown`.
The same precedence is fail-closed in the opposite direction: if the canonical
ledger row is still sparse, an older thin projection cannot override it with a
stale `source_scope=external` and send the post to image scoring prematurely.
Canonical URL lookup also precedes a thinner `source_id` projection, because
older canonical rows may legitimately lack `source_id`; otherwise the thin row
can still bypass URL-level authority.
If an older image row was rejected only because that text/source evidence was
incomplete, a later eligible pass reactivates it as
`needs_actual_image_fetch`; only confirmed no-media or unsupported-media
outcomes remain terminal. This also applies when the eligible row exists only
in the durable previous image queue and is not repeated in the current batch.
The reactivation is authoritative at the final YDB merge too: a metadata-only
ImageDiagnostic probe or old `deferred_text_gate` audit must not restore the
pre-BGE status over a now-fused `vector_accept_candidate`. CandidateReport
clears that stale image-audit reason while handing off
`needs_actual_image_fetch`; ImageDiagnostic then writes a fresh versioned
eligibility audit when it leases the actionable row. This preserves the dual
E5+BGE gate and removes the otherwise unnecessary extra CandidateReport cycle.
CandidateReport also preserves durable actual-image evidence and scores across
later metadata-only source rescans; it cannot downgrade `actual_image` to
`metadata_only` or erase `images_scored_actual_count`.
Because BGE and CandidateReport may start near each other and both read/write
YDB, CandidateReport state load is retried with bounded backoff in orchestrated
runs (`REGION_TALK_YDB_STATE_LOAD_ATTEMPTS=4`,
`REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS=20`) before treating YDB state as
unavailable. The BGE worker applies the same contention discipline at its own
read boundary: `REGION_TALK_BGE_YDB_LOAD_ATTEMPTS` (default `3`) recreates the
driver/session after an exhausted `DEADLINE_EXCEEDED` or
`RESOURCE_EXHAUSTED` attempt, with bounded delay controlled by
`REGION_TALK_BGE_YDB_LOAD_RETRY_BASE_SECONDS` (default `3`). In the normal
E5-only lane it also reuses the already loaded `text_vector_enrichment_item`
ledger as the existing-vector index instead of scanning the same multi-thousand
row kind twice. These are transport/read-efficiency safeguards only: the BGE
selection, model and dual-vector quality contract do not change.

Source-level monitoring totals must not rely only on the latest compact source
row, because a historical partial run can leave stale/lower source counters.
The orchestrator therefore reports repaired totals plus raw source-row and
repair-delta counters: `publics_scanned_with_posts_*`,
`publics_with_ko_candidates_*` and `source_queue_posts_scanned_*`. The repaired
`source_queue_posts_scanned_total` is at least `processed_posts_unique_total`,
and `publics_with_ko_candidates_total` is at least the number of sources
observed in text/image/publication candidate rows.
Rows marked `processed_*` without scan evidence (`posts_scanned`,
history-fetch timestamp or durable cursor evidence) are treated as fake
processed state, not real history scans. CandidateReport returns them to
`pending_scan`, and the orchestrator exposes
`publics_fake_processed_without_scan_evidence_total` plus
`publics_keyword_fake_processed_without_scan_evidence_total` so legacy queue
drift cannot make keyword yield look scanned when no posts were actually read.
Live progress rows such as `source_selected_for_run` are observability rows only:
they must write `online_source_item` / `source_status_item`, but must not
overwrite durable `source_queue_item` without `queue_order` and queue status.
When YDB source status rows are merged back, empty live fields do not clobber
non-empty durable queue fields.
Latest-run fast-check throughput is keyed only by the dedicated
`last_fast_check_run_id`. Generic `run_id` and `last_seen_run_id` are refreshed
by history, keyword discovery and queue handoff too, so they are explicitly not
accepted as evidence that a source was queried by fast-check. This keeps the
stats line consistent with the notebook's `fast_check_ko_done` event rather
than inflating checks/hits with old statuses touched by another stage.

The product stats surface is rendered from the same orchestrator snapshot as
the decision plan; it must not run a second aggregation with different grains.
YDB endpoint discovery for that snapshot is itself bounded-retry work:
`REGION_TALK_ORCHESTRATOR_YDB_CONNECT_ATTEMPTS` (default `3`) recreates and
closes the driver after discovery timeout, with
`REGION_TALK_ORCHESTRATOR_YDB_CONNECT_BACKOFF_SECONDS` (default `5`). This is
separate from the SDK operation retry loop because endpoint discovery happens
in `Driver.wait()` before a session pool exists.
Every cycle reports the complete stock and transition diagnostics together:
manual/keyword/hashtag/similar inflow, immutable queue-sequence integrity,
exact ready/cooldown/entity-wait/fetched states, source and post throughput,
current-version E5+BGE coverage/lag, image pending/actual/strong states, and
publication confirmed/sent/ready/review-retry states. Raw source or pending
growth is never hidden when manual/discovery inflow exceeds service throughput.
