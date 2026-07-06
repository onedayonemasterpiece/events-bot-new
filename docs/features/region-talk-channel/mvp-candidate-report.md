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

### `10_good_text_weak_media`

Good semantic candidates blocked because images are weak. These never enter the main publication queue in MVP-1.

### `11_sources_seed`

All rows loaded from [`seed-sources-v1.csv`](seed-sources-v1.csv), with normalized handles/URLs and source ids.

### `12_sources_discovered`

New source candidates discovered from links, mentions, forwards, repost attribution, catalogs, source descriptions, public travel-blogger workbooks and Telegram similar-channel recommendations.

### `12a_source_frontier_unique`

De-duplicated source frontier across seeds, link graph, public travel-blogger workbook imports and Telegram similar-channel recommendations. This is the main place to inspect what could be added to monitoring next. Public fields may include `private_state_key` for matching back to private state, but must not include Telegram `channel_id`, `access_hash`, session strings or tokens.

### `12b_telegram_similar_channels`

Raw Telegram similar/recommended-channel evidence for the current run: seed channel, recommendation rank, public username/title/url when available, method status, errors and whether the row was added to the frontier. If Telethon lacks the required API request, this sheet must still exist with `method_status=not_supported_by_telethon_version`.

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
