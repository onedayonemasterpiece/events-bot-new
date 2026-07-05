# MVP candidate report / favorites table

Status: MVP-1 target. Canonical export name: `region_talk_candidates_report`. The first practical result is a cumulative XLSX review workbook the product owner can inspect by eye.

## Storage principle

YDB is the source of truth. The XLSX is generated from cumulative YDB state plus the current run delta. The workbook must never be the only persistent storage and must not contain secrets, raw tokens or private raw payloads.

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

All posts first seen in this run, before later filters remove some of them.

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

New source candidates discovered from links, mentions, forwards, repost attribution, catalogs and source descriptions.

### `13_sources_monitored`

Actually scanned sources, cursors, fetch status, errors and next fetch timestamps.

### `14_verifier_reports`

Gemini/VLM structured decisions, policy version, model id and cache key.

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

`12_sources_discovered` includes explicit links, forwarded/repost origins, normalized URL, platform guess, edge type, source status and confidence.

`13_sources_monitored` includes source profile probe fields: sampled post count, Kaliningrad hit count, ad/news/trash ratios, original-media score, link/forward richness and monitor priority score.

New sheets:

- `17_source_graph_edges`;
- `18_place_lexicon_matches`.
