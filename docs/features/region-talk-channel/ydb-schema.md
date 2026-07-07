# YDB schema draft — Region Talk Channel

Status: draft for follow-up implementation. All new persistent data for `region-talk-channel` must live in **YDB sidecar**, not SQLite.

## Kaggle → YDB contract

Configuration:

- `REGION_TALK_YDB_ENDPOINT`
- `REGION_TALK_YDB_DATABASE`
- `REGION_TALK_YDB_AUTH_MODE=service_account|metadata|static_credentials`
- `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON` or equivalent least-privilege secret lane
- `REGION_TALK_YDB_TLS=true`

Operational requirements:

- least-privilege credentials for Region Talk tables only;
- run lock table/coordination node before crawl or publish;
- idempotency keys for source, post, candidate and publication writes;
- retries/backoff for retriable YDB errors;
- dry-run mode writes only run/report rows or isolated dry-run namespace;
- migration plan with versioned DDL files and schema version row;
- audit log for every run and publication attempt;
- rollback limitations documented: published external posts cannot be transactionally rolled back with DB state.

## Stable IDs and dedupe

Stable ids must be deterministic across runs:

```text
source_id    = stable hash(platform + canonical_source_url_or_handle)
post_id      = stable hash(platform + platform_post_key_or_canonical_post_url)
media_id     = stable hash(post_id + media_url_or_phash)
candidate_id = stable hash(post_id + semantic_bank_version + selected_media_fingerprint)
```

Dedupe requirements:

- canonicalize URLs before hashing;
- normalize Telegram handles and VK/VK Video handles;
- store `text_hash` for posts and perceptual hash for images;
- run semantic duplicate checks for near-identical reposts;
- repeated source/post observations update cumulative state (`first_seen_run_id`, `last_seen_run_id`, `seen_run_count`) instead of creating duplicate candidates.

## Table overview

### 1. `region_talk_source_candidate`

Potential source before acceptance.

| Field | Notes |
|---|---|
| `source_candidate_id` | PK, stable hash/id. |
| `platform` | `telegram|vk|web|unknown`. |
| `url`, `handle`, `title`, `description`, `language` | Source metadata. |
| `discovered_from` | `telega_catalog|tgstat_catalog|vk_group|link_graph|manual_seed|web_search|repost_graph`. |
| `discovered_from_url`, `discovered_at` | Evidence. |
| `source_type_guess` | `travel_blog|author_channel|travel_media|architecture|history|nature|city_life|unknown`. |
| `is_region_local_guess` | Bool/score. |
| `is_news_like_score`, `is_trash_like_score`, `is_author_like_score` | Quality filters. |
| `travel_relevance_score`, `source_quality_score`, `originality_score` | Source scoring. |
| `last_checked_at` | Latest evaluation. |
| `status` | `candidate|accepted|rejected|paused|blocked`. |
| `rejection_reason` | Redacted text. |

Primary key: `source_candidate_id`.

### 2. `region_talk_source`

Approved or monitored source.

Fields: `source_id` PK, `platform`, `url`, `handle`, `title`, `avatar_url`, `avatar_asset_ref`, `description`, `language`, `source_type`, `source_scope=external|regional|mixed|unknown`, `is_news_like`, `is_trash_like`, `is_author_like`, `is_travel_like`, `source_quality_score`, `media_quality_prior`, `originality_score`, `rights_policy=unknown|link_only|forward_allowed|media_reuse_allowed|blocked`, `monitor_status=active|paused|blocked|error`, `created_at`, `updated_at`.

### 3. `region_talk_source_state`

Incremental crawl state.

Fields: `source_id` PK, `platform`, `platform_source_key`, `last_seen_post_key`, `last_seen_post_published_at`, `last_fetch_at`, `last_successful_fetch_at`, `next_fetch_after`, `fetch_status=ok|skipped|forbidden|unavailable|error`, `fetch_error_code`, `fetch_error_message`, `consecutive_errors`, `updated_at`.

### 4. `region_talk_source_graph_edge`

Discovery graph.

Fields: `edge_id` PK, `from_source_id`, `to_source_candidate_id`, `edge_type=link|mention|repost|forward|catalog_neighbor|manual`, `evidence_url`, `evidence_text`, `confidence`, `discovered_at`.

### 5. `region_talk_post`

Original external post.

Fields: `post_id` PK, `source_id`, `platform`, `platform_post_key`, `post_url`, `text`, `text_normalized`, `text_hash`, `published_at`, `fetched_at`, `views_count`, `likes_count`, `reposts_count`, `comments_count`, `has_media`, `media_count`, `region_relevance_score`, `newsiness_score`, `trash_score`, `ad_score`, `sentiment_overall=positive|neutral|mixed|negative|unknown`, `status=fetched|rejected|candidate|verified|queued|published|expired`, `rejection_reason`, `raw_payload_ref`, `raw_payload_json`, `created_at`, `updated_at`.

Unique/index: `(platform, platform_post_key)`.

### 6. `region_talk_post_media`

Images/videos and image scoring.

Fields: `media_id` PK, `post_id`, `source_id`, `platform`, `original_media_url`, `local_asset_ref`, `media_type=image|video|gif|unknown`, `width`, `height`, `file_size`, `perceptual_hash`, `technical_quality_score`, `aesthetic_score`, `postcardness_score`, `region_visual_relevance_score`, `low_noise_score`, `text_overlay_score`, `face_person_risk_score`, `publication_safety_score`, `overall_media_score`, `selected_for_publication`, `model_report_json`, `created_at`, `updated_at`.

### 7. `region_talk_post_embedding`

Text embedding cache.

Fields: `post_id`, `embedding_model`, `embedding_dim`, `embedding_document_version`, `text_hash`, `embedding_vector_blob` or `embedding_vector_json`, `created_at`.

Primary key: `(post_id, embedding_model, embedding_document_version)`.

### 8. `region_talk_semantic_match`

Vector match to semantic classes.

Fields: `run_id`, `post_id`, `semantic_bank_version`, `class_id`, `positive_score`, `negative_score`, `next_best_class_id`, `next_best_score`, `margin`, `match_status=candidate|accepted_vector|rejected_vector`, `created_at`.

Primary key: `(run_id, post_id, class_id)`.

### 9. `region_talk_candidate`

Publication/report candidate.

Fields: `candidate_id` PK, `post_id`, `source_id`, `run_id`, `region_relevance_score`, `source_quality_score`, `source_novelty_score`, `text_value_score`, `positive_score`, `neutral_value_score`, `negative_constructive_score`, `media_postcardness_score`, `media_publication_safety_score`, `duplicate_score`, `rights_risk_score`, `overall_candidate_score`, `selected_media_ids_json`, `semantic_summary_json`, `llm_verification_status=not_needed|approved|rejected|needs_review|failed`, `llm_report_json`, `publication_readiness=not_ready|report_only|ready_for_manual_review|ready_for_autopublish`, `decision=candidate|favorite|approved|rejected|queued|published|expired`, `decision_reason`, `created_at`, `updated_at`.

### 10. `region_talk_favorites`

MVP favorites table exported to XLSX/CSV/JSON/Markdown.

Fields: `favorite_id` PK, `candidate_id`, `post_id`, `source_id`, `added_by=auto|manual|probe`, `added_at`, `rank`, `title`, `source_title`, `source_url`, `post_url`, `short_summary`, `why_selected`, `image_report_short`, `media_score`, `candidate_score`, `suggested_publication_channels=telegram|vk|both`, `status=active|removed|published|expired`.

### 11. `region_talk_publication_asset`

Prepared media/cards for future publishing.

Fields: `asset_id` PK, `candidate_id`, `platform_target=telegram|vk`, `asset_type=source_image|branded_card|vk_carousel_card|tg_photo`, `local_path`, `object_storage_ref`, `width`, `height`, `format`, `source_avatar_overlay`, `source_link_overlay`, `overlay_report_json`, `rights_policy_snapshot`, `created_at`.

### 12. `region_talk_publication_queue`

Future publication queue.

Fields: `queue_id` PK, `candidate_id`, `target_platform=telegram|vk`, `target_channel_id_or_group_id`, `scheduled_for`, `priority`, `slot_group`, `status=pending|locked|published|failed|skipped|cancelled`, `lock_owner`, `locked_at`, `attempts`, `last_error_json`, `created_at`, `updated_at`.

### 13. `region_talk_publication_log`

Publication ledger.

Fields: `publication_id` PK, `queue_id`, `candidate_id`, `target_platform`, `target_channel_id_or_group_id`, `published_at`, `platform_message_id`, `platform_post_id`, `platform_post_url`, `text_published`, `asset_ids_json`, `api_response_json`, `status=published|failed|deleted|unknown`, `error_json`.

### 14. `region_talk_verifier_cache`

Gemini verifier cache.

Fields: `cache_key` PK, `post_id`, `candidate_id`, `verifier_model`, `verifier_policy_version`, `input_fingerprint`, `decision=approve|reject|needs_review|downgrade`, `report_json`, `created_at`, `expires_at`.

### 15. `region_talk_discovery_run`

Run/audit table.

Fields: `run_id` PK, `started_at`, `finished_at`, `status`, `sources_checked`, `source_candidates_found`, `posts_fetched`, `posts_region_relevant`, `posts_with_strong_media`, `candidates_created`, `favorites_exported`, `llm_calls`, `telegram_published`, `vk_published`, `errors_json`, `artifacts_json`.

## MVP compact YDB state adapter

For the 1 GB sidecar budget, the MVP adapter writes a compact `region_talk_state_kv` table rather than raw Telegram/VK payloads. The keys are:

- `latest_state` — compact cumulative state pointer for the next run;
- `run:<run_id>` — compact run snapshot;
- `metrics:<run_id>` — run/all-time metrics only.

The compact payload keeps source/channel URLs, source cursors, the canonical
Telegram/VK `unified_source_queue` + cursor, post URLs/platform keys, candidate
lifecycle, the `image_candidate_queue` + cursor and image scoring metrics. It
deliberately excludes raw post text, raw API payload JSON and media bytes
because posts/images can be re-fetched from external URLs when needed.

Current compact state schema is `region-talk-ydb-compact-v2`. YDB must not carry
parallel durable queue processes such as `source_frontier_queue_next` or
`similar_seed_queue`; those are XLSX/debug/report artifacts only. XLSX-only
columns (`status_color_hint`, `row_fill_color`) and frontier/debug columns are
also pruned from durable YDB queue payloads. The Kaggle writer may rewrite old
compact state rows through the v2 compactor to remove those parasite fields
without deleting URLs/posts/candidates.
