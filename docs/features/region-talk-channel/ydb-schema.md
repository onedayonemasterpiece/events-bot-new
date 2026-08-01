# YDB schema draft — Region Talk Channel

Status: draft for follow-up implementation. All new persistent data for `region-talk-channel` must live in **YDB sidecar**, not SQLite.

## Kaggle → YDB contract

Configuration:

- `REGION_TALK_YDB_ENDPOINT`
- `REGION_TALK_YDB_DATABASE`
- `REGION_TALK_YDB_AUTH_MODE=service_account|metadata|static_credentials`
- `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON` or equivalent least-privilege secret lane
- for local-only orchestrator/debug probes, `--allow-yc-fallback` may use a pre-authenticated `/home/dev/yandex-cloud/bin/yc` profile to discover `events-bot-acq-discovery` and mint a short-lived IAM token; Kaggle/server runs should not depend on this browser-auth path
- `REGION_TALK_YDB_TLS=true`

For semi-manual live product runs, `REGION_TALK_STATE_BACKEND=ydb` is a
fail-fast contract, not a preference. CandidateReport/ImageDiagnostic launchers
must refuse to push a Kaggle run if `REGION_TALK_YDB_ENDPOINT` or
`REGION_TALK_YDB_DATABASE` is missing, and they default
`REGION_TALK_REQUIRE_YDB_STATE=1` for YDB-backed runs. A run that falls back to a
local JSON state file is an offline/debug run and must not be treated as live
YDB progress.

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

The current row-level KV projection uses the readable durable post key whenever
possible: `tg:<lowercase_handle>:<message_id>` or
`vk:<owner_id>:<post_id>`. `post_id` is payload metadata only and must not be
the YDB PK because its historical value included the fetch path (Telethon
history, public web, exact-link fetch). Online writes, snapshot writes and state
loading re-key by the platform identity and merge non-empty fields.
Normal CandidateReport snapshots do not rewrite row-level processed posts:
online post writes own those rows and retain `run_id`/stage/text-hash
observability. Snapshot row rewriting is explicit maintenance only. This avoids
both resurrecting legacy keys and erasing latest-run attribution.

Dedupe requirements:

- canonicalize URLs before hashing;
- normalize Telegram handles and VK/VK Video handles;
- store `text_hash` for posts and perceptual hash for images;
- run semantic duplicate checks for near-identical reposts;
- repeated source/post observations update cumulative state (`first_seen_run_id`, `last_seen_run_id`, `seen_run_count`) instead of creating duplicate candidates.
- vector work is additionally idempotent by `post_processing_fingerprint`
  (post text hash + active E5/BGE encoder contracts + semantic-bank version).
  A source rescan may update observation counters, but it does not recompute a
  current E5 row; arrival of BGE opens exactly one fusion pass.
- legacy `processed_post_item:post_hash_*` duplicates are normalized only by
  the dry-run-first `scripts/region_talk_post_row_normalize.py`: it UPSERTs the
  merged stable row before deleting redundant keys. The same maintenance pass
  also migrates a legacy singleton before a later online observation can create
  its canonical twin.

`candidate_memory_item` remains an audit/history layer. Rows whose source is
durably local-region or spam are retained but moved to
`source_terminal_local_audit_only` / `source_terminal_spam_audit_only`; they are
excluded from operational candidate, BGE, image and Gemini capacity. Metrics
must expose total, operational, local-audit, spam-audit, dual-pending and
image-wait populations separately.

The live KV table also contains two compact source-onboarding projections:

- `source_onboarding_evidence_item:<source_profile_id>` — canonical source
  identity, up to eight public evidence excerpts with URL/date, evidence
  status/version and fingerprint;
- `source_onboarding_profile_item:<source_profile_id>` — reusable entity type,
  evidence-backed atomic claims/angles, conflicts/missing fields, prompt/model
  version and profile fingerprint.

The candidate-specific `source_onboarding_paragraph` and its claim/evidence
references live on `publication_candidate_item`. Evidence/profile rows never
contain media bytes or an unbounded source archive.

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

Fields: `post_id` PK, `source_id`, `platform`, `platform_post_key`, `post_url`, `text_hash`, optional short `summary`, `published_at`, `fetched_at`, `views_count`, `likes_count`, `reposts_count`, `comments_count`, `has_media`, `media_count`, `region_relevance_score`, `newsiness_score`, `trash_score`, `ad_score`, `sentiment_overall=positive|neutral|mixed|negative|unknown`, `status=candidate|verified|queued|published|expired`, `candidate_stage`, `rejection_reason` for accepted/debug rows only, `created_at`, `updated_at`.

Full post text, normalized full text and raw Telegram/VK API payloads are not
stored in YDB. Non-candidate/invalid posts are represented only by per-source
cursors and aggregate counters; if a human needs the content again, it must be
re-fetched by `post_url`/platform key.
Verifier/publication rows retain at most 700 characters of text for audit; the
canonical durable fields are `post_url`, platform post key, `text_hash`, compact
summary, gate evidence and decision.

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

For the 1 GB sidecar budget, the production adapter writes the LZ4-compressed
`region_talk_compact_state_kv` table rather than raw Telegram/VK payloads. The
legacy `region_talk_state_kv` table was removed on 2026-07-12 after a complete
ordered YDB CLI dump, SHA-256 manifest, compact parity checks and more than
13 hours without legacy writes. The rollback dump is an operational artifact,
not another live YDB copy. Obsolete product/smoke/test tables were removed at
the same time. The database now contains the 46.2 MB compact Region Talk table
plus three small acquisition tables (well below the 1 GB budget). The singleton
keys are:

- `latest_state` — small restart checkpoint for the next run;
- `run:<run_id>` — retention-limited copy of that checkpoint;
- `metrics:<run_id>` — immutable compact per-run funnel + all-time metrics.

Checkpoint schema `region-talk-ydb-checkpoint-v4` keeps only singleton state:
cursors, cooldowns, queue totals/positions, publication goal and metrics. It
does **not** embed the 7k-source queue, 11k processed posts, candidate memory,
image/publication queues or vector rows. The loader reconstructs those
collections from their stable row-level kinds. This avoids rewriting two
~15.9 MB JSON snapshots on every run.

Entity compaction schema remains `region-talk-ydb-compact-v3`; checkpoint schema
is v4. The adapter writes row-level queue records
into the same KV table. For the semi-manual product discovery loop these rows are
**online durable state**, not final-report-only artifacts: notebooks must upsert
main records as soon as they are selected, discovered, fetched, scored or
reclassified. Heartbeats remain observability-only and are not sufficient proof
that a source/post/image/candidate exists in the product state.

## Live product funnel contract

Operator-visible live YDB state must answer product questions, not just expose
technical heartbeats. The core Region Talk funnel is:

1. **Source discovered** — a public/channel/community was found from catalog,
   similar-channel discovery, keyword discovery, post links or forwarded/repost
   evidence.
2. **Source status decided** — the source was selected for scan, accepted into
   the queue, skipped/rejected/deferred, marked regional-only, or failed with an
   error/reason.
3. **Source cursor advanced** — the current scan/queue cursor and current source
   are visible while the notebook is still running.
4. **Post fetched/scored** — canonical `processed_post_item` is an identity,
   status and hash projection and never retains the post body. Active E5,
   candidate/media or retryable Gemini state owns the working text instead.
   Raw platform payloads are never stored.
5. **Region/text candidate** — the post passed/failed Kaliningrad-only,
   non-ad/non-news/text-value gates and candidate memory is updated.
6. **Media queue** — text-confirmed posts with media are queued for
   ImageDiagnostic; identified video remains terminal for image scoring but can
   continue as `media_kind=video`, `manual_media_review_required=true` after the
   strict text gate.
7. **Image worker status** — image rows show lease, media fetch result, retry
   reason or actual-image score, plus image-worker cursor.
8. **Publication queue** — text/vector/source evidence joins actual-image scores
   into ranked photo candidates, or explicitly routes strict text-passed video
   to operator review without inventing visual scores.
9. **Final verifier/operator notification** — Gemini Lite decision and local
   notification status are visible per candidate.

### Working-text lifecycle

Post text is operational state, not historical content storage:

1. CandidateReport keeps the complete fetched text only in the active
   E5/candidate/media handoff needed by BGE, image/video routing and Gemini.
   It is not silently truncated to an arbitrary 500/180-character database
   excerpt when a downstream verifier still needs the complete post.
2. `processed_post_item` keeps identity, date, hashes, stage and reasons only;
   it never duplicates an excerpt.
3. E5+BGE scores remain durable, but text fields are removed from processed,
   candidate, image, publication and vector projections immediately after a
   terminal reject/accept/needs-review/sent verdict.
4. `scripts/region_talk_state_maintenance.py` performs dry-run-first historical
   cleanup and compacts terminal image-ledger payloads. The publication
   finalizer performs the same text deletion for newly terminal posts.
5. Missing text before the final Gemini verdict is recoverable work, not a
   terminal verdict. The finalizer persists `text_restore_pending`, writes a
   priority exact `post_link_queue_item`, and CandidateReport restores the body
   through governed Telethon. `no_text_for_gemini` is a legacy retry status;
   neither it nor `text_restore_pending` authorizes cross-projection text
   deletion.

`image_queue_item` is a historical idempotency ledger. Operator metrics must
label its total as a ledger size, while active photo backlog, scored photos,
broken media and manual-video review are reported separately.

`publication_candidate_item` has exactly one storage key per normalized post
URL. `publication_candidate_id` remains metadata; CandidateReport and the local
finalizer must never create competing PKs for the same URL.

Comments are source-discovery evidence only: if comment discovery is enabled,
YDB stores a redacted `comment_link_item` with comment/link/status hashes and
canonical extracted source URL. Raw comment bodies, personal ids and raw payloads
must not be stored.

Cursor rows are live rows, not end-of-run artifacts. CandidateReport must write
`queue_cursor:source_scan` while scanning selected sources and
`queue_cursor:source` / `queue_cursor:image` when source/image queues are
rebuilt. ImageDiagnostic must write `queue_cursor:image_diagnostic` while
polling/leasing image rows.

## Size and retention policy

The 1 GB YDB sidecar budget is shared with other acquisition/discovery work, so
Region Talk must keep row-level product state compact:

- durable rows are stable-key upserts, not per-run append logs:
  `source_queue_item`, `source_status_item`, `source_candidate_item`,
  `source_edge_item`, `comment_link_item`, `processed_post_item`,
  `post_link_queue_item`, `candidate_memory_item`, `image_queue_item`,
  `publication_candidate_item`;
- full post text exists only in active working rows that still need BGE,
  media routing or a Gemini retry; raw comment text, raw Telegram/VK payloads
  and media bytes are excluded from YDB;
- identity/history projections use compact field allow-lists and hashes only;
- `processed_post_item` is the single durable post record. New writes no longer
  duplicate it as `post_live_item`; the old kind is read-only migration input;
- source state uses `source_queue_item` for ordered queue identity and
  `source_status_item` for live status overlays. The former third copy,
  `online_source_item`, is legacy read-only input and is removed by state
  maintenance;
- BGE dense vectors are stored as `f16_le_base64` plus `embedding_dim`, not a
  JSON list of decimal floats. Semantic scores and E5+BGE fusion remain intact;
- E5 may retain the bounded source excerpt only while its paired BGE row is
  missing. A successful BGE write removes that transient excerpt from the E5
  row, and the BGE row itself stores scores/vector/hash rather than another text
  copy. This is lifecycle compaction, not a single-model shortcut;
- retryable publication candidates retain the exact post text while a Gemini
  retry is possible. Accepted, rejected, operator-rejected and sent terminal
  rows keep URL/hash/verdict evidence but no post text;
- exact-link queue rows may keep the bounded Telegram search-result excerpt
  only while the link is pending/retryable. After fetched, scored, rejected or
  another terminal outcome, maintenance keeps URL/query/evidence hash and
  deletes the excerpt;
- BGE run-result rows contain only compact summary/sample references and are
  retention-limited; they never duplicate the full enrichment rows;
- `latest_state` and `run_state_snapshot` use checkpoint-v4 and therefore stay
  kilobyte-scale rather than carrying all row-level product state;
- queue admission sequence repair writes only rows marked
  `queue_seq_repaired_this_run=true`; one repaired row must never rewrite the
  whole source frontier;
- the table default column family uses LZ4. The dry-run-first migration command
  is `scripts/region_talk_ydb_compact.py`; it copies into a separate namespace,
  drops completed embedding-research rows and applies bounded retention without
  mutating the source table;
- migration reconciles `processed_post_item` and legacy `post_live_item` by the
  durable Telegram/VK post identity before removing the mirror. The live audit
  proved 22,936 projection rows collapse to 11,331 canonical posts with no
  legacy-only identity loss;
- migration records the source `MAX(updated_at)` before and after its read and
  aborts if writers changed the source. Replacing an existing target requires
  both `--replace-target` and the explicit bootstrap data-loss acknowledgement;
  normal online operation must never recreate the active compact namespace;
- large `run_state_snapshot` rows are retention-limited
  (`REGION_TALK_YDB_RUN_SNAPSHOT_KEEP_LAST`, default `1`) because the durable
  source/post/image/candidate rows already carry the live product state;
- ephemeral observability rows are retention-limited:
  `business_event`, per-run `business_heartbeat`, per-run `online_stats`,
  per-run `queue_cursor`, and `run_metrics`;
- semantic-bank embedding cache rows are retention-limited too
  (`REGION_TALK_YDB_SEMANTIC_BANK_KEEP_LAST`, default `4`), so model/bank
  version churn cannot silently consume the shared 1 GB sidecar;
- protected latest rows such as `latest_state`, `latest_business_heartbeat`,
  `online_stats:latest`, `queue_cursor:source`, `queue_cursor:image`,
  `queue_cursor:source_scan` and `queue_cursor:image_diagnostic` are kept.

The 2026-07-11 live audit measured 644 MB physical storage for only ~171 MiB of
live JSON. The main cause was write amplification: two full 15.9 MB snapshots
per run plus occasional full 7k-row queue rewrites. The validated LZ4 target has
52,286 product/operational rows and 77.4 MB logical JSON at cutover; after the
acceptance runs and lifecycle text pruning it held about 56.4k logical rows / 44.5 MB physical while preserving all
critical-kind row counts. A new
`run_funnel_metrics` payload in every `run_metrics` row provides a reliable
daily grain; mutable entity `run_id` fields must not be used to reconstruct
historical daily throughput.

Because checkpoint-v4 embeds no product collections, every CandidateReport
launch must read the complete row-level population. Current orchestrator floors
are 20,000 processed posts, 20,000 text-vector rows, 5,000 candidate/image rows
and 20,000 source rows. A lower debug read cap is not allowed in a production
write run: it could create a correct-looking checkpoint from incomplete state.
The separate BGE reader uses the same 20,000-row floor for the shared vector
kind; batch size limits model CPU work, while scan limit guarantees queue
visibility.


- kind `source_queue_item`, pk `source_queue_item:<canonical_source_key>` — the
  canonical Telegram/VK source queue row with immutable admission `queue_seq`,
  legacy cursor/display `queue_order`, status, status-change
  fields, cursor display markers, KO/candidate counters and image-quality rollup;
  local-region source routing is stored as
  `source_queue_status=rejected_local_region_source`,
  `source_scope=local_region`, `source_geo_class=kaliningrad_local`,
  `source_quick_class=local_region_source`, with
  `monitoring_exclusion_reason`/`source_probe_reason` explaining that the source
  is retained for a future local-source monitor but excluded from the current
  external blogger/travel publication funnel;
  source-local `fast-check-KO` preflight writes `fast_check_status`,
  `fast_check_at`, `fast_check_matched_query`, `fast_check_hit_post_url`,
  `fast_check_hit_post_date`, `fast_check_query_strategy`,
  `fast_check_query_cursor`, `fast_check_query_terms_total`,
  `fast_check_query_wave`, completed-RPC count, elapsed seconds, attempted terms
  and error fields back to this same row. Only
  `fast_check_status=ko_hit` gives the source immediate priority in the same
  ledger;
  `no_hit`, `no_hit_partial`, `no_hit_exhausted`, deferred and error states are
  not source rejections and are not promoted; adaptive partial rows may resume
  from the persisted cursor while exhausted rows fall back to normal scanning;
  terminal high-volume rejections use
  `source_queue_status=rejected_high_volume_text_posts_per_day` plus
  `monitoring_exclusion_reason`, `source_probe_reason`,
  `high_volume_text_posts_date/count/threshold` so news-like feeds are not
  rescanned silently;
- kind `source_status_item`, pk `source_status_item:<canonical_source_key>` — live
  source/public registry alias for statistics and operator views; written when a
  source is selected for a run, discovered by similar/keyword discovery, skipped,
  rejected, resolved, scanned or later enriched by final queue scoring;
- kind `source_candidate_item`, pk
  `source_candidate_item:<source_candidate_id|canonical_source_key>` — the
  discovery-frontier public/channel candidate as soon as discovery sees it
  (similar channels, keyword hits, post links, catalog rows), before final queue
  assembly. Iterative Kaggle runs cap online discovery row writes with
  `REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES` /
  `REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_EDGES`; final compact state and
  queue assembly remain the canonical durable product state;
- kind `source_edge_item`, pk `source_edge_item:<edge_id>` — source-discovery
  graph evidence such as forward origins, post-text links and similar/keyword
  source edges;
- kind `comment_link_item`, pk `comment_link_item:<comment_link_id|edge_id>` —
  redacted comment-link discovery evidence when comment scanning is enabled;
  comments are source-discovery evidence only and never publication candidates;
- kind `processed_post_item`, pk `processed_post_item:<post_id>` and
  `post_live_item:<post_id>` — compact fetched/scored post state without raw text
  or raw API payloads; text is represented only by hashes/excerpts already allowed
  in report artifacts;
- kind `post_link_queue_item`, pk `post_link_queue_item:<post_url_hash>` —
  known-post fetch/scoring queue for exact post URLs found by global keyword
  search or source-local preflight. It stores `post_url`, `source_key`,
  `matched_query`, `matched_hashtag`, `post_date`, `hit_age_days`,
  `priority_reason`, status/lease fields and a short evidence excerpt. Consuming
  this queue refetches the exact post and sends it through the normal
  E5+BGE/text/image/LLM funnel; it is not a publication acceptance shortcut.
  `post_link_status` is `pending_fetch`/`fetch_error`/`retry_wait_entity_cache`/
  `fetched`/terminal; retry rows carry `next_attempt_after`, and
  `first_attempt_at`, `last_attempt_at`, `attempt_count` and
  `fetch_attempt_count`. Rediscovery updates evidence but cannot reset a retry,
  cooldown, fetched or terminal lifecycle to `pending_fetch`. The consumer scans
  a broad durable window before choosing its small execution batch, so blocked
  primary-key head rows do not starve ready links. The
  normal CandidateReport runs consume a bounded exact-link batch before source
  history scans when `REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST=1`;
- kind `candidate_memory_item`, pk `candidate_memory_item:<candidate_memory_id>` —
  compact cumulative candidate-memory row. CandidateReport reads and writes this
  row-level kind directly; it must not depend only on `latest_state.candidate_memory`;
- kind `image_queue_item`, pk `image_queue_item:<image_queue_id>` — the downstream
  image-analysis work item for a text-confirmed Kaliningrad-only post, including
  lease/status fields, actual-image consensus scores and the versioned upstream
  fields `publication_eligibility_decision`,
  `publication_eligibility_gate_version`, reason and evidence. ImageDiagnostic
  leases only signed `accept` rows for the expected gate version;
- kind `publication_candidate_item`, pk
  `publication_candidate_item:<publication_candidate_id|post_url>` — the ranked
  product shortlist row joined from text/source/vector evidence and
  ImageDiagnostic actual-image scores. It carries
  `publication_candidate_status=llm_confirmed|sent_to_chat|filtered_before_llm|llm_needs_review|llm_rejected|llm_budget_deferred`,
  rank/score fields, authoritative source eligibility verdict/evidence/version,
  retry metadata (`attempt_count`, `next_attempt_after`), authoritative source
  fingerprint/version, tombstone/revoke
  markers, the Gemini Lite verifier decision, goal-stop marker and
  local notification markers (`sent_to_chat`, `sent_message_id`). Newly
  accepted rows also carry the v7 grounded writer projection:
  `publication_draft_status`, title/source attribution, Telegram/VK text,
  compact `publication_draft_fact_points_json` claim/support evidence and the
  draft prompt version. These fields are finalizer-owned and preserved by
  later CandidateReport refreshes;
- kind `telegram_entity_cache_item`, pk
  `telegram_entity_cache_item:<canonical-entity-key>` — private durable
  `channel_id/access_hash` cache written in bounded batches immediately after
  successful entity observation, so an early report-tail exit cannot lose
  access data required by the next cached-first run. During migration,
  `latest_state.telegram_entity_cache` remains a read-compatible fallback for
  cache-first selection and is reported separately from row-level cache rows;
- kind `text_vector_enrichment_item`, pk
  `text_vector_enrichment_item:<post_id>:<model_short>:<text_hash>` — durable
  per-text/per-model vector enrichment. CandidateReport now writes E5 rows
  (`model_id=intfloat/multilingual-e5-base`,
  `encoder_contract=e5_semantic_bank_scores_v1`) with the compact text/hash it
  scored locally. `RegionTalkBgeM3Enrichment` reads those E5 rows first, scores
  the same compact text in isolation and writes BGE rows
  (`model_id=BAAI/bge-m3`,
  `encoder_contract=bge_m3_flagembedding_dense_v1`) with semantic-bank scores,
  KO-vs-external geo-bank scores and, for bounded candidate/history batches, an
  optional compact dense vector. CandidateReport then consumes E5+BGE rows for
  fusion and must not load BGE-M3 in the main notebook;
- kind `bge_m3_enrichment_result`, pk
  `bge_m3_enrichment_result:<run_id>` and `bge_m3_enrichment_result:latest` —
  BGE worker run evidence: loaded rows, scored rows, written rows, encoder
  contract, device/backend, bank hashes, elapsed time and error summary;
- kind `qwen3_embedding_0_6b_enrichment_item`, pk
  `qwen3_embedding_0_6b_enrichment_item:<post_id>:qwen3_embedding_0_6b:<text_hash>` —
  research-only Qwen3-Embedding-0.6B per-text enrichment. It mirrors BGE semantic
  and KO-vs-external geo scores for comparison but is not consumed by production
  fusion until explicitly promoted;
- kind `qwen3_embedding_0_6b_enrichment_result`, pk
  `qwen3_embedding_0_6b_enrichment_result:<run_id>` and
  `qwen3_embedding_0_6b_enrichment_result:latest` — Qwen3 research run evidence;
- kind `embeddinggemma_300m_enrichment_item`, pk
  `embeddinggemma_300m_enrichment_item:<post_id>:embeddinggemma_300m:<text_hash>` —
  research-only EmbeddingGemma-300M CPU per-text enrichment. It mirrors BGE
  semantic and KO-vs-external geo scores for comparison but is not consumed by
  production fusion until explicitly promoted;
- kind `embeddinggemma_300m_enrichment_result`, pk
  `embeddinggemma_300m_enrichment_result:<run_id>` and
  `embeddinggemma_300m_enrichment_result:latest` — EmbeddingGemma research run
  evidence;
- kind `vector_bank_embedding_item`, pk
  `vector_bank_embedding_item:<bank_hash>:<model_short>:<encoder_contract>` —
  target cache for semantic-bank, Kaliningrad geo-bank and external geo-bank
  prototype vectors. This is separate from legacy `semantic_bank_embedding`
  because the exact encoder contract matters: old AutoModel/mean-pool vectors
  must not be mixed with BGE-M3 FlagEmbedding dense vectors;
- kind `publication_semantic_history_item`, pk
  `publication_semantic_history_item:<publication_candidate_id|post_id>` —
  target history row for semantic anti-vector diversity. It references/stores
  the E5+BGE vector fingerprints for already Gemini-confirmed, sent or
  published posts so later ranking can penalize nearest-neighbour semantic
  overlap instead of only same-source/place heuristics. If Qwen3 is promoted
  after the research gate, its fingerprint may be stored here as an additional
  or replacement model slot; the anti-vector is not E5-only;
- kind `publication_schedule_item`, pk
  `publication_schedule_item:<YYYY-MM-DD>:<article|social>` — the current
  deterministic daily slot. Future `planned`/`vacant` rows are recalculated as
  candidates arrive; `locked`/`published` rows are immutable. The payload
  stores lane, local scheduled time, candidate URL/id, both target platforms,
  quality/diversity evidence and the same-day pair-similarity decision;
- kind `publication_schedule_snapshot`, pk
  `publication_schedule_snapshot:latest` — compact 14-day result of
  `region_talk_daily_pair_antivector_v1`, including lane/history/vacancy counts.
  It is a selection ledger and does not itself call Telegram/VK publication
  APIs;
- kind `external_publication_intake_item`, pk
  `external_publication_intake_item:extpub_<stable-id>` — validated external
  editorial/academic research staging. It is not a publication candidate and
  cannot bypass the E5+BGE/image/final-verifier/operator gates;
- kind `external_publication_source_item`, pk
  `external_publication_source_item:extpubsrc_<stable-id>` — compact external
  publisher identity and externality attestation, keyed in payload by
  `canonical_source_key=web:<domain>`. CandidateReport loads the row into a
  separate publisher-attestation lookup and joins it before the pre-image
  publication gate; the finalizer joins the same row again as authoritative
  source evidence. Neither consumer inserts the publisher into the Telegram/VK
  scan queue;
- kind `external_publication_seen_item`, pk
  `external_publication_seen_item:extseen_<stable-doi-or-url-id>` — durable
  cross-run duplicate ledger for every valid candidate, explicit exclusion and
  unresolved lead. It stores normalized DOI/canonical URL, compact title/source,
  disposition and request lineage. The read-only request generator merges this
  ledger with legacy intake rows and emits the complete immutable seen snapshot
  supplied to the next external research agent;
- kind `external_publication_import_batch`, pk
  `external_publication_import_batch:extpubrun_<stable-id>` — idempotent input
  batch counts, research request/window and bounded coverage evidence;
- kind `external_publication_import_error_item`, pk
  `external_publication_import_error_item:<stable-error-id>` — row-level
  contract errors. One bad result remains auditable without discarding the
  valid part of the external research batch;

When an external intake reaches `publication_candidate_item`, its compact row
retains `content_origin_type`, external publication/research identifiers and
score, image-quality decision/reason, Region Talk scope, and
`rights_policy`/`media_use_policy`/`media_reuse_allowed`. Operator and queue
consumers therefore do not need to reconstruct rights or visual-review state
from the transient image workbook.
- kind `queue_cursor`, pk `queue_cursor:source|image` — cursor position/key and
  quick counts for source and image queues;
- kind `queue_metrics`, pk `queue_metrics:latest` — latest compact queue counters.
- kind `semantic_bank_embedding`, pk `semantic_bank_embedding:<hash>` — cached
  prototype vectors for the finite semantic-bank meaning list and one embedding
  model (`intfloat/multilingual-e5-base` or `BAAI/bge-m3`), keyed by
  `semantic_bank_version + bank_hash + model_id`;
- kind `business_heartbeat`, pk `latest_business_heartbeat` and
  `business_heartbeat:<run_id>` — CandidateReport live status only, including
  compact fetch/vector/report counters such as `posts_fetched`,
  `posts_to_score`, `posts_scored`, `posts_deferred` and `progress_label`;
- kind `business_heartbeat_image_diagnostic`, pk
  `latest_business_heartbeat:image_diagnostic` and
  `business_heartbeat:image_diagnostic:<run_id>` — ImageDiagnostic live status
  only.

`RegionTalkCandidateReport` is the source/source-cursor writer and reads row-level
items before every report. Source/public state reads must overlay
`source_queue_item`, `source_status_item` and `online_source_item`; the status
aliases are not heartbeat-only rows because they may contain live selected,
skipped, rejected or visual-rollup updates that have not yet been folded into a
final snapshot. CandidateReport does source/text work and consumes
already-written image scores; it does not run local image-scoring models in
normal runs.
`RegionTalkImageDiagnostic` is an image worker/poller: it leases
`image_queue_item` rows, writes scores/statuses back, updates the source visual
rollup on matching `source_queue_item` and `source_status_item` rows, waits
bounded intervals for an empty or just-drained queue, and exits after its fixed
per-run item budget. Heartbeat rows remain observability-only and must not be
used as durable queue state.
For publication readiness, metadata-only visual estimates are not final image
evidence: `actual_scored` is complete only together with
`image_model_input_type=actual_image`. Metadata-only rows with old
`actual_scored` statuses must stay eligible for an actual-image retry.
The two notebooks must run with different Telethon auth bundles and must not
share one Telegram session concurrently.

Publication finalization may run as a lightweight live-YDB consumer after
ImageDiagnostic, without requiring the CandidateReport workbook/report tail. The
current script is `scripts/region_talk_publication_finalizer.py`: it reads
row-level `candidate_memory_item`, `image_queue_item(actual_scored)` and
existing `publication_candidate_item` rows, refreshes unsigned/old eligibility
attestations, retries only due retryable Gemini rows, preserves terminal rows,
and treats the XLSX/CSV shortlist as an operator artifact rather than
persistent state.

Additional row kinds used by the full publication tail:

- `region_talk_llm_budget_item:<budget_id>`: hard-clamped cumulative Region
  Talk Gemini budget (`reserved_total`, `remaining`, `budget_max<=100`);
- `region_talk_llm_request_item:<budget_id>:<fingerprint>`: deterministic
  final-verifier reservation/result used to avoid repeat calls after restart;
- `publication_delivery_item:<sha256(chat_id|canonical_post_url|operator_review_fingerprint)>`:
  prepared Telegram-chat delivery state, deterministic MTProto `random_id`,
  peer id, message id and timestamps. It explicitly carries the v1 operator
  review fingerprint, exact draft fingerprint, ordered media/presentation
  manifest JSON and candidate id/PK. A materially changed copy or presentation
  therefore creates a new delivery rather than inheriting an old reaction;
- `publication_review_state_item:<operator_review_fingerprint>`: current fully
  observed exact allowlisted per-reactor map for one immutable review revision,
  its `approved|pending|rejected|conflict` decision, independent
  `clean|rewrite_requested` state, message/chat identity and observation hash;
- `publication_review_event_item:<operator_review_fingerprint>:<observation-hash-prefix>`:
  idempotent reaction revision history. An addition or removal creates one new
  event only after complete Telegram pagination; an unchanged observation is a
  no-op. Old-fingerprint events are history and never project onto changed
  draft/media revisions.

The matching `publication_candidate_item` review projection uses
`operator_review_fingerprint`, `operator_review_state_version`,
`operator_review_decision`, `operator_review_rewrite_status`, compact booleans,
the exact allowlisted reaction map JSON and the observation hash/time. These
fields do not alter source/candidate lifecycle. When the publication-plan
reaction rollout gate is active, only an exact-current `approved + clean`
projection is eligible.

Strong rows blocked only by sparse source evidence update the existing
`source_queue_item` (not a second source queue) with
`publication_source_evidence_priority`, target post count and finalist URL.
CandidateReport consumes that marker through the normal unified source selector
and stops prioritizing it once the target sample depth is reached.

For VK rows whose API token is IP-bound, the local/server prefetch stage may
add `image_url_or_local_path`, `vk_media_photo_urls`,
`vk_media_prefetch_status=ready` and `vk_media_prefetch_source` to the same
`image_queue_item`. These are acquisition hints only: completion still requires
ImageDiagnostic to download/decode actual bytes and write
`image_model_input_type=actual_image` plus `actual_scored`. CandidateReport owns
a merge, not a replacement, for this row: its subsequent text/fusion refresh
must retain the prefetch URL and provenance until actual scoring or an explicit
terminal media result supersedes them.

YDB must not carry parallel durable queue processes such as
`source_frontier_queue_next` or `similar_seed_queue`; those are XLSX/debug/report
artifacts only. The Kaggle writer may rewrite old compact state rows through the
v3 compactor to remove parasite legacy queue payloads without deleting
URLs/posts/candidates. Row-level queue reads must be paginated by primary-key
prefix range (`pk >= '<kind>:' AND pk < '<kind>;'`, then `pk > after`) rather
than table-wide `WHERE kind=...` scans, with
`REGION_TALK_YDB_SELECT_PAGE_SIZE` (default 200) to avoid YDB truncated response
and `RESOURCE_EXHAUSTED` errors. Row-level queue writes default to changed/current-run rows only
(`REGION_TALK_YDB_ROW_WRITE_MODE=changed`); use `full` only for a deliberate
maintenance rewrite, and `REGION_TALK_YDB_SKIP_ROW_LEVEL_REWRITE=1` for
snapshot-only recovery.
