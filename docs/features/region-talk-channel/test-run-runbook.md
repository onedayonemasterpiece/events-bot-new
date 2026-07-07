# MVP-1 test-run runbook — Region Talk Channel

Status: operational design for the first Candidate Report Only run. This runbook is not production code and does not require Telegram/VK publishing tokens.

## Purpose

Run one bounded offline discovery/scoring pass that reads [`seed-sources-v1.csv`](seed-sources-v1.csv), writes YDB dev/test or dry-run state, and exports a cumulative XLSX workbook with current-run delta.

## Implemented MVP-1 entrypoints

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` — Telethon-based bounded fetch/scoring/export script.
- `kaggle/execute_region_talk_candidate_report.py` — Kaggle push/poll/download launcher using private encrypted input datasets for secrets.
- `tests/test_region_talk_candidate_report.py` — workbook/seed/scoring smoke coverage.

Telegram reading is through Telethon, not through Bot API. Role-scoped manual Region Talk runs use `TELEGRAM_AUTH_BUNDLE_DISCOVERY` for CandidateReport and ImageDiagnostic, and only the E2E human session for local Saved Messages delivery. `TELEGRAM_AUTH_BUNDLE_S22` remains reserved for production Kaggle/remote monitoring and is not packaged unless explicitly selected as `REGION_TALK_AUTH_BUNDLE_ENV`.

## Hard stop rules

- `REGION_TALK_DRY_RUN=1` is required.
- `REGION_TALK_DISABLE_PUBLISH=1` is required.
- Do not create Telegram/VK channel/community.
- Do not call Telegram/VK publisher paths.
- Do not add SQLite tables for this feature.
- Do not print secrets in logs/artifacts/notebooks.

## Reuse existing Kaggle infrastructure

Before implementation, inspect and reuse repo patterns from:

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/service.py`
- `source_parsing/telegram/split_secrets.py`
- `kaggle/CherryFlash/`
- `scripts/run_cherryflash_live.py`
- `kaggle_status.py`
- `kaggle/kaggle_status_client.py`
- `kaggle_registry.py`
- `video_announce/kaggle_client.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`

Reuse run id generation, status ledger, progress events, locks/leases, artifact layout, immutable run config, encrypted/Kaggle secret handling, retry/backoff and failure reporting. Do not write a new runner from scratch while these patterns exist.

Search keys already checked in this docs pass:

- `telegram_monitoring`, `telegram-monitoring`, `tg_monitor`
- `cherryflash`, `cherry_flash`
- `kaggle`, `run_status`, `run_lock`, `publish`

Found local patterns include TelegramMonitor, CherryFlash, generic `kaggle_status`/`kaggle_registry`, StaticSiteBuilder and KaggleClient. If a later implementer cannot use a specific contour, document the reason and fall back to generic `kaggle_status.py` + `kaggle_registry.py` conventions.

## Minimal run config

```bash
REGION_TALK_DRY_RUN=1
REGION_TALK_DISABLE_PUBLISH=1
REGION_TALK_SEED_FILE=docs/features/region-talk-channel/seed-sources-v2.csv
REGION_TALK_PLACE_LEXICON_FILE=docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv
REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE=public_travel_blogger_channel_links.xlsx
REGION_TALK_OUTPUT_DIR=artifacts/region-talk/runs/${RUN_ID}
REGION_TALK_MAX_SOURCES=30
REGION_TALK_MAX_POSTS_PER_SOURCE=50
REGION_TALK_MAX_IMAGES_PER_POST=8
# LLM call/key/rate limits are strict Supabase google_ai limiter state, not env counters.
# Required for live/Kaggle quality runs: SUPABASE_URL + SUPABASE_KEY/SUPABASE_SERVICE_KEY.
REGION_TALK_MAX_VLM_CALLS=10
REGION_TALK_LLM_MODEL=gemini-3.1-flash-lite
REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME=GOOGLE_API_KEY3
REGION_TALK_IMAGE_SCORING_MODE=cv_aesthetic_clip
REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING=1
REGION_TALK_MIN_POST_DATE=2026-01-01
REGION_TALK_FRESHNESS_HALF_LIFE_DAYS=30
REGION_TALK_SEMANTIC_GATE_MODE=vector_first_final_llm
REGION_TALK_ENABLE_EARLY_LLM=0
REGION_TALK_ENABLE_VECTOR_GATES=1
REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS=1
REGION_TALK_TARGET_LLM_CALLS=10
REGION_TALK_MAX_LLM_FINAL_VERIFY=10
# Debug-only, never for final quality claims:
# REGION_TALK_ALLOW_DETERMINISTIC_SEMANTIC_GATES=0
REGION_TALK_MAX_DISCOVERED_LINKS_PER_RUN=3000
REGION_TALK_MAX_NEW_SOURCE_CANDIDATES_PER_RUN=800
REGION_TALK_MAX_COMMENTS_PER_POST_FOR_LINKS=50
REGION_TALK_MAX_DISCOVERY_DEPTH_PER_RUN=2
REGION_TALK_TG_GOVERNOR_ENABLED=1
REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN=300
REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN=8
REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN=40
REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE=25
REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN=60
REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN=20
REGION_TALK_MAX_NEW_SOURCE_PROBES=30
REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS=60
REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS=300
REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS=1800
REGION_TALK_TG_SIMILAR_ENABLED=1
REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN=20
REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED=10
REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN=150
```

z7 growth-run override:

```bash
REGION_TALK_DISCOVERY_MODE=mixed
REGION_TALK_HISTORY_SCAN_MODE=primary_and_delta
REGION_TALK_MAX_SOURCES=120
REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN=800
REGION_TALK_HISTORY_SOURCES_TARGET=100
REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN=100
REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN=100
REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN=100
REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN=100
REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN=1000
REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY=1
REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES=30
REGION_TALK_MEDIA_SCORING_MODE=retry_queue_first
REGION_TALK_ACTUAL_IMAGE_TARGET=30
REGION_TALK_REQUIRE_PREVIOUS_STATE=1
```


## Telegram session and human-like discovery constraints

Telegram monitoring/discovery is Telethon-based and role-scoped. Use `TELEGRAM_AUTH_BUNDLE_DISCOVERY` for CandidateReport and ImageDiagnostic Region Talk manual runs unless the operator explicitly changes the role mapping for a single run. Do **not** use `TELEGRAM_AUTH_BUNDLE_E2E` for Kaggle kernels; it is reserved for local live E2E / Saved Messages delivery. Never run the same Telegram auth bundle concurrently in local and Kaggle contexts.

The runner must prefer cached Telegram entities and stop expanding work when the governor hits network-resolve/history/media/recommendation caps. A `FloodWait` above `REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS` is recorded as cooldown/degraded mode and the workbook is still written. Similar-channel discovery is limited by `REGION_TALK_TG_SIMILAR_*` and only adds source-frontier candidates; it must not join channels or publish anything.

For z6 throughput validation, the next real run should target at least `sources_history_fetched_ok >= 25` without bypassing FloodWait/cooldown evidence. If it misses, `00_product_summary` and `20_telegram_rate_observability` must show whether the blocker was network resolve budget, history source budget, cached entity coverage, FloodWait or Telegram errors.

`public_travel_blogger_channel_links.xlsx` is copied into the Kaggle private input dataset by the launcher when present under `artifacts/` in either the active worktree or canonical `/home/dev/projects/events-bot-new/artifacts/`. Telegram/VK rows are imported into the single canonical `12_source_queue`; non-target URLs stay out of that product queue and may appear only in diagnostic/quarantine sheets.

`REGION_TALK_MAX_LLM_CALLS` is intentionally not part of the authoritative config. The run must reserve/finalize calls through Supabase `google_ai`; local/env counters may not be used as a substitute for the shared limiter.

YDB mode must be one of:

- configured dev/test namespace through `REGION_TALK_YDB_*`; or
- explicit dry-run JSON namespace under `artifacts/region-talk/runs/{run_id}/dry-run-state/`.


For z8 product-acceleration runs, prefer explicit state settings. Use `REGION_TALK_STATE_BACKEND=ydb` only when the YDB env set is provisioned; otherwise run with fallback intentionally visible (`REGION_TALK_STATE_BACKEND=ydb`, `REGION_TALK_REQUIRE_YDB_STATE=0`) and require the XLSX to show `state_backend=json_fallback`, `state_fallback_used=true` and the YDB fail reason. A production-like acceptance run must not silently use JSON state.

Minimum z8 acceptance evidence in `01_run_summary` / `00_product_summary`:

- state: `state_backend`, `previous_state_loaded`, `previous_state_hash`, `state_write_status`, `state_fallback_used`, `ydb_read_status`, `ydb_write_status`;
- discovery: catalog/import coverage, similar seed/raw/unique counts, keyword query/source counts, duplicate/self-loop counts;
- scanning: primary and delta scanned this run/all time, new posts and delta-new posts;
- conversion: per-1000 source metrics plus `sample_bias_note`;
- product: current-run reviewable candidates, candidate memory, publication-ready/favorites/final candidates and actual-image retry metrics.

## Expected artifacts

- `artifacts/region-talk/runs/{run_id}/region-talk-candidates-{run_id}.xlsx`
- `artifacts/region-talk/candidates-latest.xlsx`
- XLSX files must be desktop-Excel compatible OpenXML packages, including workbook relationships, `xl/styles.xml`, and `docProps/*`; do not rely on a zip-only smoke check.
- CSV/JSON/Markdown/HTML companions as defined in [MVP candidate report](mvp-candidate-report.md)
- run audit JSON
- redacted log excerpt if needed

## Human acceptance for first run

The reviewer should be able to answer from XLSX alone:

1. Which seeds were loaded?
2. Which sources were actually scanned?
3. Which new sources were discovered from graph/catalogs?
4. Which posts are new this run?
5. Which posts became candidates because of strong photos?
6. Which posts dropped and why?
7. Which image model reports explain the selected photos?
8. Which candidates need manual review/favorite/reject decisions?

## MVP-1.x/z5 filtering order

`post fetched → freshness gate → Kaliningrad-only scope → deterministic ad/event evidence → local/prototype vector gate → Kaggle-local actual-image scoring → cumulative candidate memory/manual shortlist → optional small final LLM verifier`.

The broad funnel must be local/vector-first: `REGION_TALK_ENABLE_EARLY_LLM=0` by default, so fetched posts, current-run posts and wide review queues are not mass-classified by LLM. Obvious ads/events/news/roundups are rejected before LLM through deterministic/vector gates. Actual-image scoring may run before the optional final verifier so expensive LLM calls are saved for top/ambiguous finalists only. Acceptance evidence is `wide_funnel_llm_calls=0`, `14d_llm_usage_by_stage`, vector reject counts and image rows scored before LLM.

Comments are only for source discovery/link evidence and never publication material. Forwarded/reposted origins become source-frontier graph edges, not automatically monitored sources.

## MVP-1.y reviewable pre-candidate policy

If the LLM semantic gate is required but Supabase limiter/RPC is unavailable, rows are fail-closed into `pre_candidate_needs_llm` with `llm_gate_status=not_run_supabase_limiter_unavailable`; there is no direct SDK/key fallback. If Supabase reserve/provider returns quota/error, rows become `needs_llm_retry` and are exported in `04b_needs_llm_retry` / `14c_llm_errors`.

Recommended next dry-run:

```bash
REGION_TALK_MAX_VLM_CALLS=20
REGION_TALK_LLM_MODEL=gemini-3.1-flash-lite
REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME=GOOGLE_API_KEY3
GOOGLE_AI_ALLOW_RESERVE_FALLBACK=0
GOOGLE_AI_LOCAL_LIMITER_FALLBACK=0
GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR=0
REGION_TALK_IMAGE_SCORING_MODE=cv_aesthetic_clip
REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING=1
```

The XLSX has:

- `00_product_summary` — product-readable counts and model/limit source;
- `03_funnel` — sequential funnel;
- `03b_gate_counts` — independent evidence counts;
- `04a_final_shortlist` — human shortlist, not debug trash;
- `04b_needs_llm_retry` / `14c_llm_errors` — rows blocked by Supabase/provider quota/errors;
- `04c_debug_rejects` — obvious non-region/ad/low-substance rejects;
- `14b_pre_candidates_needing_llm` — reviewable rows waiting for semantic model/manual review.
- `19_image_model_observability` — model id/type/runtime/input/device and fallback honesty for image scoring.
- `12a_source_frontier_unique` — deduped next-source frontier from seeds, public blogger workbook, links and Telegram similar-channel recommendations.
- `12b_telegram_similar_channels` — raw Telegram recommendations/status/errors.
- `12d_similar_seed_queue` — persistent recursive seed queue for the next similar-channel pass.
- `12e_telegram_keyword_discovery` — source candidates found by bounded Telegram keyword search over Kaliningrad toponyms.
- `12f_source_classification` — local/provisional source geo/topic/value classes.
- `13b_source_delta_scan` — per-source cursor/delta audit.
- `20_telegram_rate_observability` — Excel-safe request-governor/FloodWait sheet name for the longer P0 `20_telegram_rate_limit_observability`.
- `24_source_yield_metrics` — yield per 1000 scanned sources.

## MVP-1.z2 validation checklist

- `01_run_summary.increment_state_loaded=true` means a real previous dry-run state was loaded; `false` must be explained as `baseline run, not real increment`.
- `13_sources_monitored` includes every selected source, including VK/VKVideo/web rows as `skipped_*` when no fetcher/token exists.
- `vk_wall_probe_status` is visible in summary/source rows.
- `04a_final_shortlist` must not label rows as `reviewable_image` unless `image_reviewable=true`.
- LLM-accepted rows with weak media go to `10_good_text_weak_media`.
- `04b_needs_llm_retry` and `14c_llm_errors` keep useful headers even when there are no retry rows.
- `09_image_quality` and `19_image_model_observability` must show whether scoring was actual-image local CLIP or metadata fallback.
- `09a_image_candidate_queue` shows the media-acquisition/scoring cursor and the next ~30 image rows to process; `09d_image_driven_top` is sorted by actual-image model scores only.


## MVP-1.z3 validation checklist

- `00_product_summary` exposes git provenance, Telegram governor metrics, similar-channel counts and source-frontier counts.
- `12a_source_frontier_unique` contains public blogger workbook rows and any Telegram similar-channel rows, deduped by normalized URL/source id.
- `12b_telegram_similar_channels` exists even if the run has no recommendations or Telethon reports `not_supported_by_telethon_version`.
- `20_telegram_rate_observability` exists and shows request caps, cache/network resolve counts, FloodWait/cooldown/degraded-mode state and private ledger path.
- Public XLSX sheets contain no Telegram `channel_id`, `access_hash`, auth bundles, tokens or raw private payloads.

## MVP-1.z6 validation checklist

- `01_run_summary.wide_funnel_llm_calls=0`; any LLM use is only `final_verifier_llm_calls` for top-N rows and remains Supabase-limiter controlled.
- Non-Kaliningrad/multi-region rows do not appear in `06a_candidate_memory`, `04a_final_shortlist` or `21_manual_review_queue`.
- `01_run_summary` exposes `history_sources_target`, attempted/ok/new/cached/network counts, `history_fetch_runtime_seconds`, `posts_per_source_distribution`, `similar_seed_queue_total` and `similar_seed_queue_ready`.
- `12d_similar_seed_queue`, `13b_source_delta_scan` and `24_source_yield_metrics` exist even when empty.
- `09b_image_fetch_retry_queue` is the active retry queue for metadata-only/image-fetch-pending rows; actual visual quality claims require actual-image local model rows in `09_image_quality`.

## MVP-1.z7 validation checklist

- `01_run_summary.previous_state_loaded=true`, `previous_state_hash` and `latest_state_hash` are visible.
- All-time metrics exist: `sources_primary_scanned_total_all_time`, `telegram_sources_primary_scanned_total_all_time`, `sources_delta_scanned_total_all_time`, `frontier_pending_primary_scan_total`, `posts_memory_total`, `publication_ready_total_all_time`.
- `similar_seed_queue_used_total` is consistent with `telegram_similar_channels_seed_count`; used seeds no longer stay at `similar_seed_use_count=0`.
- `keyword_search_queries_processed >= 30` unless FloodWait/cooldown/error is explicit.
- `12e_telegram_keyword_discovery` and `12f_source_classification` exist even when empty/error.
- `source_geo_class`, `source_topic_class`, `has_firsthand_visit_evidence`, `visit_evidence_type`, `publication_story_score` are populated for shortlist/candidate-memory rows.
- `favorites_candidates_consistency_status=ok`; blank `_sheet_note` placeholders are not counted as favorites/candidates.
- `run_events.jsonl`, `candidate_found.jsonl`, and `stage_status.json` are present in Kaggle output.


## MVP-1.z4 validation checklist

- `06a_candidate_memory` exists and keeps previous candidates even when their source is not refetched.
- `06b_candidate_memory_top`, `21_manual_review_queue` and `22_candidate_deltas` make cumulative review possible even if `04a_current_run_shortlist` is empty.
- `07b_prev_candidates_not_refetch` shows retained previous candidates skipped by budget/cooldown/not-configured source status.
- `12c_source_frontier_queue_next` contains a small but nonzero actionable queue from P0/P1/P2 frontier rows; 500+ catalog rows must not be invisible.
- `12_source_queue` is the canonical single Telegram/VK URL queue: it is deduped, has one cursor, has `added_at`, color/status hints, shows processed rows above pending rows, and imports Telegram/VK rows from `public_travel_blogger_channel_links.xlsx`.
- Entity-cache observability includes loaded path/write path/hit rate/resolved-source counts.
- Metadata-only image rows are `needs_actual_image_fetch`, not final weak/strong image decisions.
- Single-location Kaliningrad cards are not rejected as multi-region roundups.
- Supabase `google_ai` limiter remains authoritative; no direct `GOOGLE KEY2` bypass; no publishing; no secrets in XLSX.

- Kaggle output download is filtered to report/log/state files by default; media files remain in Kaggle output and are not pulled locally unless `REGION_TALK_KAGGLE_OUTPUT_FILE_PATTERN` is overridden.
- The mounted `region_talk_run_config.json` is authoritative for per-run `REGION_TALK_*` controls, including `REGION_TALK_RUN_ID`, so a stale Kaggle environment cannot rename a fresh run.
- Waited Kaggle runs delete their temporary private input datasets (`region-talk-config-*`, `rt-secret-bundle-*`) after output download. Use `--keep-input-datasets` only when debugging a still-running kernel; otherwise secret/config datasets must not accumulate in Kaggle.

## z8/z9 YDB and VK smoke requirements

- `REGION_TALK_STATE_BACKEND=ydb` is production-like only when `REGION_TALK_YDB_ENDPOINT` and `REGION_TALK_YDB_DATABASE` are set. `REGION_TALK_YDB_ENDPOINT` may be the full YC value with `?database=...`; the runner strips the trailing slash and extracts `REGION_TALK_YDB_DATABASE`.
- Kaggle YDB auth must be encrypted: pass `REGION_TALK_YDB_IAM_TOKEN`, `YC_IAM_TOKEN`, `YDB_ACCESS_TOKEN`, or `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON` through the encrypted secrets bundle, never through the public config dataset.
- YDB stores compact valuable state only: run metrics, source cursors, channel/source links, processed post links/keys/stages, candidate lifecycle and image scoring metrics. Raw post text, raw payloads and media bytes stay out of YDB and can be re-fetched from post URLs.
- Before a product run, probe the row-level YDB queues and prune/repair garbage:
  `source_queue_item` must contain only Telegram channel roots and VK
  community/wall roots; `image_queue_item` must contain only text-confirmed
  Kaliningrad Oblast candidate-post media. Do not keep invalid/rejected post
  rows in YDB just for audit: source cursors and aggregate counters are the
  durable evidence that a channel was scanned.
- CandidateReport and ImageDiagnostic heartbeat rows are separate:
  `latest_business_heartbeat` for source/text processing and
  `latest_business_heartbeat:image_diagnostic` for image scoring. Poll both while
  the notebooks run.
- Run notebooks sequentially when launching manually. For Region Talk validation,
  CandidateReport and ImageDiagnostic default to the Discovery bundle
  (`TELEGRAM_AUTH_BUNDLE_DISCOVERY`), so do not overlap them on Kaggle. Keep
  `TELEGRAM_AUTH_BUNDLE_S22` reserved for production Kaggle/remote monitoring
  unless explicitly changed. Never run two Kaggle kernels against the same
  Telethon auth key.
- CandidateReport vector/report assembly is bounded by `REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN` (default `180`) and emits `report_build_started`, text-embedding model pass events, periodic `vector_scoring_alive`, `vector_scoring_done` and `report_write_started` business heartbeats. Rows beyond the per-run scoring cap are listed in `02b_runtime_deferred_posts` for the next bounded run; this is report visibility, not raw-post durable YDB storage. Real E5+BGE-M3 scoring is sequential by model pass (load/score/release E5, then load/score/release BGE-M3) so Kaggle does not hold both text encoders in memory simultaneously. For model/runtime validation without discovery, run CandidateReport with `REGION_TALK_POST_INPUT_MODE=ydb_candidate_links`, `REGION_TALK_AUTH_BUNDLE_ENV=TELEGRAM_AUTH_BUNDLE_DISCOVERY`, `REGION_TALK_DISCOVERY_MODE=off` and a local/non-YDB state backend; this reads only existing YDB candidate/image post links and refetches those public Telegram posts for text.
- Each required text-embedding model pass is bounded by
  `REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS` (default `300`) and runs in
  its own subprocess by default (`REGION_TALK_TEXT_EMBEDDING_SUBPROCESS=1`).
  Hugging Face Hub timeouts are set before import
  (`REGION_TALK_HF_HUB_DOWNLOAD_TIMEOUT`, `REGION_TALK_HF_HUB_ETAG_TIMEOUT`,
  `REGION_TALK_HF_HUB_DISABLE_XET`). If a pass times out, CandidateReport emits
  `text_embedding_batch_deferred`, writes state/report, and defers scoring
  rather than silently replacing the accepted dual-vector process.
- For true live-YDB product runs, the CandidateReport Kaggle runner performs a
  local YDB preflight when `REGION_TALK_REQUIRE_YDB_STATE=1`; set
  `REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL=1` for production-like
  semi-manual runs so a short-lived/expired user IAM token cannot launch a
  notebook that later fails to write online state.
  The preferred credential is `REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON` either
  in the local encrypted runner bundle or as a Kaggle User Secret with the same
  name. If the secret exists only in Kaggle, set
  `REGION_TALK_ALLOW_KAGGLE_YDB_SECRET=1`; this intentionally skips the local
  YDB driver preflight and shifts the proof requirement to the notebook's first
  YDB heartbeat/row-level write. Do not use this mode as completion evidence
  until live YDB rows are observed.
- Final product queue assembly lives in CandidateReport, not ImageDiagnostic: CandidateReport joins text/vector/source evidence with `actual_scored` image rows from YDB, writes row-level `publication_candidate_item` records, and exposes `04p_publication_queue` / `04q_publication_confirmed`. ImageDiagnostic remains the visual scoring notebook.
- Source scanning must be cursor/due driven, not “top seeds every run”.
  `source_queue_item.source_queue_status=processed_*` plus `_ydb_updated_at`/
  `source_cursors.next_history_scan_at` suppresses immediate re-scan until the
  delta due time or `REGION_TALK_SOURCE_RESCAN_PROCESSED_AFTER_SECONDS` elapses;
  `pending_scan` / `needs_rescan_or_retry` rows stay eligible.
- In semi-manual discovery mode, online YDB row-level writes are mandatory:
  CandidateReport must upsert `source_status_item`/`source_queue_item` when
  sources are selected/discovered/status-changed, `source_candidate_item` and
  `source_edge_item` when discovery sees a new public/channel/edge,
  `comment_link_item` for redacted comment-link evidence if comment discovery is
  enabled, `processed_post_item`/`post_live_item` when posts are fetched/scored,
  `candidate_memory_item` after memory build, and image/publication queue rows
  immediately after queue assembly. ImageDiagnostic must upsert every image
  lease, media-fetch result and final score/status per row, not only at notebook
  end.
  Send periodic operator stats with `scripts/region_talk_goal_notify.py --stats`; it must read row-level YDB state, not heartbeat-only rows.
- The 20-candidate product goal is tracked in YDB `publication_goal` with `target_confirmed=20` and `llm_budget_max=100`; Gemini Lite confirmations must go through the Supabase limiter. Use local `scripts/region_talk_goal_notify.py` with the E2E human session to send confirmed links to the operator chat and mark them as sent.
- Semantic prototype vectors are cached in YDB as `semantic_bank_embedding`
  rows keyed by semantic-bank hash and embedding model, so only fresh post query
  vectors are recomputed every run.
- VK read-only wall fetch should use `VK_SERVICE_KEY`/`VK_SERVICE_TOKEN` before IP-bound user tokens (`REGION_TALK_VK_READ_SERVICE_FIRST=1`). `error_code=5` from `VK_ACCESS_TOKEN` means the token is bound to another IP and is not suitable for Kaggle wall reads.
- VK catalog/search/private pages are not global failures: they should be reported as `domain_missing` or `access_denied`. Acceptance for smoke is at least one real `vk_wall_probe_status=ok` when a public VK wall source is selected.
- Kaggle status events must include factual `state_backend`, `ydb_read_status`, `ydb_write_status`, `ydb_state_mode`, `vk_wall_probe_status` in preflight/report logs so failures can be diagnosed without opening the workbook.

## z9 product-loop run additions

After the YDB/VK smoke fix, the next product dry-run is expected to use full discovery/scoring budgets rather than a shortened smoke profile. The report now includes explicit product-loop observability:

- `12a_active_tg_vk_frontier` — active scan frontier restricted to Telegram/VK.
- `12g_external_links_quarantine` — website/YouTube/Dzen/Rutube and other non-target links retained for observability but excluded from active scan metrics.
- `12e_keyword_posts` — Telegram keyword post hits with source URL, post URL, query and pipeline note; keyword hits are discovery/context only and do not auto-accept posts.
- `04k_keyword_hit_candidates` — keyword-hit sources that classification ranks into the nonlocal product-priority pool.
- `09_image_quality` — actual-image rows only; metadata/fallback rows go to `09c_image_debug_fallback`.
- `candidate_found.jsonl` includes `new_nonlocal_ko_channel_found`, `keyword_hit_candidate_found`, `reviewable_image_candidate_found`, `publication_ready_candidate_found` in addition to retry/debug events.

The full product dry-run should keep `REGION_TALK_DISABLE_PUBLISH=1`, but should not artificially lower source, keyword, similar-channel or actual-image targets unless debugging a broken infrastructure path.
