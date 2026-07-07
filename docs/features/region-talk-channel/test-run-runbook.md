# MVP-1 test-run runbook — Region Talk Channel

Status: operational design for the first Candidate Report Only run. This runbook is not production code and does not require Telegram/VK publishing tokens.

## Purpose

Run one bounded offline discovery/scoring pass that reads [`seed-sources-v1.csv`](seed-sources-v1.csv), writes YDB dev/test or dry-run state, and exports a cumulative XLSX workbook with current-run delta.

## Implemented MVP-1 entrypoints

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` — Telethon-based bounded fetch/scoring/export script.
- `kaggle/execute_region_talk_candidate_report.py` — Kaggle push/poll/download launcher using private encrypted input datasets for secrets.
- `tests/test_region_talk_candidate_report.py` — workbook/seed/scoring smoke coverage.

Telegram reading is through Telethon (`TELEGRAM_AUTH_BUNDLE_DISCOVERY` preferred for this feature), not through Bot API.

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

Telegram monitoring/discovery is Telethon-based. Use the feature's discovery/Kaggle auth bundle by default; use `TELEGRAM_AUTH_BUNDLE_E2E` for a Kaggle run only when the operator explicitly tells this run to avoid the Discovery/S22 session because another agent/job owns it. Never run the same Telegram auth bundle concurrently in local and Kaggle contexts.

The runner must prefer cached Telegram entities and stop expanding work when the governor hits network-resolve/history/media/recommendation caps. A `FloodWait` above `REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS` is recorded as cooldown/degraded mode and the workbook is still written. Similar-channel discovery is limited by `REGION_TALK_TG_SIMILAR_*` and only adds source-frontier candidates; it must not join channels or publish anything.

For z6 throughput validation, the next real run should target at least `sources_history_fetched_ok >= 25` without bypassing FloodWait/cooldown evidence. If it misses, `00_product_summary` and `20_telegram_rate_observability` must show whether the blocker was network resolve budget, history source budget, cached entity coverage, FloodWait or Telegram errors.

`public_travel_blogger_channel_links.xlsx` is copied into the Kaggle private input dataset by the launcher when present under `artifacts/`. It is imported into source frontier only and deduped in `12a_source_frontier_unique`.

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
- Entity-cache observability includes loaded path/write path/hit rate/resolved-source counts.
- Metadata-only image rows are `needs_actual_image_fetch`, not final weak/strong image decisions.
- Single-location Kaliningrad cards are not rejected as multi-region roundups.
- Supabase `google_ai` limiter remains authoritative; no direct `GOOGLE KEY2` bypass; no publishing; no secrets in XLSX.

- Kaggle output download is filtered to report/log/state files by default; media files remain in Kaggle output and are not pulled locally unless `REGION_TALK_KAGGLE_OUTPUT_FILE_PATTERN` is overridden.
- The mounted `region_talk_run_config.json` is authoritative for per-run `REGION_TALK_*` controls, including `REGION_TALK_RUN_ID`, so a stale Kaggle environment cannot rename a fresh run.
