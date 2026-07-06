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
REGION_TALK_SEMANTIC_GATE_MODE=llm_required
# Debug-only, never for final quality claims:
# REGION_TALK_ALLOW_DETERMINISTIC_SEMANTIC_GATES=0
REGION_TALK_MAX_DISCOVERED_LINKS_PER_RUN=3000
REGION_TALK_MAX_NEW_SOURCE_CANDIDATES_PER_RUN=800
REGION_TALK_MAX_COMMENTS_PER_POST_FOR_LINKS=50
REGION_TALK_MAX_DISCOVERY_DEPTH_PER_RUN=2
```

`REGION_TALK_MAX_LLM_CALLS` is intentionally not part of the authoritative config. The run must reserve/finalize calls through Supabase `google_ai`; local/env counters may not be used as a substitute for the shared limiter.

YDB mode must be one of:

- configured dev/test namespace through `REGION_TALK_YDB_*`; or
- explicit dry-run JSON namespace under `artifacts/region-talk/runs/{run_id}/dry-run-state/`.

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

## MVP-1.x filtering order

`post fetched → freshness gate → cheap pre-LLM cost guard for obvious non-region/ad/low-substance rows → LLM final semantic gate through Supabase google_ai limiter → Kaggle-local actual-image scoring → reviewable/publication-ready split → candidate/favorite`.

The deterministic scope/ad/substance/news checks are evidence and cost guards only. They may prevent spending Supabase quota on obvious rejects, but accepted/reviewable semantic fit for publication is owned by the LLM final gate. Image scoring is skipped until `llm_decision=accept`, and skipped rows must expose `visual_scoring_stage`, `visual_scoring_skip_reason`, and `image_scoring_cost_saved=true`.

Comments are only for source discovery/link evidence and never publication material. Forwarded/reposted origins become source-frontier graph edges, not automatically monitored sources.

LLM-first note: deterministic checks are evidence only. Do not close a run as quality-screened if it produced `semantic_review_required` rows because the LLM semantic gate was not configured.

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

## MVP-1.z2 validation checklist

- `01_run_summary.increment_state_loaded=true` means a real previous dry-run state was loaded; `false` must be explained as `baseline run, not real increment`.
- `13_sources_monitored` includes every selected source, including VK/VKVideo/web rows as `skipped_*` when no fetcher/token exists.
- `vk_wall_probe_status` is visible in summary/source rows.
- `04a_final_shortlist` must not label rows as `reviewable_image` unless `image_reviewable=true`.
- LLM-accepted rows with weak media go to `10_good_text_weak_media`.
- `04b_needs_llm_retry` and `14c_llm_errors` keep useful headers even when there are no retry rows.
- `09_image_quality` and `19_image_model_observability` must show whether scoring was actual-image local CLIP or metadata fallback.
