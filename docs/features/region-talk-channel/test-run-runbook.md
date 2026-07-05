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
REGION_TALK_SEED_FILE=docs/features/region-talk-channel/seed-sources-v1.csv
REGION_TALK_OUTPUT_DIR=artifacts/region-talk/runs/${RUN_ID}
REGION_TALK_MAX_SOURCES=5
REGION_TALK_MAX_POSTS_PER_SOURCE=20
REGION_TALK_MAX_IMAGES_PER_POST=8
REGION_TALK_MAX_LLM_CALLS=10
REGION_TALK_MAX_VLM_CALLS=10
REGION_TALK_IMAGE_SCORING_MODE=cv_only|cv_aesthetic|cv_aesthetic_clip|cv_aesthetic_clip_vlm
```

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
