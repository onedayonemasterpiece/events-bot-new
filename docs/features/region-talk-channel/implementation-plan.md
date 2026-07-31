# Implementation plan — Region Talk Channel

## MVP-0 Documentation

Done when:

- feature docs exist;
- YDB schema draft exists;
- source/post discovery design exists;
- image scoring design exists;
- Telegram/VK publication contracts exist;
- risk register exists;
- seed list and MVP-1 runbook exist;
- no production code/tokens/publishing introduced.

## Reuse existing Kaggle infrastructure

Before implementation, the Region Talk runner must inspect and reuse existing Kaggle runner / monitoring / artifact / status / lock / secrets patterns from this repository. Do not write a new self-made runner from scratch while working patterns exist.

Required local patterns found during this docs pass:

- Telegram Monitoring:
  - `kaggle/TelegramMonitor/telegram_monitor.py`
  - `kaggle/TelegramMonitor/kernel-metadata.json`
  - `source_parsing/telegram/service.py`
  - `source_parsing/telegram/split_secrets.py`
- CherryFlash:
  - `kaggle/CherryFlash/`
  - `scripts/run_cherryflash_live.py`
  - `tests/test_cherryflash_live_runner.py`
  - `tests/test_cherryflash_runtime_locator.py`
- Generic Kaggle status/registry:
  - `kaggle_status.py`
  - `kaggle/kaggle_status_client.py`
  - `kaggle_registry.py`
- Kaggle client/dataset push pattern:
  - `video_announce/kaggle_client.py`
- Static/offline artifact discipline:
  - `kaggle/StaticSiteBuilder/static_site_builder.py`
  - `scripts/run_static_site_builder_kaggle.py`

Reuse:

- run id generation;
- status ledger;
- run lock / resource lease;
- dry-run mode;
- progress events;
- artifact paths;
- secrets handling / encrypted split datasets where applicable;
- retry/backoff pattern;
- failure reporting;
- immutable run config;
- publication lock pattern if/when publishing is implemented;
- Telegram/VK publisher patterns only in later MVPs, never in MVP-1.

Search keys checked for this docs pass: `telegram_monitoring`, `telegram-monitoring`, `tg_monitor`, `cherryflash`, `cherry_flash`, `kaggle`, `run_status`, `run_lock`, `publish`.

If a later implementer cannot locate or reuse a specific Telegram Monitoring / CherryFlash pattern, they must document the missing files/search result here and fall back to generic `kaggle_status.py` + `kaggle_registry.py` conventions.

## MVP-1 Candidate Report Only

- Load [`seed-sources-v1.csv`](seed-sources-v1.csv).
- Normalize source identities and create/update YDB dev/test or dry-run JSON state.
- Fetch recent posts under strict caps.
- Run source discovery graph expansion but keep new sources as candidates.
- Score text/media.
- Use dual-model E5 + BGE-M3 recall enrichment/fusion, but do not require both
  models to be resident in one Kaggle process. CandidateReport may compute E5
  in the main run and consume BGE-M3 rows written by the clean
  `RegionTalkBgeM3Enrichment` worker.
- Include selected image model reports.
- Export cumulative/delta-aware XLSX and companion artifacts.
- No publishing and no publication tokens.

## MVP-1 test-run readiness

The first test run can start only when:

1. `seed-sources-v1.csv` exists.
2. YDB dev/test namespace is configured or dry-run JSON namespace is explicitly chosen.
3. Kaggle runner reuse decision is documented:
   - Telegram Monitoring / CherryFlash patterns reused; or
   - not found + generic `kaggle_status` / `kaggle_registry` fallback documented with search keys.
4. Run config exists:
   - `REGION_TALK_DRY_RUN=1`
   - `REGION_TALK_DISABLE_PUBLISH=1`
   - `REGION_TALK_MAX_SOURCES`
   - `REGION_TALK_MAX_POSTS_PER_SOURCE`
   - `REGION_TALK_MAX_IMAGES_PER_POST`
   - Supabase `google_ai` limiter (`google_ai_model_limits` + `google_ai_reserve`) for LLM call/key/rate budget; no local `REGION_TALK_MAX_LLM_CALLS` authority
   - `REGION_TALK_MAX_VLM_CALLS`
   - `REGION_TALK_SEED_FILE`
   - `REGION_TALK_OUTPUT_DIR`
5. No Telegram/VK publication token is required for MVP-1.
6. Image scoring can run in at least one practical mode:
   - `cv_only`;
   - `cv_aesthetic`;
   - `cv_aesthetic_clip`;
   - `cv_aesthetic_clip_vlm`.
7. XLSX artifact is generated with cumulative/delta sheets.
8. Report includes model image reports for selected strong photos.
9. Run audit is written.
10. All source/post/media/candidate ids are stable.
11. Same post is not duplicated across runs.
12. Manual reviewer can understand XLSX without reading logs.

## MVP-2 Favorites + manual approval

- Import/manual edit favorites decisions.
- Render Telegram/VK preview cards where rights allow.
- Add approval status.
- Still no auto-publish.

## MVP-3 Telegram dry-run/controlled publishing

- Create channel and bot admin setup outside docs.
- Publish only manually approved candidates.
- Ledger and idempotency.
- Dry-run first; controlled live only after evidence.

## MVP-4 VK dry-run/controlled publishing

- Verify VK image upload token path.
- Render carousel cards.
- Publish manually approved candidates.
- Ledger and fallback modes.

## MVP-5 Autonomous publishing

- Strict gates.
- Max 4 posts/day.
- Source diversity caps.
- Both platforms where allowed.
- Canary monitoring and rollback procedures.

## Open questions

1. Canonical public brand/handle for Telegram/VK surfaces.
2. Which YDB project/folder and credential lane should own the sidecar.
3. Whether MVP-1 writes to real YDB dev/test or dry-run JSON first.
4. Fusion policy for dual-model recall: top-K per model, score normalization, union/rerank weights and disagreement handling for e5-base + BGE-M3 enrichment rows.
5. Final model id/default env lane and quota budget must be visible in Supabase limiter/report summary; `GOOGLE_API_KEY2` is not a default for Region Talk; current scoped default is the Supabase-registered reserve lane `GOOGLE_API_KEY3`.
6. Media rights policy thresholds for `media_reuse_allowed`.
7. First-run caps for `REGION_TALK_MAX_SOURCES` and post/image budgets.
