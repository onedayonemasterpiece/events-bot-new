# Video producer lane results

## Scope

Implemented the Kaggle Telegram-monitor video producer in the assigned runtime,
notebook, secret-staging, environment-example, and focused test files only.
Implementation commit: `d018e068`.

## Delivered

- Event-confirmed gate before any video download/hash/cache/model/CDN work,
  including media-group caption/event resolution.
- Strict `<10 MiB` hint and actual-byte checks plus fail-closed Telegram video
  metadata rollout envelope (`0.50..0.80` width/height, at least `540x960`,
  duration `2..60s`).
- Dedicated `gemini-3.1-flash-lite` request through strict `GoogleAIClient`
  shared Supabase reserve/finalize control, explicit `GOOGLE_API_KEY3,KEY5`
  pool, no overflow/local/model fallback, and process-global six-call cap.
- Versioned exact-SHA Yandex sidecar cache for accepted/review/rejected semantic
  decisions. Cache hits never call the video model; accepted cache hits restore
  a missing content-addressed CDN object without re-analysis.
- Canonical T/V/M/L/U/R structured contract, strict score/confidence validation,
  deterministic aesthetic/showcase/per-event rank formulas, high-precision
  acceptance thresholds, risk gates, and fail-closed review behavior.
- Accepted-only Yandex CDN upload, multi-event relation payloads, source URL,
  relationship reason/confidence/rank, explicit `analysis_status=accepted`, and
  legacy URL/path aliases for current importer compatibility.
- Album merge preservation/deduplication of video fields and synchronized
  generated Kaggle notebook.
- Kaggle secret payload now requires and ships only text primary/fallback plus
  explicitly declared video-pool Google keys (alongside the legacy primary-key
  alias), with no unrelated Google keys.

## Validation

- `python3 -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/service.py tests/test_tg_monitor_gemma4_contract.py tests/test_telegram_monitor_service.py`
- `git diff --check`
- `uv run --with-requirements requirements.txt pytest -q tests/test_tg_monitor_gemma4_contract.py tests/test_telegram_monitor_service.py`
  - Result: `56 passed in 6.60s`
- Notebook regenerated with `source_parsing.telegram.service._sync_notebook_entrypoint`.

## Integration notes

- Parent/integrator owns DB/import/static-data schema, shared-object orphan
  cleanup, canonical docs/routes/changelog, release, and live Kaggle execution.
- The content-addressed paths emitted by this lane are
  `v/video/v1/<prefix>/<sha>.<ext>` and persistent sidecars are
  `v/analysis/v1/<prefix>/<sha>.json`; cleanup must never treat sidecars as
  event media or delete a shared video while any live event relation remains.
