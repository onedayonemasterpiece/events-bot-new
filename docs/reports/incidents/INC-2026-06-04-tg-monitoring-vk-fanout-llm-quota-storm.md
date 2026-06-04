# INC-2026-06-04 Telegram Monitoring VK fanout and LLM quota storm

Status: open
Severity: sev1
Service: Telegram Monitoring / Smart Update / VK fanout / CherryFlash
Opened: 2026-06-04
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel`, `INC-2026-06-03-smart-update-flash-lite-rpd`, `INC-2026-05-18-konb-cherryflash-render-lock-and-empty-selection`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/llm-gateway/README.md`, `docs/features/vk-publishing/README.md`, `docs/features/promo-campaigns/README.md`, `docs/operations/runtime-logs.md`

## Summary

After the focused `@kraftmarket39/271` repair, the named event reached VK, but other events imported through Telegram Monitoring still did not appear in VK. At the same time production showed CherryFlash `AuthKeyDuplicatedError`, Google AI `RateLimitError(blocked_reason='rpd')`, Gemma 4 31B RPM overrun, and Smart Update fallback pressure toward 4o.

## User / Business Impact

- Telegram Monitoring can import events without reliable downstream VK publication.
- Promo/festival surfaces can silently under-deliver because only a single repaired event is visible in VK.
- CherryFlash scheduled production can fail or lose Telegram session validity.
- LLM fallback can burn 4o budget while additional Google keys remain unused or mis-scoped.

## Detection

- User reported that only the explicitly repaired event appeared in VK; manual `/tg` followed by VK update produced no new VK events.
- User reported CherryFlash `AuthKeyDuplicatedError`.
- User reported `RateLimitError(blocked_reason='rpd')` and Gemma 4 31B RPM exceeding limit.
- User added local `TELEGRAM_AUTH_BUNDLE_S22_2` and a fourth local Google key for potential production rotation.

## Timeline

- 2026-06-04: `@kraftmarket39/271` event `5656` was repaired and published to VK.
- 2026-06-04: broader Telegram Monitoring → VK fanout and LLM quota failures were reported.

## Root Cause

1. Telegram Monitoring import had no final reconciliation guard for already-imported Telegram-origin events that were
   missing VK fanout. A successful import could therefore leave active future events without `vk_sync` rows; repeated
   `/tg` did not repair the backlog unless the exact Smart Update status path re-armed tasks.
2. `enqueue_job(vk_sync)` treated any latest `done` job as terminal even if the event still had no managed klgdevents
   `source_vk_post_url`, so stale/empty VK outcomes could block requeue.
3. Google AI reserve overflow depended on Supabase `google_ai_api_keys` metadata, but the newly supplied
   `GOOGLE_API_KEY4` was initially only local/prod env. An env-only key is invisible to the shared reserve RPC.
4. Smart Update's Gemma→4o fallback was binary and unbudgeted, so a mass Gemma quota/provider failure could convert many
   cheap-stage requests into expensive 4o calls.
5. Production logs after catch-up showed `smart_update` stages hitting Gemma 4 provider `500 INTERNAL`; the shared
   `GoogleAIClient` default retry loop retried the provider call three times, and each retry made a fresh reservation
   against Gemma 4 31B RPM/RPD before Smart Update reached its own fallback decision.
6. After VK fanout reconciliation, `vk_sync` jobs were present but still starved: the outbox global priority put
   `vk_sync` after all Telegraph/ICS/page jobs, so ready VK publications could wait behind unrelated rebuild backlog.

## Contributing Factors

- The previous incident had already exposed import-boundary and promo-hand-off gaps.
- Google AI limiter/key metadata may be out of sync with runtime secrets.
- Telegram auth bundles are role-scoped; reusing S22 concurrently can invalidate remote sessions.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring import, Smart Update scheduling, `JobOutbox(vk_sync)`, VK fanout, promo VK, CherryFlash Telegram sessions, or Google AI limiter/key routing.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- `main.py` / `main_part2.py` job outbox and `vk_sync`
- `promo.py`
- `google_ai/client.py`
- Fly secrets / runtime env
- Kaggle Telegram auth and Google key secrets
- CherryFlash scheduled runs

### Mandatory checks before closure or deploy

- Production evidence for current Telegram Monitoring/Smart Update/JobOutbox runs and any active fallback storm.
- Verify S22 session secret source and update production/Kaggle path without reusing E2E auth.
- Verify Google key registry rows and runtime secrets for every intended key.
- Verify Gemma 4 31B RPM/RPD reserve behavior and concurrent-run behavior.
- Verify imported active/non-silent Telegram events either have done `vk_sync` jobs or a documented terminal reason.
- Run targeted regression tests for changed surfaces.
- Deploy from clean `origin/main`-reachable SHA and collect `/healthz`.
- Run compensating catch-up for affected Telegram imports and verify VK evidence.

### Required evidence

- Runtime logs / Fly logs / `ops_run` rows for Telegram Monitoring, Smart Update, CherryFlash, and Google AI failures.
- Production DB queries for affected recent Telegram imports and VK job state.
- Fly/Kaggle secret names updated without exposing secret values.
- Deployed SHA and image if code/config changes are deployed.
- Post-fix VK URLs or terminal diagnostics for affected imported events.

## Immediate Mitigation

- Added `GOOGLE_API_KEY4` as a Fly production secret without replacing existing Google keys.
- Added `GOOGLE_API_KEY4` to `google_ai_api_keys` metadata and prioritized it as emergency overflow.
- Set `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS=GOOGLE_API_KEY4,GOOGLE_API_KEY3,GOOGLE_API_KEY2` on Fly production.
- Replaced the production S22 Telegram auth value with the user-provided `TELEGRAM_AUTH_BUNDLE_S22_2` and also exposed
  `TELEGRAM_AUTH_BUNDLE_S22_2` as a production secret for auditability. `TELEGRAM_AUTH_BUNDLE_E2E` was not used.
- Temporarily set `SMART_UPDATE_4O_FALLBACK=0` while old code lacked a budget guard, to stop ongoing fallback spend
  before deploying the limited fallback implementation.

## Corrective Actions

- Add Smart Update 4o fallback hourly budget (`SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR`) while keeping 4o available for
  isolated emergency calls.
- Cap Smart Update's internal Google AI provider retries separately (`SMART_UPDATE_GOOGLE_AI_MAX_RETRIES`, default `1`)
  so mass Smart Update catch-up does not multiply Gemma RPM/RPD burn on provider `500/504` before reaching the bounded
  4o fallback path.
- Requeue `vk_sync` when a stale `done` job exists but the event has no managed VK URL.
- Add Telegram Monitoring post-import reconciliation for active future non-silent Telegram-origin events missing VK
  fanout.
- Raise `vk_sync` global outbox priority above unrelated rebuild tasks while preserving the existing per-event
  prerequisite check (`id < current and pending/running`) so an event still waits for its own Telegraph/ICS jobs.

## Follow-up Actions

- [ ] Add alert/reporting for imported active/non-silent events missing `vk_sync`/managed VK evidence.
- [ ] Add Google AI key-rotation smoke that compares runtime env secrets with `google_ai_api_keys`.
- [ ] Add concurrency/RPM regression coverage for Telegram Monitoring and Smart Update Gemma 4 lanes.

## Release And Closure Evidence

- deployed SHA: `68fccbf4036827c89f834ccd9c58e39420ef0f60` (`origin/main`)
- deploy path: `flyctl deploy -a events-bot-new-wngqia --remote-only`
- deployed image: `registry.fly.io/events-bot-new-wngqia:deployment-01KT9F2PB6ASX73JH1KY1EPKDM`
- regression checks: `tests/test_smart_update_native_schema.py tests/test_tg_monitor_reprocess_incomplete_scan.py tests/test_job_dedup.py`
  printed `35 passed`; the pytest process hung after the passing summary and was terminated. A broader
  `tests/test_vk_source.py` run exposed an unrelated local missing-`GOOGLE_API_KEY` test issue in
  `test_add_events_from_text_preserves_links`.
- follow-up regression checks after retry/priority fixes:
  - `tests/test_smart_update_native_schema.py tests/test_tg_monitor_reprocess_incomplete_scan.py tests/test_job_dedup.py`
    printed `36 passed`; pytest again hung after the passing summary and was terminated.
  - `tests/test_job_due_filter.py tests/test_job_dedup.py tests/test_smart_update_native_schema.py tests/test_tg_monitor_reprocess_incomplete_scan.py`
    printed `38 passed`; pytest again hung after the passing summary and was terminated.
  - `compileall` passed for `main.py`, `smart_event_update.py`, and the changed tests.
- post-deploy verification:
  - `/healthz` after deploy: `ok=true`, `ready=true`, `job_outbox_worker=ok`, `issues=[]`.
  - VK catch-up reconciliation enqueued missing Telegram-origin VK jobs; active future non-silent Telegram-origin
    events with no VK job and no managed VK URL: `0`.
  - Production log evidence at `2026-06-04T13:46Z`: one `telegraph_render_remove_logistics` Gemma 4 provider
    `500 INTERNAL` created three `google_ai.reserve_ok` attempts against `gemma-4-31b` before this retry-cap follow-up.
    This is the reason for `SMART_UPDATE_GOOGLE_AI_MAX_RETRIES=1`.
  - Production env evidence after retry-cap deploy: `smart_update_client_max_retries=1`,
    `SMART_UPDATE_4O_FALLBACK=1`, `SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR=4`,
    `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS=GOOGLE_API_KEY4,GOOGLE_API_KEY3,GOOGLE_API_KEY2`,
    `GOOGLE_API_KEY4` present.
  - Production log evidence after retry-cap deploy: reserve overflow used `GOOGLE_API_KEY4` for
    `gemini-3.1-flash-lite`; `smart_update_gemma_fallback_4o` count was `0` in the checked post-deploy window, with
    old `4o fallback budget exhausted` lines only before the retry-cap deploy.
  - Production catch-up after `vk_sync` priority deploy: `vk_sync` done count increased from `1086` to `1103`, pending
    decreased from `88` to `70`, and fresh Telegram-origin events reached VK, including
    `https://vk.com/wall-231920894_1983` through `https://vk.com/wall-231920894_1993`.
    The original `@kraftmarket39/271` event remains at `https://vk.com/wall-231920894_1974`.
    Active future non-silent Telegram-origin events with neither VK job nor managed VK URL remained `0`.

## Prevention

- Keep `vk_sync` priority regression covered so VK fanout cannot be starved by unrelated rebuild backlog.
- Add alert/reporting for active future Telegram-origin events with no VK job/managed VK URL.
