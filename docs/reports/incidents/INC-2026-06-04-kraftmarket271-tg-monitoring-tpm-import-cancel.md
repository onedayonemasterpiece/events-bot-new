# INC-2026-06-04-kraftmarket271 Telegram Monitoring TPM zero-events and import cancellation

Status: mitigated
Severity: sev1
Service: Telegram Monitoring producer/importer and promo-campaign event intake
Opened: 2026-06-04
Closed: —
Owners: bot operator / incident owner
Related incidents: `INC-2026-05-05-80-stories-source-coverage`, `INC-2026-05-15-cherryflash-partner-fanout-promo-filter`, `INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss`, `INC-2026-06-03-smart-update-flash-lite-rpd`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/promo-campaigns/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Telegram Monitoring failed to add `https://t.me/kraftmarket39/271` to production DB. The missed post is a concrete
`80 историй о главном` promo-campaign event: `Калининград корабельный — от первых дней к вершинам славы судостроительного завода Янтарь`,
scheduled for 2026-07-08 with registration URL on `kgd80.ru`.

This is a production incident because a promoted festival event that should reach public/promo surfaces was absent
from the event inventory. The failure was not an isolated data-quality nit: the same run also left `@kraftmarket39`
cursor state at message `268`, while the latest scanned Telegram message in the Kaggle output was `271`.

## User / Business Impact

- The promo event from `80 историй о главном` was not present in production DB and therefore could not be selected
  by promo-campaign or public event surfaces.
- `@kraftmarket39/270` had five extracted events in the Kaggle output, but production import did not reach that
  source either, so the whole `269..271` tail requires catch-up verification.
- Operators had no durable prod-side diagnostic row for `271`; the event looked simply absent unless Kaggle output
  and runtime logs were inspected together.

## Detection

- Detected by operator report that `https://t.me/kraftmarket39/271` was missing.
- Kaggle output confirmed the producer scanned the message but emitted `events=[]`.
- Production DB confirmed no `event_source` row and no matching `event` row for the source URL/title.
- Runtime file mirror on Fly was available and showed both the scheduled run cancellation and recovery import
  cancellation.

## Timeline

- 2026-06-03 20:43:57 UTC — `@kraftmarket39/271` is published.
- 2026-06-03 21:40:00 UTC — production `tg_monitoring` scheduled run starts, `run_id=c73df9ee31394778bdfcf5832b155db3`.
- 2026-06-03 22:55 UTC — Kaggle starts processing `@kraftmarket39`; source window is `last_id=268`, `latest_id=271`.
- 2026-06-03 22:56 UTC — Kaggle local limiter reaches near-full minute TPM reservation; OCR/extraction/rescue
  for `@kraftmarket39` messages hit `RateLimitError(blocked_reason='tpm')`.
- 2026-06-03 22:56:53 UTC — Kaggle logs `extract_events single-event rescue failed: Rate limit exceeded: tpm`;
  source completes with only message `270` extracted (`events=5`), while `269` and `271` stay `events=[]`.
- 2026-06-03 23:02:59 UTC — Kaggle output is generated: 190 messages, 43 extracted events.
- 2026-06-03 22:38:16 UTC and 2026-06-03 23:27:30 UTC — production runtime logs record `asyncio.exceptions.CancelledError`
  for the scheduled monitor and later `kaggle_recovery` import-only path.
- 2026-06-04 — investigation confirms production DB cursor for `@kraftmarket39` is still `268`; there are no prod
  rows for `@kraftmarket39/269..271`.
- 2026-06-04 08:31:22..09:23:03 UTC — after deploy, stale-recovery import-only `ops_run id=1882` completes
  (`status=partial`, 190 messages processed) and advances `@kraftmarket39` through message `271`, preserving
  diagnostics for `269`/`271` and importing part of `270`.
- 2026-06-04 10:05:29..10:10:43 UTC — focused forced catch-up `ops_run id=1884`,
  `run_id=inc_kraftmarket271_force_20260604a`, processes only `@kraftmarket39/271` and creates event `5656`.

## Root Cause

1. The Telegram Monitoring Kaggle runtime selected `GOOGLE_API_KEY3`, but Supabase key metadata did not resolve
   active rows for `GOOGLE_API_KEY3` / `GOOGLE_API_KEY_3` during that run. The Google AI client fell back to the
   process-local limiter under `key_alias=local-fallback-default-env-missing`.
2. The local limiter allowed a bursty full scan to reserve nearly the full configured minute TPM. Around
   `@kraftmarket39`, one minute had three large reservations consuming 14,848 TPM, leaving too little headroom for
   the next extraction/OCR/rescue calls.
3. Producer extraction failed open: `extract_events` caught the rate-limit exception, logged a warning, and returned
   `events=[]`; the single-event rescue also failed on TPM and did not mark the message as an extraction failure.
4. Production recovery did not compensate the producer false negative. The scheduled run was cancelled while polling
   Kaggle, and the later `import_only` recovery was cancelled during Smart Update before it reached `@kraftmarket39`.
5. The stale recovery registry entry remained present, but the recovery loop skipped it because its stored
   `meta.pid` matched the current process id even when the process no longer held the Telegram Monitoring run lock.
6. Because import did not reach the source, production did not even persist `producer_zero_events` diagnostics for
   messages `269`/`271`; the source cursor remained at `268`.
7. Follow-up process gap: after focused catch-up created event `5656`, the primary Telegram import path was found
   to call Smart Update with `schedule_kwargs={"skip_vk_sync": True}`. That allowed `event_source`/Telegraph/festival
   queue restoration while suppressing the normal Smart Update → `JobOutbox(vk_sync)` → VK publication path required
   for promo events.
8. Follow-up VK promo gap: after `vk_sync` was restored, VK postponed publishing returned draft id
   `wall-231920894_1973`, while the public wall post became `wall-231920894_1974` with `postponed_id=1973`.
   `vk_repost` looked only at `Event.source_vk_post_url` via `wall.getById`, so it could miss an otherwise valid
   source post until another sync/edit happened to reconcile the URL.

## Contributing Factors

- `GOOGLE_API_KEY3` registry drift had already been identified in `INC-2026-06-03-smart-update-flash-lite-rpd`,
  but Telegram Monitoring still ran in the missing-candidate fallback state.
- The producer treats rate-limit failures the same as a legitimate zero-event extraction result.
- Recovery import processes a large result batch serially and can lose the unprocessed tail when the app restarts
  or a scheduled job is cancelled.
- The promo-campaign surface depends on upstream event inventory; it had no independent alert that a known
  `80 историй о главном` source post was scanned but absent from DB.
- The Telegram Monitoring import boundary treated VK sync as optional workflow overhead, but for promo-campaign
  sources VK sync is part of the user-visible product contract.
- Promo VK URL reconciliation covered `vk_publication` exposure rows, but not organic `vk_sync` URLs stored on
  `event.source_vk_post_url`.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring producer extraction, OCR, rate-limit retry, or single-event rescue;
- changing `GoogleAIClient` reserve scoping/fallback behavior for `TG_MONITORING_GOOGLE_KEY_ENV=GOOGLE_API_KEY3`;
- changing Telegram result import ordering, cancellation handling, recovery/resume, or scanned-message diagnostics;
- changing promo-campaign candidate selection for `80 историй о главном`.

### Affected surfaces

- code paths: `kaggle/TelegramMonitor/telegram_monitor.py`, `google_ai/client.py`,
  `source_parsing/telegram/service.py`, `source_parsing/telegram/handlers.py`, `smart_event_update.py`;
- env/config: `TG_MONITORING_GOOGLE_KEY_ENV`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY_3`,
  `GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV`, Supabase `google_ai_api_keys`;
- production data: `telegram_source`, `telegram_scanned_message`, `event_source`, `ops_run`;
- external systems: Kaggle kernel `zigomaro/telegram-monitor-bot`, Fly app `events-bot-new-wngqia`,
  Google AI quota registry.

### Mandatory checks before closure or deploy

- Verify Fly/Kaggle Google key env names have active matching rows in `google_ai_api_keys`; no
  `key_candidates_missing_primary`, `google_ai.default_env_candidates_missing`, or
  `reserve_default_env_candidates_missing_fallback` in the Telegram Monitoring run log.
- Replay or focused-smoke `@kraftmarket39/271` and `@kraftmarket39/269`; rate-limit failures must not be recorded
  as legitimate `events=[]`.
- Verify import/recovery can complete or resume after cancellation without losing an unprocessed result tail.
- Production DB must contain durable evidence for `@kraftmarket39/271`: either an imported event/source row or a
  terminal diagnostic row explaining why import is impossible.
- For imported active/non-silent Telegram events, the primary Smart Update pass must not suppress standard scheduling:
  `JobOutbox` must contain a `vk_sync` job or the event must already have a managed VK post.
- VK repost eligibility must resolve stale VK postponed ids (`postponed_id -> live id`) for `event.source_vk_post_url`
  and persist the live wall URL before declaring that no recent source post exists.
- Production DB must show `telegram_source.last_scanned_message_id >= 271` for `kraftmarket39` after catch-up.
- If `@kraftmarket39/270` remains relevant, verify its five producer-extracted events are imported or deliberately
  rejected with per-event reasons.
- Run promo-campaign visibility check for the `80 историй о главном` event after catch-up.

### Required evidence

- Kaggle output/log artifact path for `run_id=c73df9ee31394778bdfcf5832b155db3`.
- Runtime-log excerpt from `/data/runtime_logs/events-bot.log.2026-06-03_23` showing both cancellations.
- Pre/post production DB queries for `telegram_source`, `telegram_scanned_message`, `event_source`, and matching
  event title/source URL.
- Deployed SHA and confirmation the fix is reachable from `origin/main` if code changes are required.
- Compensating import/catch-up `ops_run` evidence.

## Immediate Mitigation

- Investigation retrieved Kaggle output and production DB evidence.
- Runtime file logging was verified enabled on Fly (`ENABLE_RUNTIME_FILE_LOGGING=1`,
  `RUNTIME_LOG_DIR=/data/runtime_logs`) and used as primary evidence.
- Stale/malformed local prod DB snapshots on the dev server were removed to free working disk space for continued
  incident work.

## Corrective Actions

- Done: recovery now skips same-process jobs only while the current process still holds the Telegram Monitoring
  `_RUN_LOCK`; stale registry entries with the same pid can be imported after cancellation/restart.
- Done: deployed recovery fix to Fly release `v1175` from `bf527e6f`.
- Done: compensating catch-up restored `@kraftmarket39/271` as event `5656` with
  `event_source.source_url='https://t.me/kraftmarket39/271'`, date `2026-07-08`, time `18:30`,
  location `Историко-художественный музей`, and one poster.
- Done: primary Telegram result import no longer passes `schedule_kwargs.skip_vk_sync` to Smart Update; linked-source
  auxiliary passes still suppress VK sync so source-log enrichment cannot duplicate VK publication work.
- Done: repeated primary imports that return `skipped_nochange` for an existing event now re-arm standard event tasks,
  so catch-up can repair old "event exists, VK job missing" states without deleting/recreating the event.
- Done: forced single-event replay with an exact existing Telegram `event_source` now uses the same re-arm path without
  rerunning the LLM merge pipeline, avoiding unnecessary provider/SQLite contention during repair.
- Done: promo VK recent-post detection now lazy-resolves stale `event.source_vk_post_url` values after VK postponed
  publish, falls back to the VK user actor for live post-date lookup when the service actor returns an empty
  `wall.getById`, persists the live wall URL, and can use the restored `vk_sync` post as a future repost source.
- Done: import-boundary fix and promo URL reconciliation follow-ups were deployed from `origin/main`; focused import
  replay and promo VK catch-up verified event `5656` end-to-end through `vk_sync`, video promo selection, and repost
  eligibility.
- Pending: producer must distinguish provider/rate-limit failure from legitimate zero-event output, retry bounded
  minute TPM blocks when safe, and persist failure diagnostics into the result payload.

## Follow-up Actions

- [x] Fix/verify `GOOGLE_API_KEY3` registry drift for Telegram Monitoring producer runs.
- [x] Add a focused regression test/smoke for `@kraftmarket39/271`.
- [x] Add cancellation-tail regression coverage for Telegram result import/recovery.
- [x] Add regression coverage that primary Telegram import does not suppress Smart Update `vk_sync` scheduling.
- [x] Add regression coverage that promo VK resolves stale `event.source_vk_post_url` postponed ids before repost
  eligibility checks.
- [ ] Add alert/reporting for scanned event-like messages with producer `events=[]` and no durable prod diagnostic.
- [x] Complete compensating catch-up for `@kraftmarket39/269..271`.
- [ ] Add producer-side retry/failure payload handling for TPM/provider failures.

## Release And Closure Evidence

- deployed SHA: `bf527e6fbcb38a7262eb7f7963c744c0db5ff572` (`origin/main`).
- deploy path: manual `flyctl deploy --remote-only` from clean linked worktree; Fly release `v1175`,
  machine `48e42d5b714228`, health check passing.
- import-boundary follow-up deployed SHA: `e46f292dee4178f4b72a8372ccb31a90ffc6fd41`, image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KT941THXEVBJ3DXXPG4XEWBF`, `/healthz ok=true ready=true`.
- promo URL reconciliation follow-ups deployed from `origin/main`:
  - `71cb26408473c38a609c68911913b04383552fed` (`fix(promo): resolve vk postponed ids for event posts`),
    image `registry.fly.io/events-bot-new-wngqia:deployment-01KT9572WJT5XCCBHPTV6Q599F`;
  - `93bdfd3159e03e9c59074895806729bb055e4780` (`fix(promo): use user actor for vk post date lookup`),
    image `registry.fly.io/events-bot-new-wngqia:deployment-01KT95GB7ZY19N8Q77ZPBEWR0S`, `/healthz ok=true ready=true`.
- regression checks:
  - `tests/test_tg_monitor_recovery.py tests/test_google_ai_client.py` — 17 passed.
  - `tests/test_promo.py tests/test_tg_monitor_reprocess_incomplete_scan.py tests/test_tg_monitor_recovery.py` —
    24 passed after the import-boundary fix.
  - `tests/test_promo.py` — 16 passed after the promo URL reconciliation follow-up.
  - `GOOGLE_API_KEY3` currently has an active `google_ai_api_keys` row in Supabase.
  - stale registry import-only rerun `ops_run id=1882` completed instead of being skipped by same-PID recovery.
- post-deploy verification:
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`.
  - Before focused catch-up: `event_source` and matching event queries for `https://t.me/kraftmarket39/271` were empty.
  - Focused catch-up: `ops_run id=1884`, `status=success`, `messages_processed=1`, `messages_forced=1`,
    `events_created=1`, `errors_count=0`.
  - Production DB after catch-up: `telegram_scanned_message(1177,271)` is `status='done'`,
    `events_extracted=1`, `events_imported=1`, `error=NULL`; `event_source` links event `5656` to
    `https://t.me/kraftmarket39/271`; `telegram_source.last_scanned_message_id=271`.
  - Focused re-arm replay after the import-boundary fix: `ops_run id=1892`, `status=success`,
    `messages_processed=1`, `messages_forced=1`, `events_imported=0`, `errors_count=0`.
  - Standard `JobOutbox(vk_sync)` for event `5656`: job `21914`, `status=done`, result initially
    `https://vk.com/wall-231920894_1973`; after VK postponed publish, promo catch-up resolved and persisted the live
    URL `https://vk.com/wall-231920894_1974`.
  - Current video promo selection (`popular_review`) includes event `5656` as `source_window='promo'`,
    `promo_campaign_id=1`, `promo_activity_id=1`, `promo_placement_kind='guaranteed_any_position'`.
  - Current promo VK check sees event `5656` as a recent source post for both `vk_publication` and `vk_repost`
    activities (`posted_at=2026-06-04T11:03:00+00:00`). At verification time `vk_repost.due_count=0`, so no early
    repost was published before the configured one-per-day slot around `15:00` Europe/Kaliningrad (`13:00 UTC`).

## Prevention

- Treat clear event-like Telegram posts returning `events=[]` as a reportable failure mode, not a benign no-op.
- Keep Google key runtime secrets and Supabase quota metadata synchronized before scheduled producer windows.
- Do not close Telegram Monitoring incidents that affect daily/scheduled imports until the same-day catch-up has
  completed and production DB evidence confirms the missed source rows are restored or diagnostically accounted for.
