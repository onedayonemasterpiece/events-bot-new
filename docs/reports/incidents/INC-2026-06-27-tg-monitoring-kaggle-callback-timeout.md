# INC-2026-06-27 Telegram Monitoring Kaggle callback timeout

Status: open
Severity: sev1
Service: scheduled `tg_monitoring` / Kaggle `TelegramMonitor`
Opened: 2026-06-27
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce`, `INC-2026-06-20-tg-on-demand-scheduler-run-id`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/kaggle-status-framework/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The scheduled Telegram Monitoring slot on 2026-06-27 21:40 UTC started normally, created its temporary Kaggle input datasets, and pushed `zigomaro/telegram-monitor-bot`, but the Kaggle notebook failed in the injected preflight cell before scanning any Telegram sources.

Kaggle status output shows callback timeouts for `kernel_started` and `resource_acquire`; the generic injected preflight treated the missing `resource_action=acquired` response as `Required Kaggle resource is busy: telegram_session:s22` and aborted. Production `ops_run` `2999` ended `error` with `sources_scanned=0`, `messages_processed=0`, and `events_imported=0`.

## User / Business Impact

- The daily Telegram source scan for 2026-06-27 did not run.
- New Telegram-source event posts that should have been imported by the daily slot were not processed until a compensating rerun.
- The operator-facing Kaggle UI showed `Telegram Monitor Bot` as failed, while the server-side `ops_run.details_json` had no business errors because the failure happened before source processing.

## Detection

- Detected from the operator screenshot showing `Telegram Monitor Bot` failed on 2026-06-27.
- Production file mirror was available: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, active `/data/runtime_logs/events-bot.log` plus rotated files.
- Evidence collected:
  - runtime log lines for `run_id=896e0f0270b1468aafe119e3f4411c8d`;
  - production DB `ops_run` row `2999`;
  - downloaded Kaggle output `kaggle_status_events.jsonl` and `telegram-monitor-bot.log`.
- Observability gap: the Kaggle callback timeout remained only in Kaggle local output; no server `kaggle_run_event` row was recorded because the callbacks timed out.

## Timeline

- 2026-06-27 21:40:00 UTC — APScheduler submitted `tg_monitoring` with `run_id=896e0f0270b1468aafe119e3f4411c8d`.
- 2026-06-27 21:40:33 UTC — runner acquired the local Telegram Monitoring lock and built config for 51 sources.
- 2026-06-27 21:40:49-21:40:54 UTC — temporary Kaggle cipher/key datasets were created and verified.
- 2026-06-27 21:41:54 UTC — Kaggle local status JSONL recorded `kernel_started` callback `TimeoutError: The read operation timed out`.
- 2026-06-27 21:42:04 UTC — Kaggle local status JSONL recorded `resource_acquire` callback `TimeoutError: The read operation timed out`.
- 2026-06-27 21:42:37 UTC — production polling saw `zigomaro/telegram-monitor-bot` status `ERROR`.
- 2026-06-27 21:42:38 UTC — `ops_run` `2999` finished as `error`; metrics show zero sources/messages/events processed.

## Root Cause

1. The Kaggle status helper had a single-attempt `resource_acquire` callback with a 10 second timeout.
2. A transient callback timeout produced no response body, so `acquire_resource()` returned `False` even though this was an unknown callback state, not a confirmed active holder conflict.
3. The injected Kaggle preflight maps every `False` from `acquire_resource()` to `Required Kaggle resource is busy: telegram_session:s22`, aborting before the Telegram scanner starts.

## Contributing Factors

- The shared S22 Telegram session lease guard is intentionally fail-closed, but it did not distinguish a definite `resource_action=blocked` response from transient callback transport failure.
- Nearby runtime logs show SQLite lock pressure and temporary `/healthz` 503s during the same minute, increasing the chance that callback writes exceeded the Kaggle helper timeout.
- The status framework did not emit a terminal failure event when preflight resource acquisition failed before alive/report instrumentation started.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/kaggle_status_client.py`, `kaggle_status.py`, or `video_announce/kaggle_client.py` status injection;
- changing Telegram-monitor/guide-monitor Kaggle resource leases;
- changing scheduled `tg_monitoring` recovery/catch-up behavior;
- investigating any `Required Kaggle resource is busy: telegram_session:s22` failure where the server has no active lease row.

### Affected surfaces

- code paths: `kaggle/kaggle_status_client.py::acquire_resource`, status-injected notebook/script preflight, `source_parsing/telegram/service.py` Kaggle runner;
- env/config: `KAGGLE_STATUS_CALLBACK_URL` / `WEBHOOK_URL`, callback timeout/retry envs;
- release path: production Fly deploy from `origin/main`;
- external systems: Kaggle kernel execution, Fly public callback route, Telegram S22 auth bundle lease;
- smoke paths / alerts / health checks: `/healthz`, runtime file mirror, `ops_run` `tg_monitoring`, Kaggle output `kaggle_status_events.jsonl`.

### Mandatory checks before closure or deploy

- unit test proving `resource_acquire` retries transient callback timeouts and succeeds on a later `resource_action=acquired` response;
- existing Kaggle status tests for resource blocking, alive lease renewal, and local JSONL token redaction;
- `python3 -m py_compile kaggle/kaggle_status_client.py kaggle_status.py video_announce/kaggle_client.py source_parsing/telegram/service.py`;
- `git diff --check`;
- post-deploy `/healthz` check;
- compensating Telegram Monitoring rerun for the missed 2026-06-27 slot, with production `ops_run` success/empty/partial evidence that scanned sources/messages rather than failing preflight.

### Required evidence

- deployed SHA reachable from `origin/main`;
- test output for the focused Kaggle status tests;
- runtime log evidence for the original failed run and the compensating run;
- production DB `ops_run` rows for the failed run and rerun;
- Kaggle output evidence for original failure and, if applicable, rerun success/failure.

## Immediate Mitigation

- Downloaded the failed Kaggle output before cleanup/expiry and confirmed the failure is preflight-only, before Telegram source scanning.
- Verified no active `telegram_session:s22` lease remained in production and `/healthz` returned healthy after the failure.

## Corrective Actions

- Add retry/backoff around Kaggle `resource_acquire` callbacks so transient callback timeouts do not immediately masquerade as a confirmed busy S22 resource.
- Keep fail-closed behavior for a real `resource_action=blocked` response.
- Add local JSONL evidence with attempt counters and a stable `resource_acquire:<key>` event UID across retries.

## Follow-up Actions

- [ ] Consider a terminal `report_written`/`failed` status event around injected preflight failures so the server ledger is not left at `created` when preflight aborts.
- [ ] Review job-outbox SQLite lock pressure around late-night scheduled jobs; repeated `/healthz` 503s during Kaggle callback windows can still delay non-critical callbacks.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The regression contract makes future S22 lease/preflight changes verify both sides: confirmed busy responses still block, while transient callback transport failures get bounded retries and leave enough local Kaggle evidence to diagnose without guessing.
