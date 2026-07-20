# INC-2026-07-20 Telegram Monitoring blocked by stale S22 lease

Status: open
Severity: sev2
Service: Telegram Monitoring / Guide Excursions Monitoring / Kaggle resource leases
Opened: 2026-07-20
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-10-guide-watchdog-kaggle-failure-loop`, `INC-2026-06-13-kaggle-duplicate-videoannounce`
Related docs: `docs/features/kaggle-status-framework/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The scheduled Telegram Monitoring run at `2026-07-20 21:40 UTC` failed before
source scanning because `telegram_session:s22` was still leased to an already
failed Guide Excursions kernel. The guide host path observed terminal Kaggle
failure at `19:13 UTC`, but neither reconciled the ledger nor released the
exact run-owned lease; its three-hour TTL therefore blocked the later critical
Telegram slot.

## User / Business Impact

- The July 20 Telegram source tail was not imported at the scheduled time.
- New messages in monitored sources, including forum topic
  `@klassster/8809` (`Анонсы мероприятий`), remained beyond the production
  cursor until a compensating run.
- Existing event data and the bot remained available; impact was limited to
  delayed Telegram intake.

## Detection

- Detected while verifying whether `https://t.me/klassster/8809` is covered by
  production Telegram Monitoring.
- `ops_run id=4245` recorded `tg_monitoring`, `status=error`, zero scanned
  sources/messages.
- Runtime mirror `/data/runtime_logs/events-bot.log` showed Kaggle preflight
  `resource_acquire=blocked`.
- Kaggle kernel logs identified holder `guide_monitor:12639cb5f52a` and the
  active `telegram_session:s22` lease.
- Production DB showed guide `ops_run id=4242` already terminal `error` at
  `19:13:05 UTC`, while its ledger remained `running` and lease stayed active
  until `22:13:30 UTC`.

## Timeline

- `2026-07-20 19:10:13 UTC` — scheduled guide full run starts.
- `2026-07-20 19:11:59 UTC` — guide run acquires `telegram_session:s22`.
- `2026-07-20 19:13:05 UTC` — guide `ops_run 4242` finishes `error`; Kaggle
  ledger/lease remain non-terminal/active.
- `2026-07-20 21:40:00 UTC` — scheduled Telegram Monitoring starts.
- `2026-07-20 21:41:17 UTC` — Telegram kernel is blocked by the failed guide
  holder and exits during bootstrap.
- `2026-07-20 21:41:48 UTC` — Telegram `ops_run 4245` finishes `error` with
  zero sources scanned.
- `2026-07-20 22:26 UTC` — compensating Telegram run
  `inc-20260720-tg-stale-s22-catchup-r3` acquires S22 and starts normally.
- `2026-07-20 22:33–22:46 UTC` — the guide critical watchdog emits repeated
  remote-session-busy skips and admin notifications because its persisted
  retry query ended at local midnight and could not see catch-up attempts
  recorded after that boundary.

## Root Cause

1. Kaggle callbacks are best-effort; the guide kernel exited without a terminal
   callback that would release the lease.
2. The guide host poller knew that the kernel was terminal `failed`, but its
   failure branch only raised `RuntimeError`; host reconciliation existed only
   for validated successful output.
3. The failed guide ledger therefore remained `running`, and its exact-owned
   `telegram_session:s22` lease remained active for the full TTL.

## Contributing Factors

- Guide full monitoring overlaps the later Telegram Monitoring window closely
  enough that a three-hour stale lease crosses the `21:40 UTC` slot.
- The resource guard correctly failed closed, preventing concurrent use of the
  role-scoped S22 Telegram session; the missing behavior was terminal cleanup,
  not lease bypass.
- The guide watchdog's retry evidence window ended at midnight of the missed
  slot's local day. After-midnight remote-busy skips therefore fell outside the
  query on every tick, bypassing the existing five-minute/one-hour cooldown and
  flooding the admin chat.

## Automation Contract

### Treat as regression guard when

- changing `kaggle_status.py` terminal/lease reconciliation;
- changing Guide Excursions or Telegram Monitoring Kaggle poll failure paths;
- changing `telegram_session:s22` acquisition, renewal, release, or TTL;
- changing the scheduled guide/Telegram monitoring windows.

### Affected surfaces

- `kaggle_status.py` host terminal reconciliation;
- `guide_excursions/kaggle_service.py` terminal failure path;
- `source_parsing/telegram/service.py` terminal failure/recovery path;
- `kaggle_run_ledger`, `kaggle_run_event`, `kaggle_resource_lease`;
- Kaggle `GuideExcursionsMonitor` and `TelegramMonitor`;
- critical scheduler/catch-up and the daily Telegram source cursor.

### Mandatory checks before closure or deploy

- `python3 -m py_compile kaggle_status.py guide_excursions/kaggle_service.py source_parsing/telegram/service.py tests/test_kaggle_status.py tests/test_guide_kaggle_service.py`;
- focused Kaggle status, guide service, and Telegram service tests;
- regression from `INC-2026-06-13`: callback/event-ledger and exact-owner
  resource lease tests;
- production proof that a host-observed failed run becomes terminal and its
  exact-owned lease is released without touching a successor lease;
- same-day Telegram Monitoring compensating run with non-zero source coverage;
- production cursor/source evidence for `@klassster`, plus verification that
  forum-topic messages are included rather than only General-topic messages;
- deployed SHA reachable from `origin/main`, `/healthz` ready, and clean Fly
  release evidence.

### Required evidence

- pre-fix `ops_run 4242`/`4245`, ledger and lease rows;
- redacted Kaggle log showing `resource_acquire=blocked`;
- focused test output;
- deployed SHA and Fly release;
- catch-up `ops_run` metrics and updated `@klassster` cursor.

## Immediate Mitigation

- Pending: release only the stale failed guide-owned lease after confirming the
  guide kernel is terminal, then run the compensating Telegram scan.
- Do not borrow `TELEGRAM_AUTH_BUNDLE_E2E` or bypass the S22 resource guard.

## Corrective Actions

- Add host-observed failure reconciliation that atomically marks the Kaggle
  ledger failed, records `host_failure_observed`, and releases only active
  leases owned by that exact run.
- Invoke it from Guide Excursions and Telegram Monitoring terminal failure
  paths before propagating the run error; recovery metadata/output probing
  remains independent.
- Extend the guide watchdog evidence window through the current invocation so
  that after-midnight catch-up outcomes participate in the persisted retry
  cooldown.

## Follow-up Actions

- [ ] events-bot / closure / verify the next naturally failed Kaggle monitor
  also produces terminal ledger and release evidence without manual SQL.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Failed host poll outcomes now follow the same exact-owner lease cleanup
discipline as host-validated success. Resource contention remains fail-closed;
terminal cleanup, not concurrent session reuse, restores availability.
