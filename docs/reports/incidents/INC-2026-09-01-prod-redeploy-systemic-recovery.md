# INC-2026-09-01 production redeploy systemic recovery

Status: open
Severity: sev1
Service: Fly production scheduler, VK intake and CherryFlash/Kaggle video lane
Opened: 2026-09-01
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-05-cherryflash-disk-full.md`,
`INC-2026-08-30-video-lane-stale-render-alert-storm.md`,
`INC-2026-06-13-kaggle-duplicate-videoannounce.md`,
`INC-2026-04-23-cherryflash-pre-handoff-loss.md`
Related docs: `docs/features/cherryflash/README.md`,
`docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Three Fly releases (`v2059`, `v2060`, `v2061`) restarted the production bot
between 06:31 and 08:17 UTC. The last restart killed the CherryFlash catch-up
between local session creation and the durable Kaggle dataset handoff. At the
same time, a slow status callback had already committed the Telegram-session
resource lease for the first CherryFlash attempt but timed out at the Kaggle
client. The notebook interpreted the ambiguous response as a real resource
conflict, stopped without releasing the lease, and later produced no video.

Separately, long-lived backup artifacts had reduced `/data` below the VK crawl
writer admission floor. This predated the redeploy, but it made multiple
scheduled and watchdog VK import batches fail during the same incident window.
The incident is therefore handled as one systemic scheduler/recovery event,
not as independent Telegram notifications.

## User / Business Impact

- The scheduled 2026-09-01 popular-review CherryFlash release did not publish.
- Session `1175` ended `FAILED` without video; session `1176` remained orphaned
  as `RENDERING` without a dataset after the `v2061` restart.
- The story Telegram auth resource remained leased to terminal run
  `videoannounce:1175`, blocking safe replacement work.
- VK auto-import rows `8039`, `8040`, `8041`, `8047` and `8049` failed or were
  partial; the 08:15 and 08:47 UTC batches were rejected below the `512 MiB`
  storage admission floor.
- Other audited scheduled surfaces continued: 3DI and the visual guide digest
  were successful; guide monitoring completed partial with durable results.

## Detection

- Telegram sent `cherryflash.log — Логи (нет видео) сессии #1175` and VK
  batch error summaries.
- The attached operator report was correlated with the persistent Fly mirror
  `/data/runtime_logs/events-bot.log*`, `ops_run`, `videoannounce_session`,
  `kaggle_run_ledger`, the resource lease table and the exact Kaggle output.
- `/healthz` remained ready and SQLite `PRAGMA quick_check` was `ok`, so the
  generic serving health did not expose the product-process failures.

## Timeline

- 2026-09-01 06:31 UTC — Fly release `v2059` starts.
- 2026-09-01 07:44 UTC — scheduled popular review starts (`ops_run 8045`).
- 2026-09-01 07:50–07:51 UTC — CherryFlash `1175` hands off. Both callback
  requests exceed the notebook's 10-second timeout; the server commits the
  resource lease and callback events at 07:51:15.
- 2026-09-01 08:06 UTC — `1175` is marked `FAILED`, no video output.
- 2026-09-01 08:13 UTC — watchdog catch-up `8046` starts.
- 2026-09-01 08:17 UTC — Fly release `v2061` kills the catch-up after session
  `1176` creation but before dataset persistence. Startup grants the fresh
  pre-handoff row its normal grace period, but no periodic recovery pass later
  revisits it.
- 2026-09-01 08:15 and 08:47 UTC — VK batches fail below the storage floor.
- 2026-09-01 09:08 UTC — `/data` inventory finds about `348 MiB` free and an
  obsolete 2026-08-12 predeploy SQLite copy using `339,795,968` bytes.
- 2026-09-01 09:09 UTC — the exact obsolete copy and empty sidecars are
  fingerprinted and removed; free space becomes about `680 MiB`.
- 2026-09-01 09:12 UTC — exact terminal Kaggle failure is reconciled through
  the host helper, the `1175` lease is released, and orphan `1176` is changed
  from `RENDERING` to `FAILED` by a compare-and-set update.

## Root Cause

1. `KaggleStatusClient.acquire_resource()` made one callback attempt. A client
   timeout was indistinguishable from a real `blocked` decision even though
   same-run acquisition is server-side idempotent.
2. Poller terminal failure updated the video row but did not reconcile the
   `kaggle_run_ledger`; the fresh nonterminal callback projection therefore
   suppressed catch-up and retained its exact resource lease.
3. Pre-handoff recovery ran at startup only. A fresh row skipped during the
   handoff grace window had no later mandatory recovery pass.
4. The popular-review retry cap counted deploy-killed pre-handoff rows as real
   remote renderer attempts, which could consume the one allowed recovery.
5. Obsolete incident/predeploy SQLite copies shared the small production
   volume with the live database. `/healthz` did not become critical at the VK
   subsystem's higher `512 MiB` admission floor.

## Contributing Factors

- Multiple releases landed during the scheduled critical-task window.
- The host callback was delayed by SQLite writer contention but still committed.
- Product health was degraded while generic HTTP/SQLite readiness stayed green.
- The volume had enough space for serving but not for VK crawl admission.

## Automation Contract

### Treat as regression guard when

- changing Kaggle callback transport, resource leases, video poller terminal
  classification, CherryFlash startup/watchdog recovery or Fly volume cleanup;
- deploying during a scheduled CherryFlash or VK import window.

### Affected surfaces

- `kaggle/kaggle_status_client.py`, `kaggle_status.py`;
- `video_announce/poller.py`, `scheduling.py`;
- Fly `/data`, runtime mirror, production SQLite and deploy/restart path;
- CherryFlash story auth lane and VK auto-import watchdog.

### Mandatory checks before closure or deploy

- ambiguous resource-acquire timeout retries with the same `event_uid`; a real
  `blocked` response is not retried;
- terminal provider failure closes the exact ledger and releases only its own
  lease;
- watchdog recovery revisits a pre-handoff row after grace and fails it closed
  even when no operator notification target can be resolved;
- a deploy-killed pre-handoff row does not consume the remote retry cap;
- focused Kaggle-status, scheduler and video-poller tests pass;
- `df`, `/healthz`, `PRAGMA quick_check`, scratch write/fsync/remove and current
  runtime logs are healthy;
- all missed current-day critical slots receive verified catch-up or an
  explicit evidence-backed blocker without duplicate publication.

### Required evidence

- implementation and deployed SHA reachable from `origin/main`;
- Fly release/machine version and post-deploy health;
- exact session/ledger/lease and Kaggle provider readback;
- VK catch-up outcome after restoring more than `512 MiB` free;
- CherryFlash dataset, exact kernel terminal state, video and configured story
  target receipts for the compensating run.

## Immediate Mitigation

- Fingerprinted and removed only the obsolete
  `/data/release_snapshots/INC-2026-08-10-predeploy-v1969-20260812T075131Z.sqlite`
  family (`339,828,736` bytes); the live database, WAL/SHM, current auth stores,
  guide media and runtime mirror were not touched.
- Reconciled `videoannounce:1175` as terminal failed, releasing its exact story
  resource lease.
- Marked stale pre-handoff session `1176` failed with the canonical
  `runtime restart before Kaggle handoff; rerun required` reason.

## Corrective Actions

- [x] Add bounded idempotent resource-acquire retry with a stable event UID.
- [x] Reconcile exact Kaggle ledger/lease when the poller accepts a terminal
  provider failure with no video.
- [x] Run the idempotent video recovery sweep from the popular-review watchdog,
  not only once at startup.
- [x] Fail stale pre-handoff rows without requiring a notification destination.
- [x] Exclude pre-handoff deploy orphans from the remote failure retry cap.
- [ ] Merge to `origin/main`, deploy and pass all release gates.
- [ ] Complete and verify same-day CherryFlash and VK catch-up.

## Follow-up Actions

- [ ] Add subsystem admission state to health/operations telemetry so a ready
  bot cannot hide `VK_CRAWL_MIN_FREE_MB` rejection.
- [ ] Inventory and define expiry for manual incident SQLite copies outside
  automatic static-site snapshot retention.
- [ ] Avoid routine deploys inside known critical scheduled handoff windows or
  require explicit active-job drain/readback before restart.

## Release And Closure Evidence

- implementation SHA: pending
- deployed SHA: pending
- deploy path: pending
- focused regression checks: `121 passed`; `py_compile` and `git diff --check`
  passed before release
- post-deploy verification: pending

## Prevention

The recovery contract now treats callback responses as an idempotent commit
protocol, makes provider-terminal ledger closure responsible for exact lease
release, and guarantees a post-grace periodic pass for sessions that a deploy
can orphan between local creation and remote handoff.
