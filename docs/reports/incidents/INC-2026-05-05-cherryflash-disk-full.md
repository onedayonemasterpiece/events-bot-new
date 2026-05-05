# INC-2026-05-05 CherryFlash Popular Review Blocked By Full Fly Volume

Status: monitoring
Severity: sev1
Service: CherryFlash scheduled `popular_review` / Fly production `/data` volume / Guide Excursions results retention
Opened: 2026-05-05
Closed: —
Owners: operations / video announce runtime
Related incidents: `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`, `INC-2026-04-23-cherryflash-pre-handoff-loss.md`, `INC-2026-04-27-cherryflash-missing-photo-urls.md`, `INC-2026-04-27-prod-unresponsive-during-cherryflash-recovery.md`, `INC-2026-04-23-guide-digest-extraction-loss.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The 2026-05-05 CherryFlash `popular_review` slot could not build a valid popularity publication set because SQLite writes failed with `sqlite3.OperationalError: database or disk is full`. Production `/data` was at `100%` usage with `Avail 0`, mainly because persisted Guide Excursions Kaggle result bundles accumulated under `/data/guide_monitoring_results`.

## User / Business Impact

- The daily CherryFlash publication for 2026-05-05 missed its scheduled local slot.
- SQLite writes were failing on the production bot volume, so unrelated scheduled jobs were also at risk.
- Guide monitoring had already degraded on 2026-05-04 with `Errno 28` while copying new result bundles.

## Detection

- User reported the CherryFlash failure with `OperationalError: database or disk is full`.
- Fly logs showed the same error at 2026-05-05 09:13 UTC.
- Direct production checks showed `/data` at `100%`, runtime file mirror disabled (`ENABLE_RUNTIME_FILE_LOGGING=0`) and empty, and fallback evidence in Fly logs plus production SQLite.

## Timeline

- 2026-05-04 07:05 UTC: Guide monitoring `ops_run #1022` failed with `OSError: [Errno 28] No space left on device` while writing `/data/guide_monitoring_results/guide-excursions-2bea518401ab`.
- 2026-05-04 11:20 UTC: Guide monitoring `ops_run #1026` repeated the same `Errno 28`.
- 2026-05-04 18:10 UTC: Guide monitoring `ops_run #1031` repeated the same `Errno 28`.
- 2026-05-05 09:13 UTC: Fly logs showed `sqlite3.OperationalError: database or disk is full`.
- 2026-05-05 09:14 UTC: production `/data` measured `974M` total, `958M` used, `0` available; `/data/guide_monitoring_results` used `584M`, `/data/guide_media` used `209M`.
- 2026-05-05 09:14 UTC: old guide result bundles were pruned manually; `/data` recovered to about `469M` free, then SQLite WAL checkpoint reduced WAL from about `68M` to `13K`.
- 2026-05-05 09:16 UTC: compensating CherryFlash run started and created session `#238` / `ops_run #1035`.
- 2026-05-05 09:17 UTC: session `#238` reached confirmed Kaggle handoff with dataset `zigomaro/cherryflash-session-238-1777972597`, kernel `zigomaro/cherryflash`, and matching kernel `dataset_sources`.

## Root Cause

1. Guide monitoring persists full downloaded Kaggle output bundles under `/data/guide_monitoring_results` for restart recovery.
2. That store had no automatic retention or disk-budget guard, so old bundles accumulated on the same small Fly volume as SQLite.
3. Once `/data` reached `100%`, SQLite could no longer create/extend WAL/journal/temp writes, causing CherryFlash popularity selection/session creation to fail with `database or disk is full`.

## Contributing Factors

- `INC-2026-04-23-guide-digest-extraction-loss` already had an open follow-up to add retention for `/data/guide_monitoring_results`, but it had not been implemented.
- Production runtime file logging is intentionally disabled after `INC-2026-04-16-prod-disk-pressure-runtime-logs`, so incident evidence had to come from Fly logs, SQLite rows, and direct volume inspection.
- The CherryFlash watchdog did not leave a `video_popular_review` row before the reported failure, so the same-day slot needed manual compensating catch-up after volume recovery.

## Automation Contract

### Treat as regression guard when

- changing Guide Excursions result download/persistence/recovery behavior;
- changing `/data` artifact retention or adding new persistent production artifacts;
- changing CherryFlash scheduled `popular_review`, startup catch-up, watchdog, or live runner behavior;
- handling any production `database or disk is full`, `Errno 28`, `/healthz`, or Fly volume pressure event.

### Affected surfaces

- `guide_excursions/kaggle_service.py`
- `GUIDE_MONITORING_RESULTS_STORE_ROOT` and `/data/guide_monitoring_results`
- Fly production volume `/data`
- SQLite `/data/db.sqlite`, WAL, and scheduled writes
- CherryFlash scheduled `popular_review`, `ops_run`, `videoannounce_session`, Kaggle handoff, and story publish evidence
- production `/healthz` / webhook serving during long CherryFlash catch-up

### Mandatory checks before closure or deploy

- Verify `/data` free space and SQLite write health after cleanup/deploy.
- Verify runtime file mirror state and fallback evidence path.
- Prove Guide monitoring result persistence prunes old bundles by age/count/size/free-space guard.
- Run targeted retention tests and `py_compile` for touched Guide monitoring code.
- Run CherryFlash scheduled/catch-up regression checks from `INC-2026-04-23-cherryflash-pre-handoff-loss` and `INC-2026-04-27-cherryflash-missing-photo-urls` where touched.
- For the missed same-day CherryFlash slot, perform a compensating catch-up and collect session, Kaggle dataset/kernel, story/video, `/healthz`, and disk evidence.
- Release-governance checks: clean worktree, branch from `origin/main`, deployed SHA reachable from `origin/main`, and release evidence recorded.

### Required evidence

- production `/data` before/after cleanup;
- runtime file mirror env/directory status;
- Fly logs or production DB rows containing `database or disk is full` / `Errno 28`;
- SQLite `PRAGMA quick_check`, WAL checkpoint/write evidence after mitigation;
- test output for Guide result retention;
- post-deploy `/healthz`;
- compensating CherryFlash session id, dataset/kernel refs, and terminal publication status for 2026-05-05.

## Immediate Mitigation

- Deleted old `/data/guide_monitoring_results/guide-excursions-*` bundles older than roughly one day.
- Recovered `/data` from `0` available to about `469M` free, then checkpointed SQLite WAL.
- Verified `/healthz` stayed `ok=true`, `ready=true`, `db=ok`.
- Started compensating CherryFlash run through `scripts/run_cherryflash_live.py` after confirming no duplicate live runner was active.

## Corrective Actions

- Guide monitoring result persistence now prunes `/data/guide_monitoring_results` before and after copying a new output bundle.
- The retention guard is configurable by `GUIDE_MONITORING_RESULTS_STORE_RETENTION_DAYS`, `GUIDE_MONITORING_RESULTS_STORE_MAX_RUNS`, `GUIDE_MONITORING_RESULTS_STORE_MAX_MB`, and `GUIDE_MONITORING_RESULTS_STORE_MIN_FREE_MB`.
- Added focused regression coverage for old bundle pruning and max-run retention.
- Updated operations and Guide Excursions docs with the production retention contract.

## Follow-up Actions

- [ ] Consider a separate volume/object-store budget for durable Guide monitoring media/result bundles if recovery evidence needs more than the default short retention.
- [ ] Add a production disk-space alert for `/data` before SQLite reaches write failure.
- [ ] Decide whether CherryFlash watchdog should emit an operator alert when the local-day slot has no `ops_run` row after its grace window.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending
- compensating CherryFlash 2026-05-05: session `#238`, dataset `zigomaro/cherryflash-session-238-1777972597`, kernel `zigomaro/cherryflash`; terminal story/video evidence pending

## Prevention

- Recovery bundles that share the SQLite volume must have explicit retention and free-space guards.
- Disk-pressure incidents must be treated as scheduler/product incidents when a daily publication slot has already been missed.
