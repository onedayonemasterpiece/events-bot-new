# INC-2026-09-05 VK auto import blocked by volume headroom

Status: open
Severity: sev1
Service: events-bot-new-wngqia / VK auto import
Opened: 2026-09-05
Closed: —
Owners: bot operations
Related incidents: INC-2026-07-08-prod-root-overlay-disk-full, INC-2026-04-16-prod-disk-pressure-runtime-logs

## Summary / Impact

VK imports on September 5 fail before source retrieval with
`vk_crawl_storage_admission_blocked:free_mb=400:min_free_mb=512`.
The generic row exception handler terminalizes these infrastructure failures as
`failed_technical / UNEXPECTED_ERROR`, so freeing storage alone will not retry
the affected carriers.

## Detection / Timeline

- Operator supplied Telegram transcripts for 10:14–10:52 (display timezone).
- Production ops_run 8236: 2026-09-05 08:15 UTC, 25 technical failures.
- Production ops_run 8244: 08:52 UTC, 24 technical failures and one past-event exclusion.
- Initial investigation: /data free 399–400 MiB; root /tmp free 6871 MiB.
- Runtime mirror verified enabled, 48h setting, active log plus seven ~8 MiB rotations.
- Local health: ok=true, ready=true, db=ok; /tmp write probe ok.

## Root Cause

- The 3 GB volume is below the 512 MiB VK write-admission floor.
- Largest file: live db.sqlite 1,222,328,320 bytes. Three historical top-level
  backup databases consume another 456,765,440 bytes.
- Health remains green at 399 MiB because its warning floor is 350 MiB, below VK admission.
- Auto-import catches admission failure as a per-source unexpected technical terminal.

## Automation Contract

### Treat as regression guard when

Changing VK import admission, infrastructure failure classification, disk hygiene or health thresholds.

### Affected surfaces

vk_intake.py storage guard; vk_auto_queue.py carrier selection/exception handling;
Fly /data; vk_inbox; ops_run; runtime health.

### Mandatory checks before closure or deploy

- Verify /data headroom above 512 MiB and /tmp writable; SQLite quick_check.
- Low storage must not consume/terminalize successive untouched inbox rows.
- Retain genuine source technical terminals and intentional exclusions.
- Recover only storage-failed carriers supported by exact ops_run evidence.
- Same-day bounded production catch-up with terminal results and health checks.
- Clean release provenance, relevant tests, fix reachable from origin/main.

### Required evidence

Incident artifacts: main checkout artifacts/codex/INC-2026-09-05-vk-storage-admission/
(probe.py/.txt, detail.py/.txt); tests; before/after state; deployed SHA if code deployed.

## Immediate Mitigation

Pending. Requested approval to archive and remove only three historical backups.
No production data or configuration changed at initial triage.

## Corrective / Follow-up Actions

- [x] Restore disk headroom without touching live databases.
- [ ] Prevent batch carrier consumption during infrastructure admission failure.
- [ ] Requeue exact affected carriers and verify same-day catch-up.
- [ ] Budget live DB growth, historical backups, and health/admission thresholds.

## Release And Closure Evidence

Open; no fix deployed, no catch-up completed.

## Investigation update

- Exact ops_run error URL matching found 321 currently failed_technical /
  UNEXPECTED_ERROR carriers across 13 runs from 04:15 through 08:52 UTC.
  IDs and run breakdown saved in affected.txt.
- July backup downloaded to local incident artifacts (257232896 bytes);
  local and production SHA-256 both
  `672fce8db5570383007b3cfdc56c292f5a4ffece2f1b76810606bfa7b13f5cee`.
  Production original retained pending operator cleanup approval.
- Batch-start protection implemented. Two parametrized regression cases failed
  before the change (bounded/unbounded queue claim attempted), then passed.
- Relevant test command:
  `python -m pytest tests/test_vk_auto_queue_import.py tests/test_vk_default_time.py -q --disable-warnings`;
  result 66 passed in 26.80s; process exit code 0 verified.
- No deploy, requeue or production cleanup yet. Same-day catch-up remains required.

- Fix pushed: `60731f5a21f3e13556f502abd78b770052983181`; PR https://github.com/onedayonemasterpiece/events-bot-new/pull/637. Not deployed.

## Approved production cleanup (2026-09-05, ~09:12 UTC)

Operator approved archiving/removing the three historical backups. Downloaded
all three locally and verified SHA-256 against production immediately before
unlinking. Checked /proc open descriptors and zero-length backup WAL files.
Removed only the three exact .bak files and their unused -wal/-shm sidecars.

- Removed 456,863,744 logical bytes (~436 MiB).
- /data free increased from 419,098,624 to 875,970,560 bytes (~835 MiB).
- Local artifacts retain all three backups plus backup-manifest.json.
- Evidence: cleanup.py/.txt, inventory.py/.txt, extra.py/.txt.
- /healthz: ok=true, ready=true, db=ok, disk status ok; scratch write probe ok.
- SQLite quick_check: ok. No VACUUM: freelist is only 46 pages (~184 KiB).

### Additional storage audit (no additional deletion)

- /data/backups/pre-static-collections-20260801T165142Z.sqlite.gz:
  59,669,371 bytes, candidate for archive-first removal.
- /data/incident_backups/R13-festival-calendar-predeploy-20260726T200141Z.sqlite.gz:
  70,380,981 bytes, candidate for archive-first removal.
- Combined additional historical backup candidates: ~124 MiB.
- /data/static_site_snapshots: ~156 MiB, two September 4 snapshots; retained
  because current issue621 work may still need them.
- /data/guide_monitoring_results: ~82 MiB, fresh latest runs; retained.
- /data/guide_media: dry-run scanned 321 files / 260,881,754 bytes, 268 existing
  protected files, 53 too recent, zero deletion candidates and no stale DB refs.
- /data/runtime_logs: ~60 MiB bounded rotations, retained for incident evidence.
- Live DB ~1.14 GiB is actual allocated data, not recoverable free-page bloat.

### Queue restoration and release gap

Exact 321 incident-evidenced failed carriers returned to pending; intentional
exclusions and other technical failures untouched. Readback: pending=321.
Preimage saved on prod at
/data/incident_backups/INC-2026-09-05-vk-storage-requeue-preimage.json.
Evidence: requeue.py/.txt.

PR #637 remains undeployed: python-ci failed in
test_four_image_schedule_runs_one_prepare_commit_operation_in_order with
InvalidArgumentsError (1 failed, 567 passed in the MCP gate); the other two
CI jobs passed. Full failure evidence saved as ci-failure.txt.
Release governance requires merged main plus green CI for the standard deploy.
The VK-specific local suite remains 66 passed, exit 0.

Bounded catch-up could not start through the available Telegram E2E account:
production @events_love39_bot replied "Not authorized" (command 35820, reply
35821). Production identity was verified using getMe on the Fly runtime.

The local .env bot token instead resolves to @eventsbotTestBot. An initial
command was mistakenly sent there (35818); no reply was observed, a stop
command was sent and the original command deleted. The readback process was
stopped before reconnecting to the same E2E session. No successful test-bot
execution is claimed. Evidence: local-env-target-mismatch.txt and catchup.txt.

321 restored rows remain pending for the production scheduler (observed next
slot 10:00 UTC). Requeue is NOT catch-up completion. Do not close this incident
until actual terminal results and the deployment gate are verified.


## Release gate repair

Reproduced the MCP CI failure locally and traced the normalized exception:
`SocialWorkspaceRuntimeError('asset reverification is unavailable')`.
The album test fake implements ingest but lacks the production reverify
contract. Added reference-specific owner/size/role-checked reverify to the
test double; production asset safety checks remain unchanged.
