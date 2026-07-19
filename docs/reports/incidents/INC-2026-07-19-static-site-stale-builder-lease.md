# INC-2026-07-19 Static-site host reconciliation left a stale builder lease

Status: mitigated
Severity: sev2
Service: Smart Update → Kaggle StaticSiteBuilder → immutable secret candidate
Opened: 2026-07-18
Closed: —
Owners: static-site release pipeline
Related incidents: `INC-2026-07-16-static-event-media-action-regressions`, `INC-2026-07-18-static-snapshot-disk-pressure`
Related docs: `docs/operations/kaggle-static-site-builder.md`, `docs/features/static-site-pages/README.md`

## Summary

The host successfully validated and published secret build
`production-secret-20260719T005945-f008d70d`, but the kernel's terminal
callbacks were absent. The existing host reconciliation then hit a concurrent
SQLite writer lock and was treated as best-effort. Its generic implementation
also did not release resource leases. The completed run therefore retained the
exclusive `static_site:builder` lease and two later Kaggle kernels were pushed
only to fail at resource acquisition.

## User / Business Impact

- The current immutable review candidate remained available; the production
  root was not changed.
- Smart Update could not refresh the secret candidate with the latest main SHA
  and production data until the stale lease was reconciled.
- Two redundant Kaggle submissions consumed time and created misleading
  `running` ledger rows before the outbox attempt was superseded.

## Detection

- The release audit found a successful build-history/current-candidate receipt
  paired with a non-terminal Kaggle ledger and an active lease owned by that
  successful run.
- Runtime logs contained `sqlite3.OperationalError: database is locked` in
  `_finish_static_site_candidate` host reconciliation.
- The next runs emitted `resource_acquire ... resource=blocked` and never
  reached export/build phases.

## Timeline

- 2026-07-18 23:01 UTC — run
  `static-site:production-secret-20260719T005945-f008d70d:960f46c5c014`
  acquired `static_site:builder` and began export.
- 2026-07-18 23:08 UTC — Fly downloaded and hash-validated the complete Kaggle
  result.
- 2026-07-18 23:20 UTC — immutable candidate publication succeeded, but host
  terminal reconciliation failed on `BEGIN IMMEDIATE` with `database is locked`.
- 2026-07-18 23:32 and 23:34 UTC — replacement runs reached
  `resource_acquire` and were blocked by the completed run's lease.
- 2026-07-18 23:45 UTC — assertion-guarded mitigation verified the published
  receipt/history/job identity, backed up the affected rows, released only the
  exact owner's lease, recorded `host_result_validated`, and expedited the
  pending Smart Update follow-up.

## Root Cause

1. Kernel callbacks are allowed to be absent after the accepting host has
   independently validated a complete immutable result; this is why host
   reconciliation exists.
2. `reconcile_kaggle_run_terminal_from_host` marked the ledger terminal but did
   not release active leases owned by the exact completed run.
3. The one-shot reconciliation shared the production SQLite writer path. A
   concurrent Smart Update transaction caused `BEGIN IMMEDIATE` to fail, and
   `_finish_static_site_candidate` deliberately preserved publication success
   while only logging the status drift.
4. The next static build did not replay reconciliation for the durable current
   candidate before pushing a new kernel.
5. The same single-writer contention could also abort creation of the next
   run's status ledger before kernel push because that `BEGIN IMMEDIATE` path
   had no bounded retry.

This is a mechanical idempotency/status-lifecycle failure; it does not make an
event-semantic decision and does not require an LLM-first repair.

## Contributing Factors

- The lease TTL is three hours, so a missed release has a large recovery delay.
- Ledger success, release-claim success and resource release were separately
  durable but not reconciled from the same immutable host receipt.
- Diagnostics correctly exposed the mismatch, but there was no pre-push
  self-heal guard.

## Automation Contract

### Treat as regression guard when

- changing `kaggle_status.py` host reconciliation or resource leases;
- changing `main.py` static-site finish/preflight/recovery paths;
- changing Smart Update static-site single-flight, Kaggle callbacks or deploy
  catch-up behavior.

### Affected surfaces

- `kaggle_status.reconcile_kaggle_run_terminal_from_host`;
- `main._finish_static_site_candidate` and
  `main.job_static_site_build_kaggle`;
- `kaggle_resource_lease`, `kaggle_run_ledger`, `kaggle_run_event`;
- `static_site_build_state`, history/current-candidate receipt and outbox;
- Fly runtime logs and Kaggle `StaticSiteBuilder` kernels.

### Mandatory checks before closure or deploy

- host reconciliation releases active leases only where `run_id` exactly
  matches the host-validated run;
- an unrelated/successor lease remains active;
- already-terminal ledgers with a stale exact-owner lease are repaired;
- transient `database is locked` is retried with bounded backoff;
- static-site preflight replays reconciliation for the durable current
  candidate before remote recovery/push;
- Python Kaggle-status/static-handoff regression suites pass;
- a post-deploy Smart Update/operator catch-up reaches terminal success,
  publishes a current-main candidate and leaves no active lease owned by an
  older successful run;
- `/healthz`, `PRAGMA quick_check`, disk floor and runtime mirror remain healthy.

### Required evidence

- exact deployed SHA reachable from `origin/main`;
- test output for `tests/test_kaggle_status.py` and
  `tests/test_static_site_build_handoff.py`;
- redacted before/after production DB and runtime-log excerpts under
  `artifacts/codex/INC-2026-07-19-static-site-stale-builder-lease/`;
- final current-candidate receipt and public regression checks;
- Telegram review-thread receipt after public QA.

## Immediate Mitigation

- Preserved a JSON backup at
  `/data/incident_backups/INC-2026-07-19-static-site-stale-builder-lease-20260718T234507Z.json`.
- In one assertion-guarded transaction, released only
  `static_site:builder` owned by the proven successful run, marked that ledger
  host-validated, and moved pending job `38001` to the current time.
- Did not clear or release any successor run identity.

## Corrective Actions

- Host reconciliation now atomically terminates the ledger and releases all
  still-active leases owned by that exact run ID.
- It retries only SQLite lock contention with bounded backoff; other failures
  still fail visibly.
- Status-ledger configuration uses the same bounded lock-only acquisition
  policy before creating the callback dataset.
- Every static-site job preflight replays the idempotent reconciliation for the
  immutable current-candidate receipt before remote recovery or push.

## Follow-up Actions

- [ ] Complete and publicly validate a current-main compensating candidate.
- [ ] Add an operator alert when a successful static build owns an active lease
  or when a non-terminal Kaggle ledger has a successful host receipt.
- [ ] Close only after user visual acceptance of the replacement candidate.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending manual `flyctl deploy` from clean main-based worktree
- regression checks: targeted suites pass locally; production catch-up pending
- post-deploy verification: pending

## Prevention

The accepted host receipt is now a replayable recovery authority for status
and exact-owner lease cleanup. A missing callback or transient writer lock can
no longer make the next Smart Update wait for TTL or launch a knowingly blocked
replacement kernel.

The concurrency contract follows the official SQLite documentation: only one
write transaction is active at a time and `BEGIN IMMEDIATE` may return
`SQLITE_BUSY`; callers must keep the transaction short and retry boundedly:
<https://www.sqlite.org/lang_transaction.html>. Python's connection timeout is
only a wait limit before `OperationalError`, not a guarantee that a busy writer
will clear: <https://docs.python.org/3/library/sqlite3.html#sqlite3.connect>.
