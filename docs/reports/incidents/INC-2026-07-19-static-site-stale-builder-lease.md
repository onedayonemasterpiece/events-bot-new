# INC-2026-07-19 Static-site host reconciliation left a stale builder lease

Status: monitoring
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
only to fail at resource acquisition. During recovery, startup catch-up exposed
a second lifecycle defect: rearming the error outbox row replaced its payload
and erased the still-active remote handoff needed for exact-run adoption. A
subsequent compensating run exposed a third defect: the live callback handler
authenticated against a reusable SQLite connection whose stale transaction
could not see the runner's newly committed token, so a valid resource-acquire
callback was rejected as `invalid token`.

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
- 2026-07-19 01:18 UTC — the first current-main compensating build completed
  and published successfully; the queued Smart Update follow-up then started,
  rather than superseding the active recovery row.
- 2026-07-19 01:40 UTC — the queued follow-up completed from the same current
  main SHA and fresh production snapshot, atomically replaced the secret
  candidate, and left `static_site:builder` released.

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
6. The generic error-row requeue path replaced the complete outbox payload.
   When `static_site_build_state.active_job_id` still pointed at that exact job,
   this destroyed `remote_handoff` and `snapshot`, so the next worker could no
   longer adopt or terminally reconcile the already-submitted kernel.
7. `validate_run_token` and callback writes reused `Database.raw_conn`. That
   process-wide aiosqlite connection could retain a read snapshot or failed
   write transaction from another callback. Because the status config is
   committed by a separate runner process, the web handler could then miss the
   new token even though a fresh connection and the mounted status dataset had
   identical SHA-256 hashes.
8. The generic coalesced-outbox supersession check preferred a newer pending
   static follow-up over the older error row even when the durable state still
   named that older row as `active_job_id`. It therefore skipped the only job
   allowed to reconcile the exact remote run.

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
- changing static-site outbox requeue/merge semantics.

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
- externally committed callback tokens remain visible despite a deliberately
  stale shared SQLite snapshot, and a failed callback transaction cannot poison
  the next request;
- requeue of the exact active job preserves its recoverable handoff/snapshot,
  while a non-active stale job starts from the fresh request payload;
- an exact active recovery row runs before any newer pending coalesced
  follow-up instead of being marked `superseded`;
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
- Startup/Smart Update requeue now merges new effect evidence into an exact
  active job's recoverable payload instead of replacing its remote handoff and
  immutable snapshot. Non-active historical failures keep the old replacement
  behavior.
- Callback token validation now reads through a fresh connection, while every
  callback event owns one fresh bounded `BEGIN IMMEDIATE` transaction with
  rollback and close on failure.
- Generic outbox supersession now yields to the exact static-site active owner;
  the accumulated pending follow-up remains queued and runs only after remote
  recovery completes.

## Follow-up Actions

- [x] Complete and publicly validate a current-main compensating candidate.
- [ ] Add an operator alert when a successful static build owns an active lease
  or when a non-terminal Kaggle ledger has a successful host receipt.
- [ ] Close only after user visual acceptance of the replacement candidate.

## Release And Closure Evidence

- deployed SHA: `ed93a35aa55de4fb7110945facb2d05eee336578`, reachable
  from `origin/main`; Fly image
  `deployment-01KXVXSYS8K2HXBT9RJTAXZN81`.
- deploy path: manual `flyctl deploy` from a clean, main-based worktree after
  PRs #91–#95 were merged.
- regression checks: 71 focused Python tests passed; public Playwright checks
  passed for the five reported CTA/media fixtures, both requested gallery
  fixtures, desktop/mobile terminal-gallery navigation, and the six-card
  continuation component. All checked candidate routes returned `200` with
  `noindex,nofollow,noarchive,nosnippet` and `no-referrer`.
- compensating build: `production-secret-20260719T025527-89a6739b` completed,
  followed by the accumulated Smart Update build
  `production-secret-20260719T031810-5367ab9b`; the latter published `268`
  event pages / `906` generated pages / `982` files (`983` published objects)
  from the deployed SHA.
- root isolation: the public root SHA-256 remained
  `e2ddecb6c2856a94d4579a3091604b7c0804f3545220f43e94eac73e0aab450d`
  before and after publication; stable/current/ICS activation remained off.
- post-deploy verification: `/healthz` ready, `PRAGMA quick_check=ok`, runtime
  file mirror present, `/data` free space `1.66 GB`, and the exact final run's
  `static_site:builder` lease is `released`.
- review handoff: Telegram chat `-1004337049383`, reply to topic message `261`,
  new message `375`; read-back confirmed nine secret-candidate link entities.
- redacted/local evidence: `artifacts/codex/static-event-v13-final/` and
  `artifacts/codex/INC-2026-07-19-static-site-stale-builder-lease/` (ignored by
  Git).

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

## R13 production-generation regression evidence — 2026-07-27

The Festivals/unified-page production release exercised this incident contract
again and exposed one adjacent unprotected write after immutable publication:
all 1131 candidate objects had been uploaded and downloaded for verification,
but `_patch_static_site_request_payload` exhausted one 30-second ORM busy
timeout while a Smart Update transaction owned SQLite's writer. The Kaggle
ledger was already `done`, `resource_release` preceded `report_written`, and
the exact `static_site:builder` lease was released, so a second kernel was
neither necessary nor permitted.

Corrective release:

- page-source SHA `f93fdd2c99339c7a935a4c6aa2627c827f73b5c9`,
  reachable from `origin/main`;
- host recovery SHA
  `709eda27032122aca2f8d2b1e5464b2cc3289b58`, also reachable from
  `origin/main`;
- Fly image `deployment-01KYGC7D94F2272DFZG7C8DV9F`;
- 68 focused tests passed across `test_static_site_release.py`,
  `test_kaggle_status.py` and `test_static_site_build_handoff.py`;
- receipt payload writes now retry SQLite lock contention in four fresh
  sessions; an S3 precondition conflict is adopted only when the normal
  manifest-bound download check confirms exact bytes and MIME. No overwrite,
  root key or stable ICS key is expressible.

Recovery evidence:

- assertion-guarded backup:
  `/data/incident_backups/R13-receipt-lock-rearm-20260726T2335Z.json`;
- exact run
  `static-site:production-secret-20260727T004208-0af7c1de:9c46537a7bb1`
  was selected ahead of its accumulated follow-up, adopted without another
  Kaggle build and committed as current at `2026-07-26T23:54:49Z`;
- final receipt: 246 event pages, 894 generated pages, 1125 files and 1131
  published objects; `root_mutation=false`,
  `stable_ics_mutation=false`;
- current review URL:
  <https://kenigevents.ru/_review/qjjOTwpZHmmmBBv7lbHcSmluPCtWhXIa4mz1ZxYn9l4/>;
- public Playwright checks passed 19 routes at `1440×900` and `390×844`,
  including 21 Festivals cards, three club detail routes, Search editability,
  Today chronology, no horizontal overflow and the event `6667` free token;
- final operations state: no active static claim, exact lease `released`,
  `PRAGMA quick_check=ok`, runtime mirror enabled/current and `/data` free space
  approximately 1.85 GiB;
- Telegram review-thread read-back: chat `-1004337049383`, reply `548`,
  message `692`, 22 link entities.

The public root remains unchanged. Closure still requires the owner's visual
acceptance of the immutable review candidate; an ordinary debounced Smart
Update follow-up may publish a newer candidate later but cannot mutate this
review URL.

## R15 cross-deploy terminal recovery regression — 2026-08-09

The first merged Unusual production-health run exposed a narrower recurrence.
A StaticSiteBuilder kernel had already reached terminal `done`, but its Fly
wrapper had not adopted the result before a new exact-main image was deployed.
The replacement image correctly rejected the old repo SHA; however, that
permanent error left `static_site_build_state.active_job_id=50189` and the
recoverable handoff intact. Subsequent operator requests therefore rearmed the
same incompatible owner instead of reaching a current-image build.

The recovery contract now distinguishes terminal from live cross-deploy
handoffs. A live or terminal-unknown old run remains deferred and cannot be
replaced. For an exact-owner run whose ledger has both a terminal status and
`terminal_at`, the host first reconciles/releases resources for that run only,
records the claim as failed with `cross_deploy_recovery_rejected`, redacts the
handoff, removes its immutable snapshot/output, and only then continues to a
fresh build. Focused regression covers both the terminal replacement path and
the live fail-closed path. Production warm/cold run IDs and final lease/browser
evidence remain pending until the hotfix reaches exact `origin/main`.


## Static collections data-prep regression — 2026-08-01

The candidate branch `integration/static-collections-data-prep-20260801` adds
mandatory collection artifacts to the existing immutable handoff; it does not
create another notebook, lease, snapshot or publisher. The collection batch,
BGE cache/receipt and semantic receipt are validated under the same exact
run/snapshot/fingerprint before persistence. Incoming Smart Update effects while
a build is running still create exactly one recoverable follow-up and do not
replace `remote_handoff`.

Regression evidence: `123` collection/semantic/release tests and `116` Kaggle
status/handoff/unusual/outbox tests passed. No deploy/live lease exercise was
performed, so the incident's existing production evidence is unchanged and the
new branch still requires a real cold/warm candidate before release.
