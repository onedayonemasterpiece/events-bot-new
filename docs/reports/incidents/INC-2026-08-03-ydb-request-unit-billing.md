# INC-2026-08-03 YDB Request Unit billing runaway and wrong-account placement

Status: monitoring
Severity: sev1
Service: Region Talk YDB sidecar and autonomous scheduler
Opened: 2026-08-03
Closed: —
Owners: events-bot production owner
Related incidents: `INC-2026-08-02-region-talk-orchestrator-stdout-deadlock.md`
Related docs: `docs/features/region-talk-channel/ydb-schema.md`, `docs/operations/cron.md`, `docs/operations/release-governance.md`

## Summary

Region Talk used the serverless YDB `events-bot-acq-discovery` in the wrong
Yandex Cloud account. Repeated wide metric/state reads inside long autonomous
cycles, amplified by watchdog catch-up runs after crashes, consumed about
82.42 million Request Units by the time of detection. The scheduler was stopped,
the source database was throttled to 0 RU/s, and all five tables were migrated
to a 1 GiB serverless database in `cloud-art-koder/default` with exact row and
ordered-export hash verification. After explicit owner approval, the verified
source database was deleted from the wrong account.

## User / Business Impact

- The wrong billing account accrued approximately 2,006 RUB of YDB Request Unit
  charges visible at detection time.
- Region Talk autonomous processing was disabled to stop further spend.
- Canonical YDB data was not lost; public event-bot health remained ready.

## Detection

The owner noticed a sharp YDB Serverless Request Units charge in Yandex Cloud
Billing. Existing application health checks did not monitor RU consumption,
account ownership, or catch-up amplification, so the bot remained healthy while
cost accumulated.

## Timeline

- 2026-07-01 17:13 UTC — source database `etnrao7p6gh6il6b4qv9` was created in
  cloud `b1goifscr17duurhullj` and later reused by Region Talk.
- 2026-08-01 through 2026-08-03 — scheduled and watchdog catch-up cycles repeatedly
  loaded wide YDB state, including limits up to 20,000 rows per kind.
- 2026-08-03 ~13:00 UTC — billing anomaly reported; Fly scheduler and Region Talk
  watchdog were disabled.
- 2026-08-03 13:25 UTC — source YDB control-plane throttling was enabled at
  0 RU/s, blocking all new requests without deleting data.
- 2026-08-03 13:32 UTC — target serverless database
  `etnkibjidis0o6stn2cq` became available in `cloud-art-koder/default`.
- 2026-08-03 13:32–13:40 UTC — five tables were exported once under bounded
  throttling and restored. Counts matched `2`, `9`, `231`, `266`, and `58,046`;
  ordered CSV SHA-256 matched exactly for every table.
- 2026-08-03 ~13:45 UTC — both databases returned to 0 RU/s. Fly secrets and the
  protected GitHub environment database variable were switched to the target.
- 2026-08-03 ~13:55 UTC — the complete source-cloud serverless inventory was
  checked. Its only Cloud Function is the unrelated
  `pharmastaff-partnership-form`; there are no triggers or serverless
  containers. The Pharmastaff mail function was intentionally left unchanged.
- 2026-08-03 14:13 UTC — after explicit owner approval, Yandex Cloud operation
  `etndquiisa371hgl8p7r` deleted source database `etnrao7p6gh6il6b4qv9`.
  Direct lookup returned `Not Found`; the unrelated `pharmastaff-forms` and
  `kotopogoda-content` databases remained untouched.
- 2026-08-03 14:20 UTC — under a separate explicit owner request, deletion
  protection was disabled for unrelated database `kotopogoda-content`, and
  operation `etnak5qntmv84d86adlb` deleted it. Direct lookup returned
  `Not Found`; `pharmastaff-forms` remained the only database in the source
  folder.
- 2026-08-03 14:30 UTC — after explicit owner approval, obsolete service
  accounts `cat-weather-ydb-runtime`, `region-talk-discovery-runtime`, and
  `region-talk-gh-actions-importer` were deleted. All three returned
  `Not Found`. The protected GitHub environment's old
  `YANDEX_WIF_SERVICE_ACCOUNT_ID` variable was removed so imports fail closed;
  `farmpersonal-deploy` and `sa-pharmastaff-forms` remained untouched.

## Root Cause

1. The original YDB was created from an implicit/incorrect Yandex Cloud context;
   the full wrong-account path then became durable configuration.
2. Region Talk reused the pre-existing `events-bot-acq-discovery` database by
   name instead of enforcing an expected cloud/database identity.
3. The autonomous orchestrator repeatedly materialized broad row sets for
   metrics and decisions. Crash recovery triggered additional catch-up runs,
   turning an inefficient access pattern into near-continuous RU consumption.
4. No RU budget, database-ownership assertion, or billing alert guarded the
   scheduled production contour.

## Contributing Factors

- A database under 1 GiB was incorrectly assumed to imply negligible cost;
  serverless reads are billed independently of stored size.
- A 20,000-row scan limit and 180-second loop cadence were safe only as latency
  bounds, not as cost bounds.
- Health checks observed scheduler liveness, not YDB RU rate or billing account.

## Automation Contract

### Treat as regression guard when

- changing Region Talk YDB endpoint/database/credentials or Yandex Cloud owner;
- enabling `ENABLE_REGION_TALK_SCHEDULED`;
- changing orchestrator polling, retry, watchdog, scan, or metric-read behavior;
- restoring or migrating Region Talk state.

### Affected surfaces

- `fly.toml` Region Talk scheduler and YDB configuration;
- `scripts/region_talk_scheduled_runner.py` preflight;
- Fly secrets, Kaggle encrypted run datasets and GitHub protected environment;
- the deleted source and active target Yandex Managed Service for YDB databases;
- Yandex Cloud Billing and RU throttling.

### Mandatory checks before closure or deploy

- configured database exactly equals `REGION_TALK_YDB_EXPECTED_DATABASE`;
- scheduled wrapper rejects a wrong-cloud path without logging secret values;
- source and target table counts match;
- ordered data exports match by SHA-256 for all tables;
- source lookup returns `Not Found`; target remains at 0 RU/s while scheduling
  is disabled;
- Fly `/healthz` is ready and reports Region Talk plus watchdog disabled;
- no production re-enable without a bounded RU canary and billing observation;
- do not migrate or modify the unrelated `pharmastaff-partnership-form` Cloud
  Function as part of this incident.

### Required evidence

- source deletion operation plus `Not Found` verification, and target
  folder/cloud ID plus throttle state;
- row-count and ordered-export hash comparison;
- Fly runtime database/service-account identity and health response;
- GitHub protected-environment database variable and absence of any deleted
  old-account WIF service-account reference;
- deployed SHA reachable from `origin/main` for the durable guard/config change.

## Immediate Mitigation

- Set `ENABLE_REGION_TALK_SCHEDULED=0` as a Fly secret and restarted the machine.
- Set the source database throttling limit to 0 RU/s.
- Kept all data and enabled no provisioned capacity.

## Corrective Actions

- Created a 1 GiB serverless target in the correct cloud with deletion protection.
- Migrated and verified all five tables.
- Created a dedicated target service account with database-scoped `ydb.editor`.
- Switched Fly and GitHub configuration to the target database.
- Added an exact-database preflight guard and made the durable Fly scheduler
  default disabled.
- Deleted the verified source database from the wrong account after explicit
  owner approval. Preserved `pharmastaff-forms`; `kotopogoda-content` was later
  deleted only under a separate explicit owner request.
- Deleted the three explicitly approved obsolete service accounts and removed
  the old GitHub WIF service-account ID from the protected environment.

## Follow-up Actions

- [ ] Add a YDB RU budget/alert before any scheduler re-enable.
- [ ] Replace full row materialization for metrics with narrow indexed/counter
      reads and measure RU per complete cycle.
- [ ] Run a manually approved canary with a hard RU/s ceiling and confirm billing.
- [ ] Before autonomous use, add an application-side daily run/query budget;
      billing budgets only notify and YDB's RU/s throttle is not a per-day
      request counter. Keep the database at 0 RU/s until this is accepted.
- [x] Delete the verified source database after owner approval.
- [x] Delete obsolete old-account runtime and GitHub importer identities; none
      had long-lived authorized, access, or API keys at deletion time.
- [ ] Before re-enabling GitHub imports, create a new target-organization WIF
      identity and restore the protected environment only with that identity.

## Release And Closure Evidence

- deployed SHA: `797d756c1688f4084e6d9823c74567d06766ebae`, exact
  `origin/main` at deployment time
- release path: PR [#304](https://github.com/onedayonemasterpiece/events-bot-new/pull/304)
  followed by `scripts/deploy_fly_main.sh --remote-only`
- regression checks: migration counts and hashes passed; scheduled-runner unit
  suite passed (`18 passed`); PR `python-ci` and
  `static-browser-release-gate` passed
- post-deploy verification: Fly machine version `1901` started with its health
  check passing; `/healthz` ready; Region Talk and watchdog disabled; runtime
  database and expected-database paths both identify target
  `etnkibjidis0o6stn2cq`; embedded image SHA matches the deployed SHA

## Prevention

Production scheduling now requires an exact expected database path. The wrong
cloud/database produces a stable fail-closed preflight marker without leaking
paths. The source database is deleted; the scheduler remains disabled and the
target remains at 0 RU/s until a separate bounded-cost acceptance explicitly
raises its limit.
