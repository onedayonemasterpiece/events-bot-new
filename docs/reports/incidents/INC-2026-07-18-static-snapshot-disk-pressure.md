# INC-2026-07-18 Static Snapshot Disk Pressure During Candidate Retries

Status: mitigated
Severity: sev1
Service: production bot / Fly app `events-bot-new-wngqia`
Opened: 2026-07-18
Closed: —
Owners: operations / static-site pipeline
Related incidents: `INC-2026-04-16-prod-disk-pressure-runtime-logs`, `INC-2026-07-15-fly-volume-critical`, `INC-2026-07-16-static-event-media-action-regressions`
Related docs: `docs/operations/runtime-logs.md`, `docs/features/static-site-pages/astro-preview.md`, `docs/operations/release-governance.md`

## Summary

During the event-page preproduction deploy, `/healthz` became externally
unreachable because the Fly health endpoint returned `503` at the disk-critical
floor. Seven immutable static-site SQLite snapshots from successful and failed
Kaggle candidate attempts occupied about `1.8 GiB` on the `3 GiB` `/data`
volume. Snapshot creation was immutable, but terminal success/failure paths did
not remove their input pairs and no crash-retention bound existed.

## User / Business Impact

- Fly proxy stopped routing external health traffic while the only machine was
  marked critically unhealthy;
- the bot process and scheduler kept running, but deploy verification timed out
  and the service was at high risk of SQLite write failure;
- the requested secret static candidate could not safely be retried until space
  was restored.

## Detection

- `fly deploy` waited on the critical machine check and external `/healthz`
  timed out;
- machine-local `/healthz` returned `503` with
  `issues=["disk:critical_free_space"]` and only `114 MiB` free;
- `du` identified `/data/static_site_snapshots` at about `1.8 GiB`; seven
  `267–268 MiB` complete snapshots had been created between 13:43 and 17:52 UTC;
- runtime mirror was enabled and correctly bounded at about `61 MiB`, so it was
  not the source of this recurrence.

## Timeline

- 18:05 UTC — main SHA `11c6b331` deployed; Fly machine version `1705` started.
- 18:07 UTC — Consul `/healthz` checks returned `503`; external request timed out.
- 18:09 UTC — machine-local health isolated `disk:critical_free_space`, `114 MiB` free.
- 18:10 UTC — `PRAGMA quick_check=ok`, no open snapshot file descriptors, and
  durable static build state had no active claim.
- 18:10 UTC — five terminal/regenerable snapshot pairs were removed, retaining
  the latest successful and latest failed pair for evidence; `1,337,659,194`
  bytes were freed.
- 18:11 UTC — `/data` had about `1.39 GiB` free, SQLite quick check remained
  `ok`, local and public `/healthz` returned `200`, and Fly reported one passing
  critical check.
- 18:19 UTC — the final pre-fix retry drew a legal token beginning with `-`;
  separate-argument parsing rejected it before Kaggle push and created one more
  terminal snapshot. With no active claim/reader, its exact pair and sidecars
  were removed (`267,809,442` more bytes).

## Root Cause

1. `job_static_site_build_kaggle` deleted snapshots for no-op/busy decisions but
   not after successful or failed claimed runs.
2. Retried deterministic candidate failures created a fresh full SQLite backup
   on every attempt.
3. No startup/pre-build pruning bounded snapshots leaked by process death or a
   deploy restart.
4. A valid base64url candidate token beginning with `-` was handed to argparse
   as a separate value. Argparse interpreted it as an option, consumed the
   job's final fourth attempt and created one additional terminal snapshot.

## Contributing Factors

- one production SQLite snapshot is roughly `267 MiB`, so a small retry storm
  can exhaust a `3 GiB` volume quickly;
- candidate-token tests used only an `A…` value and did not cover the legal
  leading-hyphen boundary;
- health correctly failed closed at `256 MiB`, but retention enforcement was
  limited to runtime logs and did not cover static-site inputs.

## Automation Contract

### Treat as regression guard when

- changing Smart Update → static-site snapshot, retry, recovery or Kaggle
  handoff lifecycle;
- deploying the production bot or changing `/data` capacity/retention policy.

### Affected surfaces

- `main.py::job_static_site_build_kaggle`;
- `static_site_release.py` immutable snapshot lifecycle;
- `/data/static_site_snapshots`, Fly `/healthz`, deploy machine checks;
- static-site retry/recovery state in SQLite.

### Mandatory checks before closure or deploy

- unit-test active-snapshot preservation, terminal cleanup and bounded crash
  leftovers;
- `PRAGMA quick_check=ok` before and after cleanup/deploy;
- `/data` free space above the documented warning floor;
- public `/healthz` HTTP `200`, `ready=true`, disk `status=ok` and Fly critical
  check passing;
- a fresh successful candidate run leaves no unbounded snapshot growth;
- runtime logs contain no fresh `Errno 28` or `database or disk is full`.

### Required evidence

- fixed SHA reachable from `origin/main` and production machine image/version;
- exact removed bytes, retained snapshot set and post-cleanup `df`;
- focused tests plus one live Smart Update/Kaggle candidate result;
- public candidate and Telegram review-link readback required by the parent
  static-event incident.

## Immediate Mitigation

- confirmed SQLite integrity and absence of active snapshot readers;
- removed five exact terminal/regenerable snapshot pairs and their sidecars;
- retained `snapshot-20260718T165930-13a4c3fd92` (latest successful at triage)
  and `snapshot-20260718T175239-f447901bb1` (latest failed evidence);
- verified public service recovery without resizing the volume or weakening the
  health floor.

## Corrective Actions

- success/no-op/busy/recovered-success and failures without a durable remote
  dataset delete their exact snapshot, manifest and SQLite sidecars; a pushed
  dataset preserves its claim and exact input until recovery/adoption;
- candidate tokens use argparse's `--candidate-token=<value>` form so every
  valid base64url token, including a leading `-`, is one argument;
- before creating a new snapshot, a crash guard preserves durable active paths
  and bounds unreferenced complete pairs to one newest diagnostic snapshot;
- retention is configurable only by the explicit
  `STATIC_SITE_SNAPSHOT_KEEP_LATEST_TERMINAL` count, default `1`.

## Follow-up Actions

- complete the fresh main-based secret candidate and verify post-run snapshot
  usage before moving this incident from mitigated to monitoring/closed;
- keep volume auto-extension as an availability guard only, not retention.

## Release And Closure Evidence

- mitigation: `1,605,468,636` bytes removed across the initial five pairs and
  the later pre-push argparse failure; `/data` free space increased from
  `114 MiB` to about `1.3–1.39 GiB`; `PRAGMA quick_check=ok` before and after;
- service recovery: machine-local and public `/healthz` HTTP `200`,
  `ready=true`, disk `status=ok`; Fly machine version `1705` one passing check;
- prevention deploy SHA and fresh candidate evidence: pending.
