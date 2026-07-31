# INC-2026-07-20-static-claim-lost-health

Status: mitigated
Severity: sev1
Service: Fly production bot / JobOutbox / health endpoint
Opened: 2026-07-20
Closed: —
Owners: Codex incident owner
Related incidents: `INC-2026-07-20-static-event-keyboard-visual-regressions`, `INC-2026-07-19-static-site-stale-builder-lease`
Related docs: `docs/features/static-site-pages/README.md`, `docs/operations/runtime-logs.md`

## Summary

A Fly deploy while the static-site Kaggle job remained active caused the new
process to lose the outbox compare-and-swap. The loss branch rolled back the
SQLAlchemy session and then read `obj.id` from an expired ORM instance. That
attempted async SQLite IO outside `greenlet_spawn`, repeated each worker cycle,
and made `/healthz` time out.

## User / Business Impact

- the production health endpoint was unavailable after release v1726;
- the job-outbox worker could not make progress while the exception repeated;
- the already-running remote Kaggle job was not cancelled or duplicated.

## Detection

- Fly machine check became critical;
- direct `/healthz` timed out;
- production file/stdout logs showed `STATIC_SITE_CLAIM_LOST` immediately
  followed by `sqlalchemy.exc.MissingGreenlet` at the post-rollback `obj.id` read.

## Timeline

- 15:00 UTC: release v1726 restarted Fly while StaticSiteBuilder was live.
- 15:06 UTC: critical Fly check and `/healthz` timeout confirmed.
- 15:07 UTC: exact post-rollback lazy-load root cause identified in production logs.

## Root Cause

The CAS-loss diagnostic used an ORM attribute after `session.rollback()`. The
rollback expired the instance, so the diagnostic itself attempted forbidden
lazy async IO.

## Contributing Factors

- a long-running remote browser gate increased the chance of deploy/restart
  overlapping a claimed static build;
- the CAS owner-loss path had no deploy-overlap regression test.

## Automation Contract

### Treat as regression guard when

- changing JobOutbox claim/rollback handling or static-site single-flight;
- deploying while a static-site remote run is active.

### Affected surfaces

- `main.py::_run_due_jobs_once_locked`;
- Fly `/healthz` and job-outbox worker;
- static-site Smart Update/Kaggle handoff.

### Mandatory checks before closure or deploy

- compile and focused outbox/static-site tests;
- clean `origin/main` deployment;
- `/healthz` returns ready after a live-CAS-loss cycle;
- logs contain `STATIC_SITE_CLAIM_LOST` without `MissingGreenlet`.

### Required evidence

- merged and deployed SHA;
- Fly release and healthy machine check;
- post-deploy log/health receipt.

## Immediate Mitigation

Cache the integer job id before the CAS and use that scalar after rollback.

## Corrective Actions

- removed post-rollback ORM access from the claim-loss branch;
- added this incident as a mandatory regression contract.

## Follow-up Actions

- [ ] add a full deploy-overlap integration scenario for the outbox CAS loser;
- [x] verified the active/retried static build reached the successful terminal
  receipt for `production-secret-20260720T201154-77720953`; the coalesced
  successor correctly completed as a fingerprint no-op.

## Release And Closure Evidence

- deployed SHA: `1e7594d22c545f535c131aef3e9f9e5bddddd9f3`, reachable from
  `origin/main`; Fly release `v1734`
- deploy path: Fly remote build from clean exact `origin/main`
- regression checks: focused outbox/static-site tests and both required CI
  checks passed before merge; the post-deploy static job completed and released
  its claim, then the queued duplicate became a no-op
- post-deploy verification: `/healthz` returns `ok=true`, `ready=true` with no
  issues; current runtime mirror contains no `MissingGreenlet` and the exact
  corrected candidate has a successful terminal receipt
- closure caveat: a deliberately induced live deploy-overlap CAS-loser cycle
  has not yet been run, so status remains `mitigated` until that follow-up is
  completed

## Prevention

All values needed after rollback must be copied to plain scalars before the
transaction boundary; diagnostics must never lazy-load expired ORM objects.

## 2026-07-31 recurrence

Two Fly restarts interrupted job `44365` before it persisted a remote handoff.
The row remained `running` with no active build claim, while newer Smart Update
job `45903` lost its CAS roughly every two seconds. The worker remained alive,
but the July 29 checked candidate stayed current.

The regression fix gives pre-handoff orphans a separate ten-minute stale
window and defers a CAS loser for 30 seconds without spending its attempt. A
durable exact owner or remote handoff retains the full 90-minute build budget.
Closure additionally requires a compensating production build with terminal
Kaggle ledger, released exact lease and advanced checked-candidate receipt.
The recurrence also exposed an invalid deployed configuration:
`STATIC_SITE_REQUIRE_VECTOR_BARRIER=1` with `ENABLE_EVENT_VECTOR_SYNC=0`.
Production now enables the vector owner after the shared atomic limiter gate;
the barrier and its producer must be changed together.
