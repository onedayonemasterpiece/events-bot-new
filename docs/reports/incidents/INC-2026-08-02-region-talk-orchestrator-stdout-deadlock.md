# INC-2026-08-02 Region Talk orchestrator stdout deadlock

Status: investigating
Severity: sev2
Service: Region Talk scheduled discovery/orchestration
Opened: 2026-08-02
Closed: —
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-deploy-interrupted-sessions`, `INC-2026-08-01-region-talk-draft-backfill-nameerror`
Related docs: `docs/features/region-talk-channel/orchestration-to-be.md`, `docs/operations/runtime-logs.md`

## Summary

The production Region Talk orchestrator could block while reporting a cycle to
its scheduled wrapper. A cycle is emitted as one large JSON line. The wrapper
used `asyncio.StreamReader.readline()` with the default 64 KiB limit; after the
reader task failed to consume an oversized line, the child filled the stdout
pipe and slept in `pipe_write`. Its loop then stopped refreshing Kaggle status,
YDB heartbeats and ready actions although the watchdog process remained alive.

## User / Business Impact

- source-profile acquisition and editorial backfill could stop between remote
  Kaggle runs until the 90-minute timeout or another recovery;
- watchdog heartbeats made the wrapper look alive while its useful child loop
  no longer progressed;
- no target publication occurred and no manual-review/publication permission
  was raised automatically.

## Detection

During supervised source-profile recovery, CandidateReport itself emitted
current stage heartbeats to YDB, while the server wrapper stopped logging cycle
JSON. Production `/proc` evidence for child PID 2045 showed state `sleeping`,
wait channel `pipe_write`, stdout/stderr pointing to the same pipe, and command
`region_talk_orchestrator.py --loop --execute-ready ...`. The current runtime
JSONL remained zero bytes while the parent watchdog continued ten-second job
heartbeats.

## Timeline

- 2026-08-02 12:22 UTC — watchdog catch-up started the affected wrapper.
- 2026-08-02 12:33–12:49 UTC — a remote CandidateReport version continued and
  terminally wrote YDB heartbeat `report_written`; source capture still made
  bounded progress.
- 2026-08-02 12:54 UTC — child PID 2045 was observed blocked in `pipe_write`;
  current wrapper JSONL was still empty.
- 2026-08-02 12:57 UTC — local fix and an oversized-line regression test were
  prepared; production delivery and catch-up remained pending.

## Root Cause

The scheduled wrapper assumed each orchestrator JSON line fit asyncio's default
`StreamReader` limit. `readline()` can fail before returning a newline-delimited
payload larger than that limit. Once its consumer task stopped, no process
drained the child pipe.

## Contributing Factors

- cycle output contains full live metrics plus repeated selection snapshots;
- wrapper liveness and child-loop progress were reported separately;
- no regression test exercised a cycle line above 64 KiB.

## Root-cause classification

Mechanical transport/lifecycle failure. The fix is deterministic byte-stream
draining and does not make semantic candidate or publication decisions.

## Automation Contract

### Treat as regression guard when

- changing Region Talk scheduled-wrapper subprocess handling, orchestrator
  cycle serialization, runtime JSONL capture or Kaggle/YDB polling cadence.

### Affected surfaces

- `scripts/region_talk_scheduled_runner.py`;
- `scripts/region_talk_orchestrator.py --loop` stdout contract;
- Region Talk watchdog recovery and runtime JSONL evidence.

### Mandatory checks before closure or deploy

- a real subprocess line above 300 KiB is drained, parsed and persisted within
  a bounded test timeout;
- ordinary runner/reaction/plan tests remain green;
- deployed child advances through at least two post-fix loop cycles without
  `pipe_write` blockage;
- Kaggle status and exact YDB heartbeat correspond to the same current run;
- compensating run rereads YDB, reduces or terminally explains the current
  source-profile backlog, keeps intake unreviewed and publishes nothing.

### Required evidence

- focused and full Region Talk test results;
- deployed SHA reachable from `origin/main` and green `/healthz`;
- post-deploy runtime JSONL growth, process wait-channel/status evidence and
  current YDB queue/publication counts.

## Immediate Mitigation

Out-of-band launches were stopped. Existing CandidateReport/YDB heartbeat
evidence was used to distinguish the terminal remote run from the blocked local
wrapper. Temporary private datasets from the superseded manual launch were
deleted exactly after Kaggle reached terminal status.

## Corrective Actions

- consume child stdout with fixed-size `read()` chunks and assemble JSONL lines
  independently of asyncio's default line limit;
- give the subprocess a bounded configurable 8 MiB stream buffer as secondary
  protection;
- add a real-subprocess regression with a JSON line larger than 300 KiB.

## Follow-up Actions

- [ ] Merge and deploy the fix from `origin/main`.
- [ ] Complete and verify the compensating Region Talk catch-up.
- [ ] Confirm source-profile capture queue drain and resume profile/writer
  backfill without target publication.

## Release And Closure Evidence

Pending.

## Prevention

Wrapper liveness is insufficient evidence of orchestration progress. The
transport must continuously drain arbitrarily long bounded cycle records, and
closure requires observed post-fix cycle advancement plus durable YDB effects.
