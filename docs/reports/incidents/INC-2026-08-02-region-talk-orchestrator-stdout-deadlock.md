# INC-2026-08-02 Region Talk orchestrator stdout deadlock

Status: closed
Severity: sev2
Service: Region Talk scheduled discovery/orchestration
Opened: 2026-08-02
Closed: 2026-08-02 16:32 UTC
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
- 2026-08-02 13:04 UTC — PR #232 merged the bounded chunk-drain fix to
  `origin/main` as `ee0b1920dfad7980ea05421f02e60b5a1e89de17`.
- 2026-08-02 15:50–16:17 UTC — the natural scheduled run persisted three
  consecutive 148–154 KiB cycle records (453,925 bytes total). The child
  alternated between normal `ep_poll`/sleep states and never returned to
  `pipe_write`; CandidateReport reached terminal heartbeat `report_written`
  with matching run ID and sequence 66.
- 2026-08-02 16:18 UTC — the current `origin/main` image was deployed as Fly
  version 1887; `/healthz` was ready and the in-image SHA was
  `9907a3149434d723677cde7b8eaae1bc9400ba96`.
- 2026-08-02 16:23–16:31 UTC — the watchdog autonomously started the required
  compensating catch-up. Its first post-deploy cycle persisted a 151,624-byte
  record, regenerated one social draft, sent only a preproduction review
  message and reduced current missing confirmed drafts from 16 to 15. Target
  publication count remained zero.

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

- [x] Merge and deploy the fix from `origin/main`.
- [x] Complete and verify the compensating Region Talk catch-up.
- [x] Confirm source-profile capture and profile/writer backfill resumed under
  their normal fail-closed states without target publication. Remaining
  insufficient-capture/profile rows are product backlog, not a blocked
  transport.

## Release And Closure Evidence

- PR #232 reported `164 passed` for focused runner/orchestrator/scheduling
  tests and `818 passed` for the full Region Talk test family. Its real
  subprocess regression drains and parses a JSONL record above 300 KiB within
  the bounded timeout.
- The fix merge `ee0b1920dfad7980ea05421f02e60b5a1e89de17` is reachable from
  `origin/main`. Fly version 1887 runs later main SHA
  `9907a3149434d723677cde7b8eaae1bc9400ba96`, which contains that merge;
  `/app/.static-site-repo-sha` matched and `/healthz` returned
  `ok=true, ready=true`.
- Scheduled ops run `#5127` wrote three complete cycle lines. Its last cycle
  launched CandidateReport, ran the finalizer, and executed article/VK
  backfill while the child remained pollable; runtime `/proc` inspection found
  no `pipe_write` wait state.
- CandidateReport run
  `region-talk-orchestrator-candidate-report-20260802T162548Z` emitted live YDB
  progress through `alive` and terminal `report_written` (`event_seq=66`,
  `status=done`, 26 posts fetched, 4 scored, 8 sources scanned).
- Watchdog catch-up
  `watchdog-catchup-20260802T155000Z-49c4682175324f29bf0b53f724f1dbac`
  reread live YDB and persisted cycle 1. The draft worker reported
  `llm_budget_max=100`, produced one current ready draft and left the missing
  source-profile candidate fail closed. Live counts after that cycle were 18
  confirmed, 3 draft-ready, 15 draft-missing and zero target-published.
- External intake stayed review-gated; no intake appearance raised
  `manual_review_required`, and no target-channel publication occurred.

## Prevention

Wrapper liveness is insufficient evidence of orchestration progress. The
transport must continuously drain arbitrarily long bounded cycle records, and
closure requires observed post-fix cycle advancement plus durable YDB effects.
