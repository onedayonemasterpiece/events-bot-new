# INC-2026-07-31-region-talk-deploy-interrupted-sessions Fly deploys interrupted autonomous sessions

Status: closed
Severity: sev2
Service: Region Talk scheduled discovery/orchestration
Opened: 2026-07-31
Closed: 2026-07-31
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-external-commerciality-regex`
Related docs: `docs/operations/cron.md`, `docs/features/region-talk-channel/orchestration-to-be.md`

## Summary

Fly machine replacements terminated the child Region Talk scheduled runner and
orchestrator. Startup cleanup correctly marked the durable `ops_run` rows
`crashed`, but the scheduler placed the next ordinary run on the following day
instead of resuming the latest due slot.

## User / Business Impact

- the 19:20 UTC production slot and its first manual compensation were both
  interrupted before their 90-minute bounds;
- CandidateReport/ImageDiagnostic remote work survived, but local finalizer,
  notification and publication-plan feedback could be delayed until the next
  day without an operator;
- two confirmed candidates remained undelivered until a second compensating
  run used the new role-scoped Telethon notifier.

## Detection

Production `/data/db.sqlite` showed `ops_run(kind=region_talk)` rows `4972` and
`4975` as `crashed` at the exact Fly release replacement times. The retained
JSONL files ended mid-loop, and no Region Talk process existed on the new
machine although scheduler health reported only tomorrow's next slot.

## Timeline

- 2026-07-31 19:20 UTC — ordinary evening slot started.
- 2026-07-31 19:38 UTC — a parallel Fly deploy replaced the machine and ended
  the slot.
- 2026-07-31 19:48 UTC — manual compensating 90-minute wrapper started.
- 2026-07-31 20:17 UTC — another parallel deploy ended the compensation;
  `ops_run=4975` became `crashed`.
- 2026-07-31 20:27 UTC — a second compensating wrapper started on `v1816`.
- 2026-07-31 20:30 UTC — Telethon delivery completed for two durable candidates;
  unsent count became zero and delivery-ledger count increased 25 to 27.
- 2026-07-31 20:34 UTC — the 14-day article/social anti-vector plan rebuilt
  successfully.
- 2026-07-31 20:50 UTC — clean manual Fly deploy installed the watchdog as
  release `v1818`; startup cleanup marked interrupted `ops_run=4977` crashed.
- 2026-07-31 20:56 UTC — the first independent watchdog tick resumed the
  19:20 UTC slot without operator action as running `ops_run=4984`, trigger
  `watchdog_catchup`.

## Root Cause

1. The long bounded runner was a child process of the single Fly machine and
   could not survive machine replacement.
2. APScheduler cron/misfire handling covers a missed trigger, not a job that
   started and then disappeared with the process.
3. Durable crash evidence existed, but no Region Talk-specific watchdog
   converted it into a recovery run.

## Contributing Factors

- several agents deployed concurrently during the same 90-minute slot;
- scheduler health showed the next cron time but not absence of catch-up
  coverage for an interrupted current-day slot.

## Automation Contract

### Treat as regression guard when

- changing Region Talk schedules, `ops_run` lifecycle, Fly release behavior,
  scheduled wrapper locking or scheduler health.

### Affected surfaces

- `scheduling.py` Region Talk registration/health/watchdog;
- `scripts/region_talk_scheduled_runner.py` trigger accounting;
- Fly single-machine release replacement;
- production SQLite `ops_run` and Region Talk runtime JSONL.

### Mandatory checks before closure or deploy

- latest due slot selection respects `REGION_TALK_TIMES_LOCAL` and timezone;
- a crashed/missing slot inside lookback dispatches one recovery;
- running/success slots do not dispatch;
- recovery uses the existing wrapper/file lock and a bounded attempt cap;
- health keeps the real next daily slot separate from watchdog cadence;
- a post-deploy live probe shows watchdog registration and healthy scheduler.

### Required evidence

- focused scheduling/runner tests;
- deployed SHA reachable from `origin/main`;
- remote code/health probe and post-deploy `ops_run` evidence.

## Immediate Mitigation

A supervised compensating wrapper restored the current-day chain. It delivered
the two pending candidates through idle `DISCOVERY2`, launched CandidateReport,
BGE and finalizer work, and rebuilt the anti-vector plan.

## Corrective Actions

- register an independent five-minute `region_talk_watchdog`;
- read the durable ledger for the latest due slot within a three-hour window;
- resume only missing/crashed/failed work and record `watchdog_catchup` trigger;
- fail closed for running/success rows, the existing file lock and six attempts.

## Follow-up Actions

- [x] Verify a deployed watchdog registration and health field.
- [x] Record the first natural or deploy-triggered `watchdog_catchup` production
  row; do not intentionally destroy useful work solely to create evidence.

## Release And Closure Evidence

- deployed SHA: `8b264739b8fca7416e4b46e02adc0efe2278519e`
  (`origin/main` ancestor at verification time)
- Fly release: `v1818`, image
  `deployment-01KYWZ0QB3E1JJXAWXVKD1Q8HY`
- deploy path: clean manual `flyctl deploy --remote-only`
- regression checks: 53 focused scheduling/runner tests passed; the combined
  scheduling/runner/notifier/orchestrator suite reached 172 passed before a
  post-suite background-thread shutdown hang, and the process was interrupted
  only after pytest had printed `100% passed`
- remote code/health: `/app/scheduling.py` contains the watchdog definition and
  registered job; `/healthz` reported `scheduler=ok`, `region_talk=ok`,
  `region_talk_watchdog=ok`, daily next run `2026-08-01T04:20:00+00:00`, and
  watchdog next run `2026-07-31T20:55:36.326352+00:00`
- post-deploy durable evidence: production `ops_run=4984` started at
  `2026-07-31 20:56:31`, status `running`, trigger `watchdog_catchup`, with
  scheduler run id
  `watchdog-catchup-20260731T192000Z-f626c925a35e412f8ba9ed49323d2aed`;
  the Region Talk orchestrator child was present and its retained JSONL path
  was created under `/data/runtime_logs/region_talk/`

## Prevention

The recovery decision is now durable-state-driven rather than dependent on one
agent remembering to start a compensating shell after every release.
