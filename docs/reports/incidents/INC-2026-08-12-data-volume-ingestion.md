# INC-2026-08-12 `/data` exhaustion and ingestion degradation

Status: open
Severity: sev1
Service: Fly production `events-bot-new-wngqia`, configured-source ingestion, Kaggle monitors, StaticSiteBuilder
Opened: 2026-08-12
Closed: —
Owners: events-bot production / ingestion / static-site pipelines
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce`, `INC-2026-07-15-fly-volume-critical`, `INC-2026-07-18-static-snapshot-disk-pressure`, `INC-2026-07-19-static-builder-root-overlay-recurrence`, `INC-2026-07-19-static-site-stale-builder-lease`, `INC-2026-08-01-telegram-monitor-google-ai-package-closure`, `INC-2026-08-03-static-site-builder-failure-storm`, `INC-2026-08-10-smart-update-identity-terminal-loss`
Related docs: `docs/operations/runtime-logs.md`, `docs/operations/cron.md`, `docs/features/vk-auto-queue/README.md`, `docs/operations/kaggle-static-site-builder.md`

## Summary

The only production Machine became unroutable when the 3 GiB `/data` volume
reached zero free space. `/healthz` returned 503, SQLite writes raised
`OSError: [Errno 28] No space left on device`, Fly could not find a healthy
webhook candidate, and Private Events MCP became unavailable. Operator
containment removed only old rotated/runtime logs and used SQLite's supported
`PRAGMA wal_checkpoint(TRUNCATE)`; the WAL fell from about 342 MiB to zero and
roughly 283 MiB became available.

That containment addressed the terminal symptom, not the durable cause. The
new raw-first VK release persisted 4,695 full packet revisions. The
`vk_source_packet` table alone occupied 296,042,496 bytes and explained 96.82%
of the 305,762,304-byte DB increase since the 07:51 UTC predeploy snapshot. A
single 121-source crawl persisted 3,936 packets immediately before the first
retained 503. A stale terminal StaticSiteBuilder claim also protected about
947.9 MiB of exact snapshot/output, while the current process had reached a
WAL high-water above 337.84 MiB because checkpoint reuse was blocked or
starved. These allocations eliminated the operating margin.

Ingestion did not globally stop: 19 events were added on August 12 and the
official parsers created the last one at 13:17:40 UTC. After that, VK's fixed
15-row importer repeatedly selected the same oldest 2018 rows from a 4.6k
backlog, while the 21:40 UTC Telegram slot failed before scanning any of its 57
sources. Guide monitoring retried terminal callback failures hourly by design;
Telegram's watchdog lacked the same terminal-error hold and launched 26 extra
catch-ups.

## User / Business Impact

- the sole Machine temporarily left healthy routing; webhook delivery and MCP
  operations were unavailable;
- current VK carriers were starved behind historical replay rows;
- one Telegram daily obligation across 57 configured sources was missed;
- the current event yield of the missed Telegram scan and 4,687 nonterminal VK
  packet revisions remains unknown until controlled catch-up;
- StaticSiteBuilder freshness and event vector sync remained blocked by a
  terminal August 9 owner;
- repeated Guide/TG Kaggle pushes consumed remote submissions and dataset
  versions without producing source results.

## Detection

- operator evidence: `/data` 100%, real `Errno 28`, `/healthz` 503, Fly
  no-candidate proxy errors and a roughly 342 MiB WAL;
- retained runtime file: 503 at 11:36:04/19/34 UTC, then a 42,296-second mirror
  gap until health/webhook recovery at 23:21 UTC;
- read-only `dbstat`: `vk_source_packet=296,042,496` bytes;
- production `ops_run`, Kaggle output and registry correlation exposed the
  monitor retry counts and zero-source failures;
- current health at investigation time was only warning, with 280 MiB free—24
  MiB above the 256 MiB routing-critical floor—so recovery was not capacity
  stable.

## Timeline (UTC)

- 07:51 — predeploy release snapshot: DB 339,795,968 bytes, without the new VK
  packet table.
- 08:10 — Machine version 1970 started at exact `origin/main@69ec40342`.
- 08:15–08:26 — VK import still created five events.
- 10:00 — two VK retries hit duplicate successful-parse-key constraints.
- 11:00–11:36 — raw-first crawl scanned 3,943 posts and added 3,936 packet
  revisions; first retained health 503 followed at 11:36.
- 12:15–13:18 — official parsers processed 259 items across eight sources and
  created ten events; event 7604 at 13:17:40 was the last confirmed insert.
- 13:30 and 16:30 — VK auto-import selected the same 15 posts from 2018; all
  remained technical/evidence retries.
- 18:10–23:14 — one Guide full slot plus five hourly retries all failed before
  accepted callbacks.
- 21:40 — Telegram daily run failed before scanning: 0/57 sources.
- 22:13–23:21 — Telegram watchdog launched 26 additional failed catch-ups.
- 23:21 — after bounded log cleanup and supported WAL checkpoint, health,
  webhook and Private MCP traffic resumed in place.
- 23:29 — `/healthz` HTTP 200 and SQLite `quick_check=ok`, but `/data` had only
  280 MiB free and remained warning.
- 00:47 — durable growth crossed the routing floor again: `/data` had 254.39
  MiB available, Fly reported 1/1 critical, public health timed out, and the
  Private Events MCP OAuth request failed. No managed rotated log was eligible
  for deletion.
- 01:01 — version 1971, built from merged PR #496, retired only the exact
  terminal StaticSiteBuilder owner. Root/hash/claim validation and strict
  receipts removed 337,428,488 snapshot bytes and 610,450,066 output bytes
  before releasing its handoff/claim; no generic cleanup or DB/WAL deletion was
  used.
- 01:14 — deploy interruption left vector job 50186 as the sole running outbox
  row and its `ops_run` 5653 terminal `crashed`. The supported startup
  reconciliation primitive reclaimed that exact proven state; vector run 5654
  then completed with a v2 coverage-complete receipt.
- 01:43 — version 1973 started at exact merged main
  `0aa8f90c17f24bfad0e2215d5999e02153ef135d`. Its additional pre-allocation
  reserve guard prevents the previously observed loop that copied a 646.5 MiB
  snapshot before discovering that the post-copy build floor was unavailable.
- 01:51–02:40 — exactly one scheduled Telegram catch-up, `ops_run` 5655 / run
  `93807a7e890444b0b84fbf650c4a686c`, scanned all 57 sources and 84 messages.
  It found two messages with events, created/imported none, reported zero
  errors, wrote the terminal callback report, released the S22 lease and
  cleared its registry job/intent. No duplicate watchdog launch followed.
- 02:42–02:46 — bounded VK catch-up `ops_run` 5657 processed five carriers:
  one import updated existing event 7103, one rejection, three typed deferrals,
  zero failures and zero creations. Selection was four newest carriers then
  one historical carrier, proving both current progress and bounded history.
- 02:47–03:11 — the normal scheduled parser recovery entrypoint claimed
  `dramteatr` and `sobor`. `ops_run` 5659 processed 52 rows: `dramteatr` 28
  (25 updates, three retries) and `sobor` 24 (three new, 17 updates, four
  retries), with no failed rows or fatal error. Production DB inserts 7605–7607
  at 03:10–03:11 prove that live ingestion created three new events; the seven
  typed Smart Update retries remain durably due rather than being lost.
- 03:12–03:14 — current-event vector run 5663 completed for all 7,198 events;
  its v2 receipt is coverage-complete, has valid/equal corpus hashes and matches
  every static request revision.

## Root Cause

1. The raw-first VK rollout combined complete payload retention with legacy
   idle detection based on `cursor.updated_at` (last discovered post). Quiet
   sources therefore became "idle" every 24 hours even when `checked_at`
   proved a recent successful scan, causing broad historical replay. Full
   packet JSON/attachment evidence made that replay a 296 MiB durable DB jump.
2. SQLite WAL reuse reached a proven high-water above 337.84 MiB. Default
   auto-checkpoint was configured, so this scale means a reader/end-mark or
   equivalent checkpoint starvation prevented reset/reuse; after-the-fact
   evidence cannot name the exact reader.
3. A terminal cross-deploy StaticSiteBuilder handoff raised before exact-owner
   claim cleanup. Retention correctly protected its active identity, leaving a
   337 MiB snapshot and 610 MiB output unreclaimable and blocking vector sync.
4. No admission guard stopped the high-volume VK crawl at the warning floor.
   `/healthz` detected capacity loss but did not throttle the writer.

## Contributing Factors

- release snapshots, the old age-backfill DB copy and arbitrary backup families
  have no common owner/TTL/byte-budget contract;
- guide media retention had zero eligible candidates and reported
  `policy_satisfied=false`, but this did not escalate capacity risk;
- VK automatic selection hard-ordered publication time ascending, so thousands
  of historical raw carriers starved current ones;
- Guide/TG pushed a remote kernel before best-effort local registry save;
  ENOSPC could therefore leave a successful push untracked;
- corrupt registry reads silently became an empty registry;
- Guide had an hourly terminal-error hold but no daily attempt cap; Telegram
  had neither a terminal-error hold nor a durable pre-push reservation;
- Kaggle callback transport timeouts were surfaced as a false S22 "busy"
  outcome even though the production lease was released.

## Automation Contract

### Treat as regression guard when

- changing VK crawl/backfill/cursor selection, raw packet persistence or queue
  ordering;
- changing Kaggle push/registry/recovery/watchdog behavior for Guide or
  Telegram Monitoring;
- changing StaticSiteBuilder handoff recovery/retention;
- changing SQLite checkpoint policy, `/data` writers, health disk floors,
  backups or runtime retention;
- performing any production deploy or incident catch-up touching these paths.

### Affected surfaces

- `/data/db.sqlite*`, `vk_source_packet`, `vk_inbox`, `vk_crawl_cursor`;
- `/data/kaggle_jobs.json`, Guide/TG scheduler and S22 resource lease;
- `static_site_build_state`, job 50189, exact snapshot/output retention and
  `event_vector_sync`;
- `/healthz`, `/webhook`, Private Events MCP and runtime file mirror.

### Mandatory checks before closure or deploy

- before/after `df -h / /.fly-upper-layer /tmp /data`, top-level `/data` `du`,
  Fly volume/snapshot inventory; no unknown/canonical data deletion;
- `/tmp` create/write/fsync/remove probe and SQLite `PRAGMA quick_check=ok`;
- public `/healthz` HTTP 200 with ready/db/disk ok, Fly 1/1, webhook and live
  `/start`; Private Events MCP operation succeeds;
- free `/data` exceeds the 350 MiB warning floor with material margin and WAL
  remains bounded through a real write/crawl window;
- current raw VK carriers win under historical backlog without excluding or
  semantically SQL-rejecting history; controlled batch records scanned,
  candidates, created, duplicates, retry and no-event counts;
- Telegram uses only `TELEGRAM_AUTH_BUNDLE_S22`, runs exactly one controlled
  catch-up, reaches terminal done/released lease and nonzero source metrics;
- Guide no longer relaunches uncontrolled; registry/launch intent, callback
  ledger, terminal report and lease agree;
- terminal exact StaticSiteBuilder handoff is released through code, its exact
  snapshot/output are reclaimed, vector sync is unblocked, and any replacement
  canary uses exact merged SHA without production-root promotion;
- deployed SHA is clean, exact `origin/main` and recorded with immutable release
  evidence.

### Required evidence

- ignored investigation artifacts under
  `artifacts/codex/inc-disk-full-20260812/`;
- exact commit/PR/merge/deploy SHA and Fly release/version;
- incident-window `ops_run`, Kaggle ledger/registry/lease receipts and bounded
  catch-up counts;
- before/after disk/WAL/DB/health/log evidence and static exact-owner cleanup
  receipt.

## Immediate Mitigation

- removed only old rotated/runtime logs already identified by the operator;
- used `PRAGMA wal_checkpoint(TRUNCATE)` instead of deleting WAL/SHM;
- preserved the DB, current snapshots, raw packets and unknown evidence;
- deferred any new broad VK/TG/Guide catch-up while capacity remained within
  24 MiB of the critical floor.

## Corrective Actions

- [x] use `checked_at` rather than last-post `updated_at` for idle backfill
  admission;
- [x] block production VK crawl below 512 MiB and recheck before each source,
  page and packet transaction;
- [x] interleave bounded fresh-first drain with a durable oldest-row budget so
  current carriers progress without starving historical carriers;
- [x] add fsync/read-back pre-push Guide/TG launch intents, exact dataset-source
  reconciliation plus positive exact pre-push remote-revision evidence, atomic
  intent-to-job promotion, and fail registry parse/schema corruption closed;
- [x] anchor Telegram's one-hour terminal-error hold to `finished_at` (falling
  back to `started_at` only for legacy rows);
- [x] count a successful controlled manual Telegram catch-up in the missed-slot
  delivery window so registry cleanup cannot expose a duplicate watchdog run;
- [x] safely retire only exact terminal incompatible StaticSiteBuilder owners:
  root-confine and hash-validate the claim-bound snapshot pair, require strict
  snapshot/output deletion receipts while the claim remains active, then clear
  the handoff/claim;
- [x] reserve one current DB main-file size plus the static critical floor
  before allocating a replacement snapshot; the first post-recovery canary
  proved that a post-copy-only probe otherwise allocated 646.5 MiB, failed,
  deleted it and scheduled another retry;
- [x] merge/deploy exact runtime-bearing main and execute the bounded
  Telegram/VK/parser recovery plus live service verification.
- [ ] observe the seven durable parser retries through terminal scheduler
  receipts and complete the capacity-backed exact-main static canary.

## Recovery / Catch-up

Recovery followed the guarded order:

1. exact-owner static recovery reclaimed 947,878,554 bytes before any new
   build and left no active stale claim;
2. the interrupted vector owner was reconciled only after proving it was the
   sole running outbox row; run 5654 and the later post-ingestion run 5663 both
   produced complete receipts;
3. Guide was not relaunched: the already scheduled run 5651 had completed
   partial with valid callback/lease/report evidence (21 sources, 67 posts, one
   created, 16 updated, two errors), so another full run would have duplicated
   the daily obligation;
4. exactly one S22 Telegram scheduled catch-up ran to terminal success and
   released its lease; it scanned 57/57 sources and found no new canonical
   event to import;
5. the bounded five-row VK batch exercised the production Smart Update/dedupe
   path and advanced both fresh and historical work without duplicates;
6. the official parser recovery processed 52 rows and created events
   7605–7607. Seven item-level Smart Update retries remain represented by the
   two pending source recovery requests and will be claimed by the normal
   ledger-aware scheduler; no unbounded second catch-up was started.

## Release And Closure Evidence

The incident remains open because the stale static active guard is gone but a
fresh replacement build is capacity-deferred. The 646 MiB live DB plus the
350 MiB build floor requires about 1.64 GiB available before snapshot
allocation; production currently has about 1.145 GiB. The guard leaves job
50189 pending with no active claim and zero replacement snapshot bytes, so it
cannot recreate the ENOSPC loop. Closing the incident requires an owner-backed,
receipt-producing retention action that provides the remaining margin and a
successful exact-main static canary, plus terminal scheduler receipts that
account for all seven pending parser retries; increasing the volume or deleting
unknown files is not an accepted substitute.

Release chain:

- PR #496 merged as `866f3978e` (VK/Kaggle/static recovery and incident
  contracts), PR #497 merged as `7dc82ece2` (exact-one Telegram catch-up), and
  PR #498 merged as `0aa8f90c17f24bfad0e2215d5999e02153ef135d`
  (the runtime-bearing SHA deployed as Fly v1973 and an ancestor of current
  main). Evidence-only PR #499 merged as
  `cd0240bb2e09433f3b6b9f63413bdb526f3b828c`; follow-up PR #500 synchronized
  only the incident index, record and changelog. These documentation-only
  descendants leave production runtime-equivalent, and the deployed SHA is
  reachable from current main. All required GitHub checks were green;
- Fly version 1973 runs that exact SHA from a clean release worktree; the dirty
  unrelated root checkout was neither modified nor used for deploy;
- after catch-up, three consecutive public health calls returned HTTP 200 with
  `ready=true`, `db=ok`, `disk=ok`; Fly was 1/1 passing and a validation-only
  Kaggle callback returned the expected HTTP 400 contract response;
- `/data` had 1,200,791,552 bytes available, DB 646,381,568 bytes and WAL
  11,667,872 bytes after the real parser write window. Direct SQLite
  `quick_check=ok`; `/tmp` create/write/fsync/read/remove passed. A 20-minute
  sample before catch-up stayed near 1.157 GiB free, and retained logs since
  version 1973 contain zero ENOSPC/database-full/no-candidate signatures;
- runtime access evidence includes successful webhook POST 200 after recovery;
  live Telegram `/start` returned `Choose action`; Private Events MCP returned
  repository SHA `0aa8f90c17f...` and live counts. Its bounded 350 ms quick-check
  budget expired, while the direct read-only quick-check completed `ok`;
- no production DB, WAL/SHM, current release snapshot, raw VK packet or unknown
  artifact was deleted. The only automated data removal was the exact
  claim-bound terminal static snapshot/output described above.
- ignored receipts are under
  `artifacts/codex/inc-disk-full-20260812/final-live/`, notably
  `v1973-tg-terminal.json`, `v1973-vk-limit5-run.log`,
  `v1973-ingestion-post-catchup.json`,
  `v1973-static-vector-post-catchup-terminal.json` and
  `v1973-final-http-fly-logs.txt`; CI evidence is GitHub Actions run
  `31663528859` for PR #499, in addition to the green release PR checks.

Pre-release tests:

- exact `smart-update-identity-state-machine` local command: `585 passed`;
- focused Kaggle registry/client/Guide/TG/static recovery: `63 passed`;
- focused VK crawl/queue: `45 passed`; Telegram finished-time hold test passed;
- local root-overlay capacity itself became critical during the combined test
  run, so later tests used `/dev/shm` for isolated pytest temp. This is a local
  infrastructure constraint, not production evidence.

## Follow-up Actions

- [ ] storage owner — define owner/TTL/byte budgets for release snapshots,
  age-backfill/incident DB copies and generic backups; use that contract to
  reclaim enough receipt-backed space for the pending static canary.
- [ ] DB owner — expose WAL bytes plus checkpoint `(busy, log, checkpointed)`,
  wal-index backfill/read marks and oldest reader age.
- [ ] Kaggle owner — type callback transport failure as indeterminate, not
  resource busy, and add a bounded per-slot attempt cap across all launchers.
- [ ] ingestion owner — make manual add-event intake durable across restart and
  repair its timeout requeue contract.
- [ ] product owner — decide whether currently disabled ticket/festival sinks
  should be enabled and bounded or should stop accumulating carriers.

## Prevention

The storage guard acts before and during the high-volume writer, not after
ENOSPC. VK backfill uses scan activity plus explicit durable continuation state
rather than content activity, current and historical carriers both progress
through a replay wave, and remote pushes require a durable barrier that is
promoted only after exact remote identity. Terminal static resources are
root-confined, hash-checked and reclaimed before their active barrier is
released. Closure still requires live catch-up and capacity evidence; unit
tests or a green health check alone are insufficient.
