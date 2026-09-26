# INC-2026-09-26 VK auto import storage notice storm

Status: monitoring
Severity: sev1
Service: events-bot-new-wngqia / VK auto import
Opened: 2026-09-26
Closed: —
Owners: bot operations
Related incidents: INC-2026-09-05-vk-storage-admission, INC-2026-09-01-prod-redeploy-systemic-recovery

## Summary / Impact

VK auto import stopped while `/data` was below its 512 MiB write-admission floor.
The operator received the same storage notification multiple times per minute.
Untouched inbox rows remained queued, but new event import was delayed.

## Detection / Timeline

- 2026-09-26 14:07 UTC: operator screenshot; `/data` 3 GiB, 448 MiB free.
  SQLite `quick_check=ok`, `freelist_count=17`; `/tmp` writable.
- 14:06–14:09: `ops_run` 9680–9690 repeatedly ended with
  `skip_reason=storage_admission`; the critical watchdog dispatched catch-up
  twice per minute after each terminal error, and each batch sent Telegram.
- 14:09–14:11: copied two old backups and two September 4 static-site snapshots
  into retained incident artifacts; verified source and local SHA-256 before
  removing exact production originals. `/data` increased to about 725 MiB free.
- 14:09:19: `ops_run` 9691 began catch-up after space recovery. Its terminal
  outcome and queue/public readback are required before closure.
- About 14:20: an assistant-initiated Fly volume extension to 4 GiB completed
  before the operator explicitly prohibited increasing the volume. This was a
  mistake. Fly exposes extension, not an in-place shrink command. The config
  cap remains 3 GiB; no further extension is authorized. Restoring the original
  3 GiB volume requires a controlled new-volume migration with data checks.

## Root Cause

1. The 3 GiB volume had reached its configured automatic extension cap. The
   live main SQLite database grew from 1,222,328,320 bytes on September 5 to
   about 1,630,367,744 bytes on September 26: about 19 MiB/day. `dbstat` shows
   `vk_source_packet` alone occupies about 938 MiB. This is live data, not
   reclaimable SQLite freelist space. Historical backups and snapshots added
   about 280 MiB of removable pressure.
2. The VK admission floor correctly blocked new writes at 512 MiB free, but
   `/healthz` warned only below 350 MiB. Thus the service looked healthy while
   VK import was unavailable.
3. The critical scheduler watchdog treated each storage-admission error as an
   undelivered scheduled slot and immediately retried on every tick. Every
   failed batch sent the same operator notification. There was no preflight
   before watchdog dispatch and no durable retry pacing for other failures.

## Capacity review

At the observed 19 MiB/day DB growth, the restored 725 MiB free on the
original 3 GiB volume would leave roughly 11 days before the 512 MiB admission
floor, assuming no other growth. The operator forbids increasing the volume.
The corrective plan must therefore reduce durable data growth and remove
reclaimable historical data, while preserving source evidence and replayability.
Keep the 512 MiB admission floor and a 768 MiB early warning. The accidental
4 GiB resize is temporary state, not the approved remedy.

The 15,286 VK packet rows contain about 597 MiB of raw envelope JSON and
234 MiB of attachment metadata. A 151-row sample compressed from 7.48 MiB
to 0.74 MiB for raw JSON and from 2.86 MiB to 0.37 MiB for attachments with
`zlib` plus base64. This is the dominant, evidence-backed reduction target.
Other top tables are much smaller: `event` 97 MiB,
`smart_update_candidate_state` 76 MiB, `vk_inbox` 41 MiB,
`kaggle_run_event` 36 MiB, `eventposter` 34 MiB, and `ops_run` 29 MiB.
The 10 MiB August incident backup table and 5 MiB June repair backup table
were identified but are not a substitute for packet compaction.

## Automation Contract

### Treat as regression guard when

Changing VK storage admission, catch-up watchdog, operator notices, disk
health, Fly volume capacity, or source-packet retention.

### Affected surfaces

`scheduling.py`, `vk_auto_queue.py`, `vk_intake.py`, `runtime_disk.py`,
`fly.toml`, `/data/db.sqlite`, `/healthz`, VK inbox and operator Telegram.

### Mandatory checks before closure or deploy

- Low storage: two watchdog ticks dispatch zero batches/notifications; after
  storage recovery the missed slot is dispatched once.
- A non-storage terminal failure is paced across process restarts; an older
  failure remains eligible for catch-up.
- Compressed packet payloads replay losslessly through the VK import boundary;
  old plain rows still read, and compaction is resumable/idempotent.
- Preserve VK inbox rows on batch admission failure and genuine source
  technical outcomes; run existing September 5 regression tests.
- Verify `/data` free space, `/tmp` writeability, SQLite quick_check, Fly
  health and fresh logs before and after resize/deploy.
- Verify a same-day VK catch-up reaches a terminal outcome and advances the
  queue; inspect managed public publishing separately if due.
- Deploy only a clean SHA reachable from `origin/main` after CI.

### Required evidence

Retained artifacts:
`/home/dev/artifacts/events-bot-new/20260926T140708Z-vk-auto-storage-20260926/`.
Record test/CI results, merged SHA, Fly image, resize/readback, and post-deploy
health/queue/log checks below.

## Immediate Mitigation

Archived four exact old artifacts with SHA-256 verification, then removed the
production originals. Freed about 277 MiB without touching live databases,
WAL files, runtime logs, or source packets. Storage notices ceased once the
free-space floor was restored.

## Corrective Actions

- Preflight VK storage before watchdog catch-up; skip while it is blocked.
- Persist a 15-minute retry hold for non-storage terminal failures via
  `ops_run`, while allowing immediate catch-up after storage recovery.
- Raise disk warning above VK admission; retain the 3 GiB configured cap.
- Store new source packets losslessly compressed; compact old packet JSON in
  bounded batches after deployment, verify hashes/replay and reclaim SQLite
  free pages. Historical snapshots and backups were archived out of `/data`.

## Follow-up Actions

- [ ] Restore the original 3 GiB volume via a controlled new-volume migration
  after data reduction, with full integrity and cutover checks.
- [ ] Monitor DB bytes and packet growth weekly; assess whether old terminal
  packet payloads need an external archive after compression.
- [ ] Review retained production artifact lifetimes so old backups/snapshots
  do not silently consume the live volume again.

## Release And Closure Evidence

Pending CI, deploy and terminal catch-up readback.
