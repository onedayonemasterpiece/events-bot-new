# INC-2026-09-26 VK auto import storage notice storm

Status: resolved; capacity trend monitoring continues
Severity: sev1
Service: events-bot-new-wngqia / VK auto import
Opened: 2026-09-26
Closed: 2026-09-26 15:05 UTC
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
- 14:09:19–14:39:20: `ops_run` 9691 processed 25 inbox rows after space
  recovery: 4 imported, 16 rejected, 5 source-specific technical outcomes,
  0 unresolved. No storage-admission failure recurred.
- About 14:20: an assistant-initiated Fly volume extension to 4 GiB completed
  before the operator explicitly prohibited increasing the volume. This was a
  mistake. The physical size was restored to 3 GiB by a controlled migration
  to a new volume; the old 4 GiB volume and frozen machine were destroyed only
  after integrity and health checks. The configured cap remained 3 GiB.
- 14:57–15:05: sole app machine started with the released image on the new
  3 GiB volume. External `/healthz` was ready with no issues and about
  1,434 MiB free. A bounded VK import smoke run reached a terminal success
  with no storage or technical errors; no fresh storage notice was found.

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

At the pre-fix 19 MiB/day DB growth, clearing old files alone would have left
roughly 11 days before the 512 MiB admission floor. The operator forbids
increasing the volume. The fix therefore reduced durable packet storage while
preserving source evidence and replayability. The production volume is again
3 GiB, the 512 MiB admission floor remains, and the warning is now 768 MiB.

The 15,286 VK packet rows contain about 597 MiB of raw envelope JSON and
234 MiB of attachment metadata. A 151-row sample compressed from 7.48 MiB
to 0.74 MiB for raw JSON and from 2.86 MiB to 0.37 MiB for attachments with
`zlib` plus base64. This is the dominant, evidence-backed reduction target.
Other top tables are much smaller: `event` 97 MiB,
`smart_update_candidate_state` 76 MiB, `vk_inbox` 41 MiB,
`kaggle_run_event` 36 MiB, `eventposter` 34 MiB, and `ops_run` 29 MiB.
The 387 historical `codex_backup_` and `incident_` tables occupy about
51.7 MiB in total. They warrant a reviewed lifecycle, but deleting them is
not a substitute for packet compaction. The private auth database is about
122 MiB, including a roughly 59 MiB social-provider-binding table, and is
not the main growth driver. Guide media, guide results, runtime logs and
parsing debug remain capped at 256, 128, 64 and 16 MiB respectively; no
raised storage budget was found in the release diff.

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
  health and fresh logs before and after volume migration/deploy.
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

- [x] Restore 3 GiB capacity via a controlled new-volume migration after data
  reduction, with full integrity and cutover checks.
- [ ] Monitor DB bytes and packet growth weekly; assess whether old terminal
  packet payloads need an external archive after compression.
- [ ] Review retained production artifact lifetimes so old backups/snapshots
  do not silently consume the live volume again.

## Release And Closure Evidence

PR #675 merged as `b754a4ea6f0f302689de9f6374700d65485bd929` after all
three CI jobs passed. The exact main SHA was deployed through
`scripts/deploy_fly_main.sh --remote-only` as image
`deployment-01M3F2BBC9JETRE3RR3WCHF9H8`. Focused local checks passed:
130 scheduler/storage tests, 82 continuation/import/census/compression tests,
and 4 packet-boundary tests.

Compaction encoded 14,789 historical packet rows, saving 769,236,534 logical
bytes across raw payloads and attachment metadata. All 15,289 packet rows
matched a pre-compaction SQLite backup byte-for-byte after decoding. `VACUUM`
and WAL checkpoint reduced the main DB from 1,631,092,736 to 805,990,400
bytes; `quick_check=ok`, freelist zero.

The sole current volume is `vol_491x2m01q7589xor` at 3 GiB, attached to the
sole started app machine `148eddde9b5778` (1/1 Fly checks, correct image).
Both migrated SQLite databases passed `quick_check=ok`; main DB event, packet,
inbox and ops-run row counts matched the frozen source. The old 4 GiB volume
`vol_4m83jjyewxmjn6gr` and its machine were destroyed. External `/healthz`
returned `ready=true`, `issues=[]`, about 1,434 MiB free, warning floor
768 MiB. Same-day catch-up run 9691 ended without unresolved inbox rows.
Post-cutover bounded run 9696 processed one inbox row and ended successfully
with zero technical/storage errors. Timestamped runtime logs after cutover
contained no new storage-admission, storage-notice, `Errno 28`, or SQLite
disk-full errors. Full non-secret evidence and restricted migration archives
are in the retained incident artifact directory above.

The storage recovery does not resolve the separate media-review gate. Eight
future `vk_sync` jobs still report `vk_sync_missing_materialized_media`; seven
have no stored URL for the managed Afisha group, including event 8934 on
September 27. Some have URLs from their source groups, which are not managed
Afisha publication evidence. These
jobs have retry times and require their own publication/media follow-up. This
is also tracked in `INC-2026-09-24-vk-afisha-publication-recurrence`.
