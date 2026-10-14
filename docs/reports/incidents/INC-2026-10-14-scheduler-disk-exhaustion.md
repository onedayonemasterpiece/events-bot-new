# INC-2026-10-14-scheduler-disk-exhaustion Production disk full and missed schedules

Status: mitigated
Severity: sev2
Service: Events Bot and Kotopogoda on RunCoveer
Opened: 2026-10-14
Closed: —
Registry: `inc_895fcf531da1f27eb1f45305`
Related incidents: `INC-2026-10-03-fly-autostart-migration`, `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm`
Related docs: `docs/operations/cron.md`, `docs/operations/runtime-logs.md`, `deploy/runcoveer/README.md`

## Summary and user impact

The 24 GiB production root filesystem was full. Events' daily task had exited
with OperationalError; its last successful channel/day was October 11, despite
other scheduler tasks remaining alive. Today's 08:00 daily announcement was
missing. Kotopogoda's actual DB records a weather post at October 13 20:38 UTC,
against its stored 16:25 UTC weather slot. The owner's reported late post is
confirmed; the absent log window prevents attributing all four hours to one
individual external request.

## Detection and timeline (UTC)

- October 11 08:20: both persistent file mirrors stopped growing at their
  configured 1 GiB free-space admission floor. Existing logs were preserved.
- October 14 06:43: root disk 100%, Events unhealthy, daily task dead;
  Kotopogoda health answered but did not prove publication timeliness.
- 06:48: after removing unused superseded images, 1.5 GiB became available.
  Same-image Events restart restored the daily task and current-day catch-up.
- 06:49: both durable guards recorded October 14 as sent, two messages each;
  channel `last_daily` became October 14. No past-day catch-up was forced.

## Root cause and contributing factors

1. Video download retrieved every Kaggle output, including inherited source
   PNG frame trees. Failed/blocked sessions correctly bypassed published-only
   cleanup, but this retained about 3.3 GiB in the container: failed #1423
   1.3 GiB, blocked #1424 1.5 GiB and blocked #1425 465 MiB.
2. Historical image/source releases and an 871 MiB offline preflight DB also
   occupied the small root filesystem. The safe host maintenance pruned build
   cache, which was already zero, and could not reclaim these files.
3. One storage exception outside the per-channel daily send handler terminated
   the daily loop permanently. Health reported that exception correctly.
4. Kotopogoda ran serial weather/sea collection before checking due weather
   publications in the same loop. No collection deadline protected that cadence.
   Production source confirms this coupling; yesterday's per-request timings
   are unavailable, so it is a contributing mechanism, not a proven exclusive
   explanation of the whole delay.
5. File log rotation was bounded (Events168 MiB, Koto42 MiB). Paused mirror
   writes, rather than runaway log growth, explain the missing recent file logs.
   Docker stdout was available for the live investigation but not the entire
   previous-day window. The local capacity guard had failed since October 11
   without an external notification.
6. Events DB was approximately 881 MiB, quick_check=ok: raw VK packets199 MiB,
   events102 MiB, candidate state95 MiB, Kaggle event history42 MiB and ops32 MiB.
   Kotopogoda DB8.3 MiB, quick_check=ok. WAL was not the main capacity issue.
   Diagnostic heartbeat history lacked retention; canonical/source data is not
   disposable and must not be purged by a generic disk threshold.

This is mechanical storage/scheduling/transport work; no event/text semantics,
LLM model policy, canonical source proof or publication outcome is rewritten.

## Immediate mitigation and corrective actions

- Removed unused e679 image after verifying no container referenced it; dropped
  unused ownerack candidate tag. Active c4fa and qualified rollback6bb8 remain.
- Restarted only Events' existing image; today's native guards performed catch-up.
- Archived the old inactive preflight DB to private retained DevCoveer storage;
  verified all DB bytes against its source SHA before freeing its production copy.
- Provider-side output filtering excludes intermediate PNGs before transfer.
- Daily tick failures now log and retry next minute; CancelledError propagates.
- Kotopogoda deployment adapter separates the 30-second publication cadence
  from an owned cache refresh task with a120-second read-only collection deadline.
  Existing publication outcome/retry handling is preserved.
- Hourly bounded retention removes only redundant heartbeats older than30days
  from successful terminal Kaggle runs; terminal reports and all failed, partial,
  unknown and active histories remain. Existing lossless VK packet storage and
  deduplication remain unchanged. No blanket DB cleanup or unsafe WAL deletion.

## Automation contract

### Treat as regression guard when

Changing daily tick supervision, output downloads, temporary artifact cleanup,
Kaggle history retention, production image retention or Kotopogoda scheduling.

### Affected surfaces

`main_part2.py::daily_scheduler`, `video_announce/kaggle_client.py`,
`video_announce/poller.py`, `operational_retention.py`, `scheduling.py`,
`deploy/runcoveer/weather_scheduler.py`, deployed adapters and host capacity.

### Mandatory checks before closure or deploy

- Storage failure followed by a successful daily tick; cancellation propagates.
- Provider download pattern excludes inherited frame PNGs and retains final
  videos plus JSON/log/text diagnostic reports.
- Retention preserves terminal reports and failed/unknown/active runs; rerun safe.
- Slow weather collection does not block a due publication; shutdown joins it.
- Current-day public Telegram readback and durable sent guards; no duplicate.
- Exact-source clean main release and full tracked-source image verification.
- Live health, SQLite integrity, writable root scratch, active growing file
  mirrors and preserved rollback; free disk above the admission floors.

### Required evidence

Retained `/home/dev/artifacts/events-bot/20261014T064223Z-scheduler-delay-20261014`:
DB/queue probes, cleanup checksums/receipts, private offline preflight DB archive,
regression tests, public readback and release/live verification receipts.
The private snapshot contains production data and is mode0600 in a0700 directory;
ancillary credential files were excluded from the retained archive.

## Release and closure evidence

- Initial source tests:70passed; broader scheduler/claim/maintenance checks:
  105passed,1failed due to locally absent Telethon; repaired environment rerun:1passed.
- Same-image mitigation: Events c4fa01923bd9, readiness=true, all tasks alive,
  disk and scratch healthy, guards sent2 per channel forOctober14.
- Live human-session Telegram readback confirms the current-day announcements at
  `https://t.me/kenigevents/5270` and `https://t.me/keniggpt/3266`,06:48:55–56UTC.
  Yesterday's weather post `https://t.me/lovekenig/12882` is20:38:06UTC;
  the previous day's post12879 was16:25:30UTC, matching the existing slot.
- Final release/deploy readback is recorded in retained release receipts.

## Follow-up actions and prevention

- Verify the next natural morning/evening slots; same-day recovery does not
  prove a future slot. Preserve explicit uncertainty for yesterday's per-request
  Kotopogoda cause rather than inventing missing logs.
- Add owner-directed external capacity alerting separately: the current guard
  has local systemd/journal evidence, not an always-on external notification.
- Retain one qualified image rollback; large preflight copies and failed render
  evidence belong off the production root after checksum-verified preservation.
- Canonical event/raw-source history requires capacity planning or lossless
  archival, not arbitrary deletion of unresolved evidence or historical facts.
