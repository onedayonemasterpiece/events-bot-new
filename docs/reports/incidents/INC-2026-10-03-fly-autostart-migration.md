# INC-2026-10-03-fly-autostart-migration Legacy Fly writers restarted during migration

Status: mitigated
Severity: sev2
Service: Events Bot / Kotopogoda runtime migration
Opened: 2026-10-03
Closed: —
Registry: `inc_046b7b765c94cba6b4571359`

## Summary and impact

Machines stopped via CLI were reactivated by Fly proxy traffic to existing
webhooks. New runtime was in preflight (no schedules/workers/webhook
registration), but a direct Events `/start` smoke received duplicate responses.
The Oct 2 preserved SQLite state was therefore insufficient for final cutover:
old Events had 9,121 events versus 9,102 in the snapshot. No application jobs
were enabled on the new host while the old writer was active.

## Timeline (UTC)

- 08:36: direct preflight webhook /start yielded duplicate replies.
- 08:37: Fly machine state confirmed both legacy machines started.
- 08:38: Events autostart disabled; old command changed to sleep infinity for
  quiescent data capture. Kotopogoda main.py process explicitly quiesced.
- 08:41: final SQLite checkpoint/count/hash evidence collected.
- 08:44: both machines cordoned, blocking proxy routing. Kotopogoda config
  update could not be applied: old private GHCR tag denied (403), digest
  manifest unavailable (404). Do not describe that failed update as mitigation.
- 08:44: two in-flight transfers were interrupted by an early machine stop.
  Partial files retained and resumed by byte offset; exact SHA-256 verification
  is mandatory before restoration. Events restarted only as sleep infinity,
  still cordoned, for completion of those reads.

## Root cause and contributing factors

Stopping a Fly Machine does not disable proxy autostart. Existing webhook
origins and monitoring traffic can wake it. The original migration bundles were
valid but not final cutover snapshots. The old Kotopogoda image can no longer
be resolved from its upstream registry, so updating autostart config alone was
not a usable mitigation. This is mechanical transport/runtime behavior, not
semantic event processing; no LLM/content policy was changed.

## Mitigation

Cordon both machines, quiesce old application writers, checkpoint/fetch final
changed SQLite files and changed non-SQLite state only. Keep new runtime in
preflight until snapshot checksums, SQLite integrity and key counts pass. Then
stop old machines, switch webhooks and activate exactly one new scheduler.

## Automation contract

### Treat as regression guard when

Stopping Fly workloads, host migration, writer cutover or rollback.

### Affected surfaces

Fly proxy services/autostart, Telegram webhooks, SQLite production state,
background schedulers, deployment preflight and rollback runbook.

### Mandatory checks before closure or deploy

- Exact checksums and SQLite quick_check after final transfer.
- New data counts include all 9,121 final Events records and 154 Koto assets.
- Old-origin HTTP request does not reactivate a writer after cordon/stop.
- New direct/public health and real Telegram smoke succeed after cutover.
- Reboot, Docker restart, repeat deploy preserve data and restore services.

### Required evidence

Retained `/home/dev/artifacts/runcoveer/20261003T081708Z-production-migration`:
final snapshots, delta archive, E2E readbacks, health and persistence results.
Private snapshots must remain mode 0600 under the managed private directory.

## Release and follow-up

Application source preserved: Events main `8105d069f`; Kotopogoda exact runtime
snapshot `951065bf`. Deployment artifacts are in `deploy/runcoveer`.
Rollback must stop the new writer before uncordoning/starting Fly; restore
Events command to python main.py. Preserve latest writes before any rollback.
Unrelated historical VK/Google/TG-monitor incidents remain open.
