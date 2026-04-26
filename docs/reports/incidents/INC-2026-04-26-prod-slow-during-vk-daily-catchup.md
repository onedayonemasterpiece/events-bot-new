# INC-2026-04-26 Prod Slow During VK Daily Catch-Up

Status: closed
Severity: sev1
Service: Fly production runtime / VK daily catch-up
Opened: 2026-04-26
Closed: 2026-04-26
Owners: Codex
Related incidents: `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke`, `INC-2026-04-26-vk-daily-message-limit`
Related docs: `docs/operations/incident-management.md`, `docs/reports/incidents/README.md`, `docs/operations/cron.md`

## Summary

During compensation for `INC-2026-04-26-vk-daily-message-limit`, an ad-hoc `fly ssh console` command imported the full bot and attempted to run `send_daily_announcement_vk()` inside the serving machine. The process contended with the production bot enough that `/healthz` slowed and Fly reported `/webhook` load-balancing errors. The machine was restarted, serving recovered, and the VK daily catch-up was completed with a lighter two-step script.

## User / Business Impact

- The bot/webhook surface was degraded during the manual catch-up attempt.
- Fly logs showed `/webhook` proxy errors while the machine had no good healthy candidate.
- The intended VK daily catch-up was not completed by the heavy attempt and had to be rerun safely.

## Detection

- Detected by explicit `/healthz` checks during catch-up: `curl` timed out after 20 seconds.
- Fly logs showed slow health spans and `could not find a good candidate within 40 attempts at load balancing` for `/webhook`.

## Timeline

- 2026-04-26 08:54 UTC: manual full-bot catch-up attempt started.
- 2026-04-26 08:54-08:59 UTC: health checks became slow; `/webhook` proxy errors appeared.
- 2026-04-26 09:01 UTC: machine restart initiated to restore serving.
- 2026-04-26 09:05 UTC: bot booted and `/webhook` accepted updates again.
- 2026-04-26 09:10 UTC: VK daily catch-up completed through a safer lightweight posting script.

## Root Cause

1. The compensating rerun reused the full application import and `send_daily_announcement_vk()` path inside the only serving Fly machine.
2. That path loaded the full runtime and executed heavy daily-building work next to the production webhook server.
3. The serving machine has limited memory/CPU headroom, so the ad-hoc recovery process made health checks and webhooks slow.

## Contributing Factors

- There was no dedicated lightweight catch-up command for VK daily posts.
- The previous production-unresponsive incident contract warned against unsafe production smoke, but the manual catch-up still used the serving machine too heavily.

## Automation Contract

### Treat as regression guard when

- Running manual production validation, catch-up, or smoke that imports the full bot on the serving Fly machine.
- Changing VK daily recovery/runbooks.
- Adding new production smoke commands for scheduled surfaces.

### Affected surfaces

- Fly production runtime
- `/healthz`
- `/webhook`
- VK daily catch-up procedure
- `docs/operations/cron.md`

### Mandatory checks before closure or deploy

- Confirm `/healthz` returns quickly after mitigation.
- Confirm Fly machine has passing health checks.
- Confirm `/webhook` errors stop after mitigation.
- Complete the original scheduled-surface compensation through a safer path or explicitly leave it open.

### Required evidence

- Fly health/status evidence.
- Log evidence for detection and recovery.
- Evidence that the interrupted compensation did not leave the scheduled slot unrecovered.

## Immediate Mitigation

- Restarted machine `48e42d5b714228`.
- Avoided repeating the full `send_daily_announcement_vk()` call in a second heavy process.
- Generated VK daily chunks separately and posted them with a lightweight direct VK API script.

## Corrective Actions

- Documented the incident and added a follow-up to create a safer catch-up/runbook for heavy daily builders.

## Follow-up Actions

- [ ] Create a lightweight VK daily catch-up tool or admin command that can publish pre-split chunks without importing the full bot inside the serving machine.
- [ ] Add a runbook note: if `/healthz` slows or `/webhook` errors appear during validation/catch-up, stop the validation and restore serving first.

## Release And Closure Evidence

- deployed SHA: `07b311409783bfee69865456df7cc7a448e2b48f`
- deploy path: no additional code deploy for this secondary incident; recovery used machine restart and safer catch-up procedure
- regression checks: `/healthz` returned `ok=true`, Fly status showed machine version `1005` started with `1 total, 1 passing`
- post-deploy verification: `/webhook` accepted updates after recovery; original VK daily compensation finished with four VK post URLs and `vk_last_today=2026-04-26`

## Prevention

- Treat production catch-up scripts as production load, not harmless diagnostics.
- Prefer offline artifact generation plus lightweight publish/update steps when compensating a missed scheduled publication from the serving machine.
