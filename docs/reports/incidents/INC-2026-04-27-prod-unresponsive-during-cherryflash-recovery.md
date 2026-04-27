# INC-2026-04-27 Prod Unresponsive During CherryFlash Recovery

Status: open
Severity: sev1
Service: Fly production bot runtime / Telegram webhook
Opened: 2026-04-27
Closed: —
Owners: Codex / events bot operator
Related incidents: `INC-2026-04-27-cherryflash-missing-photo-urls`, `INC-2026-04-26-prod-slow-during-vk-daily-catchup`, `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke`
Related docs: `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`, `docs/features/cherryflash/README.md`

## Summary

During same-day CherryFlash incident recovery on 2026-04-27, the production bot became slow or unavailable for basic Telegram interaction. The operator reported that `/start` did not receive a response, while production health checks and Fly proxy/webhook traffic showed degraded serving.

## User / Business Impact

- Basic bot commands were not reliably served during a customer-visible daily publication incident.
- CherryFlash recovery validation could not be trusted while the serving runtime was unhealthy.
- The incident increased time-to-recovery because publication catch-up and bot health restoration had to be handled together.

## Detection

- Operator reported that `/start` did not respond during CherryFlash recovery.
- Production health endpoint checks timed out before the machine was restarted.
- Fly logs showed slow health/webhook handling during the affected window.
- Runtime file mirror was checked as required by `docs/operations/runtime-logs.md`; production policy keeps `ENABLE_RUNTIME_FILE_LOGGING=0`, so fallback evidence comes from Fly logs, `/healthz`, process state, Kaggle logs, and production DB rows.

## Timeline

- 2026-04-27 Europe/Kaliningrad: CherryFlash missed the scheduled daily slot and required production catch-up.
- 2026-04-27 15:06 Europe/Kaliningrad: a CherryFlash session was started while the earlier publication issue was still under investigation.
- 2026-04-27 16:08 Europe/Kaliningrad: operator sent `/start` and reported that the bot did not respond.
- 2026-04-27: `/healthz` checks timed out and Fly reported a critical health state.
- 2026-04-27: Fly restarted the production machine.
- 2026-04-27: after restart, `/healthz` returned `ok=true`, `ready=true`, `db=ok`, and no active `run_cherryflash_live` process was found.

## Root Cause

1. Production serving and incident recovery work shared the same Fly machine.
2. During CherryFlash recovery, long-running or blocked runtime work caused health/webhook handling to exceed acceptable latency.
3. Validation continued into story/Kaggle recovery while production serving was not yet proven healthy, making evidence collection noisy and increasing operator-visible impact.

## Contributing Factors

- Runtime file mirror is disabled by current production policy, so detailed per-request serving evidence is limited unless Fly logs still retain the window.
- Manual recovery commands can overlap with scheduler and webhook work unless explicitly health-gated.
- CherryFlash recovery has long remote waits and story fanout delays; this makes it tempting to leave validation running while serving health drifts.

## Automation Contract

### Treat as regression guard when

- Running any production CherryFlash catch-up, scheduled slot recovery, or long-running production smoke on the Fly serving machine.
- Changing live-run scripts, scheduler recovery, or post-deploy validation that can occupy the production runtime.
- Investigating `/healthz` timeout, `/webhook` proxy errors, or missing response to `/start`.

### Affected surfaces

- Fly app `events-bot-new-wngqia`
- `/healthz`
- Telegram webhook and `/start`
- CherryFlash live/catch-up runner
- Scheduler jobs that can run during recovery
- Runtime log mirror and fallback evidence paths

### Mandatory checks before closure or deploy

- Check production runtime file mirror env and `/data/runtime_logs` before claiming logs are unavailable.
- Verify `/healthz` responds within the configured timeout before starting any compensating CherryFlash rerun.
- Verify the bot process is not already running a conflicting CherryFlash recovery command.
- During long catch-up, stop or restart validation if `/healthz` or webhook handling degrades.
- After deploy/restart, re-check `/healthz` and basic webhook readiness before declaring CherryFlash evidence valid.
- Release-governance checks: clean worktree, deployed SHA recorded, and production fix reachable from `origin/main`.

### Required evidence

- Runtime file mirror state or fallback reason.
- Fly health evidence before and after restart/deploy.
- Process evidence showing no duplicate local CherryFlash live runner is active before rerun.
- Post-deploy `/healthz` response.
- CherryFlash catch-up evidence collected only while production serving is healthy.

## Immediate Mitigation

- Restart the Fly production machine when health checks timed out.
- Confirm `/healthz` recovered before continuing with code fixes and a later catch-up rerun.
- Confirm no stale `run_cherryflash_live` process remained after the interrupted attempt.

## Corrective Actions

- Formalized this production availability event as a regression contract tied to CherryFlash recovery.
- CherryFlash same-day recovery now treats serving health as part of closure evidence, not as unrelated background noise.

## Follow-up Actions

- [ ] Add a documented lightweight CherryFlash catch-up runbook step that health-checks the serving machine before and during the long story fanout window.
- [ ] Consider a dedicated worker/runtime split for long video recovery tasks so production webhooks are not competing with catch-up validation.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- This incident record must be raised whenever CherryFlash recovery, long production smoke, or scheduler catch-up coincides with `/healthz`, webhook, or `/start` degradation.
