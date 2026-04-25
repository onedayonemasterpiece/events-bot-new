# INC-2026-04-25 Prod Bot Unresponsive After Telegram Monitoring Smoke

Status: monitoring
Severity: sev0
Service: production Telegram bot / Fly runtime
Opened: 2026-04-25
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm`, `INC-2026-04-16-prod-disk-pressure-runtime-logs`, `INC-2026-04-10-tg-monitoring-festival-bool`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/cron.md`, `docs/features/telegram-monitoring/README.md`

## Summary

During the Telegram Monitoring Gemma 4 post-deploy validation on April 25, 2026, the production bot stopped responding to user-visible Telegram commands including `/start`. External `/healthz` requests also hung, and Fly proxy logs later showed `/webhook` delivery failures because the instance was not accepting application HTTP traffic. The immediate mitigation was a Fly machine restart, after which `/healthz` returned ready and webhook updates were processed again.

## User / Business Impact

- Telegram users could not reliably interact with the production bot.
- `/start` did not receive a response during the outage window.
- Telegram webhook delivery failed while the app listener was unavailable.
- Scheduled/background jobs showed missed/skipped runtime symptoms during the same window.

## Detection

- Detected from the operator/user report that `/start` was not responding.
- Confirmed by repeated local `/healthz` timeouts before restart.
- Confirmed in Fly logs by proxy errors for `/webhook`: `instance refused connection` and `could not find a good candidate`.
- Runtime file mirror was checked after the issue was raised: production had `ENABLE_RUNTIME_FILE_LOGGING=0`, `/data/runtime_logs` existed but contained no files, so this incident relies on Fly logs, Kaggle output, production state, and local `artifacts/codex/` captures.

## Timeline

- 2026-04-25 18:52 UTC: manual production post-deploy Telegram Monitoring smoke `run_id=prod_g4_postdeploy_20260425b` created `ops_run id=857` and launched/polled Kaggle.
- 2026-04-25 20:28 UTC: Kaggle producer completed and saved `telegram_results.json` with 45 sources, 118 messages, 39 messages with events, and 61 extracted events.
- 2026-04-25 20:39 UTC: last pre-restart scheduler log line observed in Fly buffer before the runtime stopped producing normal app logs.
- 2026-04-25 20:51 UTC: manual Fly restart initiated after repeated `/healthz` hangs.
- 2026-04-25 20:51-20:54 UTC: Fly proxy reported `instance refused connection` and `/webhook` delivery failures while the app was not yet listening.
- 2026-04-25 20:55 UTC: app logged `BOOT_OK`; `/healthz` returned `ok=true`, `ready=true`, and webhook updates resumed.
- 2026-04-25 20:55 UTC: user-visible update handling resumed; Fly logs show an update handled in 638 ms.

## Root Cause

Open / partially isolated. Current evidence shows the production app process became unresponsive while a manual Telegram Monitoring production smoke and normal scheduled/background jobs were active on the single Fly machine. The likely failure family is runtime starvation or event-loop/process blockage from production-bound heavy operations and blocking network/database work, but the exact blocking call is not yet proven.

One concrete infrastructure gap was confirmed: local `fly.toml` used `[[services.checks]]`, but `flyctl config show --app events-bot-new-wngqia` showed no deployed service checks at all. That means Fly had no active service-level `/healthz` check in the applied production config when the listener stopped responding.

## Contributing Factors

- The manual post-deploy smoke was launched on the production Fly machine instead of being isolated from the serving process.
- The production machine has one shared app process group and limited CPU/memory headroom.
- The applied Fly service config did not include `/healthz` service checks, despite local `fly.toml` having a health-check-looking block.
- Runtime file logging mirror was disabled, so high-fidelity local stack/trace evidence for the pre-restart hang was unavailable.
- The agent did not immediately escalate the unresponsive production health signal into incident mode.

## Automation Contract

### Treat as regression guard when

- changing production smoke/validation workflows;
- running Telegram Monitoring, Kaggle recovery, or import jobs from the production Fly machine;
- changing `/healthz`, webhook serving, scheduler heavy-job gating, or startup/recovery behavior;
- changing runtime logging or incident evidence collection.

### Affected surfaces

- Fly production runtime: `events-bot-new-wngqia`, machine `48e42d5b714228`;
- Telegram webhook and `/start` command handling;
- Telegram Monitoring post-deploy smoke path and Kaggle recovery/import path;
- scheduler heavy jobs and runtime health checks;
- runtime logs / evidence collection.

### Mandatory checks before closure or deploy

- `/healthz` returns `ok=true`, `ready=true`, `issues=[]` after mitigation and remains stable after the recovery/import path finishes.
- `flyctl config show --app events-bot-new-wngqia` shows an active service-level `services.http_checks` entry for `GET /healthz`.
- Fly logs show `/webhook` requests receiving HTTP 200 after mitigation.
- A user-visible Telegram command such as `/start` is handled after mitigation, either via live E2E or operator-confirmed UI plus Fly `aiogram.event ... is handled` evidence.
- Production `ops_run` rows for the smoke/recovery path reach terminal status or are explicitly cleaned up; no stale `running` row should remain from the failed validation.
- Runtime log workflow evidence is recorded: file mirror checked first, then fallbacks used when disabled/empty.
- If code/config changes are needed, they must be committed to `origin/main` and deployed with release-governance evidence.

### Required evidence

- deployed SHA and Fly image;
- `/healthz` response after mitigation;
- Fly logs for outage and restoration;
- `ops_run` status for `prod_g4_postdeploy_20260425b`;
- Kaggle `telegram_results.json`/log artifact for the post-deploy run;
- confirmation that runtime file mirror state was checked.

## Immediate Mitigation

- Restarted Fly machine `48e42d5b714228`.
- Verified `/healthz` returned ready after restart.
- Verified Fly logs showed webhook delivery resumed and at least one update was handled.
- Corrected production health-check config from non-applied `services.checks` syntax to `services.http_checks`.

## Corrective Actions

- Added runtime-log investigation workflow to `docs/operations/runtime-logs.md` and `AGENTS.md` so agents must check file mirror/rotated logs before claiming logs are unavailable.
- Opened this incident record as the regression contract for the production unresponsive bot event.
- Updated `fly.toml` to deploy a real service-level `GET /healthz` check with `interval=15s`, `timeout=5s`, and `grace_period=60s`.

## Follow-up Actions

- [ ] Prove the exact blocking source from production evidence or add instrumentation that makes the next occurrence diagnosable.
- [ ] Move post-deploy Telegram Monitoring validation off the serving Fly process or add an explicit production-safe runner contract.
- [ ] Add a release/validation rule: if `/healthz` or `/webhook` is unresponsive, create an incident immediately before continuing unrelated validation.
- [ ] Consider bounded runtime stack dump or heartbeat diagnostics for event-loop stalls while keeping disk-pressure constraints from `INC-2026-04-16-prod-disk-pressure-runtime-logs`.

## Release And Closure Evidence

- deployed SHA: `36d43e412b2c4205fb16f24211987c02364e725f` at incident record creation; Telegram Monitoring code deploy SHA was `4ec6017fc0606150b254fedb7face8be5ef2e275`.
- deploy path: Fly manual deploy before incident; docs-only push to `origin/main` after mitigation.
- regression checks: pending final `ops_run` verification, deployed health-check config verification, and post-recovery health stability check.
- post-deploy verification: `/healthz` ready after restart; webhook handling resumed.

## Prevention

- This record must be consulted for future production smoke/deploy work that touches Telegram Monitoring, Kaggle recovery, runtime health, or log collection.
