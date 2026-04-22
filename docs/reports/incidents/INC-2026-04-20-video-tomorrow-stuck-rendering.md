# INC-2026-04-20-video-tomorrow-stuck-rendering Scheduled `/v tomorrow` Crashed And Left Session Stuck In `RENDERING`

Status: open
Severity: sev2
Service: production bot / scheduled `video_tomorrow` (`/v tomorrow`) on Fly app `events-bot-new-wngqia`
Opened: 2026-04-20
Closed: —
Owners: video announce / production runtime / operations
Related incidents: `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm.md`, `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`, `INC-2026-04-10-crumple-story-prod-drift.md`
Related docs: `docs/features/crumple-video/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The scheduled production `/v tomorrow` run for April 20, 2026 created session `#176` with kernel `local:CrumpleVideo`, told operators in Telegram that it was "launching Kaggle", and then the run crashed while the session remained stuck in `RENDERING`. After restart, recovery treated the repo-local ref like a real Kaggle slug and began impossible `kernels_status` polling against `local:CrumpleVideo`, which kept the incident looking alive instead of clearly failed/rerun-required.

## User / Business Impact

- the daily video announcement slot for Tuesday, April 21, 2026 did not complete normally;
- operator chat reported "запускаю Kaggle…" even though the run had not safely reached a confirmed remote-render handoff;
- the scheduler recorded the scheduled run as `crashed`, while the live session stayed stuck in `RENDERING`, leaving recovery ambiguous and increasing the risk of losing the day without manual intervention.

## Detection

- operator report in Telegram on April 20, 2026 noted that session `#176` looked hung after "запускаю Kaggle…";
- Fly stdout logs showed the runtime later rebooting on the same machine;
- direct production sqlite inspection showed `ops_run.kind='video_tomorrow'` as `crashed` but `videoannounce_session.id=176` still `RENDERING`;
- post-restart Fly logs showed Kaggle polling attempts against `local:CrumpleVideo` returning `403 / Cannot access kernel`;
- observability gap: current production policy keeps runtime file-log mirroring disabled (`ENABLE_RUNTIME_FILE_LOGGING=0`), so post-factum evidence came from Fly logs plus direct `/data/db.sqlite` inspection rather than fresh `/data/runtime_logs` files.

## Timeline

- 2026-04-20 15:10:50 UTC: `videoannounce_session.id=176` created on prod.
- 2026-04-20 15:10:53 UTC: session `#176` moved to `RENDERING`; kernel ref still `local:CrumpleVideo`.
- 2026-04-20 17:11 Europe/Kaliningrad: operator-facing Telegram message reported "Сессия #176: выбрано 12 событий, запускаю Kaggle…".
- 2026-04-20 15:26:25 UTC: Fly stdout shows machine reboot (`reboot: Restarting system`).
- 2026-04-20 15:29:11 UTC: `ops_run.id=760` for `video_tomorrow` finished with status `crashed`.
- 2026-04-20 15:29:55 UTC: direct prod inspection still showed session `#176` stuck in `RENDERING`, `kaggle_dataset=NULL`, `error=NULL`.
- 2026-04-20 15:30:43 UTC: restarted runtime began polling Kaggle for `local:CrumpleVideo` and received `403 / Permission 'kernels.get' was denied`.

## Root Cause

1. The scheduled `/v tomorrow` path marks the session as `RENDERING` before the render handoff is durably completed, so a crash/restart can orphan the session in a non-terminal state.
2. Startup recovery resumed Kaggle polling for the repo-local pseudo-ref `local:CrumpleVideo`, even though that ref is only valid before the real Kaggle slug is persisted.
3. The operator-facing progress message says "запускаю Kaggle…" before `start_render()` finishes the heavy pre-render phase, so the chat surface overstated pipeline progress during the incident.

## Contributing Factors

- runtime file-log mirroring is intentionally disabled on prod after the April 16, 2026 disk-pressure incident, reducing the amount of retained evidence for this incident;
- the exact low-level trigger that rebooted the runtime before Kaggle handoff was not captured in `ops_run.details_json`, so sqlite state alone could not explain the crash.

## Automation Contract

### Treat as regression guard when

- changing scheduled `/v tomorrow` dispatch, run lifecycle, or recovery logic;
- changing `video_announce/scenario.py` session state transitions or operator notifications;
- changing runtime health / reboot / watchdog handling for daily video jobs;
- changing prod logging policy or the evidence available for post-factum incident analysis.

### Affected surfaces

- `video_announce/scenario.py`
- `video_announce/poller.py`
- `scheduling.py`
- `main.py` / runtime task supervision
- `fly.toml`
- Fly prod machine lifecycle and `/data/db.sqlite`

### Mandatory checks before closure or deploy

- reproduce or otherwise explain why session `#176` stayed `RENDERING` after `ops_run` became `crashed`;
- verify operator progress messages match the actual render lifecycle;
- verify restart recovery never polls Kaggle against `local:*` refs;
- verify scheduled `/v tomorrow` either reaches a terminal session state or is explicitly reset/recovered after crash/restart;
- verify prod evidence sources used for the incident (`Fly logs`, sqlite state, and any temporary runtime logs) are captured in the record;
- perform release-governance checks and confirm any prod fix is reachable from `origin/main`;
- if the April 20, 2026 slot was missed, perform compensating rerun/catch-up and verify today is restored.

### Required evidence

- deployed SHA:
- tests / smoke covering the recovered behavior:
- prod log / sqlite evidence for session lifecycle:
- confirmation that the delivered fix is reachable from `origin/main`:

## Immediate Mitigation

- investigated Fly stdout logs and direct production sqlite state;
- confirmed that fresh runtime file logs are not being written because prod keeps `ENABLE_RUNTIME_FILE_LOGGING=0`;
- formalized the incident so follow-up work can use this record as the regression contract.

## Corrective Actions

- deployed guard that fails closed on restart recovery for `local:*` kernel refs instead of polling impossible Kaggle slugs;
- operator-facing progress copy now stays on "готовлю рендер…" until real Kaggle handoff;
- added an emergency fail-open lever `VIDEO_ANNOUNCE_DISABLE_ABOUT_FILL=1` so compensating rerun can bypass LLM `about_fill` on the pre-Kaggle critical path if production recovery is blocked before render launch;
- pending recovery / catch-up for the missed April 20, 2026 scheduled slot.

## Follow-up Actions

- [ ] Identify the exact crash trigger that caused `ops_run.id=760` to finish as `crashed` before Kaggle handoff completed.
- [ ] Keep repo-local `local:*` refs out of restart recovery / Kaggle poller paths.
- [ ] Move or rename the operator-facing "запускаю Kaggle…" notification so it reflects a real handoff milestone.
- [ ] Verify whether `fill_missing_about()` was the concrete pre-handoff blocker for the missed April 20 rerun or only an emergency bypass candidate.
- [ ] After the fix, perform compensating rerun/catch-up and record evidence that the April 20, 2026 slot has been restored.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- ensure scheduled video runs have a single source of truth for terminality across `ops_run` and `videoannounce_session`;
- add logging around pre-render preparation and actual render handoff so operators can distinguish "still local" from "remote render started";
- keep a deliberate incident-mode plan for when runtime file logging is disabled, so retained evidence does not depend on assumption or operator memory.
