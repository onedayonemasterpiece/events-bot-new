# INC-2026-04-23-cherryflash-pre-handoff-loss CherryFlash Pre-Handoff Run Loss

Status: open
Severity: sev1
Service: CherryFlash / scheduled `popular_review` / Kaggle handoff and same-day catch-up
Opened: 2026-04-23
Closed: —
Owners: video announce runtime / operations
Related incidents: `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish.md`, `INC-2026-04-20-video-tomorrow-stuck-rendering.md`, `INC-2026-04-16-cherryflash-kaggle-save-kernel-drift.md`, `INC-2026-04-19-cherryflash-story-media-invalid.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/cron.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

The scheduled CherryFlash run on April 23, 2026 created production session `#183`, selected six events, and showed operators the pre-Kaggle "Готовим Kaggle" status, but never created a `cherryflash-session-*` dataset or a new visible `zigomaro/cherryflash` Kaggle run. The session later failed as `runtime restart before Kaggle handoff; rerun required`, while `ops_run(kind='video_popular_review')` still said `success`.

## User / Business Impact

- the daily CherryFlash story was not published on April 23, 2026;
- operators saw a launched session and a successful scheduled `ops_run` even though no durable Kaggle handoff existed;
- because there was no same-day CherryFlash watchdog/catch-up, the missed scheduled slot would stay lost without manual incident intervention.

## Detection

- user reported that today's CherryFlash story did not appear and Kaggle still showed only the previous `#181` run;
- production sqlite showed session `#183` as `FAILED`, `kaggle_dataset=NULL`, `kaggle_kernel_ref='local:CherryFlash'`;
- Kaggle `zigomaro/cherryflash` status was `COMPLETE`, but no session `#183` dataset was attached or launched.

## Timeline

- 2026-04-23 07:44:00 UTC: scheduled `video_popular_review` `ops_run id=809` started.
- 2026-04-23 07:44:26 UTC: production created CherryFlash session `#183`.
- 2026-04-23 07:44:32 UTC: session `#183` moved to `RENDERING` with `kaggle_kernel_ref='local:CherryFlash'`.
- 2026-04-23 07:46:29 UTC: `ops_run id=809` finished `success`, even though the render task had not reached remote Kaggle handoff.
- 2026-04-23 08:22:59 UTC: recovery marked session `#183` `FAILED` with `runtime restart before Kaggle handoff; rerun required`.
- 2026-04-23 14:30 UTC: incident investigation confirmed no `kaggle_dataset` for session `#183` and no new CherryFlash story.

## Root Cause

1. `VideoAnnounceScenario.start_render()` always handed the expensive pre-Kaggle work to a background asyncio task and returned immediately.
2. `_run_scheduled_popular_review()` treated that immediate return as scheduled success, so `ops_run` became a false durable marker before `kaggle_dataset` / real `kaggle_kernel_ref` existed.
3. CherryFlash did not have the same same-day startup/watchdog catch-up contract as `video_tomorrow`; after the pre-handoff task was interrupted, the scheduler had no automatic path to compensate.

## Contributing Factors

- the exact low-level runtime interruption before handoff was not captured because production runtime file logging is disabled by policy;
- `videoannounce_session` still overloads `RENDERING` for both "preparing local handoff" and "remote Kaggle is running";
- previous recovery hardening correctly failed stale `local:*` refs closed, but did not by itself create a replacement run for the current day.

## Automation Contract

### Treat as regression guard when

- changing `VideoAnnounceScenario.start_render()` or `_render_and_notify()`;
- changing `_run_scheduled_popular_review()` or CherryFlash scheduler/catch-up logic;
- changing local-kernel recovery behavior for `local:CherryFlash`;
- changing CherryFlash story success evidence, Kaggle dataset creation, or kernel push handling.

### Affected surfaces

- `video_announce/scenario.py`
- `video_announce/poller.py`
- `scheduling.py`
- CherryFlash scheduled `popular_review`
- production sqlite `ops_run` and `videoannounce_session`
- Kaggle `zigomaro/cherryflash` launch evidence

### Mandatory checks before closure or deploy

- prove scheduled CherryFlash waits for a confirmed non-local Kaggle handoff before marking `ops_run` success;
- prove a failed/stale local-only CherryFlash session for today's slot triggers startup/watchdog catch-up;
- prove an existing remote CherryFlash handoff (`kaggle_dataset` plus non-local kernel ref) suppresses duplicate catch-up, even if local status is misleading;
- run targeted scheduler, poller, and CherryFlash pipeline tests;
- verify the existing `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish` guard still holds for fresh-vs-stale local refs;
- after deploy, execute a compensating same-day CherryFlash rerun/catch-up and collect Kaggle/story evidence.

### Required evidence

- production sqlite evidence for failed session `#183` and `ops_run id=809`;
- test output covering scheduled handoff, catch-up retry, duplicate suppression, and local-ref recovery;
- deployed SHA reachable from `origin/main`;
- Kaggle dataset/kernel evidence for the compensating April 23, 2026 run;
- story publish evidence for `@kenigevents` and downstream configured fanout, or a precise terminal publish error if Telegram rejects the run.

## Immediate Mitigation

- confirmed that session `#183` never reached Kaggle handoff and therefore needed a compensating rerun, not just state reconciliation;
- isolated the hotfix in a clean worktree from `origin/main` because the default checkout had unrelated in-progress guide work.

## Corrective Actions

- scheduled CherryFlash now can run `start_render(..., background=False)` so `_run_scheduled_popular_review()` only records success after the dataset and real Kaggle kernel ref are persisted;
- scheduled CherryFlash records session launch evidence into `ops_run.details_json` and fails the `ops_run` if handoff is still local-only;
- CherryFlash now has same-day startup catch-up and a watchdog that reruns only when today's slot lacks a confirmed remote Kaggle handoff.

## Follow-up Actions

- [ ] Decide whether `videoannounce_session` should get an explicit `HANDOFF_PENDING` / `REMOTE_RUNNING` split.
- [ ] Add a durable remote story-success marker into production sqlite instead of relying on Kaggle output inspection.
- [ ] Investigate whether the April 23 pre-handoff interruption had a resource/OOM trigger once provider-level logs or metrics are available.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- scheduled daily video jobs must not mark dispatch success before their durable external handoff exists;
- daily CherryFlash recovery must reason from `videoannounce_session` remote handoff evidence, not just `ops_run`;
- incident closure for a missed scheduled video slot requires compensating rerun/catch-up evidence for the same local day.
