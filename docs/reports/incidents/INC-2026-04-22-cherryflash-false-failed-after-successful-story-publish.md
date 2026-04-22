# INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish CherryFlash False `FAILED` After Successful Story Publish

Status: monitoring
Severity: sev1
Service: CherryFlash / scheduled `popular_review` / Fly runtime state vs Kaggle remote completion
Opened: 2026-04-22
Closed: —
Owners: video announce runtime / operations
Related incidents: `INC-2026-04-20-video-tomorrow-stuck-rendering.md`, `INC-2026-04-16-cherryflash-kaggle-save-kernel-drift.md`, `INC-2026-04-19-cherryflash-story-media-invalid.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

The scheduled CherryFlash run on April 22, 2026 persisted production session `#181` as `FAILED` with `runtime restart before Kaggle handoff; rerun required`, while the actual Kaggle run for the same dataset (`cherryflash-session-181-1776843858`) continued remotely and successfully published stories to all configured targets.

## User / Business Impact

- operator-visible production state claimed today's CherryFlash run failed;
- the real viewer-facing output still reached Telegram Stories, so production truth split into two contradictory states;
- anti-repeat, monitoring, incident triage, and follow-up automation could all reason from the wrong session outcome.

## Detection

- direct production sqlite inspection showed `videoannounce_session.id=181` as `FAILED` within about 2 seconds of start;
- the Kaggle notebook log for Version `63` proved that the same mounted dataset still rendered and published stories successfully;
- user report noted that today's story was visible despite the local `FAILED` session state.

## Timeline

- 2026-04-22 07:44:01 UTC: production created CherryFlash session `#181`.
- 2026-04-22 07:44:03 UTC: production persisted session `#181` as `FAILED` with `runtime restart before Kaggle handoff; rerun required`.
- 2026-04-22 07:45:58 UTC: Kaggle Version `63` started from mounted dataset `cherryflash-session-181-1776843858`.
- 2026-04-22 08:09:55 UTC: Kaggle log recorded `Story published to @kenigevents (story_id=230)`.
- 2026-04-22 08:19:56 UTC: Kaggle log recorded successful repost to `@lovekenig`.
- 2026-04-22 08:29:57 UTC: Kaggle log recorded successful repost to `@loving_guide39`.
- 2026-04-22 09:16 UTC: local investigation log captured the finished Kaggle run and exposed the state split.

## Root Cause

1. `resume_rendering_sessions()` fails `RENDERING` sessions with repo-local refs like `local:CherryFlash` immediately on recovery.
2. That fail-close behavior is correct for stale orphaned sessions but too aggressive for a fresh handoff race where the previous runtime is still finishing dataset/kernel persistence.
3. In the April 22 incident, recovery marked session `#181` as `FAILED` before the remote CherryFlash handoff fully settled in sqlite, while the already-started Kaggle run continued and reached successful story publish.

## Contributing Factors

- `videoannounce_session` has only one local status field, so there was no separate "handoff pending" state for the brief overlap window;
- CherryFlash remote success lived in Kaggle outputs, not in Fly runtime logs, because production runtime-file logging remains disabled by policy;
- `PUBLISHED_TEST` rows historically did not always stamp `published_at`, which weakens later evidence queries.

## Automation Contract

### Treat as regression guard when

- changing `video_announce/poller.py` recovery logic for `RENDERING` sessions;
- changing `video_announce/scenario.py` handoff timing between local prep and remote Kaggle launch;
- changing CherryFlash scheduler/restart behavior or any status reconciliation for `popular_review`;
- changing how CherryFlash story success is inferred from session rows or remote outputs.

### Affected surfaces

- `video_announce/poller.py`
- `video_announce/scenario.py`
- CherryFlash scheduled `popular_review` startup/recovery path
- production sqlite `videoannounce_session`
- Kaggle CherryFlash runtime / output evidence

### Mandatory checks before closure or deploy

- prove that fresh `local:*` CherryFlash handoff rows are not failed immediately during the recovery race window;
- prove that stale `local:*` orphan sessions still fail closed instead of hanging forever;
- targeted `pytest` for `tests/test_video_announce_poller.py` plus CherryFlash selection/runtime regression tests;
- verify the existing `INC-2026-04-20-video-tomorrow-stuck-rendering` guard still holds: recovery must never start Kaggle polling against `local:*` refs;
- verify docs and changelog are synchronized with the new recovery contract;
- collect release evidence showing the fix is reachable from `origin/main` once deployed.

### Required evidence

- production sqlite evidence for session `#181`;
- Kaggle run evidence tying Version `63` to dataset `cherryflash-session-181-1776843858`;
- test output covering fresh-vs-stale local handoff recovery behavior;
- deployed SHA and confirmation that it is reachable from `origin/main`.

## Immediate Mitigation

- confirmed that the viewer-facing story output was real and belonged to today's CherryFlash dataset, not to a previous day;
- stopped treating `session.status == FAILED` as authoritative evidence of story failure for this run;
- formalized the incident so future changes on this surface must preserve both stale-session safety and fresh-handoff correctness.

## Corrective Actions

- introduce a bounded grace window before recovery fails a fresh `local:*` video session as pre-handoff dead;
- keep hard fail-close behavior for stale `local:*` sessions outside that grace window;
- retain publication timestamps for `PUBLISHED_TEST` rows so successful viewer-facing outcomes stay queryable later.

## Follow-up Actions

- [ ] Confirm on the next live scheduled CherryFlash run that session state and remote Kaggle/story outcome converge without manual inspection.
- [ ] Decide whether `videoannounce_session` needs an explicit "handoff pending" state instead of overloading `RENDERING`.
- [ ] Materialize remote story-success evidence into production session metadata instead of relying on notebook logs alone.

## Release And Closure Evidence

- deployed SHA: `4662e06b85a63e462d2ec0e3a9c698a11b1d5415`
- deploy path: manual `~/.fly/bin/flyctl deploy --app events-bot-new-wngqia --config fly.toml --remote-only` from clean local `main`, then Fly release `v979`
- regression checks:
  - `pytest -q tests/test_video_announce_poller.py tests/test_video_announce_popular_review.py tests/test_video_announce_v_pipeline.py` → `28 passed`
  - `python -m py_compile video_announce/poller.py video_announce/scenario.py video_announce/popular_review.py` → ok
  - verified release commit reachable from `origin/main`
- post-deploy verification:
  - Fly app `events-bot-new-wngqia` is on machine version `979`, image `deployment-01KPT9NT9QXSADD855M29823Q8`
  - `GET /healthz` after deploy returned `{"ok": true, "ready": true, ...}`
  - no compensating rerun was required for April 22, 2026 because the affected CherryFlash run already produced viewer-facing stories successfully; the defect was state drift, not lost publication

## Prevention

- recovery logic must distinguish "fresh handoff still settling" from "stale local ref truly orphaned";
- incident triage for CherryFlash must use both prod sqlite and Kaggle output evidence before declaring a viewer-facing run failed;
- session-state contracts should eventually encode remote handoff / remote completion more explicitly than a single local status flag.
