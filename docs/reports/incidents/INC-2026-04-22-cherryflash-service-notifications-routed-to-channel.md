# INC-2026-04-22-cherryflash-service-notifications-routed-to-channel CherryFlash Service Notifications Routed To Public Channel

Status: monitoring
Severity: sev2
Service: CherryFlash / video announce recovery notifications / Telegram routing
Opened: 2026-04-22
Closed: —
Owners: video announce runtime / operations
Related incidents: `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish.md`, `INC-2026-04-20-video-tomorrow-stuck-rendering.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/cron.md`, `docs/operations/incident-management.md`

## Summary

CherryFlash recovery/service diagnostics leaked into the viewer-facing Telegram channel because restart-time notification routing fell back to `test_chat_id` when no explicit operator/admin `notify_chat_id` was persisted on the session.

## User / Business Impact

- operational messages such as false `FAILED` alerts appeared in the channel feed instead of staying in the admin DM;
- the channel surface mixed service diagnostics with editorial output;
- operators could not trust that a service alert had been delivered privately, and viewers could see internal runtime state.

## Detection

- operator screenshot from April 22, 2026 showed `⚠️ Сессия #181: рантайм перезапустился...` posted in `Кёнигсберг GPT`;
- code inspection confirmed that `resume_rendering_sessions()` used `sess.test_chat_id` / `sess.main_chat_id` as the final fallback for `notify_chat_id`;
- `start_render()` also failed to persist `selection_params.notify_chat_id`, leaving recovery without an explicit admin/operator target after a restart.

## Timeline

- 2026-04-22 07:44 UTC: session `#181` entered `RENDERING` for CherryFlash.
- 2026-04-22 07:44 UTC: runtime recovery emitted a pre-handoff failure alert.
- 2026-04-22 09:44 local UI evidence showed that alert inside channel `Кёнигсберг GPT` instead of the admin DM.
- 2026-04-22 investigation: routing fallback and missing persisted `notify_chat_id` were confirmed in `video_announce/scenario.py` and `video_announce/poller.py`.

## Root Cause

1. `start_render()` did not persist the operator/admin `notify_chat_id` into `selection_params` for normal video sessions.
2. `resume_rendering_sessions()` resolved recovery notifications by falling back to `sess.test_chat_id` and then `sess.main_chat_id`.
3. For CherryFlash, `test_chat_id` is the viewer-facing channel, so missing explicit notify metadata caused service alerts to be posted publicly.

## Contributing Factors

- `test_chat_id` is a publish target, not an operational notify target, but recovery treated them as interchangeable;
- the scheduled CherryFlash path relies on restart recovery for incident visibility, so this fallback was hit on a real production session;
- there was no regression test asserting that recovery diagnostics never route to publish channels by fallback.

## Automation Contract

### Treat as regression guard when

- changing `video_announce/poller.py` notify routing or restart recovery;
- changing `video_announce/scenario.py` session start/persistence logic;
- changing CherryFlash scheduled execution or any surface that can emit background/service notifications after restart.

### Affected surfaces

- `video_announce/poller.py`
- `video_announce/scenario.py`
- CherryFlash scheduled recovery path
- admin/superadmin DM routing
- Telegram publish channels (`test_chat_id`, `main_chat_id`)

### Mandatory checks before closure or deploy

- prove that restart recovery does not use `test_chat_id` / `main_chat_id` as a service-notification fallback;
- prove that normal `start_render()` persists `selection_params.notify_chat_id` for later recovery;
- targeted `pytest` for `tests/test_video_announce_poller.py` and `tests/test_video_announce_v_pipeline.py`;
- verify docs/changelog reflect the separation between service-routing and publish-routing;
- collect release evidence showing the fix is reachable from `origin/main` once deployed.

### Required evidence

- screenshot/log evidence of the leaked channel notification;
- code diff showing removal of channel fallback in recovery;
- tests proving recovery goes to admin/superadmin DM and session start persists notify metadata;
- deployed SHA and confirmation it is reachable from `origin/main`.

## Immediate Mitigation

- formalized the routing leak as a separate incident instead of folding it into the false-`FAILED` state split;
- treated the channel alert as a routing defect, not as proof that the service alert path is correctly configured.

## Corrective Actions

- persist `selection_params.notify_chat_id` in the normal `start_render()` path;
- make recovery resolve service notifications only from explicit operator/admin targets (`chat_id`, persisted `notify_chat_id`, superadmin fallback), never from publish channels;
- add regression tests for both persistence and recovery routing.

## Follow-up Actions

- [ ] Confirm on the next live CherryFlash restart/recovery that service diagnostics stay in the admin DM and do not appear in `@keniggpt`.
- [ ] Audit sibling video-announce recovery paths for the same publish-channel fallback assumption.

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
  - next live recovery/failure event still needs monitoring confirmation that service diagnostics stay in admin DM and do not leak to the channel again

## Prevention

- service-notification routing and publish-target routing must stay separate concepts in video-announce session metadata;
- any recovery/background notifier that lacks an explicit operator context must resolve superadmin/admin DM, not a public/test channel;
- new video session start flows should persist the notify target explicitly so restart logic does not guess.
