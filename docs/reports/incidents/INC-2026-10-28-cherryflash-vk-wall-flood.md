# INC-2026-10-28 CherryFlash VK wall publication stopped

Status: open
Severity: sev2
Service: CherryFlash publication to `vk.com/club231828790` on RunCoveer
Opened: 2026-10-28
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-24-vk-afisha-publication-recurrence`, `INC-2026-09-27-vk-auto-video-reader-publisher-token`, `INC-2026-10-14-scheduler-disk-exhaustion`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/runtime-logs.md`

## Summary

CherryFlash videos continued to upload to VK, but video announcements stopped appearing on the `Полюбить Калининград Анонсы` wall after 4 October. The Kaggle per-target report exposes VK `wall.post` code 9 (`Flood control`); the target is optional for overall Telegram delivery, so the enclosing report still says `ok=true`. Separately, RunCoveer fails to download recent Kaggle output after excessive output-list pagination reaches HTTP 429.

## User / Business Impact

- Subscribers have not received daily CherryFlash wall posts since 4 October, despite upload notifications.
- Recent sessions, including 1518 on 27 October, also ended locally with `kernel output download failed`; the Kaggle handoff `ops_run` success is not a publication receipt.
- Old announcements must not be replayed as current event recommendations.

## Detection

- Owner reported uploaded videos absent from the community wall.
- Authenticated VK wall search found the last `Видеоанонс` on 4 October; RunCoveer `/healthz` was ready and did not cover per-target delivery.

## Timeline

- 2026-10-04: last observed CherryFlash `Видеоанонс` wall post.
- 2026-10-27: Kaggle kernel version 522, session 1518, reported `wall.post` code 9 for `club231828790`; Telegram targets succeeded and overall report was `ok=true`.
- 2026-10-27 09:44:57 UTC: RunCoveer output download for session 1518 failed after `ListKernelSessionOutput` HTTP 429.
- 2026-10-28: confirmed `VK_ACCESS_TOKEN4` through `7` receive code 9 on `users.get`; `VK_TOKEN` resolves to `club231828790`, has wall permission and remains usable. `VK_ACCESS_TOKEN5` still accepts `video.save`; the group token cannot call `video.save` (code 5).

## Root Cause

1. CherryFlash used one restricted user token (`VK_ACCESS_TOKEN5`) for both video upload and `wall.post`. VK currently rejects its wall publication with code 9. The precise initial VK request that caused restriction is unknown.
2. The RunCoveer Kaggle client used the SDK's default 20-item output listing page. The observed kernel had 1,532 files, requiring about 77 list calls and hitting HTTP 429. Kaggle accepts 500 items per page, reducing that listing to four calls.

## Contributing Factors

- VK wall is a best-effort fanout target; overall `ok=true` means required Telegram targets succeeded, not that VK received a post.
- RunCoveer health reports scheduler/process readiness, not successful VK wall delivery.

## Automation Contract

### Treat as regression guard when

- Changing CherryFlash VK token routing, wall/story fanout, Kaggle output listing or scheduled publication.

### Affected surfaces

- `video_announce/story_publish.py`, `video_announce/poller.py`, `video_announce/kaggle_client.py`, `kaggle/CrumpleVideo/story_publish.py`, embedded CrumpleVideo notebook helper, production `VK_ACCESS_TOKEN5` and `VK_TOKEN`.

### Mandatory checks before closure or deploy

- Verify the encrypted story bundle maps the `club231828790` wall call to its community token while `video.save` retains the user token.
- Verify the Kaggle client requests a supported 500-item output page, retains the file filter and downloads a real output.
- Check a new day's CherryFlash video appears on the public community wall, with one post and the expected video attachment; check session report, DB and RunCoveer logs.
- Prove the deployed commit belongs to `origin/main` and the current running release is its ancestor; check `/healthz` after RunCoveer deploy.

### Required evidence

- Kaggle version 522 `story_publish_report.json`, RunCoveer log at 2026-10-27 09:44:57 UTC, focused tests, public VK post URL, session/report result, deployed image and SHA.

## Immediate Mitigation

- Prepared group-token wall routing and larger output-list pages. Live publication verification remains pending.

## Corrective Actions

- Route only `club231828790` `wall.post` through its already configured community token; retain user-token video upload.
- Request 500 output entries per Kaggle list page, within the provider's accepted limit.

## Follow-up Actions

- [ ] Verify today's fresh CherryFlash post, attachment and local session after release.
- [ ] Review whether optional VK fanout needs a delivery alert; do not change Telegram success semantics as part of this transport repair.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: RunCoveer production image, pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- The group-token route and focused pagination/token-routing tests guard this failure mode.
