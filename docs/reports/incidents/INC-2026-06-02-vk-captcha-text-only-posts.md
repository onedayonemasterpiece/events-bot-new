# INC-2026-06-02-vk-captcha-text-only-posts

Status: monitoring
Severity: sev2
Service: VK outbound publishing (`klgdevents`, `kenigeventsofficial`)
Opened: 2026-06-02
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-19-vk-posts-personal-author`, `INC-2026-05-29-guide-vk-digest-missing-media`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

VK event posts in `vk.com/klgdevents` started appearing without photo attachments after the production process hit VK captcha (`error_code=14`) on the user-token media path. The text wall-post path still succeeded through the group token, so `vk_sync` continued creating text-only posts instead of pausing until captcha/token recovery.

## User / Business Impact

- New `klgdevents` event posts from the morning of 2026-06-02 were published as text-only cards.
- Event rows still had `photo_urls`; the media loss happened only at the VK outbound upload boundary.
- The main daily post in `vk.com/kenigeventsofficial` was not expected to carry event photos, but the same stuck captcha flag also affected user-token VK helper calls there.

## Detection

- Operator reported that "almost сутки" VK community posts had no pictures.
- Runtime file mirror was enabled on production (`ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`) and contained the affected window.
- VK API inspection of the latest `wall-231920894_*` posts confirmed `photos=0` for 2026-06-02 posts and `photos=1` for the previous successful 2026-06-01 posts.

## Timeline

- 2026-06-01 16:32-17:53 UTC — `klgdevents` posts `wall-231920894_1839` through `_1854` published with `attachments=1`.
- 2026-06-02 00:24 UTC — production hit VK `code=14 Captcha needed` during `wall.edit` on `owner_id=-231828790`; `_vk_captcha_needed` became true in-process.
- 2026-06-02 04:16 UTC — first observed `klgdevents` `vk_sync` photo upload failed with `Captcha needed`, then `post_to_vk` succeeded with `attachments=0`.
- 2026-06-02 04:27-08:57 UTC — VK API showed latest `klgdevents` posts `wall-231920894_1857` through `_1878` had zero photo attachments.
- 2026-06-02 13:21 UTC — production `VK_USER_TOKEN` was updated from operator-provided local `VK_ACCESS_TOKEN7` and the Fly machine restarted, clearing the stale in-memory captcha flag.
- 2026-06-02 13:22 UTC — `/healthz` returned ready; direct `photos.getWallUploadServer` checks for groups `231920894` and `231828790` returned `ok` without captcha.

## Root Cause

1. VK returned captcha (`code=14`) for a user-token call and the production process latched `_vk_captcha_needed=True`.
2. `upload_vk_photo` caught the resulting `VKAPIError(14)` in its broad outer `except Exception`, logged `VK photo upload failed: Captcha needed`, and returned `None`.
3. `sync_vk_source_post` treats a failed photo upload as optional media loss and proceeded to `post_to_vk(..., attachments=None)`.
4. `post_to_vk` succeeded through the group token, creating text-only community posts.

## Contributing Factors

- The captcha pause contract existed in the job worker only when `VKAPIError(14)` reached it; the photo upload helper swallowed the error before that boundary.
- Direct token validity and in-process captcha state diverged: direct API checks later returned `ok`, while the already-running app still had `_vk_captcha_needed=True`.
- The current smoke/checklist did not require verifying attachments after VK captcha recovery.

## Automation Contract

### Treat as regression guard when

- Changing `upload_vk_photo`, `upload_vk_photo_bytes`, `sync_vk_source_post`, `post_to_vk`, VK actor/token selection, VK captcha handling, or `vk_sync` job error handling.
- Changing production VK secrets (`VK_USER_TOKEN`, `VK_ACCESS_TOKEN4`, `VK_ACCESS_TOKEN7`, `VK_TOKEN_AFISHA`) or VK photo publishing feature flags.

### Affected surfaces

- `main.py::upload_vk_photo`
- `main.py::upload_vk_photo_bytes`
- `main_part2.py::sync_vk_source_post`
- `main_part2.py::post_to_vk`
- `JobTask.vk_sync` job worker captcha pause path
- Production env: `VK_USER_TOKEN`, `VK_ACCESS_TOKEN4`, `VK_ACCESS_TOKEN7`, `VK_EVENTS_GROUP_ID`, `VK_AFISHA_GROUP_ID`
- VK API: `photos.getWallUploadServer`, `photos.saveWallPhoto`, `wall.post`, `wall.get`

### Mandatory checks before closure or deploy

- Unit test proves VK `code=14` during event photo upload propagates and prevents text-only `wall.post`.
- Existing VK source/post actor tests still pass.
- Production config check confirms `VK_USER_TOKEN` and `VK_EVENTS_GROUP_ID` are present.
- Runtime or direct API check confirms `photos.getWallUploadServer` for `VK_EVENTS_GROUP_ID` returns `ok` without `error_code=14`.
- Post-deploy smoke verifies a new/next `klgdevents` event post has at least one photo attachment, or a controlled smoke post with a photo is created and deleted after VK API verification.
- Release-governance check: deployed SHA must be reachable from `origin/main`.

### Required evidence

- Test command output.
- Deployed SHA and deploy path.
- Production `/healthz` after restart/deploy.
- VK API evidence for `wall-231920894_*` post attachments or controlled smoke.
- Runtime log excerpt showing no new `Captcha needed` / text-only media failure after mitigation.

## Immediate Mitigation

- Updated production `VK_USER_TOKEN` from the operator-provided local `VK_ACCESS_TOKEN7` via `flyctl secrets import`.
- Stored `VK_ACCESS_TOKEN7` as a production secret as well for observability/future rotation.
- Fly machine `48e42d5b714228` rolled from version `1165` to `1166`; `/healthz` returned ready after restart.
- Direct post-restart checks: `photos.getWallUploadServer` returned `ok` for `231920894` and `231828790`.

## Corrective Actions

- `upload_vk_photo` and `upload_vk_photo_bytes` now re-raise `VKAPIError(code=14)` instead of swallowing it.
- Added regression coverage that `sync_vk_source_post` does not call `wall.post` when photo upload hits VK captcha.
- Documented the fail-closed VK media captcha contract in `docs/features/vk-publishing/README.md`.

## Follow-up Actions

- [ ] After deploy, verify the next `klgdevents` event post or a controlled smoke post has photo attachments.
- [ ] Consider an operator-visible alert when `_vk_captcha_needed` is set by a media upload path and no manual captcha prompt is attached to the current workflow.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- The regression test pins the exact failure mode: VK captcha during photo upload must pause/fail closed before text-only publication.
- Operational docs now require direct `photos.getWallUploadServer` checks and process restart after captcha/token recovery.
