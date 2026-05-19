# INC-2026-05-19-vk-posts-personal-author

Status: closed
Severity: sev2
Service: VK outbound publishing (`kenigeventsofficial`, `klgdevents`)
Opened: 2026-05-19
Closed: 2026-05-19
Owners: Codex
Related incidents: `INC-2026-04-26-vk-daily-message-limit`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/release-governance.md`, `docs/operations/runtime-logs.md`

## Summary

VK wall posts created through the shared `post_to_vk` helper were published on community walls but authored by the admin user (`from_id=868977531`) when the first successful actor was a group token. These posts could not be normally forwarded to other walls/communities, while the video-announcement VK wall path worked because it explicitly passed `from_group=1` and `signed=0`.

## User / Business Impact

- New VK daily posts in `vk.com/kenigeventsofficial` and event posts in `vk.com/klgdevents` had limited share/forward behavior.
- Operators could forward affected posts only to personal messages.
- Existing posts are left untouched by operator request; only future publication behavior is fixed.

## Detection

- Operator reported that only the VK video announcement could be forwarded normally, and noticed regular posts appeared authored by the personal `kenigevents` user.
- VK API inspection confirmed the split:
  - `wall-231828790_883`: `from_id=868977531`, `likes.can_publish=0`.
  - `wall-231920894_1101`: `from_id=868977531`, `likes.can_publish=0`.
  - `wall-231828790_884`: `from_id=-231828790`, `likes.can_publish=1`, video `can_repost=1`.
- Runtime logs showed the affected shared path succeeded through group actors:
  - `2026-05-19 06:01:00 UTC post_to_vk start group=231828790 ... actor=group:main ... post_id=883`.
  - `2026-05-19 04:32:52 UTC post_to_vk ok group=231920894 post_id=1101 ... actor=group:afisha`.

## Timeline

- 2026-05-19 06:01 UTC — VK daily `today` published to `wall-231828790_883` through `actor=group:main` without `from_group=1`.
- 2026-05-19 04:32-09:08 UTC — multiple `klgdevents` event posts published through `actor=group:afisha` without `from_group=1`.
- 2026-05-19 08:16 UTC — video announcement `wall-231828790_884` published through Kaggle VK wall helper with explicit `from_group=1`, proving the expected behavior.
- 2026-05-19 — root cause localized to `main_part2.py::post_to_vk`; fix prepared to always send community posts as community-authored.

## Root Cause

1. `post_to_vk` added `from_group=1` only for the user-token actor.
2. In production, group tokens are tried first for configured groups and often succeed.
3. When a group-token call succeeded without `from_group=1`, VK created a wall post on the community wall but with `from_id` set to the admin user, limiting share behavior.

## Contributing Factors

- Regression coverage existed for actor selection and VK video wall publishing, but not for the group-token `post_to_vk` parameter contract.
- The successful `post_id` response hid the user-authored shape unless the post was inspected via VK API or UI sharing.

## Automation Contract

### Treat as regression guard when

- Changing `post_to_vk`, `send_daily_announcement_vk`, `sync_vk_source_post`, VK actor/token selection, or VK wall publish parameters.
- Changing VK daily/event target group routing or token preference order.

### Affected surfaces

- `main_part2.py::post_to_vk`
- `main_part2.py::send_daily_announcement_vk`
- `main_part2.py::sync_vk_source_post`
- `main.py::choose_vk_actor`
- VK API `wall.post`
- Production env: `VK_MAIN_GROUP_ID`, `VK_AFISHA_GROUP_ID`, `VK_EVENTS_GROUP_ID`, `VK_TOKEN`, `VK_TOKEN_AFISHA`, `VK_USER_TOKEN`

### Mandatory checks before closure or deploy

- Unit test proves group-token `post_to_vk` includes `owner_id=-group`, `from_group=1`, and `signed=0`.
- Existing actor fallback tests still pass.
- Production config check confirms relevant VK tokens and group IDs are present.
- Post-deploy smoke publishes a new VK post to a controlled/current target or otherwise verifies the next scheduled post has `from_id=-group_id` and `likes.can_publish=1`.
- Release-governance check: deployed SHA must be reachable from `origin/main`.

### Required evidence

- Test command output.
- Deployed SHA and deploy path.
- VK API output for a new post showing `from_id=-<group_id>` and `likes.can_publish=1`.
- Confirmation that old posts were not edited/deleted.

## Immediate Mitigation

- No old posts were changed, per operator direction.
- Shared VK wall publisher now always sends community wall posts with `from_group=1` and `signed=0`.

## Corrective Actions

- Fixed `post_to_vk` to set `from_group=1` and `signed=0` in `params_base` for community wall posts regardless of which actor token succeeds.
- Added regression coverage for the group-token path.
- Documented the community-authoring contract in the VK publishing canonical doc.

## Follow-up Actions

- [x] After deploy, verify the next scheduled VK daily/event post shape or a controlled smoke post and close this incident with the exact post URL and API evidence.

## Release And Closure Evidence

- deployed SHA: `ae8494f8feb31e6f196e12a4169813ca92f498bf`
- deploy path: manual `flyctl deploy --config fly.toml --app events-bot-new-wngqia --remote-only`
- production health:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `{"ok": true, "ready": true, ... "issues": []}` after deploy.
  - Fly machine `48e42d5b714228`, version `1131`, status `started`, checks `1 passing`.
- regression checks:
  - `python3 -m py_compile main_part2.py tests/test_vk_actor.py` -> passed.
  - `timeout 90 /home/dev/projects/events-bot-new/.venv/bin/pytest -q tests/test_vk_actor.py tests/test_vk_source.py::test_vk_wall_source_still_gets_event_vk_sync tests/test_vk_source.py::test_managed_klgdevents_event_skips_vk_sync tests/test_sanitize_for_vk.py` -> `13 passed in 3.00s`.
  - Broader `tests/test_vk_source.py` still has an unrelated existing LLM-fixture drift: `test_add_events_from_text_preserves_links` monkeypatches the old 4o parser path, but current code tries Gemma and fails without `GOOGLE_API_KEY`.
- post-deploy verification:
  - Controlled smoke through deployed `/app/main.post_to_vk("231828790", ...)` created `https://vk.com/wall-231828790_885` via `actor=group:main`. VK API evidence before deletion: `owner_id=-231828790`, `from_id=-231828790`, `created_by=868977531`, `likes.can_publish=1`, `likes.repost_disabled=false`, `post_source.type=api`.
  - Controlled smoke through deployed `/app/main.post_to_vk("231920894", ...)` created `https://vk.com/wall-231920894_1152` via `actor=group:afisha`. VK API evidence before deletion: `owner_id=-231920894`, `from_id=-231920894`, `created_by=868977531`, `likes.can_publish=1`, `likes.repost_disabled=false`, `post_source.type=api`.
  - Both smoke posts were deleted immediately after verification; no pre-existing VK posts were edited or deleted.
  - Release-governance check: deployed SHA is reachable from `origin/main`; no `origin/release/*` or `origin/hotfix/*` branch was ahead of `origin/main` before deploy.

## Prevention

- `tests/test_vk_actor.py::test_post_to_vk_group_actor_posts_from_group` pins the group-token `wall.post` parameter contract that was previously missing.
