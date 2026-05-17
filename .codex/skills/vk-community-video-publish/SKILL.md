---
name: vk-community-video-publish
description: Use when publishing a local video file to a VK community wall or VK community stories, especially with env tokens such as VK_ACCESS_TOKEN4 and targets like vk.com/club231828790. Covers token checks, video.save + wall.post, and the required stories.getVideoUploadServer + stories.save sequence.
metadata:
  short-description: Publish VK community videos and stories
---

# VK Community Video Publish

Use this skill for one-off VK community video publishing from local files in this repo.

## Guardrails

- Never print access tokens. Source `.env` locally with `set -a; source .env; set +a`.
- Prefer `artifacts/` for uploaded source media; it is ignored by git.
- Use a user token from a community admin/editor for video uploads. Group tokens can read groups but fail `video.save` with `code=27`.
- Runtime uses `VK_USER_TOKEN` or local fallback `VK_ACCESS_TOKEN4`; CherryFlash Kaggle story publishing uses `VK_ACCESS_TOKEN5` inside encrypted story secrets.
- If VK returns `code=5` with `access_token was given to another ip address`, stop and ask for a fresh non-IP-bound user token.
- Check `git status --short` before edits; do not touch unrelated dirty files.

## Wall Video Post

1. Verify token:
   - `users.get` should succeed.
   - `groups.getById(group_ids=<group_id>)` should identify the target group.
2. Call `video.save` with:
   - `group_id=<positive group id>`
   - `name=<short title>`
   - `description=<caption>`
   - `wallpost=0`
3. Upload the file to returned `upload_url` with multipart field `video_file`.
4. Publish with `wall.post`:
   - `owner_id=-<group_id>`
   - `from_group=1`
   - `signed=0`
   - `message=<caption>`
   - `attachments=video<owner_id>_<video_id>[_<access_key>]`
5. Report the final URL: `https://vk.com/wall-<group_id>_<post_id>`.

## Community Story

VK story publishing is two-step. Upload success alone is not enough.

1. Call `stories.getVideoUploadServer` with:
   - `group_id=<positive group id>`
   - `add_to_news=1`
2. Upload the video to returned `upload_url` with multipart field `file`.
3. Extract `response.upload_result` from the upload response.
4. Call `stories.save` with:
   - `upload_results=<upload_result>`
   - optionally `extended=1` for evidence.
5. Treat success as `response.count >= 1`; report `owner_id`, story `id`, and `expires_at` if returned.

## Known Good Case

- Target: `https://vk.com/club231828790`
- Env: `VK_ACCESS_TOKEN4`
- Wall post evidence: `https://vk.com/wall-231828790_876`
- Story evidence: `owner_id=-231828790`, `id=456239019`, saved only after `stories.save`.
- Additional verified story target: `https://vk.com/konb39` (`club30777579`) published with `owner_id=-30777579`, story `id=456239753`.
- Verified image wall post target: `https://vk.com/klgdevents` (`club231920894`) with `photo...` attachment support.
