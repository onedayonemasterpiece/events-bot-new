# INC-2026-05-29 Guide VK Digest Missing Media

Status: closed
Severity: sev2
Service: Guide excursions VK digest / `vk.com/uhtykaliningrad`
Opened: 2026-05-29
Closed: 2026-05-29
Owners: Codex / production operator
Related incidents: —
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/release-governance.md`

## Summary

Guide excursion digests published to `https://vk.com/uhtykaliningrad` were text-only even though the corresponding Telegram digest had images. VK also showed only a generic month-level first line, which made posts less informative in feed preview.

## User / Business Impact

- VK readers saw excursion digest posts without the visual cards/posters present in Telegram.
- The first visible VK line did not distinguish issues well enough: it carried count + months, not exact dates.
- The next scheduled guide digest would have repeated the same text-only degradation without a code fix.

## Detection

- Reported by the operator on 2026-05-29.
- Existing tests covered text rendering and target tracking, but not VK photo attachments for guide digests.

## Timeline

- 2026-05-29 UTC: operator reported missing VK images and requested production fix/backfill.
- 2026-05-29 UTC: code inspection found `publish_latest_guide_digest_to_vk` calling `post_to_vk` without attachments and not reading `guide_digest_issue.media_items_json`.
- 2026-05-29 UTC: hotfix branch created from `origin/main`.
- 2026-05-29 UTC: fix implemented with local media upload, fail-closed media handling, repair path, tests and docs.
- 2026-05-29 UTC: deployed hotfix to Fly production, repaired editable VK posts, and normalized stored VK target ids.

## Root Cause

1. The guide VK fanout reused the guide issue item ids to rebuild text, but ignored the issue's `media_items_json`.
2. The shared VK photo uploader only accepted remote URLs; guide media assets are materialized local files under `GUIDE_MEDIA_STORE_ROOT`.
3. Shared postponed `post_to_vk` stored VK's returned `post_id` as the final wall URL, but for postponed posts VK exposes that value as `postponed_id` while the actual `wall.get` item has a different `id`. Later repair/edit attempts addressed `wall-..._<postponed_id>` and VK returned an empty/deleted post.
4. Tests asserted VK text and target storage, but did not assert uploaded photo attachments or real wall id resolution for postponed posts.

## Contributing Factors

- Telegram digest has a split media/text publication model, while VK requires one wall post with text and attachments.
- The initial VK MVP optimized for plain-text rendering and left media parity as an unchecked gap.

## Automation Contract

### Treat as regression guard when

- Changing `guide_excursions/service.py::publish_latest_guide_digest_to_vk`.
- Changing guide digest issue creation, `media_items_json`, or media materialization.
- Changing shared VK photo upload helpers or `post_to_vk` attachment behavior.

### Affected surfaces

- `guide_excursions/service.py` VK digest render/publish/repair path.
- `main.py` VK photo upload helpers.
- `main_part2.py::post_to_vk` postponed id resolution.
- `guide_digest_issue.published_targets_json` release evidence.
- External VK API: `photos.getWallUploadServer`, `photos.saveWallPhoto`, `wall.post`, `wall.edit`.
- Production target `vk.com/uhtykaliningrad`.

### Mandatory checks before closure or deploy

- `pytest -q tests/test_guide_vk_digest.py`
- VK actor/community publishing regression check (`tests/test_vk_actor.py` or focused equivalent).
- Photo attachment regression check for shared VK event-post upload.
- Verify guide VK text first line includes count + exact dates/range.
- Post-deploy VK API verification for repaired/backfilled posts: one wall post per digest, `from_id=-<group_id>`, photo attachments present.
- Verify next guide digest publish path has non-empty attachments or fails before text-only publish.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test command output.
- VK post URLs repaired/backfilled or explicit VK edit-window blocker.
- VK API response confirming photo attachments on old/current posts.
- Changelog and canonical docs updated.

## Immediate Mitigation

- Added a repair-capable VK digest path that can upload materialized media assets and edit an existing VK post while VK still allows edits.

## Corrective Actions

- Added `upload_vk_photo_bytes` for in-memory/local guide media upload to VK.
- `publish_latest_guide_digest_to_vk` now reads `media_items_json`, uploads materialized photo assets, passes attachments to the same `post_to_vk`, and fails closed if usable media cannot be uploaded.
- The same function can repair an already published VK digest via `wall.edit` with attachments.
- Shared `post_to_vk` now resolves postponed VK ids to the actual wall id before returning/storing the URL.
- VK repair metadata now stores the same actual wall id in `message_ids`, `text_message_ids`, and `media_message_ids`, so future checks do not address stale postponed ids.
- VK first line now uses exact dates/ranges instead of only months.
- Added regression coverage for VK guide attachments.

## Follow-up Actions

- [x] Codex / 2026-05-29: deploy to production and repair all editable recent `uhtykaliningrad` guide digest posts.
- [x] Codex / 2026-05-29: verify the next scheduled guide VK digest publish path has photo attachments or fails before text-only VK publication.

## Release And Closure Evidence

- deployed SHA: `01abc40b3075494871c5e540b65609f3e6410001`, reachable from `origin/main`.
- deploy path: `flyctl deploy --app events-bot-new-wngqia --remote-only`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KSS53412WGPRDQN1MYCHKYM2`, machine `48e42d5b714228` version `1151`.
- regression checks: `.venv/bin/pytest -q tests/test_guide_vk_digest.py tests/test_vk_actor.py tests/test_vk_source.py::test_sync_vk_source_post_attaches_photos tests/test_vk_source.py::test_sync_vk_source_post_preserves_attachments_on_partial_reupload` passed (`13 passed`); `python3 -m py_compile guide_excursions/service.py main.py main_part2.py`; `git diff --check`.
- health: `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, issues `[]`; Fly status showed `1 total, 1 passing` check.
- production log check: runtime file mirror was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`); no guide/VK digest errors were found after deploy. An unrelated `tg_ics_post` `bad time` job error was visible in runtime logs and is outside this incident's surface.
- backfill/repair: stored VK URLs were normalized from postponed ids to actual wall ids (`_1 -> _2`, `_3 -> _4`, `_5 -> _6`, `_7 -> _8`). Posts `wall-238875824_6` and `wall-238875824_8` were still editable and repaired with attachments. `wall-238875824_2` was outside VK's edit window; `wall-238875824_4` had no saved issue media to attach.
- VK API verification: `wall-238875824_6` exists, `from_id=-238875824`, `photos=3`, first line `Новые экскурсии: 4 выхода, 30 мая и 31 мая`; `wall-238875824_8` exists, `from_id=-238875824`, `photos=2`, first line `Новые экскурсии: 8 выходов, 30 мая - 28 июня`.
- production DB verification: issue `79` target `vk:uhtykaliningrad` stores `post_urls=["https://vk.com/wall-238875824_6"]`, `message_ids=[6]`, `attachments_count=3`; issue `80` stores `post_urls=["https://vk.com/wall-238875824_8"]`, `message_ids=[8]`, `attachments_count=2`.

## Prevention

- Guide VK fanout is now media-contract aware: issue media items must produce VK photo attachments or publication fails before creating another text-only post.
