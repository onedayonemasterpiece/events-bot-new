# INC-2026-05-29 Guide VK Digest Missing Media

Status: open
Severity: sev2
Service: Guide excursions VK digest / `vk.com/uhtykaliningrad`
Opened: 2026-05-29
Closed: —
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

## Root Cause

1. The guide VK fanout reused the guide issue item ids to rebuild text, but ignored the issue's `media_items_json`.
2. The shared VK photo uploader only accepted remote URLs; guide media assets are materialized local files under `GUIDE_MEDIA_STORE_ROOT`.
3. Tests asserted VK text and target storage, but did not assert uploaded photo attachments.

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
- VK first line now uses exact dates/ranges instead of only months.
- Added regression coverage for VK guide attachments.

## Follow-up Actions

- [ ] Codex / 2026-05-29: deploy to production and repair all editable recent `uhtykaliningrad` guide digest posts.
- [ ] Codex / 2026-05-29: verify the next scheduled guide VK digest has photo attachments.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Guide VK fanout is now media-contract aware: issue media items must produce VK photo attachments or publication fails before creating another text-only post.
