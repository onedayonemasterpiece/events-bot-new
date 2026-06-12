# INC-2026-06-12-vk-partial-media-family-cta VK Partial Media And Family CTA

Status: open
Severity: sev2
Service: VK event publishing / Afisha Engagement
Opened: 2026-06-12
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-02-vk-captcha-text-only-posts`
Related docs: `docs/features/vk-publishing/README.md`, `docs/features/afishaengagement/README.md`, `docs/operations/runtime-logs.md`

## Summary

Event `5951` (`Путешествие в сказку в деревне Холмогорье`) imported with four managed photos and published to Telegram with four photos, but the managed VK event post was created with only two photo attachments after transient VK upload-server failures. The same event's `afishaengagement` shadow copy used generic market repost text (`Поделись с подругой, которая любит такие ярмарки`) even though the cleaned event is a family fair.

## User / Business Impact

- The VK event post lost media parity with Telegram/Telegraph and underrepresented the event's photo set.
- The shadow CTA looked tonally wrong for a family/children event and would be unsafe to promote beyond debug shadow review.

## Detection

- Operator noticed Telegram had four photos while VK had two.
- Operator also flagged the shadow copy's `подруга / ярмарки` phrasing as too generic for a family event.
- Runtime file logs on `/data/runtime_logs` confirmed upload-server failures before the partial VK post was created.

## Timeline

- 2026-06-12 17:16 Europe/Kaliningrad: `/vk_auto_import 1` processed source `https://vk.com/wall-146688375_7432` and created event `5951` with `Иллюстрации: +4`.
- 2026-06-12 16:10-16:11 UTC: runtime logs show two VK `upload.php` failures during photo upload, followed by `post_to_vk ... attachments=2`.
- 2026-06-12 16:11 UTC: afishaengagement scheduled shadow copy with `event_type=market` and CTA `Поделись с подругой, которая любит такие ярмарки`.
- 2026-06-12: operator reported VK/Telegram media mismatch and family CTA mismatch.

## Root Cause

1. `sync_vk_source_post` treated any non-empty uploaded attachment list as publishable for new managed VK posts, even when only part of `photo_urls_for_publish` uploaded.
2. `upload_vk_photo` retried VK API calls but not the direct `upload.php` request/JSON decode step, so a transient HTML/timeout response could drop one photo from the batch.
3. `afishaengagement` let explicit stored `ярмарка` type win over strong family audience signals in the cleaned title/description.

## Contributing Factors

- Existing fail-closed guard covered text-only media loss, but not partial media loss.
- Existing family copy had `подруга` variants, but not softer parent-aware repost variants.

## Automation Contract

### Treat as regression guard when

- Changing `main.py::upload_vk_photo`, `main_part2.py::sync_vk_source_post`, VK media upload retry behavior, or `afishaengagement` event-type/CTA selection.

### Affected surfaces

- `main.py::upload_vk_photo`
- `main_part2.py::sync_vk_source_post`
- `afishaengagement.py::_event_type_key`
- `afishaengagement.py::_templates_for`
- VK `wall.post` / `wall.edit` with media attachments
- Afisha Engagement debug shadow postponed posts

### Mandatory checks before closure or deploy

- `tests/test_vk_source.py::test_sync_vk_source_post_blocks_new_post_on_partial_media_upload`
- `tests/test_vk_source.py::test_sync_vk_source_post_preserves_attachments_on_partial_reupload`
- `tests/test_vk_source.py::test_sync_vk_source_post_blocks_vk_origin_when_available_media_uploads_empty`
- `tests/test_afishaengagement.py::test_family_market_uses_soft_mom_friend_repost_copy`
- Production verification for event `5951`: managed VK event post has four photo attachments or an explicit platform/editing blocker is recorded.
- Production verification for event `5951` shadow: stale generic market CTA is removed or replaced, or an explicit VK editing/deletion blocker is recorded.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted pytest output.
- Runtime-log excerpt or DB/VK API evidence for pre-fix partial media.
- Post-deploy VK API evidence for repaired media and shadow CTA/deletion outcome.

## Immediate Mitigation

- Code fix blocks new partial-media VK posts and retries transient upload-server failures.
- Existing event `5951` requires post-deploy compensation.

## Corrective Actions

- Add upload-server retry loop with a fresh `photos.getWallUploadServer` URL per attempt.
- Make new managed VK event posts fail closed with `vk_sync_partial_media_upload` when only part of the expected media uploads.
- Classify family fairs/markets as `family` for afishaengagement when cleaned event text contains explicit child/family audience signals.
- Add a family-market repost pool with softer parent-aware templates for `подруга-мама`, `мама-подруга`, parents, and children.

## Follow-up Actions

- [ ] Repair event `5951` managed VK post media after deploy.
- [ ] Remove or replace the stale event `5951` afishaengagement shadow copy after deploy.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Regression tests pin partial media fail-closed behavior and family-market CTA selection.
