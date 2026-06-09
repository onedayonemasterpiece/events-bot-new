# INC-2026-06-09-event-media-duplicates

Status: monitoring
Severity: sev2
Service: `events-bot-new` event media publishing
Opened: 2026-06-09
Closed: —
Owners: events-bot maintainer
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-04-tg-monitoring-media-and-digest-quality`
Related docs: `docs/features/vk-publishing/README.md`, `docs/features/tg-publishing/README.md`

## Summary

On 2026-06-09 outgoing event media appeared with duplicate images on Telegram and VK, and one postponed managed VK event post was created without photo attachments.

Examples:

- Telegram `@kldevents` post `83` / event `5777` had four media URLs where two were managed `p/dh16/...webp` copies and two were direct VK CDN mirrors of the same posters.
- VK post `wall-231920894_2630` / event `5783` was a promo publication for `Великие учителя...`; the event carried both a managed storage URL and a VK CDN mirror, and promo VK publication did not share the VK source-post media dedup path.
- VK postponed post `wall-231920894_2631` / event `5282` was not a promo exposure. It was an ordinary managed VK source post for `Благотворительный концерт` from `Стендап клуб Локация`; production DB had `photo_count=2`, but the postponed VK post had `attachments=0`.

The owner requested prevention only; no public repair/catch-up publication is part of this incident.

## Root Cause

`Event.photo_urls` could contain both the canonical managed-storage image and the original source CDN image for the same poster. Some publishing paths only deduped exact URLs or managed `/p/dh16/...` hashes, so mixed managed/source mirrors survived into Telegraph rendering, Telegram event albums, and promo VK posts.

For ordinary managed VK source posts, `sync_vk_source_post` only blocked text-only creation for Telegram-origin events with missing media. VK-origin events with non-empty `photo_urls` could attempt photo upload, receive zero VK attachments, and still create a new text-only postponed wall post.

## Corrective Actions

- Canonicalize `Event.photo_urls` from persisted `EventPoster` rows at Smart Update time, preferring managed media when a managed-storage URL and source-CDN mirror represent the same image.
- Keep visual event-photo dedup as a final guard across Telegram event publishing, managed VK source posts, promo VK source posts, and Telegraph render persistence.
- Preserve the existing local/test group-token-only behavior, but in production-like upload paths fail closed when event media was available and VK upload produced zero attachments.
- Keep promo VK fail-closed behavior for Telegram-origin no-media events and extend it to cases where any promo event had media URLs but upload produced no attachments.

## Verification

- `/tmp/events-bot-test-venv/bin/python -m pytest tests/test_tg_event_publish.py tests/test_promo.py tests/test_vk_source.py -q` -> `85 passed`
- `/tmp/events-bot-test-venv/bin/python -m pytest tests/test_genai_dump_and_poster_dedup.py::test_apply_posters_dedupes_legacy_photo_urls_by_phash tests/test_genai_dump_and_poster_dedup.py::test_apply_posters_backfills_eventposter_phash_and_prunes_duplicate_rows tests/test_genai_dump_and_poster_dedup.py::test_apply_posters_prefers_persisted_managed_url_over_source_mirror tests/test_tg_event_publish.py::test_tg_event_publish_dedupes_managed_and_vk_cdn_mirror tests/test_promo.py::test_promo_vk_publication_dedupes_mirrors_and_blocks_empty_upload tests/test_vk_source.py::test_sync_vk_source_post_blocks_vk_origin_when_available_media_uploads_empty tests/test_vk_source.py::test_sync_vk_source_post_skips_group_only_photo_upload -q` -> `7 passed`

## Prevention

- Regression coverage now includes:
  - Telegram event publishing deduping a managed `p/dh16` URL plus a VK CDN mirror into one photo.
  - Smart Update replacing a persisted source mirror URL with the managed URL from the same `EventPoster` row.
  - Promo VK publication deduping the same mirror pair and failing before `wall.post` if upload returns no attachments.
  - Ordinary VK source publishing for `Благотворительный концерт`-style VK-origin events failing before `wall.post` when media exists but upload returns no attachments.
