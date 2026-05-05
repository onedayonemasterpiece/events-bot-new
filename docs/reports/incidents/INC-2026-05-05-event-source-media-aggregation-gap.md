# INC-2026-05-05 Event Source Media Aggregation Gap

Status: mitigated
Severity: sev3
Service: Telegraph event pages / source media
Opened: 2026-05-05
Closed: —
Owners: events-bot
Related incidents: `INC-2026-04-27-cherryflash-missing-photo-urls`, `INC-2026-05-05-kitoboya-garage-date`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/operations/supabase-storage.md`

## Summary

The event page for `Выставка «Куплю гараж. Калининград»` (`event #4517`, `https://telegra.ph/Vystavka-Kuplyu-garazh-Kaliningrad-05-03`) showed `4 источника` but only one event image. Source-by-source audit showed three unique image assets were available across those sources: one VK image shared by two VK sources, one Telegram image already stored, and one additional Telegram image from `https://t.me/domkitoboya/3193`.

## User / Business Impact

- Telegraph pages under-represent source media when later/older sources are attached text-only.
- Operators see multiple sources in the footer but cannot visually inspect the extra source images on the event page.
- The same media gap can reduce renderability for video announcement inputs.

## Detection

- Reported by the user with `https://t.me/domkitoboya/3193` and the Telegraph page.
- Production snapshot showed `event #4517` had `photo_count=1`, one `eventposter`, and four `event_source` rows.
- Live source audit:
  - `vk.com/wall-148784347_6671`: 1 image;
  - `vk.com/wall-75964367_17779`: same repost image;
  - `t.me/domkitoboya/3191`: image/video thumbnail already represented by the existing stored image;
  - `t.me/domkitoboya/3193`: 1 additional image missing from the event.

## Timeline

- 2026-04-01..2026-05-05: four sources accumulated on `event #4517`.
- 2026-05-05: audit confirmed production had only one poster/photo URL.
- 2026-05-05: mitigation added Telegraph rebuild-time source media rehydration from already attached `event_source` rows.

## Root Cause

1. `event_source` stores source URL/text, but not a durable per-source media manifest.
2. If an older import or merge attached a source without posters, later Telegraph rebuilds only used `event.photo_urls`/`eventposter`; they did not rehydrate media from the source graph.
3. Existing Telegram/VK media fallbacks were mostly per-current-import, not per-existing-source repair.

## Automation Contract

### Treat as regression guard when

- changing Telegraph event rebuild image selection;
- changing Telegram/VK media fallback and poster persistence;
- changing `event_source` merge/idempotency behavior.

### Affected surfaces

- `main.py::update_telegraph_event_page`;
- `event_source`, `eventposter`, `event.photo_urls`;
- `source_parsing.telegram.handlers._fallback_fetch_posters_from_public_tg_page`;
- `vk_auto_queue.fetch_vk_post_text_and_photos`.

### Mandatory checks before closure or deploy

- Rehydrate `event #4517` on a production snapshot and confirm `photo_count` grows from 1 to 3 unique images.
- Confirm duplicate VK repost media is not counted twice.
- Confirm the source media repair does not run on single-source events and does not remove existing posters.

### Required evidence

- source-by-source media count ledger;
- focused pytest for event-source media rehydration;
- production post-deploy rebuild/backfill output for `event #4517`;
- deployed SHA reachable from `origin/main`.

## Immediate Mitigation

Added best-effort Telegraph rebuild repair: for multi-source events with only one stored image, rehydrate posters from each attached Telegram/VK source and persist missing unique images into `eventposter` and `event.photo_urls`.

## Corrective Actions

- Added `EVENT_SOURCE_MEDIA_REHYDRATE_ON_TELEGRAPH` (default on), `EVENT_SOURCE_MEDIA_REHYDRATE_MAX_SOURCES`, and `EVENT_SOURCE_MEDIA_REHYDRATE_PER_SOURCE_LIMIT`.
- Added regression coverage that multi-source event pages append missing source images and dedupe existing images.

## Follow-up Actions

- [ ] Consider storing per-source media manifests so future audits do not need live refetch.
- [ ] Decide whether video thumbnails should be first-class posters or only `event_media_asset` previews.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: pending deploy/backfill
- post-deploy verification: —

## Prevention

The source graph should be enough to repair event media after older import bugs; Telegraph rebuild now performs that best-effort repair instead of rendering only the current event row image list.
