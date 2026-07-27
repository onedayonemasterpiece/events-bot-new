# R14 Medallions Results

## Delivered

- Added bounded `Event.organizer_names` JSON persistence, migration, Smart Update extraction/merge, and production-preview export.
- Grounded LLM extraction requires an exact source quote; organizer names are unioned without erasing existing facts and capped at eight normalized values.
- Added explicit curated VK self-publisher bindings for Profi-Tour (`owner_id=12286984`) and Хранители руин (`owner_id=190663987`); unknown and aggregator sources fail closed.
- Updated detail, listing, audit, and Telegram graphical resolvers to use structured organizer identity rather than venue prose or event-ID shortcuts.
- Made Янтарь-холл and Дом искусств venue medallions listing-ready.
- Reclassified Хранители руин as an organizer and removed the `Железнодорожные ворота` venue alias that caused the #6767 false positive.
- Added a locally processed, source-faithful Profi-Tour medallion in WebP and PNG formats.

## Asset provenance

- Source: public official Telegram profile `https://t.me/excursions_profitour` (page title: `Экскурсии от «Профи-тур»`).
- Captured: 2026-07-27.
- Source file: `site/src/assets/organizers/source/r14-20260727/profitur.telegram-avatar-20260727.jpg`.
- Runtime files: `site/public/assets/organizers/profitur.webp` and `profitur.png`.
- Processing: local resize and circular alpha mask only. No paid/OpenAI image generation was used.

## Verification

- `python3 -m py_compile models.py db.py smart_event_update.py vk_intake.py site/scripts/export-production-preview-data.py` — passed.
- `node --test --experimental-strip-types site/src/lib/event-medallions.test.mjs site/src/lib/listing-medallions.test.mjs site/scripts/content-media.behavior.test.mjs` — 21 passed.
- `pytest -q tests/test_event_organizer_names.py tests/test_smart_update_native_schema.py` — 40 passed.
- `pytest -q tests/test_tg_event_publish.py -k graphic_medallions` — 4 passed, 94 deselected.
- Coverage includes #6667/#6484 venue positives, #6882/#7044 organizer positives, #6767 negative, unknown/aggregator fail-closed behavior, and DB/export persistence.
- Full `npm --prefix site run build:preview` — passed (432 pages). The chained `check:preview` then hit an unrelated date-sensitive canary: event #6939 was absent from the current `vyhodnye` snapshot.
- `git diff --check` — passed.

## Operational follow-up

After integration, production rows sourced from the curated VK communities (including #6882/#7018/#7044 as applicable) need a controlled backfill/re-ingest and a fresh static export/build so persisted `organizer_names` reaches generated site data. Generated preview JSON was intentionally not edited in this lane.
