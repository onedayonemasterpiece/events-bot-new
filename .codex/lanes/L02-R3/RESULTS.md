# L02 Results — medallion audit and static-site coverage

## Lane contract

- Lane ID: `L02`
- Requirement IDs: `R02`, `R03`, `R04`
- Base SHA: `68576d5b70f57164c00386b05cff126586c3f700`
- Implementation head SHA: `bf99b6641d15e541c54d57d4eeb4d37b64ab0680`
- Final lane HEAD: the metadata-only commit containing this file; resolve with
  `git rev-parse HEAD` at handoff.
- Writable scope used:
  - `site/src/data/*Medallions.json`
  - `site/src/lib/eventMedallions.ts`
  - `site/src/components/EventTokenMedallions.astro`
  - `site/src/pages/lab/medallions/index.astro`
  - medallion runtime/source assets and source provenance READMEs
  - `scripts/audit_event_medallions.mjs`
  - medallion Node and Playwright tests
  - `artifacts/codex/` (ignored evidence only)
  - this result record
- Forbidden/integrator-owned files left untouched: `CHANGELOG.md` and canonical
  `docs/`.

## Outcome

- Audited every organizer and festival manifest entry against read-only production
  snapshots using both the real static resolver and the real Telegram graphic
  resolver.
- Generated a 38-entry Markdown/CSV ledger: 28 entries used by current canonical
  events, 10 unused by current events, and 0 unreachable assets.
- Enabled structured festival medallions on real event-detail pages, while keeping
  KGD80's event-detail/Telegram asset split and suppressing duplicate generic
  festival pills.
- Added `venue_address` as structured venue evidence below `venue_name`, repairing
  current event 6997 (`Остров Канта`) without title/description inference.
- Repaired Telegram-compatible raster fallbacks for eight SVG-backed identities:
  Dramteatr39, Yantar Hall, Dom Iskusstv, Mumod, Kaup, Kaliningrad Street Food,
  Grozd, and More Vnutri. The fallbacks are deterministic 512×512 RGBA CairoSVG
  renders and their provenance is recorded beside the source assets.
- Added grounded festival aliases and restored current festival matches, including
  Grozd (6994), More Vnutri (4211/6871/6983), and Bahosluzhenie (6153/6900).
- Preserved Unicode token boundaries, ambiguous venue fail-closed behavior,
  conflicting structured source fail-closed behavior, and special handling for
  Pushkin Card, free admission, MEOW, and RZD Lastochka.
- Added exact catalog selectors and desktop/mobile browser acceptance coverage.

## Production audit evidence

All files below are ignored and intentionally not committed:

- `artifacts/codex/L02-medallion-audit/prod-current-events-20260723.json`
  - Fly app `events-bot-new-wngqia`, `/data/db.sqlite`, read-only SQL
  - queried `2026-07-23 09:44:22 UTC`
  - 291 current active canonical events, `PRAGMA quick_check=ok`
  - SHA-256 `5a7d10753ed4f1fc7bcd7906f60e86fb3865d0e14e147f32739d1ec68592b706`
- `artifacts/codex/L02-medallion-audit/prod-event-history-20260723.json`
  - queried `2026-07-23 09:44:39 UTC`
  - 6,624 retained canonical events, `PRAGMA quick_check=ok`
  - SHA-256 `ac00535476ab7c82001c8e6d62b0620c15b8b7f4a25683ae57dbedd7606c2629`
- `artifacts/codex/L02-medallion-audit/medallion-usage-audit.md`
  - SHA-256 `635722a10d56e038a17fe2c9f61b7442f7dbef4c34516a0c0805d5456ec73c85`
- `artifacts/codex/L02-medallion-audit/medallion-usage-audit.csv`
  - 39 lines including header
  - SHA-256 `060deb49649c91e0db3696e014f12db44a3f1ef41f88212fe8dc20dea23902be`
- Visual evidence:
  - `artifacts/codex/L02-medallion-audit/generated-svg-fallbacks-contact-sheet.png`
  - `artifacts/codex/L02-medallion-audit/playwright/desktop-1440-full.png`
  - `artifacts/codex/L02-medallion-audit/playwright/mobile-390-full.png`

Current unused inventory is explicit in the ledger: `act-opus`,
`kantata-festival`, `brachert`, `greza-khutor`, `kaliningrad-city-jazz`,
`koroche`, `ostrova`, `simfoniya-vetra`, `kaliningrad-street-food`, and
`tolkin-fest`. The ledger distinguishes dormant historical identities from
never-used lab specimens.

## Commands and validation

```bash
# Production-backed audit
node --experimental-strip-types scripts/audit_event_medallions.mjs \
  --current artifacts/codex/L02-medallion-audit/prod-current-events-20260723.json \
  --history artifacts/codex/L02-medallion-audit/prod-event-history-20260723.json \
  --output artifacts/codex/L02-medallion-audit
# organizer_entries=27, festival_entries=11, used_current=28,
# unused_current=10, unreachable=0

# Static resolver, inventory, and existing behavior regressions
node --experimental-strip-types --test \
  site/src/lib/event-medallions.test.mjs \
  site/scripts/content-media.behavior.test.mjs
# 13/13 passed

# Telegram resolver and incident regressions, including short-token and
# MEOW/KGD behavior
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_tg_event_publish.py \
  -k 'tg_graphic_medallions or tg_medallions_do_not_match_short_acronym or tg_medallions_match_short_acronym or tg_medallions_match_venue_aliases_only or tg_medallions_do_not_add_festival_badge'
# 8 passed, 90 deselected

# Preview acceptance
PREVIEW_BUILD_ID=preview-l02-medallion-audit-20260723 \
  npm --prefix site run build:preview
# 383 pages built
PREVIEW_BUILD_ID=preview-l02-medallion-audit-20260723 \
  npm --prefix site run check:preview
# passed: 303 events, strict_related=false
PREVIEW_BUILD_ID=preview-l02-medallion-audit-20260723 \
  site/node_modules/.bin/playwright test \
  tests/playwright/medallion_catalog.spec.mjs --reporter=line
# 4/4 passed in 29.5s at 1440×1100 and 390×844

git diff --check
# passed
```

The browser acceptance checked exact catalog order/counts, all image loads,
horizontal overflow, and real event pages 4211, 6153, 6529, 5756, 6796, 698,
and 6911. Event 6911 correctly had no MEOW token because the final exported
source-count guard excluded it.

## Risks and integration notes

- The audit lists structured cross-surface review items rather than silently
  normalizing them. `kgd80` vs `kgd80-80-stories` is an intentional per-surface
  asset split. Signal's three Telegram-only source-identity matches
  (6835/6967/6982) remain a review item and were not broadened into static
  matching.
- Festival matching is deliberately limited to the structured `festival` field;
  venue/title/description prose is not used.
- The integrator must add the required canonical documentation and
  `[Unreleased]` changelog entry because those files are explicitly outside this
  lane.

## Changed files

- `.codex/lanes/L02/RESULTS.md`
- `scripts/audit_event_medallions.mjs`
- `site/public/assets/festivals/grozd-festival.png`
- `site/public/assets/festivals/kaliningrad-street-food.png`
- `site/public/assets/festivals/kaup.png`
- `site/public/assets/festivals/more-vnutri.png`
- `site/public/assets/organizers/dom-iskusstv.png`
- `site/public/assets/organizers/dramteatr39.png`
- `site/public/assets/organizers/mumod.png`
- `site/public/assets/organizers/yantar-hall.png`
- `site/src/assets/festivals/source/README.md`
- `site/src/assets/organizers/source/README.md`
- `site/src/components/EventTokenMedallions.astro`
- `site/src/data/festivalMedallions.json`
- `site/src/data/organizerMedallions.json`
- `site/src/lib/event-medallions.test.mjs`
- `site/src/lib/eventMedallions.ts`
- `site/src/pages/lab/medallions/index.astro`
- `tests/playwright/medallion_catalog.spec.mjs`
