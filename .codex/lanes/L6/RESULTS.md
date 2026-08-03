# L6 — Weather Calendar results

## Scope

- Lane: `L6`
- Requirement: `R4`
- Base SHA: `0bc8482dcda5cf16a89f312f9791ecbb6d0e9a3a`
- Implementation SHA: `23a50bf072b077e1eceba120e4f174e3483a915e`
- Branch: `agent/static-unified/l6-weather`
- Status: consumer implementation complete and default-off; producer and public
  rollout intentionally remain `NO-GO` behind the documented gates.

## Delivered

- Added exact `weather-calendar-v1` snapshot and
  `weather-calendar-pointer-v1` contracts, JSON Schemas, strict parsers and
  stale/date/horizon guards for `Europe/Kaliningrad`.
- Added a same-origin loader that fetches one atomic pointer and one immutable
  snapshot per page, bounds response sizes, validates paths and verifies SHA-256
  before parsing. It never calls a browser-side weather provider.
- Added a default-off (`PUBLIC_WEATHER_CALENDAR_ENABLED=1` to enable) weather
  context to date and weekend desktop/mobile surfaces. Weekend days retain
  separate date-keyed contexts. Weather does not affect SEO, ranking or event
  counts.
- Added partial city/coast/sea rendering, fail-closed stale/malformed handling,
  water-temperature boundary behavior (`16.0` hidden, `16.1` shown), reserved
  responsive space, accessibility text and reduced-motion handling.
- Added a visually reviewed CC0 SVGRepo outline family for clear/cloud/fog/rain/
  showers/heavy-rain/snow/thunderstorm/water-temperature. Provenance and the
  `currentColor` adaptation are recorded in `manifest.json`.
- Updated the canonical weather contract with the immutable producer handoff,
  rollout/canary gates, provider attribution and commercial/self-host usage-plan
  gate.

## Producer assessment and blocker evidence

A read-only shallow inspection of private `cat-weather-new` at
`82e834a7faaebe1acaf6dfd138d86b3dcd218ad9` found that the existing city/marine
flows use `forecast_days=2`, the sea cache is not date-keyed, exact production
location records live in the runtime database, and this checkout contains no
verified first-party atomic object-upload/readback adapter. Implementing a
seven-day producer there without those inputs would risk changing the existing
Telegram product or publishing an unverifiable pointer, so no producer branch or
remote write was made.

The exact safe producer interface is now in
`docs/features/static-site-pages/weather-calendar.md`: read-only location
revision/hash; separate seven-day exporter; Kaliningrad-time aggregation;
schema validation; immutable upload plus readback/hash verification; atomic
pointer written last; pointer unchanged on partial/failure; cold/warm/partial/
malformed/retention/live-smoke gates.

## Verification evidence

Commands run from `site/` unless stated otherwise:

1. `node --experimental-strip-types --test tests/weather-calendar-v1.test.ts tests/weather-calendar-surface.test.mjs`
   — 9/9 passed.
2. `node --test tests/mobile-listing-rails.test.mjs tests/preview-current-date.test.mjs`
   — 11/11 passed.
3. Python `jsonschema.Draft202012Validator` plus `FormatChecker` against both
   valid fixtures — snapshot and pointer passed.
4. `npm run build` — full Astro production build passed, 466 pages generated in
   about 5m34s. It retained one unrelated pre-existing Vite warning about
   inconsistent JSON import attributes in listing presentation.
5. Feature-on Astro dev server plus a pinned same-origin pointer/snapshot was
   probed with Playwright/Chromium at widths `320`, `390`, `720`, `1366`:
   `scrollWidth == clientWidth` at every width; CLS was respectively `0`, `0`,
   `0`, `0.0040608796`; exactly two weather requests occurred (pointer and
   immutable snapshot); direct provider requests were `0`.
6. Actual rendered surfaces were inspected at desktop and mobile, including the
   `+16.1` water boundary. Temporary evidence (not committed):
   `/tmp/weather-final-desktop.png`, `/tmp/weather-final-mobile.png`,
   `/tmp/weather-final-mobile-water.png`, and
   `/tmp/weather-icons-candidates/contact-sheet.png`.
7. `git diff --check` passed. Forbidden integration-owner paths
   `site/src/layouts/EventLayout.astro`, `CHANGELOG.md`, and `docs/routes.yml`
   have no lane diff.

## Risks / integration follow-up

- Release flag must remain off until a real producer, live pointer read-only
  smoke, accepted provider usage plan, and seven-day canary are complete.
- Exact production location revision/hash and the object publishing adapter are
  unresolved producer inputs.
- Open-Meteo's free API is documented as non-commercial; production must record
  a suitable commercial/customer endpoint or an approved self-host decision.
- The reserved loading slot meets the measured CLS gate on successful loads. A
  terminal provider failure hides the slot; the seven-day canary must still
  verify failure-path layout behavior on real pages.
- `CHANGELOG.md` and `docs/routes.yml` were forbidden in this lane and remain
  integration-owner follow-ups.
- No public publish, provider request, production data write, or paid image
  generation occurred.

## Changed files

- `docs/features/static-site-pages/weather-calendar.md`
- `site/public/assets/weather/{clear,cloud,fog,heavy-rain,rain,showers,snow,thunderstorm,water-temperature}.svg`
- `site/public/assets/weather/manifest.json`
- `site/src/components/WeatherDateContext.astro`
- `site/src/components/listings/DateListingSurface.astro`
- `site/src/components/listings/MobileListingRailSurface.astro`
- `site/src/components/listings/WeekendListingSurface.astro`
- `site/src/lib/weather-calendar-pointer-v1.schema.json`
- `site/src/lib/weather-calendar-v1.schema.json`
- `site/src/lib/weatherCalendar.ts`
- `site/src/lib/weatherCalendarRuntime.ts`
- `site/tests/fixtures/weather-calendar/weather-calendar-pointer-v1.valid.json`
- `site/tests/fixtures/weather-calendar/weather-calendar-v1.malformed.json`
- `site/tests/fixtures/weather-calendar/weather-calendar-v1.stale.json`
- `site/tests/fixtures/weather-calendar/weather-calendar-v1.valid.json`
- `site/tests/weather-calendar-surface.test.mjs`
- `site/tests/weather-calendar-v1.test.ts`
