# R12-SATURATION results

## Status

Done. Requirement `R12-01` is implemented and verified.

- Branch: `agent/unified-r12/saturation`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/r12-saturation`
- Base: `69dad0ae38b0f89ced776c2a7faf749bd3c9fbc1`
- Final commit: reported in the handoff because recording it here would change the SHA.

## Root cause on the published R11 preview

The published preview is immutable and its Today surface still carries
`data-mobile-listing-date="2026-07-24"`. At a controlled real clock of
`2026-07-26 16:30 Europe/Kaliningrad`, R11 left real no-`end_at` rows such as
`#7042`, `#6870`, `#6977`, and `#6967` in `current` with computed image
`filter:none; opacity:1`.

R11's runtime condition only classified no-end rows when
`kaliningradDate === listingDate`. After midnight, that equality is false, so an
old immutable Today preview never de-emphasized those rows. The existing R11
browser block replaced the DOM listing date and timestamps with synthetic
values, masking this real build-clock/data interaction.

Reproduction evidence is kept outside Git in:

- `artifacts/codex/r12-saturation/published-r11-clock-reproduction.json`

## Fix

`MobileListingRailSurface.astro` now treats a valid listing date strictly older
than the Kaliningrad calendar date as elapsed for rows without an explicit end.
Explicit ends retain their timestamp truth, including an explicit future end;
same-day no-end rows retain the accepted one-hour `started-earlier` threshold.
The existing CSS remains unchanged and mobile-scoped, so only main media is
muted and the accepted rail/desktop surface are unchanged.

## Real-data browser acceptance

Added `site/tests/today-temporal-media.playwright.mjs` using untouched snapshot
rows dated `2026-07-26`:

- `#7018` (`10:00`, no end): `started-earlier`, main media computed
  `grayscale(0.72) saturate(0.32)`, opacity `0.46`;
- `#6956` (`12:00–14:00`): vivid at controlled 13:30 Kaliningrad, then `past`
  and muted at 14:30 after advancing the browser clock without rebuilding;
- `#7043` (`19:00`, no end): remains `current`, `filter:none`, opacity `1`;
- after midnight, the immutable 26 July surface marks its no-end rows `past`;
- at 1366px, the hidden mobile duplicate has no applied filter/opacity change,
  proving the new visual rule does not leak into desktop.

Visual evidence (ignored artifacts):

- `artifacts/codex/r12-saturation/today-ended-6956-mobile.png`
- `artifacts/codex/r12-saturation/today-future-7043-mobile.png`

## Commands and results

```text
curl -sS https://kenigevents.ru/preview-20260724-unified-corrections-r11/segodnya/
```

Result: HTTP 200; published listing date is `2026-07-24`.

```text
node <public R11 Playwright clock/evidence probe>
```

Result: reproduced real rows as `current`, `filter:none`, `opacity:1` at the
controlled 26 July clock; JSON artifact written above.

```text
PUBLIC_STATIC_SITE_CURRENT_DATE=2026-07-26 \
PUBLIC_STATIC_SITE_REFERENCE_ISO=2026-07-26T11:30:00.000Z \
PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail npm run dev -- --port 4312
```

Result: Astro dev server ready and served the real 26 July Today data.

```text
node --test tests/mobile-listing-rails.test.mjs
```

Result: 9 passed, 0 failed. This retains the accepted 112px full-viewport rail,
gestures, media behavior, and mobile-only temporal selector contract.

```text
R12_TODAY_BASE_URL=http://127.0.0.1:4312 \
R12_SCREENSHOT_DIR=../artifacts/codex/r12-saturation \
node tests/today-temporal-media.playwright.mjs
```

Result: passed for real 2026-07-26 events at controlled Kaliningrad times;
ended/started media muted, future vivid, desktop isolated.

```text
git diff --check
```

Result: passed.

## Scope and risks

- No docs, `CHANGELOG.md`, medallions, row geometry, CSS values, or desktop
  listing files were changed; canonical docs/changelog remain integrator-owned.
- The stale-day fallback is intentionally limited to no-`end_at` rows. A row
  with a trustworthy future explicit end remains current even if its listing
  date is older.
- Date-only rows cannot be called completed during their own day; they become
  past only after the whole listing calendar day has elapsed.
- A broad pre-existing R11 Playwright script was started under the 26 July
  clock, but its unrelated hardcoded 24 July canaries are not valid under that
  route window; it was not used as evidence. The focused real-date browser gate
  and the accepted-rail static suite are the targeted gates for this lane.
- The lane-local dependency directory was removed after validation and replaced
  with the existing shared `/home/dev/projects/events-bot-new/site/node_modules`
  symlink to avoid duplicate disk use.
