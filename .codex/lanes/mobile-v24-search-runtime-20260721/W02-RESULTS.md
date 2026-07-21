# W02 results — mobile shell and toast

- Lane: `W02`
- Requirements: `R03`, `R05`
- Base SHA: `7fd31c1bd1cd5d700986781c7431d83d04ae5ed8`
- Head SHA: commit containing this report (`git rev-parse HEAD` after lane commit)
- Branch: `agent/mobile-v24/mobile-shell`
- Status: complete

## Delivered

- Added the shared `MobileBottomNav` contract with `afisha | dates | search | personal | null`, one prop-owned `aria-current`, retained preview URL routing, 64px dock, and no blur/scroll hiding/`:has()` ownership.
- Made `EventLayout` the sole shell owner: route/prop section mapping, top/bottom modes, safe-area variables and body padding, shared nav mount, drawer current-state reflection, and immersive detail nav exclusion.
- Removed Search and collection duplicate nav mounts while preserving Search content/auth.
- Added one `MobileToastRegion` immediately after the header with global API + CustomEvent, one visible toast, bounded FIFO, same-key replacement, persistent error/action states, guarded timers, pause/resume, drawer/lifecycle cleanup, live-region separation, reduced-motion behavior, and the required top geometry/countdown direction.
- Migrated keyboard visual feedback to the shared toast while retaining its SR status. Added mobile share/copy and phone-copy producers without migrating inline Search/auth/progress feedback or local like/calendar/consent feedback.
- Guarded share/phone reset timers against stale completion and page lifecycle loss.
- Updated the browser release gate selector and owned keyboard/static shell tests.

## Evidence / commands

- `npm ci --ignore-scripts` — dependencies installed locally for verification only.
- `npm run build` — PASS (Astro static build; all routes generated).
- `node --test tests/mobile-shell-toast.test.mjs tests/keyboard-event-navigation-production.test.mjs tests/visual-keyboard-regressions.test.mjs` — PASS, 26/26.
- 390x844, DPR 3 Playwright smoke against built output — PASS:
  - `/poisk/`: `search`, one nav, one current item, one toast region.
  - `/segodnya/`: `dates`, one nav, one current item, one toast region.
  - `/populyarnoe/`: `afisha`, one nav, one current item, one toast region.
  - event `6408`: `none`, zero nav, one toast region, bottom mode `cta`.
  - 1s toast remained during pointer pause and expired after resume.
- `git diff --check` — PASS.

## Regression evidence

`INC-2026-07-20-static-event-keyboard-visual-regressions` was treated as a regression contract because keyboard feedback/browser gate files changed. The extracted keyboard-router production tests and visual keyboard regression tests pass; built event `6408` retains immersive CTA ownership with no bottom nav.

## Known integration follow-up / risks

- `site/tests/search-learning.test.mjs` is outside W02 writable scope and its old last subtest still expects manual `MobileSearchBottomNav` mounts plus `body:has`. The integrator must update that stale assertion to the new `EventLayout mobileSection="search"`/shared-nav contract. Its other 7 subtests pass.
- Full release browser gate was not run; its owned toast selectors were updated and the focused 390x844 browser smoke passed.

## Changed files

- `.codex/lanes/mobile-v24-search-runtime-20260721/W02-RESULTS.md`
- `site/scripts/check-browser-release-gate.mjs`
- `site/src/components/DesktopEventActionPanel.astro`
- `site/src/components/KeyboardEventNavigationPrototype.astro`
- `site/src/components/MobileBottomNav.astro`
- `site/src/components/MobileSearchBottomNav.astro`
- `site/src/components/MobileToastRegion.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/keyboardEventNavigation.mjs`
- `site/src/pages/podborki/[slug]/index.astro`
- `site/src/pages/poisk/index.astro`
- `site/tests/keyboard-event-navigation-production.test.mjs`
- `site/tests/mobile-shell-toast.test.mjs`
