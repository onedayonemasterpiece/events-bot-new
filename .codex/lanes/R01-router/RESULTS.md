# R01 — Router productionization

## Outcome

Implemented the reviewed V7 keyboard-navigation router as a shared, reversible module and mounted it on secret-candidate event pages behind `PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED` (enabled by default only in that mode). The production wrapper suppresses the prototype teaching panel and does not autofocus.

## Owned files

- `site/src/lib/keyboardEventNavigation.mjs`
- `site/src/components/KeyboardEventNavigationPrototype.astro`
- `site/src/components/KeyboardEventNavigation.astro`
- `site/src/pages/sobytiya/[slug].astro`
- `site/scripts/check-keyboard-event-navigation-playwright.sh`
- `site/tests/keyboard-event-navigation-production.test.mjs`
- `.codex/lanes/R01-router/RESULTS.md`

## Validation

- PASS: `node --check site/src/lib/keyboardEventNavigation.mjs`
- PASS: `bash -n site/scripts/check-keyboard-event-navigation-playwright.sh`
- PASS (15/15): production keyboard tests plus gallery, desktop CTA, and continuation contract tests.
- PASS: regular Astro build, 381 pages.
- PASS: secret-candidate Astro build with `PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED=1` and `SITE_BASE_PATH=/_review/r01`, 381 pages.
- PASS: full Chromium browser gate for event 6408 (split/multi-image) and event 6593 (editorial/single-image), including no-autofocus, ArrowDown/gallery/card routing, Cyrillic physical-key shortcuts, editable/IME exclusions, no horizontal overflow, and lifecycle destroy.
- PASS: generated secret-candidate HTML contains the keyboard asset/contract, retains the review base prefix, and uses `noindex,nofollow,noarchive,nosnippet`.

## Blockers / integration notes

- Firefox and WebKit browser launches are blocked by missing host shared libraries. The gate is engine-configurable and reached the launch step, but the host lacks the GTK/Cairo dependencies required by Firefox and Wayland/Manette/Enchant/Hyphen/Secret/GLES/x264 dependencies required by WebKit. Chromium coverage is complete; no blind dependency retry was made.
- A broader existing runtime suite had one unrelated failure in `event-detail-runtime-regressions.test.mjs`: it expects `event-card__media-shell--dynamic is-image-loading` in `EventLayout.astro`. `EventLayout.astro` is outside R01 ownership and was not modified. All other tests in that invocation passed (21 pass, 1 fail).
- Documentation and `CHANGELOG.md` are integration-lane responsibilities and were intentionally not changed here.
