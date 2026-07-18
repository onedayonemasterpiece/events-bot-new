# Lane static-event-v13-gallery-nav Results

## Status
committed

## Requirement IDs
- R05
- R06

## Branch
`agent/static-event-v13/gallery-nav`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-event-v13-gallery-nav`

## Base SHA
`dc7b1dbc63643b1a5f72f64bc7305154abbb96ce`

## Head SHA
Implementation commit: `f2c930be96a81b45229f305bc8a28588af1347d9`.

The final lane head additionally contains this evidence file; resolve it with
`git rev-parse agent/static-event-v13/gallery-nav` after the evidence commit.

## Files changed
- `site/src/layouts/EventLayout.astro`
- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventHero.astro`
- `site/tests/event-gallery-interactions.test.mjs`
- `site/tests/event-gallery-interactions.playwright.js`
- `.codex/lanes/static-event-v13-gallery-nav/RESULTS.md`

## Implementation
- Standard fullscreen galleries no longer pointer-capture link/button/CTA-card
  gestures. Non-interactive image/backdrop gestures retain pointer capture and
  swipe tracking, so desktop genuine image/backdrop click-to-close remains intact.
- Touch fallback now prevents default only while a non-interactive swipe is
  actually tracked; mobile CTA links remain normal navigation targets.
- Desktop closed-page multi-image heroes consume unmodified left/right arrows
  only while the media frame is hovered or contains focus. Input/editable targets,
  modifier chords, the standard fullscreen gallery and the efficient portrait
  viewer are excluded. The current hero, opener index and polite live status stay
  synchronized.
- Reduced-motion disables gallery pan and timed advance, including a live media
  preference change. Inactive standard-slideshow controls receive `tabindex=-1`,
  counters are polite statuses, and the standard modal has a contained Tab loop.
- Existing fullscreen arrow keys and the separate efficient portrait viewer's
  keyboard navigation remain in place.

## Commands run
- `npm ci`
- `npm run build`
- `node --test tests/event-gallery-interactions.test.mjs tests/event-detail-runtime-regressions.test.mjs`
- `python3 -m http.server 47321 --directory dist`
- `playwright-cli -s=gallery-nav open http://127.0.0.1:47321/ --browser=chromium`
- `playwright-cli -s=gallery-nav --raw run-code --filename tests/event-gallery-interactions.playwright.js`
- `git diff --check`

## Tests / verification
- PASS: Astro production-form build completed: 380 pages in 2m50s.
- PASS: focused plus incident regression Node suite: 15/15.
- PASS: Playwright on built pages:
  - event 5755 closed hero arrow/status/no-scroll/modifier/input guards;
  - event 6408 closed hero arrow/status/no-scroll/modifier/input guards;
  - event 4783 closed hero arrow/status/no-scroll/modifier/input guards;
  - desktop standard gallery genuine image click closes;
  - reduced-motion standard gallery starts with timed advance/pan disabled;
  - inactive CTA is untabbable and restores tabbability when active;
  - desktop and 390px mobile terminal CTA links navigate.
- PASS output: `[{"eventId":5755,"closedHeroArrow":"pass"},{"eventId":6408,"closedHeroArrow":"pass"},{"eventId":4783,"closedHeroArrow":"pass"},{"desktopStandardGallery":"pass","mobileStandardGallery":"pass"}]`.
- PASS: `git diff --check`.

## Risks
- The full Astro/Playwright run preceded the final additive live-reduced-motion
  listener and CTA-card keep-open selector. Those final lines are covered by the
  post-change focused source suite; integration should replay the checked candidate
  browser gate.
- The closed hero swaps among the already admitted fullscreen image set. It does
  not change media admission, semantic routing or recommendation selection.
- Canonical docs, `CHANGELOG.md` and release files were intentionally excluded by
  lane ownership and remain integration-owned.

## Merge notes
- Cherry-pick the implementation and following evidence commits together.
- No recommendation, documentation, changelog or release files were changed.
- Regression contract consulted: `INC-2026-07-16-static-event-media-action-regressions`
  (gallery navigation and desktop click/backdrop close surface).
