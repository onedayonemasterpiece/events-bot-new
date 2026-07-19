# Lane R04-cards Results

## Status

committed

## Requirement IDs

- R04-CANONICAL
- R04-DESKTOP-BOUNDARY
- R04-TESTS
- R04-DOCS (integration-owned; no lane docs/CHANGELOG edits by explicit root instruction)

## Branch

`agent/static-event-continuation-parity/cards`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-event-continuation-parity-cards`

## Base SHA

`a4faf9a2a2c4d2f9318b3a534b6fb3e57993aa54`

## Head SHA

Initial implementation: `f098bd807e95418f7a8a63a60d8330e2bc2d56b0`.
Reviewer follow-up implementation: `01971be9a324e76f25c6d4d50083373c7b9f007e`.
The final lane head additionally contains evidence commits; resolve it with
`git rev-parse agent/static-event-continuation-parity/cards`.

## Files changed

- `site/src/components/EventCard.astro`
- `site/src/components/AuthorizedEventSearch.astro`
- `site/src/components/PersonalFeedSlot.astro`
- `site/src/layouts/EventLayout.astro`
- `site/tests/event-continuation-contract.test.mjs`
- `site/tests/personal-feed-surface.test.mjs`
- `.codex/lanes/R04-cards/RESULTS.md`

Forbidden-file audit: `site/src/components/DesktopEventPage.astro` was not edited.
No generated catalog/build artifact was created.

## Result

- Removed the handwritten `eventCardHtml()` DOM/SVG implementation from
  `EventLayout.astro`.
- `EventLayout` now renders inert `EventCard.astro` templates for both supported
  variants. Runtime discovery, personal-feed continuation, and compatibility
  search rendering clone that canonical DOM and populate text, datasets, links,
  media, counts, calendar state and accessibility labels without interpreting
  manifest text as HTML.
- Runtime URLs are restricted to HTTP(S). Template media uses `withBase()` and
  template calendar markup uses an explicit inert `#` href, preventing preview/
  secret-candidate root-isolation leaks.
- Internal continuation/discovery append cloned elements directly. The existing
  authorized-search compatibility API serializes the safely populated clone,
  preserving that caller without reintroducing a second card implementation.
- Desktop still has a visibly separate `Ещё события` sibling after the desktop
  detail/similar-events main surface. It remains capped at six cards (two rows at
  the three-column desktop layout), has no event-detail load-more control,
  excludes already offered/current/recent items, and enforces category/venue
  caps that broaden beyond a same-type/theatre bubble.
- The separate event-detail broad module is hidden below 1024px for every mode, including mature `personal`; gallery/keyboard handling was not changed.
- Mature desktop personalization is headed `По вашим интересам`; the non-personal broad fallback remains `Ещё события`.
- Authorized search no longer carries a handwritten EventCard lookalike. If the canonical renderer is unavailable or yields no cards, it renders an explicit non-card failure status instead.

All optional fields present in the compact runtime candidate projection have a
corresponding node in the inert template (image/fallback, type, meta, status,
place, calendar and feedback/share controls). `otherTimeLabels` remains a purely
static `PreviewEvent` feature because the compact runtime manifest does not
carry linked-session data; runtime code does not attempt to populate it.

## Commands run

- `node --test site/tests/event-continuation-contract.test.mjs`
- `node --test --test-name-pattern='personal feed keeps|event-detail continuation uses|personal feed endpoint|runtime cards|desktop keeps' site/tests/personal-feed-surface.test.mjs`
- reviewer follow-up: `node --test --test-name-pattern='personal feed keeps|event-detail continuation uses|personal feed endpoint|runtime cards|mature personalization|desktop keeps' site/tests/personal-feed-surface.test.mjs`
- `git diff --check`
- forbidden-file/name/status audits with `git status`, `git diff --name-only`, and `grep`
- `npm exec astro -- --version`
- attempted `astro check` dependency bootstrap (see verification limitation)

## Tests / verification

- PASS: continuation contract — 3/3 tests.
  - stable OCR-safe rows;
  - current/prior/recent/rejected exclusion and dedupe;
  - finite six-card cap with same-type escape and four-category breadth fixture.
- PASS: initial targeted personal-feed/card source contracts — 5/5 tests.
- PASS: reviewer follow-up personal-feed/card/search source contracts — 6/6 tests.
  - canonical `EventCard` templates and shared interaction hooks;
  - safe clone/text/dataset/URL population with no handwritten card HTML/SVG;
  - separate desktop similar and broad-discovery sections;
  - finite cap/no load-more/dedupe source contracts;
  - mature mobile exclusion and desktop heading split;
  - authorized-search canonical-renderer-only card path and explicit non-card failure state.
- PASS: `git diff --check`.
- PASS: forbidden `DesktopEventPage.astro` unchanged.
- NOT RUN by design: full catalog/Astro build, explicitly forbidden by lane scope.
- NOT RUN: complete `personal-feed-surface.test.mjs` contains a built-manifest
  assertion requiring `site/dist`; no build output existed and creating a full
  catalog was forbidden. Its five source-only tests were selected and passed.
- Astro type-check could not be completed because this isolated worktree has no
  local `site/node_modules`; `astro check` requested `@astrojs/check` and
  TypeScript installation, while an ephemeral npm attempt did not attach the
  package to Astro's project resolver. No repo dependency/lockfile was changed.

## Risks

- Every page using `EventLayout` now contains two inert canonical card templates;
  this is deliberate so authorized search pages without `PersonalFeedSlot` also
  have the shared renderer, but it adds a bounded amount of inert HTML.
- Browser/build-level interaction should be covered by the integration owner's
  normal checked candidate/build gate because this lane was prohibited from
  running a full catalog build.
- Canonical documentation and `CHANGELOG.md` are integration-owned and must be
  updated during merge, per the root agent's explicit instruction.

## Merge notes

Cherry-pick initial implementation `f098bd807e95418f7a8a63a60d8330e2bc2d56b0`,
initial evidence `738d7132313099ea1cab8ab42dc00d1211e39686`, reviewer follow-up
`01971be9a324e76f25c6d4d50083373c7b9f007e`, and the following RESULTS update,
or squash the lane range. No dependency on
`DesktopEventPage.astro`; preserve any concurrent desktop-template work. During
integration, run the normal Astro/check-preview/browser gate from an installed
site workspace and add the integration-owned docs/CHANGELOG entry.
