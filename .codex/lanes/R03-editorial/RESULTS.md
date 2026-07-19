# R03-editorial results

- **Status:** Done
- **Requirement IDs:** R03
- **Branch:** `agent/static-event-continuation-parity/editorial`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/static-event-continuation-parity-editorial`
- **Base SHA:** `a4faf9a2a2c4d2f9318b3a534b6fb3e57993aa54`
- **Implementation head SHA:** `3f061c08f47d5103d9fd118c913774523a728868`

## Outcome

- Removed the thumbnail-rail prerequisite from the non-OCR Editorial CTA layout and motion updates.
- Added one shared geometry resolver: a present rail keeps the accepted `rail bottom + 12px` dock anchor, while a rail-less one-photo Editorial page docks beneath the sticky header and still runs the same `hold` / `join` / `docked` / `release` state machine.
- Kept rail rendering and autorotation eligibility optional; no keyboard or keydown behavior changed.
- Added targeted presentation, pure geometry, and runtime-source contract tests for one-photo/multi-photo parity.

## Changed files

- `site/src/components/DesktopEventPage.astro`
- `site/src/lib/desktopEventPresentation.ts`
- `site/tests/desktop-editorial-motion.test.mjs`
- `.codex/lanes/R03-editorial/RESULTS.md` (lane evidence only)

## Commands and tests

```text
git diff --check
node --experimental-strip-types --test \
  tests/desktop-editorial-motion.test.mjs \
  tests/desktop-event-cta.test.mjs \
  tests/event-gallery-interactions.test.mjs \
  tests/event-media-quality.test.mjs
```

Result: `19/19` tests passed, `0` failed. No full static catalog build or publish was run.

## Risks

- Targeted contracts exercise router parity, the exact rail/no-rail geometry calculation, and runtime integration guards. A catalog build and browser screenshot matrix were intentionally excluded by lane scope.
- The no-rail dock anchor is the sticky-header fallback already used by the accepted Editorial companion-arrival behavior; the multi-photo rail anchor remains unchanged.
- Canonical documentation and `CHANGELOG.md` were outside this lane's writable scope and must be handled by the integration/documentation owner.

## Merge notes

- Merge implementation commit `3f061c08f47d5103d9fd118c913774523a728868` plus the following lane-results metadata commit.
- No edits were made to `EventLayout`, `EventCard`, `PersonalFeedSlot`, keyboard shortcuts, or keydown handlers.
