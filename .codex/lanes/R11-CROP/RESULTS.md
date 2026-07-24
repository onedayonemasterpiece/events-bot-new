# R11-CROP Results

## Outcome

- Desktop personal-feed contexts now use `packRelatedCardRows` in bounded six-card chunks with three-card row geometry, bounded alternates, contiguous row coordinates, and no later row after an optimizer remainder.
- Resolved desktop layouts are passed into the canonical runtime `EventCard`.
- Non-desktop personal-feed cards use `resolveMobileEventCardMedia`, so visual-only media receives the fixed horizontal 5:4 cover treatment while OCR/document media remains natural/contained and fail-closed.
- Row/media/body normalization CSS now covers every desktop personal-feed surface rather than only event detail.
- `PersonalFeedSlot` now exposes the existing `.ke-personal-feed-slot` hook used by the mobile listing rail to suppress the generic feed.
- Focused source-contract coverage was added for desktop packing, mobile resolution, layout propagation, selector repair, generalized CSS, and incomplete-row protection.

## Git

- Base SHA: `7c34d29a2ad65fc6532d934a49d4d48604f79e82`
- Implementation SHA: `cc840e4cc56bbfe3c5ca01ecf37bfe75187f240d`
- Results head SHA: recorded by the final results commit.
- Branch: `agent/unified-r11/crop`

## Changed files

- `site/src/layouts/EventLayout.astro`
- `site/src/components/PersonalFeedSlot.astro`
- `site/tests/personal-feed-surface.test.mjs`
- `.codex/lanes/R11-CROP/RESULTS.md`

## Validation

Commands run:

1. `npm ci`
   - Passed; installed the locked site dependencies.
2. `npm run build:preview`
   - Passed; 431 pages built.
   - Preview build: `site/dist/preview-20260724t062032-7c34d29a/`.
3. `PREVIEW_BUILD_ID=preview-20260724t062032-7c34d29a node --test tests/personal-feed-surface.test.mjs tests/visual-keyboard-regressions.test.mjs tests/event-continuation-contract.test.mjs`
   - Passed: 37/37 tests.
   - This includes existing executable checks for OCR crop maximum, unknown/error fail-closed behavior, no fields for visual-only media, mobile 5:4 cover, document-natural mobile treatment, and final-row-only remainders.
4. `git diff --check`
   - Passed.

Initial setup evidence:

- The first focused test invocation passed the new/source tests but could not read the built manifest because the isolated worktree had no `site/dist`.
- The first preview-build attempt reported missing `astro/config` because the isolated worktree had no `site/node_modules`.
- After `npm ci`, the preview build and the complete focused suite passed.

## Risks and notes

- The preview build emitted the pre-existing Vite warning that `listingMediaOverrides.json` is imported with inconsistent JSON attributes by another module; it did not fail the build and is outside this lane.
- `npm ci` reported four dependency audit findings (one low, three high); dependency changes are outside this lane and no lockfile changed.
- When no feasible requested desktop chunk exists even after the packer's bounded alternate pool, the feed intentionally stops after the optimizer's final remainder rather than creating an incomplete middle row.
- Generated `site/dist` output and installed `site/node_modules` are ignored local validation artifacts and are not committed.
