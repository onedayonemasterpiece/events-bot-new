# R03 visual-keyboard results

## Scope

- Lane: `R03 / visual-keyboard`
- Base SHA: `288b56790ba0866fcbf3da827c499c421425b709` (`origin/main` at lane start)
- Implementation head SHA: `88c4ca95376f5119204ba092d904ef61c861d19d`
- Branch: `agent/static-related-quality/visual-keyboard`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-related-quality-visual-keyboard`

## Outcome

- Added one shared protected-media decision used by recommendation row packing and `EventCard`.
- Packed rows now choose geometry from the final safe `cover`/fail-closed `contain` decisions and serialize exact fit, object position, and reason.
- Removed Desktop/`Ещё события` CSS fit overrides; client-created cards apply the serialized authoritative decision.
- Added a same-origin event-destination gallery handoff in `sessionStorage`, exact-destination gated and bounded to 30 seconds. The first destination `BODY` Left/Right advances a multi-image hero and restores event-surface ownership; single-image/non-event contexts remain no-ops.
- Added viewport-aware footer P/S ownership for explicit footer focus, `BODY`, or an offscreen managed-card focus only. Visible cards and unrelated controls retain their context.

## Validation evidence

Commands run from the lane worktree:

1. `node --test site/tests/*.test.mjs`
   - PASS: 60/60 tests.
2. `npm ci --ignore-scripts`
   - PASS: 265 packages installed for local validation; `node_modules` remains ignored/uncommitted.
3. `npm run build -- --mode preview`
   - PASS: Astro built 380 pages in 4m16s.
4. Built event `6408` inspection (`site/dist/sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/index.html`)
   - 10/10 related cards had declared treatment equal to serialized authoritative fit.
   - Unsafe/stale/unknown geometry remained `contain`; reasons included `document_media`, `media_role_not_crop_eligible`, and `missing_or_stale_geometry`.
5. `git diff --check`
   - PASS.

`npm run check:preview` was also attempted after the direct Astro build. It did not run its assertions because that checker requires a `dist/preview-*` wrapper produced by `build:preview`, while the successful compile command intentionally emitted directly to `dist/`.

## Focused regression coverage

- Row decision and `EventCard` decision equality.
- Fail-closed visuals participating in packed row geometry rather than retaining `visual-cover` metadata.
- No Desktop or personal-continuation surface CSS may override the canonical fit.
- Same-origin exact event destination allowlist and cross-origin/non-event negative controls.
- Visible-footer BODY/offscreen-card ownership and visible-card/offscreen-footer/unrelated-context negative controls.
- Router source contract for bounded handoff persistence/consumption and viewport-aware footer routing.

## Risks / follow-up

- This lane did not modify or run the separate live Playwright acceptance gate. The integration/browser lane must still exercise the real two-document Chromium/Firefox/WebKit journey and touchpad footer clipboard/toast behavior on the generated candidate.
- Repository preview fixtures contain mostly missing/stale semantic geometry, so the build correctly demonstrates fail-closed containment. Production-data candidate visual screenshots remain an integration acceptance responsibility.
- No deploy, push, candidate publication, or production mutation was performed.

## Changed files

- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventCard.astro`
- `site/src/components/PersonalFeedSlot.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/keyboardEventNavigation.mjs`
- `site/src/lib/relatedCardLayout.mjs`
- `site/tests/image-crop.test.mjs`
- `site/tests/visual-keyboard-regressions.test.mjs`
- `.codex/lanes/static-related-quality-20260720/visual-keyboard/RESULTS.md`
