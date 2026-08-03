# Lane qa-v2 Results

## Status

committed

## Requirement IDs

- R17 — deterministic invalid, honeypot, mocked success, mocked duplicate and synthetic network-error browser scenarios; optional explicit live success+duplicate hook.
- R18 — desktop Chromium matrix: 1366×768, 1440×900, 1536×864, 1672×941, 1920×1080.
- R19 — mobile Chromium matrix: 320×700, 360×800, 390×844, 430×932.
- R20 — animation frames at ready+0s, 5s and 10s plus a separate 5.2s reduced-motion stability assertion.
- R21 — named handoff screenshots and artifact metadata for 1672×941, 1920×1080 and 390×844.
- R22 — default PWA brand projection plus existing local photo cover exercised through the same event/query engine with two focal points.

## Branch

`agent/static-launch-tile-mosaic-v2/qa`

## Worktree

`/dev/shm/tile-mosaic-v2-qa`

## Base SHA

`8b22af29008456ec125b1404055a4283ddb2b57a`

## Head SHA

`HEAD` (the commit containing this report)

## Files changed

- `site/tests/tile-mosaic-launch.test.mjs`
- `site/scripts/check-tile-mosaic-launch-playwright.sh`
- `.codex/lanes/qa-v2/RESULTS.md`

## Implemented acceptance contract

- The shell entry point requires a supplied HTTP(S) page URL and accepts an explicit artifact directory and optional local photo path. It reuses repository/global Playwright and central browser caches instead of installing packages or browsers.
- The runner emits viewport PNGs plus `report.json` with requirement/scenario status, safe target URL, measured geometry, screenshot dimensions, bytes and SHA-256. It removes target query data and never records an optional live email.
- Desktop assertions cover: 72 tiles; square sides derived from six viewport-height rows plus five seams; top=0; 34–39vw left tolerance around the 36.5vw contract; explicit opaque-seam overlay; nearly-black opaque grid background; actual seam hit target; no vertical scroll; exact headline/status/date/four-line copy; square PWA logo; clipped accessible email label; envelope; and exact desktop form bands (input 315–375px, button 240–270px, gap 12–20px, heights 72–84px).
- Mobile assertions cover: 72 tiles, six columns, no horizontal overflow, header→mosaic→copy order, exact copy, square PWA logo, non-overlapping logo/status/headline/form, and stacked full-width controls.
- Motion evidence uses the same normal page at 0/5/10 seconds and requires state changes. Reduced-motion uses a dedicated emulated preference, waits 5.2 seconds, and requires both tile states and light position to remain unchanged.
- Projection evidence asserts default `PWA-icon.png` + `data-image-mode=brand`, then sends `tile-mosaic:set-image` with an existing local photo, `mode=cover`, and focal point 0.23/0.71. It repeats via `mosaicImage`, `mosaicMode=cover`, `focalX=0.77`, `focalY=0.31` and requires the same image element/API contract.
- Form mocks also answer the resilient transport's `transport_probe_v1` nonce contract so route selection is deterministic. Success/duplicate return the production-safe constant RPC shape. Synthetic outage returns 503. A real backend probe runs only when `TILE_MOSAIC_LIVE_EMAIL` is explicitly provided; the address is neither printed nor persisted.
- The JSON explicitly labels this as L1 headless Chromium evidence and states that it is not native Android/iOS L2 evidence.

## Commands run

- `node --check site/tests/tile-mosaic-launch.test.mjs`
- `bash -n site/scripts/check-tile-mosaic-launch-playwright.sh`
- `site/scripts/check-tile-mosaic-launch-playwright.sh --help`
- `node site/tests/tile-mosaic-launch.test.mjs --help`
- `git diff --check`
- Read-only inspection of the final in-progress UI contract in `/dev/shm/tile-mosaic-v2-ui` to align exact copy, hooks, seam overlay, mode API and geometry bands.

Before the final storage guard, an operational smoke was run against the pre-v2 local page. It produced all five desktop screenshots, all four mobile screenshots, 0/5/10 animation frames, reduced-motion evidence and JSON. As expected, it rejected the old non-square grid/old copy/missing mode and unconfigured mocked-RPC path. That smoke was not accepted as v2 evidence and its temporary artifacts were removed.

Two Chromium screenshot attempts initially crashed. Targeted diagnostics showed the actual cause (`No space left on device` in Chromium crashpad), not a page assertion. Official Playwright issue guidance was checked for Chromium target-crash diagnostics/channel behavior. The runner now:

- uses the central `/opt/ms-playwright` cache when the configured cache lacks Chromium;
- uses full Chromium rather than headless-shell for this filter-heavy scene;
- captures exact viewport frames rather than stitched full-page images;
- selects `/dev/shm` for the browser profile only when `/tmp` is below 256MiB and `/dev/shm` has more capacity;
- fails early below 128MiB browser-temp or 32MiB artifact free space;
- restarts Chromium after a disconnected browser and preserves the original scenario error if cleanup also fails.

A targeted 1920×1080 screenshot succeeded after the diagnosed temp-path correction.

## Tests / verification

- **Passed:** Node syntax check.
- **Passed:** shell syntax check.
- **Passed:** both help/usage entry points and explicit L1/not-L2 labels.
- **Passed:** whitespace/diff check.
- **Passed (pre-v2 smoke only):** runner completion/report/artifact mechanics, exact viewport PNG sizing, temporal capture and reduced-motion flow.
- **Deferred by integrator instruction:** final merged v2 end-to-end run. `/dev/shm` had roughly 185MiB free and the root instructed this lane not to create a full Astro dist/runtime copy. The integrator owns the mandatory supplied-URL execution after worker-worktree cleanup and UI merge. This report does not claim final visual acceptance.

## Risks

- Final screenshot/geometry acceptance still depends on the UI commit and must be executed from the merged exact SHA. Static compatibility was inspected, not substituted for browser evidence.
- Mock success/network scenarios deliberately fail if the supplied preview omitted public personalization configuration, because otherwise the page never enters its RPC path. The release preview must include its intended public URL/publishable key; secret/service-role keys are never accepted by this runner.
- The optional live probe performs two real submissions for the explicitly supplied address. Use only a dedicated authorized QA address. The runner does not persist it, but the backend necessarily will.
- Headless viewport emulation cannot establish native browser chrome, keyboard, Android WebView or iOS Safari behavior; those remain registry L2 scenarios.
- An arbitrary local cover defaults to an existing listing-media WebP. Override `TILE_MOSAIC_PHOTO_PATH` if that fixture is intentionally removed from a later deployment.

## Merge notes

- Cherry-pick this lane after/alongside the UI lane; it has no source-component overlap.
- Run after merge and exact-SHA build/deploy:
  `site/scripts/check-tile-mosaic-launch-playwright.sh '<supplied tile-mosaic URL>' 'artifacts/codex/tile-mosaic-v2-l1-<sha>'`
- Optional authorized live check:
  `TILE_MOSAIC_LIVE_EMAIL='<dedicated QA address>' site/scripts/check-tile-mosaic-launch-playwright.sh '<URL>' '<artifact-dir>'`
- Treat nonzero exit as a release blocker. Inspect `report.json` and the three handoff images manually; do not promote L1 results as native L2 evidence.
