# Lane `stage_ux` results

## Status

**Done** for `R02`, `R03`, and `R06`.

- Branch: `agent/autopresenter-first-test-ux/stage-ux`
- Worktree: `/home/dev/projects/events-bot-new-autopresenter-stage-ux`
- Lane base SHA: `e634403db817b3ac4c7fed4e5781f0a13ad0de2b`
- Tested implementation head SHA: `f9e2e8fa545fb31fd00410eabb7d7515c8099e59`
- Note: the lane map named `site/src/pages/internal/presenter-stage.astro`; root corrected the owned path to the repository's actual route file, `site/src/pages/internal/presenter-stage/index.astro`, before editing.

## Requirement evidence

### R02 — stage-side edge-to-edge fullscreen

- `html`, `body`, and `.stage` now use the live viewport (`100vw` × `100vh`).
- Browser body margin is zero and document overflow is hidden.
- Removed the fixed 1920×1080 canvas and transform-scaling fallback, so the stage itself fills Chromium's viewport instead of being letterboxed inside it.
- Playwright at 1920×1080 measured both document and stage at exactly `1920 × 1080`, with no scroll overflow.

### R03 — presentation-only composition

- Removed the persistent narrative/status/instruction panel, story chrome, shortcut legend, live badge, and footer from the visible presentation.
- Retained only the short phrase `Завтра — в одном движении.` outside the centered device.
- Kept status and step hook nodes visually hidden so presenter automation and status messages retain their deterministic DOM contract.
- Preserved the stage/frame IDs, readiness attributes, same-origin target handling, status event/message handlers, and fullscreen key handler.

### R06 — materially larger phone

- Increased nominal iframe dimensions from `430 × 932` to `476 × 1024`.
- At 1920×1080, Playwright measured the phone shell at `500 × 1048`, centered at `(710, 16)`, and the visible iframe at approximately `474 × 1022`.
- The phone remains entirely within the viewport and responsively scales down without document overflow or clipping.

## Commands and tests

- `git diff --check` — passed.
- `npm run build` — initial attempt could not resolve `astro/config` because this isolated worktree had no local dependencies.
- `npm ci` — passed; installed the lockfile-defined dependencies locally. NPM reported 5 dependency audit findings (1 low, 4 high); no lockfile was changed.
- `npm run build` — passed; Astro generated 465 pages, including `/internal/presenter-stage/`. Existing Vite warning about inconsistent JSON import attributes in `listingPresentation.ts` remains unrelated to this lane.
- Served `site/dist` locally with `python3 -m http.server 8123 -d dist`.
- Playwright 1920×1080 geometry/hook assertion — passed:
  - viewport/document/stage: `1920 × 1080`;
  - phone: `(710, 16, 500, 1048)`;
  - no clipping or overflow;
  - `presenter-stage`, frame-ready, hidden status, and all 3 step hooks present.
- Playwright responsive/status assertions at 1366×768, 800×600, and 390×844 — passed with no overflow/clipping; `presenter:status` updated both the hidden live-region label and status-card data attribute.
- Screenshot `/tmp/presenter-stage-1920x1080.png` visually inspected — clean centered phone, no explanatory panel, no clipping.

## Changed files

- `site/src/pages/internal/presenter-stage/index.astro`
- `.codex/lanes/stage_ux/RESULTS.md`

## Risks and merge notes

- Chromium kiosk/fullscreen launch arguments are intentionally outside this lane and remain for integration.
- The integration owner should merge this lane before final stage/agent live E2E, then verify the new interaction visualization layers against the larger responsive iframe geometry.
- Agent implementation files, shared styles, documentation, and `CHANGELOG.md` were not edited; canonical docs/changelog synchronization belongs to the integration lane.
- The screenshot is temporary evidence under `/tmp` and was not committed.
