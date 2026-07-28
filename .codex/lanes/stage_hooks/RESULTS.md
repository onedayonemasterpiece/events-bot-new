# Lane results: `stage_hooks`

## Scope

- Requirements: **R01**, **R02**.
- Base SHA: `5f7e0f3bbfe57b2ceb139268e1903ce57effd780`.
- Implementation head SHA: `91b47cf608445831ecf6d327b0c2843c205b4141`.
- Branch: `agent/autopresenter-prototype/stage`.

## Delivered

- Added the static, `noindex` same-origin stage at `/internal/presenter-stage/`.
- The stage is authored as a fixed 1920×1080 composition and embeds the real site in an exact 430×932 iframe.
- Stable automation surfaces:
  - `[data-presenter-stage-ready="true"]`
  - `#presenter-mobile-frame[data-presenter-id="mobile-site-frame"]`
  - `[data-presenter-id="stage-status"]`
- The stage status surface supports the required state vocabulary (`disconnected`, `idle`, `running`, `stopping`, `completed`, `error`) through a `presenter:status` custom event or same-window `postMessage`.
- Safe stage-only `F` fullscreen toggle, with editable-target and modifier guards.
- Added the stable product hooks:
  - homepage quick-nav link: `data-presenter-id="nav-tomorrow"`
  - `/zavtra/` main: `data-presenter-id="tomorrow-page-ready"`
- No M0, relay, agent, documentation, or changelog files were changed in this lane.

## Owned files

- `site/src/pages/internal/presenter-stage/index.astro`
- `site/src/components/HomeQuickNav.astro`
- `site/src/pages/zavtra/index.astro`
- `.codex/lanes/stage_hooks/RESULTS.md`

## Validation

1. `npm ci` in `site/` — passed (`267` packages installed; npm reported pre-existing audit findings: 1 low, 4 high).
2. `npm run build` in `site/` — passed: Astro built 465 pages, including `/internal/presenter-stage/`, `/`, and `/zavtra/`. One pre-existing Vite warning about inconsistent JSON import attributes was emitted.
3. `git diff --check` — passed.
4. Headless Playwright contract smoke against the built static site — passed:

   ```json
   {"stage":"ready","viewport":"1920x1080","iframe":"430x932","realNavigation":"http://127.0.0.1:4322/zavtra/"}
   ```

   The smoke used `locator.hover()` followed by `locator.click()` inside the iframe and waited for the destination ready marker; it did not use DOM `element.click()`.
5. Visual check — passed. A Playwright screenshot was captured as a 1920×1080 PNG at `/tmp/autopresenter-stage-check/stage.png` and inspected at original resolution. It is deliberately not committed because final evidence packaging belongs to the integration lane.

## Risks / merge notes

- The iframe target query parameter accepts only same-origin URLs; invalid or cross-origin targets are ignored by design.
- The iframe itself is exactly 430×932. The surrounding device chrome is 462×964 and fits inside the 1080px stage.
- At viewports smaller than 1920×1080 the whole fixed composition scales down; the integration capture should use an exact 1920×1080 viewport for pixel-stable evidence.
- Agent lane should target `#presenter-mobile-frame` and use frame locators for the two product hooks.
- Integration must still own canonical docs and `CHANGELOG.md` updates.
