# Lane typed-briefing-motion-crop-corrective-20260717 Results

## Status
committed (pending final closure commit at report creation)

## Requirement IDs
- R01 restore irregular entry animation
- R02 exit only for a real automatic successor; terminal/manual media persists
- R03 restore accepted crops and limit head-safe overrides
- R04 fill multi-portrait mosaic cells with contiguous cover panels

## Branch
`integration/typed-briefing-motion-crop-corrective-20260717`

## Worktree
`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration`

## Base SHA
`87dd727bc032c43d453b018b292df42e51d95343`

## Head SHA
Recorded by the integration commit containing this report.

## Files changed
- `site/src/data/briefingLab.ts`
- `site/src/pages/lab/briefing/index.astro`
- `tests/playwright/static_briefing_lab.spec.ts`
- canonical feature/consultation documentation and `CHANGELOG.md`

## Commands run
- `PREVIEW_BUILD_ID=briefing-motion-crop-corrective-dev npm --prefix site run build:lab`
- `PREVIEW_BUILD_ID=briefing-motion-crop-corrective-dev npm --prefix site run check:lab`
- `playwright test tests/playwright/static_briefing_lab.spec.ts` with global `NODE_PATH`
- focused media/mosaic Playwright rerun after the collage correction
- two strict `a-gemini` acceptance runs with exact screenshots/WebM/code

## Tests / verification
- full Playwright: `17/17` passed
- post-collage focused Playwright: `2/2` passed
- lab allowlist: passed (6 files)
- `git diff --check`: passed
- visual evidence: four 1920×900 scenes, entry→hold→exit→next-entry WebM, terminal-persistence WebM
- Gemini 3.1 Pro High: initial `FAIL`; concrete collage/pause correction plus factual recheck; final nine-contract `PASS`

## Risks
- Isolated lab only; production homepage remains unchanged.
- Crop positions remain curated review metadata, not a production automatic focal-point service.

## Merge notes
The serial integrator owned all writable files because motion, media geometry, tests and canonical documentation were coupled. Three read-only audits were incorporated before implementation.
