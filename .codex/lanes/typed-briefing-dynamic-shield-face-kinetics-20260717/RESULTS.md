# Lane typed-briefing-dynamic-shield-face-kinetics-20260717 Results

## Status

committed

## Requirement IDs

- R01 — phrase-length-driven illumination origin
- R02 — face bbox cells never empty/near-empty
- R03 — verified easing plus 2–3 delayed safe cells

## Branch

`integration/typed-briefing-dynamic-shield-face-kinetics-20260717`

## Worktree

`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration`

## Base SHA

`02095bcd03d927b58e7887b97cd451a7fcca38a4`

## Files changed

- `site/src/data/briefingLab.ts`
- `site/src/pages/lab/briefing/index.astro`
- `tests/playwright/static_briefing_lab.spec.ts`
- `docs/features/static-site-pages/typed-briefing-hero-research.md`
- `docs/features/static-site-pages/typed-briefing-face-bbox-contract.md`
- `CHANGELOG.md`
- lane and integration reports

## Commands run

- `npm --prefix site run build:lab`
- `npm --prefix site run check:lab`
- global Playwright runner against `briefing-lab-b18e95f9dbba`
- local Chromium screenshot/WebM capture
- `a-gemini` / `Gemini 3.1 Pro (High)` strict acceptance gate

## Tests / verification

- isolated Astro build: PASS
- lab allowlist: PASS (6 files)
- targeted Playwright new motion/geometry/face tests: PASS
- full Playwright: `18/18` PASS in 3.4 minutes
- computed easing: `cubic-bezier(0.65, 0, 0.35, 1)`
- delayed weather accents: 2 cells, 902/941ms delay, safe from copy/face
- Gemini Pro gate: PASS, zero publication blockers

## Risks

- Current manually curated bbox values are lab fixtures, not Smart Update facts.
- Production rollout still needs the documented asset identity/status envelope and exporter QA.
- Face floor `0.50` intentionally prioritizes face legibility over maximum alpha drama.

## Merge notes

All code converged in `configureMosaic`, so implementation remained serial after
three independent read-only audits. No worker write merge was required.
