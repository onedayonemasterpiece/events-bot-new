# Lane L03-integrator Results

## Status
committed

## Requirement IDs
- R01
- R02
- R03
- R04
- R05
- R06
- R07
- R08
- R09
- R10

## Branch
`feature/event-page-desktop-focus-v9-20260714`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/event-page-desktop-focus-v9`

## Base SHA
`6f3b610c1651d9ff07f6e456036862a782ea3d64`

## Head SHA
- Implementation/public-preview commit: `6a95b5cf`
- Closure documentation commit: recorded after final reviewer acceptance.

## Files changed
- `.codex/lanes/desktop-event-focus-v9-20260714/LANE_MAP.yml`
- `.codex/lanes/desktop-event-focus-v9-20260714/RESULTS.md`
- `.codex/integration/desktop-event-focus-v9-20260714/INTEGRATION_REPORT.md`
- `CHANGELOG.md`
- `docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md`
- `site/scripts/check-preview.mjs`
- `site/src/components/lab/DesktopEventCleanPage.astro`
- `site/src/pages/lab/event-desktop/examples/[scenario].astro`
- `site/src/pages/lab/event-desktop/index.astro`

## Commands run
- `npm run build:preview -- --build-id preview-20260714t-desktop-focus-v9`
- `npm run check:preview`
- `npm run deploy:preview`
- Public HTTP probes for overview, ten scenarios and generated Astro assets.
- Public Playwright visual/layout/interaction matrix at `1536×864`, `1440×900`, `1920×1080`, plus `390×844` isolation.
- Antigravity `Gemini 3.1 Pro (High)` pre-design and final public visual audits.

## Tests / verification
- Preview build: 448 pages, complete.
- Preview checker: pass.
- Public HTTP: all required pages/assets `200`.
- Public Playwright: `passed=true`, `failures=[]`, `browserErrors=[]`.
- Near-square crop: `19.97%`; lower-edge mismatch: `0px`.
- Multi-portrait Next: partial source promoted to left; exact selected source opened in fullscreen gallery.
- Related-card gate: bounded document safe-cover plus ambient-contain fallback both verified.
- Mobile isolation: desktop root absent at `390×844`; no production mobile file changed.
- Gemini final verdict: **SHIP**, with integrated OCR Companion B selected over A.

## Risks
- This remains a noindex lab preview, not a production event-route rollout.
- The deploy’s long idempotent per-event `/ics/*.ics` mirror tail was stopped after relevant preview HTML/CSS/JS and hashed assets were fully uploaded and public browser acceptance passed. This does not block the lab preview.
- Source-level saliency boxes remain future work; v9 uses an explicit bounded bottom-safe near-square framing rule.

## Merge notes
- Base is the pushed v8 feature branch, not `origin/main`.
- Integrate only after the parallel desktop branch strategy is reconciled by the owner of the main static-site line.
- Recommended product direction: Continuous Editorial plus integrated OCR Companion B; keep A only as comparison evidence.
