# Lane visual-audit Results

## Status
committed (read-only findings recorded by integrator)

## Requirement IDs
- R01
- R02
- R03
- R04
- R05
- R06

## Branch
none-read-only

## Worktree
`/home/dev/projects/events-bot-new-typed-briefing-mosaic-dramatic-20260717-integration`

## Base SHA
`65b9248331ee3e9713ae1cc38ce63f69c1029a0f`

## Head SHA
N/A — read-only audit.

## Files changed
None by the reviewer; this handoff records its findings.

## Commands run
Read-only inspection of the exact user screenshots, current opacity/timing arrays,
CSS image mapping, Playwright criteria and prior Gemini prompt/response.

## Tests / verification
- The old matrix is a perfect horizontal alternating pattern in every row;
  parity means differ by `.2367`.
- The previous test requiring every neighboring alpha delta `>=.139` directly
  incentivized checkerboard topology.
- Right-edge rows terminate at `1/.86/1/.82`; the penultimate column includes
  `.76/.81`, so the edge is intentionally washed out.
- Strictly monotone column averages `.235 → .920` encode the rejected smooth
  gradient.
- `background-size:1200% 400%` forces every raster into the grid's `3:1`
  aspect ratio instead of using the selected `cover` contract.
- A cell cap of `88px` makes the mosaic start around `45vw` on the user's wide
  screenshot instead of occupying the right three quarters.

## Risks
Portrait posters need curated focal points because a source-faithful wide cover
crop cannot retain both the entire poster title and the entire portrait.

## Merge notes
The previous Gemini PASS is invalid and superseded: its prompt front-loaded the
faulty metrics and the response echoed them without detecting checkerboard or
source distortion. New acceptance must be screenshot-first and blind to claimed
success metrics.
