# Lane L1-product-media-audit Results

## Status
committed (integration-owned read-only result)

## Requirement IDs
- R01
- R03
- R04
- R05

## Branch / worktree / base
- `integration/event-page-desktop-variants-20260711`
- `/home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1`
- base `e9966bb1`

## Output and verification
- Audited real event states `6345`, `6510`, `6678`, `6750` at 390/1440.
- Evidence: ignored `artifacts/codex/event-page-desktop-20260711/audit.json`.
- Confirmed rejected CTA copy, first-view onboarding, undersized overridden icons, literal calendar `+`, desktop duplication and event `6510` OCR classifier miss.

## Risks / merge notes
- Event `6510` requires an explicit temporary export override until upstream OCR classification is repaired.
- Audit changed no production data.
