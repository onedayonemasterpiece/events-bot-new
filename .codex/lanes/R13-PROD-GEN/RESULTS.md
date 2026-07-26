# Lane R13-PROD-GEN Results

## Status
completed-read-only

## Requirement IDs
- R13-04
- R13-05

## Branch
integration/festivals-production-r13-20260726

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/r13-production`

## Base SHA
`7ba887a9`

## Head SHA
Read-only lane; integration owner commits the result.

## Files changed
None by mapper.

## Commands run
Branch topology, generator, publisher, Fly runtime and root HTTP inspection.

## Tests / verification
Confirmed R12 is main-reachable; Kaggle produces checked root-form and secret artifacts; enabled publisher is create-only under `/_review/<token>/`.

## Risks
No reader-atomic root publisher exists. Root/current/stable ICS mutation remains intentionally blocked.

## Merge notes
Integrator extends generated-output and browser gates, but must not claim public root publication.
