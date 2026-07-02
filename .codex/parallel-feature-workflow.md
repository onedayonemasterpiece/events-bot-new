# Parallel Feature Workflow

Use this for large feature requests with many independent requirements.

## Start prompt

Use:

`$feature-fanout`

Then paste the full feature request.

## Required artifacts

- execution matrix
- lane map
- worker RESULTS.md per lane
- integration report
- final requirement closure table

## Safe defaults

- parallel read-only exploration is encouraged
- parallel writes require branch/worktree isolation
- final integration is serial
- every lane must be committed, rejected, or blocked with patch artifact
- dirty main worktree must be preserved, not used as an excuse to stop

## Branch naming

- worker: `agent/<feature>/<lane-id>`
- integration: `integration/<feature>`
- setup: `chore/codex-parallel-workflow`

## Completion

A run is not complete until:

- every original requirement has a final status
- every accepted worker lane is merged/cherry-picked into integration branch
- rejected/blocked lanes are documented
- relevant tests/checks are run or explicitly skipped with reason
- worktree audit shows no abandoned dirty worker worktree
