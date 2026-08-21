# Lane ui-three-way-conformance Results

## Status
committed

## Requirement IDs
- R01-R14: thin events-bot routing, golden/fresh fixture resolver, disposable
  Astro materialization/capture, actual tuple builder and CI wrapper.
- R15: carry Penpot component/state/fixture/resolved-case/asset identity into
  the exact tuple so different events fail closed before comparison.

## Branch
agent/ui-three-way-conformance-20260821

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/ui-three-way-conformance`

## Base SHA
`a68c7f23c4e014c6e9f66e95f394656e9cb0f411`

## Implementation commit
`20a007d2fcda02c530a3eb1a14224bb69db5b08b`

## Files changed
Routing instructions, reusable-workflow caller, exact fixture resolver,
materializer extension, deterministic capture and actual-tuple adapter.

## Commands run
- `node tests/ui-conformance-routing.test.mjs`
- `node --check` for adapters and materializer
- targeted pre-existing UI/resource-graph pytest suite: 52 passed
- YAML parse and `git diff --check`

## Tests / verification
PASS. Golden fixture remains event.real.5336. Fresh advisory profiles resolve
deterministically for photo, poster, gallery and no-image without updating the
golden fixture.

## Risks
No production source, promotion or deploy was changed. Exact Penpot fixture and
font blockers must be cleared before any visual parity verdict.

## Merge notes
The linked design-system Draft PR owns the full canonical skill. This repository
contains only consumer adapters and routing.

Linked design-system CI/tooling head:
`bd7f232b45291a7443c5b6b6f80b17e4b2d81d5f`.
