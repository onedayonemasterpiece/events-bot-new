# Occurrence navigation token adoption results

## Lane contract

- Lane ID: `occurrence-nav-token-adoption`
- Requirement IDs: `OCC-TOKEN-01`, `OCC-TEST-01`, `OCC-DOC-01`
- Status: done
- Base SHA: `b582b793de7b9a2d06e122c72ad987184af7edd5`
- Implementation head SHA: `625edda49cb41e3d1f9c4573a22a1d2b994c7efe`
- Branch: `agent/occurrence-nav-token-adoption`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/occurrence-nav-token-adoption`

## Outcome

- `EventOccurrenceNav` now consumes the already-defined occurrence roles for
  muted/summary/link/date paint, mobile and chip surfaces, chip current state,
  chip border, current elevation, and panel/chip radii.
- Its existing event-detail panel borders consume the exact shared
  `--ke-color-event-detail-border` and `--ke-color-event-detail-border-soft`
  roles. These resolve from the same `#793014` paint at the existing 20% and 12%
  alpha values.
- Markup, occurrence grouping, links, target sizes, type scale, layout and
  responsive geometry are unchanged.
- Added a focused regression that verifies definitions, direct consumption and
  absence of the superseded component-local paint/radius literals.
- Regenerated the deterministic token impact graph after adding 29 direct token
  consumptions. Authority counts remain 2,505 tokens, 2,787 definitions and 706
  alias edges; consumer count advances from 3,570 to 3,599.

## Changed files

- `site/src/components/EventOccurrenceNav.astro`
- `site/tests/event-occurrence-nav-token-adoption.test.mjs`
- `site/src/design-system/token-impact.generated.v1.json`
- `docs/features/static-site-pages/design-system/README.md`
- `CHANGELOG.md`
- `.codex/lanes/occurrence-nav-token-adoption/RESULTS.md` (evidence metadata)

## Commands and evidence

```text
node --experimental-strip-types --test \
  site/tests/event-occurrences.test.mjs \
  site/tests/event-detail-token-adoption.test.mjs \
  site/tests/event-occurrence-nav-token-adoption.test.mjs \
  site/tests/a0-event-detail-convergence.test.mjs \
  site/tests/event-detail-archive.test.mjs
# PASS: 22/22

node --test site/scripts/token-impact-graph.behavior.test.mjs
# PASS: 4/4

node site/scripts/generate-token-impact-graph.mjs --write
# 2505 tokens; 2787 definitions; 3599 consumers; 706 alias edges; 109 families

node site/scripts/check-token-impact-sot.mjs
# PASS: ok=true; unresolved external tokens=1 (existing accepted authority); cycles=0

node site/scripts/check-astro-family-sot.mjs
# PASS: 109 families; 106 roots; 29 unique production routes

git diff --check
git diff --cached --check
# PASS
```

## Risks and merge notes

- This is a mechanical consumer-only substitution. No token definitions or
  semantic component behavior changed.
- Cherry-pick implementation commit `625edda49cb41e3d1f9c4573a22a1d2b994c7efe`
  and the following metadata commit if lane evidence is desired.
- If the executable trunk gained other token consumers after the frozen base,
  regenerate `site/src/design-system/token-impact.generated.v1.json` once after
  applying this commit rather than resolving generated JSON manually.
- No publication or browser verdict was performed in this bounded lane.
