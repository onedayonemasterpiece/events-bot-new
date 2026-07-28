# Lane egg-program Results

## Status

committed

## Requirement IDs

- R17 — owner reward decision and auditable collection-first result
- R18 — bounded participation, fairness, tie-break, anti-abuse, accessibility and legal gates
- R19 — versioned mobile/desktop site-wide placement matrix

## Branch

`agent/focus-group/egg-program`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/focus-egg-program`

## Base SHA

`5435fb075439174f92c20ebc5e3de0f17651fecf`

## Head SHA

Documentation content commit: `a609528b338779f8ea33380a775a2d704625bc0c`.
The final metadata-only RESULTS commit SHA is reported in the lane handoff because
including its own SHA in this file would be circular.

## Files changed

- `docs/backlog/features/static-site-focus-group/easter-egg-program.md`
- `docs/backlog/features/static-site-focus-group/product-prototype.md`
- `docs/features/static-site-easter-eggs/README.md`
- `.codex/lanes/egg-program/RESULTS.md`

## Commands run

- `git status --short --branch`
- `git rev-parse HEAD`
- focused `sed` / `rg` inspection of the three canonical product documents
- Python relative-link and required-wording validation across owned documents
- arithmetic assertion for the bounded score: `4+4+4+6+6+6+10 = 40`
- `git diff --check`
- `git diff --cached --check`
- stale-copy scan across the wider focus/easter documentation

## Tests / verification

Inspection-only lane; no site code was changed.

- Relative local Markdown links in all three owned documents: **PASS**.
- Required wording for exact reward, all seven participation categories,
  collection-first rank, honest pending copy and `FG-E12`: **PASS**.
- Participation cap arithmetic: **PASS**, maximum `40`.
- Whitespace/diff check: **PASS**.
- Writable-scope check: **PASS**; no `site/`, `CHANGELOG.md` or forbidden focus
  README changes were made.

## Delivered contract

- One possible prize is exactly two theatre tickets; no unapproved `any show`
  promise.
- Ranking is lexicographic: collection coverage, bounded `0…40` participation,
  participation-category breadth, then an audited draw among exact ties.
- NPS counts only as response receipt; its `0…10` value stays analytically
  separate. Likes/dislikes and positive/critical feedback are symmetric.
- Comment length, sentiment, share/invite, purchase, speed, streak and repeat spam
  provide no advantage.
- `focus-eggs-placement-v1` defines twelve semantic placements with separate
  mobile/desktop anchors, prerequisites, accessible equivalents and fail-closed
  outcomes.
- `FG-E12` appears once after the third currently rendered saved/calendar event
  only when three distinct renderable items really exist.

## Risks

- This is a product contract, not a live contest, durable leaderboard or legal
  approval. Current localStorage state cannot prove membership or prize result.
- Organizer, exact theatre/partner terms, dates, eligibility, published rules,
  seed algorithm, data retention, tax/fulfilment and appeal process remain gates.
- Wider files outside this lane still contain superseded copy and require
  integration reconciliation: focus `README.md` retains equal-application text;
  `manual-email-templates.md` retains `two invitations / any play` wording; site
  prototype copy may do the same.
- Content-dependent placements require candidate preflight. Missing prerequisites
  must keep artifacts absent rather than silently relocating them.

## Merge notes

Cherry-pick documentation commit `a609528b338779f8ea33380a775a2d704625bc0c`
and the immediate RESULTS commit. Integration should reconcile the explicitly
listed stale references, update `CHANGELOG.md`, then run its full build/browser
and closure gates. No production deploy or backend behavior belongs to this lane.
