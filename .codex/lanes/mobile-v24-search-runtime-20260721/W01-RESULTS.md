# W01 Results — Search runtime

## Scope

- Lane: `W01`
- Requirements: `R01`, `R02`
- Base SHA: `e246fe466f826472386f822ec3e4ec104ee934bf`
- Implementation head SHA: `99fcd74f8c4ca9c128bdb899828828fd03a51295`
- Donor SHA: `9dced876ab4e8d2c69c79937d3b0186196c924db`

## Delivered

- Restored the owned donor subset from `9dced876`: exact row-packing implementation, intrinsic compact-row CSS, and donor regression tests. Donor docs, changelog, incident record, browser scripts, and temporary fixture were not transferred.
- Search cards now use `packRelatedCardRows(..., { rowSize: 1, presentation: 'flow' })`, preserve backend rank order, and pass the donor media decision as the optional third renderer argument. The two-argument renderer remains compatible.
- Flow presentation applies media ratio/fit/focal decisions without `data-lab-related-card`, `grid-row`, or `grid-column` placement.
- Unknown OCR/document dimensions fail closed to `contain` with null crop metrics, rather than advertising a measured 20% crop.
- Search vector card snapshots now carry `image_media_role`, `image_width`, `image_height`, and `focal_y` from the primary image asset; snapshot version is `event-card-v3-media-layout`.
- Removed the fake 28/55/74/92 progress timers. Opening is indeterminate; determinate progress comes from backend stream events and is monotonic by value and stage.
- Added per-run epoch plus `AbortController`, stale-response guards, owned completion timer cleanup, monotonic milestone announcements, and same-epoch success reset.
- Kept status/errors inline, preserved append results, separated button and `role="progressbar"` semantics, omitted `aria-valuenow` while unknown, added reduced-motion-compatible animation, and changed the progress visual to cream plus solid terracotta with no green gradient.

## Commands and evidence

- `git show --stat --oneline 9dced876...` and scoped `git diff` — identified and transferred only the owned donor subset.
- `node --test site/tests/visual-keyboard-regressions.test.mjs site/tests/event-continuation-contract.test.mjs site/tests/search-learning.test.mjs`
  - PASS: 26/26.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest tests/test_event_vector_sync.py -q`
  - PASS: 14/14.
- Extra source/browser-contract sweep:
  - `node --test ... site/tests/personal-feed-surface.test.mjs site/tests/event-detail-runtime-regressions.test.mjs`
  - 44 PASS, 3 environment-only failures because this isolated worktree has no generated `site/dist`; all source-only tests passed.
- `git diff --check`
  - PASS.

## Tooling note

- System `python` was absent and system `python3` did not have pytest. After checking the official pytest invocation/install contract and local project tooling, tests were run with the existing project-compatible venv at `/home/dev/.venvs/events-bot-image-geometry/bin/python` (pytest 8.1.1).

## Risks / integration follow-up

- A built-output/browser run was not performed in this lane because the isolated worktree has neither `site/node_modules` nor `site/dist`. The integration owner should run the normal Astro build and mobile browser gate after merging W01/W02.
- Documentation and `CHANGELOG.md` are intentionally untouched because they are forbidden in W01 and owned by integration lane I01.
