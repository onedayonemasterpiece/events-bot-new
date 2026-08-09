# Lane requirements_experiment_fix Results

## Status

committed

## Requirement IDs

- R-REQ-CORPUS — explicit bounded requirements archaeology corpus
- R-REQ-STATUS — truthful historical/current/conflict supersession status
- R-DYNAMIC-PATHS — fail-closed pinned dynamic sources and corrected Popular row
- R-TRANSPORT — exact `transport_timetable_layout` source decoding
- R-HISTORY — deterministic pinned ancestry plus bounded semantic records
- R-SECRETS — credential/env/URL-safe provenance evidence

## Branch

`agent/current-ui-behavioral-v1-1/requirements-experiment-fix`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/requirements-experiment-fix`

## Base SHA

`05fb34d5cb5fecb3ef03014ce4e725c889bdf3c5`

This was created before PR #444 merged. It contains the same foundation commits
`05921d84d` and `05fb34d5c` on top of then-`origin/main` `a95f9007d`; the lane
was intentionally not rebased after `origin/main` advanced to the PR #444 merge.

## Head SHA

Implementation commit: `d8dbc71f1` (`fix(ui-decoder): complete requirements and experiment archaeology`).

## Files changed

- `CHANGELOG.md`
- `docs/features/static-site-pages/current-ui-resource-graph.md`
- `scripts/current_ui_resource_graph/v1/behavioral-experiments.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral-requirements.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral.mjs`
- `tests/test_current_ui_behavioral_requirements_experiments.py`
- `.codex/lanes/requirements_experiment_fix/RESULTS.md`

No registry/harness/capture/materialize file, design-system repository file, or
production `site/src` file was changed.

## Commands run

- `git fetch origin --prune`
- `git worktree add -b agent/current-ui-behavioral-v1-1/requirements-experiment-fix ... 05fb34d5c`
- `git show ef7aa62...:<path>` / `git cat-file -e ef7aa62...:<path>` source checks
- `gh pr view` for PRs 40, 69, 70, 74, 79, 81, 85, 88, 97, 100, 117, 125 and 169
- `node --check` for all changed JavaScript modules
- focused and Current UI pytest suites listed below
- two exact-`ef7aa62` source-pass generations followed by `cmp`/SHA-256 checks
- bounded secret scans over generated JSON/JSONL/Markdown
- `git diff --check`

## Tests / verification

- PASS — `12 passed`:
  `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_current_ui_behavioral_requirements_experiments.py tests/test_current_ui_behavioral_decoder.py`
- PASS — Current UI regression glob, 27 tests / exit 0:
  `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest tests/test_current_ui*.py -q -ra`
- PASS — exact-source output determinism for:
  - `requirements-provenance-ledger.jsonl`
  - `experiment-registry.jsonl`
  - `historical-variant-evidence.jsonl`
  - `dynamic-region-loading-matrix.jsonl`
  - `audit-report.md`
- PASS — exact-source probe counts:
  - requirements provenance: 3,037 rows / 55 paths / 0 missing documents;
  - dynamic regions: 13 / 0 missing pinned source rows;
  - experiment registry: 6 rows;
  - history: 448 total = 407 pinned-ancestry commit-subject + 19 curated semantic + source-current rows;
  - only blocking source-pass unresolved row: `unresolved.behavioral-packets-pending`;
  - six transport winner-receipt rows remain nonblocking unresolved.
- PASS — generated evidence secret scan; credential/env/URL-bearing requirement
  lines retain only a bounded hash/presence marker.

## Risks

- History evidence is intentionally bounded. Generic records cover only
  ancestors of exact source `ef7aa62`; curated PR/run/branch rows do not claim
  exhaustive coverage of every mutable remote ref, tag, release or artifact.
- The requirements plane is the decoder commit, while runtime implementation is
  exact `ef7aa62`. Records preserve this separation so later accepted source
  maps are not misrepresented as files present in the UI source pin.
- The pinned client defines a qualified transport-action predicate but does not
  invoke it on click ingest; the decoder records this gap rather than asserting
  accepted analytics semantics.
- `live` is supported in source types/client ingest but has no approved pinned
  production or secret-candidate build path. No winner/owner receipt exists.
- This lane does not clear the mandatory capture/full-resolution-review gate.

## Merge notes

- Cherry-pick `d8dbc71f1`, then this RESULTS-only commit.
- Preserve the six-row experiment registry semantics: `off` renders the current
  `departure_board_v1` resilient baseline; it is not a fourth treatment.
- Do not replace pinned ancestry with `git log --all` during conflict resolution.
- All experiment rows and curated history rows remain `NOT_MERGED`; this is
  decoding evidence, not component normalization or experiment acceptance.

