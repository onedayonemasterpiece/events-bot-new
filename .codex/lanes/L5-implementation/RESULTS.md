# L5-implementation results

## Revisions

- Base SHA: `ef2e1eb28cf1d1d0899a167f7ba9c0ce6b84826e`
- Audited implementation SHA: `67a3c8ef613e46f2a14f6005c0d545a238e7130c`
- Branch: `agent/current-ui-resource-graph/implementation`
- Results-only commit: the commit containing this file (the immutable final SHA
  is reported in the integrator handoff because a commit cannot contain its own
  hash).

## Outcome

Implemented the deterministic Current UI Resource Graph v0 decoder and its
failure-safe GitHub workflow. The decoder keeps the durable candidate and
current public-root identities in separate manifest objects, validates the
exact requested Git source/tree and candidate manifest identity, scans the
manifest's HTML inventory one file at a time, writes stable sorted JSONL under
an output budget, and never retains full HTML or a candidate bearer URL.

Source evidence uses the pinned Astro compiler, `es-module-lexer` and PostCSS.
The optional bounded Playwright pass selects deterministic page-family
representatives and structural outliers at independent `390x844` and
`1728x900` viewports. Fragmentation and candidate records are guarded by the
literal `NOT_MERGED` / `unresolved` invariant.

Coverage explicitly reports `FOUND`, `MISSING`, `DISCOVERED` or `AMBIGUOUS`
for every supplied surface hypothesis. Exhibitions, For Me, Interest Clubs,
Hero-talk and Hero-talk page-end are mandatory named rows. Page-end onboarding
is explicitly excluded as Hero-talk evidence. Experimental/old-branch
archaeology is outside the graph.

## Commands and evidence

- `npm ci --ignore-scripts` in `site/` — passed; installed the pinned parser
  dependency tree.
- `node --check scripts/current_ui_resource_graph/graph-lib.mjs` — passed.
- `node --check scripts/current_ui_resource_graph/decode.mjs` — passed.
- `uv run --with-requirements requirements.txt pytest -q tests/test_current_ui_resource_graph.py`
  — **7 passed**.
- The focused suite executes the bounded local fixture decoder twice and proves
  byte-identical required outputs/receipt, mixed `date-[date].astro` routing,
  all required files/nonempty JSONL, the no-merge invariant, secret redaction,
  no `site/src` mutation, mandatory named coverage rows, and a failed partial
  receipt.
- Exact-source/local-runtime smoke: detached source
  `ef7aa62e45c60f7a12da6160f490719c0721ec03`, one bounded local HTML fixture,
  explicit runtime manifest, production-identity verification disabled only for
  the local runtime fixture — complete receipt, `exact_commit` source pin, 244
  source records and 14,553 source style observations.
- `uv run --with pyyaml python ... yaml.safe_load(.github/workflows/current-ui-resource-graph.yml)`
  — parsed successfully.
- `git diff --check` — passed.

The full 1,266-route / 686,610,720-byte candidate was intentionally not fetched
locally because `CURRENT_UI_GRAPH_CANDIDATE_BASE_URL` was absent. The workflow
is the fail-closed execution path for that exact private artifact and uploads
its partial receipt with `if: always()` if any earlier step fails.

## Changed files

- `.github/workflows/current-ui-resource-graph.yml`
- `CHANGELOG.md`
- `docs/features/static-site-pages/README.md`
- `docs/features/static-site-pages/current-ui-resource-graph.md`
- `docs/routes.yml`
- `scripts/current_ui_resource_graph/decode.mjs`
- `scripts/current_ui_resource_graph/graph-lib.mjs`
- `tests/test_current_ui_resource_graph.py`
- `.codex/lanes/L5-implementation/RESULTS.md`

No `site/src/**`, `site/public/**`, Kaggle, Astro UI/CSS, Penpot, token,
variant, pattern or component-contract file was changed.

## Risks and merge notes

- The private full-corpus download and Playwright representative pass remain an
  integration/workflow gate; they were not runnable without the repository
  secret and local Chromium. Do not characterize the bounded local smoke as the
  current production graph.
- TypeScript semantic facts not exposed by `es-module-lexer` remain explicitly
  unknown/medium confidence. This is intentional rather than a regex-invented
  contract.
- The workflow requires repository secret
  `CURRENT_UI_GRAPH_CANDIDATE_BASE_URL`; it must be the exact candidate base and
  must never be copied to inputs or logs.
- The workflow materializes the candidate SHA in a detached linked worktree so
  it does not depend on the workflow-source checkout being site-identical.
- Merge the implementation commit and this results-only commit together. No
  push was performed.
