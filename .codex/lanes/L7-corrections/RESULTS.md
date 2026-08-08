# L7 Current UI Resource Graph acceptance corrections

## Scope

Corrected the deterministic Current UI Resource Graph v0 decoder and its
unattended workflow without changing any Astro UI, CSS, tokens, Kaggle builder,
Penpot data, design-system contracts, or normalization behavior.

## Completed

- Bound candidate and public-root runtime/source evidence to separate exact
  identity planes, including manifest facts, tree hashes and root HTML SHA-256.
- Added bounded retries, safe unique manifest keys and route-relative mapping.
- Preserved distinct Day, Weekend, Popular and Event Detail families and exact
  event-detail source/runtime alignment.
- Made Hero-talk evidence marker-only; an unrelated page-end slot cannot satisfy
  either Hero-talk hypothesis.
- Added standalone CSS, semantic style cohorts, computed family-specific
  desktop/mobile evidence and multi-channel fragmentation observations.
- Allocated screenshot budget to one modal representative per family before
  round-robin outliers, with explicit uncaptured records when the budget is too
  small.
- Removed raw workflow-dispatch interpolation from shell, allowlisted every
  immutable identity input, pinned GitHub Actions by commit SHA and made the
  always-uploaded receipt fail closed after validation errors.
- Updated canonical feature documentation and `CHANGELOG.md`.

## Validation

```text
node --check scripts/current_ui_resource_graph/decode.mjs        PASS
node --check scripts/current_ui_resource_graph/graph-lib.mjs     PASS
workflow YAML parse                                              PASS
git diff --check                                                 PASS
uv run --with-requirements requirements.txt \
  pytest -q tests/test_current_ui_resource_graph.py               16 passed
```

The private 1,266-route candidate/browser run is intentionally performed only
after integration, using the repository secret/file boundary; no bearer URL is
stored in committed files or command-line arguments.
