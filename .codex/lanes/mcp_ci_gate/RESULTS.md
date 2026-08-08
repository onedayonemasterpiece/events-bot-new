# mcp_ci_gate results

## Lane contract

- Lane ID: `mcp_ci_gate`
- Requirement IDs: assigned MCP PR CI gate (no separate numeric ID was provided)
- Status: Done
- Branch: `agent/mcp-multiclient/ci`
- Base SHA: `eb9cf0c9c3412059d5cdd7568c4df4d6196d0727`
- Implementation head SHA: `3928e9a6f9112fddd1d26106a770f9d971212988`
- Push/deploy: not performed

## Outcome

- Added one explicit `python-ci` PR step after the existing dependency install.
- The step compiles `private_events_mcp`, `tests`, and `scripts`, then runs every
  `tests/test_private_events_mcp_*.py` test through the requested commands.
- Existing workflow jobs, dependency installation, and regression gates remain
  unchanged.

## Validation evidence

Commands run:

```text
PYTHONPATH=. python3 -m compileall -q private_events_mcp tests scripts
# passed (the local image exposes `python3`, while setup-python provides
# `python` in GitHub Actions)

python3 - <<'PY'
import yaml
from pathlib import Path
data = yaml.safe_load(Path('.github/workflows/ci.yaml').read_text())
assert 'python-ci' in data['jobs']
steps = data['jobs']['python-ci']['steps']
step = next(s for s in steps if s.get('name') == 'Run private-events MCP release gate')
assert step['run'].splitlines() == [
    'PYTHONPATH=. python -m compileall -q private_events_mcp tests scripts',
    'PYTHONPATH=. python -m pytest -q tests/test_private_events_mcp_*.py',
]
PY
# workflow-structure-ok

python3 -m venv /tmp/events-bot-mcp-ci-venv.mLGa1D
/tmp/events-bot-mcp-ci-venv.mLGa1D/bin/python -m pip install -q -r requirements.txt
PYTHONPATH=. /tmp/events-bot-mcp-ci-venv.mLGa1D/bin/python -m pytest -q \
  tests/test_private_events_mcp_*.py
# 29 passed in 2.34s

git diff --check
# passed
```

## Risks / notes

- `actionlint` and `yamllint` were not installed locally; PyYAML parsing plus a
  structural assertion validated the edited workflow.
- The initial system interpreter lacked the CI dependencies, so validation used
  a temporary venv populated from the same `requirements.txt` installed by the
  workflow.
- No code, documentation, tests, or other workflows were changed.

## Changed files

- `.github/workflows/ci.yaml`
- `.codex/lanes/mcp_ci_gate/RESULTS.md`
