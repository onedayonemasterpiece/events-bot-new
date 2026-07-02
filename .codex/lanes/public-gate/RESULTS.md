# Lane public-gate Results

## Status
committed

## Requirement IDs
- R09

## Branch
agent/smart-update-vector-identity-gate/public-gate

## Worktree
/home/dev/.codex/worktrees/events-bot-new/public-gate

## Base SHA
f44a3f3db3112e03e2cbf6ba4e24fff44cd1afc8

## Head SHA
Final lane head SHA is reported in the worker final response. Note: embedding the exact final commit SHA inside this committed file would change that same SHA.

## Files changed
- site/scripts/export-production-preview-data.py
- tests/test_static_site_public_gate.py
- docs/features/smart-event-update/README.md
- CHANGELOG.md
- .codex/lanes/public-gate/RESULTS.md

## Commands run
- `git status --short --branch`
- `git rev-parse HEAD`
- `git branch --show-current`
- `cat AGENTS.md`
- `sed -n '1,220p' docs/README.md`
- `sed -n '1,180p' docs/operations/incident-management.md`
- `grep -nEi 'Smart Update|location|venue|prose|duplicate|non-event|public|quality|debug|leak|prompt|canonical|quarantine|review|rejected' docs/reports/incidents/README.md docs/reports/incidents/INC-*.md | head -80`
- `find . -path './.git' -prune -o \( -name 'export-production-preview-data.py' -o -name 'check-preview.mjs' -o -iname '*preview*data*' -o -iname '*check*preview*' \) -print`
- `grep -RIn --exclude-dir=.git --exclude='*.sqlite' --exclude='*.db' -E 'production preview|preview data|check-preview|export-production|public projection|identity_status|merged_into_event_id|canonical|quarantine|review' . | head -250`
- `git -C /home/dev/projects/events-bot-new show HEAD:site/scripts/export-production-preview-data.py > site/scripts/export-production-preview-data.py`
- `python3 - <<'PY' ... run tests/test_static_site_public_gate.py test_* functions ... PY`
- `python3 -m py_compile site/scripts/export-production-preview-data.py tests/test_static_site_public_gate.py`
- `git diff --check`

## Tests / verification
- PASS: manual Python invocation of all `tests/test_static_site_public_gate.py` test functions.
- PASS: `python3 -m py_compile site/scripts/export-production-preview-data.py tests/test_static_site_public_gate.py`.
- PASS: `git diff --check`.
- Environment note: `/usr/bin/python3 -m pytest tests/test_static_site_public_gate.py -q` could not run because pytest is not installed in this worktree environment.

## Risks
- The assigned base SHA did not contain a `site/` directory, although the lane writable scope named `site/scripts/...`. To stay within scope, this lane added `site/scripts/export-production-preview-data.py` from the existing static-site exporter snapshot in the sibling events-bot-new workspace, then applied the public projection gate there. No files outside the declared writable scope were edited.
- No TypeScript/check-preview second defense was added; R09 requested exporter-side primary gate and allowed TS defense as optional only.

## Merge notes
- Gate helpers to review during integration: `public_projection_gate_reason`, `event_active_where(..., columns)`, and `fetch_rows` include/control id handling.
- The gate treats missing identity/review/merge columns as old-schema-compatible absent fields, but if `identity_status` exists it must be exactly `canonical` for public export.
