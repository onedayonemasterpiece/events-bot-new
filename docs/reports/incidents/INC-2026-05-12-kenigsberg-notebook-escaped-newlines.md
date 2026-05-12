# INC-2026-05-12-kenigsberg-notebook-escaped-newlines

Status: open
Severity: sev3
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: —
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The first production `/kenigsberg` smoke reached Kaggle but `zigomaro/koenigsberg-stories` failed before rendering. The notebook code cells contained literal backslash-n sequences (`\\n`) instead of real line breaks, so Papermill raised `SyntaxError: unexpected character after line continuation character` in cell 1.

## User / Business Impact

- The operator could launch `/kenigsberg`, but session `#253` failed with Kaggle `ERROR`.
- No test video was published to `@keniggpt`.
- The failure affected only the new manual MVP path; existing scheduler, CherryFlash and `/healthz` remained healthy.

## Detection

- Detected by the operator from Kaggle logs and the bot status message on 2026-05-12.
- Production runtime file mirror was not needed for root cause because the Kaggle notebook log contained the exact syntax error.

## Timeline

- 2026-05-12 11:43 Europe/Kaliningrad — operator ran `/kenigsberg`.
- 2026-05-12 11:43 Europe/Kaliningrad — bot created session `#253`, issue `#1`, dataset `zigomaro/kenigsberg-session-253-1778579008`.
- 2026-05-12 11:44 Europe/Kaliningrad — bot reported session `#253` failed with Kaggle `ERROR`.
- 2026-05-12 11:44 Europe/Kaliningrad — operator supplied Kaggle logs showing the notebook `SyntaxError`.

## Root Cause

1. `kaggle/KoenigsbergStories/koenigsberg_stories.ipynb` was generated with code-cell `source` entries containing literal `\\n`.
2. Kaggle/Papermill joined those source entries into a single Python line containing backslash continuations instead of line breaks.
3. Local validation only checked that the notebook JSON parsed; it did not compile code cells.

## Contributing Factors

- The MVP was shipped quickly from a handcrafted notebook JSON file.
- There was no regression test for notebook code-cell compilation.

## Automation Contract

### Treat as regression guard when

- Changing `kaggle/KoenigsbergStories/*.ipynb`.
- Changing Kenigsberg Kaggle launch/runtime packaging.
- Recreating notebook JSON manually.

### Affected surfaces

- `kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`
- `/kenigsberg` manual Kaggle generation
- Kaggle kernel `zigomaro/koenigsberg-stories`

### Mandatory checks before closure or deploy

- `python3 -m json.tool kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`
- `pytest -q tests/test_kenigsberg_notebook.py tests/test_kenigsberg_stories.py`
- A fresh `/kenigsberg` smoke must progress beyond notebook cell 1; if it fails later, the new error must be separately triaged.
- Production `/healthz` must remain `ok=true`, `ready=true` after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Kaggle log or bot status evidence for the post-fix run.
- Test output for the notebook compilation regression.

## Immediate Mitigation

- Fix the notebook code-cell source line breaks.
- Add a regression test that compiles every Kenigsberg notebook code cell and rejects literal `\\n` in joined source.

## Corrective Actions

- Added `tests/test_kenigsberg_notebook.py`.

## Follow-up Actions

- [ ] Close this incident after a fresh `/kenigsberg` run confirms Kaggle moves beyond the previous notebook syntax failure.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Notebook code cells are now compiled in tests, so this exact escaped-newline failure cannot be reintroduced silently.
