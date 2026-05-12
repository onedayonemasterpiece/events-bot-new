# INC-2026-05-12-kenigsberg-notebook-escaped-newlines

Status: monitoring
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
- Runtime file mirror check after hotfix deploy: `RUNTIME_LOG_DIR=/data/runtime_logs` exists with rotated files; current env did not explicitly set `ENABLE_RUNTIME_FILE_LOGGING`.

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

- deployed SHA: `cd88a24494610da70caedbfb8e5d797603c768db`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1064`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRDSJAF879N2K0ER51XGCQE9`
- regression checks:
  - `python3 -m json.tool kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`
  - `python3 -m py_compile scripts/render_kenigsberg_story.py handlers/kenigsberg_stories_cmd.py video_announce/kaggle_client.py video_announce/poller.py`
  - `.venv/bin/pytest -q tests/test_kenigsberg_notebook.py tests/test_kenigsberg_stories.py tests/test_kaggle_client.py tests/test_video_announce_story_publish.py` -> `22 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1064`, service check passing.
  - Production env check: `KENIGSBERG_STORIES_TEST_CHAT_ID=-1002210431821`, `KENIGSBERG_STORIES_KAGGLE_ENABLED=1`, `KAGGLE_USERNAME=zigomaro`.

## Prevention

- Notebook code cells are now compiled in tests, so this exact escaped-newline failure cannot be reintroduced silently.
