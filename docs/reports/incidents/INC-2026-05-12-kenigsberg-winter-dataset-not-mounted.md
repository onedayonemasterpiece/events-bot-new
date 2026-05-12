# INC-2026-05-12-kenigsberg-winter-dataset-not-mounted

Status: open
Severity: sev3
Service: Kenigsberg Stories manual Kaggle MVP
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-notebook-escaped-newlines`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/release-governance.md`

## Summary

The second production `/kenigsberg` smoke progressed past the notebook syntax failure but failed in the renderer because the bot selected `period=winter` while Kaggle did not mount the expected `koenigsberg-winter` input directory.

## User / Business Impact

- Session `#254` failed with Kaggle `ERROR`.
- No test video was published to `@keniggpt`.
- Existing production surfaces stayed healthy; the impact is limited to the new manual MVP path.

## Detection

- The operator supplied Kaggle logs on 2026-05-12.
- Logs showed `KoenigsbergStories v2-mvp-heuristic-render`, `Using session bundle: /kaggle/input/kenigsberg-session-254-1778579952`, then `RuntimeError: Video dataset for period='winter' is not mounted`.

## Timeline

- 2026-05-12 11:59 Europe/Kaliningrad — operator ran `/kenigsberg`.
- 2026-05-12 11:59 Europe/Kaliningrad — bot created session `#254`, issue `#1`, period `winter`, dataset `zigomaro/kenigsberg-session-254-1778579952`.
- 2026-05-12 12:01 Europe/Kaliningrad — bot reported Kaggle error and sent logs.

## Root Cause

1. MVP period selection treated `winter` as equally eligible before any successful Kaggle mount smoke for that dataset.
2. Kaggle did not expose a `koenigsberg-winter` input directory to the runtime for session `#254`.
3. The renderer failed closed, which is correct for production isolation, but the server should not have selected any period at all; the Kaggle runtime should choose randomly from mounted inputs.

## Contributing Factors

- The first working smoke should minimize moving parts; randomizing period too early increased blast radius.
- The renderer error did not include the list of actually mounted `/kaggle/input` directories.

## Automation Contract

### Treat as regression guard when

- Changing Kenigsberg period selection.
- Adding or re-enabling a Kenigsberg video dataset.
- Changing Kaggle kernel `dataset_sources` for `zigomaro/koenigsberg-stories`.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`
- `scripts/render_kenigsberg_story.py`
- `fly.toml` Kenigsberg env
- Kaggle dataset mounts for `zigomaro/koenigsberg-stories`

### Mandatory checks before closure or deploy

- Server payload must not contain a preselected period/dataset.
- Kaggle runtime chooses randomly from actually mounted video datasets.
- Renderer errors include `available_inputs=[...]` when an expected dataset is missing.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Post-deploy evidence that `KENIGSBERG_STORIES_PERIODS` is absent.
- Fresh `/kenigsberg` smoke evidence.

## Immediate Mitigation

- Remove server-side period selection and the `KENIGSBERG_STORIES_PERIODS` env switch.
- Move random period selection into the Kaggle renderer based on actually mounted video datasets.
- Add mounted-input diagnostics to renderer dataset-missing errors.

## Corrective Actions

- Add recursive discovery for Kaggle layouts such as `/kaggle/input/datasets/...`.
- Add tests for nested Kaggle dataset discovery and random selection across mounted video datasets.

## Follow-up Actions

- [ ] Verify the exact Kaggle mount directory / access state for `zigomaro/koenigsberg-winter`.
- [ ] Re-enable winter only after a successful dedicated smoke.

## Release And Closure Evidence

- deployed SHA: `f4559c0b5b33f92d4b9072fdcea5665a61863d09`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1065`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRDTETS62172WHNWW4F844G3`
- regression checks:
  - `python3 -m py_compile handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py`
  - `python3 -m json.tool kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py tests/test_kaggle_client.py tests/test_video_announce_story_publish.py` -> `24 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1065`, service check passing.
  - Production env check: `KENIGSBERG_STORIES_PERIODS=1919-1940`, `KENIGSBERG_STORIES_KAGGLE_ENABLED=1`, `KENIGSBERG_STORIES_TEST_CHAT_ID=-1002210431821`.
  - Kenigsberg MVP state reset after failed smoke: `next_issue=1`, `used_thought_ids=[]`, `issues={}`.

Superseded by follow-up fix: `KENIGSBERG_STORIES_PERIODS` must be removed entirely; the renderer chooses from mounted datasets inside Kaggle.

## Prevention

- New periods should enter production behind explicit env opt-in until one Kaggle smoke proves the dataset is mountable.
