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
3. The renderer failed closed, which is correct for production isolation, but the selector should not have chosen an unverified period for the first MVP smoke.

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

- Period selection defaults to only `1919-1940`.
- `winter` can only be selected by explicit `KENIGSBERG_STORIES_PERIODS=winter,...`.
- Renderer errors include `available_inputs=[...]` when an expected dataset is missing.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Post-deploy env evidence for `KENIGSBERG_STORIES_PERIODS=1919-1940`.
- Fresh `/kenigsberg` smoke evidence.

## Immediate Mitigation

- Restrict default production period selection to `1919-1940`.
- Keep `winter` as an explicit opt-in via `KENIGSBERG_STORIES_PERIODS` after its Kaggle mount is verified.
- Add mounted-input diagnostics to renderer dataset-missing errors.

## Corrective Actions

- Add `_enabled_periods()` with safe default.
- Add tests for default period selection and explicit winter opt-in.

## Follow-up Actions

- [ ] Verify the exact Kaggle mount directory / access state for `zigomaro/koenigsberg-winter`.
- [ ] Re-enable winter only after a successful dedicated smoke.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- New periods should enter production behind explicit env opt-in until one Kaggle smoke proves the dataset is mountable.
