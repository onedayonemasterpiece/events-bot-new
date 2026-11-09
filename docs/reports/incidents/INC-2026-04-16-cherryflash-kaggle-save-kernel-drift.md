# INC-2026-04-16-cherryflash-kaggle-save-kernel-drift CherryFlash Kaggle SaveKernel Drift

Status: open
Severity: sev2
Service: CherryFlash / `/v` / Kaggle launcher
Opened: 2026-04-16
Closed: —
Owners: video announce runtime
Related incidents: `INC-2026-04-10-crumple-story-prod-drift`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/release-governance.md`

## Summary

CherryFlash `/v -> popular_review` sessions repeatedly failed to launch on Kaggle even though the server logged `kernel deployed successfully`. The actual Kaggle `SaveKernel` response still contained launch errors, so the runtime silently kept an older CherryFlash notebook revision on Kaggle and reported false-positive launch success.

## User / Business Impact

- manual CherryFlash runs did not start a real fresh Kaggle render;
- operators saw `local:CherryFlash` / `zigomaro/cherryflash` status noise and repeated failed sessions instead of a real render;
- repeated retries burned operator time and weekly GPU quota while leaving production unhealed.

## Detection

- user-reported repeated failed CherryFlash sessions (`#161`..`#164`);
- Kaggle UI still showed older CherryFlash notebook revisions while prod app already had newer notebook code;
- direct local reproduction of `api.kernels_push()` returned `error="Maximum weekly GPU quota..."`, but the wrapper still logged success.

## Timeline

- 2026-04-16: repeated CherryFlash `/v` launches fail while CrumpleVideo continues to work.
- 2026-04-16: investigation shows server deploys are reaching the correct Fly app, but Kaggle source remains stale.
- 2026-04-16: direct Kaggle API reproduction proves `SaveKernel` returns an error for CherryFlash GPU launches and that CPU push updates the source immediately.

## Root Cause

1. `video_announce.kaggle_client.deploy_kernel_update()` treated `api.kernels_push()` as successful if the Python call returned, without inspecting `ApiSaveKernelResponse.error`.
2. CherryFlash requested a Kaggle GPU kernel by default; once weekly GPU quota was exhausted, `SaveKernel` returned an error instead of updating the kernel source.
3. The wrapper still logged `kernel deployed successfully`, so CherryFlash drifted onto stale Kaggle notebook code and operators were misled about launch state.

## Contributing Factors

- CherryFlash also depends on a fresh per-run `cherryflash-session-*` dataset source, so `invalidDatasetSources` can lag even after dataset creation looks `ready`.
- Earlier incident fixes around dataset readiness and metadata bind checks masked the new failure mode until the raw `SaveKernel` response was inspected directly.

## Automation Contract

### Treat as regression guard when

- touching `video_announce/kaggle_client.py`;
- touching CherryFlash Kaggle launch / dataset assembly / kernel push logic;
- changing CherryFlash GPU/CPU kernel metadata defaults.

### Affected surfaces

- `video_announce/kaggle_client.py`
- CherryFlash launcher flow in `video_announce/scenario.py`
- `kaggle/CherryFlash/`
- Kaggle `SaveKernel` API response handling

### Mandatory checks before closure or deploy

- targeted `pytest` for `tests/test_kaggle_client.py` and CherryFlash pipeline tests;
- prove that `deploy_kernel_update()` fails on non-empty `SaveKernel.error`;
- prove that CherryFlash retries on CPU when Kaggle returns the weekly GPU quota error;
- prove that `invalidDatasetSources` fails explicitly without resubmitting an already-created version (corrected by `INC-2026-11-09-kaggle-account-migration`);
- verify deployed SHA is reachable from `origin/main`.

### Required evidence

- deployed SHA;
- test output for `tests/test_kaggle_client.py` and relevant CherryFlash pipeline tests;
- direct evidence that CherryFlash no longer logs success on a failing `SaveKernel` response.

## Immediate Mitigation

- direct manual Kaggle API inspection was used to identify the real `SaveKernel` error instead of trusting wrapper logs.

## Corrective Actions

- inspect `ApiSaveKernelResponse` directly;
- treat `error` as fatal;
- retry CherryFlash once on CPU for weekly GPU quota failures;
- retry fresh `cherryflash-session-*` `invalidDatasetSources` for a bounded window before failing.

## Follow-up Actions

- [ ] Confirm on a real CherryFlash `/v` run that the CPU fallback path completes within acceptable runtime.
- [ ] Decide whether CherryFlash should stay CPU-first by default or keep GPU-as-preferred with bounded fallback.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Kaggle kernel launch wrappers must not treat `SaveKernel` as successful without checking `ApiSaveKernelResponse.error` and invalid source lists.

## Correction, 2026-11-09

The former invalid-source retry requirement is superseded by `INC-2026-11-09-kaggle-account-migration`: SaveKernel can return a created version together with invalid sources. Repeating push creates duplicate executions. Await dataset readiness before submission using the existing helper, preserve explicit source-binding errors, and do not resubmit accepted versions. GPU-quota rejection without an accepted version retains its existing CPU fallback.
