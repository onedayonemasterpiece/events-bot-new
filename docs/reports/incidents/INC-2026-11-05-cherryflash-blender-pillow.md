# INC-2026-11-05-cherryflash-blender-pillow

Status: open
Severity: sev2
Service: CherryFlash popular_review / Kaggle Blender renderer
Opened: 2026-11-05
Closed: —
Registry: inc_9ba5bc292ff0ca58f13fc00c
Related incidents: INC-2026-11-04-cherryflash-cross-year-label
Related docs: docs/features/cherryflash/README.md

## Summary and impact

Session 1564 failed while rendering its guide excursion scene: Blender could not import PIL, then the orchestrator found zero of 177 expected frames. No complete video was produced. This is a mechanical dependency installation failure, not an event date or identity decision.

## Detection and timeline

All times UTC, November 5, 2026.

- 07:44:11: ordinary session 1564 started with kernel zigomaro/cherryflash and dataset cherryflash-session-1564-1793864680.
- 08:04:50–08:06:51: after the Events release, the SAME remote session resumed fresh RUNNING polls and callbacks. The restart did not lose its poller.
- 08:07:09: kernel exited; 08:07:51 ERROR observed. Downloaded output contained guide input, intro and error log, but no finished video.
- 08:07:53: session terminal FAILED. Trace: ModuleNotFoundError: No module named 'PIL' at render_cherryflash_guide_true3d_v4.py:30, followed by unexpected frame count 0 != 177.
- 08:14:49: existing watchdog reconciled the failure and retried the missing ordinary slot. Session 1565 started via ops 11650, not a manual duplicate. RUNNING is not delivery acceptance.
- 08:27–08:31: official Blender 4.5.4 reproduced the import failure with exit 17; installing Pillow into its bundled Python made the same Blender command exit 0.

## Root cause and contributing factors

The bootstrap installed Pillow into notebook Python. The approved guide renderer executes in Blender's separate bundled Python, which did not contain it. Successful blender --version never checked this dependency. Blender's ordinary exit code can also hide a Python error unless python-exit-code is supplied.

The PIL import was introduced in 323cb1e407 on July 11; the notebook bootstrap last changed in f86fa4362 on June 13. Both predate the November recovery/year-label repairs. This proves a pre-existing dependency mismatch, not why this particular selected scene exposed it.

## Corrective action

Four lines reuse the existing bootstrap: discover bundled Python, run ensurepip, install the already-required Pillow there, and smoke the actual Blender import with python-exit-code 1. No new retry, state, worker, renderer or publication path. Approved scene layout and event guards are unchanged.

## Automation contract

### Treat as regression guard when

Changing CherryFlash bootstrap, Blender bundle/version or guide renderer dependencies.

### Affected surfaces

kaggle/CherryFlash/cherryflash.ipynb, scripts/render_cherryflash_guide_true3d_v4.py and the existing popular_review Kaggle input bundle.

### Mandatory checks before closure or deploy

- Distinguishing bootstrap test with separate notebook/Blender package inventories: baseline fails, fix passes.
- Actual bundled Blender import before/after installation; notebook Python import alone is insufficient.
- Required CI, merged-main ancestry, full source qualification and isolated preflight before release.
- Preserve healthy intake and reconcile session 1565 before any retry; no competing kernel.
- Verify a subsequent ordinary render output and publication separately. Dependency smoke alone does not close this incident.

### Required evidence

Retained /home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103: video1564-failure-tail-0822.log, video1564-1565-targeted-0821.log, blender-pillow-before.log, blender-pillow-after.log, blender-pillow-real-before.log and blender-pillow-real-after.log. Reproducible official bundle is in managed temporary /home/dev/artifacts/events-bot-new/20261105T082436Z-cherryflash-blender-pillow-runtime.

## Release and closure evidence

Baseline: one distinguishing failure, three controls pass. Fixed: 25 notebook, guide-scene and Kaggle-client tests pass. Actual Blender 4.5.4 fails without PIL and succeeds after bundled-interpreter installation (Pillow 13.0.0). These are local controls, not production render acceptance. Production remains 544379c810e2c9b5b418658e7a2b5b30468a8b32; this fix is not delivered.

## Follow-up

- [ ] Merge green CI, qualify and deliver exact main at a safe import boundary.
- [ ] Reconcile session 1565; verify subsequent existing-pipeline render and publication without a duplicate kernel.


## November 5 08:51 UTC — qualified release delivered, actual render pending

PR831 merged after all four required CI41995242091 green. Exact main aa489fde8b47951738b78bf7a00494bb3e2cb1cc is now delivered, immutable sha256:e887c63c249c2bf67e1a270c2365967a616cb43052d3df33f0297f68515429dd, image intake-blender-pillow-20261105. Full4449-file source proof, initialized isolated actual bootstrap (separate interpreter inventories), inherited controls, wrong-SHA refusal and HTTP isolation passed. Early HTTP polls during bootstrap returned reset; final isolated HTTP succeeded with ready=false and DB skipped, as required. Owned preflight removed.

Original cutover guards passed with no active ops, MCP intake or background claims; no exception or manual claim reset. Untracked work inspection showed only the normal launcher. Explicit live source/settings/SQLite/identity/date/ordinal/queue checks passed, ready=true, scheduler normal, Kotopogoda unchanged. No healthy source import interrupted. Retained blender-pillow-delivery-receipt.json and live/cutover/preflight logs.

Existing session1565 also terminal FAILED before release; its exact downloaded log reproduces the same missing PIL and 0/177 guide frames, not another hypothesis. No manual kernel launched. Normal enabled popular-review watchdog next tick09:00:51 UTC can recover today's absent successful slot using the existing pipeline and new notebook. Actual new kernel output, complete render and publication remain unaccepted; incident stays open. Do not duplicate an active retry or treat a RUNNING state as delivery.
