# INC-2026-06-26 CrumpleVideo missing video output after intro font failure

Status: mitigated
Severity: sev2
Service: events-bot-new CrumpleVideo scheduled `/v tomorrow`
Opened: 2026-06-26
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-26-tg-story-message-forward.md`
Related docs: `docs/features/crumple-video/README.md`, `docs/operations/cron.md`

## Summary

The scheduled CrumpleVideo session `#763` on 2026-06-26 launched on Kaggle and
Kaggle reported the kernel as `COMPLETE`, but the server marked the session
`FAILED` with `missing video output`. No `crumple_video_final.mp4` was produced,
so Telegram story publication and the new story-message feed forward never ran.

## User / Business Impact

- The daily CrumpleVideo announcement for the next-day event digest was not
  published.
- The Telegram story-to-channel feed forward could not be validated in the real
  CrumpleVideo path because the render stopped before story publish.
- The status ledger misleadingly looked green (`done/report_written`) even
  though no final video existed.

## Detection

The operator asked why no CrumpleVideo story/feed forward appeared. Read-only
Telethon inspection of `@kenigevents` showed no active stories and no recent
`MessageMediaStory` post. Production DB/log inspection then showed session `#763`
failed with `missing video output`.

## Timeline

- 2026-06-26 14:45 UTC: session `#763`, profile `default`, was created for
  CrumpleVideo and pushed to kernel `zigomaro/crumple-video-video1`.
- 2026-06-26 14:46 UTC: story target config was built with `me`, VK wall,
  `@kenigevents`, `tg:@kenigevents:story-message`, and `@lovekenig`.
- 2026-06-26 14:47 UTC: Kaggle kernel started.
- 2026-06-26 14:48 UTC: notebook failed while rendering the intro image:
  `OSError: cannot open resource` from `ImageFont.truetype(...Benzin-Bold.ttf)`.
- 2026-06-26 14:48 UTC: notebook still sent `render_done` and `report_written`.
- 2026-06-26 14:49 UTC: server downloaded output, found no final mp4, and marked
  session `#763` as `FAILED` with `missing video output`.

## Root Cause

1. CrumpleVideo notebook assumed the static assets dataset was mounted exactly
   at `/kaggle/input/video-announce-assets`. In session `#763`, the notebook did
   not find that path, even though kernel metadata listed
   `zigomaro/video-announce-assets` as a dataset source.
2. The per-session dataset included only the older minimal font set
   (`BebasNeue-Bold.ttf`, `Cygre-*`) and did not include the intro renderer's
   required `Benzin-Bold.ttf`, `Oswald-VariableFont_wght.ttf`, or
   `DrukCyr-Bold.ttf` as loose fallback files.
3. The notebook wrapper converted `main_pipeline()` returning `False` into a
   normal notebook completion, so Kaggle reported `COMPLETE` and status callbacks
   incorrectly sent `render_done/report_written`.

## Contributing Factors

- The scheduled lane retried without GPU after a quota warning, making the run
  short and easy to mistake for a successful preflight-only run.
- Runtime logs had the true Python traceback, but the status ledger did not carry
  the exception because the notebook did not fail hard.

## Automation Contract

### Treat as regression guard when

- changing CrumpleVideo notebook asset loading, session dataset construction,
  Kaggle dataset source handling, status instrumentation, or final output checks;
- changing story publish / story-message logic that depends on a successful
  CrumpleVideo render.

### Affected surfaces

- `kaggle/CrumpleVideo/crumple_video.ipynb`
- `video_announce/scenario.py` `_copy_assets()`
- `tests/test_video_announce_crumple_assets.py`
- `tests/test_crumple_build_notebook.py`
- Kaggle status callbacks and server poller final output detection

### Mandatory checks before closure or deploy

- `tests/test_video_announce_crumple_assets.py`
- `tests/test_crumple_build_notebook.py`
- story publish regression tests when story fanout is in scope
- `kaggle/CrumpleVideo/build_notebook.py`
- `git diff --check`
- production health check after deploy
- live read-only evidence from the next CrumpleVideo run: final mp4 exists,
  story publish runs, and `tg:@kenigevents:story-message` succeeds or reports a
  clear required fanout error.

## Required evidence

- deployed SHA reachable from `origin/main`;
- focused pytest output;
- production env/code verification;
- next-run Kaggle log or server output showing final video and story-message
  status.

## Immediate Mitigation

No manual Telegram publication was attempted. The failed run was left as failed
for auditability.

## Corrective Actions

- Make the notebook search all `/kaggle/input/*` roots for assets instead of
  relying only on `/kaggle/input/video-announce-assets`.
- Add required intro fonts (`Benzin-Bold.ttf`, `Oswald-VariableFont_wght.ttf`,
  `DrukCyr-Bold.ttf`) to the per-session dataset as loose fallback files.
- Fail the notebook hard when `main_pipeline()` returns false before producing
  the final video, so Kaggle/status no longer looks green after a render failure.
- Add regression tests for required CrumpleVideo assets and notebook fail-fast
  behavior.

## Follow-up Actions

- [ ] Collect read-only evidence from the next CrumpleVideo run.
- [ ] Consider adding a status event for `final_video_missing` before the poller
      marks a session failed.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `.venv/bin/python -m pytest tests/test_video_announce_crumple_assets.py tests/test_crumple_build_notebook.py tests/test_video_announce_story_publish.py tests/test_kaggle_story_publish.py -q` -> `40 passed`
  - `.venv/bin/python kaggle/CrumpleVideo/build_notebook.py`
  - `.venv/bin/python -m py_compile video_announce/scenario.py`
  - `git diff --check`
- post-deploy verification: pending

## Prevention

CrumpleVideo must not depend on a single Kaggle mount path for required fonts,
and scheduled notebooks must fail the kernel when the final video is missing.
The server-side `missing video output` guard remains a last-resort safety net,
not the primary error signal.
