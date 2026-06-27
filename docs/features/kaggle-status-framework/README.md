# Kaggle Status Framework

Status: deployed / monitoring
Owner surface: server Kaggle launchers, Kaggle runtime/notebooks, scheduled recovery

## Scope

This framework is the mandatory status, heartbeat, and diagnostic layer for every
events-bot Kaggle runtime. Repeated status callbacks with the same `event_uid`
must not corrupt run state; public publication attempts are ordinary side
effects and are not silently deduplicated.

- CherryFlash: `kaggle/CherryFlash/cherryflash.ipynb`
- CrumpleVideo: `kaggle/CrumpleVideo/crumple_video.ipynb`
- VideoAfisha: `kaggle/VideoAfisha/video_afisha.ipynb`
- KoenigsbergStories: `kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`
- Preview3D: `kaggle/Preview3D/preview_3d.ipynb`
- TelegramMonitor: `kaggle/TelegramMonitor/telegram_monitor.ipynb`, `telegram_monitor.py`
- GuideExcursionsMonitor: `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.ipynb`, `guide_excursions_monitor.py`
- parser/probe kernels: `ParseDomIskusstv`, `ParsePhilharmonia`, `ParsePyramida`, `ParseQtickets`,
  `ParseTheatres`, `TheatresAfisha`, `UniversalFestivalParser`, `TelegraphCacheProbe`,
  `LollipopCanary`, `GemmaKey2Probe`, `E2ETests`, `AfishaThumb`, `LimeglowCutoutProbe`,
  and future parser kernels.

## Runtime Contract

Every server-created session dataset must include `kaggle_run.json`:

```json
{
  "run_id": "videoannounce:664",
  "session_id": 664,
  "kind": "cherryflash",
  "notebook": "CherryFlash",
  "callback_url": "https://<app>/internal/kaggle/run-event",
  "token": "<one-time secret token>",
  "resource_leases": ["telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY"]
}
```

The token is stored server-side as a hash only. Kaggle sends it in the JSON body.
Callbacks are best-effort from Kaggle, but the notebook must also write local
`kaggle_status_events.jsonl` so output artifacts remain useful when outbound
networking is degraded.

## Integration Matrix

- Server ledger/callback endpoint: implemented in `kaggle_status.py` and
  `/internal/kaggle/run-event`.
- Generic kernel staging: `video_announce/kaggle_client.py` ships
  `kaggle_status_client.py` with local kernel pushes and auto-instruments the
  temporary pushed copy of `.ipynb` kernels with tagged status cells. It also
  wraps plain `.py` script kernels in the temporary push folder when the script
  does not already import `kaggle_status_client`. Source notebooks/scripts are
  not edited by this runtime staging step.
- Video announcement sessions: `video_announce/scenario.py` writes
  `kaggle_run.json` and status client into CherryFlash/CrumpleVideo/VideoAfisha
  session inputs without changing publication decisions.
- Preview3D sessions: `preview_3d/handlers.py` writes `kaggle_run.json` into
  the payload dataset and verifies the payload/status files before pushing the
  instrumented `Preview3D` notebook.
- KoenigsbergStories sessions: `handlers/kenigsberg_stories_cmd.py` writes
  `kaggle_run.json` into the session dataset, uses the actual story auth scope
  as the Telegram session lease key, and verifies the status file before
  pushing the instrumented notebook.
- Telegram session monitors: `TelegramMonitor` and `GuideExcursionsMonitor`
  create signed run configs, acquire `telegram_session:s22`, emit alive
  progress, release leases, and write terminal status events.
- Parser script kernels: `ParseQtickets`, `ParsePyramida`,
  `ParseDomIskusstv`, and `UniversalFestivalParser` emit
  `kernel_started`, `alive`, progress counters, and `report_written`.
- Parser launchers with DB context: source parsing, festival parsing, ticket
  site queue, Pyramida, Dom Iskusstv, Philharmonia, and Qtickets create a status
  dataset when `Database` is available.
- Notebook-only kernels (`CherryFlash`, `CrumpleVideo`, `VideoAfisha`,
  `KoenigsbergStories`, `Preview3D`, `ParseTheatres`, `ParsePhilharmonia`) get
  `kernel_started`, per-cell `cell_started`, alive heartbeat, optional
  `render_done`, and terminal `report_written` events from the temporary
  push-time instrumentation. If `resource_leases` are present in
  `kaggle_run.json`, the injected bootstrap acquires them before the notebook
  body runs and releases them on terminal/exit.
- Plain script kernels without built-in status calls get the same baseline
  `kernel_started`, `alive`, terminal `report_written`, and resource
  acquire/release behavior through a temporary `runpy` wrapper.

## Events And Heartbeat

All runtimes emit these phase events where applicable:

- `kernel_started`
- `preflight_ok`
- `render_started`
- `render_done`
- `publish_target_started`
- `publish_target_done`
- `publish_target_failed`
- `report_written`

Long-running notebooks also emit `alive` every 60 seconds with useful progress
fields. Examples: current phase, elapsed seconds, selected/total scenes, rendered
frames, current target label, processed/total messages, parser source URL,
downloaded media count, output bytes, and last successful step.

## Ledger And Diagnostics

Server callbacks are recorded in:

- `kaggle_run_ledger` for the current state of a run;
- `kaggle_run_event` for append-only event history;
- `kaggle_resource_lease` for critical exclusive resources.

Public side effects are represented only as status events such as
`publish_target_started`, `publish_target_done`, and `publish_target_failed`.
The framework must not silently suppress or deduplicate publication attempts.
Any future "skip duplicate publication" policy is a separate product decision
and must be approved explicitly.

Critical resources such as Kaggle Telegram auth sessions must be leased before
use. The canonical resource key is now based on the actual auth source, for
example `telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY` or
`telegram_session:env:TELEGRAM_AUTH_BUNDLE_S22_VIDEO1`; monitoring jobs that use
the shared S22 session still use `telegram_session:s22`. A live lease blocks
another run from using the same auth bundle until it expires or is released.
Resource acquisition is fail-closed only after bounded callback attempts. The
Kaggle helper retries transient `/internal/kaggle/run-event` timeouts for
`resource_acquire` before aborting, while an explicit `resource_action=blocked`
response still stops immediately. Defaults are
`KAGGLE_STATUS_RESOURCE_ACQUIRE_ATTEMPTS=4`,
`KAGGLE_STATUS_RESOURCE_ACQUIRE_TIMEOUT_SEC=20`, and
`KAGGLE_STATUS_RESOURCE_ACQUIRE_RETRY_DELAY_SEC=3`. Local
`kaggle_status_events.jsonl` records each attempt with a stable
`resource_acquire:<key>` event UID so callback transport failures are
distinguishable from a real busy holder.

For long CPU renders, `alive` callbacks renew active leases for the same
`run_id` (`KAGGLE_RESOURCE_LEASE_RENEW_TTL_SECONDS`, default `10800`) so a valid
render does not lose its Telegram session lease mid-run. To keep the production
DB bounded, high-frequency `alive` callbacks update `kaggle_run_ledger` every
time but are coalesced in `kaggle_run_event` when the previous `alive` event for
the same phase is newer than `KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS`
(default `300` seconds).

## Diagnostics

The server logs every accepted event as `kaggle_status.event` with run id, event,
phase, status, notebook, session id, progress keys, and resource action.
Reports written by Kaggle remain required, but they are no longer the only source
of truth: a run with outbound callbacks can be diagnosed before the final output
download finishes or fails.

Video pollers must not mark a run as `FAILED` just because the fixed local
`VIDEO_KAGGLE_TIMEOUT_MINUTES` window has elapsed while the Kaggle ledger still
has a fresh heartbeat. They extend polling in bounded increments
(`VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES`,
`VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES`) and still stop at the absolute
ceiling (`VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES`, default `720`) if the runtime
keeps running without reaching a terminal output/report.

The freshness window for notebook heartbeats defaults to 15 minutes. The
injected Kaggle helper normally emits `alive` much more often; 15 minutes is the
operator-facing SLA for deciding that a long render is still alive rather than
silently stale.

Before accepting an opaque terminal failure from Kaggle, video pollers must also
check the notebook ledger. If Kaggle says `error`/`failed` while the notebook
has a fresh non-terminal heartbeat, the poller continues waiting and the status
message shows the notebook phase/progress. If there is no fresh heartbeat, the
poller probes the kernel output before `FAILED`. This applies to repeated
`UNKNOWN`/empty status, Kaggle `error`/`failed`, and timeout paths: if a final
video/report is already downloadable, the session is classified from that
artifact and any story/publish report instead of blindly becoming `FAILED`.
Operator-initiated Kaggle cancellation is the exception: cancelled states are
respected as an explicit stop and must not be output-recovered into publication.

If no video is present but `story_publish_report.json` contains a deterministic
target blocker such as `BOOSTS_REQUIRED`, the session is `PUBLISH_BLOCKED`
rather than a render failure. A generated video that is too large for the bot
delivery limit is also `PUBLISH_BLOCKED` with `video_url` preserved, because the
artifact exists and the next action is a narrow publish/encode fix, not an
uncontrolled full rerender.

## Regression Contract

This framework is a regression guard for opaque Kaggle runs. A scheduler retry
must have enough status evidence to show whether a previous attempt was still
alive, stuck, failed preflight, rendering, publishing, or writing reports before
the final Kaggle output download/report failed.

## Deployment Evidence

- 2026-06-13: deployed from
  `hotfix/kaggle-status-framework-main-20260613` at `f4727ca0`, based on
  `origin/main` `f4727ca0`.
- Fly release: `events-bot-new-wngqia` machine version `1395`, image
  `deployment-01KV1GH881SED22QQK5HS7FV5C`.
- Runtime probe confirmed `/app/kaggle_status.py`,
  `/app/kaggle/kaggle_status_client.py`, `preview3d:` status wiring,
  `kenigsberg:` status wiring, and Telegram resource lease wiring are present.
- `/healthz` returned `ok=true`, `ready=true`, DB `ok`, scheduler `ok`.
- Local focused checks passed: `py_compile`, `git diff --check`, no
  `story_publish.py` diff, and `11 passed` for
  `tests/test_kaggle_status.py` plus
  `tests/test_kaggle_notebook_status_instrumentation.py` (pytest still needed a
  manual interrupt after the green summary because the interpreter hung during
  shutdown).
- Resource lease diagnostics now expire stale active rows during status config
  creation and callback handling, so an already-expired lease does not continue
  to look active until another holder tries to acquire the same key.
