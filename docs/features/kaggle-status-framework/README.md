# Kaggle Status Framework

Status: implementing
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
  "resource_leases": ["telegram_session:s22"]
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

Critical resources such as the shared Kaggle Telegram auth session must be leased
before use. The canonical resource key for the S22 Kaggle session is
`telegram_session:s22`. A live lease blocks another run from using the same auth
bundle until it expires or is released.

## Diagnostics

The server logs every accepted event as `kaggle_status.event` with run id, event,
phase, status, notebook, session id, progress keys, and resource action.
Reports written by Kaggle remain required, but they are no longer the only source
of truth: a run with outbound callbacks can be diagnosed before the final output
download finishes or fails.

## Regression Contract

This framework is a regression guard for opaque Kaggle runs. A scheduler retry
must have enough status evidence to show whether a previous attempt was still
alive, stuck, failed preflight, rendering, publishing, or writing reports before
the final Kaggle output download/report failed.
