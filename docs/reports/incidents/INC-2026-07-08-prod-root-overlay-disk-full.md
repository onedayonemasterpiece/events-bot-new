# INC-2026-07-08 Prod Root Overlay Disk Full From Temporary Kaggle Outputs

Status: open
Severity: sev1
Service: Fly production bot `events-bot-new-wngqia` / root overlay `/tmp` / scheduled Kaggle-backed jobs
Opened: 2026-07-08
Closed: —
Owners: operations / bot runtime
Related incidents: `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`, `INC-2026-05-05-cherryflash-disk-full.md`, `INC-2026-06-13-kaggle-duplicate-videoannounce.md`
Related docs: `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`, `docs/features/cherryflash/README.md`, `docs/features/guide-excursions-monitoring/README.md`

## Summary

On 2026-07-08 production started failing scheduled writes/downloads with `OSError: [Errno 28] No space left on device`. This time the full filesystem was not the SQLite volume `/data`, but the Fly root writable overlay mounted as `/.fly-upper-layer` and used by `/tmp`.

Visible disk pressure came from temporary Kaggle output directories such as `/tmp/videoannounce-*` and `/tmp/guide-excursions-*`. Direct deletion removed the visible files, but `df` still reported the overlay at `100%` until the Fly machine was restarted/remounted.

## User / Business Impact

- Scheduled Kaggle-backed jobs could not create or download temporary outputs.
- Affected same-day jobs included:
  - `guide_monitoring` light run at `2026-07-08 11:20 UTC`;
  - `parse` source parsing run at `2026-07-08 12:15 UTC`;
  - `3di` new-event preview run at `2026-07-08 13:15 UTC`;
  - CherryFlash partner video sessions around `#839` / `#840`.
- The serving bot stayed healthy after mitigation checks, but scheduled product surfaces had partial/missed same-day work that required follow-up catch-up decisions.

## Detection

- Operator reported production disk exhaustion.
- `df -hT` on Fly showed `/` and `/.fly-upper-layer` at `100%`, while `/data` was `71%` used.
- Runtime log mirror and Fly logs contained repeated `Errno 28` for `/tmp/videoannounce-839`, source parser temp dirs, and guide monitoring temp dirs.

## Timeline

- `2026-07-08 10:35 UTC`: CherryFlash partner eco session `#839` started and later reached Kaggle `COMPLETE`.
- `2026-07-08 11:15 UTC` onward: poller repeatedly failed to download `/tmp/videoannounce-839` due `Errno 28`.
- `2026-07-08 11:20 UTC`: scheduled `guide_monitoring` light run failed immediately with `/tmp/tmp...` `Errno 28`.
- `2026-07-08 12:15 UTC`: scheduled source parsing recorded `errors_count=3` for theatres/philharmonia/qtickets temp paths.
- `2026-07-08 13:15 UTC`: scheduled `3di` failed with `/tmp/tmp...` `Errno 28`.
- `2026-07-08 13:20 UTC`: temporary directories under `/.fly-upper-layer/tmp` were removed; visible `du` dropped from about `7.8G` to about `29M`, but `df` still showed `100%`.
- `2026-07-08 13:21 UTC`: Fly machine `2860d45f312248` was restarted; root overlay recovered to about `57M` used / `7.4G` free, `/tmp` writes succeeded, and SQLite `PRAGMA quick_check` returned `ok`.
- `2026-07-08 13:22 UTC`: production secret `ENABLE_RUNTIME_FILE_LOGGING` was reset to `0` to match `fly.toml`/runtime-log policy.

- `2026-07-19 17:11 UTC`: static-site job `38162` exhausted its fourth attempt before Kaggle push with `FileNotFoundError: No usable temporary directory`; the checked candidate pointer stayed on the 05:55 UTC success.
- `2026-07-19 20:31 UTC`: recurrence triage found `/` at `100%` with zero free bytes while `/data` retained about `1.55 GiB`; `/.fly-upper-layer/tmp` held about `6.45 GiB` in terminal-looking `videoannounce-919` through `922`, and `/app/artifacts/codex/static-site-builder` held about `1.85 GiB` in retained outputs. The builder lease and static claim were idle, and no deleted-open file descriptors existed.
- The recurrence again bypassed `/healthz`, which measured `/data` but not the root scratch filesystem.

## Root Cause

1. Large temporary Kaggle output bundles accumulated on the Fly root overlay under `/tmp`, especially `videoannounce-*` outputs.
2. The current cleanup/retention contract protected `/data` better than the root overlay; `/tmp` did not have a sufficient budget/retention guard for video output downloads.
3. After deleting visible files directly through the Fly upper-layer view, the filesystem did not report free blocks to new `/tmp` writes until the machine was restarted/remounted.
4. Static candidate results were retained under `/app/artifacts` on the same bounded root overlay; repeated successful builds added roughly 440 MiB each without a root-overlay retention budget.
5. `/healthz` checked the persistent `/data` volume only, so the service stayed green while Python could not allocate any temporary directory and Smart Update static refreshes dead-lettered.

## Contributing Factors

- Runtime file logging was enabled in production through a secret override (`ENABLE_RUNTIME_FILE_LOGGING=1`) despite `fly.toml` and docs requiring `0`; this did not cause the root overlay failure but was a config drift on a known disk-pressure surface.
- Some scheduled runs mark handoff success before terminal publication/download status, so missed same-day product delivery must be checked from `videoannounce_session` and follow-up logs, not only `ops_run.status`.

## Automation Contract

### Treat as regression guard when

- changing CherryFlash/CrumpleVideo/Kaggle output download paths or temp-directory retention;
- changing guide monitoring or source parser temp-output handling;
- handling any production `Errno 28`, `database or disk is full`, `/tmp` write failure, or Fly root overlay pressure;
- changing production runtime logging env/secrets.

### Affected surfaces

- Fly root writable overlay `/.fly-upper-layer` and `/tmp`;
- `video_announce.kaggle_client` output downloads;
- `video_announce.poller` recovery for completed Kaggle kernels;
- scheduled `guide_monitoring`, `source_parsing`, `3di`, and CherryFlash partner tracks;
- production runtime logging env and `/data/runtime_logs`.

### Mandatory checks before closure or deploy

- Verify `df -hT / /.fly-upper-layer /tmp /data` has enough free space.
- Verify the static-site preflight observes the root/temp scratch floor, not only `/data`, and fails before claiming/retrying work when scratch is unwritable.
- Verify terminal static output directories and terminal video output directories are bounded without deleting an active/recoverable handoff.
- Verify `/tmp` write probe succeeds.
- Verify `/healthz` returns `ok=true`, `ready=true`, `db=ok`.
- Verify SQLite `PRAGMA quick_check` returns `ok`.
- Verify `ENABLE_RUNTIME_FILE_LOGGING=0` unless explicitly budgeting temporary incident logging.
- Inspect same-day failed scheduled `ops_run` rows and either run catch-up or document why a catch-up is intentionally deferred/blocked.
- For CherryFlash sessions affected by disk pressure, verify terminal `videoannounce_session` status and clean up temporary output directories only after output is persisted or the session is intentionally failed/superseded.

### Required evidence

- before/after `df` for `/` / `/.fly-upper-layer` / `/tmp` / `/data`;
- list of removed temp directories and sizes;
- runtime/Fly log snippets with `Errno 28`;
- post-restart `/healthz`, `/tmp` write probe, and SQLite quick check;
- release/config evidence for `ENABLE_RUNTIME_FILE_LOGGING=0`;
- follow-up status for same-day affected scheduled jobs.

## Immediate Mitigation

- Removed temporary directories from `/.fly-upper-layer/tmp`: `videoannounce-834` through `videoannounce-840`, `guide-excursions-*`, `tg-monitor-*`, and parser output temp dirs.
- Restarted Fly machine `2860d45f312248` to recover root overlay free blocks.
- Reset production `ENABLE_RUNTIME_FILE_LOGGING=0`.

## Corrective Actions

- Pending: add explicit retention/free-space guard for Fly root-overlay `/tmp` Kaggle output downloads.
- Pending: move or bound retained static-site result artifacts so successful secret candidates cannot fill `/app` across repeated Smart Update builds.
- Pending: extend readiness/diagnostics to report root scratch capacity and an actual temporary-file probe.
- Pending: make disk-pressure recovery avoid re-downloading large completed outputs repeatedly when a session is already known `COMPLETE`.
- Pending: add/verify alerting before root overlay `/tmp` reaches write failure.

## Follow-up Actions

- [ ] Add root-overlay `/tmp` retention/budget guard for video and guide/source Kaggle outputs.
- [ ] Add a scheduled disk-space alert covering both `/data` and `/.fly-upper-layer`.
- [ ] Review session `#839` / `#840` terminal state and decide catch-up/supersede flow for the 2026-07-08 partner tracks.
- [ ] Review same-day `parse`, `guide_monitoring`, and `3di` failed slots and run catch-up where product-required.

## Release And Closure Evidence

- deployed SHA: no code deploy during immediate mitigation; production image stayed `events-bot-new-wngqia:deployment-01KWXREFD6Y71V2XR4CEF9FDAC`.
- config change: `flyctl secrets set ENABLE_RUNTIME_FILE_LOGGING=0 -a events-bot-new-wngqia`.
- restart path: `flyctl machine restart 2860d45f312248 -a events-bot-new-wngqia`.
- post-restart verification:
  - `/healthz` returned HTTP 200 with `ok=true`, `ready=true`, `db=ok`;
  - root overlay recovered from `100%` used to writable state;
  - `/tmp` write probe succeeded;
  - `/data` remained around `71%` used;
  - SQLite `PRAGMA quick_check` returned `ok`.

## Prevention

- Treat `/tmp` on Fly as capacity-constrained production storage, not disposable infinite scratch.
- Large Kaggle output downloads must be pruned immediately after terminal persistence/publication or after an intentional fail/supersede decision.
- Runtime log mirror must remain disabled by default on the current Fly volume unless an incident has an explicit retention budget.
