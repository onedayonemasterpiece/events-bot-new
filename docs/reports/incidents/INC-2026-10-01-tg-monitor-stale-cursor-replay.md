# INC-2026-10-01 Telegram Monitoring stale-cursor replay

Status: open
Severity: sev1
Service: Telegram Monitoring / Kaggle source producer
Opened: 2026-10-01
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-19-tg-monitoring-runtime-starvation`, `INC-2026-09-28-lite-fallback-overload`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`

## Summary

The scheduled 57-source monitor has not imported Telegram posts since 19
September. Its stale source cursors bypass the configured three-day cutoff, so
each failed all-source run parses the same historical gap. Gemma errors and
the exhausted Lite reserve make each replay slower; the result bundle is only
imported after all 57 sources finish.

## User / Business Impact

- No new Telegram `event_source` rows after 19 September despite active source
  posts and apparently healthy scheduler/HTTP checks.
- Three recent full runs hit the host timeout without importing any candidate;
  later sources receive no processing.
- Repeated recovery alerts are a symptom of those terminal Kaggle runs.

## Detection And Timeline

- 2026-09-19 08:36 UTC: last Telegram `event_source` import; source cursors stop
  advancing.
- 2026-09-28 through 09-30: full monitor runs fail after about 17,170 seconds.
- 2026-09-30: the producer scans 479 messages across only the first 29 of 57
  sources in about 12 hours; no result bundle reaches the importer.
- 2026-10-01 04:41 UTC: next catch-up starts. By 04:53 it is still on source
  2/57. The operator-provided Kaggle log records nine unique Gemma errors in
  the first minutes (six 45-second timeouts, two HTTP 500, one HTTP 503); the
  Gemini 3.5 Flash-Lite reserve reports RPD exhaustion.
- 2026-10-01 04:54 UTC: operator cancels the run. Kaggle reports
  `CANCEL_ACKNOWLEDGED`; the exact S22 resource lease is released.
- 2026-10-01 04:56 UTC: all 57 source cursors are retained in incident evidence.
  One-source, 24-hour-window canary starts at 04:58 with a separate model route.
- 2026-10-01 05:01 UTC: that canary finishes after 218 seconds, processing two
  recent posts but importing no events. Both are terminal
  `source_evidence_incomplete` because attachment OCR did not complete.
  Its primary `gemini-2.5-flash` calls on key 3 returned 404 (model unavailable
  to new users); `gemini-3.6-flash` fallback reached RPD.
- 2026-10-01 05:06 UTC: read-only quota ledger confirms key 3 is at 500/500
  RPD for `gemini-3.5-flash-lite` and 20/20 for `gemini-3.6-flash`;
  `gemini-3.1-flash-lite` is at 130/500. A one-message forced replay on key 3
  with 3.1 Flash-Lite starts at 05:07 UTC.

## Root Cause

1. `scan_source` applies `TG_MONITORING_DAYS_BACK` only when a source has no
   `last_scanned_message_id`. Existing stale cursors therefore scan the full
   gap rather than the configured freshness window.
2. The producer writes `telegram_results.json` only after the last source;
   timeout drops all unimported work and leaves every cursor unchanged.
3. Current Gemma errors add 45-second waits, while the Lite fallback is RPD
   blocked. This amplifies the serial scan failure.
4. The producer's fallback key selection resolves both key lanes but passes
   only the primary key ID to the shared limiter. A separate fallback key
   setting therefore cannot relieve an exhausted primary project quota.

## Contributing Factors

- Scheduler/health report a running job as healthy even when its estimated
  completion is beyond the provider deadline.
- The fixed alphabetical order starves later sources after every replay.
- Alerts describe the terminal kernel but not the missing Telegram import.

## Automation Contract

### Treat as regression guard when

- Changing source cursor/date cutoff, Telegram producer model routing, Kaggle
  run timeout, recovery import, or scheduled source ordering/checkpointing.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py::scan_source`;
- `source_parsing/telegram/service.py` launch and recovery;
- `telegram_source`, `event_source`, `ops_run`, `kaggle_run_ledger`;
- Smart Update and public Telegram/VK fanout.

### Mandatory checks before closure or deploy

- Cutoff regression for a source with and without a stored cursor.
- Shared Google provider audit has `unapproved=0` and `allowlisted_debt=0`.
- A bounded production canary imports a fresh source candidate through the
  normal Smart Update boundary; inspect terminal errors and source cursor.
- Normal scheduled 57-source run completes in the budget and imports fresh
  Telegram candidates. Verify no stale announcement reaches public fanout.
- Exact Fly SHA is reachable from `origin/main` before steady-state closure.

### Required evidence

- Retained diagnostic artifact: `/home/dev/artifacts/events-bot-new/20261001T044609Z-tg-monitor-throughput-20261001/`.
- `ops_run`/Kaggle status, source cursors and imports; release SHA/image;
  public Telegram/VK readback for an accepted new event.

## Immediate Mitigation

- Cancelled only the active doomed Kaggle run after checking its progress;
  terminal status and exact S22 lease release were verified.
- Preserved pre-mitigation cursors and reset only `agropark39` for a bounded
  fresh-source canary. The other 56 source cursors were not changed.
- Replayed only `agropark39/2292` with 3.1 Flash-Lite to validate the available
  model lane before scheduling another full-source run.

## Corrective Actions

- Apply the configured date cutoff regardless of whether a source has a cursor,
  and log the exact cutoff decision without source content or credentials.
- Use the bounded fresh-source canary to validate the alternate model route
  before broadening production recovery.
- Do not route the monitoring producer to `gemini-2.5-flash` on key 3: live
  provider evidence returned 404. Check key-specific model availability and
  remaining RPD before any durable route change.
- Route each model to its selected key lane. Use 3.1 Flash-Lite as the
  monitored primary and 3.5 Flash-Lite on key 5 as the reserve, with a
  temporary one-day freshness window. Reassess the window after a complete
  healthy run.
- Expand both Lite model lanes to registered keys 2–6 under the shared limiter
  before a broad recovery. Until source-level checkpoints exist, run bounded
  source cohorts so quota exhaustion cannot discard all 57 sources' work.

## Follow-up Actions

- [ ] Introduce durable source-level checkpoints or bounded shards; the cutoff
  prevents historical replay but does not remove the all-or-nothing run design.
- [ ] Review older posts for still-future events before any targeted backfill;
  never publish old announcements just because a backlog exists.
- [ ] Add alerting on missing Telegram imports and stalled source progress.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending
