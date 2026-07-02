# INC-2026-06-07 Guide Remote Session Stale Busy

Status: open
Severity: sev1
Service: guide excursions scheduled monitoring / shared remote Telegram session guard
Opened: 2026-06-07
Closed: —
Owners: bot operations / guide excursions
Related incidents: `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag`, `INC-2026-04-23-guide-digest-extraction-loss`, `INC-2026-04-21-guide-gemma4-partial-monitoring`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`

## Summary

7 июня 2026 evening guide full monitoring skipped immediately with `remote_telegram_session_busy` even though the operator observed no active Kaggle run. The blocking registry entry was `guide_monitoring run_id=49934d037821 kernel=zigomaro/guide-excursions-monitor`, and the only evidence keeping it busy was Kaggle `GetKernelSessionStatus` returning HTTP 500. This broke the daily scheduled guide monitoring/digest path again.

## User / Business Impact

- The scheduled evening guide full scan did not start.
- The guide digest for the day could miss fresh excursion occurrences unless a compensating run/import is completed.
- Operator chat received a zero-result `/guide_report 2034` with `remote_telegram_session_busy`, which looked like a real active remote Telegram session while Kaggle showed no active run.

## Detection

- Detected by operator report from Telegram admin messages at 2026-06-07 20:09-20:10 local time.
- The skipped run materialized as `ops_run_id=2034`, `run_id=103a1d5f3064`, with `remote_telegram_session_busy`.
- Runtime file mirror check found production file logging enabled for this incident window (`ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, active `/data/runtime_logs/events-bot.log` plus rotated hourly files), so closure evidence should use file logs before fallback sources.

## Timeline

- 2026-06-07 20:09 local: operator starts full guide monitoring, `run_id=103a1d5f3064`.
- 2026-06-07 20:10 local: run skips as `remote_telegram_session_busy`, reporting stale-looking `run_id=49934d037821` and Kaggle status lookup HTTP 500.
- 2026-06-07T18:10:05Z: runtime file log records `guide_monitor.remote_telegram_session_busy run_id=103a1d5f3064 conflicts=['zigomaro/guide-excursions-monitor']`.
- 2026-06-07T18:20Z triage: production `/data/kaggle_jobs.json` contains one `guide_monitoring` entry for `run_id=49934d037821`, `mode=light`, `created_at=2026-06-07T11:21:16.762383+00:00`; at the evening full slot this entry was about 409 minutes old.
- 2026-06-07T18:20Z triage: production `ops_run #2025` shows the same light run started at `11:20Z`, finished at `13:21:48Z` with `status=error`, `duration_sec=7308`, and `Guide Kaggle kernel failed (timeout)`.
- 2026-06-07T18:20Z triage: production `ops_run #2034` shows the full slot started at `18:10Z`, finished at `18:10:06Z` with `status=skipped`, `errors=1`, and the stale conflict above.
- 2026-06-07: incident triage identifies that `remote_telegram_session.py` treats any status lookup exception as an indefinite live lock, with no age cutoff for transient Kaggle API failure.

## Root Cause

1. The shared remote Telegram session guard correctly failed closed for fresh `UNKNOWN` Kaggle status, but had no bounded stale cutoff for old registry entries whose status lookup kept failing transiently. In production, a timed-out light run registry entry was still blocking a full run about 409 minutes after creation.
2. The previous incident's manual recovery lesson made operators/agents avoid clearing `UNKNOWN` registry entries, but the code did not provide an automated safe escape hatch once the entry was older than the maximum expected monitoring window.
3. Kaggle `GetKernelSessionStatus` HTTP 500 is an external dependency failure; without age-aware handling, it could keep yesterday's registry entry blocking today's daily full slot forever.

## Contributing Factors

- Runtime file mirror was enabled during this incident window, but the project default has historically varied during incidents; agents must verify the actual production env and files before falling back to Fly logs, DB rows, registry file, and Kaggle artifacts.
- The guide full slot is a daily critical scheduled task, so one false busy skip can directly suppress the digest pipeline.
- `remote_telegram_session_busy` messaging did not distinguish fresh `UNKNOWN` from stale transient lookup failure.

## Automation Contract

### Treat as regression guard when

- Touching `remote_telegram_session.py`, `kaggle_registry.py`, guide monitoring scheduler/watchdog, Telegram Monitoring remote-session guard, or any remote Telegram/Kaggle recovery logic.
- Handling `GetKernelSessionStatus` HTTP 5xx/timeout/SSL/network errors.
- Investigating missed guide full slots, guide digest gaps, or `remote_telegram_session_busy` skips.

### Affected surfaces

- `remote_telegram_session.py`
- `kaggle_registry` / `/data/kaggle_jobs.json`
- `guide_excursions/service.py::run_guide_monitor`
- `source_parsing/telegram/service.py` shared guard call
- `TELEGRAM_AUTH_BUNDLE_S22` session boundary
- scheduled guide full run and digest catch-up

### Mandatory checks before closure or deploy

- `pytest -q tests/test_remote_telegram_session.py`
- Guide scheduler regression checks from `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag.md` that cover remote-busy deferral and recovery-import slot satisfaction.
- `python -m py_compile remote_telegram_session.py guide_excursions/service.py source_parsing/telegram/service.py scheduling.py`
- Verify fresh `UNKNOWN` / status lookup 5xx remains blocking before cutoff.
- Verify stale transient status lookup failure after `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` no longer blocks and marks job meta with `stale_transient_status_lookup_failure`.
- Post-deploy `/healthz` must remain ok/ready and show guide scheduler slots.
- Because this hit the daily guide full slot, closure requires same-day compensation: fixed production guide full catch-up/import and digest publication, or explicit evidence that no missed still-future digest-ready occurrences remain.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Manual Fly deploy evidence from clean worktree.
- Production evidence for runtime file mirror state or fallback logs.
- Production evidence for `/data/kaggle_jobs.json` / relevant `ops_run` rows before and after fix.
- Same-day guide full catch-up/import/digest evidence.

## Immediate Mitigation

- Code hotfix prepared to keep fresh `UNKNOWN` fail-closed while ignoring only stale transient status lookup failures after a bounded age cutoff.

## Corrective Actions

- Add `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` (default `390`, minimum `60`) to the shared remote Telegram session guard. The default is intentionally above the documented 360-minute dynamic guide/TG monitoring cap, but below the observed 409-minute stale lock that blocked the evening full slot.
- Treat HTTP 5xx, timeout, SSL, and connection errors from Kaggle status lookup as transient.
- If a transient lookup failure belongs to a registry entry older than the cutoff, skip it as a conflict and mark job meta with `remote_session_guard_ignore_reason=stale_transient_status_lookup_failure`.
- Preserve fail-closed behavior for fresh `UNKNOWN` and non-transient lookup failures such as auth errors.
- Update guide and Telegram Monitoring runbooks to distinguish fresh unknown locks from stale transient status lookup failures.

## Follow-up Actions

- [ ] Add operator-facing wording that differentiates fresh remote-session lock from stale ignored registry evidence.
- [ ] Consider an admin command that shows registry age/status/meta for `remote_telegram_session_busy` conflicts without allowing unsafe manual deletion.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- pre-fix production evidence:
  - runtime file mirror: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, active `/data/runtime_logs/events-bot.log` and rotated files present.
  - registry: `/data/kaggle_jobs.json` had `guide_monitoring:zigomaro/guide-excursions-monitor`, `run_id=49934d037821`, `mode=light`, `created_at=2026-06-07T11:21:16.762383+00:00`.
  - `ops_run #2025`: `mode=light`, `status=error`, `duration_sec=7308`, `Guide Kaggle kernel failed (timeout)`.
  - `ops_run #2034`: `mode=full`, `status=skipped`, `duration_sec=6`, conflict status `UNKNOWN`, Kaggle `GetKernelSessionStatus` HTTP 500.
- post-deploy verification:
- same-day compensation:

## Prevention

- Regression tests now pin fresh-unknown blocking, stale-transient unblocking, and stale-non-transient blocking.
- The incident contract requires same-day guide full compensation for future false-busy daily slot failures.
