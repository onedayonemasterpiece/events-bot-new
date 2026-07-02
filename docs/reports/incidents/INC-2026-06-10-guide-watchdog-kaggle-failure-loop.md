# INC-2026-06-10 Guide Watchdog Kaggle Failure Retry Loop

Status: mitigation-in-progress
Severity: sev2
Service: Guide Excursions Monitoring / critical scheduler watchdog / Kaggle
Opened: 2026-06-10
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag`, `INC-2026-06-07-guide-remote-session-stale-busy`
Related docs: `docs/operations/cron.md`, `docs/features/guide-excursions-monitoring/README.md`

## Summary

On 2026-06-10 the guide excursions critical watchdog repeatedly retried the
same missed `guide_excursions_full` slot after the Kaggle kernel reached
terminal `ERROR`. This created repeated admin notifications and repeated Kaggle
kernel pushes within the same hour.

The root failure observed in the Kaggle output was Telethon startup failing with
`AuthKeyDuplicatedError`.

## Evidence

- Production `ops_run` rows showed repeated scheduled full guide attempts with
  status `error` and `Guide Kaggle kernel failed (failed)`.
- Kaggle output showed:
  `AuthKeyDuplicatedError: The authorization key (session file) was used under two different IP addresses simultaneously, and can no longer be used.`
- On 2026-06-10 production `TELEGRAM_AUTH_BUNDLE_S22` was rotated to the
  separate `/home/dev/projects/kdg80/.env` `TELEGRAM_AUTH_BUNDLE_S22`.
- A smoke run after that rotation still failed with the same
  `AuthKeyDuplicatedError`.
- Runtime file logging was enabled during the incident window
  (`ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`), and
  `/data/runtime_logs/events-bot.log` showed the critical watchdog repeatedly
  dispatching the same missed `guide_excursions_full` slot after each terminal
  Kaggle `ERROR`.
- Runtime log timeline evidence:
  - `2026-06-10 18:42:34Z`: first observed critical watchdog catch-up for the
    missed `20:10 Europe/Kaliningrad` full slot.
  - Kernel pushes for `zigomaro/guide-excursions-monitor` repeated from version
    `291` through `317`.
  - Restarts at `20:35`, `20:50`, `20:51`, and `20:56` marked running
    `ops_run` rows as `crashed`; startup catch-up then immediately dispatched
    another full catch-up while the same daily slot was still unsatisfied.
- After the replacement S22 was installed, a direct Telethon smoke on production
  returned `s22_smoke=ok user_id=8336351413 username=The_day_of_kk`.
- The first production guide run with the replacement S22 reached terminal
  `partial`, not auth failure: `ops_run id=2196`, `run_id=b3b946fc3578`,
  `transport=kaggle`, `finished_at=2026-06-10 21:13:34`, warning
  `kaggle result marked as partial; llm_deferred=3; llm_error=23`.
- Runtime log showed scheduled guide digest/VK continuation after that run:
  `guide_digest_vk_carousel issue_id=100 slides=2 afishas=0`.

## Root Cause

The immediate failure was a broken Telegram StringSession
(`AuthKeyDuplicatedError`), but the retry storm was caused by watchdog/session
boundary gaps:

1. `maybe_dispatch_critical_scheduler_watchdog()` considered the daily full slot
   unsatisfied after every `error` or `crashed` `ops_run`, so terminal Kaggle
   failures reopened the catch-up path immediately.
2. The in-memory `_critical_catchup_inflight` guard does not survive Fly
   restarts. During secret rotations/deploys, startup cleanup marked active
   guide runs as `crashed`, and startup catch-up started a new full run.
3. `run_guide_monitor_kaggle()` registers the remote job after kernel
   push/shape validation, leaving an early prepare/push window where a restart
   can lose ownership evidence before the registry is populated.
4. The main runner does not remove the guide registry job on terminal Kaggle
   error; cleanup is left to recovery. This made the registry noisy, while the
   guard correctly ignored already-terminal `ERROR` kernels.

## Current Decision

The broad watchdog throttling change was reverted after operator feedback. The
accepted remediation is narrower: the critical scheduler watchdog now reads the
persisted `ops_run` state and defers only the same daily
`guide_excursions_full` catch-up after a recent `error`, `crashed`, or
remote-session-busy full attempt. This prevents immediate repeated Kaggle
launches across restarts without changing normal next-day scheduled monitoring.

Scheduled guide digest publishing also clears a processed guide recovery job
after a successful publish or no-items outcome. Without that cleanup, a later
Fly restart can see a completed kernel with `results_path` still in
`/data/kaggle_jobs.json` and replay the same import/publish through recovery.

The previously deployed code changes were:

- `b987769393a8693f120fe4b2a5bfac32e3a53e88` — added watchdog retry cooldown;
- `9e00fc17f269f27cc11c3d9c1d2e84ac84d5d5a2` — changed Kaggle auth secret
  rotation behavior.

`9e00fc17f269f27cc11c3d9c1d2e84ac84d5d5a2` was reverted by
`087588322159d7642085c2749351b50af21795b5`.

`b987769393a8693f120fe4b2a5bfac32e3a53e88` was reverted so the earlier broad
retry cooldown did not remain. The replacement fix is covered by
`tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_after_crashed_guide_full_run`.

## Remaining Blocker

Guide monitoring still requires the current replacement
`TELEGRAM_AUTH_BUNDLE_S22` to complete a fully clean guide Kaggle run. The
session itself passed direct Telethon `get_me` and the first real Kaggle run
reached terminal `partial`, so the auth-dedup blocker is cleared; closure still
requires no rapid retry loop after deploy and a clean next scheduled slot or
accepted handling of the remaining LLM partial warnings.

## Regression Contract

### Treat as regression guard when

- Touching `scheduling.py::maybe_dispatch_critical_scheduler_watchdog`.
- Touching `ops_run` startup cleanup for `guide_monitoring`.
- Touching guide Kaggle registry ownership or remote Telegram session guard.
- Rotating `TELEGRAM_AUTH_BUNDLE_S22`.

### Mandatory checks before closure or deploy

- `python3 -m py_compile scheduling.py tests/test_scheduling.py`
- `pytest -q tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_after_crashed_guide_full_run`
- Existing remote-busy watchdog regression:
  `pytest -q tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_guide_after_remote_busy_skip`
- Scheduled guide digest recovery cleanup:
  `pytest -q tests/test_scheduling_guide_digest.py::test_scheduled_guide_digest_publishes_after_nonfatal_partial_warning`
- Production `/healthz` must show `guide_excursions_light` and
  `guide_excursions_full` enabled after any secret rotation unless the operator
  explicitly requested disabling them.
- If a guide Kaggle kernel is currently `RUNNING`, do not run local/prod
  Telethon smoke on the same S22 session.

### Required evidence

- Runtime log or `ops_run` evidence showing no rapid repeated full catch-up
  launches after a terminal failed/crashed full attempt.
- Replacement S22 direct `get_me` smoke evidence after secret rotation.
- Terminal guide monitor run evidence: success/partial import, or a new
  non-auth failure with registry/session boundary preserved.
