# INC-2026-08-01-telegram-monitor-google-ai-package-closure Daily Telegram Monitoring failed before source scan

Status: open
Severity: sev1
Service: Telegram Monitoring / Kaggle
Opened: 2026-08-01
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-08-01-guide-google-ai-package-closure`, `INC-2026-07-31-google-ai-parallel-limiter-bypass`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Daily Telegram Monitoring and every automatic catch-up since the Google AI limiter cutover failed before scanning a source. The generated Kaggle notebook embedded a stale four-file `google_ai` allowlist, while the current package imports `google_ai.limiter_supabase` and `google_ai.interactions`.

## User / Business Impact

- No Telegram sources or messages were scanned by the scheduled run on 2026-07-31, its three 2026-08-01 catch-ups, or the 2026-08-01 scheduled run.
- Event discovery from monitored Telegram sources therefore has a two-day freshness gap.
- Last successful full scheduled run was `ops_run.id=4900`: 57 sources, 127 messages and 42 imported events.

## Detection

- The owner reported the missing daily monitoring and supplied Kaggle Version 567 logs.
- Production `ops_run` did record all five failed attempts, but `errors_count=0` and an empty error list hid the launcher exception; watchdog retries could not repair a deterministic bundle defect.
- `kaggle_run_ledger` and the role-scoped resource lease correctly recorded the terminal failure and released `telegram_session:s22`.

## Timeline

- 2026-07-31 17:19 UTC — commit `022b300e` made `google_ai.client` import the Supabase limiter module; Telegram notebook packaging still used its old allowlist.
- 2026-07-31 21:40 UTC — scheduled `ops_run.id=4996` failed with zero sources/messages.
- 2026-08-01 03:46, 09:55 and 15:58 UTC — watchdog catch-ups `5026`, `5041`, `5063` failed identically.
- 2026-08-01 21:40 UTC — scheduled `ops_run.id=5074` launched Kaggle run `tg_monitor:294641dd83144329af3c0de7a5fe67ee`.
- 2026-08-01 21:41 UTC — Kaggle Version 567 raised `ModuleNotFoundError: No module named 'google_ai.limiter_supabase'`; the S22 lease was released at 21:41:37 UTC.
- 2026-08-01 — owner reported the incident; code/runtime investigation identified the incomplete embedded package as root cause.

## Root Cause

1. `_embedded_google_ai_sources()` explicitly listed only `__init__.py`, `client.py`, `exceptions.py` and `secrets.py`.
2. The generated notebook prepended that incomplete embedded package to `sys.path`, shadowing the complete auxiliary package.
3. There was no isolated generated-notebook import-closure test for Telegram Monitoring, although the Guide pipeline had already added one for the same package boundary.

## Contributing Factors

- The hand-maintained allowlist made adding any internal package import a remote-only compatibility risk.
- Telegram Monitoring did not inherit the earlier Guide package-closure hardening.
- The outer runner re-raised launcher errors without copying them into the report used to finish `ops_run`.

## Automation Contract

### Treat as regression guard when

- Telegram notebook generation or staging changes;
- files/imports change under `google_ai/`;
- Telegram Monitoring scheduler, recovery, Kaggle callbacks or S22 lease handling changes.

### Affected surfaces

- `source_parsing/telegram/service.py` notebook embed/stage and outer run telemetry;
- `google_ai/` internal module graph;
- generated Kaggle notebook and `zigomaro/telegram-monitor-bot`;
- `kaggle_run_ledger`, `ops_run`, watchdog/recovery and `telegram_session:s22`.

### Mandatory checks before closure or deploy

- deterministic embedded source tree equals every current `google_ai/**/*.py` file;
- isolated generated Telegram notebook imports the public package API and Supabase limiter;
- focused Telegram service tests and compile pass;
- fix is reachable from `origin/main`, deployed from a clean reproducible SHA, and `/healthz` is ready;
- one real full catch-up uses only `TELEGRAM_AUTH_BUNDLE_S22`, reaches terminal `done`, releases the S22 lease, and reports non-zero scanned sources/messages;
- production `ops_run` and imported data prove the missed daily window has been recovered.

### Required evidence

- deployed SHA and main ancestry;
- focused test/CI results;
- Kaggle callback ledger (`kernel_started`, resource acquire/alive/report-written/release, terminal state);
- successful catch-up `ops_run` metrics and post-deploy health/runtime-log evidence.

## Immediate Mitigation

- Replace the allowlist with the complete sorted Python source tree and support nested module paths in both staging and generated notebook materialization.
- Add the missing isolated-notebook regression test and persist outer launcher exceptions in `ops_run` diagnostics.
- Deploy the hotfix and run one compensating full scan with the released S22 role session.

## Corrective Actions

- [x] Implement complete deterministic `google_ai` embedding for Telegram Monitoring.
- [x] Add source-tree and isolated generated-notebook tests.
- [x] Preserve pre-report runner exceptions in operational diagnostics.
- [ ] Merge to `origin/main` and deploy the exact clean SHA.
- [ ] Complete and verify the full S22 catch-up.

## Follow-up Actions

- [ ] Keep package-closure tests mandatory for every generated Kaggle notebook that embeds shared Python packages.
- [ ] Close this record only after the production catch-up restores current data.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Package membership is now discovered from the source tree rather than duplicated in service-specific allowlists, while an isolated executable notebook test validates the actual import precedence used on Kaggle.
