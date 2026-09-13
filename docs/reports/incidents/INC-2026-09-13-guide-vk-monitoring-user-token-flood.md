# INC-2026-09-13 Guide VK monitoring uses flooded user token

Status: open
Severity: sev2
Service: Guide Excursions Monitoring / VK source ingestion on Kaggle
Opened: 2026-09-13
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-04-21-guide-gemma4-partial-monitoring`, `INC-2026-04-23-guide-digest-extraction-loss`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The primary Fly VK crawler already reads public VK walls with `VK_SERVICE_TOKEN`, but the separate Guide Excursions Kaggle monitor does not. Its encrypted Kaggle secret payload defaults `GUIDE_MONITORING_VK_TOKEN_ENV` to `VK_ACCESS_TOKEN5`, a user token. VK currently accepts that token for `account.getAppPermissions` but returns API error `9` (`Flood control`) for public read methods. As a result, every configured VK guide source returned zero scanned posts in recent full guide runs.

## User / Business Impact

- Six enabled VK guide sources were not ingested by full guide-monitoring runs from 2026-09-08 through 2026-09-12.
- The last run with any successful VK source was `ops_run.id=8343`, started 2026-09-07 11:21:30 UTC.
- Direct service-token comparison found 29 public wall items across the six sources published after that last successful run; these are potential missed inputs and require a compensating catch-up after remediation.
- Telegram guide sources and the separate 121-source Fly VK event crawler are different lanes and are not proven broken by this incident.

## Detection

- Operator suspected that VK monitoring was not consistently using the service token after direct token probes reproduced `Flood control` on user tokens.
- Production `ops_run.details_json` confirmed all six VK source reports had `source_status=error`, `posts_scanned=0` in full guide runs.
- Existing run-level `partial` status preserved the error but allowed the mixed Telegram/VK run to complete, so the complete VK-lane loss was not surfaced as a dedicated alert.

## Timeline

- 2026-09-07 11:21:30 UTC — last guide run with successful VK sources (`ops_run.id=8343`, 5 sources OK, 13 posts scanned).
- 2026-09-08 13:10:48 UTC — first inspected run with all six VK sources failing on VK error 9 (`ops_run.id=8398`).
- 2026-09-10 01:18:57 UTC — all six VK sources again failed with zero posts (`ops_run.id=8449`).
- 2026-09-11 18:10:00 UTC — all six failed (`ops_run.id=8555`).
- 2026-09-12 18:10:00 UTC — all six failed (`ops_run.id=8649`).
- 2026-09-13 UTC — direct production probes showed `VK_ACCESS_TOKEN5` fails `groups.getById` and `wall.get` with error 9 while `account.getAppPermissions` succeeds; production `VK_SERVICE_TOKEN` successfully executes the Guide monitor's group and personal-profile read method set.

## Root Cause

1. `guide_excursions/kaggle_service.py::_build_secrets_payload()` defaults the Guide VK credential source to `VK_ACCESS_TOKEN5`.
2. `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py::_vk_token()` keeps the same default, so Kaggle public reads use a publishing/user credential rather than the available service credential.
3. VK currently applies error 9 to public read methods invoked with that user token; the Guide monitor has no alternate service-token actor.

## Contributing Factors

- `_vk_api_call()` has no shared pacing or bounded retry policy for error 9, unlike the primary Fly crawler's `VK_MIN_INTERVAL_MS` throttle.
- Mixed-platform guide runs are marked `partial`; total loss of all VK sources does not have a dedicated lane-level failure/alert.
- The server-side `guide_source.last_scan_at` is advanced when error reports are imported, so a recent scan timestamp alone does not prove that any VK posts were fetched.
- Main event monitoring and Guide monitoring have separate token-routing implementations, making the service-token policy incomplete despite `VK_READ_VIA_SERVICE=true` on Fly.

## Automation Contract

### Treat as regression guard when

- Changing Guide monitoring VK credentials, Kaggle secret payloads, VK API pacing/retry, VK source status aggregation, or service-token routing.
- Changing the primary `main.py::vk_api` service-read allowlist or VK source crawler.

### Affected surfaces

- `guide_excursions/kaggle_service.py::_build_secrets_payload`
- `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py::_vk_token` and `_vk_api_call`
- `docs/features/guide-excursions-monitoring/README.md`
- Fly secrets/env: `VK_SERVICE_TOKEN`, `GUIDE_MONITORING_VK_TOKEN`, `GUIDE_MONITORING_VK_TOKEN_ENV`
- Kaggle GuideExcursionsMonitor runtime and result bundle
- `ops_run(kind='guide_monitoring')`, `guide_source`, `guide_monitor_post`

### Mandatory checks before closure or deploy

- Unit/contract test proves the encrypted Guide payload prefers `VK_SERVICE_TOKEN` (and documents any explicit fallback policy) rather than silently defaulting to `VK_ACCESS_TOKEN5`.
- Direct read-only smoke with the chosen service token passes `utils.resolveScreenName`, `groups.getById`, `users.get`, and `wall.get` for both a community and a personal profile.
- Kaggle production-equivalent Guide run reports all six VK sources non-error and scans posts without exposing credentials.
- Error-9 handling is paced/bounded and covered by a deterministic test; it must not create a retry storm.
- Production `ops_run.details_json` contains six VK source reports with no error-9 failures.
- Compare VK source cursors and imported `guide_monitor_post` rows before/after catch-up.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Redacted Fly env-presence/readback showing `VK_SERVICE_TOKEN` is present and Guide credential routing selects it.
- Kaggle result/run ID and production `ops_run` ID for the successful catch-up.
- Pre/post evidence for all six VK sources and the missed-window posts.
- Local investigation artifacts: `artifacts/codex/vk-service-token-analysis-2026-09-13/` (not committed).

## Immediate Mitigation

- None applied during analysis. No secret, code, deployment, scheduler, or production data was changed.

## Corrective Actions

- Pending. The likely minimal path is to route Guide public reads to `VK_SERVICE_TOKEN`; configuration-only routing is already technically possible through `GUIDE_MONITORING_VK_TOKEN_ENV`, but durable defaults, local `VK_SERVICE_KEY` compatibility, pacing, tests, and documentation must be decided together.

## Follow-up Actions

- [ ] Route Guide VK public reads through the service credential without exposing it in config artifacts.
- [ ] Add bounded pacing/retry for VK error 9 in the Kaggle Guide client.
- [ ] Add a lane-level diagnostic when every enabled VK source scans zero posts due to provider errors.
- [ ] Run a compensating full catch-up covering at least 2026-09-07 onward and verify imported posts/occurrences.
- [ ] Separately investigate the primary crawler's stale `radostidetam` cursor; the service token currently reads that wall successfully, so it is not explained by this token-routing incident.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: analysis-only probes completed; closure checks pending
- post-deploy verification: —

## Prevention

Pending corrective implementation and regression coverage.
