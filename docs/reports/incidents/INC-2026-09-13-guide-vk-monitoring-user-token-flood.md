# INC-2026-09-13 Guide VK monitoring uses flooded user token

Status: monitoring
Severity: sev2
Service: Guide Excursions Monitoring / VK source ingestion on Kaggle
Opened: 2026-09-13
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-04-21-guide-gemma4-partial-monitoring`, `INC-2026-04-23-guide-digest-extraction-loss`, `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm`, `INC-2026-07-03-current-import-vector-vk-publication`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The primary Fly VK crawler already reads public VK walls with `VK_SERVICE_TOKEN`, but the separate Guide Excursions Kaggle monitor does not. Its encrypted Kaggle secret payload defaults `GUIDE_MONITORING_VK_TOKEN_ENV` to `VK_ACCESS_TOKEN5`, a user token. VK currently accepts that token for `account.getAppPermissions` but returns API error `9` (`Flood control`) for public read methods. As a result, every configured VK guide source returned zero scanned posts in recent full guide runs.

## User / Business Impact

- Six enabled VK guide sources were not ingested by full guide-monitoring runs from 2026-09-08 through 2026-09-12.
- The last run with any successful VK source was `ops_run.id=8343`, started 2026-09-07 11:21:30 UTC.
- Direct service-token comparison found 29 public wall items across the six sources published after that last successful run. The operator explicitly excluded historical catch-up from this remediation; only new scheduled scans are in scope.
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

## Related Smart Update / Core Event Ingestion Analysis

The Smart Update module itself does not issue VK HTTP requests. Its VK source evidence enters through upstream boundaries:

1. `vk_intake.py` discovers posts with `main.vk_wall_since()` -> `main.vk_api("wall.get")`; production routes this to the service token.
2. `vk_auto_queue.fetch_vk_post_text_and_photos()` re-reads the exact post with `main.vk_api("wall.getById")`; production routes this to the service token.
3. Video evidence refresh deliberately passes `_force_user_actor=True` to `video.get`. In the inspected runtime window this path completed 249 times with no refresh failures, so it is not currently the lost-post boundary and must not be switched blindly without checking returned file metadata.
4. Smart Update then consumes the captured source envelope and performs no further VK read. Downstream `vk_sync` publication is separate from canonical event persistence.

Production evidence shows the service-read boundary is healthy, but core event filling is still unstable for other reasons:

- recent 48-hour evidence contains 100 inserted VK `event_source` rows and 59 events with VK source URLs, proving the core lane is not in a total read outage;
- recent `vk_auto_import` runs repeatedly fail before processing with `OperationalError: database is locked`;
- the available runtime logs contain 393 `database is locked` lines, including repeated failures in the VK crawl continuation claim and Telegram on-demand dispatcher;
- current storage contains 3,668 pending VK source packets, including 823 marked `ORPHANED_LEASE`, plus 1,530 `failed_technical`;
- recent Smart Update terminal failures are dominated by identity/grounding decisions (`distinct_not_grounded`, blocking merge conflicts, location/region review failures), not VK API read errors;
- 180 `vk_sync` jobs are in error and many retry indefinitely. Current samples include `Flood control` on `wall.edit`/`wall.post` and `vk_sync_missing_media_for_telegram_event`. This is downstream publication debt, but the retry storm consumes API/DB capacity and can destabilize ingestion; causality for the SQLite lock holder still requires transaction-boundary instrumentation.

A separate source-level anomaly remains: 120 of 121 primary VK crawl cursors were checked within 12 hours, while `radostidetam` has not advanced since 2026-08-30. A direct service-token `wall.get` succeeds, and the crawler suppresses the per-source exception traceback in its broad `except`, so the cause is not observable from current logs.

The production health check currently reports `guide_excursions_full=ok` despite full Guide runs importing zero VK posts from six provider-error sources. Scheduler liveness is therefore not sufficient evidence of ingestion health.

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
- Verify the next scheduled run from its normal lookback without expanding it into a historical catch-up.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Redacted Fly env-presence/readback showing `VK_SERVICE_TOKEN` is present and Guide credential routing selects it.
- Kaggle result/run ID and production `ops_run` ID for a successful normal scheduled scan.
- Post-deploy evidence for all six VK sources; historical missed-window replay is explicitly out of scope.
- Local investigation artifacts: `artifacts/codex/vk-service-token-analysis-2026-09-13/` (not committed).

## Immediate Mitigation

- Deployed runtime SHA `789bb37bfa69126892b0946986c687692edd358e`, reachable from `origin/main`: Guide public reads default to `VK_SERVICE_TOKEN`; error 9 opens a per-credential circuit and no longer receives five in-call retries; JobOutbox flood retries use the provider cooldown plus deterministic jitter.
- First production deployment showed the intended provider containment: after boot, one user-token `wall.get` received error 9 at 08:01:19 UTC and opened the circuit; the next 118 observed transport checks were rejected locally, with no additional error-9 provider response. Follow-up also suppresses those per-call INFO lines, propagates flood state through photo upload, and stops fivefold retries for explicit group-auth rejection.
- After the follow-up deployment, one `photos.getWallUploadServer` provider request received error 9 at 08:13:43 UTC. Later `vk_sync` jobs were deferred locally to staggered times after the one-hour circuit deadline; no `failed after 5 attempts` sequence appeared after that boot.
- On 2026-09-19 the 155 accumulated managed-event `vk_sync` flood rows were
  atomically contained as terminal `done` with marker
  `contained_no_replay_pre_token_rotation_20260919`; a full row backup and
  checksum were retained before mutation. This prevents the old announcement
  cohort from bursting after credential recovery.
- Production `VK_USER_TOKEN` was rotated to a newly supplied user actor that
  passed permission and read-only upload-server probes. Fly machine version
  2071 became healthy; post-restart DB verification showed 155 contained rows,
  zero flood errors, and no pending/running VK jobs. The first genuinely fresh
  Smart Update canary is still pending.

## Corrective Actions

- Route Guide public reads to `VK_SERVICE_TOKEN` by default.
- Route main-app public `users.get` through the service-read allowlist.
- Treat VK error 9 as actor-level backpressure: one provider attempt, per-credential circuit, and staggered durable retry.

## Follow-up Actions

- [x] Route Guide VK public reads through the service credential without exposing it in config artifacts.
- [x] Stop repeated provider attempts after VK error 9 and defer publication jobs behind an actor-level circuit.
- [ ] Add a lane-level diagnostic when every enabled VK source scans zero posts due to provider errors.
- [ ] Verify the next normal scheduled Guide scan; do not run a historical catch-up per operator direction.
- [ ] Separately investigate the primary crawler's stale `radostidetam` cursor; the service token currently reads that wall successfully, so it is not explained by this token-routing incident.
- [ ] Verify the first genuinely fresh managed `klgdevents` publication after
  token rotation by provider readback and the managed-publication ledger; do
  not rearm the contained cohort.

## Release And Closure Evidence

- deployed runtime SHA: `789bb37bfa69126892b0946986c687692edd358e` (merged to `origin/main` in PR #648)
- deploy path: `scripts/deploy_fly_main.sh --remote-only`; Fly image `deployment-01M2CX1ZTV30BR9GY5D150YMWZ`, machine `48e419df93e078`
- regression checks: targeted local suite `98 passed`; PR #647 and #648 CI passed; direct service-token read smoke passed for a group and a personal profile
- post-deploy verification: `/healthz` reports `ok=true`, `ready=true`, DB `ok`; one provider error 9 opened the circuit, subsequent publication jobs were deferred locally, and no five-attempt VK call sequence or `database is locked` line was observed after the final boot in the checked window
- 2026-09-19 runtime-only recovery: Fly machine version 2071, active
  `VK_USER_TOKEN` fingerprint matched the working local candidate, `/healthz`
  remained ready, `PRAGMA quick_check=ok`, and the contained-row backup checksum
  is `0984435a6308dd525b9af99678e68998610d2a106abf85551e2a93c82c11888f`.
- closure pending: the next normal Guide scheduled scan must produce six non-error VK source reports; historical catch-up remains explicitly excluded

## Prevention

Regression coverage now enforces service-token routing for Guide public reads, one-attempt handling for error 9 and explicit actor/auth rejection, actor-circuit deferral, and typed flood propagation through photo upload. The incident remains in monitoring until the next normal Guide scan validates all six production VK sources.
