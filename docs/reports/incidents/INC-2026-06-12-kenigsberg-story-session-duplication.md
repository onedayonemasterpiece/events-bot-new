# INC-2026-06-12-kenigsberg-story-session-duplication

Status: open
Severity: sev2
Service: Kenigsberg Stories / Kaggle story publish / remote Telegram sessions
Opened: 2026-06-12
Closed: —
Owners: Codex / operator
Related incidents: `INC-2026-05-13-kenigsberg-production-story-boosts-required`, `INC-2026-06-07-guide-remote-session-stale-busy`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/cron.md`, `docs/operations/kaggle-secrets.md`, `docs/operations/runtime-logs.md`

## Summary

`/kenigsberg` Kaggle story preflight hit Telegram `AuthKeyDuplicatedError` because the same remote Telethon auth bundle could be used concurrently by long-running Kaggle operations while Kaggle GPU quota exhaustion extended render duration. The session key was invalidated and the same failure appeared for `@mostvkenig`, `@loving_guide39`, and `@jane_tour39`.

## User / Business Impact

- Kenigsberg story publication failed after render/preflight work.
- The shared Telegram session key was burned and must be rotated before the affected remote role can run again.
- Slow CPU Kaggle renders increased overlap windows and made repeated failures more likely.

## Detection

- Operator reported Kaggle output lines on 2026-06-12:
  `Story preflight failed ... authorization key ... used under two different IP addresses simultaneously`.
- Gap: Kenigsberg story runs were not fully isolated by auth role; the registry did not record an auth source scope for all remote Telegram jobs.

## Timeline

- 2026-06-12: operator reported repeated `/kenigsberg` Kaggle story preflight failures after GPU quota exhaustion made renders slower.
- 2026-06-12: disk was cleaned locally to allow an isolated hotfix worktree and tests.
- 2026-06-12: hotfix added `kenigsberg_story` registry participation, auth-source-scoped locking, and poller cleanup for Kenigsberg story jobs.

## Root Cause

1. Remote Telegram operations used role-scoped sessions by convention, but the shared guard did not carry an explicit auth scope.
2. Kenigsberg story jobs could overlap with monitoring jobs that used the same `TELEGRAM_AUTH_BUNDLE_S22`.
3. A long-running Kaggle render widened the time window where another job could start with the same Telethon session from a different IP.

## Contributing Factors

- Kaggle GPU quota exhaustion made renders slower than usual.
- `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` could still point at `TELEGRAM_AUTH_BUNDLE_S22`.
- Old registry metadata did not include `remote_telegram_auth_scope`, so the safest behavior is to treat unknown-scope active jobs as conflicting.

## Automation Contract

### Treat as regression guard when

- Changing `remote_telegram_session.py`.
- Changing Kenigsberg story Kaggle handoff, story publish config, or poller cleanup.
- Changing Telegram Monitoring / Guide Monitoring Kaggle auth bundle selection.
- Changing `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV`, `TG_MONITORING_AUTH_BUNDLE_ENV`, or `GUIDE_MONITORING_AUTH_BUNDLE_ENV`.

### Affected surfaces

- `remote_telegram_session.py`
- `handlers/kenigsberg_stories_cmd.py`
- `video_announce/story_publish.py`
- `video_announce/poller.py`
- `source_parsing/telegram/service.py`
- `guide_excursions/service.py`
- `guide_excursions/kaggle_service.py`
- Fly production env/secrets and Kaggle encrypted datasets.

### Mandatory checks before closure or deploy

- `python -m py_compile` for changed modules.
- `pytest -q tests/test_remote_telegram_session.py tests/test_video_announce_poller.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_native_profile`
- Verify `kaggle_registry` entries include `remote_telegram_auth_scope` for new remote Telegram jobs.
- Verify production story auth is not borrowing `TELEGRAM_AUTH_BUNDLE_E2E`.
- For true parallel monitoring/story operation, set story publishing to a distinct `TELEGRAM_AUTH_BUNDLE_STORY`.

### Required evidence

- deployed SHA:
- deploy path:
- regression checks:
- production env/secret evidence:
- post-deploy `/kenigsberg` or dry-run evidence:

## Immediate Mitigation

- Serialize all remote Telegram jobs that share the same auth scope.
- Treat unknown auth scope on active registry jobs as conflicting.
- Clean Kenigsberg story registry entries after the poller finishes so stale story runs do not block indefinitely.

## Corrective Actions

- Added auth-source-scoped remote Telegram session guard.
- Registered `kenigsberg_story` jobs with `remote_telegram_auth_scope`.
- Registered Telegram Monitoring and Guide Monitoring jobs with `remote_telegram_auth_scope`.
- Added docs recommending `TELEGRAM_AUTH_BUNDLE_STORY` for story publishing and reserving `TELEGRAM_AUTH_BUNDLE_S22` for remote monitoring.

## Follow-up Actions

- [ ] Operator: provide a fresh `TELEGRAM_AUTH_BUNDLE_S22` because the reported key was invalidated.
- [ ] Operator: provide a separate `TELEGRAM_AUTH_BUNDLE_STORY` if Kenigsberg/story publishing should run in parallel with S22 monitoring.
- [ ] Codex: after secrets are set, run a production `/kenigsberg` preflight/render smoke and update release evidence.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks: local targeted tests passed in hotfix worktree on 2026-06-12.
- post-deploy verification:

## Prevention

- Remote Telegram jobs now carry explicit auth-source metadata.
- The guard blocks same-scope overlap but allows future separate-scope parallelism.
- `.env.example` and operations docs document role-scoped session boundaries.
