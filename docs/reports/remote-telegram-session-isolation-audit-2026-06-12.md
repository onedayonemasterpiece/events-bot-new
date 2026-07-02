# Remote Telegram Session Isolation Audit — 2026-06-12

Status: passed
Related incident: `docs/reports/incidents/INC-2026-06-12-kenigsberg-story-session-duplication.md`
Audited SHA: `572a683a0f74a3c8594eef10e1d640f8f78a55c6`
Production image: `events-bot-new-wngqia:deployment-01KTYSMKNM59F9MWD6NYBQVQ2A`

## Objective

Confirm that remote Telethon sessions used from Kaggle are isolated and controlled so that the same Telegram auth key is not used concurrently from different IPs.

## Required Invariants

1. Every Kaggle job that opens a remote Telethon user session is represented in the shared guard.
2. A job must run preflight before Kaggle push with the same auth source that will be shipped in encrypted secrets.
3. Same auth source must fail closed when another non-terminal remote job owns it.
4. Different explicit auth sources may run in parallel.
5. Unknown auth source must be conservative and conflict with all scopes.
6. Jobs must write `remote_telegram_auth_scope` to `kaggle_registry`.
7. Terminal jobs must clear registry entries or be treated as terminal by status lookup.
8. Production story publishing must not borrow `TELEGRAM_AUTH_BUNDLE_S22`.
9. Production must not expose local/E2E auth as a remote fallback.

## Code Evidence

- Guarded remote job set: `guide_monitoring`, `kenigsberg_story`, `tg_monitoring`, `telegraph_cache_probe` in `remote_telegram_session.py`.
- Scope matching:
  - `remote_telegram_auth_scope_from_meta(...)` reads `remote_telegram_auth_scope`, `auth_scope`, `auth_source`, or `auth_bundle_env`.
  - `remote_telegram_auth_scopes_conflict(...)` blocks when scopes match or either side is unknown.
  - Different explicit scopes are skipped before Kaggle status lookup.
- Telegram Monitoring:
  - preflight passes `current_auth_scope=_resolve_auth_bundle_env_key() or "TG_SESSION"`;
  - registry meta writes `remote_telegram_auth_scope`;
  - existing cleanup removes `tg_monitoring`.
- Guide Monitoring:
  - preflight passes `current_auth_scope=remote_telegram_auth_scope()`;
  - registry meta writes `remote_telegram_auth_scope`;
  - existing cleanup removes `guide_monitoring`;
  - non-S22 guide override remains blocked unless explicitly allowed.
- Kenigsberg Stories:
  - preflight passes `current_auth_scope=story_remote_auth_scope()`;
  - registry meta writes `remote_telegram_auth_scope`;
  - poller done callback removes `kenigsberg_story`;
  - production `fly.toml` uses `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV=TELEGRAM_AUTH_BUNDLE_STORY`.
- Telegraph Cache Probe:
  - preflight passes `current_auth_scope=remote_telegram_auth_scope()`;
  - registry meta writes `remote_telegram_auth_scope`;
  - existing cleanup removes `telegraph_cache_probe`.

## Test Evidence

Run on 2026-06-12 in `/tmp/events-bot-kenigsberg-session-hotfix`:

```text
python -m py_compile remote_telegram_session.py handlers/kenigsberg_stories_cmd.py source_parsing/telegram/service.py guide_excursions/service.py guide_excursions/kaggle_service.py video_announce/story_publish.py video_announce/poller.py telegraph_cache_sanitizer.py tests/test_remote_telegram_session.py tests/test_video_announce_story_publish.py tests/test_video_announce_poller.py tests/test_telegraph_cache_session_guard.py tests/test_kenigsberg_stories.py
```

```text
pytest -q tests/test_remote_telegram_session.py tests/test_video_announce_story_publish.py tests/test_video_announce_poller.py tests/test_telegraph_cache_session_guard.py tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_h264_profile
29 passed in 1.90s
```

Coverage from these checks:

- `tests/test_remote_telegram_session.py` proves terminal states do not block, running same-scope jobs block, different explicit scopes do not block, fresh unknown status fails closed, stale transient unknown status can be ignored after configured age, and non-transient auth/status failures stay busy.
- `tests/test_video_announce_story_publish.py` proves story auth scope resolves to the selected bundle env name rather than the secret value.
- `tests/test_video_announce_poller.py` proves Kenigsberg story poller cleanup removes the remote registry job.
- `tests/test_telegraph_cache_session_guard.py` proves Telegraph cache preflight uses selected auth scope and successful runs register/cleanup that scope.

## Production Evidence

Checked after deploy on 2026-06-12:

- Fly app `events-bot-new-wngqia`, machine `48e42d5b714228`, version `1365`, checks `1 passing`.
- `/healthz`: `ok=true`, `ready=true`, `kenigsberg_story_daily=ok`, no issues.
- Runtime env:
  - `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV=TELEGRAM_AUTH_BUNDLE_STORY`
  - `TELEGRAM_AUTH_BUNDLE_S22` present
  - `TELEGRAM_AUTH_BUNDLE_STORY` present
  - `TELEGRAM_AUTH_BUNDLE_E2E` absent
  - `TG_MONITORING_AUTH_BUNDLE_ENV`, `GUIDE_MONITORING_AUTH_BUNDLE_ENV`, and `TELEGRAPH_CACHE_AUTH_BUNDLE_ENV` unset, so monitoring/cache default to S22.
- Runtime log mirror:
  - `ENABLE_RUNTIME_FILE_LOGGING=1`
  - `/data/runtime_logs` exists
  - active log exists
- `/data/kaggle_jobs.json`: `{"jobs": []}` at audit time, so no stale active owner needed manual removal.
- Runtime log mirror search for `AuthKeyDuplicated`, `authorization key`, `used under two different IP`, `remote_telegram_session_busy`, and `remote_telegram_auth_scope` returned no current matches after deploy.

## Residual Risk

- This audit proves code/config isolation and current production state. It does not prove Telegram will never invalidate an auth key for external use outside this bot/Kaggle system.
- Unknown-scope historical registry entries deliberately remain fail-closed. This can temporarily block a run, but it avoids burning a session.
- A final incident closure still needs one real `/kenigsberg` or scheduled story run after this deploy with no `AuthKeyDuplicatedError`.

## Verdict

The session-isolation controls are in place and verified for all known remote Telethon/Kaggle entry points. With current production configuration, S22 is used for remote monitoring/cache, story publishing uses the separate STORY bundle, E2E auth is absent from production, same-scope concurrency fails closed, and different explicit scopes can run in parallel. The risk of burning a session through duplicate in-bot/Kaggle use of the same auth key is minimized; within audited code paths, no unguarded duplicate remote session path remains.
