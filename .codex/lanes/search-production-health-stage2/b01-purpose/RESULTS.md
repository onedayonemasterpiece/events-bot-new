# B01 broker purpose/persona lane — results

## Revisions

- Base SHA: `df730a289c0b851bfa06d47641b4da4559f4bb34`
- Implementation SHA: `0b2a526ca`
- Branch: `agent/search-stage2-b01-purpose`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/search-stage2-b01-purpose`

## Delivered

- Added the exact closed broker request wire `purpose + platform + redirect_to`.
- Server-side purpose/platform mapping selects cached browser/Android/iOS for production health, cold browser for release qualification, and cached platform personas for manual legacy debug. Unsupported purpose/platform pairs fail closed.
- In-process broker identity now includes the derived persona, so one verified run can obtain separate fresh cached-health and cold-qualification credentials while same-purpose duplicates remain idempotent.
- Release qualification uses `SEARCH_E2E_PERSONA_EMAIL_COLD_BROWSER`; its full two-variant wrapper keeps one fixture/session. Legacy wrapper/device issuance declares `legacy_debug`.
- Migration remains callable by the deployed v1 RPC through the new closed `platform='browser'` default.
- Expired encrypted credential replay material is physically nulled by a named every-minute Supabase Cron job; migration fails closed if `pg_cron` cannot be installed/resolved.
- Corrected the stale `_coalesced_issue` comment to distinguish process replay from encrypted durable replay.

## Evidence and tests

- Red regression before implementation: focused broker tests rejected the new purpose field with `request_identity_spoofed` (2 failures).
- `python -m pytest -q tests/test_static_site_auth_session_broker.py tests/test_static_site_auth_session_broker_http.py` — `38 passed`.
- `python -m pytest -q tests/test_static_site_auth_session_broker.py tests/test_static_site_auth_session_broker_http.py tests/test_static_site_auth_session_broker_sql.py tests/test_supabase_security_hardening.py` — `47 passed`.
- Ephemeral PostgreSQL 17 regression executes v1 after the platform migration; then proves cached health=`new`, cold qualification=`new`, same cold duplicate=`replay`, two separate rows, scheduled ciphertext cleanup, and physical nulling.
- `node --test site/tests/auth-session-fixture.test.mjs site/tests/search-production-health-journey.test.mjs site/tests/search-e2e-workflow-contract.test.mjs` — `50 passed`.
- `npm run test:search-production-health` — `105 passed`.
- `npm run test:search-e2e-harness` — `30 passed`.
- `node --check` passed for both GitHub scripts, the session fixture, and the production-health runner.
- `python -m py_compile` passed for broker and SQL regression test.
- `git diff --check` passed.

Supabase Cron behavior was checked against the official current documentation: named jobs are upserted when scheduled again, and `cron.schedule(name, schedule, SQL)` is the supported SQL interface: https://supabase.com/docs/guides/cron and https://supabase.com/docs/guides/cron/quickstart

## Risks / exclusions

- No production/network/deploy run was performed.
- Production migration requires Supabase Cron/`pg_cron`; absence is intentionally a migration blocker rather than silently leaving credential ciphertext without bounded cleanup.
- B11 Appium raw protocol logging is a separate mobile security lane and was not mixed into B01. Recommended remediation was sent to the integrator.
- No session, credential, raw callback, email, or target artifact is serialized by this lane.

## Changed files

- `.github/scripts/issue-static-search-session.mjs`
- `.github/scripts/run-browser-static-search.mjs`
- `.github/workflows/search-release-qualification.yml`
- `.github/workflows/static-site-search-canary.yml`
- `serverless/static-site-auth-session-broker/README.md`
- `serverless/static-site-auth-session-broker/index.py`
- `site/e2e/auth-session-fixture/session-fixture.mjs`
- `site/e2e/search/production-health-run.mjs` (persona/purpose selection blocks only)
- `site/tests/auth-session-fixture.test.mjs`
- `site/tests/search-e2e-workflow-contract.test.mjs`
- `site/tests/search-production-health-journey.test.mjs`
- `supabase/migrations/20260809143602_static_site_auth_broker_platform_claims.sql`
- `tests/test_static_site_auth_session_broker.py`
- `tests/test_static_site_auth_session_broker_http.py`
- `tests/test_static_site_auth_session_broker_sql.py`
- `tests/test_supabase_security_hardening.py`
