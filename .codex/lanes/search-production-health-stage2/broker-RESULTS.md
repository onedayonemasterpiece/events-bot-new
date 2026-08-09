# Stage 2 broker lane results

## Lane contract

- Lane ID: `search-production-health-stage2/broker`
- Requirement IDs: `R05`
- Status: Done
- Branch: `agent/search-stage2-broker`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/search-stage2-broker`
- Base SHA: `dd1e0ad9072acdad46f01459ba4ab0ff171e0318`
- Implementation head SHA: `644fb5c7af4a1e616fd6edfdd9dccb77c6a494a7`
- Final branch head: the receipt-only commit immediately after the implementation head.
- Supabase apply/deploy, Fly deploy, live broker call, and live session issue: not performed.

## Outcome

- Broker request identity is now `{platform, redirect_to}` only. `platform` is closed to `browser|android|ios`; repository, workflow ref, run ID, and run attempt come exclusively from verified GitHub OIDC claims. Caller-supplied identity mirrors fail closed.
- Platform maps server-side to the existing dedicated personas `search-cached-browser`, `search-cached-android`, and `search-cached-ios`. Startup validation requires all three configured accounts and unique normalized emails.
- New RPC `claim_static_site_auth_session_issue_v2` returns typed `new`, `duplicate_inflight`, or `persona_busy`. Only `new` reaches Supabase Auth Admin `generate_link`.
- Duplicate, persona-busy, and broker-overload responses are typed `product_health=UNKNOWN`, `execution_status=BLOCKED`, and `failure_class=UNKNOWN`; none is classified as a product failure.
- HTTP capacity is exactly three simultaneous requests, supporting one Browser, Android, and iOS issuance without the previous two-slot collision. A fourth request is rejected immediately and typed UNKNOWN.
- Identical concurrent calls on one JavaScript issuer coalesce to one broker POST. Identical concurrent calls inside one broker process coalesce to one ledger claim and one `generate_link`. In-flight results are deleted immediately on completion and are never persisted.
- The public JavaScript API remains source-compatible: `issuer.issue({ personaId?, platform, redirectTo, runId?, personaEmail?, scopeKind?, scopeId? })`. Platform is required; optional `personaId` is only a local compatibility assertion and is never sent. Only `{platform, redirect_to}` crosses HTTP.
- The job helper exports only a masked, one-time callback through the current job's `GITHUB_ENV`; OTP is not exported, and credential fields are cleared. No credential/session cache, escrow, serialized cross-job session, or artifact was added.
- Existing cleanup, no-mail counters, HTTPS/no-CORS boundary, no-store headers, keyed audit redaction, and protected owner probe behavior remain covered.

## Supabase CLI and documentation evidence

- Read the project Supabase skill security checklist before changes.
- Scanned `https://supabase.com/changelog.md` on 2026-08-09. No current hosted Supabase Auth Admin `generate_link` breaking change applied; the recent self-hosted `API_EXTERNAL_URL` change is unrelated.
- Checked the current official `auth.admin.generateLink` reference: `https://supabase.com/docs/reference/javascript/auth-admin-generatelink`.
- CLI discovery:
  - `supabase --version` -> `2.111.0`
  - `supabase migration --help` -> confirmed the `new` subcommand
  - `supabase migration new --help` -> confirmed syntax
- Migration was created by the required command, not by inventing a filename:
  - `supabase migration new static_site_auth_broker_platform_claims`
  - created `supabase/migrations/20260809143602_static_site_auth_broker_platform_claims.sql`
- `supabase migration list --local` was attempted read-only and could not connect to `127.0.0.1:54322` because no local Supabase stack was running. Starting/applying a database was forbidden by this lane, so migration verification is deterministic/static here and remains an integration apply gate.

## Deterministic validation

```text
python3 -m py_compile \
  serverless/static-site-auth-session-broker/index.py \
  serverless/static_site_auth_session_broker_http.py
# PASS

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_site_auth_session_broker.py \
  tests/test_static_site_auth_session_broker_http.py \
  tests/test_supabase_security_hardening.py
# 41 passed in 0.36s

node --test \
  site/tests/auth-session-fixture.test.mjs \
  site/tests/auth-session-fixture-acceptance.test.mjs
# 16 passed

node --test site/tests/search-e2e-workflow-contract.test.mjs
# 11 passed

node --check .github/scripts/issue-static-search-session.mjs
# PASS

git diff --check
# PASS
```

Coverage includes OIDC-derived identity, spoof rejection, all three platform-to-persona mappings, unique email enforcement, typed duplicate/persona-busy results, one client POST under concurrency, one broker claim/generate-link under concurrency, three-way HTTP admission, typed fourth-request overload, secret redaction, and cleanup.

## Integration gates and risks

1. Apply migration `20260809143602_static_site_auth_broker_platform_claims.sql` before deploying code that calls RPC v2. No schema was applied in this lane.
2. The deployed broker's exact `AUTH_SESSION_BROKER_ALLOWED_WORKFLOW_REFS` must include the final Stage 2 health workflow ref before live execution; this is environment policy, not committed code.
3. The three existing configured persona emails must be distinct. Startup intentionally fails if the environment still omits a mobile persona or aliases two platforms to one account.
4. Cross-process/cross-instance identical calls cannot share credential material by design. The database returns `duplicate_inflight` UNKNOWN; the workflow must not retry Search or misclassify it as BROKEN.
5. The legacy multi-variant browser debug runner does not supply `platform` and uses cold/degraded browser identities. Stage 2 integration must either keep it on its old broker contract or explicitly update/supersede that manual-only path; the new health contract intentionally issues only the dedicated cached platform persona.
6. Canonical feature docs and `CHANGELOG.md` were forbidden in this lane and remain integrator-owned.

## Changed files

- `.github/scripts/issue-static-search-session.mjs`
- `serverless/static-site-auth-session-broker/README.md`
- `serverless/static-site-auth-session-broker/index.py`
- `serverless/static_site_auth_session_broker_http.py`
- `site/e2e/auth-session-fixture/session-fixture.mjs`
- `site/tests/auth-session-fixture.test.mjs`
- `supabase/migrations/20260809143602_static_site_auth_broker_platform_claims.sql`
- `supabase/tests/event_search_canary_receipts_contract.sql`
- `tests/test_static_site_auth_session_broker.py`
- `tests/test_static_site_auth_session_broker_http.py`
- `tests/test_supabase_security_hardening.py`
- `.codex/lanes/search-production-health-stage2/broker-RESULTS.md`
