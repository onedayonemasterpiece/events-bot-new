# L1-auth-broker results

- Lane: `L1-auth-broker`
- Requirement: `R02`
- Base SHA: `ec09c011674eecddf9e9b8e154e3d102f9384b12`
- Implementation head SHA: `5562c3526f76e779c80d741fabfb2602913ba43d`
- Status: PASS (implementation and focused tests); integration requires the
  atomic ledger RPC listed under Risks/dependencies.

## Delivered

- Replaced the fixture's hardwired admin call with an exact issuer interface:
  `createSupabaseAdminIssuer` and `createAuthSessionBrokerIssuer` implement the
  same fail-closed credential/counter contract.
- Retained server-side OTP verification, `auth.getUser`, one owner-scoped RLS
  probe, ephemeral Playwright storage state, cleanup, and zero product OTP/mail
  counters. OTP/action link/session data is cleared and omitted from receipts.
- Added exact-target `/poisk/` acceptance primitives for production and the
  43-character secret-candidate prefix. Acceptance checks an exact release
  SHA through `candidate-build.json` (candidate) or
  `static-release-manifest.json` (production), rejects redirects, proves the
  restored authenticated path, and returns only hashed target identity.
- Added a GitHub OIDC broker with signature/issuer/audience/lifetime validation,
  exact repository/ref/workflow/environment/event allowlists, claim-bound or
  exact run admission, exact persona and redirect rules, hard per-run/persona
  limit `1`, and keyed-hash-only audit.
- Broker returns the short-lived OTP and one-time action link only to the
  authenticated caller; neither is logged or copied into evidence. The action
  link is restricted to the configured Supabase origin and
  `/auth/v1/verify`.

## Evidence and commands

All passed:

```text
node --test site/tests/auth-session-fixture.test.mjs site/tests/auth-session-fixture-acceptance.test.mjs
# 12 passed, 0 failed

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_static_site_auth_session_broker.py tests/test_focus_auth_email_hook.py
# 34 passed

git diff --check
node --check site/e2e/auth-session-fixture/session-fixture.mjs
node --check site/e2e/auth-session-fixture/acceptance.mjs
/home/dev/.codex/venvs/events-bot-new/bin/python -m py_compile \
  serverless/static-site-auth-session-broker/index.py
```

Official contract research checked the current Supabase changelog/admin auth
surface and GitHub Actions OIDC documentation before implementation. The
broker accepts only `RS256` GitHub keys from the official JWKS URL.

## Risks / integration dependencies

- Deployment must provide atomic RPC
  `claim_static_site_auth_session_issue_v1(p_run_id, p_run_attempt,
  p_persona_id, p_repository, p_workflow_ref, p_limit)`, returning boolean true
  or `{ "admitted": true }`. There is intentionally no in-memory or
  best-effort fallback.
- Deployment configuration must set exact allowlists and a protected GitHub
  environment. `github-claim-bound` permits unattended scheduled numeric run
  IDs only when request `run_id` equals the signed GitHub claim.
- Live hosted acceptance and broker deployment are integration-lane concerns;
  this lane supplies the tested primitives and serverless handler.

## Changed files

- `serverless/static-site-auth-session-broker/README.md`
- `serverless/static-site-auth-session-broker/index.py`
- `serverless/static-site-auth-session-broker/requirements.txt`
- `site/e2e/auth-session-fixture/acceptance.mjs`
- `site/e2e/auth-session-fixture/session-fixture.mjs`
- `site/tests/auth-session-fixture-acceptance.test.mjs`
- `site/tests/auth-session-fixture.test.mjs`
- `tests/test_static_site_auth_session_broker.py`
- `.codex/lanes/L1-auth-broker/RESULTS.md`
