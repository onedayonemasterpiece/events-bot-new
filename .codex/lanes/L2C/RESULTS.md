# L2C — mandatory Auth fixture protected RLS probe

## Scope

- Lane: `L2C`
- Requirement ID: `R5`
- Base SHA: `f8dbe224ed8aded5d2067d377bea5387b5635a7e`
- Head SHA (validated implementation): `8f95a06028f1abb6d7aaf50e71a28b95324d9d01`
- Branch: `agent/static-unified/l2c-auth-protected-probe`

## Result

Closed the reviewer-reported security gap where
`createAuthSessionFixture()` initialized `protectedProbe=true` and could emit
`protected_probe_verified: true` without executing a protected request.

The fixture now:

- requires a `protectedProbe` callback before issuing credentials;
- gives the callback a fixture-owned probe fetch wrapper;
- permits exactly one same-origin `GET /rest/v1/*` probe;
- requires the created session JWT in `Authorization: Bearer ...`;
- requires the configured publishable key in `apikey`;
- requires one actual successful HTTP response and callback result exactly
  `true`;
- blocks omitted callbacks, callbacks that only return `true`, missing/wrong
  session headers, non-read-only/non-REST probes and failed HTTP/RLS responses;
- emits `protected_probe_verified: true` and
  `protected_probe_request_count: 1` only after that proof;
- preserves product OTP issue / external mail send / receipt at `0/0/0`.

The positive regression uses a JWT-shaped access token and an owner-scoped REST
response; the callback validates `owner_id` against the user returned by
`auth.getUser`.

## Evidence and commands

- Test-first reproduction:
  - `node --experimental-strip-types --test site/tests/auth-session-fixture.test.mjs`
  - expected FAIL: omitted probe was accepted and the prior callback contract
    did not supply/verify the RLS owner request.
- Final targeted regression:
  - `node --experimental-strip-types --test site/tests/auth-session-fixture.test.mjs site/tests/no-mail-fault-matrix.test.ts`
  - PASS `14/14` (`6` fixture + `8` no-mail matrix).
- Registry lint:
  - `node site/e2e/auth-session-fixture/registry-lint.mjs`
  - PASS.
- YAML parse/assertion:
  - `auth_get_user_and_protected_probe: required`;
  - `protected_probe_requests: exactly_1_successful_read_only_rls_request`;
  - PASS.
- `git diff --check`: PASS.

The isolated worktree did not install dependencies. Tests reused the existing
integration worktree's pinned `site/node_modules` through a temporary ignored
symlink, which was removed after each command.

## Changed files

- `site/e2e/auth-session-fixture/session-fixture.mjs`
- `site/tests/auth-session-fixture.test.mjs`
- `docs/testing/static-site-auth-session-fixture.md`
- `docs/testing/static-site-autotest-scenarios.v1.yml`
- `docs/operations/static-site-autotest-strategy.md`
- `.codex/lanes/L2C/RESULTS.md`

## Honest boundaries / risks

- The fixture enforces that a JWT-bound same-origin REST GET executed and
  succeeded. The scenario callback remains responsible for selecting the
  allowlisted owner-protected view/table and validating subject/owner fields;
  a merely public REST resource is forbidden by contract.
- These are deterministic local tests. Hosted target, real RLS policy and
  browser/device acceptance remain separate gates.
- No real Supabase request, OTP issue, mail provider call or remote write was
  performed.
