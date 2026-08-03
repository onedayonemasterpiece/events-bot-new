# L2 — auth transport / focus precedence / onboarding docs

## Outcome

Implemented the local generic no-mail auth harness and synthesized the selected
PR #287 → PR #295 contracts while preserving focus v5 anonymous-first
precedence and the separate standard-onboarding branch.

## Delivered

- Closed auth-mode vocabulary including `anonymous_session` and
  `session_fixture`.
- `session-fixture.mjs`: allowlisted fixed personas and HTTPS origins, fresh
  admin-issued credential, normal Supabase `verifyOtp`, `auth.getUser`, optional
  protected probe, per-test/worker/job/device isolation, ephemeral mode-0600
  Playwright state, cleanup receipt, PII/token-free evidence and
  `BLOCKED_AUTH_FIXTURE` without real-mail fallback.
- Deterministic no-mail fault matrix for Auth verify, Search, personalization and
  focus feedback across normal/direct-down/relay-down/both-down. Selected-once
  operations dispatch at most once (zero when both routes are down); focus uses
  the catalogued idempotent-replay policy. OTP/mail/provider counters remain
  zero.
- Registry lint for closed auth modes, fixture dependencies/side-effect policy,
  PR #295 resilience scenarios and focus v5 anonymous-first invariants.
- Canonical strategy, release gates, Yandex dependency resilience, focus release
  and standard onboarding documentation synchronized. Existing collection
  quality registry/gate content was preserved.

## Validation

PASS:

- `node --test site/tests/auth-session-fixture.test.mjs` — 5/5.
- `node --experimental-strip-types --test site/tests/no-mail-fault-matrix.test.ts`
  — 2/2.
- `node site/e2e/auth-session-fixture/registry-lint.mjs` — PASS.
- `node --test site/tests/static-site-autotest-registry-lint.test.mjs` — 2/2.
- PyYAML parse of both scenario registries — PASS.
- `npm --prefix site run test:resilient-client` — 47/47 across its two node-test
  invocations.
- `npm --prefix site run test:focus-group-product` — 83/83.
- `git diff --check` — PASS.

Environment-only non-pass:

- `npm --prefix site run test:external-focus-email-otp` was attempted without
  issuing OTP or contacting a mailbox. Its existing unit suite could not finish
  because the shared preinstalled `site/node_modules` lacks packages `yaml` and
  `ws`; no `npm install` was performed per lane constraint. Earlier subtests in
  that invocation passed until those import failures.

## Explicit boundaries / remaining gates

- No real OTP, external mail, OAuth, remote write, workflow dispatch, publish or
  deploy was performed.
- The generic fixture has local mocked-client acceptance only. Hosted allowlisted
  target acceptance, protected issuer/OIDC-broker integration, second browser
  context and native device bootstrap remain honest pending gates.
- PR #295 YDB projection, partial component delivery, OAuth/Postbox/inbound and
  shared-upstream scenarios remain `planned` where no executable product path
  exists; the local four-route matrix is not presented as live/provider proof.
- No edits to `CHANGELOG.md`, `docs/routes.yml`, or `EventLayout.astro`.
