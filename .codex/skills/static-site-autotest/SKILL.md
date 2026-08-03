---
name: static-site-autotest
description: "Use for any KenigEvents static-site QA, browser E2E, Android Emulator, iOS Simulator, PWA, authenticated session fixture, focus OTP, Supabase/Yandex connectivity, page/data release gate, screenshot evidence or autotest analysis task. Selects only the risk-relevant scenarios and requires ChatGPT-launchable, ChatGPT-readable sanitized evidence."
---

# Static Site Autotest

## Canonical sources

Open these before changing or running static-site tests:

1. `docs/operations/static-site-autotest-strategy.md`;
2. `docs/testing/static-site-autotest-scenarios.v1.yml`;
3. `docs/testing/static-site-auth-session-fixture.md`;
4. `docs/features/static-site-pages/release-autotest-gates.md`;
5. `docs/operations/static-site-qa-chatgpt-control-plane.md`;
6. `docs/features/static-site-pages/release-plan.md`;
7. `docs/operations/e2e-scenarios.md`;
8. feature-specific docs and incident records for the affected surface.

For focus-group real-mail OTP additionally open:

- `docs/testing/external-focus-email-otp.md`;
- `docs/features/static-site-focus-group/README.md`;
- `.github/workflows/external-focus-email-otp.yml`;
- `site/e2e/focus-email/run.mjs`.

For direct/relay outage testing additionally open
`docs/testing/transport-fault-profiles.v1.yml` and the transport-failure section
of [`references/mobile-otp-acceptance.md`](references/mobile-otp-acceptance.md).
Fault control must be build-bound at the resilient transport `fetchImpl`
boundary before singleton construction; late `window.fetch` changes are only
diagnostic instrumentation.

For a new Android/iOS browser-tab implementation or an Appium failure, read
[`references/mobile-otp-acceptance.md`](references/mobile-otp-acceptance.md).
For any visible/stuck native overlay, browser first-run UI, chooser, permission
prompt or keyboard ambiguity, also use `$mobile-ui-sentry` before changing DOM
locators or retrying gestures.

The implementation handoff for the first Android/iOS milestone is
`docs/testing/static-site-autotest-codex-prompt.md`. It must also close the
ChatGPT launch boundary from the control-plane document; a UI-only
`workflow_dispatch` is not sufficient.

## Required workflow

1. Identify exact changed files, target SHA/build and affected page/data families.
2. Assign trigger tags and explicit `auth_mode` from the canonical scenario registry.
3. Select the cheapest evidence layer that can detect the risk:
   - L0 contracts first;
   - L1 browser for runtime/layout/catalog;
   - L2 Android/iOS only for mobile-system behavior or representative high-risk specimens;
   - L3 only when simulator evidence is inherently insufficient.
4. For a function after login, use `session_fixture` by default. Do not run a
   real-mail OTP merely because the scenario requires an authenticated user.
5. State the selected scenarios and explicit `not_applicable` reasons before a
   release decision. Do not run the full matrix by reflex.
6. Run blocking checks synchronously. A heavy advisory workflow may be started
   in background only under the documented policy.
7. A background run is reported as `STARTED_BACKGROUND` with run ID/URL, SHA,
   target and suite. Never call it PASS before terminal evidence.
8. Every implemented scenario must retain a safe project-level launch path from
   ChatGPT without Codex. Prefer the validated canonical issue-comment gateway
   plus the same reusable workflow used by `workflow_dispatch`; never assume a
   specific connector exposes workflow dispatch.
9. Produce or preserve the canonical sanitized evidence package and
   `qa-summary.json` so ChatGPT can inspect the run without the code agent.
10. Update scenario status, release companion, feature docs and changelog when
    an implementation moves from planned/partial to implemented.

## Auth mode boundary

Every identity-sensitive scenario must use one registry mode:

- `anonymous` — no session;
- `mocked_ui` — visual/component state only, no backend claims;
- `session_fixture` — real Supabase session without product OTP or mail;
- `admin_otp_ui` — real OTP UI/verify with fresh admin credential, no delivery;
- `real_mail_otp` — real issue, delivery, receipt and verify;
- `yandex_oauth` — real Yandex redirect/consent/callback.

Do not blur claims between modes. A mocked signed-in UI cannot prove JWT/RLS;
a session fixture cannot prove email delivery; a Search test does not need to
repeat real-mail OTP.

## Authenticated session fixture boundary

Use `session_fixture` for Search, personal pages, personalization, feedback,
saved state and other functions whose subject begins after login.

Required contract:

- trusted setup chooses only a fixed allowlisted E2E persona;
- issue one fresh admin-generated one-time link/OTP without external delivery;
- complete ordinary Supabase callback/verify and obtain a real user session;
- prove `auth.getUser` or an authenticated protected probe;
- scope session state to one worker/job/device;
- create a separate session for every parallel worker/job;
- keep state only in `$RUNNER_TEMP` or equivalent ephemeral storage;
- require `POST /auth/v1/otp = 0`;
- require external mail send/receipt `0/0`;
- delete state and pass redaction in `finally`.

Fixture failure is `BLOCKED_AUTH_FIXTURE`. Never recover it by sending a real
OTP. Never silently downgrade to `mocked_ui`.

Forbidden:

- `authorized=true`, fake user or focus participation marker as live Auth E2E;
- fixed OTP or email-specific production bypass;
- serialized Supabase session in GitHub Secrets;
- shared refresh token/session state across parallel workers/jobs;
- service-role/admin key in browser, Appium, localStorage, URL or artifact;
- self-issued JWT;
- auth state in cache, artifacts or job outputs;
- trace/HAR during credential bootstrap;
- arbitrary persona or redirect accepted from untrusted PR input.

Until an OIDC broker exists, keep the minimal issuer step in a protected trusted
Environment and ensure later browser/test steps never receive the admin secret.

## Platform boundary

- Desktop/mobile Playwright viewport is browser evidence, not Android/iPhone evidence.
- Playwright WebKit is not native iOS acceptance.
- Do not crawl the full catalog on Android Emulator or iOS Simulator.
- Use Android/iOS for system keyboard, install UI, Launcher/SpringBoard,
  Share Sheet, standalone/lifecycle and selected mobile-critical journeys.
- Pin/record runner, OS, browser, simulator/emulator, Appium and driver versions.
- An authenticated Android/iOS job receives its own one-time credential/session;
  do not copy the browser worker's state or refresh token to a device job.
- Direct JS token injection is not platform acceptance; prefer ordinary
  callback/verify in the same browser storage area used by the scenario.

## ChatGPT launch boundary

- `workflow_dispatch` alone does not prove launchability from ChatGPT.
- Use a strict allowlisted `/qa run` issue-comment command gateway when direct
  connector dispatch cannot be guaranteed.
- Validate canonical issue, actor permission, registry scenario/platform,
  exact target allowlist and full SHA before checkout or side effects.
- Never parse commands through `eval` or execute arbitrary refs/shell text.
- Protected OTP and privileged session issuer steps require Environment approval
  and bounded concurrency appropriate to their side effects.
- Serialize commands per canonical issue and deduplicate an identical comment
  posted inside the prior run's accepted-to-terminal time bracket. A queued
  duplicate must link the prior run and create no new OTP job; the same command
  posted after its terminal receipt remains an intentional rerun.
- Report accepted requests with run URL/ID as `STARTED_BACKGROUND` or blocking
  start; report terminal PASS/FAIL/BLOCKED separately.
- Build terminal comments from downloaded, redaction-gated `qa-summary.json`;
  reusable-workflow success is not a scenario result.

## Focus real-mail OTP boundary

- Modify the existing isolated harness; do not create a competing test.
- Preserve exact target SHA, one OTP issue, one verify, one participant
  registration and returning-state assertions.
- Browser/Android/iOS real-mail variants are sequential while one mailbox is shared.
- No service key in browser, fixed OTP, bypass or blind resend.
- For real OTP, never retain email, OTP, raw mail, cookies, JWT, HAR, trace or video.
- Upload only after fail-closed redaction audit.
- Android/iOS are not complete until a real emulator/simulator run reaches a
  terminal result; a workflow skeleton is not evidence.
- Drive one semantic journey through platform adapters. Do not duplicate the
  business assertions in three test files.
- On Android require working KVM before boot; do not accept slow software
  emulation as equivalent evidence.
- On iOS use the XCUITest-driver-matched prebuilt WebDriverAgent. Keep ordinary
  navigation in WebKit, but focus keyboard-critical Safari inputs through their
  exact labelled XCTest text fields. Do not use blanket `nativeWebTap`, Safari
  coordinate calibration, raw hierarchy dumps or JS value assignment.
- Let Appium own boot/shutdown of the exact shutdown simulator UDID. Do not use
  external Simulator boot/open/defaults or menu/`Cmd-K` keyboard rescues.
- Use the embedded side-effect-free control email/numeric/product keyboard
  preflight after direct iOS harness changes. A visible Safari first-run dialog
  is `BLOCKED_SAFARI_FIRST_RUN_UI`, never a keyboard verdict.
- Capture a safe device screenshot immediately after Safari launch and again
  after at most 20 seconds without the expected marker. After every native
  action, verify a real state transition; two unchanged actions stop blind
  retries and enter the `$mobile-ui-sentry` blocker workflow.
- Mask a derived recipient immediately. Mask/register an extracted OTP before
  the next WebDriver command; never upload or print a raw Appium log tail.

## Release blockers

- required scenario missing or non-terminal;
- wrong target SHA/build identity;
- mobile-sensitive change without required Android/iOS evidence;
- emulator substituted by desktop viewport;
- planned scenario represented as implemented PASS;
- background run silently ignored;
- implemented scenario has no safe ChatGPT launch path;
- unsafe evidence or failed cleanup/redaction;
- one mailbox used by parallel real OTP jobs;
- ordinary authenticated business scenario unexpectedly sent OTP/mail;
- session fixture automatically fell back to real mail;
- auth state/session token persisted in secret, artifact, cache or job output;
- one refresh token/session state shared by parallel workers/jobs;
- service/admin credential exposed to browser or untrusted PR code;
- full-catalog emulator scope without an explicit exceptional rationale.
