---
name: static-site-autotest
description: "Use for any KenigEvents static-site QA, browser E2E, Android Emulator, iOS Simulator, PWA, focus OTP, Supabase/Yandex connectivity, page/data release gate, screenshot evidence or autotest analysis task. Selects only the risk-relevant scenarios and requires ChatGPT-readable sanitized evidence."
---

# Static Site Autotest

## Canonical sources

Open these before changing or running static-site tests:

1. `docs/operations/static-site-autotest-strategy.md`;
2. `docs/testing/static-site-autotest-scenarios.v1.yml`;
3. `docs/features/static-site-pages/release-autotest-gates.md`;
4. `docs/features/static-site-pages/release-plan.md`;
5. `docs/operations/e2e-scenarios.md`;
6. feature-specific docs and incident records for the affected surface.

For focus-group OTP additionally open:

- `docs/testing/external-focus-email-otp.md`;
- `docs/features/static-site-focus-group/README.md`;
- `.github/workflows/external-focus-email-otp.yml`;
- `site/e2e/focus-email/run.mjs`.

The implementation handoff for the first Android/iOS milestone is
`docs/testing/static-site-autotest-codex-prompt.md`.

## Required workflow

1. Identify exact changed files, target SHA/build and affected page/data families.
2. Assign trigger tags from the canonical scenario registry.
3. Select the cheapest evidence layer that can detect the risk:
   - L0 contracts first;
   - L1 browser for runtime/layout/catalog;
   - L2 Android/iOS only for mobile-system behavior or representative high-risk specimens;
   - L3 only when simulator evidence is inherently insufficient.
4. State the selected scenarios and explicit `not_applicable` reasons before a
   release decision. Do not run the full matrix by reflex.
5. Run blocking checks synchronously. A heavy advisory workflow may be started
   in background only under the documented policy.
6. A background run is reported as `STARTED_BACKGROUND` with run ID/URL, SHA,
   target and suite. Never call it PASS before terminal evidence.
7. Produce or preserve the canonical sanitized evidence package and
   `qa-summary.json` so ChatGPT can inspect the run without the code agent.
8. Update scenario status, release companion, feature docs and changelog when an
   implementation moves from planned/partial to implemented.

## Platform boundary

- Desktop/mobile Playwright viewport is browser evidence, not Android/iPhone evidence.
- Playwright WebKit is not native iOS acceptance.
- Do not crawl the full catalog on Android Emulator or iOS Simulator.
- Use Android/iOS for system keyboard, install UI, Launcher/SpringBoard,
  Share Sheet, standalone/lifecycle and selected mobile-critical journeys.
- Pin/record runner, OS, browser, simulator/emulator, Appium and driver versions.

## Focus OTP boundary

- Modify the existing isolated harness; do not create a competing test.
- Preserve exact target SHA, one OTP issue, one verify, one participant
  registration and returning-state assertions.
- Browser/Android/iOS real-mail variants are sequential while one mailbox is shared.
- No service key, fixed OTP, bypass or blind resend.
- For real OTP, never retain email, OTP, raw mail, cookies, JWT, HAR, trace or video.
- Upload only after fail-closed redaction audit.
- Android/iOS are not complete until a real emulator/simulator run reaches a
  terminal result; a workflow skeleton is not evidence.

## Release blockers

- required scenario missing or non-terminal;
- wrong target SHA/build identity;
- mobile-sensitive change without required Android/iOS evidence;
- emulator substituted by desktop viewport;
- planned scenario represented as implemented PASS;
- background run silently ignored;
- unsafe evidence or failed redaction;
- one mailbox used by parallel real OTP jobs;
- full-catalog emulator scope without an explicit exceptional rationale.
