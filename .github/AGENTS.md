# GitHub Actions agent instructions

These instructions apply to `.github/` and supplement the root `AGENTS.md`.

## Static-site QA workflows

For workflows that build, test, publish or inspect the static site, read and
follow:

- `.codex/skills/static-site-autotest/SKILL.md`;
- `docs/operations/static-site-autotest-strategy.md`;
- `docs/testing/static-site-autotest-scenarios.v1.yml`;
- `docs/features/static-site-pages/release-autotest-gates.md`.

Workflow structure must reflect scenario/suite/platform, not one workflow file
per business step. Use reusable workflows or matrix jobs where this reduces
copying without hiding side effects.

## Cost and selection

- PR fast checks cover affected L0/L1 scope and do not start emulators by default.
- Android/iOS jobs run only for registry-selected mobile-sensitive scenarios or
  representative release specimens.
- Never run the full static catalog on Android Emulator or iOS Simulator.
- macOS/iOS is not used for data-only PRs.
- Heavy advisory workflows may run asynchronously, but must publish a terminal
  summary and must never be reported as PASS while still running.

## Mobile integrity

- Android means Chrome Android inside an Android Emulator with UiAutomator2.
- iOS means Mobile Safari inside an iOS Simulator with XCUITest.
- Desktop mobile viewport and Playwright WebKit do not satisfy these gates.
- Pin actions and dependencies; pin or explicitly validate runner, Xcode/iOS
  runtime, Android image/API, browser and Appium driver versions.
- Record actual runtime/device versions in the evidence artifact.

## Evidence

Every static-site QA job must upload a predictable artifact with
`qa-summary.json`, scenario/platform/SHA/target metadata and safe diagnostics.
Ordinary UI jobs may retain video/trace on failure. Real OTP jobs may not retain
email, OTP, raw mail, cookie, JWT, authorization data, HAR, trace or video.
Artifact upload for restricted jobs is fail-closed on redaction audit.

A background run must expose run ID/URL, exact repository SHA, target and suite.
A failure or `BLOCKED` result must be visible in a check summary, issue/comment
or release evidence; never silently discard it.

## Real focus OTP

Extend `.github/workflows/external-focus-email-otp.yml`; do not add an unrelated
competing workflow.

- Keep the protected `external-e2e` Environment and `contents: read` permission.
- Keep fixed routine identity and global concurrency.
- While one mailbox is shared, browser, Android and iOS real-mail variants are
  sequential or explicitly selected one at a time.
- Do not automatically run real OTP on every PR or nightly.
- A retry before any side effect may handle a proven simulator startup flake;
  never blindly retry an ambiguous OTP issuance.
- A workflow skeleton/configuration test is not mobile acceptance without a
  terminal emulator/simulator run.
