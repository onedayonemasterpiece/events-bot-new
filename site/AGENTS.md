# Static-site agent instructions

These instructions apply to all files under `site/` and supplement the root
`AGENTS.md`.

## Autotest and release contract

For any change to Astro pages, generated data consumption, PWA, Auth,
connectivity, forms, responsive behavior or browser tests, use the project skill
`.codex/skills/static-site-autotest/SKILL.md` and read:

- `docs/operations/static-site-autotest-strategy.md`;
- `docs/testing/static-site-autotest-scenarios.v1.yml`;
- `docs/features/static-site-pages/release-autotest-gates.md`;
- the affected feature document and incident regressions.

Classify the change with registry trigger tags and run only the risk-relevant
scenarios. Do not default to the full catalog or both mobile simulators.

For every material visual change, route the exact affected component/state to
the canonical `lovekgd-design-system` skill `ui-three-way-conformance` as the L1
visual/conformance scenario. The change is incomplete without its case or an
explicit `not_applicable` reason. Do not duplicate that skill in this repo.

- L0 artifact/data contracts first.
- L1 Playwright for affected routes, runtime and layout.
- L1 UI conformance compares the same resolved fixture in bounded Penpot and
  isolated Astro before any later real-consumer blocking stage.
- Android Emulator/iOS Simulator only for mobile-system behavior or selected
  high-risk page-family specimens.
- A desktop mobile viewport or Playwright WebKit never substitutes for required
  Android/iOS evidence.

Blocking checks must reach a terminal result before handoff. Heavy advisory jobs
may be started without waiting only under the documented background policy; they
must be reported as `STARTED_BACKGROUND` with run ID/URL, exact SHA and target,
never as PASS.

## Focus-group OTP

Do not create another isolated OTP implementation. Extend the existing
`site/e2e/focus-email/` harness and `.github/workflows/external-focus-email-otp.yml`.
Preserve:

- exact deployed repo SHA validation;
- one OTP issue, one verify and one participant registration;
- selected-once/no-blind-resend behavior;
- fixed routine test identity;
- sequential browser/Android/iOS real-mail execution while one mailbox is shared;
- PII-free ChatGPT-readable evidence;
- no raw mail, email, OTP, cookie, token, HAR, trace or video;
- fail-closed redaction before artifact upload.

A workflow skeleton or mocked adapter is not Android/iOS acceptance. Update the
scenario registry from `planned` only after terminal emulator/simulator evidence.

The first implementation scope is defined in
`docs/testing/static-site-autotest-codex-prompt.md`. PWA install/relaunch and the
generic all-pages runner remain separate follow-up scopes.
