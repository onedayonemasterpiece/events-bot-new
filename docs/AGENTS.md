# Documentation agent instructions

These instructions apply under `docs/` and supplement the root `AGENTS.md`.

## Static-site autotest documentation

When a task concerns static-site testing, data quality, Playwright, Android
Emulator, iOS Simulator, PWA, focus OTP, Supabase/Yandex connectivity or release
evidence, treat the following as one strategic document set:

- `docs/operations/static-site-autotest-strategy.md` — canonical strategy;
- `docs/testing/static-site-autotest-scenarios.v1.yml` — machine-readable source
  of scenario/platform/status/run policy;
- `docs/features/static-site-pages/release-autotest-gates.md` — normative
  companion to the static-site release plan;
- `docs/testing/static-site-autotest-codex-prompt.md` — first implementation
  handoff;
- `docs/testing/external-focus-email-otp.md` — protected real-mail runbook.

Do not create a competing strategy or scenario list. Update this set together
when a scenario changes status, platform, trigger, blocking policy, security
boundary or evidence contract.

A scenario may move from `planned`/`partial` to `implemented` only with a real
runnable test and terminal evidence at the claimed layer. Workflow skeletons,
fixtures, mocked user agents and desktop mobile viewports do not close native
Android/iOS scenarios.

The main `docs/features/static-site-pages/release-plan.md` remains umbrella
release truth. New test policy must link to it through
`release-autotest-gates.md`; do not duplicate the full release ledger.

Documentation must distinguish:

- blocking terminal evidence;
- `STARTED_BACKGROUND` advisory runs;
- protected manual side-effect tests;
- `BLOCKED` infrastructure;
- not-yet-implemented product/test coverage.

Never describe an unfinished background run or a planned emulator job as PASS.
