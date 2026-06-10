# INC-2026-06-10 Guide Watchdog Kaggle Failure Retry Loop

Status: blocked-on-session-validation
Severity: sev2
Service: Guide Excursions Monitoring / critical scheduler watchdog / Kaggle
Opened: 2026-06-10
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag`, `INC-2026-06-07-guide-remote-session-stale-busy`
Related docs: `docs/operations/cron.md`, `docs/features/guide-excursions-monitoring/README.md`

## Summary

On 2026-06-10 the guide excursions critical watchdog repeatedly retried the
same missed `guide_excursions_full` slot after the Kaggle kernel reached
terminal `ERROR`. This created repeated admin notifications and repeated Kaggle
kernel pushes within the same hour.

The root failure observed in the Kaggle output was Telethon startup failing with
`AuthKeyDuplicatedError`.

## Evidence

- Production `ops_run` rows showed repeated scheduled full guide attempts with
  status `error` and `Guide Kaggle kernel failed (failed)`.
- Kaggle output showed:
  `AuthKeyDuplicatedError: The authorization key (session file) was used under two different IP addresses simultaneously, and can no longer be used.`
- On 2026-06-10 production `TELEGRAM_AUTH_BUNDLE_S22` was rotated to the
  separate `/home/dev/projects/kdg80/.env` `TELEGRAM_AUTH_BUNDLE_S22`.
- A smoke run after that rotation still failed with the same
  `AuthKeyDuplicatedError`.

## Current Decision

The code-level watchdog throttling change was reverted after operator feedback:
the intended remediation path is session-secret correction/validation, not an
extra retry-cooldown layer.

The previously deployed code changes were:

- `b987769393a8693f120fe4b2a5bfac32e3a53e88` — added watchdog retry cooldown;
- `9e00fc17f269f27cc11c3d9c1d2e84ac84d5d5a2` — changed Kaggle auth secret
  rotation behavior.

`9e00fc17f269f27cc11c3d9c1d2e84ac84d5d5a2` was reverted by
`087588322159d7642085c2749351b50af21795b5`.

`b987769393a8693f120fe4b2a5bfac32e3a53e88` is being reverted as well so the
guide scheduling code returns to the previous retry behavior.

## Remaining Blocker

Guide monitoring still requires a working, exclusive `TELEGRAM_AUTH_BUNDLE_S22`
for the remote Kaggle monitoring role, and the current production validation is
not green yet.
