# Acceptance matrix фокус-группы

> **Статус:** required scenarios; not runtime evidence.

## Identity/access

1. Invite writes/reuses local focus marker.
2. Invited anonymous browser opens ordinary site.
3. No Supabase anonymous Auth request is issued.
4. Feedback block is visible.
5. Score/text/screenshot/NPS controls are disabled.
6. Attempted interaction sends zero feedback requests.
7. Invite/share/QR remain available.

## Authentication return

For email and Yandex separately:

1. Start from a concrete feedback block.
2. Activate auth CTA.
3. Complete real/session-fixture journey according to scenario.
4. Return to exact route, page revision and feedback anchor.
5. Restore only safe local draft.
6. Submit one action.
7. Prove one authoritative row/receipt.
8. Reload and show committed state.

## Revision

```text
home-r3 score=7
-> home-r4
-> old score shown as history
-> new scale open
-> home-r4 score=9
```

No duplicate row on replay.

## Transport

- direct down / relay up;
- relay down / direct up;
- both down;
- response lost after commit;
- screenshot succeeds/text fails and reciprocal case;
- outbox/recovery exactly once;
- no PII/JWT/OTP/raw body in evidence.

## Artifacts

- inventory exactly seven IDs from canonical collection;
- no invented artifact;
- pre-auth local progress is not durable eligibility;
- authenticated receipt dedupe;
- keyboard-only placement uses an existing collection ID.

## Negative regressions

- no `anonymous_session` feedback path;
- no `10 of 12` or `12 artifacts` current UI copy;
- no direct logout in mobile account block;
- profile does not render Favorites/hidden collections.

## Platforms

- Chromium/Firefox/WebKit for final immutable candidate;
- Android/iOS only for contracts that require native system behavior;
- viewport emulation does not count as mobile-system PASS.
