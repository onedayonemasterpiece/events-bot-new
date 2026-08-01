# INC-2026-08-01 Region Talk empty-reaction synchronization failure

Status: open
Severity: sev2
Service: Region Talk operator approval and daily publication planning
Opened: 2026-08-01
Closed: —
Owners: events-bot / Region Talk
Related incidents: `INC-2026-08-01-region-talk-draft-backfill-nameerror`, `INC-2026-07-31-region-talk-candidate-chat-incomplete-drafts`
Related docs: `docs/features/region-talk-channel/publication-queue.md`, `docs/operations/cron.md`

## Summary

The first production reaction synchronization after delivering current v8
article and social drafts aborted on a valid review message that had no
reactions. Telegram returned `MSG_ID_INVALID` from
`messages.getMessageReactionsList`; the synchronizer treated that response as a
missing/invalid delivery instead of first checking the message's optional
`Message.reactions` field.

## User / Business Impact

- newly delivered candidates remained visible and reactable in the operator
  chat, but the autonomous runner could not persist their pending/approved/
  rejected/rewrite state;
- one reaction-less delivery blocked the whole fail-before-write observation
  batch, so later approvals could not reach the anti-vector publication plan;
- no reaction was misclassified and no public target publication occurred.

## Detection

The manual compensating `region_talk_reaction_sync.py --execute` run failed with
`MsgIdInvalidError` immediately after messages `33783` and `33784` were
delivered. Direct `get_messages` verification proved both messages existed;
`33783` had one photo and `33784` was the captioned first item of a six-photo
album, both with no reactions.

## Timeline

- 2026-08-01 14:31–14:32 UTC — current v8 drafts were delivered as messages
  `33783` and `33784` through `telethon_discovery2`.
- 2026-08-01 14:36 UTC — the first execute reaction sync aborted on
  `MsgIdInvalidError` before writing any observation.
- 2026-08-01 14:39 UTC — a one-delivery reproduction returned the same error.
- 2026-08-01 14:40 UTC — official Telegram API research confirmed clients
  should request the exact reactor list only when `Message.reactions` is set.

## Root Cause

1. `fetch_exact_reactions()` called `messages.getMessageReactionsList` for
   every delivery without loading the current message snapshot.
2. Telegram represents the absence of reactions by omitting the optional
   `Message.reactions` field and may reject the exact-list RPC with
   `MSG_ID_INVALID` instead of returning an empty list.
3. The test client modeled a zero-count list response but did not cover the
   real no-field transport state.

## Contributing Factors

- fail-before-write batching correctly protected consistency, but amplified one
  transport-contract mismatch into a full sync outage;
- reaction removal is reversible, so blindly ignoring the RPC error would have
  been unsafe without a fresh message snapshot.

## Automation Contract

### Treat as regression guard when

- changing Region Talk reaction polling, delivery message identity, album
  delivery, reviewer allowlisting or approval projection.

### Affected surfaces

- `scripts/region_talk_reaction_sync.py`;
- Telethon `get_messages` and `messages.getMessageReactionsList` calls;
- `publication_review_state_item` / `publication_review_event_item` and current
  candidate approval projections;
- the post-session publication planner.

### Mandatory checks before closure or deploy

- unit-test a reaction-less existing message without calling the list RPC;
- unit-test the last-reaction removal race and incomplete-page fail-closed path;
- run the full Region Talk test suite;
- deploy an exact `origin/main` SHA from a clean checkout;
- run production reaction sync against current messages `33783` and `33784`,
  verify a complete two-delivery observation and then execute the publication
  planner.

### Required evidence

- the original and one-delivery `MsgIdInvalidError` reproductions;
- official Telegram API contract at `https://core.telegram.org/api/reactions`;
- focused/full test results;
- deployed SHA, Fly health/SHA checks, reaction-sync and planner receipts.

## Immediate Mitigation

Public target publishing remains disabled. The current operator messages and
delivery ledger are preserved; no manual approval projection is manufactured.

## Corrective Actions

- read the exact message first and accept an absent `Message.reactions` field
  as a complete empty observation;
- if `MSG_ID_INVALID` races a list read, re-read once and accept empty only when
  the message still exists and its reactions field has disappeared;
- keep missing messages, pagination loops and count mismatches fail-closed.

## Follow-up Actions

- [ ] Deploy and execute the two-message compensating sync.
- [ ] Recalculate the anti-vector plan after the successful observation.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: focused `11 passed`; full Region Talk `689 passed`
- post-deploy verification: pending

## Prevention

The regression suite now models Telegram's optional reactions field rather than
assuming the exact-list RPC always returns a zero-count result.
