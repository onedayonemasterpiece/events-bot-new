# INC-2026-08-30 eventsBot MCP scheduled readback and routing

Status: closed
Severity: sev2
Service: private eventsBot MCP / Telegram and VK Social Workspace scheduling
Opened: 2026-08-30
Closed: 2026-08-30
Owners: events-bot
Related incidents: `INC-2026-08-27-eventsbot-vk-image-publish-outcome-unknown.md`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

An explicitly requested image announcement for `Светлый JAZZ`, scheduled for
2026-08-30 19:00 Europe/Kaliningrad, exposed two independent production
defects. Telegram returned durable `succeeded` but its item binding contained
only a numeric message id, so exact item read resolved the same-numbered
ordinary channel-history post from 2026-01-16. VK successfully completed all
four provider mutation stages, but immediate readback used the live-post API
for a postponed item and reconciliation rejected VK's re-owned community photo.
The VK preparation had also been routed by the caller to the wrong managed
community (`klgdevents`, owner `-231920894`) instead of
`kenigeventsofficial` (owner `-231828790`).

## User / Business Impact

- Telegram could contain the requested schedule, but MCP could not prove its
  text, image, target or time and a blind retry risked a duplicate.
- VK contained one exact image-bearing postponed post on the wrong managed
  community while MCP reported non-retryable `outcome_unknown`.
- The intended VK community had no exact postponed copy.

## Detection

- Telegram operation `op_QFfsy4OCiBGmsAjiig8XlE1zW-VicAPT` returned item
  `itm_F1X-Ct9FWtw6-fqcWI4qdylmpSIhCHH_`; exact read returned the unrelated
  2026-01-16 `ИЦАЭ OPEN: игровой` post.
- VK operation `op_rJi23Bwj1jbzjXC8EBgCoF3j6LCJYwQ6` returned
  `read_after_write_failed`; durable provider evidence recorded four HTTP 200
  stages through `wall.post` and native post id `10388`.
- Authenticated postponed reads found the exact text hash and one photo at
  owner `-231920894`, post `10388`, and no exact postponed item at owner
  `-231828790`.

## Timeline

- 2026-08-30 13:45 UTC — Telegram schedule committed for 17:00 UTC and was
  recorded as succeeded without scheduled-queue readback.
- 2026-08-30 13:45 UTC — VK upload, photo save and `wall.post` all returned
  HTTP 200; live-only readback classified the result unknown.
- 2026-08-30 14:16 UTC — status reconciliation still returned unknown.
- 2026-08-30 14:29 UTC — exact wrong-target postponed post `10388` was deleted;
  a second authenticated postponed read confirmed it absent.
- 2026-08-30 14:43 UTC — the intended VK schedule was created on owner
  `-231828790` as postponed post `1765`; exact text, 17:00 UTC and one photo
  were read back while the wrong owner remained empty.
- 2026-08-30 14:51 UTC — direct Telegram scheduled-history inspection showed
  that Telethon's high-level `get_messages(..., scheduled=True)` exposed only
  one of five provider results. Raw `messages.getScheduledHistory` proved the
  original exact post `2632` was present at 17:00 UTC with one image.
- 2026-08-30 14:55 UTC — one corrective duplicate `2633`, created while the
  incomplete high-level read falsely reported absence, was deleted through
  `messages.deleteScheduledMessages`; raw readback proved only original
  `2632` remained.

## Root Cause

1. `TelegramItemBinding` modelled only `(target_ref, message_id)` although
   Telegram scheduled ids occupy a separate queue namespace. Both immediate
   schedule handling and `GET_ITEM` initially read ordinary history.
2. Telethon 1.44's high-level scheduled iterator returned an incomplete queue
   in production. Passing `ids` also takes precedence over `scheduled`; even
   `get_messages(..., limit=None, scheduled=True)` returned one of five raw
   results. Exact verification must therefore use the raw bounded
   `messages.getScheduledHistory` result and select the id locally.
3. Telegram exact item projection minted a new inner item from the fetched
   message instead of retaining the requested binding, obscuring contract
   divergence.
4. VK schedule execution used `wall.getById`, which does not prove a postponed
   queue item.
5. VK reconciliation required the `photos.saveWallPhoto` owner/id pair to
   appear unchanged on the wall, but VK re-owned the attachment from user
   photo `868977531_457259767` to community photo `-231920894_456248829`.
6. Target selection was a caller routing error, independent of transport: the
   committed opaque target resolved exactly to `klgdevents`.

## Contributing Factors

- Schedule tests asserted only that provider send methods were called; they did
  not create an ordinary-history id collision or require exact readback.
- The prior VK incident covered image upload and generic reconciliation but
  not postponed-item readback with provider photo re-ownership.

## Automation Contract

### Treat as regression guard when

- changing Telegram item bindings, schedule/send/read paths or exact item reads;
- changing VK schedule, postponed/live readback, image attachment or unknown
  outcome reconciliation;
- repairing or retrying a managed-community schedule with an unknown outcome.

### Affected surfaces

- `private_events_mcp_telegram_adapter.py`
- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_workspace_providers.py`
- encrypted provider binding and operation state
- Telegram `@kenigevents`; VK managed owners `-231920894` and `-231828790`

### Mandatory checks before closure or deploy

- Telegram schedule returns verified read-after-write and exact item read keeps
  the same opaque item while an ordinary-history message has the same id;
- VK schedule reads `filter=postponed`, verifies exact post/time/text/photo
  count and does not reject a re-owned community photo;
- post-publication VK reconciliation may find the same exact item on the live
  owner wall and never replays `wall.post`;
- complete Private Events MCP tests, compileall and production health pass;
- provider readback proves one correct Telegram schedule and one correct
  intended-community VK schedule, with no wrong-target duplicate.

### Required evidence

- implementation and deployed exact-main SHA;
- targeted and complete Private Events MCP test results;
- Telegram exact scheduled-item MCP receipt;
- authenticated VK postponed/live receipts for both managed owners;
- production `/healthz`, runtime log mirror and SQLite quick check.

## Immediate Mitigation

- The wrong-target VK postponed post `wall-231920894_10388` was deleted after
  exact owner/id/text-hash/photo verification and confirmed absent.
- The correct VK postponed post `wall-231828790_1765` was created and verified.
- Raw Telegram scheduled history proved the original requested post existed.
  The single corrective duplicate was deleted and exact raw readback proved
  one remaining post (`2632`) with the requested text, image, target and time.

## Corrective Actions

- [x] Model Telegram scheduled items in a durable namespace-compatible binding.
- [x] Require exact Telegram scheduled peer/time/text/media read-after-write.
- [x] Bypass Telethon's incomplete scheduled iterator with the raw scheduled
  history request and select exact ids locally.
- [x] Reuse the requested Telegram opaque item during exact reads.
- [x] Read VK schedules from postponed queue and reconcile postponed plus live.
- [x] Bind VK reconciliation to known post id but compare stable photo count,
  not the provider-reowned photo owner/id pair.
- [x] Deploy exact main and repair/verify the requested Telegram/VK schedules.

## Follow-up Actions

- [ ] Keep target previews explicit in multi-community publication workflows;
  do not infer brand routing from similar display names.

## Release And Closure Evidence

- implementation SHAs: `7b1f0f300cd5d17408d5356fab2ad7e87a2ceab7`,
  `6459b81eaf57b021fcdcb745ea658b3cbc7b87aa`
- deployed SHA: `6459b81eaf57b021fcdcb745ea658b3cbc7b87aa`
- deploy path: clean detached worktree from exact `origin/main`; Fly release
  health returned ready/ok and the running repository SHA matched
- regression checks: targeted Telegram/VK/provider-store tests `126 passed`;
  complete Private Events MCP suite `519 passed` with three existing aiohttp
  `NotAppKeyWarning` warnings; compileall and diff-check passed
- post-deploy verification: MCP exact `get_item` retained
  `itm_F1X-Ct9FWtw6-fqcWI4qdylmpSIhCHH_` and returned the exact
  `Светлый JAZZ` text, one photo and `2026-08-30T17:00:00Z`;
  Telegram raw scheduled history contained only original message `2632` after
  duplicate `2633` was deleted; VK postponed owner `-231828790` contained exact
  post `1765` while wrong owner `-231920894` had no exact copy

## Prevention

Schedule acceptance requires provider queue readback, not a successful send
return alone. Opaque ids are never globally meaningful without their target and
queue namespace. VK image identity is stable at the intended wall post and
attachment count boundary, not at the pre-wall saved-photo owner/id boundary.
