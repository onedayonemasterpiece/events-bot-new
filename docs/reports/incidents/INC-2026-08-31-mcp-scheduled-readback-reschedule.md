# INC-2026-08-31 eventsBot MCP scheduled readback blocked a safe reschedule

Status: open
Severity: sev2
Service: private eventsBot MCP / Telegram and VK scheduled publication
Opened: 2026-08-31
Closed: —
Owners: events-bot production / eventsBot MCP
Related incidents: `INC-2026-08-24-mcp-telegram-album-media-ref`, `INC-2026-08-25-chatgpt-frozen-mcp-actions`, `INC-2026-08-27-eventsbot-vk-image-publish-outcome-unknown`, `INC-2026-08-30-mcp-scheduled-readback-routing`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

A four-image publication was prepared for Telegram `@lovekenig` and VK owner
`-231828790`, initially for 2026-08-31 10:30 Europe/Kaliningrad. Telegram
operation `op_h5whKgjfe2rXj24zWU1yF5ynU9iEKt0T` crossed the runtime deadline,
then remained `outcome_unknown / reconciliation_pending` because its provider
claim had no terminal result and status reconciliation did not query Telegram.
Two VK operations failed definitely at the first image before any wall write.

The requested move to 14:00 could not be performed safely while the old
Telegram album might already have been scheduled or published. The incident is
therefore fail-closed as `BLOCKED_OLD_TELEGRAM_OUTCOME`. Codex did not publish,
delete, retry, alter the caption, or regenerate the four images.

## User / Business Impact

- The intended coordinated Telegram/VK publication was not safely completed.
- The Telegram outcome could not be distinguished from a successful provider
  mutation whose local receipt was cancelled; blindly restaging it could create
  a duplicate.
- The caller could not independently inspect either provider's scheduled queue
  through the public MCP contract or safely cancel the exact old Telegram
  album before the requested new slot.
- Both VK attempts were definitely pre-wall failures, but the existing retry
  ergonomics required a new preparation/idempotency key instead of one bounded,
  single-flight retry of the same logical action.

## Detection

- Commit/status receipts showed Telegram `outcome_unknown / provider_timeout`
  followed by repeated `outcome_unknown / reconciliation_pending`, with
  `retry_safe=false`.
- Read-only production database inspection found the Telegram provider row
  unchanged since `2026-08-31T07:18:58.636Z` with `result_json IS NULL`.
- The deployed runtime and adapter both used the same 12-second default because
  the provider-timeout env override was unset. Source inspection confirmed that
  the runtime's outer `asyncio.wait_for(...)` could cancel the adapter after its
  provider claim and before durable finalization.
- Both VK operation ledgers —
  `op_FQ1cqeUSAeS2mMZDXvD3tGvTz3hyswe1` and
  `op_noXSTwUeheMYYBLShjjIC_7w2_a3UpLy` — showed the first ordered image
  (`00_cover.png`) reached upload-server and `wall_photo_multipart` with HTTP
  200, but its normalized `photo` and/or `hash` was empty. Neither operation
  reached `photos.saveWallPhoto` or `wall.post`.
- Authenticated read-only VK verification found exactly **0** matching items in
  the postponed queue and exactly **0** matching text-plus-four-photo items in
  the latest 100 live owner-wall items.
- Fly release `v2053` ran SHA
  `64f75d10f7aff33fa616cee212878bd9d03673b1`, matching the audited
  `origin/main`. `/healthz` returned `ok=true`, `ready=true`, `db=ok`; both
  SQLite databases returned `PRAGMA quick_check=ok`. The production file mirror
  governed by `ENABLE_RUNTIME_FILE_LOGGING` / `RUNTIME_LOG_DIR` was enabled,
  present, fresh, and within its 64 MiB budget. This proves the service was
  otherwise healthy; it does not resolve the old Telegram outcome.

The exact audited payload identity was caption SHA-256
`b2bb4a7413b1cb63b2913a630a5cc6f1bce477fb9258a193fe4f14d7125328cd`
plus these ordered 1122×1402 PNG SHA-256 values:

1. `cec948b34c6fbf63fcc512310bfc29b11ffa1f68381477f132901592b08c4c48`
   (`00_cover.png`);
2. `3f094a48097a6ff711291435a5ba73b3b8649548926254a2ee992e6da86b9866`
   (`01_1714_paradeplatz.png`);
3. `1ea7340f519367ed98466666c36a4da3501571f7143dc80271d6639240addf86`
   (`02_1844_albertina.png`);
4. `9423f618c3246fd760f63dc8c8ec15da9e5db3b4b98cf61313e1983f90e7447e`
   (`03_1946_settlers.png`).

## Timeline

- 2026-08-31, before 10:30 Europe/Kaliningrad — the four-image Telegram and VK
  schedules were prepared for the original slot.
- 2026-08-31 07:18:58.636 UTC — the durable Telegram provider claim was
  recorded; its result remained NULL after the runtime timeout.
- 2026-08-31 — repeated Telegram status calls returned
  `reconciliation_pending` without provider scheduled/live readback.
- 2026-08-31 — two VK attempts failed on image ordinal 1/4 after multipart HTTP
  200 and before photo save or wall post.
- 2026-08-31 — the requested target time changed to 14:00
  Europe/Kaliningrad. The reschedule was withheld because the 10:30 Telegram
  outcome was still unknown.
- 2026-08-31 — a read-only production audit verified release/health/runtime
  evidence and zero exact VK postponed/live copies. It performed no provider
  mutation and retained `BLOCKED_OLD_TELEGRAM_OUTCOME`.
- 2026-08-31 — the isolated integration branch implemented the durable
  cancellation/readback repair. Its focused integration suite passed 308 tests;
  the final complete Private Events MCP suite passed 546 tests with three unrelated
  existing aiohttp warnings; `compileall` and `git diff --check` passed. The
  independent checklist review found no remaining code blocker. The incident
  remains open pending PR CI, exact-main merge/deploy and operational readback.

## Root Cause

1. **Proven cancellation race:** runtime and Telegram adapter owned equal
   provider deadlines. The runtime wrapped the whole adapter call in an outer
   `asyncio.wait_for`, so cancellation could occur after the adapter claimed an
   operation or attempted mutation but before `_complete_operation()` or
   `_release_operation()` made the provider ledger terminal.
2. **Proven non-convergent ledger/reconciliation:** the Telegram provider row
   stored a nullable result but no durable intent, mutation state, attempt,
   lease/deadline, or stale-claim recovery evidence. `reconcile()` resolved only
   that row; a NULL result became `operation_in_progress`, which runtime mapped
   back to `reconciliation_pending` indefinitely instead of reading Telegram.
3. **Proven control-plane gap:** raw Telegram scheduled-history and internal VK
   postponed reads existed, but no narrow ChatGPT-visible scheduled-queue tool
   exposed them. Telegram scheduled deletion also used the ordinary message
   namespace, and VK deletion did not prove postponed-queue absence.
4. **Proven VK failure boundary:** both VK attempts returned a normalized
   multipart receipt with HTTP 200 but an empty `photo` and/or `hash`, so they
   failed before `photos.saveWallPhoto` and `wall.post`. The exact empty field
   and original provider response shape are **not recoverable** from the old
   evidence; attributing the response to compression, actor routing, or another
   upstream cause would be a hypothesis. Existing gzip decompression and
   read-to-EOF handling were already deployed and are not an evidenced root
   cause for these two attempts.

## Contributing Factors

- One short whole-operation deadline did not account for a multi-image,
  multi-stage provider action or leave a durable-finalization/readback margin.
- Existing Telegram schedule coverage exercised the success/read-after-write
  path, not outer cancellation after a provider mutation or restart recovery of
  an old NULL claim.
- The scheduled Telegram binding did not retain every physical album member
  needed for exact scheduled-namespace cancellation and absence proof.
- Existing VK diagnostics did not retain safe field types/lengths or decoded
  receipt structure, so the exact invalid field cannot be reconstructed after
  the fact.
- Prior successful ordinary or fast scheduled publications did not exercise
  the cancellation window, four-image VK chain, or stale-claim recovery path.

## Automation Contract

### Treat as regression guard when

- changing runtime/provider timeout ownership, cancellation or durable
  operation finalization;
- changing Telegram provider claims, reconciliation, raw scheduled-history,
  logical album bindings or scheduled deletion;
- changing VK multipart upload/save, postponed/live readback, exact deletion,
  retry classification or provider-stage diagnostics;
- adding or changing a ChatGPT-visible scheduled-list/retry tool, its scopes,
  bounds, redaction or action-definition publication;
- reconciling or retrying an outcome-unknown scheduled publication.

### Affected surfaces

- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp_telegram_adapter.py`
- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_vk_transport.py`
- `private_events_mcp_vk_upload.py`
- `private_events_mcp_workspace_providers.py`
- isolated auth/provider SQLite ledgers and encrypted intent/bindings
- ChatGPT action-control snapshot and existing Telegram/VK schedule scopes
- Fly runtime health, runtime-log mirror and exact-main release path

### Mandatory checks before closure or deploy

- prove runtime cancellation after the provider mutation boundary cannot leave
  a Telegram or VK claim indefinitely without a durable terminal/leased state;
- restart from a Telegram NULL-result claim and converge through raw
  `messages.GetScheduledHistoryRequest`, logical album collapse and, after the
  scheduled time, ordinary/live history;
- cover exactly one match, bounded zero-match consistency polling, terminal
  `reconciliation_no_match`, proven pre-mutation
  `provider_mutation_not_started`, and ambiguous duplicate matches without
  blind resend or delete;
- prove a four-image Telegram album is one logical scheduled item in immutable
  order, while the ordinary feed remains distinct from the scheduled queue;
- prove Telegram scheduled deletion uses
  `messages.DeleteScheduledMessagesRequest`, deletes every exact album member
  and verifies raw scheduled-queue absence; prove VK deletion targets the exact
  owner/post and verifies postponed-queue absence;
- expose `social_scheduled_items_list` under the existing
  `telegram:schedule` / `vk:schedule` scope families with a small hard limit,
  exact optional filters, opaque refs and no native identifiers, tokens, upload
  URLs or provider payloads; verify its default limit 10 and hard maximum 25;
- prove bounded retry accepts only a terminal `retry_safe=true` attempt, retains
  one logical action/preparation identity and prevents concurrent retries;
- cover four sequential VK PNG upload/save stages and one postponed wall post,
  flat/nested receipts, gzip, fragmented reads, empty/invalid photo/hash, exact
  postponed readback, and no later mutation after a definite pre-wall failure;
- run targeted regressions, the complete Private Events MCP suite, compileall
  and repository-standard diff/schema checks;
- before production deploy, fetch `origin`, use a clean exact `origin/main`,
  verify no unmerged release/hotfix drift and confirm the changelog matches the
  deployed commit;
- after deploy, verify exact SHA, Fly release, `/healthz`, both SQLite
  `quick_check` results, runtime mirror freshness/budget and the authenticated
  tool catalogue;
- publish/review the changed ChatGPT action definition and verify the new tools
  in a genuinely new chat; OAuth reconnect or direct `tools/list` alone is not
  acceptance.

### Required evidence

- merged and deployed exact-main SHA plus Fly release identifier;
- targeted/full-suite/compile/schema test receipts;
- sanitized historical receipts for all three operation refs, including
  provider stage, mutation-boundary state, retry safety and final readback;
- exact Telegram scheduled/live and VK postponed/live match counts for the
  immutable four-image payload;
- proof whether the old 10:30 Telegram album exists, was published, or was
  safely cancelled with absence verified;
- authenticated catalogue evidence for scheduled-list, scheduled-delete and
  bounded retry behavior in the refreshed ChatGPT action snapshot;
- `/healthz`, runtime-log mirror and both SQLite health receipts;
- confirmation that the delivered fix is reachable from `origin/main`.

## Immediate Mitigation

- Kept the operation fail-closed as `BLOCKED_OLD_TELEGRAM_OUTCOME` and did not
  create a 14:00 duplicate while the old Telegram result was unresolved.
- Confirmed through authenticated read-only VK inspection that there were zero
  exact postponed and zero exact live copies at the audited owner.
- Performed no publication, cancellation, retry, image regeneration or caption
  change from Codex.

## Corrective Actions

- [x] Make the provider adapter the owner of its transport/session deadline;
  runtime must not cancel before durable finalization. Handle
  `asyncio.CancelledError` at the provider-operation boundary.
- [x] Persist encrypted/sanitized Telegram intent, mutation timestamps, bounded
  attempt/lease state and enough exact target/time/text/media evidence for
  restart-safe reconciliation, including a migration-safe legacy adoption path.
- [x] Reconcile scheduled Telegram operations against raw scheduled history and
  later live history, with exact, ambiguous, bounded-pending and terminal
  zero-match outcomes.
- [x] Add the narrow logical `social_scheduled_items_list` read for Telegram raw
  scheduled history and VK `wall.get(filter="postponed")` without a new OAuth
  scope family.
- [x] Make scheduled deletion namespace-aware and require exact absence
  readback for Telegram album members and VK postponed items.
- [x] Add one bounded, single-flight retry path for terminal
  `retry_safe=true` attempts under the same logical action/preparation.
- [x] Persist safe VK multipart/stage observability sufficient to distinguish
  response shape and empty-field failures without bodies, credentials, hashes
  or upload URLs.
- [ ] Complete code review, tests, merge, exact-main deploy and operational
  reconciliation before changing the incident status.

## Follow-up Actions

- [ ] eventsBot MCP owner — reconcile the historical Telegram operation through
  provider-backed scheduled/live reads after the fixed exact-main release.
- [ ] ChatGPT workspace administrator — review and publish the changed action
  catalogue, then verify scheduled-list/retry availability in a new chat.
- [ ] incident owner — produce a machine-readable operational handoff with one
  allowed status: `READY_FOR_14_00_PUBLICATION`,
  `BLOCKED_OLD_TELEGRAM_OUTCOME`, `BLOCKED_PROVIDER_READBACK`, or
  `BLOCKED_DEPLOYMENT`.
- [ ] incident owner — if the requested slot has passed, stop rather than
  publishing immediately or silently changing the time.

## Release And Closure Evidence

- implementation SHA(s): pending
- merged `origin/main` SHA: pending
- deployed SHA / Fly release: pending
- deploy path: pending; must be a clean exact-`origin/main` release
- regression checks: pending
- action-definition review/publication and new-chat acceptance: pending
- old Telegram operation reconciliation: pending
- exact Telegram scheduled/live counts: pending
- exact VK postponed/live counts at audit: `0 / 0`; post-deploy recheck pending
- old 10:30 exact item existence/cancellation/absence proof: pending
- live duplicate check: VK `0` in latest 100 audited owner-wall items; Telegram
  pending
- post-deploy health/runtime mirror/SQLite verification: pending
- current handoff: `BLOCKED_OLD_TELEGRAM_OUTCOME`

## Prevention

Successful provider returns are not the only publication outcome that must be
testable. Every claimed mutation must converge durably after timeout,
cancellation or restart; scheduled queues must be directly readable as logical
publications; scheduled deletion must use the provider's scheduled namespace
and prove absence; and only definite pre-mutation failures may be retried. A
schema-changing tool rollout is complete only after server, exact-main deploy,
action-control publication and real new-chat acceptance all agree.
