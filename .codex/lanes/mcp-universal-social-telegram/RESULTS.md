# Telegram Social Workspace lane results

## Lane contract

- Lane ID: `mcp-universal-social-telegram`
- Requirement IDs: Telegram provider adapter portion of the universal Social
  Workspace contract (Saved/exact resolution; bounded reads/search/editorial
  sampling; similar/stories/statistics; rich media mutations; capability,
  secrecy, cooldown, fencing, and uncertainty safety).
- Base SHA: `dd388c4b71d7bb86dbac94e4dbe19347a5ca7e2b`
- Initial implementation SHA: `4afab6aa6334b358ba2933a31c4c7a31405af33b`
- Primary review-remediation SHA: `f58862d88b73bd20906df69774223270ecacc60c`
- Final review-probe SHA: `71fb4adcb6ba0a8a5455c1f7b053da2b5ad6c3de`
- Operation-claim/permissions remediation head SHA (before this results-only
  commit): `98e9374c102fb31f0224f98ccdb5d7c741874844`
- Branch: `agent/mcp-universal-social/telegram`
- Production calls/deploy: not performed.

## Delivered

- Added `TelegramWorkspaceAdapter` with exactly five public async operations:
  `capabilities(target_ref)`, `resolve(request)`, `read(request)`, and
  `execute(intent, operation_ref=...)`, plus `reconcile(operation_ref)`. The
  mandatory caller-issued operation ref is the exact ledger/receipt/
  reconciliation key, including timeout/unknown outcomes. It is validated
  against the exact core `op_` grammar and is never minted or defaulted by the
  adapter. There is no public provider request,
  method, constructor, kwargs, or raw TL escape hatch.
- Added exact Saved Messages/self resolution and exact username, canonical
  `t.me` profile, and provider-ID resolution to user/channel/group bindings.
  Native entities and IDs remain private behind injected opaque target/item/
  asset/cursor/operation refs.
- Added bounded dialog listing/keyword target search, exact target history,
  target and global keyword search, exact item reads, comments/replies,
  aggregate reactions, stories, audience/statistic aggregates, and fixed
  channel recommendations. Pagination cursors are opaque and bound to the
  platform, operation, resolved target/item, sample, query, date range,
  purpose, access class, limits, and expected kinds; cross-context reuse fails.
- Added single-target, ephemeral/no-index editorial channel/group sampling with
  title/about/description/basic metrics, date filtering, pages no larger than
  25, and a cumulative maximum of 100. The runtime lane remains the sole owner
  of the durable server-minted `sample_ref` and cumulative state and passes a
  copied request containing that ref to this adapter.
- Adapter-local checks independently enforce `page_size <= 25`,
  `total_limit <= 100`, a valid server-issued sample ref, and coherent dates,
  even when an internal caller bypasses the provider-neutral validator.
- Added fixed mutation translations for Saved/user DM, channel/group publish,
  reply/comment, edit, delete, forward, reaction, schedule, and story. Exact
  capability preflight intersects live provider rights with stored policy.
  Stories require one staged asset and explicit stored privacy.
- A no-rights broadcast advertises no mutations. Item actions recheck the live
  source rights, forwarding rechecks both source and destination, and execution
  carries detached source/destination provider-peer snapshots from preflight
  through the provider call. Permission probes receive a separate detached
  copy, so both ref-store swaps and in-place permission-probe mutation cannot
  change the peer used for edit/delete/comment/reaction/forward.
- Capability derivation uses the real Telethon 1.44
  `ParticipantPermissions` surface (`is_creator`, `is_admin`,
  `has_default_permissions`, membership state, and post/edit/delete rights)
  plus actual participant `admin_rights.post_stories`; it does not rely on the
  nonexistent `send_messages` or `post_stories` permission properties. Group
  publishing follows membership/default-ban state and fails closed.
- Added a mandatory atomic operation-ledger claim contract bound to the full
  `compute_action_digest(intent)`. Exact completed replays return the canonical
  stored result without a provider call, a changed intent conflicts before the
  provider, an in-progress concurrent claim is denied, and completion cannot
  replace a different digest. Retry-safe pre-provider failures release only
  the matching claim; uncertain post-mutation outcomes are durably reconciled.
- Added structured rich entity compilation with deterministic codepoint to
  UTF-16 offsets, fixed link/mention/custom-emoji translation, and media only
  from opaque staged asset refs. No path, arbitrary URL fetch, base64, session,
  token, or native access material is accepted.
- Added read-after-write verification for exact DMs, sanitized provider error
  boundaries, persistent FloodWait/cooldown hooks, a process-local serializer,
  mandatory cross-process lease/fencing hooks, and outcome-unknown/no-retry
  receipts after a non-idempotent timeout or lost fence.
- Telethon imports remain lazy. The default fixed-type factory rejects
  unsupported Telethon versions or missing required requests, entity types, and
  client parameters before advertising capabilities.

## Test coverage

The fake-only focused suite covers:

- core-compatible Saved/self and exact-user resolution;
- adapter-only channel/group transport classification, explicitly separated
  from the still-blocked provider-neutral validator path;
- signed basic-group provider-ID classification at the adapter boundary;
- serialized exact-user and Saved reminder sends with verified read-back;
- four 25-item editorial pages reaching exactly 100 with description/metrics;
- dialog list/search, target history, target/global search and adversarial
  cross-platform/operation/target/query/sample/date cursor rejection;
- similar channels, comments, reactions, stories, audience/statistics;
- UTF-16 entity ranges, custom emoji, and staged media compilation;
- every closed mutation family;
- exact capability denial before provider mutation, including a no-rights
  broadcast and every item mutation family;
- mutable-ref TOCTOU swaps for edit/delete/comment/reaction and forwarding,
  plus in-place mutation of both the permission-probe and store-owned entity;
- required/invalid caller-issued operation refs, exact replay, changed-intent
  conflict, two-adapter concurrent same-key exclusion, and success/timeout
  reconciliation;
- realistic and installed Telethon permission surfaces, ordinary group member
  send rights, default-ban denial, and no-rights broadcast denial;
- provider/native-ID/secret redaction and closed public surface;
- non-idempotent timeout, FloodWait persistence, and no blind retry;
- actual `asyncio.wait_for` expiry and post-mutation lost-fence uncertainty;
- core capability, editorial response, and action status validators;
- installed Telethon compatibility guard.

## Validation commands

```text
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_telegram_workspace.py
# 44 passed

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_telegram_workspace.py
# 74 passed (final combined rerun)

/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q \
  private_events_mcp_telegram_adapter.py \
  tests/test_private_events_mcp_telegram_workspace.py
# passed

git diff --check
# passed
```

One initial combined-test command used the nonexistent path
`tests/test_private_events_mcp_social_workspace.py` and exited 4 without
running tests. The corrected canonical contract path above passed 74 tests.

## Risks and integration notes

- This adapter intentionally does not read credentials or create a logged-in
  Telethon client. The integration-owned client factory must use only
  `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP`; it must never borrow E2E/S22 sessions.
- The injected opaque-ref store and governor are security-critical integration
  dependencies: refs/cursors/operations must be durable and resource-bound;
  operation claim/release/complete must be atomic compare-and-set operations
  bound to the full action digest; lease fencing and cooldowns must be
  cross-process and persistent.
- **Open integration blocker:** the current provider-neutral
  `validate_read_request()` requires username/profile-link/provider-ID exact
  resolution to expect only `user`, so it rejects exact channel/group resolver
  requests before this adapter runs. The focused suite records that rejection
  and does not present direct adapter channel/group classification as integrated
  acceptance. The integration/core owner must reconcile that contract before
  claiming channel/group exact resolution end to end.
- Provider/legal consent, durable approvals, safety/audit hooks, catalogs,
  OAuth scopes, production smoke tests, and deployment are outside this lane.
- No canonical docs or `CHANGELOG.md` were edited because the lane map forbade
  all shared/integration files; the integration/docs owners must synchronize
  those required repository surfaces.
- No live Telegram call, credential access, production write, or deploy was
  performed.

## Changed files

- `private_events_mcp_telegram_adapter.py`
- `tests/test_private_events_mcp_telegram_workspace.py`
- `.codex/lanes/mcp-universal-social-telegram/RESULTS.md`
