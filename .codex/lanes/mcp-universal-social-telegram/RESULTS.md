# Telegram Social Workspace lane results

## Lane contract

- Lane ID: `mcp-universal-social-telegram`
- Requirement IDs: Telegram provider adapter portion of the universal Social
  Workspace contract (Saved/exact resolution; bounded reads/search/editorial
  sampling; similar/stories/statistics; rich media mutations; capability,
  secrecy, cooldown, fencing, and uncertainty safety).
- Base SHA: `dd388c4b71d7bb86dbac94e4dbe19347a5ca7e2b`
- Validated implementation SHA: `4afab6aa6334b358ba2933a31c4c7a31405af33b`
- Branch: `agent/mcp-universal-social/telegram`
- Production calls/deploy: not performed.

## Delivered

- Added `TelegramWorkspaceAdapter` with exactly four public async operations:
  `capabilities(target_ref)`, `resolve(request)`, `read(request)`, and
  `execute(intent)`. There is no public provider request, method, constructor,
  kwargs, or raw TL escape hatch.
- Added exact Saved Messages/self resolution and exact username, canonical
  `t.me` profile, and provider-ID resolution to user/channel/group bindings.
  Native entities and IDs remain private behind injected opaque target/item/
  asset/cursor/operation refs.
- Added bounded dialog listing/keyword target search, exact target history,
  target and global keyword search, exact item reads, comments/replies,
  aggregate reactions, stories, audience/statistic aggregates, and fixed
  channel recommendations. Pagination cursors are opaque and request-bound.
- Added single-target, ephemeral/no-index editorial channel/group sampling with
  title/about/description/basic metrics, date filtering, pages no larger than
  25, and a cumulative maximum of 100. The runtime lane remains the sole owner
  of the durable server-minted `sample_ref` and cumulative state and passes a
  copied request containing that ref to this adapter.
- Added fixed mutation translations for Saved/user DM, channel/group publish,
  reply/comment, edit, delete, forward, reaction, schedule, and story. Exact
  capability preflight intersects live provider rights with stored policy.
  Stories require one staged asset and explicit stored privacy.
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

- Saved/self plus user/channel/group exact resolution;
- serialized exact-user reminder send and verified read-back;
- four 25-item editorial pages reaching exactly 100 with description/metrics;
- dialog list/search, target history, target/global search and opaque cursors;
- similar channels, comments, reactions, stories, audience/statistics;
- UTF-16 entity ranges, custom emoji, and staged media compilation;
- every closed mutation family;
- exact capability denial before provider mutation;
- provider/native-ID/secret redaction and closed public surface;
- non-idempotent timeout, FloodWait persistence, and no blind retry;
- core capability, editorial response, and action status validators;
- installed Telethon compatibility guard.

## Validation commands

```text
PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_telegram_workspace.py
# 21 passed

PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_telegram_workspace.py
# 51 passed (final combined rerun)

/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q \
  private_events_mcp_telegram_adapter.py \
  tests/test_private_events_mcp_telegram_workspace.py
# passed

git diff --check
git diff --cached --check
# passed
```

## Risks and integration notes

- This adapter intentionally does not read credentials or create a logged-in
  Telethon client. The integration-owned client factory must use only
  `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP`; it must never borrow E2E/S22 sessions.
- The injected opaque-ref store and governor are security-critical integration
  dependencies: refs/cursors/operations must be durable and resource-bound;
  lease fencing and cooldowns must be cross-process and persistent.
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
