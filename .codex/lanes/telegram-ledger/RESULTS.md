# telegram-ledger lane results

## Scope

- Lane: `telegram-ledger`
- Requirements: `R1`, `R2`, `R3`, `R5`
- Base SHA: `64f75d10f7aff33fa616cee212878bd9d03673b1`
- Implementation SHA: `8744e4fd0`
- Branch: `agent/eventsbot-scheduled-readback/telegram-ledger`

## Result

Implemented the provider-owned Telegram mutation deadline/finalization boundary, restart-safe encrypted reconciliation evidence, bounded scheduled/live reconciliation, logical raw scheduled-queue listing, and exact scheduled-album deletion.

Root-cause evidence confirmed in the prior implementation:

1. `execute()` claimed the provider operation before entering `_session()`, but had no `CancelledError` finalizer at that boundary.
2. The durable Telegram operation row stored only the action digest and result. A cancelled process could therefore leave `result_json IS NULL` without target/time/text/media evidence.
3. `reconcile()` only returned a saved result or raised `operation_in_progress`; it did not query Telegram.
4. scheduled item deletion used ordinary `client.delete_messages`, not Telegram's scheduled namespace.

Key changes:

- Mutation marking is durably recorded immediately before every Telegram provider mutation.
- Caller cancellation releases only proven pre-mutation claims; post-boundary cancellation writes terminal `outcome_unknown/provider_cancelled` before propagating cancellation.
- The adapter retains its own transport/session timeout; integration must remove the equal outer mutation `wait_for` (owned by the tools-runtime lane).
- `social_provider_tg_operation` is migration-safely extended with encrypted intent, claim lease/deadline, mutation timestamp, and bounded reconciliation attempt state.
- Durable intent contains only action, opaque target binding, canonical schedule time, normalized text SHA-256, logical media count, and ordered staged-media digests. Caption text, native IDs, paths, URLs, tokens, and provider payloads are not persisted in plaintext.
- `reconcile(operation_ref, intent=...)` can adopt a legacy operation's exact recovered intent after action-digest verification.
- Scheduled reconciliation reads raw `GetScheduledHistoryRequest`, collapses albums, and checks exact UTC time/text fingerprint/media count. Once due, it checks ordinary history separately.
- Reconciliation converges to exact success, bounded pending, terminal ambiguity, proven pre-mutation safe failure, or conservative terminal no-match.
- `scheduled_items(...)` exposes the agreed provider seam and returns logical/redacted albums.
- Scheduled delete resolves all album member IDs, calls `DeleteScheduledMessagesRequest`, and proves absence through a second raw scheduled-history read.

## Changed files

- `private_events_mcp_telegram_adapter.py`
- `private_events_mcp_workspace_providers.py`
- `tests/test_private_events_mcp_telegram_workspace.py`
- `tests/test_private_events_mcp_workspace_providers.py`
- `.codex/lanes/telegram-ledger/RESULTS.md`

## Verification

Commands run from the isolated worktree:

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_workspace_providers.py
# 100 passed in 3.70s

/home/dev/.venvs/events-bot-region-talk/bin/python -m compileall -q \
  private_events_mcp_telegram_adapter.py \
  private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_workspace_providers.py
# passed

uvx ruff check \
  private_events_mcp_telegram_adapter.py \
  private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_workspace_providers.py
# All checks passed

git diff --check
# passed
```

Tests cover encrypted intent/attempt persistence, legacy adoption conflict safety, outcome-unknown-to-terminal ledger convergence, provider cancellation finalization, exact scheduled reconciliation, four-image logical album shape/order, bounded zero-match, ambiguous duplicates, terminal convergence, raw scheduled namespace deletion, and ordinary/scheduled separation.

## Risks / integration notes

- The runtime/schema lane must be merged with this commit. It removes the competing outer mutation deadline, permits bounded reconciliation fields, wires `scheduled_items`, and passes recovered preparation intent to `reconcile(..., intent=...)` for legacy rows.
- A legacy row with no mutation marker is treated conservatively; absence of the newly introduced marker is not accepted as proof that an old provider mutation did not occur.
- No Telegram provider mutation, publication, cancellation, browser automation, deployment, or production readback was performed in this lane.
- Full Private Events MCP suite and cross-provider integration checks remain owned by the integrator after all lanes merge.
