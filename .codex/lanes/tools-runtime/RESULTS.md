# tools-runtime lane results

- Lane: `tools-runtime`
- Requirements: R1 runtime half, R4, R6
- Base SHA: `64f75d10f7aff33fa616cee212878bd9d03673b1`
- Head SHA: pending commit (recorded below after commit by commit history)

## Outcome

Implemented the provider-deadline ownership correction for mutating execute and reconcile, a dedicated scheduled-queue read contract/tool, bounded same-operation retry with durable CAS single-flight state, recovered native-intent reconciliation dispatch, and additive bounded status evidence.

No provider/social mutation, publication, deletion, browser automation, or production access was performed in this lane.

## Changed files

- `private_events_mcp/auth_store.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `tests/test_private_events_mcp_social_workspace_contract.py`
- `tests/test_private_events_mcp_social_workspace_runtime.py`
- `.codex/lanes/tools-runtime/RESULTS.md`

## Evidence and design

- Removed the equal runtime `asyncio.wait_for` around mutating adapter `execute` and `reconcile`; adapters now own transport deadlines and durable finalization. Runtime explicitly persists conservative outcome-unknown state on cancellation and propagates cancellation.
- Runtime reconciliation signature-inspects adapters and passes recovered encrypted-preparation-derived native intent as `intent=` plus bounded attempt evidence when supported.
- Added `social_scheduled_items_list` with exact opaque `target_ref`, RFC3339 bounds, exact text SHA-256/media-count filters, limit default 10/hard cap 25, provider `schedule` scope, legacy `publish` scope mapping, logical publication output, and closed redacted projection.
- Added `social_action_retry(operation_ref)` for only terminal `failed` + `retry_safe=true`. The same preparation/logical-action/operation refs are retained. SQLite `BEGIN IMMEDIATE` plus a conditional update prevents concurrent attempts. Attempts are capped at three total and passed to the provider retry seam.
- Additive OAuth SQLite migration adds `logical_action_ref`, `attempt_number`, `retry_in_progress`, and `retry_started_at` without rewriting existing rows.
- Extended the closed status contract with bounded `stage`, mutation-boundary, attempt, scheduled/readback, reconciliation, ambiguity, and deletion-absence evidence. Provider/native keys remain excluded.

## Commands and tests

- Failing-before regression collection: new contract import failed because the scheduled/retry schemas did not exist.
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_private_events_mcp_social_workspace_contract.py tests/test_private_events_mcp_social_workspace_runtime.py` -> `105 passed` after final migration test.
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_private_events_mcp_social_workspace_contract.py tests/test_private_events_mcp_social_workspace_runtime.py tests/test_private_events_mcp_server.py tests/test_private_events_mcp_social_oauth_policy.py` -> `156 passed` before the final additive migration assertion; covered again by full suite.
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_private_events_mcp*.py` -> `526 passed, 3 warnings` (existing aiohttp `NotAppKeyWarning`).
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m compileall -q private_events_mcp private_events_mcp_telegram_adapter.py private_events_mcp_vk_adapter.py private_events_mcp_workspace_providers.py` -> pass.
- `git diff --check` -> pass.

## Risks / integration notes

- Provider lanes must expose the agreed `scheduled_items(*, target_ref, scheduled_from, scheduled_to, text_sha256, media_count, limit)` seam.
- VK retry must accept `retry(intent, *, operation_ref, attempt_number)` and treat attempt number as the distinct provider-ledger attempt while retaining the public logical operation ref.
- Telegram reconcile may accept `reconcile(operation_ref, *, intent=None)`; runtime passes intent only when the signature supports it.
- Public receipt key is `stage`, not `provider_stage`, because provider-prefixed keys are intentionally removed by recursive sanitization.
