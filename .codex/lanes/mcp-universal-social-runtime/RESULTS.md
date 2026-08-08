# Lane results: mcp-universal-social-runtime

## Scope

- Lane: `mcp-universal-social-runtime`
- Base SHA: `b74d8cebf8451c3a5870ae9995e5235bf19bd901`
- Implementation head SHA: `3efdc7f74abe143e00fa9b712a68aa158fb43c3f`
- Compatibility commit SHA: `33af64273c3c6730ada062e3becc8878e3414dd3`
- Security-review remediation: pending commit at the time this receipt was updated.

## Result

Implemented the durable, provider-neutral private Social Workspace runtime and its non-public ChatGPT ToolSpecs. The runtime uses only the OAuth/auth SQLite path, authenticated principal/resource bindings, encrypted native references, server-minted editorial cursors, durable prepare/approval/operation state, atomic one-use approval consumption, layered budgets, flood/circuit state, recursive provider-output redaction, bounded responses, and append-only fingerprint-only audit records. No event database access or provider/live call was added.

The store also now accepts optional `allowed_scopes` on authorization-code consumption and refresh rotation, rejecting stale/over-broad persisted grants before their transactional consume/revoke step.

## Evidence

Commands run from the lane worktree:

- `uv run --with-requirements requirements.txt python -m compileall -q private_events_mcp/auth_store.py private_events_mcp/social_workspace_runtime.py private_events_mcp/social_workspace_tools.py tests/test_private_events_mcp_social_workspace_runtime.py` — passed.
- `uv run --with-requirements requirements.txt pytest -q tests/test_private_events_mcp_social_workspace_runtime.py tests/test_private_events_mcp_oauth_store.py tests/test_private_events_mcp_social.py tests/test_private_events_mcp_social_workspace_contract.py` — `56 passed`.
- `uv run --with-requirements requirements.txt pytest -q tests/test_private_events_mcp_*.py` — `106 passed`, 3 pre-existing aiohttp `NotAppKeyWarning` warnings.
- `uvx ruff check private_events_mcp/auth_store.py private_events_mcp/social_workspace_runtime.py private_events_mcp/social_workspace_tools.py tests/test_private_events_mcp_social_workspace_runtime.py` — passed.
- Draft 2020-12 validation of every generated Social Workspace ToolSpec input/output schema — passed.
- `git diff --check` — passed.

Focused fake-adapter coverage includes self and exact-person resolution; exact DM prepare -> external operator approval -> commit with read-after-write; approval replay and client/resource/idempotency mutation denial; four 25-item editorial pages with translated server cursors and cumulative limit 100; layered budget denial plus durable audit; encrypted provider reference non-disclosure; timeout `outcome_unknown`/`retry_safe=false` and reconciliation; private/non-cacheable tool policy and feature hiding; isolated auth/event DB behavior; and transactional stale OAuth scope rejection.

After independent review requested changes, the lane was hardened further:

- the server-minted editorial `sample_ref` is now passed to the adapter on the first and every continuation page;
- all ordinary read results are projected through their exact closed output contract, so unknown/native provider fields never cross the MCP boundary;
- provider exceptions and tool errors are mapped to stable messages with no provider method, path, token or payload text;
- disabled providers are enforced inside every handler, not merely omitted from descriptor enums;
- denial audit dimensions are allowlisted and approval capabilities are hash-only at rest;
- one exact runtime `operation_ref` is passed to execute and reconcile;
- a provider success followed by response/egress rejection is persisted as `outcome_unknown/response_withheld`, never a false failure;
- budget layers are independently configurable and target/circuit buckets use the bound native-target fingerprint rather than a remintable public reference.

Post-remediation evidence:

- focused runtime suite: `17 passed`;
- full `tests/test_private_events_mcp_*.py`: `113 passed`, 3 pre-existing aiohttp warnings;
- compileall and `git diff --check`: passed.

## Changed files

- `private_events_mcp/auth_store.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `tests/test_private_events_mcp_social_workspace_runtime.py`
- `.codex/lanes/mcp-universal-social-runtime/RESULTS.md`

## Risks / integration notes

- This lane intentionally does not edit server/config/docs/changelog or register the tools. Integration must supply a durable encryption key, the approved adapter map, and the ChatGPT-only capability/feature policy; it must not add these tools to the Codex protocol.
- `approve_preparation` is a server approval-page helper only. Its required authenticated operator principal and fresh nonce must come from external server state, never MCP/model arguments.
- Provider adapters were exercised only with fakes; no live provider calls or deploy were performed.
