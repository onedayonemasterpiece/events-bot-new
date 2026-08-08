# Lane social-core Results

## Status

committed

## Requirement IDs

- R2
- R3
- R4
- R6

## Branch

`agent/mcp-multiclient/social-core`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/mcp-social-core`

## Base SHA

`eb9cf0c9c3412059d5cdd7568c4df4d6196d0727`

## Head SHA

Implementation commit: `d57e892b8e55b3218dfb4ea50c12a00cb36a4464`

## Files changed

- `.env.example`
- `CHANGELOG.md`
- `docs/operations/private-events-mcp.md`
- `private_events_mcp/__init__.py`
- `private_events_mcp/access_policy.py`
- `private_events_mcp/auth_store.py`
- `private_events_mcp/config.py`
- `private_events_mcp/integration.py`
- `private_events_mcp/protocol.py`
- `private_events_mcp/repository.py`
- `private_events_mcp/server.py`
- `private_events_mcp/social.py`
- `private_events_mcp/tool_catalog.py`
- `tests/test_private_events_mcp_server.py`
- `tests/test_private_events_mcp_social.py`
- `.codex/lanes/social-core/RESULTS.md` (evidence-only follow-up commit)

## Commands run

- `python3 -m compileall -q private_events_mcp`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_protocol.py tests/test_private_events_mcp_server.py tests/test_private_events_mcp_oauth_store.py tests/test_private_events_mcp_config.py`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_social.py`
- `python3 -m compileall -q private_events_mcp tests`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_*.py`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_*.py -k 'not test_codex_public_client_real_oauth_and_mcp_contract and not test_public_client_rejects_secret_downgrade_and_cross_client_code'`
- `git diff --check`

## Tests / verification

- Compileall: PASS.
- New social-core suite: **14 passed**.
- Private MCP suite excluding the two known OAuth-lane dependency tests:
  **41 passed, 2 deselected**.
- Full private MCP suite before OAuth integration: **41 passed, 2 failed**.
  The exact expected dependency failures are:
  - `test_codex_public_client_real_oauth_and_mcp_contract`
  - `test_public_client_rejects_secret_downgrade_and_cross_client_code`
- Both failures occur because this lane intentionally leaves `oauth.py` owned
  by the parallel OAuth lane; the base OAuth registry still accepts Codex on
  the old ChatGPT resource, while these integration-ready tests require the new
  exact Codex resource. OAuth commit `8ee16f60f70b0f030166d8340de08b1fe431b85d`
  provides the required registry/resource APIs and must be integrated first.
- Static provider safety is included in the passing private MCP suite: core has
  no provider SDK/network/process imports.
- Fake-adapter tests cover endpoint/client/scope/tool isolation, exact Codex
  seven-tool catalog, alias deny-all behavior, read redaction/trust markers,
  ticket mutation/replay/expiry, durable idempotency, timeout outcome-unknown,
  daily attempt budget across refreshed access-token identity, append-only
  redacted audit, absent unsupported provider tools, and provider call counts.

## Risks

- Integrator must apply the OAuth lane first and reconcile the server's thin
  endpoint verification/metadata wrappers with the OAuth lane's parameterized
  APIs, then run the complete suite without deselection.
- This core deliberately ships no real Telegram/VK transport. Social tools are
  added only when adapters are explicitly injected; an empty target policy
  remains deny-all.
- A timed-out/cancelled provider attempt has an unknowable outcome. The ticket
  is consumed before the call, the idempotency key remains permanently reserved,
  and audit records `outcome_unknown`; automatic replay is intentionally denied.

## Merge notes

- Apply OAuth lane commit `8ee16f60f70b0f030166d8340de08b1fe431b85d`
  before this lane.
- Reuse `private_events_mcp.access_policy` as the shared scope constants or
  reconcile duplicate constants without broadening Codex's maximum scopes.
- Provider lanes should implement the exported `SocialAdapter` contract only;
  core receives `ResolvedTarget` and never accepts caller-supplied raw IDs,
  URLs, or method names.
