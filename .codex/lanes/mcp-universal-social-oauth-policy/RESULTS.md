# Lane mcp-universal-social-oauth-policy Results

## Status

Committed.

## Requirement IDs

- R07
- R08

## Scope

- ChatGPT retains the three evidence-read default scopes and can explicitly
  request every exact granular scope exported by `social_workspace.py`.
- Codex retains exactly the three evidence-read scopes plus `offline_access`
  as its maximum and receives no legacy or granular social scope.
- Legacy `telegram/vk:read/publish` scopes remain accepted for the existing
  legacy tools but are not aliases for private reads, DMs, edit/delete, or
  story capabilities.
- Authorization, bearer validation, and refresh behavior fail closed for
  unknown, cross-client, cross-resource, and broadened scope attempts.
- Universal-social master/provider/capability switches are strict opt-ins,
  default off, and ignored safely while the entire MCP is disabled.
- No provider, catalog, runtime adapter, live-call, deploy, documentation, or
  changelog wiring was performed in this lane.

## Base SHA

`1bd813120de2c2904adab045585e6d3c71cb2894`

## Head SHA

Implementation head before this receipt commit:
`bbe205c51359c075633cfc6d0175e7de65344b35`.

The receipt itself is the only commit after that implementation SHA; the final
branch head is reported to the integrator after committing this file.

## Changed files

- `private_events_mcp/access_policy.py`
- `private_events_mcp/config.py`
- `private_events_mcp/oauth.py`
- `tests/test_private_events_mcp_social_oauth_policy.py`
- `.codex/lanes/mcp-universal-social-oauth-policy/RESULTS.md`

## Commands run

- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m compileall -q private_events_mcp/access_policy.py private_events_mcp/config.py private_events_mcp/oauth.py tests/test_private_events_mcp_social_oauth_policy.py`
- `PYTHONPATH=. /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_social_oauth_policy.py`
- `PYTHONPATH=. /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_config.py tests/test_private_events_mcp_oauth_store.py tests/test_private_events_mcp_server.py tests/test_private_events_mcp_social.py tests/test_private_events_mcp_social_workspace_contract.py tests/test_private_events_mcp_social_oauth_policy.py`
- `PYTHONPATH=. /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_scripts.py tests/test_private_events_mcp_protocol.py tests/test_private_events_mcp_static_safety.py`
- `PYTHONPATH=. /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_*.py`
- `git diff --check`
- `git diff --cached --check`

## Tests and evidence

- New focused social OAuth policy suite: `8 passed`.
- Focused OAuth/config/server/social/contract set: `77 passed`.
- Script/protocol/static-safety regression set: `9 passed`.
- Complete private Events MCP test glob: `104 passed`, with three pre-existing
  aiohttp `NotAppKeyWarning` warnings in the disabled provider-adapter test.
- Compileall passed.
- Diff checks passed.
- The first focused run exposed two test-only wording assertions
  (`external approval` versus the intentionally more explicit
  `external action approval`); assertions were corrected and all subsequent
  runs passed.

## Risks and integration notes

- The new kill-switch fields are policy/config inputs only. The integration
  owner must enforce the master, provider, and capability gates when wiring the
  new runtime catalog and adapters; all defaults are false.
- Legacy coarse scopes intentionally remain in ChatGPT's maximum set to avoid
  breaking the existing four legacy social tools. New workspace tools must
  continue checking their exact granular scopes and must never translate a
  coarse legacy grant into new powers.
- Mutation scopes are explicit opt-ins, absent from the default grant, and the
  authorization page states that external action approval plus prepare/commit
  is required. Runtime approval enforcement remains in the provider-neutral
  workspace contract/integration, not in OAuth scope issuance.
- No external services, secrets, production data, provider calls, or deploys
  were used.
