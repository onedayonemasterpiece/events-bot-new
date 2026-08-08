# OAuth implementation lane results

- Lane: `oauth_implementation` (R1/R5)
- Branch: `agent/mcp-multiclient/oauth`
- Base SHA: `ef2e1eb28cf1d1d0899a167f7ba9c0ce6b84826e`
- Implementation head SHA: `ef375be01b8fcddac71ed8d7fbfee37d926f72f6`
- Status: complete

## Delivered

- Preserved the existing confidential ChatGPT client ID, secret authentication,
  and callback allowlist.
- Added required static `PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID` registration
  with token endpoint auth method `none` and no Codex client secret.
- Enforced the literal Codex callback contract
  `http://127.0.0.1:<explicit-port>/callback/<opaque>` with a canonical IPv4
  authority, one URL-safe opaque segment, and no userinfo/query/fragment.
- Kept PKCE S256 mandatory and bound codes/refresh/access tokens to the exact
  registered client and resource; authorization codes retain exact redirect and
  verifier binding. Bearer validation accepts only the two static registry IDs.
- Kept dynamic client registration absent and preserved the seven-tool catalog,
  strict disabled no-op, rotating refresh tokens, principal-stable rate buckets,
  and access-log redaction.
- Updated generator, dual-mode production smoke, canonical runbook, env example,
  changelog, config tests, redirect negatives, downgrade/cross-client negatives,
  and a full public Codex OAuth-to-MCP contract test.

## Evidence and commands

- `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q private_events_mcp tests scripts/generate_private_events_mcp_credentials.py scripts/smoke_private_events_mcp.py` — PASS.
- `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/pytest -q tests/test_private_events_mcp_*.py` — PASS, `29 passed in 1.31s`.
- Generated credentials into an isolated `/tmp` directory and asserted Codex
  `none` auth/no secret, ChatGPT secret preservation, deploy env mapping, and
  mode `0600` on all generated files — PASS (`generator_contract=PASS`).
- `git diff --check` — PASS.
- Initial generic `python`/system `python3 -m pytest` attempts could not run
  because `python` was absent and system Python lacked pytest; the documented
  repository venv above was then used successfully.

## Risks / gaps

- No live production OAuth smoke was run because this lane explicitly excludes
  push/deploy and has no approved production activation. The production smoke
  now supports both default ChatGPT and `--client codex` modes.
- The enabled configuration intentionally fails closed until the new distinct
  Codex client ID is provisioned; disabled mode remains inert even with stale or
  malformed MCP-only values.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `docs/operations/private-events-mcp.md`
- `private_events_mcp/config.py`
- `private_events_mcp/oauth.py`
- `scripts/generate_private_events_mcp_credentials.py`
- `scripts/smoke_private_events_mcp.py`
- `tests/conftest.py`
- `tests/test_private_events_mcp_config.py`
- `tests/test_private_events_mcp_oauth_store.py`
- `tests/test_private_events_mcp_server.py`
- `.codex/lanes/oauth_implementation/RESULTS.md`
