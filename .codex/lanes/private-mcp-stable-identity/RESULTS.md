# Private MCP stable identity lane results

## Lane contract

- Lane ID: `private-mcp-stable-identity`
- Requirements: `R01` explicit stable-identity installation and `R02` narrow,
  secure bootstrap-only rotation.
- Exact base SHA: `0e1dad424811c2c2eedda2707b92263f8df1551b`
- Implementation head before this results-only commit:
  `7d177a938e537b58ae0a4c6dc6a6b82144a1792f`
- Branch: `agent/private-mcp-stable-identity`
- Production calls, secrets, push and deploy: not performed.

## Delivered

- Credential generation now requires exactly one explicit identity mode:
  `--new-install` or
  `--rotate-bootstrap-only <existing-full-credentials.json>`.
  `--new-install` requires `--base-url`; bootstrap-only rotation rejects
  `--base-url` and `--enable-chatgpt-social` so it cannot silently change the
  installed contract.
- Bootstrap-only rotation loads the complete prior JSON, validates the full
  deploy/ChatGPT/Codex structure and internal endpoint/client/operator/social
  consistency, then changes exactly:
  `deploy.PRIVATE_EVENTS_MCP_OPERATOR_TOKEN`,
  `chatgpt.bootstrap_operator_token`, and
  `codex.bootstrap_operator_token`. The fresh value is identical in all three
  locations. Every URL, private path, OAuth client value, signing/state value,
  scope, independent social-approval value and unknown forward-compatible
  field is preserved.
- Input JSON is bounded to 1 MiB, must be a regular non-symlink file, rejects
  duplicate keys, missing required deploy/signing/social fields, newline/env
  injection, inconsistent duplicates, invalid endpoints, and source/output
  overlap before output creation.
- Output must be a fresh path under an existing non-symlink parent. The new
  directory is opened no-follow and normalized to `0700`; every artifact is
  created with `O_EXCL`, no-follow and exact `0600` mode. Existing files and
  symlink targets are never overwritten.
- Receipts contain only mode, artifact paths, public origin, redacted MCP path
  and endpoint fingerprints. Tests prove that old/new bootstrap values, private
  endpoints/path, client secret, signing key and social-approval token never
  reach stdout or sanitized failure stderr.
- Canonical operations documentation now distinguishes initial/full identity
  replacement from routine bootstrap retirement and gives exact commands for
  both modes.
- Incident
  `INC-2026-08-08-private-mcp-oauth-csp-redirect` now records stable-identity
  rotation as a mandatory regression contract and preserves the outstanding
  real-browser/deploy acceptance actions.

## Tests and evidence

Focused script suite:

```text
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_scripts.py
# 11 passed
```

Full incident-mandated Private MCP regression suite:

```text
timeout 300 /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_*.py
# 213 passed, 3 existing aiohttp NotAppKeyWarning warnings
```

Compilation and diff gates:

```text
/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q \
  private_events_mcp private_events_mcp_provider_adapters.py \
  private_events_mcp_telegram_adapter.py private_events_mcp_vk_adapter.py \
  private_events_mcp_workspace_providers.py \
  scripts/generate_private_events_mcp_credentials.py \
  tests/test_private_events_mcp_*.py main_part2.py
git diff --check
# passed
```

The host filesystem initially had about 606 MiB free and a normal full
worktree checkout failed while materializing unrelated media. The lane used a
fresh non-cone sparse worktree at the exact requested base, then added only
read-only dependencies needed for the full regression run. Several early full
suite attempts stopped on sparse-checkout-only missing modules; after adding
the required tracked dependencies, the same full command passed 213 tests.

## Incident control and remaining release work

- Incident ID: `INC-2026-08-08-private-mcp-oauth-csp-redirect`.
- Current status: open; this lane supplies local prevention/regression only.
- Affected surfaces in this lane: credential generation and documented
  post-browser bootstrap retirement. OAuth/CSP/server code was forbidden and
  unchanged.
- Completed mandatory local evidence: explicit stable mode, exact three-field
  rotation, full-value preservation, incomplete/symlink/overlap rejection,
  stdout redaction, file modes, full Private MCP tests, compileall and diff.
- Still required outside this lane before incident closure: merge to
  `origin/main`, exact-main Fly release/in-container SHA, real Chrome callback,
  ChatGPT/Codex OAuth+PKCE acceptance, sanitized OAuth audit, health/DB/log
  evidence, and the operator's post-connection bootstrap-only rotation.
- The incident-required full identity/private-path replacement remains a
  deliberate `--new-install` operation when the path itself is compromised;
  `--rotate-bootstrap-only` must not be used as a substitute for that case.

## Scope and risks

- The lane intentionally did not edit `CHANGELOG.md`, config, OAuth, server,
  social workspace or integration code because those files were forbidden.
- The rotation source remains a high-value credential artifact and must stay
  owner-only outside Git; this script validates structure and symlink safety but
  does not deploy or revoke credentials.

## Changed files

- `scripts/generate_private_events_mcp_credentials.py`
- `tests/test_private_events_mcp_scripts.py`
- `docs/operations/private-events-mcp.md`
- `docs/reports/incidents/INC-2026-08-08-private-mcp-oauth-csp-redirect.md`
- `.codex/lanes/private-mcp-stable-identity/RESULTS.md`
