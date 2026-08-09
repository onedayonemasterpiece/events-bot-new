# INC-2026-08-09 Private MCP missing bearer returned HTTP 200

Status: open
Severity: sev3
Service: Private Events MCP protected-resource transport
Opened: 2026-08-09
Closed: —
Owners: events-bot production / Private Events MCP
Related incidents: `INC-2026-08-08-private-mcp-oauth-csp-redirect`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The first exact-main production acceptance run for the universal social MCP
found that an unauthenticated JSON-RPC `tools/call` was denied inside the MCP
result but returned HTTP 200 without `WWW-Authenticate`. The protected tool did
not execute and disclosed no evidence, but the OAuth protected-resource
transport contract required HTTP 401 with the endpoint-specific RFC 9728
challenge.

## User / Business Impact

- OAuth-capable clients could receive an in-band MCP authentication error
  instead of starting authorization from the HTTP challenge.
- No event, incident, Telegram or VK data was returned and no provider call was
  made.
- Existing authenticated ChatGPT/Codex calls, `/healthz`, webhook, scheduler and
  event SQLite remained healthy.

## Detection

- The post-deploy acceptance probe called `operations_snapshot` without an
  Authorization header and observed HTTP 200 plus `isError=true`.
- A local aiohttp regression assertion reproduced the same status reliably.
- The existing test covered an invalid bearer but not a completely missing
  bearer on `tools/call`.

## Timeline

- 2026-08-08T23:59Z — merged main `68b38dba…` deployed as Fly release v1937
  with universal social switches off.
- 2026-08-09T00:02Z — ChatGPT and Codex authenticated evidence smokes passed;
  the event DB digest was unchanged and provider calls remained zero.
- 2026-08-09T00:04Z — unauthenticated protected call returned HTTP 200 and the
  release gate was stopped before social activation.
- 2026-08-09T00:05Z — a failing aiohttp regression test isolated the missing
  server transport boundary.

## Root Cause

1. `PrivateEventsMCPServer.handle_mcp_post` returned HTTP 401 for malformed or
   expired bearer tokens, but represented an absent token as `identity=None`.
2. The request then reached `MCPProtocol.dispatch`, which intentionally emits
   an MCP-native authentication result for an anonymous tool call.
3. The server serialized that result with its normal HTTP 200 response instead
   of enforcing the protected-resource HTTP challenge at the transport layer.

## Contributing Factors

- Public `initialize` and `tools/list` are intentionally available for MCP/OAuth
  discovery, so the endpoint cannot reject every request lacking a bearer
  before parsing the JSON-RPC method.
- Production smoke covered valid and invalid bearer flows, but not the exact
  missing-bearer protected call.

## Automation Contract

### Treat as regression guard when

- changing MCP HTTP transport authentication, endpoint-specific resources,
  public discovery, OAuth challenges or JSON-RPC dispatch ordering.

### Affected surfaces

- `private_events_mcp/server.py::handle_mcp_post`;
- ChatGPT and Codex MCP resource paths and RFC 9728 metadata URLs;
- production OAuth/MCP negative smoke.

### Mandatory checks before closure or deploy

- missing bearer on `tools/call` returns HTTP 401 and the exact endpoint-specific
  `WWW-Authenticate` resource-metadata URL for both clients;
- invalid bearer still returns HTTP 401;
- anonymous `initialize` and public catalogue discovery remain available;
- authenticated ChatGPT and Codex OAuth/PKCE, refresh rotation and all seven
  evidence tools pass;
- JSON-RPC batch and unsupported protocol version still return HTTP 400;
- full private MCP suite, compileall, Ruff and `git diff --check` pass;
- exact-main SHA, `/healthz`, `quick_check`, DB unchanged, provider_calls=0,
  webhook/scheduler and secret-redaction checks remain green after deploy.

### Required evidence

- failing-before/passing-after regression test;
- PR/head, independent review and green CI;
- merged main SHA, Fly release, in-container SHA and sanitized production probe;
- confirmation that social providers remained disabled until the fix passed.

## Immediate Mitigation

Universal social provider switches remained off. Existing authenticated
evidence access stayed available while the narrow transport hotfix was prepared.

## Corrective Actions

- Enforce HTTP 401 at the server boundary after parsing a missing-bearer
  `tools/call`, using the already selected endpoint-specific metadata URL.
- Add ChatGPT and Codex regression cases without changing public discovery or
  authenticated protocol behavior.

## Follow-up Actions

- [ ] Complete exact-main production negative smoke for both resources.
- [ ] Continue Telegram/VK activation only after this incident is closed.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: `scripts/deploy_fly_main.sh`
- regression checks: pending full suite and independent review
- post-deploy verification: pending

## Prevention

The production acceptance matrix now treats a completely absent bearer as a
separate transport case from an invalid bearer and verifies both resource URLs.
