# INC-2026-08-09 Private MCP missing bearer returned HTTP 200

Status: closed
Severity: sev3
Service: Private Events MCP protected-resource transport
Opened: 2026-08-09
Closed: 2026-08-09
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
- 2026-08-09T00:11Z — reviewed hotfix PR #412 merged as `6836c1c0…`; the
  missing-bearer regression and complete Private Events MCP suite passed.
- 2026-08-09T00:20Z — fresh credentials were installed and the authorized
  Telegram Saved Messages canary completed prepare, browser approval, commit
  and read-after-write verification for the exact text `Привет мир`.
- 2026-08-09T00:35Z — the hotfix remained reachable from current
  `origin/main` `2992573d…`, which was deployed as Fly release v1941. Both
  ChatGPT and Codex protected resources returned the required HTTP 401
  challenge, and the post-deploy `/start` UI smoke completed in 587 ms.

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

- [x] Complete exact-main production negative smoke for both resources.
- [x] Continue Telegram activation only after this incident is closed; VK and
  media/story capabilities remain disabled pending their separate credential
  and upload-boundary gates.

## Release And Closure Evidence

- hotfix PR: [#412](https://github.com/onedayonemasterpiece/events-bot-new/pull/412),
  independently approved exact head `7d9fb87ef3d35e2e52661b5da28e0257b6352607`;
  merged hotfix commit `6836c1c000105fb7cce956aa673416b99368f755`.
- final deployed `origin/main`: `2992573dd40b0a7256a7f306fab6a127b8169d24`,
  which contains the merged hotfix; clean exact-main deploy path
  `scripts/deploy_fly_main.sh`.
- Fly release: v1941, release id `l8w0PB4QQoOwMT9gb5yLnM9ML`, image
  `deployment-01KZHZ0A20JA6M19A4WGF5RQF6`; in-container SHA exactly matched
  the final deployed main SHA.
- validation: compileall and `git diff --check` passed; the complete targeted
  suite reported `207 passed` with only three existing aiohttp AppKey warnings;
  the PR's required GitHub checks were green.
- transport regression: missing bearer on `tools/call` returned HTTP 401,
  `authentication_required`, and an endpoint-specific Bearer
  `resource_metadata` challenge for both ChatGPT and Codex. Anonymous
  `initialize` remained public; authenticated OAuth/PKCE smokes passed.
- protocol regression: refresh rotation passed, replay of the spent refresh
  token returned `400 invalid_grant`, JSON-RPC batch returned 400, and an
  unsupported MCP protocol version returned 400.
- data safety: `PRAGMA quick_check=ok`; event DB SHA, page count and event row
  count were unchanged across the evidence read suite; the repository reported
  `database_mode=read_only` and `provider_calls=0`.
- social canary: the one authorized Telegram Saved Messages mutation reached
  `succeeded`, its read-after-write receipt was verified, and a post-restart MCP
  read returned the exact text `Привет мир`; no blind retry or duplicate commit
  was performed.
- runtime safety: auth DB mode was 0600; scans of current/rotated runtime logs
  found zero literal occurrences of the MCP path, client secret, bootstrap
  token, social approval token or signing key, and no MCP HTTP 5xx.
- availability: public `/healthz` returned `ok=true`, `ready=true`, `db=ok`,
  `issues=[]`; scheduler/watchdog/outbox checks were green. A live Telegram
  `/start` returned a button-bearing UI response in 587 ms, and the runtime
  mirror recorded three webhook POSTs, all HTTP 200 and none 5xx. The deployed
  Fly check is `GET /healthz` at 15-second interval, 5-second timeout and
  1-minute grace period.

## Prevention

The production acceptance matrix now treats a completely absent bearer as a
separate transport case from an invalid bearer and verifies both resource URLs.
