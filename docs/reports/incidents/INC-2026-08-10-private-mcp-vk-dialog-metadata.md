# INC-2026-08-10 Private MCP VK dialog metadata unavailable

Status: closed
Severity: sev2
Service: Private Events MCP / VK Social Workspace
Opened: 2026-08-10
Closed: 2026-08-10
Owners: events-bot MCP
Related incidents: `INC-2026-08-08-private-mcp-oauth-csp-redirect`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The production VK role credentials and private-read/DM kill switches were active,
but ChatGPT could not answer a metadata-only question about who had unread VK
dialogs. The workspace exposed dialog history through the generic content-feed
operation and exposed target discovery as public/community search; it had no
typed operation for listing dialog identities and unread counters without
returning message bodies.

## User / Business Impact

- ChatGPT incorrectly reported that private VK provider operations were disabled.
- The operator could not inspect unread sender identities through the MCP even
  though the dedicated VK role was configured and usable.
- Reusing the generic feed would have violated the requested data minimization:
  “show who wrote, do not read the messages.”

## Detection

The operator reported repeated `social workspace request rejected` responses.
A production-safe role/config probe showed the private-read, DM, dialog-reader and
messenger roles enabled. A bounded direct provider probe using
`messages.getConversations(filter=unread)` returned eight unread conversations;
the probe projected only display names/kinds/unread counts and never accessed or
logged `last_message` content.

## Timeline

- 2026-08-10 21:20 UTC — operator reports that VK dialogs cannot be listed/read.
- 2026-08-10 21:25 UTC — production flags and dedicated VK role presence confirmed.
- 2026-08-10 21:31 UTC — metadata-only provider probe succeeds for eight unread dialogs.
- 2026-08-10 21:36 UTC — root cause localized to the MCP tool/response contract.
- 2026-08-10 21:49 UTC — exact-main release 1964 deploys the typed dialog tool;
  live MCP smoke still fails at the provider transport boundary.
- 2026-08-10 21:55 UTC — bounded direct provider request succeeds while the
  identical aiohttp transport intermittently returns truncated JSON; root cause
  traced to treating one short `StreamReader.read(n)` result as EOF.
- 2026-08-10 22:05 UTC — a production-equivalent OpenCode stable-scope
  `tools/list` regression reproduces a second boundary failure: the catalog
  exceeded the ordinary 128 KiB data-result cap.

## Root Cause

1. `social_targets_list` was implemented as public target search, not dialog list.
2. The only `messages.getConversations` route was nested under `list_items` for a
   self target and projected each conversation's `last_message` as content.
3. There was no closed response schema that could prove message bodies and native
   peer identifiers were excluded.
4. The fixed VK aiohttp transport performed a single bounded `read(n)`. Aiohttp
   is allowed to return fewer than `n` bytes as soon as data is available, so a
   fragmented or compressed provider response was parsed before EOF and collapsed
   to the generic `provider_unavailable` workspace error.

## Contributing Factors

- Tool descriptions said “accessible dialogs/managed targets” although the
  implementation performed public/community discovery.
- Provider availability errors were normalized to one generic workspace error,
  making a missing operation look like a credential/config failure.
- Unit provider fakes returned already materialized Python mappings and therefore
  did not exercise fragmented HTTP response delivery.
- OAuth catalog coverage did not previously combine both providers, all
  production capability flags and the stable coarse scopes used by the existing
  OpenCode connection.

## Automation Contract

### Treat as regression guard when

- changing VK dialog/history/search or DM-send adapter paths;
- changing private-read scopes, tool filtering, provider roles or opaque refs;
- changing the ChatGPT/OpenCode social catalog or Codex isolation.

### Affected surfaces

- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp_vk_adapter.py`
- dedicated VK dialog-reader/user-messenger runtime roles

### Mandatory checks before closure or deploy

- metadata-only unread dialog tool calls the fixed dialog-reader method with
  `filter=unread`, returns opaque target refs, names/kinds/counts only, and never
  message text or native IDs;
- cursor cannot cross unread/all modes;
- returned user/chat/community dialog refs support an explicitly requested typed
  `send_message` through the dedicated messenger role and read-after-write receipt;
- legacy `vk:read` and granular `vk:read:dialogs` authorize only this VK read;
- private-read kill switch removes the tool;
- Codex still lists exactly seven evidence tools and no social tool;
- full Private Events MCP suite, compileall, diff check, health and production
  metadata-only smoke pass on an exact `origin/main` deployment.

### Required evidence

- merged and deployed SHA reachable from `origin/main`;
- test counts and CI links;
- production `/healthz` and database quick-check;
- sanitized live receipt with dialog count only (no bodies/native IDs).

## Immediate Mitigation

A one-off production probe supplied the requested sender identities while
deliberately ignoring `last_message` and returning no native identifiers.

## Corrective Actions

- [x] Add a VK-only `social_dialogs_list` typed tool and `list_dialogs` operation.
- [x] Add a closed metadata-only dialog response schema and runtime projection.
- [x] Bind unread/all mode into opaque cursors.
- [x] Allow explicit DM sends to bound VK user/chat/community dialog targets.
- [x] Deploy from exact merged main and run the metadata-only live smoke.
- [x] Read fixed VK HTTP responses to EOF under the decoded-byte cap and cover
  fragmented/oversized streams with transport-level regressions.
- [x] Keep ordinary data responses at their configured cap while allowing the
  authenticated full workspace catalog through a separate 512 KiB metadata cap.

## Follow-up Actions

- [ ] Consider safe granular error codes for read-operation rejection without
  leaking provider details.
- [ ] Add a first-class Telegram dialog-list operation only after an equivalent
  metadata-only contract is reviewed; do not advertise it through the VK tool.

## Release And Closure Evidence

- merged implementation SHAs:
  - PR `#491`, merge `c20c0cb4d005a826f7a6697ee10c094c1be19804`
    (typed dialog surface and closed metadata schema);
  - PR `#492`, merge `55d06e4a0d2622c770093acec379abd7f2fbbddf`
    (fragment-safe VK transport and bounded full catalog).
- deployed SHA: `55d06e4a0d2622c770093acec379abd7f2fbbddf`, Fly machine
  version `1965`, via `scripts/deploy_fly_main.sh` from a clean detached exact
  `origin/main` checkout.
- regression checks: full Private Events MCP suite `395 passed` with three
  pre-existing aiohttp `NotAppKeyWarning` warnings; focused adapter/runtime/server
  suites `105 passed`; compileall, focused Ruff and `git diff --check` passed;
  PR `#492` `python-ci` and `static-browser-release-gate` both passed.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, DB
  `quick_check=ok`, Fly health check passed, and the runtime file mirror was
  enabled at `/data/runtime_logs/events-bot.log`. Since the final deploy the log
  contained no traceback, error/critical, `provider_unavailable` or rejected
  social-workspace marker.
- sanitized live MCP receipt: OAuth refresh, `initialize`, production
  `tools/list` and `social_dialogs_list(unread_only=true, limit=20)` all passed;
  eight dialog metadata rows were returned with only opaque target ref, kind,
  display title, unread count and trust marker. Message bodies and provider-native
  identifiers were neither projected nor logged. No VK message was sent.

## Prevention

Dialog identity listing and message-body reads are separate typed operations.
Provider responses are projected through closed schemas so an innocuous provider
field cannot accidentally expose message content or native peer identifiers.
