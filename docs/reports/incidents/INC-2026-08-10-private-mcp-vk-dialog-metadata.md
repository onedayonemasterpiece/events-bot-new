# INC-2026-08-10 Private MCP VK dialog metadata unavailable

Status: open
Severity: sev2
Service: Private Events MCP / VK Social Workspace
Opened: 2026-08-10
Closed: —
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

## Root Cause

1. `social_targets_list` was implemented as public target search, not dialog list.
2. The only `messages.getConversations` route was nested under `list_items` for a
   self target and projected each conversation's `last_message` as content.
3. There was no closed response schema that could prove message bodies and native
   peer identifiers were excluded.

## Contributing Factors

- Tool descriptions said “accessible dialogs/managed targets” although the
  implementation performed public/community discovery.
- Provider availability errors were normalized to one generic workspace error,
  making a missing operation look like a credential/config failure.

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

- [ ] Add a VK-only `social_dialogs_list` typed tool and `list_dialogs` operation.
- [ ] Add a closed metadata-only dialog response schema and runtime projection.
- [ ] Bind unread/all mode into opaque cursors.
- [ ] Allow explicit DM sends to bound VK user/chat/community dialog targets.
- [ ] Deploy from exact merged main and run the metadata-only live smoke.

## Follow-up Actions

- [ ] Consider safe granular error codes for read-operation rejection without
  leaking provider details.
- [ ] Add a first-class Telegram dialog-list operation only after an equivalent
  metadata-only contract is reviewed; do not advertise it through the VK tool.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending exact-main Fly release
- regression checks: pending
- post-deploy verification: pending

## Prevention

Dialog identity listing and message-body reads are separate typed operations.
Provider responses are projected through closed schemas so an innocuous provider
field cannot accidentally expose message content or native peer identifiers.
