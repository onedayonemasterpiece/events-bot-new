# INC-2026-08-25 ChatGPT retained a VK-only MCP action snapshot

Status: open — workspace action refresh and live ChatGPT acceptance pending
Severity: sev2
Service: private eventsBot ChatGPT MCP action discovery
Opened: 2026-08-25
Closed: —
Owners: eventsBot MCP / ChatGPT workspace administrator
Related incidents: `INC-2026-08-24-mcp-telegram-album-media-ref`, `INC-2026-08-15-audio-mcp-runtime-catalog-truncation`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`, [OpenAI: Developer mode and MCP apps in ChatGPT](https://help.openai.com/en/articles/12584461-developer-mode-and-full-mcp-connectors-in-chatgpt)

## Summary

After the Telegram item-link/audio rollout, a real ChatGPT conversation did
not call `social_item_resolve` for an authorized private Telegram message. It
reported that the published action schema permitted only VK and attempted a
long fallback through target search/feed/item operations instead. Production
already served the corrected Telegram+VK schema, so the server implementation
and OAuth scopes alone could not repair the workspace-approved ChatGPT action
snapshot.

## User / Business Impact

- The requested Telegram chat read did not return the requested thread or its
  voice transcripts even though the production MCP could perform that read.
- ChatGPT spent many calls on unrelated discovery paths and produced no useful
  final result.
- Connection refresh appeared successful, which obscured the separate
  workspace action-publication boundary.

## Detection

- The user supplied a ChatGPT screenshot showing the VK-only published-schema
  conclusion and unsuccessful fallback.
- A second independent ChatGPT window confirmed that a direct message-link
  attempt was rejected by its old client-side JSON Schema before the corrected
  production operation could run.
- Production audit for the affected window contained Telegram capability,
  search, feed and item attempts but no `resolve_item` call.
- The deployed authenticated `tools/list` was independently verified to expose
  `social_item_resolve.platform = [telegram, vk]`, and the recent ChatGPT token
  refresh retained both provider read families.

## Timeline

- 2026-08-25 22:11 UTC — exact-main Fly release `v2035` was healthy and the
  existing OAuth connection remained connected.
- 2026-08-25 22:25–22:32 UTC — the affected ChatGPT conversation called several
  Telegram discovery/read operations but never invoked `resolve_item`.
- 2026-08-25 22:33 UTC — production health, deployed SHA and current
  authenticated tool schema were reverified; all were correct.
- 2026-08-25 22:35 UTC — historical source inspection confirmed that the
  workspace's earlier approved `social_item_resolve` definition was VK-only.
- 2026-08-25 22:36 UTC — OpenAI's current MCP app documentation confirmed that
  approved apps use a frozen tool/input snapshot until an administrator reviews
  and publishes an action refresh.
- 2026-08-25 22:39 UTC — evidence from a second ChatGPT window independently
  reproduced the stale-schema rejection while confirming the same exact-main
  production release and server-side regression results.

## Root Cause

1. The original approved ChatGPT action definition for
   `social_item_resolve` exposed only VK.
2. PR `#575` made Telegram link resolution backward-compatible on the live MCP
   server while retaining the same tool name.
3. ChatGPT workspace approval keeps a frozen snapshot of tool definitions.
   Refreshing the OAuth connection/token does not by itself approve and publish
   the changed action schema.
4. The conversation therefore planned against the old VK-only input enum even
   though production `tools/list` and the token's scopes both supported
   Telegram.

## Contributing Factors

- The operational runbook treated connection Refresh and action-definition
  Refresh as one step.
- The server-side/OpenCode canary bypassed ChatGPT's workspace-approved action
  snapshot and therefore could not prove the real conversation schema.
- The tool name remained stable, so the stale definition was not obvious in the
  chat tool picker.
- ChatGPT does not currently notify users or administrators that a failed call
  requires action-definition review.

## Automation Contract

### Treat as regression guard when

- changing a ChatGPT-visible MCP tool name, description, input/output schema,
  security scheme or scope option;
- expanding a stable tool from one provider to another;
- changing the action-publication or connector-refresh runbook.

### Affected surfaces

- ChatGPT workspace Apps action-control snapshot and publication state;
- `private_events_mcp/tool_catalog.py::ToolSpec.descriptor`;
- `private_events_mcp/social_workspace_tools.py` social tool descriptors;
- OAuth connection refresh versus action-definition refresh;
- real new-chat tool selection and invocation.

### Mandatory checks before closure or deploy

- verify exact-main production `tools/list` exposes Telegram and VK for
  `social_item_resolve` under the ChatGPT scope set;
- in ChatGPT workspace action control, refresh and review the changed action
  definition, then enable/publish the reviewed update;
- start a new ChatGPT conversation with the refreshed app selected;
- verify the real conversation calls `social_item_resolve` for the authorized
  private Telegram link, then reads the thread and returns ready/queued voice
  status without exposing transcript or provider/native data in evidence;
- verify health, deployed SHA, OAuth scopes and sanitized MCP audit rows.

### Required evidence

- sanitized before/after action schema showing the platform enum change;
- administrator-reviewed action refresh/publication receipt;
- sanitized real ChatGPT call receipt and successful audit row;
- exact-main SHA and ready health result.

## Immediate Mitigation

- Confirmed that no server rollback or OAuth scope change is required.
- Preserved the existing endpoint, app identity, OAuth client/resource and
  refresh state; no delete/re-add or credential rotation was attempted.
- Identified the required external control-plane action: refresh, review and
  publish the updated actions in the existing ChatGPT app, then use a new chat.

## Corrective Actions

- Corrected the runbook to separate OAuth connection refresh from the frozen
  workspace action snapshot.
- Added this incident as a regression contract for future MCP schema changes.

## Follow-up Actions

- [ ] Workspace administrator: refresh/review/publish the updated `eventsBot`
  actions without changing the MCP endpoint or OAuth identity.
- [ ] Run the real new-chat Telegram link/thread/audio acceptance and attach a
  sanitized receipt.
- [ ] Keep the incident open until `resolve_item` is observed from ChatGPT and
  the requested high-level result succeeds.

## Release And Closure Evidence

- deployed SHA at detection:
  `297b3c76131a5461e9b601bea9e78afaf49a2847`, Fly `v2035`
- deploy path: prior exact-main deployment through
  `scripts/deploy_fly_main.sh`
- regression checks: production health ready; live authenticated server schema
  contains both providers; recent ChatGPT OAuth refresh contains both provider
  read families; affected conversation produced no `resolve_item` audit row
- post-deploy verification: server side passed; ChatGPT workspace action
  publication and real conversation acceptance remain pending

## Prevention

Every ChatGPT-visible schema change now requires two distinct acceptance gates:
the live authenticated MCP descriptor and the administrator-approved ChatGPT
action snapshot. Connection/token refresh is not evidence for the second gate.
