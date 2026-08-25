# INC-2026-08-25 ChatGPT retained a VK-only action and could not poll social voice jobs

Status: mitigating — principal fix and catch-up complete; action refresh and live ChatGPT acceptance pending
Severity: sev2
Service: private eventsBot ChatGPT MCP action discovery and Telegram voice reads
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
snapshot. The fallback did reach the authorized group and created durable voice
jobs, but the returned `atr_*` references could not be polled by the public
audio status/get tools because Social Workspace and standalone audio ingress
derived two different owner bindings from the same OAuth context.

## User / Business Impact

- The requested Telegram chat read did not return the requested thread or its
  voice transcripts even though the production MCP could perform that read.
- Twelve voice reads were durably queued for the affected principal, but
  `audio_transcription_status` returned `TRANSCRIPTION_PRINCIPAL_MISMATCH`, so
  the client could not distinguish legitimate serialized delay from failure or
  collect completed text.
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
- A provider-ID fallback with the exact expected target kind resolved the
  authorized private group, and a bounded feed read returned twelve voice
  attachments with durable queued transcription references.
- Polling one reference through the public audio tool reproduced
  `TRANSCRIPTION_PRINCIPAL_MISMATCH`. Production job-store evidence showed one
  serialized run under a persisted provider hold plus 23 queued jobs; the file
  log mirror was enabled and healthy, while this bounded tool error was not
  emitted as a runtime log line.
- Continued pagination later reached the local hourly media budget. The
  provider call itself had succeeded, but the runtime counted the subsequent
  local quota rejection as a provider failure; repeated attempts therefore
  opened the provider circuit and obscured the real bounded-policy reason.

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
- 2026-08-25 22:43 UTC — PR `#581` merged the action-refresh runbook and this
  regression contract to `main` after all required CI checks passed.
- 2026-08-25 22:48–22:53 UTC — the fallback resolved the private group only
  when the exact group kind accompanied its provider ID, read the bounded feed,
  reproduced the status principal mismatch, and confirmed twelve current
  queued voice jobs. Production retained one running job under a persisted
  `Retry-After` deadline at 23:01 UTC; no busy-poll or unsafe duplicate start
  was attempted.
- 2026-08-25 23:04 UTC — PR `#582` merged the same-principal compatibility
  lookup to `main`; exact-main Fly release `v2036` deployed SHA
  `e327a400f0453fa38401e4cdb32ffed5a6ce61e6` and reported ready health.
- 2026-08-25 23:05–23:30 UTC — status/get accepted already queued social jobs
  without re-ingress. The serialized lane resumed after the provider hold and
  advanced the affected fourteen-reference cohort from queued to `14/14`
  complete at its normal bounded cadence. Public get then returned `14/14`
  ready, non-empty results for the same principal.
- 2026-08-25 23:15–23:22 UTC — an independent authorized read covered the
  complete chat history and all sixteen voice messages; Telegram native
  transcription returned text for all sixteen. This recovered the requested
  idea intake but does not replace the required refreshed-ChatGPT MCP canary.
- 2026-08-25 23:13–23:18 UTC — production budget/circuit evidence identified a
  second runtime defect: a successful provider read followed by local media
  quota denial incremented the provider failure circuit. A failing-before
  regression and bounded fix were prepared; genuine provider failures still
  open the circuit.

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
5. Social Workspace auto-transcription stored jobs under SHA-256 of
   `client_id + subject + resource`. The standalone audio tools used an HMAC
   of `subject + client_id + resource` with the server signing key. Both were
   derived from the same verified OAuth context but were intentionally unequal.
6. `audio_transcription_status/get` checked only the standalone binding, so a
   valid `atr_*` returned by a social read was misclassified as belonging to
   another principal. Passing the associated `ast_*` to file ingress could not
   work because it is not a ChatGPT `fileParams` object.
7. The twelve-voice batch also encountered the already documented serialized
   Kaggle lane and a persisted provider `Retry-After`. That delay was operating
   as designed, but the owner mismatch removed the only explicit polling path
   and made an accepted queue look like a terminal failure.
8. `SocialWorkspaceRuntime.read()` used one `provider_attempted` flag for every
   later exception. A safe local media/egress budget rejection after a
   successful adapter response was consequently persisted as a provider
   failure. After the threshold, the next read received a provider-circuit
   error even though neither Telegram nor the adapter had failed.

## Contributing Factors

- The operational runbook treated connection Refresh and action-definition
  Refresh as one step.
- The server-side/OpenCode canary bypassed ChatGPT's workspace-approved action
  snapshot and therefore could not prove the real conversation schema.
- The tool name remained stable, so the stale definition was not obvious in the
  chat tool picker.
- ChatGPT does not currently notify users or administrators that a failed call
  requires action-definition review.
- The original live acceptance proved repeat-read cache behavior but did not
  call the separately exposed audio status/get tools with an `atr_*` minted by
  Social Workspace, so the owner-binding mismatch escaped coverage.
- Provider-circuit tests covered actual provider errors but did not assert that
  a successful provider call followed by a local quota refusal resets, rather
  than increments, the provider failure streak.

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
- `private_events_mcp/social_workspace_runtime.py` read-triggered audio owner
  binding and `audio_transcription/mcp.py` status/get authorization;
- durable serialized audio jobs, monitor cadence and persisted
  `retry_not_before` handling;
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
- for every queued `atr_*` returned by the social read, prove status/get accepts
  the same OAuth principal, rejects a different principal, honors durable queue
  and provider-hold states, and eventually returns every ready transcript;
- do not pass `ast_*` to standalone file ingress and do not busy-poll or bypass
  the serialized dedicated Telegram/Kaggle lane;
- verify health, deployed SHA, OAuth scopes and sanitized MCP audit rows.

### Required evidence

- sanitized before/after action schema showing the platform enum change;
- administrator-reviewed action refresh/publication receipt;
- sanitized real ChatGPT call receipt and successful audit row;
- sanitized voice counts and state transitions only (no references, transcript
  text, private links or native identifiers) through final ready coverage;
- exact-main SHA and ready health result.

## Immediate Mitigation

- Confirmed that no server rollback or OAuth scope change is required.
- Preserved the existing endpoint, app identity, OAuth client/resource and
  refresh state; no delete/re-add or credential rotation was attempted.
- Identified the required external control-plane action: refresh, review and
  publish the updated actions in the existing ChatGPT app, then use a new chat.
- Preserved the durable voice jobs and the provider's persisted hold. No
  duplicate `audio_transcription_start`, job-owner mutation, unsafe asset
  migration or aggressive status loop was used.

## Corrective Actions

- Corrected the runbook to separate OAuth connection refresh from the frozen
  workspace action snapshot.
- Added this incident as a regression contract for future MCP schema changes.
- Added a same-authenticated-principal compatibility lookup so public audio
  status/get can address both standalone-upload and historical Social
  Workspace jobs while cross-principal access still fails closed.
- Added a multi-voice regression that reads two queued social voice
  attachments and obtains the completed text for each through public
  status/get.
- Separated provider transport outcome from later local response-policy
  outcome. Successful adapter reads now remain provider successes when egress
  or media quota withholds the response, while real provider failures and flood
  waits retain the existing circuit behavior.

## Follow-up Actions

- [ ] Workspace administrator: refresh/review/publish the updated `eventsBot`
  actions without changing the MCP endpoint or OAuth identity.
- [ ] Run the real new-chat Telegram link/thread/audio acceptance and attach a
  sanitized receipt.
- [x] Merge/deploy the owner-binding correction from exact `origin/main`, then
  prove a pre-deploy queued social `atr_*` can be polled without re-ingress.
- [x] Honor the persisted provider hold and complete compensating catch-up for
  the affected voice cohort; verify every distinct voice reaches ready or a
  documented terminal error before closure.
- [ ] Keep the incident open until `resolve_item` is observed from ChatGPT and
  the requested high-level result succeeds.

## Release And Closure Evidence

- deployed SHA at detection:
  `297b3c76131a5461e9b601bea9e78afaf49a2847`, Fly `v2035`
- principal compatibility deploy:
  `e327a400f0453fa38401e4cdb32ffed5a6ce61e6`, Fly `v2036`
- deploy path: prior exact-main deployment through
  `scripts/deploy_fly_main.sh`
- regression checks: production health ready; live authenticated server schema
  contains both providers; recent ChatGPT OAuth refresh contains both provider
  read families; affected conversation produced no `resolve_item` audit row
- post-deploy verification: ready health and exact-main SHA passed; an already
  queued social job returned its durable state and later ready text through
  public status/get without re-ingress; the full affected cohort reached
  `14/14` complete and `14/14` ready/non-empty through the same public tools.
  ChatGPT workspace action publication and real refreshed-conversation
  acceptance remain pending

## Prevention

Every ChatGPT-visible schema change now requires two distinct acceptance gates:
the live authenticated MCP descriptor and the administrator-approved ChatGPT
action snapshot. Connection/token refresh is not evidence for the second gate.
