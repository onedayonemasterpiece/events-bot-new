# INC-2026-08-27 eventsBot VK image publish outcome unknown

Status: open
Severity: sev2
Service: private eventsBot MCP / VK Social Workspace publishing
Opened: 2026-08-27
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-16-tg-event-publish-timeout-duplicate`, `INC-2026-07-03-current-import-vector-vk-publication`, `INC-2026-05-19-vk-posts-personal-author`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

An explicitly requested image post for the VK community
`kenigeventsofficial` passed MCP asset staging and preparation but finished as
`outcome_unknown`; authenticated wall readback proved that the post was not
created. Preparation status returned a synthetic zero operation reference
instead of the real linked operation, and bounded content search accepted but
ignored its requested date range.

## User / Business Impact

- The VK edition of an editorial news post was missing while its Telegram
  edition had already succeeded.
- The generic status surface could be mistaken for publication success because
  it returned `committed` without provider success evidence.
- Same-day duplicate reconciliation through generic search was unsafe because
  older matching wall posts escaped the requested date window.

## Detection

- Reported from the eventsBot MCP commit receipt
  `op_Ze8l0DxXfgU09JGOhIU_VSxRnJR-pRDB`.
- Authenticated `wall.get` and postponed-wall reads for owner `-231828790`
  found no incident-window post and no unique text-fragment match.
- The durable provider governor advanced only once. In the fixed call order this
  proves that `photos.saveWallPhoto` and `wall.post` were never reached; secrets
  are excluded from evidence.

## Timeline

- 2026-08-27 14:55–15:03 UTC — image asset, preparation and VK commit attempt.
- 2026-08-27 15:22 UTC — authenticated provider readback confirmed no live,
  postponed or alternate-owner copy in the bounded incident window.
- 2026-08-27 — incident workflow opened from fresh `origin/main`
  `1ad95daa097a6fb768bb60770c2967539c7805fb`.
- 2026-08-27 — the current production media user token successfully completed
  a non-mutating `photos.getWallUploadServer` probe for the intended group.

## Root Cause

Production evidence localizes the failed attempt to the multipart step between
the one successful upload-server API call and the never-started photo-save
call. The original low-level exception is unrecoverable because the adapter
discarded its class/stage and runtime logs recorded only the outer HTTP request.
The multipart transport contained a deterministic defect in that exact stage:
it called `aiohttp.StreamReader.read(limit)` once and treated a short currently
available chunk as EOF. A fragmented, otherwise valid VK JSON response was
therefore parsed as truncated JSON and collapsed to a generic provider error.
The same short-read class had already been removed from the ordinary VK API
transport but remained in the multipart transport.

Four lifecycle defects amplified the incident: any failure after the first
provider call was broadened to non-retryable `outcome_unknown`; core runtime
audited any returned mapping as provider success; reconciliation only replayed
that cached unknown; and preparation status returned a hard-coded zero
operation ref. Separately, VK list/search code never applied accepted
`date_from`/`date_to` bounds and did not bind them to the cursor.

## Contributing Factors

- VK provider error method/code/stage were collapsed before reaching the MCP
  receipt.
- The provider adapter used one broad “any provider call started” flag for the
  complete photo-upload and wall-write chain.
- Reconciliation replayed a stored generic result instead of performing a
  bounded authenticated wall read.
- Wall-photo upload used the same nominal community-editor role as `wall.post`
  instead of exposing its distinct user-token authorization prerequisite.

## Automation Contract

### Treat as regression guard when

- changing Social Workspace preparation/operation state or status lookup;
- changing VK photo upload, wall publication, provider error normalization or
  reconciliation;
- changing VK `social_content_search` / `social_content_feed` date filtering.

### Affected surfaces

- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_vk_transport.py`
- `private_events_mcp_workspace_providers.py`
- isolated OAuth/provider SQLite state
- VK owner `-231828790` (`kenigeventsofficial`)

### Mandatory checks before closure or deploy

- successful VK text+image publish returns real operation/item/target refs;
- definite pre-wall VK API errors are `failed`, not `outcome_unknown`;
- a possibly sent `wall.post` timeout remains non-retryable unknown;
- reconciliation finds an exact bounded wall match but never invents success;
- preparation and operation status return one consistent real operation;
- inclusive `date_from` / `date_to` filtering and cursor binding pass;
- provider readback confirms one live catch-up post with image and no duplicate;
- production `/healthz` is ready and deployed SHA is reachable from `origin/main`.

### Required evidence

- sanitized runtime/provider-stage evidence for the original operation;
- targeted and complete Private Events MCP test results;
- deployed main SHA and health check;
- public VK URL, authenticated `wall.getById`, MCP item/feed receipt and
  before/after duplicate scans.

## Immediate Mitigation

- The original unknown operation is not replayed.
- Telegram is left untouched.
- A provider-level no-duplicate preflight is complete; the one catch-up write
  remains blocked on fix and deploy.

## Corrective Actions

- [x] Persist and return the real preparation-to-operation mapping.
- [x] Preserve definitive VK API error class/stage and wall-write uncertainty.
- [x] Add bounded exact VK wall reconciliation without retrying `wall.post`.
- [x] Enforce inclusive content date bounds and date-bound cursors.
- [x] Read multipart upload responses to EOF under the byte cap and split media
  upload onto an explicit user-token actor.
- [ ] Deploy and perform one verified catch-up publish.

## Follow-up Actions

- [x] Keep provider-stage telemetry free of payload text, tokens and upload URLs.
- [ ] Re-audit ChatGPT action publication only if a public tool schema changes.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The regression suite includes fragmented multipart JSON, pre-wall multipart
failure classification, definite VK API rejection, mutation timeout,
restart-safe exact wall reconciliation, preparation/operation convergence,
non-success audit classification, inclusive date filtering and date-bound
cursors. Closure still requires production deployment and the one-post live
acceptance evidence above.
