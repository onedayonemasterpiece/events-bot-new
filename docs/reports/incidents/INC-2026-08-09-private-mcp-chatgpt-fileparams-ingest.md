# INC-2026-08-09 Private MCP ChatGPT fileParams ingest rejected

Status: open
Severity: sev2
Service: Private Events MCP ChatGPT social asset staging
Opened: 2026-08-09
Closed: —
Owners: events-bot production / Private Events MCP
Related incidents: `INC-2026-08-09-private-mcp-vk-story-cdn-rejected`, `INC-2026-08-08-private-mcp-oauth-csp-redirect`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`

## Summary

The first real ChatGPT conversation-file attempt against production
`social_asset_stage` was rejected before the Telegram provider boundary. The
same OAuth principal could resolve Telegram Saved Messages and read its live
capabilities, but no `ast_*` asset was created and no message was sent.

## User / Business Impact

- ChatGPT cannot currently complete the documented conversation image to
  Telegram Saved Messages workflow.
- Text/social read capabilities remain available and healthy.
- No provider write, media upload or Telegram mutation occurred.

## Detection

- A ChatGPT conversation containing a 10,015,797-byte PNG invoked
  `social_asset_stage` and received only `social workspace request rejected`.
- ChatGPT tried a sandbox URI, mounted path and Files reference; every string
  form was rejected before asset creation.
- The generic public error did not expose a safe reason code, leaving the
  failed boundary indistinguishable without production log/audit inspection.

## Timeline

- 2026-08-09T11:35Z — operator reported the first real conversation-file
  staging failure; target resolution and capability discovery had succeeded.
- 2026-08-09T11:44Z — incident workflow opened; no retry, approval, commit or
  Telegram send was performed by the production debugging path.
- 2026-08-09T11:36:18Z–11:38:06Z — five production calls reached
  `asset_stage` validation and were denied as `mediaingressrejected` in 3–5 ms;
  no asset row, retained byte or provider call was created.
- 2026-08-09 — the live tool descriptor was verified against the official
  ChatGPT file-parameter object. The connector had already rewritten the
  conversation upload to the required object; the rejection occurred inside
  media ingress before download.

## Root Cause

The historical code collapsed every media-ingress exception into one public
message and one exception-class audit reason, so the exact sub-branch of the
five already-completed calls is irretrievable. The timing and persisted state
prove that descriptor parsing, workspace dispatch, role/size configuration and
principal derivation completed, while network streaming and Telegram provider
staging did not begin. The leading remaining hypothesis is an exact temporary
download-host/allowlist mismatch; a safe coded diagnostic deploy and one real
ChatGPT retry are required to distinguish that from DNS/fetch policy without
logging the URL.

## Contributing Factors

- `social_workspace_tools` currently collapses all validation, policy, ingest
  and provider-stage failures into one generic error string.
- Unit/fake coverage did not prove the exact payload emitted by a real ChatGPT
  conversation attachment.

## Automation Contract

### Treat as regression guard when

- changing `openai/fileParams` metadata or file input schema;
- changing ChatGPT file download/validation/storage, asset ownership, social
  asset staging or image publication to Telegram.

### Affected surfaces

- `social_asset_stage`, `social_asset_status` and media-bearing
  `social_action_prepare`;
- ChatGPT tool descriptor metadata and connector runtime payload;
- private media ingest/store and principal/resource binding;
- Telegram Saved Messages image publication acceptance.

### Mandatory checks before closure or deploy

- identify and reproduce the exact production rejection branch without
  logging a signed URL, file ID, filename, provider ID or secret;
- real-shape ChatGPT file object stages to `ast_*`, status is ready, and the
  same principal can prepare and commit the exact image to Saved Messages;
- foreign-principal, unbound, string/path/file-id-only, expired and tampered
  files remain rejected before provider I/O;
- safe stable error codes distinguish unresolved file, binding, workspace,
  MIME, size and fetch failures without returning secrets or provider IDs;
- full Private Events MCP tests, compileall, Ruff and diff-check pass;
- exact ChatGPT OAuth resource/client identity stays unchanged and Codex still
  lists exactly seven evidence tools with no file/social surface;
- exact-main deploy preserves `/healthz`, webhook/scheduler health, SQLite
  `quick_check`, event DB read immutability and log redaction.

### Required evidence

- sanitized descriptor and actual payload-shape receipt;
- failing-before/passing-after tests including foreign-principal denial;
- reviewed PR head, merged/deployed main SHA, Fly release and in-container SHA;
- Telegram Saved Messages message ID/read-back receipt for the approved image;
- confirmation that no unrelated channel/user received the test message.

## Immediate Mitigation

Do not bypass staging by passing ChatGPT `file_*`, filesystem paths or URLs into
`social_action_prepare`. Keep the canonical `ast_*` contract and do not retry a
provider send until staging and approval succeed.

## Corrective Actions

- Return stable bounded MCP error codes for asset-stage failures and retain
  only a safe reason plus optional one-way host fingerprint in audit.
- Preserve the official object-to-`ast_*` contract; do not accept raw paths,
  direct `file_*` values or file objects belonging to another principal.
- After diagnostic deploy, repeat the same current-conversation stage once and
  correct only the exact observed ingress policy before the approved Saved
  Messages publication acceptance.

## Follow-up Actions

- [x] Add a permanent real-shape ChatGPT fileParams regression.
- [x] Add safe asset-staging error codes and production audit dimensions.
- [ ] Complete one real ChatGPT retry, exact policy correction if required and
  Saved Messages image read-back.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: `scripts/deploy_fly_main.sh` from clean exact `origin/main`
- regression checks: pending
- post-deploy verification: pending

## Prevention

The production acceptance gate will include one explicit ChatGPT conversation
image through staging, status, human approval, commit and Saved Messages
read-back rather than relying only on synthetic file-ingestor fixtures.
