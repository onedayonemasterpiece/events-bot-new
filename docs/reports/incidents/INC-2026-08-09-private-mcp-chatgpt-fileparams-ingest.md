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
ingress policy repair later produced a ready `ast_*`, but the first end-to-end
Saved Messages attempt exposed a second product failure: every new outbound
preparation was unconditionally held for a redundant browser confirmation even
though the current user had explicitly requested the send. The operator stopped
that failed scenario; no message was sent.

## User / Business Impact

- Production image staging succeeds and Fly release v1957 now accepts a fresh
  explicitly requested outbound preparation without a second browser prompt.
  The stopped pre-hotfix preparation remains inert; the final live Saved
  Messages read-back intentionally waits for a new operator request.
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
- 2026-08-09 — the first coded retry returned `FILE_HOST_NOT_ALLOWED`. A
  controlled `*.oaiusercontent.com` policy probe remained denied and produced
  a different full-host fingerprint, proving that ChatGPT rotates temporary
  download hostnames outside that assumed suffix. No bytes/provider calls were
  made. The next diagnostic records only one-way full/two-label/three-label
  fingerprints so the stable DNS policy boundary can be identified safely.
- 2026-08-09 — the next real retry produced full/two-label/three-label hashes
  which identify the stable boundary as a rotating
  `<storage-account>.blob.core.windows.net` host. The full hostname remained
  undisclosed; no bytes, asset or provider call were created.
- 2026-08-09 — exact-main SHA `150358ed88b5239753bf3669a1f1e311bf3f63cc`
  was deployed as Fly release v1956 with the bounded Azure SAS policy; the next
  real ChatGPT retry staged the PNG successfully and returned a ready `ast_*`.
- 2026-08-09 — `social_action_prepare` then returned
  `awaiting_human_approval` plus an approval URL for Telegram Saved Messages.
  An immediate commit was denied. The operator explicitly stopped the scenario;
  the preparation was not approved or committed and Telegram was not called.
- 2026-08-09T13:27Z — exact merged-main SHA
  `d2b7993b41187660efa13d6d9070fda0c0d5a6cd` was deployed as Fly release
  v1957. Post-deploy inspection confirmed that the stopped preparation still
  has status `awaiting_human_approval` and has zero operation rows; it was not
  upgraded, committed or sent.
- 2026-08-11 — a source-default-off Telegram document-send candidate extended
  the same authenticated `fileParams -> ast_*` boundary for one structurally
  verified document. This is offline implementation evidence, not production
  closure: exact-main deploy, real ChatGPT APK selection, Saved Messages
  read-back, negative and narrow off/on rollback probes remain pending.

## Root Cause

The original allowlist assumed that ChatGPT conversation files would use one
exact OpenAI-controlled hostname. Real fileParams requests instead rotate
between Azure Blob storage accounts. An exact-host or guessed OpenAI suffix
policy therefore rejected the correctly rewritten file object before DNS and
download. The repair allows the explicitly configured Azure Blob suffix only
when the temporary URL carries a current blob-scoped read-only SAS; generic,
unsigned or write-enabled Azure URLs remain denied before network I/O.

The follow-up root cause was independent of ingestion: `prepare()` always wrote
`awaiting_human_approval` for every action and `commit()` always required a
separate approval row. The server cannot inspect the original conversation as
a separate trusted object; the typed action invocation in the authenticated
ChatGPT resource is the delegated assertion that the current user explicitly
requested that exact outbound action. Requiring another browser click therefore
duplicated consent without strengthening the target/payload binding.

## Contributing Factors

- `social_workspace_tools` currently collapses all validation, policy, ingest
  and provider-stage failures into one generic error string.
- Unit/fake coverage did not prove the exact payload emitted by a real ChatGPT
  conversation attachment.
- The original approval policy did not distinguish a new outbound action from
  edit/delete of existing content and therefore imposed the destructive-action
  confirmation flow on low-risk Saved Messages sends as well.

## Automation Contract

### Treat as regression guard when

- changing `openai/fileParams` metadata or file input schema;
- changing ChatGPT file download/validation/storage, asset ownership, social
  asset staging, or image/document publication to Telegram.

### Affected surfaces

- `social_asset_stage`, `social_asset_status` and media-bearing
  `social_action_prepare`;
- ChatGPT tool descriptor metadata and connector runtime payload;
- private media ingest/store and principal/resource binding;
- Telegram Saved Messages image publication acceptance;
- the default-off Telegram `send_message` document role, immutable document
  manifest/digest and one-attempt/read-after-write provider path.

### Mandatory checks before closure or deploy

- identify and reproduce the exact production rejection branch without
  logging a signed URL, file ID, filename, provider ID or secret;
- real-shape ChatGPT file object stages to `ast_*`, status is ready, and a fresh
  explicit outbound request prepares as `approved` without `approval_url`; the
  same principal can commit the exact image to Saved Messages once;
- when document send changes or is enabled, the actual ChatGPT upload UI supplies
  the closed file object for a deterministic tiny APK. Stage returns a ready
  principal/provider-bound `ast_*` with detected APK MIME, exact size/SHA-256,
  sanitized `.apk` name and expiry;
- document prepare is Telegram `send_message` with exactly one document and no
  mixed media; it is `approved` without `approval_url`, performs zero provider
  calls, and freezes role/name/MIME/size/SHA/expiry into its digest/preview.
  Commit reopens/rehashes, makes exactly one forced-document attempt and proves
  the matching downloadable Saved Messages document by read-back;
- VK, non-`send_message`, two-document, mixed-media, renamed ordinary ZIP,
  expired/tampered/foreign-principal, string/path/file-ID-only and disabled-flag
  controls fail before Telegram transport;
- old `awaiting_human_approval` preparations are never auto-upgraded or
  executed; edit/delete still require the independent operator approval;
- foreign-principal, unbound, string/path/file-id-only, expired and tampered
  files remain rejected before provider I/O;
- safe stable error codes distinguish unresolved file, binding, workspace,
  MIME, size and fetch failures without returning secrets or provider IDs;
- full Private Events MCP tests, compileall, Ruff and diff-check pass;
- exact ChatGPT OAuth resource/client identity stays unchanged and Codex still
  lists exactly seven evidence tools with no file/social surface;
- exact-main deploy preserves `/healthz`, webhook/scheduler health, SQLite
  `quick_check`, event DB read immutability and log redaction.
- disabling only `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED`
  removes document discovery/capability without breaking text/image/story;
  re-enabling restores the new-chat catalogue. Refresh the existing `eventsBot`
  connection in place; never replace or rename it.

### Required evidence

- sanitized descriptor and actual payload-shape receipt;
- failing-before/passing-after tests including foreign-principal denial;
- reviewed PR head, merged/deployed main SHA, Fly release and in-container SHA;
- Telegram Saved Messages message ID/read-back receipt for a fresh explicit
  post-fix request (never the stopped preparation);
- for document changes, sanitized actual-ChatGPT APK descriptor/stage, immutable
  prepare, one-attempt commit/read-back, negative and flag-off/on rollback
  receipts. A fake/local/filesystem smoke is supporting evidence only;
- confirmation that no unrelated channel/user received the test message.

## Immediate Mitigation

Do not bypass staging by passing ChatGPT `file_*`, filesystem paths or URLs into
`social_action_prepare`. Keep the canonical `ast_*` contract. Never approve or
commit the stopped preparation; let it expire and use only a fresh explicit
request after the hotfix is deployed.

## Corrective Actions

- Return stable bounded MCP error codes for asset-stage failures and retain
  only a safe reason plus optional one-way host fingerprint in audit.
- Preserve the official object-to-`ast_*` contract; do not accept raw paths,
  direct `file_*` values or file objects belonging to another principal.
- Treat authenticated typed outbound prepare calls as the current user's
  delegated authorization: return `approved` and allow the next exact commit
  without a browser prompt. Keep edit/delete behind the approval page.
- After diagnostic deploy, repeat the same current-conversation stage once and
  correct only the exact observed ingress policy before the approved Saved
  Messages publication acceptance.
- Keep OpenCode/local-client upload transport separate from ChatGPT fileParams:
  a future local bridge streams bytes to an authenticated bounded upload
  endpoint and receives the same opaque `ast_*`; server filesystem paths are
  never accepted as media identities.

## Follow-up Actions

- [x] Add a permanent real-shape ChatGPT fileParams regression.
- [x] Add safe asset-staging error codes and production audit dimensions.
- [x] Complete the real ChatGPT staging retry and verify ready `ast_*` state.
- [x] Add a regression for PNG stage/status -> Saved prepare=`approved` ->
  one-use commit/read-after-write, plus foreign-principal and old-preparation
  negative controls.
- [ ] After deploy and only after a fresh operator request, complete the Saved
  Messages image read-back. Do not reuse the stopped preparation.
- [ ] After the document candidate reaches exact `origin/main`, stage scoped
  config with the source-default-off flag, preflight, enable, then complete the
  actual ChatGPT APK -> Saved Messages live/negative/off-on rollback acceptance.
  Offline smoke/tests do not close this incident or the document rollout.

## Release And Closure Evidence

- ingress deployed SHA / release: `150358ed88b5239753bf3669a1f1e311bf3f63cc` / v1956
- no-second-prompt hotfix SHA / release:
  `d2b7993b41187660efa13d6d9070fda0c0d5a6cd` / v1957
- deploy path: `scripts/deploy_fly_main.sh` from clean exact `origin/main`
- regression checks: `372 passed`, compileall, Ruff, diff-check and both GitHub
  PR checks passed before merge; Codex remains the exact seven evidence tools
- post-deploy verification: in-container SHA matched; public `/healthz` returned
  `ok=true`, `ready=true`, `db=ok`, no issues; Fly machine check passed;
  Telegram webhook remained configured with zero pending updates and its one
  deploy-window 5xx timestamp did not recur; both SQLite databases returned
  `quick_check=ok`, the MCP auth DB mode was `0600`, runtime log mirror was
  present with no recent traceback/disk-full/MCP error, and the stopped
  preparation had zero provider-operation rows
- remaining closure gate: one fresh operator-requested ChatGPT PNG -> `ast_*`
  -> direct approved preparation -> commit -> Saved Messages read-back; the
  stopped preparation must never be reused
- document extension: no merged-main/Fly SHA or real ChatGPT APK/Saved
  Messages/rollback evidence has been recorded yet; this remains explicitly
  pending and does not change `Status: open`

## Prevention

The production acceptance gate will include one fresh explicit ChatGPT
conversation image through staging, status, direct approved preparation, commit
and Saved Messages read-back rather than relying only on synthetic fixtures.
Stopped or previously awaiting preparations are never part of that canary.
