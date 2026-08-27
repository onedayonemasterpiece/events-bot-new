# INC-2026-08-27 eventsBot VK image publish outcome unknown

Status: closed
Severity: sev2
Service: private eventsBot MCP / VK Social Workspace publishing
Opened: 2026-08-27
Closed: 2026-08-27
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
- 2026-08-27 16:30 UTC — the first catch-up acceptance run stopped safely at
  `wall_photo_multipart`; `photos.saveWallPhoto` and `wall.post` were not called.
- 2026-08-27 16:33 UTC — an exact production transport probe proved that VK's
  upload endpoint returned HTTP 200 JSON with `Content-Encoding: gzip`, while
  the pinned MCP session had automatic decompression disabled and attempted to
  parse the compressed 1,029-byte body as JSON.
- 2026-08-27 16:38 UTC — gzip correction PR #594 merged to `origin/main` at
  `962ced8639b0af57108cecfaf78dd6cdf3be25fe` after all three required checks.
- 2026-08-27 16:42 UTC — Fly release v2048 deployed that exact main SHA and
  passed machine smoke/health checks.
- 2026-08-27 16:44 UTC — one no-duplicate catch-up commit succeeded as
  `op_oj_QAg90nhLgc6_tt8uTZjGROtohCj_y`; immediate MCP read-after-write and
  authenticated VK readback verified `wall-231828790_1751` with one photo.
- 2026-08-27 16:45 UTC — the temporary exact-host ingress allowance used only
  for the operator-authorized source image was removed; Fly release v2050 runs
  the same v2048 image/SHA with the original allowlist restored.

## Root Cause

Production evidence localizes the failed attempt to the multipart step between
the one successful upload-server API call and the never-started photo-save
call. The original low-level exception remains unrecoverable because the
adapter discarded its class/stage and runtime logs recorded only the outer HTTP
request. A post-deploy acceptance attempt failed at the same fixed stage without
calling either later mutation, which made an exact boundary probe safe. That
probe confirmed the operative transport defect: VK returned the valid upload
receipt with HTTP 200 and `Content-Encoding: gzip`, while the pinned aiohttp
session set `auto_decompress=False`; the transport consequently parsed
compressed bytes as JSON and normalized the decode failure to
`media_upload_failed`.

The same transport also called `aiohttp.StreamReader.read(limit)` once and
treated a short currently available chunk as EOF. That independently confirmed
fragmentation defect could truncate an otherwise valid JSON receipt. Both
defects existed in the exact localized stage and are fixed: content decoding is
enabled before the decoded-byte cap, and the response is consumed to EOF. The
gzip response is directly confirmed production evidence; attribution of the
unrecoverable original attempt to either one of the two defects remains an
inference, not a recovered traceback.

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
- Provider preflight proved zero exact/unique live, postponed or personal-wall
  copies before the catch-up. The final post now exists once on the intended
  community wall.

## Corrective Actions

- [x] Persist and return the real preparation-to-operation mapping.
- [x] Preserve definitive VK API error class/stage and wall-write uncertainty.
- [x] Add bounded exact VK wall reconciliation without retrying `wall.post`.
- [x] Enforce inclusive content date bounds and date-bound cursors.
- [x] Read multipart upload responses to EOF under the byte cap and split media
  upload onto an explicit user-token actor.
- [x] Decode gzip-encoded VK upload receipts before bounded JSON parsing.
- [x] Deploy and perform one verified catch-up publish.

## Follow-up Actions

- [x] Persist provider-stage start/finish, fixed method, attempt number,
  sanitized outcome/error and encrypted native result identifiers without
  payload text, tokens or upload URLs.
- [ ] Re-audit ChatGPT action publication only if a public tool schema changes.

## Release And Closure Evidence

- implementation commits: `2832392f9`, `3b762385d`, `d06da27bf`,
  `b4ae7de37`; merged through PR #593 (`2bda57aba`) and PR #594
  (`962ced8639b0af57108cecfaf78dd6cdf3be25fe`).
- deployed SHA: `962ced8639b0af57108cecfaf78dd6cdf3be25fe`, Fly code release
  v2048. Releases v2049/v2050 only applied and then restored the exact source
  host allowlist while retaining the same image SHA.
- deploy path: clean detached worktree at exact `origin/main` via
  `scripts/deploy_fly_main.sh --remote-only --depot=false`; Fly machine smoke,
  DNS and health checks passed. Final `/healthz` returned `ok=true`,
  `ready=true`, no issues, and `/app/.static-site-repo-sha` matched the deployed
  SHA.
- tests: complete Private Events MCP suite `515 passed` with three existing
  aiohttp `NotAppKeyWarning` warnings; VK media focus `18 passed`; compileall
  and diff-check passed; all three GitHub PR checks passed.
- failed acceptance evidence: `op_eO4_PDC1b3hVe1ckPwhzQaYJHHMFS8XI`
  recorded successful `photos.getWallUploadServer` followed by definite
  `wall_photo_multipart/media_upload_failed`; no photo-save or wall-post attempt
  exists.
- successful provider-stage evidence:
  `op_oj_QAg90nhLgc6_tt8uTZjGROtohCj_y` recorded four ordered HTTP 200 stages —
  upload server, multipart, `photos.saveWallPhoto`, and `wall.post` — all
  `succeeded`; native photo/post results remain encrypted. Provider SQLite
  `quick_check` is `ok`.
- final MCP receipt: preparation
  `prep_IaXDl8rTSmfr_5ZgPrS0-bAtPVSFTsPz`, operation
  `op_oj_QAg90nhLgc6_tt8uTZjGROtohCj_y`, item
  `itm_tc--yIVvkVgfSFo6TG89cuIsEFHUJchS`, status `succeeded`. Preparation and
  operation status converged; immediate read-after-write was verified. Exact
  item read returned the requested text and one media ref; an inclusive
  2026-08-27 feed read returned exactly one exact-text item with one media ref.
- public VK post: `https://vk.com/wall-231828790_1751`, provider timestamp
  `2026-08-27T16:44:20Z`. Authenticated `wall.getById` returned owner
  `-231828790`, post `1751`, exact text SHA-256
  `8475ab426244a6492f23afa1f7a83e5a14331bda8bef824005a35696b97da9ed`,
  exact `Фото: МК` footer, and exactly one `photo` attachment.
- duplicate guard: exact/unique match count is one live post, zero postponed
  posts and zero token-personal-wall posts. Telegram received no call or replay.
- staged source evidence: JPEG, 547,392 bytes, digest
  `sha256:049cd26a439e9af1e56fd8f9dc5333af621e21f0d8763fad927815af63063493`;
  temporary `static.mk.ru` allowance is absent from the final environment.

## Prevention

The regression suite includes gzip-enabled multipart sessions, fragmented
multipart JSON, pre-wall multipart failure classification, definite VK API
rejection, mutation timeout,
restart-safe exact wall reconciliation, preparation/operation convergence,
non-success audit classification, inclusive date filtering and date-bound
cursors. Closure evidence above proves the repaired action path, one-post
catch-up, direct provider readback, MCP item/feed projection and no-duplicate
guard while leaving Telegram untouched.
