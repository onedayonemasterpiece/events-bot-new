# INC-2026-08-24 MCP Telegram albums returned unusable media references

Status: open
Severity: sev2
Service: private eventsBot MCP social workspace, Telegram reads and image preview
Opened: 2026-08-24
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-08-15-audio-mcp-runtime-catalog-truncation`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The production `eventsBot` MCP could read Telegram message metadata and return
an `ast_*` token for attached media, but `social_asset_preview` rejected that
same token instead of returning image pixels. Telegram grouped media was also
projected one provider message at a time, so an album could consume several
feed slots and an exact-item read exposed only one album member.

## User / Business Impact

- A ChatGPT user asked the MCP to inspect two private-chat messages containing
  several images each, but the model could not receive the pixels needed for
  the requested visual work.
- The model produced a semantic substitute and correctly disclosed that it was
  not a raster-grounded result, but the principal product capability—reading
  the supplied Telegram images—was unavailable.
- The defect affects both ordinary Telegram message media and Telegram story
  media because they share the read-ref materialization boundary.

## Detection

- The user reported the failed read of private chat `KenigEvents · UI review`
  and message `1162` on 2026-08-24.
- Production `social_workspace_audit` contained 17 denied Telegram
  `asset_preview` calls from 12:04 through 12:27 UTC on 2026-08-24, while
  Telegram item/story reads succeeded and reported media.
- The same database contained no outer `ref_kind='asset'` rows. That absence,
  despite media-bearing successful reads, identified the broken boundary.
- Existing audit rows recorded only `socialworkspaceruntimeerror`, which was
  sufficient to count failures but not to distinguish missing outer binding
  from provider download or image decoding failure.

## Timeline

- 2026-08-24: user requested visual inspection of two multi-image Telegram
  messages through `eventsBot` MCP.
- 2026-08-24 12:04–12:27 UTC: production recorded repeated denied
  `asset_preview` calls.
- 2026-08-24 13:20 UTC: incident investigation verified runtime-file logging,
  the MCP feature switch, production health, audit history and the missing
  outer asset rows.
- 2026-08-24: a failing local read-to-preview regression and two Telegram album
  regressions reproduced both defects.
- 2026-08-24: a fix was prepared in an isolated branch. At the user's request,
  no deploy, production config/DB mutation, or further live MCP/Telegram probe
  was performed while another ChatGPT window was actively reading through the
  connector.

## Root Cause

1. Telegram/VK adapters return provider-owned opaque media strings inside an
   item's `media` array.
2. `SocialWorkspaceRuntime._sanitize_provider_output` minted principal-bound
   outer refs only for scalar fields named `asset_ref`; it did not treat scalar
   members of the `media` array as assets.
3. The adapter's inner `ast_*` token therefore crossed the public MCP boundary
   unchanged. `social_asset_preview` correctly required an outer
   `social_workspace_ref` row and rejected the unbound token.
4. Independently, `TelegramWorkspaceAdapter._item_payload` accepted one
   Telegram `Message` and minted at most one media ref. Feed pagination counted
   each `grouped_id` member separately, and exact-item reads did not fetch the
   selected member's album siblings.

## Contributing Factors

- The preview unit test manually minted a valid outer asset ref before calling
  `social_asset_preview`; it did not exercise `social_content_* -> media[] ->
  social_asset_preview` end to end.
- Telegram tests covered story metadata and ordinary feed bounds but not
  `grouped_id` album expansion/collapse.
- The public and provider-internal asset tokens intentionally share the
  `ast_*` prefix, so schema validation alone could not detect that the wrong
  trust-boundary token escaped.
- Asset-preview audit reasons collapsed all runtime failures to the exception
  class, slowing diagnosis.

## Automation Contract

### Treat as regression guard when

- changing social read output sanitization or opaque-ref persistence;
- changing Telegram message/story media materialization;
- changing Telegram feed/search/exact-item pagination or `grouped_id` handling;
- changing `social_asset_preview`, its scopes, response image block or provider
  asset reader.

### Affected surfaces

- `private_events_mcp/social_workspace_runtime.py::_sanitize_provider_output`;
- `private_events_mcp/social_workspace_runtime.py::asset_preview`;
- `private_events_mcp_telegram_adapter.py::_item_payload` and Telegram
  list/search/get-item reads;
- isolated `/data/private-events-mcp-auth.sqlite` outer and provider binding
  tables;
- ChatGPT `eventsBot` connector tools `social_content_feed`,
  `social_content_item`, `social_content_stories`, and
  `social_asset_preview`.

### Mandatory checks before closure or deploy

- prove a media-bearing social read stores and returns a new principal-bound
  outer asset ref rather than the adapter's inner token;
- call `social_asset_preview` with every returned image ref and verify a
  bounded MCP JPEG image block;
- prove a Telegram feed containing two `grouped_id` albums returns two logical
  items with every image in original order, up to Telegram's ten-item cap;
- prove an exact-item read from any album member expands the whole album;
- run the full private MCP test glob and compile checks;
- after the active external read finishes and release is authorized, deploy an
  exact clean `origin/main` SHA and repeat the real ChatGPT read-to-preview flow
  without exposing provider/native refs;
- verify `/healthz`, MCP audit outcomes, runtime logs, and no regression to the
  fixed evidence-only Codex catalogue.

### Required evidence

- failing-before/passing-after regression outputs;
- production audit counts and outer-asset-row evidence;
- merged SHA reachable from `origin/main` and deployed machine version;
- real ChatGPT response containing all expected image blocks for both albums;
- post-deploy health and sanitized MCP audit receipt.

## Immediate Mitigation

- No misleading pixel-grounded reconstruction was claimed; the failed ChatGPT
  response explicitly labeled its fallback as semantic rather than raster
  traced.
- Production was left unchanged on the user's instruction while another MCP
  read session remained active.

## Corrective Actions

- Prepared outer binding for scalar refs nested in provider `media` arrays, so
  the read result and preview tool use the same principal/provider binding.
- Prepared Telegram `grouped_id` collapse/expansion for feed and exact-item
  reads, preserving ordered image refs and logical-post pagination.
- Added read-to-preview and album regression tests that failed on the deployed
  code shape and pass with the prepared fix.

## Follow-up Actions

- [ ] Merge and deploy only after the user confirms the active MCP read has
  finished.
- [ ] Run the real two-album ChatGPT acceptance and record sanitized evidence.
- [ ] Add stable stage-specific denial reasons for asset-preview observability
  without exposing refs, provider IDs or exception text.

## Release And Closure Evidence

- deployed SHA: pending; production change explicitly deferred by user
- deploy path: pending; must be exact merged `origin/main`
- regression checks: local targeted tests passing; complete suite pending final
  release candidate
- post-deploy verification: pending

## Prevention

- A provider-owned token and a public outer token may share a syntactic prefix,
  but no read-media contract is accepted unless persistence proves the outer
  principal binding.
- Album semantics are tested at both list pagination and exact-item expansion,
  rather than inferred from Telegram's individual message objects.
- Closure requires the actual ChatGPT content-block path; a direct runtime unit
  test or metadata-only story read is not sufficient.
