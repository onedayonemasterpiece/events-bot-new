# INC-2026-08-24 MCP Telegram albums returned unusable media references

Status: open — serialized audio-dispatch hotfix pending
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

The follow-on integration keeps that album/outer-ref fix while adding the same
closed media-detail projection and cache-first voice/audio enrichment to
Telegram item/feed/search/thread reads. This is additive: `media[]` remains an
ordered array of principal-bound outer refs, and no provider-native identity is
accepted as a public reference or returned as metadata.

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

- The user reported a failed read of an operator-authorized private chat item
  on 2026-08-24. The private target and native item identity are intentionally
  omitted from this public incident record.
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
- 2026-08-25 20:26 UTC: reviewed integration PR `#575` merged to `main` after
  all three required CI jobs passed.
- 2026-08-25 20:31 UTC: exact merged SHA deployed as Fly release `v2030`;
  `/healthz` was ready and the in-image SHA matched `origin/main`.
- 2026-08-25 20:36 UTC: the sanitized existing-connection canary resolved and
  read the private target and found multiple voice attachments, but every
  enrichment returned the generic isolated failure and no audio job row was
  created. No transcript or provider/native identity was recorded.
- 2026-08-25 20:40 UTC: a failing regression proved the generic audio store's
  512 MiB ceiling was being passed to the Telegram adapter, whose closed
  provider-media request contract accepts at most 30 MiB.
- 2026-08-25 20:48 UTC: reviewed hotfix PR `#576` was deployed from exact
  `origin/main` as Fly release `v2031`; health and the in-image SHA matched.
- 2026-08-25 20:49–20:54 UTC: the repeated sanitized canary proved provider
  byte ingress now worked, but also exposed an unstable cache identity: a
  refreshed Telegram download capability produced a new job on each read.
  The canary was stopped, 103 still-queued duplicate jobs were retired while
  retaining one active/queued job per observed content group, and no private
  target, transcript or provider-native identifier was recorded.
- 2026-08-25 21:08 UTC: the stable-identity fix from PR `#577` was deployed as
  exact-main Fly release `v2032`. A bounded repeat canary created exactly 12
  jobs for 12 discovered voice attachments and the count remained stable on
  repeat reads, proving the cache identity fix.
- 2026-08-25 21:10–21:20 UTC: the canary exposed a second multi-item dispatch
  defect. Although the backend semaphore serialized dispatch bodies, every
  queued attachment still owned a waiting task and probed the same shared
  Kaggle/Telegram session in a burst. Kaggle began returning HTTP 429 for
  status reads; the 12 canonical jobs remained safely queued and no further
  duplicate jobs were created.

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
5. The follow-on audio integration selected only
   `AudioTranscriptionService.config.max_asset_bytes` for the provider download
   request. Production uses 512 MiB for generic audio ingress, while the
   Telegram adapter intentionally accepts a maximum 30 MiB provider read
   bound. The adapter therefore rejected the request before downloading bytes
   or creating a durable job; per-attachment isolation correctly kept the
   thread response available but projected a generic transcription failure.
6. The provider-media identity HMAC also included Telegram `file_reference`.
   That field is an expiring download capability which may rotate whenever the
   same message is fetched; it is not durable media identity. Repeat reads
   therefore bypassed the intended idempotency lookup and created duplicate
   transcription jobs even though the provider object, message and bytes were
   unchanged.
7. `AudioTranscriptionService` created a dispatch task for every durable queued
   job. Its semaphore serialized the backend calls but did not serialize the
   remote-session guard/status probes performed by those waiting tasks. A
   multi-voice thread therefore produced a status burst against one shared
   Kaggle kernel and could exhaust the provider's request allowance.

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
- Synthetic cache tests supplied an already-stable fingerprint and therefore
  did not exercise the production ref store with a rotated Telegram download
  capability.
- Dispatch tests covered one job and backend mutual exclusion, but not a
  provider read that creates many durable queued jobs at once.

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
- Telegram canonical public/private item-link resolution, attachment
  classification, and trusted provider-byte audio ingress;
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
- prove public/private canonical item links, malformed/unavailable sanitization,
  VK exact-link stability, every supported Telegram media classification,
  transcription cache/dedup/opt-out/failure isolation, and absence of sensitive
  provider data in response/error/audit;
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
- [ ] Merge/deploy the provider-limit negotiation hotfix, then repeat the same
  existing-connection voice canary through ready/cache-hit without manual
  reconnect.
- [ ] Exclude expiring Telegram download capability material from durable media
  identity, deploy the regression, and repeat the canary without new duplicate
  jobs.
- [ ] Keep queued audio jobs durable without creating concurrent waiting
  dispatch tasks; schedule only the oldest job after the active run is terminal
  and apply a bounded global backoff when the shared session is busy.

## Release And Closure Evidence

- prepared fix: `6f95dec39` on draft PR
  [#572](https://github.com/onedayonemasterpiece/events-bot-new/pull/572);
  the PR is intentionally held from merge/deploy during the active user read
- integration PR: [#575](https://github.com/onedayonemasterpiece/events-bot-new/pull/575),
  merged as `980a5694cf0c172d07f4082dfbfd8f2cd1837a43`
- audio-bound hotfix PR:
  [#576](https://github.com/onedayonemasterpiece/events-bot-new/pull/576),
  merged/deployed as `4a81d12e9d24cf7b978b5f5bbe7d80cf36643e03`,
  Fly `v2031`
- stable-identity hotfix PR:
  [#577](https://github.com/onedayonemasterpiece/events-bot-new/pull/577),
  merged/deployed as `ef165165cddd765e9d0bf9cb95c5ffb5a0034426`,
  Fly `v2032`
- deploy path: `scripts/deploy_fly_main.sh` from clean exact `origin/main`
- regression checks: integrated local focused private MCP/audio/Telegram/VK
  suites pass on the integration branch; complete release suite and live
  acceptance remain pending final release candidate
- post-deploy verification: provider byte ingress and stable repeat-read cache
  identity passed; ready-result acceptance is pending serialized dispatch and
  recovery from the provider status-rate limit

## Prevention

- A provider-owned token and a public outer token may share a syntactic prefix,
  but no read-media contract is accepted unless persistence proves the outer
  principal binding.
- Album semantics are tested at both list pagination and exact-item expansion,
  rather than inferred from Telegram's individual message objects.
- Closure requires the actual ChatGPT content-block path; a direct runtime unit
  test or metadata-only story read is not sufficient.
