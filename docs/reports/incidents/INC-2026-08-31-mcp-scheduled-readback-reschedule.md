# INC-2026-08-31 eventsBot MCP scheduled readback blocked a safe reschedule

Status: closed — exact-main fixes deployed; real MCP Saved Messages schedule/read/approve/delete/absence journey passed
Severity: sev1
Service: private eventsBot MCP / Telegram and VK scheduled publication
Opened: 2026-08-31
Closed: 2026-09-01
Owners: events-bot production / eventsBot MCP
Related incidents: `INC-2026-08-24-mcp-telegram-album-media-ref`, `INC-2026-08-25-chatgpt-frozen-mcp-actions`, `INC-2026-08-27-eventsbot-vk-image-publish-outcome-unknown`, `INC-2026-08-30-mcp-scheduled-readback-routing`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

A four-image publication was prepared for Telegram `@lovekenig` and VK owner
`-231828790`, initially for 2026-08-31 10:30 Europe/Kaliningrad. Telegram
operation `op_h5whKgjfe2rXj24zWU1yF5ynU9iEKt0T` crossed the runtime deadline,
then remained `outcome_unknown / reconciliation_pending` because its provider
claim had no terminal result and status reconciliation did not query Telegram.
Two historical VK operations failed definitely at the first image before any
wall write. After PRs `#600` and `#601`, fresh acceptance operations exposed two
residual defects: Telegram correctly proved zero scheduled/live matches but
persisted `reconciliation_no_match` as `outcome_unknown`; VK successfully
uploaded and saved image 1, then received an HTTP 200 receipt with an empty
`photo` for image 2 on both the initial attempt and its one permitted retry.

The requested move to 14:00 could not be performed safely while the old
Telegram album might already have been scheduled or published. The incident is
therefore fail-closed for Codex publication. Codex did not publish, schedule,
delete, retry, alter the caption, or regenerate the four images; the explicit
handoff leaves any later production publication to ChatGPT through eventsBot
MCP.

The later ChatGPT-owned attempt for 21:15 exposed a separate control-plane
failure after the Telegram timeout repair. Production received four real
`social_action_commit` requests, but all four were denied before an operation
row or provider call because the caller-wide daily attempts bucket was already
`10/10`. Earlier Telegram/VK incident attempts had consumed that aggregate even
though the new target/action buckets were below their limits. The public tool
collapsed the exact local cause into `social workspace request rejected`, while
the visible ChatGPT tool journal omitted the four commit calls and the assistant
reported an incorrect count/cause. The real `@lovekenig` album therefore still
was not scheduled or published.

Closure required a provider-backed test rather than another fake-adapter claim.
After the durable binding-capacity and scheduled-delete-preview fixes reached
Fly releases `v2063` and `v2064`, the real eventsBot MCP staged four fresh PNGs
and scheduled one native Saved Messages album. MCP readback returned exactly one
logical item with `media_count=4`; the external operator page rendered and
approved its exact delete; MCP deletion succeeded and both MCP plus independent
Telegram reads proved zero scheduled and zero exact live copies. This closes the
server/live-MCP incident. It does **not** authorize Codex to publish the real
`@lovekenig` album: that separate publication remains owned by ChatGPT after the
user supplies a new future time.

## User / Business Impact

- The intended coordinated Telegram/VK publication was not safely completed.
- The Telegram outcome could not be distinguished from a successful provider
  mutation whose local receipt was cancelled; blindly restaging it could create
  a duplicate.
- The caller could not independently inspect either provider's scheduled queue
  through the public MCP contract or safely cancel the exact old Telegram
  album before the requested new slot.
- Both VK attempts were definitely pre-wall failures, but the existing retry
  ergonomics required a new preparation/idempotency key instead of one bounded,
  single-flight retry of the same logical action.

## Detection

- Commit/status receipts showed Telegram `outcome_unknown / provider_timeout`
  followed by repeated `outcome_unknown / reconciliation_pending`, with
  `retry_safe=false`.
- Read-only production database inspection found the Telegram provider row
  unchanged since `2026-08-31T07:18:58.636Z` with `result_json IS NULL`.
- The deployed runtime and adapter both used the same 12-second default because
  the provider-timeout env override was unset. Source inspection confirmed that
  the runtime's outer `asyncio.wait_for(...)` could cancel the adapter after its
  provider claim and before durable finalization.
- Both VK operation ledgers —
  `op_FQ1cqeUSAeS2mMZDXvD3tGvTz3hyswe1` and
  `op_noXSTwUeheMYYBLShjjIC_7w2_a3UpLy` — showed the first ordered image
  (`00_cover.png`) reached upload-server and `wall_photo_multipart` with HTTP
  200, but its normalized `photo` and/or `hash` was empty. Neither operation
  reached `photos.saveWallPhoto` or `wall.post`.
- Authenticated read-only VK verification found exactly **0** matching items in
  the postponed queue and exactly **0** matching text-plus-four-photo items in
  the latest 100 live owner-wall items.
- Fly release `v2053` ran SHA
  `64f75d10f7aff33fa616cee212878bd9d03673b1`, matching the audited
  `origin/main`. `/healthz` returned `ok=true`, `ready=true`, `db=ok`; both
  SQLite databases returned `PRAGMA quick_check=ok`. The production file mirror
  governed by `ENABLE_RUNTIME_FILE_LOGGING` / `RUNTIME_LOG_DIR` was enabled,
  present, fresh, and within its 64 MiB budget. This proves the service was
  otherwise healthy; it does not resolve the old Telegram outcome.
- Exact production release `v2058` / machine version `2058` ran immutable
  source SHA `30376f702342cd3fe57ee837800a58e01dc0cbec`; the relevant deployed
  source-file hashes matched both that commit and `origin/main`, whose
  `7b44306b0b58889506b987627fffb3848aa00ed6` head was documentation-only ahead.
- Preparations `prep_Llzoc5Wf1Lq28zbp7neiE1MJFy-8wer2` and
  `prep_76-3iOvPnmfpvf5SjJRCNfJo-1AVsQJw` durably reserved respectively
  `op_7txWNfvSt0tn_A6Trpo55TlfOfEYeDAN` and
  `op_DqVVWBsAyP8iifrq4ugROIgdUhX-WL5C`; neither operation had a
  `social_workspace_operation` row.
- Audit/access correlation proves commit ingresses at 18:41:01, 18:42:17,
  18:42:33 and 18:49:24 UTC. Each returned HTTP 200 with a small structured
  error in 6–8 ms and recorded `commit denied` before provider transport.
- The exact UTC-day budget row was already global `10` / principal `10` at
  17:14:15 UTC; production configured the principal ceiling to `10` while the
  global ceiling was `100`. The later commits therefore failed the principal
  `attempts budget exceeded` guard. Transaction rollback left both new
  operation tables and Telegram provider ledgers empty.
- After the commit fix reached Fly release `v2059`, a real Codex-selected
  deterministic PNG crossed the connector `fileParams` rewrite and was denied
  before download as `FILE_HOST_NOT_ALLOWED`. The audit fingerprint proved a
  three-label host under the stable `oaiusercontent.com` boundary, while
  production still allowed only `files.oaiusercontent.com` plus the Azure Blob
  family. OpenAI's current network guidance explicitly documents
  `*.oaiusercontent.com` for ChatGPT and Codex file traffic. No bytes were
  retained and no Telegram provider call occurred on the denied probe.

The exact audited payload identity was caption SHA-256
`b2bb4a7413b1cb63b2913a630a5cc6f1bce477fb9258a193fe4f14d7125328cd`
plus these ordered 1122×1402 PNG SHA-256 values:

1. `cec948b34c6fbf63fcc512310bfc29b11ffa1f68381477f132901592b08c4c48`
   (`00_cover.png`);
2. `3f094a48097a6ff711291435a5ba73b3b8649548926254a2ee992e6da86b9866`
   (`01_1714_paradeplatz.png`);
3. `1ea7340f519367ed98466666c36a4da3501571f7143dc80271d6639240addf86`
   (`02_1844_albertina.png`);
4. `9423f618c3246fd760f63dc8c8ec15da9e5db3b4b98cf61313e1983f90e7447e`
   (`03_1946_settlers.png`).

## Timeline

- 2026-09-01 06:27 UTC — PR `#606` merged the commit orchestration fix after
  all three required CI jobs passed. Exact `origin/main` SHA
  `9607e1977090729875b5f809d0465825246806c2` was deployed as Fly release
  `v2059`; the in-container SHA matched, `/healthz` was ready, both SQLite
  databases passed `quick_check`, and the runtime file mirror was fresh.
- 2026-09-01 06:34 UTC — the authenticated connector resolved Telegram Saved
  Messages and reported `schedule`, `delete`, images and `max_media_items=10`.
  A real locally selected canary PNG was materialized by the Codex tool host,
  but ingress returned `FILE_HOST_NOT_ALLOWED`; audit suffix fingerprints
  bound the new three-label host to `oaiusercontent.com`. This was a definite
  pre-download, pre-provider failure, so no scheduled item required cleanup.
- 2026-09-01 — OpenAI's official network guidance was compared with the live
  configuration. It requires the `*.oaiusercontent.com` family for ChatGPT and
  Codex features, establishing the narrow configuration correction; no generic
  URL downloader or non-OpenAI wildcard is introduced.
- 2026-09-01 07:52–08:03 UTC — exact `origin/main`
  `5602eaa7a36e8535935ee6fb7954a5e93c4b9caa` reached Fly release `v2060`
  with the corrected upload-host family. Four deterministic 96×96 PNG canaries
  crossed the real ChatGPT connector file materialization and server staging;
  every staged byte count, MIME, dimension and SHA-256 matched.
- 2026-09-01 07:54 UTC — a Saved Messages preparation reserved
  `op_vwK3UF6Y__P1L5Z-VL7-ooz4dEfc-2sK`; its single commit crossed the final
  Telegram mutation boundary. The adapter returned
  `outcome_unknown / provider_error`, and its immediate raw scheduled read also
  failed. An independent user-authorized Telegram provider read found exactly
  one logical album at `2026-09-01T08:20:00Z`, physical IDs `1–4`, caption
  SHA-256 `3c3db21049a16cdae3c52b017bd0acde797567dadf0a646a80fdcf2cc3db3397`
  and `media_count=4`. Thus the schedule mutation had succeeded and the public
  result was a false unknown caused by scheduled readback.
- 2026-09-01 07:59 UTC — the authorized canary album was deleted as one exact
  scheduled group; Telegram provider readback proved all IDs `1–4` absent and
  the Saved Messages queue contained zero physical/logical scheduled items.
  A second exact Telethon sequence reproduced the four-upload, album-send,
  raw-history and group-delete operations with IDs `5–8`; it also finished at
  zero queue items. No `@lovekenig` or VK mutation was made.
- 2026-09-01 08:09–08:18 UTC — PR `#608` merged and exact `origin/main`
  `ad5bac0d94bce506f20280adf6757954f80c4208` reached Fly release `v2061`.
  The embedded SHA, `/healthz`, both SQLite `quick_check` results and runtime
  mirror passed. Empty Saved Messages scheduled history now read successfully
  through MCP with canonical `InputPeerSelf`.
- 2026-09-01 08:18 UTC — a second four-image MCP canary reserved
  `op_kUmgPCArGPWR6bd2gqdGimh2jOWSahww` and crossed the Telegram mutation
  boundary exactly once. Telegram created one native scheduled album at
  `2026-09-01T08:45:00Z`, physical IDs `9–12`, caption SHA-256
  `68e9c7e168bad0e84271f8d736d2e5561a85f1016f2ac7407b37a843fa27e519`
  and `media_count=4`, but MCP again returned
  `outcome_unknown / provider_error`. The now-safe provider log classified the
  failure as `ProviderBindingError` after mutation; a non-empty scheduled-list
  call failed with the same class before mutation. The exact canary group was
  immediately deleted by the authorized provider cleanup and a fresh raw read
  proved zero scheduled items. No `@lovekenig` or VK mutation was made.
- 2026-09-01 08:25 UTC — isolated auth-DB forensics proved `tg_item` was exactly
  at its hard 20,000-row ceiling. Decryption/counting found only 11,552 unique
  native item coordinates and 8,448 duplicate bindings (up to 24 copies for one
  identity). `tg_asset` contained 15,435 rows: 7,522 immutable verified uploads
  plus 7,913 read bindings representing only 1,018 unique read identities
  (6,895 duplicates, up to 92 copies). The album mutation succeeded; minting
  its readback item ref then raised `provider reference capacity exhausted`.
- 2026-09-01 10:27–10:31 UTC — PR `#610` passed all three CI gates; exact
  `origin/main` `f473d120322436d901359834207884bd9b3d6850` reached Fly release
  `v2063`. Health, exact embedded SHA, both SQLite `quick_check` results and the
  runtime mirror passed. The real MCP then staged four fresh verified PNGs,
  prepared and committed `op_Gzo2EsTLuy4AIGUQlUYg0eSgKj3qcW_F` exactly once,
  and returned `succeeded` with verified read-after-write. A fresh scheduled
  read found exactly one native logical album at `2026-09-01T11:00:00Z`, caption
  SHA-256 `854c8b80d74ebda76f8a3742c98e047572c1d0c6390e3dec50977def4877d082`
  and `media_count=4`.
- 2026-09-01 10:31 UTC — the typed MCP delete preparation correctly required
  external operator approval, but its approval preview failed closed as
  `human item preview is unavailable`. Root-cause tracing proved
  `social_scheduled_items_list` minted a usable outer item ref but, unlike the
  ordinary read path, never stored the corresponding closed item preview. The
  exact canary group (physical IDs `13–16`) was immediately removed through the
  authorized emergency provider cleanup and raw history plus MCP both proved a
  zero-item queue. The canary did not reach its scheduled time; no `@lovekenig`
  or VK mutation occurred.
- 2026-09-01 10:42–10:45 UTC — PR `#611` passed all three CI gates and exact
  `origin/main` `ff4551e5cad36542d2dd13350c5c3e9028be5748` reached Fly release
  `v2064`. Embedded SHA, `/healthz`, both SQLite `quick_check` results and the
  runtime mirror passed. The eventsBot MCP staged four new verified 96×96 PNGs,
  prepared and committed schedule operation
  `op_wJmz7bGSHuaR-tgo1QU5Y2qzv7T3hgzZ` exactly once, and returned
  `succeeded / read_after_write.verified=true`. Scheduled-list readback found
  exactly one logical native album at `2026-09-01T11:15:00Z`, caption SHA-256
  `8cfc27d82939107c25c707eb80d4324f6dd65800425c0f204584828ed4173181`,
  four ordered image roles and `media_count=4`.
- 2026-09-01 10:44 UTC — the exact scheduled item entered the unchanged
  external approval flow. The preview displayed `Saved Messages`, action
  `delete`, schedule time, text hash and media count four. After operator
  confirmation, MCP committed delete operation
  `op_lcDhT_jwT4gC1z_fTMPIf8ahWOOPPcaf` exactly once and returned
  `succeeded / absence_verified=true`. Fresh exact-filter and whole-queue MCP
  reads both returned zero. An independent Telegram read found zero physical
  and logical scheduled items, and the latest 50 live Saved Messages contained
  zero exact caption matches. No test message reached its scheduled time.

- 2026-08-31, before 10:30 Europe/Kaliningrad — the four-image Telegram and VK
  schedules were prepared for the original slot.
- 2026-08-31 07:18:58.636 UTC — the durable Telegram provider claim was
  recorded; its result remained NULL after the runtime timeout.
- 2026-08-31 — repeated Telegram status calls returned
  `reconciliation_pending` without provider scheduled/live readback.
- 2026-08-31 — two VK attempts failed on image ordinal 1/4 after multipart HTTP
  200 and before photo save or wall post.
- 2026-08-31 — the requested target time changed to 14:00
  Europe/Kaliningrad. The reschedule was withheld because the 10:30 Telegram
  outcome was still unknown.
- 2026-08-31 — a read-only production audit verified release/health/runtime
  evidence and zero exact VK postponed/live copies. It performed no provider
  mutation and retained `BLOCKED_OLD_TELEGRAM_OUTCOME`.
- 2026-08-31 — the isolated integration branch implemented the durable
  cancellation/readback repair. Its focused integration suite passed 308 tests;
  the final complete Private Events MCP suite passed 546 tests with three unrelated
  existing aiohttp warnings; `compileall` and `git diff --check` passed. The
  independent checklist review found no remaining code blocker. The incident
  remains open pending PR CI, exact-main merge/deploy and operational readback.
- 2026-08-31 10:17 UTC — PR `#600` merged after all three required CI jobs
  passed. Clean exact `origin/main` SHA
  `5cb1fbc9e870890770ca89dfd44917feef0c40f1` was deployed as Fly release
  `v2054`.
- 2026-08-31 10:23–10:27 UTC — production health and both SQLite databases
  passed; the runtime mirror was fresh. Read-only provider-backed reconciliation
  terminalized the historical Telegram operation as
  `reconciliation_no_match`. Exact counts were Telegram scheduled/live `0 / 0`
  and VK postponed/live `0 / 0`. No publication, retry, cancellation or other
  provider-content mutation was performed.
- 2026-08-31 — PR `#601` merged and exact `origin/main`
  `ab289db1750d60242fde37f07af305a6e67b84fa` was deployed as Fly release
  `v2055` / machine version `2055`; `/healthz` remained ready.
- 2026-08-31 — fresh Telegram operation
  `op_XYhsfK0LxnCL7Qt8dm339S3HREWC6OQW` reached bounded zero scheduled/live
  matches but persisted the contradictory terminal-looking receipt
  `outcome_unknown / reconciliation_no_match / retry_safe=false`.
- 2026-08-31 11:35 UTC — fresh VK operation
  `op_rZ7orGaOgGbzsNmf6zl6NZsanxzaylOs` and its single permitted retry each
  uploaded image 1 and reached `photos.saveWallPhoto`; image 2 then returned
  HTTP 200 JSON with integer `server`, non-empty `hash`, and present string
  `photo` of length zero. Both attempts stopped before `wall.post`. Read-only
  postponed/live checks remained `0 / 0`. No additional provider mutation was
  performed for diagnosis.
- 2026-08-31 17:14:15–17:14:27 UTC — ChatGPT operation
  `op_BH5WmDcJ37MuxCOz6h6tT6wVvHuwUzhM` attempted the requested 20:00
  Telegram schedule. Runtime evidence shows four sequential uploads of about
  2.1–2.3 MiB; the fixed 12-second session deadline disconnected during the
  fourth upload. No final Telegram album/send request was emitted. Bounded
  scheduled/live readback found zero exact copies, but the upload path had
  already set `mutation_started_at_ms`, so the receipt incorrectly became
  `reconciliation_no_match / retry_safe=false`.
- 2026-08-31 17:49–17:55 UTC — PR `#604` merged as exact `origin/main`
  `30376f702342cd3fe57ee837800a58e01dc0cbec` and was deployed as Fly release
  `v2058` / machine version `2058`. All health, database and runtime-mirror
  checks passed. Post-deploy readback found zero exact scheduled copies, and
  repeated public channel read at 18:03:31 UTC still ended at message `12631`
  from 16:25:05 UTC, with no four-image album at or after the requested 18:00
  UTC slot. Codex did not call commit, retry, delete or any other
  provider-content mutation.
- 2026-08-31 18:40:57 UTC — first fresh four-image preparation was stored as
  approved with reserved `op_7tx...`; commit ingresses at 18:41:01, 18:42:17
  and 18:42:33 were denied before operation insertion/provider transport.
- 2026-08-31 18:49:18 UTC — second fresh preparation was stored as approved
  with reserved `op_Dq...`; the fourth commit ingress at 18:49:24 was denied at
  the same pre-provider budget guard.
- 2026-09-01 — forensic readback again found exact scheduled matches `0`; the
  public channel page still ended at message `12631` from 16:25:05 UTC on
  August 31. No VK or `@lovekenig` provider mutation was made during diagnosis.

## Root Cause

1. **Proven cancellation race:** runtime and Telegram adapter owned equal
   provider deadlines. The runtime wrapped the whole adapter call in an outer
   `asyncio.wait_for`, so cancellation could occur after the adapter claimed an
   operation or attempted mutation but before `_complete_operation()` or
   `_release_operation()` made the provider ledger terminal.
2. **Proven non-convergent ledger/reconciliation:** the Telegram provider row
   stored a nullable result but no durable intent, mutation state, attempt,
   lease/deadline, or stale-claim recovery evidence. `reconcile()` resolved only
   that row; a NULL result became `operation_in_progress`, which runtime mapped
   back to `reconciliation_pending` indefinitely instead of reading Telegram.
3. **Proven control-plane gap:** raw Telegram scheduled-history and internal VK
   postponed reads existed, but no narrow ChatGPT-visible scheduled-queue tool
   exposed them. Telegram scheduled deletion also used the ordinary message
   namespace, and VK deletion did not prove postponed-queue absence.
4. **Proven VK failure boundary:** both VK attempts returned a normalized
   multipart receipt with HTTP 200 but an empty `photo` and/or `hash`, so they
   failed before `photos.saveWallPhoto` and `wall.post`. The exact empty field
   and original provider response shape are **not recoverable** from the old
   evidence; attributing the response to compression, actor routing, or another
   upstream cause would be a hypothesis. Existing gzip decompression and
   read-to-EOF handling were already deployed and are not an evidenced root
   cause for these two attempts.
5. **Residual Telegram state bug:** the adapter's post-deadline zero-match
   branch explicitly constructed `status=outcome_unknown` together with
   `error_code=reconciliation_no_match`, and its early-return branch then
   treated that error as terminal. Runtime preserved the adapter receipt; it
   did not remap a correct terminal status. Thus repeated status calls stopped
   polling but exposed the wrong public state.
6. **Residual VK provider rejection plus diagnostic gap:** the fresh deployed
   transport correctly decoded gzip/JSON to EOF. Image 1 completed multipart
   and `photos.saveWallPhoto`; for image 2 VK itself returned the flat expected
   keys with `photo=""` twice. The adapter correctly refused to call
   `photos.saveWallPhoto` with that invalid receipt, but collapsed the exact
   structure into generic `media_upload_response_invalid`. Compression,
   ChatGPT ingress, PNG validity and actor routing are therefore not supported
   root causes. Why VK rejected that particular image remains provider-side
   and unproven; automatic metadata stripping or re-encoding is not justified
   by current evidence.
7. **Proven Telegram album timeout/boundary defect:** one scalar 12-second
   deadline covered connection, rights checks, every file upload, the final
   scheduled album request and read-after-write. The four verified PNG uploads
   alone consumed that budget. `_provider_media()` also marked each
   preparatory `upload_file` as the irreversible content mutation, even though
   Telegram had not yet received the final album scheduling call. This both
   prevented completion and falsely prohibited the safe retry.
8. **Residual Telegram retry wiring gap:** the public runtime exposed
   `social_action_retry`, but the Telegram adapter/provider store had no
   provider-side retry/rearm method; only VK implemented that half of the
   contract. A future correctly classified Telegram pre-mutation failure would
   therefore still be rejected as `provider retry is unavailable`.
9. **Proven caller-wide attempt-budget collision:** server wiring assigned the
   configured daily value `10` to the principal, target and action dimensions.
   Ten earlier Telegram/VK incident attempts across several targets/actions
   exhausted only the coarse principal bucket. The new schedule target/action
   were still below their own bounds, but every commit was transactionally
   rejected before operation insertion or `adapter.execute`.
10. **Proven safe-error loss:** the runtime raised the exact local
    `attempts budget exceeded` cause, then `social_workspace_tools.rejected()`
    replaced it with generic `social workspace request rejected`. The audit
    stored only the exception class, so neither ChatGPT nor an operator could
    distinguish budget, expiry, binding or asset preflight without DB forensics.
11. **Proven state-contract ambiguity:** prepare reserved an `operation_ref`
    internally but did not return it or state that commit was still required.
    Preparation status `approved` could therefore be narrated as an operation
    result even though no operation row/provider attempt existed. Status by the
    reserved operation was not a documented `not_started` state.
12. **Client/reporting observability failure:** the ChatGPT-visible journal
    omitted four server-proven commit calls, and assistant prose reported the
    wrong number and server cause. This is distinct from the server budget
    defect, but the ambiguous/generic server contract made the false report
    difficult to detect without production ingress evidence.
13. **Proven Saved Messages raw-peer mismatch:** the successful MCP mutation
    and independent provider read proved the album existed, while the adapter's
    immediate `messages.getScheduledHistory` and every later MCP scheduled read
    failed. The encrypted target binding replayed a detached Telethon `User`
    object into raw scheduled methods; the controlled sequence using Telegram's
    canonical `InputPeerSelf` completed schedule, read and delete. Raw Saved
    Messages scheduled-history/delete now construct `InputPeerSelf` explicitly;
    ordinary send/capability behavior is unchanged.
14. **Proven durable provider-binding capacity defect:** every Telegram
    target/item/read-media projection minted a random inner ref even when the
    native identity was unchanged. Production `tg_item` reached its fixed
    20,000-row ceiling with 8,448 duplicate rows. After Telegram successfully
    scheduled the v2061 canary, read-after-write failed while minting its item
    binding; the same full map made every non-empty scheduled-list projection
    fail. The opaque error boundary then represented the post-mutation binding
    failure as `outcome_unknown / provider_error`. This was not a four-image
    limit: target capabilities continued to report `max_media_items=10` and
    Telegram created the native four-image album correctly.
15. **Proven scheduled-delete approval-preview gap:** scheduled-list projection
    minted and returned principal-bound item refs but did not call the preview
    persistence used by ordinary item reads. Delete preparation therefore
    resolved the exact scheduled binding, while the independent browser
    approval correctly refused to render an opaque-only destructive action as
    `human item preview is unavailable`. The provider delete method itself was
    not reached by MCP; the canary was cleaned through the separately authorized
    exact provider path before its slot.

## Contributing Factors

- One short whole-operation deadline did not account for a multi-image,
  multi-stage provider action or leave a durable-finalization/readback margin.
- Existing Telegram schedule coverage exercised the success/read-after-write
  path, not outer cancellation after a provider mutation or restart recovery of
  an old NULL claim.
- The scheduled Telegram binding did not retain every physical album member
  needed for exact scheduled-namespace cancellation and absence proof.
- Existing VK diagnostics did not retain safe field types/lengths or decoded
  receipt structure, so the exact invalid field cannot be reconstructed after
  the fact.
- Prior successful ordinary or fast scheduled publications did not exercise
  the cancellation window, four-image VK chain, or stale-claim recovery path.
- The release gate exercised fake-provider success and transport failures but
  did not run the complete connector-like four-image
  `stage -> prepare -> pre-commit status -> commit -> readback` journey with a
  saturated coarse principal bucket.
- Incident diagnostics and intended product delivery shared one aggregate
  caller budget without reserving capacity per target/action.
- Fake-provider tests represented Saved Messages with the same generic entity
  shape as normal peers, so they did not enforce the provider's canonical
  `InputPeerSelf` constructor for raw scheduled-history/delete methods.
- Durable binding tests covered restart survival but not repeated projection of
  one native identity or operation when a per-kind map was already at capacity.
  Random refs turned ordinary reads into storage growth and obscured the exact
  local `provider reference capacity exhausted` cause behind the public safe
  error boundary.

## Automation Contract

### Treat as regression guard when

- changing runtime/provider timeout ownership, cancellation or durable
  operation finalization;
- changing Telegram provider claims, reconciliation, raw scheduled-history,
  logical album bindings or scheduled deletion;
- changing VK multipart upload/save, postponed/live readback, exact deletion,
  retry classification or provider-stage diagnostics;
- adding or changing a ChatGPT-visible scheduled-list/retry tool, its scopes,
  bounds, redaction or action-definition publication;
- changing attempt-budget dimensions, prepare/commit/status state semantics,
  commit idempotence, safe pre-provider errors or ingress audit stages;
- changing Telegram opaque target/item/read-media identity, retention or
  capacity behavior;
- reconciling or retrying an outcome-unknown scheduled publication.

### Affected surfaces

- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp/server.py`
- `private_events_mcp_telegram_adapter.py`
- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_vk_transport.py`
- `private_events_mcp_vk_upload.py`
- `private_events_mcp_workspace_providers.py`
- isolated auth/provider SQLite ledgers and encrypted intent/bindings
- ChatGPT action-control snapshot and existing Telegram/VK schedule scopes
- Fly runtime health, runtime-log mirror and exact-main release path

### Mandatory checks before closure or deploy

- prove runtime cancellation after the provider mutation boundary cannot leave
  a Telegram or VK claim indefinitely without a durable terminal/leased state;
- restart from a Telegram NULL-result claim and converge through raw
  `messages.GetScheduledHistoryRequest`, logical album collapse and, after the
  scheduled time, ordinary/live history;
- cover exactly one match, bounded zero-match consistency polling, terminal
  `reconciliation_no_match`, proven pre-mutation
  `provider_mutation_not_started`, and ambiguous duplicate matches without
  blind resend or delete;
- prove a four-image Telegram album is one logical scheduled item in immutable
  order, while the ordinary feed remains distinct from the scheduled queue;
- prove Telegram scheduled deletion uses
  `messages.DeleteScheduledMessagesRequest`, deletes every exact album member
  and verifies raw scheduled-queue absence; prove VK deletion targets the exact
  owner/post and verifies postponed-queue absence;
- expose `social_scheduled_items_list` under the existing
  `telegram:schedule` / `vk:schedule` scope families with a small hard limit,
  exact optional filters, opaque refs and no native identifiers, tokens, upload
  URLs or provider payloads; verify its default limit 10 and hard maximum 25;
- prove bounded retry accepts only a terminal `retry_safe=true` attempt, retains
  one logical action/preparation identity and prevents concurrent retries;
- cover four sequential VK PNG upload/save stages and one postponed wall post,
  flat/nested receipts, gzip, fragmented reads, empty/invalid photo/hash, exact
  postponed readback, and no later mutation after a definite pre-wall failure;
- run targeted regressions, the complete Private Events MCP suite, compileall
  and repository-standard diff/schema checks;
- before production deploy, fetch `origin`, use a clean exact `origin/main`,
  verify no unmerged release/hotfix drift and confirm the changelog matches the
  deployed commit;
- after deploy, verify exact SHA, Fly release, `/healthz`, both SQLite
  `quick_check` results, runtime mirror freshness/budget and the authenticated
  tool catalogue;
- publish/review the changed ChatGPT action definition and verify the new tools
  in a genuinely new chat; OAuth reconnect or direct `tools/list` alone is not
  acceptance.
- reproduce the production `10/10` principal collision and prove unrelated
  targets/actions no longer block a below-limit intended action while the
  narrow per-target/per-action limits remain `10`;
- prove prepare returns `commit_required`, `next_action`, the reserved
  operation and `not_started/provider_attempted=false`; status by either ref
  cannot represent pre-commit state as a provider operation;
- prove the exact two-field commit creates one operation/provider attempt,
  replay returns the same durable receipt without another provider call, and a
  pre-provider budget denial exposes a bounded code plus exact audit stages;
- repeatedly project the same Telegram target, ordinary/scheduled item and
  read-media identity across store restarts; prove stable refs and bounded row
  counts while staged upload refs remain immutable and distinct;
- after exact-main deploy and action refresh, run one four-fresh-image Saved
  Messages/test-channel schedule canary through the same MCP, prove one logical
  album, delete it immediately and prove scheduled/live absence. Never use
  `@lovekenig` for that canary.

### Required evidence

- merged and deployed exact-main SHA plus Fly release identifier;
- targeted/full-suite/compile/schema test receipts;
- sanitized historical receipts for all three operation refs, including
  provider stage, mutation-boundary state, retry safety and final readback;
- exact Telegram scheduled/live and VK postponed/live match counts for the
  immutable four-image payload;
- proof whether the old 10:30 Telegram album exists, was published, or was
  safely cancelled with absence verified;
- authenticated catalogue evidence for scheduled-list, scheduled-delete and
  bounded retry behavior in the refreshed ChatGPT action snapshot;
- `/healthz`, runtime-log mirror and both SQLite health receipts;
- confirmation that the delivered fix is reachable from `origin/main`.

## Immediate Mitigation

- Kept the operation fail-closed as `BLOCKED_OLD_TELEGRAM_OUTCOME` and did not
  create a 14:00 duplicate while the old Telegram result was unresolved.
- Confirmed through authenticated read-only VK inspection that there were zero
  exact postponed and zero exact live copies at the audited owner.
- Performed no publication, cancellation, retry, image regeneration or caption
  change from Codex.
- After exact-main deploy, confirmed zero exact old copies on both providers and
  produced the fail-closed machine handoff `READY_FOR_14_00_PUBLICATION`. The
  requested 14:00 Europe/Kaliningrad slot had not passed at readback time.

## Corrective Actions

- [x] Make the provider adapter the owner of its transport/session deadline;
  runtime must not cancel before durable finalization. Handle
  `asyncio.CancelledError` at the provider-operation boundary.
- [x] Persist encrypted/sanitized Telegram intent, mutation timestamps, bounded
  attempt/lease state and enough exact target/time/text/media evidence for
  restart-safe reconciliation, including a migration-safe legacy adoption path.
- [x] Reconcile scheduled Telegram operations against raw scheduled history and
  later live history, with exact, ambiguous, bounded-pending and terminal
  zero-match outcomes.
- [x] Add the narrow logical `social_scheduled_items_list` read for Telegram raw
  scheduled history and VK `wall.get(filter="postponed")` without a new OAuth
  scope family.
- [x] Make scheduled deletion namespace-aware and require exact absence
  readback for Telegram album members and VK postponed items.
- [x] Add one bounded, single-flight retry path for terminal
  `retry_safe=true` attempts under the same logical action/preparation.
- [x] Persist safe VK multipart/stage observability sufficient to distinguish
  response shape and empty-field failures without bodies, credentials, hashes
  or upload URLs.
- [x] Make post-deadline Telegram zero-match and duplicate-match outcomes
  terminal `failed`, normalize legacy no-match rows without provider I/O, and
  preserve fail-closed retry safety after a mutation attempt.
- [x] Classify VK multipart structural failures with exact safe subcodes and
  persist image count/input identity/JSON shape/field presence/reached-stage
  evidence without accepting empty provider fields.
- [x] Complete code review, tests, merge, exact-main deploy and operational
  reconciliation before changing the incident status.
- [x] Derive Telegram mutation deadlines from total verified media bytes and
  item count for the complete one-to-ten attachment envelope; keep the outer
  MCP commit/retry deadline above that bounded envelope.
- [x] Move the durable Telegram content-mutation marker from preparatory file
  upload to immediately before the final message/album/story send, and cover
  pre-send upload timeout as `provider_mutation_not_started / retry_safe=true`.
- [x] Implement Telegram provider-ledger CAS rearm and adapter retry so the
  existing `social_action_retry` tool can actually perform attempt 2 or 3 for a
  proven pre-content-mutation failure.
- [x] Deploy from exact `origin/main` and prove zero exact Telegram
  scheduled/live copies. The final four-image publication remains owned by
  ChatGPT through eventsBot MCP; Codex must not publish it. If a provider
  mutation smoke is still necessary, use a separate test payload and delete it
  immediately with verified absence.
- [x] Separate principal/global abuse ceilings from the configured narrow
  per-target/per-action attempt limit so unrelated incident attempts cannot
  consume product delivery capacity.
- [x] Make prepare/status explicitly expose `commit_required`, next action,
  reserved operation, `not_started`, provider-attempt and mutation-boundary
  state; make exact commit replay idempotent without a second adapter call.
- [x] Preserve bounded pre-provider error codes and commit-ingress audit stages
  instead of collapsing every rejection to a generic invalid argument.
- [x] Replace random high-churn Telegram target/item/read-media inner refs with
  secret-bound stable native-identity refs, retain random immutable upload-stage
  refs, and add immediate capacity headroom plus repeat-read row-count tests.
- [x] Persist a closed scheduled-item preview (source target, queue/time, text
  hash and media shape) when `social_scheduled_items_list` projects an item so
  the existing exact external-approval delete path can render and approve it.
- [x] Merge/deploy exact main, refresh the eventsBot MCP action snapshot, pass the
  four-image Saved Messages schedule/delete canary and only then return the
  real publication task to ChatGPT.

## Follow-up Actions

- [x] eventsBot MCP owner — reconcile the historical Telegram operation through
  provider-backed scheduled/live reads after the fixed exact-main release.
- [x] eventsBot MCP operator — verify the live current catalogue through the
  installed connector and complete the real scheduled-list/delete journey.
  The user's real publication still starts as a fresh ChatGPT conversation/action.
- [x] incident owner — produce a machine-readable operational handoff with one
  allowed status: `READY_FOR_14_00_PUBLICATION`,
  `BLOCKED_OLD_TELEGRAM_OUTCOME`, `BLOCKED_PROVIDER_READBACK`, or
  `BLOCKED_DEPLOYMENT`.
- [x] incident owner — if the requested slot has passed, stop rather than
  publishing immediately or silently changing the time.

## Release And Closure Evidence

- implementation branch head: `632b9c056085b74e337ee46ecb137e2172c58e31`
- merged/deployed implementation `origin/main` SHA:
  `5cb1fbc9e870890770ca89dfd44917feef0c40f1`
- deployed Fly release: `v2054`, machine version `2054`, one passing check
- deploy path: clean detached worktree at exact `origin/main` through
  `scripts/deploy_fly_main.sh --remote-only --depot=false`
- regression checks: final local Private Events MCP suite `546 passed`, three
  existing aiohttp warnings; independent focused review `168 passed`; PR CI
  `python-ci`, `smart-update-identity-state-machine` and
  `static-browser-release-gate` all passed; `compileall`/diff checks passed
- action-definition/live-connector acceptance: passed through the installed
  eventsBot MCP connector in the v2064 schedule/list/approve/delete journey;
  the real `@lovekenig` publication remains a new ChatGPT action
- old Telegram operation reconciliation: terminal
  legacy receipt `outcome_unknown / reconciliation_no_match / retry_safe=false`,
  attempt `1`; the residual fix normalizes that combination to terminal
  `failed` on its next read without provider I/O
- exact Telegram scheduled/live counts: `0 / 0` (12 scheduled logical items and
  77 live logical items scanned); no old item existed, so no cancellation was
  attempted or required
- exact VK postponed/live counts: `0 / 0` (latest 100 live owner items scanned);
  both historical failures remain definite pre-wall
  `media_upload_response_invalid / retry_safe=true`, image ordinal 1, mutation
  boundary not reached
- live duplicate check: zero exact copies on both platforms
- deployed catalogue: `social_scheduled_items_list` read-only under the existing
  Telegram/VK schedule/publish scopes; `social_action_retry`, status and the
  existing prepare/commit delete path present. Scheduled namespace/absence
  behavior is covered by the deployed code and regression suite.
- post-deploy health: `/healthz ok=true ready=true db=ok`; both SQLite
  `PRAGMA quick_check=ok`; runtime file mirror enabled and fresh; immutable
  in-image SHA matched the deployed implementation SHA
- historical machine handoff before the requested slot passed:
  `READY_FOR_14_00_PUBLICATION`; that handoff is no longer an authorization to
  publish late, and Codex performed no catch-up or silent reschedule
- former client-control closure gate: satisfied by the installed connector's
  real typed calls, not catalogue inspection alone; the final production post
  is intentionally outside this diagnostic acceptance
- residual implementation base: `ab289db1750d60242fde37f07af305a6e67b84fa`
  (Fly `v2055`); commits `59296ccd5` and `0e91636e8`; PR `#602`; merged and
  deployed exact-main SHA `6b43c043bf3c59dacb8e8f1ee7e2bcdee2e91a09`
  as Fly release/machine version `v2056` / `2056`
- residual regression evidence: focused `17 passed`; complete
  `tests/test_private_events_mcp_*.py` `555 passed` with the same three aiohttp
  warnings; Ruff, `compileall` and `git diff --check` passed; PR `#602`
  `python-ci`, `smart-update-identity-state-machine` and
  `static-browser-release-gate` passed
- residual post-deploy evidence: `/healthz ok=true ready=true db=ok`; Fly check
  passing; both SQLite `PRAGMA quick_check=ok`; runtime file mirror enabled,
  present and fresh; immutable image SHA exactly
  `6b43c043bf3c59dacb8e8f1ee7e2bcdee2e91a09`
- Telegram album timeout/retry hotfix: commits `a1b89a938` and `993fe660d`, PR
  `#604`; merged/deployed exact-main SHA
  `30376f702342cd3fe57ee837800a58e01dc0cbec` as Fly release/machine version
  `v2058` / `2058`
- hotfix regression evidence: focused `173 passed`; complete
  `tests/test_private_events_mcp_*.py` `558 passed` with the same three
  existing aiohttp warnings; `compileall` and `git diff --check` passed; PR
  `#604` `python-ci`, `smart-update-identity-state-machine` and
  `static-browser-release-gate` all passed
- hotfix post-deploy evidence: immutable in-image SHA exactly
  `30376f702342cd3fe57ee837800a58e01dc0cbec`; `/healthz ok=true ready=true
  db=ok`; one Fly check passing; both SQLite `PRAGMA quick_check=ok`; runtime
  mirror fresh; authenticated exact scheduled count `0`; repeated public live
  feed read at 18:03:31 UTC had no new four-image album at/after 18:00 UTC
- operation `op_BH5WmDcJ37MuxCOz6h6tT6wVvHuwUzhM` remains the immutable
  historical failed receipt `reconciliation_no_match / retry_safe=false`.
  Because its requested 20:00 Europe/Kaliningrad slot has passed, it was not
  reclassified or retried. ChatGPT must stage/prepare a new action after the
  user supplies a new publication time; Codex is not authorized to publish the
  real album.
- no historical operation ref was repeated. The specific VK image-2 provider
  rejection still needs a fresh ChatGPT-owned production action/readback; the
  server now reports its exact safe structural failure if VK repeats it.
- binding-capacity hotfix: commit `35ce8f736`, PR `#610`, merged exact-main SHA
  `f473d120322436d901359834207884bd9b3d6850`, Fly release/machine `v2063` /
  `2063`. Scheduled-delete-preview hotfix: commit `114d25c1b`, PR `#611`,
  merged exact-main SHA `ff4551e5cad36542d2dd13350c5c3e9028be5748`, Fly release/machine
  `v2064` / `2064`.
- final regression evidence: focused runtime/provider/Telegram suite
  `178 passed`; complete `tests/test_private_events_mcp_*.py` suite
  `564 passed` with three existing aiohttp warnings; `compileall` and
  `git diff --check` passed. Both PRs passed `python-ci`,
  `smart-update-identity-state-machine` and `static-browser-release-gate`.
- final release evidence: one Fly check passing; `/healthz ok=true ready=true
  db=ok`; data disk status `ok` with 673 MiB free; both SQLite
  `PRAGMA quick_check=ok`; runtime log mirror fresh; embedded source SHA exactly
  `ff4551e5cad36542d2dd13350c5c3e9028be5748` and both hotfix commits reachable
  from `origin/main`.
- final live MCP acceptance: schedule
  `op_wJmz7bGSHuaR-tgo1QU5Y2qzv7T3hgzZ` and delete
  `op_lcDhT_jwT4gC1z_fTMPIf8ahWOOPPcaf` each committed exactly once and
  succeeded. The only provider mutations were the user-authorized Saved
  Messages test schedules/deletes; every canary was removed before its slot.
  Final MCP scheduled exact/full counts were `0 / 0`, independent raw scheduled
  physical/logical counts were `0 / 0`, and latest-50 live exact count was `0`.
- **VK mutations during diagnosis: 0. `@lovekenig` mutations during diagnosis:
  0.** The real four-image album was neither scheduled nor published by Codex.

## Prevention

Successful provider returns are not the only publication outcome that must be
testable. Every claimed mutation must converge durably after timeout,
cancellation or restart; scheduled queues must be directly readable as logical
publications; scheduled deletion must use the provider's scheduled namespace
and prove absence; and only definite pre-mutation failures may be retried. A
schema-changing tool rollout is complete only after server, exact-main deploy,
action-control publication and real new-chat acceptance all agree.
