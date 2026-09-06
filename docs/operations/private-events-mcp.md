# Private Events MCP for ChatGPT, OpenCode and Codex

Status: integrated release candidate; disabled by default. The media/story gate
remains image-only; the independent file-send gate permits one structurally
verified document only for Telegram `send_message`. Production activation
requires exact-main deployment, capability probes, independent review, and the
live acceptance gate below. Video remains unsupported until a separately
verified validator and both provider implementations are merged and accepted.

## Purpose and client boundary

The service attaches to the existing events-bot `aiohttp` application. It does
not create a second Fly process, listener, scheduler, or event database.

Two OAuth resources deliberately expose different products:

- **Codex** receives exactly the seven read-only event, incident and operational
  evidence tools. It receives no social scope, tool or provider credential.
- **ChatGPT and OpenCode** receive those seven evidence tools and, only when scopes
  and runtime kill switches permit it, a provider-neutral Telegram/VK workspace
  for targeted editorial research and operator-requested communication.

The social workspace is not a raw Telethon or VK API proxy. Callers cannot name
provider methods, pass access hashes/tokens, or submit arbitrary SDK arguments.
All provider content is untrusted external data; it is never an instruction.
MAX remains outside the current scope.

## Owner queue readback (R0, #618 / #643)

The existing owner `operations_snapshot` accepts `include_jobs=true` and bounded
`event_id`, `status`, `before_job_id`, `limit` filters. A page contains at most
10 payload-free current `JobOutbox` rows in descending numeric ID order, with
nested JSON-shaped error strings decoded before recursive credential/personal-ID
redaction; malformed/oversized structured errors are omitted rather than emitted
as raw text. Missing legacy columns/table are reported explicitly. Fractional IDs
are rejected rather than truncated.
`event_id=0` selects global jobs. `include_jobs=false` with filters is rejected.
No new tool, scope, DB schema, queue worker or provider call is introduced.

The default snapshot retains its fields, but `database.quick_check` is explicitly
`not_run:interactive_budget`: full-database integrity scans are not interactive
health probes and must run as explicit operator/release checks. Counts and queue
readback never imply an integrity PASS. This prevents the production timeout
recorded in `INC-2026-09-06-mcp-snapshot-integrity-budget`.

The default snapshot remains backward-compatible apart from that honest health value. `fetch(job:...)` still returns
bounded detail through the existing evidence repository. The Codex seven-tool
projection and descriptor remain unchanged; these additional queue arguments
belong only to the full owner ChatGPT/OpenCode resource. A queue state or URL
is not authoritative publication age/applied-revision evidence.

`tests/test_private_events_mcp_queue_observability.py` verifies ordering/cursors,
query bounds, redaction, legacy schema behavior and zero-write reads. R0 can be
released separately; it does not activate R1 event writes, R4 partner mutations
or Hero delivery. #643 continues the existing owner/partner stack and adds Hero
as an activity of `promo.py`, not as a second campaign or MCP server.

## Endpoints and OAuth

```text
https://<origin>/_private/<secret>/mcp        # ChatGPT + OpenCode resource
https://<origin>/_private/<secret>/codex/mcp  # Codex resource
```

The high-entropy path is confidential defense in depth, not authentication.
Data tools require OAuth authorization-code + PKCE S256. The server provides:

- one predefined confidential ChatGPT client (`client_secret_basic`/post);
- one distinct predefined public Codex client (`none`, mandatory S256);
- one optional distinct public OpenCode client (`none`, mandatory S256) with
  an exact `http://127.0.0.1:<unprivileged-port>/mcp/oauth/callback` loopback
  path; the requested port may vary, while the code remains bound to the exact
  redirect URI;
- exact client/resource/audience binding;
- exact ChatGPT callback validation and literal Codex loopback callbacks only;
- 15-minute signed access tokens;
- rotating, replay-resistant refresh tokens only with `offline_access`;
- path-scoped protected-resource and authorization-server metadata;
- no dynamic client registration.

Omitted scopes default to only `events:read incidents:read operations:read`.
Codex can receive those scopes plus `offline_access`, and nothing social.
ChatGPT/OpenCode social scopes are granular by provider and action class:

```text
<provider>:discover
<provider>:read:public
<provider>:read:private
<provider>:read:dialogs
<provider>:dm:send
<provider>:post:publish
<provider>:edit
<provider>:delete
<provider>:forward
<provider>:reaction
<provider>:comment
<provider>:schedule
<provider>:story:read
<provider>:story:write
<provider>:analytics
<provider>:audience
vk:notifications:read
```

`<provider>` is `telegram` or `vk`. Granting a scope does not bypass a runtime
kill switch, current provider rights, request budgets or reference binding.
The two `story:*` scopes become usable only when the authenticated media store,
the media/story switch and the matching provider role are all active. The
initial production contract accepts images only; a video MIME/role fails closed.
The original connector scopes `telegram:read|publish` and `vk:read|publish`
remain stable provider-level compatibility families. On the ChatGPT resource,
`*:read` authorizes later typed reads for the same provider and `*:publish`
authorizes later typed mutations for the same provider. The mapping never
crosses provider or read/write boundaries. A typed outbound action invoked from
the current user's explicit request is prepared as `approved` and can be
committed immediately without a second browser confirmation. Edit/delete of
existing content retain the independent server-side preview/approval step.
Codex can never
receive these scopes. This lets normal MCP tool evolution preserve the existing
connector URL and ChatGPT identity/name. OpenCode has a separate public client
identity on the same full resource, so its tokens cannot be used as ChatGPT or
Codex tokens. Only a genuinely new capability family requires new OAuth consent.

After the first successful connection rotate the one-time bootstrap operator
token. Do not issue or share access/refresh tokens manually.

## Evidence tools

Codex exposes exactly these seven tools; ChatGPT and OpenCode retain them unchanged:

| Tool | Scope | Contract |
|---|---|---|
| `search` | all three read scopes | ChatGPT Search compatibility across stable evidence IDs |
| `fetch` | all three read scopes | fetch `event:`, `incident:`, `run:` and `job:` evidence |
| `events_search` | `events:read` | bounded event query |
| `event_get` | `events:read` | Event 360 evidence |
| `incidents_search` | incident + operations | repository reports plus runtime failures |
| `incident_get` | incident + operations | report/run/job evidence document |
| `operations_snapshot` | `operations:read` | bounded state, failures and SQLite health |

These seven base tools are read-only, non-destructive and idempotent. The event
SQLite evidence projection is opened with URI `mode=ro`, `PRAGMA query_only=ON`,
bounded rows and a VM/time deadline. There is no raw SQL, shell or arbitrary
outbound HTTP tool.

For the owner ChatGPT/OpenCode projection only, R0 may extend
`operations_snapshot` with an explicitly requested, at-most-ten-row `job_queue`
page from the existing `JobOutbox`. It returns no job payload or last-result
body, and `fetch(id="job:<id>")` remains the detailed evidence path. Codex keeps
the exact seven descriptors without these extra input fields.

The ordinary structured-result response cap remains
`PRIVATE_EVENTS_MCP_MAX_RESPONSE_BYTES`. Authenticated `tools/list` metadata is
separately bounded at 512 KiB (and is still charged to the shared hourly egress
budget), because the full stable-scope ChatGPT/OpenCode catalog can exceed the
ordinary 128 KiB data-result default. This does not raise provider-content,
incident or evidence response limits.

## Owner event creation (R1, source default-off)

The same EventsBot MCP process can expose three owner-only tools when
`PRIVATE_EVENTS_MCP_EVENT_CREATE_ENABLED=1` and the OAuth grant includes
`events:write`:

| Tool | Scope | R1 contract |
|---|---|---|
| `event_create_prepare` | `events:write` | validate and freeze one exact source request; no canonical DB mutation |
| `event_create_commit` | `events:write` | reserve one idempotent canonical operation and start the existing parser + full Smart Update path |
| `event_operation_get` | `operations:read` | poll durable accepted/rejected/failed/outcome-unknown state and event/job refs |

R1 is deliberately narrow:

- exactly one parsed event per committed request; multi-event packets are
  rejected before Event/EventSource/JobOutbox mutation;
- only `text_policy=smart_rewrite`; exact/original text, generic edits and
  lifecycle changes remain unavailable;
- festival-level programme sources are rejected into a dedicated reviewed
  intake instead of silently entering the legacy festival queue;
- no media upload, promo campaign or partner write projection;
- Smart Update remains the only Event writer and its existing
  `schedule_event_update_tasks()` remains the only ordinary fan-out owner;
- the MCP handler never invokes Telegram, VK or Telegraph directly. Immediate
  legacy page rebuild switches are ignored only for this queue-owned request,
  so the canonical event commit is followed by ordinary `JobOutbox` work;
- operation state is stored in the canonical event DB `event_change_log`, not
  in the separate OAuth/social-workspace SQLite file;
- the feature flag is strict and default-off. Disabled tools are absent from
  `tools/list`; existing OAuth grants do not receive `events:write`
  automatically.

Call sequence:

```text
event_create_prepare(raw_text, source_url|source_external_id, idempotency_key)
→ event_create_commit(same fields, preparation_ref, action_digest)
→ event_operation_get(operation_ref) until terminal
→ event_get(event_id) and operations_snapshot(include_jobs=true,event_id=...)
```

A timeout or stale in-progress operation becomes `outcome_unknown`. The caller
must not create a new idempotency key blindly; first inspect canonical event and
source evidence. Until exact-main deploy, OAuth scope refresh and authenticated
readback pass, this section describes staged runtime code rather than a live
production capability.

### Queued create restart recovery (#643)

`EventCreateRuntime.recover_queued(authorize=..., limit=100)` provides a bounded
restart/scheduled recovery hook over the existing canonical ledger. The host
must invoke it only for the enabled runtime, after database initialization,
and supply an async current actor-policy check. The check runs after the atomic
queued→processing claim immediately before the executor; denial records
`EVENT_CREATE_ACCESS_REVOKED` without a parser/canonical mutation. Policy errors
fail closed as `outcome_unknown`, not automatic retries.

Recovery preserves the stored subject/client/audience and exact action digest;
it never creates a Telegram identity. The raw idempotency key is not stored:
the internal restored request has an empty key and its original persisted hash,
and goes directly to `_execute`, never prepare/commit. Existing retries retain
the same ledger identity. Corrupt/unsupported request payloads are atomically
claimed and quarantined with `EVENT_CREATE_RECOVERY_REQUEST_INVALID`.
`processing`, `outcome_unknown` and terminal records are never replayed; they
still require canonical source/event reconciliation. Multiple workers share the
existing compare-and-set claim. The return value counts locally scheduled tasks,
not accepted events; callers use operation readback for the outcome.

This hook alone does not install startup scheduling or authorize partner writes.
The application integration must wire the current policy callback and repeat
bounded recovery passes for queues larger than the batch limit. Dedicated SQLite
regressions: `tests/test_private_events_mcp_event_create_recovery.py`.

### Event-scoped private image inputs (#643)

`private_events_mcp.event_assets.EventAssetService` reuses the existing
`SecureMediaAssetStore` through `AssetIngestor`; it does not call social
`stage_asset`, provider upload, or create a public event. The host injects a
stable secret `binding_key`, the durable secure store, and mandatory async
`authorize(context, action)` for `stage`, `read`, and `use`. Authorization is
checked before and after I/O. The host callback must enforce current principal,
resource, grant/epoch, scopes/actions and suspension/revocation policy.

- `await stage(ChatGPTFile(...), context)` returns an opaque `ing_...` asset ref,
  exact `sha256:` content digest, measured MIME/bytes/dimensions, role and expiry.
- `await read(asset_ref, context)` reopens/verifies the stored image and returns
  only that safe metadata, never a download URL, path or owner-binding material.
- `await reverify(asset_ref, context, expected_digest=...)` returns an **internal**
  `VerifiedAsset` for the intake adapter. It must not be serialized to MCP.
  The adapter can use the same store's `open_verified(storage_ref, owner_binding)`
  to obtain freshly hashed bytes for existing event intake media handling.

Bindings are HMAC-separated for event images and include exact
client/subject/resource. Another principal, resource or social asset binding
cannot borrow the ref. Only JPEG/PNG/WebP images are supported; ingestion uses
existing `event_image` validation and canonical stored role `image`. Reads/use
recheck expiry, role, owner, byte limits and content integrity. Errors fail closed
without exposing storage or upstream exception details.

References survive process restart only while the same secure-store directory,
manifest, owner-key material and service binding key are retained. Default
service retention is one hour; configurable retention is bounded to 24 hours
and must not exceed the injected store's TTL. Read/reverify never extends expiry.
Owner review must finish before expiry or request a newly staged image and a new
prepared/approved digest; refs are **not indefinitely durable review attachments**.
Existing secure-store cleanup still applies, including uploads whose access was
revoked during ingestion. This module adds no cleanup scheduler, MCP tool wiring,
review transition or EventCreateRequest field: those are application integration
steps, with renewed authorization and exact digest verification before mutation.

Regression evidence: `tests/test_private_events_mcp_event_assets.py` plus the
existing secure-store suite use actual local manifests/files and deterministic
HTTP transport, with no live provider calls.
### Durable partner create owner review (#643)

`PartnerEventReviewService` in `private_events_mcp/partner_event_review.py` uses
only existing canonical `event_change_log` rows. `submit(request)` invokes current
submission authorization and reserves `initial_status=review_required` with the
original actor/client/resource, action digest and idempotency binding. No Event,
Telegram actor, provider call or worker task is created. A duplicate submission
returns the existing operation unchanged; it never switches queued/review state.
The store's optional `initial_status` accepts only `queued` or `review_required`;
normal owner create retains `queued` as its default.

`decide(operation_ref, expected_action_digest=..., decision="approve"|"reject",
owner_context=...)` requires an injected async current-policy callback over the
owner context, immutable `ReviewTarget` actor/digest binding and decision.
That callback must authenticate/authorize the owner and revalidate the stored
partner's current grant, credential epoch, expiry, scopes/actions and applicable
organization policy. Missing/failed policy fails closed. It runs under the short
canonical SQLite write lock to serialize revocation and decision: **DB/read-only,
no provider/LLM/network, and no writes on a second connection**. Submission also
requires its own current-policy callback. These are service contracts, not new
OAuth claims or bypasses around `PartnerAccessStore`.

Approval only transitions `review_required` to `queued`; rejection records
terminal `rejected` with `EVENT_CREATE_OWNER_REJECTED`. Both require the exact
expected action digest. `organizer_comment` stores private immutable review audit
JSON (decision, digest, owner subject/client/audience, timestamp), deliberately
separate from `result_json`, which executor completion replaces. No public Event
field receives that audit. Repeating the same decision returns `changed=false`
and the current operation state with the original audit, even after execution;
an opposite decision/different digest conflicts and cannot resurrect work.

The existing runtime must separately recover/execute approved queued operations
with current partner authorization and media/digest rechecks at the mutation
boundary. Review approval is not publication or acceptance of a canonical event.
Partner reads continue through exact actor-bound `EventCreateOperationStore.get`;
owner listing/tool wiring, media approval binding and UI are integration work,
not implemented by this ledger service. Tests cover restart, competing decisions,
repeat decisions after finish/rejection, two actors, and real PartnerAccessStore
expiry/suspension/scope revocation: `tests/test_private_events_mcp_partner_event_review.py`.

### Dynamic publication evidence for accepted creates (#643)

`EventPublicationReceiptService` in
`private_events_mcp/event_publication_receipts.py` provides a read-only projection:
`await service.read(operation_ref, context)`. The caller supplies no event ID.
The service looks up the create operation by the exact current subject/client/
resource and uses only its accepted positive canonical `event_id`, cross-checked
against the single `result_json.event_ids` identity. Missing, invalid, multiple or
mismatched IDs fail closed. Unaccepted operations return no event/publications.

Required async `authorize(context, event_id_or_none)` checks current general
publication-read rights before ledger access, current tenant/portfolio rights
before event/job access, and again after that read. `None` requests the general
scope/current-principal check; an integer requires current access to that exact
event. The callback must use current policy, not stale creation-time grants.
Actor-bound operation lookup is never relaxed for caller-provided IDs.

Each read fetches current canonical Event publication fields and current
`JobOutbox` rows, rather than the frozen create-result job snapshot. Relevant
jobs (`telegraph_build`, `vk_sync`, `tg_event_publish`, `tg_premium_emoji_edit`,
`event_media_review`, `static_site_build`) expose only ID/task and normalized
`queued|running|done|error|paused|unknown` state. Results are newest-first and
bounded (default 50, maximum 100) with explicit `jobs_truncated`; payloads,
`last_error`, `last_result` and unrelated-event jobs are not returned.

Publications use `tg_event_post_url`, `source_vk_post_url`, `vk_repost_url`,
`telegraph_url`. Supported HTTPS Telegram public-message, VK wall and Telegraph
page URLs are narrowly validated; private invites/messages, arbitrary hosts,
credentials, ports, query strings and fragments are suppressed. A valid stored
URL means **`recorded_public_url`**, not a newly verified live publication.
The response explicitly sets `live_verified=false` and
`evidence_source=canonical_database`: no provider or public-page calls occur.
Job `done` without a stored public URL remains `no_public_receipt`.

Authoritative writers remain existing `main.job_publish_tg_event_post`,
`main._persist_vk_source_post_result`, `vk_review.save_repost_url`, and Telegraph
builders. Their persisted links may later become stale; this projection does not
claim otherwise. Static build completion/global release receipts never prove
one event's inclusion: static output is always `event_inclusion_unverified`
until a separate event-specific verification contract exists. Missing canonical
Event returns `canonical_event_missing`, not a fabricated receipt.

This service does not add MCP tools, mutate operations, schedule jobs or publish
content. Integration must inject current tenant policy and attach it to the
accepted-operation read path. Tests:
`tests/test_private_events_mcp_event_publication_receipts.py`.

## ChatGPT social workspace

When the universal workspace is enabled, ChatGPT can discover only the tools
matching its granted scopes and the enabled provider/capability flags:

| Tool | Purpose |
|---|---|
| `social_capabilities` | current provider, target and action capabilities |
| `social_target_resolve` | resolve Saved/self, exact person, channel/group/community or known provider reference into an opaque bound ref |
| `social_item_resolve` | resolve one canonical VK wall-post URL or public/private Telegram message URL directly into bound item and source-target refs; use before target search/feed when the prompt already contains the exact item URL |
| `social_targets_search` | bounded target search |
| `social_targets_list` | bounded public/managed target discovery |
| `social_dialogs_list` | VK-only metadata list of all or unread dialogs: opaque target, display name, kind and unread count, with no message body/native peer ID |
| `social_content_search` | bounded keyword search |
| `social_content_feed` | bounded target feed/history |
| `social_scheduled_items_list` | bounded provider-backed scheduled queue for one exact target, projected as logical opaque publications |
| `social_content_item` | fetch one bound item |
| `social_content_thread` | comments or reaction summaries |
| `social_comment_hints_list` | bounded recent VK comment/mention notifications as untrusted investigation hints |
| `social_content_stories` | bounded Telegram/VK story page with opaque media refs and no mark-read/viewer identities |
| `social_content_editorial_sample` | purpose-bound editorial sample, at most 25 items/page and 100 cumulatively |
| `social_content_analytics` | bounded aggregate post/story statistics or audience counts where the credential is entitled |
| `social_asset_stage` | ingest one ChatGPT `fileParams` image/eligible Telegram document, or rematerialize one fresh principal-bound provider-read image `ast_*` into immutable short-lived server storage for the selected VK/Telegram provider |
| `social_asset_status` | return only verified MIME/size/digest/dimensions or sanitized document name/classification, expiry and lifecycle state for an opaque asset ref |
| `social_asset_preview` | render one principal-bound story image as a bounded metadata-free MCP JPEG thumbnail |
| `social_action_prepare` | freeze exact typed action/content/target/media digest; return the reserved operation, `commit_required`, explicit next action and `operation_state=not_started` without a provider call |
| `social_action_commit` | atomically consume the exact preparation authorization, then make the sole provider attempt; replaying the same preparation returns its durable operation and never calls the provider twice |
| `social_action_status` | distinguish preparation state, operation state, provider-attempt state and mutation-boundary state; reconcile only a durable attempted operation |
| `social_action_retry` | make one bounded single-flight retry of a terminal operation only when durable evidence says `retry_safe=true` |

The `social_scheduled_items_list` and `social_action_retry` rows are the target
contract of the open August 31 recovery incident. They must not be treated as
production-available until exact-main deploy, authenticated catalogue readback,
administrator action review/publication and a real new-chat probe all pass.

### Targeted editorial research

`social_content_editorial_sample` is intended for a user-named target, for
example examining its description and up to 100 recent owner posts to infer
editorial policy, topics and characteristic phrasing. It is not a crawler or
bulk indexer:

- page size is at most 25 and the server cursor enforces a cumulative maximum of
  100 for the exact target, purpose and access class;
- the caller must state the editorial purpose and one closed authorization
  basis: `self_owned`, `operator_authorized`, or `provider_approved`; that basis
  is bound into the server sample state and its sanitized audit receipt;
- the cursor and sample ref cannot be replayed for another target or access
  class;
- provider text is clipped, recursively redacted, marked
  `untrusted_external_data`, and not persisted as a corpus;
- the response records sample size, date range and truncation/exclusion facts;
- no member graph, commenter identity or arbitrary linked-page fetch occurs.

### ChatGPT file ingress and immutable assets

`social_asset_stage` is the only media ingress. Its public input is exactly
`{platform, file, role}` and its tool descriptor declares
Provider-read images use the same tool without a new control plane: pass exactly one `source_asset_ref` instead of `file`. The server accepts only a fresh `ast_*` bound to the same OAuth client, subject, resource and policy, materializes it through the source provider adapter, recomputes and validates bytes/digest/MIME/dimensions, stores it under a fresh principal-owned opaque storage reference, and stages it for the selected destination provider. Raw URLs, native provider IDs and filesystem paths are not accepted.

`_meta["openai/fileParams"]=["file"]`. ChatGPT supplies one selected file per
call as a closed `file` object with required `download_url` and `file_id` plus
optional `mime_type` and `file_name`. Those values are untrusted transport
hints, not durable asset truth.

ChatGPT may display a local path, `sandbox:` URI or opaque `file_*` identifier
in the conversation UI, but none of those strings is a valid direct tool
argument. The connector must materialize the selected upload into the closed
file object above. A string/path-only value fails as `FILE_REF_UNRESOLVED` and
must never be copied into `social_action_prepare`; the successful stage result
is the only source of an `ast_*` reference.

This is not an arbitrary URL downloader. There is no tool accepting a URL,
filesystem path, raw provider method, Telegram/VK native file identifier or
caller-declared digest/size. The server accepts only HTTPS `download_url` values
inside `fileParams`, requires a configured exact-host/explicit-wildcard
allowlist, resolves only public addresses, rejects redirects and streams into a
bounded temporary file. The media/story gate detects and verifies JPEG, PNG or
WebP image bytes independently of the hint. The separate default-off file-send
gate accepts only structurally verified APK, PDF, ZIP, UTF-8 TXT/MD/CSV/JSON,
DOCX, XLSX or PPTX bytes for Telegram `role=document`. Audio, animation and
**video** remain rejected even if a provider could otherwise accept them.

Document validation is structural, not a malware, signature, provenance or
publisher assertion. It hashes immutable regular bytes, derives a path-free
bounded display filename, checks declared/detected MIME consistency, and
inventories ZIP/APK/Office containers without extracting or executing them.
Encrypted archives, unsafe/colliding paths, unsupported compression, excessive
entry count/declared expansion, an ordinary ZIP renamed `.apk`, and generic
opaque binaries fail closed. APK classification requires Android structure,
including `AndroidManifest.xml` and Android payload. An incoming
`application/octet-stream` is only a hint; accepted bytes must still classify
to the closed allowlist.

A successful stage returns only an owner- and provider-bound opaque `asset_ref`.
The server atomically stores an immutable read-only file and a manifest binding
its actual SHA-256 digest, detected MIME, byte length, role and expiry, plus
dimensions for images or a sanitized display filename/classification for a
document. `file_id`, original/unsanitized `file_name`, download URL and internal
path are never returned or persisted as usable source values.
`social_asset_status` can disclose only the opaque ref, lifecycle state, trust
marker, verified metadata and a sanitized error code. The default TTL is one
hour and configuration cannot exceed 24 hours; aggregate retained bytes and
image dimensions/pixels are also bounded. Every provider upload reopens the
stored file, checks principal/provider/role binding and TTL, requires a regular
file and recomputes the digest before reading bytes. Expired assets are unusable
and removed by bounded cleanup.

Asset-stage failures return only a bounded `structuredContent.error_code` and
`retry_safe=false`: `FILE_REF_UNRESOLVED`, `FILE_HOST_NOT_ALLOWED`,
`FILE_PRINCIPAL_MISMATCH`, `WORKSPACE_NOT_BOUND`, `MIME_NOT_ALLOWED`,
`FILE_TOO_LARGE`, `FILE_EXPIRED`, `FILE_INTEGRITY_FAILED`,
`FILE_TYPE_NOT_ALLOWED`, `FILE_TYPE_MISMATCH`, `FILE_TYPE_INVALID`,
`FILE_NAME_INVALID`, or `FILE_FETCH_FAILED`. The text never contains the signed
download URL, original file ID/name, provider/native ID or internal path. The audit ledger records the same safe
reason; a host-policy denial may append only a one-way hostname fingerprint so
operators can correct an exact allowlist without logging the temporary URL.

### Telegram document message v1

The document surface is deliberately narrower than generic media:

- platform is Telegram and the action is exactly `send_message`;
- content contains exactly one `role=document` attachment, never a second file
  or mixed image/document media; an optional caption and existing rich entities
  are allowed;
- `document` is advertised only when the file-send flag, asset ingress,
  Telegram adapter and the resolved target's current `send_message` right all
  agree. VK, stories, publish/schedule/edit/comment/forward/delete and
  read-only/publish-only targets never advertise or accept it;
- prepare reauthorizes the target and reopens/rehashes the principal/provider-
  bound asset, then freezes the role, SHA-256, size, detected MIME, sanitized
  filename, classification and expiry into the action digest and preview;
- prepare performs no Telegram upload. Commit repeats feature/rights/TTL and
  immutable-byte verification, atomically consumes the exact authorization,
  and makes exactly one Telethon `send_file` attempt with forced-document
  semantics and a sanitized `DocumentAttributeFilename`;
- read-after-write requires the intended target/message and a Telegram document,
  checking filename and size when the provider supplies them. A timeout remains
  `outcome_unknown`, `retry_safe=false`; never blindly resend.

The only valid ChatGPT end-to-end ingress is the actual `file` object supplied
by the connector through `_meta["openai/fileParams"]`. A locally fabricated
object, filesystem-only call, raw URL/path or `file_*` string is useful only as
a negative/unit probe and is not live ChatGPT acceptance.

### Story reads, publication and statistics

`social_content_stories` reads one bounded page for an opaque Telegram/VK target.
`social_content_item` may read one returned story ref, and
`social_content_analytics` may return item-level or bounded target-level story
aggregates. Results contain opaque refs, bounded story text/metadata and
aggregate counters. For an image media ref, `social_asset_preview` performs a
fresh principal/provider binding check, downloads at most the configured asset
cap through the dedicated adapter, validates JPEG/PNG/WebP pixels, strips
metadata and returns a JPEG thumbnail no larger than 768×768 and 64 KiB as a
standard MCP image content block. It never returns the provider URL, original
file, native identifier or local path. Video media may be listed as metadata,
but visual video retrieval remains unavailable in this image-only release.
VK story media URLs are accepted only from the fixed provider CDN suffixes
observed in VK API responses, including VK-hosted `*.okcdn.ru` video assets;
they still require HTTPS, public DNS, no userinfo and no redirects, and the
provider URL is never returned to the MCP client.
Viewer names, profile/user IDs, recent-viewer lists and other viewer identities
are excluded; the adapters do not mark stories read or call viewer-list methods.

Ordinary Telegram message reads use the same materialization boundary. Every
provider-owned media token in a returned `media` array is replaced by a fresh
principal/provider-bound outer `ast_*` reference before it crosses MCP; the
adapter token is never usable as a public reference even though both tokens
share the same syntactic prefix. Each returned image ref must therefore be
accepted immediately by `social_asset_preview` for that same principal. A
Telegram `grouped_id` media album is one logical feed item: up to ten members
are returned in Telegram order, do not consume separate page slots, and an
exact-item read from any member expands the complete bounded album.

Telegram item resolution accepts only canonical public message links and
canonical private `/c/` message links. Public links require public-read access;
private links require private-read access. Malformed, inaccessible or
target-mismatched links fail with a sanitized social-provider error. The public
response contains only principal-bound `tgt_*`, `itm_*` and `ast_*` references;
the native peer/message values used for the exact Telethon lookup never cross
the adapter boundary. The VK exact wall-link path remains unchanged.

The ChatGPT-visible `social_item_resolve` input is deliberately narrower than
the shared server read contract. It requires exactly `platform`,
`operation=resolve_item`, a `profile_link` locator and `read_access`, with only
optional `transcribe_audio`; it does not advertise `expected_target_kinds`,
search terms, target refs, cursors or feed limits. This is an argument-generation
contract as well as validation: when the user supplied an exact item URL, the
client must call this resolver first rather than discover a target and paginate
its history. For a connector still holding the former generic schema, the
server temporarily infers private access for canonical Telegram `/c/` item
links and public access for other exact item links. It accepts at most one
non-self legacy target-kind hint and checks that hint against the resolved
source; this compatibility path does not relax exact-link or OAuth access
binding.

Message/feed/search/thread results retain `media` as the ordered array of
opaque outer asset refs. They may add `attachments` (and the closed schema also
reserves `media_details`) with the corresponding asset ref, safe MIME/size/
duration metadata, and one of `voice`, `audio`, `photo`, `video`,
`round_video`, `animation` or `document`. Classification is derived from fixed
Telethon media/document structures, attributes and MIME, never from message
text.

When the existing audio-transcription capability is active, high-level
Telegram reads default `transcribe_audio=true`. Voice and ordinary audio
attachments are enriched in the same response through the existing
`AudioTranscriptionService`, `AudioAssetStore`, `AudioJobStore`, Kaggle backend
and Telegram-native worker. `transcribe_audio=false` skips byte ingress and job
creation while retaining media metadata. Provider bytes enter an internal
trusted ingress directly; the server never fabricates a ChatGPT `fileParams`
object, provider URL, native file identifier or filesystem path.
The materialization request uses the stricter of the audio store's configured
asset limit and the Telegram adapter's provider-media limit; the larger generic
upload ceiling must never be passed back as an invalid provider read bound.

For a batch-oriented read, send both `transcribe_audio=true` and optional
`transcription_wait_seconds=0..30`. Zero creates/finds every durable job and
returns one snapshot without active waiting; a positive value is one common
bounded wait for the entire response. The Telegram/VK provider call retains its
separate `social_provider_timeout_seconds`; no attachment can consume the batch
wait before the remaining voice/audio jobs are registered/found or explicitly
localized as a materialization failure. The complete registration stage, not
the transcription wait, may span several concurrency waves. Each attachment's
provider-media attempt remains individually bounded; the four batch-read MCP
deadlines cover the initial provider read, all schema-bounded waves, up to 30
seconds of store wait and a small projection margin. Unrelated actions keep the
short ordinary deadline.
An untrusted provider result above the 250-attachment schema ceiling is rejected
before any registration task, provider-media download or durable job side
effect, so the deadline calculation is also an enforced runtime boundary.
Materialization is capped at three concurrent ingress operations, while remote
dispatch remains strictly serialized.

Read-triggered jobs are bound to the authenticated principal and an HMAC of the
target/item/media identity, then verified against the content SHA-256. A repeat
read checks the durable job before downloading, so it neither downloads the
same media nor creates a second job. Each voice/audio attachment returns the
same closed object: `status=ready|queued|running|failed`, opaque
`transcription_ref` after successful materialization, `created`, `cache_hit`,
`text_included`, `truncated`, `next_offset`, `next_poll_after_seconds` and the
untrusted-data marker. Ready text is included immediately when the response
budget permits it. Long text uses a reproducible continuation offset rather
than silent clipping. A top-level `transcription_summary` reports
`total/ready/queued/running/failed`, unique-job `cache_hits/created`, one
`wait_expired` flag and a safe next refresh delay.

The batch wait observes only owner-bound durable rows; it never expands into
per-ref provider reconciliation and cannot bypass serialized dispatch or a
persisted `Retry-After`. Local wait expiry leaves queued/running jobs in their
real state and never fabricates `failed/TRANSCRIPTION_TIMEOUT`. A provider-byte
failure before durable job creation is localized as safe
`TRANSCRIPTION_MATERIALIZATION_FAILED` without a ref; siblings and text still
return. Responses, errors and the single sanitized batch telemetry line exclude
native IDs, access hashes, file references, provider/model details,
auth/session data, private URLs, paths and transcript bodies. This internal path
is authorized by the social read scope; it does not invoke or weaken the
standalone `audio:transcribe`/publish tool gate.
Its `response_duration_ms` spans the provider read through projection, response
cap enforcement and audit rather than reporting only enrichment time.

When the caller also has access to the standalone audio tools, the returned
`atr_*` is accepted by `audio_transcription_status` and
`audio_transcription_get` for the same verified OAuth
subject/client/resource. Read-triggered jobs use the historical Social
Workspace owner binding while uploaded-file jobs use the signed audio binding;
status/get checks both bindings derived from the current authenticated context
and still rejects another principal. Do not pass `ast_*` to
`audio_transcription_start`: a social asset reference is neither ChatGPT
`fileParams` nor a supported local/file reference.

Transcription is intentionally asynchronous and serialized because the lane
uses one dedicated Telegram session. `queued` is therefore a successful
durable acceptance, not completed text. The monitor normally advances on the
configured 20-second poll cadence, but a provider `Retry-After` is persisted
and takes precedence across restarts. Clients must not busy-poll: repeat the
same high-level social read only after its `next_poll_after_seconds` to receive
fresh aggregate states and inline ready text. `audio_transcription_status/get`
remain compatible single-job fallbacks for a long continuation or diagnostics,
not the normal N-per-ref route. A multi-voice chat may take several serialized
runs to finish.

A ChatGPT skill/custom instruction may persist the efficient client sequence:
exact URL first, request one bounded batch wait, retain returned opaque
refs/cursors, honor the summary delay, and repeat only the high-level read until
the desired coverage is ready. This removes per-attachment status/get loops and
invalid fallback calls, but it does not make cold transcription synchronous or
bypass the dedicated serialized Telegram/Kaggle lane. Server-side idempotency
remains authoritative; client memory is an orchestration optimization, not a
cache or security boundary.

The Telegram MCP workspace continues to use only
`TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP`. The remote transcription worker keeps
its distinct configured `TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION` lane; neither may
borrow the local E2E or monitoring bundle.

An image story uses the existing typed `story` action with exactly one ready
image `asset_ref` and one opaque target. Preparation freezes the exact target,
provider, asset digest/MIME/size/dimensions/expiry, resolved story policy,
idempotency key and human-readable preview. It performs no provider upload.
After the operator approves that exact preview in the separate browser page,
commit atomically consumes the approval, revalidates the feature/rights/budget,
reopens and rehashes the immutable bytes, performs at most one provider upload
and reads back the created story. A changed, expired or missing asset fails
before provider transport. Provider timeout is `outcome_unknown` and is never
blindly retried.

Telegram uses its fixed typed story methods through the dedicated MCP Telethon
role. VK uses fixed story upload/save/read/statistics methods through separate
story-editor, story-reader and analytics-reader roles. Neither adapter exposes a
native method selector, multipart endpoint, upload URL or provider credential to
ChatGPT. Video is explicitly denied by the image-only contract; it is not a
hidden or placeholder-enabled capability. Any later video support requires a
separate code/config/schema change, review and production acceptance for each
provider.

### Communication and mutation

The active workspace uses typed actions such as `send_message`, `publish`,
`comment`, `reaction`, `forward`, `schedule`, `edit` and `delete`. The target and
source item are provider-resolved opaque refs. Saved Messages is a first-class
Telegram self target; an exact user/DM is resolved and previewed as that person,
not as an alias guessed by the model.

Every external mutation follows:

1. `social_action_prepare` validates scope, feature flag, provider rights,
   budgets, target/item bindings, content/entities/assets and idempotency;
2. it persists only the exact canonical digest and does not call the provider.
   A fresh typed outbound action explicitly requested through the authenticated
   ChatGPT/OpenCode resource is returned as `approved` without an
   `approval_url`; edit/delete remain `awaiting_human_approval`. The response
   always says `commit_required=true`, names `next_action`, returns the
   `reserved_operation_ref`, and reports `operation_state=not_started` plus
   `provider_attempted=false`;
3. for edit/delete only, the operator opens the returned URL, enters the
   separate approval token, inspects the exact human-readable target, source
   item, action and text, then confirms in the browser. An opaque-only
   target/item preview or an item without a human-readable source target fails
   closed;
4. `social_action_commit` accepts only the preparation ref and digest. It
   rechecks the current action-class kill switch, then atomically consumes the
   server-side approval before one provider attempt. A repeated call for that
   exact preparation is an idempotent receipt replay and does not consume
   another attempt budget or call the adapter;
5. `social_action_status` or read-after-write evidence reconciles the receipt.
   Before commit, preparation status may be `approved`, but operation status is
   explicitly `not_started`, `provider_attempted=false`, and
   `mutation_boundary_reached=false`.

The preparation allocates the real future `operation_ref`; status by either the
preparation or operation reference therefore converges on the same durable
receipt. A definite provider rejection before a wall mutation is `failed` and
may be `retry_safe=true`. Only a transport ambiguity at a mutation boundary is
`outcome_unknown`. VK reconciliation never repeats `wall.post`: it performs a
bounded authenticated wall read and accepts success only for one exact target,
normalized-text, time-window and expected-photo match. When photo-save or
wall-post returned native identifiers before a later ambiguity, those values
are encrypted in provider state and reconciliation uses the exact photo/post
identifier rather than text alone.

#### Scheduled recovery contract

The following is the regression contract for
`INC-2026-08-31-mcp-scheduled-readback-reschedule`. PRs `#600` and `#601`
delivered its server-side scheduled queue, retry and readback surfaces. A
refreshed ChatGPT action snapshot is a separate client-control acceptance gate;
server deployment by itself is not evidence that a frozen action definition was
reviewed and published.

Scheduled items keep their provider queue namespace in the opaque binding.
Telegram schedule success and recovery use bounded raw
`messages.GetScheduledHistoryRequest`, never Telethon's incomplete high-level
scheduled iterator. Physical members with one `grouped_id` collapse into one
logical album in Telegram order. Exact verification binds the target,
UTC-equivalent schedule time, normalized-text SHA-256, expected logical media
count and ordered media roles; source-file digests remain encrypted evidence
because Telegram may re-encode uploaded photos. `social_content_item` reuses the
same opaque scheduled binding instead of resolving a numeric id from ordinary
channel history.

`social_scheduled_items_list` is the narrow read-only queue surface. It requires
an exact `platform` and `target_ref`; accepts optional bounded
`scheduled_from`, `scheduled_to`, exact `text_sha256` and `media_count` filters;
and uses `limit=10` with a hard maximum of 25. Telegram reads raw scheduled
history; VK reads the exact owner through `wall.get(filter="postponed")`.
Results contain `platform`, opaque `target_ref`, `queue=scheduled`, a bounded
`items` array, `exact_match_count`, optional `has_more`, and trust. Each logical
item contains only opaque `item_ref`/`target_ref`, `queue`, normalized
`scheduled_at`, `text_sha256`, `media_count`, ordered `media_roles` and trust.
Native message/post/peer IDs, access hashes, provider payloads, tokens and
upload URLs never cross the public boundary. The tool reuses the existing
`telegram:schedule` / `vk:schedule` scope families; it does not require a new
OAuth consent family.

Each projected scheduled item also stores an encrypted, closed human-approval
preview for the same principal: source target, scheduled queue/time, text
SHA-256, media count and ordered roles. This is the minimum exact evidence that
lets the returned `item_ref` enter the existing externally approved delete
flow. It adds no delete permission and does not store or disclose provider
native IDs or an otherwise hidden caption.

Provider adapters own their transport/session deadlines and durable uncertainty
classification. Runtime must not use an equal outer deadline that cancels an
adapter before it finalizes its claimed provider operation. Any outer protocol
deadline must leave a documented finalization/readback margin or shield the
provider task. `asyncio.CancelledError` at the provider-operation boundary must
durably complete success, definite failure, or outcome-unknown with
reconciliation evidence; release is allowed only when no provider mutation
could have occurred. A NULL provider result has a bounded lease/deadline and
cannot remain `operation_in_progress` indefinitely.

Telegram restart recovery reconstructs the exact encrypted intent and checks
the raw scheduled queue. Once the scheduled time passes, it checks ordinary/live
history as well. Exactly one exact logical match succeeds with a stable opaque
item, exact time/media count and `read_after_write.verified=true`. Multiple
matches are terminal `failed / reconciliation_ambiguous / retry_safe=false`
with only a bounded count and opaque refs and are never auto-deleted. Zero matches within
the consistency window remain outcome-unknown with a bounded attempt number,
`next_poll_after_seconds` and `reconciliation_deadline`. Zero matches after that
deadline become terminal `failed / reconciliation_no_match / retry_safe=false`,
carry `exact_match_count=0` plus an absence-verified final readback, and remain
non-retryable after the mutation boundary. Repeated status calls return that
durable terminal result without another provider read. Legacy persisted
`outcome_unknown / reconciliation_no_match` rows are normalized once to the
same terminal state without replaying the provider mutation or readback; the
normalizer does not fabricate a new observation timestamp or readback proof.
Evidence that mutation never started
may instead terminate as `failed / provider_mutation_not_started` with
`retry_safe=true`.

Exact scheduled deletion retains the existing approval and item-binding checks.
A Telegram scheduled album uses
`messages.DeleteScheduledMessagesRequest` for every bound physical member,
then raw scheduled history must prove all members absent. VK deletion targets
the exact bound owner/post and must prove absence from the postponed queue. The
ordinary delete namespace and a text-only match are not acceptable substitutes,
and direct-delete authorization is not broadened.

`social_action_retry(operation_ref)` is bounded to a terminal operation whose
durable receipt has `retry_safe=true`, normally a definite failure before the
provider mutation boundary. It keeps the logical action and preparation
identity, creates a new opaque operation attempt with an incremented number,
records the public `stage`, mutation-boundary state and final readback, and uses
an atomic single-flight
guard so concurrent retries cannot execute. It never retries a pending,
succeeded, ambiguous or outcome-unknown action and never asks the caller to
invent a replacement idempotency key.

VK schedule success is read back from the intended community's postponed queue,
not `wall.getById`. VK may re-own a saved user photo when attaching it to a
community post, so reconciliation binds the exact wall owner/post id, normalized
text, publish time and photo count but does not require the pre-wall photo
owner/id pair to survive. A later status check inspects both postponed and live
owner surfaces without replaying `wall.post`.

The approval token is never pasted into ChatGPT. It is not an OAuth token and
must not appear in model context, logs, PRs or artifacts. A provider timeout is
`outcome_unknown` and `retry_safe=false`; do not retry with a new idempotency
key until reconciliation.

#### Telegram marker for “added to GitHub”

Use the separate closed reaction option
`reaction_preset=github_added` when the operator explicitly asks to mark a
Telegram message whose idea was added to GitHub. The preset is Telegram-only
and mutually exclusive with the ordinary `reaction` field. Its selected custom
emoji document ID is configured only on the server through
`PRIVATE_EVENTS_MCP_TELEGRAM_GITHUB_REACTION_CUSTOM_EMOJI_ID`; the provider ID
is never accepted from or returned to ChatGPT. The adapter emits Telegram's
native custom-emoji reaction and `list_reactions` projects the configured icon
back as the semantic value `github_added`.

An arbitrary Unicode check mark is not a custom-emoji substitute: Telegram
accepts a normal `ReactionEmoji` only when that exact standard reaction is
supported for the message/chat. A provider rejection after a mutation attempt
remains `outcome_unknown` and must be reconciled rather than blindly retried.
This marker does not mark voice/audio as played and is never added
automatically by a read or transcription. It is an explicit, separate mutation
after the GitHub write succeeds. Because the prepare input schema changes when
this option is introduced, the ChatGPT app action definition must be refreshed,
reviewed and published, then verified in a new chat as required by
`INC-2026-08-25-chatgpt-frozen-mcp-actions`.

Runtime kill switches are additional, fail-closed controls:

```text
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_TELEGRAM_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_VK_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_PRIVATE_READ_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_POST_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_EDIT_DELETE_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED
```

Child switches cannot enable a disabled master/provider. Media/story activation
or document-send activation derives the common asset-ingress gate and requires
a configured media root, a nonempty ChatGPT download-host allowlist and the
matching injected provider roles. Document send additionally requires Telegram
and DM switches. Missing/invalid storage, document budget or provider capability
fails startup/advertisement instead of exposing placeholder tools.
The credential generator retains the original provider-level read/publish
families rather than minting a new connector identity. Text in a social message
is never treated as a media URL to fetch.

## Provider boundary

Core/runtime modules remain provider-neutral. `create_app()` lazily injects the
workspace adapters only when both MCP and the universal workspace are enabled.
Adapter construction performs no provider request.

### Telegram

Telegram uses only `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP` plus
`TELEGRAM_API_ID`/`TELEGRAM_API_HASH` (the existing `TG_*` aliases are accepted
for the API pair). It never falls back to `TELEGRAM_AUTH_BUNDLE_E2E`,
`TELEGRAM_SESSION`, `TELEGRAM_AUTH_BUNDLE_S22`, or the bot token.

The dedicated Telethon workspace provides typed resolve/read/search/editorial
sample/basic statistics and text action operations. It supports Saved via the
authorized account itself and exact-person/dialog resolution. Opaque refs hide
provider access hashes. Cross-process SQLite fencing serializes the dedicated
session, persists FloodWait/cooldown state, and rejects a lost lease. Live
rights and granular `ChatBannedRights` are rechecked before mutation. The
adapter carries immutable peer snapshots through preflight, atomically claims
caller-issued operation refs, and preserves timeout/lost-fence outcomes for
reconciliation. Encrypted target/item/cursor bindings and operation receipts are
kept in the isolated auth database, so an approval or reconciliation does not
lose its provider binding on process restart.

Telegram target, item and provider-read media bindings use secret-bound stable
refs derived from their immutable native coordinates. Re-reading the same
target/message/media upserts the latest detached provider snapshot instead of
minting another encrypted row; rotating Telegram `file_reference` bytes are
therefore refreshed without changing the public media identity. Verified
ChatGPT upload-stage bindings stay random and immutable so an older staged ref
cannot silently resolve to different bytes. Item and asset maps retain the
35-day expiry sweep and a 100,000-row per-kind fail-closed safety ceiling; this
ceiling is storage protection, not an album-size limit. Telegram publication
continues to support one to ten attachments in a native album.

The same dedicated role may advertise image/story capability only after its
live `CanSendStory`-equivalent rights probe and the server media-store injection
pass. It receives only an already verified server-owned asset handle, never a
ChatGPT URL or client-supplied path. Story reads and aggregate statistics omit
viewer collections and do not mark a story read. There is no fallback to a bot,
E2E, S22 or general-purpose human session for story work.

With the independent file-send gate enabled, the same dedicated MCP role may
advertise `document` only for a target whose live capabilities include
`send_message`. It receives a closed binding to verified immutable bytes and,
at commit, performs the single forced-document upload described above. Native
Telegram document/file IDs, access hashes and raw Telethon keyword arguments
remain internal and never enter MCP responses, audit records or smoke receipts.

### VK

The universal VK workspace does not reuse `main.vk_api` or its implicit actor
fallback. Each fixed typed method is mapped to one explicit role credential:

```text
PRIVATE_EVENTS_MCP_VK_PUBLIC_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_NOTIFICATION_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_DIALOG_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_USER_MESSENGER_TOKEN
PRIVATE_EVENTS_MCP_VK_COMMUNITY_EDITOR_TOKEN
PRIVATE_EVENTS_MCP_VK_MEDIA_EDITOR_TOKEN
PRIVATE_EVENTS_MCP_VK_ANALYTICS_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_EDITOR_TOKEN
```

Only configured actor/action capability sets are advertised. The transport uses
fixed VK API 5.199 method paths, rejects redirects, bounds responses, and emits
only sanitized provider error codes. Public-wall and private-dialog access are
separate; a public scope cannot route to conversation history. Cursor context
binds target, operation, access class, inclusive UTC `date_from`/`date_to`, item
kinds and unread/all mode. VK list/search results are post-filtered to those
inclusive date bounds even where the provider search method cannot express the
same closed interval. `social_dialogs_list`
uses the dialog-reader role and projects only an opaque target ref, display name,
dialog kind and unread count. It never returns `last_message`, message text or a
native peer ID. The returned user, group-chat or community dialog target can be
used by a later explicitly requested `send_message`; actual history remains a
separate `social_content_feed` call with dialog access. Writes bind the full intent digest and
operation ref so concurrent or mutated replays cannot duplicate provider work.
Encrypted provider refs, sample/cursor state and action receipts survive process
restart; an interrupted unreceipted operation becomes non-retryable
`outcome_unknown` rather than being executed again.
Provider continuation state is validated before use and capped before encrypted
SQLite persistence; an oversized VK `next_from` value fails closed instead of
growing the auth/state database.

Image/story operations preserve the same separation: `wall.post` uses the
community-editor actor, while wall-photo upload/save requires the explicit
user-token `media_editor` actor because the upload chain has a different
provider authorization contract. `image` is not advertised unless that actor
is configured and permitted. Story editing, story reads and story aggregate
analytics remain on their dedicated roles. A generic token or `main.vk_api`
fallback cannot satisfy a missing role. The adapter receives a
verified local asset stream and never exposes or accepts a raw VK upload-server
URL/method. Multipart responses enable bounded HTTP content decompression
because VK may return the upload receipt as gzip, then consume decoded bytes to
EOF under the cap; one short network chunk is not treated as the whole JSON
response. Safe logs record only opaque operation, fixed stage, status and
sanitized code. For `wall_photo_multipart`, the bounded finished
`provider_result` additionally records `http_status`, `content_type`,
`content_encoding`, `compressed_bytes`, `decoded_bytes`, `consumed_to_eof`,
top-level/nested key names and unknown-key counts,
JSON top-level type, response/provider-error presence and mapping flags,
`server_field`/`photo_field`/`hash_field` presence, type and capped length,
server numeric-type validity, `image_ordinal`/`image_count`, input byte length,
SHA-256 digest, MIME/format/dimensions, actor role, whether a prior
`photos.saveWallPhoto` was reached, whether `wall.post` was reached, and
`mutation_boundary_reached`.
Existing `stage` and `phase=started|finished` plus the durable attempt recorder
supply stage timing/attempt evidence. It never records a response body, field
value/hash, upload URL, token or cookie.
A missing/invalid receipt before `photos.saveWallPhoto` is definite
`failed / retry_safe=true`. Structural failures use bounded codes such as
`media_upload_missing_photo`, `media_upload_empty_photo`,
`media_upload_missing_hash`, `media_upload_empty_hash`,
`media_upload_missing_server`, `media_upload_server_type_invalid`,
`media_upload_photo_type_invalid`, `media_upload_hash_type_invalid`,
`media_upload_provider_error_shape` and `media_upload_json_shape_invalid`
instead of collapsing every case into `media_upload_response_invalid`.
An HTTP 200 response with an empty `photo` is a provider-side rejection shape,
not a valid upload receipt; it is never forwarded to `photos.saveWallPhoto`.
Uncertainty after `wall.post` remains
`outcome_unknown / retry_safe=false`. A durable attempt row records the attempt
number, fixed method/stage, start/finish, available HTTP status, normalized
outcome/error and an encrypted envelope for native photo/post results;
target/content/media fingerprints remain bounded and tokens, upload URLs and
bodies are never stored. Ordinary VK item/feed reads
project wall photos as principal-bound opaque `media[]`/attachment refs, so
MCP readback can attest image presence without exposing native IDs. Story
metrics are aggregate views/likes/replies/shares where VK
returns them; viewer/member identities are discarded.

`social_comment_hints_list` uses only the fixed `notifications.get` method,
requires the dedicated notification-reader actor and scope, returns at most 25
hints per page, and accepts a maximum 48-hour date window. A hint is untrusted
evidence, not proof of an event defect. Follow it via `root_item_ref` and
`social_content_thread`, or resolve a known canonical `vk.com/wall…` URL with
`social_item_resolve`. Then call `events_search(post_url=...)` to retrieve every
exactly related event and `event_get` for role-labelled source/publication/
identity evidence. Ambiguous URL relations are returned as all matches rather
than guessed.

## Privacy, untrusted data and logs

Decoded event/runtime/provider data is recursively redacted for credentials,
authorization values, Telegram-bot-token shapes and personal operator
identifiers. Repository incident Markdown crosses the same boundary. Social
provider text, captions, profiles, comments, errors and story text are returned
only inside explicit untrusted-data envelopes and never become instructions,
tool names, approval or target selection.

Access logs redact the private route, bearer/basic authorization and configured
secrets before stdout/runtime-log mirror. Provider logs contain no message body,
raw target, token, idempotency value or provider payload. Approval/audit ledgers
store hashes and bounded public summaries, not credentials or private message
bodies; the exact approval preview is encrypted at rest. The isolated
OAuth/social state DB is created mode `0600` and remains
separate from `/data/db.sqlite`; provider bindings are encrypted before storage.
Media files live in a separate owner-only root, are immutable after the atomic
move, expire independently from OAuth state and are addressed only by random
opaque refs. Manifests keep verified technical metadata and a keyed owner/file
binding; ChatGPT download URLs, original names and provider viewer identities
do not enter logs, receipts or durable social state.
Publication attempts are charged before transport against durable UTC-day
global/principal/target/action limits. The configured value
`PRIVATE_EVENTS_MCP_SOCIAL_PUBLISH_ATTEMPTS_PER_DAY` (default `10`) remains the
narrow per-target and per-provider-action limit. Global and principal aggregate
ceilings are ten times that value, so unrelated Telegram/VK diagnostics or
targets cannot consume the last product-delivery slot for another action while
every concrete target/action remains bounded. Forward is charged to its
destination; item-only actions are charged to the item's bound source target
rather than a shared provider bucket. A pre-provider denial returns a bounded
code such as `ATTEMPTS_BUDGET_EXCEEDED`, is durably audited, and never creates
an operation row or calls the adapter.

Social reads also use durable hourly rate, egress and media budgets. A provider
call that returned successfully remains a provider success even when its safe
projected response is subsequently withheld by one of those local budgets.
Local quota, projection/storage or response-cap failures are audited and
returned fail-closed, but they do not increment the provider transport circuit.
Only an actual adapter/provider failure or provider flood wait contributes to
that circuit; a later legitimate read must not be rejected as a provider outage
merely because an earlier response exhausted a local quota.

## Environment and credentials

Base MCP settings when enabled:

```text
PRIVATE_EVENTS_MCP_ENABLED=1
PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL=https://events-bot-new-wngqia.fly.dev
PRIVATE_EVENTS_MCP_PATH_SECRET=<fresh>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID=<fresh ChatGPT id>
PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET=<fresh>
PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID=<fresh public Codex id>
PRIVATE_EVENTS_MCP_OPENCODE_OAUTH_CLIENT_ID=<optional public OpenCode id>
PRIVATE_EVENTS_MCP_OPERATOR_TOKEN=<fresh bootstrap token>
PRIVATE_EVENTS_MCP_SIGNING_KEY=<fresh>
PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN=<fresh; operator browser only>
PRIVATE_EVENTS_MCP_AUTH_DB_PATH=/data/private-events-mcp-auth.sqlite
PRIVATE_EVENTS_MCP_REPOSITORY_ROOT=/app
PRIVATE_EVENTS_MCP_REPOSITORY_SLUG=onedayonemasterpiece/events-bot-new
PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE=/app/.static-site-repo-sha
```

Image/document ingress and storage use nonsecret bounded settings. The allowlist
is mandatory when media/story or document send is enabled and has no implicit
host default because
ChatGPT file-download hostnames are an operationally observed dependency, not a
stable name promised by the connector contract. Current ChatGPT uploads can use
a rotating Azure storage-account label, so the production policy may explicitly
allow both the exact OpenAI host and the Azure Blob suffix:

```text
PRIVATE_EVENTS_MCP_MEDIA_ROOT=/data/private-events-mcp-media
PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS=*.oaiusercontent.com,*.blob.core.windows.net
PRIVATE_EVENTS_MCP_MEDIA_MAX_ASSET_BYTES=31457280
PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES=50331648
PRIVATE_EVENTS_MCP_MEDIA_MAX_STORE_BYTES=134217728
PRIVATE_EVENTS_MCP_MEDIA_ASSET_TTL_SECONDS=3600
PRIVATE_EVENTS_MCP_MEDIA_DOWNLOAD_TIMEOUT_SECONDS=20
PRIVATE_EVENTS_MCP_MEDIA_MAX_WIDTH=8192
PRIVATE_EVENTS_MCP_MEDIA_MAX_HEIGHT=8192
PRIVATE_EVENTS_MCP_MEDIA_MAX_PIXELS=40000000
```

The document limit defaults to `50331648` bytes (48 MiB) and has a hard
configuration maximum of `67108864` bytes (64 MiB). The source-default
`PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=0` keeps the document
surface absent. When enabled it derives common asset ingress from
`media_story_enabled or file_send_enabled`; it does not weaken or implicitly
enable the image/story gate. An enabled invalid document limit, missing
allowlist/root, store quota smaller than the largest enabled asset class, or
missing Telegram/DM/provider support fails closed. When the file-send feature is
off, a stale malformed document-limit value remains inert during disabled
startup.

`*.blob.core.windows.net` is handled more narrowly than an ordinary wildcard.
The URL must have exactly one canonical Azure storage-account label, a blob
path, and a non-expired blob-scoped SAS granting only `sp=r` (and HTTPS when
`spr` is present). Unsigned, container-wide, write-enabled, duplicate-field or
expired SAS URLs fail before DNS or network I/O. Every download still uses
public-IP resolution/pinning, no redirects, no caller credentials and the
measured image/byte/dimension limits below. The signed URL, file ID and filename
are never persisted or logged.

TTL configuration is capped at `86400`. Each fileParams stage accepts exactly
one selected file. Telegram continues to use only
`TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP`; VK story work uses only the
blank-by-default `PRIVATE_EVENTS_MCP_VK_STORY_READER_TOKEN` and
`PRIVATE_EVENTS_MCP_VK_STORY_EDITOR_TOKEN` roles (and the dedicated analytics
role for statistics). Store all credential values in the runtime secret store,
not `.env.example`, docs, chat or command output.

For a genuinely new installation, generate one stable endpoint/OAuth identity
after merge from a clean exact-main checkout into a new owner-only directory
outside Git. The identity-changing mode is deliberately explicit:

```bash
umask 077
python scripts/generate_private_events_mcp_credentials.py \
  --new-install \
  --base-url https://events-bot-new-wngqia.fly.dev \
  --output-dir /secure/new-private-events-mcp-credentials \
  --enable-chatgpt-social
```

Without `--enable-chatgpt-social`, the ChatGPT profile requests only the three
evidence read scopes. With it, the profile contains the granular social scopes
including `vk:notifications:read`, plus `offline_access`; runtime switches still
remain off until explicitly set.
An already installed connector with the four original provider-level social
scopes does not need a new name or identity: the compatibility families above
cover later typed tools within the same provider/read-write boundary.
The generator has no implicit/default generation mode. `--new-install` creates
a new private path, ChatGPT client ID/secret, Codex/OpenCode public client IDs, signing key,
social-approval token and bootstrap token. Do not use it for routine bootstrap
rotation: changing those stable values can invalidate an installed connector,
refresh-token/signing state and saved endpoint configuration. Use a complete
new identity only for an initial install or an explicit full-identity response
such as a compromised private path.

### Upgrade the existing ChatGPT connection

For an already installed connector, preserve the exact endpoint/private path,
ChatGPT client ID/secret, OAuth resource/audience, signing key/state and
connector name. Treat two refresh boundaries separately:

1. **Connection/OAuth refresh** keeps the existing access/refresh-token flow
   healthy. It does not prove that a changed action definition was approved.
2. **Workspace action refresh** updates ChatGPT's frozen tool/input snapshot.
   After any tool description, schema or security-scheme change, an app owner
   or workspace administrator must open the existing app's action control,
   choose **Refresh**, review the diff, enable/publish the reviewed update, and
   only then start a **new chat** with the app selected. For an unpublished
   developer app, use its equivalent Scan Tools/Refresh flow before the new
   chat.

The voice-batch contract changes both input and output definitions for
`social_item_resolve`, `social_content_feed`, `social_content_item` and the
comment branch of `social_content_thread`. After that release, refresh/review/
publish all four in the existing app before acceptance; do not change the
endpoint, OAuth app, resource/audience or credentials.

Old chats may retain an earlier catalogue. A successful OAuth refresh or a
direct server/OpenCode `tools/list` probe is not ChatGPT action-publication
evidence. If the workspace plan does not expose an in-place action-update path,
stop and treat republishing as a controlled app-publication migration; do not
improvise by rotating the MCP identity or deleting the working connection.

Never delete/re-add the connection and never rename it for a normal media/story
upgrade. Never run `--new-install` to make these tools appear. A replacement
identity is reserved for initial installation or a deliberate full-identity
incident response (for example, a compromised private path/signing identity).
Refresh is not an activation bypass: scopes, runtime switches, media allowlist,
provider roles and typed preparation binding remain independently mandatory;
edit/delete additionally retain browser approval.

After the first successful browser connection, rotate only the one-time
bootstrap operator token from the **full** credentials JSON produced for the
installed identity:

```bash
umask 077
python scripts/generate_private_events_mcp_credentials.py \
  --rotate-bootstrap-only \
  /secure/new-private-events-mcp-credentials/chatgpt-private-app-credentials.json \
  --output-dir /secure/private-events-mcp-bootstrap-rotation-20260809
```

This mode changes the consistent bootstrap-token copies in deploy, ChatGPT,
Codex and, when present, OpenCode:
`deploy.PRIVATE_EVENTS_MCP_OPERATOR_TOKEN`, `chatgpt.bootstrap_operator_token`,
`codex.bootstrap_operator_token` and `opencode.bootstrap_operator_token`.
It preserves every URL/path, OAuth client value, signing/state path, scope,
social-approval value and all unknown forward-compatible fields value-for-value
at the JSON data level. It refuses incomplete/inconsistent full bundles,
source/output overlap, symlinked source or output paths, and mode-changing
options such as `--base-url` or `--enable-chatgpt-social`.
The base URL must be a canonical HTTPS origin with valid DNS labels or a
canonical IP literal and no credentials, legacy numeric-IP spelling, IPv6 zone,
whitespace/control characters, query, fragment or explicit port.
Every deploy field, including a forward-compatible
unknown field retained during rotation, must have a valid environment name and
a single-line NUL-free string value before `fly-secrets.env` can be written.

All modes require a fresh output path under an existing non-symlink parent.
The generator creates the output directory as `0700` and every artifact with
`O_EXCL`/no-follow semantics and exact `0600` permissions. Source credentials
must remain owner-only and outside Git. Stdout contains only artifact paths, a
public origin, redacted path, mode and endpoint fingerprints; it never contains
the private endpoint, bootstrap token, client secret, signing key, private path
or social-approval token.

### Connect OpenCode on Windows

OpenCode is a separate static public OAuth client on the full ChatGPT resource;
never reuse the ChatGPT confidential client secret or the Codex read-only
client ID. For an existing stable deployment, add only the OpenCode registration
without changing the path, issuer, signing key or existing clients:

```bash
umask 077
python scripts/generate_private_events_mcp_credentials.py \
  --add-opencode-client \
  /secure/current/chatgpt-private-app-credentials.json \
  --output-dir /secure/private-events-mcp-opencode-20260810
```

The owner-only `opencode-private-mcp-config.json` contains a ready stable-format
`opencode_config` block. Merge its `mcp.eventsBot` entry into the Windows global
config at `%USERPROFILE%\.config\opencode\opencode.json` (or use a project-root
`opencode.json`). Then run:

```powershell
opencode mcp auth eventsBot
opencode mcp list
opencode mcp debug eventsBot
```

The browser page accepts the bootstrap operator token once and redirects only
to the exact `http://127.0.0.1:<port>/mcp/oauth/callback` URI requested by
OpenCode. The generated default is port `19876`. If it is occupied, choose a
free unprivileged port and change both `oauth.callbackPort` and the port inside
`oauth.redirectUri` to the same value; do not close unrelated applications just
to retain the default. The OpenCode config includes only the public client ID—
there is no client secret.
These fields and commands follow the current official
[OpenCode MCP OAuth contract](https://opencode.ai/docs/mcp-servers/) and
[global configuration path](https://opencode.ai/docs/config/).
The requested stable scopes are the three evidence reads, `offline_access`, and
provider-level `telegram:read|publish vk:read|publish`, which map only within the
same provider/read-write family. Runtime feature flags and provider rights still
apply. OAuth tokens are stored by OpenCode outside the project config.

OpenCode does not implement ChatGPT's `openai/fileParams` rewrite. Therefore a
plain MCP connection supports event/incident tools and text/social operations,
but a Windows local image path is not a valid `social_asset_stage.file`. Local
media requires the separate authenticated streaming bridge that returns the
same principal-bound `ast_*`; never pass a Windows path or raw file ID to the
remote server.

The legacy `PRIVATE_EVENTS_MCP_SOCIAL_TARGETS_JSON` policy applies only to the
narrow compatibility adapters when the universal workspace is off. It is not a
read-target setting and cannot enable Saved or arbitrary users.

## Integration and disabled behavior

`main_part2.create_app()` selects exactly one social implementation:

- universal enabled: build the Telegram/VK workspace adapter set and omit the
  legacy tools;
- universal disabled: optionally keep the prior alias-only text adapters;
- MCP disabled: parse no MCP/provider secrets, build no adapter and attach no
  route.

The overlay installer copies the provider-neutral package plus all four
top-level adapter/provider modules, patches `main_part2.py` (or the single valid
app module), and merges private fixtures without replacing an existing
`tests/conftest.py`.

## Local verification

```bash
PYTHONPATH=. python -m compileall -q \
  private_events_mcp \
  private_events_mcp*.py \
  tests scripts main_part2.py
PYTHONPATH=. pytest -q tests/test_private_events_mcp_*.py

git diff --check
```

The repository CI runs this explicit complete Private Events MCP test glob,
compiles the package plus every top-level MCP adapter/workspace-provider module,
and runs `git diff --check` without removing any existing repository job. A full
release requires every existing GitHub Actions job to be green. Independent
review must use the exact proposed head SHA.

The optional live smoke is nonmutating unless its write gate is supplied:

```bash
# catalogue only; prints sanitized counts/fingerprints, never credentials/URLs
python scripts/smoke_private_events_mcp_media.py \
  --credentials /secure/chatgpt-private-app-credentials.json

# bounded reads require explicit opaque refs
python scripts/smoke_private_events_mcp_media.py \
  --credentials /secure/chatgpt-private-app-credentials.json \
  --platform telegram --target-ref '<opaque-target-ref>' --read-stories

# one bounded image content block; receipt prints metadata/fingerprint only
python scripts/smoke_private_events_mcp_media.py \
  --credentials /secure/chatgpt-private-app-credentials.json \
  --platform telegram --preview-asset-ref '<opaque-story-image-ref>'

# nonmutating document descriptor check; add --target-ref to check live rights
python scripts/smoke_private_events_mcp_media.py \
  --credentials /secure/chatgpt-private-app-credentials.json \
  --platform telegram --check-document-contract \
  --target-ref '<opaque-saved-target-ref>'
```

Preparing an image story additionally requires `--allow-write`, explicit
target/asset refs, an idempotency key and a fresh owner-only receipt path.
Committing requires `--allow-write` and that owner-only preparation receipt.
The analogous `--prepare-document` mode requires an `ast_*` that was already
staged by the real ChatGPT conversation, a Telegram target, explicit caption,
idempotency key and fresh receipt; `--commit-document` consumes that owner-only
receipt. Both print only safe metadata and one-way ref/digest fingerprints.
They neither accept a local file/URL/file ID nor produce genuine `fileParams`,
so real ChatGPT UI staging and Telegram UI/download read-back remain mandatory.
The script expects an explicitly requested outbound preparation to be directly
`approved`; it never opens or prints a browser approval URL. Edit/delete are
outside this smoke and retain the separate approval page. It never prints access/refresh
tokens, credential values, private URLs, raw provider payloads or the approval
URL.

## Production acceptance gate

Deploy only an exact merged `origin/main` SHA via `scripts/deploy_fly_main.sh`.
Keep all new social switches off until credentials are staged and the
provider-specific canary is authorized.

### Telegram document rollout and rollback

Use one clean worktree checked out at the exact current `origin/main`; do not
deploy this feature from an integration/feature branch:

1. `git fetch origin --prune`; record `git rev-parse origin/main`; require a
   clean status and `HEAD == origin/main`, then run
   `scripts/deploy_fly_main.sh`. Record merged, Fly release and in-container
   immutable SHAs and require equality.
2. Keep `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=0`. Through the
   existing owner terminal and Fly secret store, set only the scoped document
   names needed for this rollout (the flag and, if overriding the default,
   `PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES`). Reuse the already approved
   owner-only media root and observed ChatGPT host policy; never paste secret
   values, the private endpoint or signed file URLs into chat, logs or artifacts.
3. With the flag still off, preflight public/internal `/healthz`, Fly machine
   state, exact SHA, both SQLite `quick_check=ok`, auth DB/root permissions,
   retained-byte quota and disk headroom, webhook/scheduler health, runtime-log
   mirror, OAuth metadata, Codex exact-seven catalogue, text send, and existing
   image/story behavior. Confirm staging/prepare causes zero provider calls.
4. Enable only `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=1` through
   the same scoped secret mechanism. Recheck readiness/logs and refresh the
   existing `eventsBot` ChatGPT connection **in place**, then start a new chat.
   Do not use `--new-install`, rename, delete/re-add or mint another connector
   identity.
5. In that new ChatGPT chat, select a deterministic tiny APK-shaped fixture in
   the actual upload UI. The connector—not a local script—must supply the actual
   closed `fileParams` object. Stage with
   `platform=telegram, role=document`; require ready `ast_*`, detected APK MIME,
   exact size/SHA-256, sanitized `.apk` filename and expiry, with no URL, file
   ID, original name, native ID or path in response/logs/artifacts.
6. Resolve Saved Messages with `social_target_resolve`; require target
   capabilities to include both `send_message` and `document`. Prepare exactly
   one document plus a clear canary caption, require `approved` and no
   `approval_url`, and inspect the frozen preview/digest for target, filename,
   size, MIME, digest prefix and expiry. Provider attempt count must remain zero.
7. Commit once. Require exactly one Telethon attempt, a durable `succeeded`
   receipt, read-after-write confirmation, and a downloadable Telegram document
   whose filename, size and caption match the frozen preview. Record only opaque
   ref fingerprints/counters and safe metadata. Delete the canary only through
   the existing typed delete flow if policy permits; otherwise leave exactly one
   clearly labelled canary and report that state.
8. Run a live negative probe that stops before Telegram transport (for example,
   an ordinary ZIP renamed `.apk` or an expired asset). Also confirm VK,
   multiple documents, mixed image/document and non-`send_message` actions fail
   before provider I/O.
9. Rollback-probe the narrow switch: set only file-send to `0`; require the
   document role/capability to disappear while evidence tools, text sends and
   image/story remain healthy. Then restore it to `1`, recheck the new-chat
   catalogue and harmless readiness so the intended final production state is
   enabled. For a real incident, leave it `0` until remediation is merged and
   accepted; staged document assets simply expire under bounded cleanup.

The sanitized smoke below can verify catalogues, capabilities and an already
staged opaque asset's prepare/commit boundary, but it cannot create the actual
ChatGPT upload or replace steps 5–7 in the ChatGPT UI.

Record all of the following without secrets:

1. merged main SHA, Fly release and in-container immutable SHA are identical;
2. public/internal `/healthz` remains ready, DB check is healthy and
   `PRAGMA quick_check` is `ok`;
3. ChatGPT confidential plus OpenCode/Codex public OAuth/PKCE flows, exact resource
   metadata, access expiry and refresh rotation pass; every unauthenticated MCP
   JSON-RPC request, including `initialize`, `tools/list` and `tools/call`, returns
   HTTP 401 with the exact endpoint-specific `WWW-Authenticate` resource-metadata
   challenge. This is required for OpenCode to start OAuth instead of accepting
   anonymous initialization as a completed connection;
4. Codex `tools/list` is exactly the seven evidence tools and direct social calls
   fail; ChatGPT/OpenCode list only granted and enabled workspace tools; neither
   full-resource client token is accepted on the Codex endpoint;
5. real event `search -> fetch`, `events_search -> event_get`, incident evidence
   and `operations_snapshot` pass; the event DB digest/row state is unchanged;
6. unsupported MCP protocol version and JSON-RPC batch return HTTP 400;
7. nested synthetic credentials and provider native identifiers never appear in
   responses, logs or artifacts;
8. targeted editorial canary records its closed authorization basis, reads the
   named target in pages and cannot exceed 100 or replay a cursor against
   another target/access class;
9. a fresh, explicitly requested Telegram Saved canary prepares as `approved`
   without an `approval_url`, commits exact text/image, then reads back the exact
   provider receipt/message; do not auto-delete it and never execute an older
   `awaiting_human_approval` preparation;
10. one explicitly authorized exact-person reminder canary resolves and previews
    the intended person before any send; no arbitrary/bulk fan-out;
11. VK public/editorial and any write canary use the explicit configured actor,
    never fallback, and verify the exact receipt/read-back;
12. mutations fail before provider transport when scope, feature flag, current
    rights, target/item binding, preparation authorization, budget or idempotency
    is invalid; edit/delete additionally require external approval;
13. provider timeout/fence loss remains outcome-unknown and reconciliation does
    not blind-retry; encrypted provider refs/cursors/receipts remain usable
    after a controlled restart, while an interrupted action remains unknown;
14. provider call evidence is zero for Codex/evidence queries and accurately
    attributed for the separately approved social canary;
15. webhook latency/errors, scheduler/jobs, runtime-log mirror, disk free space
    and auth DB permissions show no regression; the auth/provider state file is
    `0600` from creation and daily attempt budget rows use the current UTC date.
16. the existing `eventsBot` ChatGPT connection was refreshed in place and a
    new chat sees the enabled image/story/document tools; endpoint,
    client/resource/audience, signing identity
    and connector name are unchanged, while Codex still lists exactly seven
    evidence tools and no social/file tool. When audio transcription is
    enabled, `audio_transcription_start`, `audio_transcription_status` and
    `audio_transcription_get` remain positions 1–3 without dropping or renaming
    any existing tool;
17. one ChatGPT-selected image stages through `fileParams`; status reports the
    server-detected MIME, exact size, SHA-256, dimensions and expiry, and neither
    the response nor logs/artifacts contain its download URL, file ID/name or
    local path; a path/string-only input returns `FILE_REF_UNRESOLVED`, and any
    real-shape rejection returns one of the documented safe codes rather than
    the generic `social workspace request rejected`;
18. wrong host/private DNS, redirect, oversize, quota exhaustion, MIME spoof,
    decompression/pixel bomb, changed digest, expired ref, second file and video
    all fail before provider upload; the image role accepts only verified
    JPEG/PNG/WebP bytes, while the independent document role follows its closed
    structural allowlist;
19. for Telegram and VK separately, one explicitly requested safe-target image
    story follows `stage -> prepare(approved) -> commit -> provider read-back`;
    the preparation binds the exact provider/target/action and immutable asset
    digest/MIME/size/dimensions/expiry, and no upload occurs before commit;
20. bounded story list/item/statistics reads return opaque story/media refs and
    aggregate counters; `social_asset_preview` returns one bounded, stripped
    JPEG thumbnail for an image ref while never exposing provider URLs or native
    IDs. Viewer identities are absent, the trace has no viewer-list/mark-read
    operation, and provider role attribution matches the dedicated Telegram MCP
    or VK story/analytics credential;
21. restart preserves valid immutable refs/receipts, expired cleanup remains
    bounded, retained bytes stay within quota, and disabling only the media/story
    switch removes asset/story tools without changing the connector identity or
    the seven evidence tools.
22. when document send is enabled, one actual ChatGPT-selected tiny APK follows
    `fileParams -> stage(document) -> Saved resolve -> prepare(approved) -> one
    commit -> read-back`; the immutable filename/MIME/size/SHA/expiry binding,
    actual downloadable Telegram document, one-attempt count, negative probe
    and file-send off/on rollback probe all pass without secret/path/native-ID
    disclosure.
23. a sanitized public and private Telegram item-link canary resolves through
    `social_item_resolve`; a voice-bearing `social_content_item` response keeps
    an outer `media[]` ref, safe attachment metadata and either ready text or an
    opaque queued/running transcription ref. A repeat read is a cache hit with
    no second provider download/job, `transcribe_audio=false` makes no ingress,
    sibling audio failures stay isolated, and response/log/audit contain no
    native/provider/session/path data. The existing connector reconnects only
    through the normal service restart path and is never deleted/re-added.
24. a cancelled/restarted scheduled Telegram operation with a claimed provider
    row converges through raw scheduled/live readback; no NULL result or
    `reconciliation_pending` loop survives its finite deadline;
25. `social_scheduled_items_list` is present only under the existing schedule
    scope families, returns one logical item for a Telegram album and the exact
    VK postponed owner queue, enforces filters/limits/redaction, and remains
    distinct from ordinary feed/history;
26. exact scheduled deletion uses the Telegram scheduled namespace or exact VK
    postponed owner/post and verifies queue absence; ambiguous matches are not
    auto-deleted;
27. bounded retry accepts one terminal pre-mutation `retry_safe=true` attempt,
    rejects concurrent/unknown/pending retries, preserves logical action and
    preparation refs, rearms the Telegram or VK provider ledger through its own
    compare-and-set guard, and records the new attempt plus final readback. The
    changed actions are administrator-reviewed/published and exercised from a
    genuinely new ChatGPT conversation.
28. Telegram media publication exercises the full supported one-to-ten item
    envelope. Its adapter deadline is derived from total verified bytes and
    item count (bounded to ten minutes), and the MCP commit/retry deadline
    covers that adapter envelope. Provider file uploads are preparatory only:
    the durable content-mutation boundary is crossed immediately before the
    final message/album/story send. A timeout during upload must be terminal
    `provider_mutation_not_started / retry_safe=true`; a timeout after the final
    send remains `outcome_unknown / retry_safe=false` until readback resolves it.
29. The regression canary for
    `INC-2026-08-31-mcp-scheduled-readback-reschedule` uses four fresh small
    images in Telegram Saved Messages (or an owner-approved isolated test
    channel), never the production target. In a genuinely new ChatGPT chat it
    performs four `fileParams -> stage` calls, one `schedule prepare(approved)`,
    the real two-field commit, and provider-backed scheduled readback. Exactly
    one logical media group with `media_count=4` must exist in the original
    order; four separate messages are a failure. The exact scheduled canary is
    then deleted through the typed scheduled namespace and a fresh read proves
    zero matches before the slot. A real target publication is still a separate
    owner action and is never inferred from this canary.

Rollback order: first turn
`PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=0` for a document-only
incident; this removes document staging/capability while leaving text and the
independent image/story flag unchanged. For an image/story incident, turn
`PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED=0`. If the issue is
broader, turn the universal social master off; turn
`PRIVATE_EVENTS_MCP_ENABLED=0` only for a complete MCP rollback. Preserve the
existing endpoint/client/resource/signing identity throughout a routine
rollback—do not delete/re-add/rename the ChatGPT connection. The isolated
OAuth/social DB may remain on the volume. The media root contains only
short-lived immutable files and may be drained by the bounded expiry cleanup
after the feature is off; do not manually reuse its refs or copy it into the
event database.

Scheduled-queue edit/delete is direct only for an exact principal-bound scheduled/postponed item; published/live content still requires external approval.


## Partner access implementation checkpoint (2026-09-06)

**Default OFF, not a complete partner event/promo product.**
`PRIVATE_EVENTS_MCP_PARTNER_ENABLED=1` installs the independent resource
`<public-base><private-prefix>/events-partner/mcp` in the existing runtime.
The existing issuer/token/authorization endpoints are reused. Owner, Codex,
OpenCode and partner resources do not accept each other's tokens.

An owner must explicitly authorize the additional `partners:manage` scope.
Existing grants do not acquire that scope automatically. `partner_create`
accepts tenant, organization, display name, exact redirect URIs, expiry,
portfolio event IDs and a closed policy (scopes/actions/auto_approve/limits).
It returns the public `client_id` and a login code **once**. Deliver the login
code privately; do not put it in an app as an OAuth `client_secret`.
The partner enters it only on the issuer's browser consent form and uses
Authorization Code + S256 PKCE. Telegram registration is not involved.

`partner_get` reads current rights and portfolio without a stored secret.
`partner_access_change` requires `expected_revision` and performs suspend,
resume, terminal revoke, credential rotation, policy replacement or portfolio
replacement. Suspend/rotation/revoke increment the credential epoch, so old
access tokens and refresh grants cannot revive on resume. Current grants are
read again on every HTTP request and before protocol cache/discovery. Policy
changes invalidate overbroad old tokens; acquiring new scopes needs consent.

`partner_workspace_get` and `partner_events_list` expose only the assigned
portfolio. Search filtering happens after the principal/tenant/organization
join and before pagination. Direct foreign IDs fail closed. There is no
partner access to owner search/fetch, incidents, operational snapshots or
Social Workspace. Unsupported event/promo mutations are explicitly false.

The three `mcp_partner*` tables are additive canonical-DB policy tables. They
are not a second event database, scheduler or Social Workspace store. Startup
never backfills credentials or assigns existing events. Repeated init is
covered by `tests/test_private_events_mcp_partner_access.py`; real local
OAuth -> MCP -> policy -> canonical SQLite read and restart tests are in
`tests/test_private_events_mcp_partner_protocol.py`. No external provider or
production destination is involved. This checkpoint does not satisfy the
full registry-v2 event/media/review/lifecycle/promo acceptance gates.

Rollout remains R0 -> R1 -> R1b -> R2 -> R3 -> R4. This source checkpoint is an
R4 prerequisite behind its own flag, not permission to enable R4 before the
preceding product gates. Rollback disables the flag first and keeps additive
tables; it never deletes partner history or rewrites the canonical event DB.
