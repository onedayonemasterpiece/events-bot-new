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

These tools are read-only, non-destructive and idempotent. The event SQLite file
is opened with URI `mode=ro`, `PRAGMA query_only=ON`, bounded rows and a VM/time
deadline. There is no raw SQL, shell, arbitrary outbound HTTP or write tool.

The ordinary structured-result response cap remains
`PRIVATE_EVENTS_MCP_MAX_RESPONSE_BYTES`. Authenticated `tools/list` metadata is
separately bounded at 512 KiB (and is still charged to the shared hourly egress
budget), because the full stable-scope ChatGPT/OpenCode catalog can exceed the
ordinary 128 KiB data-result default. This does not raise provider-content,
incident or evidence response limits.

## ChatGPT social workspace

When the universal workspace is enabled, ChatGPT can discover only the tools
matching its granted scopes and the enabled provider/capability flags:

| Tool | Purpose |
|---|---|
| `social_capabilities` | current provider, target and action capabilities |
| `social_target_resolve` | resolve Saved/self, exact person, channel/group/community or known provider reference into an opaque bound ref |
| `social_item_resolve` | resolve one canonical VK wall-post URL or public/private Telegram message URL into bound item and source-target refs |
| `social_targets_search` | bounded target search |
| `social_targets_list` | bounded public/managed target discovery |
| `social_dialogs_list` | VK-only metadata list of all or unread dialogs: opaque target, display name, kind and unread count, with no message body/native peer ID |
| `social_content_search` | bounded keyword search |
| `social_content_feed` | bounded target feed/history |
| `social_content_item` | fetch one bound item |
| `social_content_thread` | comments or reaction summaries |
| `social_comment_hints_list` | bounded recent VK comment/mention notifications as untrusted investigation hints |
| `social_content_stories` | bounded Telegram/VK story page with opaque media refs and no mark-read/viewer identities |
| `social_content_editorial_sample` | purpose-bound editorial sample, at most 25 items/page and 100 cumulatively |
| `social_content_analytics` | bounded aggregate post/story statistics or audience counts where the credential is entitled |
| `social_asset_stage` | ingest one ChatGPT `fileParams` image or eligible Telegram document into immutable short-lived server storage |
| `social_asset_status` | return only verified MIME/size/digest/dimensions or sanitized document name/classification, expiry and lifecycle state for an opaque asset ref |
| `social_asset_preview` | render one principal-bound story image as a bounded metadata-free MCP JPEG thumbnail |
| `social_action_prepare` | freeze exact typed action/content/target/media digest; explicitly requested outbound actions return `approved` with no provider call, while edit/delete wait for external approval |
| `social_action_commit` | atomically consume the exact preparation authorization, then make the sole provider attempt |
| `social_action_status` | reconcile durable success/failure/outcome-unknown state |

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

Read-triggered jobs are bound to the authenticated principal and an HMAC of the
target/item/media identity, then verified against the content SHA-256. A repeat
read checks the durable job before downloading, so it neither downloads the
same media nor creates a second job. Fast completion projects `status=ready`
and bounded transcript text; otherwise the attachment carries
`queued|running` plus an opaque `atr_*` `transcription_ref`. A later identical
read projects the completed text automatically. One attachment failure is a
typed per-attachment `failed` result and cannot fail sibling media or the
message/thread page. Transcript text and attachment metadata carry the exact
`untrusted_external_data` marker. Responses, errors and audit exclude native
IDs, access hashes, file references, provider/model details, auth/session data,
URLs and paths. This internal path is authorized by the social read scope; it
does not invoke or weaken the standalone `audio:transcribe`/publish tool gate.

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
and takes precedence across restarts. Clients must not busy-poll or invent a
fixed completion deadline: inspect status at a bounded cadence, honor the
reported durable state, call `audio_transcription_get` only after `complete`,
or repeat the same high-level social read later to receive ready text from the
cache. A multi-voice chat may take several serialized runs to finish.

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
   `approval_url`; edit/delete remain `awaiting_human_approval`;
3. for edit/delete only, the operator opens the returned URL, enters the
   separate approval token, inspects the exact human-readable target, source
   item, action and text, then confirms in the browser. An opaque-only
   target/item preview or an item without a human-readable source target fails
   closed;
4. `social_action_commit` accepts only the preparation ref and digest. It
   rechecks the current action-class kill switch, then atomically consumes the
   server-side approval before one provider attempt;
5. `social_action_status` or read-after-write evidence reconciles the receipt.

The approval token is never pasted into ChatGPT. It is not an OAuth token and
must not appear in model context, logs, PRs or artifacts. A provider timeout is
`outcome_unknown` and `retry_safe=false`; do not retry with a new idempotency
key until reconciliation.

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
PRIVATE_EVENTS_MCP_VK_ANALYTICS_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_READER_TOKEN
PRIVATE_EVENTS_MCP_VK_STORY_EDITOR_TOKEN
```

Only configured actor/action capability sets are advertised. The transport uses
fixed VK API 5.199 method paths, rejects redirects, bounds responses, and emits
only sanitized provider error codes. Public-wall and private-dialog access are
separate; a public scope cannot route to conversation history. Cursor context
binds target, operation, access class and unread/all mode. `social_dialogs_list`
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

Image/story operations preserve the same separation: community publication,
story editing, story reads and story aggregate analytics are authorized by
their dedicated actor roles and fixed capability sets. A generic token or
`main.vk_api` fallback cannot satisfy a missing role. The adapter receives a
verified local asset stream and never exposes or accepts a raw VK upload-server
URL/method. Story metrics are aggregate views/likes/replies/shares where VK
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
global/principal/target/action limits. Forward is charged to its destination;
item-only actions are charged to the item's bound source target rather than a
shared provider bucket (the configured per-principal default is
`PRIVATE_EVENTS_MCP_SOCIAL_PUBLISH_ATTEMPTS_PER_DAY=10`).

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
PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS=files.oaiusercontent.com,*.blob.core.windows.net
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
