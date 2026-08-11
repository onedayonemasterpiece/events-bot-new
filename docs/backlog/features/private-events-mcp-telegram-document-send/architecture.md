# Design: Telegram document sending through private Events MCP

## 1. Decision

Implement a narrow first release named **Telegram message document v1**.

The release allows ChatGPT/OpenCode to stage one authenticated file and attach it to a typed Telegram `send_message` action. The first concrete acceptance case is delivery of an APK to Telegram Saved Messages. The same contract may work for a user or group only when the existing Telegram adapter already grants `send_message` for that exact opaque target.

This is an extension of the existing immutable asset pipeline, not a new downloader and not a raw Telethon proxy.

### Included

- provider: Telegram;
- action: `SocialAction.SEND_MESSAGE`;
- target: only a resolved opaque target that already permits `send_message`;
- content: zero or one `MediaRole.DOCUMENT` attachment and optional caption/rich text;
- ingress: ChatGPT `fileParams` only;
- initial default types: APK, PDF, ZIP, UTF-8 TXT/MD/CSV/JSON, DOCX/XLSX/PPTX;
- maximum: configurable, default 48 MiB, hard cap 64 MiB;
- immutable digest and expiry checks before both prepare and commit;
- read-after-write evidence and durable `outcome_unknown` handling.

### Explicitly excluded

- caller-supplied arbitrary URL, native Telegram file ID, filesystem path, base64, or raw bytes in MCP JSON;
- VK documents;
- stories, video, audio, animation, voice, stickers, executables other than structurally recognized APK, generic `application/octet-stream`, media albums, multiple files, or mixed image+document actions;
- `publish`, `schedule`, `comment`, `edit`, `forward`, and `delete` with a new document in v1;
- antivirus or publisher-authenticity claims;
- automatic production activation or deployment.

## 2. Current-state diagnosis

The common domain model is already mostly ready. `MediaRole` and `ContentFeature` contain `DOCUMENT`, and rich content maps document attachments into the feature gate. The blockers are deliberate image-only restrictions around the reusable model:

1. `private_events_mcp/social_workspace.py`
   - `validate_asset_stage_request()` rejects every role except `IMAGE`;
   - the MCP JSON Schema advertises only `role=image`;
   - MIME validation omits the `text/*` family;
   - the incoming `file_name` is validated but the current image contract intentionally does not preserve a safe display filename.

2. `private_events_mcp/social_workspace_runtime.py`
   - `stage_asset()` rejects non-image roles;
   - asset staging authorization only accepts post/story scopes;
   - capability projection removes `document`, even if the Telegram adapter advertises it;
   - runtime asset limits and validation assume image dimensions.

3. `private_events_mcp/social_workspace_tools.py`
   - `social_asset_stage` and `social_asset_status` are exposed only under the `media_story` feature;
   - every action containing any media requires `media_story`;
   - asset scope options contain only post/story write families.

4. `private_events_mcp/config.py`
   - storage host allowlist, size, TTL, and download limits are parsed and validated only when `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED` is true;
   - no independent file-send kill switch exists.

5. Telegram provider path
   - the project already uses Telethon and its test doubles already expose `send_file`;
   - provider code must be extended to bind a verified document at stage time and call exactly one `send_file` at commit time;
   - the provider must never receive or fetch the ChatGPT download URL.

The correction is therefore a controlled split of **asset ingress** from **image story**, followed by one Telegram execution branch.

## 3. Target flow

```text
user explicitly asks to send a file
        │
        ▼
ChatGPT obtains a local/uploaded file
        │  fileParams; no path or URL in MCP JSON
        ▼
social_asset_stage(platform=telegram, role=document, file=...)
        │
        ├─ OAuth scope check: telegram:dm:send (legacy publish compatibility remains server-owned)
        ├─ HTTPS fileParams host allowlist / public DNS / no redirect
        ├─ bounded streaming into immutable store
        ├─ document byte classifier + filename sanitizer
        ├─ size + SHA-256 + TTL + regular-file checks
        └─ opaque principal/provider-bound ast_* reference
        │
        ▼
social_action_prepare(action=send_message, target_ref=tgt_*,
                      content={text/caption, media:[{role:document, asset_ref:ast_*}]})
        │
        ├─ target/action permission check
        ├─ exactly one document and no mixed media
        ├─ re-open + re-hash immutable bytes
        ├─ freeze digest, MIME, byte length, safe display name, expiry
        └─ approved preparation for the explicit user request
        │
        ▼
social_action_commit(preparation_ref, action_digest)
        │
        ├─ atomically consume authorization
        ├─ re-check flags/scopes/rights/budgets/TTL/digest
        ├─ exactly one Telethon send_file attempt
        ├─ force document semantics; caption/entities preserved
        └─ read-after-write verification and opaque item_ref
```

A provider timeout remains `outcome_unknown`, `retry_safe=false`. No second send is attempted until reconciliation.

## 4. Contract changes

### 4.1 Feature flags

Add to `PrivateEventsMCPConfig`:

```python
universal_social_file_send_enabled: bool = False
max_document_bytes: int = 48 * 1024 * 1024
```

Environment variables:

```text
PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=false
PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES=50331648
```

Use this derived gate everywhere shared storage is needed:

```python
asset_ingress_enabled = media_story_enabled or file_send_enabled
```

`PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS`, media root, store capacity, TTL, and download timeout are parsed/validated when `asset_ingress_enabled` is true. Image dimension/pixel settings remain meaningful only for image roles. The new document size setting must be in `1..64 MiB` and the aggregate store must cover the largest enabled asset class.

Startup must fail closed when file send is enabled without:

- universal social master flag;
- Telegram provider flag;
- DM action flag;
- absolute media root;
- non-empty ChatGPT download-host allowlist;
- injected asset ingestor;
- Telegram adapter document staging/execution support.

Do not silently enable the feature in existing deployments.

### 4.2 Asset-stage input

Keep the transport shape and `fileParams=("file",)`:

```json
{
  "platform": "telegram",
  "file": {
    "download_url": "<connector supplied>",
    "file_id": "<connector supplied>",
    "mime_type": "application/vnd.android.package-archive",
    "file_name": "tailscale-universal.apk"
  },
  "role": "document"
}
```

Changes:

- base schema role enum becomes `image|document`;
- tool construction narrows the enum dynamically to actually enabled roles;
- validator accepts `document` only for Telegram;
- MIME syntax accepts `text/*` in addition to image/video/audio/application, but document policy still uses a closed allowlist;
- `download_url`, `file_id`, declared MIME, and original filename remain untrusted transport hints;
- a sanitized `display_name` is derived server-side and is the only filename persisted for document delivery.

Do **not** add `source_url`, `path`, `native_file_id`, `bytes`, or provider method parameters.

### 4.3 Verified asset and manifest

Extend `VerifiedAsset` and the immutable manifest with:

```python
role: MediaRole
display_name: str | None
classification: str | None
```

For `IMAGE`, existing verified dimensions remain mandatory. For `DOCUMENT`, dimensions must be absent. Shared fields remain mandatory:

- immutable local locator/internal path;
- principal binding;
- SHA-256 content digest;
- byte length;
- detected MIME;
- expiry;
- lifecycle state.

`social_asset_status` may return the sanitized `display_name` for a document. It must never return the original download URL, ChatGPT file ID, original unsanitized name, internal path, Telegram native ID, or access hash.

### 4.4 Document policy

Do not broaden the image validator. Add a separate document validator/classifier, using the supplied prototype as a starting point.

Required invariants:

- regular file only; reject symlinks, devices, directories, empty files;
- stream download under the configured byte cap;
- detected bytes, not extension or declared MIME, decide classification;
- declared `application/octet-stream` may be treated as unknown/generic hint; any other explicit mismatch fails closed;
- filename: Unicode NFKC, strip controls/bidi overrides, basename only, bounded UTF-8 length, detected extension enforced;
- SHA-256 recomputed from stored bytes;
- ZIP/APK/Office inventory inspected without extraction;
- bounded ZIP entry count, declared uncompressed size, expansion ratio, encrypted archive rejection, and unsafe entry-name rejection;
- APK classification requires `AndroidManifest.xml` and Android payload such as `classes.dex`, `resources.arsc`, or `lib/`;
- no claim that an APK is genuine, signed, malware-free, or from a particular publisher merely because it is structurally valid.

Recommended initial MIME allowlist:

```text
application/vnd.android.package-archive
application/pdf
application/zip
application/json
text/plain
text/csv
text/markdown
application/vnd.openxmlformats-officedocument.wordprocessingml.document
application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
application/vnd.openxmlformats-officedocument.presentationml.presentation
```

Do not enable unrestricted `application/octet-stream` in v1. It may be accepted only as an incoming hint when bytes classify into one of the allowed types.

### 4.5 Scopes and tool exposure

Create a role-aware asset scope selector.

- image role: preserve current post/story behavior and legacy compatibility;
- document role: require the scope used by `SocialAction.SEND_MESSAGE` for Telegram, normally `telegram:dm:send`;
- the final prepare/commit path still re-authorizes the exact action and target, so staging cannot launder a document into a publish/story action.

Tool feature mapping:

```text
social_asset_stage   -> asset_ingress (media_story OR file_send)
social_asset_status  -> asset_ingress (media_story OR file_send)
social_asset_preview -> media_story only
social_content_stories -> media_story only
```

Replace the current blanket “any media requires media_story” rule with role-aware checks:

```text
IMAGE     -> existing media/story gate
DOCUMENT  -> file_send gate AND action == send_message AND platform == telegram
other     -> denied
```

### 4.6 Capability projection

The runtime currently allows only `image` through its provider capability projection. Change it to compute enabled media roles:

```python
allowed_media = set()
if image ingress/story is enabled:
    allowed_media.add("image")
if file send is enabled and platform == "telegram":
    allowed_media.add("document")
```

Further narrow by target capabilities. `document` must appear only when the resolved target advertises `send_message`. It must not appear for a read-only channel or a target that only supports publish/schedule.

### 4.7 Prepare-time restrictions

Before computing the action digest, enforce:

- platform is Telegram;
- action is `send_message`;
- exactly one attachment;
- role is `document`;
- no mixed image/document content;
- target already permits `send_message`;
- asset principal/provider binding matches;
- asset status is ready and unexpired;
- re-opened bytes are regular, within limit, and match stored digest/size;
- safe display filename and detected MIME are present.

Add these fields to `verified_assets` before `compute_action_digest()`:

```json
{
  "asset_ref": "ast_*",
  "role": "document",
  "content_digest": "sha256:...",
  "byte_length": 123,
  "mime_type": "application/vnd.android.package-archive",
  "display_name": "tailscale-universal.apk",
  "expires_at": "..."
}
```

The human-readable preview should contain target, action, sanitized filename, size, detected MIME, and a short SHA-256 prefix. Audit rows may retain only the existing safe digest/reason metadata; do not log URLs, paths, file IDs, or unsanitized names.

## 5. Telegram adapter implementation

### 5.1 Stage

`stage_asset(verified, role=DOCUMENT)` must create a provider binding to the immutable verified asset. It must not upload to Telegram and must not receive the ChatGPT URL. If the current adapter binding stores an input-media object only, extend it with a closed document binding containing the internal immutable locator and verified metadata.

### 5.2 Execute

Add a dedicated branch in the typed `send_message` execution path:

```python
await client.send_file(
    entity,
    verified_local_file_or_uploaded_input_file,
    caption=caption,
    formatting_entities=caption_entities,
    force_document=True,
    attributes=[types.DocumentAttributeFilename(file_name=display_name)],
)
```

Adapt the exact keyword names to the installed Telethon 1.44 signature and existing adapter conventions. Do not expose them as MCP arguments.

Provider requirements:

- reopen asset immediately before upload;
- verify owner, provider, TTL, regular file, byte length, and SHA-256 again;
- preserve the sanitized display filename rather than the opaque storage filename;
- use one provider attempt under the existing operation fence/idempotency claim;
- optional caption must use existing rich-text/entity conversion;
- zero-length caption is valid;
- timeout maps to `outcome_unknown`, never an automatic second send;
- success returns an opaque `item_ref` and existing read-after-write evidence.

### 5.3 Read-after-write

Verify at minimum:

- returned message ID exists in the intended target;
- returned message contains a document;
- provider-reported size equals the staged size when available;
- returned filename attribute equals the sanitized display name when available.

Telegram cannot prove the SHA-256 of uploaded content from message metadata; do not claim remote digest verification. The local pre-upload rehash plus provider message metadata is the correct bounded evidence.

## 6. Exact implementation map

| File | Required change |
|---|---|
| `private_events_mcp/document_policy.py` | Add the independent validator/classifier; integrate or adapt the supplied prototype. |
| `private_events_mcp/media_contract.py` | Make verified metadata role-aware; add safe display name/classification; preserve immutable storage and image validation. |
| `private_events_mcp/social_workspace.py` | Permit and schema-advertise document role; accept text MIME syntax; return optional display name; add role/action validation. |
| `private_events_mcp/social_workspace_runtime.py` | Role-aware scopes, staging, verified-asset validation, capability projection, prepare restrictions, digest metadata. |
| `private_events_mcp/social_workspace_tools.py` | Split asset ingress from media_story; role-aware action feature and asset scope selection; dynamic role enum. |
| `private_events_mcp/config.py` | New file-send flag/document byte limit; derived asset-ingress gate; fail-closed validation. |
| `private_events_mcp_workspace_providers.py` | Build/inject the asset store and document-capable Telegram adapter when either relevant feature is enabled. |
| `private_events_mcp_telegram_adapter.py` | Stage document binding; one-attempt `send_file`; filename/caption/entities; read-after-write. |
| `.env.example` | Document new flags and conservative defaults. |
| `tests/test_private_events_mcp_social_asset_ingress.py` | Positive/negative document ingress and immutable-store checks. |
| `tests/test_private_events_mcp_social_workspace_contract.py` | Schema, role, MIME, action/cardinality constraints. |
| `tests/test_private_events_mcp_social_workspace_runtime.py` | Scope, capability, digest, expiry, binding, feature-flag tests. |
| `tests/test_private_events_mcp_telegram_workspace.py` | Exact `send_file` call, caption, filename, timeout, Saved Messages. |
| `tests/test_private_events_mcp_server.py` | Tool discovery, scopes, startup fail-closed, flag-off compatibility. |
| `scripts/smoke_private_events_mcp_media.py` | Add a document mode or a separate bounded document smoke. |
| `docs/operations/private-events-mcp.md` | Update image-only claims, security boundary, operations and live acceptance. |
| `CHANGELOG.md` | `[Unreleased] / Added`: guarded Telegram document send. |

Do not modify unrelated bot media pipelines.

## 7. Compatibility and migration

- Existing deployments with the new flag absent behave exactly as before.
- Existing image/story schemas, fixtures, digests, and live behavior must remain unchanged.
- No database migration should be required if asset metadata is stored as an extensible encrypted/JSON payload. If a rigid SQLite schema exists, add a forward-only nullable migration and test old rows.
- Existing asset refs remain valid only for their original role/provider/principal.
- The media root remains shared, but quotas must account for documents independently so one large upload cannot evict all story images.

## 8. Testing strategy

Run targeted tests first, then the repository’s required MCP suite. Minimum targeted command:

```bash
pytest -q \
  tests/test_private_events_mcp_social_asset_ingress.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_server.py
```

Required test classes are listed in `acceptance-matrix.md`.

A live acceptance must use a harmless generated fixture, not a real production APK in CI:

1. create a small deterministic ZIP with APK markers and `.apk` filename;
2. stage through real ChatGPT `fileParams`;
3. resolve Saved Messages;
4. prepare and commit one `send_message` with a caption;
5. verify read-after-write and manually confirm the document is downloadable;
6. delete the test message if the existing typed delete flow permits it;
7. save only sanitized receipts, no signed URLs or provider IDs.

## 9. Rollout

1. Merge code with `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=false`.
2. Confirm all existing image/story regression tests.
3. Enable only in an isolated operator/staging configuration.
4. Run the live Saved Messages fixture acceptance.
5. Review receipts and storage cleanup/TTL behavior.
6. Enable for the production ChatGPT principal only after explicit owner approval.

Rollback is immediate: disable the file-send flag. Existing text and image/story functionality must remain available; staged document assets expire and are cleaned up normally.

## 10. Completion definition

The feature is complete only when:

- a ChatGPT-provided APK fixture reaches Saved Messages as an actual Telegram document, not a link;
- the filename and optional caption are correct;
- the exact action makes one provider attempt and returns verified opaque evidence;
- flag-off behavior is byte-for-byte/schema-compatible where expected;
- all negative controls fail closed without leaking URL, path, file ID, native Telegram ID, access hash, or unsanitized filename;
- documentation and `CHANGELOG.md` are synchronized;
- a Draft PR contains test evidence and the feature remains disabled by default.
