# core-document-runtime results

## Scope

- Lane: `core-document-runtime`
- Requirements: R02, R05, R06, R10, R11
- Base SHA: `dcd51b7a1dc50eacdaffe5401a808d2c4285eec0`
- Implementation head SHA: `bd6b7ec0c725a03d53c02989682c2b3e9b384ee5`

## Outcome

Implemented the default-off Telegram document runtime and wiring:

- independent `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED` switch;
- 48 MiB document default with a strict 64 MiB configuration ceiling;
- derived `asset_ingress_enabled` and inert disabled/stale settings;
- Telegram-only `role=document` staging and `send_message` cardinality/provider/action validation;
- role-aware scopes, tool exposure, schemas, and capability projection;
- immutable document metadata binding (role, SHA-256, size, detected MIME, safe display name, classification, expiry) without public storage/provider locators;
- store `reverify(...)` calls at prepare and commit, plus principal/provider/role/TTL/digest checks;
- current target-capability and OAuth-scope reauthorization before prepare/commit;
- stale preparation denial after the document kill switch is disabled;
- file-only store construction and Telegram-only injection (VK receives no file-only store);
- explicit startup marker contract `document_send_supported is True` so the old image-only Telegram adapter cannot accidentally enable document execution;
- ChatGPT-only document catalogue; Codex remains read-only.

## Integration notes

This lane consumes the frozen policy contract:

```text
AssetIngestor.ingest(..., role=...)
AssetIngestor.reverify(storage_ref, *, owner_binding, max_bytes, role) -> VerifiedAsset
VerifiedAsset: role, content_digest, mime_type, byte_length, expires_at,
               dimensions, display_name, classification
SecureMediaAssetStore(..., max_document_bytes=...)
```

For backward compatibility, legacy image ingestors continue receiving the existing `story_media` role and old image manifests/digests remain accepted. Documents fail closed without the new fields or synchronous `reverify` method. The Telegram adapter integration must expose `document_send_supported = True` (coordinated with the Telegram lane).

## Validation evidence

Commands run from the lane worktree:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q \
  private_events_mcp private_events_mcp_workspace_providers.py main_part2.py \
  tests/test_private_events_mcp_config.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_server.py \
  tests/test_private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_static_safety.py
# PASS

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_social_asset_ingress.py \
  tests/test_private_events_mcp_config.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_server.py \
  tests/test_private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_static_safety.py
# 183 passed in 5.84s

uvx ruff check --output-format concise \
  private_events_mcp/config.py private_events_mcp/social_workspace.py \
  private_events_mcp/social_workspace_runtime.py \
  private_events_mcp/social_workspace_tools.py private_events_mcp/server.py \
  private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_config.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_server.py \
  tests/test_private_events_mcp_workspace_providers.py
# All checks passed!

git diff --check
# PASS
```

A repository-wide Ruff invocation including `main_part2.py` remains non-green because that pre-existing split module has more than 1,000 undefined-name/import/style findings unrelated to this three-line wiring change. Strict compileall and the targeted MCP tests above pass.

## Security/adversarial coverage

Tests cover default-off/inert config, hard limits and parent gates, Telegram-only stage schema, document provider/action/cardinality and mixed-media denial, dynamic document catalogue, Telegram/VK/Codex capability isolation, principal/provider/role metadata binding through existing runtime tests, TTL/digest binding, mutation before commit, kill-switch disable after prepare, safe status/approval preview fields, and no internal storage/provider locator disclosure.

## Changed files

- `main_part2.py`
- `private_events_mcp/config.py`
- `private_events_mcp/server.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp_workspace_providers.py`
- `tests/test_private_events_mcp_config.py`
- `tests/test_private_events_mcp_server.py`
- `tests/test_private_events_mcp_social_workspace_contract.py`
- `tests/test_private_events_mcp_social_workspace_runtime.py`
- `tests/test_private_events_mcp_workspace_providers.py`
- `.codex/lanes/core-document-runtime/RESULTS.md`

## Risks

- Final integration must run the combined policy + Telegram adapter suites because this isolated lane intentionally did not edit those dependency-owned files.
- The full `main_part2.py` Ruff surface has substantial unrelated baseline debt; no attempt was made to broaden scope and rewrite it.

## B-09 follow-up

The request parser now preserves bounded, untrusted document filename hints—including path separators and bidi/control characters—until the document-policy sanitizer. Image filename validation remains unchanged and continues to reject separators/NUL. An actual document stage test passes `../unsafe\u202e.apk` into the fake policy boundary, receives only `safe.apk`, and verifies the raw sentinel is absent from asset status, approval preview, and durable runtime database bytes.

Focused verification:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_social_workspace_contract.py::test_document_stage_is_telegram_only_and_accepts_text_mime_hint \
  tests/test_private_events_mcp_social_workspace_runtime.py::test_document_runtime_reverifies_digest_and_kill_switch \
  tests/test_private_events_mcp_social_asset_ingress.py::test_official_file_param_descriptor_and_schema_are_exact
# 3 passed in 0.57s

uvx ruff check private_events_mcp/social_workspace.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py
# All checks passed!
```

## Async reverify follow-up

Document prepare/commit reverification now runs the synchronous immutable-store `reverify` operation in `asyncio.to_thread`, bounded by `max(provider_timeout_seconds, asset_ingest_timeout_seconds)`. Timeout fails closed before preparation/operation consumption and before provider transport. The slow fake regression proves the loop remains responsive while a 48 MiB-class local verification is in progress, verifies timeout denial, and retains independent prepare-time and commit-time checks.

Focused verification:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_private_events_mcp_social_workspace_runtime.py::test_document_runtime_reverifies_digest_and_kill_switch
# 1 passed in 0.54s

uvx ruff check private_events_mcp/social_workspace_runtime.py \
  tests/test_private_events_mcp_social_workspace_runtime.py
# All checks passed!

python3 -m compileall -q private_events_mcp/social_workspace_runtime.py \
  tests/test_private_events_mcp_social_workspace_runtime.py
git diff --check
# PASS
```
