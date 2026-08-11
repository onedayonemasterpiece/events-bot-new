# Lane results: document-policy-store

## Scope

- Lane ID: `document-policy-store`
- Base SHA: `dcd51b7a1dc50eacdaffe5401a808d2c4285eec0`
- Validated implementation head SHA: `7e8400f85e3fa013bf7b2db0f6fa18a109f0b932`
- Branch: `agent/private-mcp-document-send/document-policy-store`

## Delivered

- Independent fail-closed document classifier/validator for APK, PDF, ZIP,
  complete bounded UTF-8 TXT/MD/CSV/JSON, and structurally distinguished
  DOCX/XLSX/PPTX.
- ZIP central/local-header inventory without filesystem extraction, with
  traversal, absolute/drive path, encryption, unsafe compression, entry-count,
  declared-size, expansion-ratio, duplicate, Unicode/casefold collision,
  inconsistent-header, overlap, and malformed-container rejection.
- APK Android manifest plus non-empty payload checks and exact OOXML base/main
  part checks.
- NFKC/control/bidi/path-safe display filename normalization with detected
  extension enforcement and a 180 UTF-8-byte ceiling.
- Role-aware `VerifiedAsset` metadata (`role`, `display_name`,
  `classification`) and exact `AssetIngestor.reverify(...)` contract.
- Backward-compatible immutable store manifest extension, document-specific
  byte cap, structural revalidation on reopen, principal/role/size/digest
  checks, and no persistence/return of URL, raw file id, original unsafe name,
  or internal path. The existing Pillow image validator remains independent.
- Adversarial and mutation-focused policy/store tests, including late invalid
  UTF-8, malformed/truncated structures, archive attacks, filename attacks,
  manifest privacy, role/size mismatch, and post-ingress byte mutation.

## Evidence and commands

1. `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/pytest -q tests/test_private_events_mcp_document_policy.py tests/test_private_events_mcp_media_store.py tests/test_private_events_mcp_social_asset_ingress.py`
   - PASS: `110 passed in 1.53s`.
2. `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/pytest -q tests/test_private_events_mcp_*.py`
   - PASS: `425 passed, 3 warnings in 20.22s`.
   - Warnings are pre-existing aiohttp `NotAppKeyWarning` instances in
     `main_part2.py` from the disabled provider-adapter test.
3. `python3 -m compileall -q private_events_mcp private_events_mcp_media.py tests/test_private_events_mcp_document_policy.py tests/test_private_events_mcp_social_asset_ingress.py`
   - PASS.
4. `/home/dev/.cache/uv/archive-v0/klptK945vMs2Ma60/bin/ruff check private_events_mcp/document_policy.py private_events_mcp/media_contract.py private_events_mcp_media.py tests/test_private_events_mcp_document_policy.py tests/test_private_events_mcp_social_asset_ingress.py`
   - PASS: `All checks passed!`.
5. `git diff --check` and `git diff --cached --check`
   - PASS.

## Changed files

- `private_events_mcp/document_policy.py`
- `private_events_mcp/media_contract.py`
- `private_events_mcp_media.py`
- `tests/test_private_events_mcp_document_policy.py`
- `tests/test_private_events_mcp_social_asset_ingress.py`
- `.codex/lanes/document-policy-store/RESULTS.md`

## Integration notes

- Core should call `ingest(..., role=request.role.value)`; legacy omitted role
  continues to mean the existing image/story path.
- Constructor wiring for document-enabled deployments is
  `SecureMediaAssetStore(..., max_document_bytes=<configured limit>)`.
- Exact synchronous recheck API is
  `reverify(storage_ref, *, owner_binding, max_bytes, role) -> VerifiedAsset`;
  async runtime callers should invoke it through their normal bounded thread
  boundary if needed.
- `role` in returned store assets is canonical `image` or `document`; image
  role aliases are normalized only at ingress.
- Existing SQLite manifests migrate in place with default `role='image'` and
  nullable display/classification fields. New images receive an image
  classification; legacy image rows remain accepted.

## Risks / explicit non-claims

- ZIP validation is deliberately inventory-only and does not inflate entries;
  it therefore does not claim payload CRC validation, malware detection, APK
  signing/authenticity, or Office active-content safety.
- PDF validation proves a bounded PDF header/EOF envelope, not semantic parser
  correctness or content safety.
- Transport/action authorization and Telegram delivery are owned by other
  lanes; this lane supplies the closed byte/store contract only.
