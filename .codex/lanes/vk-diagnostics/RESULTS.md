# vk-diagnostics lane results

## Scope

- Lane: `vk-diagnostics`
- Requirements: R7 and VK portions of R4/R5; adapter retry seam needed by R6 runtime.
- Base SHA: `64f75d10f7aff33fa616cee212878bd9d03673b1`
- Implementation head SHA: `bd6945be154f83909a8b0afc7fe339eba12e7b11`
- Branch: `agent/eventsbot-scheduled-readback/vk-diagnostics`
- No provider/social mutation or browser automation was performed.

## Outcome

- **R7 — Done.** Multipart receipt transport now consumes fragmented responses to EOF, performs explicit bounded gzip/deflate decoding, supports flat and nested VK JSON shapes, and returns only structural diagnostics. Adapter attempt evidence includes HTTP status, safe content type/encoding, compressed/decoded sizes, EOF, allowlisted key names/counts, field types/bounded lengths, image ordinal, digest prefix, stage phase, and wall-mutation-boundary state. The safe evidence is nested in encrypted `provider_result` so the existing durable recorder persists it. Bodies, field values, upload URLs, full hashes/digests, tokens, and cookies are excluded.
- **R7 failure classification — Done.** Empty, wrong-type, or over-bound `server`/`photo`/`hash` receipts become exact `media_upload_response_invalid`, `stage=wall_photo_multipart`, `failed`, `retry_safe=true`; `photos.saveWallPhoto` and `wall.post` are not called. Existing uncertainty after `wall.post` remains non-retryable `outcome_unknown`.
- **VK four-image chain — Done.** Four verified PNGs are materialized/uploaded/saved sequentially in source order, then exactly one scheduled `wall.post` is issued and verified through `filter=postponed`.
- **R4 VK scheduled read — Done.** `scheduled_items(...)` reads `wall.get` as the community editor with `filter=postponed`, binds the exact owner, applies bounded time/text-hash/media-count filters, and returns opaque logical scheduled items with ordered media roles.
- **R5 VK scheduled delete — Done.** Scheduled item bindings carry `queue=postponed`; deletion targets the exact owner/post through `wall.delete`, then uses the delete-scoped postponed read policy to prove that exact binding absent before returning verified success.
- **Retry seam — Done in adapter.** `retry(intent, operation_ref=same_ref, attempt_number=n)` supports restart-empty delegate memory behind the runtime's durable CAS, preserves the logical operation/idempotency binding, serializes attempts, and rejects retry after any non-safe terminal result. Runtime/tool persistence and public CAS are owned by the integration lane.

## Root-cause evidence and hypotheses

- Confirmed code condition: prior adapter validation occurred only after multipart normalization and discarded structural response evidence, so an empty `photo`/`hash` was reduced to `media_upload_response_invalid` without enough safe metadata to distinguish shape/type/decoding conditions.
- Confirmed regression risk: the prior session delegated decompression to aiohttp, which made wire-size evidence unavailable; the new explicit decoder independently bounds compressed and decoded bytes.
- This lane did not query historical production operation payloads. The exact historical upstream response condition therefore remains to be established from newly persisted safe evidence; no unsupported attribution was made.

## Validation

Commands run with `/home/dev/.venvs/events-bot-region-talk/bin/python`:

- `python -m pytest -q tests/test_private_events_mcp_vk_workspace.py tests/test_private_events_mcp_vk_media_stories.py` — **60 passed**.
- `python -m pytest -q tests/test_private_events_mcp_workspace_providers.py tests/test_private_events_mcp_vk_workspace.py tests/test_private_events_mcp_vk_media_stories.py` — **79 passed**.
- `python -m compileall -q private_events_mcp_vk_adapter.py private_events_mcp_vk_transport.py private_events_mcp_vk_upload.py` — passed.
- `git diff --check` — passed.
- `python -m ruff ...` — not run: the required venv has no `ruff` module and no standalone `ruff` binary was installed.

Coverage added for flat fragmented JSON, nested actual-gzip fragmented JSON, bounded diagnostics, invalid/empty receipt fields with no later mutation, four ordered PNG stages plus one postponed wall post, scheduled queue filtering/projection, exact postponed deletion/absence, and same-operation safe retry.

## Risks / integration notes

- The runtime/provider wrapper must proxy `scheduled_items` and `retry` and the public action schema must admit `stage`/`attempt_number`; the tools/runtime lane confirmed it uses these exact signatures.
- Full Private Events MCP suite is owned by the integrator. This lane completed the affected VK/provider-store suite above.

## Changed files

- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_vk_transport.py`
- `private_events_mcp_vk_upload.py`
- `tests/test_private_events_mcp_vk_workspace.py`
- `tests/test_private_events_mcp_vk_media_stories.py`
- `.codex/lanes/vk-diagnostics/RESULTS.md`
