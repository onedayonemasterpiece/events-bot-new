# Lane results: document-docs-smoke

## Identity

- Lane ID: `document-docs-smoke`
- Branch: `agent/private-mcp-document-send/docs-smoke`
- Base SHA: `ff3eb663db5d57e616834802d9b2ec7bf21a1963`
- Implementation head SHA: `8bce00d54ec234f7e4acd7b9d5bc2d26425a0955`
- Status: complete; offline/merge-ready only, with production activation and real ChatGPT UI acceptance explicitly pending

## Delivered

- Added the source-default-off `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=0` example and `PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES=50331648` default; documented the 67,108,864-byte hard cap and derived asset-ingress gate.
- Made `docs/operations/private-events-mcp.md` canonical for the closed Telegram `send_message`/one-document contract, supported APK/PDF/ZIP/UTF-8/Office types, immutable stage/prepare/commit/read-back path, redaction boundary, exact-main rollout, scoped configuration, actual ChatGPT APK acceptance, negative probe and file-send off/on rollback.
- Preserved the existing `eventsBot` connector identity and required refresh-in-place plus a new chat; no new connection/client/path/signing identity is prescribed.
- Extended the E2E index and open `INC-2026-08-09-private-mcp-chatgpt-fileparams-ingest` regression contract without claiming deployment or live closure.
- Marked the implementation handoff archived/merge-ready while retaining its source materials. Removal of its active entry from `docs/backlog/README.md` is integrator-owned because that file was outside this lane.
- Extended the existing sanitized smoke with nonmutating document descriptor/target-capability checks and explicit-gated prepare/commit modes for an already ChatGPT-staged opaque asset. The script states and reports that it cannot replace actual ChatGPT `fileParams` or Telegram UI/download evidence.
- Updated `[Unreleased]` changelog.

## Validation evidence

Commands run from the lane worktree:

```text
uv run --with-requirements requirements.txt --with ruff python scripts/smoke_private_events_mcp_media.py --help
PASS: help listed document contract, prepare and commit modes.

uv run --with-requirements requirements.txt python scripts/smoke_private_events_mcp_media.py --credentials /nonexistent --platform vk --check-document-contract
PASS negative CLI gate: exited 2 with --check-document-contract requires --platform telegram.

python3 -m py_compile scripts/smoke_private_events_mcp_media.py
PASS

uv run --with-requirements requirements.txt python -m compileall -q private_events_mcp private_events_mcp*.py scripts/smoke_private_events_mcp_media.py main_part2.py
PASS

uv run --with ruff ruff check scripts/smoke_private_events_mcp_media.py
PASS

uv run --with-requirements requirements.txt pytest -q tests/test_private_events_mcp_config.py tests/test_private_events_mcp_social_asset_ingress.py tests/test_private_events_mcp_social_workspace_runtime.py tests/test_private_events_mcp_telegram_workspace.py
PASS: 149 passed in 8.79s

git diff --check
PASS
```

The first direct system-Python help/Ruff attempt found the environment lacked `aiohttp` and the `ruff` module. The repository requirements were then executed in an isolated `uv run` environment; no runtime contract guess or code workaround was made.

## Risks and pending external gates

- No production deploy, Fly secret mutation, connector refresh, ChatGPT upload, Telegram send, cleanup, or rollback toggle was performed in this lane.
- Real acceptance remains mandatory: exact merged-main SHA = Fly/in-container SHA, scoped flag activation, a new ChatGPT chat using the actual upload UI and actual `fileParams` object, tiny APK -> Saved Messages one-attempt/read-back, negative probe, then narrow file-send off/on rollback.
- Local smoke accepts only already-issued opaque refs and owner-only receipts; it deliberately cannot prove ChatGPT conversation ingestion or Telegram downloadability.
- `docs/backlog/README.md` still has the active handoff entry; the parent integrator accepted ownership of its one-line removal.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `docs/backlog/features/private-events-mcp-telegram-document-send/README.md`
- `docs/operations/e2e-scenarios.md`
- `docs/operations/private-events-mcp.md`
- `docs/reports/incidents/INC-2026-08-09-private-mcp-chatgpt-fileparams-ingest.md`
- `scripts/smoke_private_events_mcp_media.py`
- `.codex/lanes/document-docs-smoke/RESULTS.md`
