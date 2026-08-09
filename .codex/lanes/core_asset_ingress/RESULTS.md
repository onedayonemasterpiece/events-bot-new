# Lane core_asset_ingress Results

## Status
committed

## Requirement IDs
- R01
- R02
- R05

## Branch
`agent/social-workspace/core_asset_ingress`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/core_asset_ingress`

## Base SHA
`80f7bc6c31125abba67575dc94d0fa2b730db247`

## Head SHA
Implementation head: `d89ec10a61aa869aa4dcb6f72c7e5a8ddbce1a3b`

Security follow-up head: `6301f577e4647e7866d0e49a0d42147250039b1c`

The lane-results metadata commit is the direct clean successor to this implementation head.

## Files changed
- `private_events_mcp/config.py`
- `private_events_mcp/media_contract.py` (new)
- `private_events_mcp/server.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp/tool_catalog.py`
- `tests/test_private_events_mcp_social_asset_ingress.py` (new)
- `.codex/lanes/core_asset_ingress/RESULTS.md`

## Delivered
- Added the official single-file ChatGPT descriptor contract:
  `_meta["openai/fileParams"] == ["file"]`, with a top-level `file` object
  declaring all four official snake-case fields and requiring only
  `download_url` and `file_id`.
- Removed caller-authoritative upload handles, digests, MIME and sizes. The
  public stage request is exactly `platform`, `file`, and image-only `role`.
- Added provider-neutral `AssetIngestor`, `ChatGPTFile`, and `VerifiedAsset`
  contracts. Core performs no download or other outbound implementation.
  Secret storage and owner bindings are excluded from dataclass repr output.
- Runtime authorizes the bound OAuth principal and reserves rate/media budgets
  before calling the injected ingestor, validates principal ownership, measured
  digest/MIME/size/dimensions/TTL, then calls the selected adapter's
  `stage_asset(VerifiedAsset, role=...)` and mints the outer `ast_` ref.
- Persisted only encrypted provider refs and encrypted safe verified metadata;
  neither ChatGPT `download_url`, `file_id`, nor `file_name` reaches adapters,
  public results, audits, or durable state.
- Asset status rechecks owner/resource/policy and current TTL and returns only
  verified safe metadata, including an explicit expired lifecycle state.
- Preparation, browser approval preview, approval minting, and commit recompute
  an action digest that includes the verified asset metadata. Asset TTL also
  caps preparation TTL. Metadata tampering fails before provider execution.
- Media activation now requires an injected ingestor, absolute storage policy,
  nonempty exact/wildcard host allowlist and provider `stage_asset` support.
  Codex remains the same seven evidence-only tools and OAuth/scopes are unchanged.
- Added the exact bounded media env contract in config (30 MiB default asset,
  128 MiB store, 3600s TTL, 20s download timeout, 8192 dimensions, 40M pixels).
  Host wildcards permit only a leading `*.` DNS suffix.

## Commands run
```text
python3 -m py_compile \
  private_events_mcp/config.py private_events_mcp/server.py \
  private_events_mcp/media_contract.py private_events_mcp/social_workspace.py \
  private_events_mcp/social_workspace_runtime.py \
  private_events_mcp/social_workspace_tools.py \
  private_events_mcp/tool_catalog.py \
  tests/test_private_events_mcp_social_asset_ingress.py

uv run --with ruff ruff check \
  private_events_mcp/config.py private_events_mcp/server.py \
  private_events_mcp/media_contract.py private_events_mcp/social_workspace.py \
  private_events_mcp/social_workspace_runtime.py \
  private_events_mcp/social_workspace_tools.py \
  private_events_mcp/tool_catalog.py \
  tests/test_private_events_mcp_social_asset_ingress.py

uv run --with-requirements requirements.txt pytest -q \
  tests/test_private_events_mcp_social_asset_ingress.py

uv run --with-requirements requirements.txt pytest -q \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_server.py \
  tests/test_private_events_mcp_config.py \
  tests/test_private_events_mcp_social_oauth_policy.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  -k 'not asset_lifecycle_uses_upload_and_opaque_refs_not_paths_or_urls'

uv run --with-requirements requirements.txt pytest -q \
  tests/test_private_events_mcp_protocol.py \
  tests/test_private_events_mcp_static_safety.py

git diff --check
```

## Tests / verification
- Focused adversarial ingress suite: **19 passed**.
- Related contract/runtime/server/config/OAuth suite: **110 passed, 1 deselected**.
- Protocol/static-safety regression suite: **6 passed**.
- Ruff: **all checks passed**.
- Python compile and `git diff --check`: passed.
- Official contract source checked: OpenAI Plugins reference, "Define file
  inputs" (`https://developers.openai.com/plugins/reference#define-file-inputs`).

Adversarial cases cover cross-principal ownership, wrong owner binding and
secret-free verified-asset repr, malformed
or changed digest/MIME/size/dimensions/pixel count/expiry, optional declared MIME
mismatch, expired status/prepare/preview, post-approval metadata tampering,
no file URL/ID in adapter/output/SQLite, exact descriptor metadata, image-only
advertising, missing ingestor and invalid storage/host policy.

## Risks
- The base test
  `test_asset_lifecycle_uses_upload_and_opaque_refs_not_paths_or_urls` encodes
  the superseded caller-supplied `upload_ref`/digest/size schema. It was
  deliberately deselected; integration must update/remove that assertion.
  Keeping it compatible would violate the exact new file-parameter contract.
- `private_events_mcp/integration.py` and `main_part2.py` are outside this lane's
  writable scope. Integration must pass the production `asset_ingestor` through
  `attach_private_events_mcp`; without it, media/story activation correctly
  fails closed.
- Provider lanes must implement the committed exact adapter signature
  `stage_asset(VerifiedAsset, *, role: MediaRole) -> str` and ingestor signature
  documented in `media_contract.py`.
- MCP binary/image tool results require changes to out-of-scope `protocol.py`.
  Therefore this lane does not add or claim a visual `social_asset_read/get`
  tool; metadata-only `social_asset_status` is not a story-image reader.
- Canonical docs, `.env.example`, legacy contract-test cleanup and `CHANGELOG.md`
  are integration/docs-lane responsibilities and were intentionally not edited.

## Merge notes
Cherry-pick the full branch range after the base SHA. The code commits are
`d89ec10a61aa869aa4dcb6f72c7e5a8ddbce1a3b` and security follow-up
`6301f577e4647e7866d0e49a0d42147250039b1c`; the intervening/final commits are
lane-results evidence only. The implementation is based
exactly on the requested `origin/main` SHA and does not touch provider modules,
workspace-provider wiring, docs, `CHANGELOG.md`, or `main_part2.py`.
