# Lane vk_media_stories Results

## Status
committed

## Requirement IDs
- R01
- R02
- R03
- R04

## Branch
`agent/social-workspace/vk_media_stories`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/vk_media_stories`

## Base SHA
`80f7bc6c31125abba67575dc94d0fa2b730db247`

## Head SHA
Implementation commit: `c57a3d88d` (`feat(mcp): add safe VK media and stories adapter`).
The lane metadata commit containing this file follows that implementation commit.

## Files changed
- `private_events_mcp_vk_adapter.py`
- `private_events_mcp_vk_upload.py`
- `tests/test_private_events_mcp_vk_media_stories.py`
- `.codex/lanes/vk_media_stories/RESULTS.md`

## Implementation evidence
- `stage_asset(asset: VerifiedAsset, *, role: MediaRole) -> str` stores only a private descriptor and returns an inner `ast_...` provider ref. It accepts exact `ing_...` storage refs, exact lowercase SHA-256 owner bindings, `sha256:<hex>` digests, bounded measured metadata, future expiry, and image roles only.
- Provider bytes are not opened during staging. `execute` calls the injected `asset_reader.open_verified(storage_ref, owner_binding)` after the core approval boundary, then rechecks owner/storage/digest/MIME/length and recomputes SHA-256 before any provider call.
- Wall image publication uses fixed `photos.getWallUploadServer` -> narrow multipart -> `photos.saveWallPhoto` -> `wall.post`; no arbitrary VK method or caller URL is accepted.
- Story image publication uses fixed `stories.getPhotoUploadServer` -> narrow multipart -> `stories.save(upload_results)` with no `group_id` -> `stories.getById` readback. `stories.getVideoUploadServer` remains a fixed dormant policy; new video staging and advertised video writes are denied for the image-only release.
- Story deletion uses fixed `stories.delete(owner_id, story_id)`.
- `stories.get(owner_id)` parses only official v5.199 `items[].stories`; caption-only stories are omitted. Photo/video visuals become opaque asset refs, with an optional closed `read_asset`/VK-CDN materialization hook. Provider URLs never appear in public responses.
- VK CDN/upload URLs require canonical lowercase HTTPS VK-owned hosts, port 443/default, no credentials, fragments, controls, whitespace, or backslashes; signed queries are preserved.
- Story stats parse only nested `{count,state}` entries. Community `stats.get` uses `timestamp_from`, `timestamp_to`, `interval`, and official nested `visitors`/`activity` periods. Only aggregate counts are returned; viewer/profile/demographic identities are discarded.
- Once a provider call or multipart operation starts, later failures produce durable `outcome_unknown` with `retry_safe=false`. Pre-provider policy denial and byte-integrity failure remain ordinary failed outcomes. Exact idempotent replay does not repeat materialization or VK calls.
- Official source checked: `VKCOM/vk-api-schema` v5.199 (`stories/methods.json`, `stories/responses.json`, `stories/objects.json`, `photos/methods.json`, `photos/responses.json`, `stats/methods.json`, `stats/objects.json`).

## Commands run
- `python3` scripts against `https://raw.githubusercontent.com/VKCOM/vk-api-schema/master/...` to inspect official v5.199 method/response/object schemas.
- `python3 -m venv /tmp/vk-media-stories-venv`
- `/tmp/vk-media-stories-venv/bin/pip install -q -r requirements.txt`
- `/tmp/vk-media-stories-venv/bin/pytest -q tests/test_private_events_mcp_vk_media_stories.py`
- `/tmp/vk-media-stories-venv/bin/pytest -q tests/test_private_events_mcp_static_safety.py tests/test_private_events_mcp_social_workspace_contract.py`
- `/tmp/vk-media-stories-venv/bin/pytest -q tests/test_private_events_mcp_vk_media_stories.py tests/test_private_events_mcp_static_safety.py tests/test_private_events_mcp_social_workspace_contract.py`
- `/tmp/vk-media-stories-venv/bin/pytest -q tests/test_private_events_mcp_vk_workspace.py`
- `python3 -m py_compile private_events_mcp_vk_adapter.py private_events_mcp_vk_upload.py tests/test_private_events_mcp_vk_media_stories.py`
- `git diff --check`

## Tests / verification
- New VK media/story suite: **12 passed**.
- New suite plus private MCP static-safety and workspace-contract suites: **47 passed**.
- Existing VK workspace suite: **18 passed, 1 failed** because its forbidden, pre-v5.199 fake returns flat `stories.get.items[]` without nested `stories` or visual media. Once that fake is updated, the same test must also replace flat `stats.get` rows with official nested `visitors`/`activity` periods. No legacy production parser was retained merely to satisfy the stale fake.
- Python compilation and `git diff --check`: passed.

## Risks / open integration dependencies
- Core must merge `private_events_mcp.media_contract.VerifiedAsset` before this lane. The adapter has a narrow import fallback only so this exact-base lane can run independently; the merged core module becomes the runtime type source.
- Core/runtime must persist the returned inner provider ref as the outer asset provider binding and resolve it back unchanged during approved commit.
- Provider integration must inject implementations of `VKVerifiedAssetReader`, `VKMultipartTransport`, and optionally `VKStoryMediaReader`. The multipart implementation must not follow redirects outside the adapter-validated VK host.
- `photos.getWallUploadServer` / `photos.saveWallPhoto` are official user-token methods in v5.199. The `community_editor` media-upload actor must therefore be mapped to a dedicated appropriately scoped user-token credential, never a generic/community-token fallback.
- Video writes are intentionally not advertised or stageable in this image-only release. The fixed video upload policy and read-only video visual extraction are present for a later verified-ingress release.
- The integrator/test owner must update the pre-existing `tests/test_private_events_mcp_vk_workspace.py` fake to official story/stat shapes; this file was explicitly outside lane ownership.
- Canonical docs and `CHANGELOG.md` are owned by another lane and were forbidden here.

## Merge notes
Cherry-pick implementation commit `c57a3d88d`, then the following results-only commit. Merge the core asset-ingress lane first (or in the same integration sequence), inject the three narrow provider hooks, and update the stale pre-existing VK fake before the full suite gate.
