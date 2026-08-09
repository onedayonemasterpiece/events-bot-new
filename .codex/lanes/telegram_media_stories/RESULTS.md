# Telegram media/stories lane results

## Lane

- Lane ID: `telegram_media_stories`
- Requirements: R01, R02, R03, R04
- Base SHA: `80f7bc6c31125abba67575dc94d0fa2b730db247`
- Implementation SHA: `210edc2baaa242c00487b6ce2ef647364156df62`
- Branch: `agent/social-workspace/telegram_media_stories`

## Outcome

Implemented the closed Telegram provider portion of authenticated media ingestion and stories:

- `stage_asset(asset, *, role)` accepts only server-owned `ing_...` references with a 64-hex OAuth owner binding, `sha256:<hex>` digest, byte length, TTL, dimensions, and a closed MIME allowlist. Caller paths, URLs, provider file IDs, bad roles, stale assets, and assets over 30 MiB fail closed.
- Current release write support is image-only (`image/jpeg`, `image/png`, `image/webp`). Video staging/writes fail before a provider call because the current core `VerifiedAsset` has no measured duration/codec contract. Existing provider-read story video handling remains available.
- Commit reopens the opaque storage reference only through injected `asset_reader.open_verified(storage_ref, owner_binding)`, recomputes exact byte length/SHA-256, uploads through the existing injected Telethon client, and converts to `InputMediaUploadedPhoto`.
- Channel story privacy is constructed internally as `InputPrivacyValueAllowAll`; provider storage does not persist Telethon privacy TL objects. Self/user stories remain fail-closed unless an explicit typed privacy binding exists.
- Story preflight performs `stories.CanSendStory` before byte upload. Commit uses `stories.SendStory`, matches `UpdateStoryID.random_id`, and hydrates/read-verifies via `stories.GetStoriesByID`; post-upload/send timeouts and unmatched IDs become non-retryable `outcome_unknown`.
- Story bindings carry `SocialItemKind.STORY`, so `GET_ITEM` and per-item stats route separately from messages.
- `LIST_STORIES` uses `stories.GetPeerStories` and returns bounded media refs plus `StoryViews` aggregate metrics. `GET_STATISTICS` uses `stories.GetStoriesViews` for exact-item or bounded story-only aggregate stats.
- Viewer identities (`recent_viewers`, response `users`) are ignored. The adapter never constructs `ReadStoriesRequest` or `GetStoryViewsListRequest` and never marks a story read.
- `read_asset` materializes story media with a caller-supplied hard byte cap through the same dedicated Telethon session and `iter_download`, without a story-read call.

## Integration contract / open dependencies

The serial provider-storage/core integrator must:

1. Add/inject the core `SocialWorkspaceAdapter.stage_asset(VerifiedAsset, *, role)` contract and the same secure media-store facade as `asset_reader`; `open_verified` may be async and return verified bytes or a bounded readable stream, never a path.
2. Implement `refs.mint_upload_asset(*, role, upload) -> TelegramAssetBinding` and persist only the immutable `TelegramVerifiedUpload` metadata.
3. Extend `refs.mint_item` with optional `kind`; persist `SocialItemKind.STORY` distinctly from message/post IDs.
4. Extend `refs.mint_read_asset` with optional `story_id`, `expires_at`, and `item_kind`, binding the inner ref to exact target/story/media/expiry. Outer runtime encrypted refs remain the authoritative OAuth owner binding for provider-read assets.
5. Wire `read_asset` only after outer runtime reference authorization. Uploaded assets additionally compare their stored `owner_binding` inside the adapter.
6. Update the legacy closed-public-surface assertion in `tests/test_private_events_mcp_telegram_workspace.py` to include `stage_asset` and `read_asset`.
7. Keep the asset-stage schema/capabilities image-only for this release. Do not advertise video staging/story writes until core supplies measured duration/codec validation.
8. Update canonical docs and `CHANGELOG.md` in their separately owned lane.

## Evidence / commands

- `PYTHONPATH=/tmp/telethon-1.44-inspect-20260809 python3 -m pytest --confcutdir=/tmp -q tests/test_private_events_mcp_telegram_media_stories.py`
  - Result: `7 passed in 0.28s`.
- `PYTHONPATH=/tmp/telethon-1.44-inspect-20260809 python3 -m pytest --confcutdir=/tmp -q tests/test_private_events_mcp_telegram_workspace.py`
  - Result: `44 passed, 1 expected integration failure`; sole failure is the legacy five-method public-surface assertion described above.
- `PYTHONPATH=/tmp/telethon-1.44-inspect-20260809 python3 -m py_compile private_events_mcp_telegram_adapter.py tests/test_private_events_mcp_telegram_media_stories.py`
  - Result: pass.
- `git diff --check`
  - Result: pass.
- Telethon 1.44.0 constructor probe verified:
  - `CanSendStoryRequest`
  - `GetPeerStoriesRequest`
  - `GetStoriesByIDRequest`
  - `GetStoriesViewsRequest`
  - `InputPrivacyValueAllowAll`
  - `InputMediaUploadedPhoto`
  - `InputMediaUploadedDocument` (dormant until verified video ingress exists)
  - matching `UpdateStoryID` extraction.
- Primary references inspected:
  - Telethon 1.44 generated TL pages for `stories.canSendStory`, `stories.sendStory`, `stories.getPeerStories`, `stories.getStoriesByID`, and `stories.getStoriesViews`.
  - Installed Telethon 1.44.0 generated request/type sources and constructor signatures in an isolated `/tmp` target.

## Risks / limits

- No live Telegram mutation/read was run in this provider-only lane; integration/live acceptance remains required.
- `read_asset` returns bytes at the adapter boundary; core must keep its outer encrypted reference authorization and response/materialization budget intact.
- Story pagination is bounded over the active stories returned by Telegram; expired/unavailable stories depend on provider behavior.
- No provider credentials, OAuth identity paths, environment files, docs, core, workspace provider store, or `main_part2.py` were modified.

## Changed files

- `private_events_mcp_telegram_adapter.py`
- `tests/test_private_events_mcp_telegram_media_stories.py`
- `.codex/lanes/telegram_media_stories/RESULTS.md` (results-only follow-up commit)
