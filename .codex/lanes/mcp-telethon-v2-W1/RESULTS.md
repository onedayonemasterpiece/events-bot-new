# MCP Telethon V2 W1 Results — Telegram exact reads and audio enrichment

## Lane contract

- **Lane ID:** `mcp-telethon-v2-W1`
- **Requirement IDs:** `R4`, `R5`
- **Status:** Done; committed, not pushed or deployed.
- **Effort / risk:** High — schema, principal binding, provider-byte ingress,
  durable idempotency, and incident-regression surfaces were changed together.
- **Branch:** `integration/mcp-telethon-v2`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/mcp-telethon-v2`
- **Base SHA:** `821e816b2c8317b1cc5e4b85c5ece72aa27a5c44`
- **Foundational media-fix commits:** `f5341152f`, `41cda73c6`
  (reconciled cherry-picks of the two supplied hotfix commits).
- **Implementation head SHA:** `6321cc5a3`
- **Writable scope used:** existing private MCP social/Telegram/audio modules,
  focused repository tests, canonical MCP/incident docs, `CHANGELOG.md`, and
  this result record.
- **Forbidden scope respected:** the dirty main checkout was not touched; no
  generic policy package or `mcp_telethon_v2` package was added; no push, PR,
  merge, deploy, connector replacement, auth-bundle substitution, or live
  provider mutation was performed.

## Requirement results

| ID | Status | Result |
|---|---|---|
| R4 | Done | `social_item_resolve` accepts exact canonical public and private Telegram message links with access-mode binding and sanitized unavailable/malformed failures. VK exact-link behavior remains covered. Telegram item/feed/search/thread projections preserve albums and principal-bound `media[]` refs while adding closed, optional attachment metadata classified from Telethon media/document structures, attributes, and MIME. |
| R5 | Done | High-level Telegram reads accept optional `transcribe_audio` (default true). The existing audio service/store/job/backend pipeline receives trusted provider bytes without fabricated URLs, `fileParams`, native IDs, or paths. Owner/fingerprint/content binding, durable cache-first job lookup, repeat-read deduplication, ready/pending projection, later ready-text projection, per-audio isolation, opt-out, TTL/ownership checks, and exact `untrusted_external_data` marking are implemented. Standalone audio tool order/scopes and separate workspace/transcription auth bundles are preserved. |

## Changed files

- `audio_transcription/asset_store.py`
- `audio_transcription/job_store.py`
- `audio_transcription/service.py`
- `private_events_mcp/integration.py`
- `private_events_mcp/social_workspace.py`
- `private_events_mcp/social_workspace_runtime.py`
- `private_events_mcp/social_workspace_tools.py`
- `private_events_mcp_telegram_adapter.py`
- `private_events_mcp_workspace_providers.py`
- `tests/test_audio_transcription_asset_store.py`
- `tests/test_audio_transcription_job_store.py`
- `tests/test_private_events_mcp_social_workspace_contract.py`
- `tests/test_private_events_mcp_social_workspace_runtime.py`
- `tests/test_private_events_mcp_telegram_workspace.py`
- `docs/operations/private-events-mcp.md`
- `docs/reports/incidents/INC-2026-08-15-audio-mcp-runtime-catalog-truncation.md`
- `docs/reports/incidents/INC-2026-08-23-private-mcp-chatgpt-refresh-reconnect-loop.md`
- `docs/reports/incidents/INC-2026-08-24-mcp-telegram-album-media-ref.md`
- `CHANGELOG.md`
- `.codex/lanes/W1/RESULTS.md`

## Commands and evidence

```text
git fetch origin --prune
git status --short --branch
git cherry-pick 6f95dec39
git cherry-pick f7e7426b1
```

- Worktree was clean at the requested base before edits.
- The supplied ZIP passed archive integrity, manifest size/SHA verification,
  compile checks, and its self-contained suite (`44 passed`). It was used only
  as reference material and was deleted after processing as requested.

```text
/home/dev/.cache/venvs/events-bot-mcp/bin/python -m pytest -q \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_workspace_providers.py \
  tests/test_private_events_mcp_social_workspace_contract.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_audio_transcription_asset_store.py \
  tests/test_audio_transcription_job_store.py \
  tests/test_audio_transcription_mcp.py
```

- Focused implementation pass: `161 passed` before the final service-concurrency
  regression was added.

```text
/home/dev/.cache/venvs/events-bot-mcp/bin/python -m pytest -q <VK/server/OAuth/refresh/remote-session/Telegram-native focused inventory>
```

- Compatibility pass: `81 passed`.

```text
PATH=/home/dev/.local/ffmpeg-static:$PATH \
  /home/dev/.cache/venvs/events-bot-mcp/bin/python -m pytest -q \
  tests/test_private_events_mcp_*.py tests/test_audio_transcription_*.py
```

- Initial broad private-MCP/audio result: **`488 passed, 3 warnings`**.
- The warnings are existing aiohttp `NotAppKeyWarning` diagnostics in
  `main_part2.py`.
- The first broad run had one environment-only failure because `ffmpeg` and
  `ffprobe` were absent. After two install probes failed, the external-tool
  research gate was followed: the official FFmpeg download guidance was
  checked and a static local binary was installed outside the repository. The
  same inventory then passed completely.

```text
PATH=/home/dev/.local/ffmpeg-static:$PATH \
  /home/dev/.cache/venvs/events-bot-mcp/bin/python -m pytest -q \
  tests/test_audio_transcription_job_store.py \
  tests/test_audio_transcription_asset_store.py \
  tests/test_private_events_mcp_social_workspace_runtime.py \
  tests/test_private_events_mcp_telegram_workspace.py \
  tests/test_private_events_mcp_social_workspace_contract.py
```

- Final focused result: `146 passed`.

```text
/home/dev/.cache/venvs/events-bot-mcp/bin/python -m compileall -q \
  audio_transcription private_events_mcp \
  private_events_mcp_telegram_adapter.py \
  private_events_mcp_workspace_providers.py <changed tests>
git diff --check
git diff --cached --check
```

- Compile and whitespace checks passed.
- The final diff contains none of the user's private link, transcript, chat
  title, or concrete native identifiers.

## Risks and release notes

- No live ChatGPT/Telegram canary, service restart, reconnect, deploy, or
  production audit inspection was authorized in this lane. Those incident
  closure gates remain with the integrator/release owner.
- Read-triggered transcription is active only when the existing audio service
  is enabled; no new top-level tool or independent worker/configuration surface
  was introduced.
- Preserve both foundational cherry-picks during integration: they are the
  album-collapse/expansion and outer-ref regression base for this work.
- Preserve the current endpoint/OAuth/client/resource/signing identity and the
  separate workspace and transcription Telegram auth bundles during release.

## Integrator follow-up validation

- Restored the pre-existing `.codex/lanes/W1/RESULTS.md` and moved this task's evidence to the unique `mcp-telethon-v2-W1` lane so unrelated static-site evidence is not overwritten.
- Sanitized the incident record and synthetic album fixture IDs away from the reported private target/item identity.
- Added closed editorial-media stripping, bounded whole-read/per-attachment timeout behavior, instant-backend first-read readiness, album-safe global-search/comments scans, and full real-catalog descriptor/size regression coverage.
- Final compile plus broad private-MCP/audio/remote-session inventory: **`499 passed, 3 existing aiohttp warnings`**.
- Supplied archive evidence was retained only under ignored `artifacts/codex/mcp-telethon-v2/`; its isolated extraction and the requested ZIP were deleted.
