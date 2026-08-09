# Lane media_story_docs_ci Results

## Status

committed

## Requirement IDs

- R05
- R06

## Branch

`agent/social-workspace/media_story_docs_ci`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/media_story_docs_ci`

## Base SHA

`80f7bc6c31125abba67575dc94d0fa2b730db247`

## Head SHA

Tested implementation commit: `3287ac4047af48278bec18e128e62c775002211f`.
The lane-final metadata commit after this one adds only this `RESULTS.md`; use
the branch HEAD reported in the handoff for that SHA.

## Files changed

- `.env.example`
- `.github/workflows/ci.yaml`
- `CHANGELOG.md`
- `docs/operations/e2e-scenarios.md`
- `docs/operations/private-events-mcp.md`
- `scripts/smoke_private_events_mcp_media.py`
- `.codex/lanes/media_story_docs_ci/RESULTS.md`

## Commands run

```text
git fetch origin --prune
git worktree add -b agent/social-workspace/media_story_docs_ci ... 80f7bc6c31125abba67575dc94d0fa2b730db247
PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_*.py
PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q private_events_mcp private_events_mcp*.py tests scripts main_part2.py
python3 -m py_compile scripts/smoke_private_events_mcp_media.py
git diff --check
/home/dev/.codex/venvs/events-bot-new/bin/python scripts/smoke_private_events_mcp_media.py --help
PyYAML parse plus owner-only smoke receipt contract probe
negative CLI probe proving prepare is rejected without --allow-write
```

The system `python` alias was absent during one documentation-edit helper
attempt; it made no change. The command was rerun successfully with `python3`.

## Tests / verification

- Private Events MCP suite: **257 passed**, 3 existing `aiohttp`
  `NotAppKeyWarning` warnings.
- MCP package, every current top-level `private_events_mcp*.py` adapter/provider,
  tests, scripts and `main_part2.py`: compileall passed.
- `git diff --check`: passed.
- GitHub Actions YAML parse: passed.
- New smoke `--help`, private receipt mode `0600`, and fail-closed write-flag
  validation: passed.
- No live OAuth/provider mutation was run in this documentation/CI lane.

## Interface placeholders / risks

- The release contract is intentionally **image-only**. Video is explicitly
  denied; it is not activated by the media/story flag and requires a separate
  future change/review/gate.
- Provider story media refs are metadata-only. `social_asset_preview` and
  `social_asset_read` are named only as absent interface placeholders; no visual
  inspection/download claim is made until an integrated implementation proves
  authorization, bytes and redaction.
- `PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS` is deliberately blank in the example.
  Activation must discover and configure exact current ChatGPT file hosts (or
  explicit `*.suffix` entries); there is no implicit global allowlist.
- The integration owner must confirm the merged config consumes the documented
  root/host/size/quota/TTL/timeout/dimension env names and that
  `social_asset_stage` exposes exactly `{platform,file,role}` with
  `_meta["openai/fileParams"]=["file"]`.
- Live acceptance remains an integration/release responsibility: refresh the
  existing ChatGPT connection, start a new chat, preserve endpoint/client/
  resource/signing identity, verify Codex remains exact-seven/no-social, and run
  separately approved Telegram/VK image-story canaries.

## Merge notes

- Cherry-pick the implementation commit and this results-only commit.
- The CI change preserves every existing job and the exact private-MCP pytest
  glob; it adds `git diff --check` and uses `private_events_mcp*.py` so the merged
  top-level media store is compiled without replacing repository-wide checks.
- Do not activate media/story from this lane alone. Merge the core media store,
  core ingress, provider adapters and provider-storage wiring first, then run the
  documented exact-main acceptance and independent review.
