# R07 video export results

- **Status:** Done
- **Requirement IDs:** R07
- **Branch:** `agent/telegram-video-quality/video-export`
- **Worktree:** `/home/dev/projects/events-bot-new.worktrees/video-export`
- **Base SHA:** `c128bde4fe7a7b289c3ed4a64a4fe56d33124ad9`
- **Implementation head SHA:** `8d6c8ff3`

## Outcome

- Added a backwards-compatible `PreviewEvent.video_assets?: EventVideoAsset[]`
  contract; the exporter emits a zero-or-more list for every current and archived
  event, without adding any Astro component or playback UI.
- Added one bulk, snapshot-tolerant M:N projection from `event_video_link` to the
  global content-addressed `video_asset` ledger. Missing tables, missing required
  columns and missing optional columns fail closed without breaking an older
  static-site snapshot.
- Public projection admits only stable SHA-256 assets with a canonical public CDN
  URL and valid vertical dimensions (`height > width`). When the snapshot has
  `video_asset.analysis_status`, only `accepted` rows are public. The same asset
  can be projected for multiple events with a different event-relative score.
- Assets sort deterministically by persisted link-level `ranking_score`, then
  `showcase_score`, `event_relevance_score`, SHA-256 and asset id. The public
  contract exposes CDN path/type/geometry/duration, quality/relevance/ranking
  scores, source-post attribution URL, description and future retrieval text.
- Added optional-contract validation to `check-preview.mjs`: public URL, SHA-256,
  vertical positive dimensions, bounded finite scores, duration, uniqueness and
  rank order are checked whenever `video_assets` is present. Old JSON fixtures
  without the optional field remain valid.

## Changed files

- `site/scripts/export-production-preview-data.py`
- `site/src/lib/types.ts`
- `site/scripts/check-preview.mjs`
- `tests/test_event_video_static_export.py`
- `.codex/lanes/video-export/RESULTS.md` (lane evidence only)

## Commands and tests

```text
uv run --with pytest python -m pytest --noconftest -q \
  tests/test_event_video_static_export.py \
  tests/test_event_participants_static_export.py \
  tests/test_static_site_content_projection.py
# 20 passed in 0.32s

python3 -m compileall -q \
  site/scripts/export-production-preview-data.py \
  tests/test_event_video_static_export.py
# passed

node --check site/scripts/check-preview.mjs
# passed

git diff --check
# passed
```

Focused video tests cover absent tables, a partial old schema, multiple ranked
assets, one asset linked to two events, invalid/nonvertical rows filtered, the
accepted-status gate and source-URL projection.

## Risks and integration notes

- `ranking_score` is read from the canonical event-video link written by the
  persistence lane (`0.75 * showcase + 0.25 * relevance`); the exporter does not
  silently recompute or override persisted ranking evidence.
- URLs outside the configured KenigEvents CDN (including raw Telegram source
  URLs) fail closed through the existing `canonical_event_media_cdn_url` guard.
- A complete Astro catalog build and generated-dist `check:preview` were not run
  in this lane because the dedicated worktree has no installed `site/node_modules`
  or generated `dist`. They remain integration gates after all schema lanes are
  merged.
- Canonical docs and `CHANGELOG.md` were outside this lane's writable scope and
  remain integration-owner work.

## Merge notes

- Cherry-pick implementation commits `f4393458` and `8d6c8ff3`, plus both
  lane-results metadata commits (or cherry-pick the branch range in order).
- No changes were made to `site/src/lib/events.ts` because imported JSON already
  conforms structurally to the optional `PreviewEvent` field.
