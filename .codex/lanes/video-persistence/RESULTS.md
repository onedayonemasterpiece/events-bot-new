# Video persistence lane result

Implementation commit: `406da0b4` (`Add normalized event video persistence`).

## Delivered

- Added global, SHA-256-unique `VideoAsset` analysis/CDN cache and M:N `EventVideoLink` rows with unique `(event_id, video_asset_id)` links.
- Added per-event relevance, match reason/confidence, and `ranking_score = round(0.75 * showcase_score + 0.25 * event_relevance_score)` plus event/rank index.
- Normalized and validated Kaggle video payloads, including `cache_hit`, terminal rejected analyses, non-finite scores, and event-specific `event_relevance_scores`.
- Preserved original extracted event indexes through dedupe/poster expansion and mapped Smart Update `created`, `merged`, and `skipped_nochange` results to video links.
- Made exact-SHA imports idempotent, allowed the same video to link to multiple events, preserved terminal analysis, and canceled stale deletion intents on relink.
- Added orphan cleanup that waits for the last event link, queues only the main CDN binary, clears CDN state without deleting the analysis row, and does not select producer sidecars.
- Added pre-delete live-reference rechecks; stale queue rows are canceled instead of deleting relinked media.
- Made Yandex queue draining work without a Supabase client and prioritized Yandex rows so an older unavailable-Supabase row cannot block them.

## Validation

- `python3 -m py_compile models.py db.py main_part2.py supabase_storage.py source_parsing/telegram/handlers.py tests/test_event_video_persistence.py` — passed.
- `git diff --check` — passed before commit.
- `uv run --with-requirements requirements.txt pytest -q tests/test_event_video_persistence.py` — `8 passed`.
- A combined run with existing `tests/test_supabase_storage.py` printed `16 passed`; that legacy file does not close its `Database` engines, so the process required termination after pytest completed. All relevant cases are also covered by the new clean-exiting test module.
- `tests/test_tg_monitor_reprocess_incomplete_scan.py` currently has 6 date-sensitive failures because its 2026-07-15 fixtures are past events on current date 2026-07-31; per integration instruction this was recorded rather than changed. Four tests in that file passed.

## Integration notes

- Producer contract consumed here: accepted/cache-hit items need SHA-256 and managed CDN URL/path; canonical per-event matches are `event_relevance_scores: [{event_index, relevance_score, reason?, confidence?}]` (relation fields may also use their normalized aliases).
- Static export/consumer changes are intentionally outside this lane. Query `EventVideoLink -> VideoAsset`, filter accepted assets with a CDN URL, and order `ranking_score DESC`.
- Legacy `EventMediaAsset` rows are left intact; new Telegram imports write only the normalized tables.
