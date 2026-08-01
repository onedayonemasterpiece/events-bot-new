# R04/R08 — editorial writer and regeneration lane

Status: implemented; no production/deploy actions performed.

## Delivered

- `region_talk_editorial_onboarding_writer_v8_staged`: controlled Strategy → Grounded Writer → deterministic validation → Critic flow, at most one writer retry.
- Existing Google AI gateway/Supabase reserve and durable YDB budget only; stage prompt/model/usage/request fingerprints are auditable and budget/quota defers cleanly.
- Exactly two Russian editorial paragraphs, 150–500 chars each, 550–900 visible-character atomic caption, source/bridge in P1, third-person specifics/motivation in P2, grounding-ID/history/voice/language/anti-clickbait checks.
- Bounded history from actual publication or exact-current `approved + clean` rows only; forced/invalid history becomes `fresh_start`.
- Exact Telegram/VK social refetch through role-scoped paths and retained external article intake; no generic/E2E Telegram auth.
- Lossless YDB persistence of evidence, history, plan, grounding, critic, stage audit and exact media revision.
- Explicit v7 stale contract and one-time legacy `principle approved + rewrite requested` audit; current operator review projection is reset so approval cannot leak to v8.
- Media-first review renderer contract: article hero, social hero, ordered 3–6 album, source video, or diagnosed link-preview fallback. Telethon D2 materializes exact source bytes/refs; Bot API supports public media URLs. Caption and ordered media share one fingerprint/delivery revision.
- Planner no longer promotes retained article teaser projection as finished public copy; orchestrator schedules article/social v8 backfill separately.

## Validation

- Focused writer/notifier/planner/orchestrator/finalizer/reaction suite: **193 passed**.
- Full `pytest -q tests/test_region_talk*.py`: **652 passed in 44.24s**.
- `python3 -m py_compile` for changed scripts/tests: passed.
- `git diff --check`: passed.
- Generated root-level CandidateReport test artifacts removed before commit.

## Integration notes

- Expected conflicts: `CHANGELOG.md`, Region Talk docs, `region_talk_goal_notify.py`, `region_talk_orchestrator.py`, and `region_talk_publication_finalizer.py` overlap reaction/runtime/visual lanes. Preserve both sets of fields/contracts.
- Visual lane materialization inputs consumed by this lane: `selected_media_materialization_json`, `media_materialization_items_json`, `selected_media_ids`, `selected_primary_media_id`, and presentation recommendations.
- Production regeneration/re-delivery is intentionally not executed from this worker.
