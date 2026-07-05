# MVP candidate report / favorites table

Status: MVP-1 target. The first practical result is an XLSX report the product owner can inspect by eye. Canonical export name: `region_talk_candidates_report`.

## Artifact paths

Primary:

- `artifacts/region-talk/candidates-latest.xlsx`

Companions:

- `artifacts/region-talk/candidates-latest.csv`
- `data/region-talk/candidates-latest.json`
- `artifacts/region-talk/candidates-latest.md` or `.html`

Artifacts are not committed unless converted into minimal fixtures later.

## XLSX workbook sheets

1. `run_summary` — run id, git SHA, config, seeds, budgets, model ids, counts, errors.
2. `source_candidates` — found sources, score, status, rejection reason.
3. `monitored_sources` — sources scanned, cursors, fetch status.
4. `found_posts` — all posts fetched with basic filters and links.
5. `dropped_posts` — rejects with `rejection_reason` (`news`, `trash`, `not_region`, `weak_media`, `duplicate`, `rights`).
6. `image_quality` — one row per media item with thumbnail/link and model report summary.
7. `candidates` — all post candidates after semantic/media gates.
8. `favorites` — final shortlist for human review.
9. `good_text_weak_media` — good semantic matches blocked by weak media.
10. `verifier_reports` — Gemini/VLM decisions for candidates where called.

## Required candidate columns

- `candidate_id`
- `source_title`
- `source_platform`
- `source_url`
- `post_url`
- `post_date`
- `text_excerpt`
- `short_summary`
- `why_region_relevant`
- `positive_points`
- `neutral_points`
- `concern_points`
- `why_not_news`
- `why_source_external`
- `selected_image_urls`
- `image_thumbnails_or_local_paths`
- `postcardness_score`
- `aesthetic_score`
- `technical_quality_score`
- `region_visual_relevance_score`
- `publication_safety_score`
- `image_model_report_short`
- `candidate_score`
- `suggested_action=report_only|manual_review|ready_for_queue|reject`
- `suggested_platforms=telegram|vk|both`
- `publication_risk_flags`
- `rights_policy_snapshot`

## Strong media gate

Main favorites require strong photos. Without strong photos a post may be shown only in `good_text_weak_media`, not in main publication queue.

## Manual review workflow

MVP-1 does not need an admin UI. The XLSX is the review UI. Human can mark rows externally; MVP-2 may import decisions into `region_talk_favorites` / `region_talk_candidate.decision`.
