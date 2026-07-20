# Backend lane results

- Lane ID: `geometry-backend`
- Requirements: R01, R02, R03
- Base SHA: `c587a0cf86e144a88c0457035866c8325ea59dc5`
- Implementation head SHA: `32f572e2`
- Status: complete

## Requirement evidence

### R01 — exact image identity and geometry freshness: Done

- New canonical poster writes use immutable `p/image/v2/<sha-prefix>/<encoded-sha256>.webp` keys. The legacy `build_supabase_poster_object_path` and legacy `/p/dh16/` URLs remain readable, but both production writers (`event_media` materialization and `main.upload_images`) now use the exact-content v2 helper.
- `raw_sha256` records the exact hosted WebP bytes while `PosterCandidate.sha256` remains source/candidate identity.
- `invalidate_event_poster_visual_evidence` centrally clears fingerprints, linked geometry, OCR/semantic role, focal point, safe-crop decision, and derivatives when display identity or pixel identity changes. It is used by Smart Update existing-row replacement, fingerprint refresh, and the Catbox-to-Yandex backfill writer.
- Current-geometry lookup and next-candidate selection require exact `EventPoster.pixel_sha256 == EventImageGeometry.pixel_sha256` plus current model/prompt.

### R02 — Smart Update accumulation: Done

- Existing single-poster Smart Update enqueue path remains covered.
- Resolving the final pending pair to `approved` now arms a follow-up enrichment job, closing the path where the newly promoted poster previously received no geometry.
- Coalesced pending jobs move earlier when immediately eligible work appears, without creating duplicate outbox rows.
- The bounded enrichment enqueue script now includes missing/stale/model/prompt/pixel-mismatched geometry, not only derivative/semantic gaps.

### R03 — semantic retry and key rotation: Done

- Semantic role calls use the declared normal key pool (`EVENT_MEDIA_ROLE_GOOGLE_KEY_ENVS`, default `GOOGLE_API_KEY4,GOOGLE_API_KEY5`) with no overflow/model fallback.
- 429/RPM/TPM retries are delayed (default 10 minutes), timeouts/5xx are delayed (default 15 minutes), and RPD/daily quota waits until the next UTC day. Retry eligibility is stored on the poster and survives intervening geometry jobs.
- Invalid/low-confidence schema/content responses remain terminal `error` and require explicit operator retry.

## Changed files

- `event_media.py`
- `main.py`
- `media_dedup.py`
- `smart_event_update.py`
- `scripts/backfill_catbox_posters_to_yandex.py`
- `scripts/enqueue_static_event_media_enrichment.py`
- `tests/test_event_media_gate.py`
- `tests/test_event_media_semantics_and_derivatives.py`
- `tests/test_static_media_enrichment_scope.py`
- `tests/test_upload_images.py`

Scope expansion for the narrow production writers in `smart_event_update.py`, `main.py`, `media_dedup.py`, and `scripts/backfill_catbox_posters_to_yandex.py` was explicitly approved by the parent integrator after mapper evidence identified the dHash overwrite and URL-replacement paths.

## Validation

```text
python -m py_compile event_media.py smart_event_update.py main.py media_dedup.py \
  scripts/backfill_catbox_posters_to_yandex.py \
  scripts/enqueue_static_event_media_enrichment.py
# passed

git diff --check
# passed

pytest -q \
  tests/test_event_media_semantics_and_derivatives.py \
  tests/test_event_media_gate.py \
  tests/test_static_media_enrichment_scope.py \
  tests/test_upload_images.py \
  tests/test_smart_event_update_date_media_helpers.py \
  tests/test_backfill_catbox_posters_to_yandex.py
# 57 passed, 19 warnings in 14.14s
```

An expanded run additionally included `tests/test_genai_dump_and_poster_dedup.py`: 75 passed and one unrelated pre-existing date-sensitive failure, `test_media_rehydrate_does_not_refetch_ordinary_single_source_event` (its fixed event date `2026-06-01` is past the current runtime date and the failure is outside this lane). The isolated lane-owned backfill tests passed.

## Residual risks / integration notes

- Existing legacy dHash objects are not rewritten automatically. New writes cannot mutate them; known stale poster rows still need a paced post-deploy enqueue/repair so the exact pixel invariant is re-established.
- No schema migration was introduced; delayed retry eligibility reuses `media_semantic_classified_at` while status is `pending`.
- Static exporter/renderer consumption is owned by the separate static lane.
