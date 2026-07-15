# R03 renderer lane results

## Status

Committed; final SHA is reported in the lane handoff because embedding it here
would change that SHA.

## Requirement

- R03 — remove contain/letterbox framing from non-OCR event photos on cube
  faces while preserving the existing safe treatment for OCR posters.

## Branch and base

- Branch: `agent/f18-service-share-refine/renderer`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/f18-refine-renderer`
- Base: `46751dcb4096abb354877417d18227727b91a07b`

## Implemented

- Added an auditable `image_has_ocr_text` boolean from the exact first
  approved, non-duplicate `eventposter` selected by `display_order, id`.
- Made the `safe_crop` and `image_text_mode` metadata subqueries use the same
  approved/non-duplicate predicate and order as the public poster URL.
- Propagated OCR presence through catalog snapshot and daily selection into face
  preparation.
- Added one framing decision point:
  - `safe_crop=true` remains centered cover;
  - `visual_only`, or null/unknown mode with no OCR, uses a centered square
    photo cover and therefore has no generated contain/letterbox field;
  - unsafe `ocr_text` or any asset with actual OCR remains full-image contain
    over blurred fill, preserving poster-edge copy.
- Retained the title/date overlay and all selection/composition behavior.
- Recorded the chosen framing mode and source crop in the existing face
  manifest.

## Verification

- `/tmp/f18-renderer-venv/bin/python -m pytest -q tests/test_service_share_face_preparation.py tests/test_service_share_still_kaggle.py tests/test_service_share_card_daily.py`
  — **22 passed**.
- `/tmp/f18-renderer-venv/bin/python -m compileall -q ...` — passed for all
  changed Python modules/tests.
- `git diff --check` — passed.
- Read-only inspection of `prod-20260715.sqlite` confirmed the visible
  landscape event `5370` uses the first approved face (`eventposter.id=9513`),
  has null `image_text_mode`, empty OCR, and a 1280x853 source. It now resolves
  to `non_ocr_photo_center_cover`; the representative center crop is
  `[213, 0, 1066, 853]` before 1024x1024 scaling instead of full-image contain.
- The same production inspection confirmed OCR-bearing selected faces remain
  protected, including events `6811` (269 OCR characters), `4517` (176),
  `5459` (`ocr_text`, 240), and `2601` (`ocr_text`, 301).

## Risks / integration notes

- A fresh catalog snapshot/selection is required before rerendering. Reusing an
  old selection that predates `image_has_ocr_text` would interpret a null mode
  as non-OCR.
- The rule intentionally follows the supplied accepted metadata; it does not
  add a deterministic image-content/OCR classifier or change event meaning.
- No site/UI, documentation, changelog, schedule, Kaggle, or production files
  were changed in this lane.
