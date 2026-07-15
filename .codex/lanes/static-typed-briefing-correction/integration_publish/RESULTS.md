# Integration and publication result

## Outcome

The rejected shell-centric artifact was superseded by a communication-first, immutable, `noindex` lab. No production route, production homepage or stable object was changed.

Primary review URL:

<https://kenigevents.ru/preview-20260715t1407-briefing-lab-9c8c9a62/lab/briefing/?variant=c&scenario=today_count&replay=1>

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Variant C reveals semantic fragments with a visible cursor; `Повторить` works without reload. |
| R02 | Done | Eight demo scenario IDs plus explicit fallback are selectable; `Показать все 8` is finite and stoppable. |
| R03 | Done | Every demo uses strong 1–3-line typography, an inline linked accent and no more than two CTAs. |
| R04 | Done | Hero is bounded to `min(42svh, 250px)` mobile / `min(34svh, 306px)` desktop; all five categories wrap immediately below. |
| R05 | Done | `Что оценивать` explains the review surface; the feed is labeled draft context and not a design subject. |
| R06 | Done | Isolated build/check, Playwright, screenshot/video QA, immutable deploy and public verification passed. |

The `communication_ui` worker lane was superseded after producing only a data draft; integration resumed serially and owns the final implementation and verification.

## Verification

- Source commit used for the immutable build: `9c8c9a62`.
- Build ID: `preview-20260715t1407-briefing-lab-9c8c9a62`.
- Playwright: `6 passed`.
- Geometry/copy matrix: `9 scenarios × B/C × 4 viewports = 72` combinations.
- Public A/B/C viewport capture: 12 pages, HTTP 200, no forbidden requests, no failed requests, no external beacons.
- Motion-phase capture: 12 frames plus one WebM; visual inspection confirms progressive fragment reveal on mobile and desktop.
- Robots: `noindex,nofollow,noarchive`.
- Evidence: `artifacts/codex/static-typed-briefing-correction-20260715/` (ignored, not committed).

Disk cleanup did not alter either published preview. It removed the active capacity blocker: the corrected isolated build completed with about 6 GiB free. This is not evidence that an unrelated full-catalog build was run.
