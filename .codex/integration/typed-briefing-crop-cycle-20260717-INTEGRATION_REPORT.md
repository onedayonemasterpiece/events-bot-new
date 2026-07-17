# Integration report: typed briefing crop cycle

## Scope

Isolated `/lab/briefing/` changes only. The production homepage, production
database and runtime page-view LLM path were not modified.

## Integrated behavior

1. Narrative media no longer exits independently or leaves an empty terminal
   hero.
2. Automatic review/public transitions preload and decode the next exact media
   plan before atomically changing copy, CTA and media.
3. Manual review remains the default and persists indefinitely; `По очереди`
   is an explicit optional finite sequence.
4. One vertical image uses a source-faithful contiguous right cluster. Three
   vertical images use three contiguous macro-panels without per-cell source
   mixing.
5. Face-risk scenes use curated contain/focal fallbacks; unsafe panorama cover
   is rejected.
6. A bounded Gemma 4 probe returns semantic vertical intervals only. A
   deterministic solver owns target geometry and can reject infeasible crops.

## Validation

- `playwright test tests/playwright/static_briefing_lab.spec.ts --workers=1`:
  `17 passed`.
- `pytest -q tests/test_briefing_crop_interval_probe.py`: `3 passed`.
- `npm --prefix site run check:lab`: allowlist pass (`6 files`).
- `git diff --check`: pass.
- Gemini 3.1 Pro High final gate: `R01–R07 PASS`, `OVERALL PASS`, isolated-lab
  publication allowed; earlier FAIL artifacts remain committed as provenance.
- Immutable public build:
  `preview-20260717t1237-briefing-lab-139d9809`; public verification pass.
- Telegram mobile-review topic `6`: URL, three desktop screenshots, one mobile
  screenshot and sequential WebM delivered and verified as messages `185–190`;
  no later incoming comment was present at closure.
