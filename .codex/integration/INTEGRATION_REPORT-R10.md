# Integration report R10 — accepted mobile rail restoration

## Scope

Serial integration after three independent read-only mapping lanes. The current implementation selectively ports the accepted v23/v28 donor instead of merging its branch wholesale. Shared Astro feedback, media selection and medallion resolvers remain authoritative.

## Integration decisions

1. Shared `Icon.astro` owns hollow/filled heart geometry; no duplicate rail SVG state implementation.
2. Donor edge gestures call the canonical `data-feedback-action` controls. Trusted post-drag clicks are captured before links/buttons; untrusted canonical `.click()` calls remain available to the shared feedback runtime.
3. UI completion follows `aria-pressed` through `MutationObserver`, not fixed timing. Suppression state has a bounded 550ms cleanup.
4. OCR stays fail-closed. Only an individually reviewed no-text override with `listing_no_ocr_review=true` can override an event-level OCR marker.
5. `Море внутри` is enabled only by structured festival binding; its token is a rail sibling, never an image overlay.
6. The confirmation modal hides the fixed bottom dock while open; Playwright verifies cancel and both edge paths without intercepted controls.

## Validation and release boundary

- Tests/build/check/browser evidence: see `.codex/lanes/R10/RESULTS.md`.
- Gemini 3.1 Pro correction review: GO after exact source/generated-output checks.
- Release boundary: immutable secret/noindex preview only. No production generation/deploy.
