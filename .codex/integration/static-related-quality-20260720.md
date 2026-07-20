# Static related quality integration — 2026-07-20

Branch: `integration/static-related-quality-20260720`
Base: `origin/main` (`288b56790ba0866fcbf3da827c499c421425b709`)
Publication boundary: immutable noindex `/_review/<token>/` only; no stable-root promotion.

## Requirement matrix

| ID | Requirement | Status | Implementation / evidence |
|---|---|---|---|
| R01 | Repair recommendation crop/component drift | Done locally | `relatedCardLayout.mjs` is the one final treatment resolver used by row packing and `EventCard`; runtime continuation consumes the serialized fit. Generated 6408 + dynamic specimen geometry is blocking in `check-browser-release-gate.mjs`. |
| R02 | Repair similar recall and decide LLM policy | Done locally | Vector receipt/cache revision barrier, reciprocal exact-normalized-title and cosine `>=.88` repair, zero-incoming broader rescue and topology gate. Gemini 3.1 Pro recommends routine pgvector plus targeted offline LLM audit; the old per-anchor verifier remains disabled because a 248-anchor build is about 496 successful provider calls, not one call. |
| R03 | Make real visual/keyboard journeys mandatory | Done locally | Pinned Chromium gate checks final computed fit, canonical cards on both surfaces, real gallery Enter to a new document and destination BODY arrows, plus footer P/S clipboard/toast from BODY and off-screen event focus. Manifests receive `browser_visual=ok` only after all assertions pass. |
| R04 | Smart Update produces corrected pages | Implemented, release pending | Atomic vector sync receipt feeds Smart Update → Kaggle corpus revision; root proof and secret candidate both run the gate. Immutable navigation is mounted for the production artifact family. Fly deploy, fresh vector receipt and new secret candidate remain release steps. |

## Local verification

- `node --test site/tests/*.test.mjs`: 60 passed.
- `npm --prefix site run test:browser-release-gate`: 4 passed.
- Generated 382-page preview with production-family keyboard path.
- `npm --prefix site run check:browser-release -- --root site/dist/preview-integration-static-related-quality`: passed all four browser journeys; report: `artifacts/codex/static-related-quality-20260720/local-browser-release.json`.
- Python incident suite: 66 passed.
- Python compile and `git diff --check`: passed.
- `npm --prefix site run check:preview` still rejects the pre-existing Garage autorotation fixture because the current exact-geometry data intentionally fail closes all six images to `contain`; production release checks use the reasoned protected-crop contract. This is not used to waive the new browser gate.

## Consultant evidence

Agy Gemini model `gemini-3.1-pro-preview` returned NO-SHIP for the old candidate and selected pgvector + targeted offline LLM audit for the corrected architecture. The immutable corrected candidate still requires a fresh Gemini Pro acceptance before incident closure.

## Release evidence

Pending: merge to `origin/main`, Fly deployment, vector receipt, Smart Update/Kaggle run, immutable candidate URL/manifest, post-run 48-hour diagnostics and Gemini acceptance.
