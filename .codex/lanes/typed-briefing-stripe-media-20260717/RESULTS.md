# Stripe/media corrective results

- **R01 — Done.** Removed opaque fragment slabs, four-sided shadows and horizontal mosaic gutters. Final treatment is a single 12%-high, 28%-alpha midline wash; real links keep one underline. Media narratives keep the horizontal underscore through fragment reveal and timed continuation.
- **R02 — Done.** Admission requires curated focal `cover`, `>=1000px`, `>=1MP`, and runtime cover-upscale `<=1.10`; otherwise the narrative is text-only. Final source is 2560×1707 and renders at `.422/.563` scale without aspect distortion.
- **R03 — Done.** Scenario-seeded matrix retains seven opacity bands and adds sparse independent `.03`/`.96` contrast accents while preserving fully opaque right columns and rejecting parity/checkerboard structure.
- **R04 — Done.** Media is enabled only for explicitly curated `visual_only` photos with `ocrSafe=true`; named-person, rare, festival, storm and other uncertain/poster sources abstain. Safe coverage is deliberately 3/19 rather than filling the hero with unsafe images.

## Verification

- Isolated build/check: pass.
- Playwright: `16/16` pass against `briefing-lab-21ca7a495173`.
- Gemini 3.1 Pro (High): all R01–R04 plus mobile/motion PASS; overall PASS; publish for user review YES; blockers none.
- Immutable lab: `preview-20260717t0951-briefing-lab-21ca7a49` public verification pass.
- Telegram topic 6: `#144–148`, all receipts verified; post-send top message `148`.

The abandoned read-only reviewer lane produced no usable output and was superseded by the integrator's direct pixel/DOM/source audit. No production homepage files or routes were changed.
