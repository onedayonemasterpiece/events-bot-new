# Desktop multimedia analysis — lane map

Date: 2026-07-12
Integration branch: `feature/event-page-desktop-multimedia-analysis-20260712`

| Lane | Ownership | Requirements | Mode | Effort | Output / integration gate |
| --- | --- | --- | --- | --- | --- |
| L1 — catalog evidence | Production snapshot audit of eligible events, image ordering/orientation, resolution tiers, exact/near-duplicate candidates | R01, R02, R03 | Read-only discovery | high | Reproducible script plus ignored raw artifacts; counts and sampled cases must pass data-quality checks before design decisions |
| L2 — product evidence | Desktop media/reading/carousel/parallax research and Gemini Pro constructive review | R04, R05 | Read-only discovery | max | Source-backed design constraints and a saved Pro-class review; no Flash/Lite substitution |
| L3 — integration | Full desktop-only event flow, feedback and transport placeholders, multiple-media behavior, separate real-event prototype URLs, browser QA | R06, R07, R08, R09 | Serial writes in this worktree | high | Starts only after L1/L2 evidence; mobile production UI remains unchanged; full scroll reaches `Смотрите дальше` |

## Requirement ownership

- **R01:** Count no-OCR multi-image events whose current primary is portrait/square but which have a landscape alternative — L1.
- **R02:** Define and count source-quality tiers for Editorial Slab versus Split Canvas — L1.
- **R03:** Verify same-visual/different-ratio duplicates and specify a neural dedup policy — L1.
- **R04:** Obtain constructive critical review from Gemini Pro class — L2.
- **R05:** Research and specify bounded desktop parallax, sticky release, carousel, crop, viewport-height and accessibility behavior — L2.
- **R06:** Extend prototypes through description and the complete event information flow to related events — L3.
- **R07:** Place a planned consolidated comment-feedback marker without exposing raw comments — L3.
- **R08:** Place future out-of-city transport schedule media variants without inventing schedule facts — L3.
- **R09:** Publish separate concrete-event prototype URLs and verify representative desktop viewports/scroll states — L3.

## Integration order

1. Freeze L1 denominators, eligibility contract, quality definitions and sample event IDs.
2. Give L1 evidence and the current candidate layouts to L2; record Gemini model/status exactly.
3. Integrate only evidence-supported candidates in L3. Shared Astro/CSS files have one serial owner.
4. Run data audit tests, Astro checks/build, Playwright desktop screenshots/scroll assertions, docs/CHANGELOG audit, then commit and push.

## Scope guardrails

- The task changes desktop lab/prototype surfaces only; no fundamental mobile page changes.
- Production data is read from a dated local snapshot; the snapshot and generated audit artifacts stay uncommitted under `artifacts/`.
- The uncommitted parallel `event-comment-feedback` work in the root checkout is read-only context, not a merge base.
- No paid image generation is used.
