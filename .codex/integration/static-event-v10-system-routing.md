# Static event v10 integration report

Base: `fix/static-site-v4-personalization-media-20260716@9e00f9a6`
Integration branch: `integration/static-event-v10-system-routing`

| Lane | Requirements | Branch / commit | Status | Evidence |
|---|---|---|---|---|
| north-transport | R1 | `agent/static-event-v10/north-transport@4363f2b7` → integrated as `172cca76` | done | shared preferred-boarding resolver; terminal provenance + North `+15`; build/static/unit gates in `.codex/lanes/north-transport/RESULTS.md` |
| design-system-copy-action | R3,R4 | `feature/static-design-system-catalog-20260717@29cb5fa1` (pushed) | done, coordinated branch | reusable 44px CopyAction, catalog states, DS checks, `ADD-DS-08`; `.codex/lanes/design-system-copy-action/RESULTS.md`. Full DS branch is not cherry-picked because its catalog base is newer/divergent; the product branch implements the same primitive contract. |
| mobile-link-routing | R5,R6 | `agent/static-event-v10/mobile-routing@1585f64b` → integrated as `1574d152` | done | base-scoped cache + current-preview normalization; 72 mobile pages, 5 real clicks and poisoned-v7 cache pass; `.codex/lanes/mobile-routing/RESULTS.md` |
| serial-integrator | R2,R3,R5,R6 | `integration/static-event-v10-system-routing` | running | Gemini 3.1 Pro (High) concise hierarchy review; KAUP/product CopyAction integration and final public acceptance |

## External design review

The first agy response was truncated and is excluded. The completed focused
Gemini 3.1 Pro (High) review is retained at
`artifacts/codex/static-site-v10-system-routing/agy-kaup-laconic-followup-review.md`.
Its recommendation—one flat hierarchy, closed transfer fine print, two concise
bus rows, visible last-mile/return risks and icon-only map actions—was applied
without dropping North, Romanovo or venue arrival calculations.
