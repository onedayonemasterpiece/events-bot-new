# Static event v10 integration report

Base: `fix/static-site-v4-personalization-media-20260716@9e00f9a6`
Integration branch: `integration/static-event-v10-system-routing`

| Lane | Requirements | Branch / commit | Status | Evidence |
|---|---|---|---|---|
| north-transport | R1 | `agent/static-event-v10/north-transport@4363f2b7` → integrated as `172cca76` | done | shared preferred-boarding resolver; terminal provenance + North `+15`; build/static/unit gates in `.codex/lanes/north-transport/RESULTS.md` |
| design-system-copy-action | R3,R4 | `feature/static-design-system-catalog-20260717@29cb5fa1` (pushed) | done, coordinated branch | reusable 44px CopyAction, catalog states, DS checks, `ADD-DS-08`; `.codex/lanes/design-system-copy-action/RESULTS.md`. Full DS branch is not cherry-picked because its catalog base is newer/divergent; the product branch implements the same primitive contract. |
| mobile-link-routing | R5,R6 | `agent/static-event-v10/mobile-routing@1585f64b` → integrated as `1574d152` | done | base-scoped cache + current-preview normalization; 72 mobile pages, 5 real clicks and poisoned-v7 cache pass; `.codex/lanes/mobile-routing/RESULTS.md` |
| serial-integrator | R2,R3,R5,R6 | `integration/static-event-v10-system-routing@6ee7c8b5` | done | v10 immutable preview; public component geometry and 72-page mobile crawl pass; Gemini 3.1 Pro (High) PASS |

## External design review

The first agy response was truncated and is excluded. The completed focused
Gemini 3.1 Pro (High) review is retained at
`artifacts/codex/static-site-v10-system-routing/agy-kaup-laconic-followup-review.md`.
Its recommendation—one flat hierarchy, closed transfer fine print, two concise
bus rows, visible last-mile/return risks and icon-only map actions—was applied
without dropping North, Romanovo or venue arrival calculations.

## Final gate

- build: `373` routes / `303` event pages;
- generated contracts: preview, production desktop, rail directory and bus directory pass;
- public component Playwright: KAUP `320/390`, phone `1366/1536/1920`, copy state and before/after geometry, `failures: []`;
- public catalog Playwright: `36/36` events at each of `320` and `390`, five actual related clicks and poisoned-v7 cache rebasing, `failures: []`;
- external review: `artifacts/codex/static-site-v10-system-routing/agy-v10-final-review.md` — `PASS`, no material blockers;
- immutable noindex preview: `https://kenigevents.ru/preview-20260717t-static-personalization-v10-system-routing/__preview/`; production root unchanged.
- Telegram handoff: existing mobile UI review topic, verified message `254` (`reply_to_msg_id=2`).
