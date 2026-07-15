# Static typed briefing integration results

## Outcome

`GO_TO_PROTOTYPE_ONLY` is implemented as an isolated `/lab/briefing/` research page. No production homepage, event-detail template, personalization path, remote telemetry, Gemini runtime, publisher or deployment configuration changed.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Canonical document labels desk-research synthesis complete, production effect none, user/metric validation false. |
| R02 | Done | Gemini Lite and personalization are absent from V0 code and deferred to backlog. |
| R03 | Done | Lab inventory contains exactly eight canonical deterministic IDs plus `neutral_fallback`, one fixed copy each. |
| R04 | Done | QA variants A control, B static and C one reveal use identical local categories/feed fixtures. |
| R05 | Done | First decision region is fully visible at `320×568`; visual QA rejected the clipping `15svh` challenger and accepted a content-fit `114px` (`≈20.1svh`) lab value. |
| R06 | Done | Diff is lab/docs/tests only; no production route or deploy. |
| R07 | Done | Bounded 24-record `window.__briefingTelemetry`, qualified visibility, interruption, session dedupe and BFCache behavior are documented/implemented locally. |
| R08 | Done | Future primary metric is destination-confirmed `event_detail_open / eligible_listing_session`; lab emits only `event_detail_activate`. |
| R09 | Done | Eight P0 blockers remain canonical; expanded platform work moved to `docs/backlog/`. |
| R10 | Done | Prompts/full outputs, run timestamps, display model, agy/wrapper version, session, input SHA, SHA-256 hashes and decision trace are under `docs/reports/`. Exact provider ID/sampling are explicitly “not exposed”. |

## Final verification

- `git diff --check`: pass.
- Playwright spec discovery: 3 tests found.
- Astro dev compilation: pass.
- Chromium `320×568`, B/static: briefing `114px`, no content overflow; first decision region `322–414px`, fully visible.
- Chromium `320×568`, JavaScript disabled: full static briefing visible; first decision region `274–366px`; QA tabs hidden.
- Chromium `1440×900`: briefing `180px`; categories and first feed card visible.
- Qualified telemetry ordering: `eligible_session` → qualified impression/first-event visibility → static completion.
- C/reveal: `pointerdown` produces `briefing_interrupt` then `briefing_complete(interrupt)`.
- Reduced motion: reveal never runs and completes with `reduced_motion`.
- Remote/runtime requests authored by the lab: none; only local Astro dev resources were requested.
- Full preview build generated `/lab/briefing/` successfully, then stopped on unrelated bulk event pages with `ENOSPC`. Targeted page compilation/browser verification passed; full repository preview regression remains constrained by host disk space.

Screenshots and local QA output are ignored under `artifacts/codex/static-typed-briefing-final-20260715/`.
