# R12 integration report

## Release scope

- Branch: `integration/unified-corrections-r12-20260726`
- Base: `69dad0ae38b0f89ced776c2a7faf749bd3c9fbc1` (published R11 noindex prototype)
- Runtime commits: `7c7ae335`, `009c4798`
- Canonical docs/gates commit: `d3b9763b`
- Preview: `preview-20260726-unified-corrections-r12`
- Scope: immutable noindex prototype only; production generation is unchanged.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R12-01 | Done | The real R11 root cause was reproduced: after the immutable Today date crossed midnight, no-end rows stayed vivid because the runtime required exact date equality. The elapsed-day branch now mutes only main mobile media. Controlled-clock Playwright on real events `7018`, `6956`, `7043` verifies past/started media muted, future vivid, explicit end transition, post-midnight behavior and 1366px desktop isolation. The same focused gate passed against the published R12 URL. |
| R12-02 | Done | Event `7018` resolves the existing `ruin-keepers` asset through the exact manifest `listingEventIds:[7018]` relation. Evidence is `event_id / curated_event`; title, description, city and unrelated venue `центр «Крупорушка»` cannot match. Public 390px Chromium found the horizontally revealed WebP with `naturalWidth=512` and no overflow/errors. |
| R12-03 | Done | RZD Lastochka is projected only from non-null `getEventTransportSuggestion(desktopEventWithExplicitEnd(event))`. Control event `6529` keeps MUMOD Main/TopSlot and renders Lastochka Secondary/InlineSlot; public desktop/mobile QA found loaded `768px` artwork, zero RZD TopSlot instances, zero horizontal overflow and zero page errors. Null-suggestion regression passes. |
| R12-04 | Missing | The user supplied an empty item `4.`. No requirement was inferred; it remains available for clarification. |

## Integration and validation

1. `7c7ae335` — elapsed mobile Today temporal state plus real-data Playwright.
2. `009c4798` — explicit Ruin Keepers listing binding and grounded RZD event-detail token.
3. `d3b9763b` — canonical docs, CHANGELOG and build-clock-aware generated-output gate.

Validation:

- Astro preview: **431 pages**, authorized Search configured.
- `check:preview`: **PASS**, 288 events.
- `check:unified-prototype`: **PASS**, 18 primary routes / 288 event pages.
- Focused integrated Node suites: **50/50 PASS**, including occurrence regressions.
- Local controlled-clock Today Playwright: **PASS**.
- Public controlled-clock Today Playwright: **PASS**.
- Public HTTP: hub, 26 July and event `6529` all **HTTP/2 200**.
- Public medallion browser gate: Ruin Keepers loaded; MUMOD Main; Lastochka Inline Secondary; no TopSlot, overflow or page errors.
- Generated-output gate was repaired to read `preview-build.json` clock rather than stale snapshot metadata, and to retain the expired Pianissimo crop canary without requiring an obsolete date route.
- Preview upload and both main-domain/website-endpoint verification: **PASS**; writes remained below the versioned preview prefix.
- Telegram topic `548` handoff: message `690`, verified reply to `548`.

Worker evidence:

- `.codex/lanes/R12-SATURATION/RESULTS.md`
- `.codex/lanes/R12-MEDALLIONS/RESULTS.md`
- ignored browser artifacts: `artifacts/codex/r12-integration/`

## External consultant gate

A fresh `/home/dev/.local/bin/a-gemini --print-timeout 10m` acceptance attempt
was made after integration. It failed before model execution with Antigravity's
account eligibility/location message (`exit=1`, empty stdout). This records only
the provider response; it does not infer the account's actual region. No
Flash/Lite/Gemma or API-key call was substituted, so Gemini review is **blocked,
not complete**. Redacted evidence is in ignored
`artifacts/codex/r12-gemini/`.
