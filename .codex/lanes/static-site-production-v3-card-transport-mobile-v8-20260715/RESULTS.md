# Static-site production v3 integration results

| Requirement | Status | Evidence |
|---|---|---|
| R01 — desktop `Смотрите дальше` geometry and crop policy | Done | `DesktopEventPage.astro`, `EventCard.astro`; 282 pages / 1090 rows / 0 corpus failures |
| R02 — train image restored | Done | `EventTransportSchedule.astro`; 21 rail pages plus real 390×844 decode/geometry check |
| R03 — exact accepted mobile V8 | Done | exact `fd8766b1` lab/runtime source integrated into production route; three real mobile cases and related navigation pass |

Integration order was mapping → shared implementation → full build → corpus
browser gate → public noindex deploy → public browser gate. The public target is
`preview-20260715t-production-mobile-v8-related-transport-v3`; HTTP and
Playwright passed without touching the production root. No writable worker was
left outstanding; temporary Playwright artifacts remain ignored under
`artifacts/codex/`.
