# Integration Report — static-site production release 2026-07-15

Base: `origin/main@926dad8a91fc7f1070126d32a05281aa92ff1666`
Integration branch: `integration/static-site-production-release-20260715-v2`
Mode: bounded production-publisher worker, then one serial integrator for shared Astro/export/release surfaces.

## Requirement matrix

| ID | Requirement | Owner / dependency | Status | Evidence |
|---|---|---|---|---|
| R01 | Approved desktop/mobile header lockups and favicon | integrated before this release | Done | `AnnouncementsLockup.astro`, `EventLayout.astro`, `favicon.svg`; source range `8a1bbc59^..d9ccc527` already integrated into main |
| R02 | Desktop hero/parallax, pinned media rail and CTA arrival/safe-exit mechanics; mobile unchanged | prior desktop integration + release QA | Done | main desktop layouts; mobile rules outside the desktop override |
| R03 | Fail-closed semantic poster/document vs event-photo rendering | media enrichment + Astro | Done | only explicit `event_photo` receives cover; unknown/document/poster keeps complete frame |
| R04 | Efficient multi-photo gallery and symmetric group paging | prior desktop integration | Done | fullscreen gallery/group paging checks in `check-preview.mjs` |
| R05 | Related event cards without cropped posters or side fields | serial integrator | In progress | full-width natural-ratio document surface and focused Playwright evidence; final public QA pending |
| R06 | Safe production-root publisher, immutable staging, verification and rollback | worker `static-site-production-promotion` | Merged into integration | implementation `55ff1f0c`; cherry-picked as `62ba7110`; lane result `82c98557` |
| R07 | Production media enrichment scoped to current public events | merged PR #42 | Done | `origin/main@926dad8a`; production queue audit has zero ineligible pending/running rows |
| R08 | Yandex/Supabase authorized search readiness | existing auth/search integration | Done locally | environment/provider/redirect/Edge readiness probe passed; public UI QA pending |
| R09 | Full current production build, CDN media and stable ICS promotion | depends on R05/R06 merge | Pending | production build/publish gates not yet executed |
| R10 | Root production Playwright/HTTP/release/incident evidence | depends on R09 | Pending | final root checks not yet executed |

## Lane closure

| Lane | Requirement IDs | Branch | Status | Head / integration | Evidence |
|---|---|---|---|---|---|
| static-site-production-promotion | R06, R09 tooling | `agent/static-site-production-promotion/static-site-production-promotion-20260715` | merged | `55ff1f0c` -> `62ba7110` | `.codex/lanes/static-site-production-promotion/RESULTS.md` |
| serial-integrator | R01-R05, R07-R10 | `integration/static-site-production-release-20260715-v2` | in progress | current HEAD | local source assertions and Playwright artifact under `artifacts/codex/static-site-production-final-20260715/` |

## Current acceptance evidence

- Gemini Pro class review selected a natural-ratio, full-width poster/document treatment with equal outer card bottoms and variable internal media/body boundary.
- Local Chromium at 1536x864 confirmed poster shell/image width equality, natural ratio, `overflow: visible`, `height: auto`, no transform and equal card bottoms.
- Preview build succeeds; final check is rerunning with browser-safe Supabase/Yandex public env.
- Production bucket has not been mutated from this integration branch.
