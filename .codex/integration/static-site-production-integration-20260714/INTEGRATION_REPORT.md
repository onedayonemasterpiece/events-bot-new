# Integration Report — static-site production integration 2026-07-14

- Base: `origin/main` at `91a8e92741688b1298d3b234aecbe61994b18762`
- Integration branch: `integration/static-site-production-20260714`
- Implementation commit: `6c1dc3c1c9e73531bd2923a43af34ae8341f31db`
Mode: parallel read-only mapping followed by one serial integrator and one
Gemini 3.1 Pro (High) acceptance gate.

## Lane integration

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| L01 | — | — | merged | — | Findings incorporated serially | Header ancestry/file-range audit |
| L02 | — | — | merged | — | Findings incorporated serially | Production DOM/mobile-coupling map |
| L03 | — | — | merged | — | Findings incorporated serially | Derivative/CDN request-cost review |
| L04 | — | — | merged | — | Findings incorporated serially | Fail-closed role/prompt contract |
| L05 | R01–R10 | `integration/static-site-production-20260714` | committed | `6c1dc3c1` | Awaiting clean latest-main release cherry-pick | Build, tests, Playwright evidence |
| L06 | — | — | merged | — | Acceptance gate recorded | Gemini 3.1 Pro `PASS` |

## Requirement closure at integration checkpoint

| ID | Requirement | Status | Evidence | Missing / risk |
|---|---|---|---|---|
| R01 | Selectively import agreed header range | Done | Shared final assets/components restored; stale branch not merged | None |
| R02 | Shared lockups, right menu, exact active state | Done | Desktop `240×88`, mobile `128×96`; event detail has no active nav | None |
| R03 | Approved transparent wide-`о` favicon | Done | `favicon.svg` and brand master match final geometry | None |
| R04 | Safe CTA/poster release boundary | Done | Browser geometry keeps `100px` safe shell gap before feed | None |
| R05 | Continuous Editorial desktop + strict poster placement | Done | Production event page, not lab-only; mobile media contract unchanged | Unknown roles intentionally fail closed |
| R06 | LLM-first strict poster classification | Done | Schema, prompt, classifier, export routing, docs and tests | Prod backfill is release work |
| R07 | Symmetric grouped viewer + event title | Done | Forward/back deltas are viewport-sized; title in top bar | None |
| R08 | Fast prepared thumbnails | Done | Immutable 256/512 WebP, `srcset`; sprite rejected after review | Prod derivative fill is release work |
| R09 | Related cards without side fields/horizontal crop | Done | Width-fit geometry: image width equals shell width, zero side gaps | Trusted `event_photo` remains the only cover role |
| R10 | Integrate, preserve mobile, publish, return to main | Partial | Full local build/check/Playwright and Gemini pass | Public preview, `origin/main`, backend/schema and production release pending |

## Local acceptance evidence

- focused pytest: `35 passed`;
- full preview: `420` pages;
- `npm run check:preview`: passed;
- Python/Node syntax and `git diff --check`: passed;
- desktop and mobile Playwright screenshots/geometry captured under ignored
  `artifacts/codex/static-site-production-integration-20260714/`;
- Gemini 3.1 Pro (High): `PASS`, no blocking or non-blocking findings.
