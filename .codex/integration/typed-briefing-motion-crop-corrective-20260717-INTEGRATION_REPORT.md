# Typed briefing motion/crop corrective integration report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| motion-contract-audit | advisory-R01, advisory-R02 | integration/typed-briefing-motion-crop-corrective-20260717 | merged | read-only | advice incorporated serially | lifecycle regression located at atomic-swap/removed-exit overcorrection |
| crop-regression-audit | advisory-R03 | integration/typed-briefing-motion-crop-corrective-20260717 | merged | read-only | advice incorporated serially | exact 38425f28 baseline values restored; only two head-safe exceptions retained |
| portrait-fill-audit | advisory-R04 | integration/typed-briefing-motion-crop-corrective-20260717 | merged | read-only | advice incorporated serially | contiguous source panels, cover geometry, all active columns owned |
| serial-integrator | R01, R02, R03, R04 | integration/typed-briefing-motion-crop-corrective-20260717 | merged | this report commit | direct integration | Playwright 17/17 + 2/2; Gemini FAIL→PASS; screenshots/WebM |

## Closure audit

- R01 **Done** — every newly rendered mosaic starts from `is-entering` and transitions through deterministic irregular tile timings.
- R02 **Done** — only automatic transitions invoke exit after successor preload; terminal/manual media remains; pause invalidates pending transition.
- R03 **Done** — Ivana Kupala and hay day match the accepted crop baseline; unrelated focal regressions are reverted; Writing/Vertinsky remain explicit head-safe exceptions.
- R04 **Done** — multi-source portrait collage allocates all columns (`7/7/6` at 20), each tile belongs to one source and uses `cover`.
- Production homepage **unchanged**.
