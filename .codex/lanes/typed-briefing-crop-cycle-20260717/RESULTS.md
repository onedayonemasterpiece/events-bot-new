# Typed briefing crop-cycle results

## Requirement closure

| ID | Status | Result |
|---|---|---|
| R01 | Done | Removed independent media exit; manual/final media persists. Sequential transitions preload and decode the next exact plan, then atomically commit copy, CTA and media. |
| R02 | Done | Corrected affected face scenes; the rejected group portrait now uses a contained contiguous right cluster with visible head margin. |
| R03 | Done | Added one-source portrait and three-source portrait-collage states. Each source owns a contiguous five-column macro-panel and retains its aspect ratio. |
| R04 | Done | Added a reproducible Gemma 4 semantic crop-interval probe. 31B is only an interval candidate; deterministic geometry owns fit/reject. 26B is not accepted as sole author. |

## Gates

- Playwright briefing lab: `17 passed`.
- Crop probe tests: `3 passed`.
- Lab allowlist: `6 files`, pass.
- Gemini 3.1 Pro High: two corrective FAIL rounds retained; final `R01–R07 PASS`, `OVERALL PASS`, no blockers.
- Immutable public build `preview-20260717t1237-briefing-lab-139d9809`: deploy and public capture pass.
- Telegram topic `6`: evidence messages `185–190` verified; no new incoming feedback at closure.
- Production homepage and database: unchanged.
