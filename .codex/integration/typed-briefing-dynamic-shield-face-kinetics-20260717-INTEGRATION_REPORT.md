# Integration report: typed briefing dynamic shield / face kinetics

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| dynamic-copy-shield-audit | R01 | integration/typed-briefing-dynamic-shield-face-kinetics-20260717 | accepted | pending final commit | read-only synthesis | measured-line algorithm and mutation/font/resize test plan |
| face-bbox-contract-audit | R02 | integration/typed-briefing-dynamic-shield-face-kinetics-20260717 | accepted | pending final commit | read-only synthesis | per-source projection, `0.50` floor, status/identity contract |
| kinetics-easing-audit | R03 | integration/typed-briefing-dynamic-shield-face-kinetics-20260717 | accepted | pending final commit | read-only synthesis | easing/timing budget and deterministic safe accent selection |
| serial-integrator | R01, R02, R03 | integration/typed-briefing-dynamic-shield-face-kinetics-20260717 | accepted | pending final commit | direct integration | build/check PASS; Playwright 18/18; Gemini Pro PASS |

## Closure audit

- R01 Done: illumination origins now follow actual rendered line rights and recalculate on generated-copy/reflow changes.
- R02 Done in lab: per-asset boxes project through each panel; intersecting tiles end at opacity `>=0.50`; producer rollout is explicitly documented future work.
- R03 Done: accepted symmetric ease-in-out is explicit for entry/exit; 2–3 delayed accents are deterministic and exclude copy/face cells.
- No mobile narrative raster regression; reduced motion remains immediate.
- No homepage production route or Smart Update runtime was changed.
