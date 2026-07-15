# Publish integration results

## Outcome

Published and verified a lab-only immutable preview; no production root deployment or feature integration occurred.

- Build ID: `preview-20260715t1241-briefing-lab-0e94a440`
- Build source: `0e94a440adbbfb08ec3f56d0096ee32100b7d12d`
- URL: <https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=a>
- Alternatives: `?variant=b`, `?variant=c`; deterministic QA selection also supports `&scenario=<id>`.
- Uploaded object inventory: exactly 5 allowlisted files under the versioned prefix.
- Public deploy verifier: A/B/C HTTP 200, exact noindex, versioned CSS, no personalization slot; website endpoint fallback HTTP 200.
- Public browser matrix: 12/12 HTTP 200, no briefing overflow, no forbidden remote telemetry, beacons or HTTP failures.
- Screenshots/test artifacts: `artifacts/codex/static-typed-briefing-shareable-20260715/` (ignored, not committed).

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | one command `npm --prefix site run lab` builds, checks and serves the separate one-route `srcDir`/`dist-lab` |
| R02 | Done | verified public URL above and local `preview:lab` |
| R03 | Done | public A/B/C URLs; legacy names retained |
| R04 | Done | Playwright exact fixture/category equality and B/C geometry equality |
| R05 | Done | actual `EventLayout`, `.page-shell`, `.source-links`, `EventListItem`; production card not shrunk |
| R06 | Done | 8 scenarios + fallback × B/C × four requested viewports |
| R07 | Done | `exhibitions_count` and every matrix cell satisfy scroll/client bounds |
| R08 | Done | no-JS/reduced motion/pointer/focus/scroll/session replay acceptance |
| R09 | Done | exact noindex, no prod nav entry, no remote telemetry, versioned prefix-only deploy |
| R10 | Done | bounded `window.__briefingTelemetry` plus JSON export |
| R11 | Done | no Gemini, personalization, scenario/motion expansion, product integration or platform work |
| R12 | Done | block-capacity ENOSPC and full-build limitation documented |
| R13 | Done | URL, 12 screenshots, commands and test report delivered |

## Commands/results

- `PREVIEW_BUILD_ID=... PUBLIC_SITE_ORIGIN=https://kenigevents.ru npm --prefix site run build:lab`: PASS, 1 page.
- `PREVIEW_BUILD_ID=... npm --prefix site run check:lab`: PASS, 5 files.
- Playwright `static_briefing_lab.spec.ts --workers=1`: PASS, 3/3 in 1.2m.
- `npm --prefix site run deploy:lab`: PASS; public and website-endpoint checks passed.
- Browser capture/monitor: PASS, 12 screenshots, zero forbidden requests/beacons/failures.

## Constraint retained

The host still lacks safe capacity for an ordinary full-catalog build. Lab success is not a production-build green signal.
