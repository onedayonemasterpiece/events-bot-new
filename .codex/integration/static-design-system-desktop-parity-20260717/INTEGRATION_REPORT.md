# Static design-system desktop parity integration

## Lanes

| Lane | Requirement IDs | Branch | Status | Head SHA | Integration | Evidence |
|---|---|---|---|---|---|---|
| catalog-gap-audit | R03, R06 | read-only | merged | n/a | findings applied serially | `.codex/lanes/catalog-gap-audit/RESULTS.md` |
| desktop-inventory | R01, R02 | read-only | merged | n/a | findings applied serially | `.codex/lanes/desktop-inventory/RESULTS.md` |
| loader-search-audit | R04, R05 | read-only | merged | n/a | findings applied serially | `.codex/lanes/loader-search-audit/RESULTS.md` |
| serial-integrator | R01–R06 | `integration/static-design-system-desktop-parity-20260717` | committed and published | build source `b96575e5` | merged accepted runtime `d5dab75a` into governance `efbfde30` | immutable preview and exact-build gates below |

## Requirement closure

| ID | Requirement | Status | Evidence | Remaining risk |
|---|---|---|---|---|
| R01 | Graphite CTA and states | Done | real `DesktopEventActionPanel@2`, nine primary states, responsive/attachment links | visual owner sign-off pending |
| R02 | Poster + preview block and states | Done | real `DesktopEventPage@14` iframe fixtures and 14-scenario desktop lab | visual owner sign-off pending |
| R03 | Exhaustive accepted desktop parity | Done | accepted runtime merge and 303-page production desktop contract | real-device/browser matrix remains release work |
| R04 | Replace faulty loader with skeleton | Done | `Skeleton@1`, no loading StatePanel, no personal-feed hidden override | live auth E2E remains required by open incident |
| R05 | Real search form/progress states | Done | actual `AuthorizedEventSearch@2` fixtures and first-page skeleton runtime | live Yandex auth/query evidence pending |
| R06 | Runtime enforcement, not demo | Done | machine registry, import/consumer/state checks, production route imports | promotion to `origin/main` is separate release gate |

## Validation

- `npm --prefix site run check:design-system` — passed: 22 core tokens, 6 primitives, 23 versioned registry rows, 8 AA pairs.
- `PREVIEW_BUILD_ID=preview-design-system-b96575e5 npm --prefix site run check:preview` — passed, 303 events.
- `PREVIEW_BUILD_ID=preview-design-system-b96575e5 npm --prefix site run check:production-desktop` — passed, 303/303 pages.
- bus and rail directory checks — passed.
- Node design-system/event-detail/personal-feed tests — 10/10 passed.
- targeted Python regressions — 5/5 passed via the project venv.
- `git diff --check` — passed.

## Immutable public evidence

- Build source: `b96575e5` (`integration/static-design-system-desktop-parity-20260717`).
- Build id: `preview-design-system-b96575e5`.
- Catalog: <https://kenigevents.ru/preview-design-system-b96575e5/lab/design-system/>.
- Preview index: <https://kenigevents.ru/preview-design-system-b96575e5/__preview/>.
- Public HTTP verification returned `200 text/html` for the catalog, desktop lab, both embedded poster/preview scenarios, `/poisk/` and the preview index.
- Playwright desktop inspection at `1440×1000` found 9 graphite action states, 8 search states, 10 skeleton fixtures and 2 real desktop composition iframes, with no document-level horizontal overflow.
- Visual artifacts are stored outside git in `artifacts/codex/static-design-system-desktop-parity-20260717/` (`action-panel-states.png`, `poster-preview-runtime.png`, `search-states.png`, `desktop-eval.json`).
- This is a noindex preview-only publication. It does not promote the build to the stable production root.

## Incident gates

- `INC-2026-07-02`: code-level first-page skeleton regression fixed and catalog states added; incident remains open because live production auth/search for all three named queries and backend audit evidence were not executed in this preview-only task.
- `INC-2026-07-15-static-desktop-template-regression`: source identity and full 303-page desktop contract pass; the public catalog received a desktop Playwright inspection, while the full browser/real-device release matrix remains pending.
- `INC-2026-07-16-static-event-media-action-regressions`: accepted v10 source/tests integrated; targeted phone/medallion/personal-feed regressions pass.
