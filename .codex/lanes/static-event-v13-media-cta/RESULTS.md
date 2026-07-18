# Lane results: static-event-v13-media-cta

- Lane: `static-event-v13-media-cta`
- Requirements: `R02`, `R03`, `R04`
- Base SHA: `fb5a35ddec00157692d75e1610c8fd43f46b4c1e`
- Implementation SHA: `762758ee9c7bb8e5c38b9967a538868da24ba8cf`
- Branch: `agent/static-event-v13/media-cta`
- Push/deploy: not performed

## Outcome

- Split desktop CTA now measures the comfortable one-row layout, synchronously collapses the secondary calendar utility to its icon, remeasures, and stacks only when the compact row still cannot fit.
- Primary labels participate in wrap/clip/overflow admission and use non-wrapping presentation; registration fixture 6811 is frozen as a Split acceptance specimen.
- Free, one-day, `ticket.kind=free` events without an action URL render an actionable primary `В календарь` link and omit the duplicate secondary calendar control. Event 6901 is frozen; a synthetic event-6959 semantic twin covers the same branch because 6959 is absent from the current preview/frozen catalogs.
- Editorial remains explicitly stacked/wide-photo; production media presentation code was not changed and the role-first/classified-event-photo boundaries remain covered by focused tests. Event 6592 semantic repair was not touched.
- Added frozen lab routes and an expanded Playwright geometry checker for registration and free calendar-primary cases.

## Validation

Passed:

```text
cd site && node --test tests/desktop-event-cta.test.mjs tests/event-media-quality.test.mjs tests/event-detail-runtime-regressions.test.mjs
# 22 tests, 22 passed, 0 failed
```

```text
cd site && npm run build:preview
# 382 pages built; both new CTA lab routes generated
# Preview build IDs exercised: preview-20260718t215048-fb5a35dd and preview-20260718t215431-fb5a35dd
```

```text
git diff --check
# clean
```

Playwright evidence at CSS viewport `1536x864`:

- First local gate: the existing phone Split specimen passed compact inline; registration exposed a clipped primary label under the old admission timing.
- Second local gate after the refit hook: an expanding calendar-label CSS transition exposed an intermediate width and caused a false comfortable admission. Targeted CSS Transitions/CSSOM research confirmed that transitions expose current/intermediate computed values to geometry reads.
- Final patch disables width/max-width transitions on the measured calendar utility and orders `data-action-fit=measuring` before density mutation. Per integrator direction, the long preview rebuild/Playwright cycle was not repeated after this final source-only fix.

## Changed files

- `site/src/components/DesktopEventActionPanel.astro`
- `site/src/components/DesktopEventPage.astro`
- `site/src/data/desktop-event-examples.json`
- `site/src/pages/lab/event-desktop/examples/[scenario].astro`
- `site/src/pages/lab/event-desktop/index.astro`
- `site/scripts/check-desktop-cta-geometry-playwright.sh`
- `site/tests/desktop-event-cta.test.mjs`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `.codex/lanes/static-event-v13-media-cta/RESULTS.md`

## Risks / integration notes

1. Integrator must rebuild the candidate and rerun `npm run check:desktop-cta-geometry` (or the shell checker with `STATIC_SITE_REVIEW_BASE_URL`) to close final browser geometry after the transition fix.
2. Event 6959 is not present in this branch's generated preview or frozen fixture data. The behavior is general and Node-covered with a 6959 semantic clone; add/verify the actual event only in the root-owned semantic-data lane.
3. The new lab routes are present in preview builds. This lane intentionally did not edit secret-candidate retention/static-release files; the integrator decides whether candidate pruning must retain the two routes.
