# Lane transport_design results

## Scope

- Lane: `transport_design`
- Requirements: `R01`, `R03`, `R06`
- Base SHA: `2fda48d8ba1fb8cda13878a2e9fb726c984eb0f3`
- Implementation head SHA: `68565f2b` (`fix(static-site): restore transport and CTA fidelity`)
- Branch: `agent/static-event-v12/transport-design`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-event-v12-transport-design`

## Result

- R01: implemented all three accepted KAUP timetable readings as complete journey treatments. Each arm owns the shared route badge/headline and its arm-specific last-mile/warning presentation. A is the numbered departure board and resilient no-JS/off fallback; B is the green stop-line reading with tinted walk/warning bands; C is the dark-green nearest-departure card plus queue. Assignment/controller attributes, real trip data, QA forcing, action telemetry, and fallback behavior remain present. The deterministic desktop lab fixture `editorial-ocr-companion-arrival` explicitly renders transport even after event 4671 leaves current public catalogs. No prototype strength/risk copy is rendered, and journey copy uses `на Кауп`.
- R03: replaced the mechanical footer prompt with `Понравились Анонсы? Поделитесь`, using `AnnouncementsWordmark.astro` inline at `1em` height with an exact accessible label/screen-reader string and unchanged share controls/controller.
- R06: removed the phone-only compact top-row override. Production `DesktopEventActionPanel` now enforces admission, primary CTA, then a separate calendar/share/like row in that order. The phone CTA remains reveal-and-copy. Event 6551 now has the same 227px action-panel structure as ticket event 5374. `EventCtaPanel` and two desktop lab fixtures encode the same invariant.

## Visual and geometry evidence

Accepted Telegram references inspected directly:

- `telegram-262.jpg` (departure board)
- `telegram-263.jpg` (route strips)
- `telegram-264.jpg` (nearest departure + queue)

Forced QA arms on the stable lab fixture at 1536px:

- `departure_board_v1`: assigned/rendered/visible all match; badge `119`; heading `Автобус до Романово`; 2 real trips; last-mile and warning are inside the visible arm.
- `route_strips_v1`: assigned/rendered/visible all match; heading `Северный → Кауп`; green route line; walk background `rgb(228,239,236)`; warning background `rgb(255,240,210)`; both alerts inside the visible arm.
- `next_departure_queue_v1`: assigned/rendered/visible all match; heading `Из Калининграда`; nearest card background `rgb(23,102,83)`; 2 real trips; white primary times and three-stop timeline; both alerts inside the visible arm.

Mobile geometry at `390×844` for all three forced arms:

- root width `358px`, treatment width `312.84px`;
- treatment scroll width equals treatment width;
- `documentElement.scrollWidth <= innerWidth` for every arm.

Production CTA geometry (Playwright/Chromium):

- Event 6551 at `1536×864`: panel `475.5×227.1`, primary bottom `617.4`, action row top `631.2`, row bottom `687.2`, panel bottom `709.6`, bottom padding `22.4`; row is below primary and remains inside padding; order `calendar, share, like`.
- Event 6551 at `1366×864`: panel `421.1×227.1`; row below primary and inside `22.4px` bottom padding; order unchanged.
- Event 6551 at `1920×1080`: panel `544×231.5`; row below primary and inside `22.4px` bottom padding; order unchanged.
- Event 5374 at `1536×864`: panel `475.5×227.1`; primary bottom `698.2`, action row top `712.0`; order `calendar, share, like`.
- Share wordmark at mobile width: accessible label exact, rendered `74.3×14.4px` for a `14.4px` prompt font, CSS transform `none`.

## Commands and tests

- `uv run --with pytest==8.1.1 pytest -q --noconftest tests/test_static_site_transport_experiment.py` — **10 passed**.
- `node --test tests/event-detail-runtime-regressions.test.mjs` — **7 passed** after the full build.
- `npm run build` (from `site/`) — **passed**, Astro built 377 pages.
- Targeted `curl` dev-route compilation for production 6551/5374, production KAUP 4671, CTA lab fixtures, and the stable KAUP lab fixture — all HTTP 200.
- Playwright/Chromium forced-arm and DOM-geometry probes described above — **passed**.
- `npm run check:production-desktop` — **one unrelated pre-existing routing mismatch**: event 5756 routed `editorial/editorial-replaces-non-identity-document-with-classified-photo`, while its checked expectation is `split/split-resolution-constrained-landscape`. KAUP transport and CTA checks did not report failures. This lane did not edit media routing/data or the checker.
- `git diff --check` — **passed** before implementation commit.

## Changed files

- `site/src/components/DesktopEventActionPanel.astro`
- `site/src/components/EventCtaPanel.astro`
- `site/src/components/KaupTransportSchedule.astro`
- `site/src/components/ServiceShareAction.astro`
- `site/src/components/transport/DepartureBoardTimetable.astro`
- `site/src/components/transport/NextDepartureQueueTimetable.astro`
- `site/src/components/transport/RouteStripsTimetable.astro`
- `site/src/components/transport/TransportIcon.astro` (removed; consolidated on shared `Icon.astro`)
- `site/src/components/transport/TransportJourneyAlerts.astro`
- `site/src/components/transport/TransportRouteHeading.astro`
- `site/src/components/transport/TransportTimetableExperiment.astro`
- `site/src/pages/lab/event-desktop/examples/[scenario].astro`
- `site/src/pages/lab/event-desktop/index.astro`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `tests/test_static_site_transport_experiment.py`
- `.codex/lanes/transport_design/RESULTS.md`

## Risks and merge notes

- The full production desktop checker has the unrelated event-5756 routing expectation failure above; do not "fix" it in this lane because media routing/data/checker files are forbidden here.
- Canonical docs and `CHANGELOG.md` were explicitly forbidden in this lane. The integration owner must add the consolidated documentation/changelog entries.
- Merge the implementation commit and this results-only commit together. No push was performed.
