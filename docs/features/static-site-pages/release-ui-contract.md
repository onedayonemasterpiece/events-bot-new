# Static event-page release UI contract

> Status: **design-system baseline implemented; immutable RC preview and product/design sign-off pending**.

## Purpose

This is the single current UI acceptance contract. Historical hero/date/decision/onboarding labs remain research evidence and must not silently redefine the release baseline.

The canonical component catalog is [the static-site design system](design-system/README.md), rendered at `/lab/design-system/`. Release sign-off must name an immutable preview build id and git SHA and must include that route; screenshots or approval of one event page are not sufficient.

## Design-system gate

- shared foundations come only from `site/src/styles/design-system.css`;
- approved primitives come from `site/src/components/design-system/`;
- approved product components have a visible registry row with runtime source and state coverage;
- every registered component exposes an integer version; material redesigns add `vN+1`, keep `vN` as visibly deprecated with a replacement relation, and include a complete production-consumer migration;
- any new pattern starts as an explicit lab/candidate and is promoted only after approval;
- page-local forks of approved buttons, fields, badges, cards, feedback states or geometry block release;
- mixed component versions without a named feature flag, consumer list, owner and removal deadline block release;
- `npm run check:design-system` and the generated catalog assertions in `npm run check:preview` pass on the RC SHA.

## Required surfaces

- root/index and navigation;
- today, tomorrow, weekend and relevant category listings;
- event detail and gallery;
- related/search/personal feed cards;
- favorite/calendar/share/not-interested actions;
- auth/search state and anonymous fallback;
- personal page, transport, discussion signals and admin report when included in launch scope;
- empty, loading, degraded, cancelled/rescheduled and stale-data states.

## Acceptance matrix

- 375px mobile, 768px tablet, 1366/1440px desktop;
- no horizontal overflow or nested interactive controls;
- keyboard/focus/accessible names and contrast;
- reduced-motion and no-JS behavior;
- slow network and unavailable optional backend;
- real Android/iOS browser checks for auth, calendar and share;
- visual baselines tied to one immutable preview build id;
- `/lab/design-system/` reviewed at the acceptance widths with default/hover/focus/pressed/loading/disabled/error/degraded states;
- product/design owner signs off exact branch/SHA and open deviations.

## Current component decisions

- `EventCard split-actions` is the `v2` release baseline; production consumer migration is complete.
- `EventCard overlay-controls` is deprecated `v1`, replaced by `EventCard v2`, and retained only in the catalog for regression comparison.
- Hero families remain governed by `/lab/hero/`; the default used by real pages is also visible in the design-system catalog.
- `Button`, `Badge`, `Field` and `StatePanel` are approved primitives. New visual variants require a registry/state/check update rather than local CSS.
- `DesktopEventPage@14` is the accepted production desktop event shell; `/sobytiya/[slug]` must mount it directly. Its poster/preview families are reviewed in `/lab/design-system/` and `/lab/event-desktop/`, not represented by a parallel mock.
- `DesktopEventActionPanel@2` is the graphite CTA baseline. `EventCtaPanel@1` is deprecated for desktop-v14 and has zero production consumers.
- `AuthorizedEventSearch@2` owns the Yandex-auth search form and in-button progress. `Skeleton@1` owns first-page content loading for search and personal feed; spinner panels and forced-visible hidden runtime blocks are release blockers.
- The machine registry `site/src/data/design-system-registry.json` must match visible catalog rows, source imports, state fixtures and production consumer inventory.

## Branch rule

`feature/event-page-ux-lab-v3-20260710` is not mergeable as a release branch because its history mixes F17, Smart Update incident fixes, medallions/assets and generated preview data. After F11/F17 integration decisions, manually port/reimplement only the chosen UX/onboarding changes on a fresh main-based branch. Generated preview manifests are build evidence, not feature source.
