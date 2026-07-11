# Static event-page release UI contract

> Status: **release baseline pending product/design sign-off**.

## Purpose

This is the single current UI acceptance contract. Historical hero/date/decision/onboarding labs remain research evidence and must not silently redefine the release baseline.

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
- product/design owner signs off exact branch/SHA and open deviations.

## Branch rule

`feature/event-page-ux-lab-v3-20260710` is not mergeable as a release branch because its history mixes F17, Smart Update incident fixes, medallions/assets and generated preview data. After F11/F17 integration decisions, manually port/reimplement only the chosen UX/onboarding changes on a fresh main-based branch. Generated preview manifests are build evidence, not feature source.
