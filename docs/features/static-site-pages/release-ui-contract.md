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

## Share the service itself (F18)

F18 is separate from event sharing. The current desktop-focus v11-derived test
slice intentionally renders one adaptive `ServiceShareAction` only in the common
footer. Mobile uses system Share; desktop uses clipboard; both consume one
manifest, canonical `https://kenigevents.ru/` URL, copy source and analytics family.

The full release contract still requires reuse of the same component in the
navigation shell (under the expanded mobile brand tag). That placement is
**Deferred until V12** and the footer-only preview must not be described as full
F18 closure. Desktop D0 «Скопировать ссылку» remains default; D1/D2
«Скопировать карточку» remain research modes until native Windows/macOS evidence
and owner decision. Exact behavior, claims, daily card pipeline and gates are in
[service sharing](service-sharing.md).

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
