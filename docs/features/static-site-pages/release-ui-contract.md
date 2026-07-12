# Static event-page release UI contract

> Status: **release baseline pending selection; final product/design sign-off owner is the project owner/user who accepted the release plan on 2026-07-12**.

## Purpose

This is the single current UI acceptance contract. Historical hero/date/decision/onboarding labs remain research evidence and must not silently redefine the release baseline.

## Required surfaces

- root/index and navigation;
- today, tomorrow, weekend and relevant category listings;
- event detail, gallery and quick-read organizer/venue/festival medallion row;
- related/search/personal feed cards;
- favorite/calendar/share/not-interested actions;
- shared site-wide identity/account state and anonymous fallback on every static HTML page;
- personal page, transport, discussion signals and admin report when included in launch scope;
- empty, loading, degraded, cancelled/rescheduled and stale-data states.

## Global identity/account UI

The release baseline must include one shared compact identity control in the common HTML shell, not a search-only login block. Root, listings/categories/tags, event detail, search, personal-secret pages, transport-enabled pages and admin HTML surfaces all render the same controller and state vocabulary. Non-HTML artifacts are excluded.

Acceptance requires:

- anonymous menu offers equal **«Войти через Яндекс»** and **«Добавить почту»** paths;
- verified manual email acts as lightweight passwordless authorization after code/link proof, without requesting extra profile information;
- account menu consistently exposes masked identity, **«Выйти»** and, where applicable, **«Забыть почту на этом устройстве»** with non-destructive wording;
- callback/verification returns to the initiating clean URL and session state survives navigation/reload/new same-origin tab;
- pending, Yandex-without-email, expired and backend-degraded states stay understandable and never break static content/CTA;
- search, favorites, reminder and personalization surfaces consume the shared controller instead of creating their own auth stores or logout behavior;
- forwardable personal-secret pages remain accessible by secret link and do not bind the viewer's current account to the page owner merely because the shared account control is visible.

The detailed identity and action semantics live in [Site user identity](../site-user-identity/README.md#global-static-page-identity-shell-release-requirement).

## Event medallion acceptance

The frozen event-detail UI consumes the single medallion slice from draft PR [#38](https://github.com/onedayonemasterpiece/events-bot-new/pull/38); it must not copy assets/manifests from the mixed historical branches. Release acceptance requires the canonical [medallion P0 shortlist and RC gap gate](event-token-medallions.md#release-consolidation-and-remaining-shortlist), source-faithful artwork/provenance, bounded aliases, no duplicate identity tokens, loaded SVG/WebP+PNG assets, accessible labels and no overflow at mobile/desktop baselines. Listing/search-card medallion rows remain out of P0 unless separately approved.

## Acceptance matrix

- 375px mobile, 768px tablet, 1366/1440px desktop;
- no horizontal overflow or nested interactive controls;
- keyboard/focus/accessible names and contrast;
- reduced-motion and no-JS behavior;
- slow network and unavailable optional backend;
- real Android/iOS browser checks for Yandex login, email code/link login, logout, forget-email, calendar and share;
- visual baselines tied to one immutable preview build id;
- the project owner/user signs off the exact branch/SHA, immutable preview build id and any explicitly accepted deviations; no proxy or automated check may grant final UI approval.

## Branch rule

`feature/event-page-ux-lab-v3-20260710` is not mergeable as a release branch because its history mixes F17, Smart Update incident fixes, medallions/assets and generated preview data. After F11/F17 integration decisions, manually port/reimplement only the chosen UX/onboarding changes on a fresh main-based branch. Generated preview manifests are build evidence, not feature source.
