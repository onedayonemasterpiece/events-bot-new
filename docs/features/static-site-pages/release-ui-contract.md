# Static event-page release UI contract

> Status: **release baseline pending selection; final product/design sign-off owner is the project owner/user who accepted the release plan on 2026-07-12**.

## Purpose

This is the single current UI acceptance contract. Historical hero/date/decision/onboarding labs remain research evidence and must not silently redefine the release baseline.

## Required surfaces

- root/index and responsive navigation with one shared information architecture;
- common mobile menu/footer service-share actions and their desktop copy-link variants;
- today, tomorrow, weekend and relevant category listings;
- event detail, gallery and quick-read organizer/venue/festival medallion row;
- related/search/personal feed cards;
- favorite/calendar/share/not-interested actions plus global `Моё избранное` count and `/izbrannoe/` complete list;
- shared site-wide identity/account state and anonymous fallback on every static HTML page;
- personal page, transport (including the optional gallery slide «Как добраться»), discussion signals and admin report when included in launch scope;
- empty, loading, degraded, cancelled/rescheduled and stale-data states.

## Preliminary homepage «Городской обзор» candidate

H1 is provisionally routed into release planning but is not yet part of the immutable UI baseline. Its [research contract](typed-briefing-hero-research.md) permits only a staged `Conditional Go`: categories-first control → static editorial briefing → semantic-fragment motion. Before this document can be frozen, the owner must explicitly choose `ship` or `defer` on an immutable preview.

The candidate may ship only if the first useful scene is already in SSG/no-JS HTML, search/categories and at least the beginning of the feed remain visible and independently usable, the briefing stays within its 320/360/390/1366/1440 viewport budgets, and mobile plus `prefers-reduced-motion` are static/manual rather than autoplay. Links have stable hitboxes, motion interruption creates zero CLS, the approved lockup is unchanged, and every number/time/event/popularity/personal claim is backed by a versioned deterministic fact and route token. Client/runtime LLM, invented urgency and a terminal/chat-assistant skin are out of scope.

If H1 is accepted, its branch/SHA, V1/V2 screenshots, accessibility/performance evidence and homepage experiment contract become part of F5 acceptance, and M4 SEO/GEO starts only after the resulting homepage is refrozen. If it is deferred, the standard categories/feed homepage remains the release baseline and H1 does not block F1–F18.

## Responsive navigation

The release direction is **adaptive consistency**, not pixel-identical mobile/desktop chrome: mobile keeps the stronger brand tag/disclosure pattern, while desktop keeps all primary destinations in a persistent horizontal header and carries only a shallow, restrained version of the tag motif. Labels, order, active state, search/account semantics and accessible names stay invariant across breakpoints and page families.

The research basis, A/B/control preview, cross-device task test and owner decision gate are canonical in [Responsive navigation decision](responsive-navigation.md). The recommended desktop candidate is the shallow hybrid; its final geometry still requires immutable-preview sign-off.

`Моё избранное` is one invariant navigation destination on mobile and desktop. After state restore its accessible badge is rendered only for distinct durable saved-event count `N>0`; likes, ICS downloads, reminder count and transport legs never inflate it. It opens the privacy-safe `/izbrannoe/` shell defined by [Favorites and calendar](../event-favorites-calendar/README.md#global-menu-and-saved-events-page).

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

## Share the service itself

F18 is separate from sharing an event. Every public HTML page family uses one common adaptive component with two placements: under the expanded mobile brand tag/navigation shell and in the footer. These are the same semantic action and shared payload across breakpoints, not separate mobile and desktop features: mobile invokes system share with the centrally prerendered service-card WebP, concise text and `https://kenigevents.ru/`, while desktop changes only the transport to clipboard and never invokes native share. The UI must not expose both transports at once or allow their copy/manifest/analytics semantics to drift. Until the required Windows/macOS matrix is complete, the accepted desktop baseline remains «Скопировать ссылку». The candidate «Скопировать карточку» (`image/png` + `text/html` + `text/plain` in one `ClipboardItem`) cannot enter the frozen UI merely from API support: actual paste results in Windows/macOS browsers and target apps must be measured first, and final acceptance covers mobile system share plus desktop clipboard on the same RC SHA.

The frozen UI must prove that these actions remain visible without competing with primary event/navigation CTAs, have consistent accessible names/focus/feedback and do not add the service image to event media. The asset is generated centrally from a catalog-bound metrics manifest, not by browser canvas or a per-click backend. Initial art uses the existing lettering/brand mark and a visible CTA; the historical poster-cube composition remains a future variant. Full copy, claim-evidence, CDN and fallback acceptance live in [Service sharing card](service-sharing.md); desktop selection and evidence live in [Windows/macOS clipboard research](service-sharing-desktop-clipboard-research.md).

## Optional gallery transport card

The UI-freeze task may prototype one generated non-photo gallery slide **«Как добраться»** after genuine event media. It is derived from the same validated F11 snapshot/selector as the normal transport block, fails closed for stale/unsupported data and never becomes hero/OG/JSON-LD event media. The full accessible schedule block remains canonical; the slide itself is optional and may be owner-deferred without removing F11. See [the gallery-card contract](../event-transport/gallery-how-to-get-there-card.md).

## Gallery image uniqueness

No public event gallery may show exact, mirror/re-encoded or visually redundant crop/minor-overlay copies of the same underlying image. RC acceptance requires zero confirmed duplicates and zero unreviewed clusters in the full eligible active/future inventory, using the canonical [event image duplicate audit](../../operations/event-image-duplicate-audit.md). Distinct posters/slides/photos must not be collapsed merely because they share a template; generated product cards are typed separately from source media.

## Event medallion acceptance

The frozen event-detail UI consumes the single medallion slice from draft PR [#38](https://github.com/onedayonemasterpiece/events-bot-new/pull/38); it must not copy assets/manifests from the mixed historical branches. Release acceptance requires the canonical [medallion P0 shortlist and RC gap gate](event-token-medallions.md#release-consolidation-and-remaining-shortlist), source-faithful artwork/provenance, bounded aliases, no duplicate identity tokens, loaded SVG/WebP+PNG assets, accessible labels and no overflow at mobile/desktop baselines. Listing/search-card medallion rows remain out of P0 unless separately approved.

## Acceptance matrix

- 375px mobile, 768px tablet, 1366/1440px desktop;
- no horizontal overflow or nested interactive controls;
- keyboard/focus/accessible names and contrast;
- reduced-motion and no-JS behavior;
- slow network and unavailable optional backend;
- real Android/iOS browser checks for Yandex login, email code/link login, logout, forget-email, calendar and share;
- mobile menu/footer service-card share and desktop menu/footer copy checks, including D0 text/link fallback and the owner-selected D1/D2 rich clipboard behavior after native Windows/macOS evidence;
- visual baselines tied to one immutable preview build id;
- an explicit H1 `ship|defer` record; if `ship`, categories-first/static/motion comparison, 320–1440 viewport captures, no-JS/reduced-motion/keyboard/low-end checks and downstream discovery instrumentation are bound to that same preview;
- the project owner/user signs off the exact branch/SHA, immutable preview build id and any explicitly accepted deviations; no proxy or automated check may grant final UI approval.

## Branch rule

`feature/event-page-ux-lab-v3-20260710` is not mergeable as a release branch because its history mixes F17, Smart Update incident fixes, medallions/assets and generated preview data. After F11/F17 integration decisions, manually port/reimplement only the chosen UX/onboarding changes on a fresh main-based branch. Generated preview manifests are build evidence, not feature source.
