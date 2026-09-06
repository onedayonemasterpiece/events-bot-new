> Latest owner override: Hero begins at viewport top, header occupies no flow space. On mobile the scene reserves internal clearance for the shared brand/menu so its words do not run behind the navigation; the Hero background still begins at y=0. HomeSearchEntry v2 floating-link opens existing /poisk/; no inline field or mounted capture controller on Home. Content order is Hero → quick links → feed → page-end. Earlier inline capture remains a reusable compatibility variant, not the Home composition.

# Home composition: global navigation without context islands

Runtime owner: `EventLayout` v3 with `shellCompositionForRoute` /
`shell-composition-v1`. After the existing canonical-base normalization, `/`
selects `home-navigation-only`; other routes remain `contextual`.

Owner clarification (2026-09-06): global navigation is mandatory on Home.
Desktop retains the existing header navigation as a fixed upper island; mobile
retains the existing Reference4 menu/brand trigger. Only contextual title,
city and section participants are absent and their runtime is not invoked.
One shared MobileBottomNav remains. Hero-talk cannot enable context islands.
The common measured lower-stack owner suspends the dock for an open modal or
editable focus with at least 120px visual-viewport keyboard occupancy. Ordinary
focus/browser toolbar movement does not hide it. No separate home z-index owner
or second voice dock is created; native keyboard acceptance remains separate.

`HomePage` v2 orders `HomeHeroTalk` → `HomeSearchEntry` → `HomeQuickNav` →
`HomeColdStartFeed` → `HeroTalkPageEnd`. The eligible feed is not truncated by the
composition. Page padding reserves at least the existing lower-stack height,
including no-JS, and consumes the larger measured occupied offset when available.

`HomeQuickNav` v2 (`inline-routes`) uses Button v2 quiet/compact links in a wrapping
flow. Today, tomorrow, weekend and exhibitions remain real routes; free/unusual
use the shared collection navigation eligibility owner. Links use `withBase`.
No new geographic state or shelf is introduced; existing route-owned filters
retain their ownership.

## Hero-talk placements

`heroTalkPlacement.ts` owns `hero-talk-placement-v1`: typed semantic fragments,
route/placement context, capability readiness, completed action, hidden/suppressed
message/capability IDs, and exclusion of upper scene IDs. It only supplies service
content; it does not activate a campaign, invent a cap, count exposure, persist
another user profile, or replace the inert StandardOnboarding placement marker.

`HeroTalkPageEnd` v1 (`compact-service`, `service-continuation`/`suppressed`) is a
real compact section after the feed with one primary Button continuation. Home
uses “Подобрать точнее?” → Search, without asserting the person failed to choose.
Other routes must supply an explicit truthful typed next step; they are not
installed by this delivery. The resolver omits unavailable/suppressed/completed
or duplicate content. The home consumer reads the shared Search compile-time
capability. Its runtime follows the real HomeSearchEntry readiness/state and
suppresses the CTA during requesting/recording/saving/submitted or unavailable
storage/capability; sign-in alone does not remove a benign route-to-Search CTA.
`data-hero-talk-suppressed` is an in-memory consumer input, not a new durable store.

`HomeHeroTalk` v2 retains its editorial fragments, verified source photo mosaic,
finite timing and reduced-motion behavior unchanged. Empty/stale decks render
an additive `service-fallback` / `generic` state using the same typed service
model and an actual calendar link, without reviving stale events. Auto-content
research #642 and any later Hero redesign remain independent work.

## Shared card contribution port

The minimal Search#587 `KenigEventsSearchCardHost` port contributes candidates,
served-list identity and the existing action/profile store. Home grids use that
same port with `home_feed` identity; both `contextForCard` and `controllerForCard`
recognize them. The port does not rank/render cards. Feedback requests home
reconciliation through `KenigEventsHomeCardHost.sync('feedback')`, with no callback
from that reconciliation into the shared sync path.

## Focused checks and delivery boundary

- `node --experimental-strip-types --test site/tests/home-composition.test.ts`
- `node --experimental-strip-types site/tests/home-composition.playwright.mjs`
  tests the actual placement controller on an isolated DOM fixture (`mocked_ui`).
- Add `CHECK_BASE=<immutable base>` and `CHECK_OUTPUT=artifacts/codex/home-composition`
  for actual integrated route checks at 1440/1920/390/360, top/end screenshots,
  lower-only presence, flow order, route-prefix links and page-end separation.

Focused fixture results do not prove the published route, live voice/ASR,
authenticated personalization, native keyboard behavior or Penpot conformance.
The integration owner synchronizes registry/catalog/SoT, CHANGELOG, and immutable
preview acceptance for this delivery; no standalone component publication.

## Return to Home

Shared bottom “Афиша” links to the base-prefixed Home `/`, not Popular. Mobile Reference4 also exposes an explicit “Главная” link in addition to its linked brand. Popular remains a separate destination.

## Preview validation boundary

The 2026-09-06 all-pages candidate `da3ea046` built successfully but failed `check:unified-prototype`: the 300-event real slice contained no bus-navigation specimen. It was not published. Home review uses the existing bounded page-class rail (event/date/weekend/collection/personal/partner) and `check:preview-slice`; the full-site transportation gate is unchanged and remains unverified for this corpus. This is not production/full-site acceptance.

## Voice review 2026-09-06 22:01–22:08

Source: IdeaHub `0ad65c3725b88a2cd9791be7fc539708c0055740`, `inbox/voice/2026/09/voice-20260906-220155-703a0cf7.md` (full transcript, not inferred summary).

In progress: calendar 404; negative-feedback scroll stability; approved voice Search destination/icon; two-column rectangular quick links; honest feed label/badge; mobile Hero imagery/word timing; shared animated page-end; footer/lower-shadow consistency. Footer docking remains a proposed behavior, not silent acceptance.

Reproduced on published b7431426: event8339 calendar href points to CDN root `/8339.ics`, HTTP404. Negative feedback hides the card in a valid fresh local profile but changes scroll position; preserve reading position during feedback reconciliation. Preview calendars now reference packaged event.ics; download failures show an error rather than navigate to known failed URL. Actual OS calendar insertion cannot be confirmed by browser download alone.

Hero review implementation (not yet rendered acceptance): HomeHeroTalk v3 reveals words at 190ms intervals instead of animating an entire catalog-title fragment at once. Mobile media CSS and runtime no longer exclude mosaic; crop/face/upscale guards remain. HeroTalkPageEnd v2 reuses the same renderer with a separately seeded catalog deck excluding upper event IDs. Reduced-motion remains static. This supersedes the compact-only page-end target for Home.
