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
