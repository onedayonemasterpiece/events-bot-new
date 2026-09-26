# Desktop voice review — 2026-09-26

Source: [IdeaHub voice packet](https://github.com/onedayonemasterpiece/idea-hub/blob/82163a569e9947d4d1268e146d1280807fe02c3e/inbox/voice/2026/09/voice-20260926-090623-08876172.md).

Scope: owner reviewed DESKTOP ONLY. Mobile has no owner sign-off; responsive browser checks only guard accidental regressions. This is an isolated UI preview, not production promotion or Penpot acceptance.

## Acceptance checklist

1. Single Hero Talk cursor; no Pause control.
2. Shared, calmer heading typography and weight.
3. Hero mosaic aligned to the top on desktop.
4. Page-end Hero: larger preceding gap, reduced following whitespace.
5. Same top navigation geometry, font, radii and hover across Home and listings.
6. No initial nav or city-island position jump.
7. Time context measured from actual time headings, not unrelated editorial headings.
8. Desktop Dates opens the existing calendar on every navigation-bearing page; exact dates resolve inside this build.
9. Contrasting Saturday/Sunday headers with shared typography.
10. Fresh production public projection (2026-09-26); no old July reference clock.
11. Continuous exhibitions canvas and standard illustration radii.
12. One sticky festival month switcher; no duplicate month context.
13. Versioned runtime DS, catalog comparison, complete consumer migration and checks.

## Version migration

EventLayout 3 → 4; HomeHeroTalk 3 → 4; HeroTalkPageEnd 2 → 3;
MobileDateAccessory 2 → 3; WeekendListingSurface 1 → 2;
ExhibitionsPersonalSurface 2 → 3; FestivalsTimelineRouteComposition 1 → 2.
The predecessor remains immutable at `preview-integrated-islands-home-20260926` and is deprecated for new review. Shared tokens own geometry; all source consumers move together. Candidate approval is pending owner review.

## Data provenance

Canonical immutable public projection `snapshot-ui-review-20260926`, created 2026-09-26T07:22:20.829736Z.
SHA-256: `e7d103511e8c22c2064ff724a0dfbc66f8b94ecdb85bbd2fc39f928e2af6b280`.
Size: 90,517,504 bytes; local hash and SQLite quick_check verified. Export uses the existing production exporter, full catalog, Kaliningrad clock, sparse related mode and no paid LLM calls.

## Verification

Local focused browser run PASS: 12 desktop route/viewport cases (1920 and 1440), delayed-module first-paint comparison, and 3 mobile viewport regressions (320/390/430). 22 focused unit checks, design-system primitives, family graph and token graph checks PASS. Public full-build checks pending. Evidence retained in `/home/dev/artifacts/events-bot-new/20260926T071729Z-voice-review-20260926`.

## Follow-up voice 09:40

[voice-20260926-094037-4582c258](https://github.com/onedayonemasterpiece/idea-hub/blob/2c30acba/inbox/voice/2026/09/voice-20260926-094037-4582c258.md) requires precise removal of ended festivals and suggests an optional historical list. The current exporter already excludes ended editions. Added a disjoint archived projection and a closed-by-default historical list with neutral/grayscale artwork and organizer source links. Fresh snapshot: 8 current/future, 13 archived, 21 total. Exact end date is inclusive; no speculative end date is invented for broad/unknown periods.

## Mobile extension — voice-20260926-100814-a927be2f

Owner added mobile review at 10:08 (IdeaHub immutable source
`f5a4fc542d8964a21197a44aedfb05a25be6a9e5`). Desktop-only scope above describes
its source review, not the scope of the combined successor candidate.

- [x] Shared HomeHeroTalk v4 candidate: mobile 8×8 square crop, top aligned, same
  Page End implementation; Pause already removed across breakpoints.
- [x] MobileListingRailSurface v3: SSR city row on Today/Tomorrow/Date, same
  heading and control geometry before/after hydration.
- [x] City contraction includes vertical displacement in one animation; no
  second sticky-top settling phase. Caption appears after geometry completes.
- [x] Exhibition canvas/header theme now applies to mobile too.
- [x] Shared Breadcrumbs suppresses mobile breadcrumbs and parent link.
- [x] ForMeRouteComposition v2 uses PersonalFeedSlot v3 with real server-rendered
  cards, eager same-origin feed, persistent local reactions, no consent dialog.
  Local storage failure keeps a useful static feed; reset remains available.
- [ ] Unusual nonempty delivery: blocked by the existing semantic qualification
  contract. Source contains `source-fallback-empty`, no accepted current BGE
  output. See INC-2026-08-01-unusual-feed-disabled-by-config. New snapshot is not
  a replacement for the required gold/cold/warm classifier receipt.
- [ ] Gastronomy nonempty delivery: current checked decisions are empty with
  `audit_status=incomplete` on both this branch and origin/main. Bound manifest
  fails `checked_audit_incomplete`; no current accepted snapshot found in bucket
  data/metadata/build prefixes. Do not fabricate membership/approval or use
  keyword classification. Both unavailable pages now offer a working Today link
  without internal pipeline terminology. These two data defects remain open.

Verification: `site/tests/mobile-review-20260926.playwright.mjs` runs L1 browser
checks at 320/390/430: both Hero placements, cities dock/expand and picker,
no-JS/JS geometry parity, exhibition canvas, breadcrumbs, immediate personal
feed, first like without consent and persistence after reload. It does not claim
native Android/iOS acceptance; no OS keyboard/share/install behavior changed.
The existing desktop suite remains a blocking regression check.

## Owner-requested stop checkpoint

Owner asked to retain only critical work and stop to conserve tokens. Runtime
commit `26868cfc4fabd0b91c8606cee2f98f9dee0bcb42` is pushed. Mobile L1 run passed
22 checks; desktop predecessor passed 16 checks. Combined r3 build was stopped
before publication. No r3 URL was sent to Telegram; combined public verification
and the two collection data repairs remain unfinished. Resume from this commit,
not from the interrupted local dist tree.
