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
