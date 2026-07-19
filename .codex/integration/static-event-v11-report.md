# Static event V11 integration report

Branch: `integration/static-event-v11-transport-phone-carousel`

Build: `preview-20260717t-static-event-v11-phone-carousel`

## Requirement closure

| ID | Status | Outcome |
|---|---|---|
| R01 | Done (research/handoff) | Reviewed 120 Pinterest references plus authoritative transport standards, prepared three responsive alternatives, and obtained a completed Gemini 3.1 Pro (High) comparison. Recommendation: Alternative A as the scalable departure board, with one Alternative B route strip as shared/expanded context. The production KAUP component is intentionally unchanged until product choice. |
| R02 | Done | Restored the existing branded `Показать телефон` CTA. One click reveals the formatted number inside the same button, copies its normalized value and announces `Номер скопирован` without changing component geometry. |
| R03 | Done | Restored viewport-bounded `cover` for positive no-OCR photos, including event 5658, while OCR and classified documents remain `contain`. |
| R04 | Done | Added the real event-4783 vertical-series example with seven height-fit items, three visible in the first view, symmetric navigation and `7 из 12` disclosure. |

## Integrated lanes

- transport research: `d0e97a13` → `51f9c039`;
- phone CTA: `8b7438c5` → `afbbd1ec`;
- carousel contracts: `2f61ba4a` → `63e4c1b0`.

All lane worktrees finished cleanly. Root resolved only the shared CHANGELOG and
incident-record overlap when integrating the two implementation lanes.

## Research and external review

- Pinterest funnel: 12 queries, 120 candidates, 120/120 self-reviewed, keep 10 /
  maybe 8 / reject 102.
- Alternatives: dense departure board (A), repeated route strips (B), next trip
  plus queue (C), each checked at desktop and 390px.
- Authoritative references included TfWM timetable patterns, TfL line/bus-stop
  standards, Transport for Ireland information guidelines, Transit departure
  guidance and GTFS schedule practices.
- Gemini 3.1 Pro (High) result: `A > C > B`; preferred production composition is
  one shared route strip/legend plus a dense time table, 3–5 trips initially and
  `Показать все N рейсов` for the remainder. C remains unsuitable until reliable
  current-time/live semantics exist.
- Alternatives and verdict were sent to the existing Telegram UI-review topic as
  messages `261–265`; readback confirms topic `2` in chat `4337049383`.

## Local integration gates

- full build: `374` routes / `303` event pages;
- preview, production-desktop (`303`), rail and bus directory checks: pass;
- focused Node event-detail tests: `5/5` pass;
- phone CTA Playwright: `1366×768`, `1536×864`, `1920×1080`, `3/3`, no failures;
- event 5658 at `1536×864`: Editorial hero `1536×807`, computed `cover`, opened
  pending/visual-only fullscreen slide also `cover`, no overflow/errors;
- event 4783 portrait example at `1536×864`: seven items, three visible,
  `Фото 1–3 из 7`, `7 из 12`, no overflow/errors;
- event 5658 at `390×844`: `accepted-v8`, `cover`, zero overflow/errors;
- current-prefix mobile regression: `36/36` pages at both 320 and 390, five
  actual related transitions and poisoned stale-prefix cache rebasing, no
  failures;
- `git diff --check`: pass.

## Release state

Published immutable noindex preview:

`https://kenigevents.ru/preview-20260717t-static-event-v11-phone-carousel/__preview/`

- public HTTP returned `200` plus `noindex,nofollow,noarchive` for the index,
  phone event, wide-photo event and portrait-carousel example;
- public phone Playwright passed `3/3` desktop viewports;
- public wide-photo, portrait-series and accepted-mobile checks returned no
  failures or console errors;
- public current-prefix mobile gate passed `36/36` event pages at both 320 and
  390 plus five transitions and the poisoned-cache scenario;
- fixed-page links were delivered and read back in the existing Telegram topic
  as message `268`.

Production-root promotion remains blocked on explicit product acceptance. The
transport infographic remains a product choice rather than an implemented
change.
