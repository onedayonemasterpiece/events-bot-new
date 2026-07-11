# Lane L4-integrator Results

## Status
committed

## Requirement IDs
- R02
- R08
- R09

## Branch / worktree / base
- `integration/event-page-desktop-variants-20260711`
- `/home/dev/.codex/worktrees/events-bot-new/event-page-desktop-v1`
- base `e9966bb1`
- head `869bd0b7`

## Files changed
- Event CTA/calendar/onboarding/media export components and checks.
- `/lab/event-desktop/` plus reusable six-option prototype component.
- Canonical static-site docs, env example, CHANGELOG and exporter regression test.

## Commands / verification
- Fresh Fly snapshot export: 312 active/future events through id 6832.
- Astro: 336 pages; `check:preview` passed.
- Manual exporter QA passed; Python files compile.
- Local Chromium Playwright: 4/4.
- Public HTTP: lab/index/search/representative events/stable ICS all 200.
- Public Chromium Playwright: 4/4.

## Risks / merge notes
- The six desktop options are a noindex choice lab. No option changes the default desktop layout until product selection.
- Default mobile decision-sheet behavior is preserved; only requested CTA/icon/onboarding/media corrections are behind the existing trial build flag where applicable.
