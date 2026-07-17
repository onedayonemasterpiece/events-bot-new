# Integration Report — static-site transport/mobile real events 2026-07-15

- Integration branch: `integration/static-site-transport-mobile-real-events-20260715`
- Public build: `preview-20260715t-production-transport-mobile-real-events-v1`
- Implementation commit: `9d669856`
- Mode: one serial integrator because UI, generated manifests, scheduler and
  release prefix are coupled mutable state; Gemini 3.1 Pro (High) supplied the
  bounded responsive transport review.

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Accepted desktop/header + mobile v4 | Done | Shared production page; 1920 and 390 Playwright |
| R02 | Rail/bus on desktop and mobile | Done | Events 5789 and 6710, directory checks, public screenshots |
| R03 | Mass current-data generation | Done | 282 public future/ongoing events from 2026-07-15 18:55 Kaliningrad snapshot |
| R04 | Current related + Smart Update follow-up | Done | 282 anchors × 40, no dangling ids; focused debounce tests |
| R05 | Safe old-preview cleanup | Done | 19 exact pre-July prefixes, 1783 objects / 174481334 bytes |
| R06 | Public acceptance | Done | HTTP/Playwright/ICS/footer share/navigation checks |
| R07 | Docs, push and Telegram handoff | Done | Branch pushed; `KenigEvents · UI review` topic 37, messages 59–62 verified |

## Release boundary

The unique public preview is complete. The Fly enable flag and pgvector builder
settings are staged in this branch and activate only after merge/deploy.
Production-root promotion remains a separate gate.
