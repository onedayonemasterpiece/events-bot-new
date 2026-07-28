# Focus-group live E2E execution matrix — 2026-07-28

| ID | Requirement | Dependency | Acceptance evidence |
|---|---|---|---|
| R01 | Public enrollment link and downloadable/scannable QR | Current focus invite contract | URL opens onboarding; QR decodes to same URL; mobile/desktop QA |
| R02 | Logout and repeatable enrollment as a focus-group user | Auth/session separation from membership | Logout changes global shell state; membership/re-enrollment follows documented contract; reset personalization does not delete membership |
| R03 | Full real E2E with `@kenigevents.ru` test mailbox | Yandex 360 admin authorization, mail delivery and live deployed/preview endpoint | Inbox receives confirmation; link can be used; Yandex/email/no-auth paths and logout verified in browser |
| R04 | Telegram feedback since the last handoff | Telegram E2E session and topic inventory | Every actionable comment classified and mapped to code/test/docs or explicit blocker |
| R05 | Release/incident regression safety | INC-2026-07-27 contract and release plan | Incident checks, static gates, build, browser desktop/mobile, docs/CHANGELOG |

## Telegram acceptance deltas

| ID | Telegram | Requirement | Acceptance evidence |
|---|---|---|---|
| R06 | #780, #787 | Stable scroll after reactions on paginated cards; fast future favorites hydration | Browser regression after `Показать ещё`; measured favorites ready state |
| R07 | #781, #782 | `Сегодня` never serves yesterday; calendar marks/permits only event dates across months | Clock/date fixture + mobile calendar browser checks |
| R08 | #785–786 | Search works for real queries and recovers without connection-error loop | Live/preview search queries with network evidence |
| R09 | #790, #791 | Free medallion sticky without opaque shelf; exhibitions grouped separately at end | Mobile visual/browser assertions and grouping contract |
| R10 | #783–784, #788–789 | Popular freshness and semantic selection/title/category correctness | Data provenance and LLM-first correction; selection tests |
| R11 | #792 | Restore confirmed clubs, never replace known data with dishonest empty state | Export/source provenance + rendered cards |
| R12 | #793 | Transport control event route exists in candidate | generated-output gate and HTTP 200 |
