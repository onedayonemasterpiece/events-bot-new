# INC-2026-05-05 80 Stories Source Coverage Gap

Status: mitigated
Severity: sev2
Service: Source ingestion / festival coverage
Opened: 2026-05-05
Closed: —
Owners: events-bot
Related incidents: `INC-2026-04-27-tg-monitoring-sticky-skipped-post`, `INC-2026-04-30-tg-monitoring-work-schedule-false-skips`, `INC-2026-04-28-vk-smart-update-false-skips`
Related docs: `docs/features/festivals/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`

## Summary

The festival `80 историй о главном` was underrepresented in production. A public `@kraftmarket39` ledger found concrete festival event announcements that were imported only partially: some were `done`, some were `skipped` despite extracted payloads, and some had no durable `telegram_scanned_message` row in the inspected snapshot. The live VK post `https://vk.ru/wall-30777579_15138` was passed by the crawl cursor but absent from `vk_inbox`.

## User / Business Impact

- Concrete future festival events were missing from the event base and therefore from daily/month/video inventories.
- The loss was not only "whole festival monitoring is off": individual event announcements from festival sources were also weakly accumulated.
- Operators could not reconstruct the issue from DB alone because some Telegram skipped rows lacked source text/diagnostics.

## Detection

- Reported by the user on 2026-05-05.
- Evidence: production snapshot `artifacts/db/incident-80-stories-prod-snapshot.sqlite`, public Telegram ledger `artifacts/codex/80-stories-kraftmarket39-ledger/festival_posts_ledger.md`, live VK API reproduction for `wall-30777579_15138`.

## Timeline

- 2026-04-14..2026-04-30: several `@kraftmarket39` `80 историй` posts imported correctly (`/130`, `/182`, `/196`, `/199`, `/205`).
- 2026-04..2026-05: `@kraftmarket39/140`, `/193`, `/202` had extracted concrete events but were not imported; `/202` was `skipped_non_event:work_schedule`.
- 2026-05-05: VK post `wall-30777579_15138` announced the 2026-05-16 lecture `Заводы и пароходы`; the crawl cursor advanced past it without `vk_inbox` row.
- 2026-05-05: mitigation added LLM-first fail-open for event-like VK posts with uncertain timestamp hints, preserved Russian month-name dates through phone normalization, and routed festival program-like Telegram posts through the main LLM extractor instead of immediate schedule suppression.

## Root Cause

1. `vk_intake.normalize_phone_candidates()` could mask month-name dates like `16 мая 2026 г. в 16:00`, making `extract_event_ts_hint()` return `None`.
2. VK crawl treated keyword+date posts without a future timestamp hint as terminal rejects before the normal LLM import path could decide.
3. Telegram Monitoring's broad schedule/work-hours handling could suppress concrete festival program posts and individual festival lectures.
4. Official festival-site coverage is still not a durable source path; the current mitigation improves social-source accumulation but does not replace a future full program parser.

## Automation Contract

### Treat as regression guard when

- changing `vk_intake.py` crawl/date-hint/prefilter logic;
- changing Telegram Monitoring schedule extraction or festival prompt wording;
- changing festival queue or official-site ingestion.

### Affected surfaces

- `vk_intake.py`, `vk_auto_queue.py`, `vk_inbox`, `vk_crawl_cursor`;
- `kaggle/TelegramMonitor/telegram_monitor.py`;
- `source_parsing/telegram/handlers.py`, `telegram_scanned_message`;
- Smart Update non-event guards and festival context routing.

### Mandatory checks before closure or deploy

- `extract_event_ts_hint()` resolves the `wall-30777579_15138` date to `2026-05-16 16:00 Europe/Kaliningrad`.
- `@kraftmarket39/202` is not classified as `work_schedule`.
- `@kraftmarket39/140`, `/193`, `/202` replay through Smart Update as concrete events or merge into existing rows with `event_source`.
- Backfill creates/updates the missing future `80 историй о главном` events in production.

### Required evidence

- focused pytest output for the date and work-schedule regressions;
- Smart Update replay/backfill output for the offending sources;
- production SQL after backfill showing source URLs attached to event rows;
- deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- Added crawl-time LLM fail-open for VK posts with strong invite/registration/place signals when deterministic timestamp hints are uncertain.
- Preserved valid month-name dates inside phone-like text normalization.
- Let festival program-like Telegram messages go through the main LLM extraction path.

## Corrective Actions

- Fixed VK timestamp-hint loss and LLM-first rescue.
- Fixed Telegram festival-program prompt routing.
- Added regression tests for `wall-30777579_15138` date parsing and `@kraftmarket39/202` work-schedule false skip.

## Follow-up Actions

- [ ] Add official `kgd80.ru` program ingestion/backfill as a durable source path.
- [ ] Add durable source text/skip diagnostics for Telegram skipped rows.
- [ ] Add a coverage audit command for official festival program cards vs active event rows.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: pending full deploy/backfill
- post-deploy verification: —

## Prevention

Named high-priority festivals should have a source-coverage audit and a durable official source path, not depend only on opportunistic social reposts.
