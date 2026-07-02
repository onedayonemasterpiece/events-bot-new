# INC-2026-06-13 Kraftmarket Promoted Events Dropped As Zero Events

Status: open
Severity: sev1
Service: Telegram Monitoring, Smart Update import, promo campaigns
Opened: 2026-06-13
Closed: —
Owners: engineering
Related incidents: `INC-2026-06-13-kantata-education-promo-id-only-design`, `INC-2026-06-08-festival-vk-aggregate-regression`, `INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss`, `INC-2026-06-12-raffle-source-publication-false-skip`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/promo-campaigns/README.md`, `docs/backlog/features/festival-monitoring-debt/README.md`, `tests/replays/INC-2026-06-13-kraftmarket-promo-zero-events/sources.json`

## Summary

Two `@kraftmarket39` posts that belong to active promoted festival inventory were scanned in production but discarded by Telegram Monitoring with `producer_zero_events:clear_event_signals`. The event for `80 историй о главном` (`kraftmarket39/285`) and the education-program event for `Кантата` (`kraftmarket39/287`) therefore had no `event` / `event_source` rows and could not be picked up by the active promo campaign layer.

## User / Business Impact

- Promoted events were absent from the public event inventory and from promo campaign candidate pools.
- The failure happened before campaign matching, so festival-target and event-target promo surfaces could not compensate.
- The `Кантата` educational programme campaign exposed an additional product-design defect: a live programme was partially modelled through fixed event ids, so newly imported programme events would still need manual campaign edits unless a dynamic programme target exists.

## Detection

- Detected manually by the operator on 2026-06-13 after the future-event audit found missing `https://t.me/kraftmarket39/285` and `https://t.me/kraftmarket39/287`.
- Production DB evidence showed `telegram_scanned_message.status='skipped'`, `events_extracted=0`, `events_imported=0`, `error='producer_zero_events:clear_event_signals'` for both messages.
- Production runtime logs showed `tg_monitor: producer_zero_events source=kraftmarket39 message_id=285` at 2026-06-13 01:54 UTC and the same for message `287` at 2026-06-13 02:04 UTC.

## Timeline

- 2026-06-12 10:58 UTC: `@kraftmarket39/285` posted a transferred lecture for `80 историй о главном`: 2026-06-19 18:30, Историко-художественный музей, Клиническая 21, free by registration.
- 2026-06-12 16:14 UTC: `@kraftmarket39/287` posted `Бородин. Гениальный дилетант` for `Кантата`: 2026-06-15 12:45, филиал Третьяковской галереи, Парадная наб. 3, free by registration.
- 2026-06-13 01:54 UTC: production Telegram Monitoring scanned message `285` and recorded `producer_zero_events:clear_event_signals`.
- 2026-06-13 02:04 UTC: production Telegram Monitoring scanned message `287` and recorded `producer_zero_events:clear_event_signals`.
- 2026-06-13 UTC: operator reported the missing promoted events as a red production incident.
- 2026-06-13 UTC: hotfix branch `hotfix/INC-2026-06-13-kraftmarket-promo-zero-events` opened from clean `origin/main`.

## Root Cause

1. Telegram Monitoring could still return `events=[]` for short festival/promo-campaign source posts that had clear event structure: title, future date, time, venue, and registration/ticket evidence.
2. Promo/congratulatory/giveaway framing was treated too strongly as a skip signal. The deterministic layer could bypass LLM extraction instead of only routing clear event-shaped posts into an LLM decision.
3. The extraction prompt did not make festival/campaign anchors a closure contract. A `Кантата` or `80 историй о главном` source could be parsed as a generic lecture and lose the `event.festival` value needed by downstream promo campaigns.
4. The promo campaign design allowed a live educational programme to be represented by fixed `event_id` lists. That is safe only for a closed, already-audited set; it is unsafe while programme events are still being imported.

## Contributing Factors

- Prior incident coverage existed for clear single-event false negatives and raffle publication skips, but not for transfer/promo-wrapper posts inside active promoted festival inventory.
- The emergency `@kraftmarket39` single-source import path existed, but the producer regression was not pinned with the current campaign anchors.
- Promo-campaign docs did not explicitly separate campaign eligibility from per-surface publication curation.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompts, skip decisions, or single-event rescue logic;
- changing Telegram result import/reprocess handling for `producer_zero_events`;
- changing promo campaign target modelling for live festival/educational programmes;
- changing `@kraftmarket39`, `Кантата`, or `80 историй о главном` source/festival handling.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `docs/features/promo-campaigns/README.md`
- `docs/backlog/features/festival-monitoring-debt/README.md`
- production Fly deploy path and Kaggle Telegram Monitoring execution
- active promo campaigns for `80 историй о главном` and `Кантата`

### Mandatory checks before closure or deploy

- `tests/test_tg_monitor_gemma4_contract.py` must prove clear `@kraftmarket39/285` and `/287` shapes stay in the LLM extraction path and require festival anchors.
- `tests/test_tg_monitor_reprocess_incomplete_scan.py` must prove old `producer_zero_events` rows are reprocessed when a fixed producer payload contains valid events.
- `tests/test_promo.py` must retain the live-programme rule: no `event_id`-only promo campaign for an open festival/educational programme.
- Production rerun must import `@kraftmarket39/285` and `/287` through the normal Telegram Monitoring + Smart Update path.
- Production verification must show event rows with correct title, date, time, real venue/address, original registration link, and `event.festival` values `80 историй о главном` and `Кантата`.
- Promo verification must show the newly imported events are eligible through dynamic campaign anchors, not only through a frozen event-id list.

### Required evidence

- deployed SHA and branch;
- targeted pytest output;
- Fly deploy evidence and `/healthz`;
- production `telegram_scanned_message`, `event_source`, `event`, and promo-target evidence for messages `285` and `287`;
- runtime log lines for the repeat import showing no `producer_zero_events` for those messages.

## Immediate Mitigation

- 2026-06-13 UTC: LLM-first hotfix prepared on clean branch `hotfix/INC-2026-06-13-kraftmarket-promo-zero-events`.
- 2026-06-13 UTC: Kaggle notebook entrypoint was regenerated from `kaggle/TelegramMonitor/telegram_monitor.py` so the producer changes are deployable.
- Pending: deploy the hotfix and rerun `@kraftmarket39` import through the production UI/E2E path.
- Pending: verify both promoted events are present and campaign-eligible in production.

## Corrective Actions

- Harden Telegram Monitoring prompt/rescue rules so transfer notices and promo/giveaway-result wrappers with concrete future event facts are routed to LLM extraction instead of skipped.
- Require festival/campaign anchor extraction for `Кантата`, `80 историй о главном`, and `kgd80.ru` contexts.
- Add replay fixtures for `kraftmarket39/285` and `/287`.
- Document and test the promo-design rule: live festival/program campaigns cannot be `event.id`-only.

## Follow-up Actions

- [ ] Add an alert/report for active promo campaign targets whose source messages are scanned as `producer_zero_events`.
- [ ] Convert the `Кантата` educational programme campaign from fixed event-id-only eligibility to a dynamic `festival=Кантата` + education segment target, keeping event ids only for curated publication subsets. Tracked in `INC-2026-06-13-kantata-education-promo-id-only-design`.
- [ ] Add E2E coverage proving a newly imported `Кантата` programme event becomes promo-eligible without editing campaign event ids.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `.venv/bin/python -m pytest tests/test_tg_monitor_gemma4_contract.py tests/test_tg_monitor_reprocess_incomplete_scan.py -q` -> `36 passed`
  - `.venv/bin/python -m pytest tests/test_promo.py::test_promo_docs_forbid_event_id_only_live_programme_campaigns -q` -> `1 passed`
  - `.venv/bin/python -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/handlers.py source_parsing/telegram/service.py promo.py` -> ok
  - `git diff --check` -> ok
- post-deploy verification: pending

## Prevention

- The hotfix makes the LLM-first producer responsible for event-shaped transfer/promo-wrapper posts instead of letting deterministic skip rules discard them.
- The incident replay and prompt-contract tests pin both failed posts and their downstream campaign anchors.
- Promo/festival docs now reject ID-only modelling for live programmes, so future campaign design must preserve dynamic eligibility.
