# INC-2026-06-14 Festival Recap / Logistics False Events

Status: mitigated (deployed; pending operator confirmation)
Severity: sev2
Service: Telegram Monitoring -> Smart Update -> managed VK/TG event fanout
Opened: 2026-06-14
Closed: —
Owners: engineering
Related incidents: `INC-2026-06-08-festival-vk-aggregate-regression`, `INC-2026-06-12-future-event-quality-llm-first-repair`
Related docs: `docs/llm/prompts.md`, `docs/features/smart-event-update/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/backlog/features/festival-monitoring-debt/README.md`

## Summary

Telegram Monitoring imported two non-event Telegram posts as active future events and the normal fanout created managed VK posts:

- `https://vk.com/wall-231920894_3307` / event `6000`: `t.me/garazhka_kld/1505` was a post-event recap and gratitude post. It only mentioned the next festival dates with `Локация уточняется`, but Smart Update created `Гаражка` on `2026-09-05..2026-09-06` with bogus location `спасибо!, Мира 9`.
- `https://vk.com/wall-231920894_3310` / event `6002`: `t.me/festdir/4673` was an operational notice for guests of a `2026-06-14` concert at `Понарт` about a changed entrance route. The extractor/Smart Update path created a fake `2026-09-04 14:00` `Кантата` concert even though the festival ends in June.

## User / Business Impact

- Subscribers could see incorrect managed VK event posts with false dates and weak/fabricated logistics.
- Pending Telegram event publication jobs could have sent the same false events to `@kldevents`.
- Festival/promo surfaces treated the false rows as real future inventory.

## Detection

- Detected manually by the operator from managed VK posts `wall-231920894_3307` and `wall-231920894_3310` on 2026-06-14.
- Runtime logs and production DB tied both VK posts to Telegram Monitoring source rows and Smart Update-created event ids.

## Timeline

- 2026-06-13 23:56:35 UTC: `garazhka_kld/1505` draft reached Smart Update as `title=Гаражка`, `date=2026-09-05`, `location=спасибо!`, `festival=Гаражка`.
- 2026-06-13 23:58:13 UTC: Smart Update created event `6000`.
- 2026-06-13 23:58:48 UTC: managed VK post `wall-231920894_3307` was created.
- 2026-06-14 00:07:07 UTC: `festdir/4673` draft reached Smart Update as `title=Концерт`, `date=2026-09-04`, `time=14:00`, `location=Понарт`, `festival=Кантата`.
- 2026-06-14 00:07:26 UTC: Smart Update created event `6002`.
- 2026-06-14 00:07:48 UTC: managed VK post `wall-231920894_3310` was created.
- 2026-06-14 UTC: operator reported both posts; investigation found source text, DB rows, joboutbox rows, and runtime-log evidence.
- 2026-06-14 00:25 UTC: production backup tables were created for affected event rows and related fanout/source/promo rows.
- 2026-06-14 00:25 UTC: managed VK posts `wall-231920894_3307` and `wall-231920894_3310` were deleted via `wall.delete`; follow-up `wall.getById` returned no posts.
- 2026-06-14 00:25 UTC: Telegram calendar post `https://t.me/kenigeventscalendar/6697` for event `6002` was deleted via Bot API.
- 2026-06-14 00:25 UTC: production DB rows for events `6000` and `6002`, related sources/facts/posters/joboutbox/promo exposure rows were removed; verification found zero remaining rows for those event ids.

## Root Cause

1. TelegramMonitor prompt had generic retrospective-report guidance, but did not explicitly forbid recap posts that only mention a next festival date while the location remains unknown.
2. Smart Update's completed-event guard treated `end_date` as a strong future-event signal and therefore did not catch the `Гаражка` recap once the extractor had already filled a date range.
3. TelegramMonitor prompt and Smart Update guards did not cover operational attendee logistics posts, such as changed entrance/navigation instructions for an already-announced event.
4. Telegram producer-side location repair was too permissive for weak extracted drafts. It inferred `спасибо!` from `Калининград, спасибо!` and could also match the real venue `Стендап клуб Локация` from the service phrase `Локация уточняется!`.
5. Downstream fanout trusted the created active rows and scheduled/created VK/TG jobs as designed.

## Contributing Factors

- Festival sources are currently high-value but not production-complete; see `docs/backlog/features/festival-monitoring-debt/README.md`.
- Afisha Engagement can amplify false future rows into public VK CTA cards once Smart Update creates them.
- The source posts contained real event/festival words, dates, and venue words, making them plausible to the extractor without sharper non-event instructions.
- Recent producer-side quality guardrails were not purely LLM-first: they were intended as deterministic safety-nets, but location recovery still repaired semantic fields after the LLM produced an invalid/weak draft instead of failing closed.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction prompts or rescue prompts;
- changing Smart Update non-event guards for VK/TG sources;
- changing festival-source handling, festival queue detection, or festival event fanout;
- changing managed VK/TG event publication from Smart Update rows.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `smart_event_update.py`
- `source_parsing/telegram/handlers.py`
- `schedule_event_update_tasks` fanout to `vk_sync` / `tg_event_publish`
- managed VK group `VK_EVENTS_GROUP_ID=231920894`
- production DB cleanup for events `6000` and `6002`

### Mandatory checks before closure or deploy

- Unit/replay tests proving `t.me/garazhka_kld/1505`-shaped recap returns `skipped_non_event:completed_event_report`.
- Unit/replay tests proving `t.me/festdir/4673`-shaped entry-route notice returns `skipped_non_event:event_logistics_notice`.
- Positive control proving a real future festival announcement with a grounded location is not skipped by the recap guard.
- TelegramMonitor prompt contract test includes both new instructions.
- Regression check for `INC-2026-06-08-festival-vk-aggregate-regression`: festival aggregate VK posts remain disabled by default and event-specific festival fanout remains possible for real events.
- Production mitigation deletes/neutralizes managed VK posts and pending fanout for false events `6000` and `6002`.

### Required evidence

- test command output;
- production DB/VK verification after cleanup;
- deployed SHA and Fly deploy evidence if code is deployed;
- confirmation that the deployed fix is reachable from `origin/main` before incident closure.

## Immediate Mitigation

- Created production backup tables:
  `incident_20260614_false_events_event`,
  `incident_20260614_false_events_event_source`,
  `incident_20260614_false_events_event_source_fact`,
  `incident_20260614_false_events_eventposter`,
  `incident_20260614_false_events_joboutbox`,
  `incident_20260614_false_events_promo_exposure`.
- Deleted managed VK posts `wall-231920894_3307` and `wall-231920894_3310` after verifying non-empty managed `vk_source_hash` rows.
- Deleted Telegram calendar asset post `https://t.me/kenigeventscalendar/6697`.
- Removed production DB rows for events `6000` and `6002` plus related fanout/source/fact/poster/promo rows.
- Verified after cleanup: VK `wall.getById` returned `[]`, event rows were absent, and related `joboutbox`, `event_source`, and `promo_exposure` counts were `0`.

## Corrective Actions

- Tighten TelegramMonitor prompt for:
  - recap + only `следующий фестиваль` + unknown location;
  - operational updates for people already attending an event.
- Add Smart Update safety-net guards:
  - allow `completed_event_report` skip even when the bad draft contains `end_date`, if the only future festival signal says location/place/address is unknown;
  - skip attendee logistics notices with entry/navigation/parking/queue/cloakroom instructions unless they are also full new event invitations.
- Tighten Telegram producer location repair:
  - do not infer venue names from city-salutation recap lines like `Калининград, спасибо!`;
  - strip `локация/место/адрес уточняется` lines before known-venue matching, so the `Стендап клуб Локация` alias cannot fire on an unknown-location placeholder.
- Add replay fixtures and tests for both bad cases plus a positive control.

## Follow-up Actions

- [ ] Add a broader Telegram Monitoring replay harness that reads `tests/replays/*/sources.json` and runs the same producer boundary before Smart Update.
- [ ] Add an audit for recent festival-source rows whose event date falls outside the known festival date range.
- [ ] Close or update the festival monitoring technical-debt gates after full festival queue E2E.

## Release And Closure Evidence

- deployed SHA: `cb19486c` (`fix(tg): skip festival recap false events`)
- deploy path: pushed `cb19486c` to `origin/main`, then `flyctl deploy --remote-only --app events-bot-new-wngqia`
  - Fly image: `registry.fly.io/events-bot-new-wngqia:deployment-01KV1RQ5Z0Z71TBYB5FWXQRB9H`
  - Fly machine: `48e42d5b714228`, version `1399`, health check passing
- regression checks:
  - `.venv/bin/python -m pytest tests/test_tg_candidate_location_grounding.py tests/test_smart_event_update_non_event_guards.py tests/test_tg_monitor_gemma4_contract.py -q` -> `75 passed in 4.83s`
  - `.venv/bin/python -m pytest tests/test_bot.py::test_festival_vk_sync_disabled_by_default -q` -> `1 passed in 2.12s`
  - `.venv/bin/python -m py_compile source_parsing/telegram/handlers.py smart_event_update.py kaggle/TelegramMonitor/telegram_monitor.py tests/test_tg_candidate_location_grounding.py tests/test_smart_event_update_non_event_guards.py tests/test_tg_monitor_gemma4_contract.py` -> passed
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` -> `ok=true`, `ready=true`, scheduler/tasks `ok`, no health issues.
  - Container source contains `_CITY_SALUTATION_LOCATION_RE`, `_UNKNOWN_LOCATION_LINE_RE`, `_EVENT_LOGISTICS_NOTICE_RE`, and `_COMPLETED_FESTIVAL_TEASER_UNCONFIRMED_RE`.
  - Production DB: `event` rows for `6000`/`6002` absent; related `joboutbox`, `event_source`, `event_source_fact`, `eventposter`, and `promo_exposure` counts are `0`.
  - Production backup tables remain present with counts: events `2`, sources `4`, source facts `22`, posters `7`, joboutbox `10`, promo exposure `2`.
  - VK API `wall.getById` for `-231920894_3307,-231920894_3310` returned `items=[]`.

## Prevention

- Keep these examples as replay fixtures.
- Keep LLM-first prompt rules and downstream Smart Update guards synchronized for high-risk non-event categories.
