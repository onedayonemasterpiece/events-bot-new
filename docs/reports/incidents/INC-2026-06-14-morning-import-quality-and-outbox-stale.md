# INC-2026-06-14 Morning Import Quality And Outbox Stale

Status: active
Severity: sev2
Service: Telegram Monitoring / Smart Update / VK and Telegram event fanout / Afisha Engagement CTA
Opened: 2026-06-14
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-12-future-event-quality-llm-first-repair`, `INC-2026-06-10-event-outbox-fanout-deadlock`, `INC-2026-06-14-afishaengagement-shadow-fallback-regression`, `INC-2026-06-13-vk-poster-text-datetime-conflict-and-duplicate-cta`
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The operator reviewed overnight and morning production imports on 2026-06-14 and found several public quality defects:

- VK `wall-231920894_3339` omitted `19:00` although the event poster carries the time.
- VK `wall-231920894_3334` / Telegram `@kldevents/499` used `location_name='Завтра'`.
- VK `wall-231920894_3336` and `wall-231920894_3337` were consecutive `Романтика города К` posts from one source with different dates; the second event's `vk_sync` job was later terminally marked `stale` and blocked Telegram publication.
- Telegram `@kldevents/498` displayed the settlement as `посёлке Железнодорожный` instead of canonical `Железнодорожный`.
- Telegram `@kldevents/500` had weak/generated description copy for `ХАУС2000`.
- Telegram `@kldevents/494` appeared to lack a visible VK counterpart despite `source_vk_post_url='https://vk.com/wall-231920894_3002'`.
- A CTA artifact was visible around Telegram `@kldevents/489`; production DB showed this was legacy public Afisha Engagement canary behavior from 2026-06-13, before the CTA/plain single-path hotfix.

## Root Cause

1. The Telegram handoff prose-location guard rejected long prose/person fragments but did not reject exact temporal/deictic values such as `Завтра`, allowing an LLM field placement error to persist as a venue.
2. Smart Update had the same last-line unsupported-location guard gap; if a temporal venue reached Smart Update directly, it could create a bad row unless another guard caught it.
3. Running event-pipeline jobs that exceeded runtime were marked `error/stale` with `next_run_at` 10 years in the future. For `vk_sync`, this turned a deploy/runtime interruption into a permanent dependency blocker for `tg_event_publish`.
4. Producer location-review did not treat inflected settlement phrases such as `посёлке Железнодорожный` as a suspicious city field requiring LLM repair; this must not be solved by runtime regex replacement.
5. `@kldevents/489` CTA exposure was not a new Telegram CTA rollout; it mapped to promo exposure `296`, campaign `10`, created 2026-06-13 with `publish_mode=public`, while ordinary `vk_sync` also created a separate managed VK post for the same event.

## Production Evidence

- Event `6003` (`https://t.me/barn_kaliningrad/1058`) had `location_name='Завтра'`, `location_address='Каштановая аллея 1а'`; source default for `barn_kaliningrad` is `Барн, Каштановая аллея 1а, Калининград`.
- Event `6006` had `joboutbox.vk_sync` `status='error'`, `last_error='stale'`, `next_run_at='2036-06-11 00:28:58.375266'`; dependent `tg_event_publish` stayed pending with `depends_on='telegraph_build:6006,tg_ics_post:6006,vk_sync:6006'`.
- Event `6008` had `location_name='Кирха Гердауэн'`, `location_address='Первомайская 1'`, `city='посёлке Железнодорожный'`; source text says the concert is in the settlement of Железнодорожный.
- Event `5972` had `promo_exposure.id=296`, `campaign_id=10`, `surface='afishaengagement'`, `publish_status='VK_SCHEDULED'`, `public_targets_json=[{"type":"vk_wall","url":"https://vk.com/wall-231920894_3180"}]`, while `vk_sync` result was `https://vk.com/wall-231920894_3238`.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring candidate handoff, default-location handling, location/city cleanup, or source/poster grounding;
- changing Smart Update unsupported-location gates or create/update acceptance;
- changing `JobOutbox` stale handling, dependency retry behavior, or event fanout order;
- changing Afisha Engagement CTA public/plain selection or legacy canary cleanup;
- repairing VK/TG event publication rows or stale managed post URLs.

### Must not regress

- Temporal/date words such as `Завтра`, `Сегодня`, `14 июня` must never be persisted as `location_name`; code may only trigger LLM review or fail closed/drop these fields. Source defaults/reference venues may be chosen by LLM review, not by runtime fallback.
- Event-pipeline `running` jobs (`telegraph_build`, `vk_sync`, `tg_event_publish`, `ics_publish`, `tg_ics_post`) must retry with bounded backoff after stale runtime expiry instead of becoming 10-year terminal blockers.
- Inflected city phrases (`посёлке Железнодорожный`) must trigger LLM venue-review or fail closed; runtime code must not rewrite them with regex replacement.
- CTA/plain VK publication must remain a single production decision; legacy rows that already have both CTA and plain posts must be treated as cleanup/reconciliation, not a desired steady state.

## Corrective Actions

- [x] Add temporal/date-fragment rejection to Telegram location prose guard.
- [x] Add the same temporal/date-fragment rejection to Smart Update unsupported-location guard.
- [x] Change stale event-pipeline jobs to retry with `BACKOFF_SCHEDULE` instead of freezing dependencies until 2036.
- [x] Add `Кирха Гердауэн, Первомайская 1, Железнодорожный` to the reference location list as passive canonical venue data.
- [x] Route inflected city phrases such as `посёлке Железнодорожный` to LLM venue-review instead of runtime regex replacement.
- [ ] Deploy hotfix and verify `/healthz`.
- [ ] Repair production rows for `6003`, `6006`, `6007`, `6008`, `5924`, and verify or repair `5932` VK visibility without creating duplicate VK posts.
- [ ] Re-check new VK postponed/event posts after deploy for public CTA/plain single-path behavior.

## Verification Plan

- Targeted tests:
  - `tests/test_tg_candidate_location_grounding.py::test_tg_build_candidate_drops_temporal_location_without_default_repair`
  - `tests/test_tg_monitor_gemma4_contract.py::test_tg_monitor_location_review_triggers_on_inflected_city_phrase`
  - `tests/test_smart_event_update_duplicate_guards.py::test_smart_update_rejects_temporal_location_candidate`
  - `tests/test_job_running_stale.py::test_running_vk_sync_stale_retries_instead_of_terminal_dependency_block`
- `py_compile` for touched modules.
- Production DB `PRAGMA quick_check` before/after data repair.
- Runtime-log evidence that stale `vk_sync` no longer creates permanent dependency skips.
- VK/TG public evidence for repaired rows, or explicit platform/API blockers.

## Current Verification

- `py_compile` passed for `source_parsing/telegram/handlers.py`, `smart_event_update.py`, `main.py`, `location_reference.py`, and touched tests.
- Full local pytest was blocked by local disk exhaustion during dependency installation (`No space left on device`); targeted tests are present but still need execution in an environment with dependencies installed.
