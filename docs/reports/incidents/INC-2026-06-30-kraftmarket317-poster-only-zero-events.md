# INC-2026-06-30 kraftmarket317 poster-only zero-events miss

Status: closed
Severity: sev2
Service: Telegram Monitoring producer/importer, Smart Update event fanout
Opened: 2026-06-30
Closed: 2026-06-30
Owners: Codex / Telegram Monitoring maintainers
Related incidents: `INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss`, `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel`, `INC-2026-05-09-event-location-alias-free-dup-regressions`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Telegram Monitoring scanned `https://t.me/kraftmarket39/317`, a concrete poster-only announcement for a calligraphy masterclass at museum `Восток на Западе`, but production recorded it as `producer_zero_events:clear_event_signals` and created no `event` / `event_source` row.

The post has an empty Telegram caption; all event facts are in the poster OCR: title, date `1 июля`, time `19:00`, venue `музей «Восток на Западе», ул. Клиническая, 19А`, price `1000 рублей`, and phone registration.

## User / Business Impact

- A future attendable event for 2026-07-01 was absent from the event inventory and could not reach Telegram/VK/Telegraph surfaces.
- The source cursor advanced through message `317`, so the miss would not self-heal without forced replay.
- The venue was not in standard location references, causing spelling/address drift across existing museum events.

## Detection

- Operator provided `https://t.me/kraftmarket39/317` and asked whether it entered the DB.
- Authenticated Telegram inspection showed an empty-caption poster with event facts in the image.
- Production DB showed `telegram_scanned_message(source_id=1177,message_id=317)` as `status='skipped'`, `events_extracted=0`, `events_imported=0`, `error='producer_zero_events:clear_event_signals'`.
- Runtime file mirror showed `tg_monitor: producer_zero_events source=kraftmarket39 message_id=317` at `2026-06-30 00:12:58 UTC`.

## Timeline

- 2026-06-29 21:39:03 UTC — `@kraftmarket39/317` is published as a poster-only message.
- 2026-06-29 23:01:18 UTC — production `tg_monitoring` recovery import starts, `ops_run id=3109`, `run_id=58a16de34cca4af4af9df9923d533ea7`.
- 2026-06-30 00:12:58 UTC — server imports producer output and records message `317` as `producer_zero_events:clear_event_signals`.
- 2026-06-30 — investigation confirms no `event_source` / event row for `kraftmarket39/317`, while `kraftmarket39/306` and `/312` imported as museum events.
- 2026-06-30 — prevention patch prepared: OCR-only poster text is passed into the LLM extraction path instead of being dropped by the empty-caption guard; museum reference row/aliases added.
- 2026-06-30 16:07–16:10 UTC — forced targeted replay `ops_run id=3127` imports one event, `event_id=6524`.
- 2026-06-30 16:23–16:28 UTC — canonical repair corrects phone registration, preserves OCR source text, reruns standard `telegraph_build`, `vk_sync`, and `tg_event_publish`, deletes stale managed VK duplicate `wall-231920894_5232`, and rebuilds Telegraph after stale source cleanup.

## Root Cause

1. `kaggle/TelegramMonitor/telegram_monitor.py::extract_events()` initialized `content` from Telegram caption text only and returned `[]` before OCR text was considered when caption was empty or shorter than 10 characters.
2. The existing clear-single-event rescue was unreachable for poster-only posts because it runs after the early caption guard.
3. Server-side diagnostics correctly detected clear event signals from `posters[].ocr_text`, but diagnostics alone only marked the message skipped; it could not create an event without a producer payload.
4. `Музей «Восток на Западе»` was missing from `docs/reference/locations.md` / aliases, so even successful imports used drifted venue spellings.
5. After the first forced replay, downstream Telegram import preserved empty caption as empty `source_text` and evaluated group post-author fallback without OCR phone evidence. That allowed the post author's Telegram username to become a false `ticket_link`, while the poster's real phone booking contact stayed non-actionable.

## Contributing Factors

- Prior `kraftmarket39` incidents focused on text/caption extraction misses and TPM/provider failures; poster-only OCR as the primary text path had no regression fixture.
- Empty-caption poster posts are common enough for event channels, but the producer treated empty caption as empty source before OCR merge.
- Standard location references lagged the new museum opening.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/TelegramMonitor/telegram_monitor.py::extract_events`, OCR handling, single-event rescue, or clear-event structural detectors;
- changing server import handling for `producer_zero_events` rows;
- changing `docs/reference/locations.md` / `docs/reference/location-aliases.md` for museum venue normalization;
- running catch-up/import-only flows for `@kraftmarket39`.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py` diagnostics
- `source_parsing/telegram/handlers.py` candidate source-text/contact extraction
- production `telegram_source`, `telegram_scanned_message`, `event`, `event_source`, `joboutbox`, `ops_run`
- `docs/reference/locations.md`, `docs/reference/location-aliases.md`
- public Telegram/VK/Telegraph event surfaces after repair

### Mandatory checks before closure or deploy

- Replay fixture `tests/replays/INC-2026-06-30-kraftmarket317-poster-only-zero-events/source_posts.json` must keep poster-only `kraftmarket39/317` in the clear single-event LLM path.
- Negative control with multiple time anchors must not trigger single-event rescue.
- Targeted tests for Telegram Monitor OCR-only handling and location reference aliases must pass.
- Forced production replay for `@kraftmarket39/317` must create/update one source-grounded event via Smart Update and standard `JobOutbox` fanout, not by manual public posting.
- Production DB must contain `event_source.source_url='https://t.me/kraftmarket39/317'` and the `telegram_scanned_message` row must be `done` with imported event count.
- Verify public Telegram/VK/Telegraph surfaces or record a concrete blocker.

### Required evidence

- Pre/post production DB query for `telegram_scanned_message`, `event_source`, `event`, `joboutbox`.
- Runtime/ops_run evidence for forced replay.
- Test output for targeted regressions.
- Deployed SHA reachable from `origin/main` if code changes ship.
- Public links for repaired event surfaces.

## Immediate Mitigation

- Production evidence collected and saved under `artifacts/codex/kraftmarket-museum-20260630/`.
- Prevention patch changes the producer to use OCR text as primary LLM evidence when caption is empty.
- Added standard location and aliases for `Музей «Восток на Западе», Клиническая 19А, Калининград`.

## Corrective Actions

- [x] Producer no longer drops OCR-only poster posts at the empty-caption guard.
- [x] Prompt/rescue contract explicitly covers empty-caption OCR-only poster announcements.
- [x] Telegram import preserves OCR text as `source_text` for empty-caption poster-only posts.
- [x] Phone-only OCR booking contacts are normalized to `tel:+...` before post-author fallback can run.
- [x] Added regression fixture and tests for `kraftmarket39/317` poster-only OCR signal and schedule-like negative control.
- [x] Added museum standard location and aliases.
- [x] Deploy to production and run forced replay for `@kraftmarket39/317`.
- [x] Verify public Telegram/VK/Telegraph surfaces after replay.

## Follow-up Actions

- [ ] Add a production alert/report for new `producer_zero_events:clear_event_signals` rows so operators do not have to manually discover future poster-only misses.
- [ ] Consider preserving OCR snippets in diagnostic rows/artifacts for faster root-cause triage without re-reading Telegram media.

## Release And Closure Evidence

- deployed SHAs:
  - `91942df8 fix(tg-monitor): extract poster-only OCR events`
  - `951a82ce fix(tg-monitor): preserve OCR phone contacts`
- deploy path:
  - Fly app `events-bot-new-wngqia`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KWCN8TV17Z732Z5DN8X1SM7Q`
- regression checks:
  - `pytest tests/test_tg_candidate_location_grounding.py::test_tg_build_candidate_ocr_only_phone_contact_beats_group_author_fallback tests/test_tg_monitor_gemma4_contract.py::test_tg_monitor_clear_event_signal_accepts_poster_only_ocr tests/test_location_reference_bastion.py::test_vostok_na_zapade_reference_aliases_normalize_to_museum -q` → `3 passed`
  - `python3 -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/handlers.py`
  - `git diff --check`
- forced replay:
  - `ops_run id=3127`, `kind=tg_monitoring`, `trigger=incident_replay`, `status=success`
  - metrics: `sources_scanned=1`, `messages_processed=1`, `messages_with_events=1`, `events_imported=1`, `events_created=1`, `errors_count=0`
- repaired event:
  - `event_id=6524`
  - source: `https://t.me/kraftmarket39/317`
  - title/date/time/location: `Мастер-класс по каллиграфии`, `2026-07-01 19:00`, `Музей «Восток на Западе», Клиническая 19А, Калининград`
  - registration contact: `ticket_link='tel:+79316160888'`, `ticket_trust_level='source_ocr_phone'`
  - `telegram_scanned_message(source_id=1177,message_id=317)` → `status='done'`, `events_extracted=1`, `events_imported=1`, `error=NULL`
- public surfaces:
  - Telegram: `https://t.me/kldevents/1670` (`tg_event_post_id=1670`, `photo_caption`) contains the phone registration number and no `tasha9917`; premium emoji edit completed at `2026-06-30 16:26:44 UTC`.
  - Telegraph: `https://telegra.ph/Master-klass-po-kalligrafii-06-30` contains full event facts, `+7 (931) 616-08-88`, `Источников: 2`, and no `tasha9917`.
  - VK: canonical managed post is `https://vk.com/wall-231920894_5234`, contains `tel:+79316160888`; stale managed duplicate `https://vk.com/wall-231920894_5232` was deleted after owner/content verification.
  - `joboutbox` for `event_id=6524`: `ics_publish`, `telegraph_build`, `tg_ics_post`, `vk_sync`, `tg_event_publish` all `done`, `last_error=NULL`.
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, `job_outbox_worker=ok`, `issues=[]`.

## Prevention

Poster-only source posts must be treated as LLM-first OCR text, not as empty messages. Deterministic guards only decide routing/diagnostics; event title/date/venue extraction remains owned by the LLM using OCR evidence.
