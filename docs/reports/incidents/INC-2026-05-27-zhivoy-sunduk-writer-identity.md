# INC-2026-05-27-zhivoy-sunduk-writer-identity

Status: monitoring
Severity: sev2
Service: Telegram Monitoring / Smart Update writer / Telegraph event pages
Opened: 2026-05-27
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-17-future-event-quality-regressions`, `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone`, `INC-2026-05-11-lecturer-name-and-title-dropped-from-description`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/prompts.md`, `docs/llm/request-guide.md`

## Summary

The public Telegraph card `https://telegra.ph/CHitajte-bumazhnye-knigi-05-27` published the event as `Читайте бумажные книги!`, while the organizer reported the event name is `Живой сундук`. The description also attributed the event to `Живой Замок` and invented an inspiration from `мир «Живого Замка»` instead of the source-correct `ОКЦ на Горького 116` community and `Плоский мир Терри Пратчетта`.

## User / Business Impact

- Organizer-facing factual error in title and organizer attribution.
- Public card misrepresented who organizes the event and what cultural world inspired it.
- The error damages trust in automatic writer output and source-grounding.

## Detection

- Organizer complaint relayed by operator on 2026-05-27 with exact paragraph-level corrections.
- Public Telegraph page confirmed the wrong title and wrong organizer/inspiration statements.

## Timeline

- 2026-05-27 03:40 UTC+02: Telegraph page last updated with wrong title/description.
- 2026-05-27: organizer reported corrections; incident opened; prompt contracts tightened.

## Root Cause

1. Title extraction/writer path over-weighted a poster/slogan-like phrase (`Читайте бумажные книги!`) over the attendee-facing event name (`Живой сундук`).
2. Smart Update writer prompts treated organizer/community/world references as ordinary prose, allowing the model to smooth or infer identity facts.
3. There was no regression contract pinning organizer/community/source-of-inspiration as literal identity facts.

## Contributing Factors

- The existing title prompt already handled service headings, but did not explicitly cover poster slogans or reading-imperative slogans.
- Rich-facts prompts preserved speaker names and program bullets, but not organizer/world attribution as first-class identity facts.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitor title extraction, poster OCR title priority, or title-review stages.
- Changing Smart Update rich facts, create bundle, split description writer, or final writer prompts.
- Changing public event-page rebuild logic for organizer/community attribution.

### Affected surfaces

- `docs/llm/prompts.md`
- `kaggle/TelegramMonitor/telegram_monitor.py`
- `smart_event_update.py`
- Telegraph event pages

### Mandatory checks before closure or deploy

- Replay `tests/replays/INC-2026-05-27-zhivoy-sunduk-writer-identity/sources.json` as prompt contract evidence.
- `pytest tests/test_smart_update_native_schema.py -k 'organizer_and_inspiration_identity'`
- `pytest tests/test_tg_monitor_gemma4_contract.py -k 'caption_event_name_over_poster_slogan'`
- Production data repair: Telegraph card title and description contain `Живой сундук`, `ОКЦ на Горького 116`, and `Плоский мир Терри Пратчетта`; it must not contain the reported wrong `Живой Замок` attribution.

### Required evidence

- Test output for prompt contract checks.
- Corrected Telegraph URL contents.
- Deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- Added title prompt guidance: caption/source event name beats poster slogans/CTA.
- Added Smart Update identity-fact rules: organizers, communities, venues, worlds/franchises/inspiration sources must be preserved from source evidence and not inferred.

## Corrective Actions

- Updated master parser prompt, Telegram Monitor prompt, Smart Update rich-facts/create-bundle/split-writer prompts.
- Added tests pinning the `Живой сундук` title-priority and identity-fact contracts.
- Added replay fixture.

## Follow-up Actions

- [x] Repair production Telegraph page and DB row.
- [ ] Add a compact eval case with source text + poster slogan to the next Telegram Monitor quality pack.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks: `pytest tests/test_smart_update_native_schema.py -k 'organizer_and_inspiration_identity'`; `pytest tests/test_tg_monitor_gemma4_contract.py -k 'caption_event_name_over_poster_slogan'`; `pytest tests/test_smart_update_native_schema.py tests/test_tg_monitor_gemma4_contract.py`; `python3 -m compileall -q smart_event_update.py kaggle/TelegramMonitor/telegram_monitor.py ...`
- production runtime logs: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, `RUNTIME_LOG_RETENTION_HOURS=24`; 2026-05-27 import is inside retention, while source-of-truth evidence for the correction is preserved in `event_source` and operator report.
- production repair: backup `/data/repair_backups/db_inc_2026_05_27_20260527T074945Z.sqlite`; event `5342` retitled to `Живой сундук`, short description and description corrected to `ОКЦ на Горького 116` and `Плоский мир Терри Пратчетта`; rebuild jobs `20728` (Telegraph), `20729` (month page), `20730` (weekend page) enqueued.
- post-deploy verification:

## Prevention

- Future prompt changes must keep title priority and identity facts source-grounded.
- The regression tests make a prompt diff that drops `Живой сундук` / `Плоский мир Терри Пратчетта` guidance fail visibly.
