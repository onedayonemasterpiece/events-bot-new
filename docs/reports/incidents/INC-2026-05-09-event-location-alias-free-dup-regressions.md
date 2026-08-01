# INC-2026-05-09 Event Location Alias / Free / Duplicate Regressions

Status: open
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Update / public Telegraph cards
Opened: 2026-05-09
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe`, `INC-2026-05-08-vk-quality-false-skips`, `INC-2026-05-05-event-quality-regression`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-04-26-daily-location-fragments`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/llm/prompts.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/features/cherryflash/partner-story-tracks.md`

## Summary

On 2026-05-09 the operator reported a new batch of user-visible public event quality defects across Telegraph cards and daily/list surfaces. The defects repeat the same family as earlier May incidents but cover a broader set of concrete examples: wrong canonical venue/address, missing city, venue alias drift, poster/OCR address not making it into the event, false or uncertain `is_free`, duplicate event cards, prose in `location_name`, and a phone-only contact that was not clickable.

This is production-relevant because the affected rows can appear in Telegram `/daily`, Telegraph event pages, month/day pages, VK daily, and CherryFlash candidate pools. The requested fix must stay LLM-first: semantic venue identity, alias choice, free/paid status, and duplicate/match decisions must be owned by Gemma/Smart Update prompts or small native-schema Gemma review stages, with deterministic code limited to reference normalization, safe guards, replay plumbing, and output validation.

One requested item is explicitly not part of the incident: ИЦАЭ events should later support a separate CherryFlash partner video/story track. It is documented as a planned product enhancement in `docs/features/cherryflash/partner-story-tracks.md`.

## User / Business Impact

- Readers see wrong or incomplete venues and addresses, including a likely venue swap between `ДС Юность` and `ДС Янтарный`.
- Readers may see events marked free when a ticket, venue entry, or registration nuance means "not free" or "free only after paid entry".
- Duplicate cards split one real event across several Telegraph pages.
- Public pages may expose prose instead of a venue, which breaks trust and downstream selection.
- Phone-only registration can become non-actionable if it is not rendered as a clickable/visible contact.

## Detection

- Detected by operator review of public Telegraph cards and daily/list outputs on 2026-05-09.
- User supplied the concrete public links and expected corrections.
- Raw source replays have not yet been collected; this record is created first by request so the remaining work has a durable regression contract even if the coding/debugging session is interrupted.

## Reported Case Inventory

Incident cases:

- `18.07` `Фестиваль добровольчества #МЫВМЕСТЕ` (`https://telegra.ph/Festival-dobrovolchestva-MYVMESTE-05-07`) — likely not `ДС Юность`; should be checked against `ДС Янтарный`, `Согласия 39`.
- `10.06` `Спектакль «Любовь по-итальянски»` (`https://telegra.ph/Spektakl-Lyubov-po-italyanski-05-08`) — city is missing; address may be correct but the venue phrasing is absent from `locations.md` / aliases.
- `17.05` `Квартирник вокальной студии Истина` (`https://telegra.ph/Kvartirnik-vokalnoj-studii-Istina-05-07`) — `Сигнал` venue wording drift; canonical aliasing should converge to one public spelling.
- `17.05` `Мастер-класс по созданию коллажа` (`https://telegra.ph/Master-klass-po-sozdaniyu-kollazha-05-08`) — poster contains an address, but event address is empty/missing.
- `16.05` `Шаманское путешествие` (`https://telegra.ph/SHamanskoe-puteshestvie-05-07`) — location problem; source evidence required.
- `15.05` `Музей изнутри` / `Экскурсия «Музей изнутри»` (`https://telegra.ph/Muzej-iznutri-05-08`, `https://telegra.ph/Muzej-iznutri-05-08-2`, `https://telegra.ph/EHkskursiya-Muzej-iznutri-05-08`) — likely duplicate cards.
- `15.05` `Образовательная программа Социальный рупор` (`https://telegra.ph/Obrazovatelnaya-programma-Socialnyj-rupor-05-08`) — likely wrong location.
- `14.05` `Выпускной концерт Школы Ведущих` (`https://telegra.ph/Vypusknoj-koncert-SHkoly-Vedushchih-05-08`) — phone number is not clickable/actionable.
- `12.05` `InQuiz [Литература] #1` (`https://telegra.ph/InQuiz-Literatura-1-05-08`) — likely bar venue missing from `locations.md`.
- `11.05` `Стендап-Экскурсия по Калининграду` (`https://telegra.ph/Stendap-EHkskursiya-po-Kaliningradu-05-07-2`) — free status is uncertain and must be checked against source evidence.
- `9.05` `Великие произведения органной музыки` (`https://telegra.ph/Velikie-proizvedeniya-organnoj-muzyki-05-04`) — unlikely to be free; venue `Евангелистко-Лютеранская церковь, Мира 101`.
- `9.05` `«В списках не значился» — кинопоказ` (`https://telegra.ph/V-spiskah-ne-znachilsya--kinopokaz-05-04-2`) — `Кинозал:` should resolve to the Tretyakov Gallery branch cinema hall via alias/refinement, not remain a fragment.
- `9.05` `Музыкальный вечер «Песни Великой Победы»` (`https://telegra.ph/Programma-Pesni-Velikoj-Pobedy-04-30`) — concert registration is free, but zoo entry may be paid; public free/ticket status must preserve that nuance.
- `9.05` `Будь здоров, школяр!` (`https://telegra.ph/Bud-zdorov-shkolyar-03-05`, `https://telegra.ph/Bud-zdorov-shkolyar-03-11`) — duplicate cards despite same title/date/venue with conflicting times / extra box-office-like address text.
- `9.05` `Поэтическая программа Нам нужна одна Победа` / `Нам нужна одна Победа` (`https://telegra.ph/Poehticheskaya-programma-Nam-nuzhna-odna-Pobeda-05-09`, `https://telegra.ph/Nam-nuzhna-odna-Pobeda-05-07`) — one card has event prose in `location_name`; likely duplicate with canonical `Научная библиотека, Мира 9`.
- `9.05` `Военный парад` (`https://telegra.ph/Voennyj-parad-05-09`) — likely wrong venue: parade should not be at `ДК Машиностроитель`.

Related planned enhancement, not an incident fix:

- `14.05` `Интеллектуальный турнир «БрейнШейкер»` (`https://telegra.ph/Intellektualnyj-turnir-BrejnSHejker-05-08`) — ИЦАЭ events should be eligible for a separate CherryFlash partner video/story track published through a Telegram Business target supplied out-of-band.

## Timeline

- 2026-05-09 UTC — operator reports the defect batch and asks to document the incident first because model/tooling limits may interrupt the repair.
- 2026-05-09 UTC — incident record created as `INC-2026-05-09-event-location-alias-free-dup-regressions`.
- 2026-05-09 UTC — implementation/debugging started after the record was filed.
- 2026-05-09 UTC — production SQLite snapshot collected as `artifacts/db/INC-2026-05-09-event-location-alias-prod-snapshot.sqlite`; SFTP stalled twice, so snapshot was recovered via `fly ssh console` gzip/base64 stream and verified with `PRAGMA quick_check`.
- 2026-05-09 UTC — minimal source fixture saved as `tests/replays/INC-2026-05-09-event-location-alias-free-dup-regressions/sources.json`.

## Root Cause

Working hypotheses until replay confirms the exact source path:

1. Venue canonicalization is still too dependent on broad prompt context and deterministic fallback after extraction. Aliases are reference data, but venue alias resolution is not yet a small dedicated Gemma 4 native-schema step that can pick one canonical `location_name` / address with source evidence.
2. Poster/OCR evidence can contain the strongest address, but the import path does not consistently preserve it into Smart Update venue grounding.
3. Duplicate detection still misses same-event variants when title wording, time, venue alias, or box-office/location fragments drift.
4. `is_free` still lacks enough explicit nuance for cases like "free by registration after paid venue entry", "registration only", and likely paid concerts with no price in source text.
5. Phone-only contact handling may be split between extraction and renderer surfaces, so a phone can be preserved but not rendered as an actionable contact everywhere.

## Contributing Factors

- The same user-visible surface aggregates Telegram Monitoring, VK auto-import, parser imports, Smart Update, Telegraph rendering, and daily builders.
- Venue and alias examples are long-tail and local; `locations.md` cannot rely on one exact spelling per public source.
- Large source posts and posters require OCR/text fusion; source evidence may be available in a different field than the final candidate venue fields.
- Gemma 4 provider budget and latency constraints make it important to use small, staged native-schema requests instead of one heavier prompt.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction, OCR/poster evidence handling, candidate grounding, or recovery import;
- changing VK auto-import draft extraction or Smart Update create/update prompts;
- changing `docs/reference/locations.md`, `docs/reference/location-aliases.md`, or location normalization code;
- changing duplicate matching, `is_free` / ticket status handling, phone/tel rendering, Telegraph rebuild, `/daily`, month/day pages, or CherryFlash candidate selection;
- adding a venue aliasing/refinement stage or migrating an extraction path to Gemma 4 native schema.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `vk_intake.py`
- `smart_event_update.py`
- `location_reference.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- `docs/llm/prompts.md`
- Telegraph event rebuild / daily rendering paths
- `markup.py` / renderer helpers for phone-only ticket/contact display
- CherryFlash partner-track selection docs for the separate ИЦАЭ enhancement

### Mandatory checks before closure or deploy

- Create replay fixtures in `tests/replays/INC-2026-05-09-event-location-alias-free-dup-regressions/` for the supplied public/source artifacts, including at least one negative/opposite control.
- Replay the raw artifacts through the production import boundary and `smart_event_update.py` on a prod snapshot or shadow DB.
- Add a small Gemma 4 native-schema venue-alias/refinement check for location canonicalization if replay confirms alias drift; it must return source-grounded canonical venue, address, city, confidence, and reason, not public prose.
- Verify each reported class:
  - `#МЫВМЕСТЕ` resolves to the correct venue/address after source evidence review.
  - `Любовь по-итальянски` gets the correct city without fabricating venue data.
  - `Сигнал`, `Кинозал`, `Научная библиотека`, ИЦАЭ and missing bar/venue cases converge to canonical reference names/aliases.
  - poster/OCR address for `Мастер-класс по созданию коллажа` is preserved when source-grounded.
  - `Музей изнутри`, `Будь здоров, школяр!`, and `Нам нужна одна Победа` collapse to one active public event per real event.
  - `free` / registration / paid-entry nuance is correct for the tour, organ concert, zoo concert, and other ticket cases.
  - phone-only contact is visible/actionable on Telegraph and daily surfaces.
  - `Военный парад` cannot inherit an unrelated `ДК Машиностроитель` venue.
- Targeted unit tests for any deterministic guard or renderer helper touched.
- `py_compile` for touched Python modules and `git diff --check`.
- Release governance checks before deploy: fetch, clean worktree, branch relation to `origin/main`, changelog/docs synced, no relevant release/hotfix drift.

### Required evidence

- Incident-linked replay artifacts and pre/post DB diff or query output.
- Test output for targeted unit/replay checks.
- Release SHA reachable from `origin/main` if deployed.
- Post-deploy `/healthz` and relevant production logs/DB evidence.
- Data repair/rebuild evidence for public Telegraph/day/month surfaces if existing rows are corrected.

## Immediate Mitigation

- Incident record created before code changes, as requested, to prevent an interrupted session from losing the scope and regression contract.
- No production data repair has been performed yet under this incident.

## Corrective Actions

- Pending investigation and replay.
- `docs/reference/locations.md` / `docs/reference/location-aliases.md` now include verified reference rows/aliases for `Дворец спорта «Янтарный»`, `Шоурум Mysig`, `Бар Дредноут`, `Сигнал`, `ИЦАЭ`, and `Научная библиотека` variants observed in the incident sources.
- `location_reference.normalise_event_location_from_reference()` now force-applies curated aliases to all structured venue fields, so server-side reference normalization behaves like the existing main parser wrapper.
- Telegram Monitoring's separate Gemma 4 `location_review` native-schema stage now triggers not only on overlong/prose/section-label fields and `default_location`, but also when the extracted venue is not grounded in source/OCR/context while the source contains venue/address cues, or when source address evidence did not reach structured fields.

## Follow-up Actions

- [x] Build minimal replay fixture pack for the supplied source/public examples.
- [x] Implement or tighten LLM-first venue alias/refinement using a small Gemma 4 native-schema request; avoid a growing deterministic keyword dictionary.
- [x] Update canonical `locations.md` / `location-aliases.md` only for verified venue facts and aliases.
- [x] Add focused reference-layer regression tests for verified incident aliases.
- [ ] Add focused duplicate/free/phone regression tests matching the remaining replay classes.
- [ ] Repair/rebuild affected production rows/pages after prevention checks pass.
- [ ] Implement the separate ИЦАЭ CherryFlash partner track after this incident fix is scoped and tested.

## Release And Closure Evidence

- deployed SHA: `cd8c4fe6` (origin/main, includes prevention commit `5032a06f` + worktree-cleanup `cd8c4fe6`)
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia` from clean `main` worktree, 2026-05-09 ~11:01 UTC, Fly release v1055 (`deployment-01KR66436HF8QY927Y4JWZ6FD4`)
- prevention commit: `5032a06f` (`origin/main`, deployed 2026-05-09 11:01 UTC as part of v1055)
- regression checks:
  - `.venv/bin/python -m pytest tests/test_smart_event_update_location_aliases.py -q` -> `4 passed`
  - `.venv/bin/python -m pytest tests/test_smart_event_update_location_aliases.py tests/test_tg_monitor_gemma4_contract.py -q` -> `27 passed` (re-run pre-deploy on `cd8c4fe6`)
  - `python3 -m py_compile location_reference.py kaggle/TelegramMonitor/telegram_monitor.py`
  - `git diff --check`
- post-deploy verification: machine `48e42d5b714228` reached `started` state, 1/1 health check passing at 2026-05-09T11:01:06Z; full replay/shadow DB and duplicate/free/phone regression checks plus production data repair/rebuild are still pending per "Mandatory checks before closure or deploy".

## Prevention

This record is the regression contract for May 9 public event-quality drift. Closure requires a production-bound replay through the actual import + Smart Update path, not only prompt diffs or manual SQL edits. Venue aliasing should become a small, source-grounded Gemma 4 native-schema stage so the main extraction prompt stays light while public venue spelling remains consistent.


## Static collection admission regression — 2026-08-01

The data-prep candidate adds source-bound `confirmed_free|confirmed_paid|unknown`
provenance while retaining `Event.is_free`. Unknown/provider failure preserves
accepted truth; a new decision applies only after the exact `EventSource` is
attached to the same event, and manual lock/trust/recency conflicts are
fail-closed. Static export no longer infers free admission from prose
`ticket_status`. This addresses the reusable mechanism behind the current
`5370` scope false positive and `7145/7244/7246/7247` false negatives, but this
branch performs no production row repair. Event `7287` remains an adjudication
case rather than proof that festival extraction belongs to this scope.

Regression evidence: `85` Smart Update/DB/ticket/participant/May-replay tests
and `123` collection/semantic/export tests passed. Targeted production
backfill/review, public rebuild and post-deploy verification remain required;
this evidence does not close the older incident.
