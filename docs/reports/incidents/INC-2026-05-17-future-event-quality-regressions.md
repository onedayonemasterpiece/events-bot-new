# INC-2026-05-17 Future Event Quality Regressions

Status: open
Severity: sev2
Service: Telegram Monitoring / VK auto-import / parser imports / Smart Update / public Telegraph and daily surfaces
Opened: 2026-05-17
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-16-tg-location-prose-cityjazz-recurrence`, `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge`, `INC-2026-05-08-vk-tg-prompt-and-dup-probe`, `INC-2026-04-26-daily-location-fragments`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/llm/prompts.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/operations/runtime-logs.md`, `docs/operations/prod-data.md`

## Summary

Fresh production DB audit on 2026-05-17 found a new visible batch of active future event quality regressions. The batch includes prose fragments or person names in `location_name`, announcement/section text in `title`, duplicate public cards for the same real event, and cases where a clear source event was split into several cards with conflicting venue/time data.

The operator-reported examples are confirmed:

- event `5052`, `Кураторский тур по выставке «Общая кухня»`, 2026-05-17 15:30-16:30, has `location_name='ТАТЬЯНА БОРИСОВА'` although the source post is from `barn_kaliningrad/1002` and the speaker name is not a venue.
- `GROZA` on 2026-05-17 survives as several active cards: official/parser event `3688` (`Groza`, 18:00), Telegram event `4843` (`неделя в театре`, 18:00), Telegram/VK event `4744` (`GROZA`, 17:05), and Telegram event `4800` with an announcement sentence as title and no time.
- Follow-up 2026-06-05 examples add the same classes on public VK surfaces:
  `event_id=4327` used a historical narrative/OCR date (`9 октября 1947...`) as
  a future exhibition date, `event_id=3377` merged a March 29 source post into a
  false October 26 future event, and `event_id=5461` published `Культурное место
  на Острове Канта` as the unrelated known venue `Место Силы`.

The wider audit over 355 active future rows found more high-confidence examples below. The first durable prevention patch now hardens the Telegram producer/import boundary, but production data repair, deploy, catch-up, and Telegraph rebuild have not been performed yet.

## User / Business Impact

- Readers see public event cards with unusable venue lines such as a person name, prose sentence, or free-entry note.
- The same real event can appear multiple times in daily/month/Telegraph surfaces, splitting source evidence and confusing ticket links.
- Wrong title/venue/time fields reduce trust in `/daily`, Telegraph pages, VK daily output, and video announcement candidate pools.
- Several defects repeat recently closed or still-open regression classes, so repair must happen with replay evidence, not only row-level SQL.

## Detection

- Operator requested a production audit on 2026-05-17 and supplied concrete public examples for `Общая кухня` and `GROZA`.
- Fresh production snapshot: `artifacts/db/future_quality_audit_2026-05-17_070713.sqlite`, downloaded from Fly `/data/db.sqlite` on 2026-05-17 around 07:13 UTC after SFTP fallback to gzip/base64 stream; `PRAGMA quick_check=ok`.
- Snapshot counts: `event=4700`, `event_source=6228`, `telegram_scanned_message=2083`, `vk_inbox=7114`, `ops_run=1286`.
- Runtime evidence: `/data/runtime_logs` exists on Fly and contained active/rotated logs through 2026-05-17 07:13 UTC. Shell env probe printed `ENABLE_RUNTIME_FILE_LOGGING=` and defaults for `RUNTIME_LOG_DIR=/data/runtime_logs`, `RUNTIME_LOG_BASENAME=events-bot.log`, `RUNTIME_LOG_RETENTION_HOURS=24`; the file mirror was usable for this investigation.

## Timeline

- 2026-04-06 13:04 UTC — parser created official event `3688` for `Groza`, 2026-05-17 18:00, from `https://dramteatr39.ru/spektakli/Groza`.
- 2026-04-21 00:50 UTC — Telegram Monitoring created event `4074` (`Выставка «Общая кухня»`, 2026-05-24) with prose split across `location_name`/`location_address`.
- 2026-05-09 00:37 UTC — Telegram Monitoring created event `4744` (`GROZA`) with `time='17:05'` from a source line `17.05 | GROZA`.
- 2026-05-10 23:35 UTC — Telegram Monitoring created event `4800` with title `Осталось 1,5 месяца до конца театрального сезона...` and `ticket_link` for `Groza`.
- 2026-05-11 23:57 UTC — Telegram Monitoring created event `4843` with title `неделя в театре`, same date/time/venue as `3688`.
- 2026-05-16 23:39 UTC — Telegram Monitoring created event `5052` with `location_name='ТАТЬЯНА БОРИСОВА'`.
- 2026-05-17 07:13 UTC — fresh production snapshot downloaded and verified.
- 2026-05-17 UTC — heuristic future audit over active rows found the candidate inventory below and this incident record was opened.
- 2026-06-05 UTC — operator supplied public VK regressions `wall-231920894_2141`
  (historical date used as future date) and `wall-231920894_1663` (source-grounded
  `Культурное место` mapped to `Место Силы`); prod DB/log evidence confirmed
  affected `event_id=4327` and `event_id=5461`.
- 2026-06-05 UTC — operator supplied `wall-231920894_2147`; production source
  check confirmed `event_id=3377` source `https://t.me/signalkld/10140` says
  `29.03 (ВС)` at `14:00`, not `26 октября`.

## Confirmed / High-Confidence Candidate Inventory

### Bad Location / Title Fields

| Event IDs | Date/time | Source | Current bad public data | Expected repair direction |
| --- | --- | --- | --- | --- |
| `5052` | 2026-05-17 15:30-16:30 | `https://t.me/barn_kaliningrad/1002` | `location_name='ТАТЬЯНА БОРИСОВА'` | Treat speaker/person name as non-venue; recover venue from source default/reference or leave venue empty until source-grounded. |
| `4074` | 2026-05-24 | `https://t.me/barn_kaliningrad/970` | `location_name='Хорошие новости и для тех'`, `location_address` is a long prose continuation. | Same Barn/`Общая кухня` family: prose split must be dropped and venue recovered source-groundedly. |
| `4873`, `4874` | 2026-05-19 / 2026-05-24 | `https://t.me/terkatalk/4835` | `location_name='Осталось 1,5 месяца до конца театрального сезона...'` | Repeats prose-location class; should recover `Драматический театр, Мира 4, Калининград` or merge with existing official rows. |
| `4651` | 2026-05-20 19:00 | `https://t.me/zaryakinoteatr/889` | title `показ мод`, `location_name='декора и бижутерии'`; shares ticket with `4650`. | Source describes one fashion show at `Заря`; collapse/merge split duplicate and keep venue `Заря, Мира 41-43`. |
| `4978`-`4982` | 2026-06-13/14/20/21/28 14:30 | `https://t.me/dramteatr39/4238` | `location_name='Присоединиться к прогулке по театру можно 13, 14, 20, 21 и 28 июня в 14:30.'` | Multi-date theatre excursion should use `Драматический театр, Мира 4`; June 28 also duplicates VK event `5014`. |
| `4060` | 2026-06-14 17:00 | `https://t.me/signalkld/10487` | `location_name='вход свободный!'` | Source says meetings/concert are in `Сигнал`; free-entry text must not become venue. |
| `4870` | 2026-06-27 | `https://t.me/open_fest/606` plus linked `open_fest/48`, `open_fest/603` | `location_name` is a sentence about sharing news with readers. | Linked-source/fact aggregation leaked prose into venue; festival venue/date facts must be source-grounded. |

### Duplicate / Split Cards

| Event IDs | Date/time | Source evidence | Why suspicious |
| --- | --- | --- | --- |
| `3688`, `4843`, `4744`, `4800` | 2026-05-17, mostly 18:00 | `dramteatr39.ru/spektakli/Groza`, `t.me/dramteatr39/4193`, `t.me/dramteatr39/4207`, `t.me/dramteatr39/4213`, VK `wall-132625599_17678` | Same `GROZA` performance; one card has section-label title, one has announcement-sentence title, one parsed `17.05` as `17:05`. |
| `3276`, `3829` | 2026-05-17 | `yantarholl/4304`, `yantarholl/4408` | Same Sergey Trofimov concert, same venue and identical normalized ticket URL. |
| `4650`, `4651` | 2026-05-20 | `zaryakinoteatr/889`, qTickets `233819-modnyy-pokaz` | One source/ticket split into two cards; second has phrase fragment as venue. |
| `4982`, `5014` | 2026-06-28 14:30 | `dramteatr39/4238`, VK `wall-132625599_19007` | Same `Закулисье театра` occurrence, identical ticket URL; one venue is prose, one is canonical. |
| `4788`, `4832` | 2026-07-04 19:00 | qTickets `234339-bastion-open-eyr-den-goroda` | Same ticket/date/time but two public cards and conflicting venues (`Бар Бастион` vs `Железнодорожные ворота`). Related to Bar Bastion/location/dup incidents. |
| `2758`, `3655` | 2026-07-11 19:00 | `yantarholl/4277`, `yantarholl/4383`, `yantarholl/4451` | Same Sergey Makovetsky/Chekhov evening, same venue and identical ticket URL, title wording drift. |

## Root Cause

Confirmed prevention root causes for the operator-reported shapes:

1. **Location prose/person-name guards did not cover obvious person names.** The May 16 prose guard closed sentence fragments, but `ТАТЬЯНА БОРИСОВА` did not trigger producer LLM venue-review or server fail-closed location recovery.
2. **Compact repertoire lines needed explicit date-marker protection.** `17.05 | GROZA` is a date/title line, but the server could keep a producer `time='17:05'` as a public default time even though no colon time exists in the source.
3. **Service-heading title review did not include theatre digest labels.** `неделя в театре` was not part of the producer title-review trigger set.
4. **Reference fuzzy matching could use a city token as venue identity.** During the Barn prevention test, an unknown source default `Barn, ..., Калининград` matched unrelated `Калининград Сити Джаз Клуб` because `Калининград` was allowed as a one-token fuzzy overlap.

Still-open investigation areas:

1. **Duplicate probes miss same-event variants when one side lacks ticket/time, has source-default venue drift, or comes from parser vs Telegram/VK.** The `GROZA` cluster combines official parser, Telegram digest, VK digest, missing ticket, wrong time, and section title variants.
2. **Linked-source aggregation can promote non-event prose into an event venue.** `open_fest/606` appears attached to an actual festival row and leaked its narrative sentence into `location_name`.
3. **Historical dates from narrative/OCR text can be mistaken for future event dates.** Exhibition/story posts that mention document dates such as `9 октября 1947 года` must not become upcoming event rows unless the source also gives a concrete future attendee-facing slot.
4. **Old/source-cluster merge drift can attach a stale title/date family to a fresh import.** `event_id=3377` kept `Хэллоуин на ДВИ` / `2026-10-26` while the fresh source attached to it was a March 29 detective game announcement.

## Contributing Factors

- Existing audits are reactive and manual; the reusable future-event quality audit command from `INC-2026-05-01` is still a follow-up.
- Many source posts are short schedule bullets where title, date, and venue live in different blocks or external ticket pages.
- Venue recovery must stay LLM-first; deterministic checks can reject unsafe output but should not replace semantic venue decisions with broad keyword rules.
- Current duplicate evidence is scattered across `event`, `event_source`, `ticket_link`, parser rows, Telegram rows, and VK rows.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction, server import candidate building, source defaults, or linked-source handling;
- changing Smart Update create/update, duplicate probes, title/time/date normalization, or parser import merge logic;
- changing `docs/reference/locations.md`, `docs/reference/location-aliases.md`, or venue normalization;
- changing `/daily`, Telegraph, month/day pages, VK daily, or video announcement candidate selection for future active events;
- repairing any of the candidate rows above.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- parser import paths for Dramteatr, qTickets, Yantar Hall, and VK auto-import
- `location_reference.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- production SQLite `event`, `event_source`, `telegram_scanned_message`, `vk_inbox`, `ops_run`
- Telegraph event pages, `/daily`, month/day pages, VK daily, video-announcement candidate pools

### Mandatory checks before closure or deploy

- Preserve minimal raw source artifacts for the confirmed candidates in `tests/replays/INC-2026-05-17-future-event-quality-regressions/`.
- Replay the candidate sources through the same production import boundary plus Smart Update on a prod snapshot or shadow DB.
- Add negative/opposite controls:
  - legitimate person-name events must not be blanket-dropped when the person is the event title, only when the name lands in `location_name`;
  - legitimate venue names containing words like `театр`, `выставка`, `вход` in brand/context must not be dropped if source-grounded.
- Verify `GROZA` collapses to one active public event for 2026-05-17 18:00 and does not regress legitimate multi-show theatre schedules.
- Verify `Общая кухня` Barn events no longer expose person/prose fragments as venue.
- Verify duplicate clusters above have one active survivor or a documented source-grounded reason to keep separate events.
- Run targeted unit/replay tests and `py_compile` for touched Python modules.
- If production data is repaired, rebuild affected Telegraph/day/month surfaces or record why a surface is not applicable.
- Release-governance checks before deploy: fresh `git fetch`, clean task worktree, commit reachable from `origin/main`, changelog/docs synced.

### Required evidence

- Fresh production DB snapshot/query output before and after repair.
- Replay artifacts and pre/post DB diff for confirmed source rows.
- Source links/API output for each repaired/deleted row.
- Test output for targeted regression suites.
- Telegraph/month/day rebuild evidence for public rows.
- Deployed SHA reachable from `origin/main` if code changes ship.

## Immediate Mitigation

- Incident record opened and candidate inventory preserved.
- Prevention patch added in code/tests:
  - `kaggle/TelegramMonitor/telegram_monitor.py`: LLM-first prompt/review hardening for theatre digest titles, `DD.MM | Title` date markers, obvious person-name venues, and clear single-event posts.
  - `source_parsing/telegram/handlers.py`: server safety-net for person-name venues, date-marker-as-time drops, and source default parsing.
  - `location_reference.py`: fuzzy venue matching no longer treats city names as identity tokens.
- Code prevention was deployed on 2026-05-17. Row-level production repair,
  catch-up, and Telegraph rebuild are still pending.

## Corrective Actions

- [x] Producer LLM title-review trigger covers `неделя в театре` / `афиша` / `репертуар` / `анонс` service headings.
- [x] Producer LLM venue-review trigger covers obvious all-caps person names in `location_name`; venue selection remains LLM-owned.
- [x] Producer prompt and single-event rescue explicitly protect `17.05 | GROZA` as date/title, not `time=17:05`.
- [x] Server import drops person-name `location_name` and recovers only source/default/reference-grounded venue facts.
- [x] Server import drops unsupported `time` when it exactly matches the event day/month marker.
- [x] Reference fuzzy matching ignores city names as one-token venue identity, preventing unknown defaults like `Barn, ..., Калининград` from mapping to unrelated city-branded venues.
- [x] Regression coverage added for Barn person-name venue and GROZA date-marker time.
- [x] `docs/reference/locations.md` and `location-aliases.md` now distinguish
  `Культурное место, Остров Канта, Калининград` from the generic island and from
  unrelated `Место Силы`; regression coverage asserts canonicalization.
- [x] LLM-first extraction prompts now explicitly reject historical/background
  dates from exhibition/story prose or noisy OCR as future event dates.
- [x] `event_id=4327` production data repaired to `date=2026-04-10`,
  `end_date=2026-10-10`, `end_date_is_inferred=false`; public
  `wall-231920894_2141` now says `до 10 октября`, `Пн-пт 10:30–17:00` and VK API
  verification shows `attachments_count=4`.
- [x] `event_id=3377` production data repaired to source-grounded past date
  `2026-03-29 14:00`, `silent=true`, `lifecycle_status=cancelled`; public
  false-future `wall-231920894_2147` was deleted and VK API verification returns
  `deleted=true`.
- [ ] Replay full candidate inventory and repair production rows.
- [ ] Improve duplicate handling for parser + Telegram/VK same-event clusters.

## Follow-up Actions

- [ ] Owner: Smart Update / Telegram Monitoring / no due date / build replay fixture pack for the candidate inventory above.
- [ ] Owner: Smart Update / no due date / add or reuse a future-event quality audit command that flags prose/person-name venues, section-label titles, invalid time-from-date parses, and high-confidence duplicate clusters.
- [ ] Owner: Telegram Monitoring / no due date / tighten LLM-first venue grounding for person-name/prose fragments and compact theatre schedule bullets.
- [ ] Owner: Smart Update / no due date / improve duplicate handling for parser + Telegram/VK same-event clusters with ticket/time/title drift.
- [ ] Owner: operator / no due date / after prevention checks pass, repair/rebuild the confirmed production rows.

## Release And Closure Evidence

- deployed SHA: `bba67b5aa78c4bd6c516348e4e5b4cfd26cd9c35`
- deploy path: clean linked worktree `hotfix/2026-05-17-cherryflash-eco-promo`, pushed to `origin/main`, deployed with `flyctl deploy -a events-bot-new-wngqia`
- regression checks: `/home/dev/projects/events-bot-new/.venv/bin/pytest tests/test_promo.py tests/test_video_announce_popular_review.py tests/test_vk_auto_queue_import.py tests/test_tg_candidate_location_grounding.py tests/test_tg_monitor_gemma4_contract.py -q` -> `91 passed`; `py_compile` for touched Python modules passed.
- post-deploy verification: Fly image `events-bot-new-wngqia:deployment-01KRTH9RXB7P1NV3X86S4CDWAT`; Fly machine `48e42d5b714228`, version `1100`, checks `1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.

### 2026-06-05 Follow-Up

- Prevention deployed in code SHA `fa49b73095046ff468895e324c5809aabd62badb` and documented in `origin/main`.
- Public VK repairs:
  - `https://vk.ru/wall-231920894_1663` edited via `wall.edit` (`response.post_id=1663`) from `Место Силы, Галицкого 18` to `Культурное место, Остров Канта`; existing photo attachment was preserved.
  - `https://vk.ru/wall-231920894_2141` edited via `wall.edit` (`response.post_id=2141`) to remove the false `9 октября 10:30` event date and state that the exhibition is already open with archive visiting hours `Пн-пт 10:30–17:00`.
- Source verification for `wall-231920894_2141`: public `afisha80let.visit-kaliningrad.ru`
  and `visit-kaliningrad.ru/events` list `Отдыха не знали, из руин подняли...`
  as `10.04.2026 - 10.10.2026, ПН-ПТ 10:30-17:00`.
- Second repair pass for `wall-231920894_2141`: DB row `event_id=4327` now has
  `date=2026-04-10`, `time=''`, `end_date=2026-10-10`, `end_date_is_inferred=false`;
  VK text says `📅 до 10 октября`, has no `#9октября` tags, and VK API confirms
  `attachments_count=4`.
- Source verification for `wall-231920894_2147`: DB source
  `https://t.me/signalkld/10140` says `Когда: 29.03 (ВС) на ДВИ`, `Начало:
  14:00`; the public `26 октября` date was false. Production row `event_id=3377`
  was moved to `2026-03-29 14:00`, `silent=true`, `lifecycle_status=cancelled`,
  and public `wall-231920894_2147` was deleted (`deleted=true` via VK API).

## Prevention

The durable prevention should be a reusable future-event quality audit plus replay-backed LLM-first import hardening. Row-level cleanup alone is not sufficient because this batch repeats already-known May quality incident classes.
