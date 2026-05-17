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
- No row-level production repair, deploy, catch-up, or Telegraph rebuild has been performed yet.

## Corrective Actions

- [x] Producer LLM title-review trigger covers `неделя в театре` / `афиша` / `репертуар` / `анонс` service headings.
- [x] Producer LLM venue-review trigger covers obvious all-caps person names in `location_name`; venue selection remains LLM-owned.
- [x] Producer prompt and single-event rescue explicitly protect `17.05 | GROZA` as date/title, not `time=17:05`.
- [x] Server import drops person-name `location_name` and recovers only source/default/reference-grounded venue facts.
- [x] Server import drops unsupported `time` when it exactly matches the event day/month marker.
- [x] Reference fuzzy matching ignores city names as one-token venue identity, preventing unknown defaults like `Barn, ..., Калининград` from mapping to unrelated city-branded venues.
- [x] Regression coverage added for Barn person-name venue and GROZA date-marker time.
- [ ] Replay full candidate inventory and repair production rows.
- [ ] Improve duplicate handling for parser + Telegram/VK same-event clusters.

## Follow-up Actions

- [ ] Owner: Smart Update / Telegram Monitoring / no due date / build replay fixture pack for the candidate inventory above.
- [ ] Owner: Smart Update / no due date / add or reuse a future-event quality audit command that flags prose/person-name venues, section-label titles, invalid time-from-date parses, and high-confidence duplicate clusters.
- [ ] Owner: Telegram Monitoring / no due date / tighten LLM-first venue grounding for person-name/prose fragments and compact theatre schedule bullets.
- [ ] Owner: Smart Update / no due date / improve duplicate handling for parser + Telegram/VK same-event clusters with ticket/time/title drift.
- [ ] Owner: operator / no due date / after prevention checks pass, repair/rebuild the confirmed production rows.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: `pytest tests/test_tg_candidate_location_grounding.py tests/test_tg_monitor_gemma4_contract.py -q` (`42 passed`); `pytest tests/test_pre_create_duplicate_probe.py -q` (`7 passed`); `py_compile` for touched Python modules passed.
- post-deploy verification: —

## Prevention

The durable prevention should be a reusable future-event quality audit plus replay-backed LLM-first import hardening. Row-level cleanup alone is not sufficient because this batch repeats already-known May quality incident classes.
