# INC-2026-11-25 — повторные анонсы усадьбы Деда Мороза

Status: open
Severity: sev2
Service: Events Bot / Telegram monitoring / Smart Update / VK and Telegram publishing
Opened: 2026-11-25
Closed: —
Registry: `inc_6d9f5783a699551b486dd926`
Work: `work_317f058fa6467e9e58be7853`
Related: `inc_0701843eeb6896237c767628` (собственные анонсы повторно импортируются).

## Summary

Владелец сообщил о многократных анонсах новогоднего события с красной афишей.
Расследование установило цикл: собственный анонс в `kldevents` → повторное
извлечение сеансов из общей афиши → ошибочный `FINAL_DISTINCT` → новая карточка
того же сеанса → новые задания публикации. Это создание разных Event ID,
а не многократный сетевой retry одного задания.

## User / Business Impact

В production найдены 88 активных canonical-карточек кластера «усадьба Деда
Мороза». Из них 80 относятся к четырём конкретным сеансам в агропарке
«Некрасово поле»: 26 декабря 12:30 — 21 карточка, 26 декабря 15:00 — 16,
27 декабря 12:30 — 21, 27 декабря 15:00 — 22. Остальные восемь карточек
не имеют точного времени и требуют отдельного разбора; автоматически
присоединять их к четырём сеансам нельзя.

В этих 88 карточках записаны 76 различных VK URL и 65 Telegram URL.
Это число ссылок в БД, а не число существующих сейчас публичных постов.
Два Telegram-поста прочитаны у провайдера: `https://t.me/kldevents/6249`
(2026-11-25 06:56:39 UTC) и `https://t.me/kldevents/6258`
(08:35:25 UTC). Оба объявляют 26 декабря, 12:30, одну площадку и программу.
На момент снимка у кластера 25 pending-заданий `tg_event_publish`;
часть относится к карточкам с прежним URL, поэтому это не прогноз 25 новых постов.

Authenticated VK readback с штатным user actor прочитал последние 35 постов.
В них пять анонсов кластера. Прямые пары повторов:
`https://vk.com/wall-231920894_15931` (25 ноября 04:00:01 UTC) и
`https://vk.com/wall-231920894_15987` (08:01 UTC) — 26 декабря 12:30;
`https://vk.com/wall-231920894_15932` (04:10 UTC) и
`https://vk.com/wall-231920894_16009` (09:21 UTC) — 27 декабря 15:00.
Пятый пост 15879 опубликован 24 ноября 19:21 UTC.
Точный readback 15923 вернул `post_type=postpone`, 25 ноября 19:41 UTC;
15922 отсутствует в этом ответе. Service actor ранее вернул пустой `items`
для обоих ID; это не доказывает удаления отложенного поста. Старые
`event_publication.status=missing` не являются достаточной live-проверкой.

## Detection and evidence

- Сообщение владельца 25 ноября 2026 года.
- Фактический контейнер: `deployment-events-1`, RunCoveer.
- Image: `runcoveer/events:media-source-download-20261120`.
- Runtime SHA: `b1a829be2fe69f8bb8f03b0f6f55383a596e4fc9`.
- БД читалась через SQLite `mode=ro`, без копирования всей production DB.
- File mirror включён, `/data/runtime_logs/events-bot.log` и rotated files
  прочитаны адресно по кандидатам, Event ID, URL и временному окну.
- Красная афиша просмотрена: 26/27 декабря; 12:30 для детей 5–8 лет;
  15:00 для детей 8–14 лет; Дед Мороз, Снегурочка, Гринч и коза Зойка.
- `telegram_source`: собственные `kldevents` (61834) и `kenigevents` (61830)
  включены. Источники 88 карточек: 77 `kldevents`, семь `agropark39`, четыре
  `kenigevents`. Это текущий source URL карточек, не полный historical lineage.

Retained evidence directory:
`/home/dev/artifacts/events-bot-new/20261125T110340Z-newyear-repeat-20261125/`.
Основные файлы: `db-evidence.json`, `candidates-and-logs.json`,
`runtime-code-excerpts.txt`, `poster.webp`, `vk-readback.json`.
Provider evidence: `telegram-readback.json`, `vk-user-readback.json`.

## Timeline

Все времена этой секции — UTC; время Калининграда = UTC+2.

- 14 ноября 22:40–22:47: из официального `agropark39/2495` созданы карточки
  11015–11018 на четыре различных сеанса. Их даты и время надо сохранить.
- 21 ноября: созданы 15 карточек кластера; 22 ноября — десять;
  23 ноября — 11; 24 ноября — 31; 25 ноября до 00:42 — пять.
- 25 ноября 00:30:52: кандидат 32452 из собственного `kldevents/6234`
  поступил с полями 26 декабря 12:30, но заголовок исходного поста содержит
  27 декабря 12:30. На общей афише присутствуют оба дня.
- 00:31: проверка против 11015 (26 декабря 12:30) использовала заголовок
  «27 декабря 12:30» как доказательство отличия. Decision 24755:
  `FINAL_DISTINCT`. Создана копия 11447; `ENQ tg_event_publish:11447`.
- 00:39:45–00:42:16: аналогично кандидат 32467 из `kldevents/6237`
  с полями 26 декабря 12:30 и исходным заголовком 26 декабря 15:00
  сравнен с 11015. Decision 24771: `FINAL_DISTINCT`; создана копия 11451.
- 06:56 и 08:35: Telegram публикует 6249 и 6258 для этих двух копий.

## Root Cause

1. Собственные выходные каналы включены как обычные источники мониторинга.
   Каждая новая публикация даёт новый source URL и новый кандидат.
2. Из общей многосеансовой афиши повторно извлекаются все сеансы даже тогда,
   когда текст анонса посвящён одному. Поля кандидата и текст, который
   используется для решения identity, расходятся по дате/времени.
3. Merge identity LLM принимает текст другого сеанса за доказательство
   различия с правильной существующей карточкой. В runtime
   `smart_event_update.py:21544` ветвь `FINAL_DISTINCT` непосредственно
   вызывает `_create_from_prepared_candidate()`.
4. Новые Event ID получают независимые задания публикации; обычная
   идемпотентность JobOutbox по Event ID не объединяет разные карточки.

Классификация: semantic occurrence/identity defect плюс механическое
повторное потребление собственного output. Исправление смысла и области
сеанса должно быть LLM-first. Это расследование не квалифицирует конкретный
prompt patch и не доказывает, какой commit впервые внёс дефект.

## Immediate Mitigation and Corrective Actions

Production-код, БД и конфигурация не изменялись. Посты не удалялись,
импорт не отключался, deployment не запускался. Инцидент остаётся открыт.
Выполнены чтение runtime, БД, логов, афиши и точечный Telegram readback.

## Automation Contract

### Treat as regression guard when

- Меняются собственные источники Telegram, extraction общей афиши,
  occurrence scope, merge identity или переход `FINAL_DISTINCT → CREATE`.
- Меняется публикация событий или выполняется удаление дубликатов кластера.

### Affected surfaces

`telegram_source`, Telegram monitoring extraction, `smart_event_update.py`,
`smart_update_candidate_state`, `event_identity_decision_log`, JobOutbox,
Telegram `kldevents`, VK `klgdevents`.

### Mandatory checks before closure or deploy

- Replay кандидатов 32452/32467 на изолированной БД: новых карточек того же
  сеанса не возникает; сохранены четыре разных сеанса 11015–11018.
- Negative controls: действительно разные даты, время и возрастные сеансы
  не сливаются. Полная афиша не подменяет выбранную occurrence.
- Повторное потребление собственного анонса не создаёт новый Event.
- Отдельно согласовать и выполнить минимальный repair существующих данных
  и публичных дублей с сохранением sources, receipts и канонических ссылок.
- После штатного monitoring/import цикла проверить число карточек и provider
  readback. Unit tests и deployment сами по себе не означают прекращения цикла.

### Required evidence

Кандидаты/decisions до и после, четыре сохранённых сеанса, manifest repair,
provider readback, точный release SHA в `origin/main`, если меняется код.

## Release And Closure Evidence

Фикс не написан; regression replay и тесты не запускались, поскольку поручено
расследование. Deployment и production repair не выполнялись. Telegram
и VK публичный симптом проверен точечно. Полный live census VK/Telegram не выполнен.

## Follow-up Actions

- [ ] Прекратить цикл собственного output через существующий source contract.
- [ ] Исправить grounding identity именно на выбранный сеанс; квалифицировать
  на двух указанных кандидатах и controls остальных трёх сеансов.
- [ ] Составить точный manifest дублей и управляемых публичных постов;
  только затем выполнять очистку и проверять обычный следующий import.

## Продолжение 25 ноября: исключение собственных источников

Владелец поручил проверить давность добавления `kldevents` и исключить его
из мониторинга даже при случайной регистрации. Наблюдение в
`telegram_scanned_message`: первый сохранённый `processed_at` для нынешней
source row — 17 июля 2026 23:54:18 UTC (18 июля 01:54:18 Калининград).
Для `kenigevents` — 17 июля 23:19:36 UTC. Следовательно, обработка этих
каналов существовала в июле. Таблица не содержит created_at/actor;
кто и когда впервые добавил запись или повторно включал её, не установлено.
В каноническом `sources.yml` обоих каналов нет. Дата июльского сканирования
не доказывает происхождение записи из ручного интерфейса или seed.

Обе production rows 61830/61834 переведены в `enabled=0`; прочитаны прежние
и новые значения. Квитанция: `disable-own-sources-receipt.json` в retained
каталоге инцидента. Существующие события, источниковые связи и задания
публикации сохранены.

Подготовлена узкая механическая защита идентичности источника: единый
список двух собственных каналов, проверка при добавлении, фильтрация
конфигурации мониторинга и ранний пропуск source metadata/messages при
импорте. Семантика сеансов и merge identity в этом изменении не меняется.
Regression tests проверяют случайный enabled-source, ручной targeted run,
формы username/URL и импорт старого результата без регистрации источника
или создания заданий. Release evidence дополняется после доставки.

## Remediation continuation — 25 November, 19:16 UTC onward

Owner explicitly requested removal of existing and postponed duplicates and a
root-cause repair. Source exclusion alone did not cancel already-created events
and their outbound jobs. At 19:17 UTC, all 88 cohort cards were temporarily
silenced and 101 pending/error outbound jobs terminalized as skipped.

Provider cleanup used a manually reviewed exact-message manifest, with current
text checked before deletion. Nine native VK postponed duplicates were deleted;
a fresh postponed feed retained three unrelated items. Twenty-nine published VK
duplicates were deleted; provider readback retained four distinct-session posts:
14855, 14682, 14773, 14950. Telegram cleanup removed 71 duplicate messages and
verified their absence. Canonical messages 5723 (26 December, both times),
5989 (27 December 15:00), 6084 (27 December 12:30) remain. The bot API removed
38 recent messages; the local, explicitly authorized channel admin removed the
33 older messages refused by the bot. Other programmes and multi-event digests
were preserved.

The existing manifest repair implementation merged 76 exact-slot aliases into
11015–11018 transactionally, preserving obsolete event shells, source facts,
media evidence and decision history. Accepted candidate ownership was repointed;
obsolete jobs were terminalized. Canonical cards remain silent during release
qualification. The other eight cards are outside the exact-slot merge manifest.

The producer already invokes occurrence scoping for these four-session packets.
Its prompt incorrectly allowed one programme with several sessions to count as
single_event; the final merge identity payload then ignored occurrence_scope_text
and supplied the complete sibling caption again. Fix the existing scoping
contract and feed its selected evidence into the existing final identity gate.
Retain full source context for audit, while explicitly distinguishing it from
the target occurrence. No new classifier, state, queue or retry is introduced.

Local focused regression: 88 tests passed. Real model replay and exact-main
RunCoveer deployment are still being executed; this section does not claim a
release. Evidence and deletion manifests are retained under
`/home/dev/artifacts/events-bot-new/20261125T191607Z-newyear-spam-remediation-20261125/`.

## Live release and cleanup evidence — 25 November, 19:42 UTC

PR [1105](https://github.com/onedayonemasterpiece/events-bot-new/pull/1105)
passed CI and merged as `e903d755c52b60bbae10e0d64226ded1e1aed0e2`.
Image `runcoveer/events:newyear-occurrence-identity-20261125` was built from
the running title-matcher release `fc6fe00036ae5b8c7ca5263e767a3dc20a9e3a68`,
which is an ancestor of the candidate. All 4627 tracked files matched exact
main. Image qualification passed 159 tests and a network-isolated startup.
Events-only Compose cutover preserved Kotopogoda. Runtime readback reports
the merged SHA; live readiness, DB, scheduler and outbox worker are healthy.
Disk free at that readback: 7650 MiB. Rollback is the actual prior image
`runcoveer/events:vk-title-tokens-20261125`, preserving the earlier disk fixes.

Real configured-provider replay scoped both incident packets 32452/32467 and
returned `allow_merge/source_update`, confidence 1.0, against 11015. Three
negative controls against the other dates/times returned
`skip_merge_side_effects/related_but_distinct`. An initial positive request
received provider 503/fallback and exceeded the existing 90-second deadline;
the subsequent completed replay passed. No provider or retry configuration
was changed. Local focused suites passed 88 and 83 tests; PR CI passed.
The deployed image also passed the stale own-source import boundary tests.
A live monitoring config request excluded both own channels.

Final data reconciliation: 78 merged aliases, four cancelled aggregate/context
fragments, six active canonical cards. These are the four exact sessions,
original December 1 opening 10219 (copies 10241/10258 merged), and school
programme 10311. All six were restored from temporary silence after release.
For 11017 both Telegram post URL and post ID now reference 6084. Eight
canonical publication records reflect exact provider readbacks.

Public cleanup: all 71 duplicate messages in `kldevents` and 29 VK public
posts are absent; canonical session posts remain. All nine native VK scheduled
duplicates are absent; three unrelated scheduled posts remain. Sixteen
`kenigevents` digest messages were edited and read back, removing 107 duplicate
lines across split daily digests and remapping retained links to canonical
cards. Unrelated entries and entities were preserved.

Additional ICS channel inspection found 76 duplicate calendar documents.
The production bot deleted 41 and the provider readback retained the four
original session documents. Thirty-five older duplicates remain in
`kenigeventscalendar`: Bot API returned `message can't be deleted`; the local
human inspection account is not a participant and has no delete permission
in that channel. The owner was asked to grant delete access to Telegram ID
8336351413. No membership or administrator roles were changed.
The exact remaining IDs/texts are retained in `calendar-after.json`. This
remaining public cleanup keeps the incident open; source exclusion, root
repair and the primary-channel cleanup have already been deployed/applied.

The existing coalesced `event_vector_sync:prod` request was rearmed after the
repair. Its completion is tracked in the incident registry; no new publication
or monitoring mechanism was added.

## Ordinary monitoring verification — 26 November

The scheduled 25 November 23:40 Kaliningrad run (`ops_run=13479`,
`run_id=1514802b724f479d9e978265f6560270`) was later marked crashed on recovery.
Import-only recovery of the same retained result completed successfully as
`ops_run=13486` on 26 November 17:31:57 Kaliningrad: 53 messages processed,
34 messages with events, five new messages, two events created, one merged,
zero terminal errors. This is a recovered scheduled run, not an uninterrupted
success of the original process.

The retained `telegram_results.json` contains 53 messages and 54 source metadata
records; neither messages nor metadata contain `kldevents` or `kenigevents`.
A live DB read at 26 November 20:15 Kaliningrad still found six active canonical
cards, 78 merged aliases and four cancelled context fragments, zero new cards
for the incident programme after 11451 and zero active outbound publication
jobs for the cohort. The own sources remain disabled. Runtime is still
`e903d755c52b60bbae10e0d64226ded1e1aed0e2`, live readiness/DB/schedulers passed;
disk free was 7298 MiB. Fresh Telegram census retained the exact same primary
post IDs as after cleanup, with no new announcements of the four-session
programme.

The remaining action is unchanged: 35 old ICS calendar copies. A fresh local
admin access probe on 26 November returned `UserNotParticipantError`; no
messages were deleted by that probe and no roles were changed. The incident
stays open for this precise permission-dependent cleanup. No further timed
waiting for the scheduled-cycle verification is needed: recovery and import
have completed and were inspected.
