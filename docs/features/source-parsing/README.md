# Source parsing (извлечение событий из источников)

Event images recovered by full-update/source parsing are passed to
`event_media.ingest_event_media_urls()` and therefore the Smart Update automatic
gate; this surface must not assign `Event.photo_urls` directly. See
[Event media](../event-media/README.md).

Фича отвечает за извлечение и обновление событий из внешних источников: театры, отдельные сайты (например pyramida.info), спецпроекты Дом искусств, а также другие источники, которые будут добавляться.

## Точки входа

- Команда супер‑админа: `/parse` (и диагностический режим `/parse check`).
  - Точечный запуск: `/parse <source>` (например `/parse dramteatr`,
    `/parse estrada`, `/parse yantarhall`) чтобы не гонять все источники.
- Автозапуск по расписанию: `ENABLE_SOURCE_PARSING=1` (см. `source_parsing/commands.py`).
- Из VK review UI: кнопки “Извлечь …” для ссылок на поддерживаемые источники.

## Как события попадают в БД (важно)

Все результаты `/parse` сохраняются **через Smart Update** (`smart_event_update.smart_event_update`):

- `Event.search_digest` (в UI иногда называют “короткое описание”) используется как краткий дайджест.
- `Event.description` — **полное** описание события, которое публикуется на странице события в Telegraph.
- Ежедневные анонсы (daily posts) должны показывать именно `Event.search_digest` как one‑liner, а не полный `Event.description`.
- В ежедневных анонсах заголовок события должен вести на Telegraph страницу события (а не на Telegram/VK пост-источник).
- Для каждого источника создаются записи `event_source` и “факты” в `event_source_fact`, чтобы было видно вклад каждого источника в мердж.
- Telegram Monitoring канонизирует `location_name/location_address/city` через `docs/reference/locations.md` + `docs/reference/location-aliases.md` ещё до создания `EventCandidate`, чтобы `/daily` и merge-path не расходились по написанию площадок.

### LLM-first guardrails для VK/TG parse

- Для VK/TG draft extraction сохраняется LLM-first подход: массовые смысловые решения принимаются в prompt/parser, а не детерминированным “переписыванием” текста после разбора.
- Отдельный targeted guard теперь добавляет в parse prompt узкий hint для giveaway/contest постов: если матч/концерт/другое событие упомянуто только как приз розыгрыша, parser должен вернуть `[]`, а не создавать pseudo-event.
- Downstream Smart Update дублирует это как safety-net (`skipped_giveaway`), чтобы prize-only promo пост не проходил даже при неудачном upstream parse.
- Для image-heavy intro posts (`листайте афиши`, `смотрите карточки`, weekly schedule wrapper без конкретных событий в тексте) parse prompt теперь явно разрешает вернуть `[]` как штатный результат, а не пытаться “додумать” события из обёртки.
- Gemma parse path теперь жёстче требует чистый JSON (`[]` или объект с `events`) и, если Gemma после repair всё равно отдаёт битый JSON, переключается на fallback `4o` вместо немедленного падения.
- VK poster OCR остаётся source evidence даже при длинных caption'ах: если полный OCR не помещается в token budget, parse boundary обязан сохранить компактные logistics lines (дата/время/город/площадка/адрес/вход) вместо полного drop. Это предотвращает потерю времени/места, когда caption содержит длинный новостной текст, а точные `HH:MM` или venue находятся только на афише.
- Для VK multi-poster / schedule posts intake дополнительно схлопывает exact duplicate child drafts внутри одного parsed batch только при совпадении `date + explicit time + venue + normalized title`; это узкий safety-net против двойного извлечения одной и той же карточки из карусели/афиш.
- Для VK/TG дайджеста с несколькими датированными пунктами Smart Update сначала
  просит LLM выделить дословный occurrence-scoped блок и только затем проверяет
  роль date/range/time. Заголовок дайджеста вроде «с 01 по 07 августа» не может
  становиться диапазоном каждого дочернего события. Grounding остаётся
  fail-closed; при проверке дословности VK transport wrapper `[target|label]`
  эквивалентен только видимому `label`, а не произвольной перефразировке.
- Если финальный LLM-grounding уверенно (`>=0.9`) помечает конкретные public
  bundle fields как unsupported, Smart Update удаляет только перечисленные им
  поля и использует уже grounded title/raw excerpt вместо отбрасывания всего
  occurrence. `uncertain`, недословное evidence или пустой список unsupported
  остаются fail-closed; deterministic код не переписывает смысл.
- Для слабых VK/TG кандидатов-рубрик (`Дайджест`, `Афиша`, `куда сходить`,
  `посмотри/приходи` вместо площадки) Smart Update теперь делает отдельную
  LLM-first eventness проверку до создания события. Если LLM не подтверждает
  одно конкретное событие, кандидат fail-closed как `skipped_non_event`; regex
  здесь только маршрутизирует рискованный кейс в LLM, но не принимает
  смысловое решение сам.
- Для городских/фестивальных событий с тем же `title + date + time`, но
  разъехавшимся extracted venue, Smart Update расширяет shortlist перед
  матчингом, чтобы LLM увидела существующую карточку и решила merge/create.
  Это recall-only guardrail: он не схлопывает такие события без LLM.
- Reference normalization должна быть fail-closed для generic municipal venues:
  `Городской парк`, `зал`, `центр`, `культура/искусство` и похожие broad tokens
  не являются достаточным fuzzy evidence для привязки к известной площадке в
  другом городе. Curated aliases/exact name/address evidence всё ещё могут
  canonicalize venue, но одиночный generic token не должен менять city/venue.

### Каноничность сайта (/parse) при конфликтах

Источник сайта/парсера считается **каноническим** (trust high):

- если Telegram был импортирован первым, последующий `/parse` должен смержиться в тот же `event_id`;
- при противоречиях в “якорных” полях (дата/время/место) побеждает факт из `/parse` (сайт), а не Telegram.
- явные разные сеансы одного parser-источника на одну дату не склеиваются:
  если существующая карточка уже имеет `parser:<source>` provenance и оба
  времени заданы, несовпадающее время означает отдельную occurrence. Это не
  мешает официальному parser-кандидату исправить пустое/ошибочное время у
  карточки, которая до этого пришла только из VK/Telegram.

### Source-aware дедупликация в `/parse` (важно)

Для найденного в БД события `/parse` теперь проверяет, есть ли у него источник именно этого сайта (`parser:<source>`):

- если такого parser-источника **нет**, запускается Smart Update и источник сайта добавляется в `event_source` (это не “Пропущено”);
- если parser-источник этого сайта **уже есть**, выполняется лёгкий путь (ticket/link update) без лишнего LLM-мерджа;
- тот же лёгкий путь отдельно reconciles source-native `age_restriction`, чтобы
  оптимизация билетов/медиа не оставляла канонический возраст устаревшим;
- `⏭️ Пропущено` в отчёте используется только для реальных skip-статусов Smart Update (например `skipped_nochange`), а не для успешного merge.

Это снижает лишнюю нагрузку на LLM и делает отчёт `/parse` честным для E2E-проверок.

Structured возраст Qtickets/Pyramida/Дом искусств/филармонии передаётся в
`EventCandidate` и хранится в `Event.age_restriction`; text/OCR маркировка
остаётся semantic-решением Smart Update. Общий контракт:
`docs/features/event-age-rating/README.md`.

### Очередь обновления month/weekend страниц

- Для созданных/обновлённых событий `/parse` использует общий `schedule_event_update_tasks` (как и VK/TG), где `month_pages`/`weekend_pages` ставятся как debounce-задачи с `next_run_at = now + 15 минут`.
- Принятые активные parser-события считаются пользовательски ценными и по умолчанию попадают в managed Telegram/VK fanout (`skip_vk_sync=False`); исключения должны быть явными продуктово видимыми причинами (`silent`, cancelled/past/started, LLM/manual review blocker), а не скрытым page/calendar-only режимом.
- В финальном safeguard `_process_parsing_files` гарантирует постановку задач по затронутым месяцам и выходным, и тоже ставит их отложенно (`+15 минут`), чтобы не было немедленной пересборки Telegraph-страниц после массового прогона.

### Расписание автозапуска

- `ENABLE_SOURCE_PARSING=1` — включить ежедневный запуск.
- `SOURCE_PARSING_TIME_LOCAL=04:30` — локальное время запуска (HH:MM).
- `SOURCE_PARSING_TZ=Europe/Kaliningrad` — таймзона для локального времени.
- `ENABLE_SOURCE_PARSING_DAY=1` — включить дневной запуск.
- `SOURCE_PARSING_DAY_TIME_LOCAL=14:15` — локальное время дневного запуска (HH:MM).
- `SOURCE_PARSING_DAY_TZ=Europe/Kaliningrad` — таймзона дневного запуска.

Если значения не заданы, используется 04:30 по Europe/Kaliningrad. Дневной запуск пропускает Kaggle, если страницы источников не изменились с последнего успешного прогона.

Per-run debug logs в `/data/parse_debug/source_parsing_*.log` ограничены независимо от runtime mirror: `SOURCE_PARSING_DEBUG_RETENTION_DAYS=7` и `SOURCE_PARSING_DEBUG_MAX_TOTAL_MB=16`. Очистка удаляет только этот basename, не затрагивает `source_parsing_guard.json` и неизвестные operator files.

### Контроль покрытия и честный статус запуска

Theatres, Philharmonia и Qtickets запускаются параллельно, но каждый Kaggle
run-config пишет status ledger через отдельное короткое SQLite-соединение. Нельзя
начинать несколько `BEGIN IMMEDIATE` на общем cached `Database.raw_conn()`: это
теряет один или несколько источников с `cannot start a transaction within a
transaction`.

Основные parser kernels не зависят от создаваемого на каждый запуск Kaggle
status-dataset. Kaggle Dataset API — только вспомогательный transport для
callback telemetry; отказ `dataset_create_new`/upload token не должен блокировать
сам сбор источника. Для Theatres, Philharmonia и Qtickets используется host-side
kernel polling и `ops_run` report. Код Philharmonia kernel является
self-contained script: `kernel-metadata.json.code_file` указывает прямо на
`philharmonia_parser.py`, потому что Kaggle `kernels_push` загружает только
`code_file`, а не произвольные соседние файлы каталога.

Театр эстрады и Янтарь холл не используют Kaggle: их небольшие официальные
каталоги читаются host-side HTTP-парсерами параллельно с kernels. Театр эстрады
обходит все доступные месяцы билетного виджета; Янтарь холл следует bounded
Bitrix AJAX pagination до terminal page. Нулевой результат или HTTP/DOM failure
попадает в `result.errors` и не может завершить общий run ложнозелёным.

Создание нового parser occurrence повторяет только вызов Smart Update при
транзиентном SQLite `database is locked` / `database table is locked`:
по умолчанию до трёх попыток с задержками `2s`, `4s`
(`SOURCE_PARSING_DB_LOCK_RETRY_ATTEMPTS`,
`SOURCE_PARSING_DB_LOCK_RETRY_DELAY_SECONDS`). Иные ошибки не повторяются
вслепую. Это защищает длинный catch-up от краткого writer-lock фоновой задачи,
не превращая validation/semantic failures в ложный успех.
Внутреннее создание `event_media_pair_review` также использует idempotent
`ON CONFLICT DO NOTHING`: параллельный media worker не должен срывать
сохранение parser occurrence из-за гонки unique `pair_input_hash`.

Cheap refresh существующего parser event разрешён после собственной проверки
точных `date + explicit time + normalized title`. Если Smart Update ранее
стилизовал официальный заголовок, повтор всё равно может использовать cheap
refresh, но только при дополнительном точном совпадении canonical official
ticket URL, той же даты и того же явного времени. Одного совпадения
`parser:<source>`/host, URL без явного времени или fuzzy time недостаточно:
общий performance URL может содержать несколько сеансов, а legacy festival
aggregate — ссылки отдельных концертов. Такие случаи всегда возвращаются в
Smart Update identity gate для создания или разделения occurrence.
Если в этом exact replay одновременно не изменились `ticket_status` и
`ticket_link`, он считается полностью идемпотентным: parser обновляет только
provenance freshness, но не перестраивает Telegraph и не ставит публичные
страницы в очередь. Это исключает скрытый page-render LLM-вызов для каждого
элемента компенсирующего повтора; реальное изменение билетов сохраняет прежний
rebuild/scheduling contract.
После исключения same-parser explicit-time conflict последующие
city-noise/copy-post rescue-проходы не имеют права вернуть исключённый event в
shortlist: один performance/ticket URL не является доказательством одного
сеанса.

Если текущий официальный каталог снова публикует occurrence с явным
`ticket_status=available`, parser reconciliation переводит stale
`cancelled`/`postponed` lifecycle обратно в `active`. Это применяется только к
структурированному актуальному parser occurrence; `unknown`/`sold_out` не
реактивируют карточку.

`ops_run.status` для `kind=parse` отражает потерю источника:

- `success` — kernels и processing завершились без ошибок/failed items;
- `partial` — часть источников обработана, но есть kernel/parse ошибки или
  failed items;
- `error` — ошибки есть и ни один источник не дошёл до processing либо возникла
  фатальная ошибка.

Зелёный общий статус не заменяет source coverage. Для регулярной проверки нужно
смотреть `details_json.sources`, `errors_count`, дату последнего parser provenance
по каждому включённому источнику и ненулевое покрытие его официальной будущей
афиши. Канонический regression contract:
`docs/reports/incidents/INC-2026-07-27-future-event-source-coverage-drop.md`.

## Документация по источникам

- Театры (/parse): `docs/features/source-parsing/sources/theatres/README.md`
- Театр эстрады (бывший Дом искусств):
  `docs/features/source-parsing/sources/estrada/README.md`
- Янтарь холл: `docs/features/source-parsing/sources/yantarhall/README.md`
- Legacy URL-scoped спецпроекты Дом искусств:
  `docs/features/source-parsing/sources/dom-iskusstv/README.md`
- Pyramida: `docs/features/source-parsing/sources/pyramida/README.md`
- Третьяковка: `docs/features/source-parsing/sources/tretyakov/README.md`
- Филармония: `docs/features/source-parsing/sources/philharmonia/README.md`
- Qtickets: `docs/features/source-parsing/sources/qtickets/README.md`
- Universal Festival Parser: `docs/features/source-parsing/sources/festival-parser/README.md`
- Каноника по фестивальным сериям/выпускам и очереди: `docs/features/festivals/README.md`

## Артефакты

Все выгрузки/логи/результаты прогонов хранить в `artifacts/` (см. `artifacts/README.md`).

## Задачи

Связанные backlog items/планы/отчёты — в `docs/features/source-parsing/tasks/README.md` (без копирования контента).
