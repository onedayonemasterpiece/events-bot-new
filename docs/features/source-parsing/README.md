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

### Typed LLM-first boundary для VK/TG и official parsers

Source semantic parse возвращает `SourceParseDecision`, а не неразличимый
`events=[]`. `CONFIRMED_NO_EVENT` допустим только из валидного structured LLM
response при complete evidence; empty/malformed/truncated/provider failure или
неполный OCR дают retry. Regex/date/history/giveaway/cancellation сигналы — hints
или причины conditional verifier, но не terminal filters. Positive children не
удаляются downstream guardrail-ами; field conflict очищает только неподтверждённое
поле либо вызывает verification/retry.

Все доступные source text/OCR blocks представлены в semantic parse и отражены в
`EvidenceManifest`. Multi-event/multi-session source сохраняет все children, а
mixed lifecycle + new events обрабатывается независимо. Telegram, VK, parser,
ticket/festival и manual adapters считают успехом только typed accepted Smart
Update result; diagnostic ID не запускает downstream work.

Official parser occurrence key стабилен: source-native/vendor identifier имеет
приоритет, затем structured date/end-date/time schedule anchor, а producer
ordinal — только tie-breaker. Поэтому перестановка siblings или новый первый
сеанс не перепривязывает старые Events. Технический/identity retry создаёт
идемпотентный `source_parser_recovery_request`, который scheduled parser
подбирает автоматически до разрешения.

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
- Принятые активные parser-события по умолчанию попадают в managed Telegram/VK fanout (`skip_vk_sync=False`); downstream запускается только из typed accepted result. Retry/diagnostic ID не является product blocker или успехом.
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

Создание parser occurrence может сделать несколько коротких локальных повторов
Smart Update при transient SQLite lock (`SOURCE_PARSING_DB_LOCK_RETRY_*`). Любая
оставшаяся technical/identity/schema uncertainty не становится skip/error
terminal: она увеличивает `retry_scheduled` и upsert-ит source-level
`source_parser_recovery_request` с due time. Scheduled parser повторяет полный
официальный каталог идемпотентно; product/validation uncertainty не маскируется
ложным успехом.
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
Специализированные processors `philharmonia` и `qtickets` обязаны использовать
тот же exact-slot verifier до Smart Update; отдельный legacy
`event_has_parser_source` shortcut для них не является допустимой границей
identity/idempotency.
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
