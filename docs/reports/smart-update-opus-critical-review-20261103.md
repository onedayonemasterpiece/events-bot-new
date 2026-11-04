# Smart Update: критический аудит (Opus), 2026-11-03

Ответ на [бриф](2026-11-03-smart-update-opus-audit-brief.md). Срез кода: `origin/main` `40a7dbbb9` (включает PR #791 и #792). Только аудит: прод, данные и требования не менялись, исправления делает Codex.

**Ограничения доступа.** Retained-артефакты `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/` недоступны аудитору (Permission denied). SSH-алиаса `runcoveer-production` у аудитора нет. Поэтому runtime SHA и prod env (`SMART_UPDATE_IDENTITY_GATE`, `SMART_UPDATE_MERGE_IDENTITY_GATE`) не проверены. Live-утверждения ниже взяты из брифа и incident-записи `INC-2026-11-02`, код проверен по `40a7dbbb9`.

**Общая оценка.** Механизм рабочий, и большая часть добавленного с июля защищает от реальных инцидентов (replay corruption 08-04, terminal loss 08-10, Тюнин 08-22). Переписывать его не нужно. Основные потери и неясности дают три вещи: **(1)** у identity-неопределённости нет исполнителя; **(2)** контракт финального арбитра требует согласовать слишком много полей; **(3)** адаптеры искажают причину отказа. Всё это исправляется небольшими правками. Остальное — долг, который можно не трогать.

Рост с июля: `smart_event_update.py` 17 003 → 23 774 строк (+40%). Добавились `smart_update_identity.py` (1 452), `smart_update_state.py` (631), `source_parse_contract.py` (990) и `event_operation_receipts.py` (297). С июля 120 коммитов в ядре и 873 коммита в `tests/`, 28 тестовых файлов с `smart_update` в имени.

---

## Главные выводы

### 1. P1 · подтверждено · identity-retry без исполнителя («чёрная дыра»)

| Поле | |
| --- | --- |
| Ущерб | Кандидаты с `identity_final_retry`, `identity_technical_failure`, `identity_semantic_unknown`, `dedup_adjudicator_technical_failure` остаются `RETRY_SCHEDULED` с `next_retry_at`, `completed_at=NULL`, но их никто не обработает. Примеры: «Мелодии любви» (cand. 25994), carrier 87615 (cand. 25953), inbox 86941/86953. Масштаб в базе не измерен: нужен `SELECT reason, count(*) FROM smart_update_candidate_state WHERE current_outcome='RETRY_SCHEDULED' GROUP BY reason`. |
| Доказательство | `smart_update_state.py:331-345,393-404` и `smart_event_update.py:2076-2084` сохраняют durable retry. Единственный потребитель — `scheduling.py:4439` за `SMART_UPDATE_RETRY_WORKER_ENABLED` (default 0). Ограниченный потребитель удалён в `bd8a1ee97` (2026-10-23, «unsolicited»). Докстринг `finish_candidate_attempt` всё ещё ссылается на «existing worker/operator path». Оператора по продукту нет (бриф, «что нельзя потерять»). |
| Корневая причина | Состояние «ожидает повтора» пережило удаление исполнителя. Код и метрики считают такие записи живыми, хотя фактически это терминал. |
| Минимальное изменение | **Нужно решение владельца**, одно из двух: **(а)** честный терминал — эти четыре причины закрывать как `FAILED_TECHNICAL` с сохранённым `reason`, `next_retry_at=NULL`, `completed_at` проставлен (удалить ветку `durable_identity_retry` и особый случай в `_terminalize_linear_result`); **(б)** оставить как есть и явно задокументировать, что повторный приход того же источника — единственный путь повтора. Не включать worker и не делать новый. |
| Сохранённые гарантии | Неуверенность по-прежнему не разрешает CREATE. Меняется только ярлык записи, домен не трогается. |
| Размер и риск | Около 20 строк в 2 файлах, плюс ветка в `source_parsing/telegram/handlers.py:7186`, тесты linear contract и docs. Без миграции. Существующие строки `RETRY_SCHEDULED` остаются как есть: не дренировать их без отдельного решения. |
| Проверка | Тест: `FINAL_RETRY` → строка state имеет `completed_at IS NOT NULL` и `next_retry_at IS NULL`. Отрицательный контроль: `FINAL_DISTINCT`/`FINAL_MATCH` не меняются. |
| Live acceptance | Новые identity-отказы больше не увеличивают число `RETRY_SCHEDULED`. Funnel и MCP показывают их как технический терминал с причиной. |

### 2. P1 · подтверждено · арбитр требует согласовать слишком много полей → ложные `distinct_not_grounded`

| Поле | |
| --- | --- |
| Ущерб | Production-решения 18882 и 18886 (confidence 1.0, `distinct_event`, цитаты и различия есть, но `reason_code=no_candidate_match`) и 19054 (create/unknown/1.0 при явном конфликте дат) дали `FINAL_RETRY` и попали в «чёрную дыру» из вывода 1. За два дня это латали промптом минимум трижды (PR #764, prompt follow-up по 87615, `b14f6d6b6`, `b6cbb5d1a`). |
| Доказательство | `_dedup_adjudicator_final_result`, `smart_event_update.py:13842-13878`: FINAL_DISTINCT требует одновременно `action=create`, `relation∈{distinct_*}`, `reason_code∈KEEP−{no_candidate_match}`, `confidence≥0.8`, дословные `evidence`, непустые `conflicts` и `match_id is None`. При этом `action`, `relation` и `reason_code` выражают одно и то же решение тремя полями. В `_DEDUP_ADJUDICATOR_KEEP_CODES` (`:2807`) **нет кода для «та же программа, другая дата»**, хотя промпт (`:13633`, `:13650`) прямо велит в этом случае вернуть `distinct_occurrence` «с соответствующим keep-кодом». Модели приходится выбирать неподходящий код. |
| Корневая причина | Решение размазано по избыточным полям, а guard проверяет их совпадение. Каждое рассогласование небольшой модели становится ложной неопределённостью. |
| Минимальное изменение | Источник смысла — `relation` плюс дословные `evidence` и `conflicts` плюс `confidence`. `reason_code` оставить диагностикой: убрать его из условия `grounded_distinct`, оставив запрет только на `relation=unknown`. **Либо**, минимальнее, добавить keep-код `date_occurrence_keep` в enum и схему. `action` при `relation=distinct_*` и `match_id=None` считать `create`. Порог, проверку цитат и `conflicts` не трогать. |
| Сохранённые гарантии | `unknown` по-прежнему не создаёт. Без дословных цитат и конкретного различия create не разрешается. Merge-ветку (`action=match`) не трогать. |
| **Риск — внимательно** | Для случая A («Мелодии любви», 19099) в базе уже лежит, по всей видимости, перепутанная пара: 9775 «Мелодии» на 13.11 и 9815 «Кино» на 14.11. Если 19099 провалился **только** из-за `reason_code`, после правки появится карточка «Мелодии любви» 14.11 рядом с ошибочной 9775 на 13.11. Перед мёржем нужно посмотреть 19099 и понять, какое условие не выполнилось. Правка устраняет ложную неопределённость, но не исправляет старые неверные данные. |
| Размер | 3–10 строк в одной функции, плюс enum/схема при варианте с новым кодом. |
| Проверка | Офлайн-переигровка сохранённых решений 18882/18886/19054 через `_dedup_adjudicator_final_result`: до правки `FINAL_RETRY`, после — `FINAL_DISTINCT`. Отрицательные контроли: August identity corpus (`e4bb793a6`) и same-event пары остаются match/retry; решение с `relation=unknown` и 1.0 остаётся retry. |
| Live acceptance | Доля `distinct_not_grounded` среди decision log падает, количество дублей за сутки (exhibition duplicate audit и обычный дубль-мониторинг) не растёт. |

### 3. P1 · подтверждено · MCP подменяет причину: `EVENT_REQUIRED_FIELDS_MISSING` как catch-all

| Поле | |
| --- | --- |
| Ущерб | Identity-отказ «Мелодий любви» снаружи выглядел как отсутствие обязательных полей. Это вводит в заблуждение и партнёра, и Codex. |
| Доказательство | `private_events_mcp/event_create_adapter.py:222-229`: всё, что не accepted, не retry и не `CONFIRMED_NO_EVENT`, получает `EVENT_REQUIRED_FIELDS_MISSING`, хотя `candidate_receipts` с настоящим `reason` уже собраны выше (`:178`). |
| Минимальное изменение | Если в `candidate_receipts` есть неакцептованная запись, вернуть, например, `SMART_UPDATE_NOT_ACCEPTED` и `reason` из receipt. `EVENT_REQUIRED_FIELDS_MISSING` оставить только для `missing_date`/`missing_title`/`missing_location`. Около 10 строк. |
| Проверка | Тест адаптера с receipt `identity_final_retry` возвращает не `EVENT_REQUIRED_FIELDS_MISSING`. Отрицательный контроль: `missing_date` по-прежнему `EVENT_REQUIRED_FIELDS_MISSING`. |

### 4. P2 · подтверждено · `EventOperationReceiptError` оставляет попытку открытой и блокирует повтор на час

| Поле | |
| --- | --- |
| Доказательство | `smart_event_update.py:17462-17465`: `except EventOperationReceiptError: raise` выходит из фасада до `finish_candidate_attempt`. Попытка остаётся `RETRY_SCHEDULED` с lease 3600 с (`begin_candidate_attempt`, default `lease_seconds=3600`). Lease owner по умолчанию — `pid:id(task)`, поэтому повтор той же MCP-операции из другой задачи получает `candidate_attempt_in_progress` на час. Потом попытку закроет только `interrupted_before_ack` при следующем begin. |
| Ущерб | Влияние не измерено, касается только операций с `event_operation_context` (MCP/partner). |
| Минимальное изменение | Перед `raise` закрыть попытку через существующий `finish_candidate_attempt(FAILED_TECHNICAL, reason="event_operation_receipt_error")`. Доменная транзакция к этому моменту уже откатилась исключением. Около 8 строк. |
| Проверка | Тест: impl бросает `EventOperationReceiptError` → второй вызов из другой задачи не получает `CandidateAttemptInProgress`. Отрицательный контроль: исключение по-прежнему доходит до вызывающего. |

### 5. P2 · подтверждено · одна нормализация исхода в четырёх местах

`RETRY_SCHEDULED` помечен как legacy (`smart_update_state.py:22-25`), но всё ещё создаётся в 38 местах `smart_event_update.py`. Потом его последовательно переводят в другой исход: `_terminalize_linear_result` (`:2066`), затем `finish_candidate_attempt` (`smart_update_state.py:343-349`), затем адаптер (`source_parsing/handlers.py:829-832`). Вдобавок `SmartUpdateResult.__init__` маппит legacy `status`, а `@property status` отдаёт его обратно (`:1976`). Обращений к `.status` вне тестов — около 107. Поведение сейчас верное, но каждый новый слой — место для расхождения: например, ветка TG `RETRY_SCHEDULED` в `source_parsing/telegram/handlers.py:7186` расходится с адаптером парсеров, который тот же исход считает FAILED. **Сейчас не трогать.** После вывода 1 особая ветка `RETRY_SCHEDULED` в адаптерах станет мёртвой — тогда её можно удалить (см. список удаления).

### 6. P2 · подтверждено · ack-failure прячет принятое событие

`smart_event_update.py:17510-17560` и `terminalize_candidate_ack_failure`: если после успешной доменной записи и `schedule_event_update_tasks` трижды не удалось записать ack, вызывающий получает `FAILED_TECHNICAL`, а `event_id` становится только диагностическим. Докстринг ждёт «operator reconciles», но оператора нет. Событие и задачи публикации при этом уже существуют, то есть метрика врёт в сторону «потеряно». Это редкий случай (SQLite, 3 попытки), а повторный приход пакета даст exact replay. **Не менять**, исправить только докстринг. Можно возвращать `event_id` в `diagnostic_event_id` — это уже сделано.

### 7. P2 · подтверждено как хрупкость, влияние не измерено · exact replay зависит от кэша парсера

`input_packet_fingerprint` (`smart_update_identity.py:1331`) включает поля, которые выдал **LLM-парсер** (`title`, `date`, `time`, `location_*`, `city`, `event_type`, цены…), а не только сырой источник. `occurrence_key` строится из распарсенных `date/end_date/time` (`:1280-1293`). Значит, идемпотентность держится на неизменном frozen parse receipt (`source_parse_contract.py`). Если он потерян или неполон (случай C, исправлен PR #783), повтор того же поста идёт полным семантическим проходом. Если парсер выдал другое время, получается вообще новый `candidate_key`. Постер без хеша байтов берётся по URL (`:1380-1386`). **Не менять сейчас.** Нужны данные: как часто для одного `canonical_source_url` бывают разные `source_fingerprint`/`occurrence_key` без правки поста. Если редко — достаточно round-trip теста кэша (уже есть после #783).

### 8. Гипотеза · не хватает данных · «Зеленоградск → Калининград» в подготовленном кандидате

Проверено офлайн: `location_reference.normalise_event_location_from_reference` правильно оставляет Зеленоградск, если в названии есть «Солёная ворона» (с ё и без). Alias «Железнодорожные ворота» защищён (`smart_event_update.py:9077-9082`). Если же название площадки не распознано, а город уже «Калининград», эталон по одному адресу город не исправляет. Скорее всего, ошибка пришла раньше Smart Update: город от парсера или дефолт профиля источника (`_restore_configured_telegram_location`, проверка `tg_default_city` около `:18036`). Нужен receipt кандидата 25994: поля `location_name/city` на входе фасада и после `_canonicalize_location_fields`.

### 9. Наблюдение · не менять · глобальная блокировка на весь проход

`_SMART_UPDATE_LOCK = asyncio.Lock()` (`:556`) сериализует весь `_smart_event_update_impl` вместе со всеми LLM-вызовами. На путь create их может быть 10–15 последовательно: eventness, scope, anchor, location, match bundle, match, adjudicator, create bundle, grounding review, title recovery, facts, fact-first, reflow, shrink, digest, short. Один медленный Gemma-ответ (~101 с) задерживает весь импорт. Внутри процесса lock делает избыточными часть механизмов гонок (lease, `SourceBindingConflict`, final probe), но они нужны для межпроцессных CLI. **Не трогать без trace-данных:** это защита, и её снятие — большой риск. Повторные вызовы eventness (`:18312`/`:18805`) и facts (`:20578`/`:20883`) проверены: они взаимоисключающие или fallback, это не дубли.

---

## Карта достижимых исходов (фактическая)

| Исход impl | `_terminalize_linear_result` | `finish_candidate_attempt` (ledger) | Кто потребляет |
| --- | --- | --- | --- |
| CREATED / MERGED / NOOP | без изменений | accepted, `completed_at` | адаптеры: успех, JobOutbox уже поставлен |
| REJECTED + typed reason | без изменений | terminal | адаптеры: skip |
| REJECTED без typed reason | — | → FAILED `product_reason_untyped` | — |
| RETRY + 4 identity-причины | **без изменений** | **RETRY_SCHEDULED, `next_retry_at`+5 мин, без completed** | **никто** (worker off) — вывод 1 |
| RETRY + причина из product map | → REJECTED | terminal | skip |
| RETRY прочие (scope/anchor/location/source verification ×9, binding conflict, integrity…) | → FAILED | terminal | TG: terminal_error. Парсеры: FAILED |
| Исключение `EventOperationReceiptError` | — | **попытка открыта, lease 1 ч** | вывод 4 |
| Ack не записан ×3 | — | FAILED `candidate_state_ack_failed`, событие есть | вывод 6 |

Расхождения кода и документации: комментарий у enum («converts this value to FAILED_TECHNICAL rather than scheduling background work») неверен для четырёх identity-причин. Докстринг `finish_candidate_attempt` и `terminalize_candidate_ack_failure` ссылается на worker/оператора, которых нет.

## Список удаления

| Что | Почему можно | Когда |
| --- | --- | --- |
| Ветка `durable_identity_retry` в `finish_candidate_attempt` и особый случай в `_terminalize_linear_result` | Исполнителя нет, это ложное состояние | Вместе с выводом 1 (вариант а) |
| Ветка `RETRY_SCHEDULED` в `source_parsing/telegram/handlers.py:7186` и remap в `source_parsing/handlers.py:829` | После вывода 1 фасад не возвращает RETRY | После вывода 1 |
| `reason_code` как условие допуска в `grounded_distinct` | Дублирует `relation`, источник ложных retry | Вывод 2 |
| Блок `scheduling.py:4439-4510` (worker за флагом) и `retry_due_smart_update_candidates` / `claim_due_candidates` | Выключены, владелец запретил включение. Держать только если нужен ручной дренаж старых строк | Только по решению владельца, не сейчас |
| Legacy `status` bridge | 107 обращений — большая механическая правка без продуктовой ценности | **Не сейчас** |

## Последовательность PR (каждый независим и откатывается отдельно)

1. **MCP причина** (вывод 3): около 10 строк, без риска для импорта.
2. **Закрыть попытку при `EventOperationReceiptError`** (вывод 4): около 8 строк.
3. **Арбитр: `reason_code` не гейт** (или код `date_occurrence_keep`) (вывод 2): сначала разобрать решение 19099 и прогнать офлайн-переигровку 18882/18886/19054 и August corpus. После релиза сутки следить за дублями.
4. **Identity-retry → честный терминал** (вывод 1): только после решения владельца (а/б).
5. **Удалить мёртвые RETRY-ветки в адаптерах** (вывод 5): после PR 4.

PR 1–2 только делают диагностику честной и не уменьшают пропуски событий. Реально на пропуски влияет PR 3, PR 4 делает их видимыми.

## Что сознательно не менять сейчас

Identity gate, merge gate, final probe, частичные уникальные индексы `event_source`, lease и `_SMART_UPDATE_LOCK`. Набор pre-identity LLM-ревью (scope, anchor, location, eventness). Состав fingerprint. Legacy `status`. Пороги confidence и проверку дословных цитат. Не включать worker, не дренировать старые `RETRY_SCHEDULED`, не делать повтор целых пакетов (случай C).

## Нужные данные для следующего прохода

1. Распределение `RETRY_SCHEDULED` по `reason` и возрасту в prod.
2. Decision 19099: какое условие `grounded_distinct` не выполнилось.
3. Receipt кандидата 25994: поля location/city до и после канонизации.
4. Доля `source_fingerprint`/`occurrence_key` > 1 на один `canonical_source_url` за 30 дней.
5. Prod env: `SMART_UPDATE_IDENTITY_GATE`, `SMART_UPDATE_MERGE_IDENTITY_GATE`.
6. 2–3 trace source → terminal с таймингами по стадиям (для вывода 9).

## Новые сущности → обоснование

Аудит **не предлагает** новых состояний, флагов, очередей, retry, worker или кэшей. Единственное возможное добавление — keep-код `date_occurrence_keep` (вариант вывода 2). Сбой: решения 18882/18886/19054. Существующий механизм: промпт уже требует такой код, но в enum его нет. Решение: добавить код либо убрать `reason_code` из гейта, но не делать и то и другое.

---

## Проход 2026-11-04 (утро)

Срез: `origin/main` `4571309e2`. Prod runtime, по данным координационного треда: `3bb038338`, оба identity gate = `enforce`, `SMART_UPDATE_RETRY_WORKER_ENABLED` не задан. Аудитор этого сам не проверял: SSH-алиаса и доступа к retained-артефактам по-прежнему нет, включая `smart-update-locked-facts-audit-20261104.md`. Раздел про locked facts ниже основан только на коде.

### Сверка рекомендаций

| № | Рекомендация | Состояние |
| --- | --- | --- |
| 1 | identity-retry без исполнителя | **Не сделано, нужно решение владельца.** Масштаб от владельца: `identity_final_retry` 2268 строк кандидатов (22.08–03.11), `identity_technical_failure` 23, `attempt_started` 33. Это не число потерянных событий, но в разы больше, чем можно разобрать вручную. Приоритет P1 подтверждён. |
| 2 | убрать `reason_code` из гейта арбитра | **Отозвано аудитором.** PR #797: на решении 19099 снятие гейта даёт `FINAL_DISTINCT` для спорной пары дат одного концерта. Добавлять keep-код для «другой даты» тоже нельзя: он пропустит тот же случай. Гейт оставить как есть. Ложные retry для разных программ (18882/18886) уже закрыты промптом в #764. Для конфликта дат честный исход — отказ, и проблема сводится к п. 1. |
| 3 | MCP catch-all | **Сделано, PR #796** (`8fb57849c`, +4 строки). Проверено: `EVENT_REQUIRED_FIELDS_MISSING` остаётся, только когда *все* receipts — `missing_*`; при пустых receipts возвращается `SMART_UPDATE_NOT_ACCEPTED`. Новых состояний нет. Вмёржено, **в прод на 23:04 UTC не переключено** (ждали конца TG 11606). |
| 4 | закрыть попытку при `EventOperationReceiptError` | Не сделано. P2, не блокирует. |
| 5–9 | долг / не трогать | Без изменений, ожидаемо. |

### Ревью новых изменений

- **#796** — корректно, минимально, тест различает оба кода. Замечаний нет.
- **#798** (`02bc9486e`, +3 строки в `scheduling.py:2882`): watchdog не запускает catch-up VK, пока в `_run_meta` есть `vk_auto_import_*`. Проверено: `_run_meta` очищается на EXECUTED/ERROR/MISSED (`scheduling.py:4306-4314`) и живёт в памяти процесса, так что после рестарта запись не «залипает». Если задача повиснет, catch-up не придёт — но это и есть нужное поведение (не запускать второй пакет параллельно). Рабочий механизм не сломан.
- **#797** — только документация, контрпример полезный. Он закрывает мою рекомендацию 2.

### Запрос владельца: 9779 — Telegraph сохраняет «Верни мне музыку» после принятого переименования

**Причина (по коду, подтверждено).** У `event_source_fact` нет замещения: факты только дописываются, статус `added` вечный (`models.py:1237-1242`). Это одновременно ломает два места:
1. Merge-путь Smart Update: fact-first писатель получает `facts_before_list` — все прежние `added/duplicate` факты события (`smart_event_update.py:21730-21754`), в том числе факт со старым названием. Заголовок при этом уже новый (`event_db.title`, `:21625` и `:21880`), а тело пишется по старым фактам.
2. MCP edit: `locked_facts` — последние 100 фактов `added/duplicate` без учёта источника и свежести (`private_events_mcp/event_commands.py:96-109`). `canonical_text_gate` (`:707`) **не передаёт модели текущий `title`**: его нет в `keys`. Модель видит старый факт и новое название и честно отвечает `conflict`.

**Минимальное изменение (существующие механизмы, без новых таблиц, состояний и worker).** Статус `conflict` уже определён как «ignored change» (`models.py:1240`), и оба потребителя уже фильтруют `IN ('added','duplicate')`. Значит, замещение — это просто перевод в существующий статус:

- В двух местах, где Smart Update **уже принял** смену названия (`title_updated_from_authoritative_child`, `:21624`; `title_updated_from_generic`, `:21879`), в той же транзакции:
  1. Факты этого события из **других** источников, содержащие **полную старую строку названия** (casefold, точное вхождение, а не regex и не похожесть), перевести в `status='conflict'`.
  2. Записать один `note`-факт через существующий `_record_source_facts`: «Название обновлено: «old» → «new»». Это и есть provenance.
  3. Отфильтровать те же факты из уже загруженного `facts_before_list`. Внимание: generic-путь (`:21879`) выполняется **после** загрузки списка (`:21754`), поэтому одной правки статусов в БД мало для текущего прохода.
- Добавить `"title"` в `keys` `canonical_text_gate`, чтобы проверка сравнивала текст с текущим каноническим названием.

Семантическое решение («это то же событие, название сменилось») уже принято существующим identity/merge-контуром. Правка лишь перестаёт противоречить ему в двух потребителях. Оценка: один helper примерно на 25 строк плюс одна строка в `keys`. Миграции нет. Откат — revert; факты в статусе `conflict` при этом остаются читаемыми и восстанавливаемыми.

**Сохранённые гарантии.** Если identity не принял merge (FINAL_RETRY/DISTINCT), если смена названия отклонена (`semantic_title_mismatch`, `:21896`) или если это первичное создание — ничего не замещается. Факты из того же источника, который принёс новое название, не трогаются. Дата, время, площадка и цена не затрагиваются: замещается только строка названия.

**Риск.** Факт вида «В программе «Верни мне музыку» прозвучат…» уйдёт из канона целиком, вместе с полезной частью. Для переименованной программы это скорее правильно, но перед мёржем стоит проверить на 9779, что из тела не пропадает ничего, кроме старого названия.

**Различающие проверки (изолированная БД, без LLM для статусов).**
- Положительный rename control: replay принятого merge 9779 → факты со «Верни мне музыку» стали `conflict`, появился `note` о переименовании, в `facts_text_clean` для писателя нет старого названия, `canonical_text_gate` на теле с «Песни о любви» возвращает `consistent`. До правки — `conflict` (воспроизведено владельцем).
- Отрицательный disputed-date control: пара 9775/«Мелодии любви» 14.11 (решение 19099) → `FINAL_RETRY`, статусы фактов 9775 не изменились.
- Отрицательный control: `semantic_title_mismatch` → статусы не изменились.

**Live acceptance.** Следующий принятый MERGED с переименованием: публичное тело Telegraph без старого названия, MCP edit этого события не получает `PUBLIC_TEXT_FACT_CONFLICT` из-за старого названия. Для самой 9779 правка сама ничего не исправит, повтор принятой операции не нужен. Достаточно разовой операторской смены статуса одного факта (194844) тем же правилом — решение за владельцем.
