# Голосовой поиск KenigEvents: продуктовая оценка и техническое видение

Дата: 2026-09-05. Статус: **предложение для реализации, не принятый новый runtime-контракт и не release evidence**.

Документ продолжает [PR #587](https://github.com/onedayonemasterpiece/events-bot-new/pull/587). Ничего не включает в production, не меняет текущие лимиты, Auth, UI foundations, STATUS или задачи [#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621). Значения TTL и бюджетов ниже — стартовые предложения для проверки, не ранее согласованные требования владельца. Документ — аналитический companion, а не второй источник нормативных требований. После принятия решения переносятся адресно в owning contracts; этот отчёт остаётся обоснованием.

## 1. Что прочитано и что действительно существует

Прочитаны полные расшифровки трёх новых голосовых, а не только автоматически составленные разделы «Решения»:

- [09:29:49: сценарии, интерфейс, Gemini Lite, персонализация](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260905-092949-7ab7703f.md).
- [09:43:41: thin Fly и возможный сервис на Devstand](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260905-094341-67f72ad8.md).
- [09:47:37: рекомендации от источников и экономный availability manifest](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260905-094737-dfa8cdc1.md).

Учтены также [UI-review 00:21](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260905-002114-a0677098.md), [UI-review 00:32](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260905-003237-8a342775.md) и уточнения владельца в текущем запросе: продолжение речи во время обработки, видимый прогресс, память, единый контроль лимитов, Qwen-фикстуры, GitHub Actions.

Исследованный source snapshot events-bot-new: `b8f463f5c35fa62befcfed171a7a8a0886af20f7`. Документы PR #587 прочитаны на `f78e7c5974b4192bddf9eea901ee6d8b57f51560`; это открытый draft, не merged/runtime feature. Его существующая ветка на момент чтения не mergeable: интеграция позже требует fresh-read и разрешения расхождений, а не безусловного merge.

### Повторно использовать

- [Canonical Search](../features/static-site-pages/smart-vector-search/README.md): общий Auth/transport, `selected_once`, bounded JSON, existing retrieval/cache/quota, canonical EventCard snapshots. Обычный умный поиск доступен после входа; гость не отправляет cost-bearing запрос, после входа нужен явный повторный submit.
- [Agent-assisted discovery](../features/static-site-pages/smart-vector-search/agent-assisted-event-discovery.md): гибридный режим, не чат вместо выдачи; direct first-party API, а не MCP в пользовательском hot path; bounded model use, проверенные IDs и facts, classic fallback.
- [Location directory proposal](../features/location-directory/README.md): локации, координаты, provenance и безопасные map actions. Нельзя выдумывать адрес, часы или маршрут при отсутствии факта.
- [Personalization ownership](../architecture/personalization-data-ownership.md): Supabase владеет identity/profile/consent/favorites; YDB не является вторым профилем; Fly SQLite остаётся canonical event owner, Supabase — ограниченная поисковая/карточная проекция.
- [Autotest strategy](../operations/static-site-autotest-strategy.md), [scenario registry](../testing/static-site-autotest-scenarios.v1.yml), [ci.yaml](../../.github/workflows/ci.yaml), [search-production-health.yml](../../.github/workflows/search-production-health.yml).
- [AuthorizedEventSearch.astro](../../site/src/components/AuthorizedEventSearch.astro): уже есть skeletons/status и общий Auth; в прочитанном UI текст ограничен 180 символами. Поэтому голосовая реплика не должна молча обрезаться и притворяться обычным legacy query.
- [Search backend](../../supabase/functions/event-search/index.ts) и [shared Google quota adapter](../../supabase/functions/event-search/google-quota.ts): versioned search response, counters, revision-aware state; limiter `google_ai_project_model_atomic_v1`, strategy `rolling_60s_pacific_day_v2`.

В Search README сохранился датированный 2 августа incident/NO-GO текст. Это не доказательство недоступности поиска сегодня: в текущем коде уже есть более поздние revision-aware изменения и health workflow. В этом анализе не проводился живой authenticated поиск; текущий production verdict не присваивается.

## 2. Продуктовое решение

**Строить голосовой способ выразить и уточнить намерение поверх существующей выдачи. Не строить универсального автономного ассистента.**

Ценный сценарий: «В субботу вечером с ребёнком, бесплатно, не концерт» → видимые распознанные условия → обычные карточки → «Можно и платно, до пятисот рублей, но только в Калининграде» → обновлённая выдача без повторения предыдущей фразы.

### В первый функциональный пакет

Голос и текст в одном поисковом состоянии; исправляемая расшифровка; структурированные условия; уточнения и догон; прежняя выдача во время обновления; понятные состояния; короткая память задачи; общие карточки и их существующие CTA; честное отсутствие результатов и отдельно предложенное ослабление условий; availability manifest и безопасный fallback.

Микрофон — альтернативный ввод в `/poisk/` и предусмотренной общей точке входа Floating Island. Помощь при нулевой/неудачной выдаче сохраняет rescue-first гипотезу #587. Доступность голосового ввода сама по себе не требует превращать каждую страницу в чат.

Плашка-пример в поиске должна явно различать «подставить запрос» и «открыть готовую подборку». Подстановка не должна скрыто запускать платный POST, особенно после OAuth. Это закрывает конкретную неоднозначность UI-review 00:32.

### Позже, отдельно

Сравнение событий; проверенные сведения о площадке; явное сохранение интересов; голосовые команды к выбранной карточке; уведомления через существующий subscription/consent контур; рекомендации редакторов и источников, если появились проверенные данные.

«Что рекомендует Культурная чайка?» — потенциально полезный фильтр, а не повод изображать человека или придумывать его вкус. Нужна подтверждённая связь event ↔ source evidence с типом связи. Простое упоминание, рекламный анонс и рекомендация не тождественны. Если подтверждений нет, ответ: «В наших данных нет подтверждённых рекомендаций этого источника». Будущая фильтрация читает тот же source/medallion evidence, не второй ручной справочник одобрений.

### Не включать сейчас

Озвучивание каждого ответа; непрерывное фоновое прослушивание; универсальные ответы из Интернета; open-ended agent/MCP tool loops; автоматические записи/подписки по неоднозначной речи; платную подписку только ради MVP; полную реплику каталога на Devstand; отдельные очереди, профили и лимитеры ради ассистента.

«Три гостевых запроса» — гипотеза из голосового, противоречащая текущему auth-only Search contract. Сейчас сохраняется auth-only. Тест гостевого доступа возможен позже через явную поправку policy, общий лимитер и оценку abuse/cost. Не закреплять число 3 без данных.

## 3. Существенный gate до публичного запуска

Проверены официальные [Gemini API terms](https://ai.google.dev/gemini-api/terms), действующие с 23 марта 2026, и [available regions](https://ai.google.dev/gemini-api/docs/available-regions).

Terms распространяют региональные ограничения не только на сервер, но и на предоставление API Clients пользователям. России нет в опубликованном списке; также есть ограничение сайтов/приложений, направленных на аудиторию младше 18 лет или вероятно доступных ей. Для публичной калининградской афиши это существенная продуктовая зависимость. Размещение сервера в другой стране само по себе не доказывает соответствие этим условиям. Нужно проверить допустимый договорный/провайдерский путь до включения функции; при его отсутствии владелец отдельно пересматривает ограничение «только Gemini Lite». Не подменять это скрытым VPN/прокси или автоматической сменой модели.

У unpaid-сервиса отдельные условия обработки входов, включая использование для улучшения продуктов; документы предупреждают не передавать personal/confidential/sensitive information. Нельзя считать реальный пользовательский голос синтетической тестовой фразой. Проверить конкретный billing/project/data-processing режим. Удаление своей аудиокопии не удаляет автоматически данные у провайдера.

В existing personalization ownership отдельно указано, что более широкий Supabase Auth/email/profile flow ещё требует localization/data-flow аудита. Это видение не объявляет соответствие 152-ФЗ. Реализация на mocks и синтетических данных может идти параллельно; public provider enablement остаётся отдельным gate.

## 4. UX без информационного голода

Не накладывать новый независимый плавающий чат поверх Floating Island. Предпочтение: существующая общая точка входа разворачивается в компактный composer; в выдаче остаются обычные EventCard/AdaptiveEventCardGrid. На мобильном проверить safe-area и экранную клавиатуру; не перекрывать CTA и не сбрасывать scroll/focus при ответе.

Нужны две независимые шкалы состояния:

| Захват | Обработка |
|---|---|
| Выключен / слушаю / пауза / отказ микрофона | Нет запроса / передаю / распознаю / ищу / обновляю / готово / ожидание квоты / ошибка |

Запись реагирует на реальный входной уровень звука. Сетевое ожидание имеет иной индикатор и подпись. «Ищу события; можно дополнить запрос» и «Дополнение принято» полезнее одного бесконечного spinner.

При первой выдаче — общие skeletons; при уточнении — предыдущие карточки плюс пометка обновления. Текст запроса и подтверждённые ограничения всегда доступны. Расшифровка не считается услышанной до её получения; VAD-пульсация не выдаётся за ASR.

В MVP сохранить JSON transport. UI знает стадию собственного upload/interpret/search запроса и показывает честный indeterminate progress. Не изображать точный процент или внутреннюю стадию, которую backend не сообщил. SSE/NDJSON не нужны только ради красивого loader и требуют отдельного общего transport gate.

Нужны screen-reader status, управление клавиатурой, reduced motion, доступный текст ошибки и возможность продолжить обычным текстом. Звук ответа по умолчанию не включать.

VAD не должен обрезать начало слов и короткое «не». Предусмотреть небольшой pre-roll/post-roll и явно различать паузу речи и завершение мысли. Значения подбираются по записанным fixtures. MIME проверяется через browser capabilities и серверный allowlist; транспортные chunks не считаются автоматически отдельными декодируемыми файлами.

Фоновую запись при погашенном экране не обещать: [MediaRecorder](https://developer.mozilla.org/en-US/docs/Web/API/MediaRecorder/dataavailable_event) имеет платформенные задержки/приостановки. Подтверждённый контекст восстанавливается, незавершённая запись не маркируется отправленной. Долговечную очередь аудио проектировать отдельно при реальной необходимости: текущий общий browser-state budget 64 KiB и outbox не являются хранилищем голосовых файлов.

## 5. Догон: не терять речь и не применять устаревший ответ

**Отменяем устаревший поиск/показ, а не историю реплик.** Захват новой речи разрешён во время сетевой обработки. Это не означает неограниченную параллельную рассылку запросов модели.

Минимальные сущности/поля, названия предлагаемые:

```text
conversation_id + authenticated owner
conversation_epoch               # новый поиск / очистка / смена identity
utterance_id + client_sequence   # идемпотентность и порядок
accepted_sequence                # всё принятое сервером
processed_sequence               # всё последовательно интерпретированное
context_revision                 # CAS изменения intent
result_set_id                    # какая выдача была видна для «второе»
catalog_revision / policy_version
```

Пример: U1 «на выходных бесплатно», U2 «лучше вечером», U3 «и без концертов». U2/U3 захватываются пока идёт U1. Семантическая обработка реплик одной беседы упорядочена. Несколько ещё не отправленных сегментов можно объединить в один ограниченный запрос, сохраняя их идентификаторы и порядок. Уже отправленное аудио без необходимости не пересылать повторно.

После завершения интерпретации U1 её текст и intent сохраняются, даже когда поиск U1 уже устарел. Затем U2 интерпретируется с правильной базовой revision. Retrieval запускается для наиболее свежего полного intent; более старый результат не перезаписывает новые условия. Если U1 завершился ошибкой, U2 не должна молча потерять необходимую базовую фразу: показать недостающий фрагмент и предложить исправление/безопасное восстановление.

Сервер обеспечивает атомарную проверку owner, epoch, sequence и revision. Одинаковый utterance_id с тем же payload даёт существующий receipt; с другим payload — конфликт. `AbortController` — оптимизация клиента, не доказательство отмены уже отправленного provider call. Sent/unknown попытка остаётся в cost ledger. После неопределённого timeout узнаём статус существующей operation, не делаем второй cost-bearing POST по другому маршруту.

Полный exactly-once внешний эффект при аварии между provider response и сохранением не обещать. Локальный эффект идемпотентен; неоднозначное внешнее исполнение честно отражается статусом и не переигрывается автоматически.

Для нескольких вкладок: отдельные conversation IDs по умолчанию; одна общая беседа требует optimistic concurrency. «Новое обсуждение», logout, удаление контекста делают поздние ответы недействительными. «Второе событие» привязывается к показанному `result_set_id`, а не к списку, успевшему поменяться.

Очередь ограничена длительностью/байтами, но UI не навязывает пользователю ожидание каждого ответа. При исчерпании ёмкости/квоты надо явно сообщить, что новая запись не принята, а не продолжать обманчиво слушать. Размеры — эксплуатационная настройка после замера, не скрытое ограничение длины естественной мысли.

## 6. Память и Supabase

Не хранить бесконечную переписку и не строить второй vector memory index. Хранить компактную текущую задачу: цель, hard/soft constraints, временные границы, locality/location IDs, бюджет и валюту, исключения, последние показанные/отклонённые IDs и небольшой хвост реплик. В каждом условии нужны происхождение и scope: текущая задача или явно сохранённое предпочтение.

Предлагаемые стартовые сроки:

| Слой | Срок/поведение | Содержание |
|---|---|---|
| Активная задача | 30 минут бездействия; без неявного переноса через сутки | Структурированный intent и небольшой хвост последних реплик |
| Возврат к задаче | До 7 дней для вошедшего пользователя при разрешённом сохранении; после паузы явное «Продолжить предыдущий поиск?» | Только краткое состояние/summary, не весь transcript и не аудио |
| Постоянные предпочтения | Existing profile lifecycle до явного изменения/reset/delete | Только подтверждённые интересы; «с ребёнком сегодня» не становится постоянным профилем |
| Аудио | Короткая техническая обработка, удаление после подтверждения и bounded error TTL | Не обычное Supabase Storage, не GitHub/analytics; retention применяется и при сбое |

После expiration старые hard constraints не применяются тайно. Даты привязывать к дате высказывания и timezone события (по умолчанию Калининград), а не к timezone машины/CI. «Завтра» через сутки не означает механически ещё один сдвиг на день. При явном возобновлении перепроверять актуальность событий и даты.

Supabase естественен как existing identity/profile owner: private state + RPC, auth.uid()/проверенный subject, атомарная revision и автоматическое удаление. При service-role доступе RLS может обходиться, поэтому owner checks нужны и в privileged RPC/сервисе. Клиентский user_id и один anon_id не доказывают владение. Existing consent-aware anonymous profile linking не заменять вторым merge-механизмом; automatic profile link после login не даёт права автоматически отправить сохранённый поиск.

Браузер хранит только bounded projection в существующем storage budget, сервер — владеющую запись. На общем устройстве logout очищает доступную проекцию и отключает старые operations. Текст из модели/описаний событий — недоверенные данные, не команды к БД или системному prompt.

### Egress: важнее измерить выдачу, чем бояться короткого контекста

По [актуальной документации Supabase](https://supabase.com/docs/guides/platform/manage-your-usage/egress) Free включает 5 GB uncached и отдельно 5 GB cached egress; это не один свободно взаимозаменяемый лимит 10 GB для запросов к БД. Исходящие данные БД в Devstand тоже учитываются. Storage, Auth, Realtime и другие сервисы участвуют в своих соответствующих счётчиках общего использования.

Иллюстрация, НЕ измерение проекта: 10 000 бесед × 5 ходов × 8 KB исходящего состояния ≈ 0,4 GB. Если выдача в среднем 50 KB, те же ходы добавляют ≈ 2,5 GB. Другие запросы/сервисы и overhead идут сверху. Поэтому небольшая память сама по себе не причина заводить отдельную БД, но нельзя объявлять egress несущественным.

Не передавать всю историю/каталог/векторы/постеры на каждый ход, не использовать Realtime для каждого VAD tick, не запрашивать полную строку после каждой записи. Считать actual response bytes, payload sizes, storage/index size, expired-state deletion и service breakdown. Метаданные выдачи кэшировать с catalog/policy revision; персональные результаты не помещать в общий публичный cache.

## 7. Архитектура выполнения

```text
Astro / общий search composer
  ├─ компактный availability manifest с CDN
  └─ existing Auth + operation transport
       → first-party voice/intent API в изолированном runtime
       → shared admission/Google limiter
       → разрешённый Gemini Lite adapter: transcript + IntentPatch
       → validated ordered intent state в Supabase
       → existing retrieval services: query embedding + Supabase pgvector/filters
       → canonical EventCard snapshots + typed summary/chips
```

Devstand подходит как кандидат размещения этого небольшого сервиса, но не как вызов Codex/DevCoveer coding agent на каждый пользовательский запрос. Нужны отдельный процесс, TLS, auth, bounded queue/timeouts, supervisor, health и restart recovery. Fly остаётся thin; не гонять через него аудио транзитом без причины и не переносить в него ASR/LLM. Реальная доступность Devstand/region contract в этом анализе не измерялась.

Не переносить действующий Search целиком в новый сервис ради голоса. Переиспользовать его domain retrieval/SQL и общую policy; при необходимости выделять функции из monolith, не копировать второй индекс/алгоритм. Не включать public canary execution overrides из браузера.

Экономный целевой режим: один Lite-вызов на обычную реплику для текста либо audio→transcript+IntentPatch; затем retrieval и шаблонное краткое резюме из подтверждённых фактов. Query embedding — отдельный учитываемый вызов, если нет cache hit. Голосовой input и structured output поддерживаются Lite-моделями, но совместное качество ASR/intent требуется доказать.

Это предлагаемая эволюция #587, а не уже существующий API. Не добавлять сверху обязательную цепочку «ASR → planner → current verifier → writer». Дополнительный Lite verifier допустим только для измеренно нужного случая с отдельным учётом. Нельзя обещать один запрос на длинную беседу с несколькими уже отправленными фрагментами. Конкретный model_id брать из проверенной policy/registry; не повышать модель скрыто до Flash/Pro и не наследовать Gemma overflow для voice-policy вопреки owner Lite-only ограничению.

Сервер применяет hard filters и проверяет IDs. Если строгие условия дают ноль, это ноль, а не повод выдать платный концерт как бесплатное семейное событие. Ослабления явно показываются отдельно. Facts берутся из projection, не генерируются: место/дата/цена/возраст/наличие билета/источник. Неизвестное остаётся неизвестным. Owner-only Smart Update endpoints (#618/#623) не выдаются публичному ассистенту.

## 8. Единый лимитер и availability

Новый provider consumer использует existing contract:

```text
reserve → mark_sent → provider → finalize(actual usage)
```

Отказ limiter = fail-closed для model request, но не для статического сайта. Release reservation допускается лишь при доказанном отсутствии отправки. Quota scope — Google project + model, не отдельный ключ. На [официальной странице](https://ai.google.dev/gemini-api/docs/rate-limits) подтверждены project-level limits и daily reset по Pacific time.

Дополнительно нужны product per-user abuse budget, fair concurrency, priority для интерактивного поиска над bulk tests и общий circuit breaker. Это политики над единым ledger, не независимые альтернативные счётчики Google. Все реальные live-test вызовы тоже проходят limiter. Qwen на CPU не расходует Gemini RPM: единая ресурсная политика должна различать CPU slots/runtime, provider tokens и network/storage budgets. Не утверждать, что конкретная CPU-admission интеграция уже доказана лишь потому, что notebook существует.

### Манифест доступности

Небольшой status JSON в existing Object Storage/CDN namespace, обновляемый лёгким агрегатором health/limiter state. Это не пересборка Astro и не новый batch/static-site-builder. Начальные интервалы для проверки: обновление/кэш 30–60 секунд с jitter; background tabs не поллят origin. Из браузера — CDN, не периодический прямой ping Devstand от каждой вкладки.

Поля: schema_version, release/contract compatibility, enabled, state `available|degraded|unavailable|unknown`, checked_at, valid_until, optional global retry_after. Без ключей, user IDs и частных пользовательских квот. Expired/malformed manifest означает unknown, а не вечный green. Независимый health observer или expiration должен обнаруживать умерший publisher.

Манифест — подсказка UX, не гарантия и не право выполнить запрос: сервер снова проверяет auth/quota/availability. Локальная сетевая ошибка одного пользователя не публикуется как глобальная авария. Пользовательский rate limit не записывается в публичный manifest. Тяжёлый authenticated/model canary используется периодически через existing health workflow, не раз в минуту от всех клиентов.

На первом открытии не рекламировать недоступный voice entry. Но у уже взаимодействующего пользователя не убирать кнопку/контекст внезапно: показать причину и текстовый/обычный поиск. Временная недоступность voice не означает, что гарантированно доступен LLM text search: при общем provider outage остаются статические фильтры и классическая выдача, а vector-only — только если реальный backend ещё способен её выполнить.

## 9. Автотесты: расширение действующей стратегии

GitHub-hosted Actions применим. Не нужен self-hosted runner. Сценарии регистрируются в existing `docs/testing/static-site-autotest-scenarios.v1.yml`, подключаются к existing suites/harness; таблица ниже — проект кейсов, не второй исполняемый реестр и не заявление, что тесты уже написаны.

| Слой | Что проверять | Где |
|---|---|---|
| L0 / unit/property | IntentPatch/schema, revisions/epoch, даты, границы TTL, allowlist, manifest, duplicate/malformed input, cost math | Каждый relevant PR, без провайдеров |
| Integration | Настоящий test Postgres/RPC/RLS, concurrent CAS/lease/idempotency, рестарт между стадиями, чужой user/conversation | Изолированный CI test DB, не production |
| L1 browser | Реальный capture/VAD/MediaRecorder/upload path; UI/focus/keyboard; управляемые задержки/429/timeout; latest-result suppression | Existing Playwright harness и ci.yaml |
| Live quality | Замороженное аудио → настоящий разрешённый Lite → реальные retrieval/validation steps; actual quota/usage | Protected manual/scheduled subset через общий limiter |
| L2/L3 | Android/iOS permission, PWA lifecycle, background/lock, real microphone and codec | Existing mobile adapters + небольшой physical-device gate |

В Chromium возможно подать WAV в fake microphone с `--use-fake-device-for-media-stream` и `--use-file-for-fake-audio-capture=/absolute/path.wav%noloop` ([Chromium source](https://chromium.googlesource.com/chromium/src/+/refs/tags/76.0.3795.1/media/base/media_switches.cc)). Проверять, что приложение действительно записало ненулевое ожидаемое аудио, а не подменить весь поток готовым transcript. Stub provider в PR lane допустим, но он проверяет wiring, не качество распознавания.

Playwright mobile viewport не равен настоящему iPhone. Не использовать Chromium fake-media flags как доказательство WebKit/Firefox или физического микрофона. Для network fault тестов управлять порядком ответов, не ждать случайной гонки.

### Предлагаемые обязательные сценарии

1. `voice.auth_gate`: анонимный submit не создаёт audio/model/Search cost-bearing вызов; после login нет autosubmit.
2. `voice.basic`: фраза → видимая расшифровка/условия → только canonical cards.
3. `voice.long_utterance`: текст длиннее legacy 180 символов не обрезается незаметно.
4. `voice.silence`: тишина не порождает выдуманный transcript и provider spam.
5. `voice.negation`: «не концерт» и «не бесплатно, до пятисот» не теряют отрицания.
6. `voice.dogon_during_asr`: U2 принят до окончания U1, ни одна реплика не потеряна.
7. `voice.dogon_during_search`: U2 принят пока retrieval U1 выполняется; U1 не перезаписывает U2.
8. `voice.out_of_order_and_duplicate`: сетевые дубли/переупорядочивание не меняют смысл/списания.
9. `voice.id_collision`: повторный ID с иным payload отвергается.
10. `voice.ambiguous_timeout`: неизвестный результат отправки не вызывает второй provider POST/transport rescue.
11. `voice.new_epoch`: после reset/logout старый ответ не появляется.
12. `voice.concurrent_tabs`: две вкладки не смешивают беседы; общий ID защищён CAS.
13. `voice.result_reference`: «второе» разрешается относительно действительно показанного result_set_id.
14. `voice.context_ttl`: границы 30 минут/7 дней, очистка и явное resume не оставляют скрытые фильтры.
15. `voice.calendar_anchor`: полночь, выходные, выбранный timezone, переход месяца/года; старое «завтра» не сдвигается само.
16. `voice.constraint_replace`: «вместо Светлогорска Калининград» заменяет город, а не создаёт невозможный AND.
17. `voice.zero_results`: hard constraints не ослабляются молча; альтернативы помечены.
18. `voice.unknown_facts`: неизвестная цена/адрес/время не выдумываются; цена unknown ≠ бесплатно.
19. `voice.source_endorsement`: упоминание не выдаётся за рекомендацию; absent evidence честно сообщается.
20. `voice.off_topic_and_injection`: посторонний запрос и инструкции из event text не открывают произвольные tools.
21. `voice.owner_only_tools`: публичная identity не вызывает Smart Update или admin APIs.
22. `voice.explicit_profile`: временное условие не становится постоянным предпочтением без явного действия.
23. `voice.owner_isolation`: чужая session/ID/RPC, forged user_id, service-route — deny; после logout локальной утечки нет.
24. `voice.retention`: audio/error artifacts, raw text, expired state удаляются; в analytics/secrets/logs нет PII.
25. `voice.status_transitions`: capture и processing независимы, текст статуса соответствует реальному состоянию; нет fake percent.
26. `voice.accessibility`: keyboard, aria-live, reduced motion, focus и доступные ошибки.
27. `voice.permissions_and_codecs`: deny/revoke mic; WebM/Opus и MP4/AAC по support matrix; огромный/битый blob.
28. `voice.lifecycle`: offline/online, pause/tab switch/reload/lock; неполученный фрагмент не выглядит подтверждённым.
29. `voice.limiter_unavailable`: zero provider requests, honest fallback.
30. `voice.quota_contention`: site и CI/batch конкурентно расходуют один project/model scope, переучёт запрещён.
31. `voice.sent_accounting`: cancel после dispatch и timeout не освобождают quota как unsent.
32. `voice.queue_pressure`: capacity bounded; невозможно молча потерять уже принятый utterance.
33. `voice.manifest_expired`: expired/malformed/missing status не оставляет green навсегда.
34. `voice.manifest_scale`: CDN/cache/jitter/visibility policy не создаёт прямой backend polling fan-out.
35. `voice.manifest_privacy`: user quota/identity не попадают в публичный status.
36. `voice.provider_admission`: запрещённый регион/аудитория/непринятые data-processing условия не включают live provider.
37. `voice.current_catalog`: смена corpus revision, occurrence-family и отменённое событие не выдаются из старого cache как актуальные.
38. `voice.asp_states`: один corpus+UI-state fixture порождает соответствующие Astro/SoT/Penpot variants.

Первые 38 кейсов — предлагаемое покрытие, **не 38 пройденных тестов**. Детерминированные P0 invariants обязательны; ASR/semantic quality измеряется по размеченным примерам, не тестируется точным совпадением свободной прозы модели.

### CI политика

PR lane: mocks/no providers/no secrets in fork PRs; actual browser capture, reducers, DB security, fault matrix, canonical fixture contracts. Тесты источников/схем не заменяют runtime tests. Добавить сценарии в существующие scripts/workflow; не менять gates так, чтобы получить ложный green.

Live lane: protected environment, explicit bounded quota reservation, отдельная test identity, current release identity и pinned fixture manifest. Малый ручной/периодический набор; не генерировать Qwen и не гонять весь корпус при каждом PR. `SKIPPED_NO_BUDGET` и `BLOCKED_PROVIDER_POLICY` — не quality PASS. Actions concurrency не заменяет cross-system limiter. Не использовать pull_request_target для запуска недоверенного кода с секретами. Canary auth использовать existing session_fixture; не рассылать реальный OTP ради каждого voice case.

Release: product source SHA + backend/prompt/policy revision + corpus/fixture hashes + required L0/L1 evidence + фактически выполненный live subset и relevant L2/L3. Публикуемый Preview/owner review идёт через единый Kaggle StaticSiteBuilder, как требует #621; локальный CI fixture build не получает owner-review credit.

## 10. Qwen для контролируемого аудиокорпуса

Найден существующий [skill](https://github.com/onedayonemasterpiece/idea-hub/blob/main/skills/voice-file-qwen3-tts/SKILL.md), version 1.5.0. Через my-data-hub выполнены live read и download `verification.json`, а не только чтение memory/старого registry:

```text
resource: zigomaro/yazyki-rossii-qwen3-tts-cpu-0901-r2
provider run: /2
kernel: 132814941
source version: 2
source SHA-256: 4258a465431cc108e05f27b3cd1dfdccdf551e61669c346bb29d982d7bdad14a
live run_state: complete
verification state: success
final MP3 bytes: 7077981
final MP3 SHA-256: afb50f385bd932ef6f4761fa847cbdfd94546590f55c5c4c4b3c12f284544c36
verification.json SHA-256: 01d69a519589baafb761bdbfefc3a17ca4840a3bbfe478c7b6bf3545c32fccc9
```

Это подтверждает пригодную основу CPU-синтеза; не доказывает качество ещё не созданного поискового корпуса. Новые Qwen/model runs в этом анализе не запускались. Перед генерацией прочитать BASELINE.yaml, LONGFORM-BASELINE.yaml и RUNBOOK.md; сохранить pinned dependencies и правила exact source/readback. Не перезаписывать lecture job чужим сценарием: для семейства search-fixtures зарегистрировать один стабильный slug и далее версионировать его.

Начать с около 30 смысловых сценариев, двух допустимых голосов и трёх условий (чисто, паузы, умеренный шум) — около 180 фиксированных clips. Это предложение размера, не обязательная квота. Шум/кодеки — маркированные производные исходника; clean baseline не модифицируется скрыто. Часть fixtures — составные диалоги с контролируемым временем U1/U2/U3, а не только изолированные фразы.

Разметка: fixture_id, точный текст, ожидаемый IntentPatch/условия, frozen now/timezone, ссылки на existing Event Corpus revision, допустимые/запрещённые результаты, допуск неопределённости, provider/model/source version, voice consent/происхождение, duration, codec, sample rate и SHA. Реальные event IDs берутся из approved corpus, не сочиняются в этом отчёте. Покрытие новых событий расширять через принятый append-only corpus process.

Ожидаемый смысл размечается независимо до генерации. Проверить, что Qwen действительно произнёс нужное, особенно отрицания, числа и локальные названия; ASR той же модели не единственный судья. Добавить небольшой отдельный consented human holdout для акцентов/улицы/телефонов. Масса синтетики не доказывает human usability. Голоса пользователя/других людей не клонировать и не публиковать по умолчанию.

Большие бинарные fixtures хранить в versioned artifact storage, а в Git — manifest и маленький smoke набор. CI проверяет hashes и не зависит от изменяемого latest или истёкшего обычного Actions artifact. Frozen Qwen data генерируется заранее; GitHub Actions потом многократно воспроизводит её без TTS inference.

## 11. Разделение работ

### ChatGPT / GitHub: делать без дефицитного coding agent

Подготовить и согласовать scope/UX-copy/intent semantics; внести принятые решения в owning Search/ownership/test contracts. Далее отдельными bounded source changes можно реализовывать чистые TS-модули IntentPatch/revision reducer/status validation, schemas, fixtures и unit tests, привязку к existing scenario registry. Это не требует управления production процессами. Каждый source пакет сохранять с readback; test verdict только после реального исполнения.

Qwen-корпус также можно готовить в ChatGPT через my-data-hub: прочитать baseline, оформить manifest/oracle, проверить admission, запустить/проверить версионированный CPU notebook, получить exact artifacts. Это отдельный явно запущенный пакет, не фоновое обещание и не запуск внутри этой аналитики.

### Coding agent: интеграция и исполняемая проверка

Выделить/переиспользовать retrieval domain services; добавить first-party endpoint, auth/operation ownership, SQL CAS/TTL/receipts и shared limiter adapter; интегрировать реальные MediaRecorder/VAD/recovery/renderer в текущий Astro; выполнить actual DB/browser/race tests, staging runtime deployment и protected live smoke. При переносе на Devstand развернуть обычный изолированный сервис, а не пользовательский вызов агента. Не делегировать повторное широкое продуктовое исследование.

### Текущее UI/SoT/Penpot направление

Не блокировать завершение нормализации всего сайта ради ассистента. Чистую логику/fixtures вести параллельно, новую общую UI-family подключать после согласования ownership с текущими исполнителями #621. Предлагаемые компоненты: VoiceSearchControl, AssistantComposer/Status, interpreted constraints; это имена будущих семейств, не утверждение о наличии их в SoT.

Все visual state variants — idle/listening/processing/dogon/empty/quota/error/offline — должны происходить из одного versioned SoT и materialize в Astro/Penpot на одном frozen Event Corpus + UI-state fixture. Нельзя вручную нарисовать «похожую» кнопку в Penpot и назвать это A=S=P. Сетевые timing traces сами по себе не являются статической Penpot parity.

## 12. Порядок реализации и критерии результата

**Пакет A: решение/контракты, параллельно текущей нормализации.** Provider eligibility/privacy gate; Auth baseline; TTL proposals; reuse map; 38 scenarios; intended changes в owning docs. Не расширять production exposure.

**Пакет B: детерминированное ядро и text refinement.** Один intent state, ordered continuation/CAS, IDs/revisions, hard-filter validation, bounded storage, mocked browser scenarios и DB isolation. Доказывает логику без расходов на модели и без ожидания идеального дизайна.

**Пакет C: голос и короткий реальный вертикальный сценарий.** Mic→audio→разрешённый Lite→existing retrieval→canonical cards; догон во время ASR и поиска; общий limiter; friendly availability/failures; маленький frozen Qwen smoke set. Старый Search не ломается.

**Пакет D: проверка и публикация.** GitHub-hosted L0/L1/integration, bounded live quality, mobile lifecycle, same-corpus A=S=P states, единый Kaggle Preview. Продвижение только после доказанных gates; undo feature flag оставляет сайт полезным.

Показатель продукта — не количество сообщений с ИИ, а достижение пользователем ценного события/CTA: existing event_value_reached_rate, event_intent_action_rate, time_to_first_event_value; дополнительно correction/abandonment rate, rescued zero-results, hard-constraint violations, lost/duplicate utterances, stale-result application, provider calls/tokens/audio seconds на успешный выбор, latency p50/p95 и actual egress. Latency/SLA нельзя объявить измеренной по успешному TTS notebook.

**Итог:** предложение стоит реализовывать как компактный разговорный поиск. Сейчас готовы аналитическое видение и проект проверок; новый runtime, новые автотесты и поисковые аудиофикстуры этим документом не объявляются реализованными.
