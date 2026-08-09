# Умный поиск — единый продуктовый, архитектурный и эксплуатационный контракт

> **Статус:** канонический документ функции, редакция 2026-08-02.  
> **Текущий release verdict:** `INCIDENT OPEN / NO-GO` — владелец продукта сообщает, что работавший поиск недоступен около недели; точная производственная причина ещё не доказана сквозным живым прогоном.  
> **Область:** `/poisk/`, встраиваемые точки входа в поиск, авторизация, online retrieval, кэш, квоты, защита от автоматизированного злоупотребления, наблюдаемость, автотесты и возможная миграция на BGE.  
> **Исходный brief:** [`smart-vector-search-requirements.md`](smart-vector-search-requirements.md).  
> **Найденный ранее документ:** [`../../unsigned-personalization/authorized-event-search.md`](../../unsigned-personalization/authorized-event-search.md).

## 0. Владение и правило против раздробленности

Этот документ является единственным владельцем **актуального продуктового и Search-специфичного контракта**. Новые решения по Умному поиску должны вноситься сюда, а не дописываться очередным хронологическим слоем в старые исследования.

Смежные канонические документы не копируются сюда целиком:

- общий Auth, `BackendClient`, `OperationCatalog`, direct/relay routing и семантика повторов принадлежат [`../../unsigned-personalization/production-integration.md`](../../unsigned-personalization/production-integration.md);
- уровни L0–L3, выбор платформ и правила запуска принадлежат [`../../../operations/static-site-autotest-strategy.md`](../../../operations/static-site-autotest-strategy.md);
- release-gates принадлежат [`../release-autotest-gates.md`](../release-autotest-gates.md);
- машиночитаемые сценарии принадлежат [`../../../testing/static-site-autotest-scenarios.v1.yml`](../../../testing/static-site-autotest-scenarios.v1.yml);
- `authorized-event-search.md` остаётся журналом реализации, решений и исторических прототипов. При конфликте актуального продуктового решения с его старым хронологическим слоем приоритет имеет этот документ;
- offline BGE/StaticSiteBuilder остаётся частью единого batch-контура, описанного в [`../../../operations/kaggle-static-site-builder.md`](../../../operations/kaggle-static-site-builder.md), и не превращается сам по себе в online Search-сервис.

Нельзя создавать отдельные Search-auth client, Search transport, Search mobile harness, Search provider-quota ledger или второй реестр тестов. Поиск подключается к общим механизмам проекта через типизированные операции.

## 1. Принятые решения

1. **Функция сохраняется и должна быть восстановлена.** Это не экспериментальный визуальный макет, а авторизованный способ искать события по естественному русскоязычному запросу.
2. **Поиск доступен всем авторизованным пользователям**, независимо от способа входа: email OTP/magic link либо Яндекс. Отдельного платного или фокус-группового entitlement для обычного поиска не требуется.
3. **Неавторизованный пользователь видит поле поиска, но запрос не исполняется.** После submit браузер делает `0` запросов к `event-search` и показывает: «Умный поиск доступен только после входа. Войдите по email или через Яндекс». Запрос можно временно сохранить на устройстве, но после входа пользователь подтверждает поиск вторым явным действием; автоматический платный POST после OAuth callback запрещён.
4. **Текущий production baseline — не BGE.** Реализованный online путь: `gemini-embedding-2` с размерностью 768 → Supabase pgvector → необязательный verifier Gemini Lite/Gemma → канонические EventCard snapshots.
5. **BGE-M3 допускается как кандидат, а не как немедленная замена.** Для него нужен отдельный 1024-мерный индекс, отдельный online query-inference service, shadow/A-B доказательство качества и нагрузочный canary. До закрытия этих gates BGE остаётся offline/shadow lane.
6. **LLM verifier не является точкой доступности.** При его отказе пользователь должен получить честную vector-only выдачу с машинным признаком degraded mode, а не пустой экран.
7. **Поиск — cost-bearing POST с семантикой `selected_once`.** Один маршрут выбирается до отправки тела; неоднозначный timeout не даёт права повторить тот же запрос через второй transport.
8. **Канонический response transport v1 — bounded JSON.** NDJSON/streaming остаётся экспериментальным capability и не участвует в release, пока отдельно не доказан на direct, relay, Android Chrome и iOS Safari. Прогресс в JSON-режиме честно indeterminate; симулированный процент запрещён.
9. **Авторизация необходима, но недостаточна для защиты от ботов.** CAPTCHA не включается для всех по умолчанию, однако server-side quotas, concurrency, idempotency, global cost circuit breaker и адаптивная защита обязательны.
10. **Сбой, который может жить неделю без сигнала, считается архитектурным дефектом.** Release требует живой синтетический canary, corpus-freshness gate и наблюдаемую диагностику по стадиям.

## 2. Что обнаружено аудитом текущей реализации

Аудит выполнен по `main` 2026-08-02 с опорой на реальный клиент [`AuthorizedEventSearch.astro`](../../../../site/src/components/AuthorizedEventSearch.astro), Edge Function [`event-search`](../../../../supabase/functions/event-search/index.ts), SQL security contract, browser recovery tests, [PR #163](https://github.com/onedayonemasterpiece/events-bot-new/pull/163) и его [`INTEGRATION_REPORT.md`](../../../../.codex/integration/static-site-resilient-egress-20260731/INTEGRATION_REPORT.md). Указанные ниже defects — вывод из кода и release evidence; конкретная production-причина текущего отказа остаётся открытой до живого коррелированного запроса.

### 2.1 Фактическая цепочка

```text
Astro /poisk/
  -> shared StaticSiteAuth session
  -> AuthorizedEventSearch
  -> Backend transport fetch
  -> Supabase Edge Function event-search
      -> verify JWT with Supabase Auth
      -> private service client
      -> result cache lookup
      -> per-user quota reservation on cache miss
      -> query embedding cache / gemini-embedding-2 (768)
      -> service-only pgvector RPC over current event corpus
      -> occurrence-family collapse and pagination
      -> optional Gemini Lite, then bounded Gemma overflow verifier
      -> canonical EventCard snapshots + feedback identifiers
  -> same large EventCard presentation used by discovery surfaces
```

Сильные стороны уже существующего кода:

- JWT проверяется до создания privileged client;
- browser не имеет прямого доступа к service-only vector/quota RPC;
- request body ограничен 16 KiB, query — 3–180 символами;
- `client_request_id` и quota operation ledger исключают двойное списание;
- result cache проверяется до provider quota reservation;
- occurrence families сворачиваются до логической пагинации;
- при provider/LLM degradation предусмотрена vector-only классификация;
- клиент имеет bounded header/body/overall timeouts и не делает rescue POST;
- SQL contracts проверяют grants, owner scoping, idempotency и feedback caps.

### 2.2 Найденный второй документ и причина противоречий

`authorized-event-search.md` — тот документ, который, вероятнее всего, не удавалось найти. Он содержит значительную часть реальной архитектуры, но одновременно хранит большое количество последовательных версий прототипа. Из-за этого в одном файле сосуществуют:

- текущий JSON default и более ранний normative текст про backend-streamed progress;
- нынешний Yandex-only submit flow и исследовательский email UI;
- утверждение о немедленном показе vector preview и клиент, который provisional cards фактически скрывает;
- первоначальные quota/cache решения и более поздняя hardened реализация.

Решение — не переписывать историю, а закрепить здесь current truth и ссылаться на старый файл только за implementation evidence.

### 2.3 Критические разрывы

| Приоритет | Разрыв | Последствие | Обязательное исправление |
|---|---|---|---|
| P0 | Нет регулярного живого authenticated canary всей цепочки | Backend, auth, provider, corpus или опубликованный HTML могут быть сломаны днями | Protected cached canary + периодический cold canary + alert |
| P0 | Result-cache key не содержит `catalog_revision` и `embedding_corpus_revision` | Новый/исправленный event может не попасть в выдачу до TTL; старый event может остаться | Revision-bound invalidation, а TTL только safety ceiling |
| P0 | Cache policy signature читает legacy `EVENT_SEARCH_LLM_MODEL`/`FALLBACK_MODEL`, тогда как verifier выбирает `...LLM_LITE_MODELS` и `...GEMMA_OVERFLOW_MODELS` | Смена реальной модели может продолжить обслуживаться старым cache entry | Одна типизированная `SearchPolicyVersion`, используемая и исполнителем, и cache key |
| P0 | Нет обязательного corpus coverage/freshness receipt | Функция может отвечать `200`, но искать по неполному или старому индексу | 100% eligible coverage gate, corpus revision в ответе и release manifest |
| P0 | Не доказано, что опубликованный `/poisk/`, Edge deploy, migrations и vector sync принадлежат одной release identity | Исправление в `main` не означает исправление для пользователя | Bind static SHA + Edge contract version + corpus revision в acceptance evidence |
| P1 | Anonymous submit сейчас исторически запускает Yandex flow и затем auto-submit; email entry не является единым production flow | Не соответствует принятому UX и создаёт неожиданный cost-bearing POST | Inline auth gate, оба способа входа, повторное явное submit |
| P1 | Edge Function возвращает CORS `*`, тогда как общий production contract требует exact origin boundary | Auth не обходится, но abuse surface и архитектурный дрейф расширяются | Exact allowlist/relay policy, единый CORS helper |
| P1 | Query audit hash — обычный SHA-256 нормализованного текста; embedding-cache salt имеет рабочий fallback | Низкоэнтропийные запросы можно угадывать при утечке служебных данных | HMAC с обязательным production secret и версией ключа |
| P1 | Hour ledger реализован как дискретный bucket, а product brief требует скользящее часовое окно | Burst на границе часа получает двойную ёмкость; UX reset не соответствует обещанию | Rolling-window/token-bucket admission |
| P1 | Client и Edge дублируют normalization/validation regex | Допустимый на клиенте запрос может быть отвергнут сервером или наоборот | Shared versioned validation contract + parity tests |
| P1 | JSON/NDJSON и provisional/final semantics не унифицированы | Разные платформы могут видеть разные состояния и зависания | JSON v1; provisional streaming только отдельным future capability |
| P1 | LLM-degraded результат не попадает в обычный result cache, когда verifier запрашивался | Provider outage вызывает повторные дорогие попытки на одинаковых запросах | Короткий отдельно маркированный degraded cache + circuit breaker |
| P1 | Нет server-side one-in-flight per user и adaptive abuse signals | Авторизованный бот может параллелить уникальные cache misses | Concurrency lease, cheap request cap, miss/cost cap, anomaly controls |

## 3. Инцидент: почему нельзя пока назвать точную причину недельного отказа

Репозиторий доказывает, что 31 июля были объединены hardened Edge/relay/vector изменения и выполнены отдельные live probes. Одновременно integration evidence прямо не закрывал весь root publication и требовал чистого deploy из `main` и compensating vector/static catch-up. Это делает наиболее опасным классом проблемы **drift между исправленным data plane и реально опубликованной Search surface**.

Однако без одного живого запроса авторизованного пользователя, соответствующего request ID в Edge telemetry и corpus receipt нельзя честно выбрать единственную причину. Возможные ветви диагностики:

1. **Presentation/config:** опубликован старый HTML, `data-search-enabled=false`, неверные public env либо не тот immutable candidate.
2. **Identity:** session не восстанавливается на `/poisk/`, email/Yandex identity controller расходится с Search.
3. **Transport:** direct или relay недоступен, operation выбрана неверно, CORS/preflight/body decode не завершены.
4. **Edge deploy:** функция отсутствует, имеет старый env/contract, не видит service key либо migrations.
5. **Corpus:** search documents/embeddings отсутствуют, stale, другой `doc_kind`, размерность или revision.
6. **Provider:** исчерпаны/отключены Google keys, shared limiter metadata не совпадает, embedding model недоступна.
7. **Result protocol/UI:** сервер отвечает, но клиент остаётся в loading, не декодирует payload либо скрывает final cards.

### 3.1 Первый обязательный recovery run

До изменения ranking или BGE выполнить один evidence-driven проход:

1. Зафиксировать production URL, HTML build/repo SHA, Supabase project ref, Search contract version и Edge deployment revision.
2. Проверить anonymous state: поле видно; submit даёт `0` Search POST и auth-only сообщение.
3. Получить валидную fixed test session через общий Auth harness; отдельно не отправлять новый OTP, если identity уже доказана.
4. Выполнить один известный cache-hit запрос и один защищённый cold запрос; сохранить только PII-free request IDs.
5. По каждому request ID установить последнюю достигнутую стадию: auth, cache, quota, embedding, vector, verifier, response decode, render.
6. Сверить migrations/grants и `event-search` env с `origin/main`.
7. Выпустить corpus receipt: eligible/search-doc/embedding counts, model, dim, `doc_kind`, source/corpus revisions, oldest/newest update.
8. Повторить идентичный запрос с новым operation ID и доказать result-cache hit: `0` provider sends, `0` vector RPC, `0` cold-quota decrement.
9. Эмулировать direct-down/relay-up и relay-down/direct-up; в каждом случае отправляется ровно один cost-bearing POST по заранее выбранному маршруту.
10. Только после evidence классифицировать и закрыть incident root cause; «перезапустили и заработало» не является закрытием.

## 4. Продуктовый контракт

### 4.1 Неавторизованный пользователь

- `/poisk/` и общая навигация показывают Search как реальную функцию, а не disabled teaser.
- Поле редактируемо; подсказки/одобренные статические запросы могут открываться без Auth как обычные страницы.
- Submit выполняет только локальную validation.
- Сетевой счётчик по `/functions/v1/event-search` остаётся `0`.
- Появляется inline gate:

> Умный поиск доступен только после входа. Войдите по email или через Яндекс.

- Показываются действия `Войти по email` и `Войти через Яндекс`, принадлежащие общему identity controller.
- Query хранится не дольше 30 минут в versioned bounded browser storage без токена/email; cancel/expiry удаляет его.
- После успешного входа query восстанавливается в поле, но Search начинается только по новой явной команде пользователя.
- Anonymous/demo results запрещены.

### 4.2 Авторизованный пользователь

- Enter отправляет запрос; `Shift+Enter` может вставлять перенос строки на desktop. IME composition Enter не отправляет.
- На mobile поле имеет `enterkeyhint="search"`, а системная клавиша поиска запускает один submit.
- Textarea может быть многострочной, но её высота ограничена и не вытесняет выдачу.
- Submit button является зоной состояния: idle → loading/indeterminate → done/error. Фальшивые проценты и client timers, изображающие backend stages, запрещены.
- Double click, Enter + tap и повторный form event coalesce в один `client_request_id`/один POST.
- Logout/pagehide отменяет локальное ожидание; неоднозначный POST не повторяется автоматически.

### 4.3 Композиция выдачи

Порядок один для всех страниц и устройств:

1. **Точные и подходящие результаты** — большие канонические EventCards, свернутые по occurrence family.
2. **Короткий feedback block** после первой полезной группы: «Нашли то, что ожидали?» с `Да` / `Не совсем` и необязательным уточнением.
3. **Возможно, вам будет интересно** — явно отдельная discovery section, не продолжение semantic exactness.
4. **Вам может быть интересно** — персональная лента другого алгоритма и другого served-list ID; сбой персонализации не меняет Search results.

Search intent сам по себе не становится долгосрочным интересом. Сильные персональные сигналы — click-through с dwell, save, like, share, calendar, explicit positive feedback; отрицательный сигнал — `Не интересно`/negative feedback. Персонализация может быть tie-breaker внутри сопоставимой релевантности, но не скрывает лучший semantic match.

Артефакт-пасхалка допустим только как versioned editorial insertion с заранее определённым placement, frequency cap и отдельным analytics ID. Он не участвует в relevance metrics и не подменяет event result.

### 4.4 Одобренные запросы как статические страницы

Positive feedback не публикует пользовательский запрос напрямую. Он создаёт candidate для отдельного offline conveyor:

```text
explicit positive feedback
  -> privacy/safety screening
  -> query normalization and intent clustering
  -> support/overlap threshold
  -> LLM multi-pass assessment
  -> canonical label, slug and query definition
  -> deterministic current-catalog build
  -> static no-JS page and ordinary internal link
```

Правила:

- единичные, персональные, содержащие имена/контакты/чувствительные признаки запросы не материализуются;
- эквивалентность определяется нормализованным intent и overlap выдачи, а не только строкой;
- статическая страница пересобирается из текущего каталога и не является вечным снимком runtime cache;
- пустая/одиночная выдача получает typed empty/low-density state по общему контракту подборок;
- approved queries доступны анонимно, индексируемость включается только после общих SEO/GEO gates;
- moderation/evaluation хранит provenance и не переносит raw private query в public title.

## 5. Единая runtime-архитектура

### 5.1 Операции общего `BackendClient`

| Operation | Capability | Semantics | Replay | Назначение |
|---|---|---|---|---|
| `search.execute.v1` | `functions` | `selected_once` | никогда после ambiguous dispatch | Один cost-bearing Search POST |
| `search.quota.read.v1` | `data` | `safe_read` | один bounded alternate retry | Показать остаток/`retry_after` |
| `search.feedback.write.v1` | `data` | `idempotent_command` | тот же operation ID после server dedupe | Результат feedback |
| `search.health.read.v1` | `functions` | `safe_read`, protected | bounded | Contract/corpus/provider readiness без provider call |

Feature component не выбирает direct/relay URL. RouteManager проверяет capability и выбирает один healthy route **до** `search.execute.v1`. Для cost-bearing POST запрещены hedging, race и retry по второму origin после dispatch.

### 5.2 Response protocol

Production v1:

- request JSON ≤16 KiB;
- response buffered JSON с отдельным byte ceiling;
- success означает status + полный body + JSON/schema decode;
- response содержит `schema_version`, `search_contract_version`, `request_id`, `algorithm_id`, `corpus_revision`, `cache_status`, `degraded`, `items`, `fallback_items`, pagination и quota projection;
- UI не получает provider key name, service details, raw prompts или внутренние errors;
- NDJSON может остаться в коде только под default-off capability flag и не считается release evidence.

### 5.3 Деградация

| Отказ | Поведение |
|---|---|
| Result cache доступен, providers нет | Отдать cache с age/revision; если revision совместима |
| Query embedding есть, embedding provider нет | Выполнить vector search по cached embedding |
| LLM verifier нет | Отдать vector-only results, `degraded=verifier_unavailable` |
| Оба routes недоступны до dispatch | Ничего не отправлять, показать retryable connection state |
| Timeout после dispatch | Не повторять; показать ambiguous/try-again state после завершения cooldown |
| Corpus stale/incomplete | Не выдавать ложный «ничего не найдено»; вернуть typed `search_temporarily_unavailable` |
| Quota исчерпана | `429` + точный `retry_after`, без alternate route retry |

## 6. Retrieval, ranking и данные

### 6.1 Production baseline

Пока BGE не принят, Search использует один совместимый набор:

- query/document embedding model: `gemini-embedding-2`;
- dimension: 768;
- versioned `embedding_doc_kind`;
- pgvector cosine shortlist;
- deterministic facets по дню недели, времени суток, admission и диапазону дат;
- occurrence-family collapse до pagination;
- Lite-first optional verifier, Gemma только как bounded overflow;
- canonical EventCard snapshot, а не второй Search-specific DTO.

Нельзя смешивать query vector одной модели с document vectors другой. `model + dim + doc_kind + preprocessing/template version` составляют одну неделимую corpus identity.

### 6.2 LLM verifier

LLM получает только bounded top-K и compact factual digest. Он классифицирует exact/possible/rejected, но:

- не создаёт новые events/IDs;
- не определяет Auth/quota;
- не вызывается на cache hit;
- не является обязательным для непустой выдачи;
- его policy version входит в cache key;
- смена списка Lite/Gemma models автоматически меняет policy version;
- ошибки/timeout не должны запускать неограниченную ротацию ключей.

### 6.3 Corpus contract

Каждый searchable event должен иметь:

- canonical event ID/family ID;
- current source revision;
- current searchable document и digest;
- embedding model, dimension, doc kind, preprocessing version;
- embedding corpus revision;
- display snapshot schema version;
- eligibility interval.

Release/build gate:

```text
eligible_events == current_search_documents == current_embeddings
stale_embeddings == 0
orphan_embeddings == 0
wrong_model_or_dimension == 0
missing_display_snapshots == 0
```

Удаление/скрытие event и correction обязаны продвинуть corpus revision. Last-known-good допустим только с явным stale status и ограниченным age, не как бесконечный silent fallback.

## 7. Кэш: корректность, стоимость и экологичность

### 7.1 Разделить три сущности

1. **Query embedding cache** — зависит от query embedding model/template, но не от event catalog.
2. **Runtime result cache** — зависит от query, catalog/corpus, filters, ranking/verifier policy и времени.
3. **Materialized approved query page** — публичный build product, а не cache row.

### 7.2 Query embedding cache

Key:

```text
HMAC(secret_vN,
  normalized_query
  + embedding_model
  + dimension
  + query_template_version)
```

Требования:

- production secret обязателен; hard-coded fallback запрещён;
- key version поддерживает rotation;
- raw query не хранится;
- bounded rows/bytes, last-used compaction и TTL;
- provider metadata не раскрывает физический API key;
- изменение строки `task: ... | query: ...` меняет `query_template_version`.

### 7.3 Runtime result cache

Key обязательно включает:

```text
normalized query HMAC
catalog_revision
embedding_corpus_revision
embedding model/dim/doc_kind/preprocessing version
query facets and date/time bucket
limit/offset/family-collapse version
EventCard snapshot schema
LLM policy version and fallback policy
personalization mode (normally none or tie-break version)
```

Expiry:

- основная invalidation — смена catalog/corpus revision;
- временной ceiling — не более 6 часов;
- при известном lifecycle boundary: `min(next_boundary + 15 min safety, now + ceiling)`;
- переход календарной даты меняет key;
- correction/new event после успешного vector sync немедленно создаёт новую revision и cache miss.

Это точнее исходной идеи «держать до появления события + защитное время»: система не угадывает появление event по TTL, а инвалидирует cache по доказанной новой corpus revision; 15 минут — только safety для time-bound transitions.

### 7.4 Degraded cache

Если verifier запрошен, но недоступен, vector-only payload разрешено кэшировать отдельно на 1–5 минут:

- `degraded=true` и отдельный policy signature;
- не переиспользуется после восстановления verifier дольше TTL;
- предотвращает storm одинаковых provider attempts;
- не превращается в обычный long-lived result cache.

### 7.5 Обязательное доказательство cache hit

Тест не считается пройденным по одному `cache_status=hit`. Для второго идентичного запроса одновременно требуется:

- `result_cache_status=hit`;
- тот же compatible corpus/policy signature;
- `provider_embedding_sends_delta=0`;
- `provider_llm_sends_delta=0`;
- `vector_rpc_calls_delta=0`;
- `cold_user_quota_delta=0`;
- результат schema-valid и содержит только current eligible events.

## 8. Квоты и защита от ботов

### 8.1 Почему Auth не решает задачу полностью

Email/Яндекс существенно повышают цену злоупотребления, но бот может:

- зарегистрировать много почтовых аккаунтов;
- автоматизировать одну действующую session;
- параллелить уникальные cache-miss queries;
- распределять запросы по аккаунтам/IP;
- целенаправленно истощать provider quota дорогими miss-паттернами.

Поэтому Auth — первый слой, не единственный. Blanket CAPTCHA на каждый Search не нужна и ухудшит полезный сценарий.

### 8.2 Обязательные слои

1. Verified Supabase user; anonymous quota `0`.
2. Один активный cold Search на user; новые identical requests coalesce, другие получают bounded busy state.
3. Idempotent `client_request_id` и owner-scoped quota operation.
4. Отдельные cheap-request и cold-cost budgets: cache hits не тратят provider budget, но API не остаётся безлимитным.
5. Rolling 60-minute user window + daily cap.
6. Global cold-search admission по реальному headroom embedding/LLM pools.
7. Provider circuit breaker и cache/vector-only degraded mode.
8. Exact allowed origins и exact relay route allowlist.
9. PII-safe anomaly signals: account age, session churn, concurrency, cache-miss ratio, failure ratio. IP/ASN допустимы только как короткоживущий coarse signal, не как identity.
10. Adaptive challenge/cooldown только при подозрительном поведении; normal users CAPTCHA не видят.
11. Operator kill switch: cache-only или vector-only mode без выключения всей страницы.

### 8.3 Скользящее окно и 10× burst

Пусть `D` — разрешённое число cold searches на пользователя за rolling 24 hours. Тогда rolling-hour capacity:

```text
H = min(D, ceil(10 * D / 24))
```

Так пользователь может получить десятикратный часовой burst относительно равномерной суточной нагрузки, но не обойти лимит на границе дискретного часа. Реализация — token bucket или rolling event ledger, а не `date_trunc('hour')` как единственная граница.

Правила:

- cache hit: `cold_cost=0`, но учитывается cheap request cap;
- embedding-cache hit + vector miss: меньшая внутренняя cost weight, но одна user cold operation;
- LLM verifier может иметь отдельный global budget; при его исчерпании выдача остаётся vector-only;
- reserve capacity для внезапной нагрузки задаётся на global provider pool, не распределением «ещё одного ключа» в client code;
- UI получает `retry_after`, а не предположение «с начала следующего часа»;
- точные численные `D`, cheap cap и global reserve являются operational config и утверждаются после load test, не размножаются по компонентам.

### 8.4 Privacy

- Raw query не пишется в обычные request logs/audit.
- Telemetry использует HMAC query fingerprint, user hash, request ID, outcome и timings.
- Raw text допустим только при явном feedback/materialization consent, с retention и access policy.
- Нельзя экспортировать raw queries в YDB/analytics «на всякий случай».
- Search query не связывается публично с user identity.

## 9. Можно ли перевести всех пользователей на BGE

### 9.1 Что даёт BGE-M3

Официальная модель BAAI/bge-m3 имеет размерность 1024, поддерживает dense, sparse и multi-vector retrieval, более 100 языков и входы до 8192 токенов; код и model card опубликованы под MIT. Источники: [model card](https://huggingface.co/BAAI/bge-m3), [FlagEmbedding](https://github.com/FlagOpen/FlagEmbedding), [paper](https://arxiv.org/abs/2402.03216).

Для проекта потенциально полезны multilingual/Russian retrieval и возможность позже сравнить dense-only с hybrid dense+sparse. Длинный контекст 8192 токенов почти не даёт ценности для query длиной до 180 символов; важнее реальное качество на русских event intents, latency и стоимость.

### 9.2 Почему нельзя просто заменить model name

- Текущие vectors — 768, BGE-M3 — 1024.
- Query и documents должны быть закодированы одной моделью и preprocessing version.
- pgvector column/index имеет фиксированную dimension; нужен отдельный table/partition/index.
- Offline BGE batch уже не равен online inference: каждый пользовательский query требует low-latency encoder service.
- Запуск модели внутри браузера или основного Fly web process нарушит thin-runtime, memory и availability contract.
- Self-hosting убирает внешний embedding API call, но добавляет cold start, очередь, CPU/GPU capacity, deployment и собственный SLO.
- Общие benchmark claims не доказывают качество именно на коротких русскоязычных запросах афиши.

### 9.3 Целевая BGE-архитектура

```text
existing immutable catalog snapshot
  -> existing StaticSiteBuilder/BGE batch lane
  -> bge documents + corpus receipt
  -> separate pgvector(1024) namespace/index

user query
  -> search Edge orchestration
  -> dedicated bounded BGE query-inference service
  -> same-version 1024 query vector
  -> BGE index (dense; hybrid only after separate proof)
  -> optional common verifier
```

Query inference service — отдельный stateless capability с bounded queue, timeout, health, model revision и autoscaling/warm-capacity policy. Он не владеет user Auth, quota, result cache или EventCards.

### 9.4 Этапы допуска

| Этап | Режим | Условие выхода |
|---|---|---|
| B0 | Offline golden benchmark Gemini vs BGE | Стабильный русскоязычный gold set и воспроизводимый report |
| B1 | Полный BGE document corpus | 100% coverage, hash/revision checks, zero wrong dimension |
| B2 | Protected query-service canary | p95/error/cold-start/10× burst проходят budget |
| B3 | Shadow для bounded доли авторизованных queries | BGE не меняет ответ; raw query не логируется; disagreement report |
| B4 | 5–10% user-visible A/B | Нет ухудшения quality, latency, empty/error rates и feedback |
| B5 | Primary для всех авторизованных | Release decision, rollback на совместимый Gemini index доказан |

Минимальные quality gates:

- human-curated Russian golden set: intent/category/date/place/admission/family/negative cases;
- Recall@10 и nDCG@10 не хуже current baseline вне заранее утверждённой погрешности;
- zero confirmed hard-negative policy failures;
- no regression в family collapse/date constraints;
- p95 full vector path и error rate соответствуют Search SLO;
- 10× burst не создаёт unbounded queue;
- стоимость/операционное владение приемлемы;
- rollback не требует синхронной переиндексации.

### 9.5 Решение

**Всем авторизованным пользователям можно дать поиск на BGE после B5. Сейчас — нельзя.** Правильный ближайший шаг: использовать уже существующий offline BGE lane для gold/shadow сравнения, не создавать второй пользовательский Search и не менять production vectors до evidence.

## 10. Автотестирование без отдельного Search-фреймворка

Все сценарии добавляются в центральный `static-site-autotest-scenarios.v1.yml` и исполняются существующими L0/L1/L2 adapters. Один Search journey имеет browser/Android/iOS adapters; отдельный workflow на каждый случай запрещён.

### 10.1 Обязательные scenario IDs

| Scenario ID | Слой | Что доказывает |
|---|---|---|
| `search.anonymous_auth_gate` | L0/L1 + mobile specimen | Поле видно; submit делает 0 Search POST; сообщение; email/Yandex actions; query TTL |
| `search.authenticated_contract` | L1 mocked/local integration | Enter/IME, один POST, schema/cards/fallback/feedback/pagination, cleanup |
| `search.live_cached_journey` | protected L1/post-deploy | Реальные Auth session + Edge + cache + pgvector + render на текущем target |
| `search.live_cold_journey` | protected nightly/release | Реальный embedding/vector/verifier-or-degraded path с bounded provider cost |
| `search.cache_provider_zero` | backend integration/live receipt | Второй запрос: cache hit, zero provider/vector/cold-quota deltas |
| `search.vector_corpus_freshness` | L0/build/post-sync | 100% eligible coverage, revision and age constraints |
| `search.transport_route_matrix` | L1/L2 fault injection | direct-down/relay-up, обратный случай, оба down; один POST max |
| `search.quota_abuse_contract` | SQL/backend/load | rolling hour/day, idempotency, concurrency, 429/retry_after, global circuit |
| `search.relevance_golden` | offline analytics | Gemini/BGE/regression quality by segment |
| `search.mobile_keyboard` | L2 Android/iOS | Native search key, viewport, one submit, auth gate and result render |

### 10.2 Запуск

**PR fast, blocking:**

- shared validation/cache-key/unit contracts;
- SQL grants/quota/idempotency contracts;
- local/mock Playwright anonymous + authenticated journey;
- changed Search route screenshot only on failure;
- no real provider calls.

**Feature integration, blocking для Search changes:**

- local Edge/Postgres fixture with fake embedding/verifier providers;
- cache/provider-zero assertion;
- transport matrix with deterministic faults;
- corpus receipt fixture.

**Nightly/background advisory:**

- protected cached production/candidate query;
- full corpus freshness;
- Android/iOS representative Search scenario по selector;
- controlled cold query не чаще утверждённого cost budget.

**Release candidate, blocking:**

- exact immutable target SHA;
- email and Yandex identity compatibility (Auth может быть доказан общим harness, Search не обязан заново рассылать OTP);
- one live cold + immediate live cache hit;
- direct/relay fault matrix;
- Android and iOS native search-key journey;
- zero PII in evidence;
- terminal disposition всех background signals.

**Post-deploy:**

- frequent low-cost cached canary;
- corpus freshness and contract-version canary;
- cold canary по более редкому protected schedule;
- alert, а не silent log.

### 10.3 Тестовые данные

Build формирует private `search-canary-manifest` из текущего eligible corpus:

- natural query;
- expected event/family IDs и validity interval;
- catalog/corpus revision;
- no email/token/raw user data.

Canary не привязан навечно к историческому event. При lifecycle change manifest обновляется вместе с corpus. Для relevance используется отдельный versioned Russian gold set с human labels; LLM judge может помогать triage, но не закрывает gate в одиночку.

### 10.4 Сетевые отказы

Fault injection действует на общую transport layer, а не monkey-patch внутри Search component:

1. direct недоступен, relay healthy — RouteManager выбирает relay, один Search POST;
2. relay недоступен, direct healthy — выбирается direct, один POST;
3. выбранный route теряет body после headers — ambiguous failure, второй POST запрещён;
4. оба route unhealthy до dispatch — `not_dispatched`, quota/provider counters остаются 0;
5. `429` — honoring `Retry-After`, без alternate retry;
6. logout/pagehide — UI освобождён, поздний response не рисуется в новую session.

## 11. Наблюдаемость и SLO

### 11.1 Обязательные dimensions без PII

- request/operation ID;
- static build SHA и Search contract version;
- selected route/capability;
- user HMAC/hash, не UUID/email;
- query HMAC version;
- catalog/corpus/model/policy revisions;
- cache layer/status/age;
- quota result/retry_after;
- stage outcome и timings;
- candidate/exact/possible/fallback counts;
- degraded reason;
- provider sends and circuit state без secrets;
- response decode/render outcome из synthetic client.

### 11.2 Предлагаемые release/SLO targets

Эти значения — целевой контракт после baseline measurements, а не утверждение о текущем production:

- cached synthetic availability ≥99.9% за rolling 7 days;
- cold Search availability ≥99.0% за rolling 7 days, verifier degradation не считается полным отказом при полезной vector выдаче;
- p95 cached end-to-end ≤1.5 s;
- p95 cold vector result ≤4 s;
- p95 final result ≤10 s;
- 100% searchable eligible events имеют compatible documents/embeddings;
- corpus freshness ≤30 минут после успешного canonical event update/vector sync;
- stale/wrong-dimension/orphan counts = 0 на release;
- duplicate cost-bearing POST rate = 0;
- anonymous Search POST rate = 0;
- cache-hit provider-call rate = 0.

Alert после двух последовательных synthetic failures либо немедленно при corpus coverage <100%, contract-version mismatch, all-provider circuit open или duplicate POST evidence. Alert обязан создавать наблюдаемый issue/check/release signal.

## 12. План восстановления и реализации

### W0 — доказать incident root cause

- Выполнить recovery run из §3.1.
- Зафиксировать incident report с exact production identities и request-stage evidence.
- Восстановить текущий Gemini/pgvector baseline без BGE migration.

### W1 — устранить product/identity drift

- Anonymous submit без Search POST и без автоматического OAuth.
- Общие email + Yandex entry points.
- После Auth — восстановление query и явное второе submit.
- Удалить Search-specific identity decisions.

### W2 — cache/corpus correctness

- Ввести `SearchPolicyVersion` и исправить Lite/Gemma env signature mismatch.
- Добавить catalog/corpus revisions в cache key/response.
- Обязательный secret HMAC и query-template version.
- Corpus receipt/gate и короткий degraded cache.

### W3 — transport unification

- Зарегистрировать четыре Search operations в общем `OperationCatalog`.
- Убрать feature-owned route choice.
- Exact CORS/origin policy.
- JSON v1 как release path; NDJSON default-off.

### W4 — abuse/quota

- Rolling 60m + 24h admission.
- Per-user one-in-flight, cheap/cold separation, global provider circuit.
- Adaptive controls без blanket CAPTCHA.

### W5 — tests, canary, alerting

- Добавить scenario IDs в центральный registry.
- Реализовать local provider spies и live counter receipts.
- Protected cached/cold canaries.
- Android/iOS Search adapter на существующем mobile harness.
- Release/post-deploy gates и issue signal.

### W6 — relevance и materialized queries

- Russian golden set.
- Feedback candidate conveyor и privacy gate.
- Общий static collection builder для approved queries.

### W7 — BGE shadow

- B0–B3 без user-visible switch.
- Решение о B4/B5 только по evidence и с rollback.

## 13. Release acceptance checklist

Search нельзя объявлять восстановленным или готовым, пока одновременно не выполнено:

- [ ] точный incident root cause доказан и regression test добавлен;
- [ ] `/poisk/` опубликован из ожидаемого SHA и имеет compatible Search contract version;
- [ ] anonymous submit: 0 Search POST, корректное сообщение, email + Yandex actions;
- [ ] email и Yandex sessions дают один и тот же authorized Search contract;
- [ ] один user gesture = один cost-bearing POST;
- [ ] live cold query возвращает current valid EventCards;
- [ ] immediate повтор — доказанный cache hit с нулевыми provider/vector/cold-quota delta;
- [ ] verifier outage даёт vector-only degraded result, не blank;
- [ ] corpus coverage 100%, stale/orphan/wrong-dimension = 0;
- [ ] direct/relay matrix проходит без duplicate Search;
- [ ] rolling quota/concurrency/global circuit проходят;
- [ ] Android Chrome и iOS Safari native search-key journeys проходят на immutable target;
- [ ] cached и cold synthetic canaries наблюдаемы и alerting проверен;
- [ ] evidence не содержит query/email/token/OTP;
- [ ] approved query pages используют общий static collection pipeline;
- [ ] BGE остаётся shadow, пока B0–B5 gates не закрыты.

### 13.1 Исполняемый Search harness и варианты

Каноническая реализация находится в `site/e2e/search/`. `journey.mjs` знает
только семантические действия; Playwright/Appium mechanics принадлежат adapters.
Обычный Search не получает test mode. Только immutable `secret_candidate` может
принять закрытый `search_variant`, а Edge повторно авторизует canary persona и
сам задаёт cache/LLM policy:

| Variant | Cache | Provider contract | Platform |
|---|---|---|---|
| `cached_vector` | обязательный hit | embedding/vector/LLM delta `0/0/0` | browser, Android, iOS |
| `cold_vector` | read bypass | ≤1 embedding, 1 vector RPC, LLM `0` | browser |
| `cold_vector_llm` | read bypass | vector + server-reserved bounded daily LLM budget | browser |
| `degraded_vector_fallback` | read bypass | deterministic verifier failure, useful vector cards, provider LLM send `0` | browser |

Каждый request получает `client_request_id`; response и owner-scoped receipt
содержат requested/actual mode, contract/policy versions, catalog/corpus
revisions и provider/cache counters. UI acceptance требует один submit → один
POST, terminal state, equality response/rendered IDs, visible cards, real scroll,
`Показать ещё`, отсутствие duplicate IDs/families, repeat cache hit, typed empty
и validation с `0 POST`.

Workflow `.github/workflows/static-site-search-canary.yml` получает только
последний durable accepted candidate через production resolver, маскирует bearer
prefix, сверяет exact target SHA и использует `auth.session_fixture`. Browser
получает server-verified session + RLS probe; Android/iOS получают отдельный
credential и callback в том же platform browser. Ни один job не читает mailbox.
Resolver запускает в Fly image только доступный `python3`; checkout-local
`.venv` не является частью production container contract.
Root-level GitHub wrapper привязывает package resolution к `site/package.json`,
поскольку `npm --prefix site ci` устанавливает Playwright и Supabase JS только
в `site/node_modules`, а bare ESM import ищет зависимости относительно файла
caller, не относительно shell working directory.

Каждый Search upload явно включает hidden-файлы, поэтому локально проверенный
маркер `.redaction-ok` сохраняется внутри GitHub artifact. Артефакт без этого
маркера не является допустимым sanitized evidence, даже если upload-step был
разрешён локальной проверкой `hashFiles`.

Browser runtime probe сохраняет в allowlist оба неприватных server identity:
`request_id` и `receipt_id`. После UI journey root-wrapper читает каждый receipt
через owner-scoped `get_event_search_receipt_v1` и сверяет режим, revisions и
rendered response IDs; отсутствие `receipt_id` является terminal failure, а не
причиной пропустить server-side gate.

Перед remote launch static builder делает online SQLite snapshot, одним чтением
копирует v2 Search receipt в snapshot-scoped immutable файл и тем же canonical
export contract вычисляет полный `catalog_revision` snapshot. Несовпадение
завершается локально как retryable `vector_barrier_catalog_revision_pending`;
Kaggle не тратит полный Astro build на заведомо устаревший corpus. Именно
замороженный receipt передаётся runner, поэтому последующая замена mutable
owner receipt не может изменить revisions уже запущенного candidate.

Исторические расписания (cached каждые 30 минут, cold каждые 3 часа, LLM
четырежды в сутки и nightly mobile), автоматический post-deploy dispatch и
generic issue reporter отключены архитектурной коррекцией из §16. Старый
workflow сохранён только для явного ручного расследования. Production-health
и release qualification теперь разные контуры; на этапе 1 оба новых workflow
работают только как deterministic dry planner и не обращаются к production.

Default server budget равен 12 LLM provider attempts/UTC day: четыре
`cold_vector_llm` запуска по трём непагинированным regression queries. Любая
попытка резервируется атомарно до provider dispatch; исчерпание бюджета
останавливает запрос, а не увеличивает лимит на клиенте.

## 14. Нерешённые operational параметры

Они не требуют второго продуктового документа и задаются после W0/load baseline:

- `D` — daily cold-search budget пользователя;
- cheap cache-hit request cap;
- global reserve fraction Google/BGE pools;
- exact result/degraded cache ceilings в пределах заданных рамок;
- частота protected cold production canary;
- retention raw feedback queries после explicit consent;
- BGE inference placement/cost model.

Каждый параметр хранится в одном versioned runtime policy и отражается в health/telemetry; значения не дублируются в Astro, Edge и SQL.

## 15. Итоговая оценка

Архитектурная основа Search сильнее, чем следует из исходного brief: уже есть authenticated Edge boundary, pgvector, quota idempotency, cache, provider limiter, occurrence collapse и resilient UI cleanup. Главная проблема — не отсутствие алгоритма, а отсутствие **единого release identity, revision-bound cache/corpus contract и живого сквозного доказательства**.

Ближайшее правильное действие — восстановить и зафиксировать нынешний Gemini/pgvector baseline, унифицировать его с общими Auth/transport/tests, затем поставить BGE в shadow. Немедленный перевод production на BGE не лечит недельный отказ и создаст ещё одну недоказанную точку доступности.

## 16. Search production-health: архитектурная коррекция, этап 1

Этот раздел — канонический архитектурный контракт production-health. Он
заменяет прежнее смешение регулярного product health, квалификации релиза и
отладки harness. Этап 1 не выполняет live Search и не подтверждает здоровье
production. Его корректный итог:

```text
ARCHITECTURE_READY_FOR_LIVE_VALIDATION
PRODUCT_HEALTH_UNCONFIRMED
```

### 16.1 Фактическая AS-IS схема и ответы аудита

Путь восстановлен по коду `origin/main` `65926e63b436efcc275c8e4094473ab9ef1fd9e8`,
а не по старому локальному checkout:

```text
Smart Update
  -> schedule_event_update_tasks
  -> coalesced joboutbox static_site_build request
  -> job_static_site_build_kaggle
  -> repo SHA из Fly image revision file
  -> vector barrier
  -> immutable SQLite snapshot + snapshot-scoped Search receipt
  -> input_fingerprint(repo_sha + snapshot/projection/build config)
  -> durable single-flight claim + remote_handoff
  -> Kaggle StaticSiteBuilder exact staged source/snapshot/config
  -> validated immutable secret candidate
  -> create-only publication + read-back hash verification
  -> finish_static_site_build_claim
  -> static_site_build_state.current_secret_candidate_receipt_json
  -> resolve_current_secret_candidate
  -> current accepted /_review/<opaque>/poisk/ target
```

1. **Откуда data-only generation получает repo SHA.**
   [`scripts/deploy_fly_main.sh`](../../../../scripts/deploy_fly_main.sh)
   передаёт clean `origin/main` SHA в Docker build; [`Dockerfile`](../../../../Dockerfile)
   записывает его в `/app/.static-site-repo-sha`.
   [`main.py::_resolve_static_site_repo_sha`](../../../../main.py#L23129-L23167)
   читает этот файл и fail-closed отвергает mutable `STATIC_SITE_REPO_SHA` в
   Fly runtime (fallback разрешён только локально). Baked source manifest
   связывает SHA с детерминированным digest точных файлов image; runner и
   Kaggle повторно проверяют source tree/archive, а result и candidate receipt
   обязаны сохранить тот же source identity. Тот же SHA передаётся в
   [`_static_site_build_kaggle_command`](../../../../main.py#L23170-L23330) и
   remote handoff.
2. **Moving main или pin.** Build после Smart Update закреплён на runtime SHA
   уже развёрнутого Fly image, а не читает движущийся GitHub `main`. Он может
   регулярно менять данные, `snapshot_id`, `build_id` и content revisions,
   сохраняя тот же runtime SHA. Второй механизм pinning не нужен.
3. **Current accepted target.** Каноника —
   [`static_site_release.py::resolve_current_secret_candidate`](../../../../static_site_release.py#L1000-L1044),
   а CLI-адаптер — [`scripts/request_static_site_build.py`](../../../../scripts/request_static_site_build.py#L42-L64).
   Latest running/complete Kaggle kernel не является target.
4. **Где repo SHA и build metadata.** До завершения они лежат в
   `joboutbox.payload.snapshot`, `fingerprint_evidence` и `remote_handoff`
   ([`main.py`](../../../../main.py#L24347-L24440)). После проверенной публикации
   полный immutable receipt сохраняется в
   `static_site_build_state.current_secret_candidate_receipt_json`
   ([`static_site_release.py`](../../../../static_site_release.py#L1346-L1446)).
   Candidate также публикует `candidate-build.json`.
5. **Что запускало Search canary до этого PR.**
   [`static-site-search-canary.yml`](../../../../.github/workflows/static-site-search-canary.yml)
   имел ручной dispatch, post-deploy repository dispatch, cached cron дважды в
   час, cold cron каждые три часа, четыре LLM cron в сутки и nightly mobile.
6. **Откуда issue #431.** Старый inline terminal reporter считал повторным
   любой failure после последнего scheduled failure, а ключ строил только из
   platform + variants. Он смешивал `search_cache_state`, revision mismatch и
   infrastructure failure, мог закрыть scheduled issue ручным success и не
   использовал типизированный result. Это шум orchestration, не доказательство
   отказа продукта.
7. **Где `release_exact` связан с moving revisions.**
   [`run.mjs::assertTargetSha`](../../../../site/e2e/search/run.mjs#L36-L58)
   берёт замороженные revisions candidate, а
   [`acceptance.mjs::assertSearchRevisionPolicy`](../../../../site/e2e/search/acceptance.mjs#L159-L185)
   сравнивает их с живым ответом Edge. После обычного vector/catalog refresh
   здоровая выдача могла быть отвергнута как
   `search_target_response_revision_mismatch`.
8. **Что переиспользуется вместо новой release-системы.** `repo_sha`,
   `build_id`, `run_id`, `snapshot_id`, `input_fingerprint`, hashes candidate
   receipt, `catalog_revision`, `corpus_revision`, `search_document_revision`,
   Edge `search_contract_version` и `policy_versions`. Новая таблица, release
   service или active-release control plane не создаются.

### 16.2 Четыре identity и граница runtime/data

| Identity | Канонический существующий источник | Семантика health |
|---|---|---|
| `site_runtime_sha` | accepted candidate `repo_sha`, первоначально baked Fly image SHA | функциональный runtime; фиксируется на run |
| `search_backend_revision` | generated `search-backend-revision.generated.ts`; validated marker и фактический Search response | exact `sha256:<64>` digest исполняемого source tree Edge Function |
| `content_generation_id` | `snapshot_id`/`input_fingerprint`; content identity `catalog_revision` | telemetry, не exact health gate |
| `search_index_generation_id` | `corpus_revision`/`search_document_revision` | telemetry, не exact health gate |

`site_runtime_sha` берётся из реального Fly/runtime deploy и Smart Update сам
его не меняет. Stage 1 source-binding gate теперь доказывает соответствие этого
SHA точному набору source bytes, переданному в Kaggle, и запрещает recovery
handoff от другого deployed SHA; одной повторяемой строки SHA в metadata
недостаточно. Actions checkout SHA может отличаться от
target repo SHA и сам по себе не является failure. Edge identity не выдаётся за
git/deploy SHA: это отдельный точный content digest deployable source tree
`supabase/functions/event-search/`, вычисляемый
`scripts/generate_event_search_revision.mjs`. Digest использует отсортированные
portable paths и length-framed raw file bytes; generated revision module и
`*.test.*` исключены. CI запускает generator с `--check`, поэтому изменение
исполняемых `.ts/.tsx/.js/.mjs/.json` bytes без обновления generated constant
fail closed.

В текущем marker contract `search_backend_revision` обязан быть точным
ожидаемым `sha256:<64>` source digest, а непустой `deployment_run_id` служит
correlation receipt. Для deploy-triggered запуска ожидаемая revision проверяется
ограниченным `HEAD /functions/v1/event-search`: active Edge Function отвечает
публичными `X-KenigEvents-Search-Revision` и
`X-KenigEvents-Search-Contract`, не выполняя Auth, quota, DB, provider или
product Search. Первый header сравнивается с marker строго и отдельно от
совместимого contract version. Этот HEAD probe и active accepted-site receipt проходят
**до** session/Auth/Search; отсутствие или mismatch даёт
`BLOCKED_RELEASE_NOT_ACTIVE` и нулевой product POST. Единственный разрешённый
Search response содержит оба независимых поля: точный фактически наблюдённый
`search_backend_revision` и compatibility telemetry
`search_contract_version`. Evidence всегда берёт backend revision из ответа,
включая schedule/manual runs без expected marker; marker не подменяет observed
runtime evidence. Если Edge сменился между успешным HEAD и единственным Search
POST, только другой синтаксически валидный response digest даёт
`BLOCKED_RELEASE_NOT_ACTIVE` без retry и без product incident. Отсутствующая или
malformed revision в Edge error остаётся ошибкой Search response, а не ложным
release drift. Source digest нельзя называть или подменять придуманным git либо
provider deploy SHA.

### 16.3 Три тестовых контура

**A. Deterministic Search CI.** Offline suite на PR проверяет planner/trigger
policy, target normalization/pinning, один POST, допустимые cache hit и miss,
revision drift, LLM/pagination zero, failure taxonomy, retry policy, redaction и
48/96 KiB cost guard. Secrets, production target, Supabase, Kaggle, browser
device labs и providers не используются.

**B. Search production health.** Реализован на этапе 2: manual, дважды в сутки
и после явного реального Search runtime deployment marker. Утренний профиль проверяет
browser + Android Emulator, вечерний — browser + iOS Simulator; каждая platform
cell получает отдельную no-mail session. В каждой cell выполняются один
vector-only UI query, ровно один Search POST с limit ≤5, 1–5 карточек, реальный
wheel/touch scroll и открытие одной карточки с HTTP 200. Cache `hit`, `miss` и
`stored` допустимы; canary-only `bypass` не относится к health. Второго запроса, pagination, LLM, receipt RPC,
Storage image и real-mail OTP нет.

**C. Search release qualification.** Только manual/selective. Здесь остаются
cold vector, bounded LLM, degraded fallback и расширенные pagination/transport/
mobile matrices сверх минимального health journey. `release_exact` применяется
лишь к свежему immutable candidate как promotion gate. Candidate mismatch
блокирует квалификацию, но не объявляет текущий production Search сломанным.
Профиль `full` после успешного standard health ровно один раз запускает в новой
no-mail browser session отдельной persona `search-cold-browser` существующие `cold_vector_llm` и
`degraded_vector_fallback`; прямого schedule у qualification нет.

Существующий `site/e2e/search/` остаётся единственным harness. Stage-1 planner
и contracts добавлены рядом с ним; auth fixture и `site/e2e/mobile-web/` не
копируются и не переписываются.

### 16.4 Target resolver contract

1. I/O adapter читает только canonical current accepted receipt.
2. Opaque URL маскируется до первого display/log/evidence; полный URL живёт
   только в памяти live adapter. Target обязан иметь exact origin
   `https://kenigevents.ru` без credentials, нестандартного port, query или
   fragment. Публичный `https://kenigevents.ru/poisk/` не является fallback,
   пока это не реально выпущенная поверхность.
3. Полный immutable tuple фиксируется один раз до journey. Checkout SHA с ним
   не сравнивается. `content_generation_id` и `search_index_generation_id`
   записываются как telemetry. Canonical `--show-current-review` adapter
   обязан получить `input_fingerprint` вместе с build/run/repo/snapshot и
   digest-полями, а также проверить, что SHA-256 opaque URL token совпадает с
   `token_sha256` accepted receipt.
4. После terminal journey, включая failure после dispatch, выполняется только
   повторная проверка pointer. Если identity изменилась, записывается
   `target_superseded=true`; Search не повторяется, а reporter не создаёт и не
   закрывает product issue по этому результату.
5. Pre-dispatch resolver retry разрешён только при доказанных нулевых side
   effects. После Search dispatch retry запрещён.

Этап 2 подключает этот interface к production resolver. Он читает pointer до
session/Search и ровно один раз после journey; секретный URL не попадает в
output/artifact, evidence хранит только его SHA-256.

### 16.5 Trigger matrix

| Событие | Deterministic CI | Production health после этапа 2 | Release qualification |
|---|---:|---:|---:|
| PR с Search/Auth/workflow contract | да | нет | нет |
| manual health | нет | да | нет |
| twice-daily health | нет | да | нет |
| реальный `site_runtime_sha`/Search backend deploy | нет | да | selective |
| manual qualification | нет | нет | да |
| Smart Update / snapshot / data-only generation | нет | **нет** | нет |
| corpus/index/catalog refresh | при contract diff | **нет** | только по решению release owner |
| latest Kaggle job change | нет | нет | нет |

Production-health workflow принимает только две указанные cron-записи,
закрытые manual profiles и `repository_dispatch: search-runtime-deployed` с
явным `standard|full`. Smart Update, snapshot, Kaggle, corpus и index paths не
эмитят этот event. До двух terminal live proofs автоматические schedule/dispatch
fail-closed выключены repository variable `SEARCH_PRODUCTION_HEALTH_ENABLED`;
manual profiles остаются доступными для этих двух acceptance runs.

### 16.6 Failure taxonomy

Допустимы только следующие типизированные результаты:

```text
HEALTHY
DEGRADED
BROKEN_SEARCH_SURFACE
BROKEN_AUTH_INTEGRATION
BROKEN_SEARCH_REQUEST
BROKEN_NO_RESULTS
BROKEN_RESULT_RENDER
BROKEN_RESULT_ROUTE
UNKNOWN_AUTH_BROKER
UNKNOWN_RUNNER_BROWSER
UNKNOWN_ANDROID_INFRA
UNKNOWN_IOS_INFRA
BLOCKED_RELEASE_NOT_ACTIVE
COST_GUARD_FAILED
EVIDENCE_REDACTION_FAILED
```

Только `BROKEN_*` означает product failure. `UNKNOWN_*` — неизвестный результат
из-за инфраструктуры; `BLOCKED_*` — admission/release boundary; cost/redaction
fail closed, но не доказывают поломку Search. Cache miss, content/index drift,
candidate mismatch и `target_superseded` не являются product failure.

### 16.7 Incident policy

Product incident создаётся только для typed `BROKEN_*` в контуре current-target
production health. CI, release qualification, manual legacy debug,
`UNKNOWN_*`, `BLOCKED_*`, cost/redaction guard и superseded result не создают и
не закрывают product issue. Новый reporter ключует product issue по platform +
typed `BROKEN_*`; HEALTHY закрывает только product issue той же platform.
Одинаковый `UNKNOWN_*` создаёт infrastructure issue только на третьем подряд
terminal результате своей platform; cost/evidence имеют отдельные fingerprints.
Если sanitized evidence отсутствует, workflow output `BROKEN_*` не считается
product proof и fail-closed классифицируется как platform `UNKNOWN_*`.

Broker `persona_busy` также является `UNKNOWN_*`, а не Search failure. До
успешного выпуска credential новый owner блокируется исходным crash lease;
после выпуска active lease сокращается до двухминутного encrypted replay TTL.
Это сохраняет cross-process lost-response replay, но не оставляет успешно
завершившийся диагностический запуск владельцем persona на 20 минут. Повторный
workflow всё равно сериализован общей GitHub concurrency group.

### 16.8 Egress budget и byte meter

Действующий health contract:

- Search POST `1`, result limit `≤5`;
- LLM attempts `0`, pagination `0`, receipt RPC `0`, Storage images `0`;
- target client-observed Supabase bytes `≤48 KiB`;
- hard cap `96 KiB`, повышать его запрещено.

Pure byte-meter классифицирует browser-observed responses как Auth, Edge
Function и direct REST/RPC. Он использует корректный `Content-Length`, когда
body недоступен, либо UTF-8 размер реально полученного body; Yandex Object
Storage/CDN и не-Supabase origins исключаются. В evidence попадают только
категория и целые byte counters. Это **client-observed budget proxy**, а не
точный Supabase billing meter.

Browser network diagnostics считают ошибкой только failed main-document
navigation либо failed request к allowlisted Supabase Search/Auth/REST/RPC
origin. Сломанный декоративный image/font subresource другого origin не может
сам объявить Search `BROKEN_SEARCH_REQUEST`: наличие 1–5 видимых настоящих
карточек, отсутствие skeleton/placeholder, точный response/card id match,
реальный scroll и event-route HTTP 200 всё равно проверяются отдельно.

На mobile callback network receipt сворачивается сразу до категории и числа
полученных bytes; после attach тот же Appium session выполняет явные `getUser`
и один owner-RLS read через instrumented resilient transport. Raw URL, token,
body и driver log не сохраняются. Android использует Appium `3.6.0` + pinned
UiAutomator2 `8.2.2`; iOS — Appium `3.6.0`, Xcode `16.4`, pinned XCUITest
`12.1.4` и соответствующий downloaded WDA. Workflow устанавливает и проверяет
точную driver version до side-effect-free preflight. XCUITest capability
`showSafariConsoleLog=false` включает `safariConsole` bucket без печати raw
events в Appium stdout; `showSafariNetworkLog=false` аналогично сохраняет
полный `safariNetwork` bucket для закрытого meter, но не зеркалирует private URL
в server log. Оба Appium server запускаются с `--log-level error` и URL log
filter, а exact target/callback регистрируются через GitHub `add-mask` до первой
WebDriver navigation. Отсутствие capability не считается доказательством
нулевых console errors. При отсутствии `Content-Length` mobile meter ждёт
terminal `loadingFinished` через несколько log-drain циклов; незавершённый
request fail-closed становится platform `UNKNOWN_*`, а не нулём bytes.
Android emulator action запускает переданный script через POSIX `sh`, поэтому
production-health wrapper явно входит в Bash до `set -o pipefail`; это
зафиксировано workflow contract test и не относится к Appium/Auth/Search retry.
На свежем emulator Chrome DevTools/WebView context может появиться уже после
готовности WebDriver session, поэтому side-effect-free preflight bounded-waits
до 20 секунд за одновременным наличием native и web context. При session-create
failure локальный Appium log немедленно сворачивается в allowlisted booleans
(`chromedriver_missing`, download/Chrome/WebView/UiAutomator2 failure class) и
удаляется; raw строка, URL, capability, device/session id в artifact и stdout не
попадают. Это диагностический infrastructure receipt и не разрешает broker,
Auth или Search side effect.
Standalone WebdriverIO `9.30.0` не содержит deprecated JSONWP `getLogs` даже
при `wdio:enforceWebDriverClassic=true`; это официальный API-контракт, а не
отсутствие Appium performance/Safari bucket. Shared Appium adapter добавляет в
созданную session только точный `POST /session/:sessionId/log` через protocol
command factory pinned `webdriver@9.30.0`. Отсутствие/ошибка bucket по-прежнему
даёт platform `UNKNOWN_*` до product verdict. Browser failure evidence отдельно
считает document, Auth, Edge, REST и RPC failure classes, не сохраняя URL или
текст ошибки.
Android performance receipt сохраняет request id только внутри замыкания. Для
успешно завершённого ответа authoritative terminal event —
`Network.loadingFinished`; для отменённого/failed фонового Auth request —
`Network.loadingFailed`. Во втором случае учитывается только сумма фактически
полученных `Network.dataReceived.encodedDataLength`, после чего request считается
закрытым. Raw URL, request id, response body и `errorText` наружу не выходят.
Если terminal closure всё же не наступил, failure code содержит только два
закрытых измерения: класс Auth path (`verify`, `user`, `token`, `mixed`) и
достигнутую фазу (`request_only`, `response_seen`, `response_data_seen`). Это
диагностирует реальный Chrome/Appium protocol boundary без raw network artifact.

### 16.9 Migration from current workflow

- `static-site-search-canary.yml` — только ручной legacy/debug; прежние четыре
  cron и post-deploy repository dispatch удалены; generic issue mutation нет.
- `search-production-health.yml` — две bounded schedule, explicit manual profile
  и закрытый runtime-deploy marker; каждая platform job вызывает единый runner.
- `search-release-qualification.yml` — manual/selective browser qualification;
  `full` marker может запросить её ровно один раз после health terminal PASS и
  выполняет bounded LLM + deterministic degraded fallback в одной ephemeral
  browser session.
- default PR CI запускает deterministic Search architecture, существующий
  harness и broker admission/security suites.

Data-only generation по-прежнему не имеет Search workflow trigger. Legacy
cached/cold/LLM/mobile harness остаётся только manual debug и сериализован с
health по общей session-fixture concurrency group. Qualification имеет
отдельную concurrency group и отдельную cold-browser persona, поэтому вызов
`full` в том же GitHub run не переиспользует уже потреблённый health callback.

### 16.10 Ограниченный вывод по PR #436

[PR #436](https://github.com/onedayonemasterpiece/events-bot-new/pull/436)
закрывает окно между переводом `joboutbox` static owner в `running` и записью
durable `active_job_id`: vector sync дополнительно видит running outbox и
откладывается как `static_build_preclaim`. Это полезная защита immutable
candidate/release qualification, но не runtime pinning и не исправление
ошибочной scheduled `release_exact` health-модели.

Решение этапа 1: PR не merge/cherry-pick/deploy. Кодовая защита полезна
независимо и должна пройти отдельный rebase/review; она не нужна для
user-observable one-query production health. Также она остаётся point-in-time
guard, а не доказанный двунаправленный atomic mutex.

### 16.11 Что реализовано на этапе 1

- pure planner, target pin/superseded contract, typed outcomes, incident/retry
  policy и Supabase byte-meter;
- deterministic tests для всех stage-1 acceptance boundaries;
- отключение automatic legacy traffic и generic issues;
- manual-only dry workflows для health и qualification;
- deterministic Search suite в default PR CI;
- этот канонический контракт и отдельный
  [`stage-2-production-health-handoff.md`](stage-2-production-health-handoff.md).

### 16.12 Что реализовано на этапе 2 и что осталось

Реализованы production resolver/pointer reread на success и failure,
one-query browser/Appium
journey, обязательный реальный wheel/swipe, canonical
`/sobytiya/<slug>/` HTTP-200 card open, typed empty/render terminal states,
client-observed byte meter до конца event navigation, platform
broker identity/admission и bounded encrypted durable lost-response
replay (сверх in-process coalescing),
explicit deploy marker, две schedule, manual profiles и typed reporter.
Automatic schedule/dispatch остаются default-off. Перед первым live run
migration broker claims должна быть
применена, `supabase/functions/event-search` отдельно задеплоен из exact
`origin/main` и подтверждён HEAD receipt, Fly broker — задеплоен из того же
exact `origin/main`, а новый workflow ref —
добавлен в broker allowlist вместе с exact event allowlist
`workflow_dispatch,schedule,repository_dispatch`. Затем допускаются максимум
два полных workflow:
browser+Android и browser+iOS. До их terminal evidence состояние остаётся
`PRODUCT_HEALTH_UNCONFIRMED`.
