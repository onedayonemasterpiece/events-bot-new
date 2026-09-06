# Сквозная интеграция релиза: транспорт, статистика, продуктовые решения и персонализация

> 2026-09-05. Документальный integration contract в существующем release umbrella. Подготовлен в PR #587; до merge не объявляется действующей production-реализацией или новым GO. Поручение владельца: восстановить связи июльского «Плана релиза» с текущими голосовым поиском и сквозной системой Floating Island.
>
> Родитель: [Static personal announcements](README.md). Event-page/cutover owner: [План релиза](../static-site-pages/release-plan.md). Здесь определены **стыки** существующих контуров, не второй транспорт, analytics framework, профиль, backlog или release manager.

## 1. Что восстановлено из документов

Чтение выполнено по `main@b8f463f5c35fa62befcfed171a7a8a0886af20f7`, исходному пакету `PR #587@62c54ce42786eecc5b380ea3dba002af78df8fd0` и текущему телу [#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621). Это source/document review, не live connectivity/DB/dashboard acceptance. Фрагменты AS-IS и counts с датами июля/августа не считаются проверкой сегодняшнего production. При реализации нужен current remote read.

| Источник, прочитанная область | Сохраняемый смысл |
|---|---|
| [Release umbrella](README.md), [global decisions](global-product-decisions.md) | F1–F17 не исчезают из-за новых UI-задач; четыре независимых состояния: задумано, реализовано, работает, полезно. |
| [План event-page релиза](../static-site-pages/release-plan.md): ledger, data progression, publication/cutover gates, AM-track | Свежие данные и downstream consumption, canonical URLs, last-good/rollback, exact acceptance; исторические checkpoints не текущий GO. |
| [Presentation checklist](../static-site-pages/presentation-release-checklist.md) | Визуальная приёмка не означает работающие Auth/Search или регулярную продуктовую статистику. |
| [Production integration](../unsigned-personalization/production-integration.md): transport/replay/acknowledgement | Единый capability-aware клиент; полный body/decode, passive health, выбранный один dispatch, durable acknowledgement. |
| [Yandex dependency resilience](../../operations/yandex-dependency-resilience.md): capabilities, degradation/receipts | Отдельные direct/relay, analytics, OAuth, mail и CDN dependencies; sidecar не отменяет основной эффект. |
| [Analytics](../static-site-pages/analytics/README.md): §§0–5, 7–16, 19–33 | Единые actors/sessions/exposures/denominators, три класса данных, reverse bridge, daily readout, отдельные consent/retention/budgets. |
| [Personalization requirements](../static-site-pages/personalizaion/requirements.md), [TO-BE](../static-site-pages/personalizaion/personalization-to-be.md): §§1–12 | Activation, exact hides, query/context priority, разные surface policies, ниже-viewport rerank, scheduled materialization и ETag projection. Ручной requirements.md не изменялся. |
| [Data ownership](../../architecture/personalization-data-ownership.md) | Один current profile/strong-state owner; YDB analytics не второй профиль; localization gate сохраняется. |
| [Product model](../../product-model/README.md): §§1–6 | Need/Job → journey/capability → acceptance → факты/метрики → решение, а не подсчёт чатов вместо результата. |
| [Autotest strategy](../../operations/static-site-autotest-strategy.md), [реестр](../../testing/static-site-autotest-scenarios.v1.yml) | L0–L3, один набор сценариев/адаптеров, явно разные fixture/live evidence. |
| [OperationCatalog](../../../site/src/lib/backendOperationCatalog.ts), Search source ранее прочитанного пакета | Реальные safe-read/selected-once/replay/disposable классы; текущий Storage direct-only; нет подтверждения новых voice/analytics routes только из наличия документа. |

Основной дефект предыдущего пакета: Search/Auth/limiter уже упоминались, но не была восстановлена вся цепочка delivery → product fact → statistics → decision → personalization → next presentation. В результате новое voice-origin решение могло обойти общий transport, а список метрик — не дать измеримого продукта.

### Адресные исправления, а не сброс прежних решений

1. Упоминание одного voice origin заменяется явным capability/route contract. Место выполнения worker и путь доступа из браузера — разные решения.
2. «Есть статистика» разделяется на producer, принятие ingest, aggregate, readout и reviewed finding. Ни один этап не заменяет остальные.
3. Устаревшее общее «personalization consent» уточняется до activation + собственных purpose consents по принятому TO-BE. Новый обязательный checkbox не вводится; это проектное правило, не новое юридическое заключение.
4. Conversation history/active intent не становятся вторым профилем и не управляют аналитическими actors/sessions.
5. Exact hide/undo и актуальное состояние карточек применяются и в старых answer sections; immutable receipt не даёт права повторно показывать явно скрытое.
6. Старые ALB/two-bucket и host-fallback этапы не навязываются текущему пути #621. Текущая генерация/публикуемые previews используют один Kaggle builder и существующий bucket; требования целостности, production promotion/rollback не отменяются.
7. Порог/срок из проектного документа не становится измеренной ёмкостью, SLA или разрешением новых внешних расходов.

## 2. Общая модель продукта

```text
canonical events / accepted facts / fresh schedules
  → immutable catalog + card/collection projections
  → static page + shared island system
  → contextual/personalized ranking or conversational Search
  → actually visible served list
  → user intent/action
  → primary durable acknowledgement
       ├→ immediate state + later profile materialization → next projection
       └→ eligible de-identified analytics projection

optional consented weak UI summaries → one analytics ingest → YDB aggregates
  → owner readout / Product Atlas evidence → reviewed decision
  → versioned product/model/UI change → same-corpus tests / release evidence
```

Analytics не замыкается обратно в профиль произвольным join. Профиль получает только разрешённые собственным contract сигналы. Аналитическая карта действий вообще не является taste/rank input. Default static navigation и CTA не зависят от optional аналитики.

## 3. Стабильное подключение: действительно два направления

### 3.1. Product/Auth/Search → Supabase

```text
Browser → Supabase direct
     OR → Yandex API Gateway stateless relay → тот же Supabase upstream
```

Общие владельцы — BackendClient/OperationCatalog/route manager и production-integration. Auth, Data, Functions, Storage и OAuth health разделены. Success означает полученное и проверенное тело, не только headers/HTTP 200. Cold probe bounded, staggered/single-flight; реальная успешная операция обновляет здоровье, idle polling не требуется. Disposable telemetry не меняет здоровье product route.

Safe-read может один раз использовать проверенный alternate. Selected-once выбирает путь **до** тела и не повторяется при неоднозначном результате. Idempotent replay требует серверного ключа/uniqueness. 401/403 — identity/permission, 409 — conflict, 429 — cooldown, не повод обходить ограничения через другой origin.

Relay не содержит service-role, не является open proxy и не восстанавливает общий упавший Supabase. Yandex OAuth — отдельная зависимость; working Data/relay не доказывает доступность provider callback или восстановление потерянной JWT-response. Email/Auth fallback сохраняет существующую семантику.

### 3.2. Optional статистика → Yandex ingest

В analytics §24 уже выбран обратный маршрут:

```text
Browser → Yandex API Gateway → один analytics ingest → isolated YDB analytics
     OR → Supabase Edge blind bridge → тот же analytics ingest → тот же YDB
```

Это не репликация баз в обе стороны. Существуют две группы запросов с разными authoritative destinations. Ни A→B→A routing loop, ни цепочка нескольких попыток через каждый доступный proxy не допускаются.

Blind bridge не создаёт raw analytics rows в Supabase, не принимает произвольный upstream URL и не переносит YDB credentials в браузер. Подтверждение прокси без bounded downstream receipt не считается доставкой. Shared `batch_id` и sink dedupe защищают от двух copies после потерянного ACK. YDB outage остаётся pending/drop согласно классу данных, а не «recovered» из-за успешного Edge response. Наличие этой архитектуры в документе не доказывает deployed bridge.

### 3.3. Primary facts → analytics projection

Strong action подтверждается primary store. В той же согласованной transaction/outbox boundary фиксируется последующая проекция; worker отправляет обезличенный факт. Ошибка YDB не откатывает like/save/hide, не вызывает новый product command и не меняет UI на «не сохранено». Local-only, committed и projection_pending — разные состояния.

### 3.4. Голос — новый потребитель того же policy, не отдельный transport

Worker может быть на Devstand; browser-facing capability добавляется в общий каталог с explicit routes, request/response caps, timeouts, semantics и полным nonce/body probe. Названия ниже — implementation plan, не deployed endpoints:

- `assistant.control`: session/utterance metadata, получение receipts/sections, reset/delete; direct first-party route и заранее проверенный фиксированный relay route. Auth и business validation одинаковы на обоих путях.
- `assistant.audio-upload`: отдельный media capability. Стандартные независимые audio segments должны помещаться в минимальный проверенный route envelope; длинная речь разбивается без потери начала слов и sequence. Сумма частей не превращается в несколько пользовательских поисков.
- Если доступен только direct-only media adapter, UI явно показывает эту ограниченную поддерживаемость. Нельзя объявить relay-capable всю функцию, проверив лишь маленький JSON GET. Нельзя молча отправить слишком большой blob через запасной gateway.

Внешняя проверка 2026-09-05: [Yandex API Gateway](https://yandex.cloud/en/docs/api-gateway/concepts/limits) указывает maximum request/response **2.5 MB**. Следовательно, прежние proposed 8 MiB upload не совместимы с этим relay автоматически. Начальный безопасный target стандартного сегмента — до 1 MiB **итогового wire body**, с учётом multipart/base64 и downstream caps; это инженерный default, проверяемый actual probes, не provider entitlement. Сложный streaming proxy не нужен до доказанной необходимости. [Supabase Edge](https://supabase.com/docs/guides/functions/limits) имеет свои CPU/wall-clock ограничения; bridge не должен транскрибировать/перекодировать тяжёлое аудио и ждать весь длинный диалог.

Для реального alternate сначала выяснить, какой путь уже provisioned, затем добавить allowlisted operation, не второй gateway вслепую. Supabase fallback для доступа к Yandex analytics не является автоматическим audio proxy и не должен экспортировать private voice payload вне утверждённого data-flow.

Публичный availability manifest — coarse service hint. Он не доказывает соединение конкретного браузера с конкретной capability, не содержит personal quota и не включает постоянные direct pings со всех страниц. Receipt polling только у активной операции — другое, ограниченное назначение.

## 4. Статистика: что именно доходит до владельца

### Три независимых класса

| Класс | Источник истины | Доставка и потеря |
|---|---|---|
| Strong product facts | Save/calendar desired state, accepted feedback, явные действия и существующий primary store | Receipt/outbox/idempotency; потеря optional projection не отменяет факт. |
| Weak consented observations | Реальное exposure, depth, hint/section interaction, performance samples | Compact accumulator/batches, общий bounded outbox; допустимое shedding с coverage counters. |
| Essential operational evidence | Request phase, route/limiter outcome, terminal error, freshness | Минимальные технические данные; нельзя под этим названием скрыть стабильный analytics actor или raw transcript. |

Один pipeline не должен одновременно считать browser click, повторный POST и outbox projection тремя conversions. Save и calendar по global decisions — один durable saved-event state; ICS export и фактический внешний calendar import различаются. Клик ticket/route/volunteer не означает покупку, поездку, регистрацию или attendance. `general_stats` и TG/VK `post-metrics` не заменяют first-party product funnel.

### Feature-specific measurement mapping для голоса и островов

Единый envelope, actors, session timeout, source/device classes и metric semantics берутся из analytics owner. Добавляем только bounded dimensions: input modality, capability/route class, approved surface/placement, section/result-set surrogate, profile/model/corpus/UI versions, outcome/degraded class. Raw вопрос, ответ, заголовок с личными деталями, audio, полный profile, JWT, precise coordinates и private URLs не уходят в общий поток.

- Eligibility, видимость entry, старт записи, принятая содержательная реплика, rendered answer, **реально увиденный** ответ и полезное действие — разные факты. 202 receipt не считается answer/view/conversion.
- Denied/degraded opportunities учитываются отдельно, чтобы выключенная кнопка при перегрузке не улучшала conversion искусственно. Operational агрегаты без analytics identity не подмешиваются в согласованную actor conversion cohort.
- Основной показатель cards-to-value дедуплицирует canonical event по принятому session/surface правилу. Повтор карточки в трёх answer sections не означает три уникальных события. Section-local exposure допустим для изучения уточнений, но глобальный итог не суммирует его вслепую.
- Sticky/full-flow форма одного heading, повторное пересечение viewport и восстановление Back не создают новые page views или ложное новое использование функции.
- Shared shell отдаёт actual occupied rectangles; exposure учитывает перекрытие островами, viewport и visibility threshold общего контракта. Один большой bounding box вокруг разнесённых островов не равен реально закрытой площади.
- `served_list_id`/opaque presentation receipt описывает **фактически показанный** порядок, surface, версии scorer/profile/catalog и experiment, не сырой профиль. `section_id` не заменяет served-list identity. Проверка DOM↔served обязательна.
- Keyboard/touch/voice activation одной CTA дают один accepted action с modality, а не три новых metric definitions. Input text/keystroke sequence не логируются.

### Общие бюджеты, не новый лимит на каждую функцию

Weak telemetry голоса/островов входит в существующие ≤3 bounded batches/session; карта действий использует свою долю внутри них. Не добавлять запрос на каждый audio frame, изменение громкости, scroll tick или смену sticky state. Bounded aggregate `count/max_bucket/flags` достаточен для большинства вопросов. Cost/usage provider — отдельный ledger, не weak analytics.

Найденное несогласование: analytics §27 допускает weak outbox до 7 дней, shared browser transport/TO-BE задаёт 16 записей/12 KiB/24 h/5 попыток, aggregate browser state 64 KiB. Пока отдельный bigger queue не спроектирован и не принят, новые consumers используют **пересечение действующих ограничений**, то есть фактический shared envelope и не более 24 h для этой очереди; packet expiry есть минимум всех применимых сроков. Это не новый второй outbox. Server analytics retention (actor facts, daily aggregates) остаётся отдельной политикой, не сокращается автоматически до browser TTL.

## 5. Продуктовая обвязка: не просто счётчики

Для каждой значимой capability сохраняется связь:

```text
Job / user outcome / owner outcome
 → journey и capability
 → MeasurementQuestion + population + denominator + guardrails
 → registered facts + authoritative source
 → acceptance/test + query/readout
 → reviewed finding (факты отдельно от интерпретации)
 → decision + follow-up measurement
```

Не создавать вторую Product Atlas/дашборд-платформу. Общие определения принадлежат analytics и product-model; feature home добавляет её вопросы. Existing private aggregate readout/DataLens или воспроизводимый приватный report — поверхность владельца. SQL/query template без deployed sink не называется работающим dashboard. Source history/main/branch/deployed/exposure/measured/owner acceptance фиксируются раздельно.

| Вопрос | Измерение / корректный denominator | Какое решение поддерживает |
|---|---|---|
| Голос помогает найти событие? | event value/intent actions среди eligible exposed actors; latency, corrections, abandonment, capacity denied рядом | Расширить или скорректировать voice experience, не оптимизировать число реплик. |
| Уточнения полезны? | Переход к useful event после принятого refinement, сравнительно с зарегистрированным контрольным вариантом | Менять уточнение/ранжирование; не считать корреляцию causal uplift. |
| Остров сохраняет контекст и доступность CTA? | Перекрытые/недоступные target в QA + исправимые navigation errors, return-to-section, direct CTA actions | Менять placement/compactness, не увеличивать клики ради кликов. |
| Персонализация улучшает выдачу? | Registered holdout на одной eligible population; cards-to-value, hide/undo, diversity, static fallback coverage | Менять model/surface policy, не сравнивать добровольно открывших «Для меня» со всеми. |
| Сбой маршрута теряет данные? | Attempt→primary committed и committed→projection отдельно; missing/late/readout coverage | Исправить конкретную capability/outbox, а не объявить общего провайдера неисправным. |

Direct attribution по конкретному target отличается от session-assisted последовательности. Causal вывод — только из зарегистрированного эксперимента. В первую версию нужны воспроизводимые вопросы и прочитанные результаты, а не новая универсальная experiment platform. Missing sample/late batch обозначается `INSUFFICIENT_DATA`/coverage gap, не нулём и не доказанным отсутствием интереса.

Product Atlas получает reviewed aggregate evidence, а не raw stream/profile. Optional first-party action map остаётся отдельной default-OFF, ограниченной consented campaign; zero-cost OFF и AM-0…AM-4 сохраняются. Сбор полной траектории курсора, always-on heatmap и автоматическое создание product finding из hotspot не добавляются.

## 6. Персонализация сквозная, но правила поверхностей различны

| Поверхность | Обязательное поведение |
|---|---|
| Календарь/даты | Канонические состав и хронология; только explicit exact hides исключают событие. |
| Тематическая/бесплатная подборка | Сначала её eligibility, затем слабый локальный rerank невидимой части по compatible projection; не менять тему ради профиля. |
| «Для меня» | Наиболее выраженная персонализация с diversity/exploration, без возврата exact hides. |
| Search / voice sections | Явный запрос и выбранный контекст сильнее общего профиля; профиль — bounded tie-break/soft preferences внутри допустимого множества. |
| Event detail related | Контекст события сильнее общей affinity; наследуются общие card/actions/media contracts. |

### Activation, identity и отдельные цели

В принятом TO-BE activation — `interest_profile_change`, like, `not_interested` после undo-window или `personal_feed_enabled`. Просто voice request, scroll, просмотр, закрытие toast и share не являются первой активацией. Аккаунт/Auth session не равен interest profile. Новый generic «Согласен на персонализацию» не вводится; informational notice и постоянная ссылка на Правила остаются. Legal/localization prerequisites из feature owner не отменяются и не объявляются пройденными этим документом.

`product_analytics`, research-focus, recommendations email/push и другие communications остаются отдельными purpose boundaries. Без analytics consent основная функция и уже активированная персонализация работают; без personalization activation optional analytics не создаёт subject/profile join. Focus anonymous session не равна verified-account eligibility голосового Search: mere valid JWT недостаточен для разрешения всех платных capabilities. Существующие правила identity upgrade/merge сохраняются, новый аккаунт для голоса не создаётся.

### Immediate overlay и materialized projection

Сильное действие меняет локальное current-state/undo сразу и уходит через существующий подтверждаемый product command. Derived interests пересчитываются scheduled/threshold materializer, не LLM в page request и не при каждом social proof. Browser получает bounded projection по ETag/next_refresh_at opportunistically; сетевой refresh не блокирует первый render/local rank. Ошибка оставляет совместимое last-good или общий static порядок.

Session context, краткие/средние/долгие интересы различаются: текущая voice-задача не становится long-term affinity. Long horizon по ручным требованиям — не менее шести месяцев и повторные сильные доказательства, не простое старение записи. Веса и более короткие границы — model-registry hypotheses, а не новые жёсткие числа из чата. Artifact hunting/promo campaign и sensitive-topic guards сохраняются; поиск чужого мероприятия/разовая семейная задача не обучают устойчивый вкус вслепую.

### История ответов, exact hides и неподвижность контента

Committed answer receipt сохраняет своё исходное решение/membership. **Текущее отображение** всех поверхностей обязано применять global exact hide/undo, доступность и исправленные факты. Скрытое событие не возвращается обычной карточкой из старой voice section или exploration. Историческое membership может содержать его ID для восстановления смысла; UI показывает bounded скрытое состояние без повторной карточной экспозиции. Восстановление — явное existing restore/recovery действие.

Нельзя переставить уже видимый префикс при arrival новой profile revision. Во время взаимодействия frozen target не скачет. Offscreen rerank использует фактический usable viewport/occlusion и сохраняет section/element anchor, а не только `scrollY`. Новая версия профиля не переписывает задним числом ранее сгенерированную рекомендацию; для полного refresh — новое понятное состояние/выборка. Публичные числа/заголовки должны различать total logical matches, rendered, hidden и not-yet-loaded; техническое значение не подменяет название страницы.

## 7. Связь с планом релиза и текущими окнами

F1/F13 обеспечивают свежие данные, F2/F3 — retrieval/discovery, F5/F16 — представление, F6/F7/F10 — state/identity/learning, F9/F12/F15 — действия, F11 — trusted travel facts, F4/F8 — отдельные communications, F14 — source discussion evidence, F17 — обратная связь и repair history. Голос не снимает и не дублирует эти обязательства.

Проверенные отдельно microservice, UI и model call не замыкают общий release gate. Required journey:

```text
same release/corpus/config + eligible identity/activation state
 → actual route selection / one cost-bearing dispatch
 → query+profile policy → rendered canonical events
 → user action / primary receipt / no duplicate
 → optional authorized summary+outbox → sink receipt → aggregate
 → reviewed product readout
```

No analytics consent: весь product journey работает с нулевыми optional observations. YDB unavailable: product action committed, projection pending, owner видит gap. Provider unavailable: classic useful path сохраняется без ложной voice success. Статический screenshot не доказывает ни один сетевой downstream переход.

Одного статуса `DONE` недостаточно. Документ/исходники/тесты/deploy/feature exposure/измерение/owner approval отражают разные свидетельства. Сохранённые исторические NO-GO/PASS в release-plan и presentation checklist остаются датированными evidence, не считаются сегодняшним verdict; никаких новых live PASS здесь нет.

Текущий #621: одна shared реализация exporter/build/publisher, один Kaggle CPU путь published preview и существующий bucket. Будущий two-root ALB остаётся отдельно от текущего launch path; нельзя оживить его только из старого release-plan. Release safeguards целостности, lifecycle URLs, privacy, current data progression и rollback сохраняются. Без feature exposure optional action map не блокирует базовый release; полнота F1–F17 не отменяется.

## 8. Автотесты стыков

Ниже proposed acceptance groups, не второй registry и не пройденные тесты. Исполняемые случаи добавляются в существующие scenarios/suites с reuse ID, если эквивалент уже существует. Используются одни fixtures/corpus/profile/model/clock/seed для applicable визуальных состояний. Личные production profiles в Penpot не экспортируются.

| Группа | Given / When / Then | Lane |
|---|---|---|
| Product route matrix | direct down / relay down / both paths down / shared upstream down → операция → правильный маршрут, no false success, нет повторного selected-once POST | Real fault HTTP server + L1; small L2 subset. |
| Reverse analytics path | Yandex client route down → Edge bridge → тот же ingest и один sink receipt; downstream down → не proxy-only PASS | Integration/test sink. |
| Capabilities isolated | analytics 429/5xx → Search/Auth → здоровье product path не испорчено; 429 не обходится новым origin | Unit + integration. |
| Audio envelope | standard/oversize/multipart/base64 input → declared route → body в caps либо честный pre-dispatch reject/segmentation; bytes сохранились | Capture+HTTP integration. |
| Lost acknowledgement | primary commit, response потерян → retry/reconcile → один effect; analytics projection lag не повторяет действие | Actual test DB + HTTP faults. |
| Identity/activation/purposes | verified vs anonymous session, activation off/on, analytics off/on → view/Search/action → только разрешённые writes; нет implicit profile activation | Role/scoped integration + L1. |
| Profile delivery | compatible/stale/incompatible projection + lost network → page → static-first/local rank, no page-triggered provider, bounded ETag refresh | Unit + L1. |
| Surface policy | одна fixture profile → calendar/collection/personal/Search → разные принятые правила; explicit query/eligibility приоритетны | Deterministic scorer/retrieval. |
| Global hide | hide/undo на S2 → S1/calendar/collection → hide не воскрешён, pending undo не прыгает, restore явный | L1 + DB reconciliation. |
| Served/exposure | repeated sections/sticky heading/occluded card/Back → summary → correct ranks, dedupe, no invisible exposure | Geometry L1 + aggregate fixture. |
| Modality parity | voice/touch/keyboard одна CTA + retry → facts → один accepted target action; ordinal references стабильны | L1 + sink. |
| Measurement readout | known fixture actors/views/actions + tests/bots/late batches → daily query → expected numerator/denominator/coverage; не «посещение» по click | Executable SQL/aggregate tests. |
| Memory/retention | logout/reset/delete/packet expiry → late callback/outbox/old section → no leak/no resurrection, receipt/status честны | DB/L1. |
| Budget sharing | voice + profile sync + stats + action map + CI → pressure → общий storage/traffic/resource cap; accepted strong state не вытеснен молча | Fake-clock + integration. |
| Zero-cost OFF | action map off / analytics denied → normal usage → 0 optional capture/requests; controls/voice still usable | Existing OFF build proof + L1. |
| Release chain | new catalog watermark → candidate/voice results/readout → exact compatible revisions либо failed-stalled; WIP не production | Existing release/corpus gates. |

GitHub-hosted Actions покрывает unit, real isolated DB, HTTP-faults, aggregate oracle и browser capture/geometry. Scheduled/manual live subset использует существующий auth-session fixture, общий model limiter и test sink/test_run_id; не рассылает OTP ради каждой проверки. Qwen frozen corpus воспроизводит те же semantic scenarios, а не создаёт отдельную систему метрик. L2/L3 нужны там, где важны реальная mobile/PWA lifecycle и физический микрофон.

## 9. Как выполнять без нового управляющего слоя

**ChatGPT:** эти контракты/источники/конфликты, конкретные feature MeasurementQuestions, route/surface/actor scenario mapping, чистые schema/reducer/policy/aggregate fixtures и source changes, которые можно реально проверить. Не плодить десятки speculative метрик или таблиц.

**Кодовый агент:** reuse/extract общего transport с явными reverse-bridge/media adapters; primary receipt→outbox→sink; принятый profile projection/scorer/materializer; actual capture/HTTP/DB/browser/aggregation tests. Не писать второй поиск или profile service ради голоса и не объявлять наличие функций из документа deployed endpoint.

**Окно Floating Island:** общесайтовая применимость + network/activation/consent/projection/measurement states, semantic IDs, measured occlusion/served-list bridge, no below-fold jumps, no telemetry-driven rank. Слой видимых controls не владеет Auth, продуктовым state или raw analytics. Updated handoff в [window-prompts](../static-site-pages/design-system/window-prompts/20260905-floating-islands-system-design.md).

**Текущая интеграция #621 / release owner:** принять совместимые source packages и необходимые tests, проверить один candidate, потом actual release acceptance. Общие foundations/STATUS не меняются параллельно из этой задачи. Feature flags и аналитические kill-switch не отменяют baseline F1–F17 или обязательную разрешённость provider/data-flow.

## 10. Следующий проверяемый продуктовый срез

Не отдельные «написали таблицу → сделали красивую кнопку», а один полный сценарий: вошедший eligible пользователь ищет бесплатные события, уточняет побережье, видит историю, скрывает событие, возвращается в обычную подборку и не встречает скрытое снова; сеть или analytics sidecar могут отказать без ложного сохранения/дублирования. На synthetic/test identity показать receipt, projection revision, фактические exposures и воспроизводимый readout с правильным denominator. При denied analytics та же функция работает без optional writes.

Именно этот вертикальный срез связывает прежний План релиза с новой работой. Транспорт/данные/UI/метрики/персонализация отдельно не объявляются завершением продукта. Числа бесплатных пакетов из исторической документации не утверждаются действующей account capacity: перед включением нужны actual usage и approved budget, не новый финансовый лимит из этого анализа.
