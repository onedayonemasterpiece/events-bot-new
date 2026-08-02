# Персонализация KenigEvents: целевая система

> Статус: **проектное решение / implementation blueprint**
>
> Дата среза: 2026-08-01
>
> Область: статический сайт, подборки, персональная лента, карточка события и связанные browser/server-контуры
>
> Главный источник требований: [requirements.md](./requirements.md) — этот файл намеренно не изменяется автоматически.

## 1. Назначение документа

Документ сводит последние ручные требования, исследования из этой папки,
старые проектные решения и фактическое состояние репозитория в одну целевую
архитектуру. Он отвечает на четыре вопроса:

1. что именно означает персонализация на разных поверхностях сайта;
2. какие данные и модели для этого нужны;
3. как сохранить статичность, скорость, приватность и управляемость сайта;
4. как автоматически доказывать корректность и полезность модели, а не только
   наличие перестановки карточек.

Это **не** описание уже работающей production-системы. Текущий сайт имеет
локальный детерминированный rerank-прототип и отдельный semantic-search контур,
но не имеет законченного цикла `показ → сигнал → профиль → новая выдача`.

## 2. Источники и приоритет решений

При конфликте используются, в порядке убывания приоритета:

1. [requirements.md](./requirements.md) — последние зафиксированные владельцем
   продукта требования;
2. [legal-repsonalization.md](./legal-repsonalization.md) — обязательный для
   проекта legal/data-flow contract; это проектное правовое исследование, а не
   замена финального заключения юриста;
3. [interests-deep-research.md](./interests-deep-research.md) — наиболее свежая
   исследовательская модель интересов;
4. документы `Golden personas_ глобальная и русскоязычная сегментация
   аудитории.docx` и `Моделирование интересов к событиям.docx` — гипотезы и
   объяснительный материал;
5. принятая архитектура владения данными
   `docs/architecture/personalization-data-ownership.md` из `origin/main`,
   текущие канонические документы и код;
6. более старые документы `unsigned-personalization`, исторические E2E-планы и
   экспериментальные ветки — только как материал для переиспользования.

Точные веса, проценты переходов между интересами и пороги из старых документов
используются как **начальные priors**, но не считаются доказанными. Они должны
войти в реестр гипотез и пройти offline- и online-проверку. Исследовательские
ссылки с повреждёнными citation-токенами также не являются внешней верификацией
факта.

## 3. Краткое решение

Система строится как **static-first гибридная рекомендательная система**:

- канонические события и eligibility коллекций остаются в Fly SQLite и
  статических manifest-файлах;
- до первого осмысленного действия серверный профиль не создаётся: browser
  хранит только минимальное техническое/интерфейсное состояние и показывает
  общую или контекстную выдачу;
- `interest_profile_change`, like, `not_interested` или явное включение «Для
  меня» активируют персонализированную функцию; отдельный предварительный
  consent/checkbox для основной персонализации не нужен;
- активация фиксируется как акцепт функции: время, action и версии договора,
  privacy notice и Правил рекомендательных технологий; consent остаётся
  отдельным только для analytics/marketing/push/email и других целей, которым
  он действительно нужен;
- после активации и прохождения localization gate серверный durable-профиль,
  сильные действия и актуальное состояние рекомендаций принадлежат единственному
  утверждённому primary store;
- тематические подборки ранжируются локально по последней компактной
  `profile_projection` без обращения к Supabase/API при каждом просмотре;
- derived-профиль не пересчитывается после каждого social proof: действия
  пакетируются, materializer выпускает новую revision по расписанию/порогам, а
  browser периодически забирает её по ETag;
- слабая высокообъёмная telemetry, если она вообще собирается, уходит через
  серверный контур в YDB с TTL и не становится вторым владельцем профиля;
- browser не пишет напрямую в приватные таблицы и не получает service key;
- модель сначала применяет жёсткие продуктовые ограничения, затем строит
  кандидатов, ранжирует их и только после этого безопасно меняет **невидимую
  часть** страницы;
- явное `Не интересует` для конкретного события — глобальный hard exclusion,
  который никогда не отменяется exploration-механизмом;
- Golden personas — мягкая смесь объяснимых паттернов, а не единственный класс
  пользователя;
- языковой/региональный контекст — отдельный overlay, а не вывод о вкусе,
  личности или политических предпочтениях;
- browser E2E проверяет контракт и чувствительность модели, offline replay —
  качество ранжирования, A/B — причинный продуктовый эффект.

## 4. Границы и термины

### 4.1. Входит в систему

- анонимный и авторизованный профиль интересов;
- явные и допустимые неявные сигналы;
- локальный и server-side расчёт текущего профиля;
- все подборки, календарные страницы, поиск, блок похожих событий и
  `/dlya-menya/`;
- анти-пузырь, novelty и diversity;
- скрытие события, undo и список скрытого;
- безопасная доставка обновлений на статическую страницу;
- state-machine активации персонализированной функции и отдельные purpose
  consents;
- локальный zero-network rerank и периодическая доставка materialized profile;
- model registry, experiment assignment, observability и E2E.

### 4.2. Не входит

- генерация описаний событий или их смысловая перезапись;
- использование LLM при каждом открытии страницы;
- вывод чувствительных характеристик пользователя;
- создание долгосрочных interest facets из political/religious/medical и других
  специальных категорий данных;
- автоматическое объявление гипотетических persona/interest percentages
  достоверным фактом;
- изменение канонических фактов события из персонализационного контура.

### 4.3. Термины

| Термин | Значение |
|---|---|
| Surface | Поверхность выдачи: календарь, тематическая подборка, поиск, «Для меня», related и т. п. |
| Eligibility | Неперсональные правила попадания события на surface: дата, статус, город, тема, бесплатность, возраст и т. д. |
| Candidate set | Допустимый набор событий до финального ранжирования. |
| Served list | Фактически показанные пользователю карточки и их порядок. |
| Explicit tombstone | Durable-состояние `Не интересует` для конкретного события/связанной сущности. |
| Profile snapshot | Версионированная компактная проекция интересов и ограничений на момент расчёта. |
| Profile projection | Подписанная/проверяемая компактная часть snapshot, пригодная для localStorage и zero-network scoring. |
| Activation | Первое осмысленное действие, запускающее персонализированную функцию и server profile; не consent на персональные данные. |
| Model variant | Версия feature extraction, scoring, surface policy и весов. |
| Exploration | Контролируемый показ нового/неожиданного контента. |
| Persona-suppressed | Событие, пониженное или исключённое только из персональной выдачи из-за высокоуверенного несовпадения persona/facet, но не скрытое глобально. |

## 5. Фактическая отправная точка

### 5.1. Уже есть

- статические manifests событий и related-карточек из canonical Fly SQLite;
- `site/src/layouts/EventLayout.astro` с browser `localStorage`, локальными
  like/hide и детерминированным score/rerank;
- DOM-метаданные карточек (`event id`, rank, score, algorithm, surface), которые
  можно превратить в test contract;
- compact local profile с версиями и ограничениями размера;
- semantic search в отдельном personalization Supabase:
  `event_embeddings`, `event_search_documents`, `pgvector`, embedding dimension
  768;
- агрегатный счётчик реакций и заготовки experiment/PWA telemetry;
- routed Playwright-прототип для демонстрационной страницы;
- draft Gherkin feature без реализованных step definitions.

### 5.2. Есть частично или расходится

- frontend подготовлен к вызову `get_listing_personal_feed_v1`, но RPC/migration
  отсутствует и в проверенной live-схеме такой функции нет;
- reference JS/demo и фактический inline-код Astro реализуют разные алгоритмы;
  тесты сейчас защищают демонстрацию, а не production path;
- feedback log и served-list существуют только локально и не образуют
  подтверждаемую цепочку с server profile;
- текущий consent-текст обещает хранение только в браузере, тогда как
  подготовленный remote flow подразумевает отправку профиля; он также не
  реализует новую activation/contract модель;
- reset очищает только локальное состояние;
- CI не запускает полноценный Astro/Playwright контур; без `testDir` Playwright
  может обнаруживать дубликаты spec в `.codex/worktrees`;
- frontend ожидает `PUBLIC_*` env, а часть builder-контуров передаёт только
  непубличные имена;
- подборки ещё не представлены единой data-driven сущностью.

### 5.3. Отсутствует

- устойчивый anonymous subject/device credential;
- `personalization_started_at`/activation evidence и раздельные purpose consents;
- удалённый delete/reset;
- durable strong-action ledger, served-list summary, session summary;
- profile/recommendation snapshot и materialization job;
- единый model/feature/taxonomy registry;
- campaign-aware telemetry;
- канонический runtime scorer, общий для страницы и тестов;
- quality fixtures, temporal replay и автоматические model gates;
- замкнутый production learning loop.

Read-only проверка personalization Supabase на 2026-08-01 это подтверждает:
PostgreSQL 17.6, около 41 MB, расширение `vector`; обнаружены 816
`event_embeddings`, 408 `event_search_documents` и 2911 строк агрегатного
reaction counter, но не обнаружены current anonymous profile/strong-action/
served-list таблицы и routine `get_listing_personal_feed_v1`. Санитизированный
schema snapshot сохранён локально в
`artifacts/codex/personalization-design/current-personalization-schema.json` и
не является коммитимым проектным документом.

Следовательно, корректное название текущего состояния — **локальный
детерминированный rerank-прототип**, а не обученная персональная модель.

## 6. Продуктовые инварианты

Ни модель, ни эксперимент не могут нарушать следующие правила:

1. Сайт полезен и навигация/CTA работают без JavaScript, Supabase и YDB.
2. Просроченные, отменённые, несовместимые, дублированные и явно скрытые события
   не возвращаются ранжированием.
3. Exact hide не спасается ни persona, ни popularity, ни exploration.
4. До `personalization_started_at` нет server profile, impression/open telemetry
   и удалённой interest-мутации; простое открытие, scroll, impression, dwell и
   закрытие уведомления не активируют персонализацию.
5. Открытие страницы не вызывает provider/LLM API.
6. Первоначально видимый пользователю префикс карточек не переставляется.
7. Карточка, с которой пользователь взаимодействует, не прыгает и не исчезает
   до окончания undo-состояния.
8. Календарная страница сохраняет канонический состав и хронологию; из неё
   удаляются только точные explicit hides.
9. Тематическая подборка сначала соблюдает свою семантику, затем допускает лишь
   слабый ниже-fold rerank.
10. «Для меня» использует самую сильную персонализацию.
11. Search query и контекст открытого события сильнее общего профиля.
12. Пользователь с тем же профилем и тем же immutable catalog snapshot получает
    воспроизводимый результат для одного `model_version` и seed.
13. Сбой персонализации не делает карточки или страницу недоступными.
14. Фактически отрисованный served list совпадает с зафиксированным summary.
15. Профиль одного browser context не виден в другом без явной процедуры
    link/login.
16. Local rerank тематической подборки работает по последней совместимой
    `profile_projection` без сетевого запроса и без online LLM/provider.
17. Explicit action применяется немедленно, но derived profile revision не
    пересчитывается и не скачивается после каждого действия.
18. Сигналы `sensitive_topic` не создают долгосрочные пользовательские facets и
    не попадают в обычный server profile.
19. На сайте постоянно доступно публичное уведомление и Правила применения
    рекомендательных технологий; одноразовый toast не заменяет постоянную ссылку.

## 7. Целевая архитектура

```text
Fly SQLite (canonical events)
        │ static build/export
        ▼
Object Storage/CDN ── HTML + catalog/collection/feature manifests
        │
        ▼
Browser runtime
  ├─ static-first render
  ├─ bounded profile projection + explicit overlay/cache/outbox
  ├─ eligibility guard + shared scorer
  ├─ zero-network below-viewport presenter
  └─ periodic ETag profile refresh
        │ only after activation, same-origin, idempotent batch
        ▼
Personalization API
  ├─ schema/auth/activation/purpose/rate validation
  ├─ primary-store transaction: action/current state
  ├─ scheduled/threshold materialization workers
  └─ de-identified async analytics outbox
        │
        ├────────────► approved localized private store (durable SOR)
        └────────────► YDB TTL analytics (optional raw weak telemetry)
```

### 7.1. Владение данными

| Данные | Канонический владелец | Разрешённая проекция |
|---|---|---|
| События, lifecycle, публикация | Fly SQLite | Компактные card/feature/vector projections |
| Eligibility и definition подборок | Версионированный код/manifest из static build | `collection-surfaces-v1.json` в CDN |
| До активации | Browser: минимальное техническое/интерфейсное состояние | Не создаёт server interest profile |
| Activation evidence | Утверждённый localized primary store | Только минимальный audit |
| Durable профиль | Supabase private schema, только если пройден localization gate; иначе новый российский primary contour | De-identified analytics |
| Purpose consents | Primary store, отдельно по цели | Audit/aggregate |
| Exact hides, likes, saves, follows | Primary store после activation | Локальная оптимистическая проекция |
| Weak raw telemetry | YDB с TTL либо не хранится | Только агрегаты/feature evidence |
| Актуальный recommendation issue | Supabase | Оpaque static/CDN artifact |
| Эксперимент и model registry | Supabase/репозиторий | Sanitized build manifest |

YDB analytics не хранит конкурирующий current profile. Browser localStorage —
cache/offline projection, а не system of record после materialization.

Новая legal-модель создаёт обязательный архитектурный gate: до записи
`visitor_id`, сигналов и профиля нужно подтвердить регион первоначальной записи.
Если текущий personalization Supabase не удовлетворяет требованиям локализации,
remote-profile rollout блокируется. Допустимые решения: российское размещение
Supabase/Postgres как primary либо отдельный российский primary contour с
пересмотром текущего ownership ADR. Иностранный Supabase нельзя молча оставить
первичным хранилищем и «исправить» это текстом политики.

### 7.2. Browser transport

Основной путь после `personalization_started_at` — same-origin endpoint,
например:

```text
POST /api/personalization/v1/batch
```

Требования к нему:

- credential принадлежит сайту и хранится в `HttpOnly`, `Secure`, `SameSite`
  cookie; `anon_id` сам по себе не доказывает владение;
- каждое действие имеет `client_event_id`, `subject_id`, `device_id`,
  `schema_version`, `occurred_at`, `surface_id`, `served_list_id` и контекст;
- первый activation action атомарно фиксирует
  `personalization_started_at`, `activation_action`, версии договора, privacy
  notice и Правил рекомендаций; его повтор не создаёт вторую активацию;
- сервер проверяет activation state, а для analytics/marketing/push/email —
  отдельный purpose consent; также проверяются Origin/CSRF, размер, схема,
  rate/quota, idempotency и допустимость перехода состояния;
- профиль, activation evidence и purpose consents нельзя менять прямым browser
  DML/RPC;
- повтор разрешён только для idempotent records;
- stateless relay — allowlist методов/путей, а не универсальный Supabase proxy.

Публичный Supabase publishable key допустим только для отдельно утверждённых
узких aggregate reads с RLS. Secret/service key никогда не попадает в frontend.

### 7.3. Browser storage budget

Подтверждается текущая проектная граница: не более **64 KiB** KenigEvents-owned
данных суммарно, без Supabase auth storage. Shared outbox:

- максимум 16 записей;
- максимум 12 KiB;
- TTL 24 часа;
- максимум 5 попыток;
- IndexedDB с компактным localStorage fallback;
- сначала вытесняются disposable caches/queues, затем устаревшие проекции;
  explicit current state не вытесняется молча.

Рекомендуемые ключи:

```text
ke_personalization_profile_v3
ke_personalization_explicit_v2
ke_personalization_outbox_v2
ke_personalization_cache_v2
ke_personalization_activation_v1
```

`ke_personalization_profile_v3` разделяется на стабильную materialized
`profile_projection` и малый explicit/session overlay. В браузере хранятся
sparse top-K maps и bounded summaries, но не полный clickstream и не dense
embedding истории пользователя.

## 8. Данные Supabase / approved primary store

Ниже — логическая схема, рассчитанная на Supabase/Postgres; физические имена
уточняются migration-дизайном. Она разворачивается в personalization Supabase
только после положительного localization audit. Иначе та же модель переносится в
утверждённый российский primary contour, а ownership ADR обновляется до начала
реализации. Все таблицы профиля находятся в private API schema и закрыты от
прямого browser доступа.

| Сущность | Назначение | Ключевые ограничения |
|---|---|---|
| `profile_subject` | Anonymous/auth subject, profile revision, status | Один current owner; opaque id |
| `profile_identity_link` | Idempotent anonymous→auth merge | Audit, authenticated state wins |
| `personalization_activation` | Started-at, activation action, contract/privacy/rules versions | Append-only legal evidence; не называется consent |
| `purpose_consent_ledger` | Analytics/marketing/push/email и иные отдельные цели | Purpose/version/grant/revoke/evidence |
| `strong_action` | Like/hide/undo/save/share/CTA/attendance | `client_event_id` unique; typed target |
| `explicit_state` | Current tombstone/like/favorite state | Последняя валидная sequence wins |
| `served_list_summary` | Что реально показано | Catalog/model/profile hashes, ranks |
| `session_summary` | Bounded session features | Нет сырой бесконечной истории |
| `profile_snapshot` | Facets/persona/constraints по горизонтам | Immutable revision + provenance + `next_refresh_at` |
| `profile_projection_issue` | Компактный browser payload | ETag/hash, schema/model versions, expiry, size cap |
| `event_feature_snapshot` | Версия признаков card projection | Связь с canonical event revision |
| `recommendation_snapshot` | Выдача на surface/date | Idempotent issue, expiry |
| `experiment_assignment` | Stable assignment и eligibility | Не назначается задним числом |
| `model_registry` | Model/taxonomy/scorer versions | Immutable published version |
| `analytics_outbox` | Асинхронная проекция в YDB | Не блокирует UX |
| `quarantine_event` | Невалидные/ботовые действия | Не обучает профиль |

### 8.1. Retention

Начальная политика для согласования, не юридическое заключение:

- подробный журнал сигналов: не более 90 дней, далее bounded
  aggregate/current state;
- served-list summaries: 14–30 дней, затем агрегаты;
- weak raw telemetry в YDB: минимальный TTL, достаточный для диагностики;
- aggregated profile: до 12 месяцев после последней активности либо короче по
  утверждённой политике;
- anonymous visitor/device id: плановая ротация каждые 6–12 месяцев;
- quarantine: 7–14 дней;
- exact tombstone: пока событие/связанная серия актуальны либо до явного restore;
- delete/reset создаёт purge request для удалённых raw/history projections;
  необратимые агрегаты подчиняются отдельной политике.

Перед production нужно утвердить retention, data-flow и localization audit,
особенно для персональных данных и 152-ФЗ. Наличие ресурса в конкретном регионе
само по себе не является доказательством соответствия.

### 8.2. RLS и функции

- RLS включён на каждой exposed таблице; отсутствие policy означает deny;
- ownership основан на проверенном `auth.uid()`/server subject, не на
  `user_metadata`;
- `SECURITY DEFINER` функции имеют фиксированный `search_path`, явные grants и
  `REVOKE ... FROM PUBLIC`;
- секретные ключи доступны только server runtime;
- payload и returned columns минимальны;
- destructive/reset операции требуют повторной авторизации либо свежего
  device proof;
- логирование редактирует token, cookie, email, raw profile и free text.
- `visitor_id` с привязанным профилем трактуется как персональные данные, даже
  без ФИО;
- `sensitive_topic` не материализуется в пользовательский профиль и не
  экспортируется как персональная категория;
- до `personalization_started_at` ingestion endpoint отклоняет/не создаёт
  impression, open и weak-profile mutations;
- отдельные consents нельзя выводить из факта activation персонализации.

## 9. Синхронизация и разрешение конфликтов

### 9.1. Общий протокол

1. До activation отображается общая выдача; технические `seen_event_ids` могут
   оставаться локально, но impression/open не отправляются и server interest
   profile не создаётся.
2. Осмысленное activation action применяется оптимистически локально и получает
   idempotency key.
3. Рядом с первым действием показывается информационный текст о запуске
   персонализированного подбора и ссылка на правила; действие, а не закрытие
   сообщения, запускает state-machine.
4. Action записывается в bounded outbox. При сети same-origin endpoint атомарно
   фиксирует activation evidence и само strong action.
5. Следующие действия отправляются компактными batch, но не запускают отдельный
   online-recompute и profile-download после каждого события.
6. Primary store транзакционно обновляет action/current state. Сервер возвращает
   accepted/rejected ids, authoritative sequence и только необходимый delta.
7. Клиент подтверждает записи, применяет explicit delta и через
   `BroadcastChannel` синхронизирует вкладки.
8. Scheduled/threshold materializer строит immutable snapshot независимо от
   request path; profile revision увеличивается только при опубликованном
   пересчёте.
9. Browser периодически забирает compact projection по ETag; следующий local
   rerank использует эту revision без server call.

### 9.2. Приоритет конфликтов

1. Explicit state не перезаписывается derived profile.
2. Для одной typed-сущности побеждает более поздняя валидная server sequence.
3. Authenticated explicit action побеждает конфликтующую anonymous проекцию.
4. Exact hide объединяется как tombstone, кроме более позднего явного undo.
5. Неявные сигналы никогда не отменяют explicit hide/negative.
6. Устаревший response не откатывает более новый `profile_revision`.

### 9.3. Activation, отдельные consents, reset и delete

- Предварительного popup/checkbox `Разрешить персонализацию` нет.
- До осмысленного действия нет server profile; простое продолжение просмотра и
  закрытие информационного сообщения не считаются ни consent, ни activation.
- Activation actions: `interest_profile_change`, `like`, `not_interested`,
  `personal_feed_enabled`. `share`, scroll, impression, open и dwell не могут
  быть первым activation action.
- Для `not_interested` activation наступает после окончания undo-window. Pending
  hide, отменённый пользователем, не создаёт server profile.
- Одноразовое уведомление использует `Понятно`/автоисчезновение, но не кнопку
  `Согласен`; в footer и около персональной ленты постоянно доступна формулировка
  о применении рекомендательных технологий со ссылкой на публичные Правила.
- На сервере фиксируется активация услуги, а не согласие на персональные данные.
- Analytics, marketing, email recommendations, push, сторонний tracking и
  чувствительные категории имеют отдельные purpose-specific consent flows.
- При login eligible anonymous profile связывается автоматически и идемпотентно;
  переносится compact snapshot/current state, а не raw browsing history.
- `Сбросить персональные рекомендации` очищает local projection/actions,
  удаляет remote interest profile/history без неоправданной задержки и возвращает
  общую ленту. Минимальное legal evidence хранится только по отдельной
  утверждённой retention-политике.
- `Удалить данные` отзывает применимые purpose consents и запускает
  подтверждаемый purge.
- Logout не удаляет durable profile; reset/delete — отдельные явные действия.

### 9.4. Периодический materialization и profile refresh

Профиль состоит из двух слоёв:

1. **Immediate overlay:** exact hide/undo, like/unlike и session context,
   необходимые для мгновенного и понятного UX.
2. **Stable projection:** facets/personas/constraints по горизонтам, рассчитанные
   materializer и подписанные `profile_revision`.

Materializer запускается не из page request и не после каждого social proof, а:

- по расписанию для накопившихся action batches;
- при достижении versioned evidence threshold;
- после trusted attendance/repeat-attendance;
- вручную для backfill/recovery;
- никогда параллельно для одной subject revision.

Стартовая cadence-гипотеза: не чаще одного обычного пересчёта/получения projection
в 24 часа, если нет сильного trigger или schema migration. Точное значение
утверждается нагрузочными и quality-тестами.

Compact refresh contract:

```text
GET /api/personalization/v1/profile-projection
If-None-Match: "<profile-etag>"

200: projection + revision + model/schema/taxonomy + expires_at + next_refresh_at
304: локальная projection актуальна
```

- refresh выполняется opportunistically после activation, по `next_refresh_at`,
  а не при каждом page view; даже просроченный refresh запускается только после
  завершения local rank/presentation, в idle/background lane и не входит в
  critical path;
- request дедуплицируется между вкладками и имеет cooldown/jitter;
- неуспех оставляет последнюю совместимую projection и static fallback;
- несовместимая/просроченная schema не применяется, но exact local state
  сохраняется;
- payload sparse/top-K, с ETag и жёстким size cap;
- authenticated profile может синхронизироваться между устройствами; anonymous
  projection привязана к доказанному device credential.

### 9.5. Legal/data-localization gate

Перед включением server materialization обязательны:

1. подтверждённый регион первоначальной записи `visitor_id`, actions и profile;
2. утверждённые Пользовательское соглашение, Политика обработки данных и
   отдельные Правила применения рекомендательных технологий;
3. постоянное публичное уведомление, данные владельца и email для юридически
   значимых сообщений;
4. проверка уведомления Роскомнадзора и перечня обработчиков;
5. отдельная модель для несовершеннолетних либо явное исключение детского
   профилирования из запуска;
6. запрет price/access/eligibility/benefit решений по profile: модель меняет
   только порядок/подбор информации.

Если gate не пройден, разрешён только local/static MVP без удалённого visitor
profile. Это release blocker, а не warning.

## 10. Представление пользователя

Профиль — не один вектор и не одна persona. Он содержит независимые измерения с
`value`, `confidence`, `evidence_count`, `last_updated`, `horizon` и provenance.

### 10.1. Измерения

1. **Темы и сущности:** domains, topics, genres, artists, venues, organizers,
   communities, series.
2. **Мотивация:** узнать новое, отдохнуть, социализироваться, получить сильное
   впечатление, заняться с детьми, поддержать сообщество и т. п.
3. **Формат и атмосфера:** лекция/практика/шоу, камерность/массовость,
   интерактивность, интенсивность.
4. **Novelty:** знакомое, adjacent, исследовательское.
5. **Социальный контекст:** один, пара, друзья, семья, профессиональная группа.
6. **Decision style/trust:** планирует заранее, принимает решение быстро,
   нуждается в доказательствах/отзывах.
7. **Практические ограничения:** цена, дата/время, расстояние, доступность,
   компания, возраст детей.
8. **Lifecycle:** исследует, сравнивает, намерен, зарегистрирован, посетил.
9. **Язык/рынок:** страна, город, interface/query/content/transaction language,
   валюта — независимые поля.
10. **Typed negatives:** событие, серия, venue, organizer, topic, format,
    constraint reason.

### 10.2. Временные горизонты

| Горизонт | Начальная инженерная настройка | Правило |
|---|---|---|
| Session | до 30 минут бездействия, hard cap 24 часа | Контекст текущей задачи; быстро исчезает |
| Short | ориентировочно 0–7 дней | Share/CTA/session-паттерны влияют сильнее |
| Mid | ориентировочно 8–179 дней | Повторные интересы и сохранения с decay |
| Long | **не менее 6 месяцев** | Появляется только из повторных сильных свидетельств, а не от простого старения |

Границы short/mid — versioned параметры эксперимента, потому что исходные
требования их не фиксируют. Long-term правило `>= 6 месяцев` является
обязательным.

### 10.3. Golden personas

Начальные глобальные гипотезы:

- explorer-omnivore;
- evidence planner;
- social coordinator;
- intensity seeker;
- restorative contemplative;
- niche curator.

Persona хранится как распределение, например
`{explorer: .45, social: .25, unknown: .30}`, а не как hard label. Всегда
сохраняется `unknown/unexplained` масса. Persona помогает cold start,
explainability и regularization, но не заменяет facet-профиль.

Запрещено выводить или использовать как taste feature чувствительные атрибуты,
политику, этничность и подобные прокси. «Русскоязычный» означает лишь
market/language overlay с раздельными country/city/language/currency полями.

### 10.4. Cold start

Опциональный, пропускаемый опрос из 6–8 adaptive multi-select вопросов:

- желаемые темы и форматы;
- мотивация;
- обычная компания;
- цена/расстояние/время;
- appetite for novelty;
- что точно не предлагать.

До отправки первого изменения результат хранится локально. Само подтверждение
`interest_profile_change` является activation action; рядом показывается
информационный текст и ссылка на Правила. Пользователь видит `Пропустить`, может
изменить ответы и сбросить профиль. Опрос не должен быть обязательным gate к
контенту.

### 10.5. Чувствительные и детские профили

- Event enrichment ставит `sensitive_topic`, если содержание может раскрывать
  политические взгляды, религиозные/философские убеждения, здоровье, интимную
  жизнь, расовую или национальную принадлежность.
- Такие события остаются доступными через общую ленту, дату, место, поиск и
  редакционные подборки.
- Их view/open/share/CTA не создают user interest facet, persona label или
  long-term server evidence; exact hide остаётся допустимым current-state
  действием без смыслового обобщения.
- Система не собирает возраст для построения детского профиля. Подборка `Детям`
  описывает событие/семейный контекст взрослого пользователя, а не делает вывод,
  что субъект — ребёнок.
- Любое расширение на специальные категории или сознательное профилирование
  несовершеннолетних требует отдельного legal design и необходимого consent.

## 11. Представление события

Каждый `event_feature_snapshot` включает:

- canonical identifiers и linked-event relations;
- collection eligibility flags;
- domain/topic/genre/entity facets;
- motivation, atmosphere, engagement, social scenario, novelty;
- practical constraints: price, time, distance, accessibility, language;
- lifecycle/ticket status;
- quality/provenance/confidence;
- semantic embedding как дополнительный retrieval feature;
- campaign/exposure context, если карточка участвует в игре/промо.
- `sensitive_topic` и reason class только как safety gate, не как пользовательский
  interest label.

Смысловые признаки извлекаются LLM-first в build/enrichment pipeline по
версионированной схеме и проходят deterministic consistency guards. Browser не
извлекает смысл broad regex-правилами и не вызывает LLM.

## 12. Сигналы и их семантика

Все веса ниже качественные. Численные коэффициенты выбираются offline
benchmark/A-B, а не переносятся из старых документов как истина.

| Сигнал | Сила | Горизонт/семантика |
|---|---|---|
| Valid impression | очень слабая база | Только если карточка реально видима достаточное время |
| Короткий просмотр | очень слабый | Не равен отрицанию |
| Dwell/detail open | слабый–средний | Контекстный интерес |
| Повторный поиск | средний | Query/topic intent |
| Like | заметный | Short, mid и после повторения long |
| Save/calendar | высокий | Intent; не равен attendance |
| Share | высокий social intent, ограниченный personal-taste вклад | Особенно session/short; само по себе может означать рекомендацию другому человеку |
| CTA registration/purchase | высокий | Сильный intent; не равен факту посещения |
| Подтверждённое посещение | очень высокий | Только доверенный idempotent источник |
| Повторное посещение | максимальный | Основание для long-term affinity |
| Unlike | нейтрализация like | Не создаёт автоматически negative topic |
| `Не интересует` | hard для exact event | Немедленно и на всех surface |
| Указанная причина | typed negative/constraint | Расширение только на выбранную сущность/причину |
| Quick scroll/skip | слабый | Только повторный валидный паттерн, например 3 недели |

Правила защиты смысла:

- `дорого`, `далеко`, `не в это время`, `не с кем` изменяют constraint, а не
  уменьшают любовь к жанру;
- одно отсутствие клика не является отрицанием;
- attendance нельзя выводить из CTA/save;
- promo/artifact hunt помечает implicit organic evidence весом 0; explicit
  like/hide пользователя остаётся валидным;
- share не является первым activation action и без подтверждающих сигналов не
  создаёт long-term genre affinity;
- взаимодействия с `sensitive_topic` не материализуются в semantic user facet;
- подозрительные, bot-like и schema-invalid события идут в quarantine и не
  материализуют профиль.

## 13. Конвейер модели

### 13.1. Стадии

1. **Load context:** surface, date, query, anchor event, viewport, session,
   catalog/profile/model revisions.
2. **Eligibility:** canonical lifecycle, city/date, collection semantics,
   dedupe/linked-event rules, exact tombstones.
3. **Candidate generation:** surface catalog, semantic neighbors,
   editorial/popular, affinity и exploration pools.
4. **Feature join:** event snapshot + profile horizons + session + context.
5. **Scoring:** versioned interpretable components.
6. **Policy:** diversity, fatigue, persona suppression, anti-bubble, quotas.
7. **Presentation:** freeze visible/acted prefix; применить только tail delta.
8. **Evidence:** записать served-list summary/hash и component reasons.

### 13.2. Базовая формула

```text
score = surface_base
      + facet_affinity(short, mid, long)
      + soft_persona_affinity
      + session_and_context_intent
      + interest_graph_adjacent
      + quality_and_surface_native_signal
      - typed_constraint_penalty
      - negative_affinity
      - fatigue
```

Eligibility и exact hide не являются огромным штрафом: они фильтруют кандидата
**до** score. Веса и transformations задаются по surface и входят в
`model_version`.

### 13.3. Сравниваемые варианты

Гибрид не назначается победителем заранее. Минимальный model bake-off:

| Variant | Состав |
|---|---|
| `control` | Неперсональный category/popularity/editorial порядок |
| `facets` | Независимые интересы и constraints |
| `hard-persona` | Один persona label; диагностический baseline, не целевой UX |
| `soft-persona` | Смесь personas + unknown |
| `hybrid` | Facets + soft persona + session + graph + constraints |

Победитель должен улучшить quality-метрики без провала hard invariants,
diversity, worst-group и performance.

### 13.4. Filter bubble и suppression

- novelty/exploration — отдельная квота, измеряемая независимо;
- высокоуверенный persona mismatch может исключить карточку **только из
  персонального served set**, при одновременном достаточном persona confidence и
  event-feature confidence;
- это не превращает карточку в global hidden и не влияет на прямой URL;
- не более **10% persona-suppressed** карточек могут быть возвращены
  anti-bubble rescue;
- explicit tombstones не входят в denominator и никогда не rescue;
- обычные подборки используют более строгий порог suppression и слабый rerank,
  чем «Для меня»;
- отдельная exploration квота не смешивается с 10% rescue и репортится отдельно.

Если supply мал или uncertainty высока, система предпочитает demotion вместо
exclusion.

## 14. Политика поверхностей и подборок

Все поверхности описываются данными, например
`collection-surfaces-v1.json`, а не разрозненными условиями в шаблонах:

```json
{
  "id": "free",
  "eligibility": "is_free == true",
  "native_order": ["start_at", "quality"],
  "personalization_strength": "weak",
  "exact_hide": true,
  "rerank_zone": "below_viewport",
  "exploration_quota": 0.05
}
```

Числа в примере конфигурационные и не являются утверждёнными продуктовыми
порогами.

### 14.1. Матрица surface

| Surface | Eligibility / главный смысл | Сила персонализации | Обязательное поведение |
|---|---|---:|---|
| Календарь/дата | Дата, город, lifecycle; хронология | Нет для основного списка | Только exact hide удаляет событие; персональный блок допустим отдельно ниже списка |
| Бесплатно | `is_free` до preferences | Слабая | Платное нельзя вернуть affinity |
| Детям | Возраст/семейная eligibility | Слабая | Несовместимое событие не возвращается |
| Клубы по интересам | Семантика клуба/community | Слабая | Сохраняет тематическую целостность |
| Фестивали | Festival/program relation | Слабая | Не разрушает группировку программы |
| Выставки | Exhibition eligibility | Слабая | Stable below-fold rerank |
| Необычное | Editorial/novelty definition | Слабая–средняя | Нужна формализация eligibility и QA |
| Для меня | Общий eligible future catalog; durable-версия только после email/Yandex auth | Сильная | Hybrid rank, suppression, diversity, anti-bubble |
| Кино | Планируемая definition | Слабая | Сначала реализовать eligibility |
| Театр/Спектакли | Планируемые definitions | Слабая | Не смешивать venue и genre без taxonomy |
| Научпоп/Наука | Планируемые definitions | Слабая | Явно развести популяризацию и научные события |
| Поиск | Query match | Только tie-break | Query всегда сильнее профиля |
| Related event | Anchor-event relevance | Слабый tie-break | Similarity/linked context сильнее профиля |
| Популярное | Popularity | Слабый tie-break | Не переопределять смысл популярности |
| Помечены «не интересует» | Exact tombstones | Нет | Полный recovery list, restore доступен всегда |

Это проектное продолжение [podborki.md](../podborki.md): исходный документ не
переписывается здесь и остаётся отдельным продуктовым списком. До реализации
каждой новой подборки её eligibility, ordering, empty state, canonical URL и
E2E-строка добавляются в единый manifest.

### 14.2. «Для меня» и ежедневная страница

Durable server-side «Для меня» доступен только пользователю, авторизованному по
email или Yandex. Анонимному посетителю можно показать объясняющий landing либо
локальный browser-preview, но не выдавать persistent daily secret page и не
считать anonymous id авторизацией.

Для скорости не нужно генерировать отдельный полный HTML на каждый запрос.
Предлагается:

1. стабильный статический shell `/dlya-menya/`;
2. для авторизованного профиля — ежедневный compact recommendation manifest по
   opaque profile revision;
3. не более одного обычного fetch в день с локальным cache;
4. поверх него — немедленные local session/hide deltas;
5. при отказе API — локальный rank статического candidate manifest;
6. без авторизации/профиля — качественный non-personal fallback, предложение
   cold start и понятный login CTA без блокировки остальных подборок.

Secret URL допустим как отдельный forwardable snapshot для email/share, но не
как authentication:

- token не менее 128 бит, в Supabase хранится только keyed hash;
- нет PII, raw profile, score breakdown и внутренних ids;
- `noindex`, отсутствие в sitemap, `Referrer-Policy: no-referrer`;
- expiry, rotation и revoke;
- click/feedback/unsubscribe tokens разделены;
- cache policy проходит отдельный threat review.

### 14.3. Zero-network ранжирование тематических подборок

Static build вкладывает в каждую карточку только признаки, необходимые shared
local scorer: event id, eligibility, top facets, constraints, quality/native
order и feature schema version. При открытии подборки runtime читает совместимую
`profile_projection` из localStorage и **не обращается** к Supabase/API ради
перестановки.

Режимы:

1. **Обычная длинная подборка:** уже видимый префикс остаётся native; первый
   блок целевой персонализации располагается немного ниже viewport и локально
   сортируется до приближения пользователя.
2. **Заведомо конечная выборка:** весь eligible set можно score/rank локально.
   Если часть уже была показана, применяется тот же frozen-prefix contract;
   полная перестановка разрешена только до первого paint/видимости блока либо
   для ещё не отрисованного tail.
3. **Нет/несовместима projection:** native static order без пустого состояния и
   без network fallback.
4. **Есть immediate overlay:** exact hides применяются всегда; fresh like/session
   может дать ограниченный локальный overlay, но не переписывает stable persona/
   long-term weights до materialization.

Локальный scorer получает только входы:

```text
collection policy + static event features + stable profile projection
+ explicit/session overlay + deterministic seed
```

Он не получает raw history, не вызывает LLM и не пытается сам пересчитать
long-term профиль. Это позволяет иметь быстрые верхние части статических страниц
и предсказуемо обновлять модель отдельно от page navigation.

## 15. Безопасное изменение уже открытой страницы

### 15.1. Общий rerank

- `IntersectionObserver` и геометрия фиксируют visible prefix и safety margin;
- local rank plan вычисляется сразу из localStorage; сеть не входит в critical
  path и может только подготовить projection для следующего применения;
- переставляются только DOM nodes, полностью находящиеся ниже viewport anchor;
- уже видимая, focused, hovered или acted карточка pinned;
- обновление применяется одной DOM transaction с сохранением scroll anchor;
- поздний ответ со старым `profile_revision` игнорируется;
- при `prefers-reduced-motion` перестановка не анимируется;
- измеряется CLS и число moved-visible cards, которое должно быть 0.

### 15.2. `Не интересует`, countdown и undo

1. Click сразу создаёт local pending tombstone.
2. Карточка становится серой/смешанной с фоном, сохраняет высоту и блокирует
   обычную навигацию; `Вернуть` остаётся доступным.
3. Появляется компактная notification strip с текстом, Undo/Cancel и нижним
   progress indicator.
4. Таймер автоисчезновения — versioned UX-параметр; до UX-теста рекомендуется
   начать с 5 секунд.
5. Pending hide не отправляется до окончания undo-window. Undo удаляет pending
   action; если это было первое действие, activation не возникает. Только для
   редкой multi-tab/race-ситуации уже принятый hide компенсируется idempotent
   inverse с большей sequence.
6. После таймера карточка удаляется. Если она всё ещё видима, сначала остаётся
   same-height placeholder и окончательное удаление происходит после выхода из
   viewport, чтобы не было скачка.
7. Exact tombstone действует на всех surfaces, включая календарь.
8. На mobile путь `Подборки → Помечены «не интересует»` показывает все скрытые
   события и позволяет восстановить их без временного ограничения.

Accessibility:

- strip имеет `role="status"`/`aria-live="polite"`;
- Undo — настоящая keyboard-focusable button;
- progress не является единственным способом понять оставшееся время;
- focus после удаления переходит предсказуемо;
- цвет/opacity не являются единственным признаком состояния.

### 15.3. Информирование при первой активации

При первом activation action рядом с контролом или сразу после действия
показывается короткое верхнее уведомление:

> Мы учтём это действие в персональных рекомендациях. Совершая его, вы начинаете
> пользоваться персонализированным подбором на условиях Пользовательского
> соглашения. Как работают рекомендации.

Это проектный смысл текста; юридически утверждённая редакция берётся из
публичных документов. Контракт UI:

- нет кнопок `Согласен`/`Отклонить`;
- закрытие или автоисчезновение ничего не активирует и не отменяет;
- ссылка `Как работают рекомендации` ведёт на публичные Правила;
- сообщение не блокирует действие и доступно screen reader;
- повторно показывается только при существенной смене contract/rules version;
- постоянная статическая ссылка/уведомление остаётся в footer и около «Для
  меня», даже когда transient strip исчезла; его базовая формулировка:
  `На информационном ресурсе применяются рекомендательные технологии.`

Постоянное уведомление не перекрывает контент и ведёт на русскоязычные Правила с
описанием сигналов, источников, методов, неиспользуемых сведений, reset/delete,
владельца ресурса и email для юридически значимых сообщений.

## 16. Карточки процентов интереса

Карточки с процентами остаются **гипотезой**, а не обещанием точности модели.
Если они тестируются:

- `interest_index` 0–100 отделён от `data_sufficiency`;
- значение называется «индекс интереса», а не «вероятность», «уверенность» или
  «прогресс»;
- рядом показывается meter достаточности данных и простое объяснение факторов;
- пользователь может скорректировать тему `больше / нейтрально / меньше`;
- UI проходит calibration/research test: одинаковые диапазоны должны иметь
  сопоставимое наблюдаемое поведение;
- A/B должен доказать, что карточка помогает управлению профилем, а не создаёт
  ложное чувство диагностики личности.

Без этих доказательств проценты не выпускаются, но сам контроль tri-state может
быть полезен отдельно.

## 17. Наблюдаемость и объяснимость

Каждая выдача имеет:

```text
run_id / served_list_id
surface_id
catalog_snapshot_hash
feature_schema_version
taxonomy_version
model_version
profile_revision
profile_projection_etag / next_refresh_at
activation_state / activation_action
contract / privacy_notice / recommendation_rules versions
experiment_id + variant
candidate_count / eligible_count / served_count
final event ids + ranks
filter reason / score component summary
exploration and rescue markers
```

Пользовательское объяснение короткое: «похоже на сохранённый джаз», «рядом и
бесплатно», «новое для вас». Внутренний score breakdown доступен только
sanitized test/debug-контракту и не раскрывает raw history.

Минимальные SLI:

- static fallback availability;
- personalization response/cache hit latency;
- stale profile/catalog rate;
- outbox accepted/rejected/expired rate;
- served-vs-DOM mismatch;
- explicit-hide resurrection count — целевой 0;
- visible-card move count — целевой 0;
- profile cross-context leak — целевой 0;
- pre-activation server-profile/telemetry count — целевой 0;
- local-rerank personalization requests per ordinary page view — целевой 0;
- refresh 304/cache hit, refresh frequency и projection payload bytes;
- candidate starvation и surface fallback rate;
- model quality/diversity/worst-group по snapshot.

## 18. Что именно должно доказывать тестирование

Автотесты разделяют четыре разных утверждения:

1. **Correctness:** модель соблюдает contracts и не ломает страницу.
2. **Sensitivity:** изменение валидного сигнала ожидаемо меняет релевантные
   компоненты/ранги, а нерелевантные — не меняет.
3. **Offline quality:** на фиксированных каталогах модель лучше baseline по
   relevance/calibration/diversity и не ухудшает защищённые группы.
4. **Causal product value:** в реальном рандомизированном эксперименте растут
   продуктовые outcomes без ухудшения guardrails.

Playwright хорошо доказывает 1–2 и собирает evidence для 3. Он не может сам по
себе доказать причинный uplift; для этого нужен A/B.

## 19. Эталонные данные для тестов

Создать versioned immutable набор:

```text
tests/fixtures/personalization/v1/
  catalog.json
  event-features.json
  personas.json
  judgements.json
  counterfactuals.json
  surface-policies.json
  collection-surfaces-v1.json
  campaign-contexts.json
  activation-states.json
  expected-invariants.json
```

### 19.1. Требования к catalog snapshot

- реальные, но замороженные и санитизированные события;
- будущие/прошедшие/отменённые, дубли и linked occurrences;
- разный supply по темам, ценам, времени, расстоянию и языку;
- похожие события, создающие настоящий rank choice;
- campaign/artifact exposures;
- явные must-not и acceptable-exploration примеры;
- hash и canonical generation script.

### 19.2. Golden profiles

Минимум 10–12 профилей:

- шесть Golden persona hypotheses;
- cold start без сигналов;
- смешанный `55% jazz / 45% theatre`;
- семейный профиль с price/distance constraints;
- пользователь с повторным long-term интересом;
- campaign/artifact hunter;
- профиль с typed negative и exact tombstone;
- русскоязычный local и нерусскоязычный visitor с одинаковым вкусом.
- профиль с interaction на `sensitive_topic`, который не должен получить
  semantic facet.

### 19.3. Judgements

Для пары `profile × event × surface` хранить:

- relevance `0..3`;
- `must_not_serve`;
- `acceptable_exploration`;
- reason codes;
- label source/reviewer/version.

Двусмысленные примеры получают несколько оценок или low confidence, а не
искусственно точную метку.

## 20. Пирамида автоматизированных проверок

| Уровень | Когда | Что проверяет | Инструмент |
|---|---|---|---|
| T0 unit/property | каждый commit | scorer, merge, decay, eligibility, metrics, bounds | Vitest/Node/Python |
| T1 routed component | каждый PR | canonical browser module с mock API/faults | Playwright route interception |
| T2 built-site contract | каждый PR | реальный Astro build/preview, DOM/a11y/network | Playwright |
| T3 model benchmark | nightly и model PR | frozen catalog × golden profiles × variants | Offline evaluator + Playwright sampling |
| T4 staging/public canary | перед release/ежедневно | реальная цепочка ingest→rollup→next feed | Playwright + test API/DB assertions |
| T5 production | постоянно | SLI, shadow/A-B guardrails | Analytics/alerts |

### 20.1. T0

Обязательные property/invariant tests:

- exact hide idempotency и невозможность rescue;
- merge commutativity там, где она заявлена, и deterministic conflict order;
- decay monotonicity;
- typed constraint не изменяет unrelated genre affinity;
- bounded top-K/profile/outbox;
- stable output при одинаковых inputs/version/seed;
- zero personalization network requests при обычном local rerank;
- до activation server profile/weak telemetry не создаются;
- activation action атомарно сохраняет все contract versions;
- одно действие не запускает materialized profile refresh;
- ETag `304` не меняет projection, stale/invalid projection не применяется;
- `sensitive_topic` не появляется в user facets/long-term evidence;
- eligibility before score;
- rescue quota `<= 10%` persona-suppressed;
- no NaN/Infinity/unstable sorting;
- metric implementations на hand-calculated fixtures.

### 20.2. T1–T2 Playwright

Нужно удалить алгоритмический drift: browser runtime импортирует один canonical
scorer/policy module, который импортируют и unit tests. Demo не имеет отдельной
бизнес-логики.

Создать root `playwright.config`:

- явный `testDir`;
- ignore `.codex/**`, linked worktrees и `artifacts/**`;
- `webServer` для production-like Astro preview;
- projects: Chromium desktop + representative mobile viewport; WebKit как
  compatibility lane;
- trace `retain-on-failure`, screenshot `only-on-failure`, видео только для
  нестабильной interaction-группы;
- deterministic locale/timezone/clock/reduced-motion;
- console error, page error и failed request превращаются в failure по allowlist.

Playwright используется для:

- seeded storage/profile и отдельного clean BrowserContext;
- полную блокировку personalization routes для доказательства zero-network
  local rerank;
- route interception: success, timeout, 4xx/5xx, stale/out-of-order, offline;
- virtual clock для short/mid/long и undo countdown;
- измерения DOM order, viewport, focus, CLS и ARIA contract;
- проверки localStorage/IndexedDB/network batch с редактированием секретов;
- virtual clock/ETag для cadence, cooldown, `next_refresh_at` и single-flight;
- визуальных snapshots только устойчивых состояний и ARIA snapshots для
  структуры, а не как замена semantic assertions.

### 20.3. T3 offline benchmark

Для каждого variant на одном catalog/profile snapshot вычислять:

- `NDCG@K`, `MRR`/Recall@K для judgement relevance;
- calibration/Brier или ECE только там, где модель заявляет probability;
- catalog/topic/venue coverage;
- intra-list diversity;
- novelty и serendipity;
- false expansion от одного слабого сигнала;
- exact/typed-negative violation rate;
- constraint violation rate;
- lifecycle/duplicate violation rate;
- persona dominance/unknown-mass behavior;
- worst-group delta по language, supply, cold-start и persona;
- p50/p95 score/build/browser latency.

Thresholds хранятся рядом с fixture и имеют статус:

- **hard gate:** invariant/security/privacy/eligibility = 0 нарушений;
- **non-regression gate:** quality/diversity/worst-group не хуже принятого
  baseline больше согласованного tolerance;
- **candidate gate:** новый variant должен улучшить заранее выбранную primary
  metric на temporal holdout, прежде чем попасть в online experiment.

Не следует сейчас фиксировать искусственный общий «релевантная карточка должна
попасть в первые N» без утверждённого snapshot и judgement. Такой порог сначала
калибруется на baseline и human labels.

### 20.4. T4 longitudinal canary

Canary использует специального test actor с `training_eligible=false`:

1. reset actor;
2. открыть реальную staging/static страницу;
3. выполнить действия только через UI;
4. дождаться accepted strong actions/profile revision;
5. запросить следующую выдачу;
6. проверить DB/API evidence и DOM served list;
7. удалить test data.

Прямая инъекция готового профиля допустима для fixture contract, но **не**
считается доказательством longitudinal learning loop.

## 21. Gherkin как владелец поведения

Gherkin описывает продуктовую причинность и инварианты; step definitions вызывают
Playwright. `behave`-окружение нужно разделить по тегам, чтобы static-site
сценарии не поднимали Telethon автоматически.

Предлагаемые tags:

```text
@personalization @static-site @contract
@model-quality @nightly
@staging @longitudinal
@privacy @fault
@mobile @accessibility
```

### 21.1. Изоляция от Telegram E2E

- `@telegram` создаёт Telethon session;
- `@static-site` создаёт Astro/Playwright context и не читает Telegram auth
  bundle;
- live staging credentials поднимаются только для `@staging`;
- тестовые действия маркируются `training_eligible=false` и не загрязняют
  organic analytics.

## 22. Репрезентативные Gherkin-сценарии

Ниже — acceptance skeleton. Конкретные русские тексты кнопок можно вынести в
page-object, но поведение остаётся в feature.

### 22.1. Exact hide, undo и глобальность

```gherkin
Feature: Явное скрытие события

  @contract @mobile
  Scenario: Скрытое событие сереет, допускает отмену и исчезает со всех поверхностей
    Given открыт мобильный календарь с событием "event-jazz-1" в видимой области
    When пользователь нажимает "Не интересует" у события "event-jazz-1"
    Then карточка "event-jazz-1" немедленно показана в pending-состоянии той же высоты
    And показана компактная строка уведомления с кнопкой "Вернуть" и индикатором времени
    And остальные видимые карточки не меняют положение
    When время отмены истекает
    Then событие "event-jazz-1" отсутствует в календаре, подборках и "Для меня"
    And событие "event-jazz-1" есть в подборке "Помечены «не интересует»"
    And served evidence содержит причину "explicit_event_tombstone"

  @contract
  Scenario: Отмена не оставляет удалённого действия
    Given событие "event-jazz-1" находится в pending hide
    When пользователь нажимает "Вернуть" до истечения таймера
    Then исходное место карточки восстановлено без скачка страницы
    And unsent hide отсутствует в outbox
    And если hide был первым действием, server activation отсутствует
    And уже принятый hide при наличии компенсирован более новой idempotent undo записью
```

### 22.2. Невидимая область

```gherkin
Feature: Безопасная перестановка статической ленты

  @contract @accessibility
  Scenario: Новая персональная выдача меняет только карточки ниже viewport
    Given первые 4 карточки видимы и карточка 2 имеет keyboard focus
    And персональный ответ меняет порядок первых 10 кандидатов
    When browser применяет ответ текущей revision
    Then карточки 1..4 сохраняют DOM-порядок и экранные координаты
    And focused карточка остаётся в focus
    And переставлены только карточки полностью ниже safety anchor
    And cumulative layout shift не превышает утверждённый budget
    And served summary равен фактическому DOM-порядку
```

### 22.3. Политика поверхностей

```gherkin
Feature: Разная сила персонализации по surface

  Scenario Outline: Surface сохраняет свой главный смысл
    Given зафиксирован catalog snapshot "catalog-v1"
    And профиль "jazz-lover" имеет сильный интерес к джазу
    When открыта поверхность "<surface>"
    Then применена политика "<policy>"
    And ни одно событие не нарушает eligibility "<eligibility>"

    Examples:
      | surface      | policy            | eligibility                    |
      | calendar     | exact-hide-only   | canonical-date-and-lifecycle   |
      | free         | weak-tail-rerank   | is-free                        |
      | children     | weak-tail-rerank   | family-and-age-compatible      |
      | for-me       | strong-hybrid      | future-canonical-catalog       |
      | search-jazz  | query-first        | query-match                    |
      | event-detail | anchor-first       | related-to-anchor              |
```

### 22.4. Counterfactual sensitivity

```gherkin
Feature: Чувствительность модели к валидным сигналам

  @model-quality
  Scenario: Один новый jazz-like меняет джазовый компонент, но не hard constraints
    Given одинаковые catalog, model, seed и профиль до действия
    When в копии профиля добавлен один валидный like для jazz события
    Then jazz affinity не уменьшается
    And rank релевантных jazz событий в aggregate не ухудшается
    And theatre affinity не изменяется без graph evidence
    And платные события не появляются в eligibility-only подборке "Бесплатно"
    And exact hidden jazz событие не возвращается

  @model-quality
  Scenario: Ограничение цены не превращается в отрицание жанра
    Given пользователь указал причину "слишком дорого" для jazz события
    When пересчитан профиль
    Then price constraint усилился
    And jazz genre affinity не уменьшилась только из-за этой причины
```

### 22.5. Campaign/artifact noise

```gherkin
Feature: Промо-активности не обучают органический профиль

  Scenario: Охота за артефактами не создаёт ложный интерес
    Given показы имеют campaign context "artifact-hunt-2026"
    When пользователь быстро открывает 12 разных campaign карточек
    Then implicit organic interest weights не увеличиваются
    And campaign diagnostic counters увеличиваются отдельно
    But явный like одной карточки остаётся валидным explicit сигналом
```

### 22.6. Горизонты

```gherkin
Feature: Временные горизонты интереса

  Scenario: Краткий social share не становится долгосрочным вкусом сам по себе
    Given новый профиль без long-term evidence
    When пользователь один раз делится событием
    Then session и short social intent увеличиваются
    And long-term topic affinity не создаётся

  Scenario: Повторные подтверждённые посещения формируют long-term интерес
    Given валидные подтверждения посещений одной темы распределены более чем на 6 месяцев
    When materializer строит новый профиль
    Then long-term affinity темы увеличивается
    And provenance содержит только доверенные attendance evidence ids
```

### 22.7. Faults, activation и isolation

```gherkin
Feature: Приватность и отказоустойчивость

  @privacy @accessibility
  Scenario: Публичное уведомление доступно до и после активации
    Given открыта любая страница без авторизации
    Then виден текст "На информационном ресурсе применяются рекомендательные технологии."
    And ссылка ведёт на доступные без авторизации русскоязычные Правила
    And уведомление не перекрывает контент

  @privacy
  Scenario: Пассивный просмотр не запускает server personalization
    Given новый browser context без personalization_started_at
    When пользователь открывает карточки, прокручивает и закрывает уведомление
    Then показана общая или контекстная выдача
    And server profile не создан
    And network log не содержит impression, open или profile mutation

  @privacy @contract
  Scenario: Первый like атомарно активирует персонализированную функцию
    Given новый browser context без personalization_started_at
    And опубликованы версии договора, privacy notice и recommendation rules
    When пользователь нажимает like
    Then like немедленно отражён локально
    And показано информационное уведомление без кнопки "Согласен"
    And activation batch содержит действие и все версии документов
    And server сохраняет одну activation record и одно idempotent strong action
    But отдельный analytics, marketing, push или email consent не создаётся

  @fault
  Scenario Outline: Сбой backend не ломает страницу
    Given static HTML и активированная локальная profile projection уже загружены
    And personalization endpoint отвечает "<fault>"
    When пользователь открывает подборку и нажимает CTA события
    Then карточки остаются доступными в static fallback порядке
    And CTA работает
    And console не содержит необработанных ошибок
    And outbox соблюдает idempotency и retry policy

    Examples:
      | fault             |
      | timeout           |
      | http-500          |
      | malformed-json    |
      | stale-revision    |
      | offline           |

  @privacy
  Scenario: Два анонимных контекста не смешивают профили
    Given browser context A обучен на jazz действиях
    And browser context B создан с чистым storage и credential
    When оба открывают "Для меня"
    Then context B не получает ids, scores или profile revision context A
```

### 22.8. Model bake-off

```gherkin
Feature: Сравнение вариантов модели

  @model-quality @nightly
  Scenario Outline: Варианты считаются на одном immutable snapshot
    Given catalog "catalog-v1", judgements "judgements-v1" и profile set "personas-v1"
    When offline evaluator запускает model variant "<variant>" с фиксированным seed
    Then сохранены ranks, score components и metrics для каждого профиля
    And hard invariant violations равны 0
    And результат сравним с зарегистрированным baseline

    Examples:
      | variant      |
      | control      |
      | facets       |
      | hard-persona |
      | soft-persona |
      | hybrid       |
```

### 22.9. Zero-network rerank и periodic refresh

```gherkin
Feature: Локальная персонализация без запроса при каждом просмотре

  @contract @offline
  Scenario: Тематическая подборка переставляет только невидимый tail без сети
    Given localStorage содержит совместимую projection "jazz-profile-r7"
    And next_refresh_at ещё не наступил
    And personalization routes заблокированы до конца сценария
    And первые 4 карточки подборки "Фестивали" находятся в viewport
    When открывается статическая страница подборки
    Then к Supabase и personalization API не выполнено ни одного запроса
    And карточки 1..4 сохраняют native порядок и координаты
    And карточки ниже safety anchor ранжированы локальным scorer
    And DOM order воспроизводим для тех же projection, catalog и seed

  @contract @offline
  Scenario: Конечная ещё не показанная выборка полностью ранжируется локально
    Given finite eligible set ещё не был показан пользователю
    And localStorage содержит совместимую profile projection
    When блок готовится ниже viewport
    Then весь eligible set пересортирован без сети
    And eligibility и exact tombstones применены до score

  @contract
  Scenario: Social proof не скачивает новый профиль после каждого действия
    Given profile projection имеет revision 7 и next_refresh_at завтра
    When пользователь совершает три валидных действия
    Then explicit overlay и bounded outbox обновлены
    And endpoint online recompute не вызван
    And profile projection остаётся revision 7

  @contract
  Scenario Outline: Периодическое обновление сохраняет надёжный fallback
    Given next_refresh_at наступил и локально сохранена revision 7
    And profile endpoint отвечает "<response>"
    When одна из вкладок получает single-flight refresh lease
    Then выполнен не более одного conditional request с ETag revision 7
    And результат соответствует правилу "<result>"

    Examples:
      | response             | result                                  |
      | 304                  | revision 7 сохранена                    |
      | compatible-revision8 | revision 8 атомарно опубликована локально |
      | invalid-schema       | revision 7 сохранена, ответ отклонён    |
      | timeout              | revision 7 сохранена, назначен backoff  |
```

### 22.10. Sensitive-topic safety

```gherkin
Feature: Специальные категории не становятся интересами пользователя

  @privacy @model-quality
  Scenario: Взаимодействие с sensitive событием не материализует semantic facet
    Given событие помечено sensitive_topic "religious-belief"
    And персонализированная функция уже активирована
    When пользователь открывает, делится и нажимает CTA этого события
    And materializer строит следующую revision
    Then user profile не содержит sensitive topic, persona или long-term evidence
    And событие остаётся доступно через прямой URL, поиск и общую выдачу
    And exact hide при его наличии хранится только как event tombstone
```

## 23. Test/debug contract

В test/staging build доступен санитарный API:

```js
window.__KE_PERSONALIZATION_TEST__.snapshot()
```

Он возвращает только:

```json
{
  "build_id": "...",
  "catalog_snapshot_hash": "...",
  "model_version": "...",
  "feature_schema_version": "...",
  "taxonomy_version": "...",
  "surface_id": "...",
  "profile_revision": "...",
  "profile_projection_etag": "...",
  "next_refresh_at": "...",
  "activation_state": "inactive|active",
  "persona_fixture_key": "...",
  "candidates": [
    {
      "event_id": "...",
      "eligible": true,
      "filter_reasons": [],
      "score_components": {},
      "final_rank": 1,
      "presentation_state": "served"
    }
  ],
  "served_summary": {}
}
```

Контракт не содержит raw history, cookie, email, bearer tokens и secret.
В production он отсутствует либо возвращает только общедоступные build ids.
Версии legal-документов можно проверять отдельными публичными meta-полями, но
test API не возвращает доказательства активации конкретного пользователя.

## 24. Артефакты прогона

Каждый failure сохраняется в:

```text
artifacts/codex/personalization-e2e/<run_id>/
```

Минимальный состав:

- `run-manifest.json`: git SHA, build/model/schema/taxonomy, seed, browser;
- `catalog-profile-hashes.json`;
- `supply-eligibility-candidates.json`;
- `served-dom-evidence.json`;
- `redacted-network.json`;
- `storage-summary.json` без secret/raw history;
- `metrics.json` и baseline diff;
- Playwright `trace.zip`;
- screenshot/ARIA snapshot только релевантного failure;
- staging DB/API assertion и cleanup result для longitudinal test.

Артефакты не коммитятся. Fixture попадает в Git только в минимальном
санитизированном виде.

## 25. Диагностика неудачного теста

Проверять строго по слоям, чтобы не «лечить веса» при поломке данных:

1. **Supply:** есть ли нужные события в canonical snapshot?
2. **Eligibility:** не исключила ли surface/date/lifecycle политика?
3. **Candidate generation:** попал ли event в нужный pool?
4. **Activation/legal gate:** можно ли было создавать server profile и есть ли
   валидные document versions/localization state?
5. **Telemetry:** действие было видимо, валидно, activated и accepted?
6. **Materialization:** попало ли действие в нужный scheduled/threshold batch?
7. **Rollup/profile:** изменилась ли правильная facet/horizon с provenance?
8. **Projection refresh:** новая revision опубликована, ETag совместим и refresh
   действительно наступил?
9. **Application:** выдача использовала ожидаемую stable projection + overlay?
10. **Score:** какой компонент дал вклад/штраф?
11. **Policy:** diversity/suppression/exploration/fatigue изменили rank?
12. **Presentation:** DOM применил правильный tail и не получил stale response?
13. **Oracle:** judgement/expected threshold действительно корректны?

Автоматический failure report должен выводить первый расходящийся слой. Только
после этого допускается изменение модели.

## 26. Release gates

### 26.1. Hard gates

- 0 exact-hide resurrection;
- 0 eligibility/lifecycle/duplicate violations;
- 0 server profile/impression/open mutations до activation;
- 0 activation от scroll/impression/open/dwell/share/закрытия уведомления;
- 0 personalization network requests для обычного local rerank;
- 0 per-action online profile recompute/download;
- 0 cross-context leaks;
- 0 видимых/acted карточек, перемещённых rerank;
- 0 served-vs-DOM mismatch;
- 0 необработанных page/console errors;
- 0 secret/raw-profile leaks в HTML, logs и artifacts;
- static fallback и CTA проходят fault matrix;
- rescue persona-suppressed `<= 10%` и 0 rescue explicit hides;
- mobile hidden collection + restore проходят E2E.
- 0 user facets/long-term evidence из `sensitive_topic`;
- activation evidence содержит утверждённые contract/privacy/rules versions;
- постоянное уведомление и публичные Правила доступны без авторизации;
- localization/legal gate подтверждён до включения remote profile.

### 26.2. Quality gates

Точные числа устанавливаются после создания `v1` snapshot и baseline. До этого
обязательны:

- hybrid не хуже control/facets по согласованной primary relevance metric;
- diversity/coverage не хуже tolerance;
- worst-group не имеет необъяснённого существенного провала;
- false expansion от одного слабого сигнала ограничен;
- p95 runtime укладывается в performance budget и не ухудшает static page CWV;
- profile projection укладывается в size budget, а refresh cadence не создаёт
  request storm;
- calibration подтверждена до показа процентного индекса.

### 26.3. Online gates

- эксперимент заранее регистрирует hypothesis, primary metric, guardrails,
  eligibility, sample/stop rule и rollback;
- assignment устойчив и не зависит от будущего поведения;
- test/campaign traffic отделён от organic;
- novelty/diversity, hide rate, CTA, save, attendance и downstream unsubscribe/
  complaint оцениваются вместе;
- release не объявляется успешным только по CTR.

## 27. План реализации

### Phase 0 — устранить расхождение прототипов

- вынести scorer/profile/surface policy из inline/demo в canonical shared module;
- настроить root Playwright, Astro preview и CI lane;
- реализовать существующие draft Gherkin steps без Telethon для `@static-site`;
- зафиксировать current behavior characterization tests;
- заменить старую consent-модель на activation state-machine и синхронизировать
  copy с фактическим data flow;
- подготовить постоянное уведомление и публичные Правила рекомендательных
  технологий;
- закрыть data-localization/owner ADR до remote-profile кода.

### Phase 1 — локальный корректный MVP

- `collection-surfaces-v1.json` и surface policy engine;
- exact hide/undo/notification/hidden collection;
- ниже-viewport presenter;
- canonical local scorer, typed feedback, stable projection + immediate overlay;
- zero-network rerank длинных и конечных тематических подборок;
- T0/T1/T2 hard invariants;
- качественный cold-start fallback.

### Phase 2 — activated durable feedback loop

- private Supabase migrations, RLS/functions/security review;
- только после localization gate: approved primary store и обновлённый ownership
  ADR;
- same-origin idempotent batch, activation evidence, separate purpose consents,
  device proof;
- strong action + explicit state + served summary;
- scheduled/threshold profile materializer, ETag projection refresh, revisions и
  remote reset/delete;
- longitudinal staging E2E.

### Phase 3 — модель и «Для меня»

- versioned event feature snapshot и taxonomy;
- golden fixtures/judgements, offline evaluator и registry;
- variants control/facets/soft-persona/hybrid;
- daily `/dlya-menya/` manifest + fallback;
- anti-bubble/exploration/suppression metrics.

### Phase 4 — controlled rollout

- shadow scoring без изменения порядка;
- малый A/B для strong «Для меня»;
- отдельно weak rerank тематических подборок;
- campaign-aware telemetry;
- interest-index UI только после calibration gate.

### Phase 5 — расширение подборок

- завершить «Необычное» и «Для меня»;
- добавить Кино, Театр, Спектакли, Научпоп, Наука через тот же definition
  contract;
- для каждой surface добавить fixtures, eligibility и policy scenarios до
  публикации.

## 28. Решения, которые ещё нужно принять

| Вопрос | Почему открыт | Как закрыть |
|---|---|---|
| Точные short/mid границы и decay | Последние требования фиксируют только long `>=6 мес.` | Temporal replay + A/B |
| Undo duration | 5 секунд — стартовая гипотеза | Mobile usability/a11y test |
| Порог persona suppression | Зависит от supply и calibration | Offline precision + shadow report |
| Общая exploration quota | Не равна 10% rescue | Surface-specific experiment |
| Точные model weights | Старые значения не доказаны | Registered bake-off |
| Judgement owner/process | Нужна воспроизводимая разметка | Label guide + double review sample |
| Primary store и локализация | Текущий Supabase нельзя считать допустимым без подтверждения региона первоначальной записи | Localization audit; российский Supabase/Postgres либо новый ADR/primary contour |
| Retention/152-ФЗ | Архитектурный, legal и ops вопрос | Финальный legal review, privacy/rules documents, РКН/processor audit |
| Profile refresh cadence | Нужна периодичность без per-action recompute и request storm | Load/quality replay; стартовая гипотеза ≤1 раза/24ч |
| Interest percentage UI | Может вводить в заблуждение | Calibration + product research |
| Secret daily URL cache policy | Capability URL несёт риск утечки | Threat model + expiry/revoke test |
| «Необычное», Кино, Театр и Наука eligibility | `podborki.md` неполон | Product definition before code |

## 29. Трассировка последних требований

| Требование | Где закреплено |
|---|---|
| Compact notification, undo, progress, auto-hide | §15.2, Gherkin §22.1 |
| Экологичный localStorage + Supabase exchange | §7.2–7.3, §9 |
| Без предварительного consent; activation первым осмысленным действием | §3, §6, §9.1, §9.3, Gherkin §22.7 |
| Уведомление о рекомендательных технологиях и публичные Правила | §9.3, §9.5, §15.3, §26.1 |
| Менять только невидимую область | §6, §15.1, §22.2 |
| Тематический rerank без Supabase/server request | §6, §14.3, Gherkin §22.9 |
| Финальная выборка полностью сортируется локально до показа | §14.3, Gherkin §22.9 |
| Профиль пересчитывается pipeline, а projection забирается периодически | §9.4, §17, §22.9, §27 Phase 2 |
| Персонализация всех подборок | §14, §27 Phase 5 |
| Календарь: только exact hide | §6, §14.1, §22.3 |
| «Для меня» сильнее обычных подборок | §13.4, §14.1–14.2 |
| Exact hide сереет, затем исчезает, доступен mobile recovery | §15.2, §22.1 |
| Like/share/CTA/attendance/repeat attendance | §12, §22.6 |
| Слабые тени только из повторных длинных паттернов | §12 |
| Проценты интереса — гипотеза | §16 |
| Easter egg не загрязняет organic profile | §12, §22.5 |
| Short/mid/long, long ≥6 месяцев | §10.2, §22.6 |
| Filter bubble и novelty | §13.4, §20.3 |
| Static pages stable/fast | §3, §6, §7, §14.2, §26 |
| Persona mismatch + rescue ≤10% | §13.4, §20.1, §26.1 |
| Учтены текущие/планируемые подборки | §14.1, §27 Phase 5 |
| Sensitive/детские профили исключены из low-friction контура | §10.5, §11–12, §22.10, §26.1 |
| Локализация server profile является release gate | §7.1, §9.5, §26.1, §28 |

## 30. Итоговый acceptance contract

Система может называться работоспособной, когда одновременно доказано:

1. один canonical scorer управляет production runtime и тестами;
2. все surfaces объявлены data-driven и сохраняют eligibility/главный смысл;
3. exact hide, activation/legal gate, isolation и static fallback проходят hard
   gates;
4. видимая область не переставляется;
5. ordinary collection test подтверждает zero-network local rerank по stable
   projection;
6. longitudinal staging test через UI подтверждает
   `activation/action → accepted state → scheduled materialization → periodic
   projection refresh → next local feed`;
7. hybrid сравнен на immutable snapshot с control/facets/persona baselines;
8. quality/diversity/worst-group gate пройден без подмены A/B browser-тестом;
9. production rollout имеет experiment, observability, rollback и очистку
   test/campaign traffic.

До выполнения этих условий любые хорошие примеры выдачи являются полезным
prototype evidence, но не доказательством качества production-персонализации.

## 31. Связанные документы и технические источники

Внутри проекта:

- [Последние ручные требования](./requirements.md)
- [Правовой/data-flow contract персонализации](./legal-repsonalization.md)
- [Исследование интересов](./interests-deep-research.md)
- [Подборки](../podborki.md)
- `docs/architecture/personalization-data-ownership.md` из `origin/main`
- [Anonymous personalization](../../unsigned-personalization/README.md)
- [LLM-first request guide](../../../llm/request-guide.md)
- [E2E testing](../../../operations/e2e-testing.md)
- [E2E scenarios](../../../operations/e2e-scenarios.md)

Официальные внешние контракты, которые нужно сверять при реализации:

- 152-ФЗ, статья 6, включая обработку для исполнения договора:
  <https://www.consultant.ru/document/cons_doc_LAW_61801/315f051396c88f1e4f827ba3f2ae313d999a1873/>
- 152-ФЗ, статья 9 о требованиях к согласию:
  <https://www.consultant.ru/document/cons_doc_LAW_61801/6c94959bc017ac80140621762d2ac59f6006b08c/>
- 152-ФЗ, статья 10 о специальных категориях:
  <https://www.consultant.ru/document/cons_doc_LAW_61801/26edb2934b899bf9c74c3a8f7e574651c6565e6d/>
- 149-ФЗ, статья 10.2-2 о рекомендательных технологиях:
  <https://www.consultant.ru/document/cons_doc_LAW_61798/2a69c627d62738291fe0a0fd4c1253385e730784/>

- Supabase Data API security and RLS:
  <https://supabase.com/docs/guides/api/securing-your-api>
- Supabase database security:
  <https://supabase.com/docs/guides/database/secure-data>
- Supabase explicit Data API grants change:
  <https://supabase.com/changelog/45329-breaking-change-tables-not-exposed-to-data-and-graphql-api-automatically>
- Playwright network interception:
  <https://playwright.dev/docs/network>
- Playwright visual snapshots:
  <https://playwright.dev/docs/test-snapshots>
- Playwright ARIA snapshots:
  <https://playwright.dev/docs/aria-snapshots>
- Playwright tracing:
  <https://playwright.dev/docs/api/class-tracing>
- Playwright BrowserContext isolation:
  <https://playwright.dev/docs/api/class-browsercontext>
