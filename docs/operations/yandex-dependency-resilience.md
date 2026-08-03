# Отказоустойчивость Yandex-зависимостей статического сайта

> **Статус:** канонический нормативный contract для проектирования, реализации,
> тестирования и release-gates.
>
> **Дата среза:** 2026-08-03.
>
> **Область:** клиентские и серверные зависимости от Yandex Cloud/Yandex ID,
> взаимодействие с Supabase через Yandex relay, YDB-проекции, почтовые контуры,
> фокус-группа, персонализация и пользовательские подтверждения отправки.
>
> **Связанные документы:**
> [release plan](../features/static-site-pages/release-plan.md),
> [release autotest gates](../features/static-site-pages/release-autotest-gates.md),
> [стратегия автотестирования](static-site-autotest-strategy.md),
> [ownership персональных данных](../architecture/personalization-data-ownership.md),
> [целевая персонализация](../features/static-site-pages/personalizaion/personalization-to-be.md),
> [scenario registry](../testing/static-site-autotest-scenarios.v1.yml).

## 1. Полевое наблюдение 2026-08-03

Участник фокус-группы выполнил расширенную диагностику в Mobile Safari. На одном
устройстве и в рамках одной проверки наблюдалось:

| Проверка | Результат |
|---|---|
| Supabase Auth напрямую | доступно |
| Supabase Data напрямую | доступно |
| Yandex relay для Auth | нет соединения |
| Yandex relay для Data | нет соединения |
| Resilient Auth | доступно, выбран direct |
| Resilient Data | доступно, выбран direct |
| YDB control/API Gateway | нет соединения |

Это валидное подтверждение **частичного отказа клиентского пути к нескольким
Yandex endpoints** при одновременно доступном прямом пути к Supabase. Оно
согласуется с terminal mobile-матрицей, где
`client_yandex_relay_unreachable` успешно переключает обязательные операции на
`direct`.

Наблюдение **не доказывает глобальное падение всего Yandex Cloud**. Возможны:
локальная маршрутизация оператора, DNS/IPv6, фильтрация конкретного домена API
Gateway, региональный сетевой путь либо отказ конкретных endpoint/service
classes. Поэтому продукт и диагностика не используют один общий флаг
`yandex_available` и не сообщают пользователю «Яндекс не работает» без более
узкого доказательства.

## 2. Главное решение

Yandex-зависимость всегда раскладывается на отдельные capabilities. Для каждой
capability заранее определяются:

1. система записи истины (system of record, SOR);
2. допустимый резервный путь;
3. idempotency/replay semantics;
4. момент, после которого пользователь может увидеть подтверждение успеха;
5. локальное или серверное pending-состояние;
6. поведение при неоднозначном результате;
7. срок хранения и операторский способ восстановления;
8. отдельный тест отказа.

Отказ вспомогательной Yandex-проекции не может:

- отменить уже подтверждённое основное действие;
- изменить здоровье независимого прямого Supabase-маршрута;
- заставить интерфейс сообщить ложный успех или ложную потерю данных;
- породить повтор selected-once операции;
- стереть payload до terminal acknowledgement.

## 3. Yandex availability — не один boolean

| Capability | Роль | SOR / источник подтверждения | Поведение при отказе |
|---|---|---|---|
| `supabase_relay` | альтернативный клиентский путь к тому же Supabase | Supabase response + transport receipt | выбрать direct до dispatch; не считать отказ relay отказом Supabase |
| `ydb_analytics` | асинхронная аналитическая проекция | основной action/profile SOR, не YDB | основное действие успешно; projection остаётся pending и повторяется сервером |
| `ydb_control` | диагностический/read-only control endpoint | отсутствует, это probe | только диагностический degraded signal |
| `yandex_oauth` | один из способов входа | Supabase Auth session | показать email OTP как независимую альтернативу; не объявлять Auth целиком недоступным |
| `postbox_transactional` | транспорт транзакционного письма | durable Supabase email outbox + provider receipt | сохранить queued/failed state и повторять серверно по idempotency; не говорить «письмо отправлено» до provider acceptance |
| `yandex_inbound_pipeline` | автоматизированная обработка входящей почты | оригинал в SpaceWeb mailbox + durable UID/cursor/outbox | не терять оригинал; replay после восстановления; DLQ для terminal processing failure |
| `object_storage_cdn` | доставка статического артефакта | release manifest + published object readback | удерживать last-good release; не продвигать неполный candidate |
| `e2e_mail_trigger` | закрытый receipt только для автотестов | test artifact | BLOCKED test infrastructure; не влияет на продуктовый mail path |

Relay и direct ведут к одному Supabase upstream. Поэтому relay помогает при
отказе **клиентского пути**, но не является независимым хранилищем и не спасает
от общего падения Supabase upstream.

## 4. Контракт подтверждения и отсутствия потерь

### 4.1. Пользовательский успех требует durable acknowledgement

Интерфейс может показать окончательное «Сохранено»/«Отправлено» только после
подтверждения канонического владельца данных:

- strong action/profile/focus feedback state — после Supabase/approved primary
  store commit;
- email — после durable outbox commit, а формулировка «отправлено» только после
  provider acceptance;
- статическая публикация — после manifest-bound upload и readback;
- YDB analytics — никогда не является подтверждением пользовательского
  действия.

Попытка HTTP/fetch сама по себе не является доставкой. `200` промежуточного
Yandex API Gateway без принятия downstream-системой также не является terminal
успехом.

### 4.2. Общая state machine

Для операций, которые могут переживать сетевой отказ, используется явная
машина состояний:

```text
local_applied
  -> queued
  -> dispatching
  -> committed
  -> projecting
  -> terminal_complete

alternative terminal/intermediate states:
  ambiguous
  retryable_failed
  partially_committed
  terminal_failed
  expired_with_user_notice
```

Обязательные свойства:

- stable `client_event_id`/`action_id` создаётся один раз;
- payload не удаляется до `committed` либо явного terminal disposition;
- повтор idempotent операции использует тот же ключ;
- selected-once после dispatch не повторяется автоматически через второй путь;
- reconnect flush single-flight, с bounded exponential backoff и jitter;
- reload/relaunch сохраняет очередь и authoritative local state;
- max attempts/TTL не превращают запись в молчаливую потерю: появляется
  terminal state и операторский/user recovery path;
- серверный async outbox предпочтительнее browser retry для YDB/provider sidecar.

### 4.3. Составные операции

Операция из нескольких компонентов не получает общий успех до подтверждения
каждого обязательного компонента. Пример обратной связи фокус-группы:

```text
feedback_text: committed
feedback_screenshot: retryable_failed
analytics_projection: pending
```

Корректное сообщение: «Текст отправлен. Скриншот ожидает повторной отправки».
Некорректные сообщения: «Всё отправлено» или «Ничего не сохранилось».

## 5. Алгоритмы по capability

### 5.1. Supabase direct/relay

- probes/health решения ведутся отдельно по `auth`, `data`, `functions`,
  `storage`, `oauth`;
- Yandex relay failure не quarantine-ит direct route;
- disposable telemetry не изменяет route health продуктовой capability;
- safe-read может один раз перейти на подтверждённый alternate;
- selected-once сначала выбирает route, затем выполняет ровно один dispatch;
- idempotent replay допускается только при серверном ключе/uniqueness contract;
- при двух недоступных клиентских маршрутах selected-once dispatch равен нулю;
- при shared Supabase upstream outage оба маршрута честно возвращают degraded,
  relay не маркируется recovered только потому, что API Gateway ответил.

Статус на 2026-08-03: оба single-route-down направления приняты terminal
Android/iOS тестами. `both_client_routes_unreachable` и
`supabase_upstream_unavailable` остаются отдельными no-mail/degraded scenarios.

### 5.2. YDB analytics/control

- browser не пишет strong action непосредственно в YDB;
- primary transaction фиксирует действие и durable analytics-outbox record;
- worker проецирует de-identified payload в YDB с idempotency key;
- YDB timeout/5xx/network error оставляет outbox pending и не откатывает primary
  transaction;
- YDB outage не показывается пользователю как потеря его like/hide/feedback;
- если аналитика действительно нужна для исследования, dashboard показывает
  projection lag/backlog, а не подменяет его нулём;
- control probe остаётся read-only и не является обязательной частью action flow.

### 5.3. Персонализация и local-first действия

- like/hide/save/follow сначала получают понятный локальный результат;
- authoritative strong action отправляется в primary store через resilient
  direct/relay transport;
- при отсутствии обоих путей сохраняется bounded outbox и честная формулировка
  «Сохранено на устройстве, отправим после восстановления соединения»;
- последняя совместимая `profile_projection` продолжает работать;
- YDB analytics outage не задерживает rerank, CTA, profile materialization или
  primary acknowledgement;
- после reconnect один logical action создаёт один durable effect и одну
  аналитическую проекцию.

### 5.4. Обратная связь фокус-группы

- NPS, текстовая проблема, structured event error и screenshot имеют отдельные
  component receipts;
- текст/оценка — idempotent desired-state или append command с stable action id;
- screenshot upload не маскируется общим `feedback_saved`, пока object receipt
  отсутствует;
- при direct-only Storage outage текст может быть committed, screenshot остаётся
  pending; пользователь видит это различие;
- повтор страницы/кнопки не создаёт дубликаты;
- локальный pending receipt переживает reload и доступен в диагностике;
- YDB/telemetry failure не меняет состояние основной обратной связи.

### 5.5. Yandex OAuth

- OAuth availability проверяется отдельно от Supabase Auth direct/relay;
- при network/provider failure кнопка Яндекса получает понятное временное
  состояние, email OTP остаётся доступным;
- начатый callback с неоднозначным результатом сначала reconciles current Auth
  session/state, а не автоматически запускает второй OAuth flow;
- сообщение не звучит как «вход недоступен», если email-вход работоспособен.

### 5.6. Yandex Cloud Postbox

Транзакционный send pipeline:

```text
business transition
  -> durable Supabase outbox
  -> claim with lease
  -> provider dispatch with message/idempotency identity
  -> provider accepted
  -> delivery/suppression terminal event
  -> async analytics projection
```

- browser никогда не отвечает за повтор Postbox send;
- provider outage оставляет outbox retryable с bounded lease/backoff;
- ambiguous dispatch не переключается вслепую на другой provider;
- повторное отправление возможно только после reconciliation/provider lookup
  либо по explicit operator disposition;
- UI различает `запланировано`, `передано сервису`, `доставлено`, `ошибка`;
- YDB analytics не используется для send eligibility или deduplication.

### 5.7. Входящая почта через Yandex serverless

- SpaceWeb mailbox остаётся authoritative original;
- poller читает `BODY.PEEK` и UID, не удаляет и не помечает письмо прочитанным;
- cursor обновляется только после durable normalized receipt либо явной
  retry-safe фиксации;
- YMQ/DLQ и keyed idempotency позволяют replay без дублей;
- недоступность Yandex Functions/API Gateway не приводит к исчезновению письма:
  оно остаётся в mailbox и будет обработано позже;
- backlog age и oldest UID являются operational alerts.

### 5.8. Object Storage/CDN

- candidate не становится current при частичном upload/readback;
- manifest связывает exact SHA/tree/snapshot и полный object inventory;
- last-good сохраняется при Yandex storage/CDN write failure;
- пользователь не видит смешанное дерево двух release;
- retry выполняется по immutable object keys, а promotion — отдельной атомарной
  операцией после полного proof.

## 6. Коммуникация в интерфейсе

Допустимые формулировки:

- «Основные функции доступны напрямую. Резервный маршрут через Yandex сейчас не
  отвечает»;
- «Действие сохранено. Служебная статистика отправится позже»;
- «Сохранено на устройстве. Отправим после восстановления соединения»;
- «Не удалось подтвердить отправку. Автоматически не повторяем, чтобы не создать
  дубль»;
- «Текст отправлен; скриншот ожидает повторной отправки»;
- «Вход через Яндекс временно недоступен. Можно войти по email».

Запрещены без terminal proof:

- «Отправлено» после одного `fetch`;
- «Данные потеряны» только из-за отказа YDB analytics;
- «Яндекс не работает» по результату двух конкретных API Gateway probes;
- «Соединение отсутствует», если core direct route доступен;
- единый зелёный success для частично завершённой составной операции.

## 7. Контракт диагностической страницы

Диагностика должна показывать технически точные классы:

| Текущее короткое название | Требуемое значение |
|---|---|
| «Второй маршрут» | «Резервный маршрут через Yandex» |
| «Данные вторым путём» | «Данные через Yandex relay» |
| «Контрольный канал» | «Служебный канал YDB» + пояснение, что он не подтверждает сохранение основных действий |
| «Устойчивый вход/данные» | итоговый product route после выбора direct/relay |

Итог классифицируется как минимум так:

- `ALL_AVAILABLE`;
- `CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED`;
- `CORE_AVAILABLE_RELAY_DIRECT_DEGRADED`;
- `CONTROL_ONLY_DEGRADED`;
- `CORE_UNCONFIRMED`;
- `BOTH_CLIENT_ROUTES_UNAVAILABLE`.

Для наблюдения участника правильный итог:

> Основные функции доступны напрямую. Резервный маршрут и служебный канал
> Yandex с этого устройства сейчас не отвечают. Действия, подтверждённые сайтом,
> не требуется отправлять повторно.

Receipt остаётся PII-free и содержит capability states, final selected route,
время, build/revision и код проверки. Он не содержит email, токены, OTP, body
пользовательской обратной связи или bearer URL.

## 8. Обязательная тестовая матрица

| Scenario | Основные assertions |
|---|---|
| `connectivity.yandex_partial_outage_truth` | relay/control fail, direct/core work, точная degraded copy без global-outage claim |
| `transport.client_yandex_relay_unreachable` | issue/verify/write выбирают direct до dispatch; opposite route и дубли отсутствуют |
| `transport.both_client_routes_unreachable` | selected-once dispatch=0; no false success; local/static UX остаётся |
| `transport.supabase_upstream_unavailable` | оба пути degraded; relay не выдаётся за recovered |
| `personalization.ydb_projection_outage` | primary strong action committed; UX success; analytics outbox pending |
| `personalization.outbox_reconnect_exactly_once` | reload/reconnect; один durable action и одна projection |
| `focus.feedback.partial_component_delivery` | text/score committed, screenshot pending; truthful component copy |
| `auth.yandex_oauth_unavailable_email_fallback` | email login доступен; callback reconciliation; no generic auth outage |
| `email.postbox_unavailable_durable_outbox` | outbox retained, no false «sent», bounded retry/no duplicate |
| `inbound.yandex_pipeline_unavailable_replay` | original retained in SpaceWeb, cursor/replay/DLQ без дублей |
| `diagnostics.yandex_dependency_labels` | labels, summary class and receipt stable in browser/Android/iOS |

Browser tests покрывают полную deterministic fault matrix. Android/iOS нужны для
критических user journeys и диагностической/feedback коммуникации; desktop
mobile viewport не заменяет simulator acceptance.

## 9. Release gates

Release является **NO-GO**, если выполняется хотя бы одно условие:

- принятый strong action может исчезнуть при YDB/Yandex sidecar outage;
- пользователь видит «отправлено», хотя есть только local/queued attempt;
- отсутствует stable idempotency key и durable retry для Yandex-dependent
  server send/projection;
- optional telemetry меняет health core route или outcome продуктовой операции;
- составная операция скрывает partial failure;
- ambiguous selected-once автоматически повторяется через другой route/provider;
- diagnostic copy смешивает relay, YDB, OAuth, Postbox и весь Yandex в один
  статус;
- нет terminal browser/mobile evidence для изменённой критической capability;
- backlog/outbox может истечь или переполниться без terminal disposition и
  наблюдаемого сигнала;
- last-good release/profile projection уничтожается при сетевом отказе.

Single-route Supabase failover acceptance уже закрыт. Остальные строки матрицы
становятся blocking одновременно с реализацией соответствующей capability и до
её production rollout.

## 10. Evidence и наблюдаемость

Sanitized evidence для каждой операции содержит:

```text
capability
dependency_class
operation_name / semantics
client_event_id_hash
primary_store_state
component_receipts
selected_route
provider/projection_state
attempt_count / next_retry_at
failure_phase / failure_class
backlog_age
user_message_class
redaction_status
```

Ключевые SLI:

- accepted primary actions with missing current state — 0;
- false-success UI count — 0;
- duplicate durable effects — 0;
- YDB projection backlog age/count;
- email outbox oldest queued/ambiguous age;
- inbound oldest unprocessed UID;
- local outbox expired/evicted without disposition — 0;
- direct/relay route selection and fallback rate by capability;
- `CORE_AVAILABLE_*_DEGRADED` field frequency by platform/network class;
- component partial-failure/recovery rate.

## 11. План реализации

### P0 — contract и коммуникация

- обновить release plan, ownership ADR, personalization blueprint, focus-group
  brief, autotest strategy и scenario registry;
- заменить неоднозначные labels/summary диагностической страницы;
- ввести единый acknowledgement vocabulary и component receipts.

### P1 — аудит runtime call sites

Для каждого browser/server обращения к Yandex определить capability, SOR,
operation semantics, idempotency, acknowledgement и recovery. Любой вызов без
классификации блокирует rollout затронутой функции.

### P2 — durable recovery

- разделить primary commit и YDB projection;
- проверить/добавить серверные outbox для analytics, Postbox и inbound
  processing;
- завершить local-first outbox/reconciliation для персонализации и feedback;
- добавить partial component state для screenshot feedback.

### P3 — executable reliability matrix

- browser deterministic faults;
- Android/iOS critical samples;
- no-mail both-routes-down и upstream-outage scenarios;
- reconnect/reload/exactly-once proof;
- sanitized aggregate artifacts.

### P4 — production operations

- backlog/oldest-item alerts;
- operator replay/reconciliation runbooks;
- periodic field connectivity canary;
- release ledger с terminal disposition всех обязательных signals.

## 12. Текущий статус

| Область | Статус на 2026-08-03 |
|---|---|
| Direct unavailable → relay, Android/iOS OTP | terminal PASS |
| Relay unavailable → direct, Android/iOS OTP | terminal PASS |
| Полевое наблюдение direct OK / Yandex relay+control unavailable | зафиксировано как valid degraded client-path evidence |
| Both client routes unavailable | planned, no-mail |
| Shared Supabase upstream unavailable | planned, no-mail |
| YDB projection outage + durable recovery | contract закреплён; implementation audit/test требуется |
| Focus feedback partial component delivery | contract закреплён; implementation/test требуется |
| Yandex OAuth fallback | contract закреплён; acceptance требуется |
| Postbox provider outage | contract закреплён; server outbox/reconciliation acceptance требуется |
| Inbound Yandex pipeline outage/replay | ownership уже предусматривает retained mailbox; executable acceptance требуется |

Этот документ не объявляет ещё не проверенные строки готовыми. Он задаёт общую
границу, чтобы каждая следующая комплексная доработка реализовывала и доказывала
надёжность вместе с функцией, а не добавляла её постфактум после пользовательской
потери данных.
