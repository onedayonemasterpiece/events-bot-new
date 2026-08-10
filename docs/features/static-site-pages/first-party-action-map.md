First-party карта действий: интеграция с персонализацией, zero-cost OFF и компактный контур данных

Статус: проект канонического TO-BE product/architecture contract для обсуждения и последующего помещения в events-bot-new.

Дата: 9 августа 2026 года.

Предлагаемый канонический путь: docs/features/static-site-pages/first-party-action-map.md.

Область решения: статический сайт событий, first-party продуктовая аналитика, персонализация, Product Atlas, Resource Graph и Penpot-проекции.

1. Решение

Собственная карта действий должна быть напрямую связана с системой персонализации на уровне контекста показа, но не должна автоматически становиться источником пользовательского профиля.

Правильная связь выглядит так:

personalization presentation receipt
  → что, где, в какой позиции и какой версией модели было показано

component-aware action-map capture
  → что пользователь реально увидел и сделал в конкретной UI-зоне

aggregate analysis
  → отделение проблемы ранжирования от проблемы представления/аффорданса

reviewed evidence
  → Product Atlas finding / decision
  → при необходимости отдельное изменение signal policy или модели

Неправильная связь:

координата / dead tap / отсутствие клика / hover
  → немедленно изменить профиль интересов пользователя

Карта действий является временным диагностическим слоем. Персонализация остаётся отдельной продуктовой функцией с собственными activation rules, strong-action ledger, profile materialization и model governance.

1.1. Главные архитектурные инварианты

OFF означает нулевую добавочную стоимость в браузере. В выключенной сборке нет action-map JavaScript, загрузчика, observers, listeners, таймеров, запросов конфигурации, IndexedDB-операций, action-map payload и специальных HTML-атрибутов.

Включение и выключение выполняется через статическую сборку и публикацию, а не через постоянно работающий remote-config loader.

Карта действий читает только безопасный presentation_receipt_id и его аналитическую проекцию. Она не читает raw profile, interest vector, private history или score breakdown.

Диагностические события карты действий не активируют персонализацию, не создают profile facets и не меняют ранжирование в текущей сессии.

Like, hide, favorite/save, calendar, share, CTA и другие сильные действия существуют независимо от карты и считаются из authoritative product state; browser telemetry не подменяет product receipt.

Сырые координаты страницы, mouse trails, scroll stream, DOM snapshot, тексты, формы и поисковые запросы не собираются.

Точка действия хранится только как coarse local bin внутри versioned component/semantic zone и только в зарегистрированной диагностической кампании.

Сырые диагностические summaries короткоживущие; долгоживущими становятся только агрегаты и reviewed evidence packages без пользовательской идентичности.

Причинный вывод о пользе персонализации допускается только при зарегистрированном randomized/holdout contract. Обычная карта действий даёт diagnostic/assisted evidence, а не causal uplift.

Product Atlas и Penpot никогда не получают raw stream. В них попадает только проверенный evidence package по конкретному вопросу и решению.

2. Основание и оценка исследований

Исследовательские материалы правильно зафиксировали главное направление: единицей анализа должна быть не пиксель страницы и не полная запись сессии, а цепочка:

release/page version
→ archetype/layout
→ component contract/version/state
→ instance/placement
→ semantic zone
→ exposure
→ action attempt
→ expected effect
→ observed effect
→ retry/performance/layout context

Это соответствует текущему индексу исследований, который прямо помечает материалы как research input, а не готовый implementation contract.

Однако полное исследование нельзя принимать дословно. В нём есть несколько чрезмерных или нерелевантных для продукта выводов:

клик назван практически однозначным «подтверждённым намерением», хотя он подтверждает действие, но не его мотивацию, удовлетворённость или отсутствие промаха;

rage click, dead click, hesitation и form refill местами объявлены постоянными обязательными сигналами с универсальными порогами;

приводятся сценарии seat maps, покупки билетов, checkout и upsell, которых нет в текущем контракте сайта;

MutationObserver и сетевые/DOM-изменения предлагаются как общий способ доказать эффект действия, хотя для компактной системы точнее и дешевле явный expected-effect contract компонента;

встречаются психологические интерпретации dwell/hesitation, которые краткий вывод самого исследования уже корректно ограничивает.

Поэтому для implementation contract принимаются следующие части исследований:

denominator через подтверждённую exposure;

component/zone ownership вместо page coordinates;

explicit expected/observed effect;

разделение UX-фрикции и технической latency/layout instability;

отказ от mouse trails, raw scroll и session replay;

запрет автоматической интерпретации pointer dwell как внимания или затруднения;

campaign-only режим для локальной геометрии и спорных сигналов.

Не принимаются без отдельного product decision:

универсальные rage/dead/hesitation thresholds;

постоянный глобальный MutationObserver;

постоянное наблюдение за hover/dwell;

сбор исправлений форм, которых нет в утверждённом scope;

любая эвристика, автоматически создающая UI issue или профильный сигнал.

3. Связанность систем

3.1. Четыре независимых, но связанных контура

Контур

Каноническая ответственность

Связь с картой действий

Product operation

Выполненное strong action и текущее пользовательское состояние

Даёт authoritative receipt; карта не дублирует истину

Personalization

Activation, профиль, model/rule versions, выдача и served_list_id

Даёт read-only presentation context

Product analytics

Consent, compact facts, denominators, TTL, aggregation, cost

Принимает bounded action-map summaries

Design/Product evidence

Component identity, Product Atlas finding, решение, Penpot evidence

Получает только reviewed aggregate package

3.2. Прямая связь с персонализацией

Для каждой выдачи персонализация уже проектирует набор наблюдаемости: served_list_id, surface_id, catalog/model/profile versions, experiment variant, итоговые event IDs и ranks, exploration/rescue markers.

Карта действий должна использовать эту выдачу как presentation receipt:

PresentationReceipt
  presentation_receipt_id
  served_list_id
  surface_id
  presentation_mode
  model_version
  profile_revision
  experiment_id / variant
  catalog_snapshot_hash
  item position / rank / placement
  exploration / rescue marker

Browser не повторяет весь receipt в каждом событии. Он передаёт:

один opaque presentation_receipt_id на view;

компактные slot_id/rank для component instances;

action-map summary, ссылающийся на receipt.

На ingest-стороне выполняется разрешённое обогащение из санитизированной аналитической проекции, а не чтение raw user profile.

3.3. Односторонний контракт

personalization → action map: разрешено
action map → current profile: запрещено

Карта действий может показать, что:

высокоранговые персонализированные карточки не достигают viewport;

карточки видны, но зона открытия непонятна;

отдельная кнопка favorite конфликтует с кликом по карточке;

exploration-карточки имеют нормальную экспозицию, но слабый value action rate;

повторные попытки вызваны latency, а не ранжированием;

fixed/sticky или rail-composition мешает доступу к персональной выдаче.

Но она не может сама решить, что пользователь «не любит тему», потому что:

не нажал карточку;

быстро проскроллил;

нажал рядом;

долго держал мышь;

повторно нажал кнопку;

попал в dead-action heuristic.

3.4. Почему map-сигналы нельзя напрямую подавать в профиль

Это создало бы четыре системных дефекта.

Нестационарность. Профиль менялся бы в дни активной кампании и переставал меняться после её выключения.

Selection bias. В профиль попадали бы данные только sampled пользователей, страниц и компонентов.

Feedback loop. Ранжирование определяет показ, показ определяет map-сигнал, map-сигнал усиливает тот же профиль, а оценка снова выполняется по кликам.

Невоспроизводимость. Одинаковый профиль и каталог могли бы дать разные результаты только из-за конфигурации диагностической кампании, что противоречит требованию воспроизводимого ranking contract.

Если map-исследование докажет полезность нового сигнала, его продвижение выполняется отдельно:

ActionMap finding
→ signal-policy proposal
→ privacy/meaning review
→ offline benchmark
→ randomized A/B/holdout
→ versioned signal/model contract
→ отдельный core event, независимый от карты

4. Разделение телеметрии

4.1. Layer A — authoritative product actions

Существует всегда как часть соответствующей функции, независимо от action-map campaign:

like / unlike;

not_interested / undo;

favorite/save;

calendar add;

share/copy при принятой семантике;

CTA intent и подтверждённый terminal outcome;

подтверждённая регистрация/посещение, когда появится доверенный источник.

Путь:

UI intent
→ idempotent product command
→ durable acknowledgement
→ authoritative state/fact
→ async analytics projection
→ profile materializer по отдельной политике

Карта действий может сослаться на authoritative_receipt_id, но не создаёт второй факт.

4.2. Layer B — compact core analytics

По отдельному product_analytics consent:

page/view context;

semantic exposure там, где она нужна как denominator;

semantic action/outcome;

Web Vitals и delivery quality в принятом sampling;

bounded session summary.

Этот слой не обязан содержать точки карты.

4.3. Layer C — temporary action-map capture

Только при одновременном выполнении условий:

active build
AND eligible route
AND active unexpired campaign
AND product_analytics consent
AND deterministic sample hit
AND supported schema/component binding

Дополнительные данные:

component/zone-local coarse bin;

mapped/unmapped action classification;

repeat-attempt bucket;

expected/observed effect class;

optional latency/layout context;

exact render/presentation reference;

campaign data-quality counters.

4.4. Layer D — reviewed evidence

После закрытия кампании:

агрегаты;

одна или несколько санитизированных representative render fixtures;

page/component action-map images;

denominator и coverage;

ограничения;

finding;

варианты решения;

owner decision;

follow-up measurement.

5. Zero-cost OFF contract

5.1. Что означает OFF

ACTION_MAP_BUILD=off означает нулевую добавочную стоимость action-map, а не отключение всех функций сайта, персонализации или общей аналитики.

В итоговом production output отсутствуют:

action-map JavaScript chunk;

static import, dynamic import reference и modulepreload этого chunk;

inline bootstrap action-map;

remote campaign/config fetch;

event listeners action-map;

IntersectionObserver, MutationObserver, PerformanceObserver action-map;

action-map timers, idle callbacks и background tasks;

открытие/создание action-map IndexedDB store;

action-map network requests и payload fields;

action-map-only data attributes и render dictionaries;

action-map storage cleanup code, если оно не является общим TTL-механизмом analytics outbox;

backend read, необходимый обычной навигации.

Обычная статическая страница не проверяет, «не включили ли карту»: проверять нечего, потому что код отсутствует.

5.2. Почему remote toggle не подходит

Постоянный remote-config loader означал бы даже в выключенном состоянии:

дополнительные bytes;

исполнение JavaScript;

чтение consent/config;

сетевой запрос или cache validation;

потенциальный failure path.

Это нарушает требование нулевой нагрузки. Поэтому toggle является release/build parameter.

Astro использует build-time environment values, а условный module entry может полностью отсутствовать в output. В активной сборке более тяжёлый модуль допустимо загружать через import() только после локальных gates.

5.3. Включение кампании

approved campaign manifest in Git
→ active build for exact routes
→ static publish
→ eligible browser checks consent + deterministic sample
→ dynamic import capture module

В активной сборке route-scoped bootstrap:

находится только на allowlisted page families;

не импортирует capture module до consent/sample gate;

содержит встроенные campaign_id, starts_at, expires_at, schema hash и limits;

не опрашивает сервер для выяснения статуса.

5.4. Выключение кампании

campaign expiry or owner stop
→ ingest rejects new/expired campaign packets
→ off build is published
→ CDN/PWA update
→ new page loads have zero action-map code

Встроенный expires_at нужен для старых открытых вкладок и закэшированных active builds. После expiry модуль:

отсоединяет observers/listeners;

не создаёт новые summaries;

удаляет/отбрасывает unsent map records по generic TTL policy;

не пытается автоматически продлить кампанию.

5.5. Честное ограничение

Одновременно гарантировать:

мгновенный удалённый kill для любой уже открытой/закэшированной вкладки;

нулевой loader/config cost в OFF-build

невозможно.

Принятое предпочтение — zero-cost OFF. Emergency kill обеспечивается server-side rejection и коротким embedded expiry; полное удаление browser code — следующей статической публикацией.

6. Runtime-архитектура активной кампании

6.1. Принцип «не дублировать уже существующий сигнал»

Action-map module должен подписываться на существующие semantic action/outcome hooks и presentation receipts, а не устанавливать параллельную бизнес-логику.

Пример:

FavoriteButton command succeeded
  → product receipt: favorite_added
  → personalization strong-action path
  → analytics projection
  → action map adds zone/bin/performance context to campaign summary

6.2. Event capture

Использовать:

один delegated pointer/click listener на campaign root;

PointerEvent.pointerType для mouse | touch | pen;

не пытаться отличать mouse от trackpad: web-platform не даёт надёжного универсального признака, поэтому это одна desktop-pointer modality;

не собирать persistentDeviceId, pressure, tilt, twist, contact geometry и другие sensor-like pointer properties;

keyboard activation как semantic action без coordinate bin;

explicit component action hooks для expected effect;

один shared IntersectionObserver на allowlisted semantic zones, только если denominator ещё не предоставлен core exposure registry;

Page Visibility только для корректного завершения exposure windows;

существующий sampled performance collector, где возможно.

Не использовать по умолчанию:

pointermove, mousemove, touchmove;

scroll listener;

global DOM MutationObserver;

постоянный hover tracker;

full event sequence;

polling;

per-component listeners.

6.3. Dead/no-effect detection

Вместо универсального «DOM не изменился за N секунд» Component Contract описывает ожидаемый эффект:

actions:
  open_event:
    interaction_mode: single_shot
    expected_effect: route_change
  favorite:
    interaction_mode: toggle
    expected_effect: authoritative_state_ack
  rail_next:
    interaction_mode: repeatable
    expected_effect: rail_position_change
  external_cta:
    interaction_mode: single_shot
    expected_effect: external_navigation_attempt

Клиент фиксирует наблюдаемое состояние:

effect_observed;

effect_not_observed;

effect_unknown;

command_rejected;

navigation_interrupted;

technical_error.

Термин dead click используется только как human-readable finding после анализа, но не как первичный факт.

6.4. Repeat/rage detection

Первичный факт:

repeat_attempt_count_bucket = 1 | 2 | 3-4 | 5+

Интерпретация зависит от interaction_mode:

single_shot: повтор без эффекта — сильный диагностический кандидат;

toggle: возможна ошибка или отмена;

repeatable: повтор может быть нормой;

carousel/stepper: глобальный rage threshold неприменим;

drag/hold: последовательность pointer events имеет другую семантику.

Слово rage не записывается как факт и не создаёт issue автоматически.

6.5. Локальная геометрия

Для состоявшегося pointer action:

u = (clientX - zone.left) / zone.width
v = (clientY - zone.top) / zone.height

Сразу в браузере координата переводится в coarse bin, например 8×8:

bin_x = floor(clamp(u, 0, 0.999) × 8)
bin_y = floor(clamp(v, 0, 0.999) × 8)

На сервер не передаются:

pageX/pageY;

screenX/screenY;

точные float coordinates;

траектория;

абсолютная позиция документа.

Точность 8×8 является стартовой. Увеличение допускается только отдельной кампанией с решением, которому действительно нужна более точная геометрия.

7. Component Contract и runtime identity

7.1. Расширение контракта

В canonical Component Contract следует добавить observability binding:

component_id: announcements.event-card
contract_version: 5.0.0
state_key: portrait.compact.default
runtime_binding:
  astro: EventCard.astro

observability:
  action_map_eligible: true
  owner_boundary: component
  zones:
    media:
      role: content_open_target
      map_eligible: true
      allowed_actions: [open_event]
    body:
      role: content_summary
      map_eligible: true
      allowed_actions: [open_event]
    favorite:
      role: explicit_action
      map_eligible: true
      allowed_actions: [favorite_toggle]
  actions:
    open_event:
      interaction_mode: single_shot
      expected_effect: route_change
    favorite_toggle:
      interaction_mode: toggle
      expected_effect: authoritative_state_ack

7.2. Identity hierarchy

page_archetype_id
→ layout_contract_id
→ component_id + contract_version + state_key
→ component_instance_id
→ semantic_zone_id
→ semantic_action_id

CSS class, DOM path, visible text, Astro filename и Penpot layer name не являются аналитической identity.

7.3. Compact runtime binding

В active build canonical IDs компилируются в короткий словарь:

17 → announcements.event-card@5
2  → media
4  → open_event

HTML может получить только active-build binding:

<article data-am-c="17" data-am-v="5" data-am-z="2" data-am-i="7">

В OFF-build эти атрибуты отсутствуют, если они не нужны другой принятой функции.

7.4. Переходный период до дефрагментации

Resource Graph находится в reconstruction phase, поэтому до promotion канонических семейств допускаются временные IDs:

as_is_contract_id
source_snapshot_sha
observed_state_key

Условия:

authority mode явно reconstructed;

данные разных AS-IS IDs не объединяются только по визуальному сходству;

переход к accepted component выполняется explicit mapping receipt;

исторические evidence packages сохраняют исходный ID/version;

map data не используется как основание для автоматического merge компонентов.

8. Render context без дублирования JSON

8.1. Static page

Для статической страницы обычно достаточно:

release_sha;

build_id;

page_family/page_archetype;

page_revision;

content_revision;

layout_contract_id;

viewport class;

locale/timezone class, если влияют на визуал.

8.2. Dynamic/search/personalized page

Вместо передачи полного ответа поиска на каждый tap создаётся один render_context_id:

render_context_id
  → sanitized render manifest / served-list summary
  → ordered object IDs + revisions
  → layout/state flags
  → presentation receipt

Action-map summary передаёт только ссылку.

8.3. Что можно хранить в render manifest

Allowlist:

canonical object IDs;

order/slot/rank;

content revisions;

component/state IDs;

image asset ID/crop contract;

allowlisted filter IDs;

sort/presentation mode;

experiment variant;

responsive/layout state;

expanded/collapsed/rail position enums.

Запрещено:

raw search text;

query string URL;

DOM/HTML/textContent;

form values;

arbitrary JSON response;

raw profile/features/vector;

user-facing explanation text;

cookies/tokens;

full score breakdown;

произвольные attributes.

8.4. Replay и архив релиза

Для воспроизводимого visual evidence один раз на release сохраняются immutable static build assets. Для кампании выбирается один или несколько санитизированных representative render manifests.

Не нужно долгосрочно архивировать персонализированную страницу каждого пользователя. Long-term evidence строится из:

archived release
+ representative sanitized render manifest
+ aggregate action bins
+ campaign metadata

9. Компактная схема данных

9.1. Logical ActionMapViewSummary

{
  "schema_version": 1,
  "campaign_id": "am-mobile-search-001",
  "view_id": "ephemeral-idempotency-id",
  "render_context_id": "rc_...",
  "presentation_receipt_id": "pr_...",
  "release_sha": "...",
  "page_family": "search_results",
  "page_revision": "...",
  "layout_id": "mobile-list-v3",
  "device_class": "mobile",
  "pointer_capability": "coarse",
  "personalization_mode": "personalized",
  "exposures": [],
  "actions": [],
  "quality": {
    "dropped_observations": 0,
    "unmapped_observations": 0,
    "truncated": false
  }
}

9.2. Exposure tuple

component_id/version/state
instance/slot/rank
zone_id
exposed=true
max_visibility_bucket
visible_time_bucket

Не передавать поток enter/exit. Browser агрегирует один summary:

first eligible exposure;

max visible ratio bucket;

total visible time bucket;

re-entry count bucket, только если нужен вопросу.

9.3. Action tuple

component/version/state
instance/slot/rank
zone/action
pointer_type
local_bin
count_bucket
expected_effect
observed_effect
repeat_bucket
latency_bucket optional
shift_affected optional
authoritative_receipt_ref optional

9.4. Что агрегируется в браузере

одинаковые zone/bin/action points складываются в histogram;

точные timestamps удаляются после построения относительных buckets;

sequence нужна только в коротком memory window для retry/effect correlation;

после summary исходные observations уничтожаются;

repeated action не создаёт N отдельных server rows;

unsupported/unmapped points считаются отдельным quality counter.

10. Экологичность и resource budgets

10.1. Общий принцип

Action map не получает собственный безлимитный firehose. Она потребляет существующий global weak-telemetry budget.

Действующий analytics contract задаёт:

не более 3 weak telemetry batches/session;

target payload <8 KiB, hard <16 KiB;

Supabase raw analytics rows: 0;

YDB raw weak telemetry только с TTL;

при pressure первыми отключаются низкоприоритетные weak facts.

10.2. Начальные дополнительные лимиты action map

Ресурс

Начальный лимит

OFF-build incremental transfer/execute/storage

0

Active campaign default sample

5% eligible views

Без нового approval

не более 10%

Default duration

72 часа

Hard campaign duration

7 суток, затем новая регистрация

Map batches

не более 2/session, внутри общего лимита 3

Map bytes

target <4 KiB/session, hard <8 KiB/session

Distinct action observations before aggregation

не более 64/view

Local geometry

default 8×8 bins

Exact raw point retention

0 server-side

Local unsent map TTL

24 часа или campaign expiry, что раньше

YDB raw summary TTL

7 суток

Long-lived artifact

только aggregate/evidence package

Это стартовые engineering guardrails, а не универсальные продуктовые нормы. Canary обязан проверить реальный размер и CPU; после проверки лимиты можно только уменьшить либо обоснованно пересмотреть отдельным решением.

10.3. Storage policy

Данные

Owner/store

Retention

Strong current state

Authoritative product/personalization store

По product contract

Strong action analytics projection

YDB aggregate/event projection

По analytics contract

Presentation receipt / served-list summary

Personalization service + bounded projection

Достаточно для reconciliation/model evaluation

Unsent action-map summary

Existing bounded browser analytics outbox

До 24 часов / expiry

Raw action-map summaries

Isolated YDB analytics namespace

TTL 7 суток

Campaign aggregates

YDB aggregate tables

90 дней или до решения + recovery window

Release archive

Object Storage, immutable by SHA

По release evidence policy

Representative render fixture

Object Storage, sanitized

Вместе с evidence package

Product Atlas/Penpot

Finding, aggregate image, provenance refs

Без raw actor/view IDs

10.4. IndexedDB

IndexedDB открывается только если:

active capture module уже импортирован;

есть разрешённый summary;

немедленная отправка невозможна или достигнут batch threshold.

Отдельная action-map database не нужна. Используется общий bounded analytics outbox с generic purpose, expires_at, priority и idempotency.

В OFF-build action-map не вызывает IndexedDB вообще.

10.5. Loss policy

Action-map данные являются expendable weak diagnostics:

strong product command никогда не ждёт их отправки;

при budget/network pressure они удаляются раньше core semantic facts;

потеря отражается в coverage/dropped_count, а не скрывается;

неполная кампания получает статус INSUFFICIENT_DATA, а не искусственный вывод;

отсутствие доставки карты не ухудшает UX и не меняет персонализацию.

11. Consent, purpose и конфликт текущих документов

11.1. Разные цели

Personalization activation запускается осмысленным действием по действующему TO-BE contract.

Product analytics/action map требует отдельного purpose-specific analytics consent.

Отсутствие analytics consent не запрещает пользователю применять like/hide и пользоваться персонализацией.

Наличие analytics consent не активирует персонализацию.

11.2. Обнаруженный конфликт

Текущий personalization document говорит, что до activation impression/open не отправляются, а analytics document допускает product analytics после отдельного consent.

Без уточнения возможны две несовместимые реализации.

Предлагаемая каноническая формулировка:

До personalization_started_at не отправляются personalization-scoped impression/open signals, не создаётся server interest profile и не выполняется profile mutation. Отдельная de-identified product analytics telemetry может собираться только по собственному product_analytics consent и не проецируется в персональный профиль.

11.3. Pre-activation action-map context

До activation допустимо только:

personalization_mode = inactive | contextual
profile_revision = null
presentation_receipt_id = static/contextual receipt
no subject/profile join

После activation и только при analytics consent:

personalization_mode = active
presentation_receipt_id = opaque receipt
model/experiment context available through sanitized projection
raw profile unavailable

12. Privacy-by-design

12.1. Запрещённые поля

account ID, email, телефон, ФИО;

cookies, tokens, auth headers;

full IP/User-Agent/referrer;

raw search query;

DOM/HTML/text;

form/input/contenteditable values;

exact pointer coordinates;

pointer trajectories;

persistentDeviceId, pressure, tilt, twist и иные pointer sensor characteristics;

key sequences;

raw profile, embeddings, facets, score breakdown;

arbitrary URLs/JSON;

screenshots в raw analytics stream.

12.2. Идентичность

Action-map raw summary использует:

campaign-scoped ephemeral view_id для idempotency;

без стабильного account/installation ID, если вопрос этого не требует;

opaque presentation receipt;

service-side restricted join только до aggregation.

Нельзя объявлять обычный hash «анонимностью». Low-entropy query, event ID или короткий identifier может быть восстановлен/сопоставлен. Защита строится на minimization, purpose isolation, short TTL, access control и aggregation.

12.3. Sensitive topics

Для event content, помеченного sensitive_topic:

действие не создаёт user interest facet;

action map не хранит long-lived user-linked event evidence;

event-level slices публикуются только при достаточной aggregate cohort;

при малой выборке используется component/surface-level aggregate;

exact hide остаётся product state, но не расширяется тематически.

12.4. Минимальная когорта

Визуальная карта или comparative slice не публикуется при слишком малом denominator. Начальный operational default:

minimum eligible exposed views per rendered slice = 20

Это не доказательство анонимности и не статистическая универсальная норма. Порог лишь предотвращает выводы по единичным сессиям; для product decision campaign manifest может требовать более высокий minimum sample.

13. Метрики без statistics hell

13.1. Правило кампании

Одна кампания имеет:

один MeasurementQuestion;

одно решение, которое может измениться;

не более шести primary metrics;

заранее заданные denominator, slices и stop condition;

запрет ad-hoc перебора десятков heatmap interpretations.

13.2. Базовый набор

Метрика

Формула

Что позволяет решить

Eligible exposure

views with eligible zone exposure

Был ли элемент фактически доступен пользователю

Action-given-exposure

exposed views with semantic action / eligible exposed views

Работает ли affordance/placement

Activation success

actions with expected authoritative/visual effect / attempted actions

Срабатывает ли действие

Repeat-without-effect

single-shot actions with repeat bucket ≥2 and no effect / single-shot attempts

Есть ли кандидат на failure/ambiguity

Technical-confounded share

actions with latency/error/shift context / attempted actions

Не принимаем ли performance problem за UX problem

Unmapped-action rate

unmapped allowlisted points / all captured points

Насколько полон component/zone contract

Для персонализации campaign-specific метрика может заменить одну из шести:

value_action_given_exposure
= exposed views with save/share/CTA/accepted open
  / eligible exposed views

Она сравнивается по registered variant/holdout и rank bands, а не по «красоте» heatmap.

13.3. Обязательная сегментация

Не смешивать:

mobile coarse touch и desktop mouse;

page archetypes/layout contracts;

component versions/state keys;

static/editorial/contextual/personalized presentation;

rank/slot bands;

experiment variants;

release/page revisions;

mapped и unresolved component identities.

13.4. Нельзя автоматически выводить

hot area = пользователю нравится
cold area = пользователю не нравится
hover/dwell = внимание
retries = гнев
no click = отрицательный интерес
tap near target = плохой размер target
personalized variant has more clicks = causal uplift

Допустимый формат вывода:

Fact
→ plausible interpretation
→ competing explanations
→ data limitations
→ decision / additional evidence needed

14. Как карта оценивает персонализацию

14.1. Вопросы, на которые она отвечает

Достигают ли карточки персональной ленты viewport?

Отличается ли action-given-exposure у персонализированной и статической выдачи в сопоставимых rank bands?

Понимает ли пользователь, что карточка открывается целиком?

Конфликтует ли зона favorite с основным tap target?

Замечаются ли exploration/rescue элементы или их оформление делает их «чужими»?

Не ухудшает ли responsive layout доступность high-ranked items?

Не вызваны ли повторные попытки технической latency или layout shift?

Совпадает ли served_list_summary с фактическим DOM/exposure?

14.2. Вопросы, на которые она не отвечает сама

почему пользователь выбрал мероприятие;

удовлетворён ли он рекомендацией;

посетил ли событие;

действительно ли профиль отражает устойчивый вкус;

улучшила ли персонализация долгосрочную ценность;

является ли отсутствие действия отрицательным сигналом.

Для них нужны authoritative outcomes, qualitative research, attendance evidence и/или randomized experiments.

14.3. Position bias

Click/tap distribution сильно зависит от позиции. Поэтому presentation receipt обязан различать:

algorithmic rank;

visual slot;

rail/list position;

viewport exposure;

editorial pinning;

exploration/rescue placement.

Нельзя сравнивать карточку на позиции 1 с карточкой на позиции 17 только по click rate.

MVP применяет rank bands, например:

1–3
4–8
9–16
17+

Точные bands регистрируются в campaign contract и не меняются после просмотра результата.

14.4. Instrumentation effect

Активная карта сама может ухудшить performance. Поэтому campaign quality включает:

active vs non-captured control по INP/LCP/CLS;

bundle/CPU/network delta;

capture errors/dropped summaries;

page behavior parity.

Если instrumentation ухудшает interaction performance, выводы о retries/latency считаются загрязнёнными.

15. Product Atlas, Penpot и Resource Graph

15.1. Не создавать новую страницу без доказанной необходимости

Действующий Product Atlas contract уже содержит:

40 — Findings, incidents and decisions;

50 — UI and design evidence.

Для первой версии отдельная страница «45 — Product analytics evidence» не нужна. Это породило бы лишнюю топологию и дублирование.

Размещение:

50 — UI and design evidence
  reviewed page/component action maps
  scope, denominator, release/model/component versions
  evidence package link

40 — Findings, incidents and decisions
  accepted finding
  competing explanations
  options
  owner decision
  follow-up campaign

15.2. Resource Graph

90–92 — Evidence / desktop/tablet/mobile: representative replay/render и page-level overlay;

93 — Evidence / interaction and accessibility: component-local maps, zone summaries, activation/effect evidence;

68 — UI gaps, comments and decisions: только после принятого finding, не после каждого hotspot.

Карта действий является runtime evidence, а не visual baseline и не автоматическим источником Component Contract.

15.3. Синхронизация

Product Atlas plugin:

не читает production DB;

не интерпретирует raw metrics;

получает immutable ProductAnalyticsEvidencePackage из конкретного analysis record;

обновляет Penpot только по явной команде Обновить Product Atlas;

не создаёт Problem Bubble, пока finding не принят;

сохраняет deep links в Resource Graph.

common-analytics остаётся источником общей методологии/исследований, но не прямым runtime input Product Atlas. Канонический product-specific analysis record должен находиться в events-bot-new.

15.4. Evidence package

evidence_id: pae-action-map-mobile-search-001
measurement_question: >-
  Понимают ли пользователи мобильной персонализированной выдачи,
  что media/body карточки открывают событие, не конфликтуя с favorite?
decision_use: keep-or-change-event-card-hit-areas

scope:
  campaign_id: am-mobile-search-001
  date_from: 2026-08-10
  date_to: 2026-08-13
  release_sha: ...
  page_archetype: search-results
  layout_contract: mobile-list-v3
  component_contract: announcements.event-card@5
  model_version: ...
  experiment_id: ...

quality:
  eligible_views: ...
  captured_views: ...
  delivery_coverage: ...
  unmapped_rate: ...
  performance_parity: PASS | FAIL | UNKNOWN

facts:
  - ...
limitations:
  - ...
finding:
  status: accepted | rejected | insufficient-data
  text: ...
options:
  - ...
decision:
  owner: ...
  outcome: ship | change | instrument-better | stop

artifacts:
  page_map: ...
  component_map: ...
  representative_render: ...
  aggregate_snapshot: ...
  methodology_receipt: ...

resource_links:
  product_atlas_ids: [...]
  resource_graph_ids: [...]

16. Campaign contract

campaign_id: am-mobile-search-001
status: approved
measurement_question: ...
decision_use: ...

build:
  enabled: true
  starts_at: 2026-08-10T00:00:00Z
  expires_at: 2026-08-13T00:00:00Z
  schema_sha256: ...

scope:
  routes: [/poisk]
  page_archetypes: [search-results]
  layouts: [mobile-list-v3]
  components:
    - announcements.event-card@5
  zones: [media, body, favorite]
  devices: [mobile]
  presentation_modes: [static, personalized]

sampling:
  deterministic_key: campaign_view
  rate: 0.05

signals:
  exposure: true
  semantic_action: true
  local_bin: 8x8
  expected_effect: true
  repeat_bucket: true
  latency_context: sampled
  layout_shift_context: sampled
  hover_dwell: false
  mouse_trail: false
  raw_scroll: false

limits:
  max_actions_per_view: 64
  target_session_bytes: 4096
  hard_session_bytes: 8192
  max_batches_per_session: 2

retention:
  local_ttl_hours: 24
  raw_ydb_ttl_days: 7

stop_conditions:
  - expiry
  - owner_stop
  - privacy_violation
  - performance_regression
  - budget_shedding
  - schema_mismatch

17. Release и test gates

17.1. OFF-build gates — обязательные

Build manifest не содержит action-map chunk/entry.

HTML не содержит action-map script/import/modulepreload.

HTML не содержит action-map-only attributes.

Playwright подтверждает 0 action-map requests.

Instrumented browser подтверждает 0 action-map listeners/observers/timers.

IndexedDB не создаёт action-map records/stores.

Network payload registry не содержит action-map event names.

Bundle-size diff action-map OFF относительно baseline: 0 incremental bytes, кроме явно объяснённого общего build metadata noise.

Обычная навигация не обращается к analytics DB/config.

Static page остаётся функциональной при полном запрете analytics route.

17.2. Active-build gates

Неверный/отсутствующий consent → capture module не импортируется и 0 optional writes.

Route вне allowlist → 0 import/request/write.

Sample miss → 0 capture module import.

Expired campaign → listeners/observers не запускаются.

Unknown component/version/zone → fail closed, quality counter без DOM/text fallback.

Strong action receipt reconciles with authoritative state; карта не создаёт второй action.

Action-map data не вызывает profile mutation и не меняет ranks.

PII/prohibited-field scanner: 0 violations.

Payload/batch/TTL limits соблюдены.

Retry/idempotency не создают duplicate summaries.

YDB unavailable → UX и product action остаются успешными.

Active vs control performance parity проходит guardrail.

served_list_id/DOM/render context reconciliation проходит либо slice исключается.

Sensitive-topic policy проходит negative tests.

Generated maps скрывают low-sample slices.

Service Worker/PWA canary доказывает: cached active build после embedded expiry не запускает capture, а после OFF-публикации новый navigation не получает action-map assets.

17.3. Performance acceptance

Начальные требования:

OFF-build: абсолютный 0 action-map runtime work;

active eligible route: action-map module загружается только после gates;

отсутствуют long tasks, порождённые capture module, в reference flow;

action processing выполняется bounded и не блокирует product command/navigation;

observer targets ограничены campaign allowlist;

no pointermove/raw scroll pipeline;

p75 INP/LCP/CLS active sample не ухудшается относительно non-captured control сверх заранее принятой measurement tolerance;

при сомнении campaign останавливается, а не повышает sampling.

18. Предлагаемый rollout

Phase 0 — contracts

принять этот product/architecture contract;

зарегистрировать schemas и campaign manifest;

расширить Component Contract observability fields;

устранить conflict analytics consent vs pre-activation personalization telemetry;

определить presentation receipt projection.

Phase 1 — OFF proof

Сначала реализовать пустой/off path и доказать:

feature exists in repository
but production OFF build has 0 bytes / 0 work / 0 writes

Без этого active capture не разрабатывать дальше.

Phase 2 — статический pilot

Три поверхности без обязательной зависимости от готового server profile:

мобильная поисковая/листинговая карточка;

горизонтальная полка;

event detail CTA/favorite.

Цель — доказать component mapping, local bins, expected effect, batching, TTL и evidence generation.

Phase 3 — personalization receipt

После готовности served_list_summary:

связать view с presentation receipt;

проверить served-vs-DOM;

сегментировать по presentation mode/model/experiment/rank band;

не включать profile write-back.

Phase 4 — randomized evaluation

Только зарегистрированный holdout позволяет оценивать uplift персонализации. Action map используется как объясняющее UI evidence рядом с authoritative value outcomes.

Phase 5 — Product Atlas / Resource Graph

сформировать один reviewed evidence package;

отобразить карту на Product Atlas page 50;

accepted finding/decision — на page 40;

component evidence — Resource Graph 93;

проверить explicit on-demand sync.

19. Изменения, необходимые в существующих документах

events-bot-new

docs/features/static-site-pages/analytics/README.md

добавить зарегистрированный action_map_diagnostic purpose/campaign contract;

уточнить запрет координат: запрещены raw/absolute coordinates и trajectories; разрешён только coarse component/zone-local bin в ограниченной кампании;

зафиксировать zero-cost OFF, общий batch budget и TTL;

добавить ActionMapViewSummary, evidence package и low-sample policy;

добавить OFF-build автотесты.

docs/features/static-site-pages/personalizaion/personalization-to-be.md

уточнить pre-activation wording: запрет относится к personalization-scoped telemetry/profile mutation, а отдельная product analytics возможна по своему consent;

оформить presentation_receipt_id как read-only observability bridge;

явно запретить map-capture → profile mutation;

добавить promotion workflow нового сигнала через benchmark/A-B/model version;

добавить action-map instrumentation-effect guardrail.

docs/architecture/personalization-data-ownership.md

добавить raw action-map summary → YDB TTL only;

presentation receipt/current profile owner не меняется;

Supabase raw map rows остаются 0;

browser direct YDB write остаётся запрещён;

long-lived evidence → aggregate Object Storage package.

docs/features/static-site-pages/release-plan.md

добавить Phase AM-0…AM-4;

OFF proof как первый mandatory gate;

active campaign не является обязательным условием публичного релиза: функция default-off;

включение capture без OFF-build proof, consent, TTL, budget и kill/expiry — NO-GO.

Product-model/analysis documents

определить ProductAnalyticsEvidencePackage для action-map campaign;

связать MeasurementQuestion → evidence → finding → decision → follow-up;

запретить автоматический finding из hotspot.

lovekgd-design-system

docs/component-contract-authority.md

добавить observability.zones/actions/expected_effect/interaction_mode;

задать active-build runtime binding и OFF omission;

определить migration mapping AS-IS → accepted contract.

docs/resource-graph-004.md

page maps на 90–92, component-local maps на 93;

evidence не меняет contract/promotion автоматически;

accepted UI gap создаётся только из reviewed finding.

docs/product-atlas-penpot-extension.md

использовать существующие pages 40 и 50, не создавать новую страницу в первой версии;

ingest только immutable reviewed evidence package;

explicit Обновить Product Atlas, без live DB connection/background refresh.

common-analytics

хранить общую methodology/schema guidance;

не делать его прямым input Product Atlas;

product-specific decision, campaign и evidence остаются в events-bot-new.

20. Итоговая архитектура

STATIC/PERSONALIZED RENDER
  release/page/layout/component identities
  + presentation_receipt_id
              │
              ├──────────────┐
              │              │
AUTHORITATIVE ACTIONS    ACTION MAP OFF
  product command         no code
  durable receipt         no request
  profile input policy    no storage
              │              │
              │         ACTION MAP ACTIVE BUILD
              │           consent + sample + scope
              │           delegated action capture
              │           exposure summary
              │           local coarse bins
              │           expected/observed effect
              │           compact in-memory aggregate
              │                    │
              └──────────────┬─────┘
                             │
                    idempotent analytics ingest
                             │
                       YDB raw TTL 7d
                             │
                      compact aggregation
                             │
             ProductAnalyticsEvidencePackage
                      │                 │
              Product Atlas        Resource Graph
                 40 / 50               90–93
                      │                 │
                      └──── owner decision ────┐
                                              │
                  UI/component change OR explicit signal/model proposal
                                              │
                           offline benchmark + randomized experiment

21. Финальный вердикт

Прямую увязку с персонализацией делать нужно, потому что без served_list_id, model/experiment version, rank/slot и presentation mode карта не сможет отличить качество алгоритма от качества UI.

Но это должна быть увязка наблюдаемости, а не автоматический канал обучения профиля.

Минимальный правильный продукт:

authoritative semantic actions
+ personalization presentation receipt
+ temporary component-local action map
+ client-side aggregation
+ short raw TTL
+ reviewed evidence package
+ explicit owner decision

Ключевое техническое решение для статического сайта:

OFF реализуется не условием внутри постоянно загруженного collector, а отсутствием collector в статической сборке.

Тем самым карта действий остаётся включаемым диагностическим инструментом, а не новым постоянным налогом на каждый просмотр страницы.

22. Источники и действующие контракты

Проектные документы

[Индекс исследований action map — events-bot-new](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/first-party-action-map-research/README.md)

[Полное исследование сигналов](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/first-party-action-map-research/first-party-action-map-signal-architecture.md)

[Краткий evidence-based вывод](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/first-party-action-map-research/minimum-sufficient-first-party-action-signals.md)

[Personalization TO-BE](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/personalizaion/personalization-to-be.md)

[Personalization data ownership](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/architecture/personalization-data-ownership.md)

[Unified site analytics contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/static-site-pages/analytics/README.md)

[Component Contract authority](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/main/docs/component-contract-authority.md)

[Resource Graph 004](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/main/docs/resource-graph-004.md)

[Product Atlas Penpot extension](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/main/docs/product-atlas-penpot-extension.md)

Web-platform основания

[Astro: environment variables](https://docs.astro.build/en/guides/environment-variables/)

[MDN: dynamic import()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/import)

[MDN: IndexedDB API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API)

[MDN: Intersection Observer API](https://developer.mozilla.org/en-US/docs/Web/API/Intersection_Observer_API)

[W3C: Pointer Events](https://www.w3.org/TR/pointerevents/)

[W3C: Event Timing API](https://www.w3.org/TR/event-timing/)