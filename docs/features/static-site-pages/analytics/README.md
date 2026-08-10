# Аналитика и продуктовая статистика статического сайта

> **Статус:** принятый сквозной TO-BE-контракт; реализация частичная.  
> **Дата среза:** 2026-08-04.  
> **Область:** публичный статический сайт, PWA, фокус-группа, авторизация, письма, персонализация, поиск, подборки, Hero Talk, клавиатурная навигация, медальоны, артефакты, волонтёрские заявки, SEO/GEO и эксплуатационные контуры.  
> **Source of truth:** версия этого документа в `main`. Ветки, PR body, временные отчёты и preview считаются WIP/evidence, пока решение не дошло до `main`.  
> **Не является:** разрешением немедленно включить сбор production-данных, юридическим заключением или заменой feature-specific контрактов.

## 0. Управление документацией

Для аналитики действует жёсткое правило против расползания требований:

1. Принятое TO-BE-решение должно быть перенесено в канонический документ в `main` без ожидания реализации runtime.
2. Ветка может содержать исследование, сравнение вариантов и implementation handoff, но не остаётся единственным местом принятого требования.
3. PR body и комментарии не являются долговременным source of truth.
4. Этот документ владеет общими определениями пользователей, сессий, экспозиций, конверсий, consent-классов, хранения, доставки, стоимости и качества данных.
5. Feature-документ владеет смыслом конкретной функции и ссылается сюда. Он может ужесточить требования, но не может молча переопределить общую метрику.
6. Для нового пользовательского поведения обязательный раздел `Статистика и критерии результата` входит в Definition of Done.

Связанные действующие документы:

- [`production-integration.md`](../../unsigned-personalization/production-integration.md) — thin runtime, transport и уже принятый R14 product readout;
- [`personalization-data-ownership.md`](../../../architecture/personalization-data-ownership.md) — владение Fly/Supabase/YDB/Object Storage;
- [`general-stats`](../../general-stats/README.md) — операторский суточный отчёт пайплайнов; он не заменяет продуктовую аналитику сайта;
- [`post-metrics`](../../post-metrics/README.md) — Telegram/VK source metrics и popularity;
- [`focus-group.md`](../focus-group.md) — обратная связь фокус-группы;
- [`smart-vector-search`](../smart-vector-search/README.md) — Search-specific contract;
- [`personalization`](../personalizaion/README.md) — product/model contract персонализации.

## 1. Какие решения должна давать статистика

Статистика нужна не для накопления кликов, а для ответа на пять классов вопросов.

### 1.1. Ценность продукта

- нашёл ли пользователь событие, ради которого открыл сервис;
- сколько карточек пришлось просмотреть до первого полезного результата;
- сделал ли он действие намерения: сохранить, добавить в календарь, поделиться, перейти к билету, регистрации, маршруту или волонтёрской заявке;
- возвращается ли он за новыми событиями.

### 1.2. Принятие функций

- заметили ли клавиатурную навигацию, Hero Talk, медальоны, артефакты, подборки, поиск и `/dlya-menya/`;
- начали ли пользоваться после подсказки;
- довели ли сценарий до полезного результата;
- какая функция помогает, а какая только создаёт интерфейсный шум.

### 1.3. Качество контента и выдачи

- какие события и типы страниц реально читают;
- какие позиции карточек доходят до пользователя;
- где много скрытий, возвратов, нулевых результатов или ошибочных CTA;
- даёт ли персонализация улучшение относительно статического fallback.

### 1.4. Надёжность

- дошло ли действие до system of record;
- работают ли direct/relay-маршруты, OTP, письма, outbox и внешние зависимости;
- не выдаёт ли интерфейс false success;
- не загрязняют ли статистику боты, preview и автотесты.

### 1.5. Экономичность

- сколько записей, RU, запросов, egress и байтов создаёт одна сессия;
- какую долю бесплатных лимитов съедает аналитика;
- можно ли получить тот же продуктовый ответ меньшим количеством данных.

## 2. Главные продуктовые метрики

### 2.1. North Star: полезное обнаружение события

`event_value_reached_rate`:

```text
уникальные активные actors, которые за день достигли хотя бы одного полезного результата
----------------------------------------------------------------------------------------
уникальные активные actors за день
```

Полезный результат — одно из:

- `engaged_event_detail`: пользователь действительно прочитал ключевую часть страницы события;
- `event_saved`;
- `calendar_saved`;
- `event_shared`;
- `ticket_or_registration_clicked`;
- `route_or_transport_clicked`;
- `volunteer_application_clicked`.

Отдельно считается более сильная метрика `event_intent_action_rate`, в которой engaged view без действия намерения не является конверсией.

### 2.2. Скорость нахождения интересного

- `cards_to_first_event_value_p50/p75/p95`;
- `cards_to_first_intent_action_p50/p75/p95`;
- доля actors, достигших полезного события не более чем за `30` уникальных карточек;
- время от начала сессии до первого полезного результата.

Это связывает аналитику с уже принятой целью персонализации: помочь найти интересное событие в пределах ограниченной выдачи, а не заставить пользователя бесконечно листать.

### 2.3. Возврат

- D1/D7/D30 return rate новых actors;
- WAU/MAU;
- доля повторных PWA standalone launches;
- возврат после сохранения события, Search, `/dlya-menya/`, Hero Talk и артефакта.

### 2.4. Guardrails

North Star не может улучшаться ценой:

- ухудшения LCP/INP/CLS;
- роста ошибок и потерь действий;
- роста скрытий и отмен;
- сужения разнообразия городов, тем и форматов;
- увеличения обязательной авторизации;
- нарушения consent/retention;
- превышения ресурсного бюджета.

## 3. Кого именно считаем

Слово «пользователь» слишком неоднозначно. В отчётах используются отдельные сущности.

| Сущность | Определение | Что можно утверждать |
|---|---|---|
| `browser_installation` | Один случайный first-party installation id в профиле браузера/PWA | Отдельный браузерный профиль, не человек |
| `anonymous_actor` | Псевдонимизированный installation id | Анонимный actor на одном браузерном профиле |
| `authenticated_account` | Псевдонимизированный `auth.uid()` | Уникальная учётная запись; можно дедуплицировать её устройства |
| `actor_day` | Actor, проявивший продуктовую активность в конкретные сутки | DAU на уровне actor/account |
| `session` | Период активности, новый после 30 минут бездействия | Сценарий использования, не человек |
| `pwa_installation` | Подтверждённая установка из существующего PWA telemetry contract | Установка PWA |
| `pwa_standalone_session` | Сессия в `display-mode=standalone` | Фактическое использование установленного приложения |

### 3.1. Обязательные показатели аудитории

- уникальные активные browser installations по дням;
- уникальные активные authenticated accounts по дням;
- новые и повторные actors;
- mobile / desktop / tablet;
- browser tab / PWA standalone;
- количество подтверждённых PWA installs;
- количество аккаунтов, которые хотя бы раз успешно авторизовались;
- количество фактически активных авторизованных аккаунтов в сутки;
- среднее, median, p75 и p95 сессий на actor;
- среднее, median, p75 и p95 действий на сессию;
- доля сессий без какого-либо полезного действия.

### 3.2. Чего нельзя утверждать

- Несвязанные anonymous installations нельзя называть уникальными людьми.
- Несколько устройств одного человека дедуплицируются только после входа в один account.
- Общее число TG/VK/site views нельзя называть уникальной аудиторией: это reach observations разных поверхностей.
- «Сейчас авторизованы» без live-presence сервиса не является корректной метрикой. Нужна `authenticated_active_accounts_daily`.

## 4. Классы данных и источник истины

### 4.1. Сильные продуктовые факты

Like, unlike, hide, restore, favorite, calendar save, отправленный feedback, регистрация участника, subscription и delivery receipt считаются из их authoritative product store. Браузерное событие не подменяет подтверждённую запись.

```text
UI intent
→ idempotent product command
→ durable acknowledgement
→ authoritative state/fact
→ async analytics projection
```

### 4.2. Слабая поведенческая телеметрия

Page view, card visibility, depth, hint exposure, Hero Talk exposure и Web Vitals не требуют отдельной строки на каждое микродействие. Браузер агрегирует их в bounded session summary.

### 4.3. Эксплуатационная телеметрия

Transport, email provider, limiter, YDB RU, outbox age, build и error metrics живут отдельно. Их можно связать с продуктовым эффектом по release/build/request id, но нельзя смешивать с clickstream в одну неуправляемую таблицу.

### 4.4. Данные фокус-группы

Свободный текст, screenshot и диагностические вложения находятся в защищённом feedback-контуре. В аналитике хранится только opaque `feedback_id`, тип, page/release revision, delivery status и operator workflow.

## 5. Privacy и consent

### 5.1. Запрещённые данные общего аналитического потока

- email, телефон, имя;
- OTP, JWT, refresh token, cookies и bearer preview URL;
- полный IP и полный User-Agent;
- полный referrer URL;
- raw Search text;
- текст feedback, содержимое письма и screenshot bytes;
- raw/absolute pointer coordinates, координаты свайпа, trajectories движения
  мыши и последовательность клавиш; исключение не распространяется на
  немедленно вычисленный coarse component/semantic-zone-local bin внутри
  зарегистрированной ограниченной `action_map_diagnostic` кампании;
- embedding, полный interest profile или prompt/response LLM;
- arbitrary DOM text и arbitrary JSON payload.

### 5.2. Consent-классы

| Класс | Примеры | Правило |
|---|---|---|
| `essential_operational` | false-success prevention, security, terminal delivery receipt, bounded error class | Без маркетингового профилирования и без стабильного analytics actor, минимальный retention |
| `product_analytics` | page/card exposure, depth, Hero Talk, keyboard cohort, Web Vitals | Только после принятого analytics consent; сайт полностью работает без него |
| `action_map_diagnostic` | временные component/zone-local action summaries для одного зарегистрированного MeasurementQuestion | Purpose внутри `product_analytics`: только при действующем `product_analytics` consent, approved unexpired campaign manifest и deterministic sample; не активирует и не меняет персонализацию |
| `personalization_activation` | durable interest/profile writes | Отдельный activation contract; page view/scroll/dwell не являются активацией |
| `research_focus` | page score, NPS, focus feedback | Отдельный контур фокус-группы и условия участия |
| `communications` | launch reminder, recommendations, transactional lifecycle | Purpose-specific consent/основание, не выводится из аналитики |

Yandex Metrica, PostHog, Umami Cloud или любой другой внешний tag относятся к `product_analytics`, а не к essential runtime.

## 6. Единый envelope события

```json
{
  "event_id": "01J...",
  "event_name": "event_card_opened",
  "event_schema_version": 1,
  "occurred_at": "2026-08-04T12:34:56.789Z",
  "received_at": "2026-08-04T12:34:57.012Z",
  "environment": "production",
  "test_run_id": null,
  "feature": "event_discovery",
  "release_sha": "...",
  "build_id": "...",
  "page_family": "date_listing",
  "page_revision": "...",
  "content_revision": "...",
  "feature_version": "...",
  "model_or_rule_version": null,
  "session_id": "...",
  "installation_id": "...",
  "account_id": null,
  "consent_state": "product_analytics_v1",
  "device_class": "desktop",
  "app_mode": "browser",
  "source_class": "telegram",
  "surface": "listing_card",
  "properties": {}
}
```

Требования:

- `event_id` создаётся один раз и является idempotency key;
- unknown fields отклоняются либо попадают в quarantine;
- `properties` состоит только из allowlisted enums, чисел, bool и коротких opaque ids;
- production web events без `release_sha`, `page_revision` и `page_family` не принимаются;
- test, preview, crawler и synthetic actors маркируются и не попадают в обычные dashboards;
- изменение смысла требует новой schema version, старое имя не переиспользуется.

## 7. Не raw clickstream, а компактные факты

Рекомендуемая модель хранения рассчитана на бесплатные лимиты и прозрачные запросы.

| Логический факт | Grain | Назначение | Raw/Pseudonymous retention |
|---|---|---|---:|
| `analytics_actor_day` | day × actor | DAU, device, app mode, auth state, new/returning | 180 дней |
| `analytics_session_summary` | session | page counts, card depth, actions, duration bucket, source | 90 дней |
| `analytics_page_actor_day` | day × actor × page family | unique viewers и conversions по типам страниц | 35 дней |
| `analytics_event_actor_day` | day × actor × event | exact daily unique event viewers/actions | 35 дней |
| `analytics_feature_actor_day` | day × actor × feature | exposed / engaged / converted flags | 90 дней |
| `analytics_action_fact` | action id | сильные действия либо projection authoritative receipts | 180 дней или feature policy |
| `analytics_delivery_fact` | provider attempt/message id | accepted/delivered/bounced/suppressed | 180 дней или email policy |
| `analytics_daily_metric` | day × metric × dimensions | долговременные отчёты без user ids | 25 месяцев |
| `analytics_data_quality_day` | day × source/schema | rejects, duplicates, lateness, missing revisions | 25 месяцев |

После materialization долгосрочные отчёты читают `analytics_daily_metric`, а не actor-level facts.

### 7.1. Browser-side aggregation

- один page view на реальный route visit;
- карточка считается exposed один раз на `session × surface × event` после фактической видимости;
- depth хранится как максимальный достигнутый bucket, а не серия scroll events;
- Hero Talk и hint exposure дедуплицируются;
- слабая телеметрия отправляется не чаще трёх bounded batches за сессию;
- сильные действия отправляются сразу своим product command и не ждут analytics batch;
- CTA/navigation никогда не ждут телеметрию.

### 7.2. Зарегистрированная `action_map_diagnostic` кампания

Канонический feature-specific contract: [First-party карта действий](../first-party-action-map.md).
Карта — временный диагностический слой, а не постоянный clickstream и не
источник профильных сигналов. Кампания имеет один зарегистрированный
`MeasurementQuestion`, один `decision_use`, не более шести primary metrics,
фиксированные denominator/slices/stop conditions и immutable manifest с
`campaign_id`, route/component/zone allowlist, schema hash, sampling,
`starts_at`, `expires_at`, limits и owner. Одновременно обязательны active
build, eligible route, unexpired manifest, `product_analytics` consent,
deterministic sample hit и поддерживаемый component binding.

Логическая единица доставки — компактный `ActionMapViewSummary`:

```text
campaign_id + campaign-scoped ephemeral view_id
render_context_id + opaque presentation_receipt_id
release/page/layout/device/personalization mode
aggregated exposure tuples
aggregated action tuples: component/version/state + instance/slot/rank
  + semantic zone/action + pointer type + coarse local bin
  + expected/observed effect + repeat/optional performance buckets
quality: dropped/unmapped/truncated counters
```

Browser сворачивает одинаковые observations в histogram/buckets и уничтожает
точные промежуточные coordinates/timestamps. На сервер не передаются
`pageX/pageY`, `screenX/screenY`, точные float coordinates, absolute document
position или trajectory. Разрешён только coarse local bin (default `8×8`),
вычисленный внутри versioned component/semantic zone; повышение точности требует
новой зарегистрированной кампании.

#### Zero-cost OFF, budget и TTL

`ACTION_MAP_BUILD=off` — default. OFF-build не содержит action-map chunk,
bootstrap/import/modulepreload, special HTML attributes, listeners, observers,
timers, remote config fetch, IndexedDB work, request или payload fields. Поэтому
OFF имеет `0` incremental transfer/execute/storage; включение и выключение —
build/publish decision, не постоянно загруженный remote toggle.

Активная карта потребляет общий лимит weak telemetry, а не создаёт новый
firehose: default sample `5%` eligible views (`>10%` только новым approval),
default duration `72h`, hard expiry `7d`, не более `64` observations/view,
не более `2` map batches/session внутри общего лимита `3`, target `<4 KiB` и
hard `<8 KiB` map bytes/session. Unsent local summary живёт не более `24h` или
до campaign expiry; raw summary допускается только в изолированном YDB
namespace с TTL `7d`; Supabase raw map rows остаются `0`. При pressure map
summaries удаляются раньше core semantic facts.

#### Reviewed evidence и low-sample policy

Долгоживущий результат — immutable `ProductAnalyticsEvidencePackage`, а не raw
stream: campaign/release/model/component scope, denominator и coverage,
performance parity, facts, limitations, reviewed finding, options, owner
decision, follow-up, aggregate snapshot, sanitized representative render и
page/component maps. Связь обязательна:
`MeasurementQuestion → evidence → finding → decision → follow-up`.
Hotspot сам по себе не создаёт finding, UI issue или profile signal.

Visual/comparative slice не публикуется при малом denominator. Начальный
операционный минимум — `20` eligible exposed views на rendered slice; manifest
может требовать больше. Меньшая/неполная выборка получает
`INSUFFICIENT_DATA`, объединяется до безопасного component/surface aggregate
либо не публикуется, но никогда не превращается в автоматический вывод.

## 8. Базовая воронка страницы и карточек

### 8.1. Просмотры страниц

Обязательные метрики:

- page views и unique actors по `page_family`;
- mobile / desktop / PWA;
- new / returning;
- source/referrer class;
- p50/p75 active duration bucket;
- content-end reach;
- Web Vitals и client errors по release SHA.

Минимальные `page_family`:

```text
home
prelaunch
today_listing
tomorrow_listing
date_listing
weekend_listing
category_listing
collection_listing
popular_listing
search
personal_for_me
event_detail
festival
organization_or_club
favorites
hidden_recovery
profile
focus_hub
diagnostics
```

### 8.2. Глубина просмотра карточек

- число уникальных карточек, реально попавших в viewport;
- maximum rank reached: `1`, `5`, `10`, `20`, `30`, `50+`;
- p50/p75/p95 unique cards seen per session;
- доля сессий, где не увидена ни одна карточка;
- cards-to-first-open;
- cards-to-first-event-value;
- глубина отдельно по обычной, Search, collection и personalized выдаче.

### 8.3. Страница события

По каждому event id посуточно:

- card impressions;
- detail page views;
- unique detail viewers;
- engaged detail views;
- key facts reached;
- related section reached;
- page-end reached;
- save/calendar/share/CTA;
- conversion rates с правильными denominators;
- device, source, surface и release revision.

## 9. Действия намерения и social proof

### 9.1. Основные действия

```text
like / unlike
favorite_save / favorite_remove
calendar_save / calendar_remove
hide / restore
share_event
share_service
share_collection
copy_link
open_ticket
open_registration
open_route
open_transport
open_volunteer_application
open_source
```

Для каждого считаются:

- total actions/day;
- unique actors/day;
- actions per active actor;
- distribution `0`, `1`, `2`, `3–5`, `6+`;
- undo/cancel rate;
- action success, pending и terminal failure;
- page family, surface, placement, event, device и source.

### 9.2. First-party и внешние social metrics

- Site actions считаются из first-party authoritative state/receipts.
- Telegram/VK views/reactions/reposts продолжают жить в source metrics.
- Консолидированный event-level отчёт может показывать компоненты рядом, но не суммирует их как уникальных людей.
- `site_views + tg_views + vk_views` — суммарные reach observations, а не audience size.

### 9.3. Поделиться

Отдельно:

- поделились конкретным событием;
- поделились страницей сервиса/root;
- поделились подборкой/фестивалем;
- использовали native share, copy link или конкретный messenger;
- downstream open по share token/UTM, когда это возможно без идентификации получателя.

## 10. Клавиатурная навигация

### 10.1. Когорты

Для desktop sessions формируются взаимоисключающие/пересекающиеся признаки:

- `desktop_keyboard_eligible`: desktop non-editor surface, где command grammar доступна;
- `keyboard_hint_exposed`: явная подсказка реально видима не менее принятого visibility threshold;
- `keyboard_navigation_started`: command router принял navigation key вне input/editor;
- `keyboard_target_action`: карточка открыта/действие выполнено клавиатурой;
- `keyboard_artifact_found`: найден существующий keyboard-only артефакт.

Не записывается последовательность клавиш и текст, введённый в формы.

### 10.2. Обязательные показатели

- сколько distinct actors пользовались клавиатурной навигацией за день/неделю;
- сколько desktop actors ни разу не использовали её;
- сколько desktop actors увидели явную подсказку и не использовали её;
- общая adoption conversion:

```text
actors keyboard_navigation_started / actors desktop_keyboard_eligible
```

- hint-assisted conversion:

```text
actors hint_exposed AND keyboard_started / actors hint_exposed
```

- no-response after hint;
- keyboard start reliability;
- context recovery success;
- actions per keyboard session;
- cards-to-first-keyboard-action;
- artifact find rate среди keyboard users и среди hint-exposed users.

Показатель «не пользовался клавиатурой» считается на actor/day и actor/lifetime отдельно: отсутствие команды в одной сессии ещё не означает, что человек никогда не пользовался функцией.

## 11. Волонтёрские заявки и CTA

### 11.1. Волонтёрский контур

Посуточно и по exact application URL identity:

- label impressions;
- details opens;
- application CTA clicks;
- unique actors who clicked;
- event/festival/organization;
- surface and rank;
- current/expired/closed supply state;
- application-link failure.

Клик не называется поданной заявкой. `application_submitted` допустим только при подтверждённом callback/API от владельца формы.

### 11.2. Общая CTA-статистика

Все CTA используют один справочник `cta_kind`:

```text
ticket
registration
calendar
route
transport
volunteer
source
share
auth
launch_reminder
feedback
profile
collection
```

Для каждого: clicks/day, unique actors/day, conversion from eligible exposure, success/failure, event/page/surface/device/source.

## 12. Медальоны и артефакты — разные сущности

### 12.1. Медальоны организаций/площадок

- medallion impressions;
- medallion opens/clicks;
- переходы к событиям организации;
- последующие event opens/save/CTA;
- coverage событий и организаций медальонами;
- broken/fallback assets;
- uplift только через контролируемое сравнение, а не по простой корреляции.

### 12.2. Артефакты-пасхалки

- artifact eligible exposure;
- artifact hint exposure;
- artifact found;
- artifact detail open;
- collection open;
- found count distribution;
- find rate по exact artifact id и placement;
- first-artifact onboarding conversion;
- keyboard-only artifact отдельно.

Нельзя смешивать «нашёл артефакт» с интересом к типу события или автоматически использовать это как taste signal.

## 13. Hero Talk и Page-end Talk

### 13.1. Определение вовлечения

Impression не равен вовлечению. Actor считается вовлечённым, если выполнено хотя бы одно:

- дочитал первую смысловую единицу/chain checkpoint;
- явно продолжил/раскрыл chain;
- нажал CTA;
- совершил связанное feature action в той же сессии после chain.

### 13.2. Метрики home hero

- eligible actors;
- impression;
- engaged actors;
- chain completion;
- skip/dismiss point;
- CTA click;
- associated first-value action;
- repeat suppression and return behavior.

### 13.3. Метрики page end

Отдельный `placement=page_end`:

- сколько actors дошли до page end;
- сколько увидели Page-end Talk;
- сколько вовлеклись;
- какой context intent был показан;
- continuation/related/collection/club/artifact action;
- return to content;
- conversion by page family.

`hero_talk_assisted_conversion` — последовательностная метрика, а не доказательство причинности. Causal uplift требует зарегистрированного эксперимента.

## 14. `/dlya-menya/` и персонализация

### 14.1. Использование раздела «Для меня»

- unique actors/day, sessions/day;
- eligible vs auth/activation-gated;
- static/cold-start/personalized state;
- non-empty feed rate;
- cards seen;
- event opens, saves, hides, shares, CTA;
- return D1/D7;
- errors and stale projection state.

### 14.2. Качество персонализации

- доля sessions, где применён compatible local/profile projection;
- доля static fallback;
- profile freshness age class;
- first relevant/event-value rank;
- cards-to-first-value `<=30`;
- save/calendar/share/CTA per 100 impressions;
- hide/not-interested rate;
- diversity: categories, cities, organizers and event families;
- cold-start performance;
- profile resets/disables;
- anonymous→account merge success/conflicts;
- multi-device revision lag;
- pending action age.

### 14.3. Uplift

`personalization_uplift` вычисляется только на зарегистрированном holdout/A-B:

```text
conversion_personalized - conversion_static_fallback
```

Нельзя сравнивать добровольно открывших `/dlya-menya/` с общей аудиторией и объявлять разницу эффектом модели.

## 15. Умный поиск

- Search opens;
- auth gate shown;
- submitted queries;
- cache hit/cold path;
- results rendered;
- zero results;
- reformulation;
- result impressions;
- result opens/save/share/CTA;
- p50/p95 end-to-end latency;
- error stage;
- route, Edge revision, corpus revision, model/rule version;
- cold/cached canary health;
- result diversity and current-event coverage.

Raw query text не попадает в общий analytics flow. Допустимы query length bucket, language, intent class, filter class и keyed HMAC только в отдельном restricted audit contract.

## 16. Онбординг

- capability hint eligible/exposed;
- first use;
- success/undo;
- repeated hint suppression;
- first-value action;
- first artifact hint/found;
- time-to-first-value;
- feature use within 1/7 days after onboarding;
- abandonment by contextual step.

Общий `onboarding_completed` не является достаточной метрикой: стратегия contextual и utility-first. Измеряется освоение конкретных capabilities.

## 17. Подборки, гастрособытия, фестивали и редакционные страницы

Для каждой collection/editorial edition:

- page views and unique actors;
- item impressions/open/save/share/CTA;
- cards-to-first-value;
- empty/low-supply state views;
- freshness and nearest-event horizon;
- independent concept/family count;
- concentration by one organizer/series;
- geography diversity;
- review precision/recall on owner sample;
- publication/block/deferred state;
- content revision and edition period;
- return from dated edition to evergreen hub.

Pipeline PASS не является product success. Технические coverage/evaluated/deferred metrics показываются рядом, но отдельно от user funnel.

## 18. Фокус-группа

- invited/eligible participants;
- site opened before auth;
- feedback block exposed locked/unlocked;
- auth CTA conversion;
- page score by `page_family × page_revision`;
- service NPS by service revision;
- problem/suggestion/event-error/screenshot counts;
- anonymous/verified split в рамках принятой модели;
- delivery committed/pending/failed/recovered;
- time to triage/owner assignment/fix/verify;
- repeat issue and reopened issue;
- participant/device/page coverage;
- artifact progress and task completion, если это входит в конкретный focus contract.

NPS, page score и issue rate не смешиваются в один показатель.

## 19. Авторизация и письма

### 19.1. Авторизация

- auth start by method;
- OTP issue accepted/rejected/ambiguous;
- provider accepted;
- delivery terminal state;
- verify success/failure;
- full auth completion;
- abandonment by stage;
- direct/relay route;
- returning/new;
- session restore;
- logout;
- profile link/merge outcome.

### 19.2. Письма

По purpose/provider/day:

- planned/admitted;
- suppressed;
- provider attempted;
- provider accepted;
- delivered;
- deferred;
- bounced/rejected/complaint;
- duplicate prevented;
- ambiguous dispatch;
- retry/replay;
- time to provider accept and delivery;
- CTA click through purpose-specific redirect/UTM when allowed.

Open pixels не включаются по умолчанию: они неточны и создают лишний privacy surface. Для transactional/OTP важнее terminal delivery и последующее целевое действие.

## 20. Prelaunch-заглушки и PWA

Для каждой visual variant (`techno`, `techno-2`, `glass`, tile variants):

- variant views and unique actors;
- key content visible without scroll by viewport class — из browser QA, не только telemetry;
- launch-reminder form start/submit/failure/duplicate;
- CTA conversion;
- Web Vitals;
- reduced-motion correctness — CI/browser gate;
- source/QR/referral;
- return visits.

PWA:

- confirmed installs;
- install prompt eligible/shown/accepted/dismissed;
- standalone launches/day;
- active standalone actors;
- browser→PWA return;
- install→D1/D7 return;
- uninstall напрямую браузером надёжно не измеряется.

Существующий compact PWA telemetry остаётся отдельным реализованным slice и должен быть приведён к общим actor/release definitions.

## 21. SEO, GEO и привлечение

Источники отличаются от browser analytics:

- CDN/access logs;
- Yandex/Google webmaster/search data;
- Yandex Metrica как optional secondary source;
- sitemap/robots/structured-data checks;
- first-party referrer classes.

Метрики:

- organic landings by engine/page family;
- search impressions/clicks/CTR;
- indexable/indexed URLs;
- crawl/canonical/robots/sitemap errors;
- structured data validity and rich-result eligibility;
- external referrers, TG/VK/email/QR;
- landing→event-value conversion;
- verified generative/AI referrals only when referrer/evidence exists;
- reproducible GEO query audit, not invented universal «GEO score».

## 22. Дополнительные продуктовые показатели

Эвристически обязательны также:

### 22.1. Retention и привычка

- new/returning split;
- cohort retention after first save, first Search, first `/dlya-menya/`, first artifact and PWA install;
- days active per month;
- repeat event-value sessions;
- dormant actor reactivation.

### 22.2. Качество каталога глазами пользователя

- event page with missing time/place/price/CTA;
- ended/canceled event exposed as active;
- broken image/media;
- duplicate family exposed in one list;
- event hidden immediately after open;
- outbound link failure;
- report-an-error conversion and resolution.

### 22.3. География и доступность предложения

- actors and event-value by city/coast/east-region groups;
- supply vs demand by geography;
- out-of-city transport CTA;
- event distance/other-city preference only from explicit settings, never inferred from IP as truth;
- page/value conversion for small towns vs Kaliningrad.

### 22.4. Время

- day of week and hour of first visit/action;
- today/tomorrow/weekend usage;
- lead time between discovery and event start;
- same-day vs advance save/calendar/CTA;
- seasonal cohorts.

### 22.5. Ошибки и performance

- JS errors by release/page family;
- failed resource/media requests;
- offline/degraded sessions;
- p75 LCP/INP/CLS by device/page family;
- service worker version drift;
- telemetry delivery lag/reject/duplicate rate.

### 22.6. Privacy health

- consent acceptance/withdrawal by version;
- analytics-disabled active actors — только aggregate, без дальнейшего tracking;
- deletion/export requests;
- payload PII scanner violations;
- retention deletion success;
- foreign analytics tag availability/blocked coverage difference.

## 23. Формулы, denominators и атрибуция

Каждый dashboard обязан показывать denominator. Примеры:

```text
card_open_rate = unique actors who opened / unique actors exposed to card
share_rate = unique sharers / unique event detail viewers
cta_rate = unique CTA actors / unique eligible detail viewers
hero_engagement_rate = engaged actors / eligible actors who saw Hero Talk
page_end_talk_engagement = engaged / actors who actually reached page end and saw placement
for_me_adoption = actors who opened / eligible active actors
keyboard_hint_conversion = keyboard starters after hint / hint-exposed desktop actors
email_delivery_rate = delivered / provider accepted
```

Attribution levels:

1. **Direct:** действие по конкретной видимой CTA/карточке.
2. **Session sequence:** действие произошло после функции в той же сессии; обозначается `assisted`, не causal.
3. **Experiment:** только зарегистрированный randomized/holdout contract позволяет говорить об uplift.

## 24. Рекомендуемая бесплатная архитектура

### 24.1. Решение

```text
Browser AnalyticsClient
  -> memory/session accumulator
  -> bounded IndexedDB outbox
  -> direct Yandex API Gateway OR Supabase Edge blind bridge
  -> one idempotent analytics ingest
  -> isolated YDB analytics namespace/database with TTL
  -> daily compact aggregation job
  -> YDB daily aggregates + Object Storage Parquet/JSON snapshot
  -> DataLens owner dashboard / generated private static report
```

При этом:

- Fly SQLite остаётся canonical event/product operation store;
- Supabase Auth остаётся источником account/session identity в текущем контуре;
- strong user state читается из его принятого authoritative store, а не из browser telemetry;
- YDB analytics не становится competing personalization/profile SOR;
- Object Storage хранит только aggregate/archive artifacts;
- ordinary site navigation не читает аналитику и не зависит от неё.

Если будущая принятая архитектура перенесёт часть user-linked current state из Supabase в YDB, общая семантика метрик и daily facts не меняется.

### 24.2. Почему YDB для compact analytics

- уже есть Yandex infrastructure и service identities;
- serverless YDB поддерживает TTL и нативно подключается к DataLens;
- аналитика изолируется от Supabase Auth/profile quota;
- daily aggregate запросы предсказуемы;
- бесплатный пакет достаточен только при batching и отсутствии raw firehose.

После инцидента с RU любые auto-increase и unlimited scheduler запрещены. Нужны отдельная БД/namespace, hard throughput/budget, kill switch, idempotency и last-good dashboards.

### 24.3. Роль Supabase

Supabase не получает raw page-view/click stream.

Он продолжает хранить то, что требует Auth/RLS/current user state, а также может:

- выступать stateless blind bridge к тому же analytics ingest;
- отдавать service-only daily authoritative counts сильных действий;
- хранить короткие send-critical email/auth receipts по существующим контрактам.

Browser не читает analytics rows из Supabase. Analytics не должна расходовать существенную долю `500 MB` DB и `5 GB` uncached egress Free Plan.

### 24.4. Object Storage и Yandex Query

- ежедневные анонимные агрегаты экспортируются в compressed Parquet/JSON;
- долгий горизонт хранится дешевле без user identifiers;
- Yandex Query используется для редких ad-hoc запросов к архиву;
- static owner report может собираться GitHub Actions/Kaggle и публиковаться в private/noindex operator surface.

### 24.5. DataLens

DataLens — основной интерактивный dashboard для владельца:

- native YDB connection;
- только aggregate datasets;
- cache TTL не менее 300 секунд;
- selectors с ограниченным cardinality;
- один seat остаётся бесплатным; collaboration более чем одного пользователя уже платная, поэтому для дополнительных читателей предпочтителен generated static report.

### 24.6. Yandex Monitoring / Monium

Использовать для технических time-series: RU, request rate, latency, errors, queue age, provider health. Не использовать как единственное хранилище продуктовых actor/event facts.

## 25. Внешние бесплатные инструменты

| Вариант | Польза | Ограничения | Решение сейчас |
|---|---|---|---|
| **Yandex Metrica** | Pageviews, devices, traffic sources, goals, API | Может блокироваться; внешний tag; consent/legal; не знает authoritative product state | Optional secondary comparator, не SOR |
| **PostHog Cloud** | Funnels, cohorts, experiments; большой free tier | Third-party raw event flow, data residency/legal review, vendor duplication | Не подключать к production сейчас |
| **Umami self-hosted** | Open source, lightweight, basic page/event analytics | Нужны отдельные always-on compute + Postgres; дублирует собственный контур | Не разворачивать до реальной необходимости |
| **Только Supabase** | Простая SQL-модель | 500 MB DB, egress, конкуренция с Auth/profile, риск raw accumulation | Только authoritative strong state, не clickstream |
| **Raw firehose в YDB** | Простое append-only начало | RU/write explosion, cardinality, инцидентный риск | Запрещён |
| **Static JSON reports** | Почти нулевая стоимость, простая публикация | Нет ad-hoc actor-level analysis | Использовать для daily aggregates и handoff |

### 25.1. Ограниченный режим Yandex Metrica

При отдельном принятом решении:

- загружать только после analytics consent;
- Session Replay/Webvisor по умолчанию выключен;
- не отправлять user parameters, account id, email, Search text и secret URLs;
- использовать pageviews и не более небольшого набора top-level goals;
- сравнивать с first-party counts как coverage sanity check;
- отсутствие/блокировка Metrica не влияет на UX и не считается потерей authoritative action.

## 26. Бюджет без оплаты

Актуальные бесплатные лимиты проверяются перед rollout; они общие на billing account и могут уже расходоваться другими контурами.

Срез официальной документации на 2026-08-04:

- Yandex API Gateway: первые `100 000` запросов/месяц;
- Yandex Cloud Functions: `1 000 000` вызовов и `10 GB×hour`/месяц;
- YDB Serverless: первые `1 000 000 RU` и `1 GB` хранения/месяц;
- Object Storage: первый `1 GB`, `10 000` write/list и `100 000` read operations;
- Yandex Query: чтение менее `10 GB`/месяц;
- DataLens: один seat бесплатно;
- Supabase Free: `500 MB` database, `5 GB` uncached и `5 GB` cached egress, `500 000` Edge invocations.

### 26.1. Внутренние более строгие бюджеты

- не более `3` weak telemetry batches/session;
- payload target `<8 KiB`, hard `<16 KiB`;
- API Gateway analytics target `<50 000` requests/month;
- YDB analytics target `<250 000 RU/month`, warning `400 000`, shedding `600 000`; hard stop до исчерпания free package;
- analytics YDB storage target `<250 MB`, warning `<500 MB`;
- Supabase raw analytics rows: `0`;
- Supabase analytics egress: практически `0`, только bounded service aggregates/bridge;
- daily aggregate archive target `<100 MB/year` compressed;
- DataLens читает только aggregate tables, не actor facts.

Пороги должны учитывать фактический расход всего billing account. Если общий YDB free package уже занят, analytics снижает sampling/retention либо остаётся shadow — она не создаёт неожиданный счёт.

## 27. Деградация и loss policy

- Strong action никогда не зависит от analytics delivery.
- Weak analytics остаётся в browser outbox до 7 дней, затем может быть удалена.
- При pressure сначала отключаются Web Vitals samples, повторные exposures и низкоприоритетные weak facts.
- Не отключаются authoritative action receipts, email delivery и false-success diagnostics.
- YDB outage не переводит пользовательское действие в failed, если product SOR уже подтвердил его.
- Dashboard показывает coverage gap и age of last complete aggregate.

## 28. Набор dashboards

### 28.1. Owner daily overview

- DAU installations/accounts;
- mobile/desktop/PWA;
- new/returning;
- event-value and intent-action rates;
- event views/shares/saves/CTA;
- cards-to-first-value;
- errors/Web Vitals;
- auth/email/transport health;
- analytics coverage and RU budget.

### 28.2. Events and content

- top events by unique engaged viewers, not only raw views;
- view→save/share/CTA conversion;
- page depth;
- source/device/geography;
- broken/ended/canceled quality flags;
- collection and editorial edition performance.

### 28.3. Feature adoption

- keyboard eligible/hint/use/conversion;
- Hero Talk home/page-end;
- medallions and artifacts;
- `/dlya-menya/`;
- Search;
- volunteer CTA;
- onboarding capabilities.

### 28.4. Personalization and Search

- personalized/static coverage;
- cards-to-value;
- hide/save/CTA rates;
- diversity;
- Search zero results/reformulation/latency;
- model/corpus/release revisions;
- experiment holdout where available.

### 28.5. Auth, email and focus

- login funnel by method/device/route;
- provider accepted/delivered/bounced;
- actual active authenticated accounts/day;
- focus score/NPS/issues/delivery/fix time.

### 28.6. Reliability and cost

- route success/latency/fallback;
- outbox age;
- schema rejects/duplicates/lateness;
- YDB RU/storage;
- Supabase DB/egress;
- API Gateway/Function calls;
- LLM limits/tokens/cache/fallback;
- last complete aggregate.

## 29. Data quality и SLO

Базовые gates:

- 100% активных event names зарегистрированы;
- 100% accepted production events имеют schema version;
- 0 production web facts без release/page revision;
- 0 PII/secret violations в automated payload scanner;
- duplicate accepted facts `<0.1%`;
- terminal delivery success weak telemetry `>=99%` при доступном маршруте;
- strong action analytics projection не влияет на product success;
- p95 `occurred_at → daily/curated availability` не более 15 минут для near-real-time и не более 24 часов для daily-only;
- bots/previews/tests исключены;
- dashboard показывает coverage, а не скрывает неполные сутки;
- feature не объявляется measured до первого terminal production-safe readout.

## 30. Автотесты

Для каждого активного события/факта:

1. trigger unit test;
2. schema test;
3. prohibited-field/PII negative test;
4. consent-off: `0` optional writes;
5. idempotency/replay test;
6. direct/relay/offline delivery test;
7. E2E с `test_run_id` и terminal test sink receipt;
8. test pollution exclusion;
9. source-of-truth reconciliation для strong actions;
10. daily aggregate fixture/query test.

Особые обязательные сценарии:

- hint exposed, keyboard not used;
- keyboard used without hint;
- mouse/touch/keyboard one action → one fact;
- event card re-enters viewport → no duplicate exposure;
- share/copy/native share semantics;
- Yandex route unavailable → bridge/retry coverage;
- YDB unavailable → UX remains usable;
- auth account link does not double-count actor-day;
- PWA browser and standalone sessions classified correctly;
- Metrica blocked → first-party stats still work;
- preview/synthetic actors do not enter production dashboard.

Для default-OFF action map обязательна отдельная OFF-build proof suite:

- build manifest: нет action-map entry/chunk;
- HTML: нет action-map script/import/modulepreload и action-map-only attributes;
- browser/network: `0` action-map requests и payload fields;
- instrumented runtime: `0` action-map listeners/observers/timers/background tasks;
- storage: не создаются action-map IndexedDB store/records;
- bundle diff: `0` incremental bytes, кроме отдельно объяснённого общего build
  metadata noise;
- ordinary navigation работает при полном запрете analytics route и не читает
  analytics DB/config.

Active capture не может разрабатываться или включаться до terminal PASS этой
suite. Для active build дополнительно проверяются consent/route/sample/expiry
fail-closed gates, prohibited fields, budgets/TTL/idempotency, authoritative
action reconciliation, `0` profile mutations/rank changes, low-sample hiding и
active-vs-control INP/LCP/CLS parity.

## 31. Definition of Done новой функции

Функция не готова к публичному rollout, пока:

- сформулирован product question;
- определены exposure, engagement, conversion и guardrails;
- задан denominator;
- actor/session/device semantics не переопределены;
- события/факты зарегистрированы;
- указаны owner, consent, prohibited data и retention;
- strong facts связаны с authoritative store;
- реализованы delivery/idempotency/tests;
- существует dashboard или воспроизводимый query;
- есть release/model/content revisions;
- проверено ресурсное влияние;
- принят loss/degradation contract;
- TO-BE-документация находится в `main`.

## 32. Приоритет реализации

### P0 — до публичного запуска и полноценной фокус-группы

1. Actor/session/device/auth definitions.
2. Page views по page family и device.
3. Event card depth и detail views по event id.
4. Save/calendar/share/CTA authoritative facts.
5. Keyboard cohorts и hint conversion.
6. Hero Talk home/page-end exposure/engagement.
7. `/dlya-menya/` opens and personalized/static state.
8. PWA install/standalone reconciliation.
9. Auth/email delivery funnel.
10. Focus feedback delivery/status.
11. Data quality, release SHA and no-test-pollution.
12. Daily owner dashboard и RU/cost budget.

### P1 — сразу после устойчивого P0

- Search funnel и corpus revisions;
- personalization quality/holdout;
- medallions/artifacts;
- volunteer CTA;
- collections/editorial editions;
- onboarding capability learning;
- SEO/organic acquisition imports.

### P2

- registered experiments;
- cross-session assisted paths;
- long-term cohorts;
- generated public/partner aggregate reports;
- optional Metrica comparator.

## 33. Текущая граница реализации

Уже есть отдельные части:

- compact PWA install/standalone telemetry;
- Telegram/VK post/source metrics;
- OTP/auth/transport terminal evidence;
- focus score/problem/screenshot/outbox slices;
- pipeline metrics и `/general_stats`;
- R14 documentation contract для Web Vitals, rails, swipes, artifacts и calendar.

Но единого production ingest, actor/session fact model, event-level first-party view statistics, daily product aggregates и общего dashboard пока нельзя считать реализованными. Этот документ фиксирует общий target, в который существующие slices должны быть приведены без создания второго параллельного analytics продукта.

## 34. Официальные ссылки для инфраструктурного решения

- Yandex Cloud free tier: <https://yandex.cloud/en/docs/billing/concepts/serverless-free-tier>
- YDB Serverless pricing: <https://yandex.cloud/en/docs/ydb/pricing/serverless>
- DataLens pricing: <https://yandex.cloud/en/docs/datalens/pricing>
- DataLens → YDB: <https://yandex.cloud/en/docs/datalens/operations/connection/create-ydb>
- DataLens connections: <https://yandex.cloud/en/docs/datalens/concepts/connection/>
- Yandex Query + Object Storage: <https://yandex.cloud/en/docs/query/tutorials/yq-storage>
- Supabase billing/free quotas: <https://supabase.com/docs/guides/platform/billing-on-supabase>
- Supabase database size: <https://supabase.com/docs/guides/platform/database-size>
- Supabase egress: <https://supabase.com/docs/guides/platform/manage-your-usage/egress>
- Yandex Metrica JavaScript event goals: <https://yandex.com/support/metrica/en/general/goal-js-event>
- PostHog pricing: <https://posthog.com/pricing>
- Umami: <https://docs.umami.is/docs>
