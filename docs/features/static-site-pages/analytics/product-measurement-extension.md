# Расширение продуктовых измерений: карточки, чтение, CTA, Hero Talk и освоение сервиса

> **Статус:** принятый TO-BE-контракт метрик; browser emitter и dashboards ещё не реализованы.  
> **Дата:** 2026-08-05.  
> **Родитель:** [`README.md`](README.md).  
> **Хранение:** [`storage-retention-architecture.md`](storage-retention-architecture.md).

## 1. Общие правила

1. Любая conversion имеет явный eligible/exposed denominator.
2. DOM render не равен visibility.
3. Click не равен terminal success.
4. External navigation не равна покупке/регистрации без callback.
5. Reload, preview, bot и acceptance traffic не входят в product metrics.
6. Любая метрика связывается с `release_sha`, `page_revision`,
   `content_revision`, feature/model/rule version и surface.
7. Нельзя хранить raw coordinates, scroll offsets, full URL, текст Hero Talk,
   описание события или Search query ради отчёта.

## 2. Типы карточек

### 2.1 Две обязательные оси

```text
card_density:
  large | compact

card_family:
  editorial
  split
  listing_large
  listing_compact
  agenda_row
  related_large
  related_compact
  search_result
  personalized
  festival
  collection
  volunteer
  other_allowlisted
```

`large/compact` — сравнимая продуктовая плотность. `card_family` — конкретная
композиция. Нельзя выводить density по CSS height на лету: она приходит из
versioned renderer contract.

### 2.2 Что считается просмотром

`card_visible` принимается один раз на:

```text
session × surface × event/concept × card_family
```

Минимум:

- не менее 50% карточки видимо;
- не менее 500 ms либо один animation frame + explicit user navigation для
  keyboard-selected card;
- background tab исключён;
- hidden hydration templates исключены.

### 2.3 Session summary

Для каждой session:

```text
large_cards_exposed
compact_cards_exposed
large_cards_opened
compact_cards_opened
large_card_intent_actions
compact_card_intent_actions
unique_large_events_exposed
unique_compact_events_exposed
max_large_position_bucket
max_compact_position_bucket
```

Отчёт показывает:

- mean;
- median;
- p75;
- p90/p95;
- distribution `0`, `1–3`, `4–10`, `11–20`, `21–30`, `31+`.

Среднее без distribution запрещено: длинный хвост может скрыть типичную сессию.

### 2.4 Conversion по карточкам

```text
card_open_rate(density/family) =
unique visible cards opened
----------------------------
unique visible cards
```

```text
card_intent_rate(density/family) =
visible cards with accepted intent action
----------------------------------------
unique visible cards
```

Отдельно:

- first-card conversion;
- conversion by position bucket;
- surface/page family;
- mobile/desktop/PWA;
- cold/static/personalized ordering;
- new/returning actor.

Большая карточка не объявляется «лучше» только из-за большего open rate: нужно
учитывать, что она занимает больше viewport и показывает меньше альтернатив.
Обязательная companion metric:

```text
intent actions per 1000 viewport-card slots
```

## 3. Глубина просмотра выдачи

### 3.1 Позиционные buckets

```text
1
2–3
4–5
6–10
11–15
16–20
21–30
31+
```

Хранится максимальный достигнутый bucket, а не каждый scroll tick.

### 3.2 Основные метрики

```text
cards_to_first_event_open
cards_to_first_event_value
cards_to_first_intent_action
```

Для сессий без результата отдельно показываются censored distributions, а не
искусственное значение `31`.

### 3.3 Сравнение поверхностей

- обычный listing;
- date/weekend;
- Popular;
- Search;
- `/dlya-menya/`;
- editorial collections;
- related/continuation;
- Favorites.

Главный вопрос: на какой поверхности пользователь быстрее находит полезное
событие при сопоставимом supply.

## 4. Глубина чтения страницы события

### 4.1 Семантические checkpoints

Вместо raw scroll percentage renderer публикует стабильные checkpoints:

```text
hero
key_facts
summary
full_description_start
description_25
description_50
description_75
description_100
practical_info
transport
related_start
related_end
page_end
```

Для короткого описания `description_25...100` могут схлопнуться; manifest
указывает число реально доступных checkpoints.

### 4.2 Visibility rule

Checkpoint считается достигнутым, когда:

- semantic marker реально вошёл в viewport;
- вкладка активна;
- marker не является hidden/duplicate template;
- видимость длится минимум 500 ms либо пользователь keyboard-командой явно
  перевёл reading focus в этот блок.

Хранится только максимальный checkpoint на `session × event`.

### 4.3 Метрики

- доля event detail viewers, достигших key facts;
- доля начавших full description;
- 25/50/75/100 completion;
- practical/transport reach;
- related reach;
- page-end reach;
- CTA click by maximum checkpoint;
- save/share/calendar by maximum checkpoint;
- median time-to-key-facts и time-to-intent в buckets.

`description_100` не означает, что человек прочитал текст: это reach/completion
proxy. В интерфейсе и отчёте нельзя называть его доказанным чтением.

### 4.4 Контентные сравнения

Разрезы:

```text
content_revision
event_type
description_length_bucket
media_family
page_family/template
device/app_mode
```

Не сохранять сам текст в analytics.

## 5. Главные CTA страниц событий

### 5.1 Allowlisted `cta_kind`

```text
ticket_buy
registration
calendar_add
favorite_save
share_event
route_open
transport_details
volunteer_apply
source_open
organizer_open
phone_call
email_contact
feedback_open
auth_start
other_reviewed
```

### 5.2 Funnel stages

```text
eligible
rendered
visible
click
dispatch_started
pending
accepted
success
failure
cancel
undo
external_navigation_started
```

Не все CTA имеют все стадии. Для external ticket/registration ссылки terminal
product evidence часто заканчивается на `external_navigation_started`.

### 5.3 Conversion

```text
CTA visibility conversion = unique click actors / unique visible actors
```

```text
CTA dispatch success = accepted or success commands / dispatch_started
```

```text
CTA event reach conversion = unique click actors / unique engaged event viewers
```

Разрезы:

- `cta_kind`;
- event id/type;
- CTA label version;
- placement;
- desktop/mobile/PWA;
- authenticated/anonymous eligibility;
- card source / entry surface;
- description checkpoint before click;
- personalization/static origin.

### 5.4 Multiple main CTA

Если страница показывает несколько главных вариантов, записывается
`cta_set_version` и eligibility каждого. Conversion одного CTA не сравнивается
с другим без учёта того, что они были одновременно доступны.

## 6. Hero Talk и Page-end Talk

### 6.1 Placements

```text
home_hero
page_end
```

Смешивать их в один показатель запрещено: page-end denominator состоит только
из пользователей, достигших конца страницы.

### 6.2 Состояния

```text
hero_talk_eligible
hero_talk_visible
hero_talk_checkpoint
hero_talk_chain_advance
hero_talk_object_visible
hero_talk_object_click
hero_talk_cta_click
hero_talk_dismiss
hero_talk_complete
hero_talk_downstream_value
```

### 6.3 Объекты и ссылки

Для клика хранится только:

```text
target_kind
target_id_allowlisted
object_role
chain_id
step_id
```

Запрещено хранить raw URL, полный текст фразы, произвольный DOM selector или
персональный content payload.

`target_kind`:

```text
event
festival
collection
artifact_hint
artifact_collection
club
search
profile
favorites
calendar
external_reviewed
other_reviewed
```

### 6.4 Engagement

`visible` не является engagement. Actor вовлечён, если выполнено одно из:

- достигнут смысловой checkpoint после первой сцены;
- chain advance;
- object/CTA click;
- downstream value within the same session.

```text
hero_talk_engagement_rate = engaged actors / visible actors
```

```text
page_end_talk_engagement_rate = engaged actors / page-end talk visible actors
```

Дополнительно:

- dismiss rate;
- click-through by target kind;
- downstream event-value rate;
- repeat suppression effectiveness;
- first-time vs returning;
- variant/chain/version.

### 6.5 Attribution

```text
direct:
  click Hero object -> target action

assisted:
  Hero engaged -> later value in same session

experiment:
  registered holdout/A-B only
```

Assisted correlation не называется causal uplift.

## 7. Освоение возможностей сервиса

Термин «уровень профессионализма пользователя» заменяется на нейтральный
`capability_maturity_tier`: **уровень освоения сервиса**.

Он не оценивает человека, не является социальным рейтингом и не используется
для доступа, призов, рекламы или чувствительных выводов.

### 7.1 Capability families

```text
discovery:
  listings, dates, collections, related

intent:
  favorite, calendar, share, CTA

advanced_discovery:
  Search, /dlya-menya/, filters

navigation:
  keyboard, contextual help

utility:
  profile, diagnostics, reminders

cultural:
  artifacts, Hero Talk, club discovery
```

### 7.2 Tiers за rolling 30 days

| Tier | Название | Минимальный evidence |
|---:|---|---|
| 0 | viewer | только page/event views |
| 1 | explorer | >=2 discovery families или >=3 event opens в >=2 sessions |
| 2 | organizer | accepted save/calendar/share/CTA в >=2 sessions |
| 3 | advanced | >=2 advanced families, например Search + `/dlya-menya/` или keyboard + collections |
| 4 | power | >=4 capability families, >=3 sessions и >=2 distinct intent actions |

Tier считается детерминированно по bounded capability mask; сырая история не
нужна.

### 7.3 Отчёты

- distribution tiers by day/week/month;
- movement between tiers;
- D7/D30 retention by starting tier;
- event-value rate by tier;
- feature discoverability gaps;
- доля actors, которые застряли на viewer despite visible hints.

### 7.4 Хранение

Actor-level tier:

```text
35 days default
90 days hard maximum
```

Долгосрочно сохраняется только daily/monthly distribution без actor ID.

## 8. Клавиатурная навигация

Когорты:

```text
desktop_keyboard_eligible
keyboard_hint_visible
keyboard_navigation_started
keyboard_target_action
keyboard_help_opened
keyboard_context_recovered
keyboard_only_artifact_found
```

Отчёт:

- distinct actors used keyboard;
- desktop actors never used keyboard in day/rolling 30d;
- hint-exposed non-users;
- adoption after hint;
- start reliability;
- target action conversion;
- keyboard-only artifact conversion.

Lifetime «никогда» не выводится при коротком observation window; используется
`never_in_observed_30d`.

## 9. Персонализация и `/dlya-menya/`

Обязательные state classes:

```text
static_fallback
local_profile
personalized_fresh
personalized_stale
personalized_degraded
cold_start
```

Метрики:

- eligible/opened users;
- cards exposed/opened by density;
- cards-to-first-value;
- save/hide/share/CTA;
- personalization coverage;
- profile age/revision class;
- fallback rate;
- diversity/concentration;
- reset/merge/multi-device lag;
- NPS/quality feedback after finite personal selection.

Качество нельзя доказывать только engagement: необходим frozen benchmark,
holdout или owner-reviewed cohort.

## 10. Daily dashboard minimum

### Audience

- installation/account DAU;
- desktop/mobile/PWA;
- new/returning;
- authenticated active daily;
- sessions and actions/session.

### Discovery

- event views;
- card exposures by large/compact;
- p50/p75/p95 cards/session;
- cards-to-first-value;
- description checkpoints.

### Intent and CTA

- accepted actions by kind;
- unique actors;
- CTA visible/click/success funnels;
- event-level conversion.

### Features

- keyboard cohorts;
- Hero Talk home/page-end;
- `/dlya-menya/`;
- Search;
- medallions/artifacts;
- volunteers;
- maturity tier distribution.

### Reliability and ecology

- missing facts/duplicates/test pollution;
- ingest latency/outbox age;
- YDB RU/bytes/rows;
- Gateway calls;
- Supabase analytics rows = 0;
- retention/archive status.

## 11. Test contract

Для каждого нового измерения:

1. source-contract test события/summary;
2. browser visibility/dedupe test;
3. no-consent = zero weak writes;
4. preview/test actor excluded;
5. strong action uses exact authoritative receipt;
6. duplicate replay is no-op;
7. unknown enums fail closed;
8. payload and row budget enforced;
9. daily aggregate numerator/denominator parity;
10. TTL/archive fixture;
11. PII/redaction scan;
12. one terminal exact-SHA evidence run before production enable.
