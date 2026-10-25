# Сквозной регистр вопросов дизайна системы

> **Статус:** активный release-control register.  
> **Назначение:** последовательно закрыть продуктовые, архитектурные, data,
> эксплуатационные и юридические неопределённости до зависимой реализации.  
> **Нормативный центр:** [`release-plan.md`](release-plan.md).  
> **Маршрутизация:** [`documentation-map.md`](documentation-map.md).  
> **Текущий статус:** [dashboard запуска 1 сентября](../../release/2026-09-01/README.md).

## 1. Почему это отдельный регистр

Обычный feature-документ хорошо отвечает на вопрос «как должна работать одна
функция», но плохо показывает, что один нерешённый выбор одновременно влияет на
Auth, профиль, аналитику, UI, storage, тесты и юридические тексты. В результате
агент может молча принять локально удобное решение и создать противоречащую
архитектуру.

Этот регистр не заменяет ADR и feature requirements. Он управляет очередью
неопределённостей:

```text
вопрос
→ варианты и ограничения
→ требуемое evidence
→ решение владельца / архитектуры / юриста
→ обновление всех затронутых каноник
→ только затем зависимая реализация и acceptance
```

## 2. Правила обработки

1. Вопросы обрабатываются в порядке приоритета и номера. Перейти к следующему
   можно, если предыдущий закрыт либо явно `BLOCKED` внешней зависимостью.
2. Вопрос получает стабильный ID `SYS-Q-NNN`; переименование не меняет ID.
3. Ответ в чате, PR comment или agent handoff не закрывает вопрос сам по себе.
4. Статус `DECIDED` допустим только когда записаны:
   - точная формулировка решения;
   - дата и decision owner;
   - затронутые канонические документы;
   - миграция/совместимость/rollback, если нужны;
   - acceptance evidence, необходимое до `DONE` в release checklist.
5. После `DECIDED` владелец вопроса обязан обновить каноники в `main`. До этого
   используется `DECIDED_PENDING_CANONICALIZATION`.
6. Старое несовместимое решение получает `SUPERSEDED`, а не остаётся вторым
   «действующим вариантом».
7. Реализация не может закрыть вопрос постфактум аргументом «так уже написан код».
8. Юридический вопрос закрывается только по фактическому data flow и UI, а не по
   абстрактному шаблону документа.

## 3. Статусы

| Статус | Значение |
|---|---|
| `OPEN` | Вопрос сформулирован, работа над ответом ещё не начата |
| `RESEARCHING` | Собираются факты, ограничения и варианты |
| `OWNER_DECISION` | Данных достаточно; нужен выбор владельца продукта/дизайна |
| `ARCH_DECISION` | Нужен архитектурный выбор и ADR |
| `LEGAL_DECISION` | Нужна правовая проверка фактической схемы |
| `VERIFY` | Решение предполагается, но target/live evidence его не подтверждает |
| `BLOCKED` | Ответ зависит от внешней информации или другого вопроса |
| `DECIDED_PENDING_CANONICALIZATION` | Решение принято, но ещё не сведено во все каноники `main` |
| `DECIDED` | Решение записано во всех канониках и зависимости обновлены |
| `SUPERSEDED` | Вопрос или прежний ответ заменён более поздним решением |
| `DEFERRED` | Осознанно исключён из текущего релиза с датой возврата |

## 4. Очередь вопросов до запуска

### `SYS-Q-001` — Модель участия в фокус-группе и доступность feedback

- **Приоритет:** P0
- **Статус:** `DECIDED_PENDING_CANONICALIZATION`
- **Decision owner:** Product owner
- **Дата решения:** 2026-08-04
- **Блокирует:** focus UI, Auth return flow, feedback tests, NPS, eligibility,
  тексты приглашения и dashboard.
- **Решение:**
  - invitation/QR открывает обычный сайт без обязательного входа;
  - local personalization доступна без server identity;
  - feedback-блок видим, но score, NPS, text и screenshot disabled до
    подтверждённого email/Яндекс;
  - рядом находится явный Auth CTA;
  - после входа пользователь возвращается в тот же feedback-блок;
  - silent anonymous Supabase Auth и anonymous server feedback запрещены.
- **Источник решения:** [PR #323](https://github.com/onedayonemasterpiece/events-bot-new/pull/323).
- **Конфликтует с:** anonymous-first моделью PR #250 и ранним dashboard PR #324.
- **Следующий шаг:** обновить `focus-group.md`, focus release companions,
  release checklist, auth scenarios и implementation handoff в одной ветке.
- **Критерий закрытия:** несовместимые anonymous-first формулировки явно
  superseded; current-main docs и generated dashboard совпадают.

### `SYS-Q-002` — Точный scope публичного запуска 1 сентября

- **Приоритет:** P0
- **Статус:** `OWNER_DECISION`
- **Decision owner:** Product owner + Release owner
- **Блокирует:** реалистичный критический путь, freeze, обязательные UI, D0 gates.
- **Нужно решить:**
  - какие поверхности должны быть публично включены на D0;
  - какие могут быть default-off, shadow или post-launch;
  - является ли завершённая фокус-группа обязательным условием открытия root;
  - какие обещания нельзя показывать без backend/legal/evidence.
- **Варианты:** минимальный полезный static-first release; расширенный release с
  Search/Auth/personalization; staged release с feature flags.
- **Следующий шаг:** принять must-have/defer table и обновить release plan плюс
  dashboard IDs.
- **Критерий закрытия:** у каждого P0 есть D0 disposition; ни один deferred
  контур не выглядит доступным в UI/SEO/коммуникациях.

### `SYS-Q-003` — Канонический SOR профиля, PII и персонализации

- **Приоритет:** P0
- **Статус:** `ARCH_DECISION`
- **Decision owner:** Architecture + Product + Legal
- **Блокирует:** `/profil/`, profile sync, identity linking, Supabase/YDB writes,
  retention, стоимость, 152-ФЗ и migration plan.
- **Текущая каноника:**
  [`personalization-data-ownership.md`](../../architecture/personalization-data-ownership.md)
  — Supabase owns identity/profile/current state; YDB owns de-identified analytics.
- **Целевая альтернатива:** [PR #328](https://github.com/onedayonemasterpiece/events-bot-new/pull/328)
  — dual-plane/YDB-primary target после ownership/localization decision.
- **Нужно решить:**
  - что является primary store на D0 и после D0;
  - допускаются ли server profile writes до локализационного аудита;
  - какая схема является временной, а какая целевой;
  - migration, rollback и delete semantics.
- **Следующий шаг:** factual data-flow inventory + RU/storage forecast + legal
  review; затем один ADR.
- **Критерий закрытия:** одна каноническая ownership table; противоречащий PR
  обновлён или superseded; runtime flags не допускают смешанной истины.

### `SYS-Q-004` — Какой prelaunch-вариант становится публичной заглушкой

- **Приоритет:** P0
- **Статус:** `OWNER_DECISION`
- **Decision owner:** Product/design owner
- **Блокирует:** root UI, Supabase migration, visual QA, launch copy, indexing.
- **Кандидаты:** PR #296 — основная prelaunch surface; PR #313 — tile mosaic
  challenger; PR #318 — физический Blender/material generator.
- **Нужно решить:** один production composition, допустимые визуальные donor-
  элементы, mobile crop/scale, lighting и motion boundaries.
- **Следующий шаг:** одинаковый frozen evidence pack на desktop/mobile/reduced
  motion; owner выбирает один candidate SHA/reference.
- **Критерий закрытия:** один canonical prelaunch contract, один accepted visual
  reference, остальные явно lab/evidence; нет stacked production roots.

### `SYS-Q-005` — Граница standard onboarding, Hero-talk, артефактов и клуба

- **Приоритет:** P0
- **Статус:** `OWNER_DECISION`
- **Decision owner:** Product owner
- **Блокирует:** onboarding runtime, Hero-talk phrase packs, keyboard hints,
  artifact placements, Friends Club claims и analytics.
- **Источники:** standard onboarding package в `main`, PR #288, Hero-talk PR #291,
  V8 keyboard PR #330.
- **Нужно решить:**
  - какие capability hints входят в D0;
  - где Hero-talk является placement, а не отдельным продуктом;
  - какой реальный артефакт используется и когда;
  - что относится к post-launch club/raffle lifecycle;
  - какие состояния не являются taste signals.
- **Следующий шаг:** утвердить baseline A/B и единую state-boundary diagram.
- **Критерий закрытия:** один onboarding home, один Hero-talk companion, exact
  artifact IDs/placements и отсутствие неподтверждённых raffle promises.

### `SYS-Q-006` — Единый реестр и таксономия подборок

- **Приоритет:** P0
- **Статус:** `ARCH_DECISION`
- **Decision owner:** Product + Editorial + Architecture
- **Блокирует:** меню, catalog, routes, sitemap, children/family/free/clubs,
  gastronomy и editorial editions.
- **Нужно решить:**
  - authoritative machine-readable registry;
  - различие utility, semantic, editorial и external-track collections;
  - route/data/navigation/sitemap lifecycle;
  - owner-reviewed membership и last-good behavior.
- **Следующий шаг:** принять registry v2 и route-integrity gate.
- **Критерий закрытия:** нет безусловных links в обход registry; blocked/deferred
  surfaces не кликабельны; каждый public path реально emitted.

### `SYS-Q-007` — Production recovery и эксплуатационная модель умного поиска

- **Приоритет:** P0
- **Статус:** `VERIFY`
- **Decision owner:** Search owner + Ops
- **Блокирует:** Search release, UI truth, cost/capacity claims.
- **Каноника:** [`smart-vector-search/README.md`](smart-vector-search/README.md).
- **Нужно доказать:** exact static SHA, Edge revision, corpus/catalog revision,
  cache key, stage timings, direct/relay route и authenticated result.
- **Следующий шаг:** коррелированный live recovery run из PR #284 без маскировки
  сбоя заменой embedding model.
- **Критерий закрытия:** root cause исправлена, cold/cached canaries terminal,
  stale corpus/cache invalidation и cost guardrails доказаны.

### `SYS-Q-008` — Identity linking между local state, email и Яндексом

- **Приоритет:** P0
- **Статус:** `ARCH_DECISION`
- **Decision owner:** Identity + Personalization owners
- **Блокирует:** возврат после Auth, profile merge, multi-device, logout/account
  switch, outbox isolation и eligibility.
- **Нужно решить:**
  - что переносится из local profile после verified login;
  - conflict policy account vs device;
  - canonical user ID при email/Яндекс;
  - account switch, logout epoch и stale outbox;
  - что вообще не синхронизируется на D0.
- **Источники:** site-user-identity current docs и identity-linking package PR #270.
- **Следующий шаг:** принять state transition table и cleanup/rollback tests.
- **Критерий закрытия:** нет cross-account leakage; duplicate profiles/actions
  дедуплицируются; local-only behavior честно описан.

### `SYS-Q-009` — Analytics, consent и допустимый объём telemetry

- **Приоритет:** P0
- **Статус:** `LEGAL_DECISION`
- **Decision owner:** Product analytics + Legal + Architecture
- **Блокирует:** launch metrics, personalization activation, keyboard/focus
  statistics, email attribution и privacy copy.
- **Каноника:** [`analytics/README.md`](analytics/README.md).
- **Нужно решить:** lawful basis/purpose для каждого класса событий, pre-consent
  zero-write boundary, identifiers, TTL, aggregation, bot/test exclusion и
  subject deletion.
- **Следующий шаг:** data-event inventory → purpose matrix → UI/legal copy →
  storage/RU budget → tests.
- **Критерий закрытия:** каждое событие имеет purpose, SOR, retention и metric
  consumer; raw weak telemetry не становится бессрочным профилем.

### `SYS-Q-010` — D0–D10 coexistence с Telegraph

- **Приоритет:** P0
- **Статус:** `OPEN`
- **Decision owner:** Release owner + Publishing owner
- **Блокирует:** outward URLs, rollback, old posts, create/recreate policy.
- **Нужно решить:** exact T0, canary percentages, static-ready invariant,
  `existing_only/off`, soak metrics и emergency rollback.
- **Следующий шаг:** подтвердить актуальность release-plan contract и реализовать
  resolver/metrics на свежем `main`.
- **Критерий закрытия:** zero broken outward URLs, current manifest proof,
  bounded rollback и нулевые запрещённые Telegraph creates после cutoff.

### `SYS-Q-011` — Публичные юридические документы и локализация потоков данных

- **Приоритет:** P0
- **Статус:** `LEGAL_DECISION`
- **Decision owner:** Qualified legal reviewer + Product owner
- **Блокирует:** server profile writes, feedback, email subscriptions, raffle,
  analytics и launch copy.
- **Нужно решить:** operator details, privacy policy, purpose-specific consents,
  informational/advertising messages, cookies/storage notice, user agreement,
  focus rules, raffle rules, localization/cross-border flow, retention/deletion
  и data-subject requests.
- **Следующий шаг:** сверить тексты с фактическим UI/data flow после `SYS-Q-003`
  и `SYS-Q-009`.
- **Критерий закрытия:** versioned public copies опубликованы; UI хранит exact
  version receipts; legal sign-off относится к фактически выпущенному SHA.

### `SYS-Q-012` — Пользовательские истории как release truth

- **Приоритет:** P0
- **Статус:** `OPEN`
- **Decision owner:** Product + QA
- **Блокирует:** полноту acceptance, focus tasks, связь статистики с реальной
  пользовательской ценностью.
- **Проблема:** общий [`docs/backlog/user-stories.md`](../../backlog/user-stories.md)
  не является актуальным реестром статического сайта и не связан системно с
  checklist/tests/incidents.
- **Нужно решить:** формат story/acceptance/evidence, story families, focus-group
  subset, связь событий аналитики с story outcome и incident-to-story mapping.
- **Следующий шаг:** создать два связанных registry: public static-site stories и
  focus-group stories; импортировать требования из release plan и plan links.
- **Критерий закрытия:** каждая P0 story имеет acceptance scenarios, UI surface,
  metric/guardrail и production status; каждый production break указывает
  сломанную story.

### `SYS-Q-013` — Release-ready SEO/GEO contract после UI freeze

- **Приоритет:** P1 до freeze, P0 перед D0
- **Статус:** `OPEN`
- **Decision owner:** SEO/GEO owner + Release owner
- **Блокирует:** final indexability, structured data, sitemap/canonical and answerability.
- **Проблема:** отдельный подробный контракт остался в старых PR #26/#65 и не
  является current-main truth.
- **Следующий шаг:** re-port актуальных требований после UI/UX freeze, удалить
  устаревшее, связать с generated full-catalog audit.
- **Критерий закрытия:** frozen current-main RC проходит canonical/indexability,
  visible-fact/JSON-LD parity, internal-link/orphan, mobile/no-JS и GEO answer pack.

### `SYS-Q-014` — Что считать завершённой UI/UX-поверхностью

- **Приоритет:** P0
- **Статус:** `DECIDED_PENDING_CANONICALIZATION`
- **Decision owner:** Product/design owner + QA
- **Решение:** отдельно отслеживаются product decision, accepted reference,
  implementation, responsive/accessibility QA, hosted exact-main evidence и
  production acceptance. Макет не равен собранному UI; собранный UI не равен
  выпущенному.
- **Следующий шаг:** применить модель ко всем `UIUX-*` и dashboard items.
- **Критерий закрытия:** checklist и UI/UX debt используют одинаковые уровни;
  ни одна поверхность не помечается `DONE` только по screenshot или source test.

## 5. Как закрывать вопрос

Для каждого решения используется короткая запись:

```text
ID:
Decision:
Date / owner:
Evidence considered:
Rejected alternatives:
Affected canonical docs:
Migration / compatibility:
Test and release gates:
Superseded documents:
```

После обновления каноник вопрос переводится в `DECIDED`. Если решение меняется,
старое не удаляется: оно получает `SUPERSEDED` со ссылкой на новый ID/decision.

## 6. Связь со статусом релиза

- Вопрос P0 в `OPEN`, `OWNER_DECISION`, `ARCH_DECISION`, `LEGAL_DECISION` или
  `CONFLICT` создаёт соответствующий blocker/owner gate в launch checklist.
- Исследование не закрывает вопрос без решения.
- Решение не закрывает implementation/QA/live gate автоматически.
- `DEFERRED` допустим только если зависимая функция также выключена в UI,
  navigation, sitemap, communications и production config.
