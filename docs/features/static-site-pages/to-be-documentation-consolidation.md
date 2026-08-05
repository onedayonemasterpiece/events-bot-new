# Консолидация TO-BE-документации из рабочих веток

> **Статус:** retrospective branch-to-main documentation debt закрыт для полного
> remote snapshot 2026-08-05. Runtime implementation debt остаётся отдельным и
> явно виден в feature contracts.  
> **PR:** `#337` (`docs/to-be-consolidation-retention-20260805`).  
> **Resolved ledger:**
> [`to-be-branch-disposition-ledger.md`](to-be-branch-disposition-ledger.md) /
> [`to-be-branch-disposition-ledger.manifest.json`](to-be-branch-disposition-ledger.manifest.json); полный machine ledger — `to-be-branch-disposition-ledger.json.gz`.

## 1. Что теперь считается закрытым

Полный remote audit на cutoff 2026-08-05 проверил 601 ветку. Для всех 218
requirement-like/manual branches и 402 обнаруженных requirement-like paths
зафиксирован явный verdict:

```text
canonical / ported
superseded by later accepted decision
historical or research evidence only
implementation/incident evidence only
backlog not accepted
not accepted historical donor
```

Ни одна старая ветка больше не является неявным TO-BE source of truth. Это не
означает механическое копирование каждого исторического Markdown-файла:
research, reports, incidents, labs, prompts, implementation diaries и raw
review artifacts остаются evidence. В `main` переносится только проверенное
нормативное решение.

## 2. Правило приоритета

При конфликте используется не «самый новый commit», а последовательность:

1. более поздняя явная owner correction;
2. принятое каноническое решение и актуальный release plan;
3. совместимость с current runtime/SOR, privacy и reliability boundaries;
4. более поздний совместимый requirement;
5. research/lab/implementation/history как evidence.

Commit date помогает найти кандидатов, но не разрешает противоречие.

## 3. Разрешённые критические конфликты

### Фокус-группа и авторизация

Победило более позднее явное решение:

- сайт можно просматривать без входа;
- server-side feedback/NPS/prize participation требуют явной авторизации;
- silent anonymous Auth и anonymous server feedback запрещены;
- старые anonymous-first focus-control ветки и построенный на них dashboard
  помечены superseded.

### `Избранное`, hidden и профиль

Каноническая граница:

- `Избранное` — union calendar/favorite state;
- calendar save и like не склеиваются;
- hidden/not-interest recovery не переносится в профиль;
- профиль владеет account, interests/personalization controls и diagnostics;
- старые `Мои события`/profile-hidden варианты не импортированы.

### Search

Текущий authenticated Search contract побеждает ранние рекомендации о
public-basic-search. Аналитические документы 2026-07-18 сохранены как product
analysis, но не являются принятым требованием.

### Персонализация и физическое хранение

Supabase-primary historical telemetry/evaluation lake отвергнут. Каноника:

- product SOR каждого домена остаётся отдельным;
- weak browser observations агрегируются до отправки;
- first-party ingest использует общий resilient direct/relay transport;
- YDB хранит compact recent facts/aggregates с TTL;
- Object Storage хранит verified Parquet history;
- Supabase не становится raw analytics warehouse.

### Сильные действия и клики

`click` не равен successful save/registration/purchase/reminder. Strong metric
появляется только из authoritative idempotent receipt. Browser visibility/click
остаётся weak consent-gated observation.

### Hero Talk

Принят chain-first contextual contract с versioned chain/step/target и
отдельными denominators для home Hero и page-end. Random isolated copy labs —
evidence, не TO-BE.

### Event age rating

Принят nullable declared-only fact без default `0+`, с обязательной паритетностью
на всех event-bearing public surfaces.

### Автопрезентатор

Owner-test vertical slice принят. Portable/public release остаётся `NO-GO` до
Windows 10 evidence, rehearsal и fallback proof. Большие integration README и
scenario diaries не интерпретируются как разрешение публичного показа.

### Датированные planned activations

Фиксированная social-brand activation 2026-07-30 не импортирована: planned date
без current accepted runtime/evidence не становится действующей каноникой.

## 4. Что перенесено/переписано в canonical package

### Фокус, профиль, персонализация, reminders

- [`focus-group.md`](focus-group.md) и
  [`focus-group-release/README.md`](focus-group-release/README.md);
- [`user-profile.md`](user-profile.md);
- [`personalizaion/transport-ecology-profile-architecture.md`](personalizaion/transport-ecology-profile-architecture.md);
- [`personalizaion/identity-linking-personalization.md`](personalizaion/identity-linking-personalization.md);
- [`personalizaion/longitudinal-e2e-personalization.md`](personalizaion/longitudinal-e2e-personalization.md);
- [`personalizaion/golden-personas-real-data-v0.md`](personalizaion/golden-personas-real-data-v0.md);
- [`event-reminders-calendar-strategy.md`](event-reminders-calendar-strategy.md) и
  [`event-action-onboarding.md`](event-action-onboarding.md);
- calendar/reminder, auth fixture и personalization transport test plans.

### Hero Talk, keyboard, volunteers

- [`../hero-talk/README.md`](../hero-talk/README.md) и
  [`hero-talk-release-track.md`](hero-talk-release-track.md);
- keyboard v8 product/onboarding/test contracts;
- [`volunteer-recruitment/README.md`](volunteer-recruitment/README.md) и its
  test/handoff package.

### Public site parity/release gates

- [`event-age-rating.md`](event-age-rating.md);
- [`responsive-navigation.md`](responsive-navigation.md);
- [`seo-geo-release-optimization.md`](seo-geo-release-optimization.md);
- [`medallion-visual-qa.md`](medallion-visual-qa.md);
- [`auto-present/README.md`](auto-present/README.md).

### Product statistics and content intelligence

- [`analytics/README.md`](analytics/README.md);
- [`analytics/product-measurement-extension.md`](analytics/product-measurement-extension.md);
- [`analytics/storage-retention-architecture.md`](analytics/storage-retention-architecture.md);
- [`analytics/unified-statistics-runtime-architecture.md`](analytics/unified-statistics-runtime-architecture.md);
- machine catalog/schema/migration inventory;
- [`../post-metrics/consolidated-event-engagement.md`](../post-metrics/consolidated-event-engagement.md);
- [`../../llm/unusual-event-detection.md`](../../llm/unusual-event-detection.md).

## 5. Единая статистика: документированный target и implementation truth

На уровне требований контур теперь единый:

```text
feature adapter
-> catalog + consent/privacy gate
-> session compaction
-> bounded idempotent outbox
-> resilient direct/relay
-> first-party ingest
-> compact YDB facts/aggregates
-> Parquet archive
-> TTL / verified delete
```

Созданы:

- runtime architecture;
- versioned event catalog;
- JSON batch schema;
- service-wide migration inventory;
- browser client foundation и unit tests.

Но документационное принятие не переименовывает незавершённый runtime в готовый.
Остаются implementation tasks: миграция emitters, first-party ingest, YDB
projector/aggregates, Parquet archive/delete cycle и удаление legacy direct
telemetry calls. Их exact scope зафиксирован в unified runtime document.

## 6. Постоянный gate

Workflow теперь выполняет три шага:

1. raw remote-branch inventory;
2. применение reviewed branch/path ledger;
3. full semantic evidence corpus для проверки.

`reconcile_to_be_documentation_audit.py` падает, если появляется новая
requirement-bearing branch/path без disposition или existing reviewed branch
изменяет head вне разрешённой current-consolidation policy. Поэтому новый долг
не скрывается за advisory ZIP.

## 7. Ограничение evidence

GitHub Actions run для этого PR не стартует из-за repository/account billing
block. Поэтому код и schemas проверены локально, а workflow сохранён и
fail-closed, но hosted-run evidence появится только после восстановления
Actions. Это инфраструктурный blocker CI, а не причина оставлять документы вне
GitHub.
