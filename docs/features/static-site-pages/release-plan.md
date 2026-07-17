# План production-релиза статических страниц событий

> **Срез:** 2026-07-17
> **Решение:** `NO-GO` для переключения event pages на canonical root прямо сейчас.
> **Scope:** production-контур статических страниц событий и переход event-detail
> с Telegraph. Полный релиз F1–F18/H1/M1–M6 и post-release stages остаётся отдельным
> umbrella-gate в [Static personal announcements](../static-personal-announcements/README.md).

## Важное восстановление scope

Этот файл — **не весь релизный план** и не сокращает его до пяти задач. После
повторного аудита истории найден базовый checklist из 228 требований; после добавления пяти D0–D10 rows текущий реестр содержит 233 требования:

- [полный Stage 0–11 readiness checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md);
- [отчёт восстановления источников и статусов](../../reports/static-site-release-context-recovery-2026-07-17.md);
- [presentation-day gate](presentation-release-checklist.md);
- [208 стабильных test scenario IDs](test-scenarios.md).

Ниже описан только зависимый platform/cutover workstream. Его нельзя использовать
как доказательство готовности identity, favorites, email/reminders,
personalization, transport, media, age/occurrences, SEO/GEO или других release gates.

## Где находится release truth

Единственная release-база — `origin/main`. Side branches, опубликованные preview и
локальные dirty checkout считаются только evidence/WIP, пока изменения не достигли
`origin/main` и не получили production evidence.

Целевой контур:

```text
immutable Fly SQLite snapshot
  -> coalesced static_site_build after effective Smart Update
  -> Kaggle CPU checked artifact
  -> immutable release prefix + static_release_manifest_v1
  -> atomic promotion/current pointer
  -> retained previous release and verified rollback
```

На дату среза в `origin/main` есть export, Astro preview build/check, Kaggle handoff,
status/resource-lease contract и preview publication. Но общий event-page контур
остаётся preview-only:

- production build/check profile отсутствует;
- event HTML, `robots.txt` и sitemap всё ещё имеют preview/noindex semantics;
- Kaggle job получает checked tarball, но не выполняет production manifest,
  staging promotion и rollback;
- canonical event slug всё ещё вычисляется из изменяемых title/city/id вместо
  persisted publication registry;
- Telegraph остаётся обязательной зависимостью части event publication flows.

Production canary клубов по интересам от 2026-07-17 не меняет этот вывод: это
ограниченный root overlay из семи файлов, а не promotion полного event catalog.

## Что изменилось за 15–17 июля 2026

| Изменение | Состояние | Учитываем в релизе |
|---|---|---|
| Desktop Editorial event experience и fail-closed media roles (`58abfb19`) | merged в `origin/main` | Да; нужен явный regression scenario, но это не закрывает production publish |
| Declared/assessed age-rating data path (`aa95900a`) | merged в `origin/main` | Да; проверять parity карточки/detail/JSON-LD/export, не считать автоматическую оценку публичной маркировкой |
| Social popularity batches и owned-channel aggregation (`fe211a88`, `d25b15d6`, `b34a97d3`) | merged в `origin/main` | Да; owned reposts не должны суммироваться как независимые аудитории |
| Telegram RichMessage medallions (`14e25b43`) | merged в `origin/main` | Смежный social surface; не является static release gate |
| Gated interest-club projection/pages и production canary (`98180d1e`, `6b234a52`, evidence `6cdae545`) | merged и canary live | Да как дополнительный consumer общего checked build; семь дней наблюдения ещё идут |
| Atomic event-site publisher (`62ba7110`) и последующее hardening | только side/local branches | **Нет:** не считать реализованным, пока не перенесён на свежую main-based ветку, не проверен и не слит |
| Transport, mobile-v8, saved-event identity, последние personalization/media fixes | side branches | **Нет:** branch evidence, не release truth |
| Подробный каталог тестов `test-scenarios.md` | добавлен этим изменением | Да как routed inventory; большинство сценариев ещё не автоматизировано |

Итог аудита: feature-документы хорошо описывают отдельные slices, но до этого
изменения не было одного актуального event-page release plan; routing не ссылался
на подробный test inventory, а E2E index не различал draft/demo/release E2E.

## Release gates до D0

`D0` нельзя назначать календарно, пока не закрыты все P0-gates:

1. **Production profile:** отдельные `build:production` и `check:production`, root
   canonical URLs, indexable robots, sitemap без preview/lab routes, полный eligible
   catalog; preview profile остаётся noindex и неизменным.
2. **Stable page identity:** persisted slug/revision/publication registry, aliases,
   redirect/tombstone/retention contract; title/location edits не меняют canonical URL.
3. **Safe publisher:** immutable release prefix, signed/hashed manifest, catalog
   parity, atomic promotion, failed-candidate isolation, retained last-good release и
   проверенный rollback.
4. **Freshness/outbox:** update B во время долгого build A гарантирует ровно один
   follow-up build более нового snapshot; есть max-staleness alert и catch-up runbook.
5. **Downstream decoupling:** Telegram/VK/import/admin flows используют единый
   public-page resolver и не требуют успешного `telegraph_build`, когда static page
   уже ready.
6. **Acceptance evidence:** автоматизируемый RC subset из
   [test-scenarios.md](test-scenarios.md) прошёл на clean main-reachable SHA; native
   share/calendar/maps/unfurl проверки приложены вручную там, где mocks недостаточны.

Release owner фиксирует точные `T0` и `T0+10 days` в UTC и
`Europe/Kaliningrad`, production SHA, snapshot id, build id, manifest hash и rollback
target. Формулировка «через 10 дней» без этих полей не является scheduled cutover.

## Десятидневный Telegraph coexistence

### Конфигурационный контракт

```text
EVENT_PUBLIC_PAGE_MODE=telegraph|dual|static
TELEGRAPH_EVENT_WRITE_MODE=create_edit|existing_only|off
STATIC_SITE_CANARY_PERCENT=0..100
```

- `EVENT_PUBLIC_PAGE_MODE` выбирает outward URL, а не сам факт записи.
- Static URL разрешён только если current promoted manifest содержит нужные
  `event_id` и source revision/hash.
- `existing_only` разрешает при необходимости обновлять уже существующую страницу,
  но запрещает create и fallback-recreate после ошибки edit.
- `off` запрещает любые event-detail Telegraph API writes, но не очищает сохранённые
  `telegraph_url`/`telegraph_path`.
- Aggregate month/weekend/festival Telegraph pages имеют отдельный режим и **не
  выключаются D10** до появления эквивалентных static surfaces. Текущий D10 scope —
  только event-detail pages.

### График после фактического T0

| День | Режим | Gate |
|---|---|---|
| D0 | full static release promoted; `dual`; static links 10%; Telegraph `create_edit` | 100% catalog parity, public canonical/robots/sitemap/JSON-LD/OG/ICS smoke |
| D2 | static links 25% | нет broken outward URLs; freshness и build health в target |
| D4 | static links 50% | sampled Telegram/VK/MAX unfurl не хуже принятого baseline |
| D6 | static links 100%; Telegraph всё ещё shadow-created/edited | каждый outward static URL подтверждён current manifest |
| D7–D9 | 72-hour soak на 100% static links | `0` release-critical errors, reconciliation всех eligible events/surfaces |
| D10 | `EVENT_PUBLIC_PAGE_MODE=static`; `TELEGRAPH_EVENT_WRITE_MODE=existing_only` | create/recreate attempts after cutoff = `0`; legacy URLs сохранены |

Старые Telegram/VK посты массово не редактируются, старые Telegraph URLs не
удаляются. Позднее отдельным решением можно перевести `existing_only -> off`.

### Go/no-go и rollback

Минимальные D10-инварианты:

- eligible event catalog parity `100%`, ineligible leak `0`;
- static-ready before outward link emission `100%`;
- HTML/assets/ICS success `>=99.9%` в soak-окне;
- preview/noindex/canonical leakage `0`;
- freshness p95 `<=30 min`, max `<=60 min` после due time;
- broken outward links `0`;
- `telegraph_create_attempts_after_cutover=0` и
  `telegraph_recreate_attempts_after_cutover=0`.

До D10 rollback возвращает outward mode в `telegraph`/`dual` и current static pointer
на last-good. После D10 emergency rollback может временно вернуть `create_edit`, но
только явным операторским решением и bounded backfill пропущенных eligible events.
Ни один rollback не очищает legacy Telegraph fields.

## Первая волна platform-задач (не полный backlog)

Это пять параллельно стартуемых work packages из полного реестра, выбранных с
учётом запрета на UI листингов/event detail. Они не являются «всем оставшимся».

| Priority | Задача | Зависимости | Acceptance |
|---|---|---|---|
| P0-1 | **Production build profile** | нет | root output; production canonicals; indexable robots; sitemap без preview/lab; full eligible catalog; `check:production` green; preview profile unchanged |
| P0-2 | **Release manifest, staged promotion и rollback** | P0-1 | immutable prefix; SHA/snapshot/counts/checks в `static_release_manifest_v1`; failed candidate не меняет current; one-command verified rollback; release/lease evidence |
| P0-3 | **Stable event URL и lifecycle registry** | может идти параллельно P0-1; нужен P0-4 | persisted slug; aliases; redirect/410/retention rules; merge/delete/update idempotence; sitemap содержит только canonical eligible URLs |
| P0-4 | **Telegraph dual-run и public-link resolver** | P0-1..3 | три режима tested; static URL только после readiness; downstream не зависит от Telegraph; D10 создаёт/recreate `0`; legacy URLs сохранены |
| P0-5 | **Observability и automated acceptance pack** | contracts можно начать сразу; full E2E после P0-1/2 | catalog/freshness/resolver metrics; `ADD-CUTOVER-*` + release subset automated; 72-hour gate report; rollback drill evidence |

## Test/evidence contract

Канонический каталог сценариев: [test-scenarios.md](test-scenarios.md).

Важно: наличие ID не означает, что сценарий реализован или пройден. На дату среза:

- `npm --prefix site run build:preview && npm --prefix site run check:preview` —
  сильный fixture/build-time gate, но не production E2E;
- `tests/test_static_site_public_gate.py`,
  `tests/test_static_site_build_handoff.py` и related exporter tests — узкие
  unit/contracts;
- `tests/playwright/static_personalization_contract.spec.ts` — standalone demo
  contract из 9 tests, не Astro/public-site release E2E;
- `tests/e2e/features/static_site_personalization.feature` имеет `@draft` и пока не
  имеет Behave step definitions;
- atomic promotion/rollback, production-root browser/HTTP, 10-day cutover и native
  device flows ещё требуют реализации/evidence.

Полный F1–F17 readiness и release evidence pack остаются в
[аудите 2026-07-11](../../reports/static-personal-announcements-release-readiness-2026-07-11.md);
этот документ не ослабляет его gates.
