# Консолидация TO-BE-документации статического сайта

> **Статус:** принятый реестр приоритета и миграции требований.  
> **Дата среза:** 2026-08-05.  
> **База аудита:** `main@5082a502b9c2f4657742104b6ce543b87761a39d`.  
> **Область:** требования, стратегии, продуктовые решения, архитектурные контракты и тестовые модели статического сайта, которые существовали только в ветках или открытых PR.  
> **Не является:** разрешением слить старые ветки целиком, подтверждением готовности runtime или заменой feature-specific документов.

## 1. Зачем нужен этот реестр

Принятое TO-BE-решение не должно оставаться только в рабочей ветке, PR body,
комментарии или preview. При этом механическое правило «самый новый commit
побеждает» опасно: поздняя ветка может быть исследованием, implementation
scaffold, техническим донором или содержать более старую продуктовую модель.

Поэтому консолидация выполняется не хронологическим копированием, а
**семантической сверкой**:

1. определить владельца требования и дату последнего явного решения;
2. проверить более поздние owner corrections;
3. разделить продуктовый смысл, архитектуру, implementation и evidence;
4. найти несовместимые модели идентичности, хранения, consent, IA и готовности;
5. перенести только совместимый нормативный срез;
6. явно пометить superseded/conflict-blocked документы;
7. оставить research и implementation evidence доступными, но не объявлять их
   каноническим требованием.

## 2. Иерархия источников

При конфликте используется следующий порядок.

1. **Более позднее явное решение владельца продукта**, если оно относится к
   тому же вопросу и не было затем отменено.
2. **Канонический accepted/owner-corrected документ в `main`.**
3. **Более поздний accepted docs-only PR**, если его границы честно отделяют
   TO-BE от реализации.
4. **Фактический runtime в `main`** — источник current behavior и migration
   constraints, но не способ отменить утверждённый TO-BE молча.
5. **Implementation design / test scaffold** — нормативен только в своей
   технической области и не меняет продуктовый смысл.
6. **Исследование, consultant report, lab и preview** — evidence, а не решение.
7. **Старые mixed/integration branches** — исторические доноры; wholesale merge
   запрещён.

Дата commit или `updated_at` — лишь сигнал для проверки, а не источник истины.

## 3. Статусы реестра

| Статус | Значение |
|---|---|
| `ported` | нормативный срез перенесён в `main` без старого unrelated diff |
| `already_in_main` | актуальный смысл уже находится в каноническом документе |
| `superseded` | существует более позднее решение; старое нельзя использовать |
| `conflict_blocked` | есть критическое противоречие; merge запрещён до исправления |
| `research_evidence` | полезное исследование без owner acceptance |
| `implementation_only` | кодовый/test handoff, не продуктовый source of truth |
| `historical_donor` | старый mixed branch, разрешено точечное чтение |
| `owner_decision_required` | решение нельзя вывести из веток без владельца |

## 4. Результат ручной сверки высокорисковых веток

### 4.1 Фокус-группа и авторизация

| Источник | Найденная модель | Решение |
|---|---|---|
| PR #323, `agent/static-site-general-follow-up-audit-20260804` | приглашение открывает сайт; feedback видим, но disabled до email/Яндекса; explicit auth CTA; anonymous server feedback и silent anonymous Auth запрещены | `ported`, это последнее owner-corrected решение |
| PR #250 и ветки `docs/focus-group-release-control-*` | silent anonymous Supabase Auth, anonymous page score/NPS/text/screenshot, 12 артефактов и старый prize threshold | `superseded`; не переносить |
| PR #324 launch dashboard | checklist содержит anonymous-first допущения из старой модели | `conflict_blocked`; dashboard должен быть регенерирован после замены требований |
| текущий `docs/testing/static-site-auth-session-fixture.md` | сохранял режим `anonymous_session` для feedback/artifacts | исправлен: fixture остаётся no-mail способом получить реальную authenticated session, но не создаёт anonymous focus identity |

Критическое решение:

```text
invite / QR
  -> обычный сайт доступен без входа
  -> feedback block виден
  -> score / issue / screenshot / NPS disabled
  -> explicit email or Yandex authentication
  -> safe return to the same feedback context
  -> authenticated idempotent writes
```

Нельзя автоматически пересчитать прежнее условие `10 из 12` на новую первую
коллекцию из семи артефактов. Старые prize rules заблокированы до отдельного
owner-approved rebaseline.

### 4.2 Персонализация, профиль и владение данными

| Источник | Решение |
|---|---|
| PR #328, `docs/p13n-transport-profile-20260804` | `ported` как staged architecture: zero-backend navigation, browser projection cache, `/profil/`, Favorites и hidden recovery раздельно |
| PR #295, Yandex resilience docs | `historical_donor`: полезны capability/SOR/ack/idempotency сценарии, но старая Supabase-primary/YDB-analytics модель не переносится целиком |
| PR #270 | перенесены только отсутствовавшие extension docs: identity linking, temporal simulation, longitudinal E2E, selection-quality feedback и report template |
| PR #266 | `ported`: отдельный юридический/activation gate; это не юридическое заключение и не разрешение включить remote writes |

Зафиксированная граница:

```text
ordinary calendar/listing/event navigation:
  YDB profile requests = 0
  Supabase profile/data requests = 0

browser projection:
  default cache

remote profile/action architecture:
  staged until ownership + localization + legal gate
```

`Избранное`, hidden recovery и профиль не объединяются:

```text
Избранное        = calendar_saved + favorite_saved
Hidden recovery  = Подборки -> Помечены «не интересует»
Профиль          = account + interests + diagnostics + management
```

### 4.3 Hero Talk, онбординг и клавиатура

| Источник | Решение |
|---|---|
| стандартный onboarding v0.4 | `already_in_main`; utility onboarding и artifact/club onboarding разделены |
| PR #291 Hero Talk | `ported`: coherent narrative chains, `home_hero` и `page_end`, static served plans, no runtime LLM |
| PR #330 Keyboard V8 | `ported`: owner-corrected reading route, recovery/help model, настоящий артефакт из фиксированной коллекции, privacy-minimal telemetry |

Hero Talk не становится вторым onboarding state machine. Onboarding владеет
eligibility/competency/dismissal, Hero Talk — контекстной доставкой цепочки.

### 4.4 Волонтёры

PR #331 перенесён как docs-only target contract:

- ежедневный availability lifecycle заявок `Добро.рф`;
- отдельный matching/research слой;
- handoff отсутствующих festival-like заявок в `festival_queue` как raw URL;
- запрет фабрикации официального URL фестиваля по названию;
- label на карточках, detail content block и external application CTA;
- smart canary без hardcoded event ID.

Runtime, notebook, schema и публичный rollout этим переносом не объявляются
готовыми.

### 4.5 Favorites, reminders и Push

PR #235 перенесён точечно:

- target двухзонного `Избранного`: `Мой календарь` + `Понравилось`;
- calendar save и like независимы;
- utility reminders: T−24h и ровно один near kind;
- promotional Web Push имеет отдельный purpose/consent;
- ICS сохраняется; Postbox calendar email остаётся research; Android connector
  — prototype;
- test design не является PASS runtime.

### 4.6 Исследования и implementation branches, которые не повышены

| Источник | Статус | Причина |
|---|---|---|
| PR #252 Editorial collections | `research_evidence` | нужны sync, screenshot matrix и два product review |
| PR #286 Editorial style workbench | `research_evidence` | living workbench, окончательный tone не выбран |
| PR #314 Gastronomy data prep | `implementation_only` | owner decision store пуст, publication blocked |
| PR #296, #313, #318 prelaunch/labs | `implementation_only` | кандидаты и визуальные эксперименты, не общий TO-BE source |
| PR #226 facts-v3 handoff | `superseded` | последующая реализация выборочно вошла через PR #299 |
| PR #26 и старые umbrella/mixed branches | `historical_donor` | большой разошедшийся diff и устаревшие решения |
| PR #38 medallions integration | `historical_donor` | implementation readiness и visual evidence; общий продуктовый контракт уже живёт в main |

## 5. Что перенесено этим срезом

### Новые канонические пакеты

- `docs/features/hero-talk/*`;
- `docs/features/static-site-pages/hero-talk-release-track.md`;
- `docs/features/static-site-pages/keyboard-event-navigation-v8-*.md`;
- `docs/testing/keyboard-event-navigation-scenarios.v2.yml`;
- `docs/features/static-site-pages/volunteer-recruitment/*`;
- `docs/features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md`;
- `docs/features/static-site-pages/user-profile.md`;
- `docs/testing/personalization-transport-profile-test-plan.md`;
- отсутствовавшие extension docs персонализации;
- `personalization-legal-release-gate-rf.md`;
- event reminder strategy/test companions.

### Исправленные канонические документы

- фокус-группа и её release companions;
- auth session fixture;
- индекс документации;
- machine-readable route map.

## 6. Автоматический branch audit

`scripts/audit_to_be_documentation.py` выполняет полный advisory-инвентарь
remote branches:

1. перечисляет `refs/remotes/origin/*`;
2. строит merge-base с `origin/main`;
3. находит изменённые Markdown/YAML/JSON в `docs/`;
4. извлекает heading, status/date hints и requirement-like markers;
5. показывает absent/modified paths относительно main;
6. связывает известные branches с ручным disposition ledger;
7. выпускает Markdown и JSON.

Скрипт **не выбирает победителя автоматически** и не делает branch merge.
Неизвестная branch остаётся `unclassified_review_required`. Weekly workflow
сохраняет полный отчёт как artifact; это защита от появления нового брошенного
TO-BE-документа, а не автоматическая канонизация.

## 7. Release/governance gates

- Принятое TO-BE-решение должно быть в `main` до implementation handoff.
- PR body не заменяет документ.
- Новый feature-doc обязан быть зарегистрирован в `docs/routes.yml`.
- При конфликте owner decision и старого implementation doc старый документ
  получает явный superseded marker или redirect-stub.
- Нельзя менять продуктовую модель под видом синхронизации документации.
- Нельзя переносить runtime readiness claims без terminal evidence.
- Unclassified requirement-bearing branch блокирует утверждение «документация
  полностью консолидирована», но не блокирует unrelated code change.

## 8. Следующий регулярный процесс

1. Weekly branch audit.
2. Немедленный audit после нового owner decision или крупного docs-only PR.
3. Review `unclassified_review_required` вручную.
4. Port accepted semantic slice в свежую main-based ветку.
5. Оставить superseded/historical disposition в этом реестре.
6. После merge закрыть или пометить старый PR, не удаляя evidence, пока он нужен
   для истории.
