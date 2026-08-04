# Volunteer Monitor: free web discovery and Kaggle control plane

> Статус: дополнение к `volunteer-recruitment/README.md` и
> `implementation-handoff.md`. Исполняемый source-monitor skeleton готов;
> fixture contract проверен, live acceptance ожидает GitHub/Kaggle canary.

## 1. Что делает сам монитор

Ежедневный монитор не использует web search для discovery заявок. Он читает
согласованную региональную поверхность `Добро.рф`, проходит пагинацию и
проверяет каждую новую либо ранее открытую заявку.

```text
Playwright search surface
  -> canonical /event/<id> URLs
  -> HTTP/HTML + JSON-LD extraction
  -> OPEN / CLOSED / EXPIRED / UNKNOWN
  -> availability_hash
  -> semantic_hash
```

BGE/LLM не определяют доступность. Они запускаются позже и только для нового или
изменившегося `semantic_hash`.

## 2. Поиск официального источника неизвестного фестиваля

Web discovery разрешён только для `FESTIVAL_DISCOVERY_SEED`, когда нет:

1. точного существующего Event/FestivalEdition match;
2. явной внешней ссылки в заявке;
3. уже утверждённого source в festival registry/source graph.

Один `source_resolution_hash` получает максимум один primary search request.
Результаты поиска — только кандидаты. Каждый URL затем повторно загружается
локальным HTTP/Playwright verifier и требует:

- совпадения series/edition identity;
- совместимого года/дат, города/площадки или организатора;
- source-local exact quote и content hash;
- отсутствия роли агрегатора/СМИ/билетной страницы;
- operator approval для нового official destination.

### Рекомендуемый бесплатный порядок

#### A. Gemini 2.5 Flash-Lite + Google Search grounding

Это наиболее близкая замена Antigravity как `LLM + web search`. Использовать
отдельный quota scope и только публичные festival hints. Контакты волонтёров,
телефоны и email в запрос не передаются.

```text
provider = gemini_google_search
model = gemini-2.5-flash-lite
max_requests_per_unresolved_group = 1
```

Host извлекает только grounded URLs; URL из свободного текста ответа без tool
evidence отклоняется.

#### B. Tavily Search API

Детерминированный search API для получения URL/snippets. Один basic search на
новый unresolved lead, затем локальная проверка и обычный LLM verifier. Это
хороший независимый fallback при низком объёме новых фестивалей.

#### C. Brave Search API

Допустимый резерв при согласии завести billing profile. Бесплатный месячный
кредит покрывает примерно 1 000 Search requests, но регистрация требует карту.

#### D. Operator-owned SearXNG

Не имеет платы за собственный API, но требует хостинга и зависит от доступности
upstream engines. Используется как challenger/fallback, не как единственный
production authority.

#### E. Common Crawl

Бесплатен и полезен для поиска старых доменов/архивных festival series, но не
доказывает текущий официальный сайт или актуальную редакцию.

### Не выбранные варианты

- Antigravity: текущий project/provider permission blocker;
- Google Programmable Search: не использовать как новую долгосрочную основу;
- Yandex Search API: технически подходит для русскоязычной выдачи, но это PAYG,
  а пользовательское ограничение сейчас — только бесплатные варианты;
- scraping HTML обычных поисковиков напрямую: хрупкий и юридически/операционно
  нежелательный production dependency.

## 3. Kaggle и GitHub Actions

### Рекомендуемая архитектура

```text
Production schedule / durable state: Fly JobOutbox
Compute/browser/BGE batch:          Kaggle CPU kernel
Manual/read-only canary:            GitHub Actions workflow_dispatch
Evidence/log download:              GitHub Actions artifacts
Production apply:                   только trusted Fly runner
```

GitHub Actions не должен становиться владельцем production SQLite. Он удобен
как доступный control plane: staging immutable kernel source, `kaggle kernels
push`, polling, output download, hash validation и artifact retention.

### Секреты

Владельцу репозитория нужно один раз создать GitHub Environment
`volunteer-monitor-canary` и добавить один environment secret:

```text
KAGGLE_API_TOKEN
```

Это текущий token-based способ аутентификации Kaggle. Legacy-пара
`KAGGLE_USERNAME` / `KAGGLE_KEY` для нового контура не требуется.

Дополнительные search secrets добавляются только для выбранных providers:

```text
GEMINI_API_KEY_VOLUNTEER_SEARCH
TAVILY_API_KEY
BRAVE_SEARCH_API_KEY
```

Non-secret variables:

```text
VOLUNTEER_KAGGLE_KERNEL_SLUG=eventsbot/kenigevents-volunteer-monitor
VOLUNTEER_MONITOR_PERMISSION_REFERENCE
VOLUNTEER_SEARCH_PROVIDER
SEARXNG_ENDPOINT
```

`kaggle.json`, API keys и production DB credentials не коммитятся. Кодовый
агент может создать workflow и ссылки на secret names, но не может безопасно
придумать или восстановить значения: их вводит владелец аккаунта.

### Как запускать

Workflow должен быть в default branch, после чего доступен:

- из GitHub Actions UI;
- через `gh workflow run`;
- через GitHub REST workflow dispatch.

В поставленном skeleton есть два ручных режима:

- `github`: быстрый Playwright canary прямо на GitHub runner;
- `kaggle`: staging, запуск Kaggle kernel, polling, скачивание и проверка
  `result_sha256`.

Для production после live acceptance предпочтительнее использовать уже
существующий Fly→Kaggle runner pattern и оставить GitHub Actions read-only
canary/diagnostic контуром.

## 4. Acceptance перед продолжением

1. Fixture suite green.
2. GitHub direct live canary доказывает региональный фильтр, available-vacancy
   filter и terminal pagination.
3. Kaggle run выдаёт тот же schema contract и hash-valid receipt.
4. Закрытая заявка определяется не по HTTP 200, а по source state/deadline.
5. DOM regression не превращается в `0 opportunities / success`.
6. Только после этого добавляются persistence, BGE shortlist, festival handoff
   и public projection.
