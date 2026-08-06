# Volunteer Monitor: web discovery и Kaggle control plane

> **Статус, 2026-08-06:** read-only source monitor и private Kaggle canary выполнены на реальной инфраструктуре. Этот документ фиксирует проверенный, а не предполагаемый способ запуска.

## 1. Разделение задач

Ежедневный монитор `Добро.рф` не использует интернет-поиск для обнаружения заявок. Он читает согласованную региональную поверхность, проходит вкладку `Вакансии`, сохраняет точные vacancy/application identities и определяет source lifecycle.

```text
Playwright regional vacancy inventory
  -> event_id + vacancy_id + exact application URL
  -> parent event enrichment
  -> OPEN / CLOSED / EXPIRED / UNKNOWN
  -> availability_hash + semantic_hash
```

BGE и LLM не участвуют в определении доступности. Они используются позже только для matching нового или семантически изменившегося документа с существующими `Event` и `festival_calendar_item`.

## 2. Как снимается закрытая заявка

Production recheck должен сравнивать новый полный inventory с ранее открытыми vacancy identities.

```text
vacancy присутствует в успешно завершённой вкладке Вакансии
  -> OPEN

ранее OPEN vacancy отсутствует в полном новом inventory
  -> больше не OPEN
  -> detail/deadline evidence уточняет CLOSED либо EXPIRED

DOM/transport failure
  -> UNKNOWN
  -> не выдавать за закрытие
```

`CLOSED` и `EXPIRED` — разные причины, но одинаковый публичный результат: лейбл и кнопка снимаются в следующем успешном apply. Если успешной проверки нет более 36 часов, projection скрывается fail-closed. Exact `CLOSED` покрывается fixture; реальные исторические страницы в статическом HTTP часто дают доказуемый `EXPIRED`, тогда как динамическая надпись `Набор закрыт` не присутствует в исходном HTML.

## 3. Поиск официального источника неизвестного фестиваля

Web discovery запускается только для `FESTIVAL_DISCOVERY_SEED`, когда одновременно нет:

1. точного Event/FestivalEdition match;
2. явной внешней ссылки в заявке;
3. уже утверждённого URL серии или организатора.

Порядок:

```text
explicit outbound URL из заявки
  -> existing festival/organizer registry
  -> один bounded grounded web-search request
  -> независимая загрузка candidate URLs
  -> source-role / edition / date verification
  -> operator approval при неоднозначности
```

Search output — только candidate set. Он никогда напрямую не становится official destination.

### Бесплатные провайдеры

Primary:

```text
Gemini 2.5 Flash-Lite + Google Search grounding
max 1 request на unresolved seed
max 8 grounded URLs
free-form prose и неграундированные URL отбрасываются
```

Fallback:

```text
Tavily Researcher free tier
search_depth=basic
max_results=8
include_answer=false
include_raw_content=false
```

Optional infrastructure fallback:

```text
operator-owned SearXNG JSON endpoint
```

Не использовать как production authority:

- угаданный домен;
- первый поисковый результат без verification;
- парсинг HTML обычной поисковой выдачи;
- Common Crawl как доказательство текущей редакции;
- LLM confidence без source evidence.

## 4. Проверенная Kaggle-идентичность проекта

Первоначальная гипотеза `eventsbot + kaggle 2.x + KAGGLE_API_TOKEN` оказалась неверной для этого репозитория.

Рабочие project kernels (`CherryFlash`, `TelegramMonitor` и другие) принадлежат Kaggle account:

```text
zigomaro
```

Репозиторный runtime использует:

```text
KAGGLE_USERNAME + KAGGLE_KEY
kaggle 1.8.x API client
KaggleApi.authenticate()
```

Volunteer Monitor приведён к тому же контракту.

### Canonical canary workflow

```text
.github/workflows/volunteer-monitor-kaggle.yml
```

Он:

1. запускается вручную через `workflow_dispatch`;
2. использует Environment `volunteer-monitor-canary`;
3. берёт существующий `KAGGLE_KEY`, а при его отсутствии безопасно нормализует значение Environment secret `KAGGLE_API_TOKEN` в legacy-compatible key;
4. получает owner из `KAGGLE_USERNAME`, default `zigomaro`;
5. использует только tail из `VOLUNTEER_KAGGLE_KERNEL_SLUG`, поэтому ошибочный owner в старом значении variable не переносится в kernel identity;
6. устанавливает доказанную версию `kaggle==1.8.4`;
7. делает read-only auth preflight;
8. собирает self-contained private kernel;
9. выполняет push, polling, output download;
10. проверяет result schema, receipt status, SHA-256 и полное source accounting;
11. сохраняет private GitHub artifact на 14 дней.

Direct browser canary и Kaggle canary разделены:

```text
volunteer-monitor-smoke.yml             fixture + scheduled/manual direct
volunteer-monitor-live-acceptance.yml   bounded PR direct + non-open probe
volunteer-monitor-kaggle.yml            manual private Kaggle acceptance
```

## 5. Принятое Kaggle evidence

```text
GitHub Actions run: 31079828744
job:                92545879373
kernel:             zigomaro/kenigevents-volunteer-monitor
kernel version:     1
run_uid:            volunteer-monitor-20260806T070946Z
status:             SUCCESS
```

Execution:

```text
started_at:         2026-08-06T07:09:46.700291Z
completed_at:       2026-08-06T07:14:27.870758Z
source_pages_seen:  24
opportunities:      24
OPEN:               24
warnings:           0
outside-region:     0
source errors:      0
```

Discovery receipt:

```text
region_proven:           true
available_filter_proven: true
parent URLs discovered:  101
vacancies discovered:    159
load-more clicks:          5
```

Integrity:

```text
result file SHA-256: 58808e44af5cac4e7577b7dc817b9344fd37771fec6c11083ca5dc28f0ebae44
internal result hash: 843b5fd966bfab88c467101ca4db88e541449ed894711838791ecbee5fcd592a
GitHub artifact ID:   8959127103
artifact ZIP SHA-256: 2a41a8a112af6bda3dc14751e2eb4464c70c0ff70ce1d031d671e2ee21d15c17
```

Downloaded output contains:

```text
volunteer-monitor-result.json
volunteer-monitor-receipt.json
volunteer-monitor-evidence/discovery-receipt.json
kenigevents-volunteer-monitor.log
volunteer-monitor-runtime.zip
```

## 6. Execution ownership

### GitHub Actions

Owns только:

- fixtures;
- read-only live acceptance;
- manual Kaggle canary;
- bounded private evidence.

Не пишет production SQLite.

### Production

```text
Fly durable JobTask / immutable input
  -> private Kaggle CPU batch
  -> hash-validated result adoption
  -> Fly transactional SQLite apply
  -> StaticSiteBuilder only when public projection changed
```

Fly остаётся владельцем last-good state, retries, freshness TTL и production apply. GitHub Actions не является production scheduler.

## 7. Текущие настройки

```text
GitHub Environment: volunteer-monitor-canary
Environment secret: KAGGLE_API_TOKEN
Repository variable: VOLUNTEER_KAGGLE_CANARY_ENABLED=true
Repository variable: VOLUNTEER_KAGGLE_KERNEL_SLUG=<owner ignored>/kenigevents-volunteer-monitor
Repository/Environment variable: KAGGLE_USERNAME=zigomaro, либо workflow default
```

Значения secrets не печатаются и не сохраняются в artifacts. `kaggle.json` не коммитится.

## 8. Acceptance status

Выполнено:

- fixture parser/matcher-provider policy suite;
- реальный GitHub-hosted Chromium source canary;
- source accounting;
- exact application URL preservation;
- source-backed OPEN;
- source-backed non-public lifecycle (`EXPIRED`) плюс exact CLOSED fixture;
- private Kaggle authentication;
- kernel push/run/complete;
- output/receipt/SHA validation.

Следующий этап — не инфраструктурный canary, а production implementation: SQLite state, daily inventory diff, BGE shortlist, LLM adjudication, `festival_queue` handoff и Astro projection.
