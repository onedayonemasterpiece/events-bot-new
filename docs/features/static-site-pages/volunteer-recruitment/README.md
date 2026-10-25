# Волонтёрские заявки: связь с событиями и фестивалями

> **Статус:** принятый продуктовый и технический контракт; runtime, миграции и UI ещё не реализованы.
> **Публичный scope:** только связь внешней волонтёрской заявки с уже существующим `Event` или `festival_calendar_item`, плюс discovery-seed в существующую фестивальную очередь.
> **Не является:** каталогом всех волонтёрских возможностей, заменой `Добро.рф`, собственной формой подачи заявки или автоматическим созданием фестивалей.

Связанные документы:

- [Event Page Product & Design Spec](../event-page-product-design.md);
- [Event token medallions](../event-token-medallions.md);
- [Festival Web Research](../../source-parsing/sources/festival-parser/preproduction-web-research.md);
- [Festival data model v2](../../festivals/data-model-v2.md);
- [Kaggle static-site builder](../../../operations/kaggle-static-site-builder.md);
- [План автотестов](test-plan.md);
- [Implementation handoff](implementation-handoff.md).

## 1. Итоговое продуктовое решение

Фича состоит из четырёх поверхностей, но использует одну связь:

1. на карточке события или фестиваля показывается простой текстовый лейбл **«Требуются волонтёры»**;
2. на detail-странице события факт повторяется в quick-read medallion row;
3. в контентной части detail-страницы появляется объясняющий блок и кнопка перехода на исходную заявку;
4. `/volontery/` показывает только связанные карточки существующих событий и фестивалей.

Постоянные волонтёрские корпуса допускаются только как отдельный редакционно управляемый page-end блок. Они не входят в автоматическую выдачу, не смешиваются с событийными заявками и не создают `VolunteerOpportunity` membership страницы.

## 2. Главный end-to-end поток

```text
Добро.рф: региональная выдача открытых заявок
  -> ежедневный Playwright/HTTP monitor
  -> нормализованная VolunteerOpportunity
  -> availability state OPEN/CLOSED/EXPIRED/UNKNOWN
  -> BGE shortlist по текущим Event + festival_calendar_item
  -> bounded LLM adjudication с source evidence
       -> exact Event match
       -> exact FestivalEdition match
       -> unmatched festival-like seed
       -> unrelated / insufficient evidence
  -> exact match: public volunteer link projection
  -> unmatched festival-like seed: existing festival_queue
  -> StaticSiteBuilder rebuild only when public projection changed
```

Availability-проверка и semantic matching — разные стадии. BGE и LLM не определяют, открыта ли заявка: это делает источник и детерминированный lifecycle parser.

## 3. Ежедневный мониторинг `Добро.рф`

### 3.1. Discovery

Один CPU notebook/kernel `kaggle/VolunteerMonitor/` запускается раз в сутки и использует тот же код, который работает в production monitor и live canary.

Он:

1. открывает региональную поисковую поверхность `Добро.рф`;
2. выбирает Калининградскую область;
3. включает выдачу с доступными вакансиями;
4. проходит пагинацию / `Показать ещё` до terminal state;
5. собирает canonical `/event/<id>` URLs;
6. загружает каждую новую и каждую ранее открытую заявку;
7. сохраняет bounded source snapshot, extraction result и hashes.

Если соглашение с добровольческим центром ограничивает конкретные host/path/frequency, эти параметры фиксируются в versioned source config. Согласие одного владельца не распространяется автоматически на другие домены.

### 3.2. Availability state

```text
OPEN       — источник явно допускает подачу, срок не истёк;
CLOSED     — источник явно сообщает о закрытии или форма недоступна как закрытая;
EXPIRED    — подтверждённый application deadline прошёл;
UNKNOWN    — сеть, DOM или parser не позволили принять достоверное решение.
```

Правила публикации:

- `CLOSED` и `EXPIRED` снимают публичную связь в том же успешном apply;
- один `UNKNOWN` не превращается в ложное закрытие;
- если последняя успешная проверка старше 36 часов, публичная проекция скрывается fail-closed до восстановления источника;
- HTTP 200 и наличие старой кнопки сами по себе не являются доказательством `OPEN`;
- unchanged availability check не вызывает BGE/LLM;
- semantic rematch запускается только при изменении `semantic_hash`, target catalog identity или match policy version.

## 4. Matching с существующим каталогом

### 4.1. Candidate documents

Волонтёрский документ включает только source-grounded поля:

```text
title
organizer
city / venue
application dates
volunteer shift dates
parent event dates
roles
source excerpt
canonical URL
```

Event document использует канонические title, description/search digest, date range, city, venue, festival identity и organizers. Festival document использует series/edition title, aliases, date range, place, category и source-grounded description.

### 4.2. BGE shortlist

- модель: существующий pinned `BAAI/bge-m3` contract из `site/scripts/static_event_bge.py`;
- отдельная реализация encoder запрещена;
- event/festival vectors reuse-ятся по content hash;
- кодируются только новые/изменившиеся volunteer documents и изменившиеся target documents;
- на LLM передаются максимум `8` Event и `5` FestivalEdition candidates;
- BGE является recall stage и никогда не публикует relation самостоятельно.

### 4.3. LLM adjudication

LLM получает volunteer source evidence и bounded shortlist. Допустимые решения:

```text
MATCH_EVENT
MATCH_FESTIVAL_EDITION
FESTIVAL_DISCOVERY_SEED
NO_RELEVANT_TARGET
NEEDS_REVIEW
```

Для publishable match обязательны:

- совместимые город/площадка;
- пересекающиеся или логически согласованные event/shift dates;
- совпадение названия, алиаса, organizer или programme relation;
- relation type `SAME_EVENT | SUBEVENT_OF | RECRUITMENT_FOR`;
- exact source quotes для решающих полей;
- отсутствие конфликта года/редакции.

LLM confidence без этих gates не является решением.

## 5. Как заявка обогащает фестивальную очередь

### 5.1. Сырая заявка входит в очередь до подтверждения фестиваля

Фестивальная очередь и существует для разбора неоднозначных источников. Поэтому `FESTIVAL_DISCOVERY_SEED` не обязан уже иметь официальный сайт фестиваля и не обязан быть стопроцентно подтверждённым фестивалем.

Создаётся существующий `FestivalQueueItem`:

```json
{
  "source_kind": "url",
  "source_url": "https://dobro.ru/event/<id>",
  "festival_name": "source-local name hint or null",
  "status": "pending",
  "source_text": "bounded exact excerpt with dates, city, organizer and roles",
  "signals_json": {
    "origin": "volunteer_monitor",
    "volunteer_opportunity_id": 123,
    "festival_hint": "candidate only",
    "city": "Калининград",
    "event_start": "2026-08-20",
    "event_end": "2026-08-23",
    "organizer": "...",
    "semantic_hash": "..."
  },
  "dedup_links_json": [
    "https://dobro.ru/event/<id>",
    "any explicit outbound link found on the source page"
  ]
}
```

Queue insertion is idempotent by normalized source URL + volunteer opportunity identity + edition/date hint. Повторный daily monitor не создаёт дубли.

### 5.2. Официальный сайт не выводится из названия

Из названия заявки нельзя получить официальный URL детерминированно. Контракт запрещает:

```text
festival name -> guessed domain
festival name -> fabricated official URL
first search result -> official source without evidence
```

Порядок источников:

1. explicit outbound links самой заявки;
2. уже известные source URLs/aliases существующей festival series;
3. Festival Web Research с настоящими search/URL tools;
4. operator-supplied URL при ручном review.

Существующий Festival Web Research является владельцем интернет-discovery. Volunteer Monitor только поставляет seed: название, даты, город, organizer, raw URL и цитаты.

Текущий важный blocker: Festival Web Research реализован как preproduction collect-only lane, но provider eligibility заблокирована. Пока blocker не снят:

- queue item остаётся `pending/review/needs_research`;
- официальный URL не выдумывается;
- `festival_calendar_item` автоматически не создаётся;
- оператор может добавить verified official URL вручную;
- exact match с уже существующим фестивалем продолжает работать независимо от web research.

### 5.3. Apply в фестивальный каталог

Только approved Festival Web Research candidate может:

- создать или обновить festival edition projection;
- сохранить официальный/авторитетный source destination;
- отправить programme subjects в Smart Update;
- связать исходную volunteer application с новым `festival_calendar_item`.

Сам `VolunteerOpportunity` никогда напрямую не пишет `festival_calendar_item`.

## 6. UI contract

### 6.1. Карточки событий и фестивалей

Показывается компактный, неинтерактивный лейбл:

```text
Требуются волонтёры
```

Правила:

- только при `OPEN`, свежей проверке и accepted match;
- размещение — в текстовой metadata/status зоне карточки, не поверх афиши;
- лейбл не является ссылкой и не меняет основную destination карточки;
- одинаковая формулировка на Event и Festival cards;
- `CLOSED`, `EXPIRED`, stale или unresolved relation удаляют лейбл при следующей валидной projection.

### 6.2. Медальон на detail-странице

`Требуются волонтёры` становится **fact pill medallion** только на event detail, потому что это важный source-grounded факт конкретного события.

Он:

- не является organizer/festival identity medallion;
- всегда Secondary/InlineSlot;
- никогда не занимает TopSlot;
- не появляется как круглый listing overlay или external identity rail;
- не является кликабельным CTA;
- скрывается вместе с закрытием заявки.

Это соответствует текущему медальонному контракту: medallion быстро сообщает факт, но не заменяет content block и действие.

### 6.3. Контентный блок и кнопка

На event detail блок вставляется после short explanation/search digest и до полного description:

```text
Требуются волонтёры
Организаторы ищут помощь на регистрации и навигации. Заявки до 12 августа.
[Открыть заявку]
Источник: Добро.рф · проверено сегодня
```

Правила CTA:

- `href` всегда равен проверенному canonical application URL;
- сайт не принимает заявку и не обещает принятие пользователя;
- кнопка не становится primary hero CTA и не конкурирует с билетами/регистрацией события;
- внешний переход получает обычные security/referrer safeguards и analytics event;
- deadline/roles показываются только при source-grounded extraction.

### 6.4. `/volontery/`

Страница содержит:

1. связанные Event cards;
2. связанные FestivalEdition cards;
3. у каждой карточки отдельную вторичную ссылку `Открыть заявку`;
4. page-end редакционный блок постоянных волонтёрских корпусов;
5. опциональный Hero Talk page-end сценарий о том, как начать помогать.

Автоматическая membership страницы состоит только из accepted links к существующим Event/FestivalEdition. Постоянные корпуса управляются отдельным curated config и не смешиваются со счётчиком актуальных заявок.

## 7. Минимальная модель данных

### `volunteer_opportunity`

```text
id
source_type
source_external_id
canonical_url UNIQUE
title
organizer_name
city
venue
application_open_at
application_close_at
shift_start_at
shift_end_at
parent_event_start_at
parent_event_end_at
availability_status
roles_json
source_excerpt
availability_hash
semantic_hash
first_seen_at
last_checked_at
last_successful_check_at
closed_at
```

### `volunteer_target_link`

```text
id
opportunity_id
target_kind              # event | festival_calendar_item
target_id
relation_type
match_status             # candidate | accepted | rejected | stale
match_policy_version
match_evidence_json
reviewed_by
reviewed_at
UNIQUE(opportunity_id, target_kind, target_id)
```

### `volunteer_monitor_run`

```text
run_uid UNIQUE
mode                      # production | live_canary | fixture
started_at
completed_at
status
source_pages_seen
opportunities_seen
open_count
closed_count
matched_event_count
matched_festival_count
festival_seed_count
result_sha256
last_error
```

Большие HTML/screenshots не сохраняются в core SQLite. Там остаются bounded excerpts, hashes и operational truth.

## 8. Расписание и инфраструктура

Рабочее расписание `Europe/Kaliningrad`:

```text
14:15  существующий daytime source parsing
15:20  Volunteer Monitor: discover + availability + changed semantic matching
16:30  существующая festival queue разбирает новые URL seeds
+15m   StaticSiteBuilder только если volunteer public projection изменилась
```

Исполнение:

- Fly: scheduler, durable job state, immutable SQLite snapshot, trusted result validation и transactional apply;
- Kaggle CPU: Playwright fetch, extraction, BGE shortlist и bounded LLM calls;
- Fly SQLite: canonical opportunity/link/run state;
- Object Storage/CDN: только generated public projection через существующий StaticSiteBuilder;
- GitHub Actions: PR tests и scheduled/read-only live canary orchestration, не production state owner.

Один notebook обслуживает discovery, recheck и matching. Отдельный notebook на каждый источник или тип match запрещён.

## 9. Public projection

StaticSiteBuilder получает один ID-bound файл:

```text
site/src/data/volunteer-links-v1.json
```

Минимальный контракт:

```json
{
  "schema_version": "volunteer-links-v1",
  "generated_at": "...",
  "source_run_uid": "...",
  "event_links": [{
    "event_id": 123,
    "application_url": "https://dobro.ru/event/456",
    "label": "Требуются волонтёры",
    "deadline": "2026-08-12",
    "roles": ["регистрация", "навигация"],
    "last_verified_at": "..."
  }],
  "festival_links": [{
    "calendar_year": 2026,
    "festival_slug": "...",
    "application_url": "https://dobro.ru/event/789",
    "label": "Требуются волонтёры",
    "last_verified_at": "..."
  }]
}
```

Astro не выполняет повторный semantic matching и не выводит volunteer state из prose.

## 10. Fail-closed правила

Публичная связь запрещена, если:

- заявка не `OPEN`;
- последняя успешная проверка старше 36 часов;
- application URL не прошёл canonical/reachability check;
- relation не accepted;
- год/даты конфликтуют;
- target отсутствует в текущей публичной projection;
- result hashes/identity не совпадают;
- source monitor завершился частично;
- BGE/LLM/provider failure пытается заменить last-good пустым результатом.

Сетевой отказ удерживает last-good только в пределах freshness TTL; закрытие, подтверждённое источником, снимает связь сразу.

## 11. Граница первой реализации

P0 включает:

- один источник `Добро.рф` по согласованному region search scope;
- ежедневный monitor и recheck;
- matching с Event и `festival_calendar_item`;
- raw FestivalQueue seed для отсутствующих фестивалей;
- card label, event detail medallion, content block/button;
- `/volontery/` из связанных Event/Festival cards;
- smart live canary и deterministic fixture suite.

P0 не включает:

- универсальный каталог добровольчества;
- профили/заявки/учёт часов волонтёра;
- automatic festival creation;
- scraping произвольных поисковиков;
- новый Search API;
- параллельный BGE model stack;
- автоматическую публикацию постоянных волонтёрских корпусов.
