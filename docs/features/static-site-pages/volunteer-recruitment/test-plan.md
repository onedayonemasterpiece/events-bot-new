# Автотесты волонтёрского мониторинга и matching

> **Статус:** обязательный test contract для реализации P0.
> **Цель:** проверять не только отдельные функции, но и реальный путь `Добро.рф → match или festival seed → public projection → UI → снятие после закрытия`.

## 1. Почему одного фиксированного event ID недостаточно

Фиксированный тестовый pair быстро устаревает: заявка закрывается, событие проходит, title меняется. Но полностью live-тест также нельзя использовать как единственный CI gate: в конкретный день в регионе может не быть ни одной открытой заявки, а внешний DOM может временно не отвечать.

Поэтому используются три взаимодополняющих уровня:

1. **детерминированные source fixtures** — обязательны на каждом PR;
2. **умный live discovery canary** — сам ищет текущий реальный specimen;
3. **browser projection E2E** — строит временный сайт из specimen, выбранного canary, и проверяет публичный UI.

Live canary не заменяет fixtures, а fixtures не заменяют live discovery.

## 2. Тестовые режимы

### 2.1. `fixture`

Работает без сети и provider calls. В репозитории хранятся bounded snapshots реальных классов источника, а не полные чужие страницы:

- открытая заявка с exact Event match;
- открытая заявка с exact FestivalEdition match;
- открытая festival-like заявка, фестивала ещё нет в каталоге;
- закрытая заявка;
- просроченная заявка;
- постоянный волонтёрский корпус / обучение, которое не должно стать Event/Festival link;
- разные годы одной festival series;
- network/DOM parser failure receipt.

Fixture содержит source URL, minimal excerpt, normalized fields, content hashes и expected result.

### 2.2. `live_canary`

Read-only запуск того же production-кода против текущей публичной выдачи `Добро.рф` и текущего immutable event/festival catalog snapshot.

Canary не имеет hardcoded event/festival ID. Он:

1. обнаруживает текущие открытые региональные заявки;
2. строит BGE shortlist по текущему каталогу;
3. запускает тот же bounded LLM adjudicator;
4. независимо проверяет date/city/evidence gates;
5. выбирает один лучший specimen в следующем порядке:
   - accepted Event match;
   - accepted FestivalEdition match;
   - unmatched `FESTIVAL_DISCOVERY_SEED`;
6. сохраняет `canary-selection.json` с source URL, selected target/queue disposition, exact evidence и hashes.

### 2.3. `browser_e2e`

Использует `canary-selection.json` или fixture specimen, формирует временный `volunteer-links-v1.json`, строит noindex test site и проверяет DOM/UX.

## 3. Что означает «умный» live canary

Тест не ищет заранее известное событие. Он должен сам найти текущий проверяемый путь и доказать, что заявка не потерялась.

Для каждой sampled open application допустим ровно один terminal disposition:

```text
MATCH_EVENT
MATCH_FESTIVAL_EDITION
FESTIVAL_DISCOVERY_SEED
NO_RELEVANT_TARGET
NEEDS_REVIEW
```

Запрещённое состояние:

```text
open source row -> silently dropped
```

Главный инвариант live canary:

```text
каждая успешно извлечённая sampled заявка имеет сохранённый disposition,
evidence и причину; минимум один наиболее сильный specimen проходит
полный downstream test path, если live supply существует.
```

## 4. Поведение при отсутствии live supply

Ноль открытых заявок — не ошибка кода. Поэтому результаты разделены:

```text
PASS_LIVE_EVENT_MATCH
PASS_LIVE_FESTIVAL_MATCH
PASS_LIVE_FESTIVAL_SEED
WARN_NO_LIVE_SUPPLY
FAIL_DISCOVERY_BROKEN
FAIL_EXTRACTION_INCOMPLETE
FAIL_DROPPED_CANDIDATE
FAIL_MATCH_CONTRACT
FAIL_UI_CONTRACT
```

`WARN_NO_LIVE_SUPPLY`:

- не делает PR красным;
- создаёт operator alert и artifact;
- browser E2E использует последний hash-bound real fixture;
- не считается доказательством live matching.

Это не ослабляет тест: внешний supply и работоспособность системы измеряются раздельно.

## 5. Независимые gates против self-confirming test

Нельзя считать тест успешным только потому, что тот же LLM, который создал match, подтвердил собственный ответ.

После LLM выполняются deterministic checks:

- target существует в current catalog snapshot;
- дата/диапазон не конфликтуют;
- city/venue совместимы или relation явно `SUBEVENT_OF`;
- source quote действительно присутствует в сохранённом snapshot hash;
- festival edition year не расходится;
- application URL canonical и доступен;
- relation schema/version валидны;
- top candidate не был исключён до LLM фильтром ошибочно;
- BGE shortlist содержит owner-reviewed positive в golden evaluation suite.

Live specimen и весь shortlist сохраняются в artifact для последующего отсмотра.

## 6. Обязательные unit/contract тесты

Предлагаемые файлы:

```text
tests/test_volunteer_source_parser.py
tests/test_volunteer_availability.py
tests/test_volunteer_matching.py
tests/test_volunteer_festival_handoff.py
tests/test_volunteer_projection.py
```

### Source/discovery

- pagination обрабатывается до terminal state;
- duplicate `/event/<id>` URLs схлопываются;
- region filter не теряется после `Показать ещё`;
- unexpected DOM даёт explicit extraction error, а не пустой success;
- source permission config ограничивает hosts/paths/frequency;
- canonical URL normalization не склеивает разные applications.

### Availability

- explicit open → `OPEN`;
- `Набор закрыт` → `CLOSED`;
- deadline passed → `EXPIRED`;
- HTTP 200 со stale CTA не становится `OPEN` без source evidence;
- первый transport failure → `UNKNOWN`, last-good временно удерживается;
- freshness >36h → public link скрывается;
- восстановление источника возвращает link без ручной DB-правки;
- unchanged status не меняет `semantic_hash` и не вызывает BGE/LLM.

### Matching

- exact title/date/city → Event match;
- volunteer title является programme/subevent → `SUBEVENT_OF` parent Event;
- festival alias/date/place → FestivalEdition match;
- same series, different year → rejection;
- generic volunteer team/training → no Event/Festival link;
- BGE top-N является shortlist, но не publication decision;
- LLM abstention сохраняется как review;
- source quote mismatch блокирует accepted relation;
- changed event hash инвалидирует только затронутые relations.

### Festival handoff

- unmatched festival-like source создаёт ровно один `FestivalQueueItem`;
- queue row хранит raw `Добро.рф` URL, exact excerpt, dates/city/organizer hints;
- official URL не фабрикуется;
- explicit outbound official link попадает в `dedup_links_json`;
- повторный monitor run не создаёт duplicate queue row;
- false-positive seed разрешён как pending/review, но не создаёт `festival_calendar_item`;
- Festival Web Research blocker оставляет queue item recoverable;
- approved research result может связать исходную opportunity с созданной edition.

### Projection

- только `OPEN + fresh + accepted` входит в `volunteer-links-v1.json`;
- отсутствующий Event/Festival ID блокирует build или исключается с explicit error согласно release mode;
- provider failure не публикует empty replacement вместо last-good;
- closed application удаляется из следующей projection;
- event и festival link counts/hashes совпадают с result receipt.

## 7. Smart live canary workflow

Предлагаемый workflow:

```text
.github/workflows/volunteer-monitor-canary.yml
```

Расписание:

```text
cron: один раз в сутки после production Volunteer Monitor
workflow_dispatch: ручной повтор / диагностика
```

Workflow не пишет production SQLite. Он:

1. получает immutable, privacy-safe current event/festival catalog artifact;
2. запускает `VolunteerMonitor` в `live_canary` mode;
3. проверяет result schema/hashes;
4. выбирает dynamic specimen;
5. строит ephemeral static candidate;
6. запускает Playwright UI assertions;
7. сохраняет bounded artifacts и summary;
8. отправляет alert только для функциональных failures или продолжительного `WARN_NO_LIVE_SUPPLY`.

## 8. Browser assertions

Предлагаемый test:

```text
site/scripts/volunteer-links.e2e.mjs
```

### Event card

- exact text `Требуются волонтёры` виден;
- label находится в metadata/status зоне;
- label не является overlay над media;
- label не перехватывает card navigation;
- closed/stale specimen label не показывает.

### Event detail

- fact pill medallion `Требуются волонтёры` присутствует в InlineSlot;
- medallion не становится TopSlot;
- medallion не является ссылкой;
- content block расположен после short explanation и до full description;
- кнопка `Открыть заявку` имеет exact canonical external URL;
- source/deadline/roles показываются только при наличии evidence;
- keyboard focus и accessible name корректны;
- CTA не заменяет primary ticket/registration CTA.

### Festival card

- matched edition показывает тот же простой label;
- label не меняет primary official festival destination;
- `/volontery/` даёт отдельную ссылку `Открыть заявку`;
- unmatched seed не появляется публично до создания/сопоставления edition.

### `/volontery/`

- содержит только accepted Event/Festival links;
- одно событие/edition не дублируется из-за нескольких application URLs;
- permanent corps page-end cards не входят в dynamic count;
- empty state не подменяется общим каталогом волонтёрства.

## 9. End-to-end сценарии

### E2E-1: live Event match

```text
live discovery
-> OPEN application
-> dynamic Event match
-> accepted link
-> card label
-> detail medallion
-> content CTA
```

### E2E-2: live FestivalEdition match

```text
live discovery
-> OPEN application
-> existing festival edition
-> festival card label
-> /volontery/ card + application CTA
```

### E2E-3: absent festival discovery

```text
live/fixture application
-> no current target
-> FESTIVAL_DISCOVERY_SEED
-> one pending festival_queue URL row
-> no invented official URL
-> no public festival card yet
```

### E2E-4: closure removal

```text
accepted OPEN link
-> source changes to CLOSED
-> daily recheck
-> transactional projection update
-> card label, medallion, block and /volontery/ membership disappear
```

### E2E-5: transport degradation

```text
accepted OPEN link
-> one UNKNOWN check
-> last-good retained within TTL
-> repeated failure crosses 36h
-> public link hides fail-closed
-> source recovery restores it
```

### E2E-6: wrong edition

```text
2025 application + 2026 festival/event target
-> BGE similarity high
-> deterministic year conflict
-> no accepted relation
```

### E2E-7: provider unavailable

```text
BGE/LLM or Festival Web Research unavailable
-> no empty overwrite
-> opportunity remains recoverable
-> exact previously accepted fresh link follows last-good policy
-> absent festival seed remains pending/review
```

## 10. Quality evaluation corpus

Owner-reviewed golden corpus должен содержать не менее следующих классов:

- exact Event matches;
- programme/subevent matches;
- exact FestivalEdition matches;
- absent festivals;
- same-name/different-year negatives;
- same-city unrelated events;
- permanent corps/training negatives;
- closed/expired sources;
- ambiguous city/date cases;
- multiple applications for one target.

Для BGE измеряется shortlist recall@8 Event и recall@5 Festival. Для финального matcher измеряются precision/abstention/false-publication counts. Thresholds фиксируются только после owner gold; они не придумываются в implementation code.

## 11. Release gates

Feature branch не может включить public UI без:

- fixture suite green;
- golden shortlist evaluation artifact;
- live canary хотя бы с одним сохранённым real run artifact;
- E2E-3 и E2E-4 green;
- no invented official URL assertion;
- last-good/freshness tests;
- browser tests на desktop/mobile;
- release manifest с volunteer projection hash/counts;
- operator review UI для ambiguous matches и festival seeds.
