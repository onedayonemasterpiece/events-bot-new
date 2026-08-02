# Smart Update facts v3: приёмка на реальных данных и реальных постах

Статус: **mandatory acceptance gate**.

Фактический прогон 2026-08-02: **Gate A PASS, Gate B NO-GO; Gates C–F не
запускались по stop-condition**. На свежем production snapshot все 50
event/source bindings были точными, каждый routed source получил ровно один
primary Gemma send, writes остались нулевыми и exact-quote rate составил 100%,
но provisional recall не достиг gate и обнаружились противоречащие ontology v2
review rows (в частности 6562/7102/7258/7290). Полные команды, hashes и
непредъявленные claims: [integration report](../../../.codex/integration/static-collection-facts-v3-INTEGRATION_REPORT.md).
Semantic publication и production apply остаются **BLOCKED**.

Этот документ проверяет не только functions и fixtures, а фактическую цепочку:

```text
реальный Telegram/VK/parser source
  -> штатный Smart Update
  -> один collection_candidate_adjudication call
  -> source-grounded facts v3
  -> Event.collection_decisions
  -> повторный warm no-op
```

Публичные подборки, manifests и Astro в этот gate не входят.

## 1. Что считается реальным тестом

Реальным считается тест, где одновременно:

- исходный текст взят из настоящего `EventSource.source_text` production Fly
  SQLite или из нового реального Telegram/VK поста;
- используется фактический provider, а не mocked JSON;
- provider output проходит production strict validator;
- решение связано с реально сохранённым `source_id`;
- evidence quote является exact substring source text;
- apply выполняется существующим production merge/apply кодом;
- DB diff и повторный запуск проверены.

Не считается real-data acceptance:

- только unit fixture;
- ручной вызов validator с заранее написанным JSON;
- только prompt inspection;
- только GitHub contract PASS;
- synthetic source, написанный специально под prompt;
- browser job без facts-v3 DB evidence.

## 2. Режимы существующего backfill

Доработать `scripts/backfill_static_collection_facts.py`, а не создавать
параллельный production pipeline.

Целевые режимы:

```text
по умолчанию / --plan  : DB read-only, provider_calls=0, writes=0
--evaluate             : реальные provider calls, DB writes=0
--apply                : реальные provider calls и bounded DB writes
```

`--evaluate` и `--apply` должны быть взаимоисключающими.

Во всех режимах report должен содержать:

```text
schema_version
repo_sha
facts_policy_version
adjudication_schema_version
DB snapshot identity/hash
started_at/finished_at
requested event_ids/source_ids
per-source input_hash
provider_called
requested_model
actual provider/model path if trace exposes it
fallback_used
validated outcomes for all three facts
legacy compatibility projection
write status
changed keys
errors/deferred reason
```

Report не должен содержать API keys или private bearer URLs.

## 3. Gate A — unit и deterministic integration

До внешних вызовов должны пройти:

```bash
python3 -m pytest tests/test_smart_event_update.py -q
python3 -m pytest tests/test_event_update_merge.py -q
python3 -m pytest tests/test_static_collection_backfills.py -q
python3 -m pytest tests/test_static_collection_*.py -q
```

Также основной incident-critical CI проекта.

Обязательные mutation regressions:

- один v3 key испорчен — payload rejected;
- quote отсутствует в corpus — payload rejected;
- joint confirmed без child/family confirmed — rejected;
- stale policy version меняет input hash;
- v2 legacy decision не закрывает v3 backfill;
- unknown не стирает existing confirmed;
- same hash не вызывает provider;
- source_id другого event не применяется;
- manual lock не перезаписывается;
- source trust precedence действует отдельно на каждый key.

## 4. Gate B — primary-only replay frozen real EventSource corpus

### 4.1. Данные

Использовать свежую read-only выгрузку из Fly SQLite, а не копировать committed
`model_excerpt`.

Минимальный corpus:

- все source-bound positives `child_directed` из PR-A seed;
- все source-bound positives `family_suitable`;
- все 4 текущих source-bound positives `joint_family_activity`;
- минимум 20 независимых hard-negative families для каждой границы;
- named boundary cases:
  - 4648 — не science/family evidence;
  - 6871 — occurrence-specific источник не доказывает audience;
  - 7103 — не strong-impressions participation и не audience proof;
  - 7307 — «семейный турнир» не доказывает joint participation;
  - 7326 — family suitable, joint unknown;
  - parents-only meeting;
  - child-authors exhibition;
  - family-atmosphere copy;
  - age-rating-only case;
  - real child theatre;
  - real parent+child joint practice.

Все единицы считать по occurrence family, не по повторным датам.

### 4.2. Запуск

Primary-only:

```bash
SMART_UPDATE_4O_FALLBACK=0 \
python3 scripts/backfill_static_collection_facts.py \
  --db /path/to/fresh-production-copy.sqlite \
  --reason audience \
  --evaluate \
  --event-id <ID> ... \
  --max-sources-per-event 2 \
  --output artifacts/static-collection-facts-v3/primary-only.json
```

Фактическая команда может использовать response file/JSON ID list, но должна
быть полностью сохранена в integration report.

### 4.3. Метрики

Обязательные family-weighted gates:

```text
child_directed candidate recall >= 0.95
family_suitable candidate recall >= 0.95
joint_family_activity: 4/4 текущих positives подтверждены либо каждый miss разобран
false confirmed на hard negatives = 0
exact quote validity = 100%
source_id/event_id binding = 100%
provider calls per routed source <= 1
provider calls for unrouted source = 0
GPT-4o fallback calls = 0 в primary-only run
DB writes = 0
```

При малом supply joint-family результат описывается как count, а не как
статистически сильная доля.

Любой false confirmed — NO-GO. `unknown` допустим, но снижает recall.

## 5. Gate C — fallback/failure drill

Выполнить отдельно от primary quality run.

### 5.1. Primary failure + существующий fallback

На 3–5 real source cases искусственно сделать primary недоступным на уровне
тестового adapter/monkeypatch, не через изменение production secret.

Проверить:

- не более одного GPT-4o fallback send на routed source;
- fallback отражён в trace/report;
- output проходит тот же strict source quote validator;
- invalid fallback output не применяется;
- ни primary failure, ни invalid fallback не стирают existing truth.

### 5.2. Полная недоступность

Primary и fallback возвращают failure:

```text
result = deferred/unknown
writes = 0
existing accepted decisions unchanged
Smart Update canonical transaction не повреждён
```

### 5.3. Malformed/evidence mismatch

На real corpus проверить:

- malformed JSON;
- missing required key;
- quote paraphrase вместо exact quote;
- quote от соседнего EventSource;
- `denied` без explicit negative evidence.

Каждый случай должен fail closed.

## 6. Gate D — apply на свежей копии production SQLite

### 6.1. Подготовка

1. Снять online backup production `/data/db.sqlite`.
2. Зафиксировать SHA-256.
3. Работать только с копией.
4. Выполнить:

```bash
sqlite3 copy.sqlite 'PRAGMA quick_check;'
```

Ожидается `ok`.

### 6.2. Bounded cohort

Минимум 20 current/future events:

- 5 child-directed positives;
- 5 family-suitable positives;
- все доступные joint-family positives, максимум одной строкой на family;
- минимум 6 hard negatives/boundaries;
- разные source types: Telegram, VK, официальный parser;
- минимум 2 события с несколькими EventSource.

### 6.3. Первый apply

```bash
python3 scripts/backfill_static_collection_facts.py \
  --db copy.sqlite \
  --reason audience \
  --apply \
  --event-id <ID> ... \
  --max-sources-per-event 2 \
  --output artifacts/static-collection-facts-v3/copy-apply.json
```

Allowed DB diff для audience-only cohort:

```text
event.collection_decisions у выбранных event IDs
```

Запрещённый diff:

```text
event.title
event.description
event.event_type
event.topics
event.date/time/location
event.identity_status/merged_into_event_id
event.is_free
EventSource rows
posters/links
любые невыбранные events
```

### 6.4. Warm identical apply

Повторить точную команду.

Обязательный результат:

```text
provider_calls = 0
writes = 0
changed_events = 0
all requested sources = cached/no-op
DB file logical row diff = 0
```

## 7. Gate E — штатный Smart Update на реальных постах в production-копии

Это обязательный тест именно ingestion path, а не backfill-only.

Подобрать минимум 12 реальных постов, ещё не превращённых в synthetic fixtures:

```text
3 direct child events
3 explicit family events
2 explicit parent+child joint activities
1 parents-only event
1 age-rating-only event
1 vague family-atmosphere event
1 ordinary adult event
```

Источники:

```text
минимум 4 Telegram
минимум 4 VK
минимум 2 official parser records
```

Каждый пост прогнать через тот же entry point, который использует штатный
Telegram/VK/parser Smart Update. Запрещено напрямую создавать provider payload
или сразу вызывать `apply_collection_decisions()`.

Для каждого source проверить:

- создан/найден правильный canonical event;
- создан правильный EventSource;
- collection stage вызван 0 или 1 раз по router expectation;
- v3 decision source_id совпадает с attached EventSource;
- evidence quote входит в EventSource.source_text;
- повторная обработка того же поста не создаёт duplicate event/source;
- повторная обработка имеет provider_calls=0 для same hash;
- event prose/identity merge остаются корректными.

Сохранить redacted LLM trace и per-post receipt.

## 8. Gate F — bounded live Fly canary

Только после Gates A–E.

### 8.1. Deploy boundary

- merge/rebase только от принятого main stack;
- deploy exact repo SHA;
- pre-deploy online DB backup + SHA-256;
- `/healthz` до и после;
- SQLite `quick_check=ok`;
- publication/routes остаются blocked.

### 8.2. Read-only live evaluate

Сначала на production DB:

```text
--evaluate
--reason audience
не более 20 current/future event IDs
```

Это реальные provider calls, но DB writes=0.

Сравнить с production-copy results. Любое необъяснённое расхождение — stop.

### 8.3. Bounded live apply

После review evaluate artifact:

- не более 12 events;
- explicit ID allowlist;
- минимум 3 source types;
- никаких historical mass runs;
- сохранить before/after JSON каждого selected event;
- выполнить warm identical apply.

Acceptance:

```text
first apply: только ожидаемые collection_decisions
warm apply: provider_calls=0, writes=0
healthz healthy
scheduler/outbox healthy
no canonical prose/identity changes
no public route changes
```

### 8.4. Реальные новые посты

После bounded existing-event canary обработать через обычный production Smart
Update минимум 6 новых реальных постов:

```text
2 child/family positives
1 joint-family positive
3 negatives/boundaries
```

Посты выбираются по фактическим свежим входящим данным; их URL/source IDs и
ожидаемые факты фиксируются до просмотра LLM output.

Не допускается выбирать только те посты, на которых модель уже дала удобный
ответ.

## 9. Call-count и model evidence

Для всех external runs сохранить:

```text
requested model
actual provider model/path
fallback flag
physical sends
rate-limit waits
input/output token usage
latency
validator result
```

Ключевой acceptance:

```text
реализация facts v3 не увеличила число collection LLM calls относительно
существующей архитектуры: максимум один call на routed source.
```

Primary `gemma-4-31b-it` и GPT-4o fallback не смешивать в одной quality metric.

## 10. Артефакты

Минимальный набор:

```text
.codex/integration/static-collection-facts-v3-INTEGRATION_REPORT.md
artifacts/static-collection-facts-v3/primary-only.json
artifacts/static-collection-facts-v3/fallback-drill.json
artifacts/static-collection-facts-v3/copy-apply.json
artifacts/static-collection-facts-v3/copy-warm.json
artifacts/static-collection-facts-v3/ingestion-real-posts.json
artifacts/static-collection-facts-v3/live-evaluate.json
artifacts/static-collection-facts-v3/live-apply.json
artifacts/static-collection-facts-v3/live-warm.json
```

Large/private DB snapshots и secrets в Git не добавлять. В Git фиксировать
hashes, commands, counts и redacted evidence.

## 11. Stop/NO-GO conditions

Остановить работу и не расширять cohort при любом из условий:

- false confirmed на hard negative;
- quote не является exact source substring;
- source_id другого event;
- более одного collection LLM call на source;
- warm provider call или DB write;
- изменение event prose/type/topics/identity на audience-only backfill;
- provider failure стирает accepted truth;
- v2 legacy decision публикуется как v3 fact;
- live/copy results необъяснимо расходятся;
- health/scheduler/outbox regression;
- попытка включить routes/navigation/sitemap.

## 12. Итоговый статус

Работа считается принятой только при статусе:

```text
FACTS_V3_CODE              PASS
PRIMARY_REAL_DATA          PASS
FALLBACK_FAILURE_DRILL     PASS
PRODUCTION_COPY APPLY      PASS
PRODUCTION COPY WARM       PASS
REAL POST SMART UPDATE     PASS
BOUNDED LIVE APPLY         PASS
BOUNDED LIVE WARM          PASS
PUBLICATION                BLOCKED
```

До этого `Smart Update facts v3 implemented` заявлять нельзя.
