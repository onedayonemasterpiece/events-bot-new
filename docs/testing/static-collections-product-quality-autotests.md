# Минимальные продуктовые автотесты статических подборок

> **Статус:** реализованы runner, GitHub Actions и facts-v3 product snapshot
> adapter на существующей границе exporter/StaticSiteBuilder. Owner-accepted
> baseline и live evidence ещё отсутствуют, поэтому публикация заблокирована.
> **Общая стратегия:** [`../operations/static-site-autotest-strategy.md`](../operations/static-site-autotest-strategy.md).
> **Workflow:** `.github/workflows/static-collections-product-quality.yml`.

## 1. Зачем нужен этот контур

Он проверяет не внутреннее устройство BGE, Smart Update или JSON-схем, а
наблюдаемый продуктовый результат:

1. в подборке есть актуальные независимые события;
2. известный очевидный мусор не вернулся, а известные хорошие примеры не исчезли
   незаметно;
3. новый расчёт не деградировал относительно принятого результата и не уничтожил
   last-good при ошибке.

Изменение внутреннего storage/schema не должно заставлять переписывать весь
набор. Производственный exporter формирует один небольшой product snapshot, а
runner игнорирует неизвестные поля.

## 2. Один runner, три продуктовые секции

### `health`

Считает по каждой подборке:

- независимые `family_id`;
- актуальные события в горизонтах 14/30/90 дней;
- ближайшую дату;
- завершившиеся события;
- дубли event/family;
- результаты с blocked/needs-source-review evidence;
- концентрацию одной площадки, организатора или типа в top-N;
- добавленные и исчезнувшие families относительно baseline;
- свежесть product snapshot.

Низкая наполняемость и высокая концентрация дают `WATCH`, а не автоматически
ломают CI.

### `semantic_sample`

Небольшой живой файл примеров хранит:

- `must_exclude_*` — известные грубые false positives;
- `must_include_*` — известные релевантные примеры, исчезновение которых нужно
  расследовать.

Возврат известного false positive — `FAIL`. Исчезновение positive — `WATCH`,
потому что событие могло закончиться или выйти из текущего каталога.

Этот набор расширяется после каждого подтверждённого продуктового дефекта. Он
не является замороженной полной моделью мира.

### `stability`

Сравнивает текущий результат с owner-accepted baseline:

- резкое падение supply и churn — `WATCH`;
- одинаковый `input_fingerprint`, но другой видимый состав — `FAIL`;
- failed/degraded rebuild, который заменил непустой baseline пустотой вместо
  last-good, — `FAIL`.

Сравнивается нормализованный видимый результат `collection -> family/event`, а
не все внутренние поля артефактов.

## 3. Когда блокировать

`FAIL` ограничен явной поломкой продукта:

- публичная подборка неожиданно пуста;
- одна family показана несколькими карточками;
- присутствуют завершившиеся или source-blocked события в public-выдаче;
- вернулся известный грубый false positive;
- одинаковые входы дали другой видимый результат;
- потерян last-good.

`WATCH` означает необходимость посмотреть отчёт, но не блокирует PR по
умолчанию:

- событий мало;
- supply снизился;
- состав резко изменился;
- выдача чрезмерно сконцентрирована;
- известный positive отсутствует;
- нет принятого baseline;
- snapshot устарел.

Для отдельного расследования manual run может включить `fail_on_watch=true`.
Это не постоянная release policy.

## 4. Facts-v3 adapter contract

`site/scripts/static_collection_product_snapshot.py` строит snapshot из уже
записанных `Event.collection_decisions` и `EventSource`. Он не вызывает LLM/BGE,
не создаёт вторую классификацию и всегда фиксирует `provider_calls=0`.
Верхний уровень содержит:

- `schema_version=static-collection-product-snapshot-v1`;
- `facts_policy_version=static-collection-facts-v3`;
- произвольный непустой `source_scope` — provenance конкретной стадии/копии БД;
- отдельный `evidence_trust_scope=all|trusted` — фильтр evidence, не provenance;
- `input_fingerprint`, `normalized_output_sha256`, `snapshot_sha256`;
- `publication.status=blocked` и только `shadow|experimental` modes.

Adapter пропускает только прямые `confirmed` facts-v3 конкретного события.
`kids` является объединением независимых `child_directed_decision` и
`family_suitable_decision`; legacy `audience_decision` не является truth.
Связанные occurrences объединяются только по взаимным явным ссылкам, а факт
соседа не переносится. Представителем family становится самый ранний
direct-fact occurrence, затем меньший event ID. `organizer` берётся из уже
нормализованного exporter/DB `organizer_names`, если он заполнен.

Для каждого факта обязательны exact `source_id` того же event, дословная quote
в `EventSource.source_text`, facts-v3 policy и валидный input hash. Malformed
confirmed fact не исчезает: он выходит как `source_status=blocked` и
`review_status=needs_source_review`; `source_grounding_required=true` заставляет
runner завершить такой snapshot с `FAIL` даже в shadow.

Runner читает объект с `collections`. Из строки используются продуктовые поля:

```json
{
  "event_id": 123,
  "family_id": "linked:123",
  "start_date": "2026-08-10",
  "end_date": "2026-08-10",
  "venue": "...",
  "organizer": "...",
  "event_type": "...",
  "source_status": "grounded",
  "review_status": "accepted"
}
```

Неизвестные дополнительные поля runner игнорирует. Сам adapter и
StaticSiteBuilder до запуска runner строго валидируют version/hash/publication
contract. Абсолютный minimum supply задаётся только после продуктового решения;
до принятого baseline низкое наполнение остаётся `WATCH`.

Hash serialization канонична: UTF-8 JSON, `ensure_ascii=false`, lexicographic
`sort_keys=true`, separators `(',', ':')`, `allow_nan=false`. `snapshot_sha256`
считает весь snapshot без собственного поля (включая `generated_at`),
`input_fingerprint` — только нормализованные входные events/facts/source hashes
и scopes, а `normalized_output_sha256` — тот же ordered visible
`collection -> (family_id,event_id)` projection, который использует PR #234
runner. Файл записывается атомарно как читаемый sorted/indented JSON; файловый
SHA поэтому является transport receipt, а не warm-equivalence key.

## 5. GitHub Actions

Workflow содержит два фактических режима.

### `skeleton`

На каждом релевантном PR:

- запускает unit/regression runner;
- строит ожидаемый `WATCH`-отчёт на example fixture;
- доказывает, что WATCH не превращён в FAIL;
- публикует JSON, Markdown и `qa-summary.json`.

Это проверка каркаса, **не** утверждение о качестве live-подборок.

### `product`

На ручном запуске или опциональном push после подключения adapter использует
пути:

```text
STATIC_COLLECTIONS_PRODUCT_SNAPSHOT_PATH
STATIC_COLLECTIONS_PRODUCT_BASELINE_PATH
STATIC_COLLECTIONS_PRODUCT_REGRESSION_PATH
```

Если artifact path не передан, результат честно `NOT_IMPLEMENTED`; наличие кода
adapter само по себе не означает live run. После первого owner-accepted run
устанавливается:

```text
STATIC_COLLECTIONS_PRODUCT_QUALITY_REQUIRED=true
```

`WATCH` остаётся успешным job с видимым отчётом. `FAIL` завершает job ошибкой.

Schedule намеренно **не включён** на этой стадии. Он появляется только вместе с
принятым baseline и terminal live evidence; эта ветка не заявляет activation.

Workflow пока имеет только GitHub UI `workflow_dispatch`. По общей стратегии это
не является гарантированным запуском из ChatGPT. Поэтому scenario остаётся
`partial/skeleton`, а следующий агент до перевода в `implemented` обязан добавить
его в канонический `/qa run` gateway и получить terminal evidence по реальному
product snapshot.

## 6. Что остаётся до активации release gate

1. Получить первый owner-reviewed результат и сохранить/опубликовать его как
   baseline, не выдавая provisional seed за baseline.
2. Заполнить living regression examples реальными известными positives и false
   positives.
3. Настроить repository variables, добавить schedule и выполнить terminal
   scheduled run только после появления принятого baseline/live evidence.
4. Добавить scenario в канонический `/qa run` gateway; UI-only dispatch не
   закрывает ChatGPT launch boundary.
5. Только после этого сделать workflow required для изменений, способных менять
   membership/ранжирование.
6. После появления Astro routes подключить отдельный
   `collections.product_page_smoke` к существующему collection Playwright
   checker; Android/iOS для обычной listing page не нужны.

## 7. Локальный запуск

```bash
python3 -m unittest discover -s tests \
  -p 'test_static_collections_product_quality.py' -v

python3 scripts/check_static_collections_product_quality.py \
  --snapshot tests/fixtures/static_collections_product_quality/current.json \
  --baseline tests/fixtures/static_collections_product_quality/baseline.json \
  --regression tests/fixtures/static_collections_product_quality/regression.json \
  --today 2026-08-02 \
  --expect-status WATCH
```

После каждого реального `--apply`, warm replay и normal-ingestion replay
snapshot строится **отдельно** из соответствующей копии БД, затем передаётся в
тот же runner:

```bash
python3 site/scripts/static_collection_product_snapshot.py \
  --db /dev/shm/static-collection-facts-v3-work.sqlite \
  --current-date 2026-08-02 \
  --source-scope production-copy-after-apply \
  --evidence-trust-scope all \
  --output artifacts/static-collection-facts-v3/product-after-apply.json

python3 scripts/check_static_collections_product_quality.py \
  --snapshot artifacts/static-collection-facts-v3/product-after-apply.json \
  --expect-status WATCH
```

Без принятого baseline ожидаемый результат — `WATCH`, а не зелёное утверждение
о качестве. Warm сравнение использует пару `input_fingerprint` + рассчитанный
PR #234 runner `normalized_output_sha256`; adapter сохраняет тот же visible-view
hash в snapshot. Файловый SHA и `snapshot_sha256` могут измениться из-за
фактического `generated_at` и потому не являются warm-equivalence key.

## 8. Не входит в каркас

- отдельный workflow на каждую подборку;
- проверка всех внутренних hashes/schema versions как продуктовый gate;
- real LLM replay на каждый commit;
- Android/iOS;
- browser page smoke до появления страниц;
- автоматическое изменение membership по результату теста;
- блокировка PR только потому, что seasonal/experimental подборка пока мала.
