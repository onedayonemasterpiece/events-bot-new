# Минимальные продуктовые автотесты статических подборок

> **Статус:** реализован каркас runner + GitHub Actions; подключение реального
> product snapshot и принятого baseline остаётся следующей кодовой итерацией.
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

## 4. Мягкий adapter contract

Runner читает объект с `collections`. Каждая подборка может быть массивом либо
объектом с `items`. Из строки используются только доступные продуктовые поля:

```json
{
  "event_id": 123,
  "family_id": "family:...",
  "start_date": "2026-08-10",
  "end_date": "2026-08-10",
  "venue": "...",
  "organizer": "...",
  "event_type": "...",
  "source_status": "grounded",
  "review_status": "accepted"
}
```

Поддерживаются несколько естественных aliases (`id`, `date`, `venue_name`,
`location_name`). Неизвестные поля игнорируются. Обязательная жёсткая версия
внутренней схемы отсутствует.

На уровне подборки допускаются:

```json
{
  "mode": "public|shadow|seasonal|experimental",
  "state": "ready|failed|degraded",
  "using_last_good": false,
  "watch_below_families": 5,
  "items": []
}
```

Абсолютный минимум задаётся только там, где продукт уже принял такое ожидание.
Иначе контроль строится относительно baseline.

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

До подключения реального adapter результат честно `NOT_IMPLEMENTED`. После
первого реального accepted run устанавливается:

```text
STATIC_COLLECTIONS_PRODUCT_QUALITY_REQUIRED=true
```

`WATCH` остаётся успешным job с видимым отчётом. `FAIL` завершает job ошибкой.

Ежедневный schedule намеренно **не включён в skeleton**: бессодержательный
`NOT_IMPLEMENTED` run создавал бы только шум. Следующий агент добавляет schedule
в том же PR, где подключает реальный product snapshot и baseline, и сразу
прикладывает terminal scheduled evidence.

Workflow пока имеет только GitHub UI `workflow_dispatch`. По общей стратегии это
не является гарантированным запуском из ChatGPT. Поэтому scenario остаётся
`partial/skeleton`, а следующий агент до перевода в `implemented` обязан добавить
его в канонический `/qa run` gateway и получить terminal evidence по реальному
product snapshot.

## 6. Что должен сделать следующий кодовый агент

1. Сформировать product snapshot из фактического `collection-batch` и frozen
   event catalog в текущем StaticSiteBuilder/exporter, не создавая второй
   pipeline.
2. Добавить `family_id`, даты, venue/organizer/type и source/review state из уже
   существующих данных.
3. Получить первый owner-reviewed результат и сохранить/опубликовать его как
   baseline, не выдавая provisional seed за baseline.
4. Заполнить living regression examples реальными известными positives и false
   positives.
5. Настроить repository variables, добавить schedule и выполнить terminal
   scheduled run только после появления реального product snapshot.
6. Добавить scenario в канонический `/qa run` gateway; UI-only dispatch не
   закрывает ChatGPT launch boundary.
7. Только после этого сделать workflow required для изменений, способных менять
   membership/ранжирование.
8. После появления Astro routes подключить отдельный
   `collections.product_page_smoke` к уже существующему collection Playwright
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

## 8. Не входит в каркас

- отдельный workflow на каждую подборку;
- проверка всех внутренних hashes/schema versions как продуктовый gate;
- real LLM replay на каждый commit;
- Android/iOS;
- browser page smoke до появления страниц;
- автоматическое изменение membership по результату теста;
- блокировка PR только потому, что seasonal/experimental подборка пока мала.
