# SQLite “миграции” и сиды (как это устроено)

В проекте нет отдельного набора SQL‑миграций для **SQLite** (как, например, для Supabase).  
Вместо этого схема и эволюция SQLite БД поддерживаются **идемпотентно** в коде: `db.py` → `Database.init()`.

## Где лежат изменения схемы

- **Создание таблиц/индексов**: `CREATE TABLE/INDEX IF NOT EXISTS` прямо в `db.py`.
- **Добавление колонок**: через helper `_add_column()` (без падения при повторном запуске).

Это означает, что “миграция” для SQLite = изменение `db.py`. Любой деплой/запуск бота автоматически подтянет схему до актуальной.

## Smart Update candidate-state migration

The automatic identity state machine is an additive `Database.init()`
migration:

- `event_source` receives nullable candidate-state, candidate-key, and
  occurrence-key linkage;
- `smart_update_candidate_state` stores the current durable automatic outcome,
  accepted versus diagnostic IDs, replay payload/locator, retry budget, and
  claim lease;
- `smart_update_attempt` is an append-only one-terminal-per-attempt ledger;
- new identity-bearing source ownership is unique by canonical source plus
  occurrence key, so one Telegram/VK carrier may contain several event
  children.

Legacy source rows remain null and are not blanket backfilled. Historical
programme/context roles and producer child slots cannot be inferred safely.
Required new indexes/checks are readiness invariants: initialization must not
silently warn and continue if they cannot be activated.

Rollback is code-first and non-destructive. Keep the additive tables/columns
for a forward-compatible binary. Do not recreate the old one-source-URL/
one-Event unique index after keyed sibling bindings exist unless a separate
audit proves it safe. Removing SQLite columns requires table rebuilds and is not
part of the normal rollback.

The full terminal/key/retry contract is canonical in
`docs/features/smart-event-update/identity-state-machine.md`.

## Где лежат сиды/дефолты (данные)

В `Database.init()` допускается добавлять **идемпотентные** “seeding”‑операции, которые:

- исправляют/нормализуют существующие записи (UPDATE с безопасными условиями),
- не перетирают ручные настройки оператора,
- работают корректно при повторных запусках.

### Пример: дефолтная локация для VK‑источника Garazhka Kaliningrad

Чтобы после обновления/смены БД (например, при скачивании нового снапшота или на прод‑деплое) не терялся дефолтный адрес,
в `db.py` добавлен seed:

- `vk_source.group_id=226847232` (`garazhka_kld`) получает `location="Понарт, Судостроительная 6/2, Калининград"`,
- только если `location` пустой или “общий” (например “Гаражка, Калининград”),
- и **не** перезаписывает вручную заданные значения.

VK monitoring operator seeds (`club194393485`, `ivsguide`, `natakkaz`) also live in `Database.init()` as `INSERT OR IGNORE` rows. They are enabled by default for production/fresh snapshots and can be disabled for narrow unit-test fixtures with `DB_INIT_SKIP_VK_SOURCES_SEED=1`.

### Пример: канонические Telegram‑источники (мониторинг)

В `Database.init()` также выполняется идемпотентный seed списка Telegram‑источников:

- источник данных: `docs/features/telegram-monitoring/sources.yml`;
- вставляет отсутствующие каналы и нормализует username (lowercase, без `@`/URL);
- повышает trust_level (только upgrade) и добавляет missing filters (не удаляет существующие);
- доступен и вручную из UI: `/tg` → «🧩 Синхронизировать источники»;
- для release/ops можно запускать CLI: `python scripts/seed_telegram_sources.py --db <path>`.

### Пример: backfill `event_source` для legacy событий

Старые снапшоты БД могут содержать заполненные `event.source_post_url` / `event.source_vk_post_url`, но без строк в таблице `event_source`.

Чтобы Smart Update мог сходиться по `source_url` (и не плодить дубли при повторной обработке одного и того же поста),
в `Database.init()` выполняется идемпотентный backfill:

- `INSERT OR IGNORE` в `event_source` из `event.source_post_url` (тип выводится как `telegram`/`vk`/`legacy` по URL),
- отдельный `INSERT OR IGNORE` из `event.source_vk_post_url` с `source_type='vk'`,
- URL собственных managed VK-публикаций исключаются из обоих backfill-проходов, а ранее ошибочно созданные строки удаляются вместе с зависимыми фактами: публичная проекция не может становиться evidence,
- можно отключить на больших БД (или для отладки) переменной окружения: `DB_INIT_SKIP_EVENT_SOURCE_BACKFILL=1`.

## Важно про Supabase‑миграции

SQL‑миграции Supabase лежат отдельно в `migrations/` (например `001_google_ai.sql`, `002_google_ai_rpc_rollout.sql`) и не имеют отношения к SQLite схеме событий.

## Event-media schema upgrade

`Database.init()` adds the event-media status/fingerprint columns, partial
per-event raw-SHA uniqueness, `event_media_pair_review` and
`event_media_review_usage`. On a legacy DB, existing `EventPoster` rows become
`approved` only when `review_status` is first introduced. This is a one-time
compatibility migration: later restarts must not reset
`pending_review`/`duplicate`/`rejected`/`unavailable` decisions. Regression:
`tests/test_event_media_gate.py::test_review_status_migration_is_one_time_and_restart_safe`.

`Database.init()` также идемпотентно создаёт `event_image_geometry` и nullable
`EventPoster.image_geometry_id`. Cache имеет unique key
`(pixel_sha256, model, prompt_version)` и хранит нормализованные face/value
`yxyx` boxes; глобальный индекс `EventPoster.pixel_sha256` позволяет связать с
одним результатом все строки с идентичными ориентированными RGB-пикселями.
