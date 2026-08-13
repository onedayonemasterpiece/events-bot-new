# Runtime Logs

Каноническая политика краткоживущих runtime-логов production bot.

## Purpose

- сохранять scheduler/import/Smart Update evidence на Fly volume после того, как короткий буфер `fly logs` уже исчез;
- обеспечивать ежедневный incident monitoring и пошаговую проверку live E2E;
- не допускать повторения `INC-2026-04-16-prod-disk-pressure-runtime-logs`: логирование не может бесконтрольно занять volume с SQLite.

## Production Policy

Production file mirror **включён постоянно** и пишет root logger в `/data/runtime_logs/events-bot.log`.
Отключение mirror не является штатной защитой диска: защита реализована ограничением размера и свободного места.

Текущий budget contract:

- `ENABLE_RUNTIME_FILE_LOGGING=1`;
- active file: `/data/runtime_logs/events-bot.log`;
- size rotation: `RUNTIME_LOG_MAX_FILE_MB=8`;
- hard budget active + rotated: `RUNTIME_LOG_MAX_TOTAL_MB=64`;
- rotated retention ceiling: `RUNTIME_LOG_RETENTION_HOURS=48`;
- volume free-space floor: `RUNTIME_LOG_MIN_FREE_MB=256`;
- level: `RUNTIME_LOG_LEVEL=INFO`.
- `/healthz` disk telemetry: warning below `350 MiB`, critical/HTTP 503 below `256 MiB` (`RUNTIME_DISK_WARN_FREE_MB`, `RUNTIME_DISK_CRITICAL_FREE_MB`).
- Fly volume capacity: `/data` is provisioned at `2 GiB`; `fly.toml` requests bounded automatic extension at `80%` usage in `1 GiB` increments, capped at `3 GiB`. Capacity growth is a last-resort availability guard, not a replacement for the log budget, retention, DB/media cleanup, snapshots, or the free-space health floors above.

При обычном потоке это даёт до двух суток evidence. При log storm размер, а не время, является приоритетным guard: старейшие rotated files удаляются, active file ротируется, а при достижении free-space floor file mirror временно пропускает записи. Console/stdout/Fly logs при этом продолжают работать. Неизвестные файлы и SQLite handler никогда не удаляет.

## Scope

В файл попадает существующий root-logger stream:

- scheduler/job события (`vk_auto_import`, `tg_monitoring`, `guide_monitoring`, `event_vector_sync`, video jobs);
- Smart Update decision/correlation lines;
- traceback и runtime warnings;
- E2E command/run evidence.

Секреты, LLM thoughts и полные private payloads специально в лог не добавляются.

### Kaggle run ledger isolation

Каждый remote run получает отдельный private status dataset с callback-конфигурацией. Его slug содержит короткий читаемый prefix **и hash полного `run_id`**; простое обрезание длинного `run_id` запрещено, потому что параллельные/повторные запуски с одинаковым prefix иначе начинают versioning одного dataset и посылают heartbeat в чужую строку `kaggle_run_ledger`. При расследовании сверяйте одновременно полный `run_id`, status-dataset ref и kernel version.

## Environment

- `ENABLE_RUNTIME_FILE_LOGGING` — включает mirror; production source of truth — `fly.toml`.
- `RUNTIME_LOG_DIR`, `RUNTIME_LOG_BASENAME` — volume directory и active filename.
- `RUNTIME_LOG_RETENTION_HOURS` — возрастной потолок rotated files.
- `RUNTIME_LOG_MAX_FILE_MB` — размер одного active/rotated файла.
- `RUNTIME_LOG_MAX_TOTAL_MB` — максимальный объём файлов этого basename.
- `RUNTIME_LOG_MIN_FREE_MB` — floor свободного места, ниже которого mirror ставит file writes на паузу.
- `RUNTIME_LOG_LEVEL` — уровень file handler независимо от console verbosity.

Нельзя повышать budget/retention или снижать free-space floor без фактического `df`/`du` и regression-check инцидента `INC-2026-04-16`.

Нельзя без отдельного capacity review повышать `auto_extend_size_limit` выше `3 GiB`. После любого ручного или автоматического resize обязательны `df -h /data`, `PRAGMA quick_check`, `/healthz`, Fly health checks и проверка свежих логов на `Errno 28` / `database or disk is full`.

## Investigation Workflow

Перед заявлением, что production-логи отсутствуют:

1. Проверить фактические env перечисленных выше переменных на активной машине.
2. Проверить `df -h /data`, `du -x` и файлы `events-bot.log*` с sizes/mtimes.
3. Искать по нескольким ключам: `run_id`, `ops_run_id`, `batch_id`, inbox/source/event id, job kind, UTC window, error class.
4. Сверить log terminal line с `ops_run.details_json`/`metrics_json`, DB rows и Telegram UI/VK response.
5. Сохранить только минимальные non-secret excerpts в `artifacts/codex/<incident>/`.
6. Если mirror реально был paused/disabled или окно уже вытеснено budget/retention, явно зафиксировать это и перейти к `fly logs`, DB/`ops_run`, Kaggle outputs и публичным API.

## Disk Hygiene Runbook

Before cleanup, compare durable DB growth by table as well as top-level paths.
For raw-first VK, compare `dbstat` bytes for `vk_source_packet` with the
predeploy DB snapshot and count packets per crawl hour. Full JSON and attachment
envelopes can grow the main DB much faster than the bounded runtime mirror;
deleting logs or truncating WAL must not be reported as the durable root cause
without that table-level comparison.

`VK_CRAWL_MIN_FREE_MB` is a writer admission floor (default `512` MiB) and must
remain above the `/healthz` warning/critical floors. It is rechecked before
every source/page fetch and packet transaction, so one admitted multi-source
crawl cannot consume the full warning-to-critical margin before the next
probe. Do not lower it to make a crawl run while `/data` is in
warning; first reconcile exact terminal artifacts or another owner-governed
retention action and repeat `df`/`quick_check`.

До удаления:

- `PRAGMA quick_check` для `/data/db.sqlite`;
- точный `du` каждого top-level path;
- mtime и назначение backup/tmp/result directories;
- список active processes/open files для сомнительных путей.

Можно удалять только доказанно terminal/regenerable artifacts: старые incident sqlite copies, stale temp/render directories и recovery bundles сверх их документированного retention. Нельзя удалять `/data/db.sqlite`, актуальные WAL/SHM вручную, Telegram/Telegraph token state или неизвестный directory только по имени.

После очистки/деплоя обязательны:

- `df -h /data` и root overlay;
- отдельный application-equivalent create/write/`fsync`/remove probe в
  configured root scratch (`RUNTIME_SCRATCH_PATH`, production `/tmp`), потому
  что свободный `/data` не доказывает работоспособность Python `tempfile`;
- `PRAGMA quick_check=ok` и write probe через обычный application path;
- `/healthz ready=true`;
- active runtime log exists, grows, and contains the startup budget line;
- total `events-bot.log* <= RUNTIME_LOG_MAX_TOTAL_MB`;
- no `Errno 28` / `database or disk is full` in fresh logs.

Static-site artifacts live only below configured
`STATIC_SITE_ARTIFACT_ROOT=/data/static_site_builder`. Automatic retention may
delete a recognized terminal `output-production-*` tree only after its durable
receipt/evidence was persisted and only when it is not the exact
active/recoverable handoff. Unknown directories, symlinks, paths outside that
root and failed/nonterminal output are manual incident decisions. Default
successful-output retention is zero; historical counts/times come from the
build history/ledger/receipt rather than from keeping duplicate archives.

Video cleanup follows the same assertion boundary. Published terminal render
trees use exact `videoannounce-<session>` names plus durable DB evidence and are
retried by startup reconciliation; active/ledger-live, failed, blocked and
pending-main sessions stay intact. Temporary publish-only source/result and log
download families are exact-name checked and removed in their own `finally`
paths. Symlinks and unknown directories are never followed or bulk-deleted.
