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

При обычном потоке это даёт до двух суток evidence. При log storm размер, а не время, является приоритетным guard: старейшие rotated files удаляются, active file ротируется, а при достижении free-space floor file mirror временно пропускает записи. Console/stdout/Fly logs при этом продолжают работать. Неизвестные файлы и SQLite handler никогда не удаляет.

## Scope

В файл попадает существующий root-logger stream:

- scheduler/job события (`vk_auto_import`, `tg_monitoring`, `guide_monitoring`, `event_vector_sync`, video jobs);
- Smart Update decision/correlation lines;
- traceback и runtime warnings;
- E2E command/run evidence.

Секреты, LLM thoughts и полные private payloads специально в лог не добавляются.

## Environment

- `ENABLE_RUNTIME_FILE_LOGGING` — включает mirror; production source of truth — `fly.toml`.
- `RUNTIME_LOG_DIR`, `RUNTIME_LOG_BASENAME` — volume directory и active filename.
- `RUNTIME_LOG_RETENTION_HOURS` — возрастной потолок rotated files.
- `RUNTIME_LOG_MAX_FILE_MB` — размер одного active/rotated файла.
- `RUNTIME_LOG_MAX_TOTAL_MB` — максимальный объём файлов этого basename.
- `RUNTIME_LOG_MIN_FREE_MB` — floor свободного места, ниже которого mirror ставит file writes на паузу.
- `RUNTIME_LOG_LEVEL` — уровень file handler независимо от console verbosity.

Нельзя повышать budget/retention или снижать free-space floor без фактического `df`/`du` и regression-check инцидента `INC-2026-04-16`.

## Investigation Workflow

Перед заявлением, что production-логи отсутствуют:

1. Проверить фактические env перечисленных выше переменных на активной машине.
2. Проверить `df -h /data`, `du -x` и файлы `events-bot.log*` с sizes/mtimes.
3. Искать по нескольким ключам: `run_id`, `ops_run_id`, `batch_id`, inbox/source/event id, job kind, UTC window, error class.
4. Сверить log terminal line с `ops_run.details_json`/`metrics_json`, DB rows и Telegram UI/VK response.
5. Сохранить только минимальные non-secret excerpts в `artifacts/codex/<incident>/`.
6. Если mirror реально был paused/disabled или окно уже вытеснено budget/retention, явно зафиксировать это и перейти к `fly logs`, DB/`ops_run`, Kaggle outputs и публичным API.

## Disk Hygiene Runbook

До удаления:

- `PRAGMA quick_check` для `/data/db.sqlite`;
- точный `du` каждого top-level path;
- mtime и назначение backup/tmp/result directories;
- список active processes/open files для сомнительных путей.

Можно удалять только доказанно terminal/regenerable artifacts: старые incident sqlite copies, stale temp/render directories и recovery bundles сверх их документированного retention. Нельзя удалять `/data/db.sqlite`, актуальные WAL/SHM вручную, Telegram/Telegraph token state или неизвестный directory только по имени.

После очистки/деплоя обязательны:

- `df -h /data` и root overlay;
- `PRAGMA quick_check=ok` и write probe через обычный application path;
- `/healthz ready=true`;
- active runtime log exists, grows, and contains the startup budget line;
- total `events-bot.log* <= RUNTIME_LOG_MAX_TOTAL_MB`;
- no `Errno 28` / `database or disk is full` in fresh logs.
