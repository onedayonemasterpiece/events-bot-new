# Runtime Logs

Каноническая политика краткоживущих runtime-логов на prod-машине.

## Purpose

- сохранить эксплуатационные логи на volume машины, чтобы можно было разбирать реальные scheduler/job инциденты постфактум;
- не полагаться только на краткий буфер Fly logs;
- не держать логи дольше суток, чтобы не раздувать `/data`.

## Current Production Policy

- production currently keeps this mirror **disabled** (`ENABLE_RUNTIME_FILE_LOGGING=0`) after the April 16, 2026 disk-pressure incident on Fly volume `/data`;
- the file-logging path remains available as an incident/debug tool, but should not stay permanently enabled on the current volume size without explicit space budgeting;
- when temporarily enabled, it writes the existing application root logger to a file mirror;
- default path: `RUNTIME_LOG_DIR=/data/runtime_logs`;
- default active file name: `RUNTIME_LOG_BASENAME=events-bot.log`;
- rotation: hourly;
- retention: about 24 hours through `RUNTIME_LOG_RETENTION_HOURS=24`.

Практически это значит:

- активный текущий час пишется в `events-bot.log`;
- прошлые часы уходят в hourly rotated файлы рядом;
- старые rotated файлы автоматически удаляются примерно после суток хранения.

## Scope

В файл попадает уже существующий runtime stream root logger, поэтому туда идут:

- scheduler/job события (`tg_monitoring`, `guide_monitoring`, `vk_auto_import`, `video_tomorrow` и т.д.);
- traceback'и и runtime warnings;
- обычные `INFO/WARNING/ERROR` сообщения приложения.

Это не отдельная “спец-диагностика по одной фиче”, а единый эксплуатационный журнал.

## Environment

- `ENABLE_RUNTIME_FILE_LOGGING` — включает file logging mirror.
- `RUNTIME_LOG_DIR` — директория хранения логов.
- `RUNTIME_LOG_BASENAME` — базовое имя текущего файла.
- `RUNTIME_LOG_RETENTION_HOURS` — сколько часов хранить rotated logs.
- `RUNTIME_LOG_LEVEL` — optional override для file handler; если не задан, используется текущий уровень root logger.

## Agent Investigation Workflow

Для production/scheduled/Kaggle-разборов агент обязан сначала проверить file mirror, а не начинать с предположения, что логов нет.

1. Проверить фактическую конфигурацию на машине: `ENABLE_RUNTIME_FILE_LOGGING`, `RUNTIME_LOG_DIR`, `RUNTIME_LOG_BASENAME`, `RUNTIME_LOG_RETENTION_HOURS`.
2. Проверить наличие директории и файлов в `RUNTIME_LOG_DIR` (по умолчанию `/data/runtime_logs`), включая активный `events-bot.log` и hourly rotated файлы.
3. Искать по нескольким ключам: `run_id`, `ops_run` id, job kind (`tg_monitoring`, `guide_monitoring`, `video_tomorrow`), Kaggle kernel ref, source username, machine-local time window and error class.
4. Если file mirror выключен, директория отсутствует или retention уже удалил нужное окно, явно зафиксировать это в отчёте и только затем переходить к fallback-источникам.
5. Fallback-источники для runtime evidence: `fly logs`, Kaggle output/log artifacts, `ops_run.details_json` / status rows в production DB, локальные артефакты в `artifacts/codex/`.
6. Для долгих или спорных расследований сохранить минимальные релевантные выдержки логов/JSON в `artifacts/codex/<task-or-run-id>/`; не коммитить эти артефакты.

Нельзя писать “логи потеряны” или “логов нет”, пока не проверены: file mirror на volume, rotated files, Fly logs, Kaggle output/logs, production `ops_run` и локальные `artifacts/codex/`.

## Operational Notes

- на Fly production лог-файлы должны жить только на volume (`/data/...`), а не в ephemeral filesystem контейнера;
- этот механизм предназначен для короткой incident-retention, а не для долгого архивирования;
- если нужен длительный аудит, данные нужно отдельно выгружать/переносить, а не увеличивать retention на машине без оценки места на диске.
