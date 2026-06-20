# TG monitoring on demand

V1 добавляет fast-path для отдельных Telegram-каналов: Bot API `channel_post` не извлекает событие сам, а только ставит source-specific запуск существующего Telegram Monitoring pipeline.

## Поведение v1

- Allowlist источников задаётся `TG_MONITORING_ON_DEMAND_SOURCES` (через запятую), default: `kraftmarket39`.
- При новом посте в allowlisted канале бот:
  - проверяет, что `telegram_source.username` существует и `enabled=1`;
  - добавляет `message_id` в `telegram_source_force_message`, чтобы Kaggle runtime явно прочитал этот пост;
  - coalesce'ит durable pending-запрос в `telegram_monitoring_on_demand_queue` по `source_username`;
  - ставит `next_run_at = now + TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS` (default `600`, 10 минут), чтобы автор канала успел внести правки.
- Periodic job `tg_monitoring_on_demand` проверяет очередь и запускает `run_telegram_monitor(..., source_usernames=[username], trigger="on_demand")`.
- Existing scheduled Telegram Monitoring остаётся catch-up контуром; on-demand — только ускоритель для новых постов.
- Если ресурс занят (`already_running`, global heavy lock, remote Telegram session busy), строка возвращается в `pending` с retry через `TG_MONITORING_ON_DEMAND_RETRY_SECONDS` (default `600`, 10 минут).
- Non-busy ошибка помечает строку `status='error'`; scheduled catch-up остаётся safety net.

## Resource-safety

On-demand не обходит существующие защиты Telegram Monitoring:

- локальный `_RUN_LOCK`;
- global heavy-operation lock;
- shared remote Telegram session guard через `kaggle_registry`;
- обычный Kaggle recovery/import контур.

За один тик по умолчанию запускается не больше одного source-specific мониторинга (`TG_MONITORING_ON_DEMAND_MAX_RUNS_PER_TICK=1`).

## ENV

- `ENABLE_TG_MONITORING_ON_DEMAND` — включает обработчик и scheduler job (default enabled when Telegram Monitoring scheduler is enabled).
- `TG_MONITORING_ON_DEMAND_SOURCES` — allowlist Telegram usernames, default `kraftmarket39`.
- `TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS` — debounce перед первым запуском, default `600`.
- `TG_MONITORING_ON_DEMAND_RETRY_SECONDS` — retry busy pending rows, default `600`.
- `TG_MONITORING_ON_DEMAND_POLL_SECONDS` — частота проверки очереди, default `60`.
- `TG_MONITORING_ON_DEMAND_SEND_PROGRESS` — если `1`, отправляет live progress в superadmin chat; финальный отчёт отправляется стандартным Telegram Monitoring кодом при наличии bot/chat.
