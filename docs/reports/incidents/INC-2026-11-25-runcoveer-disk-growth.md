# INC-2026-11-25 — повторное исчерпание места RunCoveer

Status: open
Severity: sev2
Service: RunCoveer / Events Bot / VK import
Opened: 2026-11-25
Closed: —
Registry: `inc_a8cee70d439ddda8bca27e4d`
Work: `work_e846746b8a3defdbb048b3bc`
Related: `INC-2026-10-14-scheduler-disk-exhaustion`,
`INC-2026-09-26-vk-auto-storage-notice-storm`.

## Summary and impact

Владелец увеличил диск на 5 ГБ после отказа VK-импорта 23 ноября, но получил
такой же отказ 25 ноября. Оба эпизода подтверждаются `ops_run`; это реальные
проверки свободного места, а не повторный показ прежнего уведомления.

Диск сейчас около 29 GiB в `df` (номинально 30 GB). Порог VK остаётся
512 MiB и не снижался. Отказ остановил batch до обработки inbox; данные
не были массово переведены в failed из-за нехватки диска.

## Timeline

Времена UTC; для Калининграда прибавить два часа.

- 23 ноября 08:15, ops 13188: 249 MiB свободно; 10:00, ops 13205:
  279 MiB; 13:30, ops 13260: ноль. Все — storage admission.
- Первый сохранившийся замер host maintenance после расширения, 16:14:
  5,079,658,496 байт свободно. Само расширение выполнил владелец.
- 24 ноября: свободное место колебалось примерно от 1.9 до 3.8 GB;
  скачки вниз/вверх около размера полной SQLite-копии.
- 25 ноября 08:59: host maintenance зафиксировал 12,337,152 байт свободно.
- 09:57–10:00:34: vector ops 13442 успешно выполнялся со снимком БД.
- 10:00, VK ops 13443: 419 MiB при минимуме 512, processed=0.
- 10:12:28: отдельная задача выпустила PR #1099, заменив контейнер Events
  с сохранением `/data`. Её registry timeline: `inc_af96f6510a55a8ccc1d713dc`.
- 10:14: host maintenance уже показывает 6.51 GB свободно; 10:29 — 8.08 GB.
  Между 09:59 и 10:14 автоматическая очистка builder cache/APT не освободила
  этот объём: её собственный before/after различается лишь на килобайты.
- 10:32–10:46: VK catch-up 13446 обработал 12 inbox rows, создал два события.
  Storage errors отсутствуют; три technical outcomes связаны с identity LLM.
- 11:34–11:35: прежнее поручение владельца выпустило исключение собственных
  Telegram-источников, runtime SHA `496bc7dfd8396388e0043060862a5b73525064be`.
- 11:36–11:39:27: наблюдался обычный vector ops 13453, около 1.5 GiB в
  `/tmp/event-vector-sync-7genn3rg`. После SUCCESS этот каталог исчез.

## Observed storage composition

Замеры 25 ноября около 11:37–11:43 UTC, без удаления файлов.

| Объект | Размер | Что установлено |
| --- | --- | --- |
| Persistent Events `/data` | 4.2 GiB | Сохраняется при замене контейнера |
| Основная SQLite | 1,566,040,064 bytes | Freelist три страницы, это занятые данные |
| `vk_source_packet` | 530,501,632 bytes | Самая большая таблица, source evidence |
| `smart_update_candidate_state` | 172,654,592 bytes | Меньше полной БД |
| `event` | 133,554,176 bytes | Меньше полной БД |
| Guide media / results | 1.4 GiB / 407 MiB | Persistent data, не удалялись |
| Runtime file logs | 331 MiB | Меньше установленного hard budget 512 MiB |
| Containerd | 8.4 GiB | Суммарно images, shared layers и writable layers |
| Обычный временный vector snapshot | около 1.5 GiB | Удаление после SUCCESS наблюдалось |
| Persistent vector projections | 238 MiB | Два небольших projection snapshot |

Docker перечисляет 80 images, но его reclaimability возвращает отрицательный
размер. Эти значения нельзя использовать как расчёт безопасной очистки.
Активные и rollback images, зависимости слоёв и девять старых остановленных
preflight containers требуют отдельного точного manifest, если их удалять.

## Root cause assessment

Доказано: недостаточное место действительно прервало VK import. Полная
SQLite-копия vector sync временно расходует около 1.5 GiB и в 10:00 совпала
с переходом ниже admission floor. Это регулярный рабочий пик; один конкретный
завершённый run проверен до и после и не оставил свой snapshot.

Гипотеза с сильной временной привязкой: несколько гигабайт прежнего
расхода принадлежали writable layer/открытым временным файлам старого
контейнера. После его штатной замены место вернулось, при сохранении `/data`.
Точный перечень тех файлов и процесс-создатель не установлены: старый
контейнер удалён в 10:12, до этого расследования; размеры внутренних
каталогов перед заменой не сохранялись. Нельзя называть vector snapshot
утечкой или приписывать все освобождённые гигабайты конкретному downloader.

Выборка hourly WAL checkpoints за 23–25 ноября дала `(0,0,0)`; большого
текущего WAL нет. Эти наблюдения не доказывают отсутствие любого краткого
WAL-пика, но не поддерживают его как основной источник накопленных гигабайт.

## Immediate mitigation and corrective actions

По прямому требованию владельца 25 ноября вместо одного расследования
подготовлены меры против повторения:

- `event_vector_sync` использует существующий compact public projection вместо
  полной копии SQLite со служебными таблицами. Поля public revision сохранены
  в allowlist; ревизии читаются с SQLAlchemy type conversion. Семантика экспорта
  и поиска не меняется. TemporaryDirectory сохраняет штатную очистку.
- Существующий 15-минутный disk-maintenance при warning/critical до очистки
  сохраняет размеры разрешённых host-каталогов, `/tmp` и `/data` Events и
  container writable sizes в journal и status receipt. Три read-only команды
  ограничены восемью секундами каждая и 16 KiB вывода. Ошибка измерения не
  останавливает существующую безопасную очистку builder/APT cache.
- Новых таймеров, автоматического расширения, удаления images/volumes,
  canonical data, failed/unknown outputs или изменения admission floor нет.

Это сокращает измеренный штатный пик; writer исторического накопления в
удалённом контейнере по-прежнему неизвестен. Сбор разбивки закрывает конкретный
пробел прежнего maintenance, который сохранял только общий free bytes.

## Automation Contract

### Treat as regression guard when

Меняются vector/static snapshots, временные output директории, downloader
cleanup, release image retention или host disk maintenance.

### Affected surfaces

`event_vector_sync.py`, `static_site_release.py`, video output lifecycle,
`deploy/runcoveer/disk-maintenance.py`, VK admission и `/healthz`.

### Mandatory checks before closure or deploy

- Зафиксировать конкретный путь и writer накопления либо воспроизвести рост.
- Для исправленного lifecycle сохранить активное/failed/unknown evidence;
  проверять нормальный terminal outcome и повторный обычный запуск.
- Проверить baseline и peak free bytes, readiness и VK admission.
- Для recovery читать exact ops_run/inbox receipts; не повторять уже
  завершённый catch-up 13446 и не менять semantic technical outcomes.
- Не использовать общий image/volume prune и не удалять source evidence.

### Required evidence

Retained: `/home/dev/artifacts/events-bot-new/20261125T113655Z-runcoveer-disk-growth-20261125/`.
`maintenance-journal.jsonl`, `ops-storage-vector.json`,
`storage-log-extract.json`, `current-storage.txt`.

## Current work and next step

Цель владельца: принять меры против повторного заполнения, сохранив данные и
импорты. Локальные изменения реализованы; 126 тестов прошли. Проверка на
production data нашла различие только порядка двух publication URL события
9779. У event_publication полный DB index задавал platform/target порядок,
а компактная копия без индексов давала порядок вставки. Экспорт теперь явно
сортирует platform/target, добавлен regression с разными query plans.
Измерение выполняется без сети; SELECT-only доступ к live SQLite использует
rw mount для штатных WAL/SHM metadata. Следующие шаги — CI, merge exact
main, Events-only release и установка host maintenance, затем live readback.

## Release And Closure Evidence

Локально: 126 passed (`event_vector_sync`, `runcoveer_disk_maintenance`,
`static_site_release`, projection snapshot/handoff/content, pgvector export).
Развёртывание этого исправления ещё не запускалось. Общий инцидент открыт:
исторический накопитель не установлен. VK catch-up 13446 уже завершён и
повторно не запускается; существующий vector 13453 штатно удалил snapshot.
