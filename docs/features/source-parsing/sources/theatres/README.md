# Театры: парсинг событий (/parse)

## Обзор

Команда `/parse` запускает:

- общий Kaggle parser для Драмтеатра, Музтеатра, Кафедрального собора и
  Третьяковки;
- host-side официальные HTTP-каталоги Театра эстрады и Янтарь холла.

Также доступен диагностический режим: `/parse check` (запуск без сохранения в БД).

## Логика определения существующих событий

При получении события из JSON выполняется поиск в базе данных:

```
find_existing_event(location, date, time, title)
```

### Алгоритм поиска

1. **Поиск по location + date** — находим все события в той же локации на ту же дату
2. **Fuzzy match по title** — сравниваем названия (порог 85%)
3. **Проверка времени**:
   - Если в БД время `00:00` (placeholder) → **полное обновление** события
   - Если время совпадает → **обновление статуса билетов**
   - Если время разное и карточка уже имеет provenance того же parser-источника
     → это отдельный сеанс, событие НЕ совпадает
   - Если parser provenance ещё нет (например карточка пришла только из
     Telegram), сайт может исправить её ошибочное время через Smart Update

### Результаты обработки

| Статус | Описание |
|--------|----------|
| ✅ Добавлено | Новое событие, создано в БД |
| 🔄 Обновлено | Найдено существующее, обновлён статус билетов |
| ❌ Ошибок | Не удалось обработать событие |
| ⏭️ Пропущено | Smart Update вернул реальный skip-статус |

## Источники данных

| Источник | Файл JSON | Локация в БД |
|----------|-----------|--------------|
| dramteatr | dramteatr.json | Драматический театр |
| muzteatr | muzteatr.json | Музыкальный театр |
| sobor | sobor.json | Кафедральный собор |
| tretyakov | tretyakov.json | Третьяковская галерея |
| estrada | официальный Edinoe Pole widget | Калининградский театр эстрады (Дом искусств) |
| yantarhall | официальный Bitrix/AJAX catalog | Янтарь холл, Светлогорск |

## Что обновляется при совпадении

При `ticket_updated`:
- `ticket_status` (available/sold_out)
- `ticket_link` (если отсутствовал)

При `needs_full_update`:
- `time`
- `ticket_status`, `ticket_link`
- `pushkin_card`
- `description` (если отсутствовал)
- `photo_urls` (если отсутствовали)

## Влияние на ежедневный анонс

- **Новые события** (`new_added`) → появляются в анонсе "ДОБАВИЛИ В АНОНС"
- **Обновлённые события** (`ticket_updated`) → НЕ появляются в анонсе (не меняется `added_at`)

## Устойчивость браузерного обхода

Все чтения DOM в общем Kaggle notebook проходят через bounded retry после
`domcontentloaded`. Это защищает последовательный обход источников от временной
ошибки Playwright `Page.content: ... page is navigating`: краткий redirect
Музтеатра не должен обрывать Собор и Третьяковку или отбрасывать уже собранный
Драмтеатр. Исчерпание пяти попыток остаётся fail-closed ошибкой kernel run.

## Диагностика

Для отладки включите логи:
```bash
export LOG_LEVEL=DEBUG
```

Логи покажут:
- `find_existing_event: MATCHED` — событие найдено
- `find_existing_event: NO MATCH` — событие новое

## Связанные документы

- Общий индекс фичи: `docs/features/source-parsing/README.md`
- Pyramida: `docs/features/source-parsing/sources/pyramida/README.md`
- Театр эстрады: `docs/features/source-parsing/sources/estrada/README.md`
- Янтарь холл: `docs/features/source-parsing/sources/yantarhall/README.md`
