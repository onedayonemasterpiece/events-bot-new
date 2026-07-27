# Парсер Калининградской областной филармонии

Статус: production, Kaggle-backed `/parse` source.

## Источник и контракт

- Официальная афиша: `https://filarmonia39.ru/afisha/`.
- Kernel: self-contained script
  `kaggle/ParsePhilharmonia/philharmonia_parser.py`
  (`zigomaro/parse-philharmonia-script`).
- Результат: `philharmonia_results.json`.
- Production boundary: `source_parsing/philharmonia.py` → общий Smart Update.
- Provenance: принятый результат обязан создать `event_source` с
  `source_type=philharmonia` / parser provenance, даже если событие уже было
  найдено через VK или Telegram.

Парсер не использует старые month URL `/?event&m=...` и не зависит от
Playwright. Он загружает текущий каталог и детальные страницы обычным HTTP:

- карточка: `article.entry[data-date-iso]`;
- название/URL: `a.production_detail_link`;
- дата: `data-date-iso`, время: `.session .hour`;
- зал/возраст: `data-venue`, `data-age`;
- изображение/Пушкинская карта: `.production_image`,
  `.list_logo_pushkin_card`;
- полное описание: `.production_description .text_container`;
- цена/продажа: `.price_block .value`, live `.session_entry` и buy link.

Если каталог вернул ноль будущих событий или детальная страница не содержит
описания, kernel должен завершиться ошибкой, а не публиковать пустой или
обрезанный «успешный» результат.

`kernel-metadata.json.code_file` обязан указывать прямо на parser script.
Kaggle API отправляет в kernel только этот code file; notebook-loader,
ссылающийся на соседний `.py`, не является self-contained и в remote runtime
теряет parser module. Per-run status-dataset не является входом парсера:
ошибка Kaggle Dataset API может отключить callback telemetry, но не сам импорт;
terminal source status подтверждается host polling и `ops_run`.
Script использует отдельный Kaggle slug: API не разрешает менять editor type
у существующего notebook kernel `zigomaro/parse-philharmonia`.

## Нормализация

- Площадка: `Филармония им. Светланова`.
- Адрес: `Хмельницкого 61а`.
- Сцена сохраняется из `data-venue` (обычно `Концертный зал`).
- `age_restriction`, `pushkin_card`, price bounds и source-native ticket status
  передаются в `TheatreEvent` и далее в Smart Update.
- Source URL — детальная страница события; отдельный buy URL используется
  парсером для определения доступности билетов.

## Regression contract

Изменения DOM, Kaggle status bootstrap, параллельного запуска `/parse` или
production output boundary обязаны поднять
`INC-2026-07-27-future-event-source-coverage-drop.md`.

Минимальные проверки:

1. replay текущего listing/detail DOM из
   `tests/replays/INC-2026-07-27-future-event-source-coverage-drop/`;
2. live kernel возвращает ненулевой список и его count сверяется с официальной
   будущей афишей на момент прогона;
3. production processing создаёт/обновляет parser provenance;
4. `ops_run` не `success`, если Philharmonia kernel/parse потерян;
5. параллельный старт Theatres + Philharmonia + Qtickets не даёт
   `cannot start a transaction within a transaction`;
6. `kernel-metadata.json` указывает на self-contained
   `philharmonia_parser.py`, а primary parser runners не создают обязательный
   per-run status-dataset.

## Отложенные улучшения

Семантическое форматирование раздела «В программе» и дополнительные изображения
галереи остаются отдельными улучшениями. Они не должны блокировать импорт
события при наличии полного source-grounded описания и основной афиши.
