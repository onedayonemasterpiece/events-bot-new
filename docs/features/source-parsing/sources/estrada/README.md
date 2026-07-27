# Калининградский театр эстрады (бывший Дом искусств)

## Контракт

- source id: `estrada`;
- targeted run: `/parse estrada`;
- provenance: `parser:estrada`;
- официальный каталог:
  `https://domiskusstv.edinoepole.ru/widget/events`;
- площадка: `Калининградский театр эстрады (Дом искусств), Ленинский проспект
  155, Калининград`.

`source_parsing/venue_catalogs.py` получает корневой билетный widget, обходит
все опубликованные current/future month links и извлекает каждую отдельную
дату/время как occurrence. Нулевой каталог и HTTP/DOM failure являются
source-level ошибкой `/parse`.

Старый `source_parsing/dom_iskusstv.py` остаётся только URL-scoped обработчиком
страниц спецпроектов и не заменяет этот каталог.

## Regression checks

1. `parse_estrada_widget_html()` извлекает title/date/time/зал/возраст/цену и
   occurrence-specific `event_seats` URL.
2. `fetch_estrada_catalog()` обходит будущие месяцы и отбрасывает прошедшие
   даты.
3. После production catch-up число official future occurrences сверяется с
   активными `parser:estrada`/каноническими карточками, отдельно учитывая
   пропуски и несовпадения времени.

