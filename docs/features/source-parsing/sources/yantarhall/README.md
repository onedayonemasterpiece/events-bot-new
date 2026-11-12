# Янтарь холл

## Контракт

- source id: `yantarhall`;
- targeted run: `/parse yantarhall`;
- provenance: `parser:yantarhall`;
- официальная афиша: `https://янтарьхолл.рф/`;
- площадка: `Янтарь холл, Ленина 11, Светлогорск`.

Telegram Monitoring источника `@yantarholl` остаётся включённым, но посты
канала не являются полной афишей. Полноту обеспечивает отдельный host-side
parser в `source_parsing/venue_catalogs.py`: он читает первую страницу Bitrix и
следует `data-next-page` с фиксированным AJAX id до terminal page (hard cap —
20 страниц), дедуплицируя occurrence по date/time/title/URL.

Карточка передаёт официальный detail URL, фото, возраст и минимальную цену в
общий TheatreEvent → Smart Update path. OCR для этого source по умолчанию
выключен: структурированная афиша уже содержит якоря, а poster media всё равно
проходит managed CDN gate.

Одна occurrence встречается в двух представлениях каталога. До дедупликации
parser предпочитает desktop `.event-image`, содержащий полноразмерную афишу,
перед расположенной раньше в DOM `.image-mobile` миниатюрой. Это устраняет
выбор 366×450 вместо доступной 1800×900 версии, как у «Галина, GALA!».
Если доступен только mobile layout, его изображение сохраняется. Отдельный
detail HTTP fetch и дополнительные gallery refs для этих вариантов не нужны.

## Regression checks

1. Fixture покрывает tile и table-list DOM layouts, percent-encoding URL,
   AJAX next/terminal page, переход года и реальную mobile/desktop пару
   «Галина, GALA!» с единственным полноразмерным poster ref.
2. Live probe должен возвращать ненулевой полный каталог и последнюю
   опубликованную будущую дату, а не только первую страницу.
3. Production catch-up сверяется occurrence-by-occurrence с официальной
   афишей; Telegram freshness сама по себе не считается coverage evidence.
