# Возрастная маркировка события на всех публичных поверхностях

> **Статус:** принятый TO-BE release contract. Каноническая модель данных и
> extraction pipeline описаны в
> [`../event-age-rating/README.md`](../event-age-rating/README.md). Этот документ
> владеет только сквозной публичной паритетностью и release evidence.

## Решение

Возраст события — nullable source-grounded факт:

```text
0+ | 6+ | 12+ | 16+ | 18+ | null
```

`null` означает «подтверждённая маркировка неизвестна». Значение `0+` по
умолчанию запрещено. Возраст участника, героя произведения, отдельного номера
программы или рекламная формулировка не становятся рейтингом события.

Публичный режим остаётся `declared_only`, пока отдельный independent gate не
разрешит иной источник. Internal model assessment не выдаётся за маркировку
организатора.

## Один projection и один renderer contract

Каждый public consumer получает одно и то же каноническое значение из static
projection. Повторный разбор title/description/OCR внутри Astro-компонента,
поиска, ICS или JSON-LD запрещён.

Если non-null rating есть, он обязателен на всех event-bearing поверхностях:

- главная, date/weekend/category/listing;
- поиск, related, personal feed, collections и festivals;
- `Избранное`;
- event detail и transport representations;
- first-party share card/text;
- ICS и поддерживаемая structured-data projection при полном совпадении с
  видимым HTML.

Формат:

- карточка: видимый text badge `12+`;
- detail: `Возрастное ограничение · 12+`;
- accessible name: `Возрастное ограничение 12+`;
- значение не кодируется только цветом или иконкой.

## Конфликты и lifecycle

- `manual_override` не перезаписывается автоматикой;
- разные declared values не сворачиваются через `max()`/`min()`;
- конфликт остаётся `null/conflict` до semantic adjudication;
- merge/split/source correction инвалидирует старые derivatives;
- изменение accepted rating запускает обычный static rebuild/promotion;
- ended/cancelled/merged-away lifecycle не сохраняет самостоятельную
  противоречащую projection.

## Release evidence

1. Full catalog audit различает `source_absent`, `canonical_missing`,
   `conflict`, `invalid`, `projection_lost`, `renderer_missing`.
2. Component inventory перечисляет все event-bearing renderers и generated page
   families.
3. Contract test проводит `0+/6+/12+/16+/18+/null/conflict` через source →
   canonical field → static export → HTML/ICS/JSON-LD.
4. Playwright проверяет mobile/tablet/desktop, no-JS, keyboard и screen reader.
5. Build gate падает, если non-null projected rating отсутствует хотя бы на
   одной inventoried public surface или расходится с derivative.

## Acceptance

- [ ] 100% событий с non-null rating показывают его на 100% inventoried
  event-bearing surfaces.
- [ ] Unknown/conflict не превращаются в default badge.
- [ ] Visible HTML, share, ICS и structured data совпадают.
- [ ] Evidence связан с exact release SHA и catalog revision.
- [ ] Renderer использует общий formatter/component, а не локальные формулы.
