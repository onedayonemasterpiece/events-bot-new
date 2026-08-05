# Сквозной visual QA медальонов

> **Статус:** обязательный release gate статического сайта. Каноническая
> семантика и assets остаются в
> [`event-token-medallions.md`](event-token-medallions.md).

## Решение

Dedicated lab и image-load smoke не доказывают качество реальных страниц.
Playwright строит inventory из frozen RC и захватывает **каждую фактическую
static-site surface**, где медальон виден.

Для каждого экземпляра проверяются:

- отсутствие clipping, dirty matte, alpha fringe и обрезанной тени;
- корректная геометрия, optical centering и padding;
- читаемость на реальном фоне и рядом с другими tokens;
- отсутствие overlap и horizontal overflow;
- загрузка intended SVG/WebP и допустимого fallback;
- правильный accessible name;
- отсутствие duplicate identity token.

Telegram custom emoji — отдельный runtime и не подменяет этот gate.

## Inventory contract

1. Scan всей generated public HTML tree по medallion selectors.
2. Source-tree inventory всех renderer/component invocations.
3. Capture каждой generated event/detail surface с медальоном.
4. Exhaustive isolated asset sheet в `/lab/medallions/`.
5. Negative assertions для listing/search/related/personal surfaces, пока они не
   включены в approved scope.

Новая renderer surface автоматически становится capture target; silent allowlist
исключение запрещено.

## Matrix

- каждый actual URL: `390×844` и `1440×1000`, full context + section crop;
- каждый distinct layout: `768×1024`;
- lab boundary: `320` и `375`.

Capture ждёт fonts, decoded images, stable bounds и отсутствие relevant
console/network failures. Animations отключаются. Automated geometry/diff
assertions обязательны, но финальный visual review они не заменяют.

## Verdicts

```text
pass
fail_asset
fail_layout
fail_loading
fail_semantics
blocked_capture
```

`blocked_capture` блокирует release. Исправление скрытием токена допустимо
только при подтверждённой identity ambiguity и явно принятом fail-closed
product outcome.

## Acceptance

- [ ] Inventory покрывает 100% renderer invocations и generated pages.
- [ ] Все actual targets имеют required screenshots.
- [ ] Каждый manifest medallion имеет lab capture или reviewed
  `not_rendered_in_rc`.
- [ ] Ноль `fail_*`, `blocked_capture`, clipping, matte, overlap и overflow.
- [ ] Owner sign-off привязан к immutable RC SHA/build.
