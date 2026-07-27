# Реестр артефактов

> **Статус:** реализован редакторский registry prototype и отдельный noindex
> prototype конкретной коллекции; production placements, application form и
> draw backend отсутствуют.

## Публичные точки

- HTML: `/artefakty/`
- User collection prototype:
  `/artefakty/kollektsii/znaki-yantarnogo-kraya/`
- Public JSON: `/data/artifacts.json`
- Source of truth:
  [`site/src/data/artifactRegistry.json`](../../../site/src/data/artifactRegistry.json)
- Build-time validation:
  [`site/src/lib/artifacts.ts`](../../../site/src/lib/artifacts.ts)

Публичный JSON намеренно не содержит Telegram source refs, review flags, active
placement IDs/URLs, clues, participant identity или progress. Private
operator inventory остаётся отдельной будущей surface.

`/artefakty/` — редакторский реестр, а не личный альбом. Он может раскрывать
состав только пока collection status=`draft`. После перехода в `collecting`
точный active membership не должен публиковаться в реестре: пользовательская
страница показывает только общее число slots, а для ненайденных slots — ни
изображение, ни имя.

## Что считается записью

Каждый `collectible` имеет:

- immutable `id` и `slug`;
- `public_name`;
- `story_domain`;
- editorial `registry_status`;
- membership в нуле или нескольких коллекциях;
- planned difficulty только после включения в коллекцию;
- internal review flags и source refs.

Статусы первого реестра:

| Status | Meaning |
|---|---|
| `candidate` | идея нормализована, но ещё не включена в коллекцию |
| `needs_clarification` | не установлен точный объект/сюжет |
| `collection_draft` | входит в черновик коллекции, но не опубликована как active |

Статус не означает, что факты, права или визуальный asset уже приняты.

## Первая коллекция

Канонический состав хранится только в JSON collection
`signs-of-kaliningrad-001`:

- public name: **«Знаки Янтарного края»**;
- `8` артефактов;
- первый onboarding specimen — найденный и консолидированный
  **«Янтарный космонавт»**;
- все доступны одновременно;
- planned window: `14d`;
- application-only grace: `48h`;
- unlock: `60%`, что для дискретного набора означает `ceil(8 × 0.60) = 5`;
- threshold открывает форму заявки, а не создаёт entry автоматически.

Подробные fairness и state rules:
[collection-contract.md](collection-contract.md).

## Versioning

- Любое изменение имени, membership, threshold или public status повышает
  `registry_version`.
- Запущенная коллекция фиксирует registry/rules version; редактирование source
  не меняет её задним числом.
- Active/future placement locations не появляются в public projection.
- Удаление сломанной записи из source не стирает уже найденный предмет: она
  архивируется или получает versioned replacement по safety contract.
