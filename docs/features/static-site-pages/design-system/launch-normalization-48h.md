# Нормализация UI — исполняемый Astro-маршрут

Статус: `ACTIVE`  
Координация: `onedayonemasterpiece/events-bot-new#621`  
Implementation branch: `integration/ui-normalization-launch-20260902`  
Base at programme design: `61f7a6af5f5e82515dcd42c93dd02748297112bc`

Canonical programme и thin S:

```text
repository: onedayonemasterpiece/lovekgd-design-system
branch: integration/launch-normalized-sot-penpot-20260902
paths:
  docs/launch-normalization/README.md
  docs/launch-normalization/PARALLEL-WINDOWS.md
  docs/launch-normalization/STATUS.md
  docs/launch-normalization/CONSULTANT-K0.md
  contracts/launch-normalized-ui.v1.yaml
```

## 1. Product fact

Этот repository уже содержит executable UI, foundations, shared components,
actual routes, preview hub и production/Kaggle generation. Запуск устраняет
historical drift; он не создаёт вторую дизайн-систему.

Actual routes:

```text
/segodnya/                  current build date
/zavtra/                    next date
/date-YYYY-MM-DD/           arbitrary date
/vyhodnye/                  active/nearest weekend
/vyhodnye/YYYY-MM-DD/       selected available weekend range
```

Первые три используют `DateListingSurface`; weekend routes используют отдельный
`WeekendListingSurface`. Разные композиции сохраняются, общие families
централизуются.

## 2. Порядок выполнения

N0 сначала восстанавливает existing data export/build и создаёт technical
fresh-data baseline. Параллельно F0/M0/A0 уже могут нормализовать current source.

```text
fresh production snapshot + technical build baseline
+ parallel F0/M0/A0 source normalization
→ first integration wave
→ V0 actual DOM/computed-style audit through my-browser-bridge
→ critical drift repair
→ first owner-facing normalized /<buildId>/__preview/
→ repeat waves
```

Technical baseline нужен для before/after и не обязан быть owner checkpoint.
Family не завершена без fresh-real-data build и V0 browser verdict.

## 3. Existing generation paths

Reuse, do not replace:

```text
site/scripts/export-production-preview-data.py
site/scripts/build-preview.mjs
site/scripts/build-production.mjs
scripts/run_static_site_builder_kaggle.py
npm run build:preview
npm run build:production
npm run build:secret-candidate
npm run check:design-system
npm run check:preview
npm run check:production
npm run check:browser-release
npm run check:secret-candidate
```

Checked-in production catalogue исторический; fresh export обязателен. Нельзя
считать новую документацию восстановлением generation.

## 4. Owner review

Existing entry point:

```text
/<buildId>/__preview/
```

Первый owner-facing link должен по возможности уже содержать первую
normalization wave и V0 verdict. Владелец открывает реальные top-level pages и
проверяет framing, spacing, typography, radii, colours, icons, adaptive rows,
responsive composition и видимые anomalies.

Запрещено:

- новые `/lab/launch/*`;
- требовать owner review component catalogue;
- требовать owner acceptance Golden fixtures.

`/lab/design-system/` остаётся внутренним regression harness.

## 5. Единый component root

Визуально и поведенчески одинаковая сущность обязана иметь один canonical Astro
root или variant family root. Разница допускается только как именованный
`variant`, `state` или `composition`.

Обязательное доказательство:

- actual consumer census;
- страницы импортируют canonical root;
- page-local visual markup/CSS copy запрещена;
- один owner family anatomy/CSS;
- один canonical SVG на semantic action;
- diagnostics:

```text
data-ds-family
data-ds-version
data-ds-variant
data-ds-state  # when applicable
```

V0 сравнивает normalized DOM anatomy и invariant computed styles между actual
consumers. Одинаковые pixels при разных roots не являются PASS.

## 6. Foundations, colors, icons

F0 нормализует:

- font families/weights;
- H1–H4/body/label/metadata;
- spacing/sizing/containers/breakpoints;
- radii/borders/elevation/layering;
- semantic color tokens;
- canonical SVG/brand/medallions;
- ровно четыре semantic icon-size roles.

Все видимые UI colors должны происходить из token/semantic alias. Exact
duplicates объединяются; near-duplicates кластеризуются и объединяются при
одинаковом semantic role. Сохранённая близкая пара требует explicit
semantic/contrast reason. Raw literals остаются только в token registry, media
data и documented technical exception.

Concrete icon width/height хранится только в central tokens/utilities. Local
component dimensions запрещены. Изменение role value должно менять всех
consumers.

## 7. MediaFrame/framing

Обязательные donors:

```text
docs/features/static-site-pages/image-framing.md
site/src/lib/relatedCardLayout.mjs
site/src/components/OptimizedEventCardGrid.astro
```

Shared MediaFrame contract владеет:

```text
media role
frame ratio
contain/cover
crop permission
focal/object position
clip/overflow
fallback/loading
responsive resource selection
```

Изображение не может визуально выходить за frame. Page-local grid/card CSS не
может менять framing policy. Existing optimizer/contract расширяется; второй
параллельный algorithm запрещён без доказанной невозможности reuse.

## 8. AdaptiveEventCardGrid

Текущий optimized row path реализован не во всех multi-card surfaces. Простые
`.cards-grid` используются, в частности, в free/search/unusual/gastronomy и
preview surfaces. Они входят в census.

Целевой общий family root:

- занимает available width `100%`;
- выбирает columns/card widths по container width и named density variant;
- не оставляет phantom empty column;
- final remainder использует явный one-/two-/three-up variant и заполняет row;
- media и total card heights согласованы внутри строки;
- framing остаётся у MediaFrame;
- compact desktop/mobile transitions не дают overflow;
- browser gate измеряет row/card bounds, gaps, occupancy, media ratio и
  equal-height delta.

`relatedCardLayout.mjs` и `OptimizedEventCardGrid.astro` — исходные donors.
Новый `AdaptiveEventCardGrid` должен быть эволюцией/нормализацией существующего
решения, а не второй независимой сеткой.

## 9. Actual consumer migration

A0 владеет shell, listings и routes. Он:

- сохраняет различие DateListingSurface/WeekendListingSurface;
- мигрирует actual pages на canonical roots;
- удаляет local forks и internal overrides;
- не копирует family markup в pages;
- не создаёт новый route model.

## 10. Browser/DOM audit

V0 — отдельное read-only ChatGPT window с `my-browser-bridge` и GitHub.
После каждого integrated preview проверяются минимум:

```text
/
/segodnya/
/zavtra/
/date-YYYY-MM-DD/
/vyhodnye/
/podborki/besplatnye-sobytiya/
/populyarnoe/
/vystavki/
/festivali/
/sobytiya/<real-slug>/
```

Viewport classes: desktop wide, desktop compact, mobile 390–430.

V0 проверяет component markers/anatomy, computed typography/spacing/colors/
radii/borders/icon sizes, image bounds/object-fit/overflow/clip, adaptive row
occupancy/equal heights, responsive transitions и horizontal overflow.

Verdicts:

```text
PASS
DRIFT        → F0/M0/A0 immediately
PRODUCT_GAP  → backlog after normalization gate
BLOCKER
```

Source declaration или test без factual browser evidence не закрывает drift.

## 11. Internal Golden A=S=P

Golden использует actual routes с frozen Friday clock:

```text
/segodnya/                            Friday
/zavtra/                              Saturday
/date-YYYY-MM-DD/                     Sunday
/vyhodnye/                            same Saturday + Sunday occurrences
/podborki/besplatnye-sobytiya/        free subset
```

Target density `5 / 6 / 5`, minimum `4 / 5 / 4`. Golden не является owner
review prerequisite. После завершения family thin S фиксирует source/consumers,
`R0.PENPOT` создаёт native master/linked instances, V0 сравнивает Golden Astro
и Penpot.

## 12. Parallel ownership

- `N0`: generation, technical baseline, integration, status, preview, release;
- `F0`: foundations, colors, type, spacing, four icon roles, SVG/brand;
- `M0`: component roots, MediaFrame, EventCard/ListingEventCard,
  AdaptiveEventCardGrid;
- `A0`: shell, listings, routes, consumer migration;
- `V0`: my-browser-bridge audit; later Golden Penpot audit;
- `K0`: consultant/prompt author;
- `R0`: bounded Codex worktrees and sole Penpot writer.

Нет mandatory `MAT → QA → INTEGRATE → PUBLISH`. N0 интегрирует complete family
waves; V0 проверяет собранный actual product.

## 13. ASTRO_NORMALIZATION_PASS

Product UI-gap/change work открывается только после:

- reproducible fresh-data generation;
- tokenized foundations/colors;
- four icon roles across all consumers;
- single roots for same components;
- MediaFrame/framing PASS;
- AdaptiveEventCardGrid across applicable consumers;
- actual routes migrated;
- no critical V0 browser DRIFT.

После gate можно менять palette и интерфейс. Release затронутой family требует
обновлённый thin S и Penpot binding.

## 14. Meaningful result

Результат — это один из:

- technical fresh-data generation verdict;
- normalized real-data preview link;
- compact normalization report;
- V0 browser PASS/DRIFT report;
- native Penpot master + linked route board;
- checked release candidate.

Commit, test count, isolated specimen, empty Penpot page или hidden tree сами по
себе не являются checkpoint.