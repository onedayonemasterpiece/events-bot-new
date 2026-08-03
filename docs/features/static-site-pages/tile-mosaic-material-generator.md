# Tile Mosaic Material Lab — воспроизводимый генератор плиточной мозаики

**Статус:** laboratory implementation, iteration v1; raster evidence is SHA-gated and branch-published by GitHub Actions  
**Граница:** build-time media tooling; production root и runtime сайта не меняются  
**Связанный UI-эксперимент:** `/lab/launch/tile-mosaic/`  
**Канонический код:** `tools/tile_mosaic/`  
**Review package:** `docs/review-data/tile-mosaic-material-lab/v1/`

## 1. Назначение

Генератор получает одно изображение и формирует непрерывную проекцию на решётку
из `12 × 6 = 72` отдельных плиток. Изображение не превращается в 72 независимо
кадрированные картинки: каждая фронтальная грань использует свой UV-фрагмент
одной общей текстуры. Межплиточные швы физически или растрово непрозрачны.

Задача лаборатории — отделить материал плиток от конкретной страницы запуска и
дать воспроизводимый инструмент для:

- подбора изображения, focal point и распределения открытых плиток;
- калибровки matte roughness, micro/macro grain, bevel и contact shadow;
- ограниченного blur на части плиток;
- локальных тёплых бликов на отдельных углах;
- едва заметных sub-degree наклонов, глубины и corner-lift;
- одинакового scene plan для быстрого Pillow preview и физического Blender render.

## 2. Замороженный исходный вариант

Первый принятый предварительный результат получил имя:

> **Кафель / Kafel Classic v1**

Он сохранён без перезаписи:

- render: `kafel-classic-v1.png`;
- SHA-256: `f30f78c0b585de4022ca35758a89bc7d643cf224212fd3a67b47170611ea71e7`;
- frozen scene plan: `kafel-classic-v1.png.plan.json`;
- legacy plan SHA-256: `79db292c695a749ddf7c51fe6e843fb47b761297c05a5636475ed6f3ded20212`;
- seed: `20260901`;
- focal point: `0.58 / 0.44`;
- source SHA-256: `cd4dd11f40599a6109494a49bfd33a1b9a4f62f4e7e7518d5808e5dc77e5f1e8`;
- crop: `1200 × 600`, box `[0, 52, 1200, 652]`.

`Kafel Classic` является golden baseline. Новые исследования создаются рядом и
никогда не меняют его байты, plan или имя.

## 3. Лабораторные варианты

### 3.1 Reference Balanced v1 — рекомендуемый следующий кандидат

Профиль: `refinements/reference_balanced_v1.json`.

Изменения относительно baseline ограничены:

- чуть более сухая матовая поверхность;
- редкие тёплые подсветки углов;
- selective blur для `dim`, `sleeping` и части `revealed`;
- геометрия активна примерно у 44% плиток;
- rotation до `±0.18°`, tilt до `±0.58°`;
- depth до `2.4 px`, corner-lift до `2 px`.

Это основной кандидат для дальнейшей визуальной калибровки по референсу.

### 3.2 Matte Soft v1 — исследование материала

Профиль: `refinements/matte_soft_v1.json`.

Приоритет — более мягкая и сухая matte response. Геометрические отклонения
уменьшены; blur встречается чаще, но остаётся локальным и не разрушает общую
проекцию.

### 3.3 Microtilt v1 — исследование физической неровности

Профиль: `refinements/microtilt_v1.json`.

Проверяет верхнюю допустимую границу неровности: больше sub-degree tilt,
контактных теней и sparse corner-lift. Плитки всё ещё не должны читаться как
разлетающаяся колода карточек.

## 4. Два слоя генерации

### 4.1 Source → scene plan → render

```bash
python -m tools.tile_mosaic.generate \
  --input ./source.jpg \
  --output ./out/kafel-classic-v1.png \
  --preset tools/tile_mosaic/presets/kafel_classic_v1.json \
  --backend pillow \
  --focal-x 0.58 \
  --focal-y 0.44 \
  --seed 20260901
```

Для физического рендера:

```bash
python -m tools.tile_mosaic.generate \
  --input ./source.jpg \
  --output ./out/reference-balanced-blender.png \
  --preset tools/tile_mosaic/presets/reference_balanced_v1.json \
  --backend blender \
  --blender-bin /path/to/blender \
  --engine eevee
```

Blender создаёт 72 mesh-solid, тёмные торцы, bevel, отдельную backing/grout
геометрию, procedural roughness и area-light rig. `cycles` остаётся доступным
для финального кадра, но EEVEE Next используется как быстрый физический gate.

### 4.2 Frozen baseline → bounded studies

Когда исходный remote asset недоступен, лаборатория не теряет уже принятый
результат. Variant refiner строит новые версии от frozen baseline и его plan:

```bash
python -m tools.tile_mosaic.lab \
  --frozen-base-render docs/review-data/tile-mosaic-material-lab/v1/kafel-classic-v1.png \
  --frozen-base-plan docs/review-data/tile-mosaic-material-lab/v1/kafel-classic-v1.png.plan.json \
  --output-dir artifacts/tile-mosaic-material-lab
```

Команда создаёт baseline copy, три варианта, validation reports, manifests и
contact sheet. Профили детерминированы и используют per-tile seed из frozen
plan.

## 5. Воспроизводимость

Результат идентифицируют:

1. SHA-256 исходного файла или frozen baseline;
2. SHA-256 preset/refinement profile;
3. seed и focal point;
4. immutable scene-plan SHA-256;
5. pinned Pillow `12.2.0`;
6. для Blender — checksum-pinned Blender `4.0.2` и полный render log.

Golden images перечислены в
`docs/review-data/tile-mosaic-material-lab/v1/golden-sha256.txt`. На первом PR-run
GitHub Action воспроизводит точный `Kafel Classic` из source SHA + frozen plan,
строит три bounded variants, проверяет пять exact SHA и только после этого
публикует PNG/JPEG evidence обратно в laboratory branch. Повторный run обязан
получить byte-identical package и становится no-op; wall-clock время в sidecar
manifest не создаёт drift благодаря pinned `SOURCE_DATE_EPOCH`.

Полный physical Blender result (`PNG`, `.blend`, render log, plan, manifest,
validation и Pillow-vs-Blender sheet) остаётся Actions artifact: он не раздувает
Git history, но привязан к exact PR SHA.

## 6. Acceptance gates

Обязательные автоматические проверки:

- preset/schema validation;
- exact state counts and deterministic plan;
- focal cover-crop contract;
- byte-identical repeated Pillow render;
- byte-identical repeated refinement render;
- exact source/output SHA gates for the frozen Kafel Classic baseline;
- dimensions, file size, dynamic range, entropy, dark mass, midtones and bounded highlights;
- direct arbitrary-input smoke;
- same-repository branch publication only after exact raster-golden pass;
- Blender scene creation, `.blend`, PNG and log in Actions artifact.

Обязательная ручная проверка:

- проекция остаётся непрерывной;
- grout не показывает изображение;
- matte grain не превращается в повторяющийся carbon/noise pattern;
- blur заметен только при сравнении и не выглядит ошибкой загрузки;
- угловые блики не образуют регулярную сетку;
- microtilt читается по свету и contact shadow, но не ломает спокойную плоскость стены;
- каждый остановленный кадр остаётся намеренно скомпонованным.

## 7. Что не меняется

Эта итерация не заменяет DOM-мозаику страницы запуска, не включает генератор в
production build и не меняет `/`, sitemap, navigation или indexability.
Интеграция с UI возможна только после отдельного решения: использовать
предрендеренный background, CSS-поверхность или WebGL/Blender-derived material
maps.
