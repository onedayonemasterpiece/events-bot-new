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

## 2. Continuous execution, не numbered micro-waves

N0/F0/M0/A0 назначаются на полный owned product contour. Wave, branch, commit и
`[RESULT]` — checkpoints, но не команда завершить turn.

```text
fresh-read current state
→ recompute unresolved role backlog
→ select highest-value safe reversible item
→ analyze/decide/implement/review
→ publish checkpoint when useful
→ fresh-read again
→ continue next item
→ end only when independent owned backlog is exhausted
```

Старое `finish with [RESULT]` означает «не возвращай plan-only status». Оно не
означает `после result остановись`.

Per-Wave owner resume запрещён. Допустимый standby требует одновременно:

- independent owned backlog равен нулю;
- exact resume trigger записан;
- дальнейшая работа зависит от физически отсутствующего preview, cross-owner
  API/path decision, integration output или V0 DRIFT.

Нельзя завершаться на dispatch, branch/worktree creation, одном commit,
rehearsal, baseline-only decision или ожидании dependency при наличии другой
owned работы.

## 3. Порядок выполнения

```text
N0: candidate review → same-data baseline → conditional promotion
    → full Kaggle generation → reachable preview → V0 trigger
+
F0/M0/A0: saturation всех current owned consumers/invariants
+
V0: complete harness → actual browser matrix when URL exists
+
R0: integration, local focused diagnostics and invocation/observation of the
    shared Kaggle build
→ first owner-facing normalized /<buildId>/__preview/
```

Technical baseline нужен для before/after и не обязан быть owner checkpoint.
Family не завершена без fresh-real-data Kaggle build и V0 browser verdict.

N0 не дробит critical path на обязательные owner wake-ups, если acceptance
criteria можно определить заранее. Он задаёт R0 conditional end-to-end branch:

```text
IF exact candidate focused diagnostics/tests/same-data checks PASS
  THEN promote exact candidate
  AND invoke the one shared Kaggle pipeline
  AND publish reachable preview through its checked artifact
ELSE
  no promotion/deploy
  continue safe diagnosis
  publish factual defect
```

R0 после каждого result fresh-read-ит #621/current refs и продолжает следующую
ready safe mechanical task. При ожидаемом N0 trigger R0 использует bounded watch
60–120 seconds, maximum 30 minutes, а не немедленный exit.

## 4. Один build/publish contract

### 4.1 Полные и опубликованные сборки

Существует один canonical static-site build implementation в этом repository,
один Kaggle StaticSiteBuilder и один текущий Yandex Object Storage bucket.

Через Kaggle CPU всегда выполняются:

```text
real Review Preview
Golden Review Preview
Release Candidate
production-form build
```

Они различаются только:

- data mode (`real` или `golden`);
- immutable root `slug/prefix`;
- optional allowlisted page-class filter для тестового preview;
- уровнем проверок и правом promotion.

Focused preview, который публикуется по секретной ссылке, также проходит через
тот же Kaggle pipeline. Он не является второй архитектурой и не использует
второй publisher/bucket.

Future/default-off two-root bucket/ALB design не входит в текущий launch path и
не является prerequisite для review preview, RC или production-form artifact.

### 4.2 Локальная диагностика

Без Kaggle разрешено локально отрендерить один route либо один уже определённый
page class, чтобы быстро найти syntax/import/build/layout defect.

Локальная диагностика обязана использовать те же:

- Astro route/component sources;
- exact source SHA;
- immutable real snapshot либо Golden corpus identity;
- page-class selector, которым пользуется Kaggle.

Она не:

- загружает результат в Yandex Object Storage;
- обновляет `preview.current`;
- создаёт owner-facing secret URL;
- считается Review Preview, RC, production build, V0 acceptance или A=S=P
  evidence;
- образует второй production/build path.

Термины `local build`, `tests/build` и `local browser smoke` далее в этом
документе означают только такую focused diagnostic работу. Любой полный или
опубликованный результат означает запуск Kaggle.

### 4.3 Page-class filter

Page-class filter является одной allowlisted моделью в `events-bot-new` и
используется одинаково локальной диагностикой и Kaggle. `my-data-hub` передаёт
выбранное значение, но не реализует собственную классификацию страниц.

Существующий `catalog-mode: slice|full` управляет объёмом event data и **не**
является page-class filter. Эти два понятия нельзя смешивать.

При отсутствующем фильтре строится весь review tree. Нельзя поддерживать два
разных списка классов/маршрутов в локальном runner и MCP.

### 4.4 My Data Hub

`my-data-hub` — единственный MCP control plane для опубликованных review builds:

```text
kenigevents.preview.start
kenigevents.preview.current
operation.get
```

Он может разрешить ref в SHA, выбрать snapshot/corpus, поставить operation в
очередь, вызвать runner и вернуть состояние/ссылку. Он не владеет вторыми
копиями exporter, page-class selector, Astro builder, Object Storage publisher
или retention logic.

Любая уже начатая реализация этих MCP methods должна быть найдена и продолжена;
параллельный второй implementation/worktree запрещён.

### 4.5 Existing implementation paths

Reuse, do not replace:

```text
site/scripts/export-production-preview-data.py
site/scripts/build-preview.mjs
site/scripts/build-production.mjs
scripts/run_static_site_builder_kaggle.py
kaggle/StaticSiteBuilder/static_site_builder.py
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
считать документацию восстановлением generation.

N0 не читает runtime SQLite. Build/export output должен возвращать buildId и
output identity. Read-only SQLite fallback допустим только native R0, если
конкретная generation task доказывает его необходимость.

Preview и production используют один production rail: exporter, page-class
selector, Kaggle `StaticSiteBuilder`, artifact checks, Object Storage publisher
и retention принадлежат `events-bot-new`. `my-data-hub` только разрешает
`source_ref`, выбирает/reuses snapshot или corpus, ведёт MCP operation и вызывает
этот runner; второй builder/exporter/publisher там запрещён. Локальная сборка —
диагностика, а не альтернативный deploy path.

Publisher загружает и затем независимо перечитывает каждый объект immutable
`/<buildId>/` prefix. Оба сетевых этапа выполняются bounded pool из максимум
восьми workers; create-only `IfNoneMatch=*`, SHA-256, MIME verification,
root/stable-ICS isolation и retry-safe adoption существующих byte-identical
objects сохраняются. Ограниченная конкурентность устраняет последовательные
round trips полного preview, не меняя generation/publication semantics.
Asset и Astro-asset origins принимают тот же `{buildId}` template: immutable
preview поэтому ссылается на ассеты собственного опубликованного prefix, тогда
как production без template сохраняет stable origin.
Golden build не рендерит независимую daily service-share карточку из реального
сегодняшнего каталога: её отсутствие не должно ломать frozen future corpus.
Real preview и production-form build сохраняют обычный service-share этап.

## 5. Owner review

Existing entry point:

```text
/<buildId>/__preview/
```

Первый owner-facing link содержит нормализованный candidate и V0 verdict.
Владелец открывает реальные top-level pages и проверяет framing, spacing,
typography, radii, colours, icons, adaptive rows, responsive composition и
видимые anomalies.

Запрещено:

- новые `/lab/launch/*`;
- требовать owner review component catalogue;
- требовать owner acceptance Golden fixtures.

`/lab/design-system/` остаётся внутренним regression harness.

## 6. Единый component root

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

## 7. Foundations, colors, icons

F0 нормализует:

- font families/weights;
- H1–H4/body/label/metadata;
- spacing/sizing/containers/breakpoints;
- radii/borders/elevation/layering;
- semantic color tokens;
- canonical SVG/brand/medallions;
- ровно четыре semantic icon-size roles;
- duplicate style-owner closure и доказанные compatibility boundaries.

Все видимые UI colors происходят из token/semantic alias. Exact duplicates
объединяются; near-duplicates с одним semantic role объединяются. Сохранённая
близкая пара требует explicit semantic/contrast reason. Raw literals остаются
только в token registry, media data и documented technical exception.

Concrete icon width/height хранится только в central tokens/utilities. Local
component dimensions запрещены.

## 8. MediaFrame/framing

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

## 9. AdaptiveEventCardGrid

Все применимые multi-card surfaces переходят на один family root либо получают
доказанную intentional composition reason.

Contract:

- available width `100%`;
- named density/responsive strategy;
- no phantom column;
- named final remainder;
- cards fill row;
- equal media/total heights внутри строки;
- framing остаётся у MediaFrame;
- no compact/mobile overflow;
- source/focus order сохраняется.

Responsive override обязан иметь specificity не ниже базового selector с
`data-adaptive-grid-row-size`; иначе Astro-scoped CSS оставляет две колонки на
viewport `<=620px`, хотя strategy объявляет mobile stack, и дочерняя карточка
растягивает документ по горизонтали.

`relatedCardLayout.mjs` и `OptimizedEventCardGrid.astro` — donors. Новый
`AdaptiveEventCardGrid` является эволюцией существующего решения, не второй
сеткой.

## 10. Actual consumer migration

A0 владеет shell, listings и routes. Он:

- сохраняет различие DateListingSurface/WeekendListingSurface;
- мигрирует actual pages на canonical roots;
- применяет F0 tokens/icon roles;
- применяет explicit M0 grid/media APIs;
- удаляет local forks и internal overrides;
- не копирует family markup в pages;
- не создаёт новый route model.

После каждого consumer checkpoint A0 повторно строит census и продолжает
следующий eligible consumer без отдельного owner prompt.

## 11. Browser/DOM audit

V0 — read-only ChatGPT window с `my-browser-bridge` и GitHub.

Routes:

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

Viewport classes: desktop wide, desktop compact, mobile 390–430 и required
breakpoint seams.

V0 проверяет component markers/anatomy, computed typography/spacing/colors/
radii/borders/icon sizes, image bounds/object-fit/overflow/clip, adaptive row
occupancy/equal heights, responsive transitions и horizontal overflow.

```text
PASS
DRIFT        → F0/M0/A0 immediately
PRODUCT_GAP  → backlog after normalization gate
BLOCKER      → only after all independent work and fallbacks are exhausted
```

После complete harness отсутствие preview допускает standby. После exact
reachable URL один run покрывает весь browser matrix и DRIFT routing.

## 12. Internal Golden A=S=P

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

## 13. Parallel ownership

- `N0`: generation decision, conditional execution authority, integration,
  status, preview, release;
- `F0`: foundations, colors, type, spacing, four icon roles, SVG/brand,
  duplicate style ownership;
- `M0`: component roots, MediaFrame, EventCard/ListingEventCard,
  AdaptiveEventCardGrid;
- `A0`: shell, listings, routes, consumer migration;
- `V0`: my-browser-bridge audit; later Golden Penpot audit;
- `K0`: consultant/process repair;
- `R0`: persistent bounded Codex execution, local focused diagnostics,
  invocation/observation of the shared Kaggle pipeline and sole Penpot writer;
- `my-data-hub`: sole MCP facade for published review-build operations.

Нет mandatory `MAT → QA → INTEGRATE → PUBLISH`, нового orchestrator или
per-Wave owner scheduler.

## 14. Autonomous recovery

Не являются terminal blocker:

- missing field/heading;
- combined `branch@sha`;
- stale checkpoint той же программы;
- missing formal handoff/packet;
- recoverable ENOSPC/tooling/aged fixture;
- dependent surface отсутствует, но другая owned работа существует.

Агент выводит данные из issue/refs/repository/ownership, проверяет reversible
scope, выбирает safest assumption и продолжает.

`[BLOCKER]` допустим только при исчерпанной independent work и реальном product,
external, writer-conflict или irreversible-risk boundary.

## 15. ASTRO_NORMALIZATION_PASS

Product UI-gap/change work открывается только после:

- reproducible fresh-data full Kaggle generation;
- tokenized foundations/colors;
- four icon roles across all consumers;
- single roots for same components;
- MediaFrame/framing PASS;
- AdaptiveEventCardGrid across applicable consumers;
- actual routes migrated;
- no critical V0 browser DRIFT.

После gate можно менять palette и интерфейс. Release затронутой family требует
обновлённый thin S и Penpot binding.

## 16. Meaningful checkpoints

Meaningful checkpoint:

- technical fresh-data generation verdict from the shared Kaggle path;
- normalized source convergence reviewed by role owner;
- reachable normalized real-data preview;
- V0 browser PASS/DRIFT;
- native Penpot master + linked route board;
- checked release candidate.

Локальная focused diagnostic может подтвердить/найти дефект, но не закрывает
preview/generation/owner/A=S=P checkpoint.

Checkpoint publication не завершает роль автоматически. Packet, dispatch,
worktree, commit без role review, test без output, 404 route и empty Penpot page
не являются product result.
