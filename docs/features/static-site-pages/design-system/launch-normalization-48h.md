# Нормализация UI — исполняемый Astro-маршрут

Статус: `ACTIVE`  
Координация: `onedayonemasterpiece/events-bot-new#621`  
Sole executable branch: `agent/static-site-single-kaggle-contract`  
Programme/history anchor: `integration/ui-normalization-launch-20260902`  
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

## 0. Trunk-based delivery authority

Эта коррекция меняет только delivery semantics существующей программы. Она не
создаёт новую topology, generation, T0, process, contract family, packet schema,
builder или управляющий контур.

```yaml
sole_executable_Astro_trunk:
  repository: onedayonemasterpiece/events-bot-new
  branch: agent/static-site-single-kaggle-contract
  current_head: 3ca6a143e4286c165282c2d8ceef1759a41185b7
programme_history_anchor:
  branch: integration/ui-normalization-launch-20260902
  merge_target: false
  executable_acceptance_target: false
historical_r0_branches:
  classification: evidence_only
  new_product_integration: forbidden
max_merge_ready_batches_outside_trunk_per_role: 1
current_full_real_preview: https://kenigevents.ru/preview-real-3ca6a143e-normalized-20260903-v1/__preview/
current_manifest: https://kenigevents.ru/preview-real-3ca6a143e-normalized-20260903-v1/preview-build.json
personal_V0_verdict: PENDING
active_A0_correction:
  branch: work/ui-normalization-a0-mobile-listing-rail-resource-state-20260903
  current_head: 2dac9d16031d4f1505184fc9678f88c855c3988a
  merge_ready: false
  required_action: supersede_same_branch_with_test_repairs_and_EventLayout_runtime_MediaFrame_rebinding
T0: preserved_unchanged_by_this_correction
```

Все новые принятые Astro product batches попадают только в sole trunk. Role
branches остаются временными source/test review surfaces. N0/R0 самостоятельно
fresh-read-ят current refs и втягивают один совместимый current merge-ready
batch без owner relay; второй ожидающий merge-ready batch той же роли запрещён.

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

N0/F0/FR0/M0/A0 назначаются на полный owned product contour. Wave, branch,
commit и `[RESULT]` — checkpoints, но не команда завершить turn.

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
N0: current role refs → accept one current merge-ready batch per role
    → sole executable trunk → exact tested descendant
    → full Kaggle generation → reachable preview → V0 trigger
+
F0/FR0/M0/A0: saturation всех current owned consumers/invariants
+
V0: personal browser matrix and independent domain verdicts
+
R0: compatible integration, local focused diagnostics and
    invocation/observation of the shared Kaggle build
```

N0 не ждёт все роли одновременно. Foundations, MediaFrame, EventCard/Grid и
Shell/Routes принимаются независимо. Один `DRIFT` не аннулирует `PASS`
совместимого domain или vertical slice.

```yaml
candidate_max_lag_minutes_when_merge_ready_output_exists: 30
max_merge_ready_batches_outside_trunk_per_role: 1
full_preview_after_compatible_batches: 2_to_3
full_preview_max_active_minutes_since_previous: 60
every_exact_preview_requires_V0_trigger: true
```

Technical baseline нужен для before/after и не обязан быть owner checkpoint.
Family не завершена без fresh-real-data Kaggle build и personal V0 browser
verdict.

N0 не дробит critical path на обязательные owner wake-ups, если acceptance
criteria можно определить заранее. После superseding A0 result N0 сам выдаёт
R0 acceptance; владелец не переносит SHA, не формирует packet и не запускает
pull вручную.

```text
IF exact compatible batch focused diagnostics/tests PASS
  THEN integrate into agent/static-site-single-kaggle-contract
  AND publish exact tested descendant
  AND invoke the one shared Kaggle pipeline by cadence
  AND publish reachable preview through its checked artifact
  AND trigger V0
ELSE
  reject only that batch
  continue compatible domains
  publish exact owned correction
```

R0 после каждого result fresh-read-ит #621/current refs и продолжает следующую
ready safe mechanical task. Historical `r0/*` branches не являются integration
targets и не получают новую product integration.

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
- уровнями проверок и правом promotion.

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

FR0-owned shared MediaFrame contract владеет:

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

M0 consumes MediaFrame API for card/grid families. A0 migrates runtime consumers
and may not create a route-local framing owner.

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

`relatedCardLayout.mjs` и `OptimizedEventCardGrid.astro` — donors. Новый
`AdaptiveEventCardGrid` является эволюцией существующего решения, не второй
сеткой.

## 10. Actual consumer migration

A0 владеет shell, listings и routes. Он:

- сохраняет различие DateListingSurface/WeekendListingSurface;
- мигрирует actual pages на canonical roots;
- применяет F0 tokens/icon roles;
- применяет explicit M0 grid/card и FR0 framing APIs;
- удаляет local forks и internal overrides;
- не копирует family markup в pages;
- не создаёт новый route model.

Текущая correction выполняется только на существующей ветке
`work/ui-normalization-a0-mobile-listing-rail-resource-state-20260903`.
`2dac9d16031d4f1505184fc9678f88c855c3988a` не merge-ready; следующая допустимая
A0 поставка — superseding head той же ветки.

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

Для текущего exact Preview personal V0 публикует независимые verdict sections:

```text
Foundations
MediaFrame
EventCard/Grid
Shell/Routes
```

```text
PASS
DRIFT        → F0/FR0/M0/A0 immediately, только owning domain
PRODUCT_GAP  → backlog after normalization gate
BLOCKER      → only after all independent work and fallbacks are exhausted
```

Один domain `DRIFT` не отменяет совместимый `PASS` другого domain или vertical
slice. R0 smoke не является personal V0 verdict.

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

- `N0`: sole-trunk acceptance, generation decision, status, preview, release and
  V0 trigger/review;
- `F0`: foundations, colors, type, spacing, four icon roles, SVG/brand,
  duplicate style ownership;
- `FR0`: MediaFrame, EventMediaRail and framing semantics/diagnostics;
- `M0`: component roots, EventCard/ListingEventCard and
  AdaptiveEventCardGrid;
- `A0`: shell, listings, routes, consumer migration;
- `V0`: personal my-browser-bridge audit; later Golden Penpot audit;
- `K0`: consultant/process repair;
- `R0`: compatible integration into the sole trunk, local focused diagnostics,
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

## 16. Near-term gate

```text
personal V0 verdict on exact 3ca6a143 Preview
+
one superseding A0 correction batch on the existing branch
→ N0 independently issues R0 acceptance
→ pull into agent/static-site-single-kaggle-contract
→ exact tested trunk descendant
→ next full real Kaggle Preview
→ V0 recheck
```

## 17. Meaningful checkpoints

Meaningful checkpoint:

- canonical authority correction with remote read-back;
- technical fresh-data generation verdict from the shared Kaggle path;
- exact tested descendant of the sole trunk;
- reachable normalized real-data preview;
- personal V0 browser PASS/DRIFT;
- native Penpot master + linked route board;
- checked release candidate.

Локальная focused diagnostic может подтвердить/найти дефект, но не закрывает
preview/generation/owner/A=S=P checkpoint.

Checkpoint publication не завершает роль автоматически. Packet, dispatch,
worktree, commit без role review, test без output, 404 route и empty Penpot page
не являются product result.
