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

Canonical style owner публикуется как `media-frame.css`; caller сохраняет
interaction ownership и внешнюю геометрию surface, но не переопределяет
`object-fit`, focal position, clip или radius semantics. EventCard,
ListingEventCard, EventMediaRail и MobileListingRailRow обязаны использовать
этот один protocol и публиковать его диагностические поля для browser audit.

После FR0 cutover карточки используют один resource-state vocabulary:
`EventCard` и `ListingEventCard` с URL начинают в `pending`, успешная загрузка
переходит в `loaded`, отсутствие URL — в `fallback`, а network/decode failure —
в `broken` с прямым fallback, `contain`, запрещённым crop и
`resource_load_error`. Failed `src/srcset` удаляются, поэтому broken resource
не может вернуть `cover`. Это не переносит framing paint в карточку:
`media-frame.css` остаётся единственным fit/focal/clip owner. Mobile listing
rail пока остаётся отдельным consumer-binding backlog и не считается закрытым
этим card batch.

Публичный `relatedCardLayout` resolver самостоятельно применяет тот же
максимальный 20% crop budget к OCR/document media: точная граница допустима,
запрос выше неё обязан fail-close в `contain`, даже если resolver вызван вне
row packer.

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

Диагностика разделяет три популяции и не смешивает их cardinality:

- `input-*` — полный вход до `limit`;
- `source-*` — admitted input после `limit`;
- `rendered-*` — фактически созданные direct EventCard roots.

Каждая пара `*-count` / `*-order` описывает одну и ту же популяцию. Единственный
writer этих полей объявляется через
`data-adaptive-grid-diagnostics-owner="AdaptiveEventCardGrid"`; page/consumer
не должен устанавливать второй observer для тех же полей.

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

Real Review Preview gates select browser specimens from the generated active
catalog. Historical event IDs may remain preferred canaries while present, but
must have a structural data-driven fallback with the same multi-image and
recommendation-journey contract; expiry of one historical event is not a build
failure.

An empty Date/Weekend listing is accepted only through an explicit canonical
empty state: its mobile rail root must exist and publish
`data-ds-state="empty"`. Absence of rows alone never converts a missing rail
into a passing empty fixture. Popular is intentionally different: it owns the
accepted Large/Compact representations and must not recreate a disconnected
third `MobileListingRailSurface` merely to satisfy a generic preview check.
Date/Weekend rail Playwright helpers therefore must not assert their
`.feed-head`/`.group-head` sticky anatomy on Popular. Popular browser coverage
uses its own Large/Compact roots, sticky sentinel and internal-scroll contract.
Live-route media assertions derive gallery cardinality from the exported event;
they must not invent an obsolete second image for a current single-image event.
Real data may omit any evidence shelf that has fewer than three honest
candidates; the remaining shelves preserve canonical order and 3–5 cards.

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

## 17. Accepted source-integration checkpoint (2026-09-03)

Текущая интеграция строится от точных принятых границ, а не от поздних tip
роль-веток:

```text
R0 base: 4536847f9fbdaa27326ebb3ec9ec1c825736e107
F0:      de92dabd4551e117ca1af1be7915ff223321cc32
M0:      c808c75dd975a9851e148ccf993c32787d2b6886
A0:      net projection through ec926580fa2cc003318006f4c1d671fc459ea26c
N0:      74252469c545193f8ec57624bd018fc00c87e9e6
```

После проекции A0 механически завершены принятые `A0-MECH-01..05`:

- InterestProfile больше не владеет размерами semantic icons;
- AuthorizedEventSearch/SearchResults публикуют один ranked-phase-feed family
  и точные lifecycle states;
- MobileListingRailSurface оставляет MediaFrame framing/clip/focal ownership и
  сохраняет только геометрию/gesture-контракт surface;
- DesktopEventPage использует `EventMediaRail` для hero и split rails,
  EventHero публикует MediaFrame diagnostics, а EventLayout больше не является
  вторым EventCard framing owner;
- festival/exhibitions routes публикуют route-family identity и используют
  canonical semantic icons; отрицательная action target в EventLayout поднята
  с 36px до минимальных 44px.

Event-detail family поэтому имеет статус `source_converged`; grouped source
fraction — `6/9`. Это не V0 PASS: gallery, dialog, route и viewport поведение
проверяется на immutable fresh-real-data preview.

Отдельный desktop Popular containment fix не меняет принятую композицию V28.
Полки остаются однострочными, карточки сохраняют исходные размеры/ratio, а
избыток ширины принадлежит самой полке через horizontal scroll, не документу.
Презентационный snapshot 30 июля мог полностью помещаться; свежий сентябрьский
catalog содержит более широкую комбинацию 16:9 карточек и recognition rails и
тем самым проявил старую data-dependent щель. Это исправление containment, а
не восстановление древнего интерфейса и не redesign.

Тот же fresh-real browser census выявил отдельную щель ровно над mobile
boundary: при `721..~899px` две Weekend day-summary колонки не позволяли
длинным русским day/count labels сжиматься и расширяли документ. Это латентный
seam июльской композиции (основные presentation viewports остаются без
изменений), а не возврат к более старой версии. Weekend сохраняет time axis,
обе day lanes, weekday chip, дату и count; только текстовые flex descendants
получают `min-width: 0` и локальное ellipsis вместо document overflow. Browser
geometry gate теперь явно включает `721` и `800px`.

После fresh-read N0 интегрирован точный текущий descendant `74252469...`, а
M0 расширен от ранее принятого `4c83fc77...` до принятого N0 exact head
`c808c75d...`. Несовпавшие тесты N0 приведены к compact manifest schema без
изменения gate graph; F0 checker отличает contextual `--ke-icon-size` dispatch
от token authority и проверяет aliases, заканчивающиеся canonical role suffix.

Публичный fresh-real browser census дополнительно применяет уточнённый N0
контракт для внешних ссылок: `target="_blank"` сам по себе допустим, но каждая
такая ссылка обязана содержать одновременно `noopener` и `noreferrer`.
Festival timeline ранее генерировал только `noreferrer`; route-local binding
исправлен без изменения URL, композиции или поведения карточек. Source-test и
публичный browser probe проверяют оба токена.

## 18. Post-preview F0/M0/A0 successor cycle (2026-09-03)

После публикации immutable fresh-real preview для `1bc6d9cb...` backlog был
пересчитан по актуальным role refs и `launch-normalized-ui.v1@1.10.0`. Этот
preview остаётся валидным pre-F0 baseline, но не получает credit за более
поздний source.

Следующий reversible integration transaction включает:

```text
source base:       1bc6d9cb4c122046f4782532381de953727c1da6
F0 source:         0fb2938344cf96b05be0df09dfb9e69525b3717d
M0 source:         c71351decdcee02941acb26c5e2fbaf88faf0378
M0 downstream:     5eeaba09b5ec432a77ff899ce98fb8b9f492c133
contract:          launch-normalized-ui.v1@1.10.0
```

Consumer migration сохраняет актуальный интерфейс и поведение:

- `ExhibitionsPersonalSurface` удаляет только private `--ex-*` authority и
  ссылается на эквивалентные F0 tokens;
- festival route сохраняет timeline и link-safety, но получает canonical
  geometry tokens, semantic heart и минимум 44px для favorite target;
- interest-club detail/card заменяют только перечисленные raw values и text
  arrow на canonical tokens/SemanticIcon;
- `FocusEggCollectionRouteComposition@1/collection-prototype` синхронизирует
  `found-N-of-M`, а `ClosedFocusHubRouteComposition@1/participant-hub` —
  `checking/locked/available` внутри уже существующих runtime paths;
- consumer-local sizing `.ke-icon-role` удалён: размер задаёт один из четырёх
  canonical roles.

Строгие F0 gates проходят, focused F0/M0/A0 packet проходит `111/111`.
Grouped family source census становится `9/9`, PM0 route identity source
census — `19/19`. Это только source-level convergence: V0 browser credit и
Penpot materialization не заявляются. Новый exact SHA замораживается только
после N0 integration и полного test/build checkpoint; затем тот же canonical
Kaggle runner публикует новый full `real/all` immutable preview.

## 19. Public successor smoke and focus badge registry closure (2026-09-03)

Для source `7f4d04b7...` canonical Kaggle rail опубликовал immutable full
`real/all` preview
`/preview-real-7f4d04b7-normalized-20260903-v1/__preview/`. Публичные
изолированные матрицы прошли `176/176` и `77/77`: HTTP, document overflow,
console/request failures, MediaFrame fit/escape, nested interactive и targets
ниже 44px не обнаружены на их 11 route-наборах.

Дополнительный route-identity probe включил standalone focus routes, которых
не было в этих 11 routes, и воспроизвёл overflow коллекции на 375px. Причина
не относится к июльской композиции: недавняя нормализация `FocusLabBadge`
заменила локальные размеры на roles из `surface-foundations.css`, но consumer
импортировал только `product-contour-foundations.css`. Неразрешённые размеры
оставляли intrinsic 800px SVG владельцем ширины.

Механическое исправление сохраняет badge anatomy и palette: consumer теперь
загружает также canonical `component-foundations.css`, который является
публичным entry point для `surface-foundations.css`. Source regression требует
оба registry imports; размер success icon в invite intake по-прежнему
принадлежит canonical `feature` role, а не локальному selector.

После следующего fresh-read интегрирована точная F0 source boundary
`4709dc231...`, а A0 closure из tip `dc7b722ec...` применён только к шести
заявленным consumer paths, без merge divergent A0 branch и без записи в
F0/FR0/M0 roots. Идемпотентный check сообщает `changed:false` для всех шести
путей. Строгий route-theme gate подтверждает semantic separation festival
guide/taxonomy, ровно четыре exhibitions runtime variables, 20 обязательных
central bindings и canonical gallery arrows; club-theme gate также проходит.

FR0 batches through `85d443046...` интегрированы отдельным ancestry merge.
`EventMediaRail` теперь fail-closed переводит network-broken resource в
`fallback/contain/crop-forbidden`, а настоящий fallback child принадлежит
canonical MediaFrame. Clipping изображения и fallback также имеет одного
владельца — внутренний MediaFrame; внешний rail button сохраняет только
interaction, sizing и radius, без дублирующего `overflow:hidden`.

Текущие F0/FR0 heads `a4db631db...` и `a317188bd...` затем интегрированы с
явной ancestry. Существующий шестипутевой closure runner расширен — второй
exporter или route transform не создан — и теперь материализует все 46 A0 и
20 FR0 bindings из residual exhibitions inventory. Подстановки сохраняют
эффективные значения июльской композиции: timeline, row halo/edge, deck stack,
skeleton/depth, medallion, actions, gallery и live receipt используют
центральные tokens; четыре runtime-layout `--ex-*` остаются локальными.

Strict residual gate теперь проверяет implementation library за thin
entrypoint, игнорирует только wildcard-префиксы из описаний binding map и
подтверждает: `143` token declarations с одним owner, `32` bindings, ноль raw
visible colors, canonical gallery arrows, A0 `46/46`, FR0 `20/20`. Closure
идемпотентен (`changed:false` для всех шести consumers); browser credit всё ещё
требует свежей сборки и независимого V0 verdict.

Текущий V0 executable overlay `2e71e5521...` также интегрирован как
non-gating classifier поверх канонического browser release gate. В исходном
tip была опечатка в имени `AUTHORED_AGAINST_SOURCE`, из-за которой модуль не
инициализировался; локальная коррекция покрыта собственным V0 test suite и не
меняет matrix, owner routing или критерии вердикта.

На disk-constrained worker canonical `check-browser-release-gate.mjs` и его
browser behavior tests можно запустить с `--executable-path <chromium>` либо
`PLAYWRIGHT_EXECUTABLE_PATH=<chromium>`: это меняет только способ выбора уже
установленного браузера, не сам gate и не его browser evidence.

FR0 exhibitions MediaFrame bridge `00e95e5ba...` + geometry correction
`321f2482e...` интегрированы через точный ancestry merge `2231e1d66...`.
Отдельно canonical browser gate обнаружил integration seam между июньским
client rerank и новым packed-row optimizer: hydration возвращал DOM в ranking
order, но оставлял row ratio от optimized order. Cold hydration теперь повторно
вызывает тот же `KenigEventsPackRelatedCardRows` и применяет его MediaFrame
решения перед DOM move; reaction path с зафиксированным viewport-prefix из
`#780` не изменён. Это механическое восстановление уже заявленного row
контракта, не редизайн июльской композиции.

Browser release gate перед проверкой `loaded|missing` последовательно пересекает
каждую карточку recommendations/continuation. Это сохраняет production
`loading="lazy"`, но исключает ложный timeout второй строки, которая до
скролла законно находилась вне preload distance Chromium.

Повторная footer-проверка `P` -> `S` ждёт завершения именно текущей service-share
транзакции (`aria-busy=false` и снятый pending marker), а не совпадающий текст
toast от первого `P`. Поэтому медленная подготовка изображения больше не может
создать ложный отказ следующего shortcut; продуктовая обработка клавиш не
изменялась.

## 20. Current full-real owner Preview and mobile typography closure (2026-09-04)

Exact trunk `35d1d73286c61ed8f11759bea985e65b23183d18` опубликован canonical
Review Preview rail как immutable full `real/all` prefix
`/preview-real-35d1d7328-normalized-20260904-v1/`. Owner hub содержит 16
материализованных archetype families; публичный desktop/mobile smoke прошёл
`32/32` без document overflow, page errors и same-prefix resource failures.

Последующий визуальный spot-check на 390px выявил одну route-local typography
щель: mobile festival hero наследовал `overflow-wrap:anywhere` и одновременно
сужал H1 до `12ch`, поэтому слово «Калининградской» делилось внутри слова.
Механическая коррекция не меняет текст, palette или family anatomy: mobile H1
занимает доступную ширину, запрещает intra-word break и использует уже
существующий общий `0.72em` scale для цветной строки. Browser geometry на
`320/360/390/430px` обязана подтверждать одну строку для слова, отсутствие
clipping, раздельные fact labels и нулевой document overflow.

## 21. Navigation and service-share icon role saturation (2026-09-04)

`MobileBottomNav` and `ServiceShareAction` now delegate visible glyph identity
to canonical icon sources and size ownership to the existing four-role F0
contract. No route, action, fallback or palette behavior changes.

`MobileBottomNav` uses `SemanticIcon` with the `control` role for ticket,
calendar and personal-highlight glyphs. Search keeps the classified
`reference4-v8/search-thin.svg` asset under the same central role. The
component no longer contains four inline SVG copies or a local `21px` owner;
its labels, links, active state, 38×28 icon slot and mobile navigation targets
remain unchanged.

`ServiceShareAction` uses `SemanticIcon` with the `control` role for share,
image, link and success glyphs. Five local SVG copies, the local link-mask
asset owner and the `1.15rem` size owner are removed. Mobile/image/text intents,
accessible names, live status, visible and `noscript` fallbacks, `P`/`S`
shortcuts, 44px targets, responsive state switching, controller hydration and
the accepted palette remain unchanged.

Both native `SemanticIcon` paths expose the same runtime evidence. When a
semantic icon delegates its glyph to the lower-level `Icon` renderer, the SVG
still carries `data-ke-icon-name`, `data-ke-icon-role`, the `four-role-v1`
contract and `foundations.css` size owner; source normalization without those
generated-DOM identities is incomplete.

## 22. Production transport-token consumer normalization (2026-09-04)

The production event-detail transport consumers now bind only to their existing
modality roles in
`site/src/components/design-system/transport-foundations.css`: warm rail,
cool regional bus, and the distinct Kaup/route and official-transfer families.
This is a presentation-only convergence: schedule data, experiment selection,
route semantics, controls, responsive containers, radii and geometry are
unchanged. The compact Kaup treatment keeps its separate transfer surface;
rail and regional-bus surfaces remain intentionally different modality scans.

The executable source regression is
`site/tests/transport-token-consumers.test.mjs`. It confirms every referenced
transport role is defined by that foundation owner and rejects new raw colour
literals in production-reachable schedule consumers.

The test permits only these documented non-equivalent retained literals; no
existing transport-foundation role has the same value and meaning:

- rail's `#78342f` / `rgba(151,53,46,0.08)` empty-return advisory pair;
- Kaup header brand icon `#a54821` and its independent translucent summary
  underline `rgba(255,255,255,.38)`;
- the three intentionally distinct compact timetable label inks `#6a7471`,
  `#67716e` and `#65706d`.

They are exceptions, not new token owners. A future exact semantic role must
replace the literal and tighten this allowlist rather than introducing a local
transport token.
