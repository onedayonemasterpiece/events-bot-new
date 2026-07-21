# Инвентаризация реализаций и веток

Срез выполнен 2026-07-21 после `git fetch origin --prune`.
Production code base: `origin/main@3d0af26cbe`.

## Release reality

Код в `main` не равен раскатанному stable root. На момент проверки 2026-07-21
корень `https://kenigevents.ru/` отвечал `200/noindex`, а `/segodnya/`,
`/vyhodnye/`, `/populyarnoe/`, `/vystavki/` — `404`. Актуальная event-detail UI
проверялась на immutable noindex review candidate; stable whole-tree promotion
оставалась `NO-GO`. Vector/backend projection при этом production-enabled.

Current review receipt в incident record указывает candidate
`https://kenigevents.ru/_review/2BxKLmLKkRXG7uuiNjbvUC1g_dy7Kw1gtaVdnfG5Lj4/`;
это owner-pending noindex evidence, не stable rollout.

Поэтому ниже используются статусы **main code**, **immutable candidate**,
**production backend** и **lab**; слово production без уточнения для UI
запрещено.

## Main code и production backend

| Область | Реализация | Статус/ограничение |
| --- | --- | --- |
| Same occurrence data | `Event.linked_event_ids`, `linked_events.py` | strict title + exact venue; recompute стремится к symmetry, legacy graph её не гарантирует |
| Потоки обновления | `smart_event_update.py`, `source_parsing/handlers.py`, `main.py` | пересчёт после create/merge/meaningful edit, Telegraph rebuild siblings |
| Telegraph | `main_part2.py` (`SourcePageEventSummary.other_dates`) | compact `Другие даты`, до шести siblings |
| Telegram/VK same-day | publish anchor в linked group | раннее время → меньший id; общая ссылка для siblings |
| Static event occurrence | `site/src/pages/sobytiya/[slug].astro` | mobile: тяжёлые full EventCard `Другие даты`; accepted desktop template выбора occurrence не показывает; terminal selector только в lab branch |
| Strict/static related | `site/src/lib/events.ts`, `site/src/data/preview-related.json`, `/data/discovery/<id>.json` | static HTML + hydrated manifest; lifecycle/sibling exclusions |
| Production retrieval | `site/scripts/export-production-preview-data.py`, vector sync/RPC/cache | atomic corpus receipt, pgvector/two-doc evidence; per-anchor online provider не нужен |
| Event detail desktop | `DesktopEventPage.astro` | `Смотрите дальше`, canonical EventCard, final gallery CTA, finite `Ещё события`; broad related-family exclusion неполный |
| Event detail mobile | `[slug].astro` + EventCard | vertical `Смотрите дальше`, 10 initial + порции 10; desktop personal/broad slot скрыт на `≤1023px` |
| Listing filter | `ListingPersonalFilter.astro` | local `Все / Для меня`, progressive enhancement |
| Listing tail | `PersonalFeedSlot.astro`, `/data/personal-feed.json` | same-origin compact catalog first, optional RPC fallback; до 18 cards порциями 6; cross-family dedupe пока не доказан |
| Quality/release | `check-preview.mjs`, browser release gate, keyboard tests | real generated pages, crop/focus/graph/cache gates |

### Production naming debt

В коде и docs одновременно живут `Смотрите дальше`, внутреннее `Похожие
события`, `Ещё события`, `Личная лента` и local draft `Вам могут быть
интересны`. Консолидированный target contract фиксирует смысловые названия и
не требует считать текущую подпись production уже мигрировавшей.

### Main gaps, которые contract делает обязательными

- `getPreloadedDiscoveryEvents()` ещё может смешать `pure_related` и
  `adjacent_discovery` под `Смотрите дальше`; target требует strict/broad split.
- Broad/personal continuation не получает надёжный `linked_event_ids`/
  `event_family_id`, поэтому exclusion всех других дат там не гарантирован.
- Cache key base-scoped, но acceptance требует точный `current_event_id`;
  event-detail запись может вытеснить listing cache. Target разделяет cache по
  `{surface,current_event_id,profile_hash}`.
- `get_listing_personal_feed_v1` описан как optional RPC, но соответствующей
  migration/function в репозитории нет; deployment нельзя заявлять.
- Durable Supabase analytics для browser feedback не закрыта: текущие strong
  actions остаются local/debug instrumentation.
- Exporter hard-excludes sold-out, а старые client/docs говорили downrank;
  target policy задан в `REL-010`.
- Telegraph renderer умеет `❌/⏸`, но current producer заранее отбрасывает
  non-active siblings; target `REL-043` делает disabled status reachable.

### Legacy graph quality evidence

Это **production-derived artifact**, не утверждение о текущем live DB:
`artifacts/codex/image-geometry-production-20260720/kaggle-failed-production-secret-20260720T115945-960456d0/events.sqlite`.

- 6 574 rows; 1 314 rows с links; 4 345 directed links;
- 1 967 unique symmetric pairs, но 15 dangling, 396 asymmetric directed и 49
  same-slot unique pairs;
- среди 207 future active unsilent rows на 2026-07-20 только 11 mutual future
  pairs;
- встречаются composite/generic false families (`Собачье сердце и экскурсия`,
  generic `Фестиваль Pianissimo`, festival title vs specific opera).

Текущий static normalization может скрыть one-way/dangling edge вместо ремонта
source graph. Поэтому `REL-006/007/017/018` — не архитектурная косметика, а
обязательный quality gate перед единым cross-surface rollout.

## Высокосигнальные ветки

`behind/ahead` ниже — `git rev-list --left-right --count origin/main...ref` на
момент среза. Diverged branch — источник требований/скриншотов, а не production.

| Ветка / tip | behind/ahead | Что в ней есть | Решение |
| --- | ---: | --- | --- |
| `origin/main@3d0af26cbe` | `0/0` | текущий production-bound code contract | единственный code source of truth; rollout проверяется отдельно |
| `origin/integration/static-related-quality-20260720@6dbaa3d0fd` | `28/0` | atomic vector corpus, graph repair/health, media/keyboard ownership | уже contained/merged; сохранять как regression evidence |
| `origin/integration/static-event-preprod-continuation-20260718@ce27d59361` | `142/0` | preproduction continuation regression contract | contained in main; historical gate baseline, не текущий candidate source |
| `origin/integration/keyboard-navigation-production-20260719@fe83088e4f` | `47/0` | focus stability через rerank/hydration | contained/merged |
| `origin/hotfix/static-crop-restore-20260720@8ce6686e3b` | `5/0` | compact related crop + visual regression fixtures | contained/merged |
| `origin/feature/static-related-occurrence-final-templates@a7f80b67f6` | `165/3` | `EventOccurrenceNav.astro`, mobile/desktop selector, tests | лучший occurrence UX specimen; портировать осознанно, не считать merged |
| `origin/feature/unsigned-personalization-mvp0-related@4dc829a3f5` | `1193/3` | ранний JS/demo/probe/E2E slice | superseded отдельными hardened commits в main; не cherry-pick wholesale |
| `origin/feature/personalization-product-e2e-design@17b5275a74` | `349/5` | personas, E2E, timeline/schema review pack | requirements source, не runtime base |
| `origin/feature/event-page-desktop-focus-v11-20260714@159f0b9054` | `482/46` | terminal desktop event-page visual lab chain | визуальный архив; production брать из main |
| `origin/feature/static-mobile-ui-variants-20260715@fd8766b136` | `326/8` | accepted mobile event card/related visual lab | визуальный архив; main содержит successor behavior |
| `origin/integration/listing-surfaces-v26-mobile-sticky-groups-20260719@c4f3c4ded4` | `91/24` | terminal listing/mobile chain, adaptive cards, sticky group context | лабораторный архив; не merged |
| `origin/hotfix/static-listing-desktop-preview-regression-20260720@d58119bab1` | `91/26` | V27 desktop listing recovery + incident gates | branch-only preview repair; не stable root |
| `origin/integration/popular-desktop-v28-20260720@40b309cbad` | `91/28` | latest desktop Popular: family fold, five evidence shelves, optional warm-only 4+1 tail | lab candidate; mobile остаётся на другой truth path |
| `origin/integration/exhibitions-personal-discovery-prototype-20260719@54cfa90303` | `91/19` | personalized exhibition deck/tail/keyboard | adjacent experiment; не канонический related block |
| `origin/agent/static-site-release-analytics-20260720@e686a990ce` | `91/2` | mobile rail analytics/bounded storage gates | переносить telemetry requirements, не mixed branch |
| `origin/docs/static-site-release-plan-20260717@4758542437` | `271/4` | mobile-feed analytics/release requirements | docs evidence only |

## Consolidated implementation после review

`feature/related-events-compact-unified-20260721` — целевой donor поверх
`origin/main@3d0af26c`, а не wholesale merge лабораторий. В нём:

- `eventOccurrences.ts` единолично решает наличие family из взаимных explicit
  `other_date_ids`, строит slots/issues, форматирует compact/rail DTO и задаёт
  `none` / `per-date` / `per-family` collapse;
- `EventOccurrenceLabel.astro` обслуживает большие и rail cards,
  `EventOccurrenceNav.astro` переносит принятую механику 03/04/05;
- title/type/venue/city inference удалён из frontend identity path;
- №10 реализуется общей collapse policy, но artifact renderer `build-v19.py` не
  импортируется; branch V28 также остаётся donor/research, потому что его
  heuristic family fold и отдельный mobile path противоречат explicit-only
  contract;
- synthetic lab `/lab/occurrences/` проверяет две точные November подписи и не
  использует incident-подозрительный event `5756` как data truth.

До merge эта ветка не является `origin/main` или production. Порядок переноса и
короткий prompt находятся в [handoff.md](handoff.md).

## Лабораторные цепочки

### Desktop event page

`event-page-ux-lab-v3` → desktop variants → media families → scroll
compositions v3/v4 → media polish v5/v6 → desktop focus v7…v11.

Эта цепочка исследовала sticky media/CTA, OCR-safe hero, gallery, desktop grid и
release boundary перед related. Terminal archive — `159f0b9054`; production
release затем был собран другими commits и не равен tip лаборатории.

### Listing/mobile

`listing-time-nav-media-v10` → listing date v12/v13 → listing surfaces v14…v26.

Исследованы date navigation, Popular, density, reuse canonical cards, pinch и
sticky group context. Terminal archive — `c4f3c4ded4`. Ни один v10–v26 tip не
contained в текущем main, поэтому подписи «production mobile» для них запрещены.

### Personalization

Ранний MVP-0 branch доказал static fallback + local rerank, но был superseded.
Текущий main содержит hardened successor: статический event-detail block,
same-origin manifest, consent/version gates, local actions и partial sidecar.
Backend listing personal feed остаётся prepared/partial, а не общей production
зависимостью.

## Старые/иные реализации, которые нельзя потерять

1. **Telegraph `Другие даты`.** Самая старая публичная linked-occurrence
   поверхность; должна получать тот же group/lifecycle результат.
2. **Same-day Telegram/VK anchor.** Публикационная дедупликация — presentation
   одного occurrence group, но не merge canonical rows.
3. **Static `preview-related.json` seeds.** Полезны как fixture/fallback, но не
   должны становиться ручной production truth.
4. **Sparse TF-IDF chain.** Честный lexical fallback и диагностическая база; не
   называется semantic.
5. **Fullscreen gallery CTA.** Использует первый strict candidate как конечный
   явный CTA `Смотреть похожее`.
6. **Listing `Все / Для меня`.** Это локальный filter static list, не новая SEO
   страница и не то же самое, что semantic relation.
7. **Personal/exhibition decks.** Дают feedback/keyboard/loading паттерны, но
   требуют отдельной product acceptance перед переносом.
8. **Gallery CTA stability.** Текущий CTA берёт первый SSR candidate и не
   меняется после local rerank; это безопасный crawler-consistent baseline,
   пока отдельный dynamic contract не принят.

### Дополнительная legacy archaeology

- До текущего recompute `source_parsing/parser.py::find_linked_events` добавлял
  fuzzy title/location backlinks инкрементально, без window/caps/lifecycle и
  без очистки stale edges; часть дефектов graph исторически объясняется этим.
- В publishing существует отдельный implicit same-day special case для серии
  `кормление` (source/media/location), даже без `linked_event_ids`. Это не
  каноническая relation и должно быть мигрировано или явно сохранено отдельным
  tested publication rule.
- `docs/reference/recurring-events.md` пока design-only: модели
  `RecurringEvent`/template→occurrence в коде нет. Recurrence template в будущем
  будет отдельной связью, не alias для `linked_event_ids`.
- `Event.festival` — membership конкретной программы/edition. Соседние события
  фестиваля могут влиять на discovery score, но не становятся occurrence
  siblings; identity gate уже различает `festival_sibling_not_same_event`.
- Frontend `getLinkedSessionIds()` дополняет explicit ids inference по
  normalized title/type/venue. Target запрещает считать эту эвристику source of
  truth после появления единого occurrence projection.

## Разрешённые конфликты

| Конфликт в существующих реализациях | Единое решение |
| --- | --- |
| `related` значит и «другие даты», и «похожие» | обязательный `relation_kind` и четыре разных типа |
| `Смотрите дальше` смешивает strict и broad | новые ветки используют `Похожие события` и `Ещё события` отдельно |
| `Вам могут быть интересны` без profile | не использовать как umbrella; `Для вас` только при реальном rerank |
| event detail допускает long-running active candidate, отдельный draft требует future-start only | strict event-detail — future-start; listings могут иметь отдельный «идут сейчас» policy |
| sparse/vector/LLM ветки по-разному описывают semantic | algorithm/provenance честны; strict label получает только quality-gated result |
| разные card renderers в labs | один canonical EventCard + interaction controller |
| feedback сразу переставляет cards | текущий viewport сохраняет orientation; rerank ниже anchor/на следующей загрузке |
| lab branch выглядит новее main | ancestry/status важнее даты скриншота; production = `origin/main` |
| V28 desktop и V26 mobile считают Popular по-разному | одна shared eligibility/family allocation/order projection; различается только presentation |
| V24–V26 intercept pinch и запрещают zoom, V17/V20/V21 сохраняют browser zoom | native zoom сохраняется; density меняет явный доступный switch, pinch experiment отклонён до отдельной a11y acceptance |
| Main listing cards имеют Calendar/Подробнее, V15+ labs требуют passive proof | canonical card API имеет именованный surface variant; mutable actions только на явно принятой task surface |
| Main generic personal slot стоит и на Popular/Exhibitions, V19/V28 заменяют его специфической композицией | один owner continuation на surface; generic slot отключается при наличии принятого specific tail |

## Правило переноса в другие ветки

1. Сначала cherry-pick документационный commit этой consolidation branch.
2. В implementation branch заполнить checklist из `requirements.md` только для
   реально применимых surfaces.
3. Не cherry-pick целиком terminal lab branch; переносить минимальные commits
   или port вручную поверх свежего `origin/main`.
4. Обновить subordinate canonical doc, `CHANGELOG.md`, tests и screenshot
   provenance в той же ветке.
5. Перед merge доказать, что ветка не вернула legacy naming/renderer/cache или
   не начала считать occurrence siblings semantic recommendations.
