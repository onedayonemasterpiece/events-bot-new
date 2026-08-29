# Static-site ↔ LoveKGD Design System

Status: `CURRENT_OPERATIONAL_BRIDGE / DRAFT_CONTOUR`

Последняя фактическая сверка: `2026-08-29`.

Это первая локальная точка входа для изменений интерфейса статического сайта,
Astro ↔ Git UI SoT ↔ Penpot parity, reference fixtures и component lineage.
Полный нормативный lifecycle и текущий owner-review status находятся в
`onedayonemasterpiece/lovekgd-design-system`; этот репозиторий хранит короткий
fail-closed bridge, executable Astro consumer и runtime evidence.

## 1. Не путать целевую архитектуру с текущим состоянием

Старое утверждение `native design-system components: 0 / source-first decoder:
not started` больше не описывает активную работу. Оно относилось к историческому
`lovekgd-design-system/main` snapshot.

Текущие слои:

| Слой | Точный источник | Что он означает |
|---|---|---|
| Опубликованный snapshot дизайн-системы | `lovekgd-design-system/main@c6419a62af3d73f53e81d95a518fbe62a4a1c942` | историческое состояние на 19 августа 2026; не текущий owner-review delta |
| Source-proven AS-IS baseline | Draft PR `lovekgd-design-system#52@b86bab3e91511b3d4bd7d953b22bceb847f02a51` | 17 архетипов / 34 desktop+mobile cases; round-trip evidence; без acceptance/promotion |
| Активный owner-review delta | Draft PR `lovekgd-design-system#53`, branch `fix/penpot-owner-comments-20260826` | текущие contracts, Penpot readbacks, fixtures и per-item review status; fresh-read head обязателен |
| Golden Event Corpus pilot | Draft PR `lovekgd-design-system#42@7a26772828a5d74a9683c08e7e6774ff15ac61a5` | 8 exact-event identity gates PASS; визуальный conformance pilot зафиксирован как FAIL |
| Текущий опубликованный Astro факт | `events-bot-new/main@8710e56fa3685f6c30a90cd062d532dce0348cce` | executable AS-IS до promotion семейства |
| Активный Astro/UI candidate | этот Draft PR `#596`, branch `fix/audio-audit-ui-20260828` | изолированные owner-audit corrections; не merge, не deploy, не production authority |

Перед любой работой агент обязан fresh-read heads PR `lovekgd-design-system#53`
и `events-bot-new#596`. SHA в этом документе или PR body — checkpoint, а не
вечный указатель.

## 2. Фактический Source of Truth

Долговечный UI SoT — versioned contracts, registries, fixture scenarios,
bindings и receipts в `lovekgd-design-system`.

До promotion семейства:

```text
pinned events-bot-new Astro/runtime
→ executable факт текущего AS-IS
→ candidate Git UI SoT
→ native Penpot projection
→ owner review
```

После bounded owner acceptance:

```text
accepted Git UI SoT/package
→ isolated Astro candidate
→ Penpot ↔ Astro ↔ generated-route conformance
→ browser/device approval
→ promotion + production consumer migration
```

Следовательно:

- Penpot — нативная визуальная реализация и поверхность review, но не
  самостоятельный долговечный источник решений;
- синхронизация не автоматическая: contract/decision → implementation or
  materialization → exact readback → tests → focused visual review;
- page-local Astro fork утверждённого компонента и Penpot-only fix запрещены;
- structural PASS, visual PASS, owner acceptance, promotion и deploy — разные
  состояния.

## 3. Как читать последнее owner voice

Последнее замечание правильно требует единой наследуемой системы, одинаковых
fixture identities и многоуровневого parity. Но несколько формулировок являются
целевым контрактом, а не описанием уже работающей автоматизации.

| Формулировка | Корректная операционная трактовка |
|---|---|
| «Penpot — центральная точка; изменение появляется на сайте» | Penpot может инициировать owner decision, но долговечное решение сначала фиксируется в Git UI SoT; затем отдельно материализуется в Penpot и интегрируется в Astro. |
| «Один Golden Corpus» | Для каждого bounded сравнения используется один и тот же **именованный versioned scenario/pool** с точными fixture IDs и hashes. Универсального списка только из событий для всех сущностей нет. |
| «Всё совпадает полностью» | Требование относится к одному route/state/viewport/scenario после фиксации clock, fonts, DPR и runtime state. Responsive/contextual variants могут различаться по явно записанному contract. |
| «Компоненты отдельно от архетипов» | Component masters и state catalogs живут на малых библиотечных страницах; архетипы обязаны содержать linked instances. Запрещены page-local masters, detached copies и screenshot substitutes. |
| «Визуально похожие разные реализации — ошибка» | Верно. Lineage доказывается source path/version, component/main IDs, bindings и census/readback, а не внешним сходством. |

## 4. Что уже реализовано в этом candidate

### Shared reference fixtures

Executable bridge:

- `site/src/data/design-system-reference-fixtures.json` — generated selection
  bridge; factual payloads не копируются и не редактируются здесь;
- `site/src/data/designSystemReferenceFixtures.ts` — runtime validation/resolution;
- `docs/features/static-site-pages/design-system/reference-fixture-scenarios.md`
  — правила запуска и parity;
- tests reject fixture drift and production/secret-candidate fixture mode.

Текущие пулы различаются по назначению:

- 8 events — component-conformance corpus;
- 5 events — archetype core scenario;
- 7 festivals — bounded festival reference rows;
- 3 clubs — complete factual club pool;
- 7 artifacts — complete Collection 1 pool.

Нельзя называть это одним универсальным payload corpus. Общим должен быть
точный именованный pool/scenario **внутри конкретного сравнения**.

### `ListingDiscoveryRail@6`

Этот Draft PR вводит shared `plane` / `floating-island` surface axis:

- Date and Popular сохраняют shared plane presentation;
- Weekend использует прозрачный content-sized Floating Island;
- production callers в candidate явно мигрированы на `version={6}`;
- v5 остаётся deprecated catalog comparison до sign-off;
- source-contract/regression tests и browser computed-style readback проходят.

Это bounded candidate. Универсальная Floating Island navigation для всех
архетипов ещё не принята и не промотирована.

### Остальные owner-voice corrections

Актуальные Event Detail portrait/parallax/keyboard/transport/related contracts,
packed rows/ecological crop, FestivalCard centralization и Penpot lineage
receipts принадлежат активному head `lovekgd-design-system#53`. Не дублировать
их здесь как параллельную нормативную документацию.

## 5. Маршрут для агента

1. Здесь определить локальную Astro boundary.
2. В `lovekgd-design-system#53` открыть:
   - `docs/static-site-design-system-current-state.md`;
   - `docs/ui-source-of-truth-roundtrip.md`;
   - `docs/reviews/index.md`;
   - affected family/archetype contract и самый новый receipt.
3. Выбрать именованный scenario из
   `catalog/fixtures/design-system-reference/` и проверить exact fixture-ID/hash
   parity с executable bridge в этом репозитории.
4. Менять source owner один раз; обновить catalog specimen, consumers, tests,
   docs и `CHANGELOG.md` в одном bounded change.
5. Не заявлять завершение без required structural readback, focused visual
   evidence и корректного owner-review status.

## 6. Запрещённые утверждения

Пока PR `#53` и `#596` остаются Draft, нельзя писать:

- «Penpot автоматически синхронизирован с Astro»;
- «дизайн-система полностью принята или промотирована»;
- «все визуально похожие карточки уже имеют одного технического предка»;
- «Golden Corpus визуально прошёл весь сайт»;
- «этот candidate уже находится в production»;
- «зелёный test или `validate()=[]` означает owner acceptance».

## Каноническая документация

Normative/current authority lives in `onedayonemasterpiece/lovekgd-design-system`:

- `docs/static-site-design-system-current-state.md` — current layered state and routing;
- `docs/ui-source-of-truth-roundtrip.md` — lifecycle and parity gate;
- `docs/reviews/index.md` — owner-review status;
- `docs/component-contract-authority.md` — component authority;
- `docs/normalization/design-system-family-lifecycle.md` — ordered promotion gates;
- `catalog/fixtures/design-system-reference/v1/registry.v1.json` — reference pools;
- latest source-bound family/archetype contracts and receipts on PR `#53`.
