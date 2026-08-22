# Static-site design-system integration

## Runtime status source

Не фиксируй здесь изменяемые counts или глобальный этап. Актуальные lifecycle
state, native candidate resources, review status, promotion flags и exact SHA
берутся из versioned contracts/receipts затронутого family в
`onedayonemasterpiece/lovekgd-design-system` и подтверждаются Penpot read-back.
На момент этого контракта ни одно family нельзя считать promoted без отдельного
promotion receipt; наличие native Penpot candidate этого не меняет.

Penpot generations 003–005 остаются technical experiments and historical
evidence и не являются текущим Resource Graph или UI authority.

## Exact three-way visual conformance

Material component/foundation/archetype changes route through the canonical
`ui-three-way-conformance` skill and contracts in
`onedayonemasterpiece/lovekgd-design-system`. The consumer-side adapter only
extracts the exact public `PreviewEvent`, builds a disposable Astro specimen,
freezes the corpus clock and verifies local media/font bytes. It must not keep a
second editable corpus, change production Astro, or compare a different event
with a visually similar Penpot instance.

For an owner-authorized interactive run, the upstream conformance workflow also
owns a fail-closed live progress gate: each completed or blocked exact case is
published individually to the verified review topic and read back before the
next case begins. Consumer tooling must not buffer several ready comparisons
for a final batch or report completion without the corresponding Telegram
receipt. If Penpot export is unavailable, the post is explicitly diagnostic
and cannot be counted as visual parity.

Comparison composition is aspect-ratio aware. Astro and Penpot remain at equal
scale; choose the side-by-side or stacked canvas whose final aspect ratio is
closer to square. Use Astro-left/Penpot-right for narrow surfaces and
Astro-top/Penpot-bottom for wide horizontal rows and rails.

For archetype reconstruction, capture the exact Astro route/fixture first, put
that locked evidence next to a reconstruction made only from linked Penpot
components, then inspect same-size side-by-side, 50% overlay and diff exports.
Unexplained drift returns to the owning shared component/SoT boundary before
page work continues.

## Authority during reconstruction

До promotion каждого resource family текущий Astro source в этом репозитории является executable source of fact о существующей реализации. Его нельзя автоматически считать нормализованной дизайн-системой: decoder должен обнаружить component families, inline/CSS patterns, one-to-many/many-to-one relationships, states, consumers, local overrides и unresolved fragmentation.

## Mandatory UI round trip

Полный нормативный процесс принадлежит
`onedayonemasterpiece/lovekgd-design-system`, документу
`docs/ui-source-of-truth-roundtrip.md`. Этот репозиторий хранит только
consumer/release boundary и не должен дублировать или переопределять lifecycle.

```text
initial reconstruction:
exact Astro/runtime → Git SoT UI → native Penpot → owner review

comment correction:
Penpot comments → Git SoT first → Penpot reconciliation → owner re-review

accepted implementation:
owner Penpot acceptance → exact Git contract version/hash
→ isolated Astro candidate → three-way conformance
→ immutable noindex phone/desktop preview → owner browser approval

production:
owner browser approval → promotion/migration/release gates
→ production generation/deploy → post-deploy conformance
```

### Archetype capture and comparison obligation

Этот репозиторий владеет воспроизводимым Astro evidence для обязательного
archetype visual-parity gate, а не отдельным вариантом самого процесса. Для
каждого route/state/viewport нужно:

1. зафиксировать exact commit, route, fixture/data, viewport, DPR, browser,
   загруженные fonts, locale/timezone, theme, auth/personalization/consent и
   interaction state;
2. снять детерминированный current-Astro screenshot и сохранить manifest/hash;
3. передать его в Resource Graph как locked source evidence, не как component
   или insertable resource;
4. потребовать рядом Penpot reconstruction только из linked design-system
   instances и тот же pixel size;
5. проверить экспорт/import глазами side-by-side и в 50% overlay/blink, а при
   наличии — pixel diff; каждое необъяснённое отличие привязать к component/slot;
6. после Penpot acceptance снять isolated Astro candidate на тех же fixtures и
   сравнить его уже с accepted reconstruction до device review и production.

Если компоненты не воспроизводят pinned Astro evidence, сборка archetype
останавливается: сначала исправляются Git SoT/component contracts и все их
consumers. Screenshot не является SoT, а локальная подгонка archetype не может
скрывать системную ошибку. Нормативная подробная процедура находится в
`lovekgd-design-system/docs/ui-source-of-truth-roundtrip.md`, section
`Mandatory archetype visual-parity gate`.

Критические границы:

- Penpot comment не является прямым заданием на изменение Astro;
- Penpot mutation не может быть единственным местом изменения — сначала
  обновляется Git SoT;
- до owner Penpot acceptance фактический backport в consumer Astro запрещён;
- preview и production — два разных owner gates;
- Penpot acceptance, resolved thread или зелёный preview не дают разрешения на
  production;
- incident mitigation остаётся reversible runtime hotfix и требует последующей
  сверки с Git SoT/Penpot, а не становится новым baseline автоматически.

## Target authority after promotion

После promotion семейства versioned component package в `onedayonemasterpiece/lovekgd-design-system` становится canonical для:

- `component_id`;
- contract version/hash;
- variants and states;
- public props/slots;
- canonical Astro presentation implementation;
- fixtures/specimens/tests;
- Penpot binding and accepted references.

`events-bot-new` импортирует pinned package version и сохраняет продуктовую/domain-логику, которая преобразует event/user/runtime data в разрешённый component state.

## Product/design plane routing

```text
Product Atlas
→ product meaning, Jobs, outcomes, journeys, capabilities and UI-gap identity

UI Exploration
→ visual alternatives, component/pattern candidates, compositions and shortlist

Resource Graph
→ mature components, patterns, archetypes, product representations,
  evidence, accepted exports and promotion

This repository
→ current source during reconstruction, product/domain logic,
  integration, release and runtime evidence
```

## Per-family entry stage

Для ещё не реконструированного family входом остаётся source-first decoder:

```text
Astro source/generators
→ isolated generated specimens
→ real-page verification
→ candidate Component Contracts
→ compact Git snapshot + heavy Actions artifact
→ STOP before Penpot materialization/refactor
```

Decoder must not match current Astro against removed/test Penpot components and
must not change production UI. Для уже начатого family продолжай только с
текущего доказанного lifecycle state из его contract/receipt; не сбрасывай его к
decoder и не перепрыгивай следующий owner gate.

## Canonical documentation

The normative design-system architecture lives in `onedayonemasterpiece/lovekgd-design-system`:

- `docs/ui-source-of-truth-roundtrip.md`;
- `docs/resource-graph-004.md`;
- `docs/component-contract-authority.md`;
- `docs/source-first-component-decoder.md`;
- `docs/penpot-product-design-operating-model.md`;
- `contracts/resource-graph-scaffold.v1.json`;
- `receipts/penpot/resource-graph-to-be-structure-v1.json`.

The integrated Product Atlas/UI Exploration vision in this repository must reference these documents rather than duplicate the Resource Graph page schema or component authority rules.

## Golden Corpus L0 surface-placement gate

The read-only L0 adapter is `scripts/ui_conformance/verify-surface-placement.mjs`.
It copies the exact candidate `site/src` into a disposable Astro harness, replaces
only the copied preview catalog with immutable Golden Corpus v1 events, freezes the
public clock, builds the real routes, and reads component/state markers from the
generated HTML. It never changes production Astro and never supplies ranking.

Run the portable unit gate:

```bash
node --test tests/ui-surface-placement.test.mjs
```

Run the real cross-repository gate by providing the immutable corpus and the
candidate lockfile-compatible installed dependencies:

```bash
UI_REFERENCE_CORPUS_ROOT=/path/to/catalog/fixtures/ui-reference-events/v1 \
UI_REFERENCE_NODE_MODULES=/path/to/events-bot-new/site/node_modules \
node --test tests/ui-surface-placement.test.mjs
```

Implemented v1 surfaces are detail, date, today, tomorrow, weekend and the pure
production Favorites resolver. Home, Popular, Unusual, Search, personal feed and
Related remain explicit corpus-declared gaps until their immutable full-catalog,
manifest, query/persona or anchor inputs exist; the adapter reports rather than
fabricates them.

## Golden EventCard parent-archetype specimen

Resolved v2 render cases may additionally bind an exact, controlled parent
context for `event.card`. `scripts/ui_conformance/materialize-case.mjs` then
mounts the production `OptimizedEventCardGrid.astro` in the existing disposable
specimen harness. It does not reproduce the grid in harness CSS and does not
modify `site/src`.

The parent contract is fail-closed: it must name `OptimizedEventCardGrid`, use
the `controlled-layout-only` placement claim, bind an exact permutation of the
same Golden fixture IDs before and after `packRelatedCardRows`, and carry the
viewport, container, grid and selected-card placement facts. The adapter never
supplies route placement or ranking. For the desktop-1280 reference context the
contracted container is 1180px with three 380px columns and 20px gaps.

`scripts/ui_conformance/capture-case.mjs` preserves the existing selected-card
capture as `astro.png` and also writes `astro-archetype.png`. Browser evidence in
`astro-facts.json` records the parent rect, computed CSS grid columns/gaps,
rendered event order, every card rect, media state, and the production
`data-lab-row-*` placement/row-ratio markers. Capture fails when the selected
card is not 380px wide, its placement or row contract drifts, or first-row peers
do not share height and row ratio. Both screenshot hashes and the validation
result flow into the actual tuple.

Portable adapter tests:

```bash
node --test tests/ui-conformance-archetype-specimen.test.mjs
```

The real build/capture uses the same `materialize-case.mjs` and
`capture-case.mjs` commands as a single-card case, with a v2 resolved render
case as `--resolved`. It additionally requires every declared sibling fixture
and asset to be present in the immutable corpus. Generated harnesses and PNGs
remain disposable evidence and are not committed to this repository.

## Current-v2 runtime identity and chip inventory

The current-v2 runner keeps three immutable repository identities separate:
the design candidate SHA, the historical Astro source SHA that owns the visual
AS-IS, and the events-bot tooling SHA that executes the comparison. A tooling
checkout must never be treated as proof that a different Astro source checkout
was rendered.

`scripts/ui_conformance/run-current-v2.mjs` is the stable consumer entrypoint.
Its `chip-inventory` command copies the exact pinned Astro `site/src` into a
disposable harness, mounts the real `EventCard.astro` once for every one of the
eight Golden Event Corpus v1 fixtures, runs an Astro static build, and reads the
rendered chip/action states from generated HTML. The deterministic report binds
the corpus hash, Astro source SHA, tooling SHA, source-file hashes, every exact
event-type/admission/occurrence/action label, counter value, calendar state and
the reached branch families. It is not a documentation-derived enum and it does
not mutate `site/src`.

The same entrypoint exposes fail-closed `run-case` and exact seven-case
`run-batch` commands. They assert all three 40-hex SHAs, materialize from the
separate Astro checkout, capture browser evidence, build the identity tuple,
run structural/raster comparison through the pinned design-system CLI, validate
the supplied agent-review during finalization and retain per-case elapsed time.
No command publishes Telegram, writes Penpot, deploys, promotes a component or
changes the production UI.
