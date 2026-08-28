# Static-site design-system integration

## Current status

```text
Resource Graph TO-BE structure: PASS
Penpot file_id: 3be9e5e1-190f-8090-8008-713c0fbe6260
native design-system components: 0
source-first decoder: not started
promoted component families: 0
```

Принятой компонентной дизайн-системы ранее не существовало. Предыдущие Penpot generations 003–005 являются technical experiments and historical evidence. Активный Resource Graph очищен и содержит только пустую TO-BE структуру.

## Authority during reconstruction

До promotion каждого resource family текущий Astro source в этом репозитории является executable source of fact о существующей реализации. Его нельзя автоматически считать нормализованной дизайн-системой: decoder должен обнаружить component families, inline/CSS patterns, one-to-many/many-to-one relationships, states, consumers, local overrides и unresolved fragmentation.

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

## Next bounded stage

Source-first decoder:

```text
Astro source/generators
→ isolated generated specimens
→ real-page verification
→ candidate Component Contracts
→ compact Git snapshot + heavy Actions artifact
→ STOP before Penpot materialization/refactor
```

Decoder must not match current Astro against removed/test Penpot components and must not change production UI.

## Owner-audit correction: discovery rail v6

The 2026-08-28 owner voice audit requires `61.10 Weekend Discovery rail` to be
a content-sized, translucent Floating Island rather than a full-width visible
shelf. `ListingDiscoveryRail@6` is the shared implementation of that contract:

- `surface="plane"` preserves the shared Date and Popular presentation;
- `surface="floating-island"` keeps the sticky outer plane transparent and
  gives the shared inner control group a bounded, rounded, translucent surface;
- Weekend is the first production Floating Island consumer;
- every production caller is explicitly migrated to `version={6}`;
- v5 remains only as a deprecated catalog comparison until owner sign-off.

The same version and surface axes must be represented by the native Penpot
component. A page-local rounded copy is not compatible evidence.

## Owner-audit correction: HeroTalk v2 Photo Mosaic

`HomeHeroTalk.astro` now owns the shared `HeroTalk@2` renderer with the explicit
`mode=photo-mosaic` contract. The Home route passes up to three exact catalog
events; the renderer derives only factual title/date/venue/admission text and
exact event media from those fixtures. A `5×4` tile projection, semantic text
fragments, atomic copy/CTA/media transitions, pause-on-interaction and a full
static/reduced-motion first scene form the candidate contract.

The rejected static event-feature implementation is preserved as
`HomeHeroTalkLegacy.astro`, registered as deprecated `HeroTalk@1` with
`data-ds-replaced-by="HeroTalk@2"`, and is forbidden in production consumers.
Penpot must materialize v2 as the native master and may retain v1 only as an
explicit deprecated comparison.

## Canonical documentation

The normative design-system architecture lives in `onedayonemasterpiece/lovekgd-design-system`:

- `docs/resource-graph-004.md`;
- `docs/component-contract-authority.md`;
- `docs/source-first-component-decoder.md`;
- `docs/penpot-product-design-operating-model.md`;
- `contracts/resource-graph-scaffold.v1.json`;
- `receipts/penpot/resource-graph-to-be-structure-v1.json`.

The integrated Product Atlas/UI Exploration vision in this repository must reference these documents rather than duplicate the Resource Graph page schema or component authority rules.
