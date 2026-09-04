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

## Canonical documentation

The normative design-system architecture lives in `onedayonemasterpiece/lovekgd-design-system`:

- `docs/resource-graph-004.md`;
- `docs/component-contract-authority.md`;
- `docs/source-first-component-decoder.md`;
- `docs/penpot-product-design-operating-model.md`;
- `contracts/resource-graph-scaffold.v1.json`;
- `receipts/penpot/resource-graph-to-be-structure-v1.json`.

The integrated Product Atlas/UI Exploration vision in this repository must reference these documents rather than duplicate the Resource Graph page schema or component authority rules.

## Executable Astro family SoT

Launch-critical Astro family identity and impact are executable beside the source:

- registry: `site/src/design-system/astro-family-registry.v1.json`;
- generated reverse graph: `site/src/design-system/astro-family-consumers.generated.v1.json`;
- generator / impact query: `site/scripts/generate-astro-family-consumer-graph.mjs`;
- fail-closed checker: `site/scripts/check-astro-family-sot.mjs`;
- CI entrypoint: `cd site && npm run check:astro-family-sot`.

```bash
node site/scripts/generate-astro-family-consumer-graph.mjs --impact EventCard
node site/scripts/generate-astro-family-consumer-graph.mjs --write
node site/scripts/check-astro-family-sot.mjs
```

Penpot binding is optional registry metadata and is not required by these commands.
