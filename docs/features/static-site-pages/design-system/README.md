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

## Executable token authority and reverse impact SoT

CSS custom-property authority is independently materialized without changing a
token value or a visual consumer:

- policy registry: `site/src/design-system/token-authority-registry.v1.json`;
- generated census/impact graph: `site/src/design-system/token-impact.generated.v1.json`;
- deterministic generator and token query: `site/scripts/generate-token-impact-graph.mjs`;
- fail-closed verifier: `site/scripts/check-token-impact-sot.mjs`.

The census covers CSS declarations in `.css` and Astro style surfaces, aliases
formed with `var(--token)`, direct `var()` consumers, and runtime
`style.setProperty()` definitions. The impact projection follows source imports
and the existing generated Astro-family graph, so a query yields its direct
consumer components, affected registered families, and production routes.

```bash
# Read-only component/family/route impact for one token.
node site/scripts/generate-token-impact-graph.mjs --impact --ke-color-action-primary

# Regenerate only when source or registry authority intentionally changes.
node site/scripts/generate-token-impact-graph.mjs --write

# CI-safe verifier: no writes.
node site/scripts/check-token-impact-sot.mjs
node --test site/scripts/token-impact-graph.behavior.test.mjs
```

The checker rejects stale graph materialization, a stale Astro-family route
graph, conflicting global owners for the `--ke-` authority namespace,
non-fallback `var()` consumers with no CSS/runtime definition, and alias cycles
unless an exact, reasoned exception is present in the registry. Compatibility
variables outside that namespace remain in the census but are not promoted to a
second global design-token authority. Any intentionally unresolved legacy
boundary or shared global owner is an explicit, narrowly documented registry
entry; an entry becomes an error once it is no longer needed.

The desktop event-detail surface consumes `--ke-color-event-detail-ink` and
`--ke-color-event-detail-surface` directly; its prior component-local
`--clean-ink` / `--clean-paper` / `--clean-accent` aliases were exact duplicates
of the established event-detail roles and are not a second authority boundary.

`EventOccurrenceNav` consumes the existing occurrence color, surface, border,
shape and elevation roles from `event-detail-foundations.css` directly. The
desktop, mobile and practical variants therefore share one token-owned visual
language without changing their established schedule anatomy, target sizes or
responsive geometry.

## Iconography census boundary

`site/scripts/check-design-system-iconography-contract.mjs` scans all relevant
source formats for canonical `Icon`, `SemanticIcon` and `SocialIcon` consumers
and SVG asset references. `SemanticIcon` usage is attributed to the underlying
canonical UI identity instead of being mislabeled unused. The raw-inline-SVG
gap is deliberately narrower: it counts rendered
`<svg>` elements only in `.astro` source, matching the contract's
`inline-svg-in-astro` discovery rule. Generated JSON graphs, checker regexes and
serialized runtime markup therefore cannot masquerade as product icon owners.

```bash
cd site
node scripts/check-design-system-iconography-contract.mjs
node --test scripts/iconography-contract.behavior.test.mjs
```

### Complete production-surface family coverage

The Astro-family registry now covers every required component source and every
required archetype in
`site/src/data/design-system-production-surface-contract.v1.json`, in addition
to the source-published `data-ds-family` and `data-ds-component` identities.
`production_surface_contract` is a fail-closed mapping: a required component
source, archetype, route, or identity that is absent from the registry makes
`check:astro-family-sot` fail.

The consumer graph records direct component and style-import consumers,
source-marker protocol consumers (including distributed media/foundation
protocols), explicit runtime/hydrated factory/client consumers, and canonical
route patterns. Dynamic Astro route segments are materialized as `*` (or `**`)
so contract routes compare exactly. The opening-tag reader deliberately skips
`>` inside Astro `{...}` expressions before validating a family identity.
