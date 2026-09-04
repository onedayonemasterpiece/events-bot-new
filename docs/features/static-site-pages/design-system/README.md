# Static-site design-system integration

## Действующий этап: завершение нормализации → аудиоревью владельца

Единственная executable authority — `agent/static-site-single-kaggle-contract`.
Решением владельца 2026-09-04 прежняя открытая программа заменена конечным
этапом: [scope, gates, остановка и ссылки на контракт/STATUS](launch-normalization-48h.md#действующий-этап-завершение-нормализации--аудиоревью-владельца).
Полная материализация в Penpot и перенос Astro в новый пакет не требуются.
Валидированная структурная проекция требуется; A=S=P без Penpot не заявляется.
Ниже сохранена история реконструкции, не инструкция повторять её.

## История реконструкции — не состояние текущего этапа

```text
Resource Graph TO-BE structure: PASS
Penpot file_id: 3be9e5e1-190f-8090-8008-713c0fbe6260
native design-system components: 0
source-first decoder: not started
promoted component families: 0
```

Принятой компонентной дизайн-системы ранее не существовало. Предыдущие Penpot generations 003–005 являются technical experiments and historical evidence. Активный Resource Graph очищен и содержит только пустую TO-BE структуру.

## Историческая authority во время реконструкции

До promotion каждого resource family текущий Astro source в этом репозитории является executable source of fact о существующей реализации. Его нельзя автоматически считать нормализованной дизайн-системой: decoder должен обнаружить component families, inline/CSS patterns, one-to-many/many-to-one relationships, states, consumers, local overrides и unresolved fragmentation.

## Историческая гипотеза package promotion — не требование этого этапа

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

## Исторический план decoder — superseded

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
Its inverse media fallbacks, gallery tiles and related-event surface likewise
consume the established `--ke-color-background-inverse` and
`--ke-color-background-inverse-raised` roles instead of repeating their exact
hex values; palette, layout and runtime behavior are unchanged.

`EventOccurrenceNav` consumes the existing occurrence color, surface, border,
shape and elevation roles from `event-detail-foundations.css` directly. The
desktop, mobile and practical variants therefore share one token-owned visual
language without changing their established schedule anatomy, target sizes or
responsive geometry.

`MobileBottomNav` consumes its existing shell-owned surface, border, elevation,
state-color, icon-size and icon-container roles. Its four canonical navigation
identities, current-route behavior and mobile geometry remain unchanged; the
component no longer repeats those shell values as a parallel local style owner.

`MobileToastRegion` consumes the same shell SoT for its surface, text, action,
state borders, progress, elevation, radius, touch target and close-glyph size.
The close glyph maps to the canonical 24 px action-icon role, preserving its
existing rendered size while keeping the product-wide four-size icon system.
Queueing, announcements, timing and placement remain owned by the component.

`WeatherDateContext` consumes its cross-route weather roles for desktop,
compact and mobile surfaces, including colors, borders, radii, minimum heights
and both icon tiers. The location glyph remains the 24 px action size; the
former 15 px water glyph is normalized to the canonical 16 px inline size.
Weather availability, loading and degraded-state behavior do not change.

`EventMediaRail` consumes the existing gallery, resolved-selector and poster
rail roles from `family-continuity-foundations.css` for surface, border,
elevation, spacing, shape and item geometry. This removes a second local visual
owner while preserving the established thumbnail counts, responsive packing,
MediaFrame crop authority and all resource-state behavior on event pages.

`EventFallbackArt` consumes the event-detail fallback surface and mobile-height
roles, so fallback presentation has one owner across every event page.
`UnusualListingSurface` uses the canonical editorial section-heading type role
for both populated and empty states. These handoffs do not change fallback
selection or listing behavior.

`SiteFooter` consumes its complete established continuity contract for share
banner, brand lockup, navigation columns, documents, social controls, utility
copy and responsive geometry. Because the token values were extracted from the
accepted footer, this is an exact visual handoff: footer behavior, links and
desktop/mobile composition remain unchanged while the shared SoT becomes the
only owner of those values across product routes.

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

### Media rail acceptance preservation

The poster-strip uses the existing `--ke-color-surface-inverse-raised` (#292521),
not the translucent hero-selector surface. Count/label consume the existing
primary override tokens before variant fallback tokens. This corrects accidental
value drift during token adoption; accepted palette/type semantics are unchanged.
Regression: `site/tests/event-media-rail-token-adoption.test.mjs`.

The N0 public-identity regression preserves the recorded accepted baseline state
(`PM0_3_BASELINE_DONE_CURRENT_SUCCESSOR_REBUILD_PENDING`) while still requiring
Kaggle, real/all and exact manifest/artifact identity for each successor. It must
not require the obsolete pre-baseline `BLOCKED_BY_SOURCE_CANDIDATE` value.

### Source-bound structural export for later native Penpot

The existing decoder specimen path now exports a bounded real free-collection
composition through `captureFreeCollectionStructuralProjection` in
`scripts/current_ui_resource_graph/v1/specimens/capture.mjs`, validated by
`assertFreeCollectionStructuralProjection` in adjacent `validate.mjs`.
This is a read-only exporter extension, not a new registry or materializer.

Inputs: a live Playwright page at the exact published product route, its
`preview-build.json` URL, expected full Git SHA, checked snapshot id/SHA256,
local repository and exactly five ordered real event IDs. Capture 1440×900 and
390×844 using the same IDs/snapshot/reference clock. The function itself reads
and verifies the manifest and resolves registry/owner/style bytes with
`git show <manifest SHA>:<path>`; uncommitted source is not substituted.

Output retains the actual bounded DOM/text tree, component identity/version,
parent-linked stable anatomy IDs, responsive geometry/computed/pseudo styles,
shared resolved token values plus per-node overrides, SVG markup+hash, image
URL+byte hash+natural geometry, actual action/calendar attributes, and
MediaFrame-v1 resource attributes. Only the first five grid cards enter the
specimen; the observed full grid count is context, not acceptance credit.
Nested source owners resolve through the existing family registry. No native
Penpot shape IDs are invented and no Penpot file is modified.

Example call within the existing Playwright capture session:

```js
const packet = await captureFreeCollectionStructuralProjection({
  page, manifestUrl, expectedSha, snapshot: { id: snapshotId, sha256: snapshotHash },
  repoRoot, expectedEventIds,
});
```

Regression: `node --test tests/test_current_ui_free_collection_structural_projection.mjs`.
For a stored real packet also set `PROJECTION_PACKET=<artifact.json>`.
Output goes to `artifacts/codex/`, never into a second manually maintained SoT.
Current exact packet identity/evidence belongs in the linked STATUS/#621.
The validator proves structural export integrity, **not** a native Penpot
round-trip, approved native typography rendering or full-site P materialization.

### Focus completion responsive regression

`/fokus-gruppa/zavershenie/` remains product scope because the programme page links to it. Its composition owner wraps long heading words within the available inline size; no global overflow clipping or completion-state behavior change is allowed. Regression: `site/tests/focus-programme-route-normalization.test.mjs`; browser acceptance covers completion states at mobile and desktop widths.

### Source identity completeness

The existing Astro-family checker rejects literal source identity values hidden behind empty registry arrays. Parametric state values require an explicit source-bound `dynamic_identity` contract and a real value regex, not invented enumeration labels. Inline Astro styles have one source owner (shared identity roots resolve that same source); runtime state producers remain the behavior authority. This is a description of current source, not permission for new variants or redesign.
