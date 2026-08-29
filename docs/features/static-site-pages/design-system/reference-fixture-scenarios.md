# Reference fixture scenarios

The durable fixture and archetype-scenario authority is
`lovekgd-design-system/docs/ui-reference-fixture-registry.md` and its versioned
registry under `catalog/fixtures/design-system-reference/`.

This repository contains only the executable bridge:

- `site/src/data/design-system-reference-fixtures.json` is a generated consumer
  projection: it selects factual IDs from `preview-events.json`, records the
  distinct container families and pins the SHA-256 of both canonical SoT JSON
  files. A scenario change is incomplete until those pins and the parity test
  are refreshed together;
- `site/src/data/designSystemReferenceFixtures.ts` validates and resolves one
  explicit local/preview scenario;
- a real route consumes that selection through its normal component tree and
  emits `data-ui-fixture-scenario` for browser evidence.

Do not put event payload copies or page-local ID arrays into a route. Production
and secret-candidate builds must reject fixture mode.

First scenario:

```bash
PUBLIC_DESIGN_FIXTURE_PROFILE=design-system-reference-v2 \
PUBLIC_UI_SOT_SCENARIO=free-collection-5-desktop-v1 \
PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE=2026-07-23 \
npm --prefix site run build
```

This renders `/podborki/besplatnye-sobytiya/` with the factual IDs
`7030, 7006, 6901, 6996, 6997` through `FreeCollectionSurface` →
`OptimizedEventCardGrid` → canonical `EventCard`. The optimizer's actual DOM
order is a separate asserted output; Penpot must follow that output rather than
hand-arranging input order.
