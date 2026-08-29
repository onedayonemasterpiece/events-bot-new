# Reference fixture scenarios

Status: `ACTIVE_EXECUTABLE_BRIDGE / DRAFT_CONTOUR`

The durable fixture and archetype-scenario authority is
`lovekgd-design-system/docs/ui-reference-fixture-registry.md` and its versioned
registry under `catalog/fixtures/design-system-reference/` on the current head
of Draft PR `lovekgd-design-system#53`.

This repository contains only the executable bridge:

- `site/src/data/design-system-reference-fixtures.json` is a generated consumer
  projection: it selects factual IDs from `preview-events.json`, records the
  distinct container families and pins the SHA-256 of the canonical registry and
  scenario files. A scenario change is incomplete until those pins and parity
  tests are refreshed together;
- `site/src/data/designSystemReferenceFixtures.ts` validates and resolves one
  explicit local/preview scenario;
- a real route consumes that selection through its normal component tree and
  emits `data-ui-fixture-scenario` for browser evidence.

Do not put event payload copies or page-local ID arrays into a route. Production
and secret-candidate builds must reject fixture mode.

## One comparison — one exact scenario

The owner requirement for a shared Golden Corpus means:

```text
one bounded comparison
= one named versioned scenario/pool
+ the same fixture IDs and payload/media hashes in Astro and Penpot
+ one frozen clock/locale/viewport/state manifest
```

It does **not** mean that every component, archetype and product entity must use
one universal list of events.

Current executable bridge intentionally separates scopes:

| Scope | Current pool |
|---|---|
| component conformance | 8 factual events: `3132, 4327, 6399, 6628, 7807, 7888, 7906, 8156` |
| archetype core | 5 factual events: `7030, 7006, 6901, 6996, 6997` |
| festival timeline | 7 factual festival slugs in bounded `1 / 4 / 2` rows |
| interest clubs | 3 factual club slugs |
| Artifact Collection 1 | 7 factual artifacts in the design-system registry |

The eight-event component corpus originated in the Golden Event Corpus pilot
(Draft PR `lovekgd-design-system#42`). Its identity gates passed, while the
pilot's visual conformance was explicitly recorded as FAIL. The five-event
archetype pool is a later bounded scenario registry, not a replacement claim for
that pilot.

A test, screenshot or Penpot board must always name which pool/scenario it uses.
Mixing the eight-event component set with the five-event archetype set and still
calling the comparison exact is invalid.

## First executable archetype scenario

```bash
PUBLIC_DESIGN_FIXTURE_PROFILE=design-system-reference-v2 \
PUBLIC_UI_SOT_SCENARIO=free-collection-5-desktop-v1 \
PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE=2026-07-23 \
npm --prefix site run build
```

This renders `/podborki/besplatnye-sobytiya/` with the factual IDs
`7030, 7006, 6901, 6996, 6997` through `FreeCollectionSurface` →
`OptimizedEventCardGrid` → canonical `EventCard`. The optimizer's actual DOM
order is a separately asserted output; Penpot must follow that output rather
than hand-arranging input order.

Exact parity requires the same:

- scenario ID and registry/scenario hashes;
- event IDs and complete resolved payload hashes;
- media bytes/framing decision;
- reference date, timezone and locale;
- viewport, DPR, fonts, theme, auth/consent/personalization and interaction state;
- component and container identities.

Card identity and container identity remain separate. An equal-height EventCard
grid, compact listing rows, festival timeline rows and interest-club grid must
not be collapsed into a generic `PackedCardRow` merely because they contain
multiple cards.

## Dense and production coverage

The bounded pools keep Penpot review pages small and repeatable. They do not
replace generated Astro validation over dense/full listings, negative states,
long copy and production-scale data. Full production listings remain executable
stress evidence and are not copied wholesale into Penpot unless a specific
owner-review state requires them.
