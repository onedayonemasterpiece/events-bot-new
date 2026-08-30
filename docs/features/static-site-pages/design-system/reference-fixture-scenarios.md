# Reference fixture scenarios

Status: `ACTIVE_EXECUTABLE_BRIDGE / SOT_EVENT_CORPUS_UNIFICATION_OPEN`

## Central authority

SoT UI owns the canonical fixture authority. This Astro repository contains
only an executable projection.

Target:

```text
one canonical SoT UI fixture registry
→ typed factual fixture records + payload/media hashes
→ named versioned scenarios/subsets
  ├─→ Astro projection
  └─→ Penpot projection
```

Penpot owns no fixture list. Astro owns product-data resolution but not an
independently editable design fixture authority.

Current normative route on Draft PR `lovekgd-design-system#53`:

- `docs/static-site-design-system-current-state.md`;
- `docs/ui-reference-fixture-registry.md`;
- `docs/reviews/owner-text-sot-ui-centrality-correction-20260829.md`;
- `catalog/fixtures/design-system-reference/`.

## Executable bridge

- `site/src/data/design-system-reference-fixtures.json` is a generated ID-only
  consumer projection. It selects factual payloads from `preview-events.json`
  and pins the canonical registry/scenario hashes.
- `site/src/data/designSystemReferenceFixtures.ts` validates and resolves an
  explicit local/preview scenario.
- a real route consumes that selection through its normal component tree and
  emits the scenario identity for browser evidence.

Do not copy payload fields or route-local ID arrays. Production and
secret-candidate builds must reject fixture mode.

## One comparison — one exact scenario

```text
one bounded comparison
= one SoT registry/corpus version
+ one named scenario
+ same fixture IDs and payload/media hashes in Astro and Penpot
+ frozen clock/locale/viewport/DPR/fonts/state
```

Different named scenario subsets are allowed. Different typed entity pools are
allowed under the same SoT authority. This does not permit parallel unlinked
event authorities.

## Current event-corpus discrepancy

Current bridge exposes two event scopes:

| Scope | Current set |
|---|---|
| component certification | `3132, 4327, 6399, 6628, 7807, 7888, 7906, 8156` |
| archetype core | `7030, 7006, 6901, 6996, 6997` |

The 8-event set came from the immutable Golden Event Corpus pilot; the 5-event
set came from the later archetype registry. They are disjoint. The current
SoT documentation explicitly says the archetype registry does not replace the
component corpus.

Consequences:

- exact parity inside either bounded scenario can be valid;
- mixing IDs between scenarios is invalid;
- calling the two contours one already unified Golden Corpus is also invalid;
- component → group → archetype continuity through one fixture authority is not
  yet proven.

Status: `SOT_FIXTURE_AUTHORITY_UNIFICATION_OPEN`.

Closure requires either registering both sets under one canonical SoT registry
with exact hashes/provenance, or explicitly superseding and migrating one
contour.

## Other typed pools

Current factual pools also include:

- festival reference rows: 7 festival slugs;
- interest clubs: 3 club slugs;
- Artifact Collection 1: 7 artifacts.

These may remain typed pools, but must be governed by the same central SoT
fixture authority rather than page-local lists.

## Current executable archetype scenario

```bash
PUBLIC_DESIGN_FIXTURE_PROFILE=design-system-reference-v2 \
PUBLIC_UI_SOT_SCENARIO=free-collection-5-desktop-v1 \
PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE=2026-07-23 \
npm --prefix site run build
```

This renders `/podborki/besplatnye-sobytiya/` with
`7030, 7006, 6901, 6996, 6997` through
`FreeCollectionSurface → OptimizedEventCardGrid → EventCard`.

The actual optimizer order is separately asserted; Penpot must follow the
rendered output rather than hand-arrange the input.

Exact bounded parity requires identical:

- registry/corpus and scenario hashes;
- fixture IDs and resolved payload hashes;
- media bytes and framing decisions;
- reference date, timezone and locale;
- viewport, DPR, fonts, theme, auth/consent/personalization and interaction
  state;
- component and container identities.

Card and container identities remain separate. EventCard grids, compact listing
rows, festival timeline rows and club grids must not be collapsed into one
`PackedCardRow` solely because they contain multiple cards.

## Dense and production coverage

Bounded fixtures keep Penpot review repeatable. They do not replace generated
Astro validation over dense/full listings, negative states, long copy and
production-scale data. Full listings remain executable stress evidence and are
not copied wholesale into Penpot without an explicit review need.
