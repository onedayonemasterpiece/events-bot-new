# Static-site design-system integration

## Current: REOPENED_AFTER_OWNER_AUDIO_REVIEW

The owner rejected the normalized review build at
`8b1bb81298bfca2fe2aaa3ceb7e5f654748b301f` after the completion report in
[events-bot-new#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621#issuecomment-5546939304).
Do not repeat “normalization completed”, “READY_FOR_OWNER_REVIEW”, or A=S=P
based on that report or on passing family/token inventories.

Executable integration target remains `agent/static-site-single-kaggle-contract`.
Applicable corrections continue the existing
`work/owner-audioreview-card-geometry-20260905` branch; they are not automatically
merged, published, or owner-approved.

Start with the current [STATUS](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/integration/launch-normalized-sot-penpot-20260902/docs/launch-normalization/STATUS.md)
and the existing [audio-review register](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/integration/launch-normalized-sot-penpot-20260902/docs/launch-normalization/owner-audioreview-20260905.md).
The [programme contract](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/integration/launch-normalized-sot-penpot-20260902/contracts/launch-normalized-ui.v1.yaml)
still governs scope and publication. The old “await owner review” heading in
[launch-normalization-48h.md](launch-normalization-48h.md) is historical stage
routing, not evidence that the September 5 rejection has been resolved.

## Semantic roles, not renamed accidents

Typography authority is
`site/src/components/design-system/f0-typography-authority.v1.json`.
`owner_review_roles` defines fixed H1/H2 expectations at 1440×900, 390×844 and
1920×1080. `foundations.css` owns ordinary heading roles;
`surface-foundations.css` aliases ordinary display titles to those roles.
Home editorial scenes, event-name heroes, time markers, month indexes,
card titles, footer labels and compact sticky context are distinct roles,
not licenses to introduce arbitrary per-route typography.

The existing `site/scripts/check-browser-release-gate.mjs` verifies rendered
metrics and negative local overrides, text/ancestor clipping, header occlusion,
internal card overflow, row geometry, product Home links and mobile navigation.
It also exercises restored Home scenes, pause/resume, reduced motion, no-JS
fallback, and schematic floating context. The fixed-number oracle is not
regenerated from the page being tested.

The existing F0 Actions runner is now read-only and delegates no decisions to
an agent. Its isolated page-class builds use committed fixtures: their screenshots
are useful diagnostics, **not** proof of the rejected real snapshot after repair.
Full published successors use only the existing Kaggle builder/publisher.
Production root, stable ICS and Penpot are not changed by these diagnostics.

## AR-04/05 shared internal tracks (candidate v2 correction)

AdaptiveEventCardGrid v2 remains the unaccepted owner-review candidate. Its
four card anatomy tracks now use CSS subgrid (`grid-subgrid` diagnostics), so media/body/utility/feedback
boundaries align across a rendered row, including runtime-reordered/inserted
cards. Normal explicit columns retain singleton width; no auto-fit stretching
or removal of admitted events. Multi-column flow uses the existing compact5:4
frame from relatedCardLayout; OCR/unknown documents remain contained, and
single-column mobile retains intrinsic poster geometry. The grid constrains
intrinsic child widths and allows the negative-action label to wrap instead of
forcing horizontal overflow at the621px seam. Packed media policy is unchanged.

The existing browser gate measures all four internal boundaries. Negative
controls poison a real media shell and span a singleton across all columns;
neither equal outer rectangles nor an ineffective flex override proves PASS.
Focused fixtures are diagnostics, not same-snapshot real acceptance.

## AR-11 semantic reading correction

The existing production keyboard router now traverses title → visible prose
paragraphs → practical summary → related cards with ArrowDown. ArrowUp from
the first related row returns through the reading stops. Existing editable,
modifier/composition, dialog and gallery guards remain; no global scroll-jump
or new router is introduced. Source tests are diagnostic; the same-snapshot candidate remains required. A real5370 focused probe exposed
`data-event-title` metadata on the action panel before the H1; the router now
selects `h1[data-event-title]`, with an executable DOM-collision regression.
Diagnostic module replay reaches H1 → four paragraphs → practical → first
related card at1440/1920; this is not independent candidate acceptance.

## Existing owners and reverse impact

Do not add a second registry, checker, design system or build pipeline.

```bash
# From repository root; explicit node_modules dependencies must be installed.
node site/scripts/generate-astro-family-consumer-graph.mjs --impact EventCard
node site/scripts/generate-astro-family-consumer-graph.mjs --write
node site/scripts/generate-token-impact-graph.mjs --write
node site/scripts/check-astro-family-sot.mjs
node site/scripts/check-token-impact-sot.mjs
```

Canonical registries and generated dependencies:

- `site/src/design-system/astro-family-registry.v1.json` and
  `astro-family-consumers.generated.v1.json`;
- `site/src/design-system/token-authority-registry.v1.json` and
  `token-impact.generated.v1.json`;
- `site/src/data/design-system-production-surface-contract.v1.json`.

Owners include actual static, runtime/hydrated, nested, responsive and asset
consumers. Literal identity coverage, graph freshness, alias-cycle rejection and
style ownership are necessary source checks, not visual acceptance.
AdaptiveEventCardGrid/relatedCardLayout own admission and ordinary-column
remainder geometry for both static and client-created cards. EventCard owns
final media decisions; unknown/OCR content cannot be cropped merely because
an area-loss threshold is small. EventLayout owns the shared shell; a route
must not recolor global chrome through body inheritance. Floating context
reuses that shell and Button; no final island visual style has been chosen.

Service pages, laboratories and preview-shell styling are outside normalization.
Product pages reached through preview and product-shared dependencies remain
inside scope. Keep the working preview shell/endpoints/PWA/assets.

## Structural projection and Penpot boundary

Continue the existing exporter
`scripts/current_ui_resource_graph/v1/specimens/capture.mjs` /
`captureFreeCollectionStructuralProjection` and validator in `validate.mjs`.
They resolve source owners through the same family registry and exact manifest
SHA, and capture five ordered real event IDs at desktop/mobile sizes with
DOM, styles, SVG, image hashes, actions and parent-linked anatomy.

```bash
node --test tests/test_current_ui_free_collection_structural_projection.mjs
# For an actual packet, set PROJECTION_PACKET to the existing exported JSON.
```

A packet for the rejected SHA is not a packet for corrected source. Regenerate
against the next same-snapshot published candidate without inventing Penpot
shape IDs. Native Penpot materialization is not required before this review,
but A=S=P cannot be claimed without a checked P round-trip.

## Historical implementation detail

The previous long index, component-by-component migration descriptions and
reconstruction hypotheses remain [at the rejected baseline](https://github.com/onedayonemasterpiece/events-bot-new/blob/8b1bb81298bfca2fe2aaa3ceb7e5f654748b301f/docs/features/static-site-pages/design-system/README.md).
They are history, not a current instruction to repeat reconstruction or restore
accidental old values. Operational rules not superseded by the owner remain in
`launch-normalization-48h.md` and the linked programme contract.


### Owner review: remaining visual false positives (2026-09-05)

The ordinary `HomeColdStartFeed` section title uses the shared H2 role; it is
not part of Hero Talk's editorial exception. `Breadcrumbs.headerClearance`
reserves the existing overhanging desktop brand in flush compositions;
`FreeCollectionSurface` uses it instead of a page-local CSS override.
The existing browser gate measures breadcrumb chrome occlusion, asserts a
singleton against the declared ordinary column (not another card), and
exercises 1/2/3/4/5/7/10 existing Home cards after local-profile filtering.
That runtime fixture is not an organic cloud-personalized feed. Full
same-snapshot Kaggle acceptance and the open owner-review register remain
required; no palette completion or A=S=P is inferred from this diagnostic.
