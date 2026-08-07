# Penpot Resource Graph 004: production inventory, resources, archetypes and evidence

> **Status:** accepted target contract for the next Penpot delivery.
> **Replaces as target architecture:** screenshot-only Runtime Review 003.x.
> **Keeps:** exact runtime screenshots, native Penpot comments, resumable synchronization and deterministic comment-to-prompt generation.

## Product result

Penpot must contain one connected design-system graph rather than a collection of unrelated screenshots:

```text
accepted production release
→ production route, source and iconography inventory
→ native Penpot colors, typographies and icon resources
→ native/hybrid component resources and variants
→ product patterns
→ archetypes assembled from component instances
→ automated actual/baseline/diff evidence at multiple viewports
→ comments routed to a resource, component, pattern, archetype or evidence item
```

A screenshot remains first-class evidence. It does not substitute for the component graph.

## Source selection

The inventory is built only from an **accepted production release**, never from an arbitrary checkout of `main` and never from the manually curated `/lab/design-system/` page.

The required release identity is:

```text
static-release-manifest.json
+ production-build.json
+ exact repo_sha
+ build_id
+ run_id
+ snapshot_id and snapshot_sha256
```

A page enters the current inventory only when its HTML exists in that production artifact. A component or icon enters the current inventory only when it is transitively reachable from one of those production page sources, or referenced by the accepted production artifact, at the same exact SHA. Merely existing in `site/src/components` or `site/public` is insufficient.

Excluded from current production inventory:

- `/lab/**`;
- `/__preview/**`;
- detached prototype fixtures;
- deprecated implementations not reached by production pages;
- historical screenshots and superseded candidates;
- icon files that exist in the repository but have no production consumer.

Those items may appear only in candidate, archive, legacy, unclassified or technical-test sections.

The machine-readable surface contract is [`site/src/data/design-system-production-surface-contract.v1.json`](../../../site/src/data/design-system-production-surface-contract.v1.json). It explicitly covers the current product surfaces requested for review: brand/PWA, current header and navigation, bottom navigation, footer, keyboard navigation, event cards and rails, event detail media geometries, medallions, artifacts, focus group, exhibitions, favorites, authorization, search, «Для меня», rail/bus schedules, notices and status surfaces.

The machine-readable iconography contract is [`site/src/data/design-system-iconography-contract.v1.json`](../../../site/src/data/design-system-iconography-contract.v1.json). It inventories the canonical inline UI and social icon components, external SVG collections, attribution/provenance, consumer edges, optical tests, accessibility semantics, legacy/duplicate/unclassified states and the required Penpot resource structure.

The production-surface checker [`site/scripts/check-design-system-production-surface-contract.mjs`](../../../site/scripts/check-design-system-production-surface-contract.mjs):

1. validates the source contract and all referenced source paths;
2. validates the exact production release identity;
3. inventories production HTML routes;
4. maps routes to their current Astro page sources;
5. follows relative imports to find production-reachable components;
6. reports missing required families and archetypes;
7. writes one immutable inventory for the Penpot catalog build.

The iconography checker [`site/scripts/check-design-system-iconography-contract.mjs`](../../../site/scripts/check-design-system-iconography-contract.mjs):

1. compares the exact `Icon.astro` and `SocialIcon.astro` name unions with the contract;
2. verifies expected SVG files and attribution/provenance records;
3. discovers inline SVG, external SVG, URL/mask references and component consumers;
4. separates current, candidate, legacy, unused and unclassified iconography;
5. reports raw inline SVG outside canonical icon components, duplicate/unclassified risks and missing consumers;
6. emits `artifacts/design-system/iconography-inventory.json` for Resource Graph 004.

Commands:

```bash
npm run check:design-system-production-surfaces -- \
  --dist <accepted-production-root> \
  --out artifacts/design-system/production-surface-inventory.json \
  --strict-production

npm run check:design-system-iconography -- \
  --dist <accepted-production-root> \
  --out artifacts/design-system/iconography-inventory.json \
  --strict-production
```

Resource Graph 004 publication must stop if either command reports a production gap.

## Penpot structure

```text
00 — System map
10 — Brand assets
20 — Foundations
25 — Iconography
30 — Core UI resources
40 — Announcements components
50 — Product patterns
60 — Page archetypes
70 — Coverage and fragmentation
80 — Candidate review
89 — Review archive
90 — Evidence / desktop
91 — Evidence / tablet
92 — Evidence / mobile
93 — Evidence / interaction and accessibility
99 — Technical tests
```

### Resource pages

`10–50` contain documentation boards and native Penpot resources:

- library colors;
- library typographies;
- native vector icon component masters and icon specimens;
- component masters;
- variant sets;
- component instances;
- product patterns assembled from instances;
- source paths, versions, status, known limitations and consumers.

Documentation boards use native flex/grid layouts. Components are not left as unrelated boards at absolute coordinates.

### Iconography page

`25 — Iconography` is a separate first-class resource plane, not a small row embedded in Foundations. It contains:

```text
System and actions
Navigation
Status and feedback
Social and external services
Transport
Festival and editorial categories
Product-specialized symbols
Optical alignment and size tests
Accessibility semantics
Duplicates, legacy and unclassified
```

Each **current** icon is a native vector Penpot component master under a hierarchical path such as:

```text
Icon/UI/Share
Icon/Navigation/Search
Icon/Status/Warning
Icon/Social/VK
Icon/Transport/Bus
Icon/Editorial/Festival category/Theatre
Icon/Product/Artifact
```

Rasterized screenshots are not accepted as icon masters. A specimen may show the icon at `16`, `20`, `24` and `32` px, inside a `44` px control target, on light, brand, dark and status backgrounds. The page also records source `viewBox`, optical size/alignment, stroke/fill behavior, `currentColor` support, forced-colors behavior, semantic role, accessible semantics, attribution/license and production consumers.

Current icon groups are populated only from production evidence. Repository assets without a current production consumer remain visibly classified as `candidate`, `legacy`, `unused` or `unclassified`; they are not silently promoted into the system.

PWA icons, favicon artwork and channel lockups remain on `10 — Brand assets`. They are cross-linked from Iconography because they share visual language, but they are not generic UI icons.

A comment on an icon master targets the shared icon and all consumers. A comment on an icon instance inside an archetype includes both the master and that exact page/variant context. A comment on a collection section targets system-level consistency, licensing, optical alignment or fragmentation.

### Archetype page

Each archetype is assembled from component instances. Its metadata records:

```text
archetype_id
production routes
component instance graph
icon instance graph
variant properties
source files
release repo_sha
build_id
snapshot_id
evidence_refs[]
```

Examples include home, today, tomorrow, weekend, popular, collections, festivals, exhibitions, favorites, search, «Для меня», focus group, artifacts, event detail and information pages.

The event-detail archetype must separately cover at least:

- wide image;
- narrow image;
- no image;
- desktop, tablet and mobile behavior.

## Screenshot evidence

Actual screenshots are generated by automated tests, not manually uploaded.

The canonical viewport matrix is:

```text
390×844   mobile
430×932   mobile
768×1024  tablet
1280×800  desktop
1728×900  desktop
```

A route may use a justified subset, but every required archetype has at least one mobile and one desktop actual screenshot. Responsive-boundary archetypes also include tablet evidence.

Each evidence item is one of:

- `actual` — what the accepted production artifact renders now;
- `approved-baseline` — the last owner-approved visual state;
- `diff` — the machine-produced comparison when actual differs from baseline.

Required metadata:

```text
test_id
route
viewport_id
repo_sha
build_id
snapshot_id
run_id
captured_at
sha256
baseline_sha256 (when applicable)
diff_sha256 (when applicable)
```

Evidence boards live only on pages `90–93`. Archetypes and component documentation store references to them. The plugin UI must provide an action to open the related actual/baseline/diff evidence without duplicating the raster screenshot on the component page.

This preserves both needs:

- component and icon-level inspection and defragmentation;
- exact proof of what users see at each tested resolution.

## One-update interaction contract

A design-system update is one orchestration operation. The user must never repeat synchronization per page, component, icon or file.

The plugin exposes at most three primary actions:

1. **Проверить актуальность** — optional preflight; also runs automatically when the plugin opens.
2. **Обновить дизайн-систему** — the only mutation action.
3. **Собрать промпт по комментариям** — review output.

`Обновить дизайн-систему` performs the complete pipeline:

```text
load one signed/hashed catalog
→ validate accepted release identity
→ reconcile colors and typographies
→ reconcile icon inventory, native masters and specimens
→ reconcile component masters and variants
→ reconcile instances and product patterns
→ reconcile archetypes and icon-consumer links
→ reconcile actual/baseline/diff evidence pages
→ write resource↔archetype↔evidence links
→ preserve comments and review snapshots
→ verify counts, hashes, references, provenance and currentness
→ publish one final report
```

The operation remains host-safe and resumable. Internally it may process batches and switch pages, but the user performs one action. A crash or transient error resumes from a checkpoint; it does not ask the user to continue page by page.

Expected steady-state report:

```text
PRODUCTION SOURCE        current
RESOURCE LIBRARY         current
ICONOGRAPHY              current
ARCHETYPE COMPOSITION    current
EVIDENCE                 current
COVERAGE                  complete or explicit gaps
REVIEW                    unresolved comment count
```

The old single `CURRENT` badge is insufficient because a current screenshot mirror can coexist with an incomplete component or icon library.

## Comment routing

A native Penpot comment is routed according to the object carrying it:

| Comment target | Prompt scope |
|---|---|
| color or typography resource | token plus all known consumers |
| icon master or icon variant | icon source, semantic role, provenance and every component/archetype consumer |
| icon instance in an archetype | icon master plus the exact archetype, route and variant context |
| iconography collection section | consistency, optical alignment, licensing or fragmentation policy |
| component master or variant | component API plus every archetype consumer |
| component instance | component plus the exact archetype context and variant values |
| product pattern | composition and user task |
| archetype | page composition and route family |
| actual/baseline/diff evidence | runtime divergence, regression or local override |

Generated prompts include stable resource IDs, source paths, exact production release identity, consumers, archetype/route context, evidence references and unresolved Penpot thread IDs.

## Acceptance criteria for Resource Graph 004

The delivery is accepted only when one real Penpot-file update demonstrates all of the following:

- one plugin opening;
- no more than three user actions;
- inventory generated from one accepted production release;
- no `/lab` or stale prototype source in current inventory;
- native colors and typographies present in Resources;
- a separate `25 — Iconography` plane exists;
- every current icon is a native vector component master rather than a raster screenshot;
- current icons have production consumers, source/provenance and archetype links;
- no production icon remains unclassified;
- hierarchical component resources and instances present;
- required archetypes assembled from instances;
- actual screenshots on separate evidence pages at the declared viewports;
- archetype-to-evidence navigation works;
- comments on an icon, component and archetype produce correctly scoped prompts;
- second preflight reports no pending managed changes;
- coverage and iconography gaps, if any, are explicit rather than hidden behind `CURRENT`.
