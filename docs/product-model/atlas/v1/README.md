# Product Atlas Git SoT v1

> **Status:** candidate for owner review.  
> **Product authority:** `onedayonemasterpiece/events-bot-new`.  
> **UI authority:** `onedayonemasterpiece/lovekgd-design-system`.  
> **Source cut:** 2026-08-25.  
> **Penpot:** not read or mutated in this work.

## Purpose

This directory materializes the concrete product layer of «Полюбить Калининград Анонсы» as versioned Git data:

```text
user needs
→ Jobs / Job Stories
→ user and owner outcomes
→ journeys, steps and recovery paths
→ capabilities
→ User Stories / operator jobs / technical enablers
→ guardrails and acceptance
→ implementation / release / runtime evidence
→ measurement questions
→ findings and decisions
→ exact foreign-key links to the UI SoT
```

It does not replace the existing methodology in `docs/product-model/README.md` and does not repeat general JTBD or User Story theory. Existing research is evidence; accepted product truth is reconstructed from current feature, analytics, release, acceptance, incident and runtime sources.

## Authority boundary

`events-bot-new` owns definitions and decisions for:

- user needs, Jobs and Job Stories;
- user and owner outcomes;
- journeys, capabilities and stories;
- operator jobs, technical enablers and guardrails;
- acceptance rules and scenarios;
- stable domain events and measurement questions;
- product problems, UI gaps, findings and decisions;
- independent delivery, verification, deployment, runtime and outcome facets.

`lovekgd-design-system` owns UI foundations, components, patterns, archetypes, ProductScreenStates and visual/runtime conformance. It may only store typed foreign-key links to product IDs and the exact UI context of each link. It must not duplicate product definitions.

`common-analytics` may provide methodology but is not a canonical runtime or Product Atlas input.

Future partner needs, Jobs and outcomes are `not_modeled` until separate research exists.

## Penpot delivery decision

The previously documented Product Atlas plugin path is superseded.

The active contract is:

```text
versioned Git Product Atlas SoT
→ reviewed immutable linkage/evidence package
→ explicit, scoped Penpot MCP materialization in a separate Product Atlas file
→ exact MCP read-back receipt in Git
```

No Product Atlas plugin, plugin manifest, plugin namespace or automatic background synchronization is part of the current architecture. Penpot MCP is not invoked by this Git-only change. Unknown Penpot bindings are represented only as typed `binding_pending` values; UUIDs are never fabricated.

## Files

| File | Responsibility |
|---|---|
| `source-lock.v1.json` | repository heads, paths, authority classification, currentness and supersession |
| `source-lock-exact-resolutions.v1.json` | exact Git blobs or aggregate hashes for every source whose primary lock entry had no single blob |
| `product-core.v1.json` | stakeholder lanes, user needs, Jobs, Job Stories and outcomes |
| `journeys.v1.json` | journeys, journey steps and alternative/recovery paths |
| `capabilities.v1.json` | stable product/service capabilities |
| `work-items.v1.json` | honest User Stories, operator jobs and release work boundaries |
| `enablers-and-guardrails.v1.json` | technical enablers and non-negotiable guardrails |
| `acceptance.v1.json` | acceptance rules and stable USR/ADD/live scenario entities |
| `measurement-and-decisions.v1.json` | stable domain events, measurement questions, product problems, UI gaps, findings and decisions |
| `ui-linkage.v1.json` | product entity ↔ route ↔ archetype ↔ region ↔ pattern/component/state linkage for all 17 corrected archetypes |
| `unresolved-ledger.v1.json` | conflicts, partial evidence, not-modeled areas and pending bindings |
| `scripts/validate_product_atlas_v1.py` | fail-closed structural, semantic and cross-repository validation |
| `tests/test_product_atlas_v1.py` | repository test entry point |

## Source locks

The Product Atlas input is pinned to:

- `events-bot-new@821e816b2c8317b1cc5e4b85c5ece72aa27a5c44`;
- corrected UI SoT Draft PR `lovekgd-design-system#50@9b8043f3bdb86fab4eee00bf94b0f10d4f029c50`;
- corrected UI manifest SHA-256 `ac2cb64bbccb113dd7c81cdb8caec953d3d5e2f56ea10a1f54914d7a0ed46819`;
- design-system planning Draft PR `#39@a2991f8b7cc516d7e80f95057d7b9e21ec81097f`.

The corrected route registry remains the authority for all 29 production route patterns, 29 source pages and 32 generated routes. Product Atlas stores exact archetype-scoped foreign-key contexts rather than copying and independently evolving that registry.

## Required entity fields

Every product entity has:

- stable semantic `id`;
- `kind`, `title` and precise `definition`;
- `stakeholder_lane`;
- semantic `status`;
- `confidence`;
- `source_refs`;
- typed `relations`;
- `facets`;
- `unresolved_conflicts`;
- `supersession_history`.

Allowed semantic statuses:

```text
accepted
source_proven
hypothesis
partial
unresolved
not_modeled
superseded
not_applicable
```

A single `done` status is forbidden.

Independent facets are retained for:

```text
definition
delivery
verification
deployment
runtime_health
evidence
user_outcome
owner_outcome
```

Facet values may describe the appropriate axis (`implemented`, `verified`, `deployed`, `healthy`, `degraded`, `unknown`, `not_measured`, and so on); these axes are never collapsed into one entity status. A UI, route, code path, green test or production deployment does not by itself prove a user or owner outcome.

## User Story boundary

A `user_story` must provide a useful vertical result and link to:

- one or more Jobs;
- one capability;
- an outcome and journey context;
- acceptance rules/examples;
- a measurement question;
- implementation/release evidence where it exists.

Buttons, pages, APIs, tables, migrations and internal infrastructure tasks are represented as UI implementation, operator jobs, technical enablers, guardrails or release deliverables. A false user is never invented for technical work.

## UI linkage grammar

The exact linkage shape is:

```text
product entity
↔ production route or route pattern
↔ corrected semantic archetype
↔ semantic region
↔ visual pattern
↔ configured component instance or runtime boundary
↔ ProductScreenState
↔ acceptance scenario
↔ measurement question
```

Generic component masters are not forced to have one Job. Product meaning may belong to a configured instance, product pattern, archetype region or ProductScreenState.

`ui-linkage.v1.json` links all 17 corrected archetypes. Native bindings remain `binding_pending`; it contains no invented Penpot UUIDs, file IDs, page IDs or board IDs.

## Historical pilot

The August 2026 pilot with one `job.discover-event`, two journeys and six capabilities is retained as hypothesis/prototype evidence. It is not treated as the complete accepted model. The current registry restores additional product entities from authoritative feature and operational sources and records evidence gaps explicitly.

## Validation

Run:

```bash
python scripts/validate_product_atlas_v1.py
pytest -q tests/test_product_atlas_v1.py
```

The validator checks:

- entity/source/status/lane/facet structure;
- globally unique product IDs;
- source and relation foreign keys;
- exact source-lock resolutions;
- honest User Story contracts;
- partner meaning remains `not_modeled`;
- 17/17 corrected archetypes are linked;
- acceptance and measurement references resolve;
- all native component instances remain `binding_pending`;
- no fabricated UUID and no status `done`;
- unresolved items identify a resolution gate and prohibited shortcut.

## Review boundary

This version is ready for product-owner review of:

1. entity boundaries and statuses;
2. accepted versus source-proven versus hypothesis classification;
3. unresolved outcome evidence;
4. route/archetype context links;
5. remaining `binding_pending` values.

Merge, deployment, Product Atlas Penpot materialization and owner acceptance are outside this Draft PR.