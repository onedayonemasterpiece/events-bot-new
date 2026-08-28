# Product Atlas Git SoT v1

> **Status:** recovered current candidate for product-owner review and later separate-account visualization.  
> **Product authority:** `onedayonemasterpiece/events-bot-new`.  
> **UI authority:** `onedayonemasterpiece/lovekgd-design-system`.  
> **Current source cut:** 2026-08-28.  
> **Penpot in this recovery:** not read or mutated.

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

It does not replace the existing methodology in `docs/product-model/README.md` and does not repeat general JTBD or User Story theory. Existing research is evidence; accepted product truth is reconstructed from feature, analytics, release, acceptance, incident and runtime sources.

## Effective current model

The original v1 registries remain the stable semantic base. UI SoT and product inputs advanced after the first 25 August cut, so current truth is resolved as:

```text
stable v1 product entities
+ current-source-lock.2026-08-28.v2.json
+ product-delta.2026-08-28.v2.json
+ visualization-handoff.2026-08-28.v1.json
```

The recovery does not rewrite stable IDs. It supersedes only changed source currentness, entity definitions/status facets and visualization readiness.

The durable recovery record is [`recovery-2026-08-28.md`](recovery-2026-08-28.md).

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

`lovekgd-design-system` owns UI foundations, components, patterns, archetypes, ProductScreenStates and visual/runtime conformance. It may store typed foreign-key links and exact UI context, but must not duplicate product definitions.

`common-analytics` may provide methodology but is not a canonical runtime or Product Atlas input.

Future partner needs, Jobs and outcomes remain `not_modeled` until separate research exists.

## Layered UI authority after the SoT advance

There is no honest single “latest UI head” that can replace every other source. Current Product Atlas uses separate layers:

1. **Corrected semantic origin** — `lovekgd-design-system#50@9b8043f3bdb86fab4eee00bf94b0f10d4f029c50`.
2. **Source-proven AS-IS round-trip baseline** — `#52@b86bab3e91511b3d4bd7d953b22bceb847f02a51`:
   - 17 archetypes;
   - 34 desktop/mobile boards;
   - 97 regions;
   - 97 patterns;
   - 75 component identities;
   - 180 screen states;
   - zero orphan design IDs.
3. **Active owner-review delta** — `#53@47d0fef53c33200492d92f6a086d9b8813fe187e`, still `IN_PROGRESS` and containing `READY_FOR_OWNER_REREVIEW / processed: NO` items.
4. **Active Astro/UI candidate** — `events-bot-new#596@49c351873d40a2ea55f0a32837c7376e344d9c17`.
5. **Product hypotheses** — for example agent-assisted discovery and canonical location data in `#587@f78e7c5974b4192bddf9eea901ee6d8b57f51560`.

Product visualization can therefore proceed in parallel without pretending that component normalization, owner review, promotion or deployment is complete.

## Penpot delivery decision

The Product Atlas plugin path is superseded.

The active contract is:

```text
versioned Git Product Atlas SoT
→ reviewed immutable linkage/evidence package
→ explicit scoped Penpot MCP materialization
→ exact MCP read-back receipt in Git
→ owner review
```

The next visualization target will use a **separate Penpot account and separate Product Atlas file**. The existing design-system Penpot file and IDs are evidence only and are not reusable target bindings.

No Product Atlas plugin, plugin manifest, plugin namespace or automatic background synchronization is part of the architecture. Unknown target bindings remain `binding_pending`; UUIDs are never fabricated.

## Files

| File | Responsibility |
|---|---|
| `source-lock.v1.json` | historical 25 August repository/path authority cut |
| `source-lock-exact-resolutions.v1.json` | exact Git blobs or aggregate hashes for historical source entries |
| `current-source-lock.2026-08-28.v2.json` | current product/UI layers, exact heads and supersession after the SoT advance |
| `product-core.v1.json` | stakeholder lanes, user needs, Jobs, Job Stories and outcomes |
| `journeys.v1.json` | journeys, journey steps and alternative/recovery paths |
| `capabilities.v1.json` | stable product/service capabilities |
| `work-items.v1.json` | honest User Stories, operator jobs and release work boundaries |
| `enablers-and-guardrails.v1.json` | technical enablers and non-negotiable guardrails |
| `acceptance.v1.json` | acceptance rules and stable USR/ADD/live scenario entities |
| `measurement-and-decisions.v1.json` | stable domain events, measurement questions, product problems, UI gaps, findings and decisions |
| `ui-linkage.v1.json` | v1 product entity ↔ route ↔ archetype ↔ region ↔ pattern/component/state linkage |
| `unresolved-ledger.v1.json` | conflicts, partial evidence, not-modeled areas and pending bindings |
| `product-delta.2026-08-28.v2.json` | changed entity facts, new source-grounded gaps/decisions/hypotheses and unresolved updates |
| `visualization-handoff.2026-08-28.v1.json` | deterministic views and layered site-as-is input for the future separate Penpot account |
| `recovery-2026-08-28.md` | human-readable recovery decision and future MCP entry gate |

Validators:

- `scripts/validate_product_atlas_v1.py` — stable v1 structural and semantic model;
- `scripts/validate_product_atlas_recovery_20260828.py` — current layer, exact external refs and visualization handoff;
- `tests/test_product_atlas_v1.py`;
- `tests/test_product_atlas_recovery_20260828.py`.

## Required entity model

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

A UI, route, code path, green test, Penpot readback or production deployment does not by itself prove a user or owner outcome.

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

The current visualization handoff preserves all 17 archetypes and keeps the following layers visually distinct:

- accepted product meaning;
- source-proven site-as-is;
- active owner-review delta;
- hypothesis;
- unresolved.

## Historical pilot

The August 2026 pilot with one `job.discover-event`, two journeys and six capabilities is retained as hypothesis/prototype evidence. It is not treated as the complete accepted model.

## Validation

Local structural checks:

```bash
python scripts/validate_product_atlas_v1.py
python scripts/validate_product_atlas_recovery_20260828.py
pytest --noconftest -q \
  tests/test_product_atlas_v1.py \
  tests/test_product_atlas_recovery_20260828.py
```

CI additionally checks out the exact current product main, UI baseline, owner-review delta, Astro candidate and product-hypothesis refs. It verifies their commit identities and source blobs before accepting the recovery.

The recovery validator checks:

- exact current source-layer SHAs;
- source and relation closure for delta entities;
- 17/17 visualization archetypes;
- all product/problem/outcome foreign keys;
- separate-account target fields remain null and `binding_pending`;
- no Product Atlas plugin and no fabricated/reused Penpot UUID;
- AS-IS baseline coverage equals `17 / 34 / 97 / 97 / 75 / 180`;
- active owner-review markers remain honest;
- `ListingDiscoveryRail@6` and exact seven-artifact candidate blobs match the locked Astro head;
- agent-assisted discovery remains a hypothesis.

## Review boundary

This version is ready for review of:

1. product entity boundaries and statuses;
2. accepted/source-proven/hypothesis separation;
3. the layered site-as-is versus owner-review model;
4. product problems and UI gaps selected for visualization;
5. outcome-evidence gaps;
6. later separate-account Penpot target binding.

Merge, deployment, Product Atlas Penpot materialization and owner acceptance remain outside this Draft PR.
