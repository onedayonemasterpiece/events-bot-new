# Agent route — static-site design system

This directory is the executable Astro bridge, not the design-system authority.

For every task involving static-site UI, components, archetypes, Penpot,
fixtures or visual lineage:

1. read this directory's `README.md`;
2. read `reference-fixture-scenarios.md`;
3. fresh-read Draft PR `onedayonemasterpiece/lovekgd-design-system#53` and open:
   - `docs/static-site-design-system-current-state.md`;
   - `docs/reviews/index.md`;
   - `docs/ui-source-of-truth-roundtrip.md`;
   - `docs/ui-reference-fixture-registry.md`;
   - the affected contract and newest receipt;
4. read owner correction `REV-CHAT-20260829-01` / `OV-59`;
5. fresh-read current head of this repository's Draft PR `#596`.

Critical rules:

- **SoT UI is the central system.** Its durable implementation lives in
  versioned Git contracts/package data in `lovekgd-design-system`.
- Penpot is a native visual projection/review surface, not a central system or
  direct source for Astro.
- Astro is the executable projection/consumer and pre-promotion AS-IS evidence.
- Accepted Penpot feedback returns to SoT UI before Astro or Penpot is changed.
- Target propagation is `SoT UI → Penpot` and `SoT UI → Astro`.
- Direct Penpot → Astro propagation, Penpot-only fixes and route-local Astro
  visual forks are forbidden.
- Component masters/state catalogs stay on bounded library pages; archetypes
  consume linked instances.
- Page-local masters, detached copies, screenshots-as-components and visually
  similar unrelated roots are forbidden.
- Structural PASS, visual PASS, owner acceptance, promotion and deploy are
  separate gates.

Fixture rules:

- one comparison uses one SoT corpus/registry version and one named scenario;
- Astro and Penpot must use identical fixture IDs and payload/media hashes;
- typed pools and scenario subsets are allowed under one SoT authority;
- the current 8-event component corpus and disjoint 5-event archetype pool are
  an open authority-unification gap, not a final unified Golden Corpus;
- never mix the two sets under an exact-parity claim;
- never create page-local fixture lists except explicit legacy characterization;
- Draft PR `#596` is not production.

Do not duplicate the full lifecycle or review register here. Keep local docs as
fail-closed bridges to the current source-bound SoT records in
`lovekgd-design-system#53`.
