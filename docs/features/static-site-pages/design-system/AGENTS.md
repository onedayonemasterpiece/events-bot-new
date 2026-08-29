# Agent route — static-site design system

This directory is the local Astro bridge, not the full design-system authority.

For every task under this directory or any task involving static-site UI,
components, archetypes, Penpot, fixture parity or visual lineage:

1. read `README.md` in this directory;
2. read `reference-fixture-scenarios.md` before choosing data for any comparison;
3. fresh-read Draft PR `onedayonemasterpiece/lovekgd-design-system#53`, then open:
   - `docs/static-site-design-system-current-state.md`;
   - `docs/ui-source-of-truth-roundtrip.md`;
   - `docs/reviews/index.md`;
   - the affected family/archetype contract and latest source-bound receipt;
4. fresh-read current head of this repository's Draft PR `#596`;
5. use one named versioned scenario/pool and prove exact fixture-ID/hash parity.

Critical rules:

- durable UI SoT is Git contract/package data in `lovekgd-design-system`;
- pinned Astro/runtime is the executable AS-IS fact before family promotion;
- Penpot is native visual implementation/review, not an independent release authority;
- automatic bidirectional Penpot ↔ Astro sync does not exist;
- component masters/state catalogs stay on bounded library pages; archetypes use linked instances;
- page-local masters, detached copies, screenshot substitutes and visually similar unrelated roots are forbidden;
- structural PASS, visual PASS, owner acceptance, promotion and deploy are different gates;
- the 8-event component corpus and 5-event archetype pool are different scopes; never mix them under one exact-parity claim;
- Draft PR `#596` is not production.

Do not duplicate the full lifecycle or current review register in this repository.
Keep this bridge short and point all normative decisions to the current
`lovekgd-design-system#53` source-bound contract/receipt.
