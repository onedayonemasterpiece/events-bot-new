# Current UI Decoder v1 — core lane result

## Scope delivered

- Preserved every Current UI Resource Graph v0 output and decoder contract.
- Added the compact immutable v1 snapshot writer at
  `catalog/component-decoder/<snapshot-id>/` with manifest, partial/complete
  receipt, summary, artifact index, source/binding/composition/consumer/route
  records, 107 per-component JSON records, page state signatures, specimen
  plans, verification, mismatches, unresolved records and explicit placeholder
  directories for later candidate-contract/capsule lanes.
- Added the reviewed 107/107 disposition registry and closed reachability enums
  with per-plane bindings, evidence bases, precise proof labels and canonical
  count invariants.
- Added bounded `@babel/parser` TypeScript/JavaScript facts for prop optionality,
  literal unions, defaults, branches, derived state and flags, plus Astro state
  markers, interaction attributes, media rules, responsive/container contexts
  and local override sites.
- Fixed Astro inline `<script>` imports entering the source dependency graph;
  the exact candidate now records the
  `ClubCatalogKeyboard.astro -> clubCatalogNavigation.mjs` edge.
- Added bounded component-scoped browser evidence scaffolding for element
  screenshots, safe DOM attributes, computed styles, geometry, CSS variables,
  accessibility/focus/state, breakpoint context and override provenance.
- Added explicit exceptions for `labs-preview-special`, absent/future Editorial
  Collections, Legal and page-end Hero-talk, plus source-only experiment-off
  transport treatments.
- Added a fail-closed v1 Go/No-Go manifest and Actions validation. Decoder
  completion with incomplete evidence is `complete / partial / NO_GO`, never a
  successful design-system handoff.
- Updated the canonical feature document and `[Unreleased]` changelog.

## Safety / boundary

- No `site/src`, Astro, CSS, runtime UI, Penpot, token, normalization, merge or
  split artifact was changed.
- Candidate/public-root identity planes remain separate.
- Full HTML and raw navigation/media URLs are excluded from component evidence;
  credential/URL-shaped strings are hash/length records.
- Candidate AS-IS contracts remain non-normative and are intentionally left to
  the contracts/capsules lane.

## Verification

- `node --check` passed for `decode.mjs`, `graph-lib.mjs` and all v1 modules.
- `pytest -q tests/test_current_ui_resource_graph.py`: **28 passed**.
- A local Playwright element-capture smoke at the registered 420px context
  emitted one safe `exact-candidate-browser-element` record and its bounded
  element JPEG successfully.
- Exact detached source-plane audit:
  - candidate components: 106;
  - public-root components: 107;
  - logical union: 107;
  - dispositions: 51 production UI, 20 composition/layout, 20 lab-only, 4
    experiment-only, 1 support/data, 8 nonvisual, 2 dead/unreachable, 1 needs
    verification;
  - candidate state parsing: 105 parsed, 1 source-empty, 0 parse failures
    (inline client scripts are parsed as state evidence, not only imports);
  - `clubCatalogNavigation.mjs` has the expected keyboard consumer.

## Remaining downstream gates

Transport, medallion and artifact specimen lanes must add controlled state
observations. The contracts/capsules lane must add candidate AS-IS contracts,
six reconciliation capsules and source-to-specimen-to-real-page traces before
the v1 receipt can legitimately become `GO`.
