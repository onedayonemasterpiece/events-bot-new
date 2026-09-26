---
name: static-site-design-system
description: "Use for any KenigEvents static-site UI work: creating or changing pages, components, tokens, states, responsive behavior, visual patterns, Astro layouts, or release UI acceptance. Enforces the runtime design-system catalog, explicit component versions, complete consumer migration, tests, docs, and immutable public preview evidence."
---

# Static Site Design System

Use `docs/features/static-site-pages/design-system/README.md` as the normative contract and render real runtime sources in `/lab/design-system/`.

## Workflow

1. Inventory affected page families and every consumer of the component or visual pattern.
2. Reuse approved `--ke-*` tokens and registered components. Do not create page-local equivalents.
3. For a new pattern, add an `experimental`/`candidate` catalog entry with all applicable states before production adoption.
4. For a material redesign of approved component `vN`, create `vN+1`:
   - show `vN` and `vN+1` side by side in the runtime catalog;
   - mark `vN` deprecated and name `vN+1` as replacement;
   - record the production consumer inventory and migration status;
   - migrate every consumer to `vN+1` in the same delivery unless a documented feature-flag rollout has an owner and removal deadline;
   - delete `vN` only after consumer search and regression evidence show zero production callers.
5. Never silently mutate an approved version. Same-version fixes are limited to non-contract corrections such as accessibility or browser bugs that do not change public API, geometry, hierarchy, or interaction behavior.
6. Update catalog registry/version attributes, `check-design-system.mjs`, `check-preview.mjs`, canonical docs, test scenarios, release evidence and `CHANGELOG.md` together.
7. After any material visual change, invoke the canonical `ui-three-way-conformance` skill from `lovekgd-design-system` for only the affected states/viewports. A case or explicit machine-readable `not_applicable` reason is required; do not reproduce the procedure here.
8. Run `npm --prefix site run check:design-system`, an immutable preview build, `check:preview`, responsive/a11y review and public URL smoke before sign-off.

## Release blockers

- approved component copied or restyled locally;
- new/materially changed component absent from the catalog;
- missing version or deprecated replacement;
- mixed production versions without a documented rollout contract;
- callers remain on the old version after migration is declared complete;
- catalog/demo markup diverges from the runtime component;
- missing immutable preview URL, git SHA, docs, tests or changelog.
- missing exact Git SoT/Penpot/Astro case (or justified `not_applicable`) after a material visual change.
