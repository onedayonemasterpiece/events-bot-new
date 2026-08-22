---
name: static-site-design-system
description: "Use for any KenigEvents static-site UI work: creating or changing pages, components, tokens, states, responsive behavior, visual patterns, Astro layouts, or release UI acceptance. Enforces the runtime design-system catalog, explicit component versions, complete consumer migration, tests, docs, and immutable public preview evidence."
---

# Static Site Design System

Use `docs/features/static-site-pages/design-system/README.md` as the local
consumer contract and the upstream
`onedayonemasterpiece/lovekgd-design-system:docs/ui-source-of-truth-roundtrip.md`
as the cross-repository authority/lifecycle contract. Render real runtime
sources in `/lab/design-system/`.

## Workflow

1. Resolve the affected family's authority mode, lifecycle state, exact contract
   version/hash, latest Penpot receipt, and owner-review state before editing
   Astro. If the requested change originates in Penpot and has no recorded owner
   acceptance in the Git SoT, stop before implementation and complete the
   Git-SoT-first Penpot review loop.
2. Inventory affected page families and every consumer of the component or visual pattern.
3. Reuse approved `--ke-*` tokens and registered components. Do not create page-local equivalents.
4. For a new pattern, add an `experimental`/`candidate` catalog entry with all applicable states before production adoption.
5. For a material redesign of approved component `vN`, create `vN+1`:
   - show `vN` and `vN+1` side by side in the runtime catalog;
   - mark `vN` deprecated and name `vN+1` as replacement;
   - record the production consumer inventory and migration status;
   - migrate every consumer to `vN+1` in the same delivery unless a documented feature-flag rollout has an owner and removal deadline;
   - delete `vN` only after consumer search and regression evidence show zero production callers.
6. Never silently mutate an approved version. Same-version fixes are limited to non-contract corrections such as accessibility or browser bugs that do not change public API, geometry, hierarchy, or interaction behavior.
7. After owner Penpot acceptance, implement only in an isolated candidate branch
   and bind the same component ID, contract version/hash, state key, fixture,
   viewport, and candidate package SHA used by Penpot. Publish an immutable
   noindex preview for a separate owner phone/desktop review.
   During an owner-authorized interactive Astro ↔ Penpot conformance run, each
   exact case must also pass the upstream live-publication gate: publish that
   case to the verified Telegram review topic and persist its exact read-back
   receipt before starting the next case. Never defer ready comparisons to an
   end-of-run batch. A blocked Penpot export produces an immediate truthful
   diagnostic, not silence and not a visual `PASS`.
8. Do not merge/deploy/generate production UI until the owner approves the
   browser/device result and promotion, migration, release, and post-deploy
   conformance gates are satisfied.
9. Update catalog registry/version attributes, the applicable design-system
   contract checks, `check-preview.mjs`, canonical docs, test scenarios, release
   evidence and `CHANGELOG.md` together.
10. Run the applicable registered checks from `site/package.json` (at minimum
   `check:design-system-production-surfaces` and
   `check:design-system-iconography` when their contracts are affected), an
   immutable preview build, `check:preview`, responsive/a11y review and public
   URL smoke before sign-off. Never cite a command that is absent from the
   checked-out package scripts.

## Release blockers

- approved component copied or restyled locally;
- new/materially changed component absent from the catalog;
- missing version or deprecated replacement;
- mixed production versions without a documented rollout contract;
- callers remain on the old version after migration is declared complete;
- catalog/demo markup diverges from the runtime component;
- missing immutable preview URL, git SHA, docs, tests or changelog.
- Penpot-originated change implemented without a Git SoT decision/version and
  explicit owner Penpot acceptance;
- production release inferred from Penpot acceptance, comment resolution, or a
  green candidate preview without separate browser/device owner approval.
