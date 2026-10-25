# Research archive: UX/UI decision framework

> Status: preserved research inputs and synthesis for the static-site public user-journey UX/UI decision workflow.
> Date: 2026-08-06.
> Scope: public user journeys only; admin/operator flows are explicitly out of scope.

This directory preserves the research inputs that motivated the lightweight UX/UI decision framework for the KenigEvents static site. The goal is to avoid a second product backlog while still preventing user-visible features from being implemented without UX/UI framing, design-system alignment and verification evidence.

## Uploaded research inputs

The full uploaded source files from the consultation are tracked by filename and checksum so that the exact reports can be archived or re-imported by an agent with local file access.

| Source upload | Size | SHA-256 | Working title |
|---|---:|---|---|
| `Вставленный текст(20260806-083428).txt` | 49,428 bytes | `2c90eaad70d2ea1198e26103e2de0935b32a31cdd08adc0815df6f012bb6ca60` | Архитектура управления непроработанными UX/UI-изменениями |
| `Вставленный текст (2)(8).txt` | 79,955 bytes | `0031379d98e8c75a973935e57a30bd502c00c9f78b8e406afb0bb9ed933cf3ca` | Управление непроработанными UX/UI-изменениями в небольшой продуктовой команде |

## Consensus findings adopted for the project

1. Use a registry-first, evidence-backed workflow, not a second delivery backlog.
2. Keep one short record per resolvable UX/UI gap, linked to canonical requirements, user journeys, routes and states.
3. Keep detailed rationale, rejected options and visuals in design history, not in the registry row.
4. Treat generated images as exploration evidence only; do not approve them as final UI without product, accessibility and design-system review.
5. Prefer code-first prototypes for behaviour-heavy risks: auth, persistence, recovery, offline, validation, keyboard/focus and responsive reflow.
6. Keep screenshot/state evidence reproducible: route, viewport, auth mode, fixture, time, feature flags, source revision and artifact hashes.
7. Do not use production observations as golden baselines; use them as historical evidence only.
8. Block user-visible frontend changes only at the design-impact declaration and readiness-gate level; avoid heavy process for reuse of approved components.

## Project adaptation

The external reports describe a fairly complete process. For this repository the adopted shape must remain deliberately smaller:

- **Phase 0:** Markdown/YAML registry, one human-readable index page, and manually attached screenshots.
- **Phase 1:** generated decision pages for the highest-priority gaps; each page shows the problem, live page links, screenshots, 2-4 options, and a place to record owner choice or alternative sketch.
- **Phase 2:** optional screenshot annotation maps for a limited set of core screens, showing which UI elements are tied to product outcomes, journeys, metrics and design-system components.

Anything beyond that should be justified by repeated operational pain, not by architecture aesthetics.

## Relation to the design system

The framework must not create a parallel visual authority. The static-site design system already declares runtime tokens, components, component statuses and `/lab/design-system/` as the reviewable catalog. A UX/UI gap can move to `ready-for-build` only when its chosen option either:

- reuses approved design-system components and tokens;
- creates or updates a candidate component/pattern in the design-system catalog;
- or records a temporary exception with owner, expiry condition and migration path.

See the operational proposal in `docs/features/static-site-pages/ux-ui-decision-framework.md` and the design-system integration contract in `docs/features/static-site-pages/design-system/ux-ui-decision-integration.md`.
