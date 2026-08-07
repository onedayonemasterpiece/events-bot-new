# Architecture decision: canonical design system and Penpot review boundary

- **Status:** accepted
- **Date:** 2026-08-07
- **Scope:** «Полюбить Калининград» / «Полюбить Калининград Анонсы»
- **Decision owner:** product owner
- **Supersedes:** unresolved repository/source-of-truth choice recorded in `docs/research/design-system/README.md`

## Context

Two preserved research reports agree on a layered model — brand → shared foundations and semantic tokens → components → product patterns — but disagree on the repository model and final source of truth:

- report 01 recommends keeping the system with the Astro site until a second production consumer appears, with the released Git state as the canonical contract;
- report 02 recommends creating a separate monorepo immediately and treating Figma variables as the visual source of truth.

The current product already has one real consumer, an implemented semantic token layer, shared Astro components, product components, a runtime catalog and source-level contract checks. Moving that code into a package or second repository now would add synchronization and release failure modes without creating a second real consumer.

Penpot is nevertheless required as the visual review environment. It must not become a parallel hand-redrawn implementation that can drift from production.

## Decision

### 1. Canonical implementation stays in `events-bot-new`

The released Git state of `onedayonemasterpiece/events-bot-new` is the authoritative design-system contract:

- normative documentation: `docs/features/static-site-pages/design-system/README.md`;
- semantic tokens and shared foundations: `site/src/styles/design-system.css`;
- primitive UI: `site/src/components/design-system/`;
- product components and patterns: `site/src/components/`;
- real runtime catalog: `site/src/pages/lab/design-system/index.astro`;
- checks: `site/scripts/check-design-system.mjs` and generated preview assertions.

A visual value, component state, status or product pattern is not canonical merely because it exists in Penpot. It becomes canonical only when it is represented in the Git contract, rendered by the runtime catalog and covered by the required checks.

### 2. `lovekgd-design-system` is the Penpot delivery and review layer

`onedayonemasterpiece/lovekgd-design-system` does not own a second component library. It owns the tooling that projects the exact Astro runtime into Penpot and returns review evidence to the product workflow.

The projection contract is `observed-as-is`:

- each imported visual artifact is produced by Playwright from an exact `events-bot-new` commit;
- each artifact records its source repository, source SHA, source path, runtime route, selector, viewport, byte length and SHA-256;
- the Penpot plugin imports the verified artifact and does not redraw a substitute component;
- actual public page archetypes are imported alongside the component-lab views so a lab fixture cannot be mistaken for proof of page composition.

### 3. Penpot uses named lifecycle pages

The managed file structure is:

```text
00 — System map
20 — Foundations
30 — Core UI
40 — Announcements components
60 — Page archetypes
70 — AS-IS registry
80 — Candidate review
90 — Review archive
99 — Technical tests
```

AS-IS evidence, proposals, historical review evidence and technical load fixtures are never mixed on one anonymous canvas.

### 4. Native comments are the initial feedback transport

Reviewers use ordinary Penpot comment threads attached to managed boards. The plugin reads unresolved native comments with `comment:read` and creates a deterministic prompt containing:

- exact product runtime SHA;
- exact catalog SHA and revision;
- Penpot page and managed element ID;
- source URL and runtime route;
- viewport and observed status;
- Penpot thread number and comment text.

The plugin does not automatically change production code, create GitHub issues or promote a candidate. The prompt enters the normal implementation/review process. A candidate remains separate from AS-IS until explicit product-owner sign-off.

### 5. Comments survive runtime refreshes

The current-mirror update contract is:

- identical artifact → `noop`;
- changed artifact without comments → replace current board;
- changed artifact with comments → preserve the old board as review evidence and create a new current board;
- removed artifact with comments → preserve it in the review lane;
- new/replacement boards complete staging and verification before the current mirror switches;
- hash, download, upload or verification failure stops the sync and rolls back the current switch.

This prevents a product rebuild from silently deleting the decision context attached to the prior visual state.

### 6. Repository extraction is evidence-triggered

The canonical component implementation may be extracted into a package or separate repository only when at least one of the following is true:

- a second production product consumes the same foundations or components;
- an independently released channel requires the same machine-readable token package;
- a separate team needs an independent release lifecycle.

Extraction must be based on observed shared API, not hypothetical reuse. Product-specific event and listing patterns remain extensions even after a shared core is extracted.

## Consequences

### Positive

- one canonical implementation and one visual review projection;
- no invented Penpot components that merely resemble the site;
- review is tied to exact code and runtime evidence;
- native comments remain useful across refreshes;
- current small-team delivery avoids package and release overhead;
- a later second product can trigger extraction from proven shared boundaries.

### Costs and limitations

- Penpot boards are review artifacts, not freely editable canonical components;
- a reviewer must distinguish AS-IS evidence from candidate design work;
- the current feedback loop produces an implementation prompt rather than changing Git automatically;
- importing/opening the plugin in a real Penpot file remains the final environment-specific acceptance step for each published plugin revision.

## Accepted implementation evidence

- Penpot delivery repository: `onedayonemasterpiece/lovekgd-design-system`;
- merged delivery: PR `#4`, main commit `5d25d6c59f80af15fe93e62f68be74d33b2e09f4`;
- green workflow: run `31155694854`;
- immutable plugin commit: `2d917d1e39dbcac5ee9e88bcc6dd9f988e4b688c`;
- captured product runtime: `events-bot-new@c6a679dbbb3bbd65eb096becbd5976e7ccd67a26`;
- capture result: 9 named pages, 46 desktop/mobile runtime artifacts, 0 capture errors.

The operating procedure and immutable manifest URL are recorded in [`penpot-review-flow.md`](penpot-review-flow.md).
