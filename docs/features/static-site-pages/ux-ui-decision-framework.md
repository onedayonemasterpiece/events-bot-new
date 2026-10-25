# UX/UI decision framework for public user journeys

> Status: proposed lightweight operating model.
> Scope: public static-site user journeys only.
> Excludes: admin/operator tooling, parser internals, infrastructure-only work and general project bureaucracy.
> Related: `docs/features/static-site-pages/design-system/README.md`, `docs/features/static-site-pages/design-system/ux-ui-decision-integration.md`, `docs/research/ux-ui-decision-framework/README.md`.

## Problem

New user-visible capabilities are being discovered and documented faster than their UX and UI solutions are designed. Examples include new collection pages, medallions, personalization states, feedback surfaces and related feature-specific screens. If each document carries its own unresolved UI notes, the team loses a single view of:

- what public user path is affected;
- what problem is still unresolved;
- whether the current site already has a state or page that can be inspected;
- which options have been considered;
- which decision was actually made;
- whether the decision uses the design system or accidentally forks it.

The goal is not to create a heavy DesignOps system. The goal is a small decision framework that turns scattered functional needs into prioritized, reviewable UX/UI decisions with evidence.

## Operating principle

A UX/UI gap is not a delivery task. It is a visible unresolved design risk in a public user journey.

The framework therefore has three small artifacts:

| Artifact | Purpose | Must stay small |
|---|---|---|
| Registry row | One unresolved UX/UI gap, priority, status, links | No full requirement copy, no sprint plan |
| Decision page | Human-readable problem, screenshots, live links, options and decision | No long research dump |
| Evidence map | Optional annotated screenshot showing why UI elements exist | Only core screens and states |

This is deliberately not a second backlog. The canonical feature need remains in the original product document. The registry records only the missing UX/UI decision and links back to the source.

## Minimum viable implementation

### Phase 0 — no spaceship

Implement first as documentation and static generated pages only:

```text
docs/features/static-site-pages/ux-ui-decision-framework.md
product-ux/registry/*.yaml
product-ux/decisions/*.md
product-ux/states/*.yaml
site/public/ux-evidence/<UXG-ID>/...
```

No database, no account system, no separate design platform, no mandatory GitHub Issues.

### Phase 1 — decision pages

Generate a simple static page per gap, for example:

```text
/lab/ux-decisions/
/lab/ux-decisions/UXG-COLLECTION-MEDALLIONS-001/
```

Each page should show, in this order:

1. one-sentence user problem;
2. priority and release relevance;
3. affected journey step;
4. links to live production/preview pages;
5. current screenshots, desktop and mobile;
6. 2-4 solution options;
7. option trade-offs;
8. design-system fit;
9. owner decision block;
10. prompt for generating additional options.

The page is an approval surface, not a CMS. In the first version, user input can be recorded by editing the Markdown/YAML record or commenting on the draft PR. A later version can add a form endpoint if the manual step becomes painful.

### Phase 2 — annotated evidence maps

For the core screens, add an optional hoverable screenshot map:

```text
/lab/ux-evidence/event-page-default/
/lab/ux-evidence/collection-page-default/
/lab/ux-evidence/date-listing-mobile/
```

Each mapped region answers:

- what is this element;
- which user outcome it supports;
- which journey step it belongs to;
- which component/pattern/version it uses;
- which metric or qualitative signal proves it matters;
- what evidence is missing.

This will expose abandoned or ornamental UI: an element with no journey, no outcome, no metric, no accessibility note and no design-system reference becomes suspicious.

Do not start with a complete map of the whole site. Start with the pages that create most launch risk.

## Registry row shape

Use one row per resolvable gap:

```yaml
schema_version: 1
id: UXG-COLLECTION-MEDALLIONS-001
title: Медальоны организаций на странице подборки
status: ui-exploring
priority: P1

scope:
  product_surface: public
  journey_id: discovery-collections
  journey_step: evaluate-collection
  gap_type: unresolved-composition
  excludes:
    - admin
    - operator

canonical_sources:
  - ref: docs/features/static-site-pages/collections.md#organization-medallions
    revision: main@<sha>
    need: показать пользователю, кто связан с подборкой

user_problem: >
  Пользователь смотрит подборку, но не получает быстрого объяснения,
  какие организации, площадки или редакционные источники делают её заслуживающей доверия.

user_outcome:
  primary: быстрее понять характер и источник подборки
  successful_exit: пользователь продолжает смотреть события или переходит к организации
  recovery_exit: если медальонов нет, подборка остаётся понятной без пустого блока

surfaces:
  - page_id: collection-page
    live_url: https://kenigevents.ru/<route-to-fill>
    preview_url: null
    states:
      - default
      - no-organizations
      - one-organization
      - several-organizations
      - missing-avatar
      - long-name
      - mobile
      - keyboard-focus

screenshots:
  - kind: production-observation
    viewport: desktop
    ref: site/public/ux-evidence/UXG-COLLECTION-MEDALLIONS-001/current-desktop.png
  - kind: production-observation
    viewport: mobile
    ref: site/public/ux-evidence/UXG-COLLECTION-MEDALLIONS-001/current-mobile.png

design_system:
  classification: candidate-pattern
  expected_reuse:
    - EventTokenMedallions
    - Badge
  new_pattern_needed: maybe
  design_system_record: docs/features/static-site-pages/design-system/ux-ui-decision-integration.md#candidate-patterns

options:
  - id: A
    title: Inline medallions under collection title
    status: proposed
  - id: B
    title: Compact source strip after description
    status: proposed
  - id: C
    title: Single expandable source summary
    status: proposed

decision:
  chosen_option: null
  owner_note: null
  alternative_sketch_ref: null
  requires_product_analysis: true

verification:
  required:
    - mobile-readability
    - keyboard-focus
    - missing-avatar
    - long-organization-name
    - design-system-token-use
```

## Statuses

Use fewer statuses than the research reports suggest:

```text
captured → triaged → framed → options → selected → ready-for-build → verified
```

Terminal states:

```text
parked | rejected | superseded
```

Meaning:

| Status | Meaning |
|---|---|
| captured | A user-visible unresolved UX/UI need was found. |
| triaged | It is deduplicated, scoped and prioritized. |
| framed | The user problem, journey step, states and current evidence are clear. |
| options | Comparable solution options exist. |
| selected | A preferred option exists, but build completeness may still be missing. |
| ready-for-build | States, copy, design-system fit, accessibility and verification are ready. |
| verified | Implemented and checked against real preview/production evidence. |

The key guardrail: `selected` is not the same as `ready-for-build`. A pretty main-state option is not enough if loading, empty, error, auth, mobile, focus or accessibility states are still unresolved.

## Prioritization

Use priority classes, not a fake exact score.

| Priority | When |
|---|---|
| P0 | Blocks a critical user task, causes data loss/privacy/accessibility harm, or breaks release trust. |
| P1 | Needed for the nearest release, affects a main public path, or has no acceptable fallback. |
| P2 | Recovery, empty/loading/error/offline state, medium-frequency secondary path, or visible inconsistency. |
| P3 | Visual polish, low-frequency improvement, or weakly evidenced idea. |

Within the same class sort by:

```text
release blocker
→ user harm
→ importance of user outcome
→ audience reach
→ design-system drift risk
→ uncertainty/novelty
→ age
```

Aging rule: if a feature is assigned to release and its UX/UI gap is not at least `ready-for-build`, it becomes a release blocker or must receive an explicit `parked/release-exception` decision.

## Decision page UX

The page must be comfortable to scan. Do not show a wall of metadata first.

Recommended layout:

```text
Title + one-line problem
Priority / status / owner / next action

Current evidence
- live page link
- preview page link
- desktop screenshot
- mobile screenshot

Decision needed
- what exactly must be decided
- what must not be changed

Options
A / B / C cards
- screenshot or sketch
- when this works
- risks
- design-system fit
- implementation note

Owner response
- choose A/B/C
- choose with changes
- reject all
- propose D with text and optional sketch

Safety review
- product value
- journey continuity
- accessibility
- design-system fit
- analytics/evidence
```

### Alternative sketch flow

Owner-proposed sketches are allowed, but they do not become accepted automatically.

Flow:

```text
owner attaches sketch or writes option D
→ option D is added with status submitted
→ product/design analysis compares it with A/B/C
→ trade-offs are documented
→ it can be selected, changed, parked or rejected
```

This protects the system from two common false paths:

- a visually attractive sketch that damages the user path;
- an owner preference that silently forks the design system.

## Predictive test run

Before implementation, test the process against likely owner behaviour.

| Simulated owner action | Expected process behaviour | Pass condition |
|---|---|---|
| Chooses option A quickly | Record rationale and check build completeness | Does not skip design-system/states gate |
| Says “I like B but move it lower” | Creates variant B2, not a vague comment | Variant has changed screenshot/spec or explicit copy note |
| Uploads hand sketch | Adds option D as submitted | D receives product/design trade-off review before selection |
| Rejects all options | Keeps gap in `options` or returns to `framed` | No implementation starts from rejected directions |
| Wants to approve a generated image | Blocks at `ready-for-build` until normalized to real components | Image remains exploration evidence only |
| Feature PR changes public UI without registry ref | CI asks for design-impact declaration | No silent frontend drift |
| A UI element has no product link in evidence map | Mark as missing evidence | Must be justified, redesigned or removed later |

## Generative option workflow

Use generative images for exploration, not approval.

Per single gap, the prompt should include:

- current desktop and mobile screenshots;
- live and preview URLs;
- user problem;
- immutable areas of the page;
- available design-system components;
- desired number of conceptually different approaches;
- mandatory states to consider.

A generated option becomes reviewable only after it is normalized into the project system:

```text
generated image
→ option card
→ design-system mapping
→ state list
→ accessibility risks
→ implementation note
→ owner decision
```

## Prompt template for a single-problem generative run

```markdown
You are designing one UX/UI option set for the KenigEvents static site.

Problem ID: <UXG-ID>
User problem: <one sentence>
Journey step: <journey_id / step>
Live page: <url>
Preview page: <url if available>
Current screenshots: <attach desktop and mobile>
Design-system references: <component/pattern/version links>
Do not change: navigation, brand lockup, typography scale, card geometry unless stated.

Generate 3 conceptually different UI options.
They must differ by interaction/composition model, not just visual styling.
For each option provide:
- short title;
- what user problem it solves;
- what it changes on the screen;
- why it may be better;
- main risk;
- design-system components it should reuse;
- states that must be checked.

Treat the images as exploration only, not final implementation.
```

## Screenshot/state evidence

Start with a small library:

| Screen/state | Why |
|---|---|
| Event page desktop/mobile | Core event decision path |
| Date listing desktop/mobile | Main discovery path |
| Collection page desktop/mobile | Collection and medallion work |
| Personalization/auth prompt | High-risk state and consent path |
| Empty/degraded state panel | Recovery and trust |
| Focus/navigation state | Accessibility and keyboard behaviour |

Each screenshot should know:

```yaml
page_id: collection-page
route: /collections/<slug>
kind: production-observation | deterministic-fixture | integrated-journey
viewport: mobile | desktop
source_revision: <sha or preview id>
live_url: <url>
fixture: null
captured_at: 2026-08-06
safe_for_ai: true
```

## Evidence map shape

For annotated screenshots:

```yaml
page_id: event-page
state: default-mobile
screenshot: site/public/ux-evidence/event-page-default-mobile.png
regions:
  - id: hero-title
    box: [24, 180, 342, 96]
    label: Event title
    user_outcome: understand what the event is
    journey_refs:
      - event-decision/read-event
    design_system_refs:
      - EventHero:v1
    analytics_refs:
      - event_detail_view
    evidence_status: supported
  - id: decorative-chip
    box: [24, 420, 80, 28]
    label: Unclear chip
    user_outcome: null
    evidence_status: missing
    action: justify-or-remove
```

This can be rendered by a static HTML page with an image, absolutely positioned hover rectangles and a side panel. No backend is required.

## Design-system gate

A UX/UI gap cannot become `ready-for-build` unless one of these is true:

```text
reuse-approved-pattern
candidate-pattern-created
temporary-exception-recorded
```

Required fields:

```yaml
design_system:
  classification: reuse-approved-pattern | candidate-pattern | new-token-needed | exception
  components:
    - EventTokenMedallions:v1
  tokens:
    - --ke-space-*
    - --ke-radius-*
  catalog_ref: /lab/design-system/#registry
  migration_note: null
```

If an option introduces new component semantics, it must create a candidate entry in the design-system catalog before implementation. If it only rearranges existing components, it must state which approved components are reused.

## Minimal CI later

Start non-blocking, then tighten.

1. Validate registry YAML schema.
2. Validate unique IDs and existing linked docs.
3. Warn when public UI files change without design-impact declaration.
4. Block only high-risk changes without `ready-for-build` registry reference.
5. Add screenshot manifest checks after the first useful screenshot library exists.

Do not block every visual adjustment from day one. The first aim is visibility and traceability.

## Recommended next step

Create 5-10 initial registry rows from existing documents, not more. Prioritize:

1. release-blocking public paths;
2. visible features with no UI/UX decision;
3. design-system drift risks;
4. states that are likely to be forgotten: empty, error, offline, auth, focus.

Then take one P1 gap, generate one decision page, and use it as the template for the rest.
