# UI gap visual exploration: evidence synthesis and LoveKGD operating model

> **Status:** research synthesis and proposed pilot contract; not yet an accepted implementation ADR.  
> **Date:** 2026-08-08.  
> **Canonical product and implementation repository:** `onedayonemasterpiece/events-bot-new`.  
> **Penpot topology:** two converging solutions and two plugins — Product Atlas and Design System.  
> **Primary research inputs:** two independent deep-research reports uploaded on 2026-08-08; their original names and SHA-256 hashes are recorded in the design-system research index.

## 1. Correction to the earlier interpretation

LoveKGD does **not** need a third independent product/design contour or a third Penpot plugin.

The target model has two parallel but converging Penpot solutions:

1. **Product Atlas plugin + Product Atlas file** — visual projection of product intent, Jobs, outcomes, journeys, capabilities, product gaps, evidence and decisions.
2. **Design System plugin + design-system files** — native resources, components, patterns, page archetypes, runtime evidence and the visual exploration of UI gaps.

`UI Exploration` is therefore a **working file inside the Design System solution**, not a third system alongside Product Atlas and the design system. It may have its own guarded Penpot file kind and namespace, but it is operated by the same Design System plugin and shares the same design-system identity, resources and delivery chain.

The relationship is:

```text
Product Atlas
product problem / UI gap / affected Job, journey and capability
        │
        │ stable ui_gap_id + context link
        ▼
Design System · UI Exploration file
visual alternatives + component/pattern/composition experiments
        │
        ├── selected design bundle → implementation
        ├── accepted reusable candidate → Resource Graph promotion review
        └── decision/evidence summary → Product Atlas
```

Product Atlas supplies the reason and product context. The Design System contour performs the visual work. GitHub remains canonical for decisions, implementation and test identity.

## 2. Assessment of the two research reports

### 2.1 Report 01 — primary evidence basis

The first report is the stronger source for project decisions because it consistently separates:

- findings directly supported by research or official documentation;
- transfer of practices from other contexts;
- local LoveKGD recommendations that still require a pilot.

Its most useful findings are:

- uncertain UI work benefits from several materially different alternatives before the first feedback cut;
- review of alternatives tends to produce stronger criticism than review of a single proposed answer;
- AI-generated images can stimulate both composition and component-level improvements, but can also cause design fixation;
- experimental components should remain distinct from stable design-system components;
- rejected and parked alternatives are useful when the design space is genuinely uncertain, but detailed rationale is overhead for trivial gaps;
- visual comparison can close the design-to-runtime loop but cannot replace functional, interaction or accessibility checks.

### 2.2 Report 02 — supplementary, not normative

The second report contributes useful practical vocabulary and examples:

- an exploration sandbox;
- local component incubation;
- preservation of rejected alternatives;
- explicit divergent and convergent areas;
- batch feedback rather than comment-by-comment execution.

However, several statements are stronger than the cited evidence supports. In particular, the report sometimes treats the following as established facts rather than implementation hypotheses:

- that strict physical separation of divergent and convergent work is always required;
- that monolithic Penpot/Figma files necessarily cause cascading breakage;
- that generated Penpot HTML/CSS is sufficiently semantic for direct production use;
- that Storybook/Chromatic is the only reliable bridge between design intent and code;
- that AI-image properties can be automatically converted into valid design tokens;
- that MCP should be part of the initial workflow.

Those claims must not become LoveKGD requirements without separate technical evidence. The report is preserved as research evidence, but the synthesis below uses it only where it agrees with better-supported findings or supplies a clearly labelled implementation idea.

## 3. Evidence-supported principles

### 3.1 Parallel alternatives before feedback

For an uncertain gap, the process should avoid the default sequence:

```text
one proposal
→ feedback
→ local correction
→ more feedback
→ accumulated compromise
```

The more defensible sequence is:

```text
several materially different directions
→ one review cut
→ comparative feedback
→ one coherent next iteration
```

This does not imply a mandatory number of alternatives. A large navigation hypothesis may need three or more directions; a narrowly bounded responsive inconsistency may need only a current state and one alternative.

### 3.2 AI imagery may improve components, not only layout

AI-generated UI imagery can contain useful ideas at several levels:

```text
page composition
block or product pattern
component anatomy
component visual treatment
state presentation
responsive behavior
interaction affordance
```

A generated shelf image may lead to a new rail composition, but it may also suggest a better card edge, scroll control, clipping treatment, selected state or density. A navigation image may yield a new menu surface or item treatment even when its overall page composition is rejected.

The image is therefore a **visual seed**, not a target implementation. The process should extract reusable claims from it and rebuild those claims with actual LoveKGD foundations, components and constraints.

To reduce fixation, an AI-inspired direction should not be the only alternative in the first divergent pass. At least one direction should be formed independently from the generated image.

### 3.3 Component evolution is a separate but connected track

A UI gap may simultaneously produce changes at different levels:

```text
page composition
product block or pattern
existing component variant
new local component candidate
responsive/interaction rule
```

The workflow must not force the operator to decide the abstraction level before exploration. The level becomes clearer while complete page variants are assembled and compared.

### 3.4 Experimental does not mean design-system accepted

A candidate starts locally in the gap that created it. It becomes a shared design-system resource only after its boundary, states, value and implementation evidence are understood.

A successful gap may end with:

- a promoted reusable component;
- a product pattern used only by one route family;
- a local page composition change;
- a responsive consumer rule;
- no implementation because the hypothesis was rejected.

The process is healthy only if all five outcomes are normal.

### 3.5 Feedback should produce an iteration package, not microtasks

Native Penpot comments should remain observations, constraints and owner decisions until a review cut is made. They should not automatically fan out into GitHub issues or individual agent calls.

The intended transformation is:

```text
comments across composition, blocks and candidates
→ themes, conflicts and shared causes
→ product and technical comparison
→ one iteration brief
→ one coherent regenerated package
```

This is both more consistent with parallel-prototyping evidence and better suited to a single operator.

## 4. Recommended Penpot topology

### 4.1 Product Atlas file

Product Atlas owns the visual projection of:

- the product problem;
- affected Job, journey, capability and outcome;
- the existence and importance of the UI gap;
- decision state;
- links to visual exploration, implementation and evidence.

It does not contain the full brainstorm, speculative component masters or all rejected page variants.

### 4.2 Design System · Resource Graph file

Resource Graph owns:

- current foundations and resources;
- accepted components and variants;
- accepted patterns and page archetypes;
- coverage and fragmentation evidence;
- exact browser `actual / baseline / diff` evidence;
- promotion review for mature candidates.

It should not become the primary divergent canvas.

### 4.3 Design System · UI Exploration file

The default topology is:

> **one UI Exploration Penpot file, one active meaningful UI gap = one Penpot page.**

This file is part of the Design System solution and is operated by the Design System plugin. It connects to current design-system resources but keeps local experimental objects isolated from Resource Graph until promotion.

Suggested pages:

```text
00 — Active gaps / index
10 — UI-GAP-…
11 — UI-GAP-…
12 — UI-GAP-…
80 — Shared candidates index
89 — Closed exploration archive
99 — Diagnostics
```

`80 — Shared candidates index` is a generated overview, not a second place where every experiment must be maintained manually. A candidate remains local to its gap until there is a reason to expose it across gaps.

A separate file per gap is reserved for an unusually large, long-lived or access-isolated investigation.

## 5. One gap page as a visual matrix

The page should support branching without becoming an unstructured infinite canvas.

### 5.1 Horizontal axis: whole iterations

```text
Context / current
→ Iteration A
→ Iteration B
→ Shortlist
→ Selected for build
→ Runtime review
```

A new iteration is a coherent snapshot of the whole proposed solution, not a collection of detached corrections.

### 5.2 Vertical axis: parallel design tracks

```text
Product intent and constraints
Page / journey composition
Blocks and product patterns
Components and variants
Interaction, state and responsive behavior
AI images / external references / sketches
Evaluation and decision notes
```

This creates a matrix in which a branch can begin at any level and later join a complete page variant.

Example:

```text
AI shelf image
├── ShelfCard visual candidate C2
├── ScrollControl candidate S1
└── Rail composition P3

P3 + C2 + S1
→ integrated page variant V4
```

The operator does not have to manage this as a formal graph. Stable IDs and parent relationships can be added automatically by the plugin when a board is registered or imported.

### 5.3 Required areas on the page

A practical gap page contains:

1. **Context strip** — `ui_gap_id`, problem, affected product entities, constraints, current runtime screenshot and Resource Graph revision.
2. **Text directions** — a small number of materially different ways to solve the problem.
3. **Visual seeds** — AI images, references and sketches, each optionally tagged by the idea it contributes.
4. **Local experiments** — component, block, composition and behavior candidates.
5. **Integrated alternatives** — complete page or state variants using those candidates.
6. **Review cut** — the set currently being compared and commented on.
7. **Selected for build** — the coherent future state and required scenarios.
8. **Parked/rejected strip** — thumbnails plus one-line rationale, not a backlog.
9. **Runtime evidence** — later browser actual, diff and acceptance result.

For a small gap, unused rows remain absent. The template must be elastic rather than a mandatory nine-stage ceremony.

## 6. Minimal operator workflow

The design goal is not to minimize machine work; it is to minimize **operator transactions** while preserving a reviewable decision.

### Step 1 — enter from Product Atlas

The operator follows the UI gap link from Product Atlas. The gap already has its product identity and context. No new task, duplicate brief or manual status entry is created.

### Step 2 — one update action

The Design System plugin opens in UI Exploration mode and performs preflight automatically.

`Обновить UI gap`:

- creates the page if it does not yet exist;
- imports or refreshes Product Atlas context;
- links current Resource Graph resources and runtime evidence;
- imports the newest agent-generated iteration package;
- preserves manual images, boards and comments;
- reports stale/current state.

The operator should not need separate actions for page creation, component import, evidence import or iteration import.

### Step 3 — visual work and comments

Text directions, AI images, manual sketches and generated native variants may be added. The operator reviews the whole page and leaves comments where judgment is needed.

These comments can cover multiple tracks at once. For example:

```text
The shelf direction is right, but variant B should use the denser card from C;
keep A as the conservative alternative; test the combined result on mobile and desktop.
```

### Step 4 — one batch action

`Собрать следующую итерацию` collects:

- the UI gap and Product Atlas context;
- current Resource Graph identities;
- selected visual seeds;
- all unresolved comments in the current review cut;
- candidate and integrated variant IDs;
- parked alternatives that must not be rediscovered;
- the previous iteration and required states.

It produces **one deterministic prompt/package** that asks the agent to:

1. separate observed problems from suggested solutions;
2. compare the integrated alternatives against product outcomes and constraints;
3. identify conflicting feedback and shared root causes;
4. revise composition, blocks and components as one system;
5. preserve explicitly parked alternatives;
6. generate the next complete iteration, not a list of micro-fixes;
7. return a compact change manifest and visual artifacts.

Product analysis is included in this same iteration request by default. A separate analytical request is needed only for a genuinely high-risk product decision, not for routine comparison of variants.

### Step 5 — run once and update once

The prompt is run through ChatGPT/agent tooling as one batch. The generated artifacts are committed to the integration repository. The operator then presses `Обновить UI gap` once to see the complete next iteration.

The normal loop is therefore:

```text
review visually + comment
→ one “build next iteration” action
→ one agent run
→ one update action
```

### Step 6 — one finalization action

When a coherent variant is ready for implementation, the operator uses:

`Зафиксировать для сборки`.

The plugin exports one design reference bundle containing:

- selected page/state images;
- native component and variant identities;
- local candidate identities;
- expected tokens and key geometry;
- responsive and interaction scenarios;
- fixtures/content constraints;
- selected, parked and rejected alternatives;
- product rationale and unresolved assumptions;
- approval identity and hashes.

This becomes the single implementation prompt/change package. It may contain many code changes across components, blocks and page composition, but it remains one product/design decision package.

## 7. One Design System plugin, two guarded file modes

A third UI Exploration plugin is not recommended.

The existing two-plugin architecture remains:

```text
Product Atlas plugin
Design System plugin
```

The Design System plugin supports two guarded file kinds:

```text
resource-graph
ui-exploration
```

It detects file kind on open and exposes only the relevant actions.

### 7.1 Resource Graph mode

The existing interaction remains conceptually:

- automatic preflight;
- `Обновить дизайн-систему`;
- `Собрать промпт по комментариям`.

### 7.2 UI Exploration mode

The maximum visible actions are:

1. `Обновить UI gap`;
2. `Собрать следующую итерацию`;
3. `Зафиксировать для сборки`.

No separate buttons are needed for:

- creating a page;
- registering every branch;
- importing each component;
- sending each comment;
- changing each status;
- exporting every artifact;
- updating Product Atlas manually.

Those are internal orchestration steps or generated metadata.

### 7.3 Guardrails

The same plugin must still fail closed:

- Resource Graph operations cannot mutate a UI Exploration file;
- UI Exploration sync cannot publish local candidates into the shared library;
- a Product Atlas file is rejected entirely;
- manually added images and boards are never deleted by managed sync;
- promotion requires an explicit accepted decision and later runtime evidence;
- comments are not converted into GitHub issues automatically.

Whether both modes should use one manifest with broader permissions or two immutable manifests backed by one logical Design System plugin is an implementation question for the pilot. The operator-facing model must remain one Design System tool, not a third workflow.

## 8. Experimental component lifecycle

The smallest useful lifecycle is:

```text
visual seed
→ local candidate
→ used in integrated alternative
→ selected for build/trial
→ implemented and runtime-reviewed
→ accepted local / promotion-ready / rejected / parked
→ optional Resource Graph promotion
```

### 8.1 Local first

A candidate initially belongs to the gap that gave it meaning. It remains on that page and may use local variants.

### 8.2 Shared candidate only when useful

A generated index may expose a candidate across gaps when:

- another independent gap wants to use it;
- it has a stable boundary and states;
- it is clearly not only a page-specific composition detail.

This is a signal, not an automatic promotion rule.

### 8.3 Promotion only after implementation evidence

Promotion into Resource Graph should require enough evidence that:

- an existing component or pattern cannot solve the need cleanly;
- component boundary and material states are understood;
- keyboard, focus, accessibility and interaction behavior are defined where relevant;
- the accepted implementation exists or is explicitly approved for implementation;
- material states/viewports have visual and functional coverage;
- reuse or foundational value is plausible.

A component may be accepted for one page without being promoted to the general design system.

## 9. Design reference and browser closure

Before code exists, the selected Penpot state is the `approved-design-reference`.

The implementation agent receives:

- reference PNG/SVG;
- native structure and component IDs;
- token and CSS/geometry hints;
- fixtures and states;
- product context and decision rationale.

After implementation:

```text
approved Penpot design reference
↔ browser actual screenshot
↔ visual/perceptual diff
+ geometry/style assertions
+ functional/interaction/accessibility tests
```

Once the browser result is accepted, it becomes the strict runtime regression baseline. It does not replace the original design reference; the two answer different questions:

- design reference — did implementation reach the intended future state?
- runtime baseline — did an already accepted implementation regress?

Accepted runtime evidence then flows into Resource Graph, and a compact decision/evidence summary flows back to Product Atlas.

## 10. Product Atlas linkage without duplicate work

Product Atlas should store only a compact reference record:

```yaml
ui_gap_id: UI-GAP-023
affected:
  job_ids: []
  journey_ids: []
  capability_ids: []
exploration:
  penpot_file_kind: ui-exploration
  page_id: ...
  current_iteration: B
  state: selected_for_build
selected_variant_id: ...
component_candidate_ids: []
parked_variant_ids: []
implementation_ref: ...
runtime_evidence_ref: ...
```

The operator should not maintain this record manually. It should be generated from the frozen exploration bundle and included in the next Product Atlas catalog update.

Product Atlas therefore answers:

> What product problem is affected, what decision was made, and what evidence exists?

The Design System exploration page answers:

> What alternatives were explored and how did the selected visual solution evolve?

## 11. Three pilot cases

### 11.1 Experimental menu

Tests reversibility:

- several materially different menu directions;
- local candidate components and states;
- integrated page variants;
- accept, reject or park without polluting Resource Graph;
- keyboard/focus/dismissal evidence only after implementation.

### 11.2 Scrolling shelf redesign

Tests parallel tracks:

- page placement and scroll composition;
- shelf/rail product pattern;
- card and control candidates;
- mobile/desktop behavior;
- AI imagery as a source of component improvements as well as layout ideas;
- one iteration prompt covering the complete shelf system.

### 11.3 Mobile bottom navigation on desktop

Tests that the process does not invent components unnecessarily:

- compare presence, absence and repositioning at desktop breakpoints;
- potentially change only an archetype/consumer rule;
- close with runtime assertions and no new component if that is the correct result.

## 12. Pilot success criteria

The pilot succeeds when:

1. A Product Atlas UI gap opens the correct exploration page without recreating the brief.
2. One page holds a coherent gap package without becoming an unstructured dump.
3. At least one iteration contains genuinely different alternatives before feedback.
4. AI images produce identifiable component/pattern ideas but do not become the sole design target.
5. Several related comments produce one coherent next iteration, not a set of microtasks.
6. The operator needs no more than one generation action and one update action per iteration.
7. A selected design can be frozen as one implementation/reference bundle.
8. A rejected hypothesis leaves no implementation backlog or Resource Graph pollution.
9. Accepted runtime evidence closes the loop into Resource Graph and Product Atlas.
10. A second operator could reconstruct the decision path from stable links without oral explanation.

## 13. Questions left for the pilot

The research does not determine:

- the optimal amount of visual material before one page becomes too heavy;
- the ideal number of alternatives for each type of gap;
- the maximum coherent size of one feedback batch;
- when a local candidate should appear in the shared candidate index;
- how reliably Penpot comments survive the chosen whole-iteration replacement technique;
- whether one Design System manifest can safely support both file modes or should use two manifests behind one logical plugin;
- which Penpot-to-browser visual comparison method yields an acceptable false-positive rate;
- whether direct prompt submission is worth automating after the copy/paste pilot.

These are implementation and workflow hypotheses. They should be measured in the three real pilot gaps rather than decided through additional abstract process design.

## 14. Recommended next implementation decision

Do not build a broad new workflow service.

The next concrete step is to extend the existing Design System architecture with a small `ui-exploration` pilot contract:

- one separate Penpot file inside the Design System solution;
- one page per active meaningful UI gap;
- local candidates on the gap page;
- iterations as whole-system snapshots;
- one batch prompt per review cut;
- the same Design System plugin, with context-aware guarded operations;
- generated links to Product Atlas and Resource Graph;
- no manual duplicate backlog or per-comment task fan-out.

The operator's irreducible work should be limited to judgment: looking, comparing, commenting and selecting. Everything else that is deterministic should be performed by the plugin and GitHub integration.
