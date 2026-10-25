# UX/UI decision framework integration with the design system

> Status: proposed contract.
> Scope: public static-site user journeys and runtime design-system governance.
> Parent docs: `README.md`, `../ux-ui-decision-framework.md`.

## Why this exists

The UX/UI decision framework must not become a parallel design system. Its job is to identify unresolved public user-journey decisions, compare options and record owner decisions. The design system remains the authority for reusable visual language, tokens, component contracts, runtime catalog examples and migration rules.

The current static-site design system already defines:

- `site/src/styles/design-system.css` as the semantic `--ke-*` token source;
- runtime Astro components and product components as the reviewed implementation surface;
- `/lab/design-system/` as the checked preview catalog;
- statuses `experimental`, `candidate`, `approved`, `deprecated`;
- component versioning and mandatory migration rules.

Therefore every selected UX/UI decision must terminate in one of three design-system outcomes.

## Three allowed outcomes

| Outcome | Meaning | Required evidence |
|---|---|---|
| Reuse approved pattern | The option only composes existing approved components/tokens in a compatible context. | Component/version references, state coverage, screenshots. |
| Create candidate pattern | The option needs a new reusable component, pattern or semantic variant. | Candidate registry row, runtime catalog example, owner sign-off path. |
| Temporary exception | A one-off implementation is allowed for a named reason. | Owner, expiry condition, migration path and release evidence. |

Anything else is design-system drift.

## Required field in every UX/UI gap

```yaml
design_system:
  classification: reuse-approved-pattern | candidate-pattern | new-token-needed | exception
  components:
    - EventTokenMedallions:v1
  tokens:
    - --ke-space-*
    - --ke-radius-*
  catalog_ref: /lab/design-system/#registry
  status_required_before_build: approved | candidate-with-owner-exception
  migration_note: null
```

A gap can be `selected` without this field being complete, but cannot become `ready-for-build`.

## Option review rubric

Every solution option on a decision page must be evaluated against the design system.

| Check | Question | Red flag |
|---|---|---|
| Token fit | Does it use existing `--ke-*` roles for color, type, spacing, radius, elevation and motion? | New raw values or page-local geometry. |
| Component fit | Can it reuse approved runtime components? | Visually similar duplicate component. |
| Pattern fit | Is the composition already represented in `/lab/design-system/`? | A new recurring pattern hidden inside a page. |
| State fit | Are empty/loading/error/success/focus/disabled/mobile states represented? | Happy-path-only generated image. |
| Accessibility fit | Does the option preserve semantic HTML, focus order, accessible names and non-colour-only signals? | Visual-only evidence. |
| Migration fit | If it changes an approved component, is a new version or migration path recorded? | Silent rewrite of approved version. |

## Generated images are not design-system evidence

Generated images may be attached to a decision page only as exploration. They do not prove:

- correct token usage;
- correct component API;
- responsive behaviour;
- keyboard/focus behaviour;
- accessible names or announcements;
- state completeness;
- production feasibility.

Before selection, the image must be normalized into a real option card that names the intended components, states and trade-offs. Before build, it must be converted into runtime components or a code-first prototype.

## Candidate pattern flow

When a decision requires a new reusable element, do not implement it as page-local UI. Use this flow:

```text
UX/UI gap option
→ candidate pattern/component proposal
→ runtime catalog example in /lab/design-system/
→ token and state review
→ decision page selected
→ ready-for-build
→ implementation
→ verification screenshot/state evidence
→ approved or deprecated after release learning
```

The UX/UI decision page explains why the pattern is needed. The design-system catalog explains how the pattern behaves and how it is reused.

## Medallions example

A collection page may need organization medallions.

Wrong path:

```text
Generated pretty avatar row
→ copied into collection page CSS
→ later reused differently on event page
→ inconsistent spacing, avatar fallback and semantics
```

Correct path:

```text
UXG-COLLECTION-MEDALLIONS-001
→ options compare title-inline, source-strip and expandable-summary models
→ selected option references existing EventTokenMedallions if compatible
→ if not compatible, create CollectionSourceMedallions candidate pattern
→ catalog shows no-org, one-org, many-org, missing-avatar, long-name, mobile and focus states
→ collection implementation reuses the candidate/approved pattern
```

## Evidence map integration

Annotated screenshot maps should treat design-system references as first-class evidence.

Each region should include:

```yaml
regions:
  - id: organization-medallions
    label: Organization medallions
    user_outcome: understand source/trust/context of collection
    journey_refs:
      - discovery-collections/evaluate-collection
    design_system_refs:
      - EventTokenMedallions:v1
      - CollectionSourceMedallions:candidate
    evidence_status: supported | missing | disputed
```

This turns the evidence map into a product/design-system audit: elements with no product outcome or design-system reference can be reviewed for deletion, redesign or documentation.

## Pull request rule

A public UI PR should declare one of:

```yaml
design_impact:
  classification: none
```

```yaml
design_impact:
  classification: covered
  registry_refs:
    - UXG-COLLECTION-MEDALLIONS-001
  design_system_refs:
    - CollectionSourceMedallions:candidate
```

```yaml
design_impact:
  classification: new-gap
  registry_refs:
    - UXG-NEW-...
```

For `covered`, the referenced gap must be `ready-for-build` or have an explicit owner exception. For `new-gap`, implementation should not proceed beyond discovery/prototype code until the gap is framed and prioritized.

## Minimal adoption path

1. Add the `design_system` section to the first 5-10 UX/UI gap records.
2. For one real gap, create a decision page with 2-4 options and design-system-fit notes.
3. Add only the chosen or reusable pattern to `/lab/design-system/` as candidate.
4. Add screenshot/state evidence after a real preview build exists.
5. Tighten CI only after the team has used the workflow manually at least once.

The contract is intentionally small: every visual decision either reuses the system, extends it through a candidate pattern, or documents a temporary exception.
