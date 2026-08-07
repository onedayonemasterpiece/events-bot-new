# UI gap visual exploration research

This note indexes the two deep-research inputs used for the LoveKGD synthesis and records their provenance. The raw uploads are preserved in the originating ChatGPT research session; the repository stores the reviewed project synthesis rather than treating either raw report as a normative contract.

## Canonical synthesis and worked example

- [`ui-gap-visual-exploration-synthesis-2026-08-08.md`](ui-gap-visual-exploration-synthesis-2026-08-08.md) — evidence-led synthesis, corrected for the accepted two-plugin boundary and optimized for a single operator.
- [`ui-gap-synthetic-penpot-example-2026-08-08.md`](ui-gap-synthetic-penpot-example-2026-08-08.md) — synthetic gap-page matrix, automatic `05 — Recent changes` component timeline, sample change manifest and operator flow.

## Source inputs

1. `Вставленная ​​уценка(2).md`
   - size: `83112` bytes;
   - SHA-256: `194bda3a2eb766786e4439938dc9df7e448110a3ecea2dadfe884e30859837f8`;
   - assessment: primary evidence basis; consistently separates source-supported findings, transfer and LoveKGD recommendations.
2. `Вставленных ​​уценки (2)(1).md`
   - size: `81602` bytes;
   - SHA-256: `1d757e316d59cab201d953dcf3a8f173773bdf67afb57dcab1a12127fafe8e20`;
   - assessment: supplementary; useful sandbox/incubation/batch-feedback ideas, but several technical claims require separate verification.

## Corrected project boundary

LoveKGD has two converging Penpot solutions and two plugins:

```text
Product Atlas plugin + Product Atlas file
Design System plugin + Resource Graph / UI Exploration files
```

`UI Exploration` is a separate Penpot file **inside the Design System solution**, not a third independent system or plugin. Product Atlas supplies `ui_gap_id`, product context and decision linkage; the Design System contour owns visual alternatives, local candidates, design references and runtime closure.

## Preservation rule

The repository preserves reviewed synthesis, worked examples, decisions and provenance. It does not copy every intermediate chat message as normative documentation. New accepted conclusions must move from research into an explicit ADR or operational contract under `docs/features/static-site-pages/design-system/`.

## Application rule

```text
external research
→ reviewed synthesis
→ synthetic example
→ small UI Exploration pilot
→ owner decision / ADR
→ plugin and repository implementation
→ runtime evidence
```

The synthesis and synthetic example are proposed pilot contracts. Accepted implementation decisions belong in `docs/features/static-site-pages/design-system/`.
