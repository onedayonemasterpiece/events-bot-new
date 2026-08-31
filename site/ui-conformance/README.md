# UI conformance corpus — C1 priority listings

This subtree is a **Git-only, append-only conformance input** for the current
Astro route census. It deliberately does not render or change an Astro route,
free-collection evidence, a design-system tuple, or Penpot.

## Authority and boundaries

- Astro route existence is pinned to `events-bot-new@64f75d10f7aff33fa616cee212878bd9d03673b1`.
- Five reused entities are pinned to the frozen current-A free-collection
  evidence tuple `w2-free-collection-visual-evidence-g12@c7c3e2367db8fd8865a735c8b9f5df1ef2b6efd1`.
- `event.real.4240` is the sole C1 append-only addition. It is required to
  exercise the observed `/segodnya/` route at the deterministic C1 clock; the
  frozen free-collection fixture set has no 2026-09-01 entity.
- Projection files contain references only. They must never redeclare event
  content, geometry, media, or a free-collection resolved case.
- A future route may only become `READY` after its route, shared corpus
  references, state packet, desktop scenario, and mobile scenario validate.

The records are review inputs, not a production data replacement. `registry`
keeps missing product/data contracts explicit instead of inventing routes or
semantics.

## Layout

- `route-archetype-registry.v1.json` — observed route/archetype census.
- `corpus/shared-event-corpus.v1.json` — one shared immutable entity corpus.
- `projections/` — priority route references only.
- `state-packets/` — deterministic clock and client state.
- `scenarios/` — desktop/mobile evidence scenarios.
- `schemas/` and `tests/` — offline structural contract.

Run the contract without dependencies:

```bash
node --test ui-conformance/tests/corpus-contract.test.mjs
```
