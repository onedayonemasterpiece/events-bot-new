# UI conformance corpus — C2 priority listings

This subtree is a **Git-only conformance input** for the current Astro route
census. It deliberately does not render or change an Astro route,
free-collection evidence or candidate state, a design-system/foundations tuple,
or Penpot.

## Authority and boundaries

- Astro route existence and `event.real.4240` are pinned to
  `events-bot-new@64f75d10f7aff33fa616cee212878bd9d03673b1`.
- Five reused entities are immutable references into the frozen current-A
  free-collection evidence commit
  `c7c3e2367db8fd8865a735c8b9f5df1ef2b6efd1`; they are not copied source
  records and do not modify or promote that tuple.
- `event.real.4240` is the sole append-only addition. Its explicitly named
  `priority-listing-fields.v1` projection is field-for-field checked against
  the pinned full source record, including `venue_name` and `end_date`.
- Every source reference carries both byte-level source-file and canonical
  source-record SHA-256 hashes. `receipts/` binds the corpus, projections,
  schemas, state packet, scenarios, tests, and README with cross-file hashes.
- Projection files contain references only. They must never redeclare event
  content, geometry, media, or a free-collection resolved case.
- A future route may only become `READY` after its route, shared corpus
  references, state packet, desktop scenario, and mobile scenario validate.

`sha256-jcs-lite-v1` means UTF-8 SHA-256 over JSON with recursively sorted
object keys, array order preserved, no insignificant whitespace, and JavaScript
`JSON.stringify` primitive serialization. File hashes use exact Git bytes.

The records are review inputs, not a production data replacement. `registry`
keeps missing product/data contracts explicit instead of inventing routes or
semantics.

## Layout

- `route-archetype-registry.v1.json` — observed route/archetype census.
- `corpus/shared-event-corpus.v1.json` — immutable references and explicitly
  named projections only.
- `projections/` — priority route references only.
- `state-packets/` — deterministic clock and client state.
- `scenarios/` — desktop/mobile evidence scenarios.
- `schemas/` — executable JSON Schemas for every schema-governed document.
- `receipts/` — cross-file and source/projection binding hashes.
- `tests/` — dependency-free schema, fidelity, receipt, and negative gates.

Run the narrow contract without installing dependencies:

```bash
cd site
node --test ui-conformance/tests/corpus-contract.test.mjs
```
