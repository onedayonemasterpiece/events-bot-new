# Current UI Decoder v1 — contracts and capsules lane

## Outcome

Implemented a deterministic, source-pinned AS-IS candidate-contract and reconciliation-capsule layer without changing production UI, Astro, CSS, Penpot, tokens, or normalization decisions.

## Owned outputs

- `scripts/current_ui_resource_graph/v1/contracts.mjs`
  - 9 reconstructed `0.1.0-candidate` contracts:
    - Event Detail presentation states;
    - fragmented Button/CTA family;
    - media-heavy Event Detail resources;
    - rail, bus, and Kaup transport independently;
    - EventTokenMedallions;
    - Focus Egg and Amber independently.
  - pinned source bindings, relationship/confidence, anatomy, props/defaults/unions, slots, state/variant axes, valid/invalid combinations, responsive/media/a11y contracts, consumer/reachability, overrides, gaps/blockers/alternatives;
  - detached SHA-256 over each unsigned contract;
  - fail-closed closed enums and `NOT_MERGED`/candidate-only validation.
- `scripts/current_ui_resource_graph/v1/capsules.mjs`
  - 6 canonical directory models: event presentation, Button/CTA, media-heavy, transport, medallions, artifacts;
  - nine canonical file models per capsule;
  - fact/inference/open-question/decision separation;
  - source/specimen/real-page/contract refs and state/dependency maps;
  - all screenshots, observations, human reviews remain explicitly pending;
  - consolidated specimen plan, allowed-conclusion mismatch register, and unresolved blockers;
  - 809/raw style divergence cannot automatically become a mismatch.
- `tests/test_current_ui_decoder_v1_contracts.py`
  - deterministic, referential-integrity, closed-enum, tamper/hash, STOP, no-fabricated-evidence, and negative tests.

## Deterministic counts

- candidate contracts: 9
- reconciliation capsules: 6
- consolidated specimen-plan records: 61
- consolidated mismatch records: 9
- consolidated unresolved records: 11
- specimen observations claimed by this lane: 0
- screenshot/human-review claims by this lane: 0

## Validation

- Targeted: `9 passed` (`tests/test_current_ui_decoder_v1_contracts.py`).
- v1 + legacy decoder regression: `64 passed` (`tests/test_current_ui_decoder_v1_*.py tests/test_current_ui_resource_graph.py`).
- Node dependency note: the clean lane worktree used the integration worktree's already-installed `site/node_modules` via an ignored local symlink; no dependency files were changed or committed.
- `git diff --check`: clean.

## Integration API

```js
import { buildCandidateContracts } from './scripts/current_ui_resource_graph/v1/contracts.mjs';
import {
  buildReconciliationCapsules,
  buildConsolidatedSpecimenPlan,
  buildConsolidatedMismatchRecords,
  buildConsolidatedUnresolvedRecords,
  buildDecoderReconciliationBundle,
} from './scripts/current_ui_resource_graph/v1/capsules.mjs';

const bundle = buildDecoderReconciliationBundle({ eventPresentationRecords });
```

`eventPresentationRecords` should be the existing `eventPresentationFormats(...)` output from the exact integration run. Without it, the event-presentation contract remains explicitly `production-reachable-record-binding-pending`; it never fabricates runtime evidence.

## Remaining handoff blockers (intentional)

- controlled specimen execution and component-scoped captures;
- binding actual Event Detail presentation records and selected real-page evidence;
- human visual/semantic review of all six capsules;
- integration materialization into the compact snapshot's contract/capsule directories.

The lane is therefore implementation-complete but does not claim the overall decoder Go gate.
