# Current UI Decoder v1 — artifacts lane

## Scope

- Requirement: `R04-A`.
- Pinned source: `ef7aa62e45c60f7a12da6160f490719c0721ec03`.
- Output is AS-IS evidence modelling only.
- No `site/src`, Astro, CSS, runtime UI, Penpot, token or normalization file was changed.

## Implemented

- Added deterministic `scripts/current_ui_resource_graph/v1/artifacts.mjs` builder and fail-closed validator.
- Kept two independent systems:
  - `artifact-system.focus-egg-prototype-v1` — lab-only FocusEgg prototype;
  - `artifact-system.amber-research-collectible-v1` — non-production Amber research collectible.
- Retained their possible parent as `artifact-parent.collectibles-unresolved` with `NOT_MERGED`, `merge_allowed:false`, and `synthesis_allowed:false`.
- Recorded 11 state records and 8 exact source transitions.
- Recorded the Focus static catalogue baseline: 2 found, 4 eligible, 5 locked, 1 unavailable; progress 2/11.
- Restricted executable Focus behavior to FG-E12 after three distinct renderable saved events.
- Recorded the Amber hard gate: `siteMode !== 'production' && flag === 'tail'`; both production truth-table rows remain disabled.
- Added a bounded 13-case pairwise specimen plan with component selectors and capture requirements for DOM summaries, element screenshots, computed styles, geometry, CSS variables, accessibility, focus, hidden/open state, breakpoint context, and override source.
- Included both sides of all source-derived boundaries:
  - Focus artifact: 419/421 around 420;
  - Focus saved-list header: 759/761 around 760;
  - Focus catalogue: 679/681 and 919/921 around 680 and 920;
  - Amber collection: 429/431 and 849/851 around 430 and 850;
  - Amber mobile rail consumer: 719/721 around 720.
- Recorded required mismatches:
  - eligible-to-found Focus artifact glyph remains stale;
  - Focus catalogue storage override does not update the mark glyph;
  - stored FG-E06 may look found while progress excludes unavailable items;
  - Focus catalogue has no `storage` listener;
  - Amber was falsely associated with `family.transport` despite the source contract binding to `artifacts.collection`;
  - prior page evidence covers only empty 0/5, leaving found/dialog/rail capture gaps.
- Explicitly emitted no specimen observations and no browser/private-corpus capture claim.

## Files

- `scripts/current_ui_resource_graph/v1/artifacts.mjs`
- `tests/fixtures/current-ui-decoder-v1/artifacts/expected-inventory.json`
- `tests/test_current_ui_decoder_v1_artifacts.py`
- `.codex/lanes/current-ui-decoder-v1-artifacts/RESULTS.md`

## Verification

Passed:

```text
node --check scripts/current_ui_resource_graph/v1/artifacts.mjs
/home/dev/.venvs/events-bot-image-geometry/bin/pytest -q tests/test_current_ui_decoder_v1_artifacts.py
.........                                                                [100%]
9 passed in 0.69s
git diff --check
```

An optional combined run with `tests/test_current_ui_resource_graph.py` could not initialize the existing core fixture in this clean worktree because the worktree had no complete `site/node_modules`: first `@astrojs/compiler`, then `htmlparser2` was unavailable. The focused lane suite does not depend on those packages and passed. No third dependency workaround was attempted; integration owns the full dependency-installed run.

## Handoff

- The module exports `buildArtifactDecoderLane()`, granular builders, `validateArtifactDecoderLane()`, and `stableSerializeArtifactLane()`.
- `specimen_plan` rows use v1-compatible `component_id`, `logical_path`, `plan_status`, `required_contexts`, `evidence_claim_limit`, and `normalization_allowed` fields.
- Mismatch conclusions use the accepted reconciliation vocabulary.
- The integration/capsules lane must consume these records and later attach real controlled observations; this lane does not claim those captures exist.
