# R07 — Smart Update merge-identity prevention

## Outcome

Implemented and tested a bounded prevention guard for the proven theatre-tour/performance collision. No production database, publication, environment, or deployment was changed.

## Root cause evidence

Read-only query of the 2026-07-18 production snapshot found:

- 552 `merge_identity_gate` decisions: 402 `allow_merge`, 150 `skip_merge_side_effects`;
- every decision was recorded with `mode=shadow`;
- the exact `zakulise` source against events 5754–5757 produced 48 `skip_merge_side_effects` decisions (12 for each event) between 2026-07-03 and 2026-07-17, all in `shadow`.

Therefore the classifier recognized that the 14:30 theatre tour and the 18:00 plays were distinct, but shadow mode intentionally recorded the verdict without stopping merge side effects.

## Code and regression coverage

`smart_update_identity.py` now treats a collision as a deterministic structural veto only when all of these are present:

1. related place and overlapping date;
2. unrelated titles;
3. incompatible coarse event types;
4. two explicit, valid, non-default start times that differ;
5. no strong shared source/ticket/poster identity anchor.

This is deliberately **not** a date-only or date+venue rule. Missing/default time does not trigger it. A genuine correction with a strong specific identity anchor remains eligible for an LLM-approved merge.

The merge-gate prompt now explicitly states that same date and venue are insufficient identity evidence when title, type, explicit time, and specific ticket link conflict.

Exact regression coverage in `tests/test_smart_update_merge_identity_gate.py` includes:

- structural negative control: 14:30 `Экскурсия «Закулисье театра»` versus 18:00 `Женитьба`, including an intentionally wrong high-confidence LLM `allow_merge`;
- end-to-end shadow replay proving the source still attaches in observability-only mode;
- end-to-end enforce replay proving `skipped_identity_gate` and no source attachment/event mutation;
- positive same-performance source update in enforce;
- positive same-ticket time-correction control.

Validation: `15 passed` for the exact merge-identity test module.

## Deployment/configuration gate

**Current recommendation: keep production in `shadow`. Do not promote to `enforce` yet.** The exact incident is fixed at the decision layer, but a human precision audit of a representative shadow sample is still required before changing blocking behavior. Promotion must never be automatic.

There is no schema or data migration for this prevention change. When the audit blocker is cleared, the operator gate is:

1. integrate the commit into clean `origin/main` and run the exact negative/positive suite;
2. take a fresh read-only production DB snapshot;
3. review a representative sample of both `allow_merge` and `skip_merge_side_effects`, including legitimate same-date/same-venue multi-session events, recurring programmes, time corrections, and shared-ticket updates;
4. require no unexplained fail-safe/provider errors in the reviewed window and obtain explicit human approval of skip precision;
5. deploy the code from clean `main` while still in `shadow` and verify decision payloads remain healthy;
6. only after approval, explicitly set `SMART_UPDATE_MERGE_IDENTITY_GATE=enforce` and restart; do not change the code default as a substitute for controlled rollout;
7. run one controlled unsafe replay and one same-event positive update, verifying the unsafe replay has `skipped_identity_gate` with no event/source/poster/fact/job mutation while the positive update merges;
8. monitor allow/skip/error decisions and events 5754–5757 for at least 24 hours before closure.

Existing polluted event data requires a separate backed-up repair after prevention is deployed; this lane does not mutate it.

## Rollback

1. set `SMART_UPDATE_MERGE_IDENTITY_GATE=shadow` and restart;
2. verify new decision payloads report `mode=shadow`;
3. retain the code and observability evidence rather than deleting the gate;
4. if false positives blocked legitimate updates, reprocess only individually reviewed same-event sources after rollback—never bulk replay the skipped set.
